"""
MWAA (Managed Workflows for Apache Airflow) Service Emulator.
REST API (path-based routing) for the MWAA control plane.
Supports: CreateEnvironment, GetEnvironment, DeleteEnvironment, UpdateEnvironment,
          ListEnvironments, CreateWebLoginToken, CreateCliToken.

When Docker is available, CreateEnvironment spins up a real Apache Airflow 3.x
container (standalone mode) and returns the actual host:port as the WebserverUrl.
DAGs are synced from the S3 bucket configured in DagS3Path.

The Airflow REST API (v2) is served directly by the container — no proxying needed.
Callers use the WebserverUrl from GetEnvironment to hit Airflow's /api/v2/ endpoints.
"""

import asyncio
import copy
import json
import logging
import os
import socket
import threading
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    apply_image_prefix,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("mwaa")

BASE_PORT = int(os.environ.get("MWAA_BASE_PORT", "18080"))
MWAA_PERSIST = os.environ.get("MWAA_PERSIST", "0").lower() in ("1", "true", "yes")
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")
DEFAULT_AIRFLOW_IMAGE = os.environ.get("MWAA_AIRFLOW_IMAGE", "apache/airflow:3.0.6")

_environments = AccountScopedDict()
_port_counter = [BASE_PORT]
_allocated_ports: set[int] = set()
_freed_ports: list[int] = []
_port_lock = threading.Lock()
_docker = None
_ministack_network = None


def get_state():
    envs = copy.deepcopy(_environments)
    for key in list(envs._data):
        envs._data[key].pop("_docker_container_id", None)
    return {"environments": envs}


def restore_state(data):
    if not data:
        return
    envs_data = data.get("environments")
    if not envs_data:
        return
    names_to_restart = []
    if isinstance(envs_data, AccountScopedDict):
        for key, env in list(envs_data._data.items()):
            env["_docker_container_id"] = None
            env["Status"] = "CREATING"
            _environments._data[key] = env
            names_to_restart.append((env.get("Name", key), env))
    else:
        for name, env in envs_data.items():
            env["_docker_container_id"] = None
            env["Status"] = "CREATING"
            _environments[name] = env
            names_to_restart.append((name, env))

    # Re-spin containers for persisted environments
    for name, env in names_to_restart:
        threading.Thread(target=_start_airflow_container, args=(name, env), daemon=True).start()


try:
    _restored = load_state("mwaa")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted state; continuing with fresh store")


def _s3_bucket_from_arn(bucket_arn: str) -> str:
    try:
        spec = parse_arn(bucket_arn)
    except ArnParseError as exc:
        raise ValueError("SourceBucketArn must be an S3 bucket ARN.") from exc

    if (
        spec.service != "s3"
        or spec.region
        or spec.account_id
        or not spec.resource
        or "/" in spec.resource
        or ":" in spec.resource
    ):
        raise ValueError("SourceBucketArn must be an S3 bucket ARN.")
    return spec.resource


def _validate_source_bucket_arn(bucket_arn: str):
    if not bucket_arn:
        return None
    try:
        _s3_bucket_from_arn(bucket_arn)
    except ValueError as exc:
        return error_response_json("ValidationException", str(exc), 400)
    return None


def _get_docker():
    global _docker
    if _docker is None:
        try:
            import docker
            _docker = docker.from_env()
        except Exception:
            pass
    return _docker


def _get_ministack_network(docker_client):
    global _ministack_network
    if _ministack_network is not None:
        return _ministack_network or None
    if DOCKER_NETWORK:
        _ministack_network = DOCKER_NETWORK
        return DOCKER_NETWORK
    try:
        self_container = docker_client.containers.get(
            os.environ.get("HOSTNAME", ""))
        nets = list(self_container.attrs["NetworkSettings"]["Networks"].keys())
        if nets:
            _ministack_network = nets[0]
            return _ministack_network
    except Exception:
        _ministack_network = ""
    return None


def _next_port():
    with _port_lock:
        if _freed_ports:
            port = _freed_ports.pop()
        else:
            port = _port_counter[0]
            _port_counter[0] += 1
        _allocated_ports.add(port)
    return port


def _release_port(port):
    if not port:
        return
    with _port_lock:
        if port in _allocated_ports:
            _allocated_ports.discard(port)
            _freed_ports.append(port)


def _wait_for_port(host, port, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(2)
    return False


def _capture_v2_admin_password(env, container):
    """Pull the Airflow 2 standalone admin password out of the container so
    proxied REST calls can authenticate. Standalone writes the password to
    ``/opt/airflow/standalone_admin_password.txt`` on first launch.
    """
    for _ in range(12):  # up to ~60s — standalone writes the file after webserver init
        try:
            rc, out = container.exec_run(
                "cat /opt/airflow/standalone_admin_password.txt"
            )
            if rc == 0:
                pw = (out.decode("utf-8") if isinstance(out, bytes) else str(out)).strip()
                if pw:
                    env["_v2_admin_password"] = pw
                    logger.info("MWAA: captured Airflow 2 standalone admin password for %s", env.get("Name"))
                    return
        except Exception:
            pass
        time.sleep(5)
    logger.warning("MWAA: failed to capture Airflow 2 standalone admin password for %s", env.get("Name"))


def _sync_dags_from_s3(env, docker_client, container):
    """Copy DAG files from ministack S3 into the Airflow container's dags folder.

    Reads directly from the S3 service's internal ``_buckets`` store rather
    than going through HTTP — same process, same account context, much faster.
    """
    try:
        from ministack.services import s3 as s3_svc
        bucket_arn = env.get("SourceBucketArn", "")
        bucket_name = _s3_bucket_from_arn(bucket_arn) if bucket_arn else ""
        dag_path = env.get("DagS3Path", "dags/")

        if not bucket_name:
            return

        bucket = s3_svc._buckets.get(bucket_name)
        if not bucket:
            logger.info("MWAA: source bucket %s not found in S3", bucket_name)
            return

        # Collect every object under DagS3Path. S3 keys are flat strings; treat
        # DagS3Path as a prefix and recurse implicitly (any key starting with
        # it is in scope).
        prefix = dag_path if dag_path.endswith("/") else dag_path + "/"
        matched = [
            (key, obj) for key, obj in bucket.get("objects", {}).items()
            if key.startswith(prefix) and key.endswith(".py")
        ]
        if not matched:
            logger.info("MWAA: no DAGs found in s3://%s/%s", bucket_name, prefix)
            return

        import io
        import tarfile

        tar_buffer = io.BytesIO()
        with tarfile.open(fileobj=tar_buffer, mode="w") as tar:
            for obj_key, obj_data in matched:
                relative = obj_key[len(prefix):].lstrip("/")
                if not relative:
                    continue
                body = obj_data.get("body", b"") if isinstance(obj_data, dict) else b""
                if isinstance(body, str):
                    body = body.encode("utf-8")
                info = tarfile.TarInfo(name=f"dags/{relative}")
                info.size = len(body)
                info.mode = 0o644
                info.mtime = int(time.time())
                tar.addfile(info, io.BytesIO(body))

        tar_buffer.seek(0)
        container.put_archive("/opt/airflow", tar_buffer)
        logger.info(
            "MWAA: synced %d DAG file(s) from s3://%s/%s",
            len(matched), bucket_name, prefix,
        )
    except Exception:
        logger.exception("MWAA: failed to sync DAGs from S3")


def _start_airflow_container(env_name, env):
    docker_client = _get_docker()
    if not docker_client:
        logger.warning("MWAA: Docker not available — environment %s will be stub-only", env_name)
        env["Status"] = "AVAILABLE"
        env["WebserverUrl"] = f"localhost:{_next_port()}"
        return

    host_port = _next_port()
    env["_host_port"] = host_port
    ms_network = _get_ministack_network(docker_client)

    airflow_version = env.get("AirflowVersion", "3.0.6")
    is_v3 = airflow_version.startswith("3.")
    image = os.environ.get("MWAA_AIRFLOW_IMAGE", f"apache/airflow:{airflow_version}")
    image = apply_image_prefix(image)

    container_port = 8080
    container_env = {
        "AIRFLOW__CORE__EXECUTOR": "SequentialExecutor",
        "AIRFLOW__CORE__LOAD_EXAMPLES": "false",
        "AIRFLOW__CORE__DAGS_FOLDER": "/opt/airflow/dags",
        # Force the dag-processor to scan aggressively so DAGs synced after
        # container boot become visible within seconds, not minutes. MWAA in
        # real AWS lets users tune this via AirflowConfigurationOptions; we
        # default to fast to keep local iteration tight.
        "AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL": "5",
        "AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL": "5",
    }

    if is_v3:
        # Airflow 3: SQLAlchemy conn under [database], auth via Simple Auth Manager
        container_env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = "sqlite:////opt/airflow/airflow.db"
        container_env["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS"] = "true"
    else:
        # Airflow 2: SQLAlchemy conn under [core] or [database], auth via basic_auth backend
        container_env["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = "sqlite:////opt/airflow/airflow.db"
        container_env["AIRFLOW__API__AUTH_BACKENDS"] = "airflow.api.auth.backend.basic_auth"
        container_env["_AIRFLOW_WWW_USER_USERNAME"] = "admin"
        container_env["_AIRFLOW_WWW_USER_PASSWORD"] = "admin"

    env["_is_v3"] = is_v3

    # Merge user-supplied Airflow config options
    for key, value in env.get("AirflowConfigurationOptions", {}).items():
        env_key = key.replace(".", "__").upper()
        container_env[f"AIRFLOW__{env_key}"] = value

    try:
        container_name = f"ministack-mwaa-{env_name}"

        # Remove stale container from previous runs (same pattern as RDS)
        try:
            existing = docker_client.containers.get(container_name)
            existing.remove(force=True)
        except Exception:
            pass

        container_kwargs = dict(
            image=image,
            detach=True,
            command="standalone",
            environment=container_env,
            ports={f"{container_port}/tcp": host_port},
            name=container_name,
            labels={"ministack": "mwaa", "env_name": env_name},
        )

        if ms_network:
            container_kwargs["network"] = ms_network

        if MWAA_PERSIST:
            container_kwargs["volumes"] = {
                f"ministack-mwaa-{env_name}-dags": {"bind": "/opt/airflow/dags", "mode": "rw"},
                f"ministack-mwaa-{env_name}-db": {"bind": "/opt/airflow", "mode": "rw"},
            }

        container = docker_client.containers.run(**container_kwargs)
        env["_docker_container_id"] = container.id

        internal_host = "localhost"
        internal_port = host_port

        if ms_network:
            container.reload()
            networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
            container_ip = networks.get(ms_network, {}).get("IPAddress", "")
            if container_ip:
                internal_host = container_ip
                internal_port = container_port

        env["WebserverUrl"] = f"{internal_host}:{internal_port}"

        def _bg_init():
            if _wait_for_port(internal_host, internal_port, timeout=120):
                logger.info("MWAA: Airflow container for %s ready at %s:%s",
                            env_name, internal_host, internal_port)
                env["Status"] = "AVAILABLE"
                # Sync DAGs from S3 once Airflow is ready
                _sync_dags_from_s3(env, docker_client, container)
                # Airflow 2 standalone generates a random admin password and
                # writes it to standalone_admin_password.txt. Capture it so
                # _invoke_rest_api can authenticate against /api/v1/ — basic
                # auth `admin:admin` from the env vars only takes effect
                # outside standalone mode. v3 uses Simple Auth Manager with
                # ALL_ADMINS=true so no token is needed.
                if not is_v3:
                    _capture_v2_admin_password(env, container)
            else:
                logger.warning("MWAA: Airflow container for %s not ready after timeout", env_name)
                env["Status"] = "CREATE_FAILED"
                env.setdefault("LastUpdate", {})["Error"] = {
                    "ErrorCode": "CONTAINER_STARTUP_TIMEOUT",
                    "ErrorMessage": "Airflow container did not become healthy within 120s",
                }

        threading.Thread(target=_bg_init, daemon=True).start()

    except Exception as e:
        logger.exception("MWAA: failed to start Airflow container for %s", env_name)
        env["Status"] = "CREATE_FAILED"
        env["WebserverUrl"] = ""
        env.setdefault("LastUpdate", {})["Error"] = {
            "ErrorCode": "CONTAINER_LAUNCH_FAILED",
            "ErrorMessage": str(e)[:500],
        }


def _build_env_response(env):
    """Build the Environment response object (strip internal fields)."""
    result = {k: v for k, v in env.items() if not k.startswith("_")}
    return result


# ── API Handlers ──────────────────────────────────────────────────────────────


def _create_environment(method, path, headers, body, query_params):
    data = json.loads(body) if body else {}
    name = path.strip("/").split("/")[-1]

    if name in _environments:
        # AWS MWAA service model has no ResourceAlreadyExistsException — the
        # documented errors for CreateEnvironment are ValidationException,
        # InternalServerException, and ServiceUnavailableException only.
        return error_response_json("ValidationException",
                                   f"Environment {name} already exists", 400)

    account_id = get_account_id()
    region = get_region()
    source_bucket_arn = data.get("SourceBucketArn", "")
    validation_error = _validate_source_bucket_arn(source_bucket_arn)
    if validation_error:
        return validation_error

    env = {
        "Name": name,
        "Arn": f"arn:aws:airflow:{region}:{account_id}:environment/{name}",
        "Status": "CREATING",
        "AirflowVersion": data.get("AirflowVersion", "3.0.6"),
        "EnvironmentClass": data.get("EnvironmentClass", "mw1.small"),
        "WebserverAccessMode": data.get("WebserverAccessMode", "PUBLIC_ONLY"),
        "SourceBucketArn": source_bucket_arn,
        "DagS3Path": data.get("DagS3Path", "dags/"),
        "ExecutionRoleArn": data.get("ExecutionRoleArn", ""),
        "NetworkConfiguration": data.get("NetworkConfiguration", {}),
        "AirflowConfigurationOptions": data.get("AirflowConfigurationOptions", {}),
        "LoggingConfiguration": data.get("LoggingConfiguration", {}),
        "MaxWorkers": data.get("MaxWorkers", 5),
        "MinWorkers": data.get("MinWorkers", 1),
        "Schedulers": data.get("Schedulers", 2),
        "MaxWebservers": data.get("MaxWebservers", 2),
        "MinWebservers": data.get("MinWebservers", 2),
        "WebserverUrl": "",
        "Tags": data.get("Tags", {}),
        "CreatedAt": time.time(),
        "LastUpdate": {"Status": "SUCCESS"},
        "ServiceRoleArn": f"arn:aws:iam::{account_id}:role/aws-service-role/airflow.amazonaws.com/AWSServiceRoleForAmazonMWAA",
        "_docker_container_id": None,
    }

    _environments[name] = env

    # Start container in background
    threading.Thread(target=_start_airflow_container, args=(name, env), daemon=True).start()

    return json_response({"Arn": env["Arn"]}, status=200)


def _get_environment(method, path, headers, body, query_params):
    name = path.strip("/").split("/")[-1]
    env = _environments.get(name)
    if not env:
        return error_response_json("ResourceNotFoundException",
                                   f"Environment {name} not found", 404)
    return json_response({"Environment": _build_env_response(env)})


def _delete_environment(method, path, headers, body, query_params):
    name = path.strip("/").split("/")[-1]
    env = _environments.get(name)
    if not env:
        return error_response_json("ResourceNotFoundException",
                                   f"Environment {name} not found", 404)

    container_id = env.get("_docker_container_id")
    if container_id:
        docker_client = _get_docker()
        if docker_client:
            try:
                container = docker_client.containers.get(container_id)
                container.stop(timeout=5)
                container.remove(force=True)
            except Exception:
                logger.debug("MWAA: container cleanup for %s failed (may be already gone)", name)

    _release_port(env.get("_host_port"))
    del _environments[name]
    return json_response({}, status=200)


def _update_environment(method, path, headers, body, query_params):
    data = json.loads(body) if body else {}
    name = path.strip("/").split("/")[-1]
    env = _environments.get(name)
    if not env:
        return error_response_json("ResourceNotFoundException",
                                   f"Environment {name} not found", 404)

    if "SourceBucketArn" in data:
        validation_error = _validate_source_bucket_arn(data["SourceBucketArn"])
        if validation_error:
            return validation_error

    for field in ("AirflowConfigurationOptions", "LoggingConfiguration",
                  "MaxWorkers", "MinWorkers", "Schedulers",
                  "EnvironmentClass", "ExecutionRoleArn", "SourceBucketArn",
                  "DagS3Path", "NetworkConfiguration", "WebserverAccessMode"):
        if field in data:
            env[field] = data[field]

    env["LastUpdate"] = {"Status": "SUCCESS"}
    return json_response({"Arn": env["Arn"]})


def _list_environments(method, path, headers, body, query_params):
    names = list(_environments.keys())
    return json_response({"Environments": names})


def _create_web_login_token(method, path, headers, body, query_params):
    data = json.loads(body) if body else {}
    parts = [p for p in path.strip("/").split("/") if p]
    name = data.get("Name") or (parts[-1] if parts else "")
    env = _environments.get(name)
    if not env:
        return error_response_json("ResourceNotFoundException",
                                   f"Environment {name} not found", 404)

    webserver_url = env.get("WebserverUrl", "")
    token = new_uuid()

    return json_response({
        "WebToken": token,
        "WebServerHostname": webserver_url,
        "IamIdentity": f"arn:aws:iam::{get_account_id()}:user/ministack",
        "AirflowIdentity": "admin",
    })


def _create_cli_token(method, path, headers, body, query_params):
    data = json.loads(body) if body else {}
    parts = [p for p in path.strip("/").split("/") if p]
    name = data.get("Name") or (parts[-1] if parts else "")
    env = _environments.get(name)
    if not env:
        return error_response_json("ResourceNotFoundException",
                                   f"Environment {name} not found", 404)

    webserver_url = env.get("WebserverUrl", "")
    token = new_uuid()

    return json_response({
        "CliToken": token,
        "WebServerHostname": webserver_url,
    })


async def _invoke_rest_api(method, path, headers, body, query_params):
    """Proxy InvokeRestApi to the local Airflow container's REST API.

    The outbound HTTP call uses the blocking ``requests`` client off-loaded
    to a worker thread so it does not stall the single-process event loop
    while Airflow processes the request (which can take seconds).

    boto3 places the environment ``Name`` in the URL path (``/restapi/{Name}``)
    per the MWAA service model, not in the JSON body. Extract from the path,
    fall back to the body for callers that bypass the SDK.
    """
    data = json.loads(body) if body else {}
    parts = [p for p in path.strip("/").split("/") if p]
    name = (parts[-1] if parts else "") or data.get("Name", "")
    env = _environments.get(name)
    if not env:
        return error_response_json("ResourceNotFoundException",
                                   f"Environment {name} not found", 404)

    webserver_url = env.get("WebserverUrl", "")
    if not webserver_url:
        return error_response_json("InternalServerException",
                                   "Environment webserver not available", 500)

    api_method = (data.get("Method") or "GET").upper()
    if api_method not in ("GET", "POST", "PATCH", "DELETE"):
        return error_response_json("ValidationException",
                                   f"Unsupported method: {api_method}", 400)

    api_path = data.get("Path", "/")
    api_body = data.get("Body", {})

    is_v3 = env.get("_is_v3", True)
    api_base = "/api/v2" if is_v3 else "/api/v1"
    url = f"http://{webserver_url}{api_base}{api_path}"

    req_headers = {"Content-Type": "application/json"}
    if not is_v3:
        # Airflow 2 standalone generates a random admin password; we captured
        # it after the container reached AVAILABLE. Fall back to admin/admin
        # if capture failed so the caller at least sees a clean 401 instead
        # of a TypeError.
        import base64
        pw = env.get("_v2_admin_password") or "admin"
        creds = base64.b64encode(f"admin:{pw}".encode()).decode()
        req_headers["Authorization"] = f"Basic {creds}"

    def _do_call():
        import requests as req_lib
        if api_method == "GET":
            return req_lib.get(url, headers=req_headers, timeout=30)
        if api_method in ("POST", "PATCH"):
            return req_lib.request(api_method, url, headers=req_headers, json=api_body, timeout=30)
        return req_lib.delete(url, headers=req_headers, timeout=30)

    try:
        resp = await asyncio.to_thread(_do_call)
        ctype = resp.headers.get("content-type", "")
        payload = resp.json() if ctype.startswith("application/json") else {"body": resp.text}
        return json_response({
            "RestApiStatusCode": resp.status_code,
            "RestApiResponse": payload,
        })
    except Exception as e:
        logger.exception("MWAA InvokeRestApi failure")
        # AWS doesn't leak the underlying Python exception text. The
        # InternalServerException message is opaque to the caller.
        return error_response_json("InternalServerException",
            "InternalServerException: An internal error has occurred.", 500)


# ── Request Router ────────────────────────────────────────────────────────────


async def handle_request(method, path, headers, body, query_params):
    """Route MWAA REST API requests.

    MWAA uses two API endpoints in AWS:
      - api.airflow.{region}.amazonaws.com — management (CRUD environments)
      - env.airflow.{region}.amazonaws.com — runtime (tokens, CLI, InvokeRestApi)

    In ministack, both are served from the same handler. Paths match the
    botocore MWAA service model exactly:
      PUT    /environments/{Name}    → CreateEnvironment
      GET    /environments/{Name}    → GetEnvironment
      PATCH  /environments/{Name}    → UpdateEnvironment
      DELETE /environments/{Name}    → DeleteEnvironment
      GET    /environments           → ListEnvironments
      POST   /webtoken/{Name}        → CreateWebLoginToken (runtime)
      POST   /clitoken/{Name}        → CreateCliToken (runtime)
      POST   /restapi/{Name}         → InvokeRestApi (runtime)
    """
    clean_path = path.rstrip("/")

    # CreateWebLoginToken (real AWS path: /webtoken/{Name})
    if method == "POST" and clean_path.startswith("/webtoken/"):
        return _create_web_login_token(method, path, headers, body, query_params)

    # CreateCliToken (real AWS path: /clitoken/{Name})
    if method == "POST" and clean_path.startswith("/clitoken/"):
        return _create_cli_token(method, path, headers, body, query_params)

    # InvokeRestApi
    if method == "POST" and clean_path.startswith("/restapi/"):
        return await _invoke_rest_api(method, path, headers, body, query_params)

    # ListEnvironments
    if method == "GET" and clean_path in ("/environments", "/api/environments"):
        return _list_environments(method, path, headers, body, query_params)

    # CRUD on /environments/{Name}
    if clean_path.startswith("/environments/"):
        if method == "PUT":
            return _create_environment(method, path, headers, body, query_params)
        if method == "GET":
            return _get_environment(method, path, headers, body, query_params)
        if method == "PATCH":
            return _update_environment(method, path, headers, body, query_params)
        if method == "DELETE":
            return _delete_environment(method, path, headers, body, query_params)

    return error_response_json("ValidationException",
                               f"Unknown MWAA path: {method} {path}", 400)


def reset():
    """Stop all containers and clear state."""
    docker_client = _get_docker()
    for name, env in list(_environments.items()):
        container_id = env.get("_docker_container_id")
        if container_id and docker_client:
            try:
                container = docker_client.containers.get(container_id)
                container.stop(timeout=5)
                container.remove(force=True)
            except Exception:
                pass
        _release_port(env.get("_host_port"))
    _environments.clear()
