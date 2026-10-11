# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Managed Service for Apache Flink data plane: Docker-backed job execution.

The control plane (``kinesisanalyticsv2``) owns application records and status;
this module owns the containers. When Docker is available, starting an
application launches a JobManager and a TaskManager from the official Apache
Flink image for the application's runtime, fetches the application jar from
MiniStack S3 and submits it through the Flink REST API. Stop, snapshot and
restore map to Flink savepoints.

Interface used by the control plane (all synchronous; none raise):

- ``available()``: Docker is reachable.
- ``supports(runtime)``: this data plane can run jobs for the runtime
  (``FLINK-1_19`` and ``FLINK-1_20``); other runtimes stay control plane only.
- ``start(app, savepoint_path, on_status)``: launch the cluster and submit the
  job in a background thread; ``on_status("RUNNING")`` or
  ``on_status("FAILED", message)`` reports the outcome.
- ``snapshot(app, snapshot_name)``: take a savepoint; returns its path, or
  ``None`` if it failed.
- ``stop(app, force, take_snapshot)``: stop the job and remove the containers;
  returns the savepoint path when one was taken.
- ``delete(app)``: remove containers, network and savepoint volume.
- ``reset()``: remove everything this MiniStack instance created.

``app`` is the stored record in the DescribeApplication shape plus
``AccountId`` and ``Region``.

Savepoints live in a per-application Docker volume mounted at the same path in
both containers, so they survive a stop (which removes the containers) and a
MiniStack restart, and need no S3 filesystem configuration.

Runtime images match the Flink patch version and Java version AWS documents for
each runtime (Managed Flink developer guide, "Amazon Managed Service for Apache
Flink 1.19" and "1.20" pages: Flink 1.19.1 and 1.20.5, Java 11).
"""

import hashlib
import io
import json
import logging
import os
import re
import tarfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

from ministack.core import container_reaper
from ministack.core.responses import apply_image_prefix, request_scope

logger = logging.getLogger("kinesisanalyticsv2")

# Cap any single Docker daemon call, as the other container-backed services do.
_DOCKER_TIMEOUT = float(os.environ.get("MINISTACK_DOCKER_TIMEOUT", "10"))
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")

_RUNTIME_IMAGES = {
    "FLINK-1_20": "flink:1.20.5-java11",
    "FLINK-1_19": "flink:1.19.1-java11",
}

_LABEL = "kinesisanalyticsv2"
_REST_PORT = 8081
_SAVEPOINT_DIR = "/flink-savepoints"
# Path the aws-kinesisanalytics-runtime library reads runtime properties from
# (com.amazonaws:aws-kinesisanalytics-runtime 1.2.0, config.properties).
_PROPERTIES_PATH = "/etc/flink/application_properties.json"
# Jars added to every cluster's /opt/flink/lib, like the libraries Managed Flink
# provides on its classpath. Managed Flink provides
# com.amazonaws:aws-kinesisanalytics-runtime ("Provided dependencies",
# Managed Flink developer guide, best practices), and AWS documents building
# jobs with it in provided scope, so it must come from here. The full image
# ships it; elsewhere, mount a directory here. Same pattern as the RDS IAM
# plugin's artifact root.
_FLINK_LIB_DIR = "/opt/ministack/flink-lib"
# UID of the ``flink`` user in the official image; the savepoint volume must be
# writable by it.
_FLINK_UID = 9999

_CLUSTER_TIMEOUT = 120.0
_JOB_TIMEOUT = 120.0
_SAVEPOINT_TIMEOUT = 120.0
_POLL_INTERVAL = 1.0

_docker = None
_ministack_network = None

# (account, region, application name) -> cluster record
_clusters: dict = {}
# (account, region, application name) -> thread of the latest start, so a new
# start waits until an earlier one (possibly stopped mid-launch) has finished.
_starts: dict = {}
_lock = threading.Lock()


def _get_docker():
    global _docker
    if _docker is None:
        try:
            import docker

            _docker = docker.from_env(timeout=_DOCKER_TIMEOUT)
        except Exception:
            pass
    return _docker


def _get_ministack_network(docker_client):
    """Detect the Docker network MiniStack is running on (if containerised)."""
    global _ministack_network
    if _ministack_network is not None:
        return _ministack_network or None
    if DOCKER_NETWORK:
        _ministack_network = DOCKER_NETWORK
        return DOCKER_NETWORK
    try:
        self_container = docker_client.containers.get(os.environ.get("HOSTNAME", ""))
        nets = list(self_container.attrs["NetworkSettings"]["Networks"].keys())
        if nets:
            _ministack_network = nets[0]
            return nets[0]
    except Exception:
        pass
    _ministack_network = ""
    return None


def _self_ip(docker_client, ms_network):
    """MiniStack's own address on ``ms_network`` when it runs in a container."""
    if not ms_network:
        return None
    try:
        self_container = docker_client.containers.get(os.environ.get("HOSTNAME", ""))
        nets = self_container.attrs["NetworkSettings"]["Networks"]
        return (nets.get(ms_network) or {}).get("IPAddress") or None
    except Exception:
        return None


def _gateway_endpoint(docker_client, ms_network):
    """MiniStack's endpoint as seen from inside a Flink container.

    Same resolution as EKS: MiniStack's address on the shared network when it
    is containerised, otherwise ``host.docker.internal``, which the containers
    map to ``host-gateway``.
    """
    host = _self_ip(docker_client, ms_network) or "host.docker.internal"
    port = os.environ.get("GATEWAY_PORT") or os.environ.get("EDGE_PORT") or "4566"
    return f"http://{host}:{port}"


def supports(runtime) -> bool:
    """Whether this data plane can run jobs for ``runtime``."""
    return runtime in _RUNTIME_IMAGES


def available() -> bool:
    client = _get_docker()
    if client is None:
        return False
    try:
        client.ping()
        return True
    except Exception:
        return False


# ── Application record helpers ────────────────────────────────────────────────


def _key(app):
    return (app.get("AccountId", ""), app.get("Region", ""), app.get("ApplicationName", ""))


def _base_name(app):
    """Docker name prefix for the application's containers, network and volume.

    The JobManager's name is its address on the Docker network, so it must be
    a valid DNS label: at most 63 characters, letters, digits and hyphens.
    Application names allow up to 128 characters including '.' and '_', so the
    readable part is cleaned and shortened, and a hash of the account, region
    and full name keeps it unique. Labels carry the full identity.
    """
    account_id, region, name = _key(app)
    readable = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")[:20].strip("-") or "app"
    digest = hashlib.sha256(f"{account_id}/{region}/{name}".encode()).hexdigest()[:10]
    return f"ministack-flink-{readable}-{digest}"


def _access_key(app):
    """Access key the job signs with. MiniStack treats a 12-digit key as the
    account id, so the job's calls land in the application's own account."""
    account_id = app.get("AccountId", "")
    return account_id if len(account_id) == 12 and account_id.isdigit() else "test"


def _image_for(app):
    image = _RUNTIME_IMAGES.get(app.get("RuntimeEnvironment", ""))
    return apply_image_prefix(image) if image else None


def _app_config(app):
    return app.get("ApplicationConfigurationDescription") or {}


def _code_location(app):
    """(bucket, key, object version) of the application code in S3."""
    loc = (
        (_app_config(app).get("ApplicationCodeConfigurationDescription") or {})
        .get("CodeContentDescription", {})
        .get("S3ApplicationCodeLocationDescription")
    ) or {}
    bucket = loc.get("BucketARN", "").split(":::", 1)[-1]
    return bucket, loc.get("FileKey", ""), loc.get("ObjectVersion") or None


def _parallelism(app):
    cfg = (
        (_app_config(app).get("FlinkApplicationConfigurationDescription") or {})
        .get("ParallelismConfigurationDescription")
    ) or {}
    value = cfg.get("CurrentParallelism") or cfg.get("Parallelism") or 1
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return 1


def _checkpointing(app):
    """(interval ms, min pause ms) when checkpointing is enabled, else None.

    Managed Flink applies the application's CheckpointConfiguration to the
    cluster; the control plane fills in the DEFAULT values (enabled, 60000,
    5000) from the developer guide.
    """
    cfg = (
        (_app_config(app).get("FlinkApplicationConfigurationDescription") or {})
        .get("CheckpointConfigurationDescription")
    ) or {}
    if not cfg.get("CheckpointingEnabled", True):
        return None
    return int(cfg.get("CheckpointInterval") or 60000), int(cfg.get("MinPauseBetweenCheckpoints") or 5000)


def _allow_non_restored_state(app):
    run_cfg = _app_config(app).get("RunConfigurationDescription") or {}
    flink_run = run_cfg.get("FlinkRunConfigurationDescription") or {}
    return bool(flink_run.get("AllowNonRestoredState", False))


def _properties_json(app):
    groups = (
        (_app_config(app).get("EnvironmentPropertyDescriptions") or {})
        .get("PropertyGroupDescriptions")
    ) or []
    return json.dumps(
        [{"PropertyGroupId": g.get("PropertyGroupId", ""), "PropertyMap": g.get("PropertyMap") or {}} for g in groups]
    ).encode()


def _fetch_jar(app):
    bucket, key, version = _code_location(app)
    if not bucket or not key:
        return None, "Application code location is not an S3 object"
    from ministack.services import s3 as _s3

    with request_scope(app.get("AccountId", ""), app.get("Region", "")):
        data = _s3._get_object_data(bucket, key, version)
    if data is None:
        return None, f"Application code s3://{bucket}/{key} was not found"
    return data, None


# ── Flink REST client ─────────────────────────────────────────────────────────


def _rest(cluster, method, path, body=None, content_type="application/json", timeout=10.0):
    url = f"{cluster['rest_url']}{path}"
    data = None
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", content_type)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"Flink REST {method} {path} returned {e.code}: {_root_cause(e.read())}") from e
    return json.loads(raw) if raw else {}


def _root_cause(body):
    """The innermost ``Caused by`` line of a Flink REST error, which names the
    real failure (a missing class, an exception from the job's main)."""
    text = body.decode(errors="replace")
    try:
        text = "\n".join(json.loads(text).get("errors") or []) or text
    except ValueError:
        pass
    causes = [line.strip() for line in text.splitlines() if line.strip().startswith("Caused by:")]
    return (causes[-1] if causes else text.strip().splitlines()[0] if text.strip() else "")[:500]


def _upload_jar(cluster, jar_bytes):
    boundary = uuid.uuid4().hex
    body = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="jarfile"; filename="application.jar"\r\n'
        "Content-Type: application/x-java-archive\r\n\r\n"
    ).encode() + jar_bytes + f"\r\n--{boundary}--\r\n".encode()
    resp = _rest(cluster, "POST", "/jars/upload", body, f"multipart/form-data; boundary={boundary}", timeout=60.0)
    return resp["filename"].rsplit("/", 1)[-1]


def _wait_for_slots(cluster, slots, deadline):
    while time.monotonic() < deadline:
        if cluster.get("cancelled"):
            raise RuntimeError("Application was stopped before it started")
        try:
            overview = _rest(cluster, "GET", "/overview", timeout=3.0)
            if overview.get("taskmanagers", 0) >= 1 and overview.get("slots-total", 0) >= slots:
                return
        except Exception:
            pass
        time.sleep(_POLL_INTERVAL)
    raise RuntimeError(f"Flink cluster did not become ready within {int(_CLUSTER_TIMEOUT)}s")


def _root_exception(cluster, job_id):
    try:
        exc = _rest(cluster, "GET", f"/jobs/{job_id}/exceptions", timeout=5.0)
        text = (exc.get("rootException") or "").strip()
        return text.splitlines()[0] if text else ""
    except Exception:
        return ""


def _wait_for_job_running(cluster, job_id, deadline):
    while time.monotonic() < deadline:
        if cluster.get("cancelled"):
            raise RuntimeError("Application was stopped before it started")
        state = _rest(cluster, "GET", f"/jobs/{job_id}", timeout=5.0).get("state")
        if state == "RUNNING":
            return
        if state in ("FAILED", "CANCELED", "FINISHED"):
            cause = _root_exception(cluster, job_id)
            raise RuntimeError(f"Flink job {state}" + (f": {cause}" if cause else ""))
        time.sleep(_POLL_INTERVAL)
    raise RuntimeError(f"Flink job did not reach RUNNING within {int(_JOB_TIMEOUT)}s")


def _wait_for_savepoint(cluster, job_id, trigger_id):
    deadline = time.monotonic() + _SAVEPOINT_TIMEOUT
    while time.monotonic() < deadline:
        resp = _rest(cluster, "GET", f"/jobs/{job_id}/savepoints/{trigger_id}", timeout=5.0)
        if (resp.get("status") or {}).get("id") == "COMPLETED":
            op = resp.get("operation") or {}
            if op.get("location"):
                return op["location"]
            cause = (op.get("failure-cause") or {}).get("stack-trace", "")
            raise RuntimeError("Savepoint failed: " + (cause.strip().splitlines() or ["unknown cause"])[0])
        time.sleep(_POLL_INTERVAL)
    raise RuntimeError(f"Savepoint did not complete within {int(_SAVEPOINT_TIMEOUT)}s")


# ── Containers ────────────────────────────────────────────────────────────────


def _flink_properties(jm_name, slots, parallelism, endpoint, access_key, checkpointing):
    lines = [
        f"jobmanager.rpc.address: {jm_name}",
        f"taskmanager.numberOfTaskSlots: {slots}",
        f"parallelism.default: {parallelism}",
        f"state.savepoints.dir: file://{_SAVEPOINT_DIR}",
        f"state.checkpoints.dir: file://{_SAVEPOINT_DIR}/checkpoints",
        # Flink's S3 filesystem shades AWS SDK v1 and ignores
        # AWS_ENDPOINT_URL, so point it at MiniStack explicitly.
        f"s3.endpoint: {endpoint}",
        "s3.path.style.access: true",
        f"s3.access-key: {access_key}",
        "s3.secret-key: test",
    ]
    if checkpointing:
        interval, min_pause = checkpointing
        lines += [
            f"execution.checkpointing.interval: {interval}ms",
            f"execution.checkpointing.min-pause: {min_pause}ms",
        ]
    return "\n".join(lines)


def _flink_lib_jars():
    """(name, bytes) for each jar in _FLINK_LIB_DIR; empty when there are none."""
    root = Path(_FLINK_LIB_DIR)
    jars = sorted(root.glob("*.jar")) if root.is_dir() else []
    if not jars:
        logger.info(
            "Flink: no jars in %s; jobs must bundle aws-kinesisanalytics-runtime themselves", _FLINK_LIB_DIR
        )
    return [(jar.name, jar.read_bytes()) for jar in jars]


def _container_archive(properties, jars):
    """The runtime properties file, plus the jars for the Flink lib directory."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        info = tarfile.TarInfo("etc/flink")
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        tar.addfile(info)
        files = [(_PROPERTIES_PATH.lstrip("/"), properties)]
        files += [(f"opt/flink/lib/{name}", payload) for name, payload in jars]
        for path, payload in files:
            info = tarfile.TarInfo(path)
            info.size = len(payload)
            info.mode = 0o644
            tar.addfile(info, io.BytesIO(payload))
    return buf.getvalue()


def _remove_container(client, name_or_id):
    try:
        client.containers.get(name_or_id).remove(force=True, v=True)
    except Exception:
        pass


def _ensure_image(client, image):
    """Pull the runtime image when it is not local. ``containers.create`` does
    not pull the way ``containers.run`` does."""
    try:
        client.images.get(image)
        return
    except Exception:
        pass
    logger.info("Flink: pulling %s", image)
    try:
        client.images.pull(image)
    except Exception as e:
        raise RuntimeError(f"Could not pull Flink image {image}: {e}") from e


def _ensure_volume(client, app, image):
    """Create the application's savepoint volume, writable by the flink user."""
    name = f"{_base_name(app)}-savepoints"
    try:
        client.volumes.get(name)
        return name
    except Exception:
        pass
    account_id, region, app_name = _key(app)
    client.volumes.create(
        name=name,
        labels=container_reaper.own_labels(_LABEL, account_id=account_id, region=region, application=app_name),
    )
    # A fresh named volume is owned by root; hand it to the flink user once.
    client.containers.run(
        image,
        entrypoint=["chown", f"{_FLINK_UID}:{_FLINK_UID}", _SAVEPOINT_DIR],
        volumes={name: {"bind": _SAVEPOINT_DIR, "mode": "rw"}},
        user="root",
        remove=True,
        labels=container_reaper.own_labels(_LABEL),
    )
    return name


def _ensure_network(client, app, ms_network):
    """The network JobManager and TaskManager share: MiniStack's own network
    when it has one, otherwise a per-application bridge network."""
    if ms_network:
        return ms_network, False
    name = f"{_base_name(app)}-net"
    try:
        client.networks.get(name)
    except Exception:
        account_id, region, app_name = _key(app)
        client.networks.create(
            name,
            driver="bridge",
            labels=container_reaper.own_labels(_LABEL, account_id=account_id, region=region, application=app_name),
        )
    return name, True


def _launch(client, app, cluster):
    image = cluster["image"]
    base = _base_name(app)
    jm_name, tm_name = f"{base}-jm", f"{base}-tm"
    for stale in (jm_name, tm_name):
        _remove_container(client, stale)

    _ensure_image(client, image)
    ms_network = _get_ministack_network(client)
    network, own_network = _ensure_network(client, app, ms_network)
    cluster["network"] = network if own_network else None
    volume = _ensure_volume(client, app, image)
    endpoint = _gateway_endpoint(client, ms_network)

    parallelism = _parallelism(app)
    env = {
        "FLINK_PROPERTIES": _flink_properties(
            jm_name, parallelism, parallelism, endpoint, _access_key(app), _checkpointing(app)
        ),
        "ENABLE_BUILT_IN_PLUGINS": f"flink-s3-fs-hadoop-{image.rsplit(':', 1)[-1].split('-')[0]}.jar",
        # Fixed credentials so the job always authenticates against MiniStack,
        # whatever the host environment carries, in the application's account.
        "AWS_ACCESS_KEY_ID": _access_key(app),
        "AWS_SECRET_ACCESS_KEY": "test",
        "AWS_REGION": app.get("Region", ""),
        "AWS_DEFAULT_REGION": app.get("Region", ""),
        "AWS_ENDPOINT_URL": endpoint,
    }
    account_id, region, app_name = _key(app)
    common = dict(
        image=image,
        environment=env,
        network=network,
        volumes={volume: {"bind": _SAVEPOINT_DIR, "mode": "rw"}},
        extra_hosts={"host.docker.internal": "host-gateway"},
        detach=True,
    )
    properties = _container_archive(_properties_json(app), _flink_lib_jars())

    jm = client.containers.create(
        command="jobmanager",
        name=jm_name,
        hostname=jm_name,
        ports={f"{_REST_PORT}/tcp": ("127.0.0.1", None)},
        labels=container_reaper.own_labels(
            _LABEL, account_id=account_id, region=region, application=app_name, role="jobmanager"
        ),
        **common,
    )
    cluster["jm_id"] = jm.id
    jm.put_archive("/", properties)
    jm.start()

    tm = client.containers.create(
        command="taskmanager",
        name=tm_name,
        hostname=tm_name,
        labels=container_reaper.own_labels(
            _LABEL, account_id=account_id, region=region, application=app_name, role="taskmanager"
        ),
        **common,
    )
    cluster["tm_id"] = tm.id
    tm.put_archive("/", properties)
    tm.start()

    # Reach the REST API on the JobManager's network address when MiniStack
    # shares its network, otherwise through the loopback-published port.
    if _self_ip(client, ms_network):
        jm.reload()
        ip = jm.attrs["NetworkSettings"]["Networks"][network]["IPAddress"]
        cluster["rest_url"] = f"http://{ip}:{_REST_PORT}"
    else:
        jm.reload()
        binding = jm.attrs["NetworkSettings"]["Ports"][f"{_REST_PORT}/tcp"][0]
        cluster["rest_url"] = f"http://127.0.0.1:{binding['HostPort']}"
    return parallelism


def _teardown(client, cluster, remove_volume=False, app=None):
    for cid in (cluster.get("tm_id"), cluster.get("jm_id")):
        if cid:
            _remove_container(client, cid)
    if cluster.get("network"):
        try:
            client.networks.get(cluster["network"]).remove()
        except Exception:
            pass
    if remove_volume and app is not None:
        try:
            client.volumes.get(f"{_base_name(app)}-savepoints").remove(force=True)
        except Exception:
            pass


def _run(app, cluster, savepoint_path, on_status, prior=None, previous=None):
    client = _get_docker()
    # An earlier start of this application may still be launching containers
    # under the same names; let it finish (it removes them once it sees it was
    # cancelled), then make sure nothing of the replaced cluster is left.
    if prior is not None:
        prior.join(timeout=_CLUSTER_TIMEOUT + _JOB_TIMEOUT)
    if previous is not None:
        _teardown(client, previous)
    try:
        jar, error = _fetch_jar(app)
        if error:
            raise RuntimeError(error)
        parallelism = _launch(client, app, cluster)
        _wait_for_slots(cluster, parallelism, time.monotonic() + _CLUSTER_TIMEOUT)
        jar_id = _upload_jar(cluster, jar)
        run_body = {"parallelism": parallelism, "allowNonRestoredState": _allow_non_restored_state(app)}
        if savepoint_path:
            run_body["savepointPath"] = savepoint_path
        job_id = _rest(cluster, "POST", f"/jars/{jar_id}/run", run_body, timeout=60.0)["jobid"]
        cluster["job_id"] = job_id
        _wait_for_job_running(cluster, job_id, time.monotonic() + _JOB_TIMEOUT)
    except Exception as e:
        if cluster.get("cancelled"):
            # Stopped or replaced mid-launch: containers created after the
            # stop's own cleanup would otherwise be left behind.
            _teardown(client, cluster)
            return
        with _lock:
            if _clusters.get(_key(app)) is cluster:
                _clusters.pop(_key(app), None)
        _teardown(client, cluster)
        _notify(on_status, "FAILED", str(e)[:1000])
        return
    logger.info("Flink: application %s running as job %s", app.get("ApplicationName"), cluster["job_id"])
    _notify(on_status, "RUNNING")


def _notify(on_status, status, message=None):
    try:
        if message is None:
            on_status(status)
        else:
            on_status(status, message)
    except Exception:
        logger.exception("Flink: status callback failed")


# ── Interface ─────────────────────────────────────────────────────────────────


def start(app, savepoint_path, on_status):
    image = _image_for(app)
    if image is None:
        logger.info(
            "Flink: runtime %s has no image mapping; application %s runs control plane only",
            app.get("RuntimeEnvironment"), app.get("ApplicationName"),
        )
        _notify(on_status, "RUNNING")
        return
    cluster = {"image": image}
    with _lock:
        previous = _clusters.get(_key(app))
        _clusters[_key(app)] = cluster
        if previous is not None:
            previous["cancelled"] = True
        thread = threading.Thread(
            target=_run, args=(app, cluster, savepoint_path, on_status, _starts.get(_key(app)), previous), daemon=True
        )
        _starts[_key(app)] = thread
    thread.start()


def snapshot(app, snapshot_name):
    with _lock:
        cluster = _clusters.get(_key(app))
    if not cluster or not cluster.get("job_id"):
        return None
    try:
        resp = _rest(
            cluster, "POST", f"/jobs/{cluster['job_id']}/savepoints",
            {"target-directory": f"file://{_SAVEPOINT_DIR}/{snapshot_name}", "cancel-job": False},
        )
        return _wait_for_savepoint(cluster, cluster["job_id"], resp["request-id"])
    except Exception as e:
        logger.warning("Flink: snapshot %s of %s failed: %s", snapshot_name, app.get("ApplicationName"), e)
        return None


def stop(app, force, take_snapshot):
    with _lock:
        cluster = _clusters.pop(_key(app), None)
    if not cluster:
        return None
    cluster["cancelled"] = True
    client = _get_docker()
    location = None
    job_id = cluster.get("job_id")
    if job_id:
        try:
            if take_snapshot and not force:
                target = f"file://{_SAVEPOINT_DIR}/stop-{int(time.time() * 1000)}"
                resp = _rest(cluster, "POST", f"/jobs/{job_id}/stop", {"targetDirectory": target, "drain": False})
                location = _wait_for_savepoint(cluster, job_id, resp["request-id"])
            else:
                _rest(cluster, "PATCH", f"/jobs/{job_id}?mode=cancel")
        except Exception as e:
            logger.warning("Flink: stopping %s: %s", app.get("ApplicationName"), e)
    if client is not None:
        _teardown(client, cluster)
    return location


def delete(app):
    with _lock:
        cluster = _clusters.pop(_key(app), None) or {}
    cluster["cancelled"] = True
    client = _get_docker()
    if client is None:
        return
    base = _base_name(app)
    cluster.setdefault("jm_id", f"{base}-jm")
    cluster.setdefault("tm_id", f"{base}-tm")
    cluster.setdefault("network", f"{base}-net")
    _teardown(client, cluster, remove_volume=True, app=app)


def reset():
    with _lock:
        clusters = list(_clusters.values())
        _clusters.clear()
        _starts.clear()
    for cluster in clusters:
        cluster["cancelled"] = True
    client = _get_docker()
    if client is None:
        return
    filters = {"label": [f"ministack={_LABEL}", f"{container_reaper.INSTANCE_LABEL}={container_reaper.instance_id()}"]}
    try:
        for c in client.containers.list(all=True, filters=filters):
            _remove_container(client, c.id)
        for n in client.networks.list(filters=filters):
            try:
                n.remove()
            except Exception:
                pass
        for v in client.volumes.list(filters=filters):
            try:
                v.remove(force=True)
            except Exception:
                pass
    except Exception as e:
        logger.warning("Flink: reset could not list containers: %s", e)


def _live_container_ids():
    with _lock:
        return [cid for c in _clusters.values() for cid in (c.get("jm_id"), c.get("tm_id")) if cid]


container_reaper.register_live_ids(_LABEL, _live_container_ids)
