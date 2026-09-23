# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""AWS Lambda MicroVMs emulation (REST-JSON, API version 2025-09-09).

Control-plane emulation of the Lambda MicroVM API: build images, run MicroVMs,
manage their lifecycle (suspend/resume/terminate), and mint access tokens.
There is no real VM behind a MicroVM here — a RunMicrovm goes straight to
RUNNING and an image build straight to CREATED, which is what a client polling
GetMicrovm / GetMicrovmImage needs to proceed.

Shapes verified against the AWS Lambda MicroVM API reference (2025-09-09):
RunMicrovm, GetMicrovm, ListMicrovms, SuspendMicrovm, ResumeMicrovm,
TerminateMicrovm, ListMicrovmImages, CreateMicrovmImage, CreateMicrovmAuthToken,
CreateMicrovmShellAuthToken, GetMicrovmImage, GetMicrovmImageVersion,
UpdateMicrovmImage.
"""

import copy
import io
import json
import logging
import os
import pathlib
import secrets
import stat
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from urllib.parse import unquote, urlparse

from ministack.core import container_reaper
from ministack.core.responses import (
    AccountRegionScopedDict,
    apply_image_prefix,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)

logger = logging.getLogger("lambda_microvms")

# Docker is deliberately opt-in. The normal MicroVM emulator remains a fast
# control-plane stub, while local smoke tests can ask for a real workload
# behind a MicroVM by setting MINISTACK_MICROVM_BACKEND=docker and registering
# a MicroVM image with the non-AWS `containerImage` extension.
MICROVM_BACKEND = os.environ.get("MINISTACK_MICROVM_BACKEND", "metadata").strip().lower()
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")
_DOCKER_TIMEOUT = float(os.environ.get("MINISTACK_DOCKER_TIMEOUT", "10"))
_docker = None
_docker_in_use = False

# MicroVM lifecycle states (AWS enum).
_MICROVM_STATES = ("PENDING", "RUNNING", "SUSPENDING", "SUSPENDED",
                   "TERMINATING", "TERMINATED")
# MicroVM image states (AWS enum).
_IMAGE_STATES = ("CREATING", "CREATED", "CREATE_FAILED", "UPDATING", "UPDATED",
                 "UPDATE_FAILED", "DELETING", "DELETE_FAILED", "DELETED")

# ---------------------------------------------------------------------------
# State (account + region scoped)
# ---------------------------------------------------------------------------

_microvms = AccountRegionScopedDict()   # microvmId -> record
_images = AccountRegionScopedDict()     # imageName -> record


def get_state():
    return copy.deepcopy({"microvms": _microvms, "images": _images})


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not data:
        return
    _microvms.clear()
    _images.clear()
    _microvms.update(data.get("microvms", {}))
    _images.update(data.get("images", {}))

    # Container handles are intentionally not persisted. A Docker-backed
    # MicroVM is instance-store-like: after MiniStack restarts, the workload
    # behind it is gone and the API must not report a phantom RUNNING VM.
    for record in _microvms.values():
        if record.get("backend") == "docker" and record.get("state") not in (
            "TERMINATED", "TERMINATING"
        ):
            record["state"] = "TERMINATED"
            record["terminatedAt"] = record.get("terminatedAt") or _now()
            record.pop("_container_id", None)
            record.pop("_container_name", None)
            record.pop("_container_ip", None)




def reset():
    _sweep_containers()
    _microvms.clear()
    _images.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> int:
    return int(time.time())


def _parse_body(body) -> dict:
    if not body:
        return {}
    try:
        parsed = json.loads(body)
    except (ValueError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _validation(message: str):
    return error_response_json("ValidationException", message, 400)


def _not_found(message: str):
    return error_response_json("ResourceNotFoundException", message, 404)


def _empty_ok():
    return 200, {}, b""


def _microvm_id() -> str:
    return f"mvm-{secrets.token_hex(8)}"


def _microvm_endpoint(microvm_id: str) -> str:
    return f"https://{microvm_id}.microvm.{get_region()}.amazonaws.com"


def _image_arn(name: str) -> str:
    return (f"arn:aws:lambda:{get_region()}:{get_account_id()}:"
            f"microvm-image/{name}")


def _auth_token_value() -> str:
    return secrets.token_urlsafe(48)


def _resolve_image_arn(image_identifier: str) -> str:
    """imageIdentifier may be an ARN or a bare image name/id."""
    if image_identifier.startswith("arn:"):
        return image_identifier
    return _image_arn(image_identifier)


# ---------------------------------------------------------------------------
# Optional Docker-backed workload
# ---------------------------------------------------------------------------

def _docker_enabled() -> bool:
    return MICROVM_BACKEND == "docker"


def _get_docker():
    """Return a lazily-created Docker client, or None when unavailable."""
    global _docker
    if _docker is None:
        try:
            import docker

            _docker = docker.from_env(timeout=_DOCKER_TIMEOUT)
        except Exception as exc:
            logger.debug("MicroVM: no Docker client available: %s", exc)
    return _docker


def _get_ministack_network(client):
    """Use the configured MiniStack network when one is available."""
    if DOCKER_NETWORK:
        return DOCKER_NETWORK
    try:
        hostname = os.environ.get("HOSTNAME", "")
        if not hostname:
            return None
        own_container = client.containers.get(hostname)
        networks = own_container.attrs.get("NetworkSettings", {}).get("Networks", {})
        return next(iter(networks), None)
    except Exception:
        return None


def _valid_container_image(value) -> bool:
    """Reject values that cannot be a Docker image reference.

    The reference is passed to docker-py as an argument, never through a
    shell. This check mainly prevents control characters and accidental empty
    values from crossing the service boundary.
    """
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 512
        and not any(ord(char) < 32 or ord(char) == 127 for char in value)
        and not any(char.isspace() for char in value)
    )


def _hook_config(record):
    return record.get("hooks") or {}


def _hook_enabled(record, group, name):
    return (_hook_config(record).get(group) or {}).get(name) == "ENABLED"


def _hook_port(record):
    try:
        port = int(_hook_config(record).get("port"))
    except (TypeError, ValueError):
        return None
    return port if 1 <= port <= 65535 else None


def _hook_timeout(record, group, name, default=30):
    raw = (_hook_config(record).get(group) or {}).get(f"{name}TimeoutInSeconds")
    try:
        return max(1, min(3600, int(raw)))
    except (TypeError, ValueError):
        return default


def _validate_docker_image_request(data):
    """Require the application hook server needed by the Docker emulation."""
    hooks = data.get("hooks") or {}
    if not _hook_port({"hooks": hooks}):
        return _validation(
            "Docker-backed MicroVM images require hooks.port between 1 and 65535"
        )
    image_hooks = hooks.get("microvmImageHooks") or {}
    if image_hooks.get("ready") != "ENABLED":
        return _validation(
            "Docker-backed MicroVM images require hooks.microvmImageHooks.ready=ENABLED"
        )
    runtime_hooks = hooks.get("microvmHooks") or {}
    if runtime_hooks.get("run") != "ENABLED":
        return _validation(
            "Docker-backed MicroVM images require hooks.microvmHooks.run=ENABLED"
        )
    return None


def _s3_artifact_bytes(uri):
    """Read a code-artifact ZIP from MiniStack's account-scoped S3 service."""
    if not isinstance(uri, str) or not uri.startswith("s3://"):
        raise ValueError("codeArtifact.uri must be an s3:// URI")
    parsed = urlparse(uri)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    if not bucket or not key:
        raise ValueError("codeArtifact.uri must include an S3 bucket and key")
    from ministack.services import s3

    data = s3._get_object_data(bucket, key)
    if data is None:
        raise FileNotFoundError(f"code artifact not found: {uri}")
    return data


def _safe_extract_artifact(data, destination):
    """Extract an artifact without allowing ZIP path traversal or symlinks."""
    root = pathlib.Path(destination).resolve()
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        for member in archive.infolist():
            relative = pathlib.PurePosixPath(member.filename)
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError(f"unsafe path in code artifact: {member.filename}")
            mode = (member.external_attr >> 16) & 0o170000
            if mode == stat.S_IFLNK:
                raise ValueError(f"symlinks are not allowed in code artifacts: {member.filename}")
            target = (root / pathlib.Path(*relative.parts)).resolve()
            if root != target and root not in target.parents:
                raise ValueError(f"unsafe path in code artifact: {member.filename}")
            if member.is_dir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member, "r") as source, target.open("wb") as output:
                output.write(source.read())


def _container_ip(container, network):
    container.reload()
    networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
    if network and network in networks:
        ip = networks[network].get("IPAddress")
    else:
        ip = next(iter(networks.values()), {}).get("IPAddress")
    if not ip:
        raise RuntimeError("Docker workload has no reachable network address")
    return ip


def _hook_path(name, image_phase=False):
    if image_phase:
        return f"/{name}"
    return f"/aws/lambda-microvms/runtime/v1/{name}"


def _call_hook(record, container, name, *, image_phase=False, payload=None, retry=False):
    """POST one MicroVM hook and require a 2xx response."""
    port = _hook_port(record)
    if not port:
        raise RuntimeError("MicroVM hook port is not configured")
    client = _get_docker()
    network = _get_ministack_network(client) if client else None
    host = _container_ip(container, network)
    group = "microvmImageHooks" if image_phase else "microvmHooks"
    timeout = _hook_timeout(record, group, name)
    deadline = time.monotonic() + timeout if retry else None
    body = json.dumps(payload or {}).encode("utf-8")
    url = f"http://{host}:{port}{_hook_path(name, image_phase=image_phase)}"

    while True:
        request = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status = response.status
            if 200 <= status < 300:
                return
            error = RuntimeError(f"MicroVM hook {name} returned HTTP {status}")
        except urllib.error.HTTPError as exc:
            error = RuntimeError(f"MicroVM hook {name} returned HTTP {exc.code}")
            status = exc.code
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            error = RuntimeError(f"MicroVM hook {name} was unreachable: {exc}")
            status = None
        if not retry or time.monotonic() >= deadline or status not in (None, 503):
            raise error
        time.sleep(0.1)


def _image_has_entrypoint(client, image_ref):
    """Pull the image if needed and inspect whether it has a start command."""
    try:
        image = client.images.get(image_ref)
    except Exception:
        image = client.images.pull(image_ref)
        if isinstance(image, list):
            image = image[0]
    config = image.attrs.get("Config") or {}
    # AWS starts the application from either Docker ENTRYPOINT or CMD. Keep
    # both forms intact; only use the keepalive fallback for an image with
    # neither, where the required hook server will fail clearly.
    return bool(config.get("Entrypoint") or config.get("Cmd"))


def _build_docker_image(record):
    """Build a MicroVM workload image from the S3 code-artifact ZIP.

    The temporary build container must expose the required `/ready` hook. The
    built image is retained locally and referenced by the MicroVM image record;
    the temporary validation container is removed after the build hooks finish.
    """
    client = _get_docker()
    if client is None:
        raise RuntimeError("no Docker daemon available")
    artifact = (record.get("codeArtifact") or {}).get("uri")
    data = _s3_artifact_bytes(artifact)
    image_ref = f"ministack-microvm:{secrets.token_hex(12)}"

    with tempfile.TemporaryDirectory(prefix="ministack-microvm-build-") as context:
        _safe_extract_artifact(data, context)
        if not os.path.isfile(os.path.join(context, "Dockerfile")):
            raise ValueError("code artifact must contain a Dockerfile at its root")
        client.images.build(path=context, tag=image_ref, rm=True, forcerm=True)

    network = _get_ministack_network(client)
    global _docker_in_use
    _docker_in_use = True
    build_kwargs = {
        "detach": True,
        "init": True,
        "labels": container_reaper.own_labels("lambda-microvm-build"),
    }
    if network:
        build_kwargs["network"] = network
    build_container = client.containers.run(image_ref, **build_kwargs)
    try:
        # `/ready` is retried because the application server may need time to
        # initialize after the Dockerfile CMD starts.
        _call_hook(record, build_container, "ready", image_phase=True, retry=True)
        if _hook_enabled(record, "microvmImageHooks", "validate"):
            _call_hook(record, build_container, "validate", image_phase=True, retry=True)
    finally:
        try:
            build_container.remove(force=True)
        except Exception as exc:
            logger.warning("MicroVM: could not remove image validation container: %s", exc)
    return image_ref


def _container_name(microvm_id: str) -> str:
    return f"microvm-{microvm_id}"


def _container_for(record):
    client = _get_docker()
    if client is None:
        return None
    for reference in (
        record.get("_container_id"),
        record.get("_container_name"),
        _container_name(record["microvmId"]),
    ):
        if not reference:
            continue
        try:
            return client.containers.get(reference)
        except Exception:
            continue
    return None


def _remove_container(record):
    container = _container_for(record)
    if container is not None:
        try:
            container.remove(force=True)
        except Exception as exc:
            logger.warning(
                "MicroVM: could not remove container for %s: %s",
                record.get("microvmId"),
                exc,
            )
    record.pop("_container_id", None)
    record.pop("_container_name", None)
    record.pop("_container_ip", None)


def _start_container(record, image_ref):
    client = _get_docker()
    if client is None:
        raise RuntimeError("no Docker daemon available")
    if not _valid_container_image(image_ref):
        raise ValueError("containerImage is not a valid Docker image reference")

    network = _get_ministack_network(client)
    name = _container_name(record["microvmId"])
    try:
        client.containers.get(name).remove(force=True)
    except Exception:
        pass

    image_ref = apply_image_prefix(image_ref)
    kwargs = {
        "name": name,
        "detach": True,
        "init": True,
        "labels": {
            **container_reaper.own_labels("lambda-microvm"),
            "microvm_id": record["microvmId"],
            "account_id": get_account_id(),
            "region": get_region(),
        },
    }
    if not _image_has_entrypoint(client, image_ref):
        # Keep a shell-based image alive so lifecycle and shell-auth smoke
        # tests have a long-lived workload to address.
        kwargs["command"] = ["sleep", "infinity"]
    if network:
        kwargs["network"] = network

    container = client.containers.run(image_ref, **kwargs)
    global _docker_in_use
    _docker_in_use = True
    record["_container_id"] = container.id
    record["_container_name"] = name
    record["backend"] = "docker"
    if _hook_enabled(record, "microvmHooks", "run"):
        try:
            _call_hook(
                record,
                container,
                "run",
                payload={
                    "microvmId": record["microvmId"],
                    "runHookPayload": record.get("runHookPayload"),
                },
                retry=True,
            )
        except Exception:
            _remove_container(record)
            raise
    return container


def _container_image_for(record):
    image = _find_microvm_image(record.get("imageArn"))
    if not image:
        return None
    return image.get("_docker_image")


def _reconcile_microvm(record):
    """Turn a Docker-backed record into TERMINATED when its workload is gone."""
    if record.get("backend") != "docker" or record.get("state") != "RUNNING":
        return
    container = _container_for(record)
    if container is None:
        record["state"] = "TERMINATED"
        record["terminatedAt"] = record.get("terminatedAt") or _now()
        record.pop("_container_id", None)
        record.pop("_container_name", None)
        return
    try:
        container.reload()
    except Exception:
        return
    if getattr(container, "status", "") in ("dead", "exited"):
        record["state"] = "TERMINATED"
        record["terminatedAt"] = record.get("terminatedAt") or _now()
        record.pop("_container_id", None)
        record.pop("_container_name", None)


def _live_container_ids():
    return {
        record.get("_container_id")
        for record in _microvms.values()
        if record.get("_container_id")
    }


container_reaper.register_live_ids("lambda-microvm", _live_container_ids)


def _sweep_containers():
    if not _docker_in_use:
        return
    client = _get_docker()
    if client is None:
        return
    try:
        containers = client.containers.list(
            all=True,
            filters={
                "label": [
                    "ministack=lambda-microvm",
                    f"{container_reaper.INSTANCE_LABEL}={container_reaper.instance_id()}",
                ]
            },
        )
    except Exception as exc:
        logger.warning("MicroVM: reset container sweep failed: %s", exc)
        return
    container_reaper.drop_containers(containers, force=True)


# ---------------------------------------------------------------------------
# MicroVM views
# ---------------------------------------------------------------------------

def _microvm_view(record: dict) -> dict:
    """Full MicroVM object (RunMicrovm / GetMicrovm), omitting None fields to
    match AWS's omit-when-absent behavior (e.g. terminatedAt before terminate)."""
    fields = (
        "egressNetworkConnectors", "endpoint", "executionRoleArn", "idlePolicy",
        "imageArn", "imageVersion", "ingressNetworkConnectors",
        "maximumDurationInSeconds", "microvmId", "startedAt", "state",
        "stateReason", "terminatedAt",
    )
    return {k: record[k] for k in fields if record.get(k) is not None}


def _microvm_item(record: dict) -> dict:
    """Summary item for ListMicrovms."""
    item = {
        "imageArn": record.get("imageArn"),
        "imageVersion": record.get("imageVersion"),
        "microvmId": record.get("microvmId"),
        "startedAt": record.get("startedAt"),
        "state": record.get("state"),
    }
    return {k: v for k, v in item.items() if v is not None}


# ---------------------------------------------------------------------------
# MicroVM operations
# ---------------------------------------------------------------------------

def _run_microvm(body):
    data = _parse_body(body)
    image_identifier = data.get("imageIdentifier")
    if not image_identifier:
        return _validation("imageIdentifier is required")

    microvm_id = _microvm_id()
    record = {
        "microvmId": microvm_id,
        "state": "RUNNING",
        "endpoint": _microvm_endpoint(microvm_id),
        "imageArn": _resolve_image_arn(image_identifier),
        "imageVersion": data.get("imageVersion", "1"),
        "startedAt": _now(),
        "executionRoleArn": data.get("executionRoleArn"),
        "idlePolicy": data.get("idlePolicy"),
        "egressNetworkConnectors": data.get("egressNetworkConnectors"),
        "ingressNetworkConnectors": data.get("ingressNetworkConnectors"),
        "maximumDurationInSeconds": data.get("maximumDurationInSeconds"),
        "runHookPayload": data.get("runHookPayload"),
    }
    image_record = _find_microvm_image(record["imageArn"])
    if image_record:
        record["hooks"] = image_record.get("hooks")
    _microvms[microvm_id] = record

    # Docker-backed execution is deliberately opt-in. In that mode a MicroVM
    # must come from a successfully built MicroVM image; silently falling back
    # to a metadata-only record would hide a broken image-build workflow.
    if _docker_enabled():
        image_ref = _container_image_for(record)
        if not image_ref:
            _microvms.pop(microvm_id, None)
            return _not_found(
                f"MicroVM image {image_identifier} has no successful Docker build"
            )
        try:
            _start_container(record, image_ref)
        except Exception as exc:
            _microvms.pop(microvm_id, None)
            logger.warning("MicroVM: could not boot %s: %s", image_ref, exc)
            return error_response_json(
                "InternalError",
                f"failed to start MicroVM workload: {exc}",
                500,
            )
    return json_response(_microvm_view(record))


def _get_microvm(microvm_id):
    record = _microvms.get(microvm_id)
    if not record:
        return _not_found(f"MicroVM {microvm_id} not found")
    _reconcile_microvm(record)
    return json_response(_microvm_view(record))


def _list_microvms(query_params):
    def _qp(name):
        val = query_params.get(name) if query_params else None
        if isinstance(val, (list, tuple)):
            return val[0] if val else None
        return val

    image_filter = _qp("imageIdentifier")
    version_filter = _qp("imageVersion")
    items = []
    for record in _microvms.values():
        _reconcile_microvm(record)
        if image_filter and image_filter not in (
            record.get("imageArn"), record.get("imageIdentifier")
        ):
            continue
        if version_filter and record.get("imageVersion") != version_filter:
            continue
        items.append(_microvm_item(record))
    return json_response({"items": items})


def _suspend_microvm(microvm_id):
    record = _microvms.get(microvm_id)
    if not record:
        return _not_found(f"MicroVM {microvm_id} not found")
    _reconcile_microvm(record)
    if record.get("backend") == "docker":
        container = _container_for(record)
        if container is None:
            return _not_found(f"MicroVM {microvm_id} workload is no longer running")
        try:
            if _hook_enabled(record, "microvmHooks", "suspend"):
                _call_hook(record, container, "suspend")
            container.pause()
        except Exception as exc:
            return error_response_json("InternalError", f"could not suspend MicroVM: {exc}", 500)
    record["state"] = "SUSPENDED"
    return _empty_ok()


def _resume_microvm(microvm_id):
    record = _microvms.get(microvm_id)
    if not record:
        return _not_found(f"MicroVM {microvm_id} not found")
    if record.get("backend") == "docker":
        container = _container_for(record)
        if container is None:
            record["state"] = "TERMINATED"
            record["terminatedAt"] = record.get("terminatedAt") or _now()
            return _not_found(f"MicroVM {microvm_id} workload is no longer available")
        try:
            container.unpause()
            if _hook_enabled(record, "microvmHooks", "resume"):
                _call_hook(record, container, "resume")
        except Exception as exc:
            try:
                container.pause()
            except Exception:
                pass
            return error_response_json("InternalError", f"could not resume MicroVM: {exc}", 500)
    record["state"] = "RUNNING"
    return _empty_ok()


def _terminate_microvm(microvm_id):
    record = _microvms.get(microvm_id)
    if not record:
        return _not_found(f"MicroVM {microvm_id} not found")
    if record.get("backend") == "docker":
        container = _container_for(record)
        if container is not None and _hook_enabled(record, "microvmHooks", "terminate"):
            try:
                _call_hook(record, container, "terminate")
            except Exception as exc:
                logger.warning("MicroVM: terminate hook failed for %s: %s", microvm_id, exc)
        _remove_container(record)
    # Idempotent: terminating an already-terminated MicroVM succeeds.
    record["state"] = "TERMINATED"
    record["terminatedAt"] = record.get("terminatedAt") or _now()
    return _empty_ok()


def _create_microvm_auth_token(microvm_id, body):
    data = _parse_body(body)
    if not data.get("allowedPorts"):
        return _validation("allowedPorts is required")
    if not data.get("expirationInMinutes"):
        return _validation("expirationInMinutes is required")
    if not _microvms.get(microvm_id):
        return _not_found(f"MicroVM {microvm_id} not found")
    return json_response({"authToken": {"X-aws-proxy-auth": _auth_token_value()}})


def _create_microvm_shell_auth_token(microvm_id, body):
    data = _parse_body(body)
    if not data.get("expirationInMinutes"):
        return _validation("expirationInMinutes is required")
    if not _microvms.get(microvm_id):
        return _not_found(f"MicroVM {microvm_id} not found")
    return json_response({"authToken": {"X-aws-proxy-auth": _auth_token_value()}})


# ---------------------------------------------------------------------------
# MicroVM image operations
# ---------------------------------------------------------------------------

def _create_microvm_image(body):
    data = _parse_body(body)
    for field in ("baseImageArn", "buildRoleArn", "name", "codeArtifact"):
        if not data.get(field):
            return _validation(f"{field} is required")
    if _docker_enabled():
        hook_error = _validate_docker_image_request(data)
        if hook_error:
            return hook_error
    name = data["name"]
    now = _now()
    record = {
        "name": name,
        "imageArn": _image_arn(name),
        "imageVersion": "1",
        "latestActiveImageVersion": "1",
        "state": "CREATED",
        "createdAt": now,
        "updatedAt": now,
        "baseImageArn": data["baseImageArn"],
        "baseImageVersion": data.get("baseImageVersion"),
        "buildRoleArn": data["buildRoleArn"],
        "codeArtifact": data["codeArtifact"],
        "cpuConfigurations": data.get("cpuConfigurations"),
        "additionalOsCapabilities": data.get("additionalOsCapabilities"),
        "description": data.get("description"),
        "egressNetworkConnectors": data.get("egressNetworkConnectors"),
        "environmentVariables": data.get("environmentVariables"),
        "hooks": data.get("hooks"),
        "logging": data.get("logging"),
        "resources": data.get("resources"),
        "tags": data.get("tags"),
    }
    _images[name] = record
    if _docker_enabled():
        try:
            # This is intentionally synchronous in the draft. A production
            # implementation should move the build to a worker and expose the
            # normal CREATING -> CREATED/CREATE_FAILED polling transition.
            record["_docker_image"] = _build_docker_image(record)
        except Exception as exc:
            record["state"] = "CREATE_FAILED"
            record["latestFailedImageVersion"] = record["imageVersion"]
            logger.warning("MicroVM: image build failed for %s: %s", name, exc)
            return error_response_json("InternalError", f"MicroVM image build failed: {exc}", 500)
    view = {k: v for k, v in record.items() if v is not None and not k.startswith("_")}
    return json_response(view, 201)


def _microvm_image_item(record: dict) -> dict:
    """Summary item for ListMicrovmImages."""
    fields = (
        "imageArn", "name", "state", "latestActiveImageVersion",
        "latestFailedImageVersion", "createdAt",
    )
    return {k: record[k] for k in fields if record.get(k) is not None}


def _list_microvm_images(query_params):
    def _qp(name):
        val = query_params.get(name) if query_params else None
        if isinstance(val, (list, tuple)):
            return val[0] if val else None
        return val

    name_filter = _qp("nameFilter")
    images = [
        record for record in _images.values()
        if not name_filter or name_filter in record.get("name", "")
    ]
    images.sort(key=lambda record: record.get("name", ""))

    raw_max_results = _qp("maxResults")
    if raw_max_results is None:
        max_results = len(images) or 1
    else:
        try:
            max_results = int(raw_max_results)
        except (TypeError, ValueError):
            return _validation("maxResults must be an integer")
        if max_results < 1:
            return _validation("maxResults must be greater than zero")

    raw_next_token = _qp("nextToken")
    try:
        start = int(raw_next_token) if raw_next_token else 0
    except (TypeError, ValueError):
        return _validation("nextToken is invalid")
    if start < 0:
        return _validation("nextToken is invalid")

    end = min(start + max_results, len(images))
    response = {
        "items": [_microvm_image_item(record) for record in images[start:end]],
    }
    if end < len(images):
        response["nextToken"] = str(end)
    return json_response(response)


def _find_microvm_image(image_identifier):
    image_identifier = unquote(image_identifier)
    for record in _images.values():
        if image_identifier in (record.get("name"), record.get("imageArn")):
            return record
        # AWS examples also use the colon-delimited ARN form while the local
        # image ARN uses a slash before the image name.
        if image_identifier.rsplit(":", 1)[-1] == record.get("name"):
            return record
    return None


def _get_microvm_image_version(image_identifier, image_version):
    record = _find_microvm_image(image_identifier)
    if not record or record.get("imageVersion") != image_version:
        return _not_found(
            f"MicroVM image version {image_identifier}/{image_version} not found")

    fields = (
        "baseImageArn", "baseImageVersion", "buildRoleArn", "description",
        "codeArtifact", "logging", "egressNetworkConnectors",
        "cpuConfigurations", "resources", "additionalOsCapabilities", "hooks",
        "environmentVariables", "imageArn", "imageVersion", "createdAt",
        "updatedAt", "tags",
    )
    view = {k: record[k] for k in fields if record.get(k) is not None}
    # A completed MiniStack image represents a successful, active version.
    view.update({"state": "SUCCESSFUL", "status": "ACTIVE"})
    return json_response(view)


def _microvm_image_view(record: dict) -> dict:
    fields = (
        "createdAt", "imageArn", "latestActiveImageVersion",
        "latestFailedImageVersion", "name", "state", "tags", "updatedAt",
    )
    return {k: record[k] for k in fields if record.get(k) is not None}


def _get_microvm_image(image_identifier):
    record = _find_microvm_image(image_identifier)
    if not record:
        return _not_found(f"MicroVM image {image_identifier} not found")
    return json_response(_microvm_image_view(record))


def _update_microvm_image(image_identifier, body):
    record = _find_microvm_image(image_identifier)
    if not record:
        return _not_found(f"MicroVM image {image_identifier} not found")

    data = _parse_body(body)
    for field in ("baseImageArn", "buildRoleArn", "codeArtifact"):
        if not data.get(field):
            return _validation(f"{field} is required")

    version = str(int(record.get("imageVersion", "0")) + 1)
    for field in (
        "baseImageArn", "baseImageVersion", "buildRoleArn", "codeArtifact",
        "cpuConfigurations", "description", "egressNetworkConnectors",
        "environmentVariables", "hooks", "logging", "resources",
        "tags",
    ):
        if field in data:
            record[field] = data[field]
    if _docker_enabled():
        hook_error = _validate_docker_image_request(record)
        if hook_error:
            return hook_error
        try:
            record["_docker_image"] = _build_docker_image(record)
        except Exception as exc:
            record["state"] = "UPDATE_FAILED"
            record["latestFailedImageVersion"] = version
            logger.warning("MicroVM: image rebuild failed for %s: %s", image_identifier, exc)
            return error_response_json("InternalError", f"MicroVM image rebuild failed: {exc}", 500)
    record.update({
        "imageVersion": version,
        "latestActiveImageVersion": version,
        "state": "UPDATED",
        "updatedAt": _now(),
    })
    return json_response({
        **{k: v for k, v in record.items() if v is not None and not k.startswith("_")},
        "imageVersion": version,
    })


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    parts = [p for p in path.strip("/").split("/") if p]
    if not parts or parts[0] != "2025-09-09":
        return error_response_json(
            "InvalidAction", f"Unsupported MicroVM path: {path}", 400)

    segments = parts[1:]
    if not segments:
        return error_response_json(
            "InvalidAction", f"Unsupported MicroVM path: {path}", 400)

    root = segments[0]
    n = len(segments)

    if root == "microvm-images":
        if n >= 4 and segments[-2] == "versions" and method == "GET":
            image_identifier = "/".join(segments[1:-2])
            image_version = unquote(segments[-1])
            return _get_microvm_image_version(image_identifier, image_version)
        if n >= 2:
            image_identifier = "/".join(segments[1:])
            if method == "GET":
                return _get_microvm_image(image_identifier)
            if method == "PUT":
                return _update_microvm_image(image_identifier, body)
        if n == 1 and method == "GET":
            return _list_microvm_images(query_params)
        if n == 1 and method == "POST":
            return _create_microvm_image(body)

    elif root == "microvms":
        if n == 1:
            if method == "POST":
                return _run_microvm(body)
            if method == "GET":
                return _list_microvms(query_params)
        elif n == 2:
            microvm_id = unquote(segments[1])
            if method == "GET":
                return _get_microvm(microvm_id)
            if method == "DELETE":
                return _terminate_microvm(microvm_id)
        elif n == 3 and method == "POST":
            microvm_id = unquote(segments[1])
            action = segments[2]
            if action == "suspend":
                return _suspend_microvm(microvm_id)
            if action == "resume":
                return _resume_microvm(microvm_id)
            if action == "auth-token":
                return _create_microvm_auth_token(microvm_id, body)
            if action == "shell-auth-token":
                return _create_microvm_shell_auth_token(microvm_id, body)

    return error_response_json(
        "InvalidAction", f"Unsupported MicroVM request: {method} {path}", 400)
