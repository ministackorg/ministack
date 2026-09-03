"""AWS Lambda MicroVMs emulation (REST-JSON, API version 2025-09-09).

Control-plane emulation of the Lambda MicroVM API: build images, run MicroVMs,
manage their lifecycle (suspend/resume/terminate), and mint access tokens.
There is no real VM behind a MicroVM here — a RunMicrovm goes straight to
RUNNING and an image build straight to CREATED, which is what a client polling
GetMicrovm / GetMicrovmImage needs to proceed.

Shapes verified against the AWS Lambda MicroVM API reference (2025-09-09):
RunMicrovm, GetMicrovm, ListMicrovms, SuspendMicrovm, ResumeMicrovm,
TerminateMicrovm, CreateMicrovmImage, CreateMicrovmAuthToken,
CreateMicrovmShellAuthToken.
"""

import copy
import json
import logging
import secrets
import time
from urllib.parse import unquote

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)

logger = logging.getLogger("lambda_microvms")

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


def restore_state(data):
    if not data:
        return
    _microvms.clear()
    _images.clear()
    _microvms.update(data.get("microvms", {}))
    _images.update(data.get("images", {}))


try:
    _persisted = load_state("lambda_microvms")
    if _persisted:
        restore_state(_persisted)
except Exception:  # pragma: no cover - best-effort restore
    pass


def reset():
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
    }
    _microvms[microvm_id] = record
    return json_response(_microvm_view(record))


def _get_microvm(microvm_id):
    record = _microvms.get(microvm_id)
    if not record:
        return _not_found(f"MicroVM {microvm_id} not found")
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
    record["state"] = "SUSPENDED"
    return _empty_ok()


def _resume_microvm(microvm_id):
    record = _microvms.get(microvm_id)
    if not record:
        return _not_found(f"MicroVM {microvm_id} not found")
    record["state"] = "RUNNING"
    return _empty_ok()


def _terminate_microvm(microvm_id):
    record = _microvms.get(microvm_id)
    if not record:
        return _not_found(f"MicroVM {microvm_id} not found")
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
    view = {k: v for k, v in record.items() if v is not None}
    return json_response(view, 201)


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
