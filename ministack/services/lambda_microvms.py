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
import json
import logging
import secrets
import time
from urllib.parse import unquote

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


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not data:
        return
    _microvms.clear()
    _images.clear()
    _microvms.update(data.get("microvms", {}))
    _images.update(data.get("images", {}))




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
        "environmentVariables", "hooks", "logging", "resources", "tags",
    ):
        if field in data:
            record[field] = data[field]
    record.update({
        "imageVersion": version,
        "latestActiveImageVersion": version,
        "state": "UPDATED",
        "updatedAt": _now(),
    })
    return json_response({
        **{k: v for k, v in record.items() if v is not None},
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
