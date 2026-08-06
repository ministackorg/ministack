"""
AWS Cloud Control API Emulator.
JSON-based API via X-Amz-Target (prefix: CloudApiService).

Cloud Control is a generic CRUD-over-any-typeName control plane: the same eight
operations manage every CloudFormation resource type. Terraform's `awscc`
provider and CDK L1 constructs sit on top of it, so a small op count covers a
large surface.

Resources are stored keyed by (TypeName, Identifier). `DesiredState` /
`Properties` are JSON strings, exactly as real AWS transports them. Mutating
operations (Create/Update/Delete) are synchronous here but return the same
`ProgressEvent` shape real AWS returns for its asynchronous requests, always
with OperationStatus=SUCCESS. Every request is also recorded so
ListResourceRequests / GetResourceRequestStatus can replay it.

Supported: CreateResource, GetResource, UpdateResource, DeleteResource,
           ListResources, ListResourceRequests, GetResourceRequestStatus,
           CancelResourceRequest.

Verified against botocore cloudcontrol/2021-09-30/service-2.json:
  targetPrefix=CloudApiService, jsonVersion=1.0, protocol=json,
  endpointPrefix/signingName=cloudcontrolapi.
"""

import json
import logging
import time

from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    json_response,
    new_uuid,
)

logger = logging.getLogger("cloudcontrol")

from ministack.core.persistence import load_state

# (TypeName, Identifier) -> {"TypeName", "Identifier", "Properties" (JSON str)}
_resources = AccountRegionScopedDict()
# RequestToken -> ProgressEvent dict
_requests = AccountRegionScopedDict()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {"resources": _resources, "requests": _requests}


def restore_state(data):
    if not data:
        return
    resources = data.get("resources")
    if isinstance(resources, AccountRegionScopedDict):
        _resources.update(resources)
    requests = data.get("requests")
    if isinstance(requests, AccountRegionScopedDict):
        _requests.update(requests)


try:
    _restored = load_state("cloudcontrol")
    if _restored:
        restore_state(_restored)
except Exception:
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


# ── Helpers ────────────────────────────────────────────────

def _resource_key(type_name, identifier):
    return f"{type_name}\x00{identifier}"


def _extract_identifier(props):
    """Derive a primary identifier from a resource's properties.

    Real Cloud Control derives the identifier from the resource type's schema
    primaryIdentifier. Without those schemas we pick a stable, deterministic
    value: a common id-like property if present, else a generated id.
    """
    for candidate in ("Id", "Name", "Arn", "BucketName", "Identifier",
                      "PhysicalResourceId", "ResourceId"):
        val = props.get(candidate)
        if isinstance(val, (str, int)) and str(val):
            return str(val)
    return new_uuid()


def _progress_event(operation, status, type_name=None, identifier=None,
                    request_token=None, resource_model=None,
                    status_message=None, error_code=None):
    """Build a botocore-correct ProgressEvent.

    Members present in the botocore ProgressEvent shape: TypeName, Identifier,
    RequestToken, HooksRequestToken, Operation, OperationStatus, EventTime,
    ResourceModel, StatusMessage, ErrorCode, RetryAfter. We omit optional
    members we don't populate rather than send nulls, matching how AWS drops
    absent members from JSON responses.
    """
    event = {
        "TypeName": type_name,
        "RequestToken": request_token or new_uuid(),
        "Operation": operation,
        "OperationStatus": status,
        "EventTime": int(time.time()),
    }
    if identifier is not None:
        event["Identifier"] = identifier
    if resource_model is not None:
        event["ResourceModel"] = resource_model
    if status_message is not None:
        event["StatusMessage"] = status_message
    if error_code is not None:
        event["ErrorCode"] = error_code
    return event


def _record_request(event):
    _requests[event["RequestToken"]] = event
    return event


# ── Operations ─────────────────────────────────────────────

def _create_resource(data):
    type_name = data.get("TypeName")
    desired_state = data.get("DesiredState")
    if not type_name or desired_state is None:
        return error_response_json(
            "InvalidRequestException",
            "TypeName and DesiredState are required",
            400,
        )
    try:
        props = json.loads(desired_state) if isinstance(desired_state, str) else dict(desired_state)
    except (json.JSONDecodeError, TypeError, ValueError):
        return error_response_json(
            "InvalidRequestException",
            "DesiredState must be a JSON document",
            400,
        )

    identifier = _extract_identifier(props)
    key = _resource_key(type_name, identifier)
    if key in _resources:
        return error_response_json(
            "AlreadyExistsException",
            f"Resource of type '{type_name}' with identifier '{identifier}' already exists.",
            400,
        )
    properties = json.dumps(props)
    _resources[key] = {
        "TypeName": type_name,
        "Identifier": identifier,
        "Properties": properties,
    }
    logger.info("Created %s %s", type_name, identifier)
    event = _record_request(_progress_event(
        "CREATE", "SUCCESS", type_name=type_name, identifier=identifier,
        resource_model=properties,
    ))
    return json_response({"ProgressEvent": event})


def _get_resource(data):
    type_name = data.get("TypeName")
    identifier = data.get("Identifier")
    rec = _resources.get(_resource_key(type_name, identifier))
    if not rec:
        return error_response_json(
            "ResourceNotFoundException",
            f"Resource of type '{type_name}' with identifier '{identifier}' was not found.",
            400,
        )
    return json_response({
        "TypeName": type_name,
        "ResourceDescription": {
            "Identifier": rec["Identifier"],
            "Properties": rec["Properties"],
        },
    })


def _update_resource(data):
    type_name = data.get("TypeName")
    identifier = data.get("Identifier")
    patch_document = data.get("PatchDocument")
    key = _resource_key(type_name, identifier)
    rec = _resources.get(key)
    if not rec:
        return error_response_json(
            "ResourceNotFoundException",
            f"Resource of type '{type_name}' with identifier '{identifier}' was not found.",
            400,
        )
    if patch_document is None:
        return error_response_json(
            "InvalidRequestException",
            "PatchDocument is required",
            400,
        )
    try:
        patch = json.loads(patch_document) if isinstance(patch_document, str) else patch_document
    except (json.JSONDecodeError, TypeError, ValueError):
        return error_response_json(
            "InvalidRequestException",
            "PatchDocument must be a JSON document",
            400,
        )

    props = json.loads(rec["Properties"])
    try:
        props = _apply_json_patch(props, patch)
    except _PatchError as exc:
        return error_response_json("InvalidRequestException", str(exc), 400)

    properties = json.dumps(props)
    rec["Properties"] = properties
    _resources[key] = rec
    logger.info("Updated %s %s", type_name, identifier)
    event = _record_request(_progress_event(
        "UPDATE", "SUCCESS", type_name=type_name, identifier=identifier,
        resource_model=properties,
    ))
    return json_response({"ProgressEvent": event})


def _delete_resource(data):
    type_name = data.get("TypeName")
    identifier = data.get("Identifier")
    key = _resource_key(type_name, identifier)
    if key not in _resources:
        return error_response_json(
            "ResourceNotFoundException",
            f"Resource of type '{type_name}' with identifier '{identifier}' was not found.",
            400,
        )
    del _resources[key]
    logger.info("Deleted %s %s", type_name, identifier)
    event = _record_request(_progress_event(
        "DELETE", "SUCCESS", type_name=type_name, identifier=identifier,
    ))
    return json_response({"ProgressEvent": event})


def _list_resources(data):
    type_name = data.get("TypeName")
    if not type_name:
        return error_response_json(
            "InvalidRequestException",
            "TypeName is required",
            400,
        )
    descriptions = [
        {"Identifier": rec["Identifier"], "Properties": rec["Properties"]}
        for rec in _resources.values()
        if rec.get("TypeName") == type_name
    ]
    return json_response({
        "TypeName": type_name,
        "ResourceDescriptions": descriptions,
    })


def _list_resource_requests(data):
    filt = data.get("ResourceRequestStatusFilter") or {}
    operations = set(filt.get("Operations") or [])
    statuses = set(filt.get("OperationStatuses") or [])
    summaries = []
    for event in _requests.values():
        if operations and event.get("Operation") not in operations:
            continue
        if statuses and event.get("OperationStatus") not in statuses:
            continue
        summaries.append(event)
    return json_response({"ResourceRequestStatusSummaries": summaries})


def _get_resource_request_status(data):
    request_token = data.get("RequestToken")
    event = _requests.get(request_token)
    if not event:
        return error_response_json(
            "RequestTokenNotFoundException",
            f"Request token '{request_token}' was not found.",
            400,
        )
    return json_response({"ProgressEvent": event})


def _cancel_resource_request(data):
    request_token = data.get("RequestToken")
    event = _requests.get(request_token)
    if not event:
        return error_response_json(
            "RequestTokenNotFoundException",
            f"Request token '{request_token}' was not found.",
            400,
        )
    # A SUCCESS/FAILED request can't be cancelled: real AWS rejects it. Our
    # requests complete synchronously, so they are always terminal.
    if event.get("OperationStatus") in ("SUCCESS", "FAILED", "CANCEL_COMPLETE"):
        return error_response_json(
            "ConflictException",
            f"Only IN_PROGRESS requests can be cancelled. Request '{request_token}' "
            f"is {event.get('OperationStatus')}.",
            400,
        )
    event["OperationStatus"] = "CANCEL_COMPLETE"
    _requests[request_token] = event
    return json_response({"ProgressEvent": event})


# ── RFC 6902 JSON Patch (minimal) ──────────────────────────

class _PatchError(Exception):
    pass


def _apply_json_patch(doc, operations):
    """Apply a subset of RFC 6902 (add/replace/remove) sufficient for
    Cloud Control UpdateResource property mutations."""
    if not isinstance(operations, list):
        raise _PatchError("PatchDocument must be a JSON array of operations")
    for op in operations:
        if not isinstance(op, dict):
            raise _PatchError("Each patch operation must be an object")
        kind = op.get("op")
        path = op.get("path", "")
        tokens = [t.replace("~1", "/").replace("~0", "~")
                  for t in path.split("/")[1:]] if path else []
        if kind in ("add", "replace"):
            _patch_set(doc, tokens, op.get("value"))
        elif kind == "remove":
            _patch_remove(doc, tokens)
        else:
            raise _PatchError(f"Unsupported patch op: {kind}")
    return doc


def _patch_set(doc, tokens, value):
    if not tokens:
        raise _PatchError("Empty patch path")
    cur = doc
    for tok in tokens[:-1]:
        if isinstance(cur, dict):
            cur = cur.setdefault(tok, {})
        else:
            raise _PatchError("Patch path traverses a non-object")
    if isinstance(cur, dict):
        cur[tokens[-1]] = value
    else:
        raise _PatchError("Patch path traverses a non-object")


def _patch_remove(doc, tokens):
    if not tokens:
        raise _PatchError("Empty patch path")
    cur = doc
    for tok in tokens[:-1]:
        if isinstance(cur, dict) and tok in cur:
            cur = cur[tok]
        else:
            raise _PatchError("Patch path not found")
    if isinstance(cur, dict):
        cur.pop(tokens[-1], None)


# ── Request handler ────────────────────────────────────────

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateResource": _create_resource,
        "GetResource": _get_resource,
        "UpdateResource": _update_resource,
        "DeleteResource": _delete_resource,
        "ListResources": _list_resources,
        "ListResourceRequests": _list_resource_requests,
        "GetResourceRequestStatus": _get_resource_request_status,
        "CancelResourceRequest": _cancel_resource_request,
    }

    handler = handlers.get(action)
    if not handler:
        logger.warning("Unknown CloudControl action: %s", action)
        return error_response_json(
            "InvalidAction", f"Unknown action: {action}", 400
        )
    return handler(data)


def reset():
    _resources.clear()
    _requests.clear()
