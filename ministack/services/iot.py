"""AWS IoT Core control plane emulator.

Implements the JSON/REST APIs under ``iot.{region}.amazonaws.com``:

  - Thing registry: ``CreateThing``, ``DescribeThing``, ``ListThings``,
    ``UpdateThing``, ``DeleteThing``
  - ThingType: ``CreateThingType`` and friends
  - ThingGroup: ``CreateThingGroup`` and friends
  - Certificates: ``CreateKeysAndCertificate``, ``RegisterCertificate``,
    ``UpdateCertificate``, ``DeleteCertificate``,
    ``AttachThingPrincipal`` / ``DetachThingPrincipal``
  - Policies: ``CreatePolicy``, ``CreatePolicyVersion``, ``AttachPolicy``,
    ``DetachPolicy``, etc.
  - ``DescribeEndpoint`` returning a per-account hostname

This is the Phase 1a control plane — pure HTTP/JSON, no MQTT broker
dependency. The data plane (``iot_data.py``, ``iot_broker.py``) is
implemented separately and only depends on this module for certificate
lookups (Phase 2).

State is fully isolated per account via ``AccountScopedDict`` and persisted
through ``get_state``/``restore_state``. The Local CA (used to sign
``CreateKeysAndCertificate`` certificates) is also persisted so previously
issued client certificates remain valid across restarts.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone

from ministack.core import local_ca
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("iot")

_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_GATEWAY_PORT = os.environ.get("GATEWAY_PORT", os.environ.get("EDGE_PORT", "4566"))

# Resource name validation per AWS IoT spec: 1..128 chars, [a-zA-Z0-9:_-]
_NAME_RE = re.compile(r"^[a-zA-Z0-9:_-]{1,128}$")


# ---------------------------------------------------------------------------
# Module-level state (account-scoped)
# ---------------------------------------------------------------------------

_things: AccountScopedDict = AccountScopedDict()  # thingName -> Thing dict
_thing_types: AccountScopedDict = AccountScopedDict()
_thing_groups: AccountScopedDict = AccountScopedDict()
_certificates: AccountScopedDict = AccountScopedDict()  # certificateId -> Certificate dict
_policies: AccountScopedDict = AccountScopedDict()  # policyName -> Policy dict


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def get_state() -> dict:
    return {
        "things": copy.deepcopy(_things),
        "thing_types": copy.deepcopy(_thing_types),
        "thing_groups": copy.deepcopy(_thing_groups),
        "certificates": copy.deepcopy(_certificates),
        "policies": copy.deepcopy(_policies),
        "local_ca": local_ca.get_state(),
    }


def restore_state(data: dict | None) -> None:
    if not data:
        return
    _things.update(data.get("things", {}))
    _thing_types.update(data.get("thing_types", {}))
    _thing_groups.update(data.get("thing_groups", {}))
    _certificates.update(data.get("certificates", {}))
    _policies.update(data.get("policies", {}))
    local_ca.restore_state(data.get("local_ca"))


def reset() -> None:
    _things.clear()
    _thing_types.clear()
    _thing_groups.clear()
    _certificates.clear()
    _policies.clear()
    local_ca.reset()


try:
    _restored = load_state("iot")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted IoT state; continuing with fresh store")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_epoch() -> float:
    return datetime.now(timezone.utc).timestamp()


def _thing_arn(name: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:thing/{name}"


def _thing_type_arn(name: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:thingtype/{name}"


def _thing_group_arn(name: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:thinggroup/{name}"


def _cert_arn(certificate_id: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:cert/{certificate_id}"


def _policy_arn(name: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:policy/{name}"


def _validate_name(name: str | None, field: str) -> tuple | None:
    if not name or not _NAME_RE.match(name):
        return error_response_json(
            "InvalidRequestException",
            f"Invalid {field}: must match [a-zA-Z0-9:_-]{{1,128}}",
            400,
        )
    return None


def _parse_body(body: bytes) -> dict:
    if not body:
        return {}
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}


def _error_not_found(resource: str, name: str) -> tuple:
    return error_response_json(
        "ResourceNotFoundException", f"{resource} {name!r} not found", 404
    )


# ---------------------------------------------------------------------------
# Top-level dispatcher
# ---------------------------------------------------------------------------


async def handle_request(
    method: str, path: str, headers: dict, body: bytes, query_params: dict
) -> tuple:
    """Route an IoT control-plane request to the appropriate handler.

    The IoT API is REST-style (not JSON 1.1 with X-Amz-Target). Routing is
    therefore by HTTP verb + path. Path templates use AWS conventions:

      * ``POST /things/{thingName}``
      * ``GET  /things/{thingName}``
      * ``DELETE /things/{thingName}``
      * ``POST /keys-and-certificate``
      * ``GET  /endpoint``

    See the AWS IoT API Reference for the canonical mapping.
    """
    qp = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}
    hdr = headers or {}

    # Endpoint
    if path == "/endpoint" and method == "GET":
        return _describe_endpoint(qp)

    # Things — list/describe/update/delete
    if path == "/things" and method == "GET":
        return _list_things(qp)
    # Principal lives at /things/{name}/principals — must come BEFORE generic /things/{name}
    if path.startswith("/things/") and path.endswith("/principals"):
        return _handle_thing_principals(method, path, hdr, body, qp)
    if path.startswith("/things/") and method in ("POST", "GET", "PATCH", "DELETE"):
        return _handle_thing(method, path, body, qp)

    # ThingTypes
    if path == "/thing-types" and method == "GET":
        return _list_thing_types(qp)
    if path.startswith("/thing-types/"):
        return _handle_thing_type(method, path, body, qp)

    # ThingGroups — special add/remove paths must come BEFORE the
    # generic ``/thing-groups/{name}`` handler.
    if path == "/thing-groups/addThingToThingGroup" and method in ("PUT", "POST"):
        return _add_thing_to_group(_parse_body(body))
    if path == "/thing-groups/removeThingFromThingGroup" and method in ("PUT", "POST"):
        return _remove_thing_from_group(_parse_body(body))
    if path == "/thing-groups" and method == "GET":
        return _list_thing_groups(qp)
    if path.startswith("/thing-groups/") and path.endswith("/things") and method == "GET":
        return _list_things_in_thing_group(path)
    if path.startswith("/thing-groups/"):
        return _handle_thing_group(method, path, body, qp)

    # Certificates
    if path == "/keys-and-certificate" and method == "POST":
        return _create_keys_and_certificate(qp)
    if path == "/certificate/register" and method == "POST":
        return _register_certificate(_parse_body(body), qp)
    if path == "/certificates" and method == "GET":
        return _list_certificates(qp)
    if path.startswith("/certificates/") and method in ("GET", "PUT", "DELETE"):
        return _handle_certificate(method, path, body, qp)

    # Principal listing
    if path == "/principals/things" and method == "GET":
        return _list_principal_things(hdr, qp)

    # Policies
    if path == "/policies" and method == "GET":
        return _list_policies(qp)
    # Policy attachment paths — must come BEFORE generic /policies/ handler
    if path.startswith("/target-policies/") and method in ("PUT", "POST", "DELETE"):
        return _handle_target_policy(method, path, body, qp)
    if path.startswith("/policy-targets/") and method in ("GET", "POST"):
        return _list_targets_for_policy(path, qp)
    if path.startswith("/attached-policies/") and method in ("GET", "POST"):
        return _list_attached_policies(path, qp)
    if path.startswith("/policies/"):
        return _handle_policy(method, path, body, qp)

    return error_response_json(
        "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
    )


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


def _describe_endpoint(qp: dict) -> tuple:
    """Return a per-account endpoint hostname.

    Format: ``{prefix}-ats.iot.{region}.{MINISTACK_HOST}:{GATEWAY_PORT}``
    where ``prefix`` is the first 14 hex chars of SHA-256(account_id), so the
    hostname is stable per account and looks AWS-shaped without leaking the
    account ID.
    """
    endpoint_type = qp.get("endpointType", "iot:Data-ATS")
    account_id = get_account_id()
    prefix = hashlib.sha256(account_id.encode("utf-8")).hexdigest()[:14]
    region = get_region()

    if endpoint_type in ("iot:Data-ATS", "iot:Data", None):
        suffix = "-ats" if endpoint_type != "iot:Data" else ""
        host = f"{prefix}{suffix}.iot.{region}.{_MINISTACK_HOST}:{_GATEWAY_PORT}"
    elif endpoint_type == "iot:CredentialProvider":
        host = f"{prefix}.credentials.iot.{region}.{_MINISTACK_HOST}:{_GATEWAY_PORT}"
    elif endpoint_type == "iot:Jobs":
        host = f"{prefix}.jobs.iot.{region}.{_MINISTACK_HOST}:{_GATEWAY_PORT}"
    else:
        return error_response_json(
            "InvalidRequestException",
            f"Unknown endpointType: {endpoint_type}",
            400,
        )
    return json_response({"endpointAddress": host})


# ---------------------------------------------------------------------------
# Thing CRUD
# ---------------------------------------------------------------------------


def _handle_thing(method: str, path: str, body: bytes, qp: dict) -> tuple:
    """Dispatch /things/{name} routes (sub-paths handled separately)."""
    # /things/{name}/principals lives in _handle_thing_principals
    suffix = path[len("/things/"):]
    # Sub-resources (principals, etc.) handled by other branches; only handle
    # bare /things/{name} here. Anything containing additional segments is a
    # routing miss handled higher up.
    if "/" in suffix:
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
        )
    name = suffix
    err = _validate_name(name, "thingName")
    if err:
        return err

    if method == "POST":
        return _create_thing(name, _parse_body(body))
    if method == "GET":
        return _describe_thing(name)
    if method == "PATCH":
        return _update_thing(name, _parse_body(body))
    if method == "DELETE":
        return _delete_thing(name)
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


def _create_thing(name: str, payload: dict) -> tuple:
    attrs = (payload.get("attributePayload") or {}).get("attributes") or {}
    type_name = payload.get("thingTypeName")

    existing = _things.get(name)
    if existing is not None:
        # Idempotent: same config returns success; different config returns 409.
        if (
            existing.get("attributes") == attrs
            and existing.get("thingTypeName") == type_name
        ):
            return json_response({
                "thingName": existing["thingName"],
                "thingArn": existing["thingArn"],
                "thingId": existing["thingId"],
            })
        return error_response_json(
            "ResourceAlreadyExistsException",
            f"Thing {name!r} already exists with different configuration",
            409,
        )

    if type_name and type_name not in _thing_types:
        return _error_not_found("ThingType", type_name)

    thing_id = new_uuid()
    record = {
        "thingName": name,
        "thingId": thing_id,
        "thingArn": _thing_arn(name),
        "thingTypeName": type_name,
        "attributes": dict(attrs),
        "version": 1,
        "creationDate": _now_epoch(),
        "principals": [],
        "thingGroupNames": [],
    }
    _things[name] = record
    logger.info("IoT Thing created: %s", name)
    return json_response({
        "thingName": name,
        "thingArn": record["thingArn"],
        "thingId": thing_id,
    })


def _describe_thing(name: str) -> tuple:
    thing = _things.get(name)
    if thing is None:
        return _error_not_found("Thing", name)
    body = {
        "thingName": thing["thingName"],
        "thingId": thing["thingId"],
        "thingArn": thing["thingArn"],
        "thingTypeName": thing.get("thingTypeName"),
        "attributes": thing.get("attributes", {}),
        "version": thing.get("version", 1),
        "defaultClientId": thing["thingName"],
    }
    return json_response(body)


def _list_things(qp: dict) -> tuple:
    attribute_name = qp.get("attributeName")
    attribute_value = qp.get("attributeValue")
    thing_type_name = qp.get("thingTypeName")
    name_prefix = qp.get("thingName")

    out = []
    for name, t in _things.items():
        if attribute_name is not None and t.get("attributes", {}).get(attribute_name) != attribute_value:
            continue
        if thing_type_name is not None and t.get("thingTypeName") != thing_type_name:
            continue
        if name_prefix is not None and not name.startswith(name_prefix):
            continue
        out.append({
            "thingName": t["thingName"],
            "thingArn": t["thingArn"],
            "thingTypeName": t.get("thingTypeName"),
            "attributes": t.get("attributes", {}),
            "version": t.get("version", 1),
        })
    return json_response({"things": out})


def _update_thing(name: str, payload: dict) -> tuple:
    thing = _things.get(name)
    if thing is None:
        return _error_not_found("Thing", name)

    attribute_payload = payload.get("attributePayload") or {}
    new_attrs = attribute_payload.get("attributes") or {}
    merge = bool(attribute_payload.get("merge", False))

    if merge:
        merged = dict(thing.get("attributes", {}))
        for k, v in new_attrs.items():
            if v is None or v == "":
                merged.pop(k, None)
            else:
                merged[k] = v
        thing["attributes"] = merged
    else:
        thing["attributes"] = dict(new_attrs)

    new_type = payload.get("thingTypeName")
    if new_type is not None:
        if new_type and new_type not in _thing_types:
            return _error_not_found("ThingType", new_type)
        thing["thingTypeName"] = new_type or None

    thing["version"] = thing.get("version", 1) + 1
    _things[name] = thing
    return json_response({})


def _delete_thing(name: str) -> tuple:
    thing = _things.get(name)
    if thing is None:
        return _error_not_found("Thing", name)
    # Detach all attached certificates
    thing_arn = thing["thingArn"]
    for cert_id, cert in list(_certificates.items()):
        if thing_arn in cert.get("attachedThings", []):
            cert["attachedThings"].remove(thing_arn)
            _certificates[cert_id] = cert
    # Remove from groups
    for gname in list(thing.get("thingGroupNames", [])):
        group = _thing_groups.get(gname)
        if group and name in group.get("things", []):
            group["things"].remove(name)
            _thing_groups[gname] = group
    del _things[name]
    logger.info("IoT Thing deleted: %s", name)
    return json_response({})


# ---------------------------------------------------------------------------
# ThingType CRUD
# ---------------------------------------------------------------------------


def _handle_thing_type(method: str, path: str, body: bytes, qp: dict) -> tuple:
    suffix = path[len("/thing-types/"):]

    # /thing-types/{name}/deprecate — boto3 uses POST, others may use PUT
    if suffix.endswith("/deprecate"):
        name = suffix[:-len("/deprecate")]
        err = _validate_name(name, "thingTypeName")
        if err:
            return err
        if method in ("POST", "PUT"):
            return _deprecate_thing_type(name, _parse_body(body))
        return error_response_json(
            "InvalidRequestException", f"Unsupported method: {method}", 400
        )

    if "/" in suffix:
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
        )

    name = suffix
    err = _validate_name(name, "thingTypeName")
    if err:
        return err
    if method == "POST":
        return _create_thing_type(name, _parse_body(body))
    if method == "GET":
        return _describe_thing_type(name)
    if method == "DELETE":
        return _delete_thing_type(name)
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


def _create_thing_type(name: str, payload: dict) -> tuple:
    if name in _thing_types:
        return error_response_json(
            "ResourceAlreadyExistsException",
            f"ThingType {name!r} already exists",
            409,
        )
    props = payload.get("thingTypeProperties") or {}
    record = {
        "thingTypeName": name,
        "thingTypeId": new_uuid(),
        "thingTypeArn": _thing_type_arn(name),
        "thingTypeProperties": {
            "thingTypeDescription": props.get("thingTypeDescription"),
            "searchableAttributes": list(props.get("searchableAttributes", []) or []),
        },
        "thingTypeMetadata": {
            "deprecated": False,
            "deprecationDate": None,
            "creationDate": _now_epoch(),
        },
    }
    _thing_types[name] = record
    return json_response({
        "thingTypeName": name,
        "thingTypeArn": record["thingTypeArn"],
        "thingTypeId": record["thingTypeId"],
    })


def _describe_thing_type(name: str) -> tuple:
    t = _thing_types.get(name)
    if t is None:
        return _error_not_found("ThingType", name)
    return json_response(t)


def _list_thing_types(qp: dict) -> tuple:
    return json_response({"thingTypes": list(_thing_types.values())})


def _deprecate_thing_type(name: str, payload: dict) -> tuple:
    t = _thing_types.get(name)
    if t is None:
        return _error_not_found("ThingType", name)
    undo = bool(payload.get("undoDeprecate", False))
    t["thingTypeMetadata"]["deprecated"] = not undo
    t["thingTypeMetadata"]["deprecationDate"] = None if undo else _now_epoch()
    _thing_types[name] = t
    return json_response({})


def _delete_thing_type(name: str) -> tuple:
    t = _thing_types.get(name)
    if t is None:
        return _error_not_found("ThingType", name)
    if not t["thingTypeMetadata"].get("deprecated"):
        return error_response_json(
            "InvalidRequestException",
            "ThingType must be deprecated for at least 5 minutes before deletion",
            400,
        )
    del _thing_types[name]
    return json_response({})


# ---------------------------------------------------------------------------
# ThingGroup CRUD
# ---------------------------------------------------------------------------


def _handle_thing_group(method: str, path: str, body: bytes, qp: dict) -> tuple:
    suffix = path[len("/thing-groups/"):]
    if "/" in suffix:
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
        )
    name = suffix
    err = _validate_name(name, "thingGroupName")
    if err:
        return err
    if method == "POST":
        return _create_thing_group(name, _parse_body(body))
    if method == "GET":
        return _describe_thing_group(name)
    if method == "PATCH":
        return _update_thing_group(name, _parse_body(body))
    if method == "DELETE":
        return _delete_thing_group(name)
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


def _create_thing_group(name: str, payload: dict) -> tuple:
    if name in _thing_groups:
        return error_response_json(
            "ResourceAlreadyExistsException",
            f"ThingGroup {name!r} already exists",
            409,
        )
    props = payload.get("thingGroupProperties") or {}
    attr_payload = props.get("attributePayload") or {}
    record = {
        "thingGroupName": name,
        "thingGroupId": new_uuid(),
        "thingGroupArn": _thing_group_arn(name),
        "thingGroupProperties": {
            "thingGroupDescription": props.get("thingGroupDescription"),
            "attributePayload": {"attributes": dict(attr_payload.get("attributes", {}))},
        },
        "version": 1,
        "things": [],
        "creationDate": _now_epoch(),
    }
    _thing_groups[name] = record
    return json_response({
        "thingGroupName": name,
        "thingGroupArn": record["thingGroupArn"],
        "thingGroupId": record["thingGroupId"],
    })


def _describe_thing_group(name: str) -> tuple:
    g = _thing_groups.get(name)
    if g is None:
        return _error_not_found("ThingGroup", name)
    return json_response(g)


def _list_thing_groups(qp: dict) -> tuple:
    return json_response({
        "thingGroups": [
            {"groupName": g["thingGroupName"], "groupArn": g["thingGroupArn"]}
            for g in _thing_groups.values()
        ]
    })


def _list_things_in_thing_group(path: str) -> tuple:
    """``GET /thing-groups/{groupName}/things``."""
    middle = path[len("/thing-groups/"):-len("/things")]
    g = _thing_groups.get(middle)
    if g is None:
        return _error_not_found("ThingGroup", middle)
    return json_response({"things": list(g.get("things", []))})


def _update_thing_group(name: str, payload: dict) -> tuple:
    g = _thing_groups.get(name)
    if g is None:
        return _error_not_found("ThingGroup", name)
    props = payload.get("thingGroupProperties") or {}
    if props:
        g["thingGroupProperties"].update({
            "thingGroupDescription": props.get("thingGroupDescription"),
        })
        attr_payload = props.get("attributePayload") or {}
        if attr_payload:
            g["thingGroupProperties"]["attributePayload"] = {
                "attributes": dict(attr_payload.get("attributes", {}))
            }
    g["version"] = g.get("version", 1) + 1
    _thing_groups[name] = g
    return json_response({"version": g["version"]})


def _delete_thing_group(name: str) -> tuple:
    g = _thing_groups.get(name)
    if g is None:
        return _error_not_found("ThingGroup", name)
    # Remove group from any Things that referenced it
    for tname in list(g.get("things", [])):
        thing = _things.get(tname)
        if thing and name in thing.get("thingGroupNames", []):
            thing["thingGroupNames"].remove(name)
            _things[tname] = thing
    del _thing_groups[name]
    return json_response({})


def _add_thing_to_group(payload: dict) -> tuple:
    gname = payload.get("thingGroupName")
    tname = payload.get("thingName")
    if not gname or not tname:
        return error_response_json(
            "InvalidRequestException", "thingGroupName and thingName are required", 400
        )
    group = _thing_groups.get(gname)
    if group is None:
        return _error_not_found("ThingGroup", gname)
    thing = _things.get(tname)
    if thing is None:
        return _error_not_found("Thing", tname)
    if tname not in group.get("things", []):
        group.setdefault("things", []).append(tname)
        _thing_groups[gname] = group
    if gname not in thing.get("thingGroupNames", []):
        thing.setdefault("thingGroupNames", []).append(gname)
        _things[tname] = thing
    return json_response({})


def _remove_thing_from_group(payload: dict) -> tuple:
    gname = payload.get("thingGroupName")
    tname = payload.get("thingName")
    group = _thing_groups.get(gname) if gname else None
    thing = _things.get(tname) if tname else None
    if group is None:
        return _error_not_found("ThingGroup", gname or "")
    if thing is None:
        return _error_not_found("Thing", tname or "")
    if tname in group.get("things", []):
        group["things"].remove(tname)
        _thing_groups[gname] = group
    if gname in thing.get("thingGroupNames", []):
        thing["thingGroupNames"].remove(gname)
        _things[tname] = thing
    return json_response({})


# ---------------------------------------------------------------------------
# Certificates
# ---------------------------------------------------------------------------


def _create_keys_and_certificate(qp: dict) -> tuple:
    """Generate a fresh keypair and sign a leaf certificate with the Local CA."""
    set_active = qp.get("setAsActive", "false").lower() == "true"
    try:
        cert_pem, private_pem, public_pem = local_ca.sign_leaf_certificate(
            common_name="AWS IoT Certificate"
        )
    except RuntimeError as e:
        return error_response_json("InternalFailureException", str(e), 503)
    cert_id = local_ca.get_certificate_id(cert_pem)
    arn = _cert_arn(cert_id)
    record = {
        "certificateId": cert_id,
        "certificateArn": arn,
        "certificatePem": cert_pem,
        "status": "ACTIVE" if set_active else "INACTIVE",
        "creationDate": _now_epoch(),
        "ownedBy": get_account_id(),
        "caCertificateId": None,
        "attachedThings": [],
        "attachedPolicies": [],
    }
    _certificates[cert_id] = record
    return json_response({
        "certificateArn": arn,
        "certificateId": cert_id,
        "certificatePem": cert_pem,
        "keyPair": {
            "PublicKey": public_pem,
            "PrivateKey": private_pem,
        },
    })


def _register_certificate(payload: dict, qp: dict) -> tuple:
    """Register a certificate that was issued elsewhere (no re-signing)."""
    cert_pem = payload.get("certificatePem") or qp.get("certificatePem")
    if not cert_pem:
        return error_response_json(
            "InvalidRequestException", "certificatePem is required", 400
        )
    set_active = bool(payload.get("setAsActive", False))
    status = payload.get("status")
    try:
        cert_id = local_ca.get_certificate_id(cert_pem)
    except Exception as e:
        return error_response_json(
            "CertificateValidationException",
            f"Invalid certificate PEM: {e}",
            400,
        )
    if cert_id in _certificates:
        return error_response_json(
            "ResourceAlreadyExistsException",
            "Certificate already registered",
            409,
        )
    record = {
        "certificateId": cert_id,
        "certificateArn": _cert_arn(cert_id),
        "certificatePem": cert_pem,  # verbatim
        "status": status or ("ACTIVE" if set_active else "INACTIVE"),
        "creationDate": _now_epoch(),
        "ownedBy": get_account_id(),
        "caCertificateId": payload.get("caCertificatePem") and local_ca.get_certificate_id(payload["caCertificatePem"]) or None,
        "attachedThings": [],
        "attachedPolicies": [],
    }
    _certificates[cert_id] = record
    return json_response({
        "certificateArn": record["certificateArn"],
        "certificateId": cert_id,
    })


def _list_certificates(qp: dict) -> tuple:
    return json_response({
        "certificates": [
            {
                "certificateArn": c["certificateArn"],
                "certificateId": c["certificateId"],
                "status": c["status"],
                "creationDate": c.get("creationDate"),
            }
            for c in _certificates.values()
        ]
    })


def _handle_certificate(method: str, path: str, body: bytes, qp: dict) -> tuple:
    cert_id = path[len("/certificates/"):]
    if not cert_id or "/" in cert_id:
        return error_response_json(
            "InvalidRequestException", "Invalid certificate path", 400
        )
    record = _certificates.get(cert_id)
    if record is None:
        return _error_not_found("Certificate", cert_id)
    if method == "GET":
        return json_response({
            "certificateDescription": {
                "certificateArn": record["certificateArn"],
                "certificateId": record["certificateId"],
                "status": record["status"],
                "certificatePem": record["certificatePem"],
                "ownedBy": record["ownedBy"],
                "creationDate": record.get("creationDate"),
            }
        })
    if method == "PUT":
        payload = _parse_body(body)
        new_status = payload.get("newStatus") or qp.get("newStatus")
        valid = {"ACTIVE", "INACTIVE", "REVOKED", "PENDING_TRANSFER", "PENDING_ACTIVATION"}
        if new_status not in valid:
            return error_response_json(
                "InvalidRequestException",
                f"newStatus must be one of {sorted(valid)}",
                400,
            )
        record["status"] = new_status
        _certificates[cert_id] = record
        return json_response({})
    if method == "DELETE":
        if record["status"] == "ACTIVE":
            return error_response_json(
                "CertificateStateException",
                "Certificate is ACTIVE; deactivate before deletion",
                409,
            )
        del _certificates[cert_id]
        return json_response({})
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


def _handle_thing_principals(method: str, path: str, headers: dict, body: bytes, qp: dict) -> tuple:
    """``PUT/DELETE /things/{name}/principals`` and ``GET /things/{name}/principals``.

    AWS uses an ``x-amzn-principal`` header containing the principal ARN
    (typically a certificate ARN) for ``AttachThingPrincipal`` /
    ``DetachThingPrincipal``.
    """
    middle = path[len("/things/"):-len("/principals")]
    if "/" in middle:
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
        )
    name = middle
    thing = _things.get(name)
    if thing is None:
        return _error_not_found("Thing", name)

    if method == "GET":
        return json_response({"principals": list(thing.get("principals", []))})

    # PUT/DELETE require x-amzn-principal header (AWS convention).
    principal = headers.get("x-amzn-principal") or qp.get("principal")
    if not principal:
        return error_response_json(
            "InvalidRequestException", "principal is required", 400
        )
    cert_id = principal.rsplit("/", 1)[-1]
    cert = _certificates.get(cert_id)
    if cert is None:
        return _error_not_found("Principal", principal)

    if method == "PUT":
        if principal not in thing.setdefault("principals", []):
            thing["principals"].append(principal)
            _things[name] = thing
        if thing["thingArn"] not in cert.setdefault("attachedThings", []):
            cert["attachedThings"].append(thing["thingArn"])
            _certificates[cert_id] = cert
        return json_response({})
    if method == "DELETE":
        if principal in thing.get("principals", []):
            thing["principals"].remove(principal)
            _things[name] = thing
        if thing["thingArn"] in cert.get("attachedThings", []):
            cert["attachedThings"].remove(thing["thingArn"])
            _certificates[cert_id] = cert
        return json_response({})
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


def _list_principal_things(headers: dict, qp: dict) -> tuple:
    """``GET /principals/things`` with the principal in the ``x-amzn-principal`` header."""
    principal = headers.get("x-amzn-principal") or qp.get("principal")
    if not principal:
        return error_response_json(
            "InvalidRequestException", "principal is required", 400
        )
    cert_id = principal.rsplit("/", 1)[-1]
    cert = _certificates.get(cert_id)
    if cert is None:
        return _error_not_found("Principal", principal)
    things = []
    for arn in cert.get("attachedThings", []):
        tname = arn.rsplit("/", 1)[-1]
        if tname in _things:
            things.append(tname)
    return json_response({"things": things})


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------


def _handle_policy(method: str, path: str, body: bytes, qp: dict) -> tuple:
    suffix = path[len("/policies/"):]
    parts = suffix.split("/")
    name = parts[0]

    err = _validate_name(name, "policyName")
    if err:
        return err

    # /policies/{name}/version/{versionId}
    if len(parts) >= 3 and parts[1] == "version":
        version_id = parts[2]
        if method == "GET":
            return _get_policy_version(name, version_id)
        if method == "DELETE":
            return _delete_policy_version(name, version_id)

    # /policies/{name}/version
    if len(parts) == 2 and parts[1] == "version":
        if method == "POST":
            return _create_policy_version(name, _parse_body(body), qp)
        if method == "GET":
            return _list_policy_versions(name)

    if len(parts) == 1:
        if method == "POST":
            return _create_policy(name, _parse_body(body))
        if method == "GET":
            return _get_policy(name)
        if method == "DELETE":
            return _delete_policy(name)

    return error_response_json(
        "InvalidRequestException", f"Unsupported policy path: {method} {path}", 400
    )


def _create_policy(name: str, payload: dict) -> tuple:
    if name in _policies:
        return error_response_json(
            "ResourceAlreadyExistsException",
            f"Policy {name!r} already exists",
            409,
        )
    doc = payload.get("policyDocument")
    if not doc:
        return error_response_json(
            "InvalidRequestException", "policyDocument is required", 400
        )
    try:
        json.loads(doc)
    except (TypeError, json.JSONDecodeError):
        return error_response_json(
            "MalformedPolicyException",
            "policyDocument is not valid JSON",
            400,
        )
    record = {
        "policyName": name,
        "policyArn": _policy_arn(name),
        "defaultVersionId": "1",
        "versions": {
            "1": {
                "document": doc,
                "isDefaultVersion": True,
                "createDate": _now_epoch(),
            },
        },
        "targets": [],
    }
    _policies[name] = record
    return json_response({
        "policyName": name,
        "policyArn": record["policyArn"],
        "policyDocument": doc,
        "policyVersionId": "1",
    })


def _get_policy(name: str) -> tuple:
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    default_id = p["defaultVersionId"]
    return json_response({
        "policyName": name,
        "policyArn": p["policyArn"],
        "policyDocument": p["versions"][default_id]["document"],
        "defaultVersionId": default_id,
    })


def _list_policies(qp: dict) -> tuple:
    return json_response({
        "policies": [
            {"policyName": p["policyName"], "policyArn": p["policyArn"]}
            for p in _policies.values()
        ]
    })


def _delete_policy(name: str) -> tuple:
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    if p.get("targets"):
        return error_response_json(
            "DeleteConflictException",
            "Policy is attached; detach it before deletion",
            409,
        )
    del _policies[name]
    return json_response({})


def _create_policy_version(name: str, payload: dict, qp: dict) -> tuple:
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    doc = payload.get("policyDocument")
    if not doc:
        return error_response_json(
            "InvalidRequestException", "policyDocument is required", 400
        )
    try:
        json.loads(doc)
    except (TypeError, json.JSONDecodeError):
        return error_response_json(
            "MalformedPolicyException",
            "policyDocument is not valid JSON",
            400,
        )
    set_default = (
        bool(payload.get("setAsDefault"))
        or qp.get("setAsDefault", "").lower() == "true"
    )
    next_id = str(max(int(v) for v in p["versions"].keys()) + 1)
    if set_default:
        for v in p["versions"].values():
            v["isDefaultVersion"] = False
        p["defaultVersionId"] = next_id
    p["versions"][next_id] = {
        "document": doc,
        "isDefaultVersion": set_default,
        "createDate": _now_epoch(),
    }
    _policies[name] = p
    return json_response({
        "policyArn": p["policyArn"],
        "policyDocument": doc,
        "policyVersionId": next_id,
        "isDefaultVersion": set_default,
    })


def _get_policy_version(name: str, version_id: str) -> tuple:
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    v = p["versions"].get(version_id)
    if v is None:
        return _error_not_found("PolicyVersion", version_id)
    return json_response({
        "policyArn": p["policyArn"],
        "policyDocument": v["document"],
        "policyVersionId": version_id,
        "isDefaultVersion": v["isDefaultVersion"],
        "creationDate": v.get("createDate"),
    })


def _list_policy_versions(name: str) -> tuple:
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    return json_response({
        "policyVersions": [
            {
                "versionId": vid,
                "isDefaultVersion": v["isDefaultVersion"],
                "createDate": v.get("createDate"),
            }
            for vid, v in p["versions"].items()
        ]
    })


def _delete_policy_version(name: str, version_id: str) -> tuple:
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    if version_id not in p["versions"]:
        return _error_not_found("PolicyVersion", version_id)
    if p["defaultVersionId"] == version_id:
        return error_response_json(
            "InvalidRequestException",
            "Cannot delete the default policy version",
            400,
        )
    del p["versions"][version_id]
    _policies[name] = p
    return json_response({})


# AttachPolicy / DetachPolicy via /target-policies/{policyName}
# Body: {"target": "arn:..."}


def _handle_target_policy(method: str, path: str, body: bytes, qp: dict) -> tuple:
    """Handles ``/target-policies/{policyName}``.

    AWS uses ``PUT`` for ``AttachPolicy`` and ``POST`` for ``DetachPolicy``
    (yes, both write methods on the same path; the verb selects the action).
    """
    name = path[len("/target-policies/"):]
    if "/" in name:
        return error_response_json(
            "InvalidRequestException", "Invalid target-policies path", 400
        )
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    payload = _parse_body(body)
    target = payload.get("target")
    if not target:
        return error_response_json(
            "InvalidRequestException", "target is required", 400
        )
    if method == "PUT":
        if target not in p.setdefault("targets", []):
            p["targets"].append(target)
            _policies[name] = p
        cert_id = target.rsplit("/", 1)[-1]
        cert = _certificates.get(cert_id)
        if cert is not None and name not in cert.setdefault("attachedPolicies", []):
            cert["attachedPolicies"].append(name)
            _certificates[cert_id] = cert
        return json_response({})
    if method in ("POST", "DELETE"):
        if target in p.get("targets", []):
            p["targets"].remove(target)
            _policies[name] = p
        cert_id = target.rsplit("/", 1)[-1]
        cert = _certificates.get(cert_id)
        if cert is not None and name in cert.get("attachedPolicies", []):
            cert["attachedPolicies"].remove(name)
            _certificates[cert_id] = cert
        return json_response({})
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


def _list_targets_for_policy(path: str, qp: dict) -> tuple:
    """``GET|POST /policy-targets/{policyName}``."""
    name = path[len("/policy-targets/"):]
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    return json_response({"targets": list(p.get("targets", []))})


def _list_attached_policies(path: str, qp: dict) -> tuple:
    """``POST /attached-policies/{target}`` — returns policies attached to target.

    The target segment is URL-encoded by the SDK (the certificate ARN
    contains colons / slashes); the ASGI layer hands us the decoded value.
    """
    target = path[len("/attached-policies/"):]
    out = []
    for p in _policies.values():
        if target in p.get("targets", []):
            out.append({"policyName": p["policyName"], "policyArn": p["policyArn"]})
    return json_response({"policies": out})


# ---------------------------------------------------------------------------
# Helper exports for iot_data / iot_broker
# ---------------------------------------------------------------------------


def lookup_certificate_by_id(cert_id: str) -> dict | None:
    """Return the Certificate record for a given certificateId in the current account, or None."""
    return _certificates.get(cert_id)
