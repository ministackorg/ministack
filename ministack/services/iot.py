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

This is the control plane — pure HTTP/JSON, no MQTT broker
dependency. The data plane (``iot_data.py``, ``iot_broker.py``) is
implemented separately and only depends on this module for certificate
lookups (mTLS).

State is fully isolated per account via ``AccountScopedDict`` and persisted
through ``get_state``/``restore_state``. The Local CA (used to sign
``CreateKeysAndCertificate`` certificates) is also persisted so previously
issued client certificates remain valid across restarts.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import json
import logging
import os
import re
import struct
import time
import uuid
from datetime import datetime, timezone
from typing import Awaitable, Callable

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    _request_account_id,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)
from ministack.core.x509_utils import (
    generate_ca,
    get_certificate_id,
    sign_leaf_certificate,
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
_topic_rules: AccountScopedDict = AccountScopedDict()  # ruleName -> TopicRule dict
_shadows: AccountScopedDict = AccountScopedDict()  # (thingName, shadowName) -> shadow dict

# Local CA state — lazily generated on first use, persisted across restarts.
import threading

_CA_LOCK = threading.Lock()
_ca_cert_pem: str | None = None
_ca_key_pem: str | None = None


def _ensure_ca() -> tuple[str, str]:
    """Return (cert_pem, key_pem), generating lazily on first use."""
    global _ca_cert_pem, _ca_key_pem
    if _ca_cert_pem is not None and _ca_key_pem is not None:
        return _ca_cert_pem, _ca_key_pem
    with _CA_LOCK:
        if _ca_cert_pem is not None and _ca_key_pem is not None:
            return _ca_cert_pem, _ca_key_pem
        cert_pem, key_pem = generate_ca()
        _ca_cert_pem = cert_pem
        _ca_key_pem = key_pem
        logger.info("Local CA: generated new self-signed root certificate")
        return cert_pem, key_pem


def get_ca_cert_pem() -> str:
    """Return the CA certificate in PEM format. Generates the CA on first call."""
    cert_pem, _ = _ensure_ca()
    return cert_pem


# ---------------------------------------------------------------------------
# Broker state
# ---------------------------------------------------------------------------

_retained: dict[str, "_RetainedMessage"] = {}


class _RetainedMessage:
    __slots__ = ("payload", "qos", "topic", "ts")

    def __init__(self, topic: str, payload: bytes, qos: int):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.ts = time.time()


def _broker_get_state() -> dict:
    retained_list = []
    for topic, msg in _retained.items():
        retained_list.append({
            "topic": msg.topic,
            "payload": base64.b64encode(msg.payload).decode("ascii"),
            "qos": msg.qos,
        })
    return {"retained": retained_list}


def _broker_restore_state(data: dict | None) -> None:
    if not data:
        return
    for entry in data.get("retained", []):
        topic = entry["topic"]
        payload = base64.b64decode(entry["payload"])
        qos = entry.get("qos", 0)
        _retained[topic] = _RetainedMessage(topic, payload, qos)


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
        "topic_rules": copy.deepcopy(_topic_rules),
        "shadows": copy.deepcopy(_shadows),
        "ca": {"ca_cert_pem": _ca_cert_pem, "ca_key_pem": _ca_key_pem}
        if _ca_cert_pem and _ca_key_pem
        else {},
        "mqtt_broker": _broker_get_state(),
    }


def restore_state(data: dict | None) -> None:
    global _ca_cert_pem, _ca_key_pem
    if not data:
        return
    _things.update(data.get("things", {}))
    _thing_types.update(data.get("thing_types", {}))
    _thing_groups.update(data.get("thing_groups", {}))
    _certificates.update(data.get("certificates", {}))
    _policies.update(data.get("policies", {}))
    _topic_rules.update(data.get("topic_rules", {}))
    _shadows.update(data.get("shadows", {}))
    ca_data = data.get("ca")
    if ca_data:
        cert = ca_data.get("ca_cert_pem")
        key = ca_data.get("ca_key_pem")
        if cert and key:
            with _CA_LOCK:
                _ca_cert_pem = cert
                _ca_key_pem = key
            logger.info("Local CA: restored from persisted state")
    _broker_restore_state(data.get("mqtt_broker"))


def reset() -> None:
    global _ca_cert_pem, _ca_key_pem
    _things.clear()
    _thing_types.clear()
    _thing_groups.clear()
    _certificates.clear()
    _policies.clear()
    _topic_rules.clear()
    _shadows.clear()
    with _CA_LOCK:
        _ca_cert_pem = None
        _ca_key_pem = None


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


def _thing_name_from_arn(arn: str) -> str:
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return ""
    prefix = "thing/"
    if (
        spec.service != "iot"
        or spec.account_id != get_account_id()
        or spec.region != get_region()
        or not spec.resource.startswith(prefix)
    ):
        return ""
    name = spec.resource[len(prefix):]
    if not name or "/" in name:
        return ""
    return name


def _thing_type_arn(name: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:thingtype/{name}"


def _thing_group_arn(name: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:thinggroup/{name}"


def _cert_arn(certificate_id: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:cert/{certificate_id}"


def _policy_arn(name: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:policy/{name}"


def _topic_rule_arn(name: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:rule/{name}"


# Rule names are stricter than other IoT resources: [a-zA-Z0-9_] only.
_RULE_NAME_RE = re.compile(r"^[a-zA-Z0-9_]{1,128}$")


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

    # Topic rules
    if path == "/rules" and method == "GET":
        return _list_topic_rules(qp)
    if path.startswith("/rules/"):
        return _handle_topic_rule(method, path, body)

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
        ca_cert_pem, ca_key_pem = _ensure_ca()
        cert_pem, private_pem, public_pem = sign_leaf_certificate(
            ca_cert_pem=ca_cert_pem,
            ca_key_pem=ca_key_pem,
            common_name="AWS IoT Certificate",
        )
    except RuntimeError as e:
        return error_response_json("InternalFailureException", str(e), 503)
    cert_id = get_certificate_id(cert_pem)
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
        cert_id = get_certificate_id(cert_pem)
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
        "caCertificateId": payload.get("caCertificatePem") and get_certificate_id(payload["caCertificatePem"]) or None,
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
        tname = _thing_name_from_arn(arn)
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
# Topic rules
# ---------------------------------------------------------------------------


def _rule_topic_filter(sql: str) -> str:
    """Extract the topic filter from a rule SQL ``FROM '<topic>'`` clause."""
    m = re.search(r"\bFROM\s+'([^']*)'", sql or "", re.IGNORECASE)
    return m.group(1) if m else ""


def put_topic_rule(name: str, payload: dict, *, created_at: float | None = None) -> dict:
    """Store a topic rule from an API-shape (camelCase) ``TopicRulePayload``."""
    rule = {
        "ruleName": name,
        "sql": payload.get("sql", ""),
        "actions": payload.get("actions", []) or [],
        "ruleDisabled": bool(payload.get("ruleDisabled", False)),
        "awsIotSqlVersion": payload.get("awsIotSqlVersion", "2016-03-23"),
        "description": payload.get("description", ""),
        "errorAction": payload.get("errorAction"),
        "createdAt": created_at if created_at is not None else _now_epoch(),
    }
    _topic_rules[name] = rule
    return rule


def delete_topic_rule(name: str) -> None:
    _topic_rules.pop(name, None)


def _handle_topic_rule(method: str, path: str, body: bytes) -> tuple:
    name = path[len("/rules/"):]
    if method == "POST":
        return _create_topic_rule(name, _parse_body(body))
    if method == "GET":
        return _get_topic_rule(name)
    if method == "PATCH":
        return _replace_topic_rule(name, _parse_body(body))
    if method == "DELETE":
        return _delete_topic_rule(name)
    return error_response_json(
        "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
    )


def _create_topic_rule(name: str, payload: dict) -> tuple:
    if not _RULE_NAME_RE.match(name or ""):
        return error_response_json(
            "InvalidRequestException",
            "Invalid ruleName: must match [a-zA-Z0-9_]{1,128}",
            400,
        )
    if name in _topic_rules:
        return error_response_json(
            "ResourceAlreadyExistsException", f"Rule {name!r} already exists", 409
        )
    if not payload.get("sql"):
        return error_response_json("SqlParseException", "sql is required", 400)
    put_topic_rule(name, payload)
    return json_response({})


def _replace_topic_rule(name: str, payload: dict) -> tuple:
    if name not in _topic_rules:
        return _error_not_found("Rule", name)
    put_topic_rule(name, payload)
    return json_response({})


def _get_topic_rule(name: str) -> tuple:
    rule = _topic_rules.get(name)
    if rule is None:
        return _error_not_found("Rule", name)
    return json_response({"ruleArn": _topic_rule_arn(name), "rule": rule})


def _delete_topic_rule(name: str) -> tuple:
    _topic_rules.pop(name, None)
    return json_response({})


def _list_topic_rules(qp: dict) -> tuple:
    rules = []
    for r in _topic_rules.values():
        rules.append({
            "ruleName": r["ruleName"],
            "ruleArn": _topic_rule_arn(r["ruleName"]),
            "topicPattern": _rule_topic_filter(r["sql"]),
            "createdAt": r["createdAt"],
            "ruleDisabled": r["ruleDisabled"],
        })
    return json_response({"rules": rules})


# ---------------------------------------------------------------------------
# Helper exports for iot_data
# ---------------------------------------------------------------------------


def lookup_certificate_by_id(cert_id: str) -> dict | None:
    """Return the Certificate record for a given certificateId in the current account, or None."""
    return _certificates.get(cert_id)


# ---------------------------------------------------------------------------
# Device Shadow (consumed by iot_data.py)
# ---------------------------------------------------------------------------


def _shadow_now() -> int:
    return int(time.time())


def _deep_merge(base: dict, patch: dict) -> dict:
    """Merge ``patch`` into ``base`` in place. A ``null`` value removes the key."""
    for k, v in patch.items():
        if v is None:
            base.pop(k, None)
        elif isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def _build_metadata(state: dict, ts: int):
    """Mirror a state subtree, replacing each leaf with ``{"timestamp": ts}``."""
    if isinstance(state, dict):
        return {k: _build_metadata(v, ts) for k, v in state.items()}
    return {"timestamp": ts}


def _compute_delta(desired: dict, reported: dict):
    """Fields in ``desired`` that differ from ``reported`` (recursing into dicts)."""
    delta = {}
    for k, dv in desired.items():
        rv = reported.get(k)
        if isinstance(dv, dict) and isinstance(rv, dict):
            sub = _compute_delta(dv, rv)
            if sub:
                delta[k] = sub
        elif dv != rv:
            delta[k] = dv
    return delta


def _merge_metadata(base: dict, patch: dict, ts: int) -> dict:
    """Stamp a new ``timestamp`` only for the leaves the patch changes,
    preserving the timestamps of attributes the patch does not touch (a
    ``null`` removes the key). Mirrors ``_deep_merge`` so metadata tracks
    per-attribute update times like real AWS IoT, instead of re-stamping the
    whole document on every update."""
    for k, v in patch.items():
        if v is None:
            base.pop(k, None)
        elif isinstance(v, dict):
            sub = base.get(k)
            if not isinstance(sub, dict):
                sub = {}
                base[k] = sub
            _merge_metadata(sub, v, ts)
        else:
            base[k] = {"timestamp": ts}
    return base


def _metadata_for_delta(delta: dict, desired_meta: dict) -> dict:
    """Project the ``desired`` metadata onto the delta shape — AWS reports the
    delta's metadata as the metadata of the matching desired attributes."""
    out = {}
    for k, dv in delta.items():
        dm = desired_meta.get(k) if isinstance(desired_meta, dict) else None
        if isinstance(dv, dict):
            out[k] = _metadata_for_delta(dv, dm if isinstance(dm, dict) else {})
        elif isinstance(dm, dict):
            out[k] = dm
        else:
            out[k] = {"timestamp": _shadow_now()}
    return out


def _shadow_error(status: int, message: str) -> tuple:
    return status, {"message": message}


def update_thing_shadow(thing_name: str, shadow_name: str, request: dict) -> tuple:
    """Merge ``request`` into the shadow. Returns (status, response_doc)."""
    if not isinstance(request, dict) or not isinstance(request.get("state"), dict):
        return _shadow_error(400, "Missing required node: state")

    req_state = request["state"]
    key = (thing_name, shadow_name)
    rec = _shadows.get(key)
    if rec is None or rec.get("deleted"):
        # A deleted shadow keeps its version — AWS does not reset it to 0, so
        # the next update resumes from the retained version instead of 1.
        rec = {"state": {"desired": {}, "reported": {}},
               "version": rec["version"] if rec else 0}

    expected = request.get("version")
    if expected is not None and expected != rec["version"]:
        return _shadow_error(409, "Version conflict")

    ts = _shadow_now()
    for section in ("desired", "reported"):
        if section in req_state:
            patch = req_state[section]
            if patch is None:
                rec["state"][section] = {}
            elif isinstance(patch, dict):
                _deep_merge(rec["state"].setdefault(section, {}), patch)

    rec["version"] += 1
    meta = rec.setdefault("metadata", {"desired": {}, "reported": {}})
    for section in ("desired", "reported"):
        if section in req_state:
            patch = req_state[section]
            if patch is None:
                meta[section] = {}
            elif isinstance(patch, dict):
                _merge_metadata(meta.setdefault(section, {}), patch, ts)
    _shadows[key] = rec

    # The /accepted response echoes only the sections present in the request.
    resp_state = {s: req_state[s] for s in ("desired", "reported") if s in req_state and req_state[s] is not None}
    resp_meta = {s: _build_metadata(req_state[s], ts) for s in resp_state}
    doc = {"state": resp_state, "metadata": resp_meta, "version": rec["version"], "timestamp": ts}
    if request.get("clientToken") is not None:
        doc["clientToken"] = request["clientToken"]
    return 200, doc


def get_thing_shadow(thing_name: str, shadow_name: str) -> tuple:
    """Return (status, full_shadow_doc) or a 404 error doc if none exists."""
    rec = _shadows.get((thing_name, shadow_name))
    if rec is None or rec.get("deleted"):
        label = f"{thing_name}" if not shadow_name else f"{thing_name}/{shadow_name}"
        return _shadow_error(404, f"No shadow exists with name: {label}")

    desired = rec["state"].get("desired", {})
    reported = rec["state"].get("reported", {})
    state = {}
    if desired:
        state["desired"] = desired
    if reported:
        state["reported"] = reported
    metadata = dict(rec.get("metadata", {"desired": {}, "reported": {}}))
    delta = _compute_delta(desired, reported)
    if delta:
        state["delta"] = delta
        metadata["delta"] = _metadata_for_delta(delta, metadata.get("desired", {}))
    doc = {
        "state": state,
        "metadata": metadata,
        "version": rec["version"],
        "timestamp": _shadow_now(),
    }
    return 200, doc


def delete_thing_shadow(thing_name: str, shadow_name: str) -> tuple:
    """Delete a shadow. Returns (status, doc). 404 if it does not exist."""
    key = (thing_name, shadow_name)
    rec = _shadows.get(key)
    if rec is None or rec.get("deleted"):
        label = f"{thing_name}" if not shadow_name else f"{thing_name}/{shadow_name}"
        return _shadow_error(404, f"No shadow exists with name: {label}")
    version = rec["version"]
    # Retain the version as a tombstone — AWS does not reset it on delete.
    _shadows[key] = {"deleted": True, "version": version}
    return 200, {"version": version, "timestamp": _shadow_now()}


# ===========================================================================
# MQTT Broker — embedded MQTT 3.1.1 broker logic over WebSocket
# ===========================================================================
#
# The broker owns a small in-process pub/sub registry plus an MQTT 3.1.1
# framing layer used between the broker and WebSocket clients (per the AWS
# WS-MQTT subprotocol).
#
# Architecture (mirrors Transfer Family's shared SFTP listener):
#   Client → WebSocket (gateway port) → Bridge → in-memory pub/sub
#
# Multi-tenancy is enforced by transparent topic prefixing: every
# PUBLISH/SUBSCRIBE topic seen on the wire is internally prefixed with the
# caller's account_id before it hits the registry, and the prefix is
# stripped on outbound delivery.

_broker_logger = logging.getLogger("iot_broker")

# ---------------------------------------------------------------------------
# In-memory pub/sub registry
# ---------------------------------------------------------------------------

_subscriptions: dict[str, set["_Subscription"]] = {}
_connected_clients: dict[tuple[str, str], "_WSSession"] = {}
_persistent_sessions: dict[tuple[str, str], "_PersistentSessionState"] = {}
_broker_lock = asyncio.Lock()

_SESSION_EXPIRY_SECONDS: int = int(os.environ.get("IOT_SESSION_EXPIRY_SECONDS", "3600"))
_MAX_QUEUED_MESSAGES = 1000


class _PersistentSessionState:
    __slots__ = ("subscriptions", "queued_messages", "created_at")

    def __init__(self, subscriptions: list[str], created_at: float):
        self.subscriptions: list[str] = subscriptions
        self.queued_messages: list[tuple[str, bytes, int]] = []
        self.created_at: float = created_at


def _is_session_expired(session_state: _PersistentSessionState) -> bool:
    return (time.time() - session_state.created_at) > _SESSION_EXPIRY_SECONDS


class _InFlightMessage:
    __slots__ = ("packet_id", "topic", "payload", "sent_at", "retransmit_count")

    def __init__(self, packet_id: int, topic: str, payload: bytes):
        self.packet_id = packet_id
        self.topic = topic
        self.payload = payload
        self.sent_at = asyncio.get_event_loop().time()
        self.retransmit_count = 0


_RETRANSMIT_INTERVAL_SECONDS = int(os.environ.get("IOT_RETRANSMIT_SECONDS", "10"))


class _Subscription:
    __slots__ = ("subscription_id", "filter_prefixed", "account_id", "deliver", "granted_qos")

    def __init__(
        self,
        filter_prefixed: str,
        account_id: str,
        deliver: Callable[[str, bytes, int], Awaitable[None]],
        granted_qos: int = 0,
    ):
        self.subscription_id = uuid.uuid4().hex
        self.filter_prefixed = filter_prefixed
        self.account_id = account_id
        self.deliver = deliver
        self.granted_qos = granted_qos

    def __hash__(self) -> int:
        return hash(self.subscription_id)

    def __eq__(self, other) -> bool:
        return isinstance(other, _Subscription) and other.subscription_id == self.subscription_id


# ---------------------------------------------------------------------------
# Topic prefixing & matching
# ---------------------------------------------------------------------------


def _scoped_topic(account_id: str, topic: str) -> str:
    return f"{account_id}/{topic}"


def _unscope_topic(account_id: str, scoped_topic: str) -> str:
    prefix = f"{account_id}/"
    if scoped_topic.startswith(prefix):
        return scoped_topic[len(prefix):]
    return scoped_topic


def _topic_matches(filter_: str, topic: str) -> bool:
    f_parts = filter_.split("/")
    t_parts = topic.split("/")
    fi = ti = 0
    while fi < len(f_parts):
        f = f_parts[fi]
        if f == "#":
            return True
        if ti >= len(t_parts):
            return False
        if f != "+" and f != t_parts[ti]:
            return False
        fi += 1
        ti += 1
    return ti == len(t_parts)


# ---------------------------------------------------------------------------
# Topic validation
# ---------------------------------------------------------------------------

_MQTT_MAX_TOPIC_BYTES = 256


def _validate_publish_topic(topic: str) -> bool:
    if not topic:
        return False
    if "+" in topic or "#" in topic:
        return False
    if len(topic.encode("utf-8")) > _MQTT_MAX_TOPIC_BYTES:
        return False
    return True


# ---------------------------------------------------------------------------
# Broker public API (consumed by iot_data.py and handle_websocket)
# ---------------------------------------------------------------------------


def broker_is_available() -> bool:
    return True


async def broker_start() -> None:
    return None


async def broker_stop() -> None:
    async with _broker_lock:
        _subscriptions.clear()
        _retained.clear()
        _connected_clients.clear()
        _persistent_sessions.clear()


_BASIC_INGEST_PREFIX = "$aws/rules/"


def _rules_for_account(account_id: str) -> list[dict]:
    return [v for (acct, _key), v in _topic_rules._data.items() if acct == account_id]


def _rule_event(payload: bytes):
    """Decode a publish payload into a rule event (JSON, else raw text)."""
    try:
        return json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return (payload or b"").decode("utf-8", "replace")


def _dispatch_rule_to_lambda(account_id: str, function_arn: str, event) -> None:
    from ministack.services import lambda_svc

    func, config, name = lambda_svc._get_func_record_for_ref_in_scope(
        function_arn, account_id=account_id
    )
    if not func or not config:
        _broker_logger.warning("IoT rule → Lambda: function %s not found", function_arn)
        return
    exec_record = lambda_svc._execution_record_for_config(func, config)
    threading.Thread(
        target=lambda_svc._execute_function_with_config_scope,
        args=(exec_record, event),
        daemon=True,
    ).start()


def _run_rule_actions(account_id: str, rule: dict, payload: bytes) -> None:
    if not rule or rule.get("ruleDisabled"):
        return
    event = _rule_event(payload)
    for action in rule.get("actions", []) or []:
        lam = action.get("lambda")
        if lam and lam.get("functionArn"):
            _dispatch_rule_to_lambda(account_id, lam["functionArn"], event)


def _evaluate_topic_rules(account_id: str, topic: str, payload: bytes) -> None:
    for rule in _rules_for_account(account_id):
        filter_ = _rule_topic_filter(rule.get("sql", ""))
        if filter_ and _topic_matches(filter_, topic):
            _run_rule_actions(account_id, rule, payload)


async def broker_publish(
    account_id: str,
    topic: str,
    payload: bytes,
    qos: int = 0,
    retain: bool = False,
) -> None:
    # Basic Ingest: a publish to `$aws/rules/<ruleName>` is delivered straight
    # to that rule's actions and bypasses pub/sub delivery entirely.
    if topic.startswith(_BASIC_INGEST_PREFIX):
        rule_name = topic[len(_BASIC_INGEST_PREFIX):].split("/", 1)[0]
        _run_rule_actions(account_id, _topic_rules.get_scoped(account_id, None, rule_name), payload)
        return

    scoped = _scoped_topic(account_id, topic)

    if retain:
        if not payload:
            _retained.pop(scoped, None)
        else:
            _retained[scoped] = _RetainedMessage(scoped, payload, qos)

    async with _broker_lock:
        subs = [s for sset in _subscriptions.values() for s in sset]

    for sub in subs:
        if _topic_matches(sub.filter_prefixed, scoped):
            try:
                effective_qos = min(qos, sub.granted_qos)
                await sub.deliver(_unscope_topic(sub.account_id, scoped), payload, effective_qos)
            except Exception:
                _broker_logger.exception("IoT broker: subscriber %s delivery failed", sub.subscription_id)

    if qos >= 1:
        for key, ps in list(_persistent_sessions.items()):
            ps_account_id, ps_client_id = key
            if ps_account_id != account_id:
                continue
            if key in _connected_clients:
                continue
            if _is_session_expired(ps):
                continue
            for filt in ps.subscriptions:
                scoped_filter = _scoped_topic(ps_account_id, filt)
                if _topic_matches(scoped_filter, scoped):
                    ps.queued_messages.append((topic, payload, qos))
                    if len(ps.queued_messages) > _MAX_QUEUED_MESSAGES:
                        ps.queued_messages = ps.queued_messages[-_MAX_QUEUED_MESSAGES:]
                    break

    _evaluate_topic_rules(account_id, topic, payload)


async def broker_subscribe(
    account_id: str,
    topic_filter: str,
    callback: Callable[[str, bytes, int], Awaitable[None]],
    granted_qos: int = 0,
) -> str:
    filter_prefixed = _scoped_topic(account_id, topic_filter)
    sub = _Subscription(filter_prefixed, account_id, callback, granted_qos)
    async with _broker_lock:
        _subscriptions.setdefault(filter_prefixed, set()).add(sub)
        has_wildcard = "+" in topic_filter or "#" in topic_filter
        if not has_wildcard:
            retained_to_send = [
                r for k, r in _retained.items() if _topic_matches(filter_prefixed, k)
            ]
        else:
            retained_to_send = []

    for r in retained_to_send:
        try:
            await sub.deliver(_unscope_topic(account_id, r.topic), r.payload, r.qos)
        except Exception:
            _broker_logger.exception("IoT broker: retained-message delivery failed")

    return sub.subscription_id


async def broker_unsubscribe(subscription_id: str) -> None:
    async with _broker_lock:
        for filter_, subs in list(_subscriptions.items()):
            for s in list(subs):
                if s.subscription_id == subscription_id:
                    subs.discard(s)
            if not subs:
                _subscriptions.pop(filter_, None)


def broker_reset() -> None:
    _subscriptions.clear()
    _retained.clear()
    _connected_clients.clear()
    _persistent_sessions.clear()


# ---------------------------------------------------------------------------
# Connected-client registry & duplicate detection
# ---------------------------------------------------------------------------


def _register_client(account_id: str, client_id: str, session: "_WSSession") -> None:
    _connected_clients[(account_id, client_id)] = session


def _deregister_client(account_id: str, client_id: str) -> None:
    _connected_clients.pop((account_id, client_id), None)


async def _force_disconnect_duplicate(account_id: str, client_id: str) -> None:
    key = (account_id, client_id)
    existing = _connected_clients.get(key)
    if existing is not None:
        _broker_logger.info("IoT broker: duplicate client_id=%s, forcing old connection closed", client_id)
        try:
            await existing._send({"type": "websocket.close", "code": 1000})
        except Exception:
            pass
        await existing.cleanup()
        _connected_clients.pop(key, None)


# ---------------------------------------------------------------------------
# MQTT 3.1.1 frame codec
# ---------------------------------------------------------------------------

PKT_CONNECT = 1
PKT_CONNACK = 2
PKT_PUBLISH = 3
PKT_PUBACK = 4
PKT_SUBSCRIBE = 8
PKT_SUBACK = 9
PKT_UNSUBSCRIBE = 10
PKT_UNSUBACK = 11
PKT_PINGREQ = 12
PKT_PINGRESP = 13
PKT_DISCONNECT = 14


def _encode_remaining_length(n: int) -> bytes:
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n > 0:
            byte |= 0x80
        out.append(byte)
        if n == 0:
            return bytes(out)


def _decode_remaining_length(buf: bytes, offset: int) -> tuple[int, int]:
    multiplier = 1
    value = 0
    pos = offset
    while True:
        if pos >= len(buf):
            raise ValueError("Truncated remaining length")
        b = buf[pos]
        pos += 1
        value += (b & 0x7F) * multiplier
        if b & 0x80 == 0:
            break
        multiplier *= 128
        if multiplier > 128 * 128 * 128:
            raise ValueError("Remaining length exceeds 4 bytes")
    return value, pos


def _read_string(buf: bytes, offset: int) -> tuple[str, int]:
    if offset + 2 > len(buf):
        raise ValueError("Truncated string length")
    n = struct.unpack_from("!H", buf, offset)[0]
    offset += 2
    if offset + n > len(buf):
        raise ValueError("Truncated string body")
    return buf[offset:offset + n].decode("utf-8"), offset + n


def _encode_string(s: str) -> bytes:
    raw = s.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw


def _make_connack(return_code: int = 0, session_present: bool = False) -> bytes:
    flags = 1 if session_present else 0
    body = bytes([flags, return_code])
    return bytes([PKT_CONNACK << 4]) + _encode_remaining_length(len(body)) + body


def _make_publish(topic: str, payload: bytes, qos: int = 0, packet_id: int | None = None,
                  retain: bool = False, dup: bool = False) -> bytes:
    fixed = (PKT_PUBLISH << 4) | (qos << 1) | (0x08 if dup else 0) | (0x01 if retain else 0)
    body = _encode_string(topic)
    if qos > 0:
        if packet_id is None:
            packet_id = 1
        body += struct.pack("!H", packet_id)
    body += payload
    return bytes([fixed]) + _encode_remaining_length(len(body)) + body


def _make_puback(packet_id: int) -> bytes:
    return bytes([PKT_PUBACK << 4]) + bytes([2]) + struct.pack("!H", packet_id)


def _make_suback(packet_id: int, granted_qos: list[int]) -> bytes:
    body = struct.pack("!H", packet_id) + bytes(granted_qos)
    return bytes([PKT_SUBACK << 4]) + _encode_remaining_length(len(body)) + body


def _make_unsuback(packet_id: int) -> bytes:
    return bytes([PKT_UNSUBACK << 4]) + bytes([2]) + struct.pack("!H", packet_id)


def _make_pingresp() -> bytes:
    return bytes([PKT_PINGRESP << 4, 0])


# ---------------------------------------------------------------------------
# WebSocket session driver
# ---------------------------------------------------------------------------


def _max_frame_buffer_bytes() -> int:
    return int(os.environ.get("IOT_WS_FRAME_MAX_BYTES", str(16 * 1024 * 1024)))


class _WSSession:
    def __init__(self, send_coro, account_id: str):
        self._send = send_coro
        self.account_id = account_id
        self._sub_ids: list[str] = []
        self._sub_filters: dict[str, str] = {}
        self._sub_granted_qos: dict[str, int] = {}
        self._buffer = bytearray()
        self._next_pid = 1
        self._send_lock = asyncio.Lock()
        self._client_id: str = ""
        self._clean_session: bool = True
        self._in_flight: dict[int, _InFlightMessage] = {}
        self._retransmit_task: asyncio.Task | None = None
        self._will_topic: str | None = None
        self._will_message: bytes | None = None
        self._will_qos: int = 0
        self._will_retain: bool = False
        self._graceful_disconnect: bool = False

    def _alloc_packet_id(self) -> int:
        pid = self._next_pid
        self._next_pid = (self._next_pid % 65535) + 1
        return pid

    def _ensure_retransmit_timer(self) -> None:
        if self._retransmit_task is None or self._retransmit_task.done():
            self._retransmit_task = asyncio.ensure_future(self._retransmit_loop())

    async def _retransmit_loop(self) -> None:
        try:
            while self._in_flight:
                await asyncio.sleep(_RETRANSMIT_INTERVAL_SECONDS)
                now = asyncio.get_event_loop().time()
                for pid, msg in list(self._in_flight.items()):
                    if now - msg.sent_at >= _RETRANSMIT_INTERVAL_SECONDS:
                        msg.retransmit_count += 1
                        msg.sent_at = now
                        await self.send_bytes(
                            _make_publish(msg.topic, msg.payload, qos=1, packet_id=pid, dup=True)
                        )
        except asyncio.CancelledError:
            pass

    async def send_bytes(self, b: bytes) -> None:
        async with self._send_lock:
            await self._send({"type": "websocket.send", "bytes": b})

    async def deliver_to_client(self, topic: str, payload: bytes, qos: int) -> None:
        if qos == 0:
            await self.send_bytes(_make_publish(topic, payload, qos=0))
        else:
            pid = self._alloc_packet_id()
            self._in_flight[pid] = _InFlightMessage(pid, topic, payload)
            await self.send_bytes(_make_publish(topic, payload, qos=1, packet_id=pid))
            self._ensure_retransmit_timer()

    def _take_packet(self) -> tuple[int, int, bytes] | None:
        if len(self._buffer) < 2:
            return None
        first = self._buffer[0]
        try:
            remaining, header_end = _decode_remaining_length(bytes(self._buffer), 1)
        except ValueError:
            if len(self._buffer) > 5:
                self._buffer.clear()
            return None
        total = header_end + remaining
        if len(self._buffer) < total:
            return None
        body = bytes(self._buffer[header_end:total])
        del self._buffer[:total]
        pkt_type = (first >> 4) & 0x0F
        flags = first & 0x0F
        return pkt_type, flags, body

    async def handle_packet(self, pkt_type: int, flags: int, body: bytes) -> bool:
        if pkt_type == PKT_CONNECT:
            off = 0
            _proto_name, off = _read_string(body, off)
            off += 1  # Protocol Level
            if off >= len(body):
                await self.send_bytes(_make_connack(return_code=0))
                return True
            connect_flags = body[off]
            off += 1
            off += 2  # Keep Alive

            will_flag = bool(connect_flags & 0x04)
            will_qos = (connect_flags >> 3) & 0x03
            will_retain = bool(connect_flags & 0x20)
            clean_session = bool(connect_flags & 0x02)

            self._clean_session = clean_session

            if off < len(body):
                client_id, off = _read_string(body, off)
            else:
                client_id = ""
            if not client_id:
                client_id = uuid.uuid4().hex
            self._client_id = client_id

            if will_flag:
                if off < len(body):
                    will_topic, off = _read_string(body, off)
                else:
                    will_topic = ""
                if off + 2 <= len(body):
                    msg_len = struct.unpack_from("!H", body, off)[0]
                    off += 2
                    will_message = body[off:off + msg_len]
                    off += msg_len
                else:
                    will_message = b""
                self._will_topic = will_topic
                self._will_message = will_message
                self._will_qos = will_qos
                self._will_retain = will_retain
            else:
                self._will_topic = None
                self._will_message = None
                self._will_qos = 0
                self._will_retain = False

            self._graceful_disconnect = False
            await _force_disconnect_duplicate(self.account_id, self._client_id)
            _register_client(self.account_id, self._client_id, self)

            session_key = (self.account_id, self._client_id)
            session_present = False

            if clean_session:
                _persistent_sessions.pop(session_key, None)
            else:
                existing_ps = _persistent_sessions.get(session_key)
                if existing_ps is not None and not _is_session_expired(existing_ps):
                    session_present = True
                    for topic_filter in existing_ps.subscriptions:
                        sid = await broker_subscribe(
                            self.account_id, topic_filter, self.deliver_to_client, 1
                        )
                        self._sub_ids.append(sid)
                        self._sub_filters[sid] = topic_filter
                        self._sub_granted_qos[sid] = 1
                    await self.send_bytes(_make_connack(return_code=0, session_present=True))
                    queued = existing_ps.queued_messages[:]
                    existing_ps.queued_messages.clear()
                    for q_topic, q_payload, q_qos in queued:
                        await self.deliver_to_client(q_topic, q_payload, q_qos)
                    return True
                else:
                    _persistent_sessions[session_key] = _PersistentSessionState(
                        subscriptions=[], created_at=time.time()
                    )

            await self.send_bytes(_make_connack(return_code=0, session_present=session_present))
            return True

        if pkt_type == PKT_PUBLISH:
            qos = (flags >> 1) & 0x03
            retain = bool(flags & 0x01)
            topic, off = _read_string(body, 0)
            packet_id = None
            if qos > 0:
                if off + 2 > len(body):
                    return True
                packet_id = struct.unpack_from("!H", body, off)[0]
                off += 2
            if not _validate_publish_topic(topic):
                _broker_logger.warning("IoT broker: PUBLISH rejected — invalid topic: %r", topic)
                return False
            payload = body[off:]
            await broker_publish(self.account_id, topic, payload, qos=qos, retain=retain)
            if qos == 1 and packet_id is not None:
                await self.send_bytes(_make_puback(packet_id))
            return True

        if pkt_type == PKT_SUBSCRIBE:
            packet_id = struct.unpack_from("!H", body, 0)[0]
            off = 2
            granted = []
            while off < len(body):
                topic, off = _read_string(body, off)
                req_qos = body[off]
                off += 1
                granted_qos = min(req_qos, 1)
                granted.append(granted_qos)
                sid = await broker_subscribe(self.account_id, topic, self.deliver_to_client, granted_qos)
                self._sub_ids.append(sid)
                self._sub_filters[sid] = topic
                self._sub_granted_qos[sid] = granted_qos
            await self.send_bytes(_make_suback(packet_id, granted))
            return True

        if pkt_type == PKT_PUBACK:
            if len(body) >= 2:
                packet_id = struct.unpack_from("!H", body, 0)[0]
                self._in_flight.pop(packet_id, None)
            return True

        if pkt_type == PKT_UNSUBSCRIBE:
            packet_id = struct.unpack_from("!H", body, 0)[0]
            for sid in list(self._sub_ids):
                await broker_unsubscribe(sid)
            self._sub_ids.clear()
            await self.send_bytes(_make_unsuback(packet_id))
            return True

        if pkt_type == PKT_PINGREQ:
            await self.send_bytes(_make_pingresp())
            return True

        if pkt_type == PKT_DISCONNECT:
            self._graceful_disconnect = True
            return False

        return True

    async def cleanup(self) -> None:
        if self._retransmit_task is not None and not self._retransmit_task.done():
            self._retransmit_task.cancel()
            try:
                await self._retransmit_task
            except asyncio.CancelledError:
                pass
            self._retransmit_task = None
        self._in_flight.clear()
        if not self._graceful_disconnect and self._will_topic is not None:
            await broker_publish(
                self.account_id,
                self._will_topic,
                self._will_message or b"",
                qos=self._will_qos,
                retain=self._will_retain,
            )
        if not self._clean_session and self._client_id:
            self._preserve_session()
        for sid in self._sub_ids:
            await broker_unsubscribe(sid)
        self._sub_ids.clear()
        self._sub_filters.clear()
        self._sub_granted_qos.clear()
        if self._client_id:
            _deregister_client(self.account_id, self._client_id)

    def _preserve_session(self) -> None:
        session_key = (self.account_id, self._client_id)
        unprefixed_filters = list(self._sub_filters.values())
        existing = _persistent_sessions.get(session_key)
        if existing is not None:
            existing.subscriptions = unprefixed_filters
            existing.created_at = time.time()
        else:
            _persistent_sessions[session_key] = _PersistentSessionState(
                subscriptions=unprefixed_filters, created_at=time.time()
            )


async def handle_websocket(scope: dict, receive, send, account_id: str) -> None:
    """Drive an MQTT-over-WebSocket session."""
    msg = await receive()
    if msg.get("type") != "websocket.connect":
        return

    sub_headers = {}
    for name, value in scope.get("headers", []):
        try:
            sub_headers[name.decode("latin-1").lower()] = value.decode("utf-8")
        except UnicodeDecodeError:
            sub_headers[name.decode("latin-1").lower()] = value.decode("latin-1")
    requested = sub_headers.get("sec-websocket-protocol", "")
    chosen = None
    for proto in [p.strip() for p in requested.split(",") if p.strip()]:
        if proto.lower() in ("mqtt", "mqttv3.1", "mqttv5"):
            chosen = proto
            break

    accept: dict = {"type": "websocket.accept"}
    if chosen:
        accept["subprotocol"] = chosen
    await send(accept)

    ctx_token = _request_account_id.set(account_id)
    session = _WSSession(send, account_id)
    max_buffer = _max_frame_buffer_bytes()

    try:
        while True:
            incoming = await receive()
            mtype = incoming.get("type")
            if mtype == "websocket.disconnect":
                break
            if mtype != "websocket.receive":
                continue
            data = incoming.get("bytes")
            if data is None:
                text = incoming.get("text")
                if text is None:
                    continue
                continue
            session._buffer.extend(data)
            if len(session._buffer) > max_buffer:
                _broker_logger.warning("IoT broker: WS buffer overflow, dropping connection")
                break
            while True:
                pkt = session._take_packet()
                if pkt is None:
                    break
                pkt_type, flags, body = pkt
                cont = await session.handle_packet(pkt_type, flags, body)
                if not cont:
                    return
    except Exception:
        _broker_logger.exception("IoT broker WebSocket session failed")
    finally:
        await session.cleanup()
        try:
            _request_account_id.reset(ctx_token)
        except Exception:
            pass
        try:
            await send({"type": "websocket.close", "code": 1000})
        except Exception:
            pass
