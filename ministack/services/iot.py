"""AWS IoT Core control plane emulator.

Implements the JSON/REST APIs under ``iot.{region}.amazonaws.com``:

  - Thing registry: ``CreateThing``, ``DescribeThing``, ``ListThings``,
    ``UpdateThing``, ``DeleteThing``
  - ThingType: ``CreateThingType`` and friends
  - ThingGroup: ``CreateThingGroup`` and friends
  - Certificates: ``CreateKeysAndCertificate``, ``RegisterCertificate``,
    ``RegisterCertificateWithoutCA``, ``UpdateCertificate``,
    ``DeleteCertificate``, ``AttachThingPrincipal`` / ``DetachThingPrincipal``
  - Policies: ``CreatePolicy``, ``CreatePolicyVersion``, ``AttachPolicy``,
    ``DetachPolicy``, etc., plus the deprecated principal-policy family
    (``AttachPrincipalPolicy`` / ``DetachPrincipalPolicy`` /
    ``ListPrincipalPolicies`` / ``ListPolicyPrincipals``)
    ``DetachPolicy``, etc.
  - Fleet indexing: ``UpdateIndexingConfiguration`` /
    ``GetIndexingConfiguration`` / ``DescribeIndex`` / ``ListIndices``, and
    ``SearchIndex`` over the live registry + shadows
  - ``DescribeEndpoint`` returning a per-account hostname

This is the control plane — pure HTTP/JSON, no MQTT broker
dependency. The data plane (``iot_data.py``) is
implemented separately and only depends on this module for certificate
lookups (mTLS).

State is fully isolated per account and region via
``AccountRegionScopedDict`` and persisted through
``get_state``/``restore_state``. The Local CA (used to sign
``CreateKeysAndCertificate`` certificates) is also persisted so previously
issued client certificates remain valid across restarts.
"""

from __future__ import annotations

import asyncio
import base64
import contextvars
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
    AccountRegionScopedDict,
    _request_account_id,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    request_scope,
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
# Module-level state (account-and-region-scoped)
# ---------------------------------------------------------------------------

_things: AccountRegionScopedDict = AccountRegionScopedDict()  # thingName -> Thing dict
_thing_types: AccountRegionScopedDict = AccountRegionScopedDict()
_thing_groups: AccountRegionScopedDict = AccountRegionScopedDict()
_certificates: AccountRegionScopedDict = AccountRegionScopedDict()
_policies: AccountRegionScopedDict = AccountRegionScopedDict()
_topic_rules: AccountRegionScopedDict = AccountRegionScopedDict()
_shadows: AccountRegionScopedDict = AccountRegionScopedDict()
# Fleet-indexing configuration, one entry per account+region.
_indexing_config: AccountRegionScopedDict = AccountRegionScopedDict()

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
    return {"region_scoped": True, "retained": retained_list}


def _broker_restore_state(data: dict | None) -> None:
    if not data:
        return
    legacy_topics = not data.get("region_scoped", False)
    for entry in data.get("retained", []):
        topic = entry["topic"]
        if legacy_topics:
            account_id, separator, unscoped_topic = topic.partition("/")
            if separator:
                topic = (
                    f"{account_id}/{get_region()}/{unscoped_topic}"
                )
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
        "indexing_config": copy.deepcopy(_indexing_config),
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
    _indexing_config.update(data.get("indexing_config", {}))
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
    # The warn-once ledger is module state keyed on rule SQL that no longer
    # exists, so a reset has to clear it or the next test to store the same rule
    # gets no warning.
    _warned_sql_funcs.clear()
    _indexing_config.clear()
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


def _qp_bool(qp: dict, name: str, default: bool = False) -> bool:
    """Read a modeled boolean query-string parameter.

    SDKs serialize these as the strings ``true``/``false``; an absent
    parameter — or a present-but-empty one, which ``parse_qs`` keeps — means
    ``default``. The value is coerced through ``str`` so a repeated parameter
    that reaches a handler as a list still yields a bool instead of raising.
    """
    raw = qp.get(name)
    if isinstance(raw, list):
        raw = raw[0] if raw else None
    if raw is None or raw == "":
        return default
    return str(raw).strip().lower() in ("1", "true", "yes")


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
    if path == "/certificate/register-no-ca" and method == "POST":
        return _register_certificate(_parse_body(body), qp, without_ca=True)
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
    # Legacy principal-policy family (deprecated API, still shipped by SDKs).
    if path == "/principal-policies" and method == "GET":
        return _list_principal_policies(hdr, qp)
    if path.startswith("/principal-policies/") and method in ("PUT", "DELETE"):
        return _handle_principal_policy(method, path, hdr, qp)
    if path == "/policy-principals" and method == "GET":
        return _list_policy_principals(hdr, qp)
    if path.startswith("/policies/"):
        return _handle_policy(method, path, body, qp)

    # Fleet indexing — the search path must precede ``/indices/{indexName}``
    if path == "/indices/search" and method == "POST":
        return _search_index(_parse_body(body))
    if path == "/indexing/config" and method == "POST":
        return _update_indexing_configuration(_parse_body(body))
    if path == "/indexing/config" and method == "GET":
        return _get_indexing_configuration()
    if path == "/indices" and method == "GET":
        return _list_indices()
    if path.startswith("/indices/") and method == "GET":
        return _describe_index(path)

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


def _thing_type_props_identity(props: dict) -> tuple:
    """Comparable identity of a thingTypeProperties block.

    Covers every member the API models, so a re-create differing only in
    mqtt5Configuration is a genuine conflict rather than a silent no-op.
    Absent, None and empty compare equal, and searchableAttributes is
    compared as a set because AWS treats it as unordered.
    """
    return (
        props.get("thingTypeDescription") or None,
        frozenset(props.get("searchableAttributes") or ()),
        json.dumps(props.get("mqtt5Configuration") or None, sort_keys=True),
    )


def _create_thing_type(name: str, payload: dict) -> tuple:
    props = payload.get("thingTypeProperties") or {}
    existing = _thing_types.get(name)
    if existing is not None:
        # Idempotent: same properties return success; different properties 409.
        stored = existing.get("thingTypeProperties") or {}
        if _thing_type_props_identity(stored) == _thing_type_props_identity(props):
            return json_response({
                "thingTypeName": existing["thingTypeName"],
                "thingTypeArn": existing["thingTypeArn"],
                "thingTypeId": existing["thingTypeId"],
            })
        return error_response_json(
            "ResourceAlreadyExistsException",
            f"ThingType {name!r} already exists with different properties",
            409,
        )
    stored_props = {
        "thingTypeDescription": props.get("thingTypeDescription"),
        "searchableAttributes": list(props.get("searchableAttributes", []) or []),
    }
    if props.get("mqtt5Configuration"):
        stored_props["mqtt5Configuration"] = props["mqtt5Configuration"]
    record = {
        "thingTypeName": name,
        "thingTypeId": new_uuid(),
        "thingTypeArn": _thing_type_arn(name),
        "thingTypeProperties": stored_props,
        "thingTypeMetadata": {
            "deprecated": False,
            "deprecationDate": None,
            "creationDate": _now_epoch(),
        },
    }
    _thing_types[name] = record
    logger.info("IoT Thing Type created: %s", name)
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


def _certificate_already_exists(cert_id: str) -> tuple:
    """409 for a duplicate PEM, carrying ``resourceId``/``resourceArn`` the way
    real AWS's ``ResourceAlreadyExistsException`` does — both register variants
    answer identically."""
    return error_response_json(
        "ResourceAlreadyExistsException",
        f"The certificate with id {cert_id} already exists.",
        409,
        extra={"resourceId": cert_id, "resourceArn": _cert_arn(cert_id)},
    )


def _register_certificate(payload: dict, qp: dict, *, without_ca: bool = False) -> tuple:
    """Register a certificate that was issued elsewhere (no re-signing).

    Serves both ``RegisterCertificate`` (``POST /certificate/register``) and
    ``RegisterCertificateWithoutCA`` (``POST /certificate/register-no-ca``).
    botocore models ``setAsActive`` as a *querystring* member (as in
    ``CreateKeysAndCertificate``), so it is read from ``qp`` first, with the
    JSON body kept as a fallback for raw callers. The no-CA variant carries no
    CA reference and takes its status from the plain ``status`` body field
    only — it has no deprecated ``setAsActive``.
    """
    cert_pem = payload.get("certificatePem") or qp.get("certificatePem")
    if not cert_pem:
        return error_response_json(
            "InvalidRequestException", "certificatePem is required", 400
        )
    status = payload.get("status")
    if without_ca:
        set_active = False
    else:
        set_active = _qp_bool(qp, "setAsActive", bool(payload.get("setAsActive", False)))
    ca_pem = None if without_ca else payload.get("caCertificatePem")
    try:
        cert_id = get_certificate_id(cert_pem)
    except Exception as e:
        return error_response_json(
            "CertificateValidationException",
            f"Invalid certificate PEM: {e}",
            400,
        )
    if cert_id in _certificates:
        return _certificate_already_exists(cert_id)
    record = {
        "certificateId": cert_id,
        "certificateArn": _cert_arn(cert_id),
        "certificatePem": cert_pem,  # verbatim
        "status": status or ("ACTIVE" if set_active else "INACTIVE"),
        "creationDate": _now_epoch(),
        "ownedBy": get_account_id(),
        "caCertificateId": get_certificate_id(ca_pem) if ca_pem else None,
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
    if method not in ("PUT", "POST", "DELETE"):
        return error_response_json(
            "InvalidRequestException", f"Unsupported method: {method}", 400
        )
    return _change_policy_target(
        name, _parse_body(body).get("target"), attach=method == "PUT"
    )


def _change_policy_target(name: str, target: str | None, *, attach: bool) -> tuple:
    """``AttachPolicy`` / ``DetachPolicy`` over the policy and certificate stores.

    The data core behind both the modern ``/target-policies/{policyName}``
    route and the deprecated ``/principal-policies/{policyName}`` one, so
    neither has to synthesize a request for the other.
    """
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    if not target:
        return error_response_json(
            "InvalidRequestException", "target is required", 400
        )
    cert_id = target.rsplit("/", 1)[-1]
    cert = _certificates.get(cert_id)
    if attach:
        if target not in p.setdefault("targets", []):
            p["targets"].append(target)
            _policies[name] = p
        if cert is not None and name not in cert.setdefault("attachedPolicies", []):
            cert["attachedPolicies"].append(name)
            _certificates[cert_id] = cert
    else:
        if target in p.get("targets", []):
            p["targets"].remove(target)
            _policies[name] = p
        if cert is not None and name in cert.get("attachedPolicies", []):
            cert["attachedPolicies"].remove(name)
            _certificates[cert_id] = cert
    return json_response({})


def _policy_targets(name: str) -> list | None:
    """The targets attached to policy ``name``, or ``None`` if it does not exist."""
    p = _policies.get(name)
    return None if p is None else list(p.get("targets", []))


def _policies_attached_to(target: str) -> list:
    """The ``{policyName, policyArn}`` entries attached to ``target``."""
    return [
        {"policyName": p["policyName"], "policyArn": p["policyArn"]}
        for p in _policies.values()
        if target in p.get("targets", [])
    ]


def _list_targets_for_policy(path: str, qp: dict) -> tuple:
    """``GET|POST /policy-targets/{policyName}``."""
    name = path[len("/policy-targets/"):]
    targets = _policy_targets(name)
    if targets is None:
        return _error_not_found("Policy", name)
    return json_response({"targets": targets})


def _list_attached_policies(path: str, qp: dict) -> tuple:
    """``POST /attached-policies/{target}`` — returns policies attached to target.

    The target segment is URL-encoded by the SDK (the certificate ARN
    contains colons / slashes); the ASGI layer hands us the decoded value.
    """
    return json_response(
        {"policies": _policies_attached_to(path[len("/attached-policies/"):])}
    )


# ---------------------------------------------------------------------------
# Legacy principal-policy family (deprecated, still shipped by every SDK).
# The principal rides in the ``x-amzn-iot-principal`` header; semantics are
# exactly the modern target-scoped operations with principal == target, so
# each handler reads the same stores through the shared cores above.
# ---------------------------------------------------------------------------


def _handle_principal_policy(method: str, path: str, headers: dict, qp: dict) -> tuple:
    """``PUT|DELETE /principal-policies/{policyName}`` —
    ``AttachPrincipalPolicy`` / ``DetachPrincipalPolicy``, with the header
    principal as the policy target (PUT attaches, DELETE detaches).
    """
    name = path[len("/principal-policies/"):]
    if not name or "/" in name:
        return error_response_json(
            "InvalidRequestException", "Invalid principal-policies path", 400
        )
    principal = headers.get("x-amzn-iot-principal")
    if not principal:
        return error_response_json(
            "InvalidRequestException", "x-amzn-iot-principal header is required", 400
        )
    return _change_policy_target(name, principal, attach=method == "PUT")


def _list_principal_policies(headers: dict, qp: dict) -> tuple:
    """``GET /principal-policies`` (``ListPrincipalPolicies``) — the same
    result ``ListAttachedPolicies`` gives for the principal."""
    principal = headers.get("x-amzn-iot-principal")
    if not principal:
        return error_response_json(
            "InvalidRequestException", "x-amzn-iot-principal header is required", 400
        )
    return json_response({"policies": _policies_attached_to(principal)})


def _list_policy_principals(headers: dict, qp: dict) -> tuple:
    """``GET /policy-principals`` (``ListPolicyPrincipals``) — the policy name
    rides in the ``x-amzn-iot-policy`` header; the principals are the modern
    ``ListTargetsForPolicy`` targets under their legacy key."""
    name = headers.get("x-amzn-iot-policy")
    if not name:
        return error_response_json(
            "InvalidRequestException", "x-amzn-iot-policy header is required", 400
        )
    principals = _policy_targets(name)
    if principals is None:
        return _error_not_found("Policy", name)
    return json_response({"principals": principals})


# ---------------------------------------------------------------------------
# Fleet indexing (indexing configuration + SearchIndex)
# ---------------------------------------------------------------------------

# One token: either a ``field:value`` term (quoted values may contain spaces)
# or a bare word, which the grammar only accepts as the ``AND`` separator.
_SEARCH_TOKEN_RE = re.compile(
    r'(?P<field>[\w.]+)\s*:\s*(?P<value>"[^"]*"|\S+)|(?P<bare>"[^"]*"|\S+)'
)
_SEARCH_TOP_FIELDS = ("thingName", "thingTypeName", "thingGroupNames")
# Only the classic shadow is indexed, and only its two state halves are
# addressable: shadow.metadata / shadow.version / shadow.name.<name> (named
# shadows) are AWS fields this emulator does not project.
_SEARCH_SHADOW_HALVES = ("desired", "reported")

# AWS names the single thing index ``AWS_Things``; it exists only while thing
# indexing is enabled, which is why every fleet stack starts with an
# ``UpdateIndexingConfiguration`` call (Terraform: aws_iot_indexing_configuration).
_THING_INDEX_NAME = "AWS_Things"
_INDEXING_CONFIG_KEY = "indexing"
_THING_INDEXING_MODES = ("OFF", "REGISTRY", "REGISTRY_AND_SHADOW")
_THING_CONNECTIVITY_INDEXING_MODES = ("OFF", "STATUS")
_THING_GROUP_INDEXING_MODES = ("OFF", "ON")
# AWS's defaults for an account that has never called
# UpdateIndexingConfiguration: nothing is indexed.
_DEFAULT_INDEXING_CONFIG = {
    "thingIndexingConfiguration": {
        "thingIndexingMode": "OFF",
        "thingConnectivityIndexingMode": "OFF",
    },
    "thingGroupIndexingConfiguration": {"thingGroupIndexingMode": "OFF"},
}
# SearchIndex's documented ceiling ("this maximum number cannot exceed 100").
_SEARCH_MAX_RESULTS_LIMIT = 100


def _indexing_configuration() -> dict:
    """The account+region's indexing configuration, defaulting to all-OFF."""
    stored = _indexing_config.get(_INDEXING_CONFIG_KEY)
    return copy.deepcopy(stored) if stored else copy.deepcopy(_DEFAULT_INDEXING_CONFIG)


def _thing_indexing_mode() -> str:
    return (
        _indexing_configuration()
        .get("thingIndexingConfiguration", {})
        .get("thingIndexingMode", "OFF")
    )


def _update_indexing_configuration(payload: dict) -> tuple:
    """``POST /indexing/config`` — enable or disable fleet indexing.

    Each sub-configuration that the request carries replaces the stored one
    wholesale (AWS's own semantics: the mode is required, so a partial update
    of a sub-configuration is not expressible); an omitted sub-configuration is
    left alone.

    Only the modes this emulator can honor are interpreted —
    ``thingIndexingMode`` gates ``SearchIndex`` and whether shadow fields are
    queryable. ``deviceDefenderIndexingMode``, ``namedShadowIndexingMode``,
    ``customFields`` and ``filter`` are stored and echoed back by
    ``GetIndexingConfiguration`` so IaC round-trips cleanly, but nothing here
    projects those fields; querying one is an out-of-grammar
    ``InvalidQueryException`` rather than a silent miss.
    """
    config = _indexing_configuration()

    thing_cfg = payload.get("thingIndexingConfiguration")
    if thing_cfg is not None:
        if not isinstance(thing_cfg, dict):
            return error_response_json(
                "InvalidRequestException",
                "thingIndexingConfiguration must be an object",
                400,
            )
        mode = thing_cfg.get("thingIndexingMode")
        if mode not in _THING_INDEXING_MODES:
            return error_response_json(
                "InvalidRequestException",
                f"thingIndexingMode must be one of {list(_THING_INDEXING_MODES)}",
                400,
            )
        connectivity_mode = thing_cfg.get("thingConnectivityIndexingMode") or "OFF"
        if connectivity_mode not in _THING_CONNECTIVITY_INDEXING_MODES:
            return error_response_json(
                "InvalidRequestException",
                "thingConnectivityIndexingMode must be one of "
                f"{list(_THING_CONNECTIVITY_INDEXING_MODES)}",
                400,
            )
        if connectivity_mode == "STATUS" and mode == "OFF":
            return error_response_json(
                "InvalidRequestException",
                "thingIndexingMode must not be OFF to enable thing connectivity "
                "indexing",
                400,
            )
        config["thingIndexingConfiguration"] = {
            **thing_cfg,
            "thingIndexingMode": mode,
            "thingConnectivityIndexingMode": connectivity_mode,
        }

    group_cfg = payload.get("thingGroupIndexingConfiguration")
    if group_cfg is not None:
        if not isinstance(group_cfg, dict):
            return error_response_json(
                "InvalidRequestException",
                "thingGroupIndexingConfiguration must be an object",
                400,
            )
        group_mode = group_cfg.get("thingGroupIndexingMode")
        if group_mode not in _THING_GROUP_INDEXING_MODES:
            return error_response_json(
                "InvalidRequestException",
                f"thingGroupIndexingMode must be one of {list(_THING_GROUP_INDEXING_MODES)}",
                400,
            )
        config["thingGroupIndexingConfiguration"] = {
            **group_cfg,
            "thingGroupIndexingMode": group_mode,
        }

    _indexing_config[_INDEXING_CONFIG_KEY] = config
    logger.info(
        "IoT fleet indexing configured: thingIndexingMode=%s",
        config["thingIndexingConfiguration"]["thingIndexingMode"],
    )
    return json_response({})


def _get_indexing_configuration() -> tuple:
    """``GET /indexing/config`` — the stored configuration, all-OFF by default."""
    return json_response(_indexing_configuration())


def _index_schema(config: dict) -> str:
    """``DescribeIndex``'s schema string for the configuration in force."""
    thing_cfg = config.get("thingIndexingConfiguration", {})
    return (
        "REGISTRY_AND_SHADOW"
        if thing_cfg.get("thingIndexingMode") == "REGISTRY_AND_SHADOW"
        else "REGISTRY"
    )


def _describe_index(path: str) -> tuple:
    """``GET /indices/{indexName}``.

    The index is a consequence of the configuration, not a resource of its
    own: while thing indexing is OFF there is nothing to describe, which is
    the same ``ResourceNotFoundException`` AWS answers with. It is never
    ``BUILDING`` here — the registry *is* the index, so it is ready the moment
    it is enabled.
    """
    name = path[len("/indices/"):]
    config = _indexing_configuration()
    if (
        name != _THING_INDEX_NAME
        or config["thingIndexingConfiguration"]["thingIndexingMode"] == "OFF"
    ):
        return error_response_json(
            "ResourceNotFoundException", f"Index {name!r} does not exist", 404
        )
    return json_response({
        "indexName": _THING_INDEX_NAME,
        "indexStatus": "ACTIVE",
        "schema": _index_schema(config),
    })


def _list_indices() -> tuple:
    """``GET /indices`` — ``AWS_Things`` once thing indexing is on, else empty.

    ``maxResults`` / ``nextToken`` are not honored because a page can never
    overflow: there is exactly one index, or none.
    """
    enabled = _thing_indexing_mode() != "OFF"
    return json_response({"indexNames": [_THING_INDEX_NAME] if enabled else []})


def _parse_search_query(query: str) -> list[tuple[str, str]] | None:
    """Parse ``field:value`` terms separated by ``AND`` (or by a bare space).

    Returns the terms, or ``None`` for anything outside that grammar — a bare
    word that is not ``AND``, a leading or doubled ``AND``, and a *dangling*
    ``AND`` with no term after it. AWS rejects a dangling ``AND``, so ignoring
    it here would answer a malformed query with results.
    """
    terms: list[tuple[str, str]] = []
    pending_and = False
    for m in _SEARCH_TOKEN_RE.finditer(query):
        bare = m.group("bare")
        if bare is not None:
            if bare.upper() != "AND" or pending_and or not terms:
                return None
            pending_and = True
            continue
        terms.append((m.group("field"), m.group("value").strip('"')))
        pending_and = False
    if pending_and or not terms:
        return None
    return terms


def _classic_shadow_state(thing_name: str) -> dict | None:
    """The classic (unnamed) shadow's ``state`` document, or None."""
    rec = _shadows.get((thing_name, ""))
    if rec is None or rec.get("deleted"):
        return None
    return rec.get("state") or {}


def _search_field_error(field: str, thing_mode: str) -> str | None:
    """Why ``field`` is not queryable, or None when it is.

    Every rejection is a 400 rather than a query that matches nothing: a
    mistyped field and a field that no thing happens to carry are
    indistinguishable in the results, and only one of them is the caller's bug.
    """
    if field in _SEARCH_TOP_FIELDS:
        return None
    if field.startswith("attributes.") and field != "attributes.":
        return None
    if field.startswith("shadow."):
        parts = field.split(".")
        if len(parts) < 3 or parts[1] not in _SEARCH_SHADOW_HALVES or not parts[-1]:
            return (
                f"Unsupported query field: {field!r} — only "
                "shadow.desired.<path> and shadow.reported.<path> of the classic "
                "shadow are indexed"
            )
        if thing_mode != "REGISTRY_AND_SHADOW":
            return (
                f"Shadow field {field!r} is not indexed: thingIndexingMode is "
                f"{thing_mode}, and shadow data needs REGISTRY_AND_SHADOW"
            )
        return None
    return f"Unsupported query field: {field!r}"


def _search_field_value(thing: dict, field: str, shadow_state: dict | None):
    if field == "thingName":
        return thing.get("thingName")
    if field == "thingTypeName":
        return thing.get("thingTypeName")
    if field == "thingGroupNames":
        return thing.get("thingGroupNames") or None
    if field.startswith("attributes."):
        return (thing.get("attributes") or {}).get(field.split(".", 1)[1])
    if field.startswith("shadow.") and shadow_state is not None:
        node = shadow_state
        for part in field.split(".")[1:]:
            if not isinstance(node, dict):
                return None
            node = node.get(part)
        return node
    return None


def _as_number(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _search_value_matches(actual, expected: str) -> bool:
    """Compare an indexed value against a term value, case-insensitively.

    ``*`` (any run of characters) and ``?`` (exactly one) are honored — a
    prefix query like ``thingName:my-fleet-*`` is what fleet dashboards
    actually send. Every other character matches literally, so a ``[`` in a
    thing name is not read as a character class the way ``fnmatch`` would.

    A JSON *number* in a shadow compares numerically, so a reported
    ``{"temp": 10.0}`` answers ``shadow.reported.temp:10`` — as an index that
    knows the field's type does. Registry strings keep comparing as strings:
    a thing named ``007`` is not a hit for ``thingName:7``.

    A list field (``thingGroupNames``) matches when any of its members does,
    which is what makes ``thingGroupNames:production`` a membership test.
    """
    if isinstance(actual, list):
        return any(_search_value_matches(item, expected) for item in actual)
    if actual is None:
        # A missing field never matches — otherwise str(None) would compare as
        # "none" and thingTypeName:none would match every untyped thing.
        return False
    if isinstance(actual, (int, float)) and not isinstance(actual, bool):
        wanted = _as_number(expected)
        if wanted is not None:
            return float(actual) == wanted
        # Not a number on the query side: fall through so wildcards still work.
    actual = str(actual).lower()
    expected = expected.lower()
    if "*" not in expected and "?" not in expected:
        return actual == expected
    pattern = "".join(
        "." if ch == "?" else ".*" if ch == "*" else re.escape(ch) for ch in expected
    )
    return re.fullmatch(pattern, actual, re.DOTALL) is not None


def _search_next_token(offset: int) -> str:
    """An opaque page cursor. It carries an offset, as AWS's own tokens do."""
    raw = f"{_THING_INDEX_NAME}:{offset}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _search_token_offset(token: str) -> int | None:
    """The offset a token stands for, or None if it was not one of ours."""
    try:
        raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
        prefix, sep, value = raw.partition(":")
        if not sep or prefix != _THING_INDEX_NAME:
            return None
        offset = int(value)
    except Exception:
        return None
    return offset if offset >= 0 else None


def _search_index(payload: dict) -> tuple:
    """``POST /indices/search`` — fleet indexing over the live registry.

    Real AWS queries a fleet-index projection; here the thing registry and
    shadow stores ARE the index, so results are always current and the index
    is never ``BUILDING``. It still has to be *enabled*: while
    ``thingIndexingMode`` is OFF the ``AWS_Things`` index does not exist, and
    querying it answers ``ResourceNotFoundException`` exactly as AWS does —
    otherwise fleet code would work here and fail in the cloud against the
    account nobody ran ``UpdateIndexingConfiguration`` on.

    Supported grammar (the subset device fleets actually use):
    ``AND``-separated ``field:value`` terms over ``thingName``,
    ``thingTypeName``, ``thingGroupNames``, ``attributes.<name>`` and — once
    ``thingIndexingMode`` is REGISTRY_AND_SHADOW —
    ``shadow.desired|reported.<path>``, compared case-insensitively, with
    ``*`` / ``?`` wildcards in the value. Anything outside that grammar is
    rejected with ``InvalidQueryException``.

    Results page through ``maxResults`` + ``nextToken``; the token is an
    offset into the live match list, so a thing created between pages can
    shift a later page, the same way a live index does.
    """
    index_name = payload.get("indexName") or _THING_INDEX_NAME
    config = _indexing_configuration()
    thing_mode = config["thingIndexingConfiguration"]["thingIndexingMode"]
    if index_name != _THING_INDEX_NAME or thing_mode == "OFF":
        return error_response_json(
            "ResourceNotFoundException", f"Index {index_name!r} does not exist", 404
        )

    query = (payload.get("queryString") or "").strip()
    if not query:
        return error_response_json(
            "InvalidRequestException", "queryString is required", 400
        )
    terms = _parse_search_query(query)
    if terms is None:
        return error_response_json(
            "InvalidQueryException",
            f"Unsupported query syntax: {query!r}",
            400,
        )
    for field, _ in terms:
        problem = _search_field_error(field, thing_mode)
        if problem is not None:
            return error_response_json("InvalidQueryException", problem, 400)

    max_results = payload.get("maxResults", _SEARCH_MAX_RESULTS_LIMIT)
    if (
        not isinstance(max_results, int)
        or isinstance(max_results, bool)
        or not 1 <= max_results <= _SEARCH_MAX_RESULTS_LIMIT
    ):
        return error_response_json(
            "InvalidRequestException",
            f"maxResults must be between 1 and {_SEARCH_MAX_RESULTS_LIMIT}",
            400,
        )
    token = payload.get("nextToken")
    start = 0 if token is None else _search_token_offset(token)
    if start is None:
        return error_response_json(
            "InvalidRequestException", "nextToken is not a valid page token", 400
        )

    shadows_indexed = thing_mode == "REGISTRY_AND_SHADOW"
    matched = []
    for name, thing in _things.items():
        shadow_state = _classic_shadow_state(name) if shadows_indexed else None
        if not all(
            _search_value_matches(_search_field_value(thing, f, shadow_state), v)
            for f, v in terms
        ):
            continue
        entry = {
            "thingName": thing["thingName"],
            "thingId": thing.get("thingId", name),
            "attributes": thing.get("attributes") or {},
            "thingGroupNames": list(thing.get("thingGroupNames") or []),
        }
        if thing.get("thingTypeName"):
            entry["thingTypeName"] = thing["thingTypeName"]
        if shadow_state:
            # AWS returns the shadow as a JSON *string* document, and both
            # halves of the state are searchable, so both are in the document.
            entry["shadow"] = json.dumps({
                half: shadow_state[half]
                for half in ("desired", "reported")
                if half in shadow_state
            })
        matched.append(entry)

    end = start + max_results
    body = {"things": matched[start:end]}
    if end < len(matched):
        body["nextToken"] = _search_next_token(end)
    return json_response(body)


# ---------------------------------------------------------------------------
# Topic rules
# ---------------------------------------------------------------------------


def _rule_topic_filter(sql: str) -> str:
    """Extract the topic filter from a rule SQL ``FROM '<topic>'`` clause."""
    m = re.search(r"\bFROM\s+'([^']*)'", sql or "", re.IGNORECASE)
    return m.group(1) if m else ""


_MISSING = object()

_SELECT_CLAUSE_RE = re.compile(r"\bSELECT\s+(.*?)\s+FROM\s+'", re.IGNORECASE | re.DOTALL)
_SELECT_ALIAS_RE = re.compile(
    r"^(?P<expr>.+?)\s+AS\s+(?P<alias>[A-Za-z_][\w.]*)$", re.IGNORECASE | re.DOTALL
)
_SELECT_FUNC_RE = re.compile(r"^(?P<name>[A-Za-z_]\w*)\s*\((?P<args>.*)\)$", re.DOTALL)
_SELECT_ATTR_RE = re.compile(r"^[A-Za-z_][\w]*(\.[A-Za-z_][\w]*)*$")
# A SQL string literal escapes an embedded quote by doubling it: 'it''s'.
_SQL_STRING_RE = re.compile(r"^'(?:[^']|'')*'$")


def _sql_string_value(expr: str) -> str | None:
    """Return the value of a SQL string literal, or ``None`` when ``expr`` is not
    one. A doubled ``''`` inside the literal is an escaped quote."""
    if _SQL_STRING_RE.match(expr):
        return expr[1:-1].replace("''", "'")
    return None


# ---------------------------------------------------------------------------
# Rule SQL type conversions
# ---------------------------------------------------------------------------
#
# AWS specifies one conversion table for the whole rule SQL dialect, applied
# whenever an operator or a function is handed a value of a type it does not
# want: https://docs.aws.amazon.com/iot/latest/developerguide/iot-sql-data-types.html
# These three helpers are that table. Every operator below coerces through
# them, so a value of the wrong type resolves the same way wherever it turns up
# rather than each operator inventing its own policy — the trap this replaces
# was `regexp_matches(temp, '^2')` matching {"temp": 22} while `temp LIKE '2%'`
# did not. ``None`` is the "no conversion" answer, i.e. AWS's Undefined.

# The pattern AWS parses a string with when it wants a number, verbatim from the
# conversion table.
_SQL_NUMERIC_STRING_RE = re.compile(r"^-?\d+(\.\d+)?([eE]-?\d+)?$")


def _sql_as_number(value) -> int | float | None:
    """Convert to Int/Decimal: numbers pass through, a numeric-looking string
    converts, and anything else — Boolean included, which AWS converts only
    through an explicit cast() — is Undefined."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str) and _SQL_NUMERIC_STRING_RE.match(value.strip()):
        text = value.strip()
        try:
            return int(text)
        except ValueError:
            return float(text)
    return None


def _sql_as_string(value) -> str | None:
    """Convert to String: an Int, Decimal, Boolean, Array or Object renders;
    Null and Undefined do not, and stay Undefined."""
    if value is _MISSING or value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        # AWS renders a Boolean lowercase, the way JSON does — not Python's
        # "True"/"False".
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    # Array and Object render as their JSON serialization.
    return json.dumps(value, separators=(",", ":"))


def _sql_as_bool(value) -> bool | None:
    """Convert to Boolean: a Boolean passes through and the strings "true" and
    "false" convert (case insensitive). Every other value, a number included, is
    Undefined."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
    return None


def _rule_select_clause(sql: str) -> str:
    """Extract the SELECT clause of a rule SQL statement.

    FROM is optional for Basic Ingest rules, so a rule may be just
    ``SELECT <projection>`` with no ``FROM '<topic>'``. Try the FROM form first
    (it stops at ``FROM '`` so inner subquery FROMs are preserved), then fall
    back to everything after SELECT for the FROM-less form.
    """
    sql = sql or ""
    m = _SELECT_CLAUSE_RE.search(sql)
    if m:
        return m.group(1).strip()
    m = re.match(r"\s*SELECT\s+(.+)$", sql, re.IGNORECASE | re.DOTALL)
    if not m:
        return "*"
    proj = re.split(r"\s+WHERE\s+", m.group(1), maxsplit=1, flags=re.IGNORECASE)[0]
    return proj.strip()


def _rule_where_clause(sql: str) -> str:
    """Extract the WHERE predicate of a rule SQL statement ("" when absent).

    When a ``FROM '<topic>'`` clause is present the predicate is whatever
    follows it; for the FROM-less Basic Ingest form the predicate follows the
    projection directly.
    """
    sql = sql or ""
    m = re.search(r"\bFROM\s+'[^']*'", sql, re.IGNORECASE)
    tail = sql[m.end():] if m else sql
    m = re.search(r"\bWHERE\s+(.+)$", tail, re.IGNORECASE | re.DOTALL)
    return m.group(1).strip() if m else ""


def _split_select_items(clause: str) -> list[str]:
    """Split a SELECT (or function-argument) list on its top-level commas."""
    items: list[str] = []
    current: list[str] = []
    depth = 0
    quoted = False
    for ch in clause:
        if quoted:
            current.append(ch)
            if ch == "'":
                quoted = False
        elif ch == "'":
            quoted = True
            current.append(ch)
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            items.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    tail = "".join(current).strip()
    if tail:
        items.append(tail)
    return [i for i in items if i]


def _split_select_alias(item: str) -> tuple[str, str | None]:
    m = _SELECT_ALIAS_RE.match(item.strip())
    if m:
        return m.group("expr").strip(), m.group("alias")
    return item.strip(), None


def _select_default_key(expr: str) -> str:
    """Name an unaliased SELECT item the way AWS does — the trailing attribute
    segment, or the function name for a function call."""
    m = _SELECT_FUNC_RE.match(expr)
    if m:
        return m.group("name")
    return expr.rsplit(".", 1)[-1]


def _resolve_attribute(message, path: str):
    value = message
    for segment in path.split("."):
        if not isinstance(value, dict) or segment not in value:
            return _MISSING
        value = value[segment]
    return value


def _encode_base64(value, payload: bytes):
    if value is _MISSING:
        return _MISSING
    if isinstance(value, (bytes, bytearray)):
        raw = bytes(value)
    elif isinstance(value, str):
        raw = value.encode("utf-8")
    else:
        raw = json.dumps(value, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


# Rule SQL functions this evaluator implements. AWS's function library is much
# larger, and a rule using one of the others deploys on AWS, so the emulator
# accepts them at rule creation rather than failing a working stack — but such a
# call resolves to Undefined, which silently drops a SELECT field and never
# satisfies a WHERE clause. Both ends therefore warn, so the misfire is visible
# instead of looking like "the rule did not match".
_IMPLEMENTED_SQL_FUNCS = frozenset({
    "clientid",
    "encode",
    "isundefined",
    "newuuid",
    "regexp_matches",
    "replace",
    "timestamp",
    "topic",
})
_warned_sql_funcs: set[str] = set()


def _warn_unimplemented_sql_function(name: str) -> None:
    """Warn once per function name — rule evaluation runs on every publish, so
    an unconditional warning would flood the log for one broken rule."""
    if name in _warned_sql_funcs:
        return
    _warned_sql_funcs.add(name)
    logger.warning(
        "Rule SQL function %s() is not implemented by MiniStack: it resolves to "
        "Undefined, so its SELECT field is dropped and a WHERE clause over it "
        "never matches",
        name,
    )


def _eval_select_function(
    name: str, args: list[str], topic: str, payload: bytes, message, client_id: str | None = None
):
    if name == "encode":
        if len(args) != 2:
            return _MISSING
        source, encoding = args[0].strip(), args[1].strip().strip("'").lower()
        if encoding != "base64":
            return _MISSING
        # `encode(*, 'base64')` encodes the payload as published — the bytes
        # never round-trip through a text decode.
        value = payload if source == "*" else _eval_select_expr(source, topic, payload, message, client_id)
        return _encode_base64(value, payload)
    if name == "topic":
        if not args:
            return topic
        try:
            index = int(args[0].strip())
        except ValueError:
            return _MISSING
        segments = topic.split("/")
        if index < 1 or index > len(segments):
            return _MISSING
        return segments[index - 1]
    if name == "timestamp" and not args:
        return int(time.time() * 1000)
    if name == "isundefined":
        if len(args) != 1:
            return _MISSING
        return _eval_select_expr(args[0], topic, payload, message, client_id) is _MISSING
    if name == "newuuid" and not args:
        return new_uuid()
    if name == "replace":
        if len(args) != 3:
            return _MISSING
        old, new = _sql_string_value(args[1].strip()), _sql_string_value(args[2].strip())
        if old is None or new is None:
            return _MISSING
        value = _eval_select_expr(args[0], topic, payload, message, client_id)
        if not isinstance(value, str):
            # An Undefined (or non-string) source is Undefined on AWS.
            return _MISSING
        return value.replace(old, new)
    if name == "clientid" and not args:
        # HTTP publishes carry no MQTT client id — AWS resolves clientid() to
        # Undefined there, so the field is omitted from the projection.
        return client_id if client_id else _MISSING
    # principal() and traceid() land here too: this publish path carries no
    # certificate identity or trace id to report, so they warn like any other
    # function the evaluator does not implement.
    _warn_unimplemented_sql_function(name)
    return _MISSING


def _eval_select_expr(expr: str, topic: str, payload: bytes, message, client_id: str | None = None):
    expr = expr.strip()
    if expr == "*":
        return message
    literal = _sql_string_value(expr)
    if literal is not None:
        return literal
    m = _SELECT_FUNC_RE.match(expr)
    if m:
        return _eval_select_function(
            m.group("name").lower(),
            _split_select_items(m.group("args")),
            topic,
            payload,
            message,
            client_id,
        )
    if _SELECT_ATTR_RE.match(expr):
        return _resolve_attribute(message, expr)
    try:
        return int(expr)
    except ValueError:
        pass
    try:
        return float(expr)
    except ValueError:
        pass
    # Last, because every form above is a single token and would otherwise be
    # re-split: a parenthesised group or an arithmetic expression over them.
    # Its scanners live with the WHERE parser below, which splits on the same
    # quote/paren rules.
    return _eval_sql_arithmetic(expr, topic, payload, message, client_id)


# ---------------------------------------------------------------------------
# Rule SQL WHERE evaluation
# ---------------------------------------------------------------------------
#
# Grammar (validated at rule creation, so anything a stored rule carries is
# evaluable): a boolean expression over leaf clauses, with AWS's precedence —
# OR binds loosest, then AND, then NOT, and a parenthesised group overrides all
# three. A predicate is one of
#   NOT <predicate>
#   <operand> <op> <operand>     with op in  =  ==  <>  !=  <  >  <=  >=
#   <operand> [NOT] IN (<operand>, ...)
#   <operand> [NOT] LIKE '<pattern>'      % matches a run, _ a single character
#   <operand> BETWEEN <operand> AND <operand>
#   <operand> IS [NOT] NULL
#   <operand>                             a boolean-valued attribute or call
#   regexp_matches(<operand>, '<regex>')
# where an operand is a quoted string, a numeric literal, a JSON payload
# attribute path, a SQL function call, or an arithmetic expression over those
# (+ - * / %, the multiplicative three binding tighter, parentheses regrouping).
#
# Evaluation is three-valued — true, false, or Undefined — because AWS's is:
# every operator here answers Undefined for an operand it cannot use, and only
# NOT can tell that apart from false. `_eval_where` then fires the rule on true
# alone, so an Undefined predicate fails closed however it was reached.

_WHERE_OPERATORS = ("<>", "!=", "==", "<=", ">=", "=", "<", ">")
_REGEXP_MATCHES_RE = re.compile(
    r"regexp_matches\s*\(\s*(?P<expr>.+?)\s*,\s*'(?P<regex>[^']*)'\s*\)",
    re.IGNORECASE | re.DOTALL,
)


def _top_level_indices(text: str) -> list[int] | None:
    """Index every character of ``text`` that sits at parenthesis depth 0 and
    outside a string literal — the only positions where a top-level token (a
    boolean keyword, a comparison operator, a group delimiter) can start.

    This is the single scanner the WHERE parser splits on; the helpers below
    differ only in what they look for at those positions. Returns ``None`` when
    the text is malformed — an unterminated literal or unbalanced parentheses —
    so the caller rejects it rather than guessing at a grouping.
    """
    tops: list[int] = []
    depth = 0
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if ch == "'":
            i += 1
            # A doubled '' inside the literal is an escaped quote, not its end.
            while i < n and not (text[i] == "'" and text[i + 1:i + 2] != "'"):
                i += 2 if text[i] == "'" else 1
            if i >= n:
                return None
            i += 1
            continue
        if ch == "(":
            if depth == 0:
                tops.append(i)
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return None
            if depth == 0:
                tops.append(i)
        elif depth == 0:
            tops.append(i)
        i += 1
    return None if depth else tops


def _is_keyword_at(text: str, index: int, keyword: str) -> bool:
    """True when ``keyword`` starts at ``index`` as a standalone SQL word.

    A keyword may abut a bracket instead of whitespace — ``(a = 1)AND(b = 2)``
    and ``x IN('a')`` are both SQL AWS accepts — so a bracket counts as a word
    boundary alongside whitespace.
    """
    end = index + len(keyword)
    if text[index:end].upper() != keyword:
        return False
    before = text[index - 1] if index else " "
    after = text[end] if end < len(text) else " "
    return (before.isspace() or before == ")") and (after.isspace() or after == "(")


def _split_where_terms(pred: str, keyword: str) -> list[str] | None:
    """Split a WHERE predicate on its top-level ``AND`` / ``OR`` keywords.

    A keyword inside a string literal or a parenthesised group is not a split
    point, and neither is the ``AND`` belonging to a ``BETWEEN <lo> AND <hi>``.
    Returns ``None`` when the predicate is malformed.
    """
    tops = _top_level_indices(pred)
    if tops is None:
        return None
    parts: list[str] = []
    start = 0
    pending_between = 0
    for i in tops:
        if _is_keyword_at(pred, i, "BETWEEN"):
            pending_between += 1
        elif _is_keyword_at(pred, i, keyword):
            if keyword == "AND" and pending_between:
                pending_between -= 1
                continue
            parts.append(pred[start:i].strip())
            start = i + len(keyword)
    parts.append(pred[start:].strip())
    return parts


def _split_where_keyword(clause: str, keyword: str) -> tuple[str, str] | None:
    """Split ``clause`` around its first top-level ``keyword``, or ``None`` when
    the clause does not carry one (or is malformed)."""
    tops = _top_level_indices(clause)
    if tops is None:
        return None
    for i in tops:
        if _is_keyword_at(clause, i, keyword):
            return clause[:i].strip(), clause[i + len(keyword):].strip()
    return None


def _strip_where_group(pred: str) -> str | None:
    """Return the body of ``pred`` when a single pair of parentheses wraps the
    whole predicate, else ``None`` (``(a) AND (b)`` is not a wrapped group)."""
    if not (pred.startswith("(") and pred.endswith(")")):
        return None
    # Everything a wrapping group encloses sits at depth 1 or deeper, so its own
    # delimiters are the only characters left at top level.
    if _top_level_indices(pred) != [0, len(pred) - 1]:
        return None
    return pred[1:-1].strip()


# Arithmetic binds tighter than any comparison, so an operand is split on these
# only after the comparison split has run. Multiplicative binds tighter than
# additive; within a tier the split is left-associative.
_ARITHMETIC_TIERS = (("+", "-"), ("*", "/", "%"))


def _split_arithmetic(expr: str) -> tuple[str, str, str] | None:
    """Split ``expr`` at its loosest-binding top-level arithmetic operator, or
    ``None`` when it carries none (or is malformed).

    Scanning each tier right to left makes the split left-associative: the last
    top-level ``-`` in ``a - b - c`` is the root, so it groups as ``(a - b) - c``.
    A ``+``/``-`` that opens the expression or follows another operator is a
    sign rather than a binary operator — ``a * -5`` is a product, not a
    difference — so it is not a split point.
    """
    tops = _top_level_indices(expr)
    if tops is None:
        return None
    for tier in _ARITHMETIC_TIERS:
        for i in reversed(tops):
            if expr[i] not in tier:
                continue
            left = expr[:i].strip()
            if not left or left[-1] in "+-*/%":
                continue
            return left, expr[i], expr[i + 1:].strip()
    return None


def _sql_arithmetic(op: str, left, right):
    """Apply one arithmetic operator under AWS's operand rules.

    https://docs.aws.amazon.com/iot/latest/developerguide/iot-sql-operators.html
    ``+`` is overloaded: a String operand on either side makes it concatenation,
    which is why it is settled before the numeric conversion. Everything else is
    numeric, Int op Int stays Int, and an operand that will not convert leaves
    the whole expression Undefined.
    """
    if op == "+" and (isinstance(left, str) or isinstance(right, str)):
        parts = (_sql_as_string(left), _sql_as_string(right))
        return _MISSING if None in parts else parts[0] + parts[1]
    lnum, rnum = _sql_as_number(left), _sql_as_number(right)
    if lnum is None or rnum is None:
        return _MISSING
    if op == "+":
        return lnum + rnum
    if op == "-":
        return lnum - rnum
    if op == "*":
        return lnum * rnum
    if rnum == 0:
        # AWS does not document division by zero; Undefined is the answer that
        # keeps a rule from firing on a value the emulator had to invent.
        return _MISSING
    if op == "%":
        # SQL's remainder takes the sign of the dividend; Python's % takes the
        # divisor's, so it needs correcting rather than using directly.
        remainder = abs(lnum) % abs(rnum)
        return -remainder if lnum < 0 else remainder
    if isinstance(lnum, int) and isinstance(rnum, int):
        # Int / Int is an Int on AWS. Python's // floors, so negative quotients
        # need nudging back to the truncation SQL divides with.
        quotient = lnum // rnum
        return quotient + 1 if quotient < 0 and quotient * rnum != lnum else quotient
    return lnum / rnum


def _eval_sql_arithmetic(expr: str, topic: str, payload: bytes, message, client_id: str | None):
    """Evaluate a parenthesised group or an arithmetic expression, or return the
    missing-value sentinel when ``expr`` is neither."""
    inner = _strip_where_group(expr)
    if inner is not None:
        return _eval_select_expr(inner, topic, payload, message, client_id)
    parts = _split_arithmetic(expr)
    if parts is None:
        return _MISSING
    left_expr, op, right_expr = parts
    left = _eval_select_expr(left_expr, topic, payload, message, client_id)
    right = _eval_select_expr(right_expr, topic, payload, message, client_id)
    if left is _MISSING or right is _MISSING:
        return _MISSING
    return _sql_arithmetic(op, left, right)


def _split_comparison(clause: str) -> tuple[str, str, str] | None:
    """Split ``<left> <op> <right>`` at the first top-level comparison operator."""
    tops = _top_level_indices(clause)
    if tops is None:
        return None
    for i in tops:
        for op in _WHERE_OPERATORS:
            if clause.startswith(op, i):
                return clause[:i].strip(), op, clause[i + len(op):].strip()
    return None


def _strip_trailing_not(expr: str) -> tuple[str, bool]:
    """Peel the ``NOT`` off the left operand of ``NOT IN`` / ``NOT LIKE``."""
    m = re.fullmatch(r"(?P<expr>.*?)\s+NOT", expr.strip(), re.IGNORECASE | re.DOTALL)
    if m:
        return m.group("expr").strip(), True
    return expr.strip(), False


def _like_to_regex(pattern: str) -> str:
    """Translate a SQL LIKE pattern to a regex: ``%`` matches any run of
    characters, ``_`` exactly one, everything else is literal."""
    return "".join(
        ".*" if ch == "%" else "." if ch == "_" else re.escape(ch) for ch in pattern
    )


def _valid_where_operand(expr: str) -> bool:
    """A WHERE operand `_eval_select_expr` can resolve: string literal, number,
    payload attribute path, function call, or arithmetic over those.

    The order mirrors the evaluator's, so validation accepts exactly what
    evaluation resolves.
    """
    expr = expr.strip()
    if not expr:
        return False
    if _sql_string_value(expr) is not None:
        return True
    if _SELECT_ATTR_RE.match(expr) or _SELECT_FUNC_RE.match(expr):
        return True
    try:
        float(expr)
        return True
    except ValueError:
        pass
    inner = _strip_where_group(expr)
    if inner is not None:
        return _valid_where_operand(inner)
    parts = _split_arithmetic(expr)
    return parts is not None and _valid_where_operand(parts[0]) and _valid_where_operand(parts[2])


def _parse_where_leaf(clause: str) -> tuple | None:
    """Parse one leaf clause of the grammar above into a node, or ``None``."""
    m = _REGEXP_MATCHES_RE.fullmatch(clause)
    if m:
        if not _valid_where_operand(m.group("expr")):
            return None
        try:
            re.compile(m.group("regex"))
        except re.error:
            return None
        return ("regexp", m.group("expr"), m.group("regex"))

    parts = _split_where_keyword(clause, "IS")
    if parts is not None:
        left, rest = parts
        negated = re.fullmatch(r"NOT\s+NULL", rest, re.IGNORECASE) is not None
        if not negated and rest.upper() != "NULL":
            return None
        return ("isnull", left, negated) if _valid_where_operand(left) else None

    parts = _split_where_keyword(clause, "BETWEEN")
    if parts is not None:
        left, rest = parts
        bounds = _split_where_keyword(rest, "AND")
        if bounds is None:
            return None
        low, high = bounds
        if not all(_valid_where_operand(o) for o in (left, low, high)):
            return None
        return ("between", left, low, high)

    parts = _split_where_keyword(clause, "IN")
    if parts is not None:
        left, negated = _strip_trailing_not(parts[0])
        inner = _strip_where_group(parts[1])
        if inner is None or not _valid_where_operand(left):
            return None
        options = _split_select_items(inner)
        if not options or not all(_valid_where_operand(o) for o in options):
            return None
        return ("in", left, tuple(options), negated)

    parts = _split_where_keyword(clause, "LIKE")
    if parts is not None:
        left, negated = _strip_trailing_not(parts[0])
        pattern = _sql_string_value(parts[1])
        if pattern is None or not _valid_where_operand(left):
            return None
        return ("like", left, _like_to_regex(pattern), negated)

    cmp_parts = _split_comparison(clause)
    if cmp_parts is not None:
        left, op, right = cmp_parts
        if not (_valid_where_operand(left) and _valid_where_operand(right)):
            return None
        return ("cmp", left, op, right)

    # A bare operand is a leaf in its own right: AWS accepts `WHERE enabled` and
    # `WHERE isUndefined(x)`, each holding only when it resolves to boolean true.
    clause = clause.strip()
    if _SELECT_ATTR_RE.match(clause) or _SELECT_FUNC_RE.match(clause):
        return ("truth", clause)
    return None


def _parse_where(pred: str) -> tuple | None:
    """Parse a WHERE predicate into an evaluable node tree, or ``None`` if
    unparseable.

    Nodes are ``("or", [...])`` / ``("and", [...])`` over leaves. OR is split
    first, so it binds loosest — ``a = 1 AND b = 2 OR c = 3`` groups as
    ``(a = 1 AND b = 2) OR c = 3``, as on AWS.
    """
    pred = (pred or "").strip()
    if not pred:
        return None
    for keyword, kind in (("OR", "or"), ("AND", "and")):
        terms = _split_where_terms(pred, keyword)
        if terms is None:
            return None
        if len(terms) > 1:
            nodes: list[tuple] = []
            for term in terms:
                node = _parse_where(term)
                if node is None:
                    return None
                nodes.append(node)
            return (kind, tuple(nodes))
    # NOT binds tighter than AND and looser than any comparison, so it is peeled
    # after the boolean splits and before the group and leaf forms. It may abut
    # its operand's bracket — `NOT(a = 1)` is SQL AWS accepts.
    if re.match(r"NOT\b", pred, re.IGNORECASE):
        node = _parse_where(pred[3:].strip())
        return ("not", node) if node is not None else None
    inner = _strip_where_group(pred)
    if inner is not None:
        return _parse_where(inner)
    return _parse_where_leaf(pred)


def _where_values_equal(left, right) -> bool:
    # Keep JSON booleans distinct from 0/1 (Python would conflate them).
    if isinstance(left, bool) or isinstance(right, bool):
        return isinstance(left, bool) and isinstance(right, bool) and left is right
    return left == right


def _eval_where_node(node: tuple, topic: str, payload: bytes, message, client_id: str | None):
    """Evaluate one parsed WHERE node to true, false, or Undefined.

    Undefined is what each AWS operator answers for an operand it cannot use,
    and it composes as SQL's three-valued logic: it loses to a true OR sibling
    and to a false AND sibling, survives every other combination, and is the one
    value NOT cannot flip. `_eval_where` turns whatever reaches the top into a
    fire/do-not-fire decision.
    """
    kind = node[0]
    if kind == "or":
        undefined = False
        for child in node[1]:
            result = _eval_where_node(child, topic, payload, message, client_id)
            if result is True:
                return True
            undefined = undefined or result is _MISSING
        return _MISSING if undefined else False
    if kind == "and":
        undefined = False
        for child in node[1]:
            result = _eval_where_node(child, topic, payload, message, client_id)
            if result is False:
                return False
            undefined = undefined or result is _MISSING
        return _MISSING if undefined else True
    if kind == "not":
        inner = _eval_where_node(node[1], topic, payload, message, client_id)
        # NOT Undefined is Undefined, so a predicate over an absent attribute
        # still fails closed once negated.
        return _MISSING if inner is _MISSING else not inner
    if kind == "regexp":
        text = _sql_as_string(_eval_select_expr(node[1], topic, payload, message, client_id))
        return _MISSING if text is None else re.search(node[2], text) is not None
    if kind == "truth":
        # A bare operand is a predicate only if it converts to a Boolean: true,
        # false, or the strings spelling them. A number does not, on AWS.
        value = _sql_as_bool(_eval_select_expr(node[1], topic, payload, message, client_id))
        return _MISSING if value is None else value
    if kind == "isnull":
        value = _eval_select_expr(node[1], topic, payload, message, client_id)
        if value is _MISSING:
            # Undefined is not NULL, and it is not "not NULL" either.
            return _MISSING
        return value is not None if node[2] else value is None
    if kind == "between":
        bounds = [
            _sql_as_number(_eval_select_expr(expr, topic, payload, message, client_id))
            for expr in node[1:]
        ]
        if any(bound is None for bound in bounds):
            return _MISSING
        value, low, high = bounds
        return low <= value <= high
    if kind == "in":
        value = _eval_select_expr(node[1], topic, payload, message, client_id)
        if value is _MISSING:
            return _MISSING
        hit = any(
            _where_values_equal(value, _eval_select_expr(opt, topic, payload, message, client_id))
            for opt in node[2]
        )
        return not hit if node[3] else hit
    if kind == "like":
        # LIKE wants a String, so a number or a boolean converts to one rather
        # than dropping out — the same conversion regexp_matches() applies.
        text = _sql_as_string(_eval_select_expr(node[1], topic, payload, message, client_id))
        if text is None:
            return _MISSING
        hit = re.fullmatch(node[2], text) is not None
        return not hit if node[3] else hit

    _, left_expr, op, right_expr = node
    left = _eval_select_expr(left_expr, topic, payload, message, client_id)
    right = _eval_select_expr(right_expr, topic, payload, message, client_id)
    if left is _MISSING or right is _MISSING:
        return _MISSING
    if op in ("=", "=="):
        # Equality does not convert: on AWS a mismatched pair is simply unequal.
        return _where_values_equal(left, right)
    if op in ("<>", "!="):
        return not _where_values_equal(left, right)
    lnum, rnum = _sql_as_number(left), _sql_as_number(right)
    if lnum is None or rnum is None:
        # Ordering converts both sides to a number first, and is Undefined for
        # an operand that will not convert.
        return _MISSING
    if op == "<":
        return lnum < rnum
    if op == ">":
        return lnum > rnum
    if op == "<=":
        return lnum <= rnum
    return lnum >= rnum


def _eval_where(pred: str, topic: str, payload: bytes, message, client_id: str | None = None) -> bool:
    """Evaluate a WHERE predicate against one publish.

    AWS-faithful failure mode: a clause referencing an attribute missing from
    the payload evaluates to Undefined, and only a predicate that comes out
    *true* fires the rule — so Undefined does not fire, and neither does its
    negation (fail closed). Unparseable predicates are rejected at rule
    creation; one that slips through (legacy stored state) also fails closed.
    """
    node = _parse_where(pred)
    if node is None:
        _broker_logger.warning(
            "IoT rule WHERE clause %r cannot be parsed — no publish will ever "
            "match it (fail closed)",
            pred,
        )
        return False
    return _eval_where_node(node, topic, payload, message, client_id) is True


class RuleSqlError(ValueError):
    """Rule SQL the engine cannot evaluate, raised by `put_topic_rule`."""


def _validate_rule_sql(sql: str) -> str | None:
    """Return an error message when the rule SQL cannot be parsed, else None."""
    sql = sql or ""
    if not re.match(r"\s*SELECT\s+\S", sql, re.IGNORECASE):
        return "Rule SQL must be of the form SELECT ... [FROM '<topic>'] [WHERE ...]"
    if re.search(r"\bFROM\b", sql, re.IGNORECASE) and not re.search(
        r"\bFROM\s+'[^']*'", sql, re.IGNORECASE
    ):
        return "FROM clause must name a topic filter in single quotes"
    pred = _rule_where_clause(sql)
    if pred and _parse_where(pred) is None:
        return f"Unsupported WHERE clause: {pred}"
    return None


_SQL_LITERAL_RE = re.compile(r"'(?:[^']|'')*'")
_SQL_CALL_RE = re.compile(r"\b([A-Za-z_]\w*)\s*\(")
# Words that may precede a bracket without opening a call: `WHERE (a = 1)`,
# `x IN ('a', 'b')` and `AND (b = 2)` all put a keyword where the pattern above
# looks for a function name, and warning about a missing where()/in()/and() is
# worse than saying nothing.
_SQL_KEYWORDS = frozenset({
    "and",
    "as",
    "between",
    "from",
    "in",
    "is",
    "like",
    "not",
    "null",
    "or",
    "select",
    "where",
})


def _unimplemented_sql_functions(sql: str) -> list[str]:
    """Names of the SQL functions ``sql`` calls that this evaluator does not
    implement. String literals are blanked first so text inside them cannot look
    like a call, and SQL's own keywords are discounted so a bracketed group or
    value list is not read as one."""
    stripped = _SQL_LITERAL_RE.sub("''", sql or "")
    names = {m.group(1).lower() for m in _SQL_CALL_RE.finditer(stripped)}
    return sorted(names - _IMPLEMENTED_SQL_FUNCS - _SQL_KEYWORDS)


def put_topic_rule(name: str, payload: dict, *, created_at: float | None = None) -> dict:
    """Store a topic rule from an API-shape (camelCase) ``TopicRulePayload``.

    Raises `RuleSqlError` for SQL the engine cannot evaluate, so a rule that
    would silently never fire cannot reach the store through any door — the IoT
    API or the CloudFormation provisioner. SQL that only *calls* something the
    evaluator lacks is stored (AWS accepts a larger function library than this
    emulator implements, and rejecting it would fail a stack that deploys on
    AWS) but warns, so the resulting misfire is visible.
    """
    sql = payload.get("sql", "")
    error = _validate_rule_sql(sql)
    if error:
        raise RuleSqlError(error)
    for func in _unimplemented_sql_functions(sql):
        logger.warning(
            "Topic rule %s calls %s(), which MiniStack does not implement: it "
            "resolves to Undefined, so the rule will not match on it",
            name,
            func,
        )
    rule = {
        "ruleName": name,
        "sql": sql,
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
    try:
        put_topic_rule(name, payload)
    except RuleSqlError as exc:
        return error_response_json("SqlParseException", str(exc), 400)
    return json_response({})


def _replace_topic_rule(name: str, payload: dict) -> tuple:
    if name not in _topic_rules:
        return _error_not_found("Rule", name)
    try:
        put_topic_rule(name, payload)
    except RuleSqlError as exc:
        return error_response_json("SqlParseException", str(exc), 400)
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


def lookup_certificate_by_id(cert_id: str, region: str) -> dict | None:
    """Return a certificate in the current account and explicit region."""
    return _certificates.get_scoped(get_account_id(), region, cert_id)


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
# caller's account_id and region before it hits the registry, and the prefix
# is stripped on outbound delivery.

_broker_logger = logging.getLogger("iot_broker")

# ---------------------------------------------------------------------------
# In-memory pub/sub registry
# ---------------------------------------------------------------------------

_subscriptions: dict[str, set["_Subscription"]] = {}
_connected_clients: dict[tuple[str, str, str], "_WSSession"] = {}
_persistent_sessions: dict[tuple[str, str, str], "_PersistentSessionState"] = {}
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
    __slots__ = (
        "subscription_id",
        "filter_prefixed",
        "account_id",
        "region",
        "deliver",
        "granted_qos",
    )

    def __init__(
        self,
        filter_prefixed: str,
        account_id: str,
        region: str,
        deliver: Callable[[str, bytes, int], Awaitable[None]],
        granted_qos: int = 0,
    ):
        self.subscription_id = uuid.uuid4().hex
        self.filter_prefixed = filter_prefixed
        self.account_id = account_id
        self.region = region
        self.deliver = deliver
        self.granted_qos = granted_qos

    def __hash__(self) -> int:
        return hash(self.subscription_id)

    def __eq__(self, other) -> bool:
        return isinstance(other, _Subscription) and other.subscription_id == self.subscription_id


# ---------------------------------------------------------------------------
# Topic prefixing & matching
# ---------------------------------------------------------------------------


def _scoped_topic(account_id: str, region: str, topic: str) -> str:
    return f"{account_id}/{region}/{topic}"


def _unscope_topic(account_id: str, region: str, scoped_topic: str) -> str:
    prefix = f"{account_id}/{region}/"
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
_SHADOW_TOPIC_PREFIX = "$aws/things/"


def _rules_for_account(account_id: str, region: str) -> list[dict]:
    return [
        value
        for (acct, reg, _key), value in _topic_rules._data.items()
        if acct == account_id and reg == region
    ]


def _rule_message(payload: bytes):
    """Decode a publish payload into the message the SELECT clause reads.

    Returns ``_MISSING`` for a payload that is not valid UTF-8. Such a payload
    has no attributes to project, but its bytes stay intact for
    ``encode(*, 'base64')``; the decode is never lossy.
    """
    try:
        text = (payload or b"").decode("utf-8")
    except UnicodeDecodeError:
        return _MISSING
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


# Sentinel for "the caller has not decoded the payload yet" — distinct from
# _MISSING, which is itself a legitimate decoded message (a non-UTF-8 payload).
_UNDECODED = object()


def _rule_event(
    sql: str,
    topic: str,
    payload: bytes,
    client_id: str | None = None,
    message=_UNDECODED,
):
    """Project a publish payload through a rule's SELECT clause.

    ``message`` lets a caller that already decoded the payload (the dispatch
    path, which also needs it for the WHERE predicate) pass it in instead of
    paying for a second JSON parse.
    """
    payload = payload or b""
    if message is _UNDECODED:
        message = _rule_message(payload)
    items = _split_select_items(_rule_select_clause(sql)) or ["*"]

    if len(items) == 1:
        expr, alias = _split_select_alias(items[0])
        if expr == "*" and alias is None:
            return message

    event: dict = {}
    for item in items:
        expr, alias = _split_select_alias(item)
        value = _eval_select_expr(expr, topic, payload, message, client_id)
        if value is _MISSING:
            continue
        if expr == "*" and alias is None:
            if isinstance(value, dict):
                event.update(value)
            continue
        event[alias or _select_default_key(expr)] = value
    return event


def _dispatch_rule_to_lambda(
    account_id: str, region: str, function_arn: str, event
) -> None:
    from ministack.services import lambda_svc

    func, config, name = lambda_svc._get_func_record_for_ref_in_scope(
        function_arn, account_id=account_id, region=region
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


# Depth guard for republish chains: rule A republishing onto a topic that rule
# A (or a cycle of rules) matches again would recurse without bound. Real AWS
# does not loop-protect either — such a rule is user error — but the emulator
# must at least not crash, so the chain is cut and logged past this depth.
_MAX_REPUBLISH_DEPTH = 8
_republish_depth: contextvars.ContextVar[int] = contextvars.ContextVar(
    "_iot_republish_depth", default=0
)


async def _dispatch_rule_republish(
    account_id: str, region: str, rule_name: str, spec: dict, event
) -> None:
    target_topic = spec.get("topic") or ""
    if not target_topic:
        _broker_logger.warning(
            "IoT rule %s: republish action has no topic — skipped", rule_name
        )
        return
    depth = _republish_depth.get()
    if depth >= _MAX_REPUBLISH_DEPTH:
        _broker_logger.warning(
            "IoT rule %s: republish depth limit (%d) reached — dropping republish to %r "
            "(a rule republishing onto its own topic filter loops forever on AWS too)",
            rule_name,
            _MAX_REPUBLISH_DEPTH,
            target_topic,
        )
        return
    try:
        qos = int(spec.get("qos") or 0)
    except (TypeError, ValueError):
        qos = 0
    token = _republish_depth.set(depth + 1)
    try:
        await broker_publish(
            account_id,
            region,
            target_topic,
            json.dumps(event).encode("utf-8"),
            qos=qos,
        )
    finally:
        _republish_depth.reset(token)


def _ddb_attribute_value(raw) -> dict:
    """Map a projected value to a DynamoDB AttributeValue (dynamoDBv2 puts each
    payload attribute in its own column)."""
    if isinstance(raw, bool):
        return {"BOOL": raw}
    if isinstance(raw, (int, float)):
        return {"N": str(raw)}
    if raw is None:
        return {"NULL": True}
    if isinstance(raw, (dict, list)):
        return {"S": json.dumps(raw)}
    return {"S": str(raw)}


def _dispatch_rule_dynamodb(account_id: str, region: str, rule_name: str, spec: dict, event) -> None:
    from ministack.services import dynamodb as _dynamodb

    table = (spec.get("putItem") or {}).get("tableName") or ""
    if not table:
        _broker_logger.warning(
            "IoT rule %s: dynamoDBv2 action has no putItem.tableName — skipped", rule_name
        )
        return
    if not isinstance(event, dict):
        _broker_logger.warning(
            "IoT rule %s: dynamoDBv2 action needs a JSON-object payload, got %s — skipped",
            rule_name,
            type(event).__name__,
        )
        return
    item = {key: _ddb_attribute_value(value) for key, value in event.items()}
    with request_scope(account_id, region):
        status = _dynamodb._put_item({"TableName": table, "Item": item})[0]
    if status >= 400:
        # Raise rather than log: an undeliverable destination is precisely what
        # the rule's errorAction exists to hear about, and a missing table is
        # the failure that actually happens locally.
        raise RuntimeError(
            f"DynamoDB PutItem to {table} failed with status {status}"
        )
    _broker_logger.debug("IoT rule %s → DynamoDB %s", rule_name, table)


def _dispatch_rule_sns(account_id: str, region: str, rule_name: str, spec: dict, event) -> None:
    from ministack.services import sns as _sns

    target = spec.get("targetArn") or ""
    if not target:
        _broker_logger.warning(
            "IoT rule %s: sns action has no targetArn — skipped", rule_name
        )
        return
    with request_scope(account_id, region):
        # Through SNS's own internal publish, so the message record, the payload
        # size limit and the FIFO rules are the ones every other producer gets.
        result = _sns.publish_internal(target, json.dumps(event))
    if result is None:
        # As with DynamoDB above: a topic that is not there is an undeliverable
        # destination, so it reaches the errorAction instead of a log line.
        raise RuntimeError(f"SNS topic {target} not found")
    _broker_logger.debug("IoT rule %s → SNS %s", rule_name, target)


async def _dispatch_rule_action(
    account_id: str, region: str, rule_name: str, action: dict, event
) -> None:
    """Dispatch one rule action, letting the target service's failure surface.

    Rule actions and the rule's errorAction share the same shapes, so both go
    through here.
    """
    lam = action.get("lambda")
    if lam and lam.get("functionArn"):
        _dispatch_rule_to_lambda(account_id, region, lam["functionArn"], event)
    elif "republish" in action:
        await _dispatch_rule_republish(
            account_id, region, rule_name, action["republish"] or {}, event
        )
    elif "dynamoDBv2" in action:
        _dispatch_rule_dynamodb(
            account_id, region, rule_name, action["dynamoDBv2"] or {}, event
        )
    elif "sns" in action:
        _dispatch_rule_sns(account_id, region, rule_name, action["sns"] or {}, event)
    else:
        _broker_logger.debug(
            "IoT rule %s: unsupported action type %s — skipped",
            rule_name,
            next(iter(action), "?"),
        )


async def _dispatch_rule_error_action(
    account_id: str,
    region: str,
    rule: dict,
    topic: str,
    payload: bytes,
    client_id: str | None,
    failures: list[dict],
) -> None:
    """Run the rule's errorAction, carrying AWS's error message document.

    https://docs.aws.amazon.com/iot/latest/developerguide/rule-error-handling.html
    """
    error_action = rule.get("errorAction")
    if not error_action:
        return
    rule_name = rule.get("ruleName", "")
    error_event = {
        "ruleName": rule_name,
        "topic": topic,
        # The emulator's publish path carries no CloudWatch trace id.
        "cloudwatchTraceId": "",
        "clientId": client_id or "",
        "base64OriginalPayload": base64.b64encode(payload).decode("ascii"),
        "failures": failures,
    }
    try:
        await _dispatch_rule_action(
            account_id, region, rule_name, error_action, error_event
        )
    except Exception as exc:
        _broker_logger.warning(
            "IoT rule %s: errorAction failed: %s: %s",
            rule_name,
            type(exc).__name__,
            exc,
        )


async def _run_rule_actions(
    account_id: str,
    region: str,
    rule: dict,
    payload: bytes,
    topic: str = "",
    client_id: str | None = None,
) -> None:
    if not rule or rule.get("ruleDisabled"):
        return
    payload = payload or b""
    # Decode once: both the SELECT projection and the WHERE predicate read the
    # same message.
    message = _rule_message(payload)
    event = _rule_event(rule.get("sql", ""), topic, payload, client_id, message=message)
    if event is _MISSING:
        _broker_logger.warning(
            "IoT rule %s: payload is not valid UTF-8 and its SELECT clause "
            "projects no attributes — no action dispatched",
            rule.get("ruleName"),
        )
        return
    where_pred = _rule_where_clause(rule.get("sql", ""))
    if where_pred and not _eval_where(where_pred, topic, payload, message, client_id):
        _broker_logger.debug(
            "IoT rule %s: WHERE clause did not match on %r — no action dispatched",
            rule.get("ruleName"),
            topic,
        )
        return
    rule_name = rule.get("ruleName", "")
    failures: list[dict] = []
    for action in rule.get("actions", []) or []:
        action_type = next(iter(action), "?")
        try:
            await _dispatch_rule_action(account_id, region, rule_name, action, event)
        except Exception as exc:
            # Never silently, and never let one failing action kill the loop: a
            # dropped dispatch is indistinguishable from "the rule did not
            # match", the hardest kind of local failure to diagnose.
            _broker_logger.warning(
                "IoT rule %s: %s action failed: %s: %s",
                rule_name,
                action_type,
                type(exc).__name__,
                exc,
            )
            failures.append(
                {"action": action_type, "errorMessage": f"{type(exc).__name__}: {exc}"}
            )
    if failures:
        await _dispatch_rule_error_action(
            account_id, region, rule, topic, payload, client_id, failures
        )


async def _evaluate_topic_rules(
    account_id: str, region: str, topic: str, payload: bytes, client_id: str | None = None
) -> None:
    for rule in _rules_for_account(account_id, region):
        filter_ = _rule_topic_filter(rule.get("sql", ""))
        if filter_ and _topic_matches(filter_, topic):
            await _run_rule_actions(account_id, region, rule, payload, topic, client_id)


def _parse_shadow_topic(topic: str) -> tuple[str, str, str | None] | None:
    """``$aws/things/<thing>/shadow[/name/<n>]/<verb>`` → (thing, verb, name).

    Anything longer — the ``accepted`` / ``rejected`` / ``delta`` /
    ``documents`` response suffixes — parses as ``None``, so the bridge's own
    response publishes can never re-trigger it (no recursion)."""
    if not topic.startswith(_SHADOW_TOPIC_PREFIX):
        return None
    rest = topic[len(_SHADOW_TOPIC_PREFIX):]
    parts = rest.split("/")
    if len(parts) < 3 or parts[1] != "shadow":
        return None
    thing = parts[0]
    tail = parts[2:]
    name = None
    if tail and tail[0] == "name" and len(tail) >= 2:
        name, tail = tail[1], tail[2:]
    if not tail:
        return None
    verb = tail[0]
    # Responses (accepted/rejected/delta/documents) are ours, never inputs.
    if len(tail) > 1 or verb not in ("update", "get", "delete"):
        return None
    return thing, verb, name


def _shadow_rejected(status: int, message: str, client_token: str | None = None) -> dict:
    """AWS's shadow error response document."""
    doc = {"code": status, "message": message}
    if client_token is not None:
        doc["clientToken"] = client_token
    return doc


def _shadow_document(doc: dict) -> dict:
    """Strip the computed ``delta`` sections out of a shadow snapshot.

    `get_thing_shadow` injects ``state.delta`` (and its ``metadata.delta``) by
    design, because that is what the GET response carries. The documents topic
    is not a GET: on AWS the ``previous``/``current`` documents report only
    ``desired`` and ``reported``, and the delta has its own topic. Publishing
    the snapshot verbatim invented a section real devices never see.
    """
    stripped = dict(doc)
    for section in ("state", "metadata"):
        block = stripped.get(section)
        if isinstance(block, dict) and "delta" in block:
            stripped[section] = {k: v for k, v in block.items() if k != "delta"}
    return stripped


async def _handle_shadow_publish(
    account_id: str,
    region: str,
    thing: str,
    verb: str,
    name: str | None,
    payload: bytes,
) -> None:
    """Shadow-over-MQTT bridge: feed a request-topic publish into the shadow
    store and publish the AWS reserved-topic responses back through the broker
    (``accepted`` / ``rejected``, plus ``delta`` and ``documents`` on update)."""
    # `name is not None` rather than a truthiness test: an empty named shadow
    # (`.../shadow/name//update`) must answer on the topic the requester is
    # listening on, not collapse onto the classic shadow's.
    base = f"{_SHADOW_TOPIC_PREFIX}{thing}/shadow" + (f"/name/{name}" if name is not None else "")
    shadow_name = name or ""

    async def _respond(suffix: str, doc: dict) -> None:
        await broker_publish(
            account_id,
            region,
            f"{base}/{suffix}",
            json.dumps(doc).encode("utf-8"),
            qos=1,
        )

    try:
        request = json.loads(payload) if payload else {}
    except (ValueError, UnicodeDecodeError):
        # All three request verbs reject malformed JSON, as on AWS — a `get`
        # or `delete` payload is optional but must parse when present.
        await _respond(f"{verb}/rejected", _shadow_rejected(400, "invalid json"))
        return
    if not isinstance(request, dict):
        request = {}
    client_token = request.get("clientToken")

    if name is not None and not name:
        await _respond(
            f"{verb}/rejected",
            _shadow_rejected(400, "Invalid shadow name: must not be empty", client_token),
        )
        return

    if verb == "update":
        # The broker's WS path sets only the account contextvar, so pin both
        # around every shadow-store access.
        with request_scope(account_id, region):
            # get_thing_shadow hands back the stored state dicts themselves and
            # update_thing_shadow merges into them in place, so `previous` has
            # to be snapshotted before the update runs — otherwise it reports
            # the state *after* it, and from the second update on `previous`
            # and `current` are the same document. `current` is snapshotted for
            # the same reason: a concurrent update would otherwise edit it out
            # from under the publish below.
            pre_status, pre_doc = get_thing_shadow(thing, shadow_name)
            pre_doc = copy.deepcopy(pre_doc)
            status, doc = update_thing_shadow(thing, shadow_name, request)
            post_status, post_doc = (
                get_thing_shadow(thing, shadow_name) if status == 200 else (0, {})
            )
            post_doc = copy.deepcopy(post_doc)
        if status != 200:
            await _respond(
                "update/rejected",
                _shadow_rejected(status, doc.get("message", ""), client_token),
            )
            return
        # The core update response already echoes clientToken when present.
        await _respond("update/accepted", doc)
        if post_status == 200:
            delta = (post_doc.get("state") or {}).get("delta")
            if delta:
                # The delta section of the GET document, with the metadata AWS
                # reports alongside it and the triggering request's clientToken.
                delta_doc = {
                    "state": delta,
                    "metadata": (post_doc.get("metadata") or {}).get("delta", {}),
                    "version": post_doc["version"],
                    "timestamp": post_doc["timestamp"],
                }
                if client_token is not None:
                    delta_doc["clientToken"] = client_token
                await _respond("update/delta", delta_doc)
        await _respond(
            "update/documents",
            {
                "previous": _shadow_document(pre_doc) if pre_status == 200 else None,
                "current": _shadow_document(post_doc) if post_status == 200 else None,
                "timestamp": _shadow_now(),
            },
        )
        return

    with request_scope(account_id, region):
        status, doc = (
            get_thing_shadow(thing, shadow_name)
            if verb == "get"
            else delete_thing_shadow(thing, shadow_name)
        )
    if status == 200:
        doc = copy.deepcopy(doc)
        if client_token is not None:
            doc["clientToken"] = client_token
        await _respond(f"{verb}/accepted", doc)
    else:
        await _respond(
            f"{verb}/rejected",
            _shadow_rejected(status, doc.get("message", ""), client_token),
        )


async def broker_publish(
    account_id: str,
    region: str,
    topic: str,
    payload: bytes,
    qos: int = 0,
    retain: bool = False,
    client_id: str | None = None,
) -> None:
    # Basic Ingest: a publish to `$aws/rules/<ruleName>` is delivered straight
    # to that rule's actions and bypasses pub/sub delivery entirely.
    if topic.startswith(_BASIC_INGEST_PREFIX):
        remainder = topic[len(_BASIC_INGEST_PREFIX):].split("/", 1)
        rule_name = remainder[0]
        await _run_rule_actions(
            account_id,
            region,
            _topic_rules.get_scoped(account_id, region, rule_name),
            payload,
            # Under Basic Ingest `topic()` reports the topic after the
            # `$aws/rules/<ruleName>/` prefix, as it does on AWS.
            remainder[1] if len(remainder) > 1 else "",
            client_id,
        )
        return

    scoped = _scoped_topic(account_id, region, topic)

    if retain:
        if not payload:
            _retained.pop(scoped, None)
        else:
            _retained[scoped] = _RetainedMessage(scoped, payload, qos)

    async with _broker_lock:
        subs = [s for sset in _subscriptions.values() for s in sset]

    for sub in subs:
        if sub.account_id != account_id or sub.region != region:
            continue
        if _topic_matches(sub.filter_prefixed, scoped):
            try:
                effective_qos = min(qos, sub.granted_qos)
                await sub.deliver(
                    _unscope_topic(sub.account_id, sub.region, scoped),
                    payload,
                    effective_qos,
                )
            except Exception:
                _broker_logger.exception("IoT broker: subscriber %s delivery failed", sub.subscription_id)

    if qos >= 1:
        for key, ps in list(_persistent_sessions.items()):
            ps_account_id, ps_region, _ps_client_id = key
            if ps_account_id != account_id or ps_region != region:
                continue
            if key in _connected_clients:
                continue
            if _is_session_expired(ps):
                continue
            for filt in ps.subscriptions:
                scoped_filter = _scoped_topic(ps_account_id, ps_region, filt)
                if _topic_matches(scoped_filter, scoped):
                    ps.queued_messages.append((topic, payload, qos))
                    if len(ps.queued_messages) > _MAX_QUEUED_MESSAGES:
                        ps.queued_messages = ps.queued_messages[-_MAX_QUEUED_MESSAGES:]
                    break

    await _evaluate_topic_rules(account_id, region, topic, payload, client_id)

    # Shadow-over-MQTT bridge — AFTER delivery and rule evaluation, so rules
    # matching `$aws/things/+/shadow/update` fire on the request, and the
    # recursive `accepted`/`delta`/`documents` publishes then flow back
    # through this function to drive their own subscribers and rules. The
    # response suffixes parse as None, so the bridge never re-triggers itself.
    # NOTE known divergence: `_topic_matches` lets a bare `#` subscription
    # match `$aws/...` topics, unlike real AWS where `#` excludes the
    # reserved topic space.
    shadow = _parse_shadow_topic(topic)
    if shadow is not None:
        try:
            await _handle_shadow_publish(account_id, region, *shadow, payload)
        except Exception:
            # Guarded like subscriber delivery and rule dispatch above: a
            # payload the bridge chokes on (a deeply nested document raises
            # RecursionError out of json.loads, which the ValueError handler
            # inside does not catch) must not tear down the MQTT session.
            _broker_logger.warning(
                "IoT shadow bridge failed for %r", topic, exc_info=True
            )


async def broker_subscribe(
    account_id: str,
    region: str,
    topic_filter: str,
    callback: Callable[[str, bytes, int], Awaitable[None]],
    granted_qos: int = 0,
) -> str:
    filter_prefixed = _scoped_topic(account_id, region, topic_filter)
    sub = _Subscription(
        filter_prefixed, account_id, region, callback, granted_qos
    )
    async with _broker_lock:
        _subscriptions.setdefault(filter_prefixed, set()).add(sub)
        has_wildcard = "+" in topic_filter or "#" in topic_filter
        if not has_wildcard:
            scope_prefix = f"{account_id}/{region}/"
            retained_to_send = [
                r
                for k, r in _retained.items()
                if k.startswith(scope_prefix)
                and _topic_matches(filter_prefixed, k)
            ]
        else:
            retained_to_send = []

    for r in retained_to_send:
        try:
            await sub.deliver(
                _unscope_topic(account_id, region, r.topic), r.payload, r.qos
            )
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


def _register_client(
    account_id: str, region: str, client_id: str, session: "_WSSession"
) -> None:
    _connected_clients[(account_id, region, client_id)] = session


def _deregister_client(account_id: str, region: str, client_id: str) -> None:
    _connected_clients.pop((account_id, region, client_id), None)


async def _force_disconnect_duplicate(
    account_id: str, region: str, client_id: str
) -> None:
    key = (account_id, region, client_id)
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
    def __init__(self, send_coro, account_id: str, region: str):
        self._send = send_coro
        self.account_id = account_id
        self.region = region
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
            await _force_disconnect_duplicate(
                self.account_id, self.region, self._client_id
            )
            _register_client(
                self.account_id, self.region, self._client_id, self
            )

            session_key = (self.account_id, self.region, self._client_id)
            session_present = False

            if clean_session:
                _persistent_sessions.pop(session_key, None)
            else:
                existing_ps = _persistent_sessions.get(session_key)
                if existing_ps is not None and not _is_session_expired(existing_ps):
                    session_present = True
                    for topic_filter in existing_ps.subscriptions:
                        sid = await broker_subscribe(
                            self.account_id,
                            self.region,
                            topic_filter,
                            self.deliver_to_client,
                            1,
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
            await broker_publish(
                self.account_id,
                self.region,
                topic,
                payload,
                qos=qos,
                retain=retain,
                client_id=self._client_id,
            )
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
                sid = await broker_subscribe(
                    self.account_id,
                    self.region,
                    topic,
                    self.deliver_to_client,
                    granted_qos,
                )
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
            off = 2
            filters = []
            while off < len(body):
                try:
                    topic_filter, off = _read_string(body, off)
                except ValueError:
                    _broker_logger.warning(
                        "IoT broker: UNSUBSCRIBE payload truncated after %d "
                        "topic filter(s), dropping the remainder",
                        len(filters),
                    )
                    break
                filters.append(topic_filter)

            # MQTT 3.1.1 §3.10.4: each filter is compared character-by-character
            # with the session's subscriptions, and one that matches none of them
            # is simply skipped — a wildcard filter goes only by its own text, not
            # by the topics it happened to match. `_sub_filters` keeps the filter
            # as it arrived on the wire, so the comparison happens before topic
            # prefixing, in the same form SUBSCRIBE stored it.
            for topic_filter in filters:
                removed = [
                    sid
                    for sid, stored in self._sub_filters.items()
                    if stored == topic_filter
                ]
                for sid in removed:
                    await broker_unsubscribe(sid)
                    self._sub_filters.pop(sid, None)
                    self._sub_granted_qos.pop(sid, None)
                self._sub_ids[:] = [
                    sid for sid in self._sub_ids if sid not in removed
                ]
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
                self.region,
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
            _deregister_client(self.account_id, self.region, self._client_id)

    def _preserve_session(self) -> None:
        session_key = (self.account_id, self.region, self._client_id)
        unprefixed_filters = list(self._sub_filters.values())
        existing = _persistent_sessions.get(session_key)
        if existing is not None:
            existing.subscriptions = unprefixed_filters
            existing.created_at = time.time()
        else:
            _persistent_sessions[session_key] = _PersistentSessionState(
                subscriptions=unprefixed_filters, created_at=time.time()
            )


async def handle_websocket(
    scope: dict, receive, send, account_id: str, region: str
) -> None:
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
    session = _WSSession(send, account_id, region)
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
