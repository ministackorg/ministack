"""AWS IoT Core control plane emulator.

Implements the JSON/REST APIs under ``iot.{region}.amazonaws.com``:

  - Thing registry: ``CreateThing``, ``DescribeThing``, ``ListThings``,
    ``UpdateThing``, ``DeleteThing``
  - ThingType: ``CreateThingType`` and friends
  - ThingGroup: ``CreateThingGroup`` and friends
  - Certificates: ``CreateKeysAndCertificate``, ``RegisterCertificate``,
    ``RegisterCertificateWithoutCA``, ``UpdateCertificate``,
    ``DeleteCertificate``, ``AttachThingPrincipal`` / ``DetachThingPrincipal``
  - CA certificates + JITR: ``GetRegistrationCode`` / ``DeleteRegistrationCode``,
    ``RegisterCACertificate``, ``DescribeCACertificate``, ``UpdateCACertificate``,
    ``ListCACertificates``, ``DeleteCACertificate``; registering a device
    certificate under a CA with auto-registration enabled publishes the AWS
    JITR event to ``$aws/events/certificates/registered/{caCertificateId}``
  - Policies: ``CreatePolicy``, ``CreatePolicyVersion``, ``AttachPolicy``,
    ``DetachPolicy``, etc., plus the deprecated principal-policy family
    (``AttachPrincipalPolicy`` / ``DetachPrincipalPolicy`` /
    ``ListPrincipalPolicies`` / ``ListPolicyPrincipals``)
    ``DetachPolicy``, etc.
  - Fleet indexing: ``UpdateIndexingConfiguration`` /
    ``GetIndexingConfiguration`` / ``DescribeIndex`` / ``ListIndices``, and
    ``SearchIndex`` over the live registry, shadows and MQTT connectivity
  - Jobs (control plane): ``CreateJob``, ``DescribeJob``, ``ListJobs``,
    ``GetJobDocument``, ``CancelJob``, ``DeleteJob``,
    ``ListJobExecutionsForThing``, ``DescribeJobExecution``,
    ``CancelJobExecution`` — execution state shared with the
    ``iot-jobs-data`` device data plane (``iot_jobs_data.py``)
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
import ssl
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
    _request_region,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    request_scope,
)
from ministack.core.x509_utils import (
    certificate_is_signed_by,
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
# CA-certificate registry (RegisterCACertificate & friends): caCertificateId -> record
_ca_certificates: AccountRegionScopedDict = AccountRegionScopedDict()
# JITR registration code — a single "code" key per account/region
_registration_codes: AccountRegionScopedDict = AccountRegionScopedDict()
_jobs: AccountRegionScopedDict = AccountRegionScopedDict()  # jobId -> Job dict
# (thingName, jobId) -> JobExecution dict — tuple keys, same pattern as _shadows
_job_executions: AccountRegionScopedDict = AccountRegionScopedDict()

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


# Server certificate for the mTLS MQTT listener (the broker's TCP transport,
# further down in this file). Persisted alongside the CA in the same snapshot:
# a device that pinned the broker's chain keeps working across restarts,
# exactly like the client certificates the CA signed.
_mtls_server_cert_pem: str | None = None
_mtls_server_key_pem: str | None = None


def get_mtls_server_cert() -> tuple[str | None, str | None]:
    """Return the persisted (cert_pem, key_pem) of the mTLS listener, if any."""
    return _mtls_server_cert_pem, _mtls_server_key_pem


def set_mtls_server_cert(cert_pem: str, key_pem: str) -> None:
    """Record a freshly minted mTLS listener certificate for persistence."""
    global _mtls_server_cert_pem, _mtls_server_key_pem
    with _CA_LOCK:
        _mtls_server_cert_pem = cert_pem
        _mtls_server_key_pem = key_pem


# ---------------------------------------------------------------------------
# Broker state
# ---------------------------------------------------------------------------

_retained: dict[str, "_RetainedMessage"] = {}


class _RetainedMessage:
    __slots__ = ("payload", "properties", "qos", "topic", "ts")

    def __init__(self, topic: str, payload: bytes, qos: int, properties: bytes = b""):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        # Encoded MQTT 5 property block forwarded to v5 subscribers, empty for
        # a message published by a 3.1.1 client. Runtime only: properties are
        # not part of the persisted retained-message state.
        self.properties = properties
        self.ts = time.time()


def _broker_get_state() -> dict:
    # Retained messages only. Connectivity is deliberately not persisted: no
    # session survives a restart, so restoring a thing as connected would put a
    # claim in the fleet index that the broker cannot back with a live session.
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
        "ca_certificates": copy.deepcopy(_ca_certificates),
        "registration_codes": copy.deepcopy(_registration_codes),
        "jobs": copy.deepcopy(_jobs),
        "job_executions": copy.deepcopy(_job_executions),
        "ca": {"ca_cert_pem": _ca_cert_pem, "ca_key_pem": _ca_key_pem}
        if _ca_cert_pem and _ca_key_pem
        else {},
        "mtls_server": {
            "cert_pem": _mtls_server_cert_pem,
            "key_pem": _mtls_server_key_pem,
        }
        if _mtls_server_cert_pem and _mtls_server_key_pem
        else {},
        "mqtt_broker": _broker_get_state(),
    }


def restore_state(data: dict | None) -> None:
    global _ca_cert_pem, _ca_key_pem
    global _mtls_server_cert_pem, _mtls_server_key_pem
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
    _ca_certificates.update(data.get("ca_certificates", {}))
    _registration_codes.update(data.get("registration_codes", {}))
    _jobs.update(data.get("jobs", {}))
    _job_executions.update(data.get("job_executions", {}))
    ca_data = data.get("ca")
    if ca_data:
        cert = ca_data.get("ca_cert_pem")
        key = ca_data.get("ca_key_pem")
        if cert and key:
            with _CA_LOCK:
                _ca_cert_pem = cert
                _ca_key_pem = key
            logger.info("Local CA: restored from persisted state")
    mtls_data = data.get("mtls_server")
    if mtls_data:
        cert = mtls_data.get("cert_pem")
        key = mtls_data.get("key_pem")
        if cert and key:
            with _CA_LOCK:
                _mtls_server_cert_pem = cert
                _mtls_server_key_pem = key
    _broker_restore_state(data.get("mqtt_broker"))


def reset() -> None:
    global _ca_cert_pem, _ca_key_pem
    global _mtls_server_cert_pem, _mtls_server_key_pem
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
    _ca_certificates.clear()
    _registration_codes.clear()
    _jobs.clear()
    _job_executions.clear()
    with _CA_LOCK:
        _ca_cert_pem = None
        _ca_key_pem = None
        _mtls_server_cert_pem = None
        _mtls_server_key_pem = None
    # The mTLS listener's server certificate and trust anchors were minted from
    # the CA we just dropped, so it has to re-bind with fresh material. Cheap
    # when the listener never started: with no bound loop the call returns
    # immediately.
    try:
        mtls_schedule_restart()
    except Exception:
        logger.debug("IoT mTLS: restart after reset failed", exc_info=True)


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


def _ca_cert_arn(certificate_id: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:cacert/{certificate_id}"


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
    """Decode a JSON request body. Anything but a JSON object yields ``{}`` —
    every caller treats the result as a dict, so a bare array or string body
    must not reach them as one."""
    if not body:
        return {}
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


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
    # Job executions live at /things/{name}/jobs[/{jobId}[/cancel]] — like
    # /principals, must come BEFORE the generic /things/{name} branch. The
    # `jobs` segment only counts when a thing name precedes it: `/things/jobs`
    # is thing CRUD for a thing that happens to be named `jobs`.
    if path.startswith("/things/"):
        _thing, _, _sub = path[len("/things/"):].partition("/")
        if _thing and (_sub == "jobs" or _sub.startswith("jobs/")):
            return _handle_thing_jobs(method, path, body, qp)
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
        return await _register_certificate(_parse_body(body), qp)
    if path == "/certificate/register-no-ca" and method == "POST":
        return await _register_certificate(_parse_body(body), qp, without_ca=True)

    # CA certificates + JITR registration code
    if path == "/registrationcode" and method in ("GET", "DELETE"):
        return _handle_registration_code(method)
    if path == "/cacertificate" and method == "POST":
        return _register_ca_certificate(_parse_body(body), qp)
    if path == "/cacertificates" and method == "GET":
        return _list_ca_certificates(qp)
    if path.startswith("/cacertificate/") and method in ("GET", "PUT", "DELETE"):
        return _handle_ca_certificate(method, path, body, qp)

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
    # Jobs (control plane) — CreateJob is PUT /jobs/{jobId} per the botocore
    # `iot` service model; sub-resources (/cancel, /job-document) are
    # dispatched inside _handle_job.
    if path == "/jobs" and method == "GET":
        return _list_jobs(qp)
    if path.startswith("/jobs/"):
        return _handle_job(method, path, body, qp)

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
    # Drop the thing's job executions with it. Left behind, a live execution
    # for a thing that no longer exists holds its job out of COMPLETED forever,
    # and a later thing of the same name would inherit that stale history.
    orphaned_jobs = [key[1] for key in _job_executions.keys() if key[0] == name]
    for job_id in orphaned_jobs:
        del _job_executions[(name, job_id)]
    del _things[name]
    for job_id in orphaned_jobs:
        _jobs_maybe_complete(job_id)
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


def _certificate_already_exists(cert_id: str, arn: str | None = None) -> tuple:
    """409 for a duplicate PEM, carrying ``resourceId``/``resourceArn`` the way
    real AWS's ``ResourceAlreadyExistsException`` does — all register variants
    (device certs and CA certs) answer identically."""
    return error_response_json(
        "ResourceAlreadyExistsException",
        f"The certificate with id {cert_id} already exists.",
        409,
        extra={"resourceId": cert_id, "resourceArn": arn or _cert_arn(cert_id)},
    )


async def _register_certificate(
    payload: dict, qp: dict, *, without_ca: bool = False
) -> tuple:
    """Register a certificate that was issued elsewhere (no re-signing).

    Serves both ``RegisterCertificate`` (``POST /certificate/register``) and
    ``RegisterCertificateWithoutCA`` (``POST /certificate/register-no-ca``).
    botocore models ``setAsActive`` as a *querystring* member (as in
    ``CreateKeysAndCertificate``), so it is read from ``qp`` first, with the
    JSON body kept as a fallback for raw callers. The no-CA variant carries no
    CA reference and takes its status from the plain ``status`` body field
    only — it has no deprecated ``setAsActive``.

    ``caCertificatePem`` must name a CA registered via
    ``RegisterCACertificate`` in this account/region that really signed the
    leaf; anything else is a ``CertificateValidationException``. When that CA's
    ``autoRegistrationStatus`` is ``ENABLE``, the AWS JITR lifecycle event is
    published to ``$aws/events/certificates/registered/{caCertificateId}`` so
    just-in-time-registration Lambdas subscribed via topic rules fire exactly
    as on AWS.
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
        ca_id = get_certificate_id(ca_pem) if ca_pem else None
    except Exception as e:
        return error_response_json(
            "CertificateValidationException",
            f"Invalid certificate PEM: {e}",
            400,
        )
    # caCertificatePem is a claim about who signed the leaf, and the whole JITR
    # flow keys off it: the event topic, and with it the provisioning template a
    # JITR Lambda picks, is chosen by caCertificateId. An unchecked claim lets a
    # certificate signed by CA-X register as CA-Y's and provision the wrong
    # fleet, so the claim is verified the way AWS does before anything is
    # stored — the CA has to be registered in this account/region, and it has to
    # be the certificate that actually signed the leaf.
    ca = None
    if ca_id is not None:
        ca = _ca_certificates.get(ca_id)
        if ca is None:
            return error_response_json(
                "CertificateValidationException",
                (
                    f"The CA certificate {ca_id} is not registered in this "
                    "account and region. Register it with RegisterCACertificate, "
                    "or use RegisterCertificateWithoutCA."
                ),
                400,
            )
        if not certificate_is_signed_by(cert_pem, ca_pem):
            return error_response_json(
                "CertificateValidationException",
                (
                    f"The certificate was not signed by CA {ca_id}. "
                    "caCertificatePem must be the CA certificate that issued "
                    "certificatePem."
                ),
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
        "caCertificateId": ca_id,
        "attachedThings": [],
        "attachedPolicies": [],
    }
    _certificates[cert_id] = record

    # JITR: the registered-certificate lifecycle event, fired when the
    # referenced CA is ACTIVE and has auto-registration enabled — AWS
    # auto-registers nothing under an INACTIVE CA. certificateStatus echoes
    # the requested register status; real connect-triggered auto-registration
    # carries PENDING_ACTIVATION, but here registration is always explicit —
    # there is no mTLS first-connect path to auto-register from.
    if (
        ca
        and ca.get("status") == "ACTIVE"
        and ca.get("autoRegistrationStatus") == "ENABLE"
    ):
        now_ms = int(time.time() * 1000)
        event = {
            "certificateId": cert_id,
            "caCertificateId": ca_id,
            "timestamp": now_ms,
            "certificateStatus": record["status"],
            "awsAccountId": get_account_id(),
            "certificateRegistrationTimestamp": str(now_ms),
        }
        try:
            await broker_publish(
                get_account_id(),
                get_region(),
                f"$aws/events/certificates/registered/{ca_id}",
                json.dumps(event).encode("utf-8"),
                qos=0,
            )
        except Exception:
            # The certificate is already registered at this point, so a broker
            # failure must not turn a successful registration into a 500 — the
            # iot-data publish path guards the same call the same way.
            logger.warning(
                "JITR registered-event publish failed for CA %s", ca_id, exc_info=True
            )

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
        description = {
            "certificateArn": record["certificateArn"],
            "certificateId": record["certificateId"],
            "status": record["status"],
            "certificatePem": record["certificatePem"],
            "ownedBy": record["ownedBy"],
            "creationDate": record.get("creationDate"),
        }
        # Present only for CA-signed registrations, so JITR consumers can
        # resolve the signing CA (per the CertificateDescription model).
        if record.get("caCertificateId"):
            description["caCertificateId"] = record["caCertificateId"]
        return json_response({"certificateDescription": description})
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
                406,
            )
        del _certificates[cert_id]
        return json_response({})
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


# ---------------------------------------------------------------------------
# CA-certificate registry + JITR registration code
# ---------------------------------------------------------------------------


def _handle_registration_code(method: str) -> tuple:
    """``GET|DELETE /registrationcode`` — ``GetRegistrationCode`` /
    ``DeleteRegistrationCode``.

    The code is minted once per account/region (a random SHA-256 hex string,
    AWS-shaped), persisted, and returned unchanged on every subsequent GET.
    DELETE discards it; the next GET mints a fresh one.
    """
    if method == "GET":
        code = _registration_codes.get("code")
        if not code:
            code = hashlib.sha256(os.urandom(32)).hexdigest()
            _registration_codes["code"] = code
        return json_response({"registrationCode": code})
    # DELETE
    _registration_codes.pop("code", None)
    return json_response({})


def _register_ca_certificate(payload: dict, qp: dict) -> tuple:
    """``POST /cacertificate`` (``RegisterCACertificate``).

    Body members per the botocore model: ``caCertificate`` (required) and
    ``verificationCertificate`` (accepted; the registration-code CN handshake
    is not enforced locally). ``setAsActive`` / ``allowAutoRegistration`` ride
    as query-string booleans, as on AWS.
    """
    ca_pem = payload.get("caCertificate")
    if not ca_pem:
        return error_response_json(
            "InvalidRequestException", "caCertificate is required", 400
        )
    try:
        ca_id = get_certificate_id(ca_pem)
    except Exception as e:
        return error_response_json(
            "CertificateValidationException",
            f"Invalid CA certificate PEM: {e}",
            400,
        )
    if ca_id in _ca_certificates:
        return _certificate_already_exists(ca_id, _ca_cert_arn(ca_id))
    set_active = _qp_bool(qp, "setAsActive")
    allow_auto = _qp_bool(qp, "allowAutoRegistration")
    record = {
        "certificateId": ca_id,
        "certificateArn": _ca_cert_arn(ca_id),
        "certificatePem": ca_pem,  # verbatim
        "status": "ACTIVE" if set_active else "INACTIVE",
        "autoRegistrationStatus": "ENABLE" if allow_auto else "DISABLE",
        "creationDate": _now_epoch(),
        "ownedBy": get_account_id(),
    }
    _ca_certificates[ca_id] = record
    return json_response({
        "certificateArn": record["certificateArn"],
        "certificateId": ca_id,
    })


def _list_ca_certificates(qp: dict) -> tuple:
    """``GET /cacertificates`` (``ListCACertificates``)."""
    return json_response({
        "certificates": [
            {
                "certificateArn": c["certificateArn"],
                "certificateId": c["certificateId"],
                "status": c["status"],
                "creationDate": c.get("creationDate"),
            }
            for c in _ca_certificates.values()
        ]
    })


def _handle_ca_certificate(method: str, path: str, body: bytes, qp: dict) -> tuple:
    """``GET|PUT|DELETE /cacertificate/{caCertificateId}`` —
    ``DescribeCACertificate`` / ``UpdateCACertificate`` /
    ``DeleteCACertificate``.
    """
    ca_id = path[len("/cacertificate/"):]
    if not ca_id or "/" in ca_id:
        return error_response_json(
            "InvalidRequestException", "Invalid CA certificate path", 400
        )
    record = _ca_certificates.get(ca_id)
    if record is None:
        return _error_not_found("CACertificate", ca_id)
    if method == "GET":
        return json_response({
            "certificateDescription": {
                "certificateArn": record["certificateArn"],
                "certificateId": record["certificateId"],
                "status": record["status"],
                "certificatePem": record["certificatePem"],
                "autoRegistrationStatus": record["autoRegistrationStatus"],
                "ownedBy": record["ownedBy"],
                "creationDate": record.get("creationDate"),
            }
        })
    if method == "PUT":
        # newStatus / newAutoRegistrationStatus are query-string params per the
        # botocore model; both are optional and independently applied. The body
        # is read as a fallback, as UpdateCertificate does — otherwise a raw
        # caller that puts them in the JSON body gets a 200 that applied
        # nothing.
        payload = _parse_body(body)
        new_status = payload.get("newStatus") or qp.get("newStatus")
        new_auto = (
            payload.get("newAutoRegistrationStatus")
            or qp.get("newAutoRegistrationStatus")
        )
        if new_status is not None and new_status not in ("ACTIVE", "INACTIVE"):
            return error_response_json(
                "InvalidRequestException",
                "newStatus must be one of ['ACTIVE', 'INACTIVE']",
                400,
            )
        if new_auto is not None and new_auto not in ("ENABLE", "DISABLE"):
            return error_response_json(
                "InvalidRequestException",
                "newAutoRegistrationStatus must be one of ['DISABLE', 'ENABLE']",
                400,
            )
        if new_status is not None:
            record["status"] = new_status
        if new_auto is not None:
            record["autoRegistrationStatus"] = new_auto
        _ca_certificates[ca_id] = record
        return json_response({})
    if method == "DELETE":
        if record["status"] == "ACTIVE":
            return error_response_json(
                "CertificateStateException",
                "CA certificate is ACTIVE; deactivate before deletion",
                406,
            )
        del _ca_certificates[ca_id]
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
# Unlike attributes.* and shadow.*, the connectivity group is closed — AWS
# defines exactly these three — so an unknown leaf is a typo worth reporting
# rather than a field that happens to be absent from this thing.
_SEARCH_CONNECTIVITY_FIELDS = ("connected", "timestamp", "disconnectReason")

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


def _thing_connectivity_indexing_mode(config: dict) -> str:
    return config.get("thingIndexingConfiguration", {}).get(
        "thingConnectivityIndexingMode", "OFF"
    )


def _update_indexing_configuration(payload: dict) -> tuple:
    """``POST /indexing/config`` — enable or disable fleet indexing.

    Each sub-configuration that the request carries replaces the stored one
    wholesale (AWS's own semantics: the mode is required, so a partial update
    of a sub-configuration is not expressible); an omitted sub-configuration is
    left alone.

    Only the modes this emulator can honor are interpreted —
    ``thingIndexingMode`` gates ``SearchIndex`` and whether shadow fields are
    queryable, and ``thingConnectivityIndexingMode`` gates the
    ``connectivity`` group. ``deviceDefenderIndexingMode``,
    ``namedShadowIndexingMode``, ``customFields`` and ``filter`` are stored and
    echoed back by ``GetIndexingConfiguration`` so IaC round-trips cleanly, but
    nothing here projects those fields; querying one is an out-of-grammar
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
    schema = (
        "REGISTRY_AND_SHADOW"
        if thing_cfg.get("thingIndexingMode") == "REGISTRY_AND_SHADOW"
        else "REGISTRY"
    )
    if _thing_connectivity_indexing_mode(config) == "STATUS":
        schema += "_AND_CONNECTIVITY_STATUS"
    return schema


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


def _thing_connectivity(account_id: str, region: str, thing_name: str) -> dict:
    """The ``connectivity`` group for a thing, as fleet indexing reports it.

    A thing counts as connected when a live MQTT session's *client id* equals
    the thing name. That is the association AWS makes, and it is the only one
    available: the broker knows client ids, and a device that connects under
    some other client id is legitimately not this thing's connectivity, even
    if the same hardware is behind it.

    ``connected`` is read from the session registry on every call rather than
    stored, so it cannot drift; only the transition timestamp and the reason
    the last session ended are remembered. A thing that has never connected
    reports ``connected: false`` with no timestamp — the honest answer, rather
    than an epoch that implies a disconnect that never happened.

    ``disconnectReason`` is reported whenever one is on record, including
    while the thing is connected: after a takeover that reason is
    DUPLICATE_CLIENTID, and the takeover is precisely the transition that
    produced the session now reporting itself online. An ordinary reconnect
    clears it (see ``_register_client``), so a reason that is present always
    describes how the *current* state came about.
    """
    key = (account_id, region, thing_name)
    doc: dict = {"connected": key in _connected_clients}
    record = _connectivity.get(key)
    if record is None:
        return doc
    doc["timestamp"] = record["timestamp"]
    if record.get("disconnectReason"):
        doc["disconnectReason"] = record["disconnectReason"]
    return doc


def _search_field_error(
    field: str, thing_mode: str, connectivity_mode: str
) -> str | None:
    """Why ``field`` is not queryable, or None when it is.

    Every rejection is a 400 rather than a query that matches nothing: a
    mistyped field and a field that no thing happens to carry are
    indistinguishable in the results, and only one of them is the caller's bug.
    """
    if field in _SEARCH_TOP_FIELDS:
        return None
    if field.startswith("attributes.") and field != "attributes.":
        return None
    if field.startswith("connectivity."):
        leaf = field.split(".", 1)[1]
        if leaf not in _SEARCH_CONNECTIVITY_FIELDS:
            return f"Unsupported query field: {field!r}"
        if connectivity_mode != "STATUS":
            return (
                f"Connectivity field {field!r} is not indexed: "
                "thingConnectivityIndexingMode is OFF"
            )
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


def _search_field_value(
    thing: dict, field: str, shadow_state: dict | None, connectivity: dict
):
    if field == "thingName":
        return thing.get("thingName")
    if field == "thingTypeName":
        return thing.get("thingTypeName")
    if field == "thingGroupNames":
        return thing.get("thingGroupNames") or None
    if field.startswith("connectivity."):
        # A missing key answers None, which never matches — so a thing that is
        # currently connected does not match connectivity.disconnectReason:*.
        return connectivity.get(field.split(".", 1)[1])
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
    ``thingTypeName``, ``thingGroupNames`` and ``attributes.<name>``; over
    ``shadow.desired|reported.<path>`` once ``thingIndexingMode`` is
    REGISTRY_AND_SHADOW; and over ``connectivity.connected`` / ``.timestamp``
    / ``.disconnectReason`` once ``thingConnectivityIndexingMode`` is STATUS.
    Values compare case-insensitively, with ``*`` / ``?`` wildcards.
    Anything outside that grammar is rejected with ``InvalidQueryException``.

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
    connectivity_mode = _thing_connectivity_indexing_mode(config)
    for field, _ in terms:
        problem = _search_field_error(field, thing_mode, connectivity_mode)
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
    connectivity_indexed = connectivity_mode == "STATUS"
    account_id, region = get_account_id(), get_region()
    matched = []
    for name, thing in _things.items():
        shadow_state = _classic_shadow_state(name) if shadows_indexed else None
        connectivity = (
            _thing_connectivity(account_id, region, name)
            if connectivity_indexed
            else {}
        )
        if not all(
            _search_value_matches(
                _search_field_value(thing, f, shadow_state, connectivity), v
            )
            for f, v in terms
        ):
            continue
        entry = {
            "thingName": thing["thingName"],
            "thingId": thing.get("thingId", name),
            "attributes": thing.get("attributes") or {},
            "thingGroupNames": list(thing.get("thingGroupNames") or []),
        }
        if connectivity_indexed:
            entry["connectivity"] = connectivity
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


class RuleRoleError(ValueError):
    """An action role ARN IAM cannot resolve (AUTH=true only), raised by
    `put_topic_rule`."""


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
    AWS) but warns, so the resulting misfire is visible. Raises `RuleRoleError`
    (under AUTH=true) for an action role IAM cannot resolve, through the same
    two doors — real IoT probes the role at create time and fails the call.
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
    # Validate role ARNs in actions (AUTH=true only)
    from ministack.core.iam_evaluator import validate_role_arn
    for _iot_action in (payload.get("actions") or []):
        if isinstance(_iot_action, dict):
            for _act_cfg in _iot_action.values():
                if isinstance(_act_cfg, dict) and "roleArn" in _act_cfg:
                    _iot_role_err = validate_role_arn(_act_cfg["roleArn"])
                    if _iot_role_err:
                        raise RuleRoleError(
                            f"Unable to assume role: {_act_cfg['roleArn']}")

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
    except RuleRoleError as exc:
        return error_response_json("InvalidRequestException", str(exc), 400)
    return json_response({})


def _replace_topic_rule(name: str, payload: dict) -> tuple:
    if name not in _topic_rules:
        return _error_not_found("Rule", name)
    try:
        put_topic_rule(name, payload)
    except RuleSqlError as exc:
        return error_response_json("SqlParseException", str(exc), 400)
    except RuleRoleError as exc:
        return error_response_json("InvalidRequestException", str(exc), 400)
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
# IoT Jobs (control plane; execution state shared with iot_jobs_data.py)
# ---------------------------------------------------------------------------
#
# The job store and per-thing execution store live here so that the `iot`
# control plane and the `iot-jobs-data` data plane (iot_jobs_data.py, which
# imports this module directly — same pattern as iot_data.py and the shadow
# store) operate on the same records. Every rule of the state machine lives in
# this file; the data-plane module is a wire adapter over the `jobs_*` seam
# below. Transitions MiniStack can actually make:
#
#   QUEUED → IN_PROGRESS → {SUCCEEDED, FAILED, REJECTED, CANCELED}
#
# `versionNumber` gives optimistic concurrency: every transition bumps it, and
# a stale `expectedVersion` is rejected with a 409. `executionNumber` is NOT a
# concurrency token here — an execution is created once at 1 and nothing
# re-queues it, so it never changes (AWS increments it when a job execution is
# retried, which MiniStack does not model).
#
# TIMED_OUT and REMOVED are recognized as terminal (so a restored record in
# either state behaves, and `jobProcessDetails` counts them) but nothing sets
# them: there are no execution timeouts, and deleting a thing DELETES its
# executions (see `_delete_thing`) rather than marking them REMOVED.

# Job ids are stricter than thing names, and identically so in both service
# models (`iot` and `iot-jobs-data` declare JobId as [a-zA-Z0-9_-], max 64).
# The generic thing-name pattern would let a `:` through control-side, and the
# device that has to fetch that job by id could then never reach it.
_JOB_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")

_JOB_TARGET_SELECTIONS = {"SNAPSHOT", "CONTINUOUS"}

_JOB_EXECUTION_TERMINAL = {
    "SUCCEEDED", "FAILED", "TIMED_OUT", "CANCELED", "REJECTED", "REMOVED",
}
_JOB_EXECUTION_STATUSES = _JOB_EXECUTION_TERMINAL | {"QUEUED", "IN_PROGRESS"}

# The statuses a device may report through UpdateJobExecution, per the AWS API
# reference. The service-side statuses (CANCELED, TIMED_OUT, REMOVED) are only
# ever set by the control plane; a device sending one is rejected with
# InvalidRequestException, as on AWS.
_DEVICE_SETTABLE_STATUSES = {"IN_PROGRESS", "SUCCEEDED", "FAILED", "REJECTED"}


def _jobs_now_ms() -> int:
    """Epoch MILLISECONDS — the unit job/execution records store internally.

    The two planes emit different units for these same instants: the `iot`
    control plane models them as `timestamp` shapes, which botocore parses
    as epoch SECONDS (so control-plane responses divide by 1000 via
    :func:`_jobs_ms_to_s`), while `iot-jobs-data` models them as raw `long`
    shapes carrying epoch MILLISECONDS (emitted as-is). Mixing the units up
    makes botocore explode while parsing ("year 58580 is out of range"), so
    every response path converts explicitly at the edge.
    """
    return int(time.time() * 1000)


def _jobs_ms_to_s(millis: int | None) -> float | None:
    """Millisecond record stamp → epoch-seconds float for `timestamp` shapes."""
    return None if millis is None else millis / 1000.0


def _job_arn(job_id: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:job/{job_id}"


def _job_target_resolve(arn: str) -> list[str] | None:
    """Thing names one job target ARN resolves to, under the caller's scope.

    ``None`` means the ARN targets nothing this caller can reach: another
    service, another account, another region, or a thing / thing group that
    does not exist here. A thing group that exists but is empty resolves to an
    empty list — a real target with no members today.

    Group membership is read natively from `_thing_groups` — no HTTP hop.
    :func:`_create_job` gates on exactly this function, so a target it accepts
    is a target the materializer can resolve; validating creation any more
    loosely (e.g. on the ARN's resource segment alone) admits a cross-region
    target that silently resolves to zero executions, leaving a job that can
    never complete and can only be deleted with `force`.
    """
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None
    if (
        spec.service != "iot"
        or spec.account_id != get_account_id()
        or spec.region != get_region()
    ):
        return None
    if spec.resource.startswith("thing/"):
        thing = spec.resource[len("thing/"):]
        return [thing] if thing in _things else None
    if spec.resource.startswith("thinggroup/"):
        group = _thing_groups.get(spec.resource[len("thinggroup/"):])
        return None if group is None else list(group.get("things", []))
    return None


def _job_target_things(targets: list) -> list:
    """Resolve job targets (thing / thing-group ARNs) to thing names."""
    things: list[str] = []
    for arn in targets or []:
        things.extend(_job_target_resolve(arn) or [])
    return list(dict.fromkeys(things))


def _jobs_materialize_executions(job_id: str) -> None:
    """Materialize QUEUED executions for a job's current targets.

    SNAPSHOT jobs resolve group membership exactly once (at creation) — the
    `snapshotted` flag latches that. CONTINUOUS jobs re-resolve lazily on
    every read, which is exactly when new membership matters and avoids
    watching group mutations.
    """
    job = _jobs.get(job_id)
    if not job or job.get("status") == "CANCELED":
        return
    if job.get("targetSelection") == "SNAPSHOT" and job.get("snapshotted"):
        return
    now = _jobs_now_ms()
    for thing in _job_target_things(job.get("targets")):
        key = (thing, job_id)
        if key in _job_executions:
            continue
        _job_executions[key] = {
            "jobId": job_id,
            "thingName": thing,
            "status": "QUEUED",
            "statusDetails": {},
            "queuedAt": now,
            "startedAt": None,
            "lastUpdatedAt": now,
            "executionNumber": 1,
            "versionNumber": 1,
        }
    if job.get("targetSelection") == "SNAPSHOT":
        job["snapshotted"] = True


def _jobs_materialize_all() -> None:
    for job_id in list(_jobs.keys()):
        _jobs_materialize_executions(job_id)


def _jobs_maybe_complete(job_id: str) -> None:
    """A non-CONTINUOUS job whose executions are all terminal is COMPLETED.

    "All" includes none left: every caller reaches here through a path that
    materialized the job's executions first, so an empty list means the last
    one just went away (its thing was deleted), not that the job has yet to
    start. Waiting for a non-empty list there would strand the job.
    """
    job = _jobs.get(job_id)
    if (
        not job
        or job.get("targetSelection") == "CONTINUOUS"
        or job.get("status") != "IN_PROGRESS"
    ):
        return
    executions = [e for e in _job_executions.values() if e["jobId"] == job_id]
    if all(e["status"] in _JOB_EXECUTION_TERMINAL for e in executions):
        now = _jobs_now_ms()
        job["status"] = "COMPLETED"
        job["completedAt"] = now
        job["lastUpdatedAt"] = now


def _jobs_check_expected_version(
    execution: dict, expected, conflict_code: str
) -> tuple | None:
    """Optimistic-concurrency gate, shared by the two update paths.

    The rejection message carries the current version so a device can resync
    without a separate describe. `conflict_code` differs by plane and is NOT
    cosmetic: the `iot` model declares VersionConflictException for
    CancelJobExecution, while the `iot-jobs-data` model declares only
    InvalidStateTransitionException for UpdateJobExecution — returning the
    unmodeled code there makes a device's
    `except client.exceptions.VersionConflictException` raise AttributeError
    instead of catching, because botocore only synthesizes exception classes
    for the errors its model lists.
    """
    if expected is None:
        return None
    try:
        expected = int(expected)
    except (TypeError, ValueError):
        return error_response_json(
            "InvalidRequestException", f"Invalid expectedVersion: {expected!r}", 400
        )
    if expected != execution["versionNumber"]:
        return error_response_json(
            conflict_code,
            f"Expected version {expected} does not match current version "
            f"{execution['versionNumber']} of the job execution",
            409,
        )
    return None


def _jobs_reject_if_terminal(execution: dict, verb: str) -> tuple | None:
    """Terminal executions are frozen — no update, no cancel."""
    if execution["status"] not in _JOB_EXECUTION_TERMINAL:
        return None
    return error_response_json(
        "InvalidStateTransitionException",
        f"Job execution is in terminal status {execution['status']} and "
        f"cannot be {verb}",
        409,
    )


def _handle_job(method: str, path: str, body: bytes, qp: dict) -> tuple:
    """Dispatch /jobs/{jobId}[...] control-plane routes."""
    suffix = path[len("/jobs/"):]
    if suffix.endswith("/cancel") and method == "PUT":
        return _cancel_job(suffix[:-len("/cancel")], _parse_body(body), qp)
    if suffix.endswith("/job-document") and method == "GET":
        return _get_job_document(suffix[:-len("/job-document")])
    if "/" in suffix:
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
        )
    job_id = suffix
    if method == "PUT":
        return _create_job(job_id, _parse_body(body))
    if method == "GET":
        return _describe_job(job_id)
    if method == "DELETE":
        return _delete_job(job_id, qp)
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


def _create_job(job_id: str, payload: dict) -> tuple:
    if not job_id or not _JOB_ID_RE.match(job_id):
        return error_response_json(
            "InvalidRequestException",
            "Invalid jobId: must match [a-zA-Z0-9_-]{1,64}",
            400,
        )
    if job_id in _jobs:
        return error_response_json(
            "ResourceAlreadyExistsException", f"Job {job_id} already exists", 409
        )
    # Absent means SNAPSHOT (the AWS default); present means it has to be one
    # of the two modeled values — including the empty string, which would
    # otherwise default its way to a SNAPSHOT job the caller never asked for.
    target_selection = payload.get("targetSelection")
    if target_selection is None:
        target_selection = "SNAPSHOT"
    if target_selection not in _JOB_TARGET_SELECTIONS:
        return error_response_json(
            "InvalidRequestException",
            f"Invalid targetSelection: {target_selection!r}; must be one of "
            + ", ".join(sorted(_JOB_TARGET_SELECTIONS)),
            400,
        )
    targets = payload.get("targets")
    if not targets:
        return error_response_json(
            "InvalidRequestException", "targets must not be empty", 400
        )
    # Every target must name a thing or thing group this caller can reach, as
    # on AWS — and reach means under the SAME account and region, which is what
    # the materializer resolves against.
    for arn in targets:
        try:
            parse_arn(arn)
        except ArnParseError:
            return error_response_json(
                "InvalidRequestException", f"Invalid target arn: {arn}", 400
            )
        if _job_target_resolve(arn) is None:
            return error_response_json(
                "ResourceNotFoundException", f"Job target {arn} not found", 404
            )
    document = payload.get("document")
    if not document and payload.get("documentSource"):
        # DELIBERATE DIVERGENCE: AWS fetches the document from the S3 URL and
        # serves its CONTENT to devices. MiniStack does not fetch it — it
        # stands in a placeholder naming the source, so a `documentSource` job
        # still creates, describes, and runs its whole execution lifecycle.
        # A device that parses the document therefore sees the URL, not the
        # payload; pass `document` when the content matters.
        document = json.dumps({"documentSource": payload["documentSource"]})
    now = _jobs_now_ms()
    _jobs[job_id] = {
        "jobId": job_id,
        "jobArn": _job_arn(job_id),
        "description": payload.get("description"),
        "targets": list(targets),
        "targetSelection": target_selection,
        "document": document or "{}",
        "documentSource": payload.get("documentSource"),
        "status": "IN_PROGRESS",
        "createdAt": now,
        "lastUpdatedAt": now,
        "completedAt": None,
        "presignedUrlConfig": payload.get("presignedUrlConfig") or {},
        "jobExecutionsRolloutConfig": payload.get("jobExecutionsRolloutConfig")
        or {},
        "snapshotted": False,
    }
    _jobs_materialize_executions(job_id)
    response = {"jobArn": _job_arn(job_id), "jobId": job_id}
    if payload.get("description") is not None:
        response["description"] = payload["description"]
    return json_response(response)


def _job_summary(job: dict) -> dict:
    summary = {
        "jobArn": job["jobArn"],
        "jobId": job["jobId"],
        "targetSelection": job["targetSelection"],
        "status": job["status"],
        "createdAt": _jobs_ms_to_s(job["createdAt"]),
        "lastUpdatedAt": _jobs_ms_to_s(job["lastUpdatedAt"]),
    }
    if job.get("completedAt") is not None:
        summary["completedAt"] = _jobs_ms_to_s(job["completedAt"])
    return summary


def _describe_job(job_id: str) -> tuple:
    job = _jobs.get(job_id)
    if job is None:
        return _error_not_found("Job", job_id)
    _jobs_materialize_executions(job_id)
    counts = {status: 0 for status in _JOB_EXECUTION_STATUSES}
    for execution in _job_executions.values():
        if execution["jobId"] == job_id:
            counts[execution["status"]] += 1
    job_doc = {
        **_job_summary(job),
        "targets": list(job.get("targets") or []),
        "presignedUrlConfig": job.get("presignedUrlConfig") or {},
        "jobExecutionsRolloutConfig": job.get("jobExecutionsRolloutConfig") or {},
        "jobProcessDetails": {
            "numberOfQueuedThings": counts["QUEUED"],
            "numberOfInProgressThings": counts["IN_PROGRESS"],
            "numberOfSucceededThings": counts["SUCCEEDED"],
            "numberOfFailedThings": counts["FAILED"],
            "numberOfCanceledThings": counts["CANCELED"],
            "numberOfRejectedThings": counts["REJECTED"],
            "numberOfRemovedThings": counts["REMOVED"],
            "numberOfTimedOutThings": counts["TIMED_OUT"],
        },
    }
    if job.get("description") is not None:
        job_doc["description"] = job["description"]
    if job.get("comment") is not None:
        job_doc["comment"] = job["comment"]
    if job.get("reasonCode") is not None:
        job_doc["reasonCode"] = job["reasonCode"]
    response = {"job": job_doc}
    if job.get("documentSource") is not None:
        response["documentSource"] = job["documentSource"]
    return json_response(response)


def _delete_job(job_id: str, qp: dict) -> tuple:
    job = _jobs.get(job_id)
    if job is None:
        return _error_not_found("Job", job_id)
    force = _qp_bool(qp, "force")
    if job["status"] == "IN_PROGRESS" and not force:
        return error_response_json(
            "InvalidStateTransitionException",
            f"Job {job_id} is in status IN_PROGRESS and cannot be deleted "
            "without force",
            409,
        )
    del _jobs[job_id]
    for key in [k for k in _job_executions.keys() if k[1] == job_id]:
        del _job_executions[key]
    return json_response({})


def _list_jobs(qp: dict) -> tuple:
    """``GET /jobs`` with ``status`` / ``targetSelection`` filters.

    ``maxResults`` / ``nextToken`` are not honored: the full list comes back
    as one page and no token is ever returned, so a paginator terminates
    after its first call (same stance as ``_list_indices``).
    """
    wanted_status = qp.get("status")
    wanted_selection = qp.get("targetSelection")
    jobs = [
        _job_summary(job)
        for job in _jobs.values()
        if (not wanted_status or job["status"] == wanted_status)
        and (not wanted_selection or job["targetSelection"] == wanted_selection)
    ]
    return json_response({"jobs": jobs})


def _get_job_document(job_id: str) -> tuple:
    job = _jobs.get(job_id)
    if job is None:
        return _error_not_found("Job", job_id)
    return json_response({"document": job.get("document") or "{}"})


def _cancel_job(job_id: str, payload: dict, qp: dict) -> tuple:
    job = _jobs.get(job_id)
    if job is None:
        return _error_not_found("Job", job_id)
    if job["status"] != "IN_PROGRESS":
        return error_response_json(
            "InvalidRequestException",
            f"Job {job_id} is in status {job['status']} and cannot be canceled",
            400,
        )
    force = _qp_bool(qp, "force")
    now = _jobs_now_ms()
    job["status"] = "CANCELED"
    job["lastUpdatedAt"] = now
    job["completedAt"] = now
    if payload.get("comment") is not None:
        job["comment"] = payload["comment"]
    if payload.get("reasonCode") is not None:
        job["reasonCode"] = payload["reasonCode"]
    # QUEUED executions are always canceled with the job; IN_PROGRESS ones
    # only when force is set — as on AWS.
    for execution in _job_executions.values():
        if execution["jobId"] != job_id:
            continue
        if execution["status"] == "QUEUED" or (
            force and execution["status"] == "IN_PROGRESS"
        ):
            execution["status"] = "CANCELED"
            execution["lastUpdatedAt"] = now
            execution["versionNumber"] += 1
    response = {"jobArn": job["jobArn"], "jobId": job_id}
    if job.get("description") is not None:
        response["description"] = job["description"]
    return json_response(response)


def _handle_thing_jobs(method: str, path: str, body: bytes, qp: dict) -> tuple:
    """Dispatch /things/{name}/jobs[...] control-plane routes."""
    rest = path[len("/things/"):]
    thing, _, sub = rest.partition("/")
    err = _validate_name(thing, "thingName")
    if err:
        return err
    if sub == "jobs":
        if method == "GET":
            return _list_job_executions_for_thing(thing, qp)
        return error_response_json(
            "InvalidRequestException", f"Unsupported method: {method}", 400
        )
    if not sub.startswith("jobs/"):
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
        )
    tail = sub[len("jobs/"):]
    if tail.endswith("/cancel") and method == "PUT":
        return _cancel_job_execution(
            thing, tail[:-len("/cancel")], _parse_body(body), qp
        )
    if "/" not in tail and method == "GET":
        return _describe_job_execution(thing, tail)
    return error_response_json(
        "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
    )


def _list_job_executions_for_thing(thing: str, qp: dict) -> tuple:
    """``GET /things/{thing}/jobs`` — one unbounded page, like ``_list_jobs``."""
    _jobs_materialize_all()
    wanted_status = qp.get("status")
    wanted_job = qp.get("jobId")
    # The wire shape nests the detail under `jobExecutionSummary`, with only
    # the jobId beside it (JobExecutionSummaryForThing).
    summaries = []
    for execution in _job_executions.values():
        if execution["thingName"] != thing:
            continue
        if wanted_status and execution["status"] != wanted_status:
            continue
        if wanted_job and execution["jobId"] != wanted_job:
            continue
        summary = {
            "status": execution["status"],
            "queuedAt": _jobs_ms_to_s(execution["queuedAt"]),
            "lastUpdatedAt": _jobs_ms_to_s(execution["lastUpdatedAt"]),
            "executionNumber": execution["executionNumber"],
        }
        if execution.get("startedAt") is not None:
            summary["startedAt"] = _jobs_ms_to_s(execution["startedAt"])
        summaries.append(
            {"jobId": execution["jobId"], "jobExecutionSummary": summary}
        )
    return json_response({"executionSummaries": summaries})


def _describe_job_execution(thing: str, job_id: str) -> tuple:
    _jobs_materialize_executions(job_id)
    execution = _job_executions.get((thing, job_id))
    if execution is None:
        return _error_not_found("Job execution", f"{thing}/{job_id}")
    view = {
        "jobId": execution["jobId"],
        "status": execution["status"],
        # Control-plane JobExecution nests the map under `detailsMap`
        # (JobExecutionStatusDetails); the data plane returns it flat.
        "statusDetails": {"detailsMap": execution.get("statusDetails") or {}},
        "thingArn": _thing_arn(thing),
        "queuedAt": _jobs_ms_to_s(execution["queuedAt"]),
        "lastUpdatedAt": _jobs_ms_to_s(execution["lastUpdatedAt"]),
        "executionNumber": execution["executionNumber"],
        "versionNumber": execution["versionNumber"],
    }
    if execution.get("startedAt") is not None:
        view["startedAt"] = _jobs_ms_to_s(execution["startedAt"])
    return json_response({"execution": view})


def _cancel_job_execution(
    thing: str, job_id: str, payload: dict, qp: dict
) -> tuple:
    _jobs_materialize_executions(job_id)
    execution = _job_executions.get((thing, job_id))
    if execution is None:
        return _error_not_found("Job execution", f"{thing}/{job_id}")
    error = _jobs_check_expected_version(
        execution, payload.get("expectedVersion"), "VersionConflictException"
    )
    if error:
        return error
    error = _jobs_reject_if_terminal(execution, "canceled")
    if error:
        return error
    force = _qp_bool(qp, "force")
    if execution["status"] == "IN_PROGRESS" and not force:
        return error_response_json(
            "InvalidStateTransitionException",
            "Job execution is IN_PROGRESS and cannot be canceled without force",
            409,
        )
    execution["status"] = "CANCELED"
    if payload.get("statusDetails"):
        execution["statusDetails"] = dict(payload["statusDetails"])
    execution["lastUpdatedAt"] = _jobs_now_ms()
    execution["versionNumber"] += 1
    _jobs_maybe_complete(job_id)
    return json_response({})


# ---------------------------------------------------------------------------
# Helper exports for the data planes (iot_data, iot_jobs_data)
# ---------------------------------------------------------------------------


def lookup_certificate_by_id(cert_id: str, region: str) -> dict | None:
    """Return a certificate in the current account and explicit region."""
    return _certificates.get_scoped(get_account_id(), region, cert_id)


# The data planes share the control plane's body decoding instead of
# re-implementing it (empty, undecodable, or non-object body → {}).
parse_json_body = _parse_body


# --- IoT Jobs seam (consumed by iot_jobs_data.py) --------------------------
#
# The device plane is a wire adapter: it parses requests, shapes responses,
# and logs — it never touches _jobs / _job_executions, so the state machine
# has exactly one home. The mutating calls return `(execution, error)`, where
# `execution` is a COPY (a caller cannot corrupt the store through it) and
# `error` is a ready-to-return response tuple.


def jobs_pending_for_thing(thing_name: str) -> list[dict]:
    """Every non-terminal execution for a thing, oldest queued first (copies).

    Materializes first: a thing added to a CONTINUOUS job's target group after
    creation gets its execution here, on the read that first needs it.
    """
    _jobs_materialize_all()
    pending = [
        dict(execution)
        for execution in _job_executions.values()
        if execution["thingName"] == thing_name
        and execution["status"] in ("QUEUED", "IN_PROGRESS")
    ]
    pending.sort(key=lambda execution: execution["queuedAt"])
    return pending


def jobs_start_next_execution(
    thing_name: str, *, status_details: dict | None = None, peek: bool = False
) -> dict | None:
    """Hand a thing its next pending execution, or None when it is idle.

    A QUEUED execution moves to IN_PROGRESS (stamping `startedAt`, bumping
    `versionNumber`); with `peek=True` — GET of the `$next` sentinel — the
    execution is read without being started. An already IN_PROGRESS execution
    is re-handed unchanged, so a device that restarts mid-job resumes it.
    """
    pending = jobs_pending_for_thing(thing_name)
    if not pending:
        return None
    execution = _job_executions[(thing_name, pending[0]["jobId"])]
    if not peek and execution["status"] == "QUEUED":
        now = _jobs_now_ms()
        execution["status"] = "IN_PROGRESS"
        execution["startedAt"] = now
        execution["lastUpdatedAt"] = now
        execution["versionNumber"] += 1
        if status_details:
            execution["statusDetails"] = dict(status_details)
    return dict(execution)


def jobs_describe_execution(thing_name: str, job_id: str) -> dict | None:
    """One execution (a copy), materializing the job's targets first."""
    _jobs_materialize_executions(job_id)
    execution = _job_executions.get((thing_name, job_id))
    return None if execution is None else dict(execution)


def jobs_job_document(job_id: str) -> str:
    """The document a device should run for a job — ``{}`` if there is none."""
    job = _jobs.get(job_id)
    return (job or {}).get("document") or "{}"


def jobs_update_execution(
    thing_name: str,
    job_id: str,
    *,
    status,
    expected_version=None,
    status_details: dict | None = None,
) -> tuple:
    """Apply a device-reported status to an execution.

    Returns ``(execution copy, None)``, or ``(None, error)`` for every
    rejection: unknown execution (404), stale or non-numeric
    `expectedVersion`, an already-terminal execution, an unknown status, the
    illegal walk back to QUEUED, and the service-side statuses a device may
    not set. A successful update may complete the job.
    """
    _jobs_materialize_executions(job_id)
    execution = _job_executions.get((thing_name, job_id))
    if execution is None:
        return None, error_response_json(
            "ResourceNotFoundException",
            f"No job execution found for thing {thing_name} and job {job_id}",
            404,
        )
    error = _jobs_check_expected_version(
        execution, expected_version, "InvalidStateTransitionException"
    )
    if error:
        return None, error
    error = _jobs_reject_if_terminal(execution, "updated")
    if error:
        return None, error
    if not status or status not in _JOB_EXECUTION_STATUSES:
        return None, error_response_json(
            "InvalidRequestException",
            f"Invalid job execution status: {status!r}",
            400,
        )
    if status == "QUEUED":
        return None, error_response_json(
            "InvalidStateTransitionException",
            "A device cannot move a job execution back to QUEUED",
            409,
        )
    if status not in _DEVICE_SETTABLE_STATUSES:
        return None, error_response_json(
            "InvalidRequestException",
            f"A device cannot set status {status} via UpdateJobExecution; "
            "allowed statuses are IN_PROGRESS, SUCCEEDED, FAILED, and REJECTED",
            400,
        )
    now = _jobs_now_ms()
    execution["status"] = status
    if status_details:
        execution["statusDetails"] = dict(status_details)
    if status == "IN_PROGRESS" and execution.get("startedAt") is None:
        execution["startedAt"] = now
    execution["lastUpdatedAt"] = now
    execution["versionNumber"] += 1
    _jobs_maybe_complete(job_id)
    return dict(execution), None


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
# MQTT Broker — embedded MQTT 3.1.1 + 5.0 broker logic over WebSocket
# ===========================================================================
#
# The broker owns a small in-process pub/sub registry plus an MQTT framing
# layer used between the broker and WebSocket clients (per the AWS WS-MQTT
# subprotocol).
#
# Architecture (mirrors Transfer Family's shared SFTP listener):
#   Client → WebSocket (gateway port) → Bridge → in-memory pub/sub
#
# Multi-tenancy is enforced by transparent topic prefixing: every
# PUBLISH/SUBSCRIBE topic seen on the wire is internally prefixed with the
# caller's account_id and region before it hits the registry, and the prefix
# is stripped on outbound delivery.
#
# Protocol version
# ----------------
# The CONNECT packet's protocol level picks the wire format for the rest of
# the connection: 4 selects MQTT 3.1.1, 5 selects MQTT 5.0, anything else is
# refused with a 3.1.1 CONNACK carrying return code 0x01 (unacceptable
# protocol version) so the client sees a diagnosable answer instead of a
# mis-framed one. The level is per connection and per session — a 3.1.1
# publisher and a 5.0 subscriber talk to each other through the same
# registry, each seeing its own framing.
#
# MQTT 5 support is deliberately partial: enough that a real SDK connects,
# subscribes, publishes, receives and disconnects cleanly. Implemented are
# property blocks on every packet that carries one, all four fields of the
# subscription-options byte (QoS, No Local, Retain As Published, Retain
# Handling), v5 reason codes on CONNACK/SUBACK/PUBACK/UNSUBACK/DISCONNECT, an
# assigned client identifier, and pass-through of the PUBLISH properties a
# subscriber may echo back. Out of scope, and advertised as unavailable in
# CONNACK where the protocol has a flag for it: shared subscriptions, topic
# aliases, message expiry, flow control (receive maximum), subscription
# identifiers, and request/response semantics beyond forwarding the
# properties untouched. Properties this broker does not model are parsed and
# ignored, never echoed back.
#
# Two further limits are worth naming because the protocol has no flag to
# advertise them. A stored session keeps its topic filters but not the
# subscription options they arrived with, so a resumed subscription comes back
# at the defaults. And Session Expiry Interval is honoured as an interval —
# 0 means do not persist, 0xFFFFFFFF means never expire — but only when a
# session is looked at, on reconnect or when a QoS 1 message hunts for offline
# sessions to queue for; nothing reaps expired sessions on a timer.

_broker_logger = logging.getLogger("iot_broker")

# ---------------------------------------------------------------------------
# In-memory pub/sub registry
# ---------------------------------------------------------------------------

_subscriptions: dict[str, set["_Subscription"]] = {}
_connected_clients: dict[tuple[str, str, str], "_WSSession"] = {}
_persistent_sessions: dict[tuple[str, str, str], "_PersistentSessionState"] = {}
# Last connect/disconnect transition per (account, region, client id), as
# ``{"timestamp": <epoch ms>, "disconnectReason": <str|None>}``. Deliberately
# NOT a connected flag: liveness is read from ``_connected_clients``, which is
# the broker's own session registry, so the two can never disagree and a
# restart cannot leave a thing stuck reporting itself online.
_connectivity: dict[tuple[str, str, str], dict] = {}
_broker_lock = asyncio.Lock()

_SESSION_EXPIRY_SECONDS: int = int(os.environ.get("IOT_SESSION_EXPIRY_SECONDS", "3600"))
# MQTT 5 §3.1.2.11.2: 0xFFFFFFFF means the session never expires.
_SESSION_EXPIRY_NEVER = 0xFFFFFFFF
_MAX_QUEUED_MESSAGES = 1000


class _PersistentSessionState:
    __slots__ = ("subscriptions", "queued_messages", "created_at", "expiry_interval")

    def __init__(
        self,
        subscriptions: list[str],
        created_at: float,
        expiry_interval: int | None = None,
    ):
        self.subscriptions: list[str] = subscriptions
        self.queued_messages: list[tuple[str, bytes, int]] = []
        self.created_at: float = created_at
        # Seconds this session outlives its connection, as the client asked
        # for it in the MQTT 5 Session Expiry Interval. None means the client
        # named no interval (every 3.1.1 client, and a v5 client that omitted
        # the property), so the module-wide default applies.
        self.expiry_interval: int | None = expiry_interval


def _is_session_expired(session_state: _PersistentSessionState) -> bool:
    """Whether a stored session has outlived its expiry interval.

    Evaluated on access — when a client reconnects and when a QoS 1 message
    looks for offline sessions to queue for — not by a timer. An expired
    session is therefore never resumed and never queued for, but its entry
    survives in ``_persistent_sessions`` until something touches that key or
    the broker is reset. Reaping on a schedule would need a timer this broker
    deliberately does not run.
    """
    interval = session_state.expiry_interval
    if interval is None:
        interval = _SESSION_EXPIRY_SECONDS
    if interval >= _SESSION_EXPIRY_NEVER:
        return False
    return (time.time() - session_state.created_at) > interval


class _InFlightMessage:
    __slots__ = (
        "packet_id", "topic", "payload", "properties", "retain", "sent_at",
        "retransmit_count",
    )

    def __init__(self, packet_id: int, topic: str, payload: bytes, properties: bytes = b"",
                 retain: bool = False):
        self.packet_id = packet_id
        self.topic = topic
        self.payload = payload
        # Encoded MQTT 5 property block and the RETAIN flag the first attempt
        # went out with, so a retransmit repeats the original packet rather
        # than a stripped or reflagged one.
        self.properties = properties
        self.retain = retain
        self.sent_at = asyncio.get_event_loop().time()
        self.retransmit_count = 0


_RETRANSMIT_INTERVAL_SECONDS = int(os.environ.get("IOT_RETRANSMIT_SECONDS", "10"))


# MQTT 5 Retain Handling (§3.8.3.1), the one subscription option that is spent
# at subscribe time: send retained messages on every SUBSCRIBE, send them only
# when this subscription is new, or never send them. 0 is also what a 3.1.1
# SUBSCRIBE means, since it has no options byte.
RETAIN_HANDLING_SEND_ALWAYS = 0
RETAIN_HANDLING_SEND_IF_NEW = 1
RETAIN_HANDLING_SEND_NEVER = 2


class _Subscription:
    __slots__ = (
        "subscription_id",
        "filter_prefixed",
        "account_id",
        "region",
        "deliver",
        "granted_qos",
        "client_id",
        "no_local",
        "retain_as_published",
    )

    def __init__(
        self,
        filter_prefixed: str,
        account_id: str,
        region: str,
        deliver: Callable[[str, bytes, int], Awaitable[None]],
        granted_qos: int = 0,
        client_id: str | None = None,
        no_local: bool = False,
        retain_as_published: bool = False,
    ):
        self.subscription_id = uuid.uuid4().hex
        self.filter_prefixed = filter_prefixed
        self.account_id = account_id
        self.region = region
        self.deliver = deliver
        self.granted_qos = granted_qos
        # Owning MQTT client, when there is one. iot_data's HTTP publishes and
        # in-process subscribers have no client identity, which is what No
        # Local compares against — a subscriber with no identity can never be
        # the publisher.
        self.client_id = client_id
        # MQTT 5 subscription options (§3.8.3.1). Retain Handling is consumed
        # at subscribe time and so is not kept here.
        self.no_local = no_local
        self.retain_as_published = retain_as_published

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
        _connectivity.clear()


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


def _dispatch_rule_sqs(account_id: str, region: str, rule_name: str, spec: dict, event) -> None:
    from ministack.services import sqs as _sqs

    queue_url = spec.get("queueUrl") or ""
    if not queue_url:
        _broker_logger.warning(
            "IoT rule %s: sqs action has no queueUrl — skipped", rule_name
        )
        return
    body = event if isinstance(event, str) else json.dumps(event)
    if spec.get("useBase64"):
        # useBase64 encodes the message *body*: the consumer receives Base64 text
        # and has to decode it. It is not a transport hint.
        body = base64.b64encode(body.encode("utf-8")).decode("ascii")
    # roleArn is accepted and ignored, as everywhere else here — no IAM to fail.
    with request_scope(account_id, region):
        # _get_q matches the URL as stored and then by queue name, which is what
        # lets a rule written against localhost reach a queue created through a
        # different host alias, and refuses a URL naming another account rather
        # than silently resolving to a same-account queue of that name.
        try:
            queue = _sqs._get_q(queue_url)
            if queue["is_fifo"]:
                # AWS documents FIFO queues as unsupported for this action: the
                # rules engine is distributed, so it cannot promise the ordering
                # a FIFO queue exists to provide, and enqueuing anyway would need
                # a MessageGroupId we would have to invent.
                _broker_logger.warning(
                    "IoT rule %s: sqs target %s is a FIFO queue, which the AWS SQS "
                    "rule action does not support — nothing delivered",
                    rule_name,
                    queue_url,
                )
                return
            # Through SQS's own SendMessage body, so the queue's DelaySeconds and
            # MaximumMessageSize apply and the MessageId is minted the way every
            # other producer's is.
            result = _sqs._act_send_message({"MessageBody": body}, queue_url)
        except _sqs._Err as exc:
            # SQS raises in its wire vocabulary; the action collector and the
            # rule's errorAction want a plain exception, so translate at the
            # boundary as stepfunctions does for the same call.
            raise RuntimeError(
                f"SQS SendMessage to {queue_url} failed: {exc.code}: {exc.message}"
            ) from exc
    _broker_logger.debug(
        "IoT rule %s → SQS %s (%s)", rule_name, queue_url, result["MessageId"]
    )


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
    elif "sqs" in action:
        _dispatch_rule_sqs(account_id, region, rule_name, action["sqs"] or {}, event)
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
    properties: bytes = b"",
    client_id: str | None = None,
) -> int:
    """Deliver a message to every matching subscriber.

    ``properties`` is an encoded MQTT 5 property block forwarded verbatim to
    v5 subscribers (empty for a 3.1.1 publisher or an HTTP publish). Returns
    how many recipients the message reached — subscribers delivered to plus
    offline persistent sessions it was queued for — which is what an MQTT 5
    PUBACK reports as Success versus No matching subscribers.

    ``client_id`` names the publishing MQTT client, which is what a No Local
    subscription is defined against (§3.8.3.1): that subscriber does not want
    its own messages back. A publish with no client identity — iot_data's HTTP
    Publish, a rule, an in-process caller — matches no subscriber's identity
    and so is delivered to all of them.
    """
    delivered = 0
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
        return delivered

    scoped = _scoped_topic(account_id, region, topic)

    if retain:
        if not payload:
            _retained.pop(scoped, None)
        else:
            _retained[scoped] = _RetainedMessage(scoped, payload, qos, properties)

    async with _broker_lock:
        subs = [s for sset in _subscriptions.values() for s in sset]

    token = _delivery_properties.set(properties)
    try:
        for sub in subs:
            if sub.account_id != account_id or sub.region != region:
                continue
            if sub.no_local and client_id is not None and sub.client_id == client_id:
                continue
            if _topic_matches(sub.filter_prefixed, scoped):
                # Retain As Published forwards the publisher's flag; without
                # it the flag is cleared, so a subscriber can tell a live
                # message from a retained one (§3.3.1.3).
                retain_token = _delivery_retain.set(
                    retain if sub.retain_as_published else False
                )
                try:
                    effective_qos = min(qos, sub.granted_qos)
                    await sub.deliver(
                        _unscope_topic(sub.account_id, sub.region, scoped),
                        payload,
                        effective_qos,
                    )
                    delivered += 1
                except Exception:
                    _broker_logger.exception("IoT broker: subscriber %s delivery failed", sub.subscription_id)
                finally:
                    _delivery_retain.reset(retain_token)
    finally:
        _delivery_properties.reset(token)

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
                    delivered += 1
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

    return delivered


async def broker_subscribe(
    account_id: str,
    region: str,
    topic_filter: str,
    callback: Callable[[str, bytes, int], Awaitable[None]],
    granted_qos: int = 0,
    client_id: str | None = None,
    no_local: bool = False,
    retain_as_published: bool = False,
    retain_handling: int = RETAIN_HANDLING_SEND_ALWAYS,
) -> str:
    """Register a subscriber and hand it any matching retained message.

    The last four arguments are the MQTT 5 subscription options (§3.8.3.1);
    their defaults are what an MQTT 3.1.1 SUBSCRIBE means, so an existing
    caller keeps its behaviour. ``retain_handling`` is acted on here and
    nowhere else — it only decides whether this call replays retained
    messages — while No Local and Retain As Published are stored on the
    subscription because they apply to every later delivery.
    """
    filter_prefixed = _scoped_topic(account_id, region, topic_filter)
    sub = _Subscription(
        filter_prefixed, account_id, region, callback, granted_qos,
        client_id=client_id, no_local=no_local,
        retain_as_published=retain_as_published,
    )
    async with _broker_lock:
        existing_subscribers = _subscriptions.setdefault(filter_prefixed, set())
        # "Existing" is per client: whether *this* client already held the
        # filter, which is what Retain Handling 1 asks about. A subscriber
        # with no client identity has no subscription to re-establish.
        already_subscribed = client_id is not None and any(
            s.client_id == client_id for s in existing_subscribers
        )
        existing_subscribers.add(sub)
        send_retained = retain_handling == RETAIN_HANDLING_SEND_ALWAYS or (
            retain_handling == RETAIN_HANDLING_SEND_IF_NEW and not already_subscribed
        )
        has_wildcard = "+" in topic_filter or "#" in topic_filter
        if send_retained and not has_wildcard:
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
        token = _delivery_properties.set(r.properties)
        # §3.3.1.3: a message sent because a subscription was established
        # carries RETAIN 1 whatever the subscription's Retain As Published
        # says — that flag is how the client tells this apart from live
        # traffic on the same topic.
        retain_token = _delivery_retain.set(True)
        try:
            await sub.deliver(
                _unscope_topic(account_id, region, r.topic), r.payload, r.qos
            )
        except Exception:
            _broker_logger.exception("IoT broker: retained-message delivery failed")
        finally:
            _delivery_retain.reset(retain_token)
            _delivery_properties.reset(token)

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
    _connectivity.clear()


# ---------------------------------------------------------------------------
# Connected-client registry & duplicate detection
# ---------------------------------------------------------------------------


def _now_epoch_millis() -> int:
    return int(time.time() * 1000)


def _register_client(
    account_id: str,
    region: str,
    client_id: str,
    session: "_WSSession",
    superseded_reason: str | None = None,
) -> None:
    """Record a live session, and with it the transition that started it.

    ``superseded_reason`` is the reason the connect itself ended a previous
    session — only a takeover can do that, and only DUPLICATE_CLIENTID names
    it. Carrying it onto the new record is what makes the reason observable
    at all: the eviction and this registration are the same transition, so
    overwriting it here (as this did) left a value no caller could ever read.
    An ordinary reconnect passes None and clears the older reason, because
    that disconnect was a transition of its own that this connect supersedes.
    """
    key = (account_id, region, client_id)
    _connected_clients[key] = session
    _connectivity[key] = {
        "timestamp": _now_epoch_millis(),
        "disconnectReason": superseded_reason,
    }


def _deregister_client(
    account_id: str, region: str, client_id: str, session: "_WSSession | None" = None
) -> None:
    """Drop a session from the registry and record its disconnect.

    ``session`` identifies the caller so a session that has already been
    replaced cannot deregister its successor. A client that reconnects under
    the same client id is taken over: the old session is cleaned up first, and
    its socket close then drives its own handler through cleanup a second time.
    Without this guard that late second pass would evict the live session from
    the registry and backdate its connectivity to the moment the loser died.
    """
    key = (account_id, region, client_id)
    if session is not None and _connected_clients.get(key) is not session:
        return
    _connected_clients.pop(key, None)
    _connectivity[key] = {
        "timestamp": _now_epoch_millis(),
        "disconnectReason": (
            session.disconnect_reason() if session is not None else "CONNECTION_LOST"
        ),
    }


async def _force_disconnect_duplicate(
    account_id: str, region: str, client_id: str
) -> str | None:
    """Evict a session that the incoming one is taking over.

    Returns DUPLICATE_CLIENTID when there was one to evict, so the caller can
    carry that reason onto the record it is about to write; otherwise None.
    """
    key = (account_id, region, client_id)
    existing = _connected_clients.get(key)
    if existing is None:
        return None
    _broker_logger.info("IoT broker: duplicate client_id=%s, forcing old connection closed", client_id)
    existing._forced_disconnect_reason = "DUPLICATE_CLIENTID"
    if existing.protocol_version == MQTT_5:
        # An MQTT 5 client is told why it lost the connection; 3.1.1 has
        # no server-initiated DISCONNECT, so it just sees the close.
        try:
            await existing.send_bytes(_make_disconnect(RC5_SESSION_TAKEN_OVER))
        except Exception:
            pass
    try:
        await existing._send({"type": "websocket.close", "code": 1000})
    except Exception:
        pass
    await existing.cleanup()
    _connected_clients.pop(key, None)
    return "DUPLICATE_CLIENTID"


# ---------------------------------------------------------------------------
# MQTT 3.1.1 / 5.0 frame codec
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

# Protocol levels carried by the CONNECT variable header.
MQTT_311 = 4
MQTT_5 = 5

# MQTT 3.1.1 CONNACK return codes (§3.2.2.3).
CONNACK_311_ACCEPTED = 0x00
CONNACK_311_UNACCEPTABLE_PROTOCOL_VERSION = 0x01
CONNACK_311_NOT_AUTHORIZED = 0x05

# MQTT 5 reason codes, only the ones this broker produces (§2.4). Success and
# Normal disconnection share the value 0x00.
RC5_SUCCESS = 0x00
RC5_NO_MATCHING_SUBSCRIBERS = 0x10
RC5_NO_SUBSCRIPTION_EXISTED = 0x11
RC5_MALFORMED_PACKET = 0x81
RC5_NOT_AUTHORIZED = 0x87
RC5_SESSION_TAKEN_OVER = 0x8E
RC5_TOPIC_NAME_INVALID = 0x90

# Layout of the MQTT 5 SUBSCRIBE options byte (§3.8.3.1). MQTT 3.1.1 sends a
# bare QoS byte with the upper bits reserved zero, so the same masks read both
# and a 3.1.1 subscription comes out with every option off.
SUB_OPT_QOS_MASK = 0x03
SUB_OPT_NO_LOCAL = 0x04
SUB_OPT_RETAIN_AS_PUBLISHED = 0x08
SUB_OPT_RETAIN_HANDLING_SHIFT = 4
SUB_OPT_RETAIN_HANDLING_MASK = 0x03

# MQTT 5 property identifiers (§2.2.2.2).
PROP_PAYLOAD_FORMAT_INDICATOR = 0x01
PROP_MESSAGE_EXPIRY_INTERVAL = 0x02
PROP_CONTENT_TYPE = 0x03
PROP_RESPONSE_TOPIC = 0x08
PROP_CORRELATION_DATA = 0x09
PROP_SUBSCRIPTION_IDENTIFIER = 0x0B
PROP_SESSION_EXPIRY_INTERVAL = 0x11
PROP_ASSIGNED_CLIENT_IDENTIFIER = 0x12
PROP_SERVER_KEEP_ALIVE = 0x13
PROP_AUTHENTICATION_METHOD = 0x15
PROP_AUTHENTICATION_DATA = 0x16
PROP_REQUEST_PROBLEM_INFORMATION = 0x17
PROP_WILL_DELAY_INTERVAL = 0x18
PROP_REQUEST_RESPONSE_INFORMATION = 0x19
PROP_RESPONSE_INFORMATION = 0x1A
PROP_SERVER_REFERENCE = 0x1C
PROP_REASON_STRING = 0x1F
PROP_RECEIVE_MAXIMUM = 0x21
PROP_TOPIC_ALIAS_MAXIMUM = 0x22
PROP_TOPIC_ALIAS = 0x23
PROP_MAXIMUM_QOS = 0x24
PROP_RETAIN_AVAILABLE = 0x25
PROP_USER_PROPERTY = 0x26
PROP_MAXIMUM_PACKET_SIZE = 0x27
PROP_WILDCARD_SUBSCRIPTION_AVAILABLE = 0x28
PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE = 0x29
PROP_SHARED_SUBSCRIPTION_AVAILABLE = 0x2A

# Wire type of every property identifier the protocol defines: "byte",
# "u16", "u32", "vbi" (variable byte integer), "str", "bin" and "pair" (a
# UTF-8 string pair). The table is complete even for properties the broker
# has no use for, because knowing a property's *width* is what lets the
# parser step over it — see _decode_properties.
_PROPERTY_TYPES: dict[int, str] = {
    PROP_PAYLOAD_FORMAT_INDICATOR: "byte",
    PROP_MESSAGE_EXPIRY_INTERVAL: "u32",
    PROP_CONTENT_TYPE: "str",
    PROP_RESPONSE_TOPIC: "str",
    PROP_CORRELATION_DATA: "bin",
    PROP_SUBSCRIPTION_IDENTIFIER: "vbi",
    PROP_SESSION_EXPIRY_INTERVAL: "u32",
    PROP_ASSIGNED_CLIENT_IDENTIFIER: "str",
    PROP_SERVER_KEEP_ALIVE: "u16",
    PROP_AUTHENTICATION_METHOD: "str",
    PROP_AUTHENTICATION_DATA: "bin",
    PROP_REQUEST_PROBLEM_INFORMATION: "byte",
    PROP_WILL_DELAY_INTERVAL: "u32",
    PROP_REQUEST_RESPONSE_INFORMATION: "byte",
    PROP_RESPONSE_INFORMATION: "str",
    PROP_SERVER_REFERENCE: "str",
    PROP_REASON_STRING: "str",
    PROP_RECEIVE_MAXIMUM: "u16",
    PROP_TOPIC_ALIAS_MAXIMUM: "u16",
    PROP_TOPIC_ALIAS: "u16",
    PROP_MAXIMUM_QOS: "byte",
    PROP_RETAIN_AVAILABLE: "byte",
    PROP_USER_PROPERTY: "pair",
    PROP_MAXIMUM_PACKET_SIZE: "u32",
    PROP_WILDCARD_SUBSCRIPTION_AVAILABLE: "byte",
    PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE: "byte",
    PROP_SHARED_SUBSCRIPTION_AVAILABLE: "byte",
}

# PUBLISH properties handed on to subscribers unchanged. §3.3.2.3 requires a
# server to forward these, and a request/response client depends on getting
# its own response topic and correlation data back. Everything else a
# publisher sends (topic alias, subscription identifier, anything this broker
# does not model) is dropped rather than echoed.
_FORWARDED_PUBLISH_PROPERTIES = frozenset({
    PROP_PAYLOAD_FORMAT_INDICATOR,
    PROP_MESSAGE_EXPIRY_INTERVAL,
    PROP_CONTENT_TYPE,
    PROP_RESPONSE_TOPIC,
    PROP_CORRELATION_DATA,
    PROP_USER_PROPERTY,
})

# MQTT 5 PUBLISH properties ride a contextvar rather than the subscriber
# callback signature. ``deliver(topic, payload, qos)`` is the broker's
# extension point — iot_data, persistent-session replay and tests all supply
# their own three-argument callbacks — and only an MQTT 5 session has any use
# for properties. broker_publish sets this around each delivery; every other
# caller sees the empty default and sends an empty property block.
_delivery_properties: contextvars.ContextVar[bytes] = contextvars.ContextVar(
    "_iot_delivery_properties", default=b""
)

# The RETAIN flag one delivery goes out with, carried the same way and for the
# same reason. It is resolved per subscriber rather than per message, because
# Retain As Published makes it a property of the subscription: two subscribers
# to one publish can legitimately see different flags.
_delivery_retain: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "_iot_delivery_retain", default=False
)


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


def _read_binary(buf: bytes, offset: int) -> tuple[bytes, int]:
    if offset + 2 > len(buf):
        raise ValueError("Truncated binary length")
    n = struct.unpack_from("!H", buf, offset)[0]
    offset += 2
    if offset + n > len(buf):
        raise ValueError("Truncated binary body")
    return buf[offset:offset + n], offset + n


def _encode_binary(b: bytes) -> bytes:
    return struct.pack("!H", len(b)) + b


def _decode_properties(buf: bytes, offset: int) -> tuple[list[tuple[int, object]], int]:
    """Read one MQTT 5 property block, returning (properties, new_offset).

    Properties keep wire order and duplicates (User Property is allowed to
    repeat), so the result is a list of ``(identifier, value)`` rather than a
    dict. A property whose identifier is not in ``_PROPERTY_TYPES`` cannot be
    stepped over — its width is unknown — so parsing stops there and the
    remainder of the block is skipped wholesale. The block's own length is
    always honoured, which keeps the packet parse in sync either way.

    Values are read out of a slice that ends where the block's declared length
    ends, never out of the whole packet. A property whose value runs past that
    point raises ``ValueError`` like the sibling field readers, instead of
    quietly consuming the field that follows the block (a PUBLISH payload, a
    will topic) or walking off the end of the packet.
    """
    length, offset = _decode_remaining_length(buf, offset)
    end = offset + length
    if end > len(buf):
        raise ValueError("Truncated property block")
    block = buf[offset:end]
    props: list[tuple[int, object]] = []
    pos = 0
    while pos < len(block):
        identifier = block[pos]
        pos += 1
        kind = _PROPERTY_TYPES.get(identifier)
        if kind is None:
            _broker_logger.warning(
                "IoT broker: unknown MQTT 5 property 0x%02X — skipping rest of block",
                identifier,
            )
            break
        if kind == "byte":
            if pos + 1 > len(block):
                raise ValueError("Truncated property value")
            value: object = block[pos]
            pos += 1
        elif kind == "u16":
            if pos + 2 > len(block):
                raise ValueError("Truncated property value")
            value = struct.unpack_from("!H", block, pos)[0]
            pos += 2
        elif kind == "u32":
            if pos + 4 > len(block):
                raise ValueError("Truncated property value")
            value = struct.unpack_from("!I", block, pos)[0]
            pos += 4
        elif kind == "vbi":
            value, pos = _decode_remaining_length(block, pos)
        elif kind == "str":
            value, pos = _read_string(block, pos)
        elif kind == "bin":
            value, pos = _read_binary(block, pos)
        else:  # pair
            name, pos = _read_string(block, pos)
            pair_value, pos = _read_string(block, pos)
            value = (name, pair_value)
        props.append((identifier, value))
    return props, end


def _encode_properties(props: list[tuple[int, object]] | None) -> bytes:
    """Encode a property list, length prefix included. Empty block is ``b'\\x00'``."""
    body = bytearray()
    for identifier, value in props or []:
        kind = _PROPERTY_TYPES[identifier]
        body.append(identifier)
        if kind == "byte":
            body.append(int(value))  # type: ignore[arg-type]
        elif kind == "u16":
            body += struct.pack("!H", int(value))  # type: ignore[arg-type]
        elif kind == "u32":
            body += struct.pack("!I", int(value))  # type: ignore[arg-type]
        elif kind == "vbi":
            body += _encode_remaining_length(int(value))  # type: ignore[arg-type]
        elif kind == "str":
            body += _encode_string(str(value))
        elif kind == "bin":
            body += _encode_binary(bytes(value))  # type: ignore[arg-type]
        else:  # pair
            name, pair_value = value  # type: ignore[misc]
            body += _encode_string(name) + _encode_string(pair_value)
    return _encode_remaining_length(len(body)) + bytes(body)


def _property_value(props: list[tuple[int, object]], identifier: int, default: object) -> object:
    for prop_id, value in props:
        if prop_id == identifier:
            return value
    return default


def _forwardable_publish_properties(props: list[tuple[int, object]]) -> bytes:
    """Encoded property block a subscriber receives for an incoming PUBLISH."""
    kept = [p for p in props if p[0] in _FORWARDED_PUBLISH_PROPERTIES]
    if not kept:
        return b""
    return _encode_properties(kept)


def _make_connack(
    return_code: int = 0,
    session_present: bool = False,
    protocol_version: int = MQTT_311,
    properties: list[tuple[int, object]] | None = None,
) -> bytes:
    flags = 1 if session_present else 0
    body = bytes([flags, return_code])
    if protocol_version == MQTT_5:
        body += _encode_properties(properties)
    return bytes([PKT_CONNACK << 4]) + _encode_remaining_length(len(body)) + body


def _make_publish(topic: str, payload: bytes, qos: int = 0, packet_id: int | None = None,
                  retain: bool = False, dup: bool = False,
                  properties: bytes | None = None) -> bytes:
    """Build a PUBLISH. ``properties`` is an already-encoded MQTT 5 property
    block; ``None`` means MQTT 3.1.1, which has no such field."""
    fixed = (PKT_PUBLISH << 4) | (qos << 1) | (0x08 if dup else 0) | (0x01 if retain else 0)
    body = _encode_string(topic)
    if qos > 0:
        if packet_id is None:
            packet_id = 1
        body += struct.pack("!H", packet_id)
    if properties is not None:
        body += properties or b"\x00"
    body += payload
    return bytes([fixed]) + _encode_remaining_length(len(body)) + body


def _make_puback(packet_id: int, reason_code: int | None = None,
                 properties: list[tuple[int, object]] | None = None) -> bytes:
    """Build a PUBACK. ``reason_code`` is MQTT 5 only; ``None`` yields the
    bare 3.1.1 packet."""
    body = struct.pack("!H", packet_id)
    if reason_code is not None:
        body += bytes([reason_code]) + _encode_properties(properties)
    return bytes([PKT_PUBACK << 4]) + _encode_remaining_length(len(body)) + body


def _make_suback(packet_id: int, granted_qos: list[int],
                 protocol_version: int = MQTT_311,
                 properties: list[tuple[int, object]] | None = None) -> bytes:
    body = struct.pack("!H", packet_id)
    if protocol_version == MQTT_5:
        body += _encode_properties(properties)
    body += bytes(granted_qos)
    return bytes([PKT_SUBACK << 4]) + _encode_remaining_length(len(body)) + body


def _make_unsuback(packet_id: int, reason_codes: list[int] | None = None,
                   properties: list[tuple[int, object]] | None = None) -> bytes:
    """Build an UNSUBACK. ``reason_codes`` is MQTT 5 only — 3.1.1's UNSUBACK
    carries the packet identifier and nothing else."""
    body = struct.pack("!H", packet_id)
    if reason_codes is not None:
        body += _encode_properties(properties) + bytes(reason_codes)
    return bytes([PKT_UNSUBACK << 4]) + _encode_remaining_length(len(body)) + body


def _make_disconnect(reason_code: int,
                     properties: list[tuple[int, object]] | None = None) -> bytes:
    """Build a server-initiated MQTT 5 DISCONNECT. 3.1.1 has no such packet —
    a 3.1.1 server just closes the transport."""
    body = bytes([reason_code]) + _encode_properties(properties)
    return bytes([PKT_DISCONNECT << 4]) + _encode_remaining_length(len(body)) + body


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
        # Negotiated in CONNECT; every packet built for this session is framed
        # for this version.
        self.protocol_version: int = MQTT_311
        # 3.1.1 derives both from one flag; MQTT 5 splits them into Clean
        # Start (resume or not) and Session Expiry Interval (keep or not).
        self._resume_session: bool = False
        self._persist_session: bool = False
        self._session_expiry_interval: int | None = None
        self._in_flight: dict[int, _InFlightMessage] = {}
        self._retransmit_task: asyncio.Task | None = None
        self._will_topic: str | None = None
        self._will_message: bytes | None = None
        self._will_qos: int = 0
        self._will_retain: bool = False
        self._will_properties: bytes = b""
        self._graceful_disconnect: bool = False
        # Set when the broker itself ends the session, overriding whatever the
        # socket then looks like from the client's side.
        self._forced_disconnect_reason: str | None = None

    def disconnect_reason(self) -> str:
        """Why this session ended, in AWS's disconnect-reason vocabulary.

        Only the three the broker can actually tell apart are ever reported: a
        DISCONNECT packet (``CLIENT_INITIATED_DISCONNECT``), a takeover by a
        second connection using the same client id (``DUPLICATE_CLIENTID``),
        and everything else — a transport that went away (``CONNECTION_LOST``).
        AWS's remaining reasons name machinery this broker does not have
        (throttling, credential expiry, keep-alive enforcement), so reporting
        one would be a guess dressed up as a fact.
        """
        if self._forced_disconnect_reason:
            return self._forced_disconnect_reason
        return (
            "CLIENT_INITIATED_DISCONNECT"
            if self._graceful_disconnect
            else "CONNECTION_LOST"
        )

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
                            _make_publish(
                                msg.topic, msg.payload, qos=1, packet_id=pid, dup=True,
                                retain=msg.retain,
                                properties=self._publish_properties(msg.properties),
                            )
                        )
        except asyncio.CancelledError:
            pass

    async def send_bytes(self, b: bytes) -> None:
        async with self._send_lock:
            await self._send({"type": "websocket.send", "bytes": b})

    def _publish_properties(self, properties: bytes) -> bytes | None:
        """Property block for an outbound PUBLISH, or None on a 3.1.1 session.

        This is where cross-version delivery is resolved: a 3.1.1 subscriber
        never sees the field, and a v5 subscriber always gets one — empty when
        the publisher was 3.1.1 or sent nothing forwardable.
        """
        if self.protocol_version != MQTT_5:
            return None
        return properties or b"\x00"

    async def deliver_to_client(self, topic: str, payload: bytes, qos: int) -> None:
        properties = _delivery_properties.get()
        retain = _delivery_retain.get()
        if qos == 0:
            await self.send_bytes(
                _make_publish(topic, payload, qos=0, retain=retain,
                              properties=self._publish_properties(properties))
            )
        else:
            pid = self._alloc_packet_id()
            self._in_flight[pid] = _InFlightMessage(pid, topic, payload, properties, retain)
            await self.send_bytes(
                _make_publish(topic, payload, qos=1, packet_id=pid, retain=retain,
                              properties=self._publish_properties(properties))
            )
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

    def _connack_properties(self, assigned_client_id: str | None) -> list[tuple[int, object]]:
        """CONNACK properties for an MQTT 5 session.

        The three "available" flags set to 0 are the protocol's own way of
        telling a client not to use a feature this broker does not implement,
        which is why advertising them beats staying silent (their defaults all
        mean "available"). Topic Alias Maximum 0 does the same for aliases.
        """
        props: list[tuple[int, object]] = []
        if assigned_client_id:
            props.append((PROP_ASSIGNED_CLIENT_IDENTIFIER, assigned_client_id))
        props += [
            (PROP_MAXIMUM_QOS, 1),
            (PROP_RETAIN_AVAILABLE, 1),
            # AWS IoT caps Maximum Packet Size at 128 KB and never advertises
            # more; clamp the WS transport frame buffer down to it so a client
            # sized off this CONNACK never sends a packet real AWS would reject.
            (PROP_MAXIMUM_PACKET_SIZE, min(_max_frame_buffer_bytes(), 128 * 1024)),
            (PROP_TOPIC_ALIAS_MAXIMUM, 0),
            (PROP_WILDCARD_SUBSCRIPTION_AVAILABLE, 1),
            (PROP_SUBSCRIPTION_IDENTIFIER_AVAILABLE, 0),
            (PROP_SHARED_SUBSCRIPTION_AVAILABLE, 0),
        ]
        return props

    async def _send_connack(
        self, session_present: bool = False, assigned_client_id: str | None = None
    ) -> None:
        """Accept the connection, framed for the negotiated protocol version."""
        if self.protocol_version == MQTT_5:
            await self.send_bytes(
                _make_connack(
                    return_code=RC5_SUCCESS,
                    session_present=session_present,
                    protocol_version=MQTT_5,
                    properties=self._connack_properties(assigned_client_id),
                )
            )
        else:
            await self.send_bytes(
                _make_connack(
                    return_code=CONNACK_311_ACCEPTED, session_present=session_present
                )
            )

    async def _reject_malformed_packet(self, pkt_type: int, exc: Exception) -> None:
        """Answer a packet this broker could not parse, then let the caller close.

        MQTT 5 §4.13.1 requires a server that detects a Malformed Packet to
        close the connection, and to say why first: in a CONNACK when the
        CONNECT itself is what failed and no CONNACK has gone out yet, in a
        DISCONNECT once the connection is established. Both carry reason code
        0x81. MQTT 3.1.1 has no CONNACK return code for a malformed packet and
        no server-sent DISCONNECT at all, so there the connection just closes,
        which is what §4.8 prescribes.
        """
        _broker_logger.warning(
            "IoT broker: malformed MQTT packet (type %d) — closing connection: %s",
            pkt_type,
            exc,
        )
        if self.protocol_version != MQTT_5:
            return
        packet = (
            _make_connack(return_code=RC5_MALFORMED_PACKET, protocol_version=MQTT_5)
            if pkt_type == PKT_CONNECT
            else _make_disconnect(RC5_MALFORMED_PACKET)
        )
        try:
            await self.send_bytes(packet)
        except Exception:
            # The transport is going away regardless; a client that already
            # hung up must not turn into an unhandled error here.
            _broker_logger.debug(
                "IoT broker: malformed-packet reason code not delivered", exc_info=True
            )

    async def handle_packet(self, pkt_type: int, flags: int, body: bytes) -> bool:
        """Dispatch one packet. Returns False when the connection must close.

        Every field reader in this codec raises on truncation rather than
        reading past its own bounds, so a packet the client mis-framed lands
        here as an exception instead of as silently wrong values. Catching it
        at the one dispatch point is what turns it into the protocol's own
        answer — a reason code and a close — for any transport driving this
        session, rather than an exception the caller has to know about.
        """
        try:
            return await self._dispatch_packet(pkt_type, flags, body)
        except (ValueError, IndexError, struct.error) as exc:
            await self._reject_malformed_packet(pkt_type, exc)
            return False

    async def _dispatch_packet(self, pkt_type: int, flags: int, body: bytes) -> bool:
        if pkt_type == PKT_CONNECT:
            off = 0
            _proto_name, off = _read_string(body, off)
            if off >= len(body):
                await self.send_bytes(_make_connack(return_code=CONNACK_311_ACCEPTED))
                return True
            protocol_level = body[off]
            off += 1
            if protocol_level not in (MQTT_311, MQTT_5):
                # The client asked for a version this broker cannot frame, so
                # the refusal goes out in the 3.1.1 shape: it is the one form
                # whose second body byte every MQTT decoder reads as a return
                # code, which turns an otherwise silent decode failure into a
                # diagnosable "unacceptable protocol version".
                _broker_logger.warning(
                    "IoT broker: CONNECT with unsupported protocol level %d refused",
                    protocol_level,
                )
                await self.send_bytes(
                    _make_connack(return_code=CONNACK_311_UNACCEPTABLE_PROTOCOL_VERSION)
                )
                return False
            self.protocol_version = protocol_level
            is_v5 = protocol_level == MQTT_5

            if off >= len(body):
                await self._send_connack()
                return True
            connect_flags = body[off]
            off += 1
            off += 2  # Keep Alive

            will_flag = bool(connect_flags & 0x04)
            will_qos = (connect_flags >> 3) & 0x03
            will_retain = bool(connect_flags & 0x20)
            # Bit 1 is Clean Session in 3.1.1 and Clean Start in MQTT 5.
            clean_start = bool(connect_flags & 0x02)

            session_expiry = 0
            if is_v5:
                connect_props, off = _decode_properties(body, off)
                session_expiry = int(
                    _property_value(connect_props, PROP_SESSION_EXPIRY_INTERVAL, 0)
                )

            # 3.1.1 ties resumption and retention to the one flag. MQTT 5
            # separates them: Clean Start decides whether an existing session
            # is picked up, Session Expiry Interval whether this one outlives
            # the connection and for how long.
            self._resume_session = not clean_start
            self._persist_session = session_expiry > 0 if is_v5 else not clean_start
            # The interval the client asked for, kept so a stored session
            # expires on the client's terms rather than on the module-wide
            # default. A 3.1.1 client names no interval and gets the default.
            self._session_expiry_interval = session_expiry if is_v5 else None

            if off < len(body):
                client_id, off = _read_string(body, off)
            else:
                client_id = ""
            assigned_client_id = None
            if not client_id:
                client_id = uuid.uuid4().hex
                # MQTT 5 requires the server to hand back the identifier it
                # made up; a 3.1.1 client is never told and cannot ask.
                assigned_client_id = client_id
            self._client_id = client_id

            if will_flag:
                will_props: list[tuple[int, object]] = []
                if is_v5:
                    # Will properties precede the will topic. Will Delay
                    # Interval is parsed and ignored — the will goes out as
                    # soon as the connection drops.
                    will_props, off = _decode_properties(body, off)
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
                self._will_properties = _forwardable_publish_properties(will_props)
            else:
                self._will_topic = None
                self._will_message = None
                self._will_qos = 0
                self._will_retain = False
                self._will_properties = b""

            self._graceful_disconnect = False
            self._forced_disconnect_reason = None
            takeover_reason = await _force_disconnect_duplicate(
                self.account_id, self.region, self._client_id
            )
            _register_client(
                self.account_id, self.region, self._client_id, self, takeover_reason
            )

            session_key = (self.account_id, self.region, self._client_id)
            session_present = False

            if not self._resume_session:
                _persistent_sessions.pop(session_key, None)
            else:
                existing_ps = _persistent_sessions.get(session_key)
                if existing_ps is not None and not _is_session_expired(existing_ps):
                    session_present = True
                    for topic_filter in existing_ps.subscriptions:
                        # A stored session keeps its topic filters, not the
                        # subscription options they were made with: those live
                        # on the connection that sent them, so a resumed
                        # subscription comes back with the defaults.
                        sid = await broker_subscribe(
                            self.account_id,
                            self.region,
                            topic_filter,
                            self.deliver_to_client,
                            1,
                            client_id=self._client_id or None,
                        )
                        self._sub_ids.append(sid)
                        self._sub_filters[sid] = topic_filter
                        self._sub_granted_qos[sid] = 1
                    await self._send_connack(
                        session_present=True, assigned_client_id=assigned_client_id
                    )
                    queued = existing_ps.queued_messages[:]
                    existing_ps.queued_messages.clear()
                    for q_topic, q_payload, q_qos in queued:
                        await self.deliver_to_client(q_topic, q_payload, q_qos)
                    return True
                else:
                    _persistent_sessions[session_key] = _PersistentSessionState(
                        subscriptions=[],
                        created_at=time.time(),
                        expiry_interval=self._session_expiry_interval,
                    )

            await self._send_connack(
                session_present=session_present, assigned_client_id=assigned_client_id
            )
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
            properties = b""
            if self.protocol_version == MQTT_5:
                publish_props, off = _decode_properties(body, off)
                properties = _forwardable_publish_properties(publish_props)
            if not _validate_publish_topic(topic):
                _broker_logger.warning("IoT broker: PUBLISH rejected — invalid topic: %r", topic)
                if self.protocol_version == MQTT_5:
                    await self.send_bytes(_make_disconnect(RC5_TOPIC_NAME_INVALID))
                return False
            payload = body[off:]
            delivered = await broker_publish(
                self.account_id,
                self.region,
                topic,
                payload,
                qos=qos,
                retain=retain,
                properties=properties,
                client_id=self._client_id or None,
            )
            if qos == 1 and packet_id is not None:
                if self.protocol_version == MQTT_5:
                    reason = RC5_SUCCESS if delivered else RC5_NO_MATCHING_SUBSCRIBERS
                    await self.send_bytes(_make_puback(packet_id, reason_code=reason))
                else:
                    await self.send_bytes(_make_puback(packet_id))
            return True

        if pkt_type == PKT_SUBSCRIBE:
            packet_id = struct.unpack_from("!H", body, 0)[0]
            off = 2
            if self.protocol_version == MQTT_5:
                # Subscription Identifier lives here; the broker advertises it
                # as unavailable in CONNACK and ignores it if sent anyway.
                _subscribe_props, off = _decode_properties(body, off)
            granted = []
            while off < len(body):
                topic, off = _read_string(body, off)
                # MQTT 5 replaces 3.1.1's bare QoS byte with a subscription
                # options byte: the low two bits are the QoS, the rest are No
                # Local, Retain As Published and Retain Handling. In 3.1.1 the
                # upper bits are reserved zero, so reading the same byte the
                # same way leaves every option off.
                if off >= len(body):
                    raise ValueError("SUBSCRIBE topic filter without options byte")
                subscription_options = body[off]
                off += 1
                granted_qos = min(subscription_options & SUB_OPT_QOS_MASK, 1)
                # A granted QoS doubles as an MQTT 5 SUBACK reason code: 0x00
                # and 0x01 mean the same thing in both versions.
                granted.append(granted_qos)
                sid = await broker_subscribe(
                    self.account_id,
                    self.region,
                    topic,
                    self.deliver_to_client,
                    granted_qos,
                    client_id=self._client_id or None,
                    no_local=bool(subscription_options & SUB_OPT_NO_LOCAL),
                    retain_as_published=bool(
                        subscription_options & SUB_OPT_RETAIN_AS_PUBLISHED
                    ),
                    retain_handling=(
                        subscription_options >> SUB_OPT_RETAIN_HANDLING_SHIFT
                    ) & SUB_OPT_RETAIN_HANDLING_MASK,
                )
                self._sub_ids.append(sid)
                self._sub_filters[sid] = topic
                self._sub_granted_qos[sid] = granted_qos
            await self.send_bytes(
                _make_suback(packet_id, granted, protocol_version=self.protocol_version)
            )
            return True

        if pkt_type == PKT_PUBACK:
            # MQTT 5 appends a reason code and properties after the packet
            # identifier; nothing here acts on either.
            if len(body) >= 2:
                packet_id = struct.unpack_from("!H", body, 0)[0]
                self._in_flight.pop(packet_id, None)
            return True

        if pkt_type == PKT_UNSUBSCRIBE:
            packet_id = struct.unpack_from("!H", body, 0)[0]
            off = 2
            if self.protocol_version == MQTT_5:
                # 5.0 puts a property block between the packet identifier and
                # the topic filters; none of the defined ones apply to a server.
                _unsubscribe_props, off = _decode_properties(body, off)
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

            # MQTT 3.1.1 §3.10.4 (5.0 §3.10.4 is the same rule): each filter is
            # compared character-by-character with the session's subscriptions,
            # and one that matches none of them is simply skipped — a wildcard
            # filter goes only by its own text, not by the topics it happened to
            # match. `_sub_filters` keeps the filter as it arrived on the wire,
            # so the comparison happens before topic prefixing, in the same form
            # SUBSCRIBE stored it.
            reason_codes = []
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
                # MQTT 5.0 §3.11.3: the reason code says what this filter's
                # removal actually did, so it is read off the removal itself
                # rather than off a snapshot taken before it. A filter repeated
                # inside one packet is therefore Success once and then No
                # subscription existed, which is what the session now holds.
                reason_codes.append(
                    RC5_SUCCESS if removed else RC5_NO_SUBSCRIPTION_EXISTED
                )
            if self.protocol_version == MQTT_5:
                await self.send_bytes(_make_unsuback(packet_id, reason_codes))
            else:
                # 3.1.1's UNSUBACK is the packet identifier and nothing else.
                await self.send_bytes(_make_unsuback(packet_id))
            return True

        if pkt_type == PKT_PINGREQ:
            await self.send_bytes(_make_pingresp())
            return True

        if pkt_type == PKT_DISCONNECT:
            self._graceful_disconnect = True
            if self.protocol_version == MQTT_5 and body:
                # 0x04 "Disconnect with Will Message" is the one reason code
                # that asks for the will to be published anyway.
                if body[0] == 0x04:
                    self._graceful_disconnect = False
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
                properties=self._will_properties,
            )
        if self._client_id:
            if self._persist_session:
                self._preserve_session()
            else:
                # A connection can resume a stored session and still not
                # persist one: MQTT 5 splits the two, so Clean Start 0 with no
                # Session Expiry Interval picks the session up and then ends
                # it. Without this the entry outlives every such connection,
                # queueing QoS 1 messages nobody will collect and reporting
                # session_present=1 to a client that asked for a clean one.
                _persistent_sessions.pop(
                    (self.account_id, self.region, self._client_id), None
                )
        for sid in self._sub_ids:
            await broker_unsubscribe(sid)
        self._sub_ids.clear()
        self._sub_filters.clear()
        self._sub_granted_qos.clear()
        if self._client_id:
            _deregister_client(
                self.account_id, self.region, self._client_id, self
            )

    def _preserve_session(self) -> None:
        session_key = (self.account_id, self.region, self._client_id)
        unprefixed_filters = list(self._sub_filters.values())
        existing = _persistent_sessions.get(session_key)
        if existing is not None:
            existing.subscriptions = unprefixed_filters
            existing.created_at = time.time()
            existing.expiry_interval = self._session_expiry_interval
        else:
            _persistent_sessions[session_key] = _PersistentSessionState(
                subscriptions=unprefixed_filters,
                created_at=time.time(),
                expiry_interval=self._session_expiry_interval,
            )


async def _feed_session(session: _WSSession, data: bytes, max_buffer: int) -> bool:
    """Push raw MQTT bytes into ``session`` and dispatch every complete packet.

    Returns False when the connection must be dropped — either the accumulated
    frame buffer blew past ``max_buffer`` or a packet handler asked to stop
    (DISCONNECT, invalid publish topic). Shared by the WebSocket transport and
    the optional mTLS TCP listener; ``_take_packet`` does the framing, so both
    transports see identical semantics for partial and coalesced packets.
    """
    session._buffer.extend(data)
    if len(session._buffer) > max_buffer:
        _broker_logger.warning("IoT broker: frame buffer overflow, dropping connection")
        return False
    while True:
        pkt = session._take_packet()
        if pkt is None:
            return True
        pkt_type, flags, body = pkt
        if not await session.handle_packet(pkt_type, flags, body):
            return False


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
            if not await _feed_session(session, data, max_buffer):
                break
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


# ---------------------------------------------------------------------------
# mTLS MQTT listener — the broker's TCP transport
# ---------------------------------------------------------------------------
# The broker above speaks MQTT over WebSocket on the gateway port, which real
# device binaries built on the AWS IoT Device SDK cannot use: they open a plain
# TCP socket, present a client certificate, and speak MQTT directly. This
# listener is that transport. Everything above the socket is the same broker —
# ``_WSSession`` handles framing and packet semantics unchanged, fed through
# ``_feed_session`` exactly like the WebSocket path.
#
# On by default at port 8883 (the MQTT-over-TLS port AWS uses), configured the
# way the Transfer Family SFTP listener is: it starts when the optional
# ``cryptography`` package is importable, ``IOT_MTLS_ENABLED=0`` turns it off,
# and ``IOT_MTLS_PORT`` moves it. A port that cannot be bound (8883 is a
# popular port, a local broker may already hold it) degrades to a log line
# rather than failing the boot.
#
# The listener's server certificate is minted from the Local CA (the one that
# signs ``CreateKeysAndCertificate`` leaves, served at
# ``GET /_ministack/iot/ca.pem``), so a device trusting that one file verifies
# the broker.
#
# The client certificate is OPTIONAL and the rule for it is the one S3 already
# applies to a presigned URL: a request carrying no credential is served, and
# a request carrying one that does not hold up is refused. Concretely:
#
# * no client certificate: the session runs under ``MINISTACK_ACCOUNT_ID``,
#   exactly like an unsigned MQTT-over-WebSocket upgrade. Nothing is required
#   of a caller that claims nothing.
# * a certificate registered ACTIVE in exactly one account: that account and
#   region serve the session, which is what gives a device fleet per-tenant
#   isolation without any credential handling.
# * a certificate that is unknown, not ACTIVE, or ACTIVE in several accounts:
#   refused with a "not authorized" MQTT CONNACK. This reads the X.509
#   identity lifecycle that the registry already keeps and that the control plane already
#   enforces (deleting an ACTIVE certificate is CertificateStateException
#   406), so it is not policy evaluation and not a credential check; a
#   deactivated certificate stops working, which is what makes deactivation
#   mean anything and what real AWS IoT does. The ambiguous case cannot be
#   attributed at all: two registrations see byte-identical bytes, so any
#   tie-break would award the session to whoever registered the copy.
#
# The TLS layer adds one constraint of its own: a presented chain is verified,
# so a certificate from a CA the listener does not trust fails the handshake
# before any of the above. The trusted set is the Local CA plus every ACTIVE
# CA in the registry.

_MTLS_DEFAULT_PORT = 8883
_MTLS_READ_CHUNK = 65536
# How long a client whose certificate was refused gets to send its CONNECT
# before the connection is dropped without a CONNACK.
_MTLS_REFUSE_READ_TIMEOUT = 5.0
_MTLS_REFUSE_MAX_BYTES = 8192

_mtls_logger = logging.getLogger("iot_mtls")
_mtls_server: asyncio.AbstractServer | None = None
_mtls_loop: asyncio.AbstractEventLoop | None = None
_mtls_lock = asyncio.Lock()
# Every live connection's writer, so that stopping the listener can close them
# deliberately and in bounded time (see ``mtls_stop``).
_mtls_sessions: set[asyncio.StreamWriter] = set()
# Digests of the trust anchors already loaded into the live TLS context;
# emptied whenever a context is built, since a fresh one starts with none.
_mtls_loaded_anchors: set[str] = set()

import importlib.util as _importlib_util

_CRYPTOGRAPHY_AVAILABLE = _importlib_util.find_spec("cryptography") is not None


def _mtls_is_truthy(value: str) -> bool:
    return value.strip().lower() in ("1", "true", "yes", "on")


def _mtls_port() -> int | None:
    """The listener port, or None when the configured value is unusable."""
    raw = os.environ.get("IOT_MTLS_PORT", "").strip()
    if not raw:
        return _MTLS_DEFAULT_PORT
    try:
        port = int(raw)
    except ValueError:
        _mtls_logger.warning(
            "IoT mTLS: IOT_MTLS_PORT=%r is not a number; listener disabled", raw
        )
        return None
    if not 0 < port < 65536:
        _mtls_logger.warning(
            "IoT mTLS: IOT_MTLS_PORT=%d is not a usable port; listener disabled", port
        )
        return None
    return port


def mtls_enabled() -> bool:
    """True when the listener should run.

    The same two conditions the SFTP listener uses, in the same order: the
    optional dependency has to be importable (no ``cryptography`` means no CA
    to mint a server certificate from, so the listener stays off rather than
    failing the boot), and ``IOT_MTLS_ENABLED`` has to be unset or truthy.
    """
    if not _CRYPTOGRAPHY_AVAILABLE:
        return False
    raw = os.environ.get("IOT_MTLS_ENABLED")
    if raw is not None and not _mtls_is_truthy(raw):
        return False
    return _mtls_port() is not None


def _mtls_server_cert_sans() -> tuple[list[str], list[str]]:
    """(dns_names, ip_addresses) for the listener's certificate.

    Device SDKs verify the broker hostname, so the certificate carries every
    name a device plausibly dials: the loopbacks, this machine's hostname (the
    name compose networks resolve a container by), and ``MINISTACK_HOST``.
    """
    import ipaddress
    import socket as _socket

    dns_names = ["localhost"]
    ip_addresses = ["127.0.0.1", "::1"]
    candidates = [os.environ.get("MINISTACK_HOST", "localhost"), _socket.gethostname()]
    for candidate in candidates:
        if not candidate:
            continue
        try:
            ipaddress.ip_address(candidate)
        except ValueError:
            if candidate not in dns_names:
                dns_names.append(candidate)
        else:
            if candidate not in ip_addresses:
                ip_addresses.append(candidate)
    return dns_names, ip_addresses


def _mtls_cert_is_reusable(cert_pem: str, ca_cert_pem: str) -> bool:
    """True when a persisted server certificate still matches the current CA
    and still covers every configured name."""
    from cryptography import x509

    # The CA subject name is a constant, so issuer equality alone would also
    # accept a leaf signed by a *previous* CA generation — which is exactly
    # what a reset leaves behind. certificate_is_signed_by verifies the
    # signature as well.
    if not certificate_is_signed_by(cert_pem, ca_cert_pem):
        return False
    try:
        leaf = x509.load_pem_x509_certificate(cert_pem.encode("utf-8"))
        san = leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        present = {str(v) for v in san.get_values_for_type(x509.DNSName)}
        present |= {str(v) for v in san.get_values_for_type(x509.IPAddress)}
        wanted_dns, wanted_ips = _mtls_server_cert_sans()
        return set(wanted_dns) <= present and set(wanted_ips) <= present
    except Exception:
        return False


def _mtls_ensure_server_cert() -> tuple[str, str]:
    """Return the listener's (cert_pem, key_pem), minting one when needed.

    A persisted certificate is reused as long as the current CA signed it and
    it still carries every configured SAN, so devices keep their pinned chain
    across restarts.
    """
    from ministack.core.x509_utils import sign_leaf_certificate

    ca_cert_pem, ca_key_pem = _ensure_ca()
    cert_pem, key_pem = get_mtls_server_cert()
    if cert_pem and key_pem and _mtls_cert_is_reusable(cert_pem, ca_cert_pem):
        return cert_pem, key_pem

    dns_names, ip_addresses = _mtls_server_cert_sans()
    cert_pem, key_pem, _public_pem = sign_leaf_certificate(
        ca_cert_pem,
        ca_key_pem,
        common_name="Ministack IoT Broker",
        san_dns=dns_names,
        san_ips=ip_addresses,
    )
    set_mtls_server_cert(cert_pem, key_pem)
    _mtls_logger.info(
        "IoT mTLS: issued broker certificate for %s", ", ".join(dns_names + ip_addresses)
    )
    return cert_pem, key_pem


def _mtls_registered_ca_pems() -> list[str]:
    """ACTIVE CA certificates from the CA registry, across every scope.

    The store is scoped by account and region; a single listener serves every
    tenant, so it is read across all scopes. Only ACTIVE CAs qualify:
    registering a CA without activating it is how AWS says "not yet", and
    honouring that keeps ``UpdateCACertificate`` meaningful for connections
    opened after it.
    """
    pems = []
    for record in list(_ca_certificates._data.values()):
        if not isinstance(record, dict) or record.get("status") != "ACTIVE":
            continue
        pem = record.get("certificatePem")
        if isinstance(pem, str) and pem.strip():
            pems.append(pem)
    return pems


def _mtls_trust_anchors() -> list[str]:
    """CAs whose client certificates verify at the TLS layer: the Local CA plus
    every ACTIVE registered CA. Presenting a certificate is optional, so this
    set only decides which presented chains survive the handshake; who a
    session belongs to is decided afterwards, in ``_mtls_serve_conn``."""
    return [get_ca_cert_pem(), *_mtls_registered_ca_pems()]


def _mtls_refresh_trust_anchors(ctx: ssl.SSLContext) -> None:
    """Load every anchor the context does not already have.

    Anchors are loaded one PEM at a time so that a single unparseable
    registered certificate costs only itself, and each is remembered — by
    digest, including the ones that failed — so a handshake never repeats the
    work or the warning. ``load_verify_locations`` is additive and a live
    context cannot forget an anchor, so a CA deactivated after loading stays
    trusted at the TLS layer until ``reset()`` rebinds; the session-attribution
    lookup still runs per connection.
    """
    for pem in _mtls_trust_anchors():
        digest = hashlib.sha256(pem.encode("utf-8")).hexdigest()
        if digest in _mtls_loaded_anchors:
            continue
        _mtls_loaded_anchors.add(digest)
        try:
            ctx.load_verify_locations(cadata=pem)
        except Exception as e:
            _mtls_logger.warning("IoT mTLS: ignoring an unusable trust anchor: %s", e)


def _mtls_on_client_hello(
    ssl_object: ssl.SSLObject, server_name: str | None, ctx: ssl.SSLContext
) -> None:
    """Pull newly registered CAs in mid-handshake, before the peer is verified.

    A CA registered while the listener is bound has to be trusted without a
    restart, and ``sni_callback`` is the only hook that runs late enough to see
    the current registry yet early enough to matter. It runs on every
    handshake, including those carrying no server name — which is what a
    device dialling the broker by IP sends. An exception here aborts the
    handshake with an internal-error alert, so nothing is allowed to escape.
    """
    try:
        _mtls_refresh_trust_anchors(ctx)
    except Exception:
        _mtls_logger.warning("IoT mTLS: trust-anchor refresh failed", exc_info=True)


def _mtls_build_ssl_context() -> ssl.SSLContext:
    import tempfile

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    # Optional, not required: the WebSocket path accepts any client, and this
    # transport keeps that default. A presented certificate is verified (that
    # is the TLS layer's rule, not ours) and then used purely as a tenancy
    # signal; absence of one falls back the same way an unsigned WS upgrade
    # does.
    ctx.verify_mode = ssl.CERT_OPTIONAL
    cert_pem, key_pem = _mtls_ensure_server_cert()
    # load_cert_chain only takes paths; a combined cert+key file is the least
    # material to leave on disk, and it is unlinked before the handshake.
    with tempfile.NamedTemporaryFile("w", suffix=".pem", delete=False) as fh:
        fh.write(cert_pem)
        fh.write(key_pem)
        chain_path = fh.name
    try:
        ctx.load_cert_chain(chain_path)
    finally:
        try:
            os.unlink(chain_path)
        except OSError:
            pass
    _mtls_loaded_anchors.clear()
    _mtls_refresh_trust_anchors(ctx)
    ctx.sni_callback = _mtls_on_client_hello
    return ctx


async def mtls_start() -> None:
    """Idempotent: bind the mTLS MQTT listener, unless disabled."""
    if not mtls_enabled():
        _mtls_logger.info("IoT mTLS: disabled (cryptography missing or IOT_MTLS_ENABLED=0)")
        return
    async with _mtls_lock:
        await _mtls_start_locked()


async def mtls_stop() -> None:
    """Close the listener and, with it, the sessions running on it.

    Bounded by construction, whatever is connected: the listening socket is
    closed synchronously and each live connection is aborted, so nothing here
    waits on a peer to acknowledge anything.

    Note what is deliberately *not* here — ``await server.wait_closed()``.
    Since Python 3.12.1 that waits for every established connection to finish
    as well as for the acceptor, which for a broker means "until the last
    device disconnects", i.e. indefinitely. Awaiting it while holding the lock
    wedged both callers: ``lifespan.shutdown`` hung with a device attached, and
    a ``reset()`` rebind never reached its start half.

    Devices are disconnected rather than left running: on shutdown the process
    is going away, and on ``reset()`` the CA that signed their certificates has
    just been dropped. Device SDKs reconnect on their own.
    """
    async with _mtls_lock:
        await _mtls_stop_locked()


async def _mtls_start_locked() -> None:
    global _mtls_server, _mtls_loop
    if _mtls_server is not None:
        return
    port = _mtls_port()
    if port is None:
        return
    try:
        # Building the context can mint the server certificate — and, on first
        # boot, the CA — which is RSA keygen, so keep it off the event loop.
        ctx = await asyncio.to_thread(_mtls_build_ssl_context)
    except Exception as e:
        _mtls_logger.warning(
            "IoT mTLS: could not build the TLS context (%s); listener unavailable", e
        )
        return
    try:
        _mtls_server = await asyncio.start_server(
            _mtls_handle_conn, host="0.0.0.0", port=port, ssl=ctx
        )
    except OSError as e:
        _mtls_logger.warning(
            "IoT mTLS: failed to bind port %d (%s); listener unavailable", port, e
        )
        _mtls_server = None
        return
    _mtls_loop = asyncio.get_running_loop()
    _mtls_logger.info(
        "IoT mTLS: MQTT listening on port %d (client certificate optional)", port
    )


async def _mtls_stop_locked() -> None:
    global _mtls_server
    if _mtls_server is None and not _mtls_sessions:
        return
    if _mtls_server is not None:
        try:
            _mtls_server.close()
        except Exception as e:
            _mtls_logger.debug("IoT mTLS: error closing the listener: %s", e)
        _mtls_server = None
    for writer in list(_mtls_sessions):
        try:
            # abort(), not close(): closing a TLS transport sends close_notify
            # and then waits for the peer's, which a device that has stopped
            # reading never sends — the same unbounded wait this function
            # exists to avoid, just one layer down.
            writer.transport.abort()
        except Exception as e:
            _mtls_logger.debug("IoT mTLS: error closing a session: %s", e)
    _mtls_sessions.clear()
    _mtls_loaded_anchors.clear()
    _mtls_logger.info("IoT mTLS: stopped")


async def _mtls_restart() -> None:
    """Rebind with fresh TLS material, under a single lock acquisition, so two
    resets in flight cannot interleave and no connection is accepted against
    half-replaced material."""
    try:
        async with _mtls_lock:
            await _mtls_stop_locked()
            if mtls_enabled():
                await _mtls_start_locked()
    except Exception:
        _mtls_logger.warning("IoT mTLS: restart failed", exc_info=True)


def mtls_schedule_restart() -> None:
    """Rebind the listener with fresh TLS material, from any thread.

    ``reset()`` runs in a worker thread, so it cannot await; it hands the
    restart back to the event loop the listener was bound on. The rebind
    disconnects whatever was attached: the CA those sessions were admitted on
    no longer exists.
    """
    loop = _mtls_loop
    if loop is None or loop.is_closed():
        return
    try:
        loop.call_soon_threadsafe(lambda: asyncio.ensure_future(_mtls_restart()))
    except RuntimeError as e:
        _mtls_logger.debug("IoT mTLS: could not schedule a restart: %s", e)


def _mtls_active_registrations(cert_id: str) -> list[tuple[str, str, dict]]:
    """Every (account_id, region, record) holding this certificate ACTIVE."""
    return [
        (account_id, region, record)
        for (account_id, region, key), record in list(_certificates._data.items())
        if key == cert_id and isinstance(record, dict) and record.get("status") == "ACTIVE"
    ]


def _mtls_refusal_reason(cert_id: str) -> str:
    """Why a presented certificate was refused, for the log line, including the
    ambiguous case, which is the one an operator needs spelled out."""
    active = _mtls_active_registrations(cert_id)
    if len(active) > 1:
        scopes = ", ".join(f"{a}/{r}" for a, r, _record in sorted(active, key=lambda m: m[:2]))
        return f"registered ACTIVE in {len(active)} scopes ({scopes}); owner is ambiguous"
    for (_account_id, _region, key), record in list(_certificates._data.items()):
        if key == cert_id and isinstance(record, dict):
            return f"status {record.get('status')!r}"
    return "not registered"


def _mtls_resolve_identity(der: bytes | None) -> tuple[str, str] | None:
    """Map a peer certificate (or none) to (account_id, region), or None.

    None means "this connection is refused". Presenting no certificate is not
    refused: that caller claims nothing and is served under the default
    account, the same way ``_ws_resolve_iot_account_id`` treats an unsigned
    WebSocket upgrade. A certificate that IS presented is read against the
    registry, which is the identity lifecycle this service already keeps:
    ACTIVE in exactly one account selects it, and unknown, not ACTIVE, or
    ACTIVE in several accounts is refused by the caller with a "not
    authorized" CONNACK.
    """
    if not der:
        return (
            os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000"),
            os.environ.get("MINISTACK_REGION", "us-east-1"),
        )
    # Same derivation as x509_utils.get_certificate_id, without a PEM round-trip.
    cert_id = hashlib.sha256(der).hexdigest()
    active = _mtls_active_registrations(cert_id)
    if len(active) == 1:
        account_id, region, _record = active[0]
        return account_id, region
    return None


async def _mtls_refuse(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter, cert_id: str
) -> None:
    """Answer an unusable client certificate with a "not authorized" CONNACK
    (0x05, or reason code 0x87 for an MQTT 5 client), then close.

    AWS closes the socket without a CONNACK unless just-in-time registration
    applies. Diverging here is deliberate: an emulator's job is
    diagnosability, "not authorized" is reported cleanly by every device SDK,
    and a TLS-layer rejection could not tell the user "unknown" from
    "deactivated".
    """
    _mtls_logger.warning(
        "IoT mTLS: refusing client certificate %s (%s)", cert_id, _mtls_refusal_reason(cert_id)
    )
    buffer = bytearray()
    try:
        while len(buffer) < _MTLS_REFUSE_MAX_BYTES:
            data = await asyncio.wait_for(
                reader.read(_MTLS_READ_CHUNK), timeout=_MTLS_REFUSE_READ_TIMEOUT
            )
            if not data:
                return
            buffer.extend(data)
            if len(buffer) < 2:
                continue
            try:
                remaining, header_end = _decode_remaining_length(bytes(buffer), 1)
            except ValueError:
                continue
            if len(buffer) < header_end + remaining:
                continue
            if (buffer[0] >> 4) & 0x0F == PKT_CONNECT:
                # The CONNACK has to match the client's protocol level: a v5
                # client handed the two-byte 3.1.1 form dies in its decoder
                # instead of reporting the refusal, and 0x05 is not a defined
                # v5 reason code.
                body = bytes(buffer[header_end:header_end + remaining])
                try:
                    _proto_name, level_at = _read_string(body, 0)
                    protocol_level = body[level_at]
                except (ValueError, IndexError):
                    protocol_level = MQTT_311
                if protocol_level == MQTT_5:
                    connack = _make_connack(
                        return_code=RC5_NOT_AUTHORIZED, protocol_version=MQTT_5
                    )
                else:
                    connack = _make_connack(return_code=CONNACK_311_NOT_AUTHORIZED)
                writer.write(connack)
                await writer.drain()
            return
    except (asyncio.TimeoutError, OSError, ssl.SSLError):
        return


async def _mtls_close(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
        await writer.wait_closed()
    except (OSError, ssl.SSLError):
        pass


async def _mtls_handle_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Track the connection for as long as it lives, then serve it, so that
    ``mtls_stop`` can close every peer in bounded time."""
    _mtls_sessions.add(writer)
    try:
        await _mtls_serve_conn(reader, writer)
    finally:
        _mtls_sessions.discard(writer)


async def _mtls_serve_conn(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    ssl_object = writer.get_extra_info("ssl_object")
    der = ssl_object.getpeercert(binary_form=True) if ssl_object is not None else None
    owner = _mtls_resolve_identity(der)
    if owner is None:
        try:
            await _mtls_refuse(reader, writer, hashlib.sha256(der).hexdigest())
        finally:
            await _mtls_close(writer)
        return
    account_id, region = owner

    async def _tcp_send(message: dict) -> None:
        """Adapter for the two ASGI messages a session ever sends."""
        message_type = message.get("type")
        if message_type == "websocket.send":
            writer.write(message["bytes"])
            await writer.drain()
        elif message_type == "websocket.close":
            writer.close()

    account_token = _request_account_id.set(account_id)
    region_token = _request_region.set(region)
    session = _WSSession(_tcp_send, account_id, region)
    max_buffer = _max_frame_buffer_bytes()
    _mtls_logger.info(
        "IoT mTLS: connection from %s serving %s/%s (%s)",
        writer.get_extra_info("peername"),
        account_id,
        region,
        "certificate presented" if der else "no client certificate",
    )
    try:
        while True:
            data = await reader.read(_MTLS_READ_CHUNK)
            if not data:
                break
            if not await _feed_session(session, data, max_buffer):
                break
    except (OSError, ssl.SSLError) as e:
        _mtls_logger.debug("IoT mTLS: connection dropped: %s", e)
    except Exception:
        _mtls_logger.exception("IoT mTLS session failed")
    finally:
        try:
            await session.cleanup()
        except Exception:
            _mtls_logger.exception("IoT mTLS: session cleanup failed")
        for var, token in ((_request_account_id, account_token), (_request_region, region_token)):
            try:
                var.reset(token)
            except ValueError:
                pass
        await _mtls_close(writer)
