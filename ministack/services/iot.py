# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""AWS IoT Core control plane emulator.

Implements the JSON/REST APIs under ``iot.{region}.amazonaws.com``:

  - Thing registry: ``CreateThing``, ``DescribeThing``, ``ListThings``,
    ``UpdateThing``, ``DeleteThing``
  - ThingType: ``CreateThingType`` and friends
  - ThingGroup: ``CreateThingGroup`` and friends
  - Certificates: ``CreateKeysAndCertificate``, ``CreateCertificateFromCsr``,
    ``RegisterCertificate``, ``RegisterCertificateWithoutCA``, ``UpdateCertificate``,
    ``DeleteCertificate``, ``AttachThingPrincipal`` / ``DetachThingPrincipal``
  - CA certificates + JITR: ``GetRegistrationCode`` / ``DeleteRegistrationCode``,
    ``RegisterCACertificate``, ``DescribeCACertificate``, ``UpdateCACertificate``,
    ``ListCACertificates``, ``DeleteCACertificate``, ``ListCertificatesByCA``;
    an mTLS connect with an unknown certificate signed by a CA with
    auto-registration enabled creates it PENDING_ACTIVATION and publishes the
    AWS JITR event to
    ``$aws/events/certificates/registered/{caCertificateId}``
  - Provisioning templates: ``CreateProvisioningTemplate``,
    ``DescribeProvisioningTemplate``, ``ListProvisioningTemplates``,
    ``UpdateProvisioningTemplate``, ``DeleteProvisioningTemplate``,
    ``CreateProvisioningClaim``, and fleet provisioning over MQTT
    (``$aws/certificates/create/json`` and
    ``$aws/provisioning-templates/{name}/provision/json``, after the
    template's pre-provisioning hook allows it); with ``AUTH=true``
    RegisterThing needs a ``provisioningRoleArn`` that IoT may assume and
    that allows its writes
  - Policies: ``CreatePolicy``, ``CreatePolicyVersion``, ``AttachPolicy``,
    ``DetachPolicy``, etc., plus the deprecated principal-policy family
    (``AttachPrincipalPolicy`` / ``DetachPrincipalPolicy`` /
    ``ListPrincipalPolicies`` / ``ListPolicyPrincipals``)
    ``DetachPolicy``, etc.
  - Fleet indexing: ``UpdateIndexingConfiguration`` /
    ``GetIndexingConfiguration`` / ``DescribeIndex`` / ``ListIndices``, and
    ``SearchIndex`` over the live registry, shadows and MQTT connectivity
  - Registry events: ``DescribeEventConfigurations`` /
    ``UpdateEventConfigurations``
  - Jobs (control plane): ``CreateJob``, ``UpdateJob``, ``DescribeJob``,
    ``ListJobs``, ``GetJobDocument``, ``CancelJob``, ``DeleteJob``,
    ``ListJobExecutionsForThing``, ``ListJobExecutionsForJob``,
    ``DescribeJobExecution``, ``CancelJobExecution``,
    ``DeleteJobExecution`` — execution state shared with the
    ``iot-jobs-data`` device data plane (``iot_jobs_data.py``)
  - Job templates: ``CreateJobTemplate``, ``DescribeJobTemplate``,
    ``ListJobTemplates``, ``DeleteJobTemplate``; ``CreateJob`` with a
    ``jobTemplateArn`` takes the template's document and configuration
  - ``DescribeEndpoint`` returning a per-account hostname
  - Domain configurations: ``CreateDomainConfiguration``,
    ``DescribeDomainConfiguration``, ``ListDomainConfigurations``,
    ``UpdateDomainConfiguration``, ``DeleteDomainConfiguration``, including
    the ``iot:Data-ATS`` / ``iot:CredentialProvider`` configurations IoT owns
    (control plane only: the broker does not serve a configured domain)
  - Tags: ``TagResource``, ``UntagResource``, ``ListTagsForResource`` on the
    taggable types above, which also take ``tags`` when created

This is the control plane — pure HTTP/JSON, no MQTT broker
dependency. The data plane (``iot_data.py``) is
implemented separately and only depends on this module for certificate
lookups (mTLS).

State is fully isolated per account and region via
``AccountRegionScopedDict`` and persisted through
``get_state``/``_restore_state``. The Local CA (used to sign
``CreateKeysAndCertificate`` certificates) is also persisted so previously
issued client certificates remain valid across restarts.
"""

from __future__ import annotations

import asyncio
import base64
import contextlib
import contextvars
import copy
import functools
import hashlib
import json
import logging
import math
import os
import re
import secrets
import ssl
import string
import struct
import time
import unicodedata
import uuid
import weakref
from datetime import datetime, timezone
from typing import Awaitable, Callable
from urllib.parse import parse_qs, parse_qsl

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import run_reentrant
from ministack.core.iam_evaluator import EvalContext, evaluate, parse_policy_document
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
from ministack.core.router import extract_access_key_id
from ministack.core.x509_utils import (
    certificate_common_name,
    certificate_is_signed_by,
    generate_ca,
    get_certificate_id,
    sign_certificate_request,
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
# Registry event configuration (UpdateEventConfigurations), one entry per account+region.
_event_config: AccountRegionScopedDict = AccountRegionScopedDict()
# Domain configurations (CreateDomainConfiguration & friends): name -> record
_domain_configurations: AccountRegionScopedDict = AccountRegionScopedDict()
# CA-certificate registry (RegisterCACertificate & friends): caCertificateId -> record
_ca_certificates: AccountRegionScopedDict = AccountRegionScopedDict()
# JITR registration code — a single "code" key per account/region
_registration_codes: AccountRegionScopedDict = AccountRegionScopedDict()
# Provisioning templates (CreateProvisioningTemplate & friends): templateName -> record
_provisioning_templates: AccountRegionScopedDict = AccountRegionScopedDict()
# Claim certificates (CreateProvisioningClaim): certificateId -> record, until they expire
_provisioning_claims: AccountRegionScopedDict = AccountRegionScopedDict()
# Ownership tokens of certificates created over MQTT: token -> record, until they expire
_ownership_tokens: AccountRegionScopedDict = AccountRegionScopedDict()
_jobs: AccountRegionScopedDict = AccountRegionScopedDict()  # jobId -> Job dict
# (thingName, jobId) -> JobExecution dict — tuple keys, same pattern as _shadows
_job_executions: AccountRegionScopedDict = AccountRegionScopedDict()
_job_templates: AccountRegionScopedDict = AccountRegionScopedDict()  # jobTemplateId -> record
# Tags of the taggable resources: resource ARN -> {key: value}
_tags: AccountRegionScopedDict = AccountRegionScopedDict()

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
    __slots__ = ("expires_at", "payload", "properties", "qos", "topic", "ts")

    def __init__(self, topic: str, payload: bytes, qos: int, properties: bytes = b"",
                 ts: float | None = None):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        # Encoded MQTT 5 property block forwarded to v5 subscribers, empty for
        # a message published by a 3.1.1 client.
        self.properties = properties
        self.ts = time.time() if ts is None else ts
        self.expires_at = _message_expires_at(properties, self.ts)

    def expired(self, now: float) -> bool:
        """Whether the message's MQTT 5 expiry interval has run out."""
        return self.expires_at is not None and now >= self.expires_at


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
            "properties": base64.b64encode(msg.properties).decode("ascii"),
            "ts": msg.ts,
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
        properties = base64.b64decode(entry.get("properties", ""))
        _retained[topic] = _RetainedMessage(topic, payload, qos, properties, entry.get("ts"))


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
        "event_config": copy.deepcopy(_event_config),
        "domain_configurations": copy.deepcopy(_domain_configurations),
        "ca_certificates": copy.deepcopy(_ca_certificates),
        "registration_codes": copy.deepcopy(_registration_codes),
        "provisioning_templates": copy.deepcopy(_provisioning_templates),
        "provisioning_claims": copy.deepcopy(_provisioning_claims),
        "ownership_tokens": copy.deepcopy(_ownership_tokens),
        "jobs": copy.deepcopy(_jobs),
        "job_executions": copy.deepcopy(_job_executions),
        "job_templates": copy.deepcopy(_job_templates),
        "tags": copy.deepcopy(_tags),
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


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data: dict | None) -> None:
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
    _event_config.update(data.get("event_config", {}))
    _domain_configurations.update(data.get("domain_configurations", {}))
    _ca_certificates.update(data.get("ca_certificates", {}))
    _registration_codes.update(data.get("registration_codes", {}))
    _provisioning_templates.update(data.get("provisioning_templates", {}))
    # Older state kept a template's body on the record, as its only version.
    for record in _provisioning_templates.all_values():
        if "versions" not in record:
            body = record.pop("templateBody")
            record["versions"] = {"1": _provisioning_template_version(1, body, record["creationDate"])}
            record["lastVersionId"] = 1
    _provisioning_claims.update(data.get("provisioning_claims", {}))
    _ownership_tokens.update(data.get("ownership_tokens", {}))
    _jobs.update(data.get("jobs", {}))
    _job_executions.update(data.get("job_executions", {}))
    _job_templates.update(data.get("job_templates", {}))
    _tags.update(data.get("tags", {}))
    # Older state kept these tags on the records.
    for store, arn_key in ((_thing_groups, "thingGroupArn"), (_jobs, "jobArn"),
                           (_job_templates, "jobTemplateArn")):
        for (account, region, _), record in store.all_items():
            legacy = record.pop("tags", None)
            if legacy and not _tags.contains_scoped(account, region, record[arn_key]):
                _tags.set_scoped(account, region, record[arn_key],
                                 {t["Key"]: t.get("Value", "") for t in legacy})
    for (account_id, region, _), job in _jobs.all_items():
        if job.get("rolloutQueue"):
            _jobs_rollout_start(account_id, region, job)
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
    _event_config.clear()
    _domain_configurations.clear()
    _ca_certificates.clear()
    _registration_codes.clear()
    _provisioning_templates.clear()
    _provisioning_claims.clear()
    _ownership_tokens.clear()
    _jobs.clear()
    _job_executions.clear()
    _job_templates.clear()
    _tags.clear()
    # Retained messages are part of the state get_state() saves.
    _retained.clear()
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


def _provisioning_template_arn(name: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:provisioningtemplate/{name}"


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


# AWS's ResourceNotFoundException wording per resource, or per operation where it differs.
_NOT_FOUND_MESSAGES = {
    "Thing": "Thing {name} not found.",
    "DescribeThing": "Thing {account}:{name} not found.",
    "ListThingGroupsForThing": "thing does not exist",
    "ListThingPrincipals": "Failed to list principals for thing{name} because the thing does not exist in your account",
    "AttachThingPrincipal": "Failed to attach the principal {principal} to thing {name} because the thing does not "
    "exist in your account",
    "DetachThingPrincipal": "Failed to detach the principal {principal} from thing {name} because the thing does not "
    "exist in your account",
    "AttachThingPrincipal:principal": "Can't attach the principal {principal} to thing {name} because the principal "
    "does not exist in your account",
    "DetachThingPrincipal:principal": "Can't detach the principal {principal} from thing {name} because the principal "
    "does not exist in your account",
    "ListPrincipalThings": "Can't list things for the principal {name} because the principal does not exist in your "
    "account",
    "ThingType": "Thing Type {account}:{name} not found.",
    "ThingGroup": "ThingGroup {account}:{name} not found.",
    "UpdateThingGroup": "Group {name} does not exist",
    "ThingGroupMember": "Cannot find requested thing group for accountId: {account} and thing group name: {name}",
    "Certificate": "CertificateId {name} does not exist",
    "CACertificate": "Requested CA Certificate does not exist.",
    "Policy": "Policy not found",
    "GetPolicy": "Policy default version not found",
    "PolicyVersion": "Policy version does not exist (name={policy}, versionId={name})",
    "Job": "Job {name} cannot be found.",
    "JobExecution": "Job execution for thing {thing} cannot be found within job {name}.",
    "JobExecution:job": "Job execution cannot be found as the Job {name} does not exist.",
}


def _error_not_found(resource: str, name: str, **fields) -> tuple:
    template = _NOT_FOUND_MESSAGES.get(resource, "{resource} {name!r} not found")
    message = template.format(resource=resource, name=name, account=get_account_id(), **fields)
    return error_response_json("ResourceNotFoundException", message, 404)


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
    """Route an IoT control-plane request, then publish the registry events it raised."""
    async with publishing_events():
        return await _route_request(method, path, headers, body, query_params)


async def _route_request(
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
    if path == "/domainConfigurations" and method == "GET":
        return _list_domain_configurations(qp)
    if path.startswith("/domainConfigurations/"):
        return _handle_domain_configuration(method, path, body)

    # Things — list/describe/update/delete
    if path == "/things" and method == "GET":
        return _list_things(qp)
    # Principal lives at /things/{name}/principals — must come BEFORE generic /things/{name}
    if path.startswith("/things/") and path.endswith("/principals"):
        return _handle_thing_principals(method, path, hdr, body, qp)
    # Group membership lives at /things/{name}/thing-groups — like /principals,
    # must come BEFORE the generic /things/{name} branch.
    if path.startswith("/things/") and path.endswith("/thing-groups") and method == "GET":
        return _list_thing_groups_for_thing(path)
    # Job executions live at /things/{name}/jobs[/{jobId}[/cancel]] and
    # /things/{name}/jobs/{jobId}/executionNumber/{n} — like
    # /principals, must come BEFORE the generic /things/{name} branch. The
    # `jobs` segment only counts when a thing name precedes it: `/things/jobs`
    # is thing CRUD for a thing that happens to be named `jobs`.
    if path.startswith("/things/"):
        _thing, _, _sub = path[len("/things/"):].partition("/")
        if _thing and (_sub == "jobs" or _sub.startswith("jobs/")):
            return await _handle_thing_jobs(method, path, body, qp)
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
        return await _add_thing_to_group(_parse_body(body))
    if path == "/thing-groups/removeThingFromThingGroup" and method in ("PUT", "POST"):
        return await _remove_thing_from_group(_parse_body(body))
    if path == "/thing-groups" and method == "GET":
        return _list_thing_groups(qp)
    if path.startswith("/thing-groups/") and path.endswith("/things") and method == "GET":
        return _list_things_in_thing_group(path)
    if path.startswith("/thing-groups/"):
        return _handle_thing_group(method, path, body, qp)

    # Provisioning templates
    if path == "/provisioning-templates" and method == "POST":
        return _create_provisioning_template(_parse_body(body))
    if path == "/provisioning-templates" and method == "GET":
        return _list_provisioning_templates(qp)
    if path.startswith("/provisioning-templates/"):
        return _handle_provisioning_template(method, path, body, qp)

    # Certificates
    if path == "/keys-and-certificate" and method == "POST":
        return _create_keys_and_certificate(qp)
    if path == "/certificates" and method == "POST":
        return _create_certificate_from_csr(_parse_body(body), qp)
    if path == "/certificate/register" and method == "POST":
        return await _register_certificate(_parse_body(body), qp)
    if path == "/certificate/register-no-ca" and method == "POST":
        return await _register_certificate_without_ca(_parse_body(body), qp)

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
    if path.startswith("/certificates-by-ca/") and method == "GET":
        return _list_certificates_by_ca(path, qp)
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
    if path == "/event-configurations" and method == "GET":
        return _describe_event_configurations()
    if path == "/event-configurations" and method == "PATCH":
        return _update_event_configurations(_parse_body(body))
    # Jobs (control plane) — CreateJob is PUT /jobs/{jobId} per the botocore
    # `iot` service model; sub-resources (/cancel, /job-document) are
    # dispatched inside _handle_job.
    if path == "/jobs" and method == "GET":
        return _list_jobs(qp)
    if path.startswith("/jobs/"):
        return await _handle_job(method, path, body, qp)
    if path == "/job-templates" and method == "GET":
        return _list_job_templates(qp)
    if path.startswith("/job-templates/"):
        return _handle_job_template(method, path, body)

    # Topic rules
    if path == "/rules" and method == "GET":
        return _list_topic_rules(qp)
    if path.startswith("/rules/"):
        return _handle_topic_rule(method, path, body, hdr)

    if path == "/tags" and method == "POST":
        return _tag_resource(_parse_body(body))
    if path == "/untag" and method == "POST":
        return _untag_resource(_parse_body(body))
    if path == "/tags" and method == "GET":
        return _list_tags_for_resource(qp)

    return error_response_json(
        "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
    )


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


# AWS refuses the legacy endpoint types; the jobs data plane is served from
# the iot:Data-ATS endpoint.
_RETIRED_ENDPOINT_TYPES = {
    "iot:Data": "iot:Data is not supported. Please use iot:Data-ATS instead.",
    "iot:Jobs": (
        "IoT Jobs and Commands APIs are now available through iot:Data-ATS "
        "endpoints instead of iot:Jobs endpoints. Please use iot:Data-ATS."
    ),
}


def _endpoint_prefix(account_id: str) -> str:
    """The account's endpoint host prefix: the first 14 hex chars of SHA-256(account_id)."""
    return hashlib.sha256(account_id.encode("utf-8")).hexdigest()[:14]


def _describe_endpoint(qp: dict) -> tuple:
    """Return a per-account endpoint hostname.

    Format: ``{prefix}-ats.iot.{region}.{MINISTACK_HOST}:{GATEWAY_PORT}``
    where ``prefix`` is the first 14 hex chars of SHA-256(account_id), so the
    hostname is stable per account and looks AWS-shaped without leaking the
    account ID.
    """
    endpoint_type = qp.get("endpointType", "iot:Data-ATS")
    account_id = get_account_id()
    prefix = _endpoint_prefix(account_id)
    region = get_region()

    if endpoint_type in _RETIRED_ENDPOINT_TYPES:
        return error_response_json(
            "InvalidRequestException", _RETIRED_ENDPOINT_TYPES[endpoint_type], 400
        )
    if endpoint_type == "iot:Data-ATS":
        host = f"{prefix}-ats.iot.{region}.{_MINISTACK_HOST}:{_GATEWAY_PORT}"
    elif endpoint_type == "iot:CredentialProvider":
        host = f"{prefix}.credentials.iot.{region}.{_MINISTACK_HOST}:{_GATEWAY_PORT}"
    elif endpoint_type == "iot:Jobs":
        # Retired: AWS now serves Jobs and Commands from the Data-ATS endpoint
        # (captured eu-north-1 2026-09-19). Signing with the `iot-jobs-data`
        # scope against the Data-ATS host routes there already.
        return error_response_json(
            "InvalidRequestException",
            "IoT Jobs and Commands APIs are now available through iot:Data-ATS "
            "endpoints instead of iot:Jobs endpoints. Please use iot:Data-ATS.",
            400,
        )
    else:
        return error_response_json(
            "InvalidRequestException",
            f"Unknown endpointType: {endpoint_type}",
            400,
        )
    return json_response({"endpointAddress": host})


# ---------------------------------------------------------------------------
# Domain configurations
# ---------------------------------------------------------------------------

# Names: Create and Delete take ``[\w.-]+``; Describe and Update also accept
# ``:`` so they reach the configurations IoT owns (``iot:Data-ATS``).
_DOMAIN_CONFIG_NAME_RE = re.compile(r"[\w.-]+", re.ASCII)
_RESERVED_DOMAIN_CONFIG_NAME_RE = re.compile(r"[\w.:-]+", re.ASCII)
_AUTHORIZER_NAME_RE = re.compile(r"[\w=,@-]+", re.ASCII)
# The configurations IoT owns. ListDomainConfigurations shows the first two on
# an account that never created one; ``iot:Jobs`` is retired with its endpoint.
_IOT_OWNED_DOMAIN_CONFIGS = {
    "iot:Data-ATS": "DATA",
    "iot:CredentialProvider": "CREDENTIAL_PROVIDER",
}
_RETIRED_DOMAIN_CONFIG_NAMES = frozenset({"iot:Jobs"})
_DEFAULT_SECURITY_POLICY = "IoTSecurityPolicy_TLS13_1_2_2022_10"
# Matched case-insensitively and stored in this spelling, as AWS does.
_SECURITY_POLICIES = {
    p.lower(): p
    for p in (
        "IoTSecurityPolicy_TLS13_1_3_2022_10",
        "IoTSecurityPolicy_TLS13_1_2_2022_10",
        "IoTSecurityPolicy_TLS12_1_2_2022_10",
        "IoTSecurityPolicy_TLS12_1_0_2016_01",
        "IoTSecurityPolicy_TLS12_1_0_2015_01",
    )
}
# Regions that answer "not supported in region" for the 2016 policy (measured
# 2026-10-05; the other ten commercial regions probed accept it).
_TLS12_2016_UNSUPPORTED_REGIONS = frozenset({
    "us-east-1", "us-west-2", "eu-west-1", "eu-central-1",
    "ap-southeast-1", "ap-northeast-1",
})
# authenticationType -> the applicationProtocols it combines with.
_AUTH_PROTOCOLS = {
    "DEFAULT": ("DEFAULT",),
    "AWS_X509": ("SECURE_MQTT", "HTTPS"),
    "AWS_SIGV4": ("MQTT_WSS", "HTTPS"),
    "CUSTOM_AUTH_X509": ("SECURE_MQTT", "HTTPS"),
    "CUSTOM_AUTH": ("SECURE_MQTT", "MQTT_WSS", "HTTPS"),
}
_DOMAIN_CONFIG_ENUMS = (
    ("serviceType", ("DATA", "JOBS", "CREDENTIAL_PROVIDER")),
    ("authenticationType", (
        "DEFAULT", "AWS_X509", "AWS_SIGV4", "CUSTOM_AUTH_X509", "CUSTOM_AUTH",
        "CUSTOM_AUTH_WITH_X509",
    )),
    ("applicationProtocol", ("DEFAULT", "HTTPS", "SECURE_MQTT", "MQTT_WSS")),
    ("domainConfigurationStatus", ("ENABLED", "DISABLED")),
)
_DOMAIN_CONFIG_UPDATE_MEMBERS = (
    "authorizerConfig", "domainConfigurationStatus", "tlsConfig",
    "serverCertificateConfig", "authenticationType", "applicationProtocol",
    "clientCertificateConfig",
)
_DOMAIN_CONFIG_LIST_LIMIT = 250
_PROTOCOLS_DOC = "<https://docs.aws.amazon.com/iot/latest/developerguide/protocols.html>"


def _domain_config_invalid(message: str) -> tuple:
    return error_response_json("InvalidRequestException", message, 400)


def _domain_config_constraint(field: str, constraint: str) -> tuple:
    return _domain_config_invalid(
        f"1 validation error detected: Value at '{field}' failed to satisfy "
        f"constraint: Member must {constraint}"
    )


def _domain_config_name_error(name: str, pattern: re.Pattern) -> tuple | None:
    if len(name) > 128:
        return _domain_config_constraint(
            "domainConfigurationName", "have length less than or equal to 128")
    if not pattern.fullmatch(name):
        return _domain_config_constraint(
            "domainConfigurationName",
            f"satisfy regular expression pattern: {pattern.pattern}")
    return None


def _domain_config_model_error(payload: dict) -> tuple | None:
    """The request-model checks AWS runs before anything else."""
    for field, allowed in _DOMAIN_CONFIG_ENUMS:
        value = payload.get(field)
        if value is not None and value not in allowed:
            return _domain_config_constraint(
                field, f"satisfy enum value set: [{', '.join(allowed)}]")
    authorizer = payload.get("authorizerConfig")
    authorizer_name = (authorizer or {}).get("defaultAuthorizerName")
    if authorizer_name is not None:
        if not 1 <= len(authorizer_name) <= 128:
            return _domain_config_constraint(
                "authorizerConfig.defaultAuthorizerName",
                "have length less than or equal to 128")
        if not _AUTHORIZER_NAME_RE.fullmatch(authorizer_name):
            return _domain_config_constraint(
                "authorizerConfig.defaultAuthorizerName",
                f"satisfy regular expression pattern: {_AUTHORIZER_NAME_RE.pattern}")
    if len(payload.get("serverCertificateArns") or ()) > 1:
        return _domain_config_constraint(
            "serverCertificateArns", "have length less than or equal to 1")
    return None


def _domain_config_security_policy(tls_config: dict | None) -> tuple[str | None, tuple | None]:
    """The stored spelling of ``tlsConfig.securityPolicy`` (the default when
    absent), or the error AWS answers for a policy it does not offer here."""
    policy = (tls_config or {}).get("securityPolicy")
    if policy is None:
        return _DEFAULT_SECURITY_POLICY, None
    canonical = _SECURITY_POLICIES.get(str(policy).lower())
    if canonical is None:
        return None, _domain_config_invalid(f"SecurityPolicy {policy} not recognized")
    region = get_region()
    if canonical.endswith("_2016_01") and region in _TLS12_2016_UNSUPPORTED_REGIONS:
        return None, _domain_config_invalid(
            f"SecurityPolicy {policy} is not supported in region {region}")
    return canonical, None


def _domain_config_auth_error(auth: str, protocol: str, authorizer: dict | None) -> tuple | None:
    if auth not in _AUTH_PROTOCOLS:
        return _domain_config_invalid(
            "The AuthenticationType provided in the request was not recognized. "
            f"Please refer to {_PROTOCOLS_DOC} for supported Authentication Type "
            "values and try again.")
    if protocol not in _AUTH_PROTOCOLS[auth]:
        return _domain_config_invalid(
            "Unsupported combination of AuthenticationType and ApplicationProtocol "
            f"provided. AuthenticationType: [{auth}], ApplicationProtocol: "
            f"[{protocol}]. Please refer to {_PROTOCOLS_DOC} for supported "
            "combinations and try again.")
    if auth.startswith("CUSTOM_AUTH") and not authorizer:
        return _domain_config_invalid(
            "The domain configuration must have a valid AuthorizerConfig configured "
            f"when using {auth} as the AuthenticationType. Please refer to "
            f"{_PROTOCOLS_DOC} for supported combinations and try again.")
    return None


def _domain_config_arn(name: str, suffix: str | None) -> str:
    resource = f"domainconfiguration/{name}" + (f"/{suffix}" if suffix else "")
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:{resource}"


def _endpoint_address(endpoint_type: str) -> str:
    """The host DescribeEndpoint answers for ``iot:Data-ATS`` or
    ``iot:CredentialProvider``, which AWS's configurations of the same name
    carry as their ``domainName``. Same host as ``_describe_endpoint``."""
    prefix = _endpoint_prefix(get_account_id())
    region = get_region()
    if endpoint_type == "iot:CredentialProvider":
        return f"{prefix}.credentials.iot.{region}.{_MINISTACK_HOST}:{_GATEWAY_PORT}"
    return f"{prefix}-ats.iot.{region}.{_MINISTACK_HOST}:{_GATEWAY_PORT}"


def _ensure_owned_domain_configurations() -> None:
    """Materialise the configurations IoT owns, the way AWS does on the
    first domain-configuration call of an account and region. A stored one
    follows the current DescribeEndpoint address (the host or port may differ
    after a restart with persisted state)."""
    for name, service_type in _IOT_OWNED_DOMAIN_CONFIGS.items():
        existing = _domain_configurations.get(name)
        if existing is not None:
            existing["domainName"] = _endpoint_address(name)
            continue
        record = {
            "domainConfigurationName": name,
            "domainConfigurationArn": _domain_config_arn(name, None),
            "domainName": _endpoint_address(name),
            "serverCertificates": [],
            "domainConfigurationStatus": "ENABLED",
            "serviceType": service_type,
            "domainType": "ENDPOINT",
            "lastStatusChangeDate": _now_epoch(),
        }
        if service_type == "DATA":
            record["tlsConfig"] = {"securityPolicy": _DEFAULT_SECURITY_POLICY}
            record["authenticationType"] = "DEFAULT"
            record["applicationProtocol"] = "DEFAULT"
        _domain_configurations[name] = record


def _domain_config_not_found(name: str) -> tuple:
    return error_response_json(
        "ResourceNotFoundException",
        "Could not find domain configuration for given account ID and domain "
        f"configuration name, AccountId: {get_account_id()}, "
        f"DomainConfigurationName: {name}",
        404,
    )


def _acm_certificate(arn: str) -> dict | None:
    from ministack.services import acm as _acm

    return _acm._get_local_certificate(arn)


def _certificate_lacks_server_auth(cert: dict) -> bool:
    """True for an imported certificate whose extended key usage does not
    include TLS server authentication (no EKU extension at all counts)."""
    pem = cert.get("_pem_body")
    if cert.get("Type") != "IMPORTED" or not pem:
        return False
    try:
        from cryptography import x509
        from cryptography.x509.oid import ExtendedKeyUsageOID

        parsed = x509.load_pem_x509_certificate(pem.encode() if isinstance(pem, str) else pem)
    except Exception:
        return False
    try:
        usages = parsed.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
    except x509.ExtensionNotFound:
        return True
    return ExtendedKeyUsageOID.SERVER_AUTH not in usages


def _customer_managed_certificate_error(payload: dict) -> tuple | None:
    server_arn = payload["serverCertificateArns"][0]
    validation_arn = payload.get("validationCertificateArn")
    if not payload.get("domainName"):
        return _domain_config_invalid("Must provide a valid domain name to configure.")
    if validation_arn is not None and validation_arn == server_arn:
        return _domain_config_invalid(
            "Cannot provide a ServerCertificateArn that is the same as a "
            "ValidationCertificateArn")
    for arn in (server_arn, validation_arn):
        if arn is not None and _acm_certificate(arn) is None:
            return _domain_config_invalid(f"Certificate {arn} does not exist.")
    server_cert = _acm_certificate(server_arn)
    if _certificate_lacks_server_auth(server_cert):
        return error_response_json(
            "CertificateValidationException",
            "The certificate is missing ServerAuth in extended key usage of certificate.",
            400,
        )
    # Only an ACM-issued certificate counts as signed by a trusted CA here; an
    # imported one has to be vouched for by a trusted validation certificate.
    if validation_arn is None and server_cert.get("Type") == "IMPORTED":
        return _domain_config_invalid(
            "Must provide validation certificate when using server certificates not "
            "signed by trusted CA or not within")
    if validation_arn is not None and _acm_certificate(validation_arn).get("Type") == "IMPORTED":
        return _domain_config_invalid("Validation certificate must be signed by trusted CA")
    return None


def _create_domain_configuration(name: str, payload: dict) -> tuple:
    """``POST /domainConfigurations/{name}``. Without a server certificate
    the configuration is AWS_MANAGED and gets a generated ``-ats`` host; with
    one it is CUSTOMER_MANAGED on the caller's ``domainName``."""
    err = (_domain_config_name_error(name, _DOMAIN_CONFIG_NAME_RE)
           or _domain_config_model_error(payload)
           or _tags_error(payload.get("tags"), required=False))
    if err:
        return err
    service_type = payload.get("serviceType") or "DATA"
    if service_type == "JOBS":
        return _domain_config_invalid("Invalid Service Type")
    if service_type != "DATA":
        return _domain_config_invalid(
            "CreateDomainConfiguration only supports DATA Service Type")
    existing = _domain_configurations.get(name)
    if existing is not None:
        return error_response_json(
            "ResourceAlreadyExistsException",
            "Domain configuration already exists",
            409,
            extra={"resourceId": existing["domainConfigurationArn"].split(":", 5)[5]},
        )
    policy, err = _domain_config_security_policy(payload.get("tlsConfig"))
    if err:
        return err
    auth = payload.get("authenticationType")
    protocol = payload.get("applicationProtocol")
    authorizer = payload.get("authorizerConfig")
    if auth is not None and protocol is None:
        return _domain_config_invalid(
            "The domain must configure a supported ApplicationProtocol when "
            f"configuring an AuthenticationType. Please refer to {_PROTOCOLS_DOC} "
            "for supported combinations and try again.")
    if protocol is not None and auth is None:
        return _domain_config_invalid(
            "The domain must configure a supported AuthenticationType when "
            f"configuring an ApplicationProtocol. Please refer to {_PROTOCOLS_DOC} "
            "for supported combinations and try again.")
    auth = auth or "DEFAULT"
    protocol = protocol or "DEFAULT"
    err = _domain_config_auth_error(auth, protocol, authorizer)
    if err:
        return err
    if payload.get("serverCertificateArns"):
        err = _customer_managed_certificate_error(payload)
        if err:
            return err
        domain_type = "CUSTOMER_MANAGED"
        domain_name = payload["domainName"]
        server_certificates = [{
            "serverCertificateArn": payload["serverCertificateArns"][0],
            "serverCertificateStatus": "VALID",
        }]
    else:
        if payload.get("domainName"):
            return _domain_config_invalid(
                "Cannot provide a custom domain name for an AWS Managed Domain")
        if payload.get("validationCertificateArn"):
            return _domain_config_invalid(
                "Cannot provide a Validation Certificate for AWS Managed Domain Configurations")
        if payload.get("serverCertificateConfig") is not None:
            return _domain_config_invalid(
                "Cannot create a server certificate config for an AWS Managed Domain")
        domain_type = "AWS_MANAGED"
        domain_name = (f"d{uuid.uuid4().hex[:20]}-ats.iot.{get_region()}."
                       f"{_MINISTACK_HOST}:{_GATEWAY_PORT}")
        server_certificates = []
    record = {
        "domainConfigurationName": name,
        "domainConfigurationArn": _domain_config_arn(name, uuid.uuid4().hex[:5]),
        "domainName": domain_name,
        "serverCertificates": server_certificates,
        "domainConfigurationStatus": "ENABLED",
        "serviceType": service_type,
        "domainType": domain_type,
        "lastStatusChangeDate": _now_epoch(),
        "tlsConfig": {"securityPolicy": policy},
        "authenticationType": auth,
        "applicationProtocol": protocol,
    }
    for member in ("authorizerConfig", "serverCertificateConfig", "clientCertificateConfig"):
        if payload.get(member):
            record[member] = payload[member]
    _domain_configurations[name] = record
    _set_created_tags(record["domainConfigurationArn"], payload.get("tags"))
    return json_response({
        "domainConfigurationName": name,
        "domainConfigurationArn": record["domainConfigurationArn"],
    })


def _describe_domain_configuration(name: str) -> tuple:
    err = _domain_config_name_error(name, _RESERVED_DOMAIN_CONFIG_NAME_RE)
    if err:
        return err
    if name in _RETIRED_DOMAIN_CONFIG_NAMES:
        return _domain_config_invalid("Invalid DomainConfiguration Name")
    _ensure_owned_domain_configurations()
    record = _domain_configurations.get(name)
    if record is None:
        return _domain_config_not_found(name)
    return json_response(record)


def _list_domain_configurations(qp: dict) -> tuple:
    """``GET /domainConfigurations``. AWS pages with ``pageSize`` (1..250) and
    an opaque ``marker``; its ``serviceType`` filter answers every
    configuration for DATA and CREDENTIAL_PROVIDER alike and refuses JOBS."""
    raw_size = qp.get("pageSize")
    try:
        page_size = int(raw_size) if raw_size not in (None, "") else None
    except (TypeError, ValueError):
        return _domain_config_invalid("The request is not valid.")
    if page_size is not None and page_size > _DOMAIN_CONFIG_LIST_LIMIT:
        return _domain_config_constraint(
            "pageSize", f"have value less than or equal to {_DOMAIN_CONFIG_LIST_LIMIT}")
    if page_size is not None and page_size < 1:
        return _domain_config_constraint("pageSize", "have value greater than or equal to 1")
    err = _domain_config_model_error({"serviceType": qp.get("serviceType")})
    if err:
        return err
    if qp.get("serviceType") == "JOBS":
        return _domain_config_invalid("Invalid Service Type")
    offset = 0
    marker = qp.get("marker")
    if marker:
        try:
            prefix, _, value = base64.b64decode(marker, validate=True).decode().partition(":")
            offset = int(value) if prefix == "domainConfigurations" else -1
        except Exception:
            offset = -1
        if offset < 0:
            return _domain_config_invalid("Invalid Token")
    _ensure_owned_domain_configurations()
    records = list(_domain_configurations.values())
    end = len(records) if page_size is None else offset + page_size
    body = {
        "domainConfigurations": [
            {k: r[k] for k in ("domainConfigurationName", "domainConfigurationArn", "serviceType")}
            for r in records[offset:end]
        ]
    }
    if end < len(records):
        body["nextMarker"] = base64.b64encode(f"domainConfigurations:{end}".encode()).decode()
    return json_response(body)


def _update_domain_configuration(name: str, payload: dict) -> tuple:
    """``PUT /domainConfigurations/{name}``. Unset members keep their value;
    ``tlsConfig`` is replaced whole (no policy means the default one), and
    ``lastStatusChangeDate`` only moves when the status does."""
    err = (_domain_config_name_error(name, _RESERVED_DOMAIN_CONFIG_NAME_RE)
           or _domain_config_model_error(payload))
    if err:
        return err
    remove_authorizer = payload.get("removeAuthorizerConfig") is True
    supplied = {m for m in _DOMAIN_CONFIG_UPDATE_MEMBERS if payload.get(m) is not None}
    if not supplied and not remove_authorizer:
        return _domain_config_invalid("No arguments supplied to update")
    if name in _IOT_OWNED_DOMAIN_CONFIGS or name in _RETIRED_DOMAIN_CONFIG_NAMES:
        # Of an owned configuration only iot:Data-ATS's TLS policy is settable.
        if name != "iot:Data-ATS" or supplied != {"tlsConfig"} or remove_authorizer:
            return _domain_config_invalid(
                "IoT owned domain configuration cannot be updated with the supplied arguments")
    if "authorizerConfig" in supplied and remove_authorizer:
        return _domain_config_invalid(
            "AuthorizerConfig supplied and removeAuthorizer is true, cannot set "
            "authorizer and remove it")
    policy = None
    if "tlsConfig" in supplied:
        policy, err = _domain_config_security_policy(payload["tlsConfig"])
        if err:
            return err
    _ensure_owned_domain_configurations()
    record = _domain_configurations.get(name)
    if record is None:
        return _domain_config_not_found(name)
    if "serverCertificateConfig" in supplied and record["domainType"] != "CUSTOMER_MANAGED":
        return _domain_config_invalid(
            "Cannot configure ServerCertificateConfig for non-customer-managed domains")
    authorizer = record.get("authorizerConfig")
    if remove_authorizer:
        authorizer = None
    elif "authorizerConfig" in supplied:
        authorizer = {**(authorizer or {}), **payload["authorizerConfig"]}
    if record["domainType"] != "ENDPOINT":
        err = _domain_config_auth_error(
            payload.get("authenticationType", record["authenticationType"]),
            payload.get("applicationProtocol", record["applicationProtocol"]),
            authorizer,
        )
        if err:
            return err
    if authorizer:
        record["authorizerConfig"] = authorizer
    else:
        record.pop("authorizerConfig", None)
    if policy is not None:
        record["tlsConfig"] = {"securityPolicy": policy}
    status = payload.get("domainConfigurationStatus")
    if status is not None and status != record["domainConfigurationStatus"]:
        record["domainConfigurationStatus"] = status
        record["lastStatusChangeDate"] = _now_epoch()
    for member in ("authenticationType", "applicationProtocol",
                   "serverCertificateConfig", "clientCertificateConfig"):
        if member in supplied:
            record[member] = payload[member]
    _domain_configurations[name] = record
    return json_response({
        "domainConfigurationName": name,
        "domainConfigurationArn": record["domainConfigurationArn"],
    })


def _delete_domain_configuration(name: str) -> tuple:
    """Only a DISABLED configuration goes (an AWS-managed one after seven days); an unknown name is 200."""
    err = _domain_config_name_error(name, _DOMAIN_CONFIG_NAME_RE)
    if err:
        return err
    record = _domain_configurations.get(name)
    if record is None:
        return json_response({})
    if record["domainConfigurationStatus"] != "DISABLED":
        return _domain_config_invalid(
            "Cannot delete a domain configuration that is not disabled")
    if (record.get("domainType") == "AWS_MANAGED"
            and _now_epoch() - float(record.get("lastStatusChangeDate") or 0) < 7 * 86400):
        return _domain_config_invalid(
            "AWS Managed Domain Configuration must be disabled for at least 7 days "
            "before it can be deleted")
    del _domain_configurations[name]
    _tags.pop(record["domainConfigurationArn"], None)
    return json_response({})


def _handle_domain_configuration(method: str, path: str, body: bytes) -> tuple:
    """Dispatch ``/domainConfigurations/{domainConfigurationName}``."""
    name = path[len("/domainConfigurations/"):]
    if not name or "/" in name:
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
        )
    if method == "POST":
        return _create_domain_configuration(name, _parse_body(body))
    if method == "GET":
        return _describe_domain_configuration(name)
    if method == "PUT":
        return _update_domain_configuration(name, _parse_body(body))
    if method == "DELETE":
        return _delete_domain_configuration(name)
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


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
        return _delete_thing(name, qp.get("expectedVersion"))
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
    _thing_events("CREATED", record, None)
    logger.info("IoT Thing created: %s", name)
    return json_response({
        "thingName": name,
        "thingArn": record["thingArn"],
        "thingId": thing_id,
    })


def _describe_thing(name: str) -> tuple:
    thing = _things.get(name)
    if thing is None:
        return _error_not_found("DescribeThing", name)
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
    elif "attributePayload" in payload:
        thing["attributes"] = dict(new_attrs)

    old_type = thing.get("thingTypeName")
    new_type = "" if payload.get("removeThingType") else payload.get("thingTypeName")
    if new_type is not None:
        if new_type and new_type not in _thing_types:
            return _error_not_found("ThingType", new_type)
        thing["thingTypeName"] = new_type or None

    thing["version"] = thing.get("version", 1) + 1
    _things[name] = thing
    _thing_events("UPDATED", thing, old_type)
    return json_response({})


def _delete_thing(name: str, expected_version=None) -> tuple:
    """DeleteThing; an unknown name answers 200 like a completed delete."""
    thing = _things.get(name)
    if thing is None:
        return json_response({})
    if expected_version not in (None, "") and str(expected_version) != str(thing.get("version", 1)):
        return error_response_json(
            "VersionConflictException",
            f"The version for thing {name} does not match the expected version.",
            409,
        )
    if any(p.rsplit("/", 1)[-1] in _certificates for p in thing.get("principals", [])):
        return error_response_json(
            "InvalidRequestException",
            f"Cannot delete. Thing {name} is still attached to one or more principals",
            400,
        )
    _thing_events("DELETED", thing, thing.get("thingTypeName"))
    # Remove from groups
    for gname in list(thing.get("thingGroupNames", [])):
        group = _thing_groups.get(gname)
        if group and name in group.get("things", []):
            group["things"].remove(name)
            _thing_groups[gname] = group
            _membership_event("REMOVED", group, thing)
    # Drop the thing's job executions with it. Left behind, a live execution
    # for a thing that no longer exists holds its job out of COMPLETED forever,
    # and a later thing of the same name would inherit that stale history.
    orphaned_jobs = [key[1] for key in _job_executions.keys() if key[0] == name]
    for job_id in orphaned_jobs:
        del _job_executions[(name, job_id)]
    for job in _jobs.values():
        if name in (job.get("executionsDeletedFor") or ()):
            job["executionsDeletedFor"].remove(name)
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
    err = _tags_error(payload.get("tags"), required=False, missing_value=_TAG_WITHOUT_VALUE)
    if err:
        return err
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
    _set_created_tags(record["thingTypeArn"], payload.get("tags"))
    _thing_type_event("CREATED", record)
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
    _thing_type_event("UPDATED", t)
    return json_response({})


def _delete_thing_type(name: str) -> tuple:
    t = _thing_types.get(name)
    if t is None:
        return json_response({})
    if not t["thingTypeMetadata"].get("deprecated"):
        return error_response_json(
            "InvalidRequestException",
            "ThingType must be deprecated for at least 5 minutes before deletion",
            400,
        )
    if any(thing.get("thingTypeName") == name for thing in _things.values()):
        return error_response_json(
            "InvalidRequestException",
            f"Can not delete thing type {name} with things associated with it",
            400,
        )
    del _thing_types[name]
    _tags.pop(t["thingTypeArn"], None)
    _thing_type_event("DELETED", t)
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
        payload = _parse_body(body)
        existing = _thing_groups.get(name)
        if existing is not None:
            return _create_existing_thing_group(existing, payload)
        return _create_thing_group(name, payload)
    if method == "GET":
        return _describe_thing_group(name)
    if method == "PATCH":
        return _update_thing_group(name, _parse_body(body))
    if method == "DELETE":
        return _delete_thing_group(name)
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


def _create_existing_thing_group(group: dict, payload: dict) -> tuple:
    """CreateThingGroup on a name that exists. AWS answers 200 with the
    existing group when the request describes it exactly: description,
    attributes (in any key order), parent and tags. The merge flag does not
    count, and an empty attribute map or tag list equals none. Any difference
    is a 409 that leaves the group as it was; a property or parent difference
    names the group, a tag difference names the ARN, and both carry
    resourceArn/resourceId as null (measured eu-central-1, 2026-10-05).

    CloudFormation creates through ``_create_thing_group``, which keeps
    refusing an existing name as the AWS::IoT::ThingGroup handler does."""
    props = payload.get("thingGroupProperties") or {}
    stored = group["thingGroupProperties"]
    stored_parent = (group.get("thingGroupMetadata") or {}).get("parentGroupName")
    if (
        props.get("thingGroupDescription") != stored.get("thingGroupDescription")
        or ((props.get("attributePayload") or {}).get("attributes") or {})
        != ((stored.get("attributePayload") or {}).get("attributes") or {})
        or (payload.get("parentGroupName") or None) != stored_parent
    ):
        message = f"Thing Group {group['thingGroupName']} already exists in current account"
    elif ({t.get("Key"): t.get("Value") for t in payload.get("tags") or []}
          != (_tags.get(group["thingGroupArn"]) or {})):
        message = f"Resource {group['thingGroupArn']} already exists in current account"
    else:
        return json_response({
            "thingGroupName": group["thingGroupName"],
            "thingGroupArn": group["thingGroupArn"],
            "thingGroupId": group["thingGroupId"],
        })
    return error_response_json(
        "ResourceAlreadyExistsException",
        f"{message} with different properties",
        409,
        extra={"resourceArn": None, "resourceId": None},
    )


def _create_thing_group(name: str, payload: dict) -> tuple:
    if name in _thing_groups:
        return error_response_json(
            "ResourceAlreadyExistsException",
            f"ThingGroup {name!r} already exists",
            409,
        )
    err = _tags_error(payload.get("tags"), required=False, missing_value=_TAG_WITHOUT_VALUE)
    if err:
        return err
    props = payload.get("thingGroupProperties") or {}
    attr_payload = props.get("attributePayload") or {}
    parent = payload.get("parentGroupName")
    if parent and parent not in _thing_groups:
        return error_response_json(
            "InvalidRequestException", f"Cannot create group using non-existing group as parent : {parent}", 400
        )
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
    if parent:
        # DescribeThingGroup reports the parent under thingGroupMetadata, as
        # the API reference documents it.
        record["thingGroupMetadata"] = {
            "parentGroupName": parent,
            "creationDate": record["creationDate"],
        }
    _thing_groups[name] = record
    _set_created_tags(record["thingGroupArn"], payload.get("tags"))
    _thing_group_event("CREATED", record)
    if parent:
        _hierarchy_event("ADDED", _thing_groups[parent], record)
    return json_response({
        "thingGroupName": name,
        "thingGroupArn": record["thingGroupArn"],
        "thingGroupId": record["thingGroupId"],
    })


def _describe_thing_group(name: str) -> tuple:
    g = _thing_groups.get(name)
    if g is None:
        return _error_not_found("ThingGroup", name)
    return json_response({k: v for k, v in g.items() if k != "tags"})


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
        return _error_not_found("ThingGroupMember", middle)
    return json_response({"things": list(g.get("things", []))})


def _list_thing_groups_for_thing(path: str) -> tuple:
    """``GET /things/{thingName}/thing-groups``.

    The reverse of ``ListThingsInThingGroup``: the forward lookup existed, but
    resolving which groups a *thing* belongs to meant scanning every group. The
    membership store is bidirectional, so this reads the thing record's
    ``thingGroupNames`` directly.

    ``maxResults`` / ``nextToken`` are not honored — the full list is returned
    in one page, like the other IoT list operations here.
    """
    middle = path[len("/things/"):-len("/thing-groups")]
    if "/" in middle:
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: GET {path}", 400
        )
    thing = _things.get(middle)
    if thing is None:
        return _error_not_found("ListThingGroupsForThing", middle)
    return json_response({
        "thingGroups": [
            {"groupName": g, "groupArn": _thing_group_arn(g)}
            for g in thing.get("thingGroupNames", [])
            # Membership is kept consistent on both sides, but a group deleted
            # out from under a stale reference must not surface here.
            if g in _thing_groups
        ]
    })


def _update_thing_group(name: str, payload: dict) -> tuple:
    g = _thing_groups.get(name)
    if g is None:
        return _error_not_found("UpdateThingGroup", name)
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
    _thing_group_event("UPDATED", g)
    return json_response({"version": g["version"]})


def _delete_thing_group(name: str) -> tuple:
    g = _thing_groups.get(name)
    if g is None:
        return json_response({})
    if any((c.get("thingGroupMetadata") or {}).get("parentGroupName") == name for c in _thing_groups.values()):
        return error_response_json(
            "InvalidRequestException",
            f"Cannot delete thing group : {name} when there are still child groups attached to it", 400,
        )
    # Remove group from any Things that referenced it
    for tname in list(g.get("things", [])):
        thing = _things.get(tname)
        if thing and name in thing.get("thingGroupNames", []):
            thing["thingGroupNames"].remove(name)
            _things[tname] = thing
            _membership_event("REMOVED", g, thing)
    _thing_group_event("DELETED", g)
    parent = _thing_groups.get((g.get("thingGroupMetadata") or {}).get("parentGroupName") or "")
    if parent:
        _hierarchy_event("REMOVED", parent, g)
    del _thing_groups[name]
    _tags.pop(g["thingGroupArn"], None)
    return json_response({})


async def _add_thing_to_group(payload: dict) -> tuple:
    """Async because a CONTINUOUS job the thing joins publishes
    jobs/notify(-next) through the broker."""
    gname = payload.get("thingGroupName")
    tname = payload.get("thingName")
    if not gname or not tname:
        return error_response_json(
            "InvalidRequestException", "thingGroupName and thingName are required", 400
        )
    group = _thing_groups.get(gname)
    if group is None:
        return _error_not_found("ThingGroupMember", gname)
    thing = _things.get(tname)
    if thing is None:
        return _error_not_found("Thing", tname)
    joined = tname not in group.get("things", [])
    if joined:
        prev_next = jobs_first_pending_job_id(tname)
        group.setdefault("things", []).append(tname)
        _thing_groups[gname] = group
        _membership_event("ADDED", group, thing)
    if gname not in thing.get("thingGroupNames", []):
        thing.setdefault("thingGroupNames", []).append(gname)
        _things[tname] = thing
    if joined and _jobs_thing_joined_group(tname, _thing_group_arn(gname)):
        await jobs_notify_thing(get_account_id(), get_region(), tname, prev_next)
    return json_response({})


async def _remove_thing_from_group(payload: dict) -> tuple:
    """Async because a QUEUED CONTINUOUS execution the removal retires
    publishes jobs/notify(-next) through the broker."""
    gname = payload.get("thingGroupName")
    tname = payload.get("thingName")
    group = _thing_groups.get(gname) if gname else None
    thing = _things.get(tname) if tname else None
    if thing is None:
        return _error_not_found("Thing", tname or "")
    if group is None:
        # Only a named group that does not exist answers 200; a group ARN is not resolved.
        return json_response({}) if gname else _error_not_found("ThingGroup", "")
    prev_next = jobs_first_pending_job_id(tname)
    left = tname in group.get("things", [])
    if left:
        group["things"].remove(tname)
        _thing_groups[gname] = group
        _membership_event("REMOVED", group, thing)
    if gname in thing.get("thingGroupNames", []):
        thing["thingGroupNames"].remove(gname)
        _things[tname] = thing
    if _jobs_thing_left_group(tname, _thing_group_arn(gname) if left else None):
        await jobs_notify_thing(get_account_id(), get_region(), tname, prev_next)
    return json_response({})


# ---------------------------------------------------------------------------
# Provisioning templates
#
# CRUD, CreateProvisioningClaim and the fleet provisioning MQTT API (JSON
# topics) that applies a template to a certificate created over MQTT, once the
# template's pre-provisioning hook allows it; with AUTH=true RegisterThing
# answers AccessDenied unless provisioningRoleArn names a role IoT may assume
# that allows every write it makes. Template versions are not
# modeled: a template always carries defaultVersionId 1. Wire shapes and
# error messages mirror a live probe of the real service (2026-08-26).
# ---------------------------------------------------------------------------

# Template names are shorter and stricter than most IoT names: no colons,
# 36 chars max.
_PROVISIONING_TEMPLATE_NAME_RE = re.compile(r"^[0-9A-Za-z_-]{1,36}$")


def _validate_provisioning_template_name(name: str | None) -> tuple | None:
    if not name or not _PROVISIONING_TEMPLATE_NAME_RE.match(name):
        return error_response_json(
            "InvalidRequestException",
            "Invalid templateName: must match [0-9A-Za-z_-]{1,36}",
            400,
        )
    return None


def _provisioning_template_resource_error(resource, certificates: int) -> str | None:
    """Why the service refuses one resource of a template body, or None."""
    props = resource.get("Properties")
    if props is None:
        return "Properties of a resource cannot be null."
    if resource.get("Type") != "AWS::IoT::Certificate":
        return None
    if certificates:
        return "Only one certificate resource can be defined"
    if not isinstance(props, dict) or props.get("CertificateId") != {"Ref": "AWS::IoT::Certificate::Id"}:
        return "CertificateId must contain a reference to AWS::IoT::Certificate::Id"
    status = props.get("Status")
    if not isinstance(status, str) or status.lower() not in ("active", "inactive"):
        return 'A certificate resource must specify a Status of either "Active" or "Inactive"'
    return None


def _validate_provisioning_template_body(body) -> tuple | None:
    """Refuse a body the live service refuses, with its messages verbatim."""
    doc = body
    if not isinstance(doc, dict):
        try:
            doc = json.loads(body)
        except (TypeError, ValueError):
            doc = None
    if not isinstance(doc, dict):
        return error_response_json(
            "InvalidRequestException",
            "The template body is invalid",
            400,
        )
    resources = doc.get("Resources")
    values = resources.values() if isinstance(resources, dict) else ()
    certificates = 0
    for resource in values:
        if not isinstance(resource, dict):
            continue
        reason = _provisioning_template_resource_error(resource, certificates)
        if reason:
            return error_response_json(
                "InvalidRequestException", f"The template body is invalid: {reason}", 400
            )
        certificates += resource.get("Type") == "AWS::IoT::Certificate"
    if not certificates:
        return error_response_json(
            "InvalidRequestException",
            "The template body is invalid: Template must have certificate resource.",
            400,
        )
    return None


_PROVISIONING_HOOK_PAYLOAD_VERSION = "2020-04-01"
_LAMBDA_FUNCTION_ARN_RE = re.compile(
    r"^arn:aws[a-z-]*:lambda:[a-z0-9-]+:\d{12}:function:[A-Za-z0-9_-]+(:[A-Za-z0-9$_-]+)?$"
)


def _validate_provisioning_hook(hook, template_type: str, template_arn: str) -> tuple | None:
    """The live service's checks of a ``preProvisioningHook``, the function's policy last."""
    hook = hook if isinstance(hook, dict) else {}
    version, target = hook.get("payloadVersion"), hook.get("targetArn")
    if version is not None and not re.fullmatch(r"[0-9-]+", str(version)):
        return error_response_json(
            "InvalidRequestException",
            "1 validation error detected: Value at 'preProvisioningHook.payloadVersion' failed "
            "to satisfy constraint: Member must satisfy regular expression pattern: ^[0-9-]+$",
            400,
        )
    if template_type == "JITP":
        return error_response_json(
            "InvalidRequestException",
            "Provisioning hook is not supported for template type: JITP",
            400,
        )
    if not isinstance(target, str) or not _LAMBDA_FUNCTION_ARN_RE.match(target):
        return error_response_json("InvalidRequestException", "Invalid arguments in the call", 400)
    if version not in (None, _PROVISIONING_HOOK_PAYLOAD_VERSION):
        return error_response_json(
            "InvalidRequestException",
            "The hook version cannot be supported by AWS Iot, the supported versions are: "
            f"[{_PROVISIONING_HOOK_PAYLOAD_VERSION}]",
            400,
        )
    if _provisioning_hook_function(target, template_arn) is None:
        return error_response_json(
            "InvalidRequestException", f"Access denied during lambda validation: {target}", 400
        )
    return None


def _provisioning_hook(hook: dict) -> dict:
    """A validated hook as the live service stores it, with its default payloadVersion."""
    return {
        "payloadVersion": hook.get("payloadVersion") or _PROVISIONING_HOOK_PAYLOAD_VERSION,
        "targetArn": hook["targetArn"],
    }


def _provisioning_hook_function(target_arn: str, template_arn: str) -> dict | None:
    """The hook function's execution record, if its policy lets iot.amazonaws.com invoke it for the template."""
    from ministack.core.iam_evaluator import EvalContext, evaluate_resource_policy
    from ministack.services import lambda_svc

    base, _config, _name = lambda_svc._get_base_func_record_for_ref(target_arn)
    func, config, _name = lambda_svc._get_func_record_for_ref(target_arn)
    if base is None or func is None or config is None:
        return None
    ctx = EvalContext(
        principal_arn="*",
        principal_type="Service",
        principal_account="",
        action="lambda:InvokeFunction",
        resource_arn=target_arn,
        region=get_region(),
        service_context={"aws:sourcearn": template_arn, "aws:sourceaccount": get_account_id()},
    )
    if evaluate_resource_policy(base.get("policy"), ctx, service="iot.amazonaws.com").decision != "Allow":
        return None
    return lambda_svc._execution_record_for_config(func, config)


def _provisioning_template_not_found(name: str) -> tuple:
    return error_response_json(
        "ResourceNotFoundException", f"Unable to find template named {name}", 404
    )


def _handle_provisioning_template(method: str, path: str, body: bytes, qp: dict) -> tuple:
    suffix = path[len("/provisioning-templates/"):]
    name, _, sub = suffix.partition("/")
    version = sub[len("versions/"):] if sub.startswith("versions/") else None
    routes = {("POST", "provisioning-claim"), ("POST", "versions"), ("GET", "versions")}
    if sub and (method, sub) not in routes and not (
        version and "/" not in version and method in ("GET", "DELETE")
    ):
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
        )
    err = _validate_provisioning_template_name(name)
    if err:
        return err
    if sub == "provisioning-claim":
        return _create_provisioning_claim(name)
    if sub == "versions" and method == "POST":
        return _create_provisioning_template_version(name, _parse_body(body), qp)
    if sub == "versions":
        return _list_provisioning_template_versions(name)
    if version is not None and method == "GET":
        return _describe_provisioning_template_version(name, version)
    if version is not None:
        return _delete_provisioning_template_version(name, version)
    if method == "GET":
        return _describe_provisioning_template(name)
    if method == "PATCH":
        return _update_provisioning_template(name, _parse_body(body))
    if method == "DELETE":
        return _delete_provisioning_template(name)
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


def _create_provisioning_template(payload: dict) -> tuple:
    name = payload.get("templateName")
    err = _validate_provisioning_template_name(name)
    if err:
        return err
    for field in ("templateBody", "provisioningRoleArn"):
        if not payload.get(field):
            return error_response_json(
                "InvalidRequestException", f"{field} is required", 400
            )
    err = (_validate_provisioning_template_body(payload["templateBody"])
           or _tags_error(payload.get("tags"), required=False))
    if err:
        return err
    hook = payload.get("preProvisioningHook")
    if hook:
        err = _validate_provisioning_hook(
            hook, payload.get("type", "FLEET_PROVISIONING"), _provisioning_template_arn(name)
        )
        if err:
            return err
    if name in _provisioning_templates:
        return error_response_json(
            "ResourceAlreadyExistsException",
            f"Template with name {name} already exists",
            409,
        )
    now = _now_epoch()
    record = {
        "templateName": name,
        "templateArn": _provisioning_template_arn(name),
        "enabled": bool(payload.get("enabled", False)),
        "provisioningRoleArn": payload["provisioningRoleArn"],
        "type": payload.get("type", "FLEET_PROVISIONING"),
        "defaultVersionId": 1,
        "creationDate": now,
        "lastModifiedDate": now,
        "versions": {"1": _provisioning_template_version(1, payload["templateBody"], now)},
        "lastVersionId": 1,
    }
    # The live service omits description from Describe when it was never set —
    # absent, not empty — so an unset one is not stored at all.
    if payload.get("description"):
        record["description"] = payload["description"]
    if hook:
        record["preProvisioningHook"] = _provisioning_hook(hook)
    _provisioning_templates[name] = record
    _set_created_tags(record["templateArn"], payload.get("tags"))
    logger.info("IoT provisioning template created: %s", name)
    return json_response({
        "templateArn": record["templateArn"],
        "templateName": name,
        "defaultVersionId": 1,
    })


def _provisioning_template_version(version_id: int, body, created: float) -> dict:
    return {"versionId": version_id, "creationDate": created, "templateBody": body}


def _provisioning_template_body(t: dict):
    """The body of the template's default version."""
    return t["versions"][str(t["defaultVersionId"])]["templateBody"]


def _describe_provisioning_template(name: str) -> tuple:
    t = _provisioning_templates.get(name)
    if t is None:
        return _provisioning_template_not_found(name)
    out = {k: v for k, v in t.items() if k not in ("versions", "lastVersionId")}
    return json_response({**out, "templateBody": _provisioning_template_body(t)})


def _list_provisioning_templates(qp: dict) -> tuple:
    """``GET /provisioning-templates`` — no pagination (house convention).

    Summaries carry exactly what the live service's do — in particular no
    ``description``, even for a template that has one.
    """
    summary_keys = (
        "templateArn", "templateName",
        "creationDate", "lastModifiedDate", "enabled", "type",
    )
    return json_response({
        "templates": [
            {k: t[k] for k in summary_keys}
            for t in _provisioning_templates.values()
        ]
    })


def _update_provisioning_template(name: str, payload: dict) -> tuple:
    t = _provisioning_templates.get(name)
    if t is None:
        return _provisioning_template_not_found(name)
    version = payload.get("defaultVersionId")
    if version is not None and str(version) not in t["versions"]:
        return error_response_json(
            "InvalidRequestException",
            f"Unable to set version {version} as the default, version does not exist",
            400,
        )
    hook = None if payload.get("removePreProvisioningHook") else payload.get("preProvisioningHook")
    if hook:
        err = _validate_provisioning_hook(hook, t["type"], t["templateArn"])
        if err:
            return err
    if "description" in payload:
        # Same absent-not-empty rule as create: clearing removes the key.
        if payload["description"]:
            t["description"] = payload["description"]
        else:
            t.pop("description", None)
    for field in ("enabled", "provisioningRoleArn"):
        if field in payload:
            t[field] = payload[field]
    if payload.get("removePreProvisioningHook"):
        t.pop("preProvisioningHook", None)
    elif hook:
        t["preProvisioningHook"] = _provisioning_hook(hook)
    if version is not None:
        t["defaultVersionId"] = int(version)
    t["lastModifiedDate"] = _now_epoch()
    _provisioning_templates[name] = t
    # The live service answers 200 with an empty body — not an empty JSON
    # object.
    return 200, {"Content-Type": "application/x-amz-json-1.0"}, b""


def _delete_provisioning_template(name: str) -> tuple:
    if _provisioning_templates.get(name) is None:
        return error_response_json("ResourceNotFoundException", "Unable to delete template, template not found", 404)
    _tags.pop(_provisioning_templates.pop(name)["templateArn"], None)
    logger.info("IoT provisioning template deleted: %s", name)
    return json_response({})


_PROVISIONING_TEMPLATE_VERSIONS_MAX = 5


def _create_provisioning_template_version(name: str, payload: dict, qp: dict) -> tuple:
    """``CreateProvisioningTemplateVersion``: version ids count up and are never reused."""
    t = _provisioning_templates.get(name)
    if t is None:
        return _provisioning_template_not_found(name)
    body = payload.get("templateBody")
    if not body:
        return error_response_json("InvalidRequestException", "templateBody is required", 400)
    err = _validate_provisioning_template_body(body)
    if err:
        return err
    if len(t["versions"]) >= _PROVISIONING_TEMPLATE_VERSIONS_MAX:
        return error_response_json(
            "VersionsLimitExceededException",
            f"Can't create a template version for the template {name}, "
            "the maximum number of template versions has been reached",
            409,
        )
    version_id = t["lastVersionId"] + 1
    now = _now_epoch()
    t["versions"][str(version_id)] = _provisioning_template_version(version_id, body, now)
    t["lastVersionId"] = version_id
    is_default = str(qp.get("setAsDefault", "")).lower() == "true"
    if is_default:
        t["defaultVersionId"] = version_id
    t["lastModifiedDate"] = now
    _provisioning_templates[name] = t
    return json_response({
        "templateArn": t["templateArn"],
        "templateName": name,
        "versionId": version_id,
        "isDefaultVersion": is_default,
    })


def _list_provisioning_template_versions(name: str) -> tuple:
    """``ListProvisioningTemplateVersions``: all versions in one page, whatever maxResults asks for."""
    t = _provisioning_templates.get(name)
    if t is None:
        return error_response_json("ResourceNotFoundException", f"Template named {name} not found", 404)
    return json_response({"versions": [
        {
            "versionId": v["versionId"],
            "creationDate": v["creationDate"],
            "isDefaultVersion": v["versionId"] == t["defaultVersionId"],
        }
        for v in t["versions"].values()
    ]})


def _describe_provisioning_template_version(name: str, version_id: str) -> tuple:
    t = _provisioning_templates.get(name)
    if t is None:
        return error_response_json("ResourceNotFoundException", f"Unable to find a template named {name}", 404)
    v = t["versions"].get(version_id)
    if v is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"Unable to find a template version with ID {version_id} for template {name}",
            404,
        )
    return json_response({**v, "isDefaultVersion": v["versionId"] == t["defaultVersionId"]})


def _delete_provisioning_template_version(name: str, version_id: str) -> tuple:
    t = _provisioning_templates.get(name)
    if t is None:
        return _provisioning_template_not_found(name)
    v = t["versions"].get(version_id)
    if v is None:
        return error_response_json(
            "ResourceNotFoundException", f"Unable to find template version {version_id}", 404
        )
    if v["versionId"] == t["defaultVersionId"]:
        return error_response_json(
            "InvalidRequestException",
            f"Unable to delete template version {version_id}, it is the default version",
            400,
        )
    del t["versions"][version_id]
    t["lastModifiedDate"] = _now_epoch()
    _provisioning_templates[name] = t
    return json_response({})


# A claim certificate expires five minutes after it is issued, at the end of its
# X.509 validity; an ownership token expires after an hour.
_PROVISIONING_CLAIM_SECONDS = 300
_OWNERSHIP_TOKEN_SECONDS = 3600


def _drop_expired(store: AccountRegionScopedDict) -> None:
    """Forget the claims or ownership tokens past their expiration, in every scope."""
    now = time.time()
    for key, record in list(store._data.items()):
        if record["expiration"] <= now:
            store._data.pop(key, None)


def _create_provisioning_claim(name: str) -> tuple:
    """``CreateProvisioningClaim``: a claim certificate kept out of the certificate registry."""
    if _provisioning_templates.get(name) is None:
        return _provisioning_template_not_found(name)
    try:
        ca_cert_pem, ca_key_pem = _ensure_ca()
        cert_pem, private_pem, public_pem = sign_leaf_certificate(
            ca_cert_pem=ca_cert_pem,
            ca_key_pem=ca_key_pem,
            common_name="AWS IoT Certificate",
            days_valid=_PROVISIONING_CLAIM_SECONDS / 86400,
        )
    except RuntimeError as e:
        return error_response_json("InternalFailureException", str(e), 503)
    from cryptography import x509

    leaf = x509.load_pem_x509_certificate(cert_pem.encode("utf-8"))
    not_after = getattr(leaf, "not_valid_after_utc", None) or leaf.not_valid_after.replace(
        tzinfo=timezone.utc
    )
    cert_id = get_certificate_id(cert_pem)
    _drop_expired(_provisioning_claims)
    _provisioning_claims[cert_id] = {
        "certificateId": cert_id,
        "templateName": name,
        "expiration": not_after.timestamp(),
    }
    return json_response({
        "certificateId": cert_id,
        "certificatePem": cert_pem,
        "keyPair": {"PublicKey": public_pem, "PrivateKey": private_pem},
        "expiration": not_after.timestamp(),
    })


# The fleet provisioning MQTT API (JSON payloads). A request is answered on the
# connection that published it, and nowhere else.
_FLEET_CREATE_TOPIC = "$aws/certificates/create/json"
_FLEET_TEMPLATES_PREFIX = "$aws/provisioning-templates/"
_FLEET_PROVISION_SUFFIX = "/provision/json"
_CERTIFICATE_ID_PARAMETER = "AWS::IoT::Certificate::Id"


def _parse_fleet_provisioning_topic(topic: str) -> tuple[str, str | None] | None:
    """``("create", None)`` for CreateKeysAndCertificate, ``("provision", template)`` for RegisterThing."""
    if topic == _FLEET_CREATE_TOPIC:
        return "create", None
    if topic.startswith(_FLEET_TEMPLATES_PREFIX) and topic.endswith(_FLEET_PROVISION_SUFFIX):
        name = topic[len(_FLEET_TEMPLATES_PREFIX):-len(_FLEET_PROVISION_SUFFIX)]
        if name and "/" not in name:
            return "provision", name
    return None


def _fleet_rejected(code: str, message: str, status: int = 400) -> tuple[str, dict]:
    return "rejected", {"statusCode": status, "errorCode": code, "errorMessage": message}


class _ProvisioningRejected(Exception):
    """A RegisterThing request the template cannot be applied to."""

    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.reply = _fleet_rejected(code, message, status)


def _provisioning_failed(message: str) -> _ProvisioningRejected:
    """A registry refusal, worded the way AWS relays it on ``/rejected``."""
    return _ProvisioningRejected(
        "ProvisioningFailed",
        f"{message} (Service: AWSIot; Status Code: 400; Error Code: "
        f"ResourceRegistrationFailureException; Request ID: {new_uuid()}; Proxy: null)",
    )


async def _fleet_provisioning_reply(
    account_id: str, region: str, operation: str, template_name: str | None, payload: bytes,
    connection: dict,
) -> tuple[str, dict]:
    """The reply suffix (``accepted`` or ``rejected``) and document for one request."""
    if operation == "create":
        return await _fleet_create_keys_and_certificate(account_id, region)
    try:
        return await _fleet_register_thing(account_id, region, template_name, payload, connection)
    except _ProvisioningRejected as e:
        return e.reply


async def _fleet_create_keys_and_certificate(account_id: str, region: str) -> tuple[str, dict]:
    """``CreateKeysAndCertificate`` over MQTT: a PENDING_ACTIVATION certificate and its ownership token."""

    def _issue() -> tuple[str, str, str]:
        ca_cert_pem, ca_key_pem = _ensure_ca()
        return sign_leaf_certificate(ca_cert_pem, ca_key_pem, "AWS IoT Certificate")

    try:
        # RSA key generation, kept off the broker's loop.
        cert_pem, private_pem, _public_pem = await asyncio.to_thread(_issue)
    except RuntimeError as e:
        return _fleet_rejected("InternalFailure", str(e), 500)
    cert_id = get_certificate_id(cert_pem)
    token = secrets.token_urlsafe(48)
    with request_scope(account_id, region):
        _certificates[cert_id] = _certificate_record(cert_id, cert_pem, "PENDING_ACTIVATION")
        _drop_expired(_ownership_tokens)
        _ownership_tokens[token] = {
            "certificateId": cert_id,
            "expiration": time.time() + _OWNERSHIP_TOKEN_SECONDS,
        }
    return "accepted", {
        "certificateId": cert_id,
        "certificatePem": cert_pem,
        "privateKey": private_pem,
        "certificateOwnershipToken": token,
    }


async def _fleet_register_thing(
    account_id: str, region: str, template_name: str, payload: bytes, connection: dict
) -> tuple[str, dict]:
    """``RegisterThing``: apply the template to the certificate the ownership token stands for."""
    try:
        request = json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        request = None
    if not isinstance(request, dict):
        return _fleet_rejected("InvalidPayload", "Message cannot be parsed")
    with request_scope(account_id, region):
        template = _provisioning_templates.get(template_name)
        if template is None:
            return _fleet_rejected(
                "InvalidTemplate", f"Template with name {template_name} does not exist."
            )
        if not template.get("enabled"):
            return _fleet_rejected("InvalidTemplate", f"Template {template_name} is not enabled.")
        token = request.get("certificateOwnershipToken")
        if token is None or token == "":
            return _fleet_rejected(
                "InvalidCertificateOwnershipToken", "Certificate ownership token cannot be empty."
            )
        record = _ownership_tokens.get(token) if isinstance(token, str) else None
        cert = (
            _certificates.get(record["certificateId"])
            if record is not None and record["expiration"] > time.time()
            else None
        )
        if cert is None:
            return _fleet_rejected(
                "InvalidCertificateOwnershipToken", "Certificate ownership token is invalid."
            )
        parameters = request.get("parameters")
        parameters = parameters if isinstance(parameters, dict) else {}
        if template.get("preProvisioningHook"):
            parameters = await _run_provisioning_hook(template, {
                **connection,
                "certificateId": cert["certificateId"],
                "certificatePem": cert["certificatePem"],
                "templateArn": template["templateArn"],
                "parameters": parameters,
            })
            cert = _certificates.get(cert["certificateId"])
            if cert is None:
                return _fleet_rejected(
                    "InvalidCertificateOwnershipToken", "Certificate ownership token is invalid."
                )
        plan = _provisioning_plan(template, parameters, cert["certificateId"])
        thing_name = await _apply_provisioning_plan(plan, cert, template.get("provisioningRoleArn") or "")
    reply = {"deviceConfiguration": plan["deviceConfiguration"]}
    if thing_name is not None:
        reply["thingName"] = thing_name
    return "accepted", reply


# A hook that has not answered six seconds after it was invoked fails the request.
_PROVISIONING_HOOK_SECONDS = 6
_HOOK_INVALID_RESULT = ("InvalidResult", "Provisioning hook result is in an invalid format.")


async def _run_provisioning_hook(template: dict, event: dict) -> dict:
    """Invoke the template's pre-provisioning hook: the parameters with its overrides, or a rejection."""
    from ministack.services import lambda_svc

    exec_record = _provisioning_hook_function(
        template["preProvisioningHook"]["targetArn"], template["templateArn"]
    )
    if exec_record is None:
        raise _ProvisioningRejected("InvocationError", "Access denied while invoking Lambda function")
    invocation = run_reentrant(
        lambda_svc._execute_function_with_config_scope, exec_record, event,
        thread_name="ministack-iot-provisioning-hook",
    )
    try:
        # Past the limit the function keeps running; only its answer is dropped.
        result = await asyncio.wait_for(asyncio.shield(invocation), _PROVISIONING_HOOK_SECONDS)
    except Exception:
        result = None
    if not isinstance(result, dict) or result.get("throttle"):
        raise _ProvisioningRejected(
            "InvocationError", "A failure occurred while invoking the provisioning hook."
        )
    if result.get("error"):
        raise _ProvisioningRejected(
            "InvocationError", "The provisioning hook was not executed successfully."
        )
    return {**event["parameters"], **_provisioning_hook_overrides(result)}


def _provisioning_hook_overrides(result: dict) -> dict:
    """The ``parameterOverrides`` of an answer that allows provisioning, or a rejection."""
    answer = result.get("body")
    if isinstance(answer, bytes) or (isinstance(answer, str) and result.get("raw_payload")):
        try:
            answer = json.loads(answer)
        except ValueError:
            raise _ProvisioningRejected(*_HOOK_INVALID_RESULT) from None
    if answer is None:
        raise _ProvisioningRejected("InvalidResult", "The provisioning hook did not return a response.")
    answer = answer if isinstance(answer, dict) else {}
    allow, overrides = answer.get("allowProvisioning"), answer.get("parameterOverrides")
    # Read the way a Java boolean field reads JSON: "true", "TRUE" and 1 allow.
    if isinstance(allow, str):
        allow = {"true": True, "false": False}.get(allow.lower())
    elif not isinstance(allow, (bool, int, float)):
        allow = None
    if allow is None or not isinstance(overrides, (dict, type(None))) or any(
        isinstance(value, (dict, list)) for value in (overrides or {}).values()
    ):
        raise _ProvisioningRejected(*_HOOK_INVALID_RESULT)
    if not allow:
        raise _ProvisioningRejected("AccessDenied", "Access Denied", 403)
    # Numbers and booleans become strings; null leaves the parameter unresolved.
    return {
        name: value if value is None or isinstance(value, str) else json.dumps(value)
        for name, value in (overrides or {}).items()
    }


def _provisioning_plan(template: dict, parameters: dict, cert_id: str) -> dict:
    """The template's resources with ``Ref`` and the intrinsic functions resolved against the parameters."""
    body = _provisioning_template_body(template)
    body = json.loads(body) if isinstance(body, str) else body
    declared = body.get("Parameters")
    values = {
        name: spec["Default"]
        for name, spec in (declared.items() if isinstance(declared, dict) else ())
        if isinstance(spec, dict) and "Default" in spec
    }
    values.update(parameters)
    values[_CERTIFICATE_ID_PARAMETER] = cert_id
    mappings = body.get("Mappings") if isinstance(body.get("Mappings"), dict) else {}

    def invalid(function):
        return _ProvisioningRejected(
            "InvalidTemplate", f"Template {template['templateName']} has an invalid {function}."
        )

    def reference(name):
        if values.get(name) is None:
            raise _ProvisioningRejected("InvalidParameters", f"Cannot resolve reference value: {name}")
        return values[name]

    def substitute(text, variables):
        def one(match):
            name = match.group(1)
            if name.startswith("!"):
                return "${" + name[1:] + "}"
            return str(variables[name]) if name in variables else str(reference(name))

        return re.sub(r"\$\{([^}]*)\}", one, text)

    def resolve(value):
        if isinstance(value, list):
            return [resolve(v) for v in value]
        if not isinstance(value, dict):
            return value
        if len(value) == 1:
            function, argument = next(iter(value.items()))
            if function == "Ref":
                return reference(argument)
            if not function.startswith("Fn::"):
                return {function: resolve(argument)}
            argument = resolve(argument)
            if function == "Fn::Join":
                if not (isinstance(argument, list) and len(argument) == 2 and isinstance(argument[1], list)):
                    raise invalid(function)
                return str(argument[0]).join(str(v) for v in argument[1])
            if function == "Fn::Select":
                if not (isinstance(argument, list) and len(argument) == 2 and isinstance(argument[1], list)):
                    raise invalid(function)
                try:
                    index = int(argument[0])
                except (TypeError, ValueError):
                    raise invalid(function) from None
                if not 0 <= index < len(argument[1]) or argument[1][index] is None:
                    raise invalid(function)
                return argument[1][index]
            if function == "Fn::Split":
                if not (isinstance(argument, list) and len(argument) == 2
                        and isinstance(argument[0], str) and isinstance(argument[1], str) and argument[0]):
                    raise invalid(function)
                return argument[1].split(argument[0])
            if function == "Fn::FindInMap":
                if not (isinstance(argument, list) and len(argument) == 3):
                    raise invalid(function)
                found = mappings.get(argument[0])
                found = found.get(argument[1]) if isinstance(found, dict) else None
                found = found.get(argument[2]) if isinstance(found, dict) else None
                if found is None:
                    raise invalid(function)
                return found
            if function == "Fn::Sub":
                if isinstance(argument, str):
                    return substitute(argument, {})
                if (isinstance(argument, list) and len(argument) == 2
                        and isinstance(argument[0], str) and isinstance(argument[1], dict)):
                    return substitute(argument[0], argument[1])
                raise invalid(function)
            raise invalid(function)
        return {k: resolve(v) for k, v in value.items()}

    plan = {"thing": None, "status": "ACTIVE", "policies": []}
    resources = body.get("Resources")
    for resource in resources.values() if isinstance(resources, dict) else ():
        if not isinstance(resource, dict):
            continue
        properties = resolve(resource.get("Properties") or {})
        kind = resource.get("Type")
        if kind == "AWS::IoT::Thing":
            plan["thing"] = (properties, resource.get("OverrideSettings") or {})
        elif kind == "AWS::IoT::Certificate":
            plan["status"] = str(properties.get("Status") or "ACTIVE").upper()
        elif kind == "AWS::IoT::Policy":
            document = properties.get("PolicyDocument")
            if document is None:
                plan["policies"].append((properties.get("PolicyName"), None))
                continue
            if "PolicyName" in properties:
                raise invalid("AWS::IoT::Policy")
            document = document if isinstance(document, str) else json.dumps(document)
            try:
                json.loads(document)
            except ValueError:
                raise invalid("AWS::IoT::Policy") from None
            # The developer guide: PolicyName "Defaults to a hash of the policy document".
            plan["policies"].append((hashlib.sha256(document.encode("utf-8")).hexdigest(), document))
    plan["deviceConfiguration"] = resolve(body.get("DeviceConfiguration") or {})
    return plan


def _java_map(attributes: dict) -> str:
    return "{" + ", ".join(f"{k}={v}" for k, v in sorted(attributes.items())) + "}"


async def _apply_provisioning_plan(plan: dict, cert: dict, role_arn: str) -> str | None:
    """Create or update the thing, then activate and attach the certificate; a rejection changes nothing."""
    account = get_account_id()
    for policy, document in plan["policies"]:
        if document is None and _policies.get(policy) is None:
            raise _provisioning_failed(
                "Register thing workflow execution terminates due to: Policy default version not found"
            )
    cert_arn = cert["certificateArn"]
    requests = [("iot:UpdateCertificate", cert_arn)]
    for policy, document in plan["policies"]:
        if document is not None and _policies.get(policy) is None:
            requests.append(("iot:CreatePolicy", _policy_arn(policy)))
        requests.append(("iot:AttachPolicy", cert_arn))
    name = update = None
    join = leave = []
    if plan["thing"] is not None:
        properties, overrides = plan["thing"]
        name = properties.get("ThingName")
        if not isinstance(name, str) or not _NAME_RE.match(name):
            raise _provisioning_failed("Invalid thingName: must match [a-zA-Z0-9:_-]{1,128}")
        attributes = properties.get("AttributePayload")
        type_name = properties.get("ThingTypeName")
        groups = properties.get("ThingGroups")
        if type_name and type_name not in _thing_types:
            raise _provisioning_failed(
                "Register thing workflow execution terminates due to: "
                f"Thing Type {account}:{type_name} not found."
            )
        for group in groups or ():
            if group not in _thing_groups:
                raise _provisioning_failed(
                    "Register thing workflow execution terminates due to: "
                    f"ThingGroup {account}:{group} not found."
                )
        thing = _things.get(name)
        if thing is None:
            join = list(groups or ())
            requests.append(("iot:CreateThing", _thing_arn(name)))
        else:
            update, join, leave = _provisioning_thing_changes(
                thing, attributes, type_name, groups, overrides
            )
            if update:
                requests.append(("iot:UpdateThing", _thing_arn(name)))
        requests += [("iot:AddThingToThingGroup", _thing_group_arn(g)) for g in join]
        requests += [("iot:RemoveThingFromThingGroup", _thing_group_arn(g)) for g in leave]
        requests.append(("iot:AttachThingPrincipal", _thing_arn(name)))
    if not _provisioning_role_allows(role_arn, requests):
        raise _ProvisioningRejected("AccessDenied", "Access Denied", 403)
    if name is not None:
        if _things.get(name) is None:
            _create_thing(name, {
                "attributePayload": {"attributes": attributes or {}},
                "thingTypeName": type_name or None,
            })
        elif update:
            _update_thing(name, update)
        for group in join:
            await _add_thing_to_group({"thingGroupName": group, "thingName": name})
        for group in leave:
            await _remove_thing_from_group({"thingGroupName": group, "thingName": name})
        thing = _things[name]
        if cert_arn not in thing.setdefault("principals", []):
            thing["principals"].append(cert_arn)
            _things[name] = thing
        if thing["thingArn"] not in cert.setdefault("attachedThings", []):
            cert["attachedThings"].append(thing["thingArn"])
    cert["status"] = plan["status"]
    _certificates[cert["certificateId"]] = cert
    for policy, document in plan["policies"]:
        if document is not None and _policies.get(policy) is None:
            _create_policy(policy, {"policyDocument": document})
        _change_policy_target(policy, cert_arn, attach=True)
    return name


def _provisioning_role_allows(role_arn: str, requests: list[tuple[str, str]]) -> bool:
    """Under AUTH=true, whether the template's provisioning role is one IoT may assume and allows every write."""
    from ministack import app

    if not app.AUTH:
        return True
    if not _iot_role_usable(role_arn):
        return False
    from ministack.core.iam_evaluator import (
        EvalContext,
        _gather_role_policies,
        _permissions_boundary,
        evaluate,
    )
    from ministack.services import iam as _iam

    account = get_account_id()
    role_name = role_arn.rsplit("/", 1)[-1]
    policies = _gather_role_policies(role_name, account)
    limits = _permissions_boundary(_iam._roles, role_name, account)
    principal = f"arn:aws:sts::{account}:assumed-role/{role_name}/IoTProvisioning"
    for action, resource in requests:
        ctx = EvalContext(
            principal_arn=principal,
            principal_type="AssumedRole",
            principal_account=account,
            action=action,
            resource_arn=resource,
            region=get_region(),
        )
        if evaluate(ctx, policies, *limits).decision != "Allow":
            return False
    return True


def _provisioning_thing_changes(
    thing: dict, attributes, type_name, groups, overrides: dict
) -> tuple[dict, list, list]:
    """The UpdateThing payload and the groups to join and leave for a thing that exists."""
    update: dict = {}
    join: list = []
    leave: list = []
    if attributes is not None:
        current = thing.get("attributes") or {}
        setting = overrides.get("AttributePayload")
        if setting == "MERGE":
            attributes = {**current, **attributes}
        if setting in ("REPLACE", "MERGE"):
            update["attributePayload"] = {"attributes": attributes}
        elif setting != "DO_NOTHING" and attributes != current:
            raise _provisioning_failed(
                f"Existing AttributePayload: {_java_map(current)} conflicts with "
                f"AttributePayload: {_java_map(attributes)} in Template."
            )
    if type_name is not None:
        current = thing.get("thingTypeName")
        setting = overrides.get("ThingTypeName")
        if setting == "REPLACE":
            update["thingTypeName"] = type_name
        elif setting != "DO_NOTHING" and type_name != current:
            raise _provisioning_failed(
                f"Existing ThingType: {current or 'null'} conflicts with "
                f"ThingType: {type_name} in Template."
            )
    if groups is not None:
        current = list(thing.get("thingGroupNames") or [])
        setting = overrides.get("ThingGroups")
        if setting in ("REPLACE", "MERGE"):
            join = [g for g in groups if g not in current]
            leave = [g for g in current if g not in groups] if setting == "REPLACE" else []
        elif setting != "DO_NOTHING" and set(groups) != set(current):
            existing = ", ".join(
                f"{{GroupName: {g},GroupArn: {_thing_group_arn(g)}}}" for g in current
            )
            raise _provisioning_failed(
                f"Existing ThingGroups: [{existing}] conflict with "
                f"ThingGroups: [{', '.join(groups)}] in Template."
            )
    return update, join, leave


# ---------------------------------------------------------------------------
# Certificates
# ---------------------------------------------------------------------------


def _certificate_record(
    cert_id: str, cert_pem: str, status: str, ca_id: str | None = None
) -> dict:
    """The registry record of a device certificate, owned by the current scope; it takes its CA's mode."""
    ca = _ca_certificates.get(ca_id) if ca_id else None
    return {
        "certificateId": cert_id,
        "certificateArn": _cert_arn(cert_id),
        "certificatePem": cert_pem,  # verbatim
        "status": status,
        "certificateMode": (ca or {}).get("certificateMode", "DEFAULT"),
        "creationDate": _now_epoch(),
        "ownedBy": get_account_id(),
        "caCertificateId": ca_id,
        "attachedThings": [],
        "attachedPolicies": [],
    }


async def _publish_certificate_registered(
    account_id: str,
    region: str,
    record: dict,
    source_ip: str | None,
    registration_timestamp: str | None,
) -> None:
    """Publish the JITR event to ``$aws/events/certificates/registered/{caId}``.

    AWS sends it when a device connects with a certificate its CA
    auto-registers: ``certificateRegistrationTimestamp`` is null on the connect
    that created the certificate and the creation time in epoch milliseconds,
    as a string, on every later connect while it is still PENDING_ACTIVATION.
    """
    ca_id = record["caCertificateId"]
    event = {
        "certificateId": record["certificateId"],
        "caCertificateId": ca_id,
        "timestamp": int(time.time() * 1000),
        "certificateStatus": record["status"],
        "awsAccountId": account_id,
        "certificateRegistrationTimestamp": registration_timestamp,
        "sourceIp": source_ip,
    }
    await _publish_event(
        account_id, region, f"$aws/events/certificates/registered/{ca_id}", event
    )


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
    record = _certificate_record(cert_id, cert_pem, "ACTIVE" if set_active else "INACTIVE")
    _certificates[cert_id] = record
    return json_response({
        "certificateArn": record["certificateArn"],
        "certificateId": cert_id,
        "certificatePem": cert_pem,
        "keyPair": {
            "PublicKey": public_pem,
            "PrivateKey": private_pem,
        },
    })


def _create_certificate_from_csr(payload: dict, qp: dict) -> tuple:
    """``POST /certificates``: sign the caller's CSR with the Local CA.

    The certificate keeps the CSR's subject and key and is INACTIVE unless
    ``setAsActive``; a CSR that does not parse or verify, or whose key AWS
    does not accept, is refused with the message AWS uses for all of these.
    """
    try:
        ca_cert_pem, ca_key_pem = _ensure_ca()
        cert_pem = sign_certificate_request(
            ca_cert_pem, ca_key_pem, payload.get("certificateSigningRequest") or "")
    except ValueError:
        return error_response_json("InvalidRequestException", "CSR violates constraints", 400)
    except RuntimeError as e:
        return error_response_json("InternalFailureException", str(e), 503)
    cert_id = get_certificate_id(cert_pem)
    record = _certificate_record(
        cert_id, cert_pem, "ACTIVE" if _qp_bool(qp, "setAsActive") else "INACTIVE")
    _certificates[cert_id] = record
    return json_response({
        "certificateArn": record["certificateArn"],
        "certificateId": cert_id,
        "certificatePem": cert_pem,
    })


async def _register_certificate_without_ca(payload: dict, qp: dict) -> tuple:
    """``RegisterCertificateWithoutCA``: a certificate registered without a
    CA is in ``SNI_ONLY`` mode, which DescribeCertificate reports."""
    response = await _register_certificate(payload, qp, without_ca=True)
    if response[0] == 200:
        cert_id = json.loads(response[2])["certificateId"]
        _certificates[cert_id] = {**_certificates[cert_id], "certificateMode": "SNI_ONLY"}
    return response


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


def _registered_issuer_id(cert_pem: str) -> str | None:
    """The id of the registered CA that signed ``cert_pem``, an ACTIVE one first, or None."""
    issuer_id = None
    for ca in _ca_certificates.values():
        if certificate_is_signed_by(cert_pem, ca.get("certificatePem") or ""):
            if ca.get("status") == "ACTIVE":
                return ca["certificateId"]
            issuer_id = issuer_id or ca["certificateId"]
    return issuer_id


def _not_issued_by_error(cert_pem: str, ca_pem: str) -> tuple | None:
    """400 when ``ca_pem`` did not issue ``cert_pem``, checking the issuer name before the signature."""
    from cryptography import x509

    cert = x509.load_pem_x509_certificate(cert_pem.encode("utf-8"))
    ca = x509.load_pem_x509_certificate(ca_pem.encode("utf-8"))
    if cert.issuer != ca.subject:
        return error_response_json(
            "CertificateValidationException",
            "The issuer of the certificate did not match the subject of the "
            "provided CA certificate.",
            400,
        )
    if not certificate_is_signed_by(cert_pem, ca_pem):
        return error_response_json(
            "CertificateValidationException",
            "The signature of the certificate failed verification against the "
            "provided CA certificate.",
            400,
        )
    return None


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

    ``caCertificatePem`` must name an ACTIVE CA registered via
    ``RegisterCACertificate`` in this account/region that really signed the
    leaf; anything else is refused. Without it, ``RegisterCertificate`` links
    the certificate to the registered CA that signed it, as AWS does, so
    DescribeCertificate names that CA and ListCertificatesByCA lists the
    certificate, and refuses it while that CA is not ACTIVE. Registering
    publishes no JITR event, whatever the CA's ``autoRegistrationStatus``: AWS
    sends that event only from a device connect (see ``_mtls_auto_register``),
    and refuses a registration with status PENDING_ACTIVATION.
    """
    cert_pem = payload.get("certificatePem") or qp.get("certificatePem")
    if not cert_pem:
        return error_response_json(
            "InvalidRequestException", "certificatePem is required", 400
        )
    status = payload.get("status")
    if status == "PENDING_ACTIVATION":
        return error_response_json(
            "CertificateStateException",
            "Not allowed to register certificate with PENDING_ACTIVATION status",
            406,
        )
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
    if ca_id is not None:
        if ca_id not in _ca_certificates:
            return error_response_json(
                "CertificateStateException",
                "No CA certificate exists for the given certificate",
                406,
            )
        refused = _not_issued_by_error(cert_pem, ca_pem)
        if refused is not None:
            return refused
    elif not without_ca:
        ca_id = _registered_issuer_id(cert_pem)
    # A CA that is not ACTIVE refuses the leaf, ahead of the duplicate check.
    if ca_id is not None and _ca_certificates[ca_id]["status"] != "ACTIVE":
        return error_response_json(
            "CertificateStateException", "The CA certificate is not active", 406
        )
    if cert_id in _certificates:
        return _certificate_already_exists(cert_id)
    record = _certificate_record(
        cert_id, cert_pem, status or ("ACTIVE" if set_active else "INACTIVE"), ca_id
    )
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


_CERTIFICATE_ID_RE = re.compile(r"(0x)?[a-fA-F0-9]+")
_MARKER_RE = re.compile(r"[A-Za-z0-9+/]+={0,2}")
_BY_CA_MARKER_PREFIX = "certificates-by-ca"


def _by_ca_sort_key(record: dict) -> tuple[float, str]:
    """Creation order, the certificate id breaking ties within one timestamp."""
    return record.get("creationDate") or 0.0, record["certificateId"]


def _by_ca_marker(record: dict) -> str:
    """An opaque page cursor naming the last certificate of a page.

    It is the position, not an offset: AWS continues after that certificate in
    whatever order the next request asks for, so a marker from a descending
    page followed by ``isAscendingOrder=true`` yields the newer certificates.
    Standard base64, so it matches the ``Marker`` pattern botocore checks.
    """
    created, cert_id = _by_ca_sort_key(record)
    raw = f"{_BY_CA_MARKER_PREFIX}:{created!r}:{cert_id}"
    return base64.b64encode(raw.encode()).decode()


def _by_ca_marker_position(marker: str) -> tuple[float, str] | None:
    """The (creationDate, certificateId) a marker stands for, or None."""
    try:
        raw = base64.b64decode(marker, validate=True).decode()
        prefix, created, cert_id = raw.split(":")
        if prefix != _BY_CA_MARKER_PREFIX:
            return None
        return float(created), cert_id
    except ValueError:
        return None


def _list_certificates_by_ca(path: str, qp: dict) -> tuple:
    """``GET /certificates-by-ca/{caCertificateId}`` (``ListCertificatesByCA``).

    Lists the device certificates whose record names the CA: those registered
    with that ``caCertificatePem``, in every status as on AWS, and those the
    CA's auto-registration created. ``RegisterCertificateWithoutCA`` leaves
    are not listed even when the CA issued them, as on AWS. Newest first unless
    ``isAscendingOrder=true``. A well-formed id that names no CA is an empty
    list, not an error.
    """
    ca_id = path[len("/certificates-by-ca/"):]
    errors = []
    if not _CERTIFICATE_ID_RE.fullmatch(ca_id):
        errors.append(
            "Value at 'caCertificateId' failed to satisfy constraint: Member must "
            "satisfy regular expression pattern: (0x)?[a-fA-F0-9]+"
        )
    if len(ca_id) != 64:
        bound = "greater than or equal to" if len(ca_id) < 64 else "less than or equal to"
        errors.append(
            "Value at 'caCertificateId' failed to satisfy constraint: Member must "
            f"have length {bound} 64"
        )
    page_size = qp.get("pageSize")
    if page_size is not None:
        try:
            page_size = int(page_size)
        except ValueError:
            return error_response_json(
                "InvalidRequestException", "The request is not valid.", 400
            )
        if page_size < 1:
            errors.append(
                "Value at 'pageSize' failed to satisfy constraint: Member must have "
                "value greater than or equal to 1"
            )
        elif page_size > 250:
            errors.append(
                "Value at 'pageSize' failed to satisfy constraint: Member must have "
                "value less than or equal to 250"
            )
    marker = qp.get("marker")
    if marker is not None and not _MARKER_RE.fullmatch(marker):
        errors.append(
            "Value at 'marker' failed to satisfy constraint: Member must satisfy "
            "regular expression pattern: [A-Za-z0-9+/]+={0,2}"
        )
    if errors:
        noun = "error" if len(errors) == 1 else "errors"
        return error_response_json(
            "InvalidRequestException",
            f"{len(errors)} validation {noun} detected: " + "; ".join(errors),
            400,
        )
    position = None
    if marker is not None:
        position = _by_ca_marker_position(marker)
        if position is None:
            return error_response_json(
                "InvalidRequestException",
                "Invalid/Malformed marker passed for listCertificateByCA",
                400,
            )
    ascending = _qp_bool(qp, "isAscendingOrder")
    matched = sorted(
        (c for c in _certificates.values() if c.get("caCertificateId") == ca_id),
        key=_by_ca_sort_key,
        reverse=not ascending,
    )
    if position is not None:
        matched = [
            c for c in matched
            if (_by_ca_sort_key(c) > position if ascending else _by_ca_sort_key(c) < position)
        ]
    page = matched[:page_size or 250]
    body = {
        "certificates": [
            {
                "certificateArn": c["certificateArn"],
                "certificateId": c["certificateId"],
                "status": c["status"],
                "creationDate": c.get("creationDate"),
            }
            for c in page
        ]
    }
    if len(matched) > len(page):
        body["nextMarker"] = _by_ca_marker(page[-1])
    return json_response(body)


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
            "certificateMode": record.get("certificateMode", "DEFAULT"),
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
                f"Certificate must be deactivated (not ACTIVE) before deletion. Id: {cert_id}",
                406,
            )
        # Attached things refuse even forceDelete; attached policies only
        # refuse without it, and forceDelete detaches them.
        arn = record["certificateArn"]
        if any(_thing_name_from_arn(t) in _things for t in record.get("attachedThings", [])):
            return error_response_json(
                "DeleteConflictException",
                f"Things must be detached before deletion (arn: {arn})",
                409,
            )
        policies = _policies_attached_to(arn)
        if policies and not _qp_bool(qp, "forceDelete"):
            return error_response_json(
                "DeleteConflictException",
                f"Certificate policies must be detached before deletion (arn: {arn})",
                409,
            )
        for policy in policies:
            _change_policy_target(policy["policyName"], arn, attach=False)
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
        return json_response({"registrationCode": _registration_code()})
    # DELETE
    _registration_codes.pop("code", None)
    return json_response({})


def _registration_code() -> str:
    """The account/region registration code, minted on first use."""
    code = _registration_codes.get("code")
    if not code:
        code = hashlib.sha256(os.urandom(32)).hexdigest()
        _registration_codes["code"] = code
    return code


def _verification_certificate_error(
    ca_pem: str, verification_pem: str | None, certificate_mode: str
) -> tuple | None:
    """Error response for a verification certificate the mode refuses, else None."""
    from cryptography import x509
    from cryptography.x509.oid import NameOID

    if certificate_mode == "SNI_ONLY":
        if not verification_pem:
            return None
        return error_response_json(
            "InvalidRequestException",
            "Cannot provide verification certificate for registering CA "
            "Certificates in SNI_ONLY mode",
            400,
        )
    if not verification_pem:
        return error_response_json(
            "InvalidRequestException",
            "Need to provide verification certificate for registering CA "
            "Certificates in DEFAULT mode",
            400,
        )
    try:
        cert = x509.load_pem_x509_certificate(verification_pem.encode("utf-8"))
    except Exception:
        return error_response_json(
            "InvalidRequestException",
            "Contents of the Verification Certificate are not correct",
            400,
        )
    refused = _not_issued_by_error(verification_pem, ca_pem)
    if refused is not None:
        return refused
    common_names = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)
    if [a.value for a in common_names] != [_registration_code()]:
        return error_response_json(
            "RegistrationCodeValidationException",
            "Registration code provided in the CN field of verification "
            "certificate is invalid.",
            400,
        )
    return None


def _register_ca_certificate(payload: dict, qp: dict) -> tuple:
    """``POST /cacertificate`` (``RegisterCACertificate``).

    Body members per the botocore model: ``caCertificate`` (required),
    ``verificationCertificate`` (required in ``DEFAULT`` mode: signed by the
    CA, with the registration code as its CN; refused in ``SNI_ONLY`` mode)
    and ``certificateMode`` (``DEFAULT`` when omitted, as on AWS).
    ``setAsActive`` / ``allowAutoRegistration`` ride as query-string booleans,
    as on AWS.
    """
    ca_pem = payload.get("caCertificate")
    if not ca_pem:
        return error_response_json(
            "InvalidRequestException", "caCertificate is required", 400
        )
    registration_config = payload.get("registrationConfig")
    if registration_config is not None and not isinstance(registration_config, dict):
        return error_response_json(
            "InvalidRequestException", "registrationConfig must be an object", 400
        )
    certificate_mode = payload.get("certificateMode") or "DEFAULT"
    if certificate_mode not in ("DEFAULT", "SNI_ONLY"):
        return error_response_json(
            "InvalidRequestException",
            "certificateMode must be one of ['DEFAULT', 'SNI_ONLY']",
            400,
        )
    try:
        ca_id = get_certificate_id(ca_pem)
    except Exception as e:
        return error_response_json(
            "CertificateValidationException",
            f"Invalid CA certificate PEM: {e}",
            400,
        )
    refused = _verification_certificate_error(
        ca_pem, payload.get("verificationCertificate"), certificate_mode
    ) or _tags_error(payload.get("tags"), required=False)
    if refused is not None:
        return refused
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
        "certificateMode": certificate_mode,
        "creationDate": _now_epoch(),
        "ownedBy": get_account_id(),
    }
    if registration_config is not None:
        record["registrationConfig"] = registration_config
    _ca_certificates[ca_id] = record
    _set_created_tags(record["certificateArn"], payload.get("tags"))
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
                # .get: a record restored from a snapshot taken before the
                # field existed carries no mode; AWS's default stands in.
                "certificateMode": record.get("certificateMode", "DEFAULT"),
                "ownedBy": record["ownedBy"],
                "creationDate": record.get("creationDate"),
            },
            # registrationConfig is a sibling of certificateDescription in
            # DescribeCACertificateResponse, omitted when never configured.
            **({"registrationConfig": record["registrationConfig"]}
               if record.get("registrationConfig") is not None else {}),
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
        # Body members per the model: registrationConfig replaces the stored
        # JITR provisioning config; removeAutoRegistration, when true, turns
        # auto-registration off.
        registration_config = payload.get("registrationConfig")
        if registration_config is not None:
            if not isinstance(registration_config, dict):
                return error_response_json(
                    "InvalidRequestException",
                    "registrationConfig must be an object", 400,
                )
            record["registrationConfig"] = registration_config
        if payload.get("removeAutoRegistration"):
            record["autoRegistrationStatus"] = "DISABLE"
        _ca_certificates[ca_id] = record
        return json_response({})
    if method == "DELETE":
        if record["status"] == "ACTIVE":
            # Wording verified against real AWS (live probe 2026-08-26).
            return error_response_json(
                "CertificateStateException",
                "CA Certificate must be deactivated (not ACTIVE) before "
                f"deletion. Id: {ca_id}",
                406,
            )
        del _ca_certificates[ca_id]
        _tags.pop(record["certificateArn"], None)
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
    operation = {"PUT": "AttachThingPrincipal", "DELETE": "DetachThingPrincipal"}.get(method, "ListThingPrincipals")
    if thing is None:
        return _error_not_found(operation, name, principal=headers.get("x-amzn-principal") or qp.get("principal"))

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
        return _error_not_found(f"{operation}:principal", name, principal=principal)

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
        return _error_not_found("ListPrincipalThings", principal)
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
        if method == "PATCH":
            return _set_default_policy_version(name, version_id)

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
    err = _tags_error(payload.get("tags"), required=False)
    if err:
        return err
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
    _set_created_tags(record["policyArn"], payload.get("tags"))
    return json_response({
        "policyName": name,
        "policyArn": record["policyArn"],
        "policyDocument": doc,
        "policyVersionId": "1",
    })


def _get_policy(name: str) -> tuple:
    p = _policies.get(name)
    if p is None:
        return _error_not_found("GetPolicy", name)
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
    if len(p["versions"]) > 1:
        return error_response_json(
            "DeleteConflictException",
            f"The policy cannot be deleted as multiple versions of the policy exist (name={name}). "
            "Use DeletePolicyVersion action to delete all non-default versions of the policy and retry.",
            409,
        )
    if p.get("targets"):
        return error_response_json(
            "DeleteConflictException",
            f"The policy cannot be deleted as the policy is attached to one or more principals (name={name})",
            409,
        )
    del _policies[name]
    _tags.pop(p["policyArn"], None)
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
        return _error_not_found("PolicyVersion", version_id, policy=name)
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
            for vid, v in sorted(p["versions"].items(), key=lambda item: int(item[0]), reverse=True)
        ]
    })


def _delete_policy_version(name: str, version_id: str) -> tuple:
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    if version_id not in p["versions"]:
        return _error_not_found("PolicyVersion", version_id, policy=name)
    if p["defaultVersionId"] == version_id:
        return error_response_json(
            "DeleteConflictException",
            f"Cannot delete a default policy version (name={name}, versionId={version_id})",
            409,
        )
    del p["versions"][version_id]
    _policies[name] = p
    return json_response({})


def _set_default_policy_version(name: str, version_id: str) -> tuple:
    """SetDefaultPolicyVersion: 200 with an empty body, also for the current default."""
    if not version_id.isdigit():
        return error_response_json(
            "InvalidRequestException",
            "1 validation error detected: Value at 'policyVersionId' failed to satisfy constraint: "
            "Member must satisfy regular expression pattern: [0-9]+",
            400,
        )
    p = _policies.get(name)
    if p is None:
        return error_response_json("ResourceNotFoundException", "Policy not found", 404)
    if version_id not in p["versions"]:
        return error_response_json(
            "ResourceNotFoundException",
            f"Policy version does not exist (name={name}, versionId={version_id})",
            404,
        )
    for vid, v in p["versions"].items():
        v["isDefaultVersion"] = vid == version_id
    p["defaultVersionId"] = version_id
    _policies[name] = p
    return 200, {}, b""


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
    # DetachPolicy of an unknown policy succeeds; DetachPrincipalPolicy refuses it.
    if method != "PUT" and name not in _policies:
        return json_response({})
    return _change_policy_target(
        name, _parse_body(body).get("target"), attach=method == "PUT"
    )


def _change_policy_target(name: str, target: str | None, *, attach: bool, principal: bool = False) -> tuple:
    """``AttachPolicy`` / ``DetachPolicy`` over the policy and certificate stores.

    The data core behind both the modern ``/target-policies/{policyName}``
    route and the deprecated ``/principal-policies/{policyName}`` one, so
    neither has to synthesize a request for the other. ``principal`` selects
    the deprecated call's refusals.
    """
    p = _policies.get(name)
    if p is None:
        return _error_not_found("Policy", name)
    if not target:
        return error_response_json(
            "InvalidRequestException", "target is required", 400
        )
    if attach:
        err = _policy_target_error(target, principal=principal)
        if err:
            return err
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


# A Cognito identity id, the one target that is not an ARN.
_COGNITO_IDENTITY_ID_RE = re.compile(r"[\w-]+:[0-9a-f-]+")


def _policy_target_error(target: str, *, principal: bool) -> tuple | None:
    """AttachPolicy's refusal of ``target`` (AttachPrincipalPolicy's with ``principal``), or ``None``."""
    if _COGNITO_IDENTITY_ID_RE.fullmatch(target):
        return None
    try:
        spec = parse_arn(target)
    except ArnParseError:
        spec = None
    if spec is None:
        if principal and target.startswith("arn:"):
            return error_response_json("InvalidRequestException", "ARN not in proper syntax.", 400)
        message = "Invalid principal type" if principal else "Invalid Target"
        return error_response_json("InvalidRequestException", message, 400)
    kind, _, resource_id = spec.resource.partition("/")
    if spec.service != "iot" or kind not in (("cert",) if principal else ("cert", "thinggroup")):
        message = "The given ARN does not represent a cert" if principal else f"Invalid Target: {target}"
        return error_response_json("InvalidRequestException", message, 400)
    if spec.account_id != get_account_id():
        return error_response_json(
            "AccessDeniedException",
            "Cross account not allowed. Please check the account of the resource specified.",
            403,
        )
    if spec.region != get_region() and not principal:
        return error_response_json("InvalidRequestException", "Invalid Target", 400)
    if kind == "thinggroup":
        if resource_id in _thing_groups:
            return None
        return error_response_json(
            "ResourceNotFoundException",
            f"Cannot find requested thing group for accountId: {get_account_id()} and thing group name: "
            f"{resource_id}",
            404,
        )
    if resource_id in _certificates:
        return None
    return error_response_json(
        "ResourceNotFoundException", "The certificate given in the principal does not exist.", 404
    )


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
    return _change_policy_target(name, principal, attach=method == "PUT", principal=True)


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
# Registry event configuration
# ---------------------------------------------------------------------------

_EVENT_TYPES = (
    "CA_CERTIFICATE", "CERTIFICATE", "JOB", "JOB_EXECUTION", "POLICY", "THING",
    "THING_GROUP", "THING_GROUP_HIERARCHY", "THING_GROUP_MEMBERSHIP", "THING_TYPE",
    "THING_TYPE_ASSOCIATION",
)
_EVENT_CONFIG_KEY = "events"


def _event_configuration() -> dict:
    """The account+region's event configuration; every type is off until the first update."""
    stored = _event_config.get(_EVENT_CONFIG_KEY) or {}
    return {
        "eventConfigurations": {
            t: {"Enabled": t in stored.get("enabled", ())} for t in _EVENT_TYPES
        },
        "creationDate": stored.get("creationDate"),
        "lastModifiedDate": stored.get("lastModifiedDate"),
    }


def _describe_event_configurations() -> tuple:
    """``GET /event-configurations``."""
    return json_response(_event_configuration())


def _update_event_configurations(payload: dict) -> tuple:
    """``PATCH /event-configurations`` — types the request names change, the rest keep their state."""
    configurations = payload.get("eventConfigurations")
    if not isinstance(configurations, dict):
        return error_response_json(
            "InvalidRequestException", "Configuration cannot be null", 400
        )
    if set(configurations) - set(_EVENT_TYPES):
        return error_response_json(
            "InvalidRequestException",
            "1 validation error detected: Value at 'eventConfigurations' failed to "
            "satisfy constraint: Map keys must satisfy constraint: [Member must "
            f"satisfy enum value set: [{', '.join(_EVENT_TYPES)}]]",
            400,
        )
    stored = _event_config.get(_EVENT_CONFIG_KEY) or {}
    enabled = set(stored.get("enabled", ()))
    for event_type, configuration in configurations.items():
        if isinstance(configuration, dict) and configuration.get("Enabled") is True:
            enabled.add(event_type)
        else:
            enabled.discard(event_type)
    now = _now_epoch()
    _event_config[_EVENT_CONFIG_KEY] = {
        "enabled": sorted(enabled),
        "creationDate": stored.get("creationDate", now),
        "lastModifiedDate": now,
    }
    return json_response({})


# Registry events raised while handling a request, published once the handler returns.
_pending_events: contextvars.ContextVar[list | None] = contextvars.ContextVar(
    "iot_pending_events", default=None
)


async def _publish_event(account_id: str, region: str, topic: str, event: dict) -> None:
    """Publish an ``$aws/events`` message; a broker failure never undoes the operation behind it."""
    try:
        await broker_publish(
            account_id, region, topic, json.dumps(event).encode("utf-8"), qos=0
        )
    except Exception:
        logger.warning("IoT event publish failed on %s", topic, exc_info=True)


@contextlib.asynccontextmanager
async def publishing_events():
    """Collect the ``$aws/events`` messages raised inside the block and publish them after it."""
    events: list = []
    token = _pending_events.set(events)
    try:
        yield
    finally:
        _pending_events.reset(token)
    for account_id, region, topic, event in events:
        await _publish_event(account_id, region, topic, event)


def _event_wanted(config_type: str) -> bool:
    """Whether events are being collected and ``config_type`` is enabled for the account and region."""
    enabled = (_event_config.get(_EVENT_CONFIG_KEY) or {}).get("enabled", ())
    return _pending_events.get() is not None and config_type in enabled


def _registry_event(
    config_type: str, topic: str, event_type: str, operation: str, fields: dict,
    event_id: str | None = None, with_account: bool = True,
) -> None:
    """Queue ``$aws/events/{topic}`` when ``config_type`` is enabled for the request's account and region."""
    if not _event_wanted(config_type):
        return
    _pending_events.get().append((get_account_id(), get_region(), f"$aws/events/{topic}", {
        "eventType": event_type,
        "eventId": event_id or uuid.uuid4().hex,
        "timestamp": int(time.time() * 1000),
        "operation": operation,
        **({"accountId": get_account_id()} if with_account else {}),
        **fields,
    }))


def _thing_events(operation: str, thing: dict, old_type: str | None) -> None:
    """THING event plus the type association changes, which share its eventId."""
    event_id = uuid.uuid4().hex
    name = thing["thingName"]
    _registry_event("THING", f"thing/{name}/{operation.lower()}", "THING_EVENT", operation, {
        "thingId": thing["thingId"],
        "thingName": name,
        "versionNumber": thing.get("version", 1),
        "thingTypeName": thing.get("thingTypeName"),
        "billinGroupName": None,
        "attributes": dict(thing.get("attributes") or {}),
    }, event_id)
    new_type = None if operation == "DELETED" else thing.get("thingTypeName")
    if old_type == new_type:
        return
    for type_name, change in ((new_type, "ADDED"), (old_type, "REMOVED")):
        if type_name:
            _registry_event(
                "THING_TYPE_ASSOCIATION",
                f"thingTypeAssociation/thing/{name}/thingType/{type_name}/{change.lower()}",
                "THING_TYPE_ASSOCIATION_EVENT", change,
                {"thingId": thing["thingId"], "thingName": name, "thingTypeName": type_name},
                event_id, with_account=False,
            )


def _thing_type_event(operation: str, thing_type: dict) -> None:
    props = thing_type.get("thingTypeProperties") or {}
    meta = thing_type.get("thingTypeMetadata") or {}
    deprecated_at = meta.get("deprecationDate")
    name = thing_type["thingTypeName"]
    _registry_event("THING_TYPE", f"thingType/{name}/{operation.lower()}", "THING_TYPE_EVENT", operation, {
        "thingTypeId": thing_type["thingTypeId"],
        "thingTypeName": name,
        "isDeprecated": bool(meta.get("deprecated")),
        "deprecationDate": int(deprecated_at * 1000) if deprecated_at else None,
        "searchableAttributes": props.get("searchableAttributes") or None,
        "propagatingAttributes": (props.get("mqtt5Configuration") or {}).get("propagatingAttributes") or None,
        "description": props.get("thingTypeDescription"),
    })


def _thing_group_event(operation: str, group: dict) -> None:
    props = group.get("thingGroupProperties") or {}
    parent_name = (group.get("thingGroupMetadata") or {}).get("parentGroupName")
    ancestors = []
    ancestor = _thing_groups.get(parent_name) if parent_name else None
    while ancestor:
        ancestors.insert(0, {"groupArn": ancestor["thingGroupArn"], "groupId": ancestor["thingGroupId"]})
        up = (ancestor.get("thingGroupMetadata") or {}).get("parentGroupName")
        ancestor = _thing_groups.get(up) if up else None
    name = group["thingGroupName"]
    _registry_event("THING_GROUP", f"thingGroup/{name}/{operation.lower()}", "THING_GROUP_EVENT", operation, {
        "thingGroupId": group["thingGroupId"],
        "thingGroupName": name,
        "versionNumber": group.get("version", 1),
        "parentGroupName": parent_name,
        "parentGroupId": ancestors[-1]["groupId"] if ancestors else None,
        "description": props.get("thingGroupDescription"),
        "rootToParentThingGroups": ancestors or None,
        "attributes": (props.get("attributePayload") or {}).get("attributes") or None,
        "dynamicGroupMappingId": None,
    })


def _hierarchy_event(operation: str, parent: dict, child: dict) -> None:
    _registry_event(
        "THING_GROUP_HIERARCHY",
        f"thingGroupHierarchy/thingGroup/{parent['thingGroupName']}"
        f"/childThingGroup/{child['thingGroupName']}/{operation.lower()}",
        "THING_GROUP_HIERARCHY_EVENT", operation, {
            "thingGroupId": parent["thingGroupId"],
            "thingGroupName": parent["thingGroupName"],
            "childGroupId": child["thingGroupId"],
            "childGroupName": child["thingGroupName"],
        },
    )


def _membership_event(operation: str, group: dict, thing: dict) -> None:
    _registry_event(
        "THING_GROUP_MEMBERSHIP",
        f"thingGroupMembership/thingGroup/{group['thingGroupName']}"
        f"/thing/{thing['thingName']}/{operation.lower()}",
        "THING_GROUP_MEMBERSHIP_EVENT", operation, {
            "groupArn": group["thingGroupArn"],
            "groupId": group["thingGroupId"],
            "thingArn": thing["thingArn"],
            "thingId": thing["thingId"],
            "membershipId": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{group['thingGroupId']}/{thing['thingId']}")),
        },
    )


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
    "principal",
    "regexp_matches",
    "replace",
    "timestamp",
    "topic",
    "traceid",
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
        # Documented as the client id "or n/a if the message wasn't sent over MQTT".
        return client_id if client_id else "n/a"
    if name == "principal" and not args:
        return _rule_message_identity.get()[0] or _MISSING
    if name == "traceid" and not args:
        return _rule_message_identity.get()[1] or _MISSING
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

# AWS refuses `!=` ("Unexpected character '!'") and `==` ("Unexpected token,
# 'EQ'") — captured eu-north-1 2026-09-19. Longest first so <= beats <.
_WHERE_OPERATORS = ("<>", "<=", ">=", "=", "<", ">")
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
    if op == "=":
        # Equality does not convert: on AWS a mismatched pair is simply unequal.
        return _where_values_equal(left, right)
    if op == "<>":
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


class RuleRepublishError(ValueError):
    """A republish action inside the rule's own topic filter, raised by `put_topic_rule`."""


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
    if pred:
        # A nested SELECT (the documented EXISTS-subquery form) is refused by
        # AWS itself: "Unexpected token, 'SELECT'" (captured eu-north-1
        # 2026-09-19). Literals are blanked first so 'select' inside a string
        # is not mistaken for the keyword.
        if re.search(r"\bSELECT\b", _SQL_LITERAL_RE.sub("''", pred), re.IGNORECASE):
            return f"Unexpected token, 'SELECT' in WHERE clause: {pred}"
        if _parse_where(pred) is None:
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


def _republish_inside_filter(sql: str, actions) -> str | None:
    """The refusal when an action republishes (templates unexpanded) into the rule's FROM filter, else None."""
    filter_ = _rule_topic_filter(sql)
    if not filter_:
        return None
    for action in actions or []:
        republish = action.get("republish") if isinstance(action, dict) else None
        target = republish.get("topic") if isinstance(republish, dict) else None
        if not isinstance(target, str):
            continue
        # `$$` is the documented escape for a topic that starts with `$`.
        if target.startswith("$$"):
            target = target[1:]
        # As in MQTT, a filter that starts with a wildcard does not match a `$` topic.
        if target.startswith("$") and filter_[:1] in ("#", "+"):
            continue
        if _topic_matches(filter_, target):
            return f"Republish topic cannot be within topic space of rule's topic pattern '{filter_}'"
    return None


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
    Raises `RuleRepublishError` for a republish action inside the rule's own
    topic filter (the errorAction is not checked).
    """
    sql = payload.get("sql", "")
    error = _validate_rule_sql(sql)
    if error:
        raise RuleSqlError(error)
    refusal = _republish_inside_filter(sql, payload.get("actions"))
    if refusal:
        raise RuleRepublishError(refusal)
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
    _tags.pop(_topic_rule_arn(name), None)


def _handle_topic_rule(method: str, path: str, body: bytes, headers: dict) -> tuple:
    name = path[len("/rules/"):]
    if method == "POST" and name.endswith(("/enable", "/disable")):
        name, _, verb = name.rpartition("/")
        return _set_topic_rule_disabled(name, verb == "disable")
    if method == "POST":
        return _create_topic_rule(name, _parse_body(body), headers.get("x-amz-tagging"))
    if method == "GET":
        return _get_topic_rule(name)
    if method == "PATCH":
        return _replace_topic_rule(name, _parse_body(body))
    if method == "DELETE":
        return _delete_topic_rule(name)
    return error_response_json(
        "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
    )


def _create_topic_rule(name: str, payload: dict, tagging: str | None = None) -> tuple:
    """CreateTopicRule; its tags arrive as the ``x-amz-tagging`` header, ``k1=v1&k2=v2``."""
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
        pairs = parse_qsl(tagging or "", keep_blank_values=True, strict_parsing=True)
    except ValueError:
        return error_response_json(
            "InvalidRequestException",
            "Invalid tag parameters, please make sure the tags are in correct format with tagKeys "
            "not exceeding 128 characters and tagValues not exceeding 256 characters", 400)
    tags = [{"Key": k, "Value": v} for k, v in pairs]
    err = _tags_error(tags, required=False)
    if err:
        return err
    try:
        put_topic_rule(name, payload)
    except RuleSqlError as exc:
        return error_response_json("SqlParseException", str(exc), 400)
    except (RuleRoleError, RuleRepublishError) as exc:
        return error_response_json("InvalidRequestException", str(exc), 400)
    _set_created_tags(_topic_rule_arn(name), tags)
    return json_response({})


def _replace_topic_rule(name: str, payload: dict) -> tuple:
    if name not in _topic_rules:
        return _error_not_found("Rule", name)
    try:
        put_topic_rule(name, payload)
    except RuleSqlError as exc:
        return error_response_json("SqlParseException", str(exc), 400)
    except (RuleRoleError, RuleRepublishError) as exc:
        return error_response_json("InvalidRequestException", str(exc), 400)
    return json_response({})


def _get_topic_rule(name: str) -> tuple:
    rule = _topic_rules.get(name)
    if rule is None:
        return _error_not_found("Rule", name)
    return json_response({"ruleArn": _topic_rule_arn(name), "rule": rule})


def _set_topic_rule_disabled(name: str, disabled: bool) -> tuple:
    """EnableTopicRule and DisableTopicRule: 200 with an empty body, also when nothing changes."""
    if not _RULE_NAME_RE.match(name):
        return error_response_json(
            "InvalidRequestException",
            f"1 validation error detected: Value '{name}' at 'ruleName' failed to satisfy "
            "constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9_]+$",
            400,
        )
    rule = _topic_rules.get(name)
    if rule is None:
        return _error_not_found("Rule", name)
    rule["ruleDisabled"] = disabled
    return 200, {}, b""


def _delete_topic_rule(name: str) -> tuple:
    delete_topic_rule(name)
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
# Tags (TagResource / UntagResource / ListTagsForResource)
# ---------------------------------------------------------------------------

_TAG_KEY_PATTERN = r"^([\p{L}\p{Z}\p{N}_.:/=+\-@]*)$"
_TAGS_PER_RESOURCE = 50
_TOO_MANY_TAGS = "Too many tags provided in the request. Please check the input."
_INVALID_TAG_REQUEST = "Invalid request exception. Please check the input."
# What CreateThingType/CreateThingGroup and CreateJob/CreateJobTemplate answer for a tag without a value.
_TAG_WITHOUT_VALUE = "All tags in request should have keys and values."
_INVALID_JOB_TAG = "Invalid tag parameter exception. Please check the input."

# ARN resource type -> (the store keyed by what follows the type, the message of
# TagResource and UntagResource for a resource that does not exist)
_TAGGABLE_RESOURCES = {
    "thinggroup": (_thing_groups, "Thing Group {name} does not exist."),
    "thingtype": (_thing_types, "Thing Type {name} does not exist."),
    "policy": (_policies, "This resource is not found"),
    "rule": (_topic_rules, "Rule {name} not found. accountId: {account}, resourceArn: {arn}"),
    "job": (_jobs, "Job {name} cannot be found."),
    "jobtemplate": (_job_templates, "Job Template {name} cannot be found."),
    "cacert": (_ca_certificates, "CA Certificate {name} not found"),
    "provisioningtemplate": (_provisioning_templates, "Unable to get template id, template not found"),
    "domainconfiguration": (_domain_configurations, None),
}
# Resource types IoT has but does not tag.
_UNTAGGABLE_RESOURCE_TYPES = ("thing", "cert")


def _tag_request_error(message: str) -> tuple:
    return error_response_json("InvalidRequestException", message, 400)


def _tag_key_error(key) -> str | None:
    """The model constraint a tag key fails, or None."""
    if not isinstance(key, str):
        return "Member must not be null"
    if not key:
        return "Member must have length greater than or equal to 1"
    if len(key) > 128:
        return "Member must have length less than or equal to 128"
    if not all(unicodedata.category(c)[0] in "LNZ" or c in "_.:/=+-@" for c in key):
        return f"Member must satisfy regular expression pattern: {_TAG_KEY_PATTERN}"
    return None


def _tags_error(tags, required: bool = True,
                missing_value: str = _INVALID_TAG_REQUEST) -> tuple | None:
    """The refusal of the tag list of a TagResource (or, not required, a Create) call."""
    if not tags:
        if not required:
            return None
        if tags is None:
            return _job_template_validation_error(None, "tags", "Member must not be null")
        return _tag_request_error("The validated collection is empty")
    if not isinstance(tags, list) or not all(isinstance(t, dict) for t in tags):
        return _tag_request_error(_INVALID_TAG_REQUEST)
    clauses = []
    for index, tag in enumerate(tags, 1):
        constraint = _tag_key_error(tag.get("Key"))
        if constraint:
            clauses.append(_job_validation_clause(
                tag.get("Key"), f"tags.{index}.member.key", constraint))
        value = tag.get("Value")
        if isinstance(value, str) and len(value) > 256:
            clauses.append(_job_validation_clause(
                value, f"tags.{index}.member.value",
                "Member must have length less than or equal to 256"))
    if clauses:
        return _job_validation_error(clauses)
    for tag in tags:
        if not isinstance(tag.get("Value"), str):
            return _tag_request_error(missing_value)
        if tag["Key"].lower().startswith("aws:"):
            return _tag_request_error(f'Tags cannot begin with "aws:", offending tag: {tag["Key"]}')
    if len({tag["Key"] for tag in tags}) > _TAGS_PER_RESOURCE:
        return _tag_request_error(_TOO_MANY_TAGS)
    return None


def _set_created_tags(arn: str, tags: list | None) -> None:
    """Store the tags a Create call carried; none drops what a deleted namesake left."""
    if tags:
        _tags[arn] = {tag["Key"]: tag["Value"] for tag in tags}
    else:
        _tags.pop(arn, None)


def _tag_target_error(arn, must_exist: bool) -> tuple | None:
    """The refusal of a tagging call on ``arn``, or None; ListTagsForResource passes must_exist=False."""
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        spec = None
    rtype, _, rest = spec.resource.partition("/") if spec else ("", "", "")
    if not rest or spec.service != "iot" or (
            rtype not in _TAGGABLE_RESOURCES and rtype not in _UNTAGGABLE_RESOURCE_TYPES):
        return _tag_request_error("Invalid arn in request")
    if spec.account_id != get_account_id():
        return _tag_request_error("ResourceArn doesn't belong in requester's account")
    if rtype in _UNTAGGABLE_RESOURCE_TYPES:
        return _tag_request_error(f"Invalid resource type in ARN: {rtype}")
    if not must_exist:
        return None
    if spec.region != get_region():
        return _tag_request_error(_INVALID_TAG_REQUEST)
    store, message = _TAGGABLE_RESOURCES[rtype]
    # A domain configuration ARN ends in a generated id after the name.
    name = rest.partition("/")[0] if rtype == "domainconfiguration" else rest
    record = store.get(name)
    if record is not None and record.get("domainConfigurationArn", arn) == arn:
        return None
    if message is None:
        return _domain_config_not_found(name)
    return error_response_json(
        "ResourceNotFoundException",
        message.format(name=name, account=spec.account_id, arn=arn), 404)


def _tag_resource(payload: dict) -> tuple:
    """``POST /tags``: add or overwrite tags; the last of a repeated key wins."""
    arn, tags = payload.get("resourceArn"), payload.get("tags")
    err = _tags_error(tags) or _tag_target_error(arn, must_exist=True)
    if err:
        return err
    merged = {**(_tags.get(arn) or {}), **{tag["Key"]: tag["Value"] for tag in tags}}
    if len(merged) > _TAGS_PER_RESOURCE:
        return _tag_request_error(_TOO_MANY_TAGS)
    _tags[arn] = merged
    return json_response({})


def _untag_resource(payload: dict) -> tuple:
    """``POST /untag``: a key the resource does not carry is no error."""
    keys = payload.get("tagKeys")
    if keys is None:
        return _job_template_validation_error(None, "tagKeys", "Member must not be null")
    if not keys:
        return _tag_request_error("The validated collection is empty")
    if not isinstance(keys, list):
        return _tag_request_error(_INVALID_TAG_REQUEST)
    if any(_tag_key_error(key) for key in keys):
        return _job_template_validation_error(
            "[" + ", ".join(map(str, keys)) + "]", "tagKeys",
            "Member must satisfy constraint: [Member must have length less than or equal "
            "to 128, Member must have length greater than or equal to 1, Member must "
            f"satisfy regular expression pattern: {_TAG_KEY_PATTERN}]")
    reserved = next((key for key in keys if key.lower().startswith("aws:")), None)
    if reserved is not None:
        return _tag_request_error(f'Tags cannot begin with "aws:", offending tag: {reserved}')
    arn = payload.get("resourceArn")
    err = _tag_target_error(arn, must_exist=True)
    if err:
        return err
    stored = _tags.get(arn) or {}
    for key in keys:
        stored.pop(key, None)
    return json_response({})


def _list_tags_for_resource(qp: dict) -> tuple:
    """``GET /tags``: every tag in one page, whatever nextToken says."""
    arn = qp.get("resourceArn")
    err = _tag_target_error(arn, must_exist=False)
    if err:
        return err
    return json_response({
        "nextToken": None,
        "tags": [{"Key": k, "Value": v} for k, v in (_tags.get(arn) or {}).items()],
    })


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
# concurrency token here. It counts a thing's executions of one job: 1 for the
# first, one more each time the thing joins a CONTINUOUS job's target group
# again after its last execution finished (AWS also increments it when a job
# execution is retried, which MiniStack does not model). `_job_executions`
# holds the newest execution per (thing, job); the older, terminal ones ride
# along newest first under its internal `history` key.
#
# A thing leaving every target of a CONTINUOUS job moves its QUEUED execution
# to REMOVED (see `_jobs_thing_left_group`). Deleting a thing DELETES its
# executions (see `_delete_thing`) rather than marking them REMOVED.
# DeleteJobExecution removes one execution; a CONTINUOUS job's thing that
# lost all of them is listed under the job's `executionsDeletedFor`, so that
# reads do not queue a new one until the thing joins a target group again.

# Job ids are stricter than thing names, and identically so in both service
# models (`iot` and `iot-jobs-data` declare JobId as [a-zA-Z0-9_-], max 64).
# The generic thing-name pattern would let a `:` through control-side, and the
# device that has to fetch that job by id could then never reach it.
_JOB_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{1,64}$")

# schedulingConfig start/end times are minute-precision and carry no timezone
# suffix: "must follow the format YYYY-MM-DDThh:mm" (measured eu-north-1
# 2026-09-19).
_JOB_SCHEDULE_TIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$")

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
    """Epoch MILLISECONDS — the unit job/execution records store internally
    (millisecond resolution keeps same-second executions in queue order).

    Every response edge emits epoch SECONDS, which is how the API reference
    words every job/execution stamp ("the time, in seconds since the epoch"):
    the `iot` control plane models them as `timestamp` shapes (floats via
    :func:`_jobs_ms_to_s` — botocore parses them into datetimes), while
    `iot-jobs-data` and the jobs-over-MQTT bridge model them as raw `long`
    shapes carrying whole seconds (:func:`_jobs_ms_to_long_s`). Serving the
    stored milliseconds through either edge is off by 1000x — botocore
    explodes parsing a `timestamp` shape ("year 58580 is out of range"), and
    a device diffing a `long` stamp against its own clock sees the same
    year-58580 nonsense.
    """
    return int(time.time() * 1000)


def _jobs_ms_to_s(millis: int | None) -> int | None:
    """Millisecond record stamp → whole epoch seconds for `timestamp` shapes."""
    return None if millis is None else millis // 1000


def _jobs_ms_to_long_s(millis: int | None) -> int | None:
    """Millisecond record stamp → whole epoch seconds for the device plane's
    `long` shapes."""
    return None if millis is None else millis // 1000


def _job_arn(job_id: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:job/{job_id}"


def _job_target_not_found(arn: str) -> str:
    """AWS's message for a job target thing or thing group that does not exist."""
    kind, _, name = arn.rpartition(":")[2].partition("/")
    return f"{'Thing Group' if kind == 'thinggroup' else 'Thing'} {name} cannot be found."


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
    watching group mutations. Things in the job's `rolloutQueue` wait for
    their rollout batch.
    """
    job = _jobs.get(job_id)
    if not job or job.get("status") == "CANCELED":
        return
    if job.get("targetSelection") == "SNAPSHOT" and job.get("snapshotted"):
        return
    now = _jobs_now_ms()
    deleted = job.get("executionsDeletedFor") or ()
    waiting = set(job.get("rolloutQueue") or ())
    for thing in _job_target_things(job.get("targets")):
        key = (thing, job_id)
        if key in _job_executions or thing in deleted or thing in waiting:
            continue
        _jobs_queue_execution(thing, job_id, now)
    if job.get("targetSelection") == "SNAPSHOT":
        job["snapshotted"] = True


def _jobs_queue_execution(thing: str, job_id: str, now: int) -> None:
    """Queue the thing's next execution of a job, at version 1.

    The first one is number 1. A later one takes the next number and keeps
    the finished one before it under `history`.
    """
    previous = _job_executions.get((thing, job_id))
    execution = {
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
    if previous is not None:
        execution["executionNumber"] = previous["executionNumber"] + 1
        execution["history"] = [
            {k: v for k, v in previous.items() if k != "history"},
            *previous.get("history", []),
        ]
    _job_executions[(thing, job_id)] = execution
    deleted = (_jobs.get(job_id) or {}).get("executionsDeletedFor")
    if deleted and thing in deleted:
        deleted.remove(thing)


# A job with jobExecutionsRolloutConfig creates its executions in batches, one
# batch per this interval; the things still waiting are in its `rolloutQueue`.
_JOB_ROLLOUT_INTERVAL_MS = 60_000
# The running rollout tasks, kept here because the event loop holds only weak references.
_rollout_tasks: set[asyncio.Task] = set()


def _jobs_rollout_batch_size(job: dict, waiting: int) -> int:
    """How many of `waiting` things a job notifies in its next batch."""
    config = job.get("jobExecutionsRolloutConfig")
    config = config if isinstance(config, dict) else {}
    maximum = config.get("maximumPerMinute")
    size = maximum if isinstance(maximum, int) and maximum > 0 else waiting
    rate = config.get("exponentialRate")
    rate = rate if isinstance(rate, dict) else {}
    criteria = rate.get("rateIncreaseCriteria")
    criteria = criteria if isinstance(criteria, dict) else {}
    if criteria.get("numberOfSucceededThings") is not None:
        step = criteria["numberOfSucceededThings"]
        done = _jobs_status_counts(job["jobId"])["SUCCEEDED"]
    else:
        step = criteria.get("numberOfNotifiedThings")
        done = sum(1 for e in _job_executions.values() if e["jobId"] == job["jobId"])
    base, factor = rate.get("baseRatePerMinute"), rate.get("incrementFactor")
    if not (
        isinstance(base, int) and base > 0 and isinstance(step, int) and step > 0
        and isinstance(factor, (int, float)) and factor > 1
    ):
        return size
    # The rate grows by incrementFactor for every full step of notified (or
    # succeeded) things, and maximumPerMinute caps it.
    per_minute = base
    for _ in range(done // step):
        if per_minute >= size:
            break
        per_minute *= factor
    return min(size, int(per_minute))


def _jobs_rollout_release(job: dict) -> list[tuple[str, str | None]]:
    """Queue the next batch of a job's waiting things; returns (thing, previous queue front) pairs."""
    job_id, waiting = job["jobId"], job["rolloutQueue"]
    batch = waiting[:_jobs_rollout_batch_size(job, len(waiting))]
    job["rolloutQueue"] = waiting[len(batch):]
    now = _jobs_now_ms()
    job["rolloutBatchAt"] = now
    # A thing deleted meanwhile is skipped, and so is one a CONTINUOUS job no longer targets.
    targeted = (
        set(_job_target_things(job.get("targets")))
        if job.get("targetSelection") == "CONTINUOUS" else _things
    )
    released = []
    for thing in batch:
        if thing in targeted and (thing, job_id) not in _job_executions:
            released.append((thing, jobs_first_pending_job_id(thing)))
            _jobs_queue_execution(thing, job_id, now)
    _jobs_maybe_complete(job_id)
    return released


async def _jobs_rollout(account_id: str, region: str, job: dict) -> None:
    """Release a job's waiting things batch by batch until none is left or the job stops."""
    while True:
        with request_scope(account_id, region):
            if (
                _jobs.get(job["jobId"]) is not job
                or job["status"] != "IN_PROGRESS"
                or not job.get("rolloutQueue")
            ):
                return
            wait = job["rolloutBatchAt"] + _JOB_ROLLOUT_INTERVAL_MS - _jobs_now_ms()
        if wait > 0:
            await asyncio.sleep(wait / 1000)
            continue
        async with publishing_events():
            with request_scope(account_id, region):
                released = _jobs_rollout_release(job)
            for thing, prev_next in released:
                await jobs_notify_thing(account_id, region, thing, prev_next)


def _jobs_rollout_start(account_id: str, region: str, job: dict) -> None:
    """Run a job's rollout on the running event loop, if there is one."""
    try:
        task = asyncio.get_running_loop().create_task(_jobs_rollout(account_id, region, job))
    except RuntimeError:
        return
    _rollout_tasks.add(task)
    task.add_done_callback(_rollout_tasks.discard)


def _jobs_thing_joined_group(thing: str, group_arn: str) -> bool:
    """Run every CONTINUOUS job targeting a group for a thing that just
    joined it.

    The thing gets a new QUEUED execution unless its newest one is still
    QUEUED or IN_PROGRESS. That covers a thing rejoining after its execution
    finished, and a thing that finished the job joining another target group
    of it (measured eu-central-1 2026-10-05). Returns whether anything was
    queued, i.e. whether the thing's jobs/notify topics have news.
    """
    now = _jobs_now_ms()
    queued = False
    for job_id, job in _jobs.items():
        if (
            job.get("targetSelection") != "CONTINUOUS"
            or job.get("status") == "CANCELED"
            or group_arn not in (job.get("targets") or [])
        ):
            continue
        previous = _job_executions.get((thing, job_id))
        if previous is not None and previous["status"] not in _JOB_EXECUTION_TERMINAL:
            continue
        _jobs_queue_execution(thing, job_id, now)
        queued = True
    return queued


def _jobs_thing_left_group(thing: str, group_arn: str | None) -> bool:
    """Retire a thing's QUEUED CONTINUOUS executions after it left a group.

    Call after the membership change, with the ARN of the group the thing
    left (None when it was not a member). For every CONTINUOUS job the thing no
    longer resolves into, a QUEUED execution becomes REMOVED with its
    `versionNumber` unchanged. IN_PROGRESS, SUCCEEDED and FAILED executions
    keep their status (measured eu-central-1 2026-10-05). Returns whether a
    pending execution went away, i.e. whether jobs/notify has news.
    """
    now = _jobs_now_ms()
    removed = False
    for job_id, job in _jobs.items():
        execution = _job_executions.get((thing, job_id))
        if (
            execution is None
            and thing in (job.get("executionsDeletedFor") or ())
            and group_arn in (job.get("targets") or [])
            and job.get("status") != "CANCELED"
            and thing not in _job_target_things(job.get("targets"))
        ):
            # All its executions were deleted: it gets a REMOVED one, without an event.
            _jobs_queue_execution(thing, job_id, now)
            _job_executions[(thing, job_id)]["status"] = "REMOVED"
            continue
        if (
            execution is None
            or execution["status"] != "QUEUED"
            or job.get("targetSelection") != "CONTINUOUS"
            or job.get("status") == "CANCELED"
            or thing in _job_target_things(job.get("targets"))
        ):
            continue
        execution["status"] = "REMOVED"
        execution["lastUpdatedAt"] = now
        _job_execution_event(execution)
        removed = True
    return removed


def _jobs_with_history(execution: dict) -> list[dict]:
    """A thing's executions of one job, newest first."""
    return [execution, *execution.get("history", [])]


def _jobs_status_counts(job_id: str) -> dict:
    """Executions of a job per status, a rejoined thing's earlier ones included."""
    counts = {status: 0 for status in _JOB_EXECUTION_STATUSES}
    for latest in _job_executions.values():
        if latest["jobId"] == job_id:
            for execution in _jobs_with_history(latest):
                counts[execution["status"]] += 1
    return counts


def _job_event(job: dict, operation: str) -> None:
    """Queue ``$aws/events/job/{jobId}/{operation}``; completed and canceled carry the execution counts."""
    if not _event_wanted("JOB"):
        return
    fields = {
        "jobId": job["jobId"],
        "status": operation.upper(),
        "targetSelection": job["targetSelection"],
        "targets": list(job.get("targets") or []),
        "createdAt": job["createdAt"],
        "lastUpdatedAt": job["lastUpdatedAt"],
        "timestamp": int(time.time()),
    }
    if job.get("description") is not None:
        fields["description"] = job["description"]
    if operation == "completed":
        fields["completedAt"] = job["completedAt"]
    if operation in ("cancellation_in_progress", "canceled"):
        fields["forceCanceled"] = bool(job.get("forceCanceled"))
        for member in ("comment", "reasonCode"):
            if job.get(member) is not None:
                fields[member] = job[member]
    if operation in ("completed", "canceled"):
        counts = _jobs_status_counts(job["jobId"])
        fields["jobProcessDetails"] = {
            f"numberOf{name}Things": counts[status]
            for name, status in (
                ("Canceled", "CANCELED"), ("Rejected", "REJECTED"), ("Failed", "FAILED"),
                ("Removed", "REMOVED"), ("Succeeded", "SUCCEEDED"), ("TimedOut", "TIMED_OUT"),
            )
        }
    _registry_event(
        "JOB", f"job/{job['jobId']}/{operation}", "JOB", operation, fields,
        str(uuid.uuid4()), with_account=False,
    )


def _job_execution_event(
    execution: dict, status: str | None = None, force_canceled: bool | None = None
) -> None:
    """Queue ``$aws/events/jobExecution/{jobId}/{status}`` for an execution that reached ``status``."""
    status = status or execution["status"]
    fields = {
        "jobId": execution["jobId"],
        "thingArn": _thing_arn(execution["thingName"]),
        "status": status,
        "executionNumber": execution["executionNumber"],
        "timestamp": int(time.time()),
    }
    if force_canceled is not None:
        fields["forceCanceled"] = force_canceled
    if execution.get("statusDetails"):
        fields["statusDetails"] = dict(execution["statusDetails"])
    _registry_event(
        "JOB_EXECUTION", f"jobExecution/{execution['jobId']}/{status.lower()}",
        "JOB_EXECUTION", status.lower(), fields, str(uuid.uuid4()), with_account=False,
    )


def _jobs_materialize_all() -> None:
    for job_id in list(_jobs.keys()):
        _jobs_materialize_executions(job_id)


def _jobs_timeout_minutes(execution: dict, job: dict | None) -> int | None:
    """The in-progress timeout for one execution, in minutes, or None.

    ``stepTimeoutInMinutes`` from the device's own UpdateJobExecution wins over
    the job's ``timeoutConfig.inProgressTimeoutInMinutes``, which the reference
    describes as applying to every execution of the job. An execution that was
    running when UpdateJob changed the job's timeout keeps the earlier value.
    """
    step = execution.get("stepTimeoutInMinutes")
    if step is not None:
        return step
    if "jobTimeoutInMinutes" in execution:
        minutes = execution["jobTimeoutInMinutes"]
    else:
        cfg = (job or {}).get("timeoutConfig") or {}
        minutes = cfg.get("inProgressTimeoutInMinutes")
    return minutes if isinstance(minutes, int) else None


def _jobs_seconds_before_timeout(execution: dict) -> int | None:
    """Seconds an IN_PROGRESS execution has left before TIMED_OUT, or None when
    no timeout applies. The device plane models this as a ``long``."""
    if execution.get("status") != "IN_PROGRESS":
        return None
    minutes = _jobs_timeout_minutes(execution, _jobs.get(execution["jobId"]))
    if not minutes:
        return None
    started = (execution.get("timeoutStartedAt")
               or execution.get("startedAt")
               or execution.get("lastUpdatedAt"))
    if started is None:
        return None
    left = minutes * 60 - (_jobs_now_ms() - started) // 1000
    return max(0, int(left))


def _jobs_apply_timeout(execution: dict) -> dict:
    """Flip an execution that has outstayed its timeout to TIMED_OUT.

    Evaluated on read rather than by a timer, the way the broker's session
    expiry is: "whenever a job execution remains in the IN_PROGRESS status for
    longer than this interval, the job execution will fail and switch to the
    terminal TIMED_OUT status".
    """
    if execution.get("status") != "IN_PROGRESS":
        return execution
    minutes = _jobs_timeout_minutes(execution, _jobs.get(execution["jobId"]))
    if not minutes:
        return execution
    started = (execution.get("timeoutStartedAt")
               or execution.get("startedAt")
               or execution.get("lastUpdatedAt"))
    if started is None:
        return execution
    if _jobs_now_ms() - started <= minutes * 60_000:
        return execution
    execution["status"] = "TIMED_OUT"
    execution["lastUpdatedAt"] = _jobs_now_ms()
    execution["versionNumber"] += 1
    _job_execution_event(execution)
    _jobs_maybe_complete(execution["jobId"])
    # A read sends no jobs/notify, so the things an abort cancels here are not notified.
    _jobs_abort(execution["jobId"])
    return execution


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
        or job.get("rolloutQueue")
    ):
        return
    executions = [e for e in _job_executions.values() if e["jobId"] == job_id]
    if all(e["status"] in _JOB_EXECUTION_TERMINAL for e in executions):
        now = _jobs_now_ms()
        job["status"] = "COMPLETED"
        job["completedAt"] = now
        job["lastUpdatedAt"] = now
        _job_event(job, "completed")


# The execution statuses each abortConfig failureType counts.
_JOB_ABORT_FAILURE_STATUSES = {
    "FAILED": ("FAILED",), "REJECTED": ("REJECTED",), "TIMED_OUT": ("TIMED_OUT",),
    "ALL": ("FAILED", "REJECTED", "TIMED_OUT"),
}


def _jobs_abort(job_id: str) -> dict:
    """Cancel an IN_PROGRESS job once a criterion of its abortConfig is met; returns _jobs_cancel's mapping."""
    job = _jobs.get(job_id)
    criteria = ((job or {}).get("abortConfig") or {}).get("criteriaList") or []
    if not criteria or job.get("status") != "IN_PROGRESS":
        return {}
    counts = _jobs_status_counts(job_id)
    # Every terminal execution counts as completed, CANCELED and REMOVED ones included.
    completed = sum(counts[status] for status in _JOB_EXECUTION_TERMINAL)
    for criterion in criteria:
        failure_type = criterion.get("failureType")
        threshold = criterion.get("thresholdPercentage")
        minimum = criterion.get("minNumberOfExecutedThings")
        if (
            criterion.get("action") != "CANCEL"
            or failure_type not in _JOB_ABORT_FAILURE_STATUSES
            or not isinstance(threshold, (int, float))
            or not isinstance(minimum, int)
        ):
            continue
        failed = sum(counts[status] for status in _JOB_ABORT_FAILURE_STATUSES[failure_type])
        if completed >= max(minimum, 1) and failed * 100 >= threshold * completed:
            comment = (
                f"Abort criteria for failure type {failure_type} with threshold {threshold:.2f}% met. "
                f"Total completed executions: {completed}, total {failure_type} executions: {failed}"
            )
            return _jobs_cancel(job_id, {"comment": comment, "reasonCode": "ABORT_CANCEL"}, False)
    return {}


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
            f"Expected version {expected} but found version "
            f"{execution['versionNumber']}",
            409,
        )
    return None


def _jobs_reject_if_terminal(execution: dict, message: str) -> tuple | None:
    """Terminal executions are frozen — no update, no cancel."""
    if execution["status"] not in _JOB_EXECUTION_TERMINAL:
        return None
    return error_response_json("InvalidStateTransitionException", message, 409)


def jobs_reject_terminal_describe(execution: dict) -> tuple | None:
    """The device plane describes only QUEUED and IN_PROGRESS executions."""
    if execution["status"] not in _JOB_EXECUTION_TERMINAL:
        return None
    return error_response_json(
        "TerminalStateException",
        "Job Execution has reached terminal state. It is neither IN_PROGRESS nor QUEUED",
        410,
    )


async def _handle_job(method: str, path: str, body: bytes, qp: dict) -> tuple:
    """Dispatch /jobs/{jobId}[...] control-plane routes.

    Async because CreateJob, CancelJob, and DeleteJob publish
    jobs/notify(-next) through the broker — the same reason
    _register_certificate is async."""
    suffix = path[len("/jobs/"):]
    if suffix.endswith("/cancel") and method == "PUT":
        return await _cancel_job(suffix[:-len("/cancel")], _parse_body(body), qp)
    if suffix.endswith("/job-document") and method == "GET":
        return _get_job_document(suffix[:-len("/job-document")])
    if suffix.endswith("/things") and method == "GET":
        return _list_job_executions_for_job(suffix[:-len("/things")], qp)
    if "/" in suffix:
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
        )
    job_id = suffix
    if method == "PUT":
        return await _create_job(job_id, _parse_body(body))
    if method == "GET":
        return _describe_job(job_id)
    if method == "PATCH":
        return _update_job(job_id, _parse_body(body))
    if method == "DELETE":
        return await _delete_job(job_id, qp)
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


# An S3 host: path style without a bucket label, virtual-hosted with one.
_JOB_DOCUMENT_S3_HOST_RE = re.compile(r"(?:(?P<bucket>.+)\.)?s3(?:[.-][a-z0-9-]+)?\.amazonaws\.com")
_JOB_DOCUMENT_MAX_BYTES = 32768


def _job_document_s3_location(source: str) -> tuple[str, str]:
    """The bucket and key an s3:// or S3 object URL names, empty where it names none."""
    from urllib.parse import unquote, urlsplit

    bucket = key = ""
    if isinstance(source, str) and source.startswith("s3://"):
        bucket, _, key = source[len("s3://"):].partition("/")
    elif isinstance(source, str) and source.startswith(("https://", "http://")):
        with contextlib.suppress(ValueError):
            parts = urlsplit(source)
            match = _JOB_DOCUMENT_S3_HOST_RE.fullmatch(parts.hostname or "")
            if match and match.group("bucket"):
                bucket, key = match.group("bucket"), unquote(parts.path[1:])
            elif match:
                bucket, _, key = parts.path[1:].partition("/")
                key = unquote(key)
    return bucket, key


def _job_document_from_source(source: str) -> tuple[str | None, tuple | None]:
    """The job document a documentSource names in this account's S3, or an error."""
    from ministack.services import s3 as _s3

    bucket, key = _job_document_s3_location(source)
    if not bucket:
        return None, error_response_json(
            "InvalidRequestException", f"Job document source {source} is invalid.", 400
        )
    if not key:
        return None, error_response_json(
            "InvalidRequestException",
            f"Provided S3 url is not valid: {source}. Error: Key cannot be null", 400,
        )
    bucket_record = _s3._ensure_bucket(bucket)
    if bucket_record is None:
        return None, error_response_json(
            "InvalidRequestException", f"Bucket specified by url: {source} is not found.", 400
        )
    obj = bucket_record["objects"].get(key)
    if obj is None:
        return None, error_response_json(
            "InvalidRequestException", f"Job document located at {source} is not found.", 400
        )
    if obj["size"] > _JOB_DOCUMENT_MAX_BYTES:
        return None, error_response_json(
            "LimitExceededException",
            f"Job document from {source} exceeds size limit {_JOB_DOCUMENT_MAX_BYTES} bytes.",
            410,
        )
    try:
        document = _s3._get_object_data(bucket, key).decode("utf-8")
        json.loads(document)
    except ValueError:
        return None, error_response_json(
            "InvalidRequestException",
            f"Job document located at {source} is an invalid JSON document.",
            400,
        )
    return document, None


# ${aws:iot:s3-presigned-url:<url>}; the -v2 form takes the URL encoded.
_JOB_PRESIGN_RE = re.compile(r"\$\{aws:iot:s3-presigned-url(?P<v2>-v2)?:(?P<url>[^}]*)\}")


def _job_presign_error(document: str | None, config: dict | None, job_id: str) -> tuple | None:
    """AWS's refusal of presigned-URL placeholders without a role or an S3 object URL."""
    placeholders = list(_JOB_PRESIGN_RE.finditer(document or ""))
    if not placeholders:
        return None
    if not (isinstance(config, dict) and config.get("roleArn")):
        message = (
            "presignedUrlConfig role ARN is required to generate an S3 presigned url in job document."
            if all(m.group("v2") for m in placeholders)
            else f"Role must be provided for pre-sign S3 urls in job document of job {job_id}."
        )
        return error_response_json("InvalidRequestException", message, 400)
    for m in placeholders:
        url = m.group("url")
        bucket, key = _job_document_s3_location(url)
        if not bucket or (m.group("v2") and re.search(r'[\s"<>\\^`{|}]', url)):
            return error_response_json(
                "InvalidRequestException", f"Given S3 url {url} is invalid.", 400
            )
        if not key:
            return error_response_json(
                "InvalidRequestException",
                f"Provided S3 url is not valid: {url}. Error: Key cannot be null", 400,
            )
    return None


async def _create_job(job_id: str, payload: dict) -> tuple:
    if not job_id or not _JOB_ID_RE.match(job_id):
        return error_response_json(
            "InvalidRequestException",
            "Invalid jobId: must match [a-zA-Z0-9_-]{1,64}",
            400,
        )
    abort = payload.get("abortConfig")
    if violations := _job_update_violations(job_id, {"abortConfig": abort}):
        return _job_validation_error(violations)
    timeout_cfg = payload.get("timeoutConfig") or {}
    if timeout_cfg:
        err = _job_timeout_error(timeout_cfg.get("inProgressTimeoutInMinutes"))
        if err:
            return err
    if err := _job_abort_criteria_error(abort):
        return err
    if job_id in _jobs:
        return error_response_json(
            "ResourceAlreadyExistsException", f"Job {job_id} already exists", 409
        )
    if payload.get("document") and payload.get("documentSource"):
        return error_response_json(
            "InvalidRequestException",
            "Job document and job document source cannot be specified at the same time.",
            400,
        )
    if payload.get("jobTemplateArn") is not None:
        payload, err = _job_payload_from_template(payload)
        if err:
            return err
    err = _tags_error(payload.get("tags"), required=False, missing_value=_INVALID_JOB_TAG)
    if err:
        return err
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
    sched_cfg = payload.get("schedulingConfig") or {}
    for field in ("startTime", "endTime"):
        value = sched_cfg.get(field)
        if value is None:
            continue
        if not isinstance(value, str) or not _JOB_SCHEDULE_TIME_RE.match(value):
            # Exact wording measured eu-north-1 2026-09-19; note the minute
            # precision — an ISO stamp with seconds or a Z is refused.
            return error_response_json(
                "InvalidRequestException",
                f"1 validation error detected: Provided time {value} UTC must "
                "follow the format YYYY-MM-DDThh:mm.",
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
    target_things: set[str] = set()
    for arn in targets:
        try:
            parse_arn(arn)
        except ArnParseError:
            return error_response_json(
                "InvalidRequestException", f"Invalid target arn: {arn}", 400
            )
        resolved = _job_target_resolve(arn)
        if resolved is None:
            return error_response_json(
                "ResourceNotFoundException", _job_target_not_found(arn), 404
            )
        target_things.update(resolved)
    document = payload.get("document")
    if isinstance(document, str) and len(document) > 32768:
        return error_response_json(
            "InvalidRequestException",
            "document exceeds the maximum length of 32768", 400,
        )
    source = payload.get("documentSource")
    if isinstance(source, str) and not 1 <= len(source) <= 1350:
        return error_response_json(
            "InvalidRequestException",
            "documentSource must be between 1 and 1350 characters", 400,
        )
    if not document and not payload.get("documentSource"):
        return error_response_json(
            "InvalidRequestException",
            "document is required if you do not specify a value for documentSource",
            400,
        )
    if not document and payload.get("documentSource"):
        # The object is read once, here: a later overwrite changes no job.
        document, err = _job_document_from_source(payload["documentSource"])
        if err:
            return err
    err = _job_presign_error(document, payload.get("presignedUrlConfig"), job_id)
    if err:
        return err
    # Captured BEFORE the job record exists: jobs_first_pending_job_id
    # materializes, and once the new job is in the store it would already
    # count as the front of the queue.
    prev_next = {t: jobs_first_pending_job_id(t) for t in target_things}
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
        # Stored so DescribeJob echoes them. abortConfig and timeoutConfig are acted on.
        "abortConfig": payload.get("abortConfig"),
        "timeoutConfig": payload.get("timeoutConfig"),
        "jobExecutionsRetryConfig": payload.get("jobExecutionsRetryConfig"),
        "namespaceId": payload.get("namespaceId"),
        "jobTemplateArn": payload.get("jobTemplateArn"),
        "documentParameters": payload.get("documentParameters"),
        "schedulingConfig": payload.get("schedulingConfig"),
        "destinationPackageVersions": payload.get("destinationPackageVersions"),
        "snapshotted": False,
    }
    _set_created_tags(_job_arn(job_id), payload.get("tags"))
    things = _job_target_things(targets)
    batch = _jobs_rollout_batch_size(_jobs[job_id], len(things))
    if batch < len(things):
        # The other things wait for later batches of the rollout.
        _jobs[job_id].update(rolloutQueue=things[batch:], rolloutBatchAt=now)
        _jobs_rollout_start(get_account_id(), get_region(), _jobs[job_id])
    _jobs_materialize_executions(job_id)
    # New QUEUED executions joined each target's pending set — notify, as AWS
    # does (notify-next only reaches the things whose queue front changed).
    account_id, region = get_account_id(), get_region()
    for thing_name in sorted({
        execution["thingName"]
        for execution in _job_executions.values()
        if execution["jobId"] == job_id
    }):
        await jobs_notify_thing(
            account_id, region, thing_name, prev_next.get(thing_name)
        )
    response = {"jobArn": _job_arn(job_id), "jobId": job_id}
    if payload.get("description") is not None:
        response["description"] = payload["description"]
    return json_response(response)


# The members UpdateJob takes; each one given replaces the stored one whole.
_JOB_UPDATE_MEMBERS = (
    "description", "presignedUrlConfig", "jobExecutionsRolloutConfig",
    "abortConfig", "timeoutConfig", "jobExecutionsRetryConfig",
)
_JOB_ROLE_ARN_RE = re.compile(r"^arn:aws[a-z-]*:iam::\d{12}:role/.+$")


def _job_update_violations(job_id: str, payload: dict) -> list[str]:
    """Model constraint failures of an UpdateJob request; CreateJob and CreateJobTemplate check abortConfig with it."""
    found: list[str] = []

    def bound(value, member, low=None, high=None, double=False, required=False):
        if value is None and required:
            found.append(_job_validation_clause(None, member, "Member must not be null"))
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return
        shown = float(value) if double else value
        if low is not None and value < low:
            found.append(_job_validation_clause(
                shown, member, f"Member must have value greater than or equal to {low}"))
        elif high is not None and value > high:
            found.append(_job_validation_clause(
                shown, member, f"Member must have value less than or equal to {high}"))

    def enum(value, member, allowed, required=False):
        if value is None and required:
            found.append(_job_validation_clause(None, member, "Member must not be null"))
        if value is not None and value not in allowed:
            found.append(_job_validation_clause(
                value, member, f"Member must satisfy enum value set: {allowed}"))

    def criteria_list(cfg, member, high=None):
        items = cfg.get("criteriaList") if isinstance(cfg, dict) else None
        if not isinstance(items, list):
            return []
        if not items:
            found.append(_job_validation_clause(
                "[]", member, "Member must have length greater than or equal to 1"))
        elif high is not None and len(items) > high:
            shown = "[" + ", ".join(
                f"RetryCriteria(failureType={c.get('failureType')}, "
                f"numberOfRetries={c.get('numberOfRetries')})" for c in items
            ) + "]"
            found.append(_job_validation_clause(
                shown, member, f"Member must have length less than or equal to {high}"))
        return [c for c in items if isinstance(c, dict)]

    # Fixed order, independent of the request's member order.
    if len(job_id) > 64:
        found.append(_job_validation_clause(
            job_id, "jobId", "Member must have length less than or equal to 64"))
    elif not _JOB_ID_RE.match(job_id):
        found.append(_job_validation_clause(
            job_id, "jobId", "Member must satisfy regular expression pattern: [a-zA-Z0-9_-]+"))
    rollout = payload.get("jobExecutionsRolloutConfig") or {}
    if isinstance(rollout, dict):
        at = "jobExecutionsRolloutConfig"
        bound(rollout.get("maximumPerMinute"), f"{at}.maximumPerMinute", low=1, high=1000)
        rate = rollout.get("exponentialRate") or {}
        if isinstance(rate, dict):
            bound(rate.get("baseRatePerMinute"), f"{at}.exponentialRate.baseRatePerMinute",
                  low=1, high=1000)
            bound(rate.get("incrementFactor"), f"{at}.exponentialRate.incrementFactor",
                  low=1.1, high=5, double=True)
            criteria = rate.get("rateIncreaseCriteria") or {}
            if isinstance(criteria, dict):
                for name in ("numberOfNotifiedThings", "numberOfSucceededThings"):
                    bound(criteria.get(name),
                          f"{at}.exponentialRate.rateIncreaseCriteria.{name}", low=1)
    description = payload.get("description")
    if isinstance(description, str):
        if len(description) > 2028:
            found.append(_job_validation_clause(
                description, "description",
                "Member must have length less than or equal to 2028"))
        elif any(unicodedata.category(ch).startswith("C") for ch in description):
            found.append(_job_validation_clause(
                description, "description",
                r"Member must satisfy regular expression pattern: [^\p{C}]+"))
    presign = payload.get("presignedUrlConfig") or {}
    if isinstance(presign, dict):
        bound(presign.get("expiresInSec"), "presignedUrlConfig.expiresInSec",
              low=60, high=3600)
    for i, c in enumerate(criteria_list(payload.get("jobExecutionsRetryConfig"),
                                        "jobExecutionsRetryConfig.criteriaList", 2), 1):
        at = f"jobExecutionsRetryConfig.criteriaList.{i}.member"
        bound(c.get("numberOfRetries"), f"{at}.numberOfRetries", low=0, high=10)
        enum(c.get("failureType"), f"{at}.failureType", "[ALL, TIMED_OUT, FAILED]")
    abort = payload.get("abortConfig")
    if isinstance(abort, dict) and abort.get("criteriaList") is None:
        found.append(_job_validation_clause(
            None, "abortConfig.criteriaList", "Member must not be null"))
    for i, c in enumerate(criteria_list(abort, "abortConfig.criteriaList"), 1):
        at = f"abortConfig.criteriaList.{i}.member"
        bound(c.get("minNumberOfExecutedThings"), f"{at}.minNumberOfExecutedThings", low=1,
              required=True)
        enum(c.get("failureType"), f"{at}.failureType", "[ALL, TIMED_OUT, FAILED, REJECTED]",
             required=True)
        enum(c.get("action"), f"{at}.action", "[CANCEL]", required=True)
        bound(c.get("thresholdPercentage"), f"{at}.thresholdPercentage", high=100,
              double=True, required=True)
    return found


def _job_abort_criteria_error(abort) -> tuple | None:
    """The abortConfig checks that follow the model constraints, criterion by criterion."""
    criteria = abort.get("criteriaList") if isinstance(abort, dict) else None
    seen = set()
    for c in criteria if isinstance(criteria, list) else []:
        if not isinstance(c, dict):
            continue
        threshold = c.get("thresholdPercentage")
        numeric = isinstance(threshold, (int, float)) and not isinstance(threshold, bool)
        message = None
        if c.get("failureType") in seen:
            message = "Multiple AbortCriteria cannot have the same failure type"
        elif numeric and threshold <= 0:
            message = (f"ThresholdPercentage value {float(threshold)} for AbortCriteria "
                       "must have value greater than 0")
        elif numeric and round(threshold, 2) != threshold:
            message = "Abort thresholdPercentage must have two or less decimal digits"
        if message:
            return error_response_json("InvalidRequestException", message, 400)
        seen.add(c.get("failureType"))
    return None


def _job_update_request_error(job_id: str, payload: dict) -> tuple | None:
    """Checks that need no job: they answer even for an unknown job id."""
    violations = _job_update_violations(job_id, payload)
    if violations:
        return _job_validation_error(violations)
    if all(payload.get(member) is None for member in _JOB_UPDATE_MEMBERS):
        return error_response_json(
            "InvalidRequestException",
            f"Update Job request for job {job_id} cannot be empty.", 400,
        )
    if payload.get("timeoutConfig") is not None:
        err = _job_timeout_error(
            (payload["timeoutConfig"] or {}).get("inProgressTimeoutInMinutes"))
        if err:
            return err
    if err := _job_abort_criteria_error(payload.get("abortConfig")):
        return err
    rollout = payload.get("jobExecutionsRolloutConfig")
    if isinstance(rollout, dict):
        maximum, rate = rollout.get("maximumPerMinute"), rollout.get("exponentialRate")
        message = None
        if maximum is None:
            message = (
                "Provide MaximumPerMinute value when ExponentialRate is defined"
                if rate is not None else
                "Provide MaximumPerMinute value or provide ExponentialRate and "
                "MaximumPerMinute"
            )
        elif isinstance(rate, dict):
            criteria = rate.get("rateIncreaseCriteria") or {}
            given = sum(criteria.get(name) is not None for name in (
                "numberOfNotifiedThings", "numberOfSucceededThings"))
            if given != 1:
                message = (
                    f"Provide {'either' if not given else 'only one of'} "
                    "NumberOfNotifiedThings or NumberOfSucceededThings when "
                    "RateIncreaseCriteria is defined"
                )
            elif (rate.get("baseRatePerMinute") or 0) > maximum:
                message = (
                    "Exponential rollout baseRatePerMinute should be less than "
                    "maximumPerMinute."
                )
        if message:
            return error_response_json("InvalidRequestException", message, 400)
    role = (payload.get("presignedUrlConfig") or {}).get("roleArn")
    if role is not None and not _JOB_ROLE_ARN_RE.match(str(role)):
        return error_response_json(
            "InvalidRequestException", f"Given role {role} is invalid.", 400
        )
    return None


def _job_retry_update_error(job: dict, retry: dict) -> tuple | None:
    """A retry config can only set the retries of its creation-time failure types to 0."""
    criteria = [c for c in retry.get("criteriaList") or [] if isinstance(c, dict)]
    message = None
    if any(c.get("numberOfRetries") != 0 for c in criteria):
        message = "The number of retries cannot be updated to any number other than 0."
    elif len(criteria) > 1 and any(c.get("failureType") == "ALL" for c in criteria):
        message = "A retryCriteria with failure type ALL must be used by itself."
    elif not job.get("jobExecutionsRetryConfig"):
        message = (
            "RetryConfig cannot be updated if the job has no RetryConfig defined "
            "during creation."
        )
    else:
        existing = {
            c.get("failureType")
            for c in job["jobExecutionsRetryConfig"].get("criteriaList") or []
        }
        if {c.get("failureType") for c in criteria} != existing:
            message = "FailureTypes must match existing FailureTypes defined in RetryConfig."
    if message:
        return error_response_json("InvalidRequestException", message, 400)
    return None


def _update_job(job_id: str, payload: dict) -> tuple:
    err = _job_update_request_error(job_id, payload)
    if err:
        return err
    job = _jobs.get(job_id)
    if job is None:
        return error_response_json(
            "ResourceNotFoundException", f"Job {job_id} cannot be found.", 404
        )
    if job["status"] != "IN_PROGRESS":
        return error_response_json(
            "InvalidRequestException",
            f"Job {job_id} in status {job['status']} cannot be updated.", 400,
        )
    if payload.get("jobExecutionsRetryConfig") is not None:
        err = _job_retry_update_error(job, payload["jobExecutionsRetryConfig"])
        if err:
            return err
    if payload.get("timeoutConfig") is not None:
        # A running execution keeps the in-progress timeout it started with.
        _jobs_materialize_executions(job_id)
        minutes = (job.get("timeoutConfig") or {}).get("inProgressTimeoutInMinutes")
        for execution in _job_executions.values():
            if (
                execution["jobId"] == job_id
                and execution["status"] == "IN_PROGRESS"
                and "jobTimeoutInMinutes" not in execution
            ):
                execution["jobTimeoutInMinutes"] = minutes
    changes = {
        member: copy.deepcopy(payload[member])
        for member in _JOB_UPDATE_MEMBERS
        if payload.get(member) is not None
    }
    _job_config_floats(changes)
    job.update(changes)
    job["lastUpdatedAt"] = _jobs_now_ms()
    return 200, {}, b""


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
    # Every execution counts, a rejoined thing's earlier ones included
    # (measured eu-central-1 2026-10-05).
    counts = _jobs_status_counts(job_id)
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
    if job.get("forceCanceled") is not None:
        job_doc["forceCanceled"] = job["forceCanceled"]
    # Members the Job shape models and CreateJob accepted. `tags` is NOT among
    # them (tags are read through ListTagsForResource), so it is not echoed.
    for member in (
        "abortConfig", "jobExecutionsRetryConfig", "namespaceId", "jobTemplateArn",
        "documentParameters", "destinationPackageVersions",
    ):
        if job.get(member) is not None:
            job_doc[member] = job[member]
    # timeoutConfig and schedulingConfig come back as {} even when unset, unlike
    # abortConfig, which is omitted (captured eu-north-1 2026-09-19).
    job_doc["timeoutConfig"] = job.get("timeoutConfig") or {}
    job_doc["schedulingConfig"] = job.get("schedulingConfig") or {}
    # isConcurrent is false in every steady state (captured eu-north-1
    # 2026-09-19 with a QUEUED, an IN_PROGRESS and a SUCCEEDED execution). It is
    # true while a rollout still has things to notify; the cancel window the
    # reference also describes is not modeled. processingTargets is null throughout the same
    # capture, so it stays omitted rather than echoing the target ARNs.
    job_doc["isConcurrent"] = job["status"] == "IN_PROGRESS" and bool(job.get("rolloutQueue"))
    response = {"job": job_doc}
    if job.get("documentSource") is not None:
        response["documentSource"] = job["documentSource"]
    return json_response(response)


async def _delete_job(job_id: str, qp: dict) -> tuple:
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
    # Sweeping the executions removes any pending ones from their thing's
    # pending set — notify those things, as AWS does on execution removal.
    affected = sorted({
        execution["thingName"]
        for execution in _job_executions.values()
        if execution["jobId"] == job_id
        and execution["status"] in ("QUEUED", "IN_PROGRESS")
    })
    prev_next = {t: jobs_first_pending_job_id(t) for t in affected}
    job["lastUpdatedAt"] = _jobs_now_ms()
    _job_event(job, "deletion_in_progress")
    del _jobs[job_id]
    _tags.pop(job["jobArn"], None)
    for key in [k for k in _job_executions.keys() if k[1] == job_id]:
        _job_execution_event(_job_executions[key], "DELETED")
        del _job_executions[key]
    _job_event(job, "deleted")
    account_id, region = get_account_id(), get_region()
    for thing_name in affected:
        await jobs_notify_thing(account_id, region, thing_name, prev_next[thing_name])
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


# ---------------------------------------------------------------------------
# Job templates. A template has no update call; CreateJob copies what it needs
# from one at create time, so deleting a template leaves its jobs as they are.
# Messages and shapes below were measured on eu-central-1 2026-10-05.
# ---------------------------------------------------------------------------

# The configuration a template hands to a job created from it, and to a
# template created from a job (jobArn). A member the request names wins whole.
_JOB_TEMPLATE_CONFIG_MEMBERS = (
    "presignedUrlConfig", "jobExecutionsRolloutConfig", "abortConfig",
    "timeoutConfig", "jobExecutionsRetryConfig",
)
_JOB_PAGE_MAX = 250


def _job_template_arn(template_id: str) -> str:
    return f"arn:aws:iot:{get_region()}:{get_account_id()}:jobtemplate/{template_id}"


def _job_validation_clause(value, member: str, constraint: str) -> str:
    shown = "null" if value is None else f"'{value}'"
    return f"Value {shown} at '{member}' failed to satisfy constraint: {constraint}"


def _job_validation_error(clauses: list[str]) -> tuple:
    noun = "error" if len(clauses) == 1 else "errors"
    return error_response_json(
        "InvalidRequestException",
        f"{len(clauses)} validation {noun} detected: " + "; ".join(clauses),
        400,
    )


def _job_template_validation_error(value, member: str, constraint: str) -> tuple:
    return _job_validation_error([_job_validation_clause(value, member, constraint)])


def _job_timeout_error(minutes) -> tuple | None:
    if isinstance(minutes, int) and not isinstance(minutes, bool) and 1 <= minutes <= 10080:
        return None
    return error_response_json(
        "InvalidRequestException",
        "Provide valid timeout value, inProgressTimeoutInMinutes cannot be "
        f"{'null' if minutes is None else minutes}.",
        400,
    )


def _job_config_floats(record: dict) -> None:
    """The two `double` members read back as floats (25 comes back as 25.0)."""
    for criteria in (record.get("abortConfig") or {}).get("criteriaList") or []:
        if isinstance(criteria.get("thresholdPercentage"), int):
            criteria["thresholdPercentage"] = float(criteria["thresholdPercentage"])
    rate = (record.get("jobExecutionsRolloutConfig") or {}).get("exponentialRate") or {}
    if isinstance(rate.get("incrementFactor"), int):
        rate["incrementFactor"] = float(rate["incrementFactor"])


def _job_template_id_error(template_id: str) -> tuple | None:
    if len(template_id) > 64:
        return _job_template_validation_error(
            template_id, "jobTemplateId",
            "Member must have length less than or equal to 64",
        )
    if not _JOB_ID_RE.match(template_id):
        return _job_template_validation_error(
            template_id, "jobTemplateId",
            "Member must satisfy regular expression pattern: [a-zA-Z0-9_-]+",
        )
    return None


def _job_template_not_found(template_id: str) -> tuple:
    return error_response_json(
        "ResourceNotFoundException", f"Job Template {template_id} cannot be found.", 404
    )


def _handle_job_template(method: str, path: str, body: bytes) -> tuple:
    template_id = path[len("/job-templates/"):]
    if "/" in template_id:
        return error_response_json(
            "InvalidRequestException", f"Unsupported IoT path: {method} {path}", 400
        )
    err = _job_template_id_error(template_id)
    if err:
        return err
    if method == "PUT":
        return _create_job_template(template_id, _parse_body(body))
    if method == "GET":
        return _describe_job_template(template_id)
    if method == "DELETE":
        return _delete_job_template(template_id)
    return error_response_json(
        "InvalidRequestException", f"Unsupported method: {method}", 400
    )


def _create_job_template(template_id: str, payload: dict) -> tuple:
    abort = payload.get("abortConfig")
    violations = _job_update_violations(template_id, {"abortConfig": abort})
    if payload.get("description") is None:
        violations.insert(0, _job_validation_clause(None, "description", "Member must not be null"))
    if violations:
        return _job_validation_error(violations)
    document = payload.get("document")
    if isinstance(document, str) and len(document) > 32768:
        return _job_template_validation_error(
            document, "document", "Member must have length less than or equal to 32768"
        )
    minutes = (payload.get("timeoutConfig") or {}).get("inProgressTimeoutInMinutes")
    if minutes is not None and (err := _job_timeout_error(minutes)):
        return err
    if err := _job_abort_criteria_error(abort):
        return err
    source = payload.get("documentSource")
    if document and source:
        return error_response_json(
            "InvalidRequestException",
            "Job document and job document source cannot be specified at the same time.",
            400,
        )
    if err := _tags_error(payload.get("tags"), required=False, missing_value=_INVALID_JOB_TAG):
        return err
    if not document and not source and not payload.get("jobArn"):
        return error_response_json(
            "InvalidRequestException",
            "Neither job document nor job document source is specified.",
            400,
        )
    if template_id in _job_templates:
        return error_response_json(
            "ConflictException", f"Job Template {template_id} already exists.", 409,
            extra={"resourceId": None},
        )
    record = {
        "jobTemplateArn": _job_template_arn(template_id),
        "jobTemplateId": template_id,
        "description": payload["description"],
        "createdAt": _now_epoch(),
    }
    if payload.get("jobArn"):
        job_id = str(payload["jobArn"]).rsplit("/", 1)[-1]
        job = _jobs.get(job_id)
        if job is None or job["jobArn"] != payload["jobArn"]:
            return error_response_json(
                "ResourceNotFoundException", f"Job {job_id} cannot be found.", 404
            )
        record["document"] = job.get("document")
        record["documentSource"] = job.get("documentSource")
        for member in _JOB_TEMPLATE_CONFIG_MEMBERS:
            record[member] = copy.deepcopy(job.get(member)) or None
    if document or source:
        record["document"] = document
        record["documentSource"] = source
    if source and not document:
        record["document"], err = _job_document_from_source(source)
        if err:
            return err
    for member in _JOB_TEMPLATE_CONFIG_MEMBERS + (
        "maintenanceWindows", "destinationPackageVersions",
    ):
        if payload.get(member) is not None:
            record[member] = copy.deepcopy(payload[member])
    _job_config_floats(record)
    err = _job_presign_error(record.get("document"), record.get("presignedUrlConfig"), template_id)
    if err:
        return err
    _job_templates[template_id] = record
    _set_created_tags(record["jobTemplateArn"], payload.get("tags"))
    return json_response({
        "jobTemplateArn": record["jobTemplateArn"], "jobTemplateId": template_id,
    })


def _describe_job_template(template_id: str) -> tuple:
    t = _job_templates.get(template_id)
    if t is None:
        return _job_template_not_found(template_id)
    # AWS writes every member, null when unset, and the three structures
    # below as objects of nulls; botocore reads those as {}.
    rollout = {"exponentialRate": None, "maximumPerMinute": None,
               **(t.get("jobExecutionsRolloutConfig") or {})}
    if rollout["exponentialRate"] is not None:
        rate = dict(rollout["exponentialRate"])
        rate["rateIncreaseCriteria"] = {
            "numberOfNotifiedThings": None, "numberOfSucceededThings": None,
            **(rate.get("rateIncreaseCriteria") or {}),
        }
        rollout["exponentialRate"] = rate
    return json_response({
        "jobTemplateArn": t["jobTemplateArn"],
        "jobTemplateId": t["jobTemplateId"],
        "description": t["description"],
        "documentSource": t.get("documentSource"),
        "document": t.get("document"),
        "createdAt": t["createdAt"],
        "presignedUrlConfig": {"expiresInSec": None, "roleArn": None,
                               **(t.get("presignedUrlConfig") or {})},
        "jobExecutionsRolloutConfig": rollout,
        "abortConfig": t.get("abortConfig"),
        "timeoutConfig": {"inProgressTimeoutInMinutes": None,
                          **(t.get("timeoutConfig") or {})},
        "jobExecutionsRetryConfig": t.get("jobExecutionsRetryConfig"),
        "maintenanceWindows": t.get("maintenanceWindows"),
        "destinationPackageVersions": t.get("destinationPackageVersions"),
    })


def _jobs_page(items: list, qp: dict, kind: str) -> tuple[list, str | None, tuple | None]:
    """The ``maxResults`` / ``nextToken`` page of ``items`` as (page, token, error); a token is an offset."""
    raw = qp.get("maxResults")
    try:
        limit = _JOB_PAGE_MAX if raw in (None, "") else int(raw)
    except (TypeError, ValueError):
        limit = 0
    if not 1 <= limit <= _JOB_PAGE_MAX:
        bound = ("greater than or equal to 1" if limit < 1
                 else f"less than or equal to {_JOB_PAGE_MAX}")
        return [], None, _job_validation_error([_job_validation_clause(
            raw, "maxResults", f"Member must have value {bound}"
        )])
    start = 0
    token = qp.get("nextToken")
    if token:
        try:
            raw_token = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode()
            prefix, _, offset = raw_token.partition(":")
            start = int(offset) if prefix == kind else -1
        except ValueError:  # binascii.Error and UnicodeDecodeError included
            start = -1
        if start < 0:
            return [], None, error_response_json(
                "InvalidRequestException", "Next token is invalid.", 400
            )
    if start + limit >= len(items):
        return items[start:], None, None
    token = base64.urlsafe_b64encode(f"{kind}:{start + limit}".encode()).decode().rstrip("=")
    return items[start:start + limit], token, None


def _list_job_templates(qp: dict) -> tuple:
    """``GET /job-templates``, newest first, paged by ``maxResults`` /
    ``nextToken``; the token carries an offset into that order."""
    ordered = sorted(_job_templates.values(), key=lambda t: t["createdAt"], reverse=True)
    page, token, error = _jobs_page(ordered, qp, "jobtemplates")
    if error:
        return error
    response = {"jobTemplates": [
        {k: t[k] for k in ("jobTemplateArn", "jobTemplateId", "description", "createdAt")}
        for t in page
    ]}
    if token:
        response["nextToken"] = token
    return json_response(response)


def _delete_job_template(template_id: str) -> tuple:
    if _job_templates.get(template_id) is None:
        return _job_template_not_found(template_id)
    _tags.pop(_job_templates.pop(template_id)["jobTemplateArn"], None)
    # AWS answers 200 with an empty body.
    return 200, {"Content-Type": "application/x-amz-json-1.0"}, b""


def _job_payload_from_template(payload: dict) -> tuple[dict | None, tuple | None]:
    """CreateJob's request with its jobTemplateArn applied: the template's
    document (unless the request names a document or a source), its
    configuration and its maintenance windows; members of the request win.
    Returns (payload, None) or (None, error)."""
    arn = payload["jobTemplateArn"]
    try:
        parsed = parse_arn(arn)
    except ArnParseError:
        return None, error_response_json(
            "InvalidRequestException", f"Invalid jobTemplateArn: {arn}", 400
        )
    resource_type, _, template_id = parsed.resource.partition("/")
    if resource_type != "jobtemplate":
        return None, error_response_json(
            "InvalidRequestException",
            "Resource type should be jobtemplate but found invalid resource type "
            f"{resource_type}",
            400,
        )
    template = _job_templates.get(template_id)
    if template is None or template["jobTemplateArn"] != arn:
        return None, _job_template_not_found(template_id)
    merged = {
        m: copy.deepcopy(template[m]) for m in _JOB_TEMPLATE_CONFIG_MEMBERS if template.get(m)
    }
    if not payload.get("document") and not payload.get("documentSource"):
        merged["document"] = template.get("document")
        merged["documentSource"] = template.get("documentSource")
    merged.update({k: v for k, v in payload.items() if v is not None})
    windows = template.get("maintenanceWindows")
    if windows:
        if merged.get("targetSelection", "SNAPSHOT") == "SNAPSHOT":
            return None, error_response_json(
                "InvalidRequestException",
                "TargetSelection is invalid. MaintenanceWindow cannot be used with "
                "SNAPSHOT job.",
                400,
            )
        sched = dict(merged.get("schedulingConfig") or {})
        if not sched.get("startTime"):
            return None, error_response_json(
                "InvalidRequestException",
                "MaintenanceWindow cannot be used without StartTime of "
                "SchedulingConfig.",
                400,
            )
        sched.setdefault("maintenanceWindows", copy.deepcopy(windows))
        merged["schedulingConfig"] = sched
    return merged, None


def _jobs_cancel(job_id: str, payload: dict, force: bool) -> dict:
    """CancelJob's state change; maps each thing whose execution it canceled to its earlier queue front."""
    job = _jobs[job_id]
    now = _jobs_now_ms()
    job["status"] = "CANCELED"
    job["forceCanceled"] = force
    job["lastUpdatedAt"] = now
    job["completedAt"] = now
    if payload.get("comment") is not None:
        job["comment"] = payload["comment"]
    if payload.get("reasonCode") is not None:
        job["reasonCode"] = payload["reasonCode"]
    _job_event(job, "cancellation_in_progress")
    # QUEUED executions are always canceled with the job; IN_PROGRESS ones
    # only when force is set — as on AWS. Queue fronts are snapshotted before
    # the sweep so notify-next fires only where the front actually changed.
    prev_next = {
        execution["thingName"]: None
        for execution in _job_executions.values()
        if execution["jobId"] == job_id
    }
    for thing_name in prev_next:
        prev_next[thing_name] = jobs_first_pending_job_id(thing_name)
    canceled_things = set()
    for execution in _job_executions.values():
        if execution["jobId"] != job_id:
            continue
        if execution["status"] == "QUEUED" or (
            force and execution["status"] == "IN_PROGRESS"
        ):
            execution["status"] = "CANCELED"
            execution["lastUpdatedAt"] = now
            execution["versionNumber"] += 1
            _job_execution_event(execution, force_canceled=force)
            canceled_things.add(execution["thingName"])
    _job_event(job, "canceled")
    return {thing_name: prev_next[thing_name] for thing_name in canceled_things}


async def _cancel_job(job_id: str, payload: dict, qp: dict) -> tuple:
    job = _jobs.get(job_id)
    if job is None:
        return _error_not_found("Job", job_id)
    if job["status"] != "IN_PROGRESS":
        return error_response_json(
            "InvalidRequestException",
            f"Job {job_id} is in status {job['status']} and cannot be canceled",
            400,
        )
    canceled = _jobs_cancel(job_id, payload, _qp_bool(qp, "force"))
    account_id, region = get_account_id(), get_region()
    for thing_name in sorted(canceled):
        await jobs_notify_thing(account_id, region, thing_name, canceled[thing_name])
    response = {"jobArn": job["jobArn"], "jobId": job_id}
    if job.get("description") is not None:
        response["description"] = job["description"]
    return json_response(response)


async def _handle_thing_jobs(method: str, path: str, body: bytes, qp: dict) -> tuple:
    """Dispatch /things/{name}/jobs[...] control-plane routes.

    Async because CancelJobExecution and DeleteJobExecution publish
    jobs/notify(-next)."""
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
        return await _cancel_job_execution(
            thing, tail[:-len("/cancel")], _parse_body(body), qp
        )
    if "/" not in tail and method == "GET":
        return _describe_job_execution(thing, tail, qp)
    job_id, _, number = tail.partition("/executionNumber/")
    if number and "/" not in job_id and method == "DELETE":
        return await _delete_job_execution(thing, job_id, number, qp)
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
    # A rejoined thing's executions of one job come newest first, as on AWS.
    executions = [
        execution
        for latest in _job_executions.values()
        for execution in _jobs_with_history(latest)
    ]
    for execution in executions:
        if execution["thingName"] != thing:
            continue
        if wanted_status and execution["status"] != wanted_status:
            continue
        if wanted_job and execution["jobId"] != wanted_job:
            continue
        summaries.append({
            "jobId": execution["jobId"],
            "jobExecutionSummary": _job_execution_summary(execution),
        })
    return json_response({"executionSummaries": summaries})


def _job_execution_summary(execution: dict) -> dict:
    """Control-plane JobExecutionSummary, as both execution lists nest it."""
    summary = {
        "status": execution["status"],
        "queuedAt": _jobs_ms_to_s(execution["queuedAt"]),
        "lastUpdatedAt": _jobs_ms_to_s(execution["lastUpdatedAt"]),
        "executionNumber": execution["executionNumber"],
        "retryAttempt": execution.get("retryAttempt", 0),
    }
    if execution.get("startedAt") is not None:
        summary["startedAt"] = _jobs_ms_to_s(execution["startedAt"])
    return summary


def _list_job_executions_for_job(job_id: str, qp: dict) -> tuple:
    """``GET /jobs/{jobId}/things``, most recently updated first, paged like ``ListJobTemplates``."""
    clauses = []
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", job_id):
        clauses.append(_job_validation_clause(
            job_id, "jobId", "Member must satisfy regular expression pattern: [a-zA-Z0-9_-]+"
        ))
    status = qp.get("status")
    if status is not None and status not in _JOB_EXECUTION_STATUSES:
        clauses.append(_job_validation_clause(
            status, "status", "Member must satisfy enum value set: [REMOVED, SUCCEEDED, "
            "TIMED_OUT, IN_PROGRESS, QUEUED, CANCELED, FAILED, REJECTED]"
        ))
    if clauses:
        return _job_validation_error(clauses)
    _, _, error = _jobs_page([], qp, "jobexecutions")
    if error:
        return error
    if job_id not in _jobs:
        return error_response_json(
            "ResourceNotFoundException", f"Job {job_id} cannot be found.", 404
        )
    _jobs_materialize_executions(job_id)
    executions = [
        execution
        for latest in _job_executions.values()
        if latest["jobId"] == job_id
        for execution in _jobs_with_history(latest)
        if status is None or execution["status"] == status
    ]
    executions.sort(key=lambda e: e["lastUpdatedAt"], reverse=True)
    page, token, error = _jobs_page(executions, qp, "jobexecutions")
    if error:
        return error
    response = {"executionSummaries": [
        {"thingArn": _thing_arn(e["thingName"]), "jobExecutionSummary": _job_execution_summary(e)}
        for e in page
    ]}
    if token:
        response["nextToken"] = token
    return json_response(response)


def _describe_job_execution(thing: str, job_id: str, qp: dict) -> tuple:
    """Without `executionNumber` the newest execution, with it that one."""
    _jobs_materialize_executions(job_id)
    execution = _job_executions.get((thing, job_id))
    number = qp.get("executionNumber")
    if execution is not None and number is not None:
        try:
            number = int(number)
        except (TypeError, ValueError):
            return error_response_json(
                "InvalidRequestException", f"Invalid executionNumber: {number!r}", 400
            )
        execution = next(
            (e for e in _jobs_with_history(execution) if e["executionNumber"] == number),
            None,
        )
    if execution is None:
        return _error_not_found("JobExecution" if job_id in _jobs else "JobExecution:job", job_id, thing=thing)
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


async def _cancel_job_execution(
    thing: str, job_id: str, payload: dict, qp: dict
) -> tuple:
    _jobs_materialize_executions(job_id)
    execution = _job_executions.get((thing, job_id))
    if execution is None:
        return _error_not_found("JobExecution" if job_id in _jobs else "JobExecution:job", job_id, thing=thing)
    error = _jobs_check_expected_version(
        execution, payload.get("expectedVersion"), "VersionConflictException"
    )
    if error:
        return error
    error = _jobs_reject_if_terminal(
        execution, f"Job execution for thing {thing} within job {job_id} cannot be canceled."
    )
    if error:
        return error
    force = _qp_bool(qp, "force")
    if execution["status"] == "IN_PROGRESS" and not force:
        return error_response_json(
            "InvalidStateTransitionException",
            "Job execution is IN_PROGRESS and cannot be canceled without force",
            409,
        )
    prev_next = jobs_first_pending_job_id(thing)
    execution["status"] = "CANCELED"
    if payload.get("statusDetails"):
        execution["statusDetails"] = dict(payload["statusDetails"])
    execution["lastUpdatedAt"] = _jobs_now_ms()
    execution["versionNumber"] += 1
    _job_execution_event(execution, force_canceled=force)
    _jobs_maybe_complete(job_id)
    await jobs_notify_thing(get_account_id(), get_region(), thing, prev_next)
    await jobs_abort_if_met(get_account_id(), get_region(), job_id)
    return json_response({})


async def _delete_job_execution(thing: str, job_id: str, number: str, qp: dict) -> tuple:
    """Delete one of a thing's executions of a job; an IN_PROGRESS one only with force."""
    try:
        number = int(number)
    except ValueError:
        return error_response_json(
            "SerializationException", f"'{number}' can not be converted to Long", 400
        )
    if number < 1:
        return error_response_json(
            "InvalidRequestException", "Execution number is invalid.", 400
        )
    job = _jobs.get(job_id)
    if job is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"Job execution cannot be found as the Job {job_id} does not exist.",
            404,
        )
    _jobs_materialize_executions(job_id)
    latest = _job_executions.get((thing, job_id))
    executions = _jobs_with_history(_jobs_apply_timeout(latest)) if latest else []
    execution = next((e for e in executions if e["executionNumber"] == number), None)
    if execution is None:
        return error_response_json(
            "ResourceNotFoundException",
            f"Job execution for thing {thing} cannot be found within job {job_id}.",
            404,
        )
    if execution["status"] == "IN_PROGRESS" and not _qp_bool(qp, "force"):
        return error_response_json(
            "InvalidStateTransitionException",
            f"Job execution for thing {thing} within job {job_id} in status "
            "IN_PROGRESS cannot be deleted without force flag.",
            409,
        )
    pending = execution["status"] in ("QUEUED", "IN_PROGRESS")
    prev_next = jobs_first_pending_job_id(thing) if pending else None
    # The next newest execution, if any, becomes the thing's current one.
    rest = [
        {k: v for k, v in e.items() if k != "history"}
        for e in executions
        if e is not execution
    ]
    if rest:
        if len(rest) > 1:
            rest[0]["history"] = rest[1:]
        _job_executions[(thing, job_id)] = rest[0]
    else:
        del _job_executions[(thing, job_id)]
        if job.get("targetSelection") == "CONTINUOUS":
            job.setdefault("executionsDeletedFor", []).append(thing)
    _job_execution_event(execution, "DELETED")
    _jobs_maybe_complete(job_id)
    if pending:
        await jobs_notify_thing(get_account_id(), get_region(), thing, prev_next)
    await jobs_abort_if_met(get_account_id(), get_region(), job_id)
    return 200, {}, b""


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
    for execution in _job_executions.values():
        if execution["thingName"] == thing_name:
            _jobs_apply_timeout(execution)
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


def jobs_first_pending_job_id(thing_name: str) -> str | None:
    """jobId at the front of the thing's pending queue — the execution
    ``notify-next`` points at — or ``None`` when the thing is idle.

    Callers about to mutate the pending set capture this BEFORE the mutation
    and hand it to :func:`jobs_notify_thing`, which publishes ``notify-next``
    only when the front actually changed."""
    pending = jobs_pending_for_thing(thing_name)
    return pending[0]["jobId"] if pending else None


def jobs_describe_execution(thing_name: str, job_id: str) -> dict | None:
    """One execution (a copy), materializing the job's targets first."""
    _jobs_materialize_executions(job_id)
    execution = _job_executions.get((thing_name, job_id))
    if execution is None:
        return None
    return dict(_jobs_apply_timeout(execution))


# Control characters as AWS escapes them in the document it sends a device; a lone
# surrogate stays escaped too, since UTF-8 cannot carry it.
_JOBS_DEVICE_ESCAPES = {
    **{code: f"\\u{code:04X}" for code in (*range(0x20), *range(0xD800, 0xE000))},
    0x08: "\\b", 0x09: "\\t", 0x0A: "\\n", 0x0C: "\\f", 0x0D: "\\r", 0x22: '\\"', 0x5C: "\\\\",
}


def _jobs_device_float(value: float) -> str:
    """A float spelled as Java's Double.toString, with a non-finite value as a string."""
    if not math.isfinite(value):
        return f'"{json.dumps(value)}"'
    if value == 0 or 1e-3 <= abs(value) < 1e7:
        return repr(value)
    from decimal import Decimal

    sign, digits, exponent = Decimal(repr(value)).normalize().as_tuple()
    text = "".join(map(str, digits))
    return f"{'-' * sign}{text[0]}.{text[1:] or '0'}E{len(text) - 1 + exponent}"


def _jobs_device_json(value) -> str:
    """Compact JSON of a job document as AWS sends it to a device: object members that are null are left out."""
    if isinstance(value, dict):
        return "{" + ",".join(
            f"{_jobs_device_json(key)}:{_jobs_device_json(member)}"
            for key, member in value.items() if member is not None
        ) + "}"
    if isinstance(value, list):
        return "[" + ",".join(map(_jobs_device_json, value)) + "]"
    if isinstance(value, str):
        return '"' + value.translate(_JOBS_DEVICE_ESCAPES) + '"'
    if isinstance(value, float):
        return _jobs_device_float(value)
    return json.dumps(value)


# The host a device reached MiniStack on: its presigned URLs name it.
jobs_device_host: contextvars.ContextVar[str] = contextvars.ContextVar(
    "_iot_jobs_device_host", default=""
)
_JOB_PRESIGN_HOST_RE = re.compile(r"[A-Za-z0-9.\-\[\]:_]+")
# The labels of an IoT endpoint host up to its region; S3 is not routed under them.
_JOB_PRESIGN_IOT_LABELS_RE = re.compile(r"(?:[^.:]+\.)*?iot\.[a-z0-9-]+\.(?=[^:])")
# A role session signs an hour of URLs, each valid for its full expiresInSec.
_JOB_PRESIGN_SESSION_REUSE_SECONDS = 3600
# (account, role ARN) -> the access key of that role's current session.
_jobs_presign_sessions: dict[tuple[str, str], str] = {}


def _iot_role_usable(role_arn: str) -> bool:
    """Under AUTH=true, whether the role exists in the caller's account and its trust policy lets IoT assume it."""
    from ministack import app

    if not app.AUTH:
        return True
    from ministack.core.iam_evaluator import _action_matches, _service_principal_matches
    from ministack.services import iam as _iam

    parts = role_arn.split(":", 5)
    if len(parts) != 6 or parts[4] != get_account_id() or not parts[5].startswith("role/"):
        return False
    role = _iam._roles.get(parts[5].rsplit("/", 1)[-1])
    try:
        statements = json.loads((role or {}).get("AssumeRolePolicyDocument") or "{}").get("Statement", [])
    except (ValueError, AttributeError):
        return False
    allowed = False
    for stmt in statements if isinstance(statements, list) else [statements]:
        if not isinstance(stmt, dict) or not _service_principal_matches(stmt.get("Principal"), "iot.amazonaws.com"):
            continue
        actions, not_actions = stmt.get("Action", []), stmt.get("NotAction", [])
        if not _action_matches("sts:AssumeRole", [actions] if isinstance(actions, str) else actions,
                               [not_actions] if isinstance(not_actions, str) else not_actions):
            continue
        if stmt.get("Effect") == "Deny":
            return False
        allowed = allowed or stmt.get("Effect") == "Allow"
    return allowed


def _jobs_presign_session(role_arn: str, expires: int) -> tuple[str, dict]:
    """A session of the presign role, registered with STS, that outlives a URL signed now."""
    from ministack.services import iam as _iam
    from ministack.services import sts as _sts

    cache_key = (get_account_id(), role_arn)
    access_key = _jobs_presign_sessions.get(cache_key, "")
    session = _sts._sessions.get(access_key)
    if session is None or session["Expiration"] < time.time() + expires:
        partition = role_arn.split(":")[1] if _JOB_ROLE_ARN_RE.match(role_arn) else "aws"
        account = get_account_id()  # the job's account, whatever account the role ARN names
        name = uuid.uuid4().hex
        access_key = _iam._gen_session_access_key()
        session = {
            "Arn": f"arn:{partition}:sts::{account}:assumed-role/{role_arn.rsplit('/', 1)[-1]}/{name}",
            "UserId": f"AROA{uuid.uuid4().hex[:17].upper()}:{name}",
            "SecretAccessKey": _iam._gen_secret(),
            "SessionToken": _iam._gen_session_token(),
            "Expiration": time.time() + expires + _JOB_PRESIGN_SESSION_REUSE_SECONDS,
            "AccountId": account,
            "PrincipalType": "AssumedRole",
        }
        _sts.register_session(access_key, session)
        _jobs_presign_sessions[cache_key] = access_key
    return access_key, session


def _jobs_presigned_document(job: dict) -> str:
    """The job document with each placeholder replaced by a presigned GET URL of MiniStack's S3."""
    document = job.get("document") or "{}"
    config = job.get("presignedUrlConfig")
    if "${aws:iot:s3-presigned-url" not in document or not (
        isinstance(config, dict) and config.get("roleArn")
    ) or not _iot_role_usable(config["roleArn"]):
        return document
    from ministack.core.sigv4 import (
        build_canonical_query_string,
        build_canonical_request,
        build_string_to_sign,
        calculate_signature,
        uri_encode,
    )
    from ministack.core.tls import use_ssl_enabled

    expires = config.get("expiresInSec")
    expires = int(expires) if isinstance(expires, (int, float)) and expires > 0 else 3600
    access_key, session = _jobs_presign_session(config["roleArn"], expires)
    host = jobs_device_host.get()
    if not _JOB_PRESIGN_HOST_RE.fullmatch(host):
        host = f"{_MINISTACK_HOST}:{_GATEWAY_PORT}"
    elif labels := _JOB_PRESIGN_IOT_LABELS_RE.match(host):
        host = host[labels.end():]
    base = f"{'https' if use_ssl_enabled() else 'http'}://{host}"
    amz_date = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    region = get_region()
    query = {
        "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
        "X-Amz-Credential": f"{access_key}/{amz_date[:8]}/{region}/s3/aws4_request",
        "X-Amz-Date": amz_date,
        "X-Amz-Expires": str(expires),
        "X-Amz-Security-Token": session["SessionToken"],
        "X-Amz-SignedHeaders": "host",
    }
    canonical_query = build_canonical_query_string(query)

    def presign(match: re.Match) -> str:
        bucket, key = _job_document_s3_location(match.group("url"))
        if not (bucket and key):
            return match.group(0)
        path = f"/{bucket}/{key}"
        string_to_sign = build_string_to_sign(
            amz_date, amz_date[:8], region, "s3",
            build_canonical_request("GET", path, {"host": host}, query, "host"),
        )
        signature = calculate_signature(
            session["SecretAccessKey"], amz_date[:8], region, "s3", string_to_sign
        )
        return (f"{base}{uri_encode(path, encode_slash=False)}?{canonical_query}"
                f"&X-Amz-Signature={signature}")

    return _JOB_PRESIGN_RE.sub(presign, document)


def jobs_job_document(job_id: str) -> str:
    """The document a device should run for a job, re-serialized as AWS sends it — ``{}`` if there is none."""
    job = _jobs.get(job_id)
    document = _jobs_presigned_document(job) if job else "{}"
    try:
        return _jobs_device_json(json.loads(document))
    except (ValueError, RecursionError):
        return document


def _jobs_job_document_object(job_id: str):
    """The job document as a JSON object — the shape AWS serves over MQTT
    (over HTTP the same document stays a string; see jobs-mqtt-api vs the
    REST JobExecution shape). A stored document that is not valid JSON
    passes through as the raw string rather than being invented into one."""
    document = jobs_job_document(job_id)
    try:
        return json.loads(document)
    except ValueError:
        return document


def jobs_update_execution(
    thing_name: str,
    job_id: str,
    *,
    status,
    expected_version=None,
    status_details: dict | None = None,
    step_timeout_minutes=None,
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
            f"Job {job_id} not found for {thing_name}",
            404,
        )
    error = _jobs_check_expected_version(
        execution, expected_version, "InvalidStateTransitionException"
    )
    if error:
        return None, error
    if not status or status not in _JOB_EXECUTION_STATUSES:
        return None, error_response_json(
            "InvalidRequestException",
            f"Invalid job execution status: {status!r}",
            400,
        )
    if status not in _DEVICE_SETTABLE_STATUSES:
        # InvalidStateTransitionException, not InvalidRequestException
        # (measured eu-north-1 2026-09-19).
        return None, error_response_json(
            "InvalidStateTransitionException",
            f"The status of job execution cannot be changed to be {status}",
            409,
        )
    error = _jobs_reject_if_terminal(
        execution,
        f"Job {job_id} for {thing_name} has already finished with status {execution['status']}",
    )
    if error:
        return None, error
    now = _jobs_now_ms()
    execution["status"] = status
    if status_details:
        execution["statusDetails"] = dict(status_details)
    # The first report starts the execution, unless the device rejects the job.
    if status != "REJECTED" and execution.get("startedAt") is None:
        execution["startedAt"] = now
    if status == "IN_PROGRESS" and step_timeout_minutes is not None:
        # The device's own step timeout, reset every time it re-reports
        # IN_PROGRESS with a new value.
        execution["stepTimeoutInMinutes"] = step_timeout_minutes
        execution["timeoutStartedAt"] = now
    execution["lastUpdatedAt"] = now
    execution["versionNumber"] += 1
    if status in _JOB_EXECUTION_TERMINAL:
        _job_execution_event(execution)
    _jobs_maybe_complete(job_id)
    return dict(execution), None


async def jobs_abort_if_met(account_id: str, region: str, job_id: str) -> None:
    """Apply the job's abortConfig after an execution finished, notifying the things whose execution it canceled."""
    with request_scope(account_id, region):
        canceled = _jobs_abort(job_id)
    for thing_name in sorted(canceled):
        await jobs_notify_thing(account_id, region, thing_name, canceled[thing_name])


def jobs_execution_view(execution: dict, document: str | dict | None) -> dict:
    """Device-plane JobExecution — whole-second ``long`` timestamps (the API
    reference words every stamp as "the time, in seconds since the epoch"),
    flat statusDetails map. Shared by the iot-jobs-data HTTP plane (which
    passes ``document`` as the string AWS serves there) and the
    jobs-over-MQTT bridge (which passes the parsed object) so both speak one
    shape."""
    view = {
        "jobId": execution["jobId"],
        "thingName": execution["thingName"],
        "status": execution["status"],
        "statusDetails": execution.get("statusDetails") or {},
        "queuedAt": _jobs_ms_to_long_s(execution["queuedAt"]),
        "lastUpdatedAt": _jobs_ms_to_long_s(execution["lastUpdatedAt"]),
        "executionNumber": execution["executionNumber"],
        "versionNumber": execution["versionNumber"],
    }
    if execution.get("startedAt") is not None:
        view["startedAt"] = _jobs_ms_to_long_s(execution["startedAt"])
    remaining = _jobs_seconds_before_timeout(execution)
    if remaining is not None:
        view["approximateSecondsBeforeTimedOut"] = remaining
    if document is not None:
        view["jobDocument"] = document
    return view


def jobs_execution_summary(execution: dict) -> dict:
    """Device-plane JobExecutionSummary (the notify / GetPendingJobExecutions
    entry) — same whole-second stamps as the full view."""
    entry = {
        "jobId": execution["jobId"],
        "queuedAt": _jobs_ms_to_long_s(execution["queuedAt"]),
        "lastUpdatedAt": _jobs_ms_to_long_s(execution["lastUpdatedAt"]),
        "versionNumber": execution["versionNumber"],
        "executionNumber": execution["executionNumber"],
    }
    if execution.get("startedAt") is not None:
        entry["startedAt"] = _jobs_ms_to_long_s(execution["startedAt"])
    return entry


async def jobs_notify_thing(
    account_id: str, region: str, thing_name: str, prev_next_job_id: str | None
) -> None:
    """Publish ``$aws/things/<t>/jobs/notify`` — and ``notify-next`` when the
    front of the queue changed — after an execution was ADDED to or REMOVED
    from the thing's pending set (job created, canceled, or deleted, or a
    terminal device report).

    AWS publishes ``notify`` on every pending-set membership change and
    ``notify-next`` only when the FIRST pending execution is now a different
    job (jobs-comm-notifications, confirmed against a live device capture):
    creating a job behind existing ones notifies without a notify-next, and
    starting the job already at the front — QUEUED → IN_PROGRESS keeps it
    both pending and first — publishes NEITHER, so start paths never call
    here. ``prev_next_job_id`` is the front-of-queue jobId captured before
    the mutation (:func:`jobs_first_pending_job_id`).

    Payloads (all timestamps epoch seconds): ``notify`` carries
    ``{"timestamp", "jobs": {<STATUS>: [summaries]}}`` with empty status
    lists omitted (``{}`` once nothing is pending); ``notify-next`` carries
    the execution view (``jobDocument`` as a JSON object, no ``thingName``)
    or a bare ``{"timestamp"}`` when the queue emptied.

    Known boundaries: an execution materialized lazily on a *read* does not
    notify — the materialization sites are synchronous read paths (a thing
    joining a CONTINUOUS job's target group is queued eagerly by
    AddThingToThingGroup, which does notify) — and DeleteThing's
    execution sweep does not notify the deleted thing (that sweep is itself
    a divergence: AWS keeps a deleted thing's executions, so it publishes
    nothing there either)."""
    with request_scope(account_id, region):
        pending = jobs_pending_for_thing(thing_name)
        nxt = jobs_start_next_execution(thing_name, peek=True)
        next_view = (
            jobs_execution_view(nxt, _jobs_job_document_object(nxt["jobId"]))
            if nxt is not None
            else None
        )
    if next_view is not None:
        # AWS sends no thingName and no empty statusDetails here (measured
        # eu-central-1 2026-10-05).
        del next_view["thingName"]
        if not next_view["statusDetails"]:
            del next_view["statusDetails"]
    ts = int(time.time())
    jobs_by_status: dict = {}
    for execution in pending:
        jobs_by_status.setdefault(execution["status"], []).append(
            jobs_execution_summary(execution)
        )
    base = f"$aws/things/{thing_name}/jobs"
    await broker_publish(
        account_id, region, f"{base}/notify",
        json.dumps({"timestamp": ts, "jobs": jobs_by_status}).encode("utf-8"), qos=1,
    )
    next_job_id = nxt["jobId"] if nxt is not None else None
    if next_job_id == prev_next_job_id:
        return
    notify_next: dict = {"timestamp": ts}
    if next_view is not None:
        notify_next["execution"] = next_view
    await broker_publish(
        account_id, region, f"{base}/notify-next",
        json.dumps(notify_next).encode("utf-8"), qos=1,
    )


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
# assigned client identifier, message expiry, and pass-through of the
# PUBLISH properties a subscriber may echo back. Out of scope, and advertised
# as unavailable in CONNACK where the protocol has a flag for it: shared
# subscriptions, topic aliases, flow control (receive maximum), subscription
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
# Next lifecycle-event ``versionNumber`` per (account, region, client id).
_presence_versions: dict[tuple[str, str, str], int] = {}
_broker_lock = asyncio.Lock()

_SESSION_EXPIRY_SECONDS: int = int(os.environ.get("IOT_SESSION_EXPIRY_SECONDS", "3600"))
# MQTT 5 §3.1.2.11.2: 0xFFFFFFFF means the session never expires.
_SESSION_EXPIRY_NEVER = 0xFFFFFFFF
_MAX_QUEUED_MESSAGES = 1000


class _PersistentSessionState:
    __slots__ = (
        "subscriptions", "queued_messages", "created_at", "expiry_interval",
        "session_identifier",
    )

    def __init__(
        self,
        subscriptions: list[str],
        created_at: float,
        expiry_interval: int | None = None,
        session_identifier: str | None = None,
    ):
        self.subscriptions: list[str] = subscriptions
        # (topic, payload, qos, PUBLISH properties, time the broker received it)
        self.queued_messages: list[tuple[str, bytes, int, bytes, float]] = []
        self.created_at: float = created_at
        # Seconds this session outlives its connection, as the client asked
        # for it in the MQTT 5 Session Expiry Interval. None means the client
        # named no interval (every 3.1.1 client, and a v5 client that omitted
        # the property), so the module-wide default applies.
        self.expiry_interval: int | None = expiry_interval
        # Reported as ``sessionIdentifier`` by every connection that resumes it.
        self.session_identifier: str = session_identifier or str(uuid.uuid4())


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


def _subscription_matches(filter_: str, topic: str) -> bool:
    """Whether a subscription's or rule's filter matches; ``$aws/events`` skips filters that start with a wildcard."""
    if topic.startswith("$aws/events/") and filter_[:1] in ("#", "+"):
        return False
    return _topic_matches(filter_, topic)


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


# The reserved topic space a client may publish into. AWS closes the connection
# of an MQTT client that publishes to any other topic starting with ``$``
# (measured for ``$aws/events/...``, ``$aws/jobs/...`` and ``$foo/...``; a
# publish under ``$aws/rules/`` and ``$aws/things/`` is acknowledged). The HTTPS
# Publish in iot_data.py applies the same list and answers 400.
RESERVED_PUBLISH_PREFIXES = (
    "$aws/rules/",
    "$aws/things/",
    "$aws/certificates/",
    "$aws/provisioning-templates/",
    "$aws/device_location/",
    "$aws/commands/",
)


def _reserved_topic_publish_allowed(topic: str) -> bool:
    return not topic.startswith("$") or topic.startswith(RESERVED_PUBLISH_PREFIXES)


_POLICY_VARIABLE_RE = re.compile(r"\$\{(iot:[A-Za-z.]+)\}")


def _mqtt_policy_context(account_id: str, region: str, certificate_id: str, client_id: str) -> dict:
    """The policy variables of a certificate session, keyed in lower case."""
    with request_scope(account_id, region):
        record = _certificates.get(certificate_id) or {}
        attached = client_id in _things and _thing_arn(client_id) in record.get("attachedThings", [])
    context = {"iot:clientid": client_id, "iot:connection.thing.isattached": str(attached).lower()}
    if attached:
        context["iot:connection.thing.thingname"] = client_id
    common_name = certificate_common_name(record["certificatePem"]) if record.get("certificatePem") else None
    if common_name is not None:
        context["iot:certificate.subject.commonname"] = common_name
    return context


@functools.lru_cache(maxsize=1024)
def _parsed_iot_policy(document: str, context: tuple) -> list:
    """The statements of an IoT policy document with the policy variables in ``context`` substituted."""
    values = dict(context)

    def substitute(match: re.Match) -> str:
        value = values.get(match.group(1).lower())
        return match.group(0) if value is None else json.dumps(value)[1:-1]

    return parse_policy_document(_POLICY_VARIABLE_RE.sub(substitute, document))


@functools.lru_cache(maxsize=256)
def _provisioning_claim_policy(template_name: str) -> str:
    """The policy of a claim certificate: its template's fleet provisioning topics and nothing else."""
    topics = [
        "$aws/certificates/create/*",
        "$aws/certificates/create-from-csr/*",
        f"$aws/provisioning-templates/{template_name}/provision/*",
    ]
    return json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "iot:Connect", "Resource": "*"},
        {"Effect": "Allow", "Action": ["iot:Publish", "iot:Receive"],
         "Resource": [f"arn:aws:iot:*:*:topic/{topic}" for topic in topics]},
        {"Effect": "Allow", "Action": "iot:Subscribe",
         "Resource": [f"arn:aws:iot:*:*:topicfilter/{topic}" for topic in topics]},
    ]})


def _mqtt_policy_allows(
    account_id: str,
    region: str,
    certificate_id: str,
    context: dict,
    action: str,
    resource: str,
    provisioning_template: str | None = None,
) -> bool:
    """Whether the policies attached to the certificate, or a claim's own policy, allow ``action`` on ``resource``."""
    if provisioning_template is not None:
        documents = [_provisioning_claim_policy(provisioning_template)]
    else:
        with request_scope(account_id, region):
            record = _certificates.get(certificate_id) or {}
            policies = [_policies.get(name) for name in record.get("attachedPolicies", [])]
        documents = [p["versions"][p["defaultVersionId"]]["document"] for p in policies if p]
    ctx = EvalContext(
        principal_arn=f"arn:aws:iot:{region}:{account_id}:cert/{certificate_id}",
        principal_type="Certificate",
        principal_account=account_id,
        action=action,
        resource_arn=f"arn:aws:iot:{region}:{account_id}:{resource}",
        region=region,
        service_context=context,
    )
    variables = tuple(sorted(context.items()))
    return evaluate(ctx, [_parsed_iot_policy(doc, variables) for doc in documents]).decision == "Allow"


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
        _presence_versions.clear()


_BASIC_INGEST_PREFIX = "$aws/rules/"
SHADOW_TOPIC_PREFIX = "$aws/things/"


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


# Depth guard for republish chains: put_topic_rule refuses an action that
# republishes into the rule's own filter, but an errorAction that does, a
# cycle across rules or a rule restored from saved state would still recurse
# without bound, so the chain is cut and logged past this depth.
_MAX_REPUBLISH_DEPTH = 8
_republish_depth: contextvars.ContextVar[int] = contextvars.ContextVar(
    "_iot_republish_depth", default=0
)

_ROLE_SESSION_NAME_CHARS = string.ascii_letters + string.digits


def _rule_role_principal(account_id: str, role_arn: str) -> str | None:
    """principal() of a republished message: the rule role's session, as GetCallerIdentity names it."""
    from ministack.services import iam as _iam

    try:
        arn = parse_arn(role_arn)
    except ArnParseError:
        return None
    if arn.account_id != account_id or not arn.resource.startswith("role/"):
        return None
    role = _iam._roles.get_scoped(account_id, None, arn.resource.rsplit("/", 1)[-1])
    if not role:
        return None
    # The rules engine names its role sessions with eight random letters and digits.
    session = "".join(secrets.choice(_ROLE_SESSION_NAME_CHARS) for _ in range(8))
    return f"{role['RoleId']}:{session}"


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
            "IoT rule %s: republish depth limit (%d) reached — dropping republish to %r",
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
            principal=_rule_role_principal(account_id, spec.get("roleArn") or ""),
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
        # The id traceid() returns for the same message.
        "cloudwatchTraceId": _rule_message_identity.get()[1],
        # "N/A", not "" — and not clientid()'s lowercase "n/a" (measured eu-north-1 2026-09-19).
        "clientId": client_id or "N/A",
        "sourceIp": _publish_source_ip.get(),
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


# failedResource is the resource the action targeted, a different member per type.
_RULE_ACTION_RESOURCE_KEYS = {
    "dynamoDBv2": ("putItem", "tableName"),
    "sns": ("targetArn",),
    "sqs": ("queueUrl",),
    "republish": ("topic",),
    "lambda": ("functionArn",),
}


def _rule_action_name(action_type: str) -> str:
    """``failedAction`` for an action type: the key capitalised, plus ``Action``.
    ``DynamoDBv2Action`` (measured eu-north-1 2026-09-19); ``S3Action`` is the reference's own example."""
    if not action_type:
        return ""
    return action_type[0].upper() + action_type[1:] + "Action"


def _rule_action_resource(action: dict, action_type: str) -> str:
    """The resource an action names, for the errorAction document's
    ``failedResource``. Empty when the action type does not name one."""
    spec = action.get(action_type)
    if not isinstance(spec, dict):
        return ""
    for key in _RULE_ACTION_RESOURCE_KEYS.get(action_type, ()):
        value = spec.get(key)
        if isinstance(value, dict):
            spec = value
            continue
        if isinstance(value, str):
            return value
    return ""


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
            failures.append({
                "failedAction": _rule_action_name(action_type),
                "failedResource": _rule_action_resource(action, action_type),
                "errorMessage": f"{type(exc).__name__}: {exc}",
            })
    if failures:
        await _dispatch_rule_error_action(
            account_id, region, rule, topic, payload, client_id, failures
        )


async def _evaluate_topic_rules(
    account_id: str, region: str, topic: str, payload: bytes, client_id: str | None = None
) -> None:
    for rule in _rules_for_account(account_id, region):
        filter_ = _rule_topic_filter(rule.get("sql", ""))
        if filter_ and _subscription_matches(filter_, topic):
            await _run_rule_actions(account_id, region, rule, payload, topic, client_id)


def _parse_shadow_topic(topic: str) -> tuple[str, str, str | None] | None:
    """``$aws/things/<thing>/shadow[/name/<n>]/<verb>`` → (thing, verb, name).

    Anything longer — the ``accepted`` / ``rejected`` / ``delta`` /
    ``documents`` response suffixes — parses as ``None``, so the bridge's own
    response publishes can never re-trigger it (no recursion)."""
    if not topic.startswith(SHADOW_TOPIC_PREFIX):
        return None
    rest = topic[len(SHADOW_TOPIC_PREFIX):]
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


def _parse_jobs_topic(topic: str) -> tuple[str, str, str | None] | None:
    """``$aws/things/<thing>/jobs[/<jobId>]/<verb>`` → (thing, verb, jobId).

    Request verbs only: ``get`` and ``start-next`` at the thing level,
    ``get`` and ``update`` at the job level — with ``$next`` accepted as the
    jobId on ``get``, the documented sentinel that resolves like the HTTP
    plane's DescribeJobExecution of ``$next``. The response and notification
    suffixes (``accepted`` / ``rejected`` / ``notify`` / ``notify-next``)
    parse as ``None`` so the bridge's own publishes never re-trigger it —
    the same recursion guard the shadow bridge uses. A job literally named
    ``get``, ``start-next``, ``notify``, or ``notify-next`` (all valid
    jobIds control-side) is unroutable here: its topics collide with the
    reserved thing-level and notification topics, so such a job can only be
    worked over HTTP."""
    if not topic.startswith(SHADOW_TOPIC_PREFIX):
        return None
    rest = topic[len(SHADOW_TOPIC_PREFIX):]
    parts = rest.split("/")
    if len(parts) < 3 or parts[1] != "jobs":
        return None
    thing = parts[0]
    tail = parts[2:]
    if len(tail) == 1:
        if tail[0] in ("get", "start-next"):
            return thing, tail[0], None
        return None
    if len(tail) == 2:
        job_id, verb = tail
        if job_id in ("get", "start-next", "notify", "notify-next"):
            return None
        if verb == "get" or (verb == "update" and job_id != "$next"):
            return thing, verb, job_id
    return None


def _jobs_rejected(
    code: str, message: str, client_token=None, execution_state: dict | None = None
) -> dict:
    """AWS's jobs error response document — ``code`` is the string ErrorCode
    enum (InvalidRequest, InvalidJson, ResourceNotFound, VersionMismatch,
    InvalidStateTransition, TerminalStateReached), never a numeric HTTP
    status."""
    doc = {"code": code, "message": message, "timestamp": int(time.time())}
    if execution_state is not None:
        doc["executionState"] = execution_state
    if client_token is not None:
        doc["clientToken"] = client_token
    return doc


def _jobs_error_tuple_to_rejected(
    error: tuple, client_token=None, execution: dict | None = None
) -> dict:
    """Map a jobs_* seam error (a ready HTTP response tuple) onto the MQTT
    rejected document.

    The HTTP status folds onto the string ErrorCode enum; the three 409s all
    share one HTTP ``__type`` (InvalidStateTransitionException), so the
    specific code is read off the message sentence — every candidate
    sentence is authored in this module (_jobs_check_expected_version,
    _jobs_reject_if_terminal, jobs_update_execution), so the match cannot
    drift silently. ``execution`` is the execution's current state, when it
    exists: VersionMismatch and InvalidStateTransition rejections carry its
    status + versionNumber as ``executionState``, as AWS does (live-captured
    for the version case)."""
    status = error[0]
    try:
        message = json.loads(error[2]).get("message", "")
    except Exception:
        message = ""
    if status == 404:
        code = "ResourceNotFound"
    elif status == 400:
        code = "InvalidRequest"
    elif status == 410:
        code = "TerminalStateReached"
    elif message.startswith("Expected version"):
        code = "VersionMismatch"
    else:
        code = "InvalidStateTransition"
    execution_state = None
    # A refused status carries no executionState.
    refused_status = message.startswith("The status of job execution cannot be changed")
    if code in ("VersionMismatch", "InvalidStateTransition") and execution is not None and not refused_status:
        execution_state = {
            "status": execution["status"],
            "versionNumber": execution["versionNumber"],
        }
    return _jobs_rejected(code, message, client_token, execution_state)


async def _handle_jobs_publish(
    account_id: str,
    region: str,
    thing: str,
    verb: str,
    job_id: str | None,
    payload: bytes,
) -> None:
    """Jobs-over-MQTT bridge: serve the reserved jobs request topics from the
    same store the iot-jobs-data HTTP plane uses, publishing the
    ``accepted`` / ``rejected`` responses back through the broker.

    Payload members (`statusDetails`, `expectedVersion`,
    `includeJobExecutionState`, `includeJobDocument`, `clientToken`) follow
    the device SDK contract and mirror the HTTP plane's handling; the request
    members `stepTimeoutInMinutes` and `executionNumber` are accepted but
    ignored — MiniStack has no execution timeouts, and `executionNumber` is
    write-once so there is never more than one execution to address. Every
    ``timestamp`` and execution stamp is epoch seconds; ``jobDocument`` is
    served as a JSON object here (over HTTP it stays a string, as on AWS)."""
    base = f"{SHADOW_TOPIC_PREFIX}{thing}/jobs"
    suffix_base = f"{job_id}/{verb}" if job_id is not None else verb

    async def _respond(kind: str, doc: dict) -> None:
        await broker_publish(
            account_id, region,
            f"{base}/{suffix_base}/{kind}",
            json.dumps(doc).encode("utf-8"), qos=1,
        )

    try:
        request = json.loads(payload) if payload else {}
    except (ValueError, UnicodeDecodeError):
        await _respond(
            "rejected",
            _jobs_rejected(
                "InvalidJson",
                "The contents of the request could not be interpreted as "
                "valid UTF-8-encoded JSON",
            ),
        )
        return
    if not isinstance(request, dict):
        # A JSON array or scalar parses, but is not a request document.
        await _respond(
            "rejected",
            _jobs_rejected("InvalidJson", "The request payload must be a JSON object"),
        )
        return
    client_token = request.get("clientToken")
    ts = int(time.time())

    if verb == "get" and job_id is None:
        with request_scope(account_id, region):
            pending = jobs_pending_for_thing(thing)
        doc = {
            "timestamp": ts,
            "inProgressJobs": [
                jobs_execution_summary(e) for e in pending if e["status"] == "IN_PROGRESS"
            ],
            "queuedJobs": [
                jobs_execution_summary(e) for e in pending if e["status"] == "QUEUED"
            ],
        }
        if client_token is not None:
            doc["clientToken"] = client_token
        await _respond("accepted", doc)
        return

    if verb == "start-next":
        with request_scope(account_id, region):
            execution = jobs_start_next_execution(
                thing, status_details=request.get("statusDetails"), peek=False
            )
            document = (
                _jobs_job_document_object(execution["jobId"])
                if execution is not None
                else None
            )
        doc = {"timestamp": ts}
        if execution is not None:
            doc["execution"] = jobs_execution_view(execution, document)
        if client_token is not None:
            doc["clientToken"] = client_token
        await _respond("accepted", doc)
        # No notify / notify-next here: QUEUED -> IN_PROGRESS keeps the
        # execution both pending and at the front of the queue, and AWS stays
        # silent (live-captured) — likewise for re-handing an execution that
        # is already IN_PROGRESS.
        return

    if verb == "get":
        with request_scope(account_id, region):
            # "$next" resolves like the HTTP plane's DescribeJobExecution
            # sentinel: the front of the pending queue, without starting it.
            if job_id == "$next":
                execution = jobs_start_next_execution(thing, peek=True)
            else:
                execution = jobs_describe_execution(thing, job_id)
            include_document = request.get("includeJobDocument", True)
            document = (
                _jobs_job_document_object(execution["jobId"])
                if execution is not None and include_document
                else None
            )
        if execution is None and job_id != "$next":
            await _respond(
                "rejected",
                _jobs_rejected(
                    "ResourceNotFound",
                    f"Job {job_id} not found for {thing}",
                    client_token,
                ),
            )
            return
        error = jobs_reject_terminal_describe(execution) if execution is not None else None
        if error:
            await _respond("rejected", _jobs_error_tuple_to_rejected(error, client_token))
            return
        # An idle thing's "$next" mirrors the HTTP plane's empty structure.
        doc = {"timestamp": ts}
        if execution is not None:
            doc["execution"] = jobs_execution_view(execution, document)
        if client_token is not None:
            doc["clientToken"] = client_token
        await _respond("accepted", doc)
        return

    # verb == "update"
    with request_scope(account_id, region):
        prev_next = jobs_first_pending_job_id(thing)
        execution, error = jobs_update_execution(
            thing, job_id,
            status=request.get("status"),
            expected_version=request.get("expectedVersion"),
            status_details=request.get("statusDetails"),
        )
        current = jobs_describe_execution(thing, job_id) if error else None
    if error:
        await _respond(
            "rejected", _jobs_error_tuple_to_rejected(error, client_token, current)
        )
        return
    doc = {"timestamp": ts}
    if request.get("includeJobExecutionState"):
        doc["executionState"] = {
            "status": execution["status"],
            "statusDetails": execution.get("statusDetails") or {},
            "versionNumber": execution["versionNumber"],
        }
    if request.get("includeJobDocument"):
        with request_scope(account_id, region):
            doc["jobDocument"] = _jobs_job_document_object(job_id)
    if client_token is not None:
        doc["clientToken"] = client_token
    await _respond("accepted", doc)
    if execution["status"] in ("SUCCEEDED", "FAILED", "REJECTED"):
        # A terminal report removed the execution from the pending set. AWS
        # publishes update/accepted first, then notify + notify-next
        # (live-captured order) — which the _respond above just preserved.
        await jobs_notify_thing(account_id, region, thing, prev_next)
        await jobs_abort_if_met(account_id, region, job_id)


def _shadow_rejected(status: int, message: str, client_token: str | None = None) -> dict:
    """AWS's shadow error response document."""
    doc = {"code": status, "message": message}
    if client_token is not None:
        doc["clientToken"] = client_token
    return doc


def _shadow_document(doc: dict) -> dict:
    """Project a GET snapshot onto a ``documents`` member: ``state`` +
    ``metadata`` + ``version``, nothing else.

    `get_thing_shadow` injects ``state.delta`` (and its ``metadata.delta``)
    and a top-level ``timestamp`` by design, because that is what the GET
    response carries. The documents topic is not a GET: on AWS the
    ``previous``/``current`` members report only ``desired`` and ``reported``
    (the delta has its own topic) and carry no ``timestamp`` of their own —
    the single timestamp sits on the envelope. Publishing the snapshot
    verbatim invented members real devices never see.
    """
    stripped = {k: doc[k] for k in ("state", "metadata", "version") if k in doc}
    for section in ("state", "metadata"):
        block = stripped.get(section)
        if isinstance(block, dict) and "delta" in block:
            stripped[section] = {k: v for k, v in block.items() if k != "delta"}
    return stripped


async def shadow_emit_update_responses(
    account_id: str,
    region: str,
    base: str,
    doc: dict,
    pre_status: int,
    pre_doc: dict,
    post_status: int,
    post_doc: dict,
    client_token: str | None,
) -> None:
    """Publish the reserved-topic responses for one ACCEPTED shadow update:
    ``update/accepted``, ``update/delta`` (when the post-update document
    carries one) and ``update/documents``.

    Shared by the MQTT bridge and the HTTP data plane: on AWS these are
    state-change notifications the Device Shadow service emits however the
    update arrived, which is what lets a topic rule on ``update/accepted``
    fire for an HTTP ``UpdateThingShadow``. The caller must hand in
    deep-copied pre/post snapshots (the store returns live dicts)."""

    async def _respond(suffix: str, payload_doc: dict) -> None:
        await broker_publish(
            account_id,
            region,
            f"{base}/{suffix}",
            json.dumps(payload_doc).encode("utf-8"),
            qos=1,
        )

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
    documents_doc = {
        "previous": _shadow_document(pre_doc) if pre_status == 200 else None,
        "current": _shadow_document(post_doc) if post_status == 200 else None,
        "timestamp": _shadow_now(),
    }
    if client_token is not None:
        documents_doc["clientToken"] = client_token
    await _respond("update/documents", documents_doc)


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
    base = f"{SHADOW_TOPIC_PREFIX}{thing}/shadow" + (f"/name/{name}" if name is not None else "")
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
        await shadow_emit_update_responses(
            account_id, region, base, doc,
            pre_status, pre_doc, post_status, post_doc, client_token,
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
    principal: str | None = None,
) -> None:
    """Deliver a message to every matching subscriber.

    ``properties`` is an encoded MQTT 5 property block forwarded verbatim to
    v5 subscribers (empty for a 3.1.1 publisher or an HTTP publish).

    ``client_id`` names the publishing MQTT client, which is what a No Local
    subscription is defined against (§3.8.3.1): that subscriber does not want
    its own messages back. A publish with no client identity — iot_data's HTTP
    Publish, a rule, an in-process caller — matches no subscriber's identity
    and so is delivered to all of them.

    ``principal`` is who published the message, what the rule SQL
    ``principal()`` returns; rules also see a fresh trace id per message.
    """
    # Basic Ingest: a publish to `$aws/rules/<ruleName>` is delivered straight
    # to that rule's actions and bypasses pub/sub delivery entirely.
    if topic.startswith(_BASIC_INGEST_PREFIX):
        remainder = topic[len(_BASIC_INGEST_PREFIX):].split("/", 1)
        rule_name = remainder[0]
        with _rule_message_scope(principal):
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
    received_at = time.time()

    if retain:
        if not payload:
            _retained.pop(scoped, None)
        else:
            _retained[scoped] = _RetainedMessage(scoped, payload, qos, properties, received_at)

    async with _broker_lock:
        subs = [s for sset in _subscriptions.values() for s in sset]

    token = _delivery_properties.set(_aged_properties(properties, received_at, received_at) or b"")
    try:
        for sub in subs:
            if sub.account_id != account_id or sub.region != region:
                continue
            if sub.no_local and client_id is not None and sub.client_id == client_id:
                continue
            if _subscription_matches(
                _unscope_topic(sub.account_id, sub.region, sub.filter_prefixed), topic
            ):
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
                if _subscription_matches(filt, topic):
                    ps.queued_messages.append((topic, payload, qos, properties, received_at))
                    if len(ps.queued_messages) > _MAX_QUEUED_MESSAGES:
                        ps.queued_messages = ps.queued_messages[-_MAX_QUEUED_MESSAGES:]
                    break

    with _rule_message_scope(principal):
        await _evaluate_topic_rules(account_id, region, topic, payload, client_id)

    # Shadow-over-MQTT bridge — AFTER delivery and rule evaluation, so rules
    # matching `$aws/things/+/shadow/update` fire on the request, and the
    # recursive `accepted`/`delta`/`documents` publishes then flow back
    # through this function to drive their own subscribers and rules. The
    # response suffixes parse as None, so the bridge never re-triggers itself.
    # As on AWS, a `#` subscription or rule also gets these `$aws/things`
    # topics; only `$aws/events` is kept from a leading wildcard
    # (`_subscription_matches`).
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

    jobs = _parse_jobs_topic(topic)
    if jobs is not None:
        try:
            async with publishing_events():
                await _handle_jobs_publish(account_id, region, *jobs, payload)
        except Exception:
            # Same guard as the shadow bridge above.
            _broker_logger.warning(
                "IoT jobs bridge failed for %r", topic, exc_info=True
            )


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
                and _subscription_matches(topic_filter, k[len(scope_prefix):])
            ]
        else:
            retained_to_send = []

    for r in retained_to_send:
        properties = _aged_properties(r.properties, r.ts)
        if properties is None:
            continue
        token = _delivery_properties.set(properties)
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


def broker_get_retained(account_id: str, region: str, topic: str) -> "_RetainedMessage | None":
    """The retained message stored for ``topic``, or None; an expired one is dropped here."""
    scoped = _scoped_topic(account_id, region, topic)
    msg = _retained.get(scoped)
    if msg is not None and msg.expired(time.time()):
        _retained.pop(scoped, None)
        return None
    return msg


def broker_list_retained(account_id: str, region: str) -> list[tuple[str, "_RetainedMessage"]]:
    """``(topic, message)`` for every retained message here, most recently stored first."""
    prefix = f"{account_id}/{region}/"
    now = time.time()
    for key in [k for k, m in _retained.items() if k.startswith(prefix) and m.expired(now)]:
        del _retained[key]
    found = [(k[len(prefix):], m) for k, m in _retained.items() if k.startswith(prefix)]
    found.sort(key=lambda item: item[1].ts, reverse=True)
    return found


def broker_reset() -> None:
    _subscriptions.clear()
    _retained.clear()
    _connected_clients.clear()
    _persistent_sessions.clear()
    _connectivity.clear()
    _presence_versions.clear()


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
) -> bool:
    """Drop a session from the registry and record its disconnect.

    ``session`` identifies the caller so a session that has already been
    replaced cannot deregister its successor. A client that reconnects under
    the same client id is taken over: the old session is cleaned up first, and
    its socket close then drives its own handler through cleanup a second time.
    Without this guard that late second pass would evict the live session from
    the registry and backdate its connectivity to the moment the loser died.
    Returns whether this call recorded the disconnect.
    """
    key = (account_id, region, client_id)
    if session is not None and _connected_clients.get(key) is not session:
        return False
    _connected_clients.pop(key, None)
    _connectivity[key] = {
        "timestamp": _now_epoch_millis(),
        "disconnectReason": (
            session.disconnect_reason() if session is not None else "CONNECTION_LOST"
        ),
    }
    return True


def _next_presence_version(account_id: str, region: str, client_id: str) -> int:
    """Take the client id's next lifecycle-event version; connects and disconnects each take one."""
    key = (account_id, region, client_id)
    version = _presence_versions.get(key, 0)
    _presence_versions[key] = version + 1
    return version


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

# PUBLISH properties handed on to subscribers unchanged, except the expiry
# interval, which loses the time the message waited. §3.3.2.3 requires a
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

# AWS IoT Core counts a Message Expiry Interval of 0 as 1 and caps it at seven days.
_MESSAGE_EXPIRY_MAX = 604800

# MQTT 5 PUBLISH properties ride a contextvar rather than the subscriber
# callback signature. ``deliver(topic, payload, qos)`` is the broker's
# extension point — iot_data, persistent-session replay and tests all supply
# their own three-argument callbacks — and only an MQTT 5 session has any use
# for properties. broker_publish sets this around each delivery; every other
# caller sees the empty default and sends an empty property block.
_delivery_properties: contextvars.ContextVar[bytes] = contextvars.ContextVar(
    "_iot_delivery_properties", default=b""
)

# The publisher's IP, for the errorAction document's `sourceIp`. Carried the
# same way as the two below: the publish path is several frames deep and only
# the error document needs it.
_publish_source_ip: contextvars.ContextVar[str] = contextvars.ContextVar(
    "_iot_publish_source_ip", default="127.0.0.1"
)


# The RETAIN flag one delivery goes out with, carried the same way and for the
# same reason. It is resolved per subscriber rather than per message, because
# Retain As Published makes it a property of the subscription: two subscribers
# to one publish can legitimately see different flags.
_delivery_retain: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "_iot_delivery_retain", default=False
)


# The principal and trace id of the message the rules engine is evaluating, for
# principal(), traceid() and the errorAction document. Scoped to one message's
# rule evaluation, so a republished message carries its own.
_rule_message_identity: contextvars.ContextVar[tuple[str | None, str]] = contextvars.ContextVar(
    "_iot_rule_message_identity", default=(None, "")
)


@contextlib.contextmanager
def _rule_message_scope(principal: str | None):
    """Scope a message's principal and a fresh trace id over its rule evaluation."""
    token = _rule_message_identity.set((principal, new_uuid()))
    try:
        yield
    finally:
        _rule_message_identity.reset(token)


def _caller_principal(access_key: str) -> str:
    """principal() of an IAM-signed publish: the caller's user id as GetCallerIdentity reports it."""
    from ministack.core.iam_evaluator import resolve_caller_identity

    identity = resolve_caller_identity(access_key)
    return identity["userId"] if identity else get_account_id()


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


def _expiry_interval(props: list[tuple[int, object]]) -> int | None:
    """The Message Expiry Interval among decoded PUBLISH properties, as AWS IoT Core bounds it."""
    interval = _property_value(props, PROP_MESSAGE_EXPIRY_INTERVAL, None)
    if interval is None:
        return None
    return min(max(int(interval), 1), _MESSAGE_EXPIRY_MAX)  # type: ignore[arg-type]


def _message_expires_at(properties: bytes, received_at: float) -> float | None:
    """When a message with this PUBLISH property block expires, or None if it never does."""
    if not properties:
        return None
    interval = _expiry_interval(_decode_properties(properties, 0)[0])
    return None if interval is None else received_at + interval


def _aged_properties(properties: bytes, received_at: float, now: float | None = None) -> bytes | None:
    """``properties`` with the expiry interval lowered to the whole seconds left, None once expired."""
    if not properties:
        return properties
    props, _ = _decode_properties(properties, 0)
    interval = _expiry_interval(props)
    if interval is None:
        return properties
    left = received_at + interval - (time.time() if now is None else now)
    if left <= 0:
        return None
    # A started second counts as spent: 60 goes out as 59, and 999 ms left as 0.
    seconds = math.ceil(left) - 1
    return _encode_properties(
        [(i, seconds if i == PROP_MESSAGE_EXPIRY_INTERVAL else v) for i, v in props]
    )


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
    def __init__(
        self, send_coro, account_id: str, region: str, principal: str | None = None
    ):
        self._send = send_coro
        self.account_id = account_id
        self.region = region
        # The client certificate id or the IAM caller's user id, else the account:
        # lifecycle events' principalIdentifier and the rule SQL principal().
        self._principal = principal or account_id
        self._session_identifier = ""
        self._presence_version = 0
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
        # The client certificate of an mTLS session; its policies are evaluated
        # under AUTH=true.
        self.certificate_id: str | None = None
        # The template of a provisioning claim certificate, which has a policy of its own.
        self.provisioning_template: str | None = None
        self._policy_context: dict = {}

    def disconnect_reason(self) -> str:
        """Why this session ended, in AWS's disconnect-reason vocabulary.

        Only the reasons the broker can actually tell apart are ever reported: a
        DISCONNECT packet (``CLIENT_INITIATED_DISCONNECT``), a takeover by a
        second connection using the same client id (``DUPLICATE_CLIENTID``),
        a packet the certificate's policies refuse (``CLIENT_ERROR`` on 3.1.1,
        ``AUTH_ERROR`` on MQTT 5), and everything else — a transport that went
        away (``CONNECTION_LOST``).
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

    async def _publish_lifecycle(self, event_type: str, **fields) -> None:
        """Publish this session's ``$aws/events`` presence or subscription event.

        AWS documents that client ids containing ``#`` or ``+`` receive no
        lifecycle events, so none is published for them."""
        if "#" in self._client_id or "+" in self._client_id:
            return
        group = "subscriptions" if event_type.endswith("subscribed") else "presence"
        await _publish_event(
            self.account_id,
            self.region,
            f"$aws/events/{group}/{event_type}/{self._client_id}",
            {
                "clientId": self._client_id,
                "timestamp": _now_epoch_millis(),
                "eventType": event_type,
                "sessionIdentifier": self._session_identifier,
                "principalIdentifier": self._principal,
                **fields,
            },
        )

    async def _publish_connected(self) -> None:
        """Publish this session's ``connected`` event."""
        await self._publish_lifecycle(
            "connected",
            ipAddress=_publish_source_ip.get(),
            versionNumber=self._presence_version,
        )

    async def _answer_fleet_provisioning(
        self, topic: str, operation: str, template_name: str | None, payload: bytes
    ) -> None:
        """Answer a fleet provisioning request on this connection only, if it subscribed to the reply."""
        # What a pre-provisioning hook learns about the requester.
        connection = {"clientId": self._client_id}
        if self._principal != self.account_id:
            connection["claimCertificateId"] = self._principal
        try:
            async with publishing_events():
                kind, doc = await _fleet_provisioning_reply(
                    self.account_id, self.region, operation, template_name, payload, connection
                )
        except Exception:
            _broker_logger.warning("IoT fleet provisioning failed for %r", topic, exc_info=True)
            return
        reply_topic = f"{topic}/{kind}"
        granted = [
            self._sub_granted_qos.get(sid, 0)
            for sid, topic_filter in self._sub_filters.items()
            if _subscription_matches(topic_filter, reply_topic)
        ]
        if granted:
            await self.deliver_to_client(reply_topic, json.dumps(doc).encode("utf-8"), max(granted))

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
        if not self._policy_allows("iot:Receive", f"topic/{topic}"):
            return
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

    def _evaluates_policies(self) -> bool:
        """Whether this session is held to its certificate's IoT policies (AUTH=true only)."""
        from ministack import app

        return app.AUTH and self.certificate_id is not None

    def _policy_allows(self, action: str, resource: str) -> bool:
        """Whether this session may do ``action`` on ``resource``."""
        return not self._evaluates_policies() or _mqtt_policy_allows(
            self.account_id,
            self.region,
            self.certificate_id,
            self._policy_context,
            action,
            resource,
            self.provisioning_template,
        )

    def _publish_allowed(self, topic: str, retain: bool) -> bool:
        """iot:Publish on the topic, and iot:RetainPublish too for a retained message."""
        from ministack import app

        if app.AUTH and self.certificate_id is None and _parse_fleet_provisioning_topic(topic):
            # Fleet provisioning writes the registry: under AUTH=true it needs an authenticated device.
            return False
        resource = f"topic/{topic}"
        return self._policy_allows("iot:Publish", resource) and (
            not retain or self._policy_allows("iot:RetainPublish", resource)
        )

    async def _refuse_connect(self, denied: str) -> bool:
        """Refuse a CONNECT: CONNACK 0x87 on MQTT 5, a plain close on 3.1.1, and a connect_failed event."""
        _broker_logger.warning(
            "IoT broker: CONNECT from %r refused, the attached policies do not allow %s", self._client_id, denied
        )
        self._session_identifier = str(uuid.uuid4())
        await self._publish_lifecycle(
            "connect_failed", connectFailureReason="AUTHORIZATION_FAILED", ipAddress=_publish_source_ip.get()
        )
        self._client_id = ""
        self._will_topic = None
        if self.protocol_version == MQTT_5:
            await self.send_bytes(_make_connack(return_code=RC5_NOT_AUTHORIZED, protocol_version=MQTT_5))
        return False

    async def _close_unauthorized(self) -> bool:
        """End the session over a refused packet: DISCONNECT 0x87 on MQTT 5, a plain close on 3.1.1."""
        # Nothing more reaches the client, its own will included; the filters stay for a stored session.
        for sid in self._sub_ids:
            await broker_unsubscribe(sid)
        self._sub_ids.clear()
        if self.protocol_version == MQTT_5:
            self._forced_disconnect_reason = "AUTH_ERROR"
            await self.send_bytes(_make_disconnect(RC5_NOT_AUTHORIZED))
        else:
            self._forced_disconnect_reason = "CLIENT_ERROR"
        return False

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

            if self._evaluates_policies():
                self._policy_context = _mqtt_policy_context(
                    self.account_id, self.region, self.certificate_id, client_id
                )
            if not self._policy_allows("iot:Connect", f"client/{client_id}"):
                return await self._refuse_connect(f"iot:Connect on client/{client_id}")
            if self._will_topic is not None and not self._publish_allowed(self._will_topic, self._will_retain):
                return await self._refuse_connect(f"the will on topic/{self._will_topic}")

            self._graceful_disconnect = False
            self._forced_disconnect_reason = None
            self._session_identifier = str(uuid.uuid4())
            # Taken before the takeover, so the evicted session's disconnect follows it.
            self._presence_version = _next_presence_version(
                self.account_id, self.region, self._client_id
            )
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
                    self._session_identifier = existing_ps.session_identifier
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
                    for q_topic, q_payload, q_qos, q_properties, q_received_at in queued:
                        properties = _aged_properties(q_properties, q_received_at)
                        if properties is None:
                            continue
                        token = _delivery_properties.set(properties)
                        try:
                            await self.deliver_to_client(q_topic, q_payload, q_qos)
                        finally:
                            _delivery_properties.reset(token)
                    await self._publish_connected()
                    return True
                else:
                    _persistent_sessions[session_key] = _PersistentSessionState(
                        subscriptions=[],
                        created_at=time.time(),
                        expiry_interval=self._session_expiry_interval,
                        session_identifier=self._session_identifier,
                    )

            await self._send_connack(
                session_present=session_present, assigned_client_id=assigned_client_id
            )
            await self._publish_connected()
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
            if not _reserved_topic_publish_allowed(topic):
                # AWS closes the connection without a PUBACK (measured over
                # MQTT 3.1.1; the MQTT 5 reason code is not measured).
                _broker_logger.warning("IoT broker: PUBLISH to a reserved topic closes the connection: %r", topic)
                if self.protocol_version == MQTT_5:
                    await self.send_bytes(_make_disconnect(RC5_NOT_AUTHORIZED))
                return False
            if self.provisioning_template is not None and _mtls_claim_scope(self.certificate_id) is None:
                # AWS acknowledges a PUBLISH on an expired claim's connection, then closes it.
                _broker_logger.warning("IoT broker: the provisioning claim of %r has expired", self._client_id)
                if qos == 1 and packet_id is not None:
                    await self.send_bytes(_make_puback(packet_id))
                return await self._close_unauthorized()
            if not self._publish_allowed(topic, retain):
                # MQTT 5 answers a refused QoS 1 PUBLISH with PUBACK 0x87 and
                # keeps the connection.
                _broker_logger.warning(
                    "IoT broker: PUBLISH from %r to %r refused by the attached policies", self._client_id, topic
                )
                if self.protocol_version == MQTT_5 and qos == 1 and packet_id is not None:
                    await self.send_bytes(_make_puback(packet_id, reason_code=RC5_NOT_AUTHORIZED))
                    return True
                return await self._close_unauthorized()
            payload = body[off:]
            await broker_publish(
                self.account_id,
                self.region,
                topic,
                payload,
                qos=qos,
                retain=retain,
                properties=properties,
                client_id=self._client_id or None,
                principal=self._principal,
            )
            if qos == 1 and packet_id is not None:
                # Success also when nobody subscribes, as AWS IoT answers.
                if self.protocol_version == MQTT_5:
                    await self.send_bytes(_make_puback(packet_id, reason_code=RC5_SUCCESS))
                else:
                    await self.send_bytes(_make_puback(packet_id))
            fleet = _parse_fleet_provisioning_topic(topic)
            if fleet is not None:
                await self._answer_fleet_provisioning(topic, *fleet, payload)
            return True

        if pkt_type == PKT_SUBSCRIBE:
            packet_id = struct.unpack_from("!H", body, 0)[0]
            off = 2
            if self.protocol_version == MQTT_5:
                # Subscription Identifier lives here; the broker advertises it
                # as unavailable in CONNACK and ignores it if sent anyway.
                _subscribe_props, off = _decode_properties(body, off)
            granted = []
            topics = []
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
                if not self._policy_allows("iot:Subscribe", f"topicfilter/{topic}"):
                    # As on AWS: 3.1.1 closes, MQTT 5 refuses this filter alone.
                    _broker_logger.warning(
                        "IoT broker: SUBSCRIBE from %r to %r refused by the attached policies", self._client_id, topic
                    )
                    if self.protocol_version != MQTT_5:
                        return await self._close_unauthorized()
                    granted.append(RC5_NOT_AUTHORIZED)
                    continue
                topics.append(topic)
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
            # The event lists the granted filters; one with none granted raises no event.
            if topics or not granted:
                await self._publish_lifecycle("subscribed", topics=topics)
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
            await self._publish_lifecycle("unsubscribed", topics=filters)
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
        if self._client_id and _deregister_client(
            self.account_id, self.region, self._client_id, self
        ):
            _next_presence_version(self.account_id, self.region, self._client_id)
            reason = self.disconnect_reason()
            await self._publish_lifecycle(
                "disconnected",
                clientInitiatedDisconnect=reason == "CLIENT_INITIATED_DISCONNECT",
                disconnectReason=reason,
                versionNumber=self._presence_version,
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
                session_identifier=self._session_identifier,
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


def _ws_handshake_refused(scope: dict) -> bool:
    """Under AUTH=true, whether the upgrade's SigV4 signature, time or session token fails."""
    from ministack.app import AUTH
    from ministack.core.iam_evaluator import AuthError, authenticate_handshake

    return bool(AUTH) and isinstance(authenticate_handshake(scope), AuthError)


async def _refuse_ws_handshake(scope: dict, send) -> None:
    """Answer a refused upgrade with 403 and a trace id, the device gateway's answer to any credential failure."""
    if "websocket.http.response" not in (scope.get("extensions") or {}):
        await send({"type": "websocket.close", "code": 1008})
        return
    body = json.dumps({"message": "Forbidden", "traceId": str(uuid.uuid4())}, separators=(",", ":")).encode()
    await send({"type": "websocket.http.response.start", "status": 403,
                "headers": [(b"content-type", b"application/json")]})
    await send({"type": "websocket.http.response.body", "body": body})


async def handle_websocket(
    scope: dict, receive, send, account_id: str, region: str
) -> None:
    """Drive an MQTT-over-WebSocket session."""
    client = scope.get("client")
    if client:
        _publish_source_ip.set(client[0])
    msg = await receive()
    if msg.get("type") != "websocket.connect":
        return

    sub_headers = {}
    for name, value in scope.get("headers", []):
        try:
            sub_headers[name.decode("latin-1").lower()] = value.decode("utf-8")
        except UnicodeDecodeError:
            sub_headers[name.decode("latin-1").lower()] = value.decode("latin-1")
    if _ws_handshake_refused(scope):
        await _refuse_ws_handshake(scope, send)
        return
    requested = sub_headers.get("sec-websocket-protocol", "")
    chosen = None
    for proto in [p.strip() for p in requested.split(",") if p.strip()]:
        if proto.lower() in ("mqtt", "mqttv3.1", "mqttv5"):
            chosen = proto
            break

    jobs_device_host.set(sub_headers.get("host", ""))

    accept: dict = {"type": "websocket.accept"}
    if chosen:
        accept["subprotocol"] = chosen
    await send(accept)

    ctx_token = _request_account_id.set(account_id)
    query = parse_qs(scope.get("query_string", b"").decode("latin-1"))
    principal = _caller_principal(extract_access_key_id(sub_headers, query))
    session = _WSSession(send, account_id, region, principal)
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
# * a claim certificate from CreateProvisioningClaim, until it expires: the
#   account and region of its template serve the session, and under AUTH=true
#   it may use only that template's fleet provisioning topics.
# * a certificate that is unknown, not ACTIVE, or ACTIVE in several accounts:
#   refused with a "not authorized" MQTT CONNACK, or under AUTH=true closed
#   without one, as on AWS. This reads the X.509
#   identity lifecycle that the registry already keeps and that the control plane already
#   enforces (deleting an ACTIVE certificate is CertificateStateException
#   406), so it is not policy evaluation and not a credential check; a
#   deactivated certificate stops working, which is what makes deactivation
#   mean anything and what real AWS IoT does. The ambiguous case cannot be
#   attributed at all: two registrations see byte-identical bytes, so any
#   tie-break would award the session to whoever registered the copy.
# * with ``AUTH=true``, a session under a certificate is also held to the IoT
#   policies attached to it, as on AWS: ``iot:Connect`` on the client id at
#   CONNECT, ``iot:Publish`` (and ``iot:RetainPublish``) for a PUBLISH and the
#   will, ``iot:Subscribe`` per topic filter and ``iot:Receive`` per delivered
#   message. Without it no policy is evaluated.
# * just-in-time registration, as on AWS: a certificate that is unknown, or
#   still PENDING_ACTIVATION, and signed by a registered CA that is ACTIVE
#   with auto-registration enabled is created PENDING_ACTIVATION in that CA's
#   account and region, the registered event is published, and the connection
#   is closed without a CONNACK. As on AWS this happens on the CONNECT packet;
#   a handshake that sends no CONNECT registers nothing.
#
# The TLS layer adds one constraint of its own: a presented chain is verified,
# so an untrusted certificate fails the handshake first. Trusted: the Local CA and every
# registered CA and device certificate, whatever its status.

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
# Built on the first connection, not at boot, and dropped when the listener stops.
_mtls_ssl_context: ssl.SSLContext | None = None
_mtls_context_lock = asyncio.Lock()
# TLS upgrades in flight, held so the loop keeps a reference to each task.
_mtls_upgrades: set[asyncio.Task] = set()
# Digests of the trust anchors already loaded into the live TLS context;
# emptied whenever a context is built, since a fresh one starts with none.
_mtls_loaded_anchors: set[str] = set()
# The server name (SNI) each handshake's ClientHello carried, by its TLS
# object; just-in-time registration depends on it (see _mtls_server_name_reaches).
_mtls_server_names: weakref.WeakKeyDictionary[ssl.SSLObject, str] = weakref.WeakKeyDictionary()

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

    region = os.environ.get("MINISTACK_REGION", "us-east-1")
    dns_names = [f"*.iot.{region}.{_MINISTACK_HOST}", "localhost"]
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
        common_name=dns_names[0],
        san_dns=dns_names,
        san_ips=ip_addresses,
    )
    set_mtls_server_cert(cert_pem, key_pem)
    _mtls_logger.info(
        "IoT mTLS: issued broker certificate for %s", ", ".join(dns_names + ip_addresses)
    )
    return cert_pem, key_pem


def _mtls_trust_anchors() -> list[tuple[str, str]]:
    """(id, PEM) of the Local CA and every registered CA and device certificate, across every scope and status."""
    local = get_ca_cert_pem()
    anchors = [(hashlib.sha256(local.encode("utf-8")).hexdigest(), local)]
    for store in (_ca_certificates, _certificates):
        for record in list(store._data.values()):
            pem = record.get("certificatePem") if isinstance(record, dict) else None
            if isinstance(pem, str) and pem.strip():
                anchors.append((record.get("certificateId") or hashlib.sha256(pem.encode("utf-8")).hexdigest(), pem))
    return anchors


def _mtls_refresh_trust_anchors(ctx: ssl.SSLContext) -> None:
    """Load every anchor the context does not already have.

    Anchors are loaded one PEM at a time so that a single unparseable
    registered certificate costs only itself, and each is remembered — by
    certificate id, including the ones that failed — so a handshake never
    repeats the work or the warning. ``load_verify_locations`` is additive, so an anchor
    stays loaded after its record is gone; whether a presented certificate is
    served is decided per connection by the registry lookup.
    """
    for anchor_id, pem in _mtls_trust_anchors():
        if anchor_id in _mtls_loaded_anchors:
            continue
        _mtls_loaded_anchors.add(anchor_id)
        try:
            ctx.load_verify_locations(cadata=pem)
        except Exception as e:
            _mtls_logger.warning("IoT mTLS: ignoring an unusable trust anchor: %s", e)


def _mtls_on_client_hello(
    ssl_object: ssl.SSLObject, server_name: str | None, ctx: ssl.SSLContext
) -> None:
    """Pull newly registered certificates in mid-handshake, before the peer is verified.

    A certificate registered while the listener is bound has to be trusted without a
    restart, and ``sni_callback`` is the only hook that runs late enough to see
    the current registry yet early enough to matter. It runs on every
    handshake, including those carrying no server name — which is what a
    device dialling the broker by IP sends. It is also the only place the
    server side sees that name, so it records whether one was sent. An
    exception here aborts the handshake with an internal-error alert, so
    nothing is allowed to escape.
    """
    try:
        if server_name:
            _mtls_server_names[ssl_object] = server_name
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
    # A registered device certificate is an anchor of its own, whatever signed it.
    ctx.verify_flags |= ssl.VERIFY_X509_PARTIAL_CHAIN
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


def mtls_is_listening() -> bool:
    """Whether the mTLS listener holds a bound socket."""
    return _mtls_server is not None


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
        # Bound without TLS: the context imports `cryptography` and mints the
        # server certificate (and the CA), which waits for the first device.
        _mtls_server = await asyncio.get_running_loop().create_server(
            _MtlsAcceptor, host="0.0.0.0", port=port
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
    global _mtls_server, _mtls_ssl_context
    _mtls_ssl_context = None
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


def _mtls_active_registrations(
    cert_id: str, status: str | None = "ACTIVE"
) -> list[tuple[str, str, dict]]:
    """Every (account_id, region, record) holding this certificate in ``status``
    (any status when None)."""
    return [
        (account_id, region, record)
        for (account_id, region, key), record in list(_certificates._data.items())
        if key == cert_id and isinstance(record, dict)
        and (status is None or record.get("status") == status)
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


def _mtls_resolve_identity(der: bytes | None) -> tuple[str, str, str | None] | None:
    """Map a peer certificate (or none) to (account_id, region, claim template), or None.

    None means "this connection is refused". Presenting no certificate is not
    refused: that caller claims nothing and is served under the default
    account, the same way ``_ws_resolve_iot_account_id`` treats an unsigned
    WebSocket upgrade. A certificate that IS presented is read against the
    registry, which is the identity lifecycle this service already keeps:
    ACTIVE in exactly one account selects it, as does an unexpired
    provisioning claim, and unknown, not ACTIVE, or ACTIVE in several
    accounts is refused by the caller (``_mtls_refuse``). The claim template
    is set for a provisioning claim only.
    """
    if not der:
        return (
            os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000"),
            os.environ.get("MINISTACK_REGION", "us-east-1"),
            None,
        )
    # Same derivation as x509_utils.get_certificate_id, without a PEM round-trip.
    cert_id = hashlib.sha256(der).hexdigest()
    active = _mtls_active_registrations(cert_id)
    if len(active) == 1:
        account_id, region, _record = active[0]
        return account_id, region, None
    return _mtls_claim_scope(cert_id)


def _mtls_claim_scope(cert_id: str) -> tuple[str, str, str] | None:
    """(account_id, region, templateName) of the unexpired provisioning claim with this certificate id, or None."""
    now = time.time()
    for (account_id, region, key), record in list(_provisioning_claims._data.items()):
        if key == cert_id and record["expiration"] > now:
            return account_id, region, record["templateName"]
    return None


def _mtls_server_name_reaches(server_name: str, account_id: str) -> bool:
    """Whether a device that sent ``server_name`` as its SNI may reach ``account_id``.

    An endpoint-shaped name (``<prefix>-ats.iot.`` or ``<prefix>.credentials.iot.``)
    counts only with this account's prefix; any other name counts.
    """
    name = server_name.lower().rstrip(".")
    first, _, rest = name.partition(".")
    if first.endswith("-ats") and rest.startswith("iot."):
        return first[: -len("-ats")] == _endpoint_prefix(account_id)
    if rest.startswith("credentials.iot."):
        return first == _endpoint_prefix(account_id)
    return True


def _mtls_auto_registering_ca(
    account_id: str, region: str, ca_id: str | None, server_name: str | None = None
) -> dict | None:
    """The CA record when it would auto-register a device certificate now.

    Without a server name (SNI) only a CA in DEFAULT mode does. With one, a
    CA in either mode does when the name reaches the CA's account.
    """
    ca = _ca_certificates.get_scoped(account_id, region, ca_id) if ca_id else None
    if (
        isinstance(ca, dict)
        and ca.get("status") == "ACTIVE"
        and ca.get("autoRegistrationStatus") == "ENABLE"
        and (
            ca.get("certificateMode", "DEFAULT") == "DEFAULT"
            if server_name is None
            else _mtls_server_name_reaches(server_name, account_id)
        )
    ):
        return ca
    return None


def _mtls_auto_registering_signers(
    cert_pem: str, server_name: str | None = None
) -> list[tuple[str, str, str]]:
    """Every (account_id, region, ca_id) of an auto-registering CA that signed ``cert_pem``."""
    return [
        (account_id, region, ca_id)
        for (account_id, region, ca_id), ca in list(_ca_certificates._data.items())
        if _mtls_auto_registering_ca(account_id, region, ca_id, server_name) is not None
        and certificate_is_signed_by(cert_pem, ca.get("certificatePem") or "")
    ]


async def _mtls_auto_register(der: bytes, peername, server_name: str | None = None) -> bool:
    """Just-in-time registration for a presented certificate that no scope
    holds ACTIVE. True when it applied, and the caller closes without a
    CONNACK; False leaves the refusal to ``_mtls_refuse``.

    An unknown certificate is created PENDING_ACTIVATION under the one
    auto-registering CA that signed it. A certificate already
    PENDING_ACTIVATION under such a CA only publishes the event again. AWS
    does both on every such connect, and nothing for an INACTIVE certificate
    or a CA with auto-registration disabled. Which CAs qualify depends on
    the server name the device sent, if any (``_mtls_auto_registering_ca``).
    """
    cert_id = hashlib.sha256(der).hexdigest()
    held = _mtls_active_registrations(cert_id, status=None)
    if len(held) > 1:
        return False
    if held:
        account_id, region, record = held[0]
        if record.get("status") != "PENDING_ACTIVATION":
            return False
        if _mtls_auto_registering_ca(account_id, region, record.get("caCertificateId"), server_name) is None:
            return False
        registration_timestamp = str(int(record["creationDate"] * 1000))
    else:
        cert_pem = ssl.DER_cert_to_PEM_cert(der)
        # One signature check per auto-registering CA, off the broker's loop:
        # a device retrying with a refused certificate must not stall it.
        signers = await asyncio.to_thread(_mtls_auto_registering_signers, cert_pem, server_name)
        # The same CA registered in several scopes cannot be attributed, for
        # the same reason an ambiguous ACTIVE certificate is refused.
        if len(signers) != 1:
            return False
        account_id, region, ca_id = signers[0]
        with request_scope(account_id, region):
            record = _certificate_record(cert_id, cert_pem, "PENDING_ACTIVATION", ca_id)
            _certificates[cert_id] = record
        registration_timestamp = None
    _mtls_logger.info(
        "IoT mTLS: certificate %s is PENDING_ACTIVATION under CA %s in %s/%s; "
        "published the registered event and closing",
        cert_id,
        record["caCertificateId"],
        account_id,
        region,
    )
    source_ip = peername[0] if isinstance(peername, tuple) and peername else None
    await _publish_certificate_registered(
        account_id, region, record, source_ip, registration_timestamp
    )
    return True


async def _mtls_first_packet(reader: asyncio.StreamReader) -> bytes | None:
    """The client's first complete MQTT packet, or None when the peer closes,
    sends nothing within ``_MTLS_REFUSE_READ_TIMEOUT`` or exceeds
    ``_MTLS_REFUSE_MAX_BYTES`` without completing one."""
    buffer = bytearray()
    try:
        while len(buffer) < _MTLS_REFUSE_MAX_BYTES:
            data = await asyncio.wait_for(
                reader.read(_MTLS_READ_CHUNK), timeout=_MTLS_REFUSE_READ_TIMEOUT
            )
            if not data:
                return None
            buffer.extend(data)
            if len(buffer) < 2:
                continue
            try:
                remaining, header_end = _decode_remaining_length(bytes(buffer), 1)
            except ValueError:
                continue
            if len(buffer) < header_end + remaining:
                continue
            return bytes(buffer[: header_end + remaining])
    except (asyncio.TimeoutError, OSError, ssl.SSLError):
        return None
    return None


def _connect_protocol_level(packet: bytes) -> int | None:
    """The protocol level of a CONNECT packet; None for any other packet."""
    if (packet[0] >> 4) & 0x0F != PKT_CONNECT:
        return None
    remaining, header_end = _decode_remaining_length(packet, 1)
    body = packet[header_end : header_end + remaining]
    try:
        _proto_name, level_at = _read_string(body, 0)
        return body[level_at]
    except (ValueError, IndexError):
        return MQTT_311


async def _mtls_refuse(
    writer: asyncio.StreamWriter, cert_id: str, protocol_level: int | None
) -> None:
    """Answer an unusable client certificate with a "not authorized" CONNACK
    (0x05, or reason code 0x87 for an MQTT 5 client) when the client sent a
    CONNECT, then close; with AUTH=true, close without one.

    AWS closes the socket without a CONNACK unless just-in-time registration
    applies. Diverging here without AUTH is deliberate: an emulator's job is
    diagnosability, "not authorized" is reported cleanly by every device SDK,
    and a TLS-layer rejection could not tell the user "unknown" from
    "deactivated".
    """
    from ministack import app

    _mtls_logger.warning(
        "IoT mTLS: refusing client certificate %s (%s)", cert_id, _mtls_refusal_reason(cert_id)
    )
    if protocol_level is None or app.AUTH:
        return
    # The CONNACK has to match the client's protocol level: a v5 client
    # handed the two-byte 3.1.1 form dies in its decoder instead of reporting
    # the refusal, and 0x05 is not a defined v5 reason code.
    if protocol_level == MQTT_5:
        connack = _make_connack(return_code=RC5_NOT_AUTHORIZED, protocol_version=MQTT_5)
    else:
        connack = _make_connack(return_code=CONNACK_311_NOT_AUTHORIZED)
    try:
        writer.write(connack)
        await writer.drain()
    except (OSError, ssl.SSLError):
        return


async def _mtls_close(writer: asyncio.StreamWriter) -> None:
    try:
        writer.close()
        await writer.wait_closed()
    except (OSError, ssl.SSLError):
        pass


async def _mtls_context() -> ssl.SSLContext | None:
    """The listener's TLS context, built once on first use; None refuses the connection and a later one retries."""
    global _mtls_ssl_context
    async with _mtls_context_lock:
        if _mtls_ssl_context is None:
            try:
                # RSA keygen on first use, so keep it off the event loop.
                _mtls_ssl_context = await asyncio.to_thread(_mtls_build_ssl_context)
            except Exception as e:
                _mtls_logger.warning(
                    "IoT mTLS: could not build the TLS context (%s); connection refused", e
                )
                return None
        return _mtls_ssl_context


class _MtlsAcceptor(asyncio.Protocol):
    """Accepts plaintext, upgrades it to TLS, then serves the stream session.

    Reading is paused before anything arrives, so the ClientHello stays in the
    kernel buffer until ``loop.start_tls`` takes over the transport.
    """

    def connection_made(self, transport):
        transport.pause_reading()
        task = asyncio.get_running_loop().create_task(self._upgrade(transport))
        _mtls_upgrades.add(task)
        task.add_done_callback(_mtls_upgrades.discard)

    async def _upgrade(self, transport):
        loop = asyncio.get_running_loop()
        ctx = await _mtls_context()
        if ctx is None or transport.is_closing():
            transport.abort()
            return
        reader = asyncio.StreamReader(loop=loop)
        protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
        try:
            tls_transport = await loop.start_tls(transport, protocol, ctx, server_side=True)
        except Exception as e:
            _mtls_logger.debug("IoT mTLS: TLS handshake failed: %s", e)
            transport.abort()
            return
        # start_tls does not call connection_made on the new protocol.
        protocol.connection_made(tls_transport)
        writer = asyncio.StreamWriter(tls_transport, protocol, reader, loop)
        await _mtls_handle_conn(reader, writer)


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
            # AWS registers on the CONNECT packet, not on the handshake: a
            # client that closes after the handshake, sends nothing, or sends
            # another packet first registers nothing (measured).
            packet = await _mtls_first_packet(reader)
            protocol_level = _connect_protocol_level(packet) if packet else None
            if protocol_level is not None and await _mtls_auto_register(
                der, writer.get_extra_info("peername"), _mtls_server_names.get(ssl_object)
            ):
                return
            await _mtls_refuse(writer, hashlib.sha256(der).hexdigest(), protocol_level)
        finally:
            await _mtls_close(writer)
        return
    account_id, region, provisioning_template = owner

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
    # The name the device dialled (its SNI), or the address, on the gateway port.
    local = str((writer.get_extra_info("sockname") or ("",))[0])
    dialled = _mtls_server_names.get(ssl_object) if ssl_object is not None else None
    jobs_device_host.set(f"{dialled or (f'[{local}]' if ':' in local else local)}:{_GATEWAY_PORT}")
    certificate_id = hashlib.sha256(der).hexdigest() if der else None
    session = _WSSession(_tcp_send, account_id, region, certificate_id)
    session.certificate_id = certificate_id
    session.provisioning_template = provisioning_template
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
