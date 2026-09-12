# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""AWS Lambda Core network connectors (REST-JSON, ``lambda-core`` 2026-04-30).

Control-plane emulation of the network-connector family: ``CreateNetworkConnector``,
``GetNetworkConnector``, ``UpdateNetworkConnector``, ``DeleteNetworkConnector``
and ``ListNetworkConnectors``. Shapes, status codes, enums and member casing are
taken from botocore ``lambda-core/2026-04-30/service-2.json``.

Two things about this service are easy to get wrong and are worth stating here.

*It is not its own endpoint.* ``endpointPrefix`` and ``signingName`` are both
``lambda`` — unlike ``lambda-microvms``, which signs under its own name. A
client therefore signs a network-connector call exactly like a Lambda call, so
the router cannot tell them apart by credential scope and must look at the path
(``/2026-04-04/network-connectors``). Note the request URIs carry ``2026-04-04``
while the model's own ``apiVersion`` is ``2026-04-30``; the URI is what ships.

*The error envelope is not uniform.* Every exception carries a ``Type`` member,
but the message member's casing differs per shape: ``ResourceNotFoundException``
and ``ServiceException`` model ``Message``, while ``InvalidParameterValueException``,
``ResourceConflictException``, ``NetworkConnectorLimitExceededException`` and
``TooManyRequestsException`` model ``message``. ``_error`` below keys off the
code so each one goes out the way its shape declares.

There is no real VPC attachment behind a connector. ``CreateNetworkConnector``
returns ``PENDING``, matching the modelled 202, and the connector reaches
``ACTIVE`` on the next read, which is what a client polling ``GetNetworkConnector``
needs in order to proceed. Nothing validates that the subnets or security groups
exist in EC2.
"""

import copy
import json
import logging
import re
from datetime import datetime, timezone
from urllib.parse import unquote

from ministack.core.persistence import load_state
from ministack.core.responses import (
    REST_JSON_CONTENT_TYPE,
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("lambda_core")

# The URI prefix the operations ship under. Deliberately not the model's
# apiVersion (2026-04-30) — every requestUri in the model says 2026-04-04.
API_PREFIX = "2026-04-04"

# NetworkConnectorState, verbatim from the model enum.
_STATE_PENDING = "PENDING"
_STATE_ACTIVE = "ACTIVE"
_STATES = ("PENDING", "ACTIVE", "INACTIVE", "FAILED", "DELETING", "DELETE_FAILED")

# NetworkConnectorType — the model declares exactly one value today.
_TYPE_VPC_EGRESS = "VPC_EGRESS"

# NetworkProtocol enum.
_NETWORK_PROTOCOLS = ("IPv4", "DualStack")
# ComputeResourceType enum, and AssociatedComputeResourceTypesList min/max 1.
_COMPUTE_RESOURCE_TYPES = ("MicroVm",)

# NetworkConnectorName: an ARN, or a bare name of 1..64 name characters.
_NAME_RE = re.compile(r"[a-zA-Z0-9_-]{1,64}")
# NetworkConnectorSubnetIds max 16; NetworkConnectorSecurityGroupIds max 5.
_MAX_SUBNETS = 16
_MAX_SECURITY_GROUPS = 5
_SUBNET_RE = re.compile(r"subnet-[0-9a-z]*")
_SECURITY_GROUP_RE = re.compile(r"sg-[0-9a-zA-Z]*")
# MaxHundredListItems.
_MAX_ITEMS_MIN, _MAX_ITEMS_MAX = 1, 100

# Exceptions whose message member the model declares capitalised. Every other
# modelled exception uses lowercase `message`.
_CAPITALISED_MESSAGE_CODES = frozenset(
    {"ResourceNotFoundException", "ServiceException"}
)

# ---------------------------------------------------------------------------
# State (account + region scoped)
# ---------------------------------------------------------------------------

_connectors = AccountRegionScopedDict()  # Name -> record
_client_tokens = AccountRegionScopedDict()  # ClientToken -> first response


def get_state():
    return copy.deepcopy({"connectors": _connectors, "client_tokens": _client_tokens})


def restore_state(data):
    if not data:
        return
    _connectors.clear()
    _client_tokens.clear()
    _connectors.update(data.get("connectors", {}))
    _client_tokens.update(data.get("client_tokens", {}))


try:
    _persisted = load_state("lambda_core")
    if _persisted:
        restore_state(_persisted)
except Exception:  # pragma: no cover - best-effort restore
    logger.exception("Failed to restore persisted lambda_core state; continuing fresh")


def reset():
    _connectors.clear()
    _client_tokens.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    """CoreTimestamp is `timestampFormat: iso8601` in the model, so LastModified
    is an ISO-8601 string rather than the epoch number a json-protocol
    timestamp would carry."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _connector_arn(name: str) -> str:
    return (
        f"arn:aws:lambda:{get_region()}:{get_account_id()}:network-connector:{name}"
    )


def _error(code: str, message: str, status: int, extra: dict | None = None):
    """A lambda-core error body.

    The protocol is rest-json, so the body is `application/json`. Every
    modelled exception carries a `Type` member alongside the message, and the
    message member's casing is per-shape (see the module docstring).
    """
    body = dict(extra or {})
    body["Type"] = "User" if status < 500 else "Service"
    key = "Message" if code in _CAPITALISED_MESSAGE_CODES else "message"
    return error_response_json(
        code, message, status, body,
        message_key=key,
        content_type=REST_JSON_CONTENT_TYPE,
    )


def _invalid(message: str):
    return _error("InvalidParameterValueException", message, 400)


def _not_found(identifier: str):
    return _error(
        "ResourceNotFoundException",
        f"Network connector not found: {identifier}",
        404,
    )


def _parse_body(body):
    if not body:
        return {}, None
    try:
        parsed = json.loads(body)
    except ValueError:
        return None, _invalid("Could not parse request body as JSON")
    if not isinstance(parsed, dict):
        return None, _invalid("Request body must be a JSON object")
    return parsed, None


def _name_from_identifier(identifier: str) -> str:
    """`Identifier` accepts the name or the ARN; connectors are keyed by name."""
    if identifier.startswith("arn:"):
        return identifier.rsplit(":", 1)[-1]
    return identifier


def _validate_configuration(config):
    """`Configuration.VpcEgressConfiguration` and its members, to the model's
    own constraints. Wordings are MiniStack's own — the live service was not
    measured and no message string is documented."""
    if config is None:
        return None
    if not isinstance(config, dict):
        return _invalid("Configuration must be a structure")
    vpc = config.get("VpcEgressConfiguration")
    if vpc is None:
        return None
    if not isinstance(vpc, dict):
        return _invalid("Configuration.VpcEgressConfiguration must be a structure")

    subnets = vpc.get("SubnetIds")
    if subnets is not None:
        if not isinstance(subnets, list) or not 1 <= len(subnets) <= _MAX_SUBNETS:
            return _invalid(
                "Configuration.VpcEgressConfiguration.SubnetIds must have 1 to "
                f"{_MAX_SUBNETS} entries"
            )
        for subnet in subnets:
            if not isinstance(subnet, str) or not _SUBNET_RE.fullmatch(subnet):
                return _invalid(f"Invalid subnet id: {subnet!r}")

    groups = vpc.get("SecurityGroupIds")
    if groups is not None:
        if not isinstance(groups, list) or len(groups) > _MAX_SECURITY_GROUPS:
            return _invalid(
                "Configuration.VpcEgressConfiguration.SecurityGroupIds must have "
                f"at most {_MAX_SECURITY_GROUPS} entries"
            )
        for group in groups:
            if not isinstance(group, str) or not _SECURITY_GROUP_RE.fullmatch(group):
                return _invalid(f"Invalid security group id: {group!r}")

    protocol = vpc.get("NetworkProtocol")
    if protocol is not None and protocol not in _NETWORK_PROTOCOLS:
        return _invalid(
            f"NetworkProtocol must be one of {', '.join(_NETWORK_PROTOCOLS)}"
        )

    compute = vpc.get("AssociatedComputeResourceTypes")
    if compute is not None:
        # AssociatedComputeResourceTypesList is min 1, max 1.
        if not isinstance(compute, list) or len(compute) != 1:
            return _invalid(
                "AssociatedComputeResourceTypes must have exactly 1 entry"
            )
        if compute[0] not in _COMPUTE_RESOURCE_TYPES:
            return _invalid(
                "AssociatedComputeResourceTypes entries must be one of "
                f"{', '.join(_COMPUTE_RESOURCE_TYPES)}"
            )
    return None


def _summary(record: dict) -> dict:
    """NetworkConnectorSummary: the members ListNetworkConnectors returns."""
    return {
        "Arn": record["Arn"],
        "Name": record["Name"],
        "Id": record["Id"],
        "Type": record["Type"],
        "State": record["State"],
        "LastModified": record["LastModified"],
    }


def _settle(record: dict) -> dict:
    """A connector created a moment ago reads back ACTIVE.

    There is no VPC attachment to wait on here, and a client that polls
    GetNetworkConnector after a 202 needs to see the connector leave PENDING or
    it never proceeds.
    """
    if record.get("State") == _STATE_PENDING:
        record["State"] = _STATE_ACTIVE
    return record


def _view(record: dict, members: tuple) -> dict:
    return {k: record[k] for k in members if record.get(k) is not None}


# CreateNetworkConnectorResponse members, in model order.
_CREATE_MEMBERS = ("Arn", "Name", "Id", "Configuration", "OperatorRole", "State")
# GetNetworkConnectorResponse members, in model order.
_GET_MEMBERS = (
    "Arn", "Name", "Id", "Version", "Configuration", "OperatorRole", "State",
    "StateReason", "StateReasonCode", "LastUpdateStatus", "LastUpdateStatusReason",
    "LastUpdateStatusReasonCode", "LastModified",
)
# UpdateNetworkConnectorResponse members, in model order.
_UPDATE_MEMBERS = (
    "Arn", "Name", "Id", "OperatorRole", "Configuration", "State",
    "LastUpdateStatus", "LastUpdateStatusReason", "LastModified",
)
# DeleteNetworkConnectorResponse members, in model order.
_DELETE_MEMBERS = ("Arn", "Name", "Id", "Configuration", "OperatorRole", "State")


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def _create_network_connector(body):
    data, err = _parse_body(body)
    if err:
        return err

    name = data.get("Name")
    if not isinstance(name, str) or not name:
        return _invalid("Name is required")
    if not _NAME_RE.fullmatch(name):
        return _invalid(
            "Name must be 1 to 64 characters of [a-zA-Z0-9_-], or a network "
            "connector ARN"
        )

    config = data.get("Configuration")
    if config is None:
        return _invalid("Configuration is required")
    cfg_err = _validate_configuration(config)
    if cfg_err:
        return cfg_err

    token = data.get("ClientToken")
    if token is not None and (not isinstance(token, str) or not 1 <= len(token) <= 64):
        return _invalid("ClientToken must be 1 to 64 characters")
    if token:
        replay = _client_tokens.get(token)
        if replay is not None:
            # "All calls after the first that use this token return the same
            # response as the first call" — the idempotency contract the
            # ClientToken member exists for.
            return json_response(dict(replay), content_type=REST_JSON_CONTENT_TYPE)

    if name in _connectors:
        return _error(
            "ResourceConflictException",
            f"Network connector already exists: {name}",
            409,
        )

    record = {
        "Arn": _connector_arn(name),
        "Name": name,
        "Id": new_uuid(),
        "Type": _TYPE_VPC_EGRESS,
        "Version": 1,
        "Configuration": copy.deepcopy(config),
        "OperatorRole": data.get("OperatorRole"),
        "State": _STATE_PENDING,
        "LastUpdateStatus": "Successful",
        "LastModified": _now_iso(),
        "Tags": dict(data.get("Tags") or {}),
    }
    _connectors[name] = record
    logger.info("CreateNetworkConnector name=%s id=%s", name, record["Id"])

    response = _view(record, _CREATE_MEMBERS)
    if token:
        _client_tokens[token] = dict(response)
    # The model gives CreateNetworkConnector a 202.
    return json_response(response, status=202, content_type=REST_JSON_CONTENT_TYPE)


def _get_network_connector(identifier):
    record = _connectors.get(_name_from_identifier(identifier))
    if record is None:
        return _not_found(identifier)
    return json_response(
        _view(_settle(record), _GET_MEMBERS), content_type=REST_JSON_CONTENT_TYPE
    )


def _update_network_connector(identifier, body):
    record = _connectors.get(_name_from_identifier(identifier))
    if record is None:
        return _not_found(identifier)

    data, err = _parse_body(body)
    if err:
        return err

    token = data.get("ClientToken")
    if token is not None and (not isinstance(token, str) or not 1 <= len(token) <= 64):
        return _invalid("ClientToken must be 1 to 64 characters")

    if "Configuration" in data:
        cfg_err = _validate_configuration(data["Configuration"])
        if cfg_err:
            return cfg_err
        record["Configuration"] = copy.deepcopy(data["Configuration"])
    if "OperatorRole" in data:
        record["OperatorRole"] = data["OperatorRole"]

    _settle(record)
    record["Version"] = (record.get("Version") or 0) + 1
    record["LastUpdateStatus"] = "Successful"
    record["LastModified"] = _now_iso()
    logger.info("UpdateNetworkConnector name=%s", record["Name"])
    # The model gives UpdateNetworkConnector a 202.
    return json_response(
        _view(record, _UPDATE_MEMBERS), status=202,
        content_type=REST_JSON_CONTENT_TYPE,
    )


def _delete_network_connector(identifier):
    name = _name_from_identifier(identifier)
    record = _connectors.get(name)
    if record is None:
        return _not_found(identifier)
    # The response reports the connector as it goes away, so the state it
    # carries is DELETING rather than whatever it was reading before.
    record = dict(record)
    record["State"] = "DELETING"
    del _connectors[name]
    logger.info("DeleteNetworkConnector name=%s", name)
    # The model gives DeleteNetworkConnector a 202.
    return json_response(
        _view(record, _DELETE_MEMBERS), status=202,
        content_type=REST_JSON_CONTENT_TYPE,
    )


def _qp(query_params, key, default=None):
    value = query_params.get(key, default)
    if isinstance(value, list):
        return value[0] if value else default
    return value


def _list_network_connectors(query_params):
    state = _qp(query_params, "State")
    if state is not None and state not in _STATES:
        return _invalid(f"State must be one of {', '.join(_STATES)}")

    raw_max = _qp(query_params, "MaxItems")
    max_items = None
    if raw_max is not None:
        try:
            max_items = int(raw_max)
        except (TypeError, ValueError):
            return _invalid("MaxItems must be an integer")
        if not _MAX_ITEMS_MIN <= max_items <= _MAX_ITEMS_MAX:
            return _invalid(
                f"MaxItems must be between {_MAX_ITEMS_MIN} and {_MAX_ITEMS_MAX}"
            )

    records = [_settle(r) for r in _connectors.values()]
    if state is not None:
        records = [r for r in records if r["State"] == state]
    # Marker carries the name of the last item returned, so a connector created
    # between pages cannot make the walk repeat an entry the way an offset would.
    records.sort(key=lambda r: r["Name"])

    marker = _qp(query_params, "Marker")
    if marker:
        records = [r for r in records if r["Name"] > marker]

    body = {}
    if max_items is not None and len(records) > max_items:
        page = records[:max_items]
        body["NextMarker"] = page[-1]["Name"]
    else:
        page = records
    body["NetworkConnectors"] = [_summary(r) for r in page]
    if "NextMarker" in body:
        body = {"NetworkConnectors": body["NetworkConnectors"],
                "NextMarker": body["NextMarker"]}
    return json_response(body, content_type=REST_JSON_CONTENT_TYPE)


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


async def handle_request(method, path, headers, body, query_params):
    parts = [p for p in path.strip("/").split("/") if p]
    if len(parts) < 2 or parts[0] != API_PREFIX or parts[1] != "network-connectors":
        return _invalid(f"Unsupported Lambda Core path: {path}")

    segments = parts[2:]
    if not segments:
        if method == "POST":
            return _create_network_connector(body)
        if method == "GET":
            return _list_network_connectors(query_params)
    elif len(segments) == 1:
        identifier = unquote(segments[0])
        if method == "GET":
            return _get_network_connector(identifier)
        if method == "PUT":
            return _update_network_connector(identifier, body)
        if method == "DELETE":
            return _delete_network_connector(identifier)

    return _invalid(f"Unsupported Lambda Core request: {method} {path}")
