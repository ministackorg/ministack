"""
AWS Elemental MediaConnect service emulator.
REST/JSON protocol — /v1/flows/* and /tags/* paths.

Control-plane stub only: stores Flow records and tags. Does NOT spawn real
streaming/transcoder backends — MediaConnect flows are metadata in this
emulator. Sufficient for integration-testing services that call the
MediaConnect control-plane API.

Supports:
  Flows: CreateFlow, DescribeFlow, ListFlows, UpdateFlow
  Tags:  ListTagsForResource
"""

import copy
import json
import logging
import re
import time
import urllib.parse

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("mediaconnect")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_flows = AccountScopedDict()   # FlowArn -> flow record
_tags = AccountScopedDict()    # arn -> {key: value}


def reset():
    _flows.clear()
    _tags.clear()


def get_state():
    return {
        "flows": copy.deepcopy(_flows),
        "tags": copy.deepcopy(_tags),
    }


def restore_state(data):
    _flows.update(data.get("flows", {}))
    _tags.update(data.get("tags", {}))


try:
    _restored = load_state("mediaconnect")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted mediaconnect state; continuing fresh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now():
    return int(time.time())


def _flow_arn(name):
    return (
        f"arn:aws:mediaconnect:{get_region()}:{get_account_id()}:"
        f"flow:{new_uuid()}:{name}"
    )


def _error(status, code, message):
    return error_response_json(code, message, status)


# Real AWS ListFlows returns a slimmer ``ListedFlow`` projection — not the
# full Flow record. Keep this in sync with the AWS shape.
_LISTED_FLOW_FIELDS = (
    "AvailabilityZone", "Description", "FlowArn", "Name",
    "SourceType", "Status", "Maintenance",
)


# UpdateFlow only accepts these top-level fields per the AWS API model. Any
# other field on the request body is ignored to match real-AWS behavior.
_UPDATE_FLOW_FIELDS = (
    "SourceFailoverConfig", "Maintenance", "SourceMonitoringConfig", "NdiConfig",
)


def _source_type(flow):
    """Return ``OWNED`` for a flow whose Source comes from local inputs,
    ``ENTITLED`` if Source references an entitlement. Mirrors real AWS."""
    src = flow.get("Source") or {}
    if src.get("EntitlementArn"):
        return "ENTITLED"
    return "OWNED"


def _build_flow(body):
    """Build a Flow record from a CreateFlow request body."""
    name = body.get("Name", "")
    arn = _flow_arn(name)
    flow = {
        "FlowArn": arn,
        "Name": name,
        "AvailabilityZone": body.get(
            "AvailabilityZone", f"{get_region()}a"
        ),
        "Description": body.get("Description", ""),
        "EgressIp": "",
        "Entitlements": body.get("Entitlements", []),
        "MediaStreams": body.get("MediaStreams", []),
        "Outputs": body.get("Outputs", []),
        "Source": body.get("Source", {}),
        "SourceFailoverConfig": body.get("SourceFailoverConfig", {}),
        "Sources": body.get("Sources", []),
        # AWS starts new flows in STANDBY; StartFlow moves them to ACTIVE.
        # We don't implement Start/Stop — clients typically just describe.
        "Status": "STANDBY",
        "VpcInterfaces": body.get("VpcInterfaces", []),
        "Maintenance": body.get("Maintenance", {}),
        "SourceMonitoringConfig": body.get("SourceMonitoringConfig", {}),
        "FlowSize": body.get("FlowSize", "MEDIUM"),
        "NdiConfig": body.get("NdiConfig", {}),
    }
    return flow


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _create_flow(body):
    name = body.get("Name", "")
    if not name:
        return _error(400, "BadRequestException", "Flow Name is required.")
    flow = _build_flow(body)
    _flows[flow["FlowArn"]] = flow
    tags = body.get("Tags") or {}
    if tags:
        _tags[flow["FlowArn"]] = dict(tags)
    return json_response({"Flow": flow}, status=201)


def _list_flows(query):
    max_results = int(query.get("maxResults", 100))
    listed = []
    for arn, f in _flows.items():
        projection = {k: f.get(k) for k in _LISTED_FLOW_FIELDS if k in f}
        projection["SourceType"] = _source_type(f)
        listed.append(projection)
    return json_response({"Flows": listed[:max_results]})


def _describe_flow(arn):
    flow = _flows.get(arn)
    if not flow:
        return _error(404, "NotFoundException",
                      f"Flow {arn} not found.")
    return json_response({"Flow": flow})


def _update_flow(arn, body):
    flow = _flows.get(arn)
    if not flow:
        return _error(404, "NotFoundException",
                      f"Flow {arn} not found.")
    for field in _UPDATE_FLOW_FIELDS:
        if field in body:
            flow[field] = body[field]
    return json_response({"Flow": flow})


def _list_tags(arn):
    return json_response({"Tags": _tags.get(arn, {})})


# ---------------------------------------------------------------------------
# Request Router
# ---------------------------------------------------------------------------

_FLOW_ARN_RE = re.compile(r"^/v1/flows/(arn:aws:mediaconnect:[^/]+:[^/]+:flow:[^/]+:[^/]+)$")
_TAGS_ARN_RE = re.compile(r"^/tags/(.+)$")


async def handle_request(method, path, headers, body_bytes, query_params):
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {}

    query = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}

    # POST /v1/flows -- CreateFlow
    if path == "/v1/flows" and method == "POST":
        return _create_flow(body)

    # GET /v1/flows -- ListFlows
    if path == "/v1/flows" and method == "GET":
        return _list_flows(query)

    # /v1/flows/{FlowArn} -- DescribeFlow / UpdateFlow
    m = _FLOW_ARN_RE.match(path)
    if m:
        arn = urllib.parse.unquote(m.group(1))
        if method == "GET":
            return _describe_flow(arn)
        if method == "PUT":
            return _update_flow(arn, body)

    # GET /tags/{ResourceArn}
    m = _TAGS_ARN_RE.match(path)
    if m and method == "GET":
        arn = urllib.parse.unquote(m.group(1))
        return _list_tags(arn)

    return _error(400, "BadRequestException",
                  f"No route for {method} {path}")
