"""
AWS Elemental MediaConnect service emulator.
REST/JSON protocol — /v1/flows/* and /tags/* paths.

Control-plane stub only: stores Flow records and tags. Does NOT spawn real
streaming/transcoder backends — MediaConnect flows are metadata in this
emulator. Sufficient for integration-testing services that call the
MediaConnect control-plane API.

Wire field names are camelCase per the AWS service model — the in-memory
records also use camelCase so reads and writes round-trip without translation.

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

_flows = AccountScopedDict()   # flowArn -> flow record (camelCase fields)
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
# full Flow record. Keep in sync with the AWS shape.
_LISTED_FLOW_FIELDS = (
    "availabilityZone", "description", "flowArn", "name",
    "sourceType", "status", "maintenance",
)


# UpdateFlow only accepts these top-level fields per the AWS API model.
_UPDATE_FLOW_FIELDS = (
    "sourceFailoverConfig", "maintenance",
    "sourceMonitoringConfig", "ndiConfig",
)


def _source_type(flow):
    """Return ``OWNED`` if Source comes from local input, ``ENTITLED`` if
    Source references an entitlement. Mirrors real AWS."""
    src = flow.get("source") or {}
    if src.get("entitlementArn"):
        return "ENTITLED"
    return "OWNED"


def _build_flow(body):
    """Build a Flow record from a CreateFlow request body (camelCase wire)."""
    name = body.get("name", "")
    arn = _flow_arn(name)
    flow = {
        "flowArn": arn,
        "name": name,
        "availabilityZone": body.get(
            "availabilityZone", f"{get_region()}a"
        ),
        "description": body.get("description", ""),
        "egressIp": "",
        "entitlements": body.get("entitlements", []),
        "mediaStreams": body.get("mediaStreams", []),
        "outputs": body.get("outputs", []),
        "source": body.get("source", {}),
        "sourceFailoverConfig": body.get("sourceFailoverConfig", {}),
        "sources": body.get("sources", []),
        # AWS starts new flows in STANDBY; StartFlow moves them to ACTIVE.
        # Start/Stop are out of scope — clients typically just describe.
        "status": "STANDBY",
        "vpcInterfaces": body.get("vpcInterfaces", []),
        "maintenance": body.get("maintenance", {}),
        "sourceMonitoringConfig": body.get("sourceMonitoringConfig", {}),
        "flowSize": body.get("flowSize", "MEDIUM"),
        "ndiConfig": body.get("ndiConfig", {}),
    }
    return flow


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _create_flow(body):
    name = body.get("name", "")
    if not name:
        return _error(400, "BadRequestException", "Flow Name is required.")
    flow = _build_flow(body)
    _flows[flow["flowArn"]] = flow
    # CreateFlow accepts an optional tags map on newer botocore models
    # (``FlowTags`` in the SDK, ``flowTags`` on the wire); honor it if present.
    tags = body.get("flowTags") or body.get("tags") or {}
    if tags:
        _tags[flow["flowArn"]] = dict(tags)
    return json_response({"flow": flow}, status=201)


def _list_flows(query):
    max_results = int(query.get("maxResults", 100))
    listed = []
    for arn, f in _flows.items():
        projection = {k: f.get(k) for k in _LISTED_FLOW_FIELDS if k in f}
        projection["sourceType"] = _source_type(f)
        listed.append(projection)
    return json_response({"flows": listed[:max_results]})


def _describe_flow(arn):
    flow = _flows.get(arn)
    if not flow:
        return _error(404, "NotFoundException",
                      f"Flow {arn} not found.")
    return json_response({"flow": flow})


def _update_flow(arn, body):
    flow = _flows.get(arn)
    if not flow:
        return _error(404, "NotFoundException",
                      f"Flow {arn} not found.")
    for field in _UPDATE_FLOW_FIELDS:
        if field in body:
            flow[field] = body[field]
    return json_response({"flow": flow})


def _list_tags(arn):
    return json_response({"tags": _tags.get(arn, {})})


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
