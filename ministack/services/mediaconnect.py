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
  Flows: CreateFlow, DescribeFlow, ListFlows, UpdateFlow,
         DescribeFlowSourceMetadata, DescribeFlowSourceThumbnail
  Read-only collections (empty — this stub owns no such resources):
         ListBridges, ListEntitlements, ListGateways, ListGatewayInstances,
         ListOfferings, ListReservations
  Describe-by-ARN for the above (NotFoundException — never owned here):
         DescribeBridge, DescribeGateway, DescribeGatewayInstance,
         DescribeReservation, DescribeOffering
  Tags:  ListTagsForResource
"""

import copy
import json
import logging
import re
import time
import urllib.parse

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
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

_flows = AccountRegionScopedDict()   # flowArn -> flow record (camelCase fields)
_tags = AccountScopedDict()    # arn -> {key: value} (ARN key embeds region)


def reset():
    _flows.clear()
    _tags.clear()


def get_state():
    return {
        "flows": copy.deepcopy(_flows),
        "tags": copy.deepcopy(_tags),
    }


def restore_state(data):
    if not data:
        return
    _restore_flows(data.get("flows", {}))
    _tags.update(data.get("tags", {}))


def _region_from_arn(value, fallback):
    if not isinstance(value, str) or not value.startswith("arn:"):
        return fallback
    try:
        return parse_arn(value).region or fallback
    except ArnParseError:
        return fallback


def _restore_flows(restored):
    """Region-scope flows on restore. New state arrives already region-scoped;
    legacy account-scoped state is re-homed by mining the region from the
    flowArn key (each flow lives in exactly one region)."""
    if isinstance(restored, AccountRegionScopedDict):
        _flows.update(restored)
        return
    boot_region = get_region()
    if isinstance(restored, AccountScopedDict):
        entries = restored._data.items()
    elif isinstance(restored, dict):
        account_id = get_account_id()
        entries = (((account_id, arn), flow) for arn, flow in restored.items())
    else:
        _flows.update(restored)
        return
    for (account_id, arn), flow in entries:
        _flows.set_scoped(account_id, _region_from_arn(arn, boot_region), arn, flow)


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


def _resolve_flow_arn(arn):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None, _error(400, "BadRequestException", f"Invalid flow ARN: {arn}")

    if (
        spec.partition != "aws"
        or spec.service != "mediaconnect"
        or spec.region != get_region()
        or spec.account_id != get_account_id()
        or not spec.resource.startswith("flow:")
    ):
        return None, _error(400, "BadRequestException", f"Invalid flow ARN: {arn}")

    if arn not in _flows:
        return None, _error(404, "NotFoundException", f"Flow {arn} not found.")
    return arn, None


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
    arn, err = _resolve_flow_arn(arn)
    if err:
        return err
    return json_response({"tags": _tags.get(arn, {})})


# ---------------------------------------------------------------------------
# Read-only collection ops with no local state.
#
# This emulator is a Flow control-plane stub: it does not model bridges,
# gateways, entitlements, reservations, or reserved-outputs offerings. Those
# resources never exist here, so every List returns an empty collection and
# every Describe-by-ARN returns NotFoundException — matching real AWS for an
# account that owns none of them. Wire key names verified against botocore
# mediaconnect 2018-11-14 service-2.json.
# ---------------------------------------------------------------------------

def _list_bridges(query):
    return json_response({"bridges": []})


def _list_entitlements(query):
    return json_response({"entitlements": []})


def _list_gateways(query):
    return json_response({"gateways": []})


def _list_gateway_instances(query):
    return json_response({"instances": []})


def _list_offerings(query):
    return json_response({"offerings": []})


def _list_reservations(query):
    return json_response({"reservations": []})


def _not_found_by_arn(kind, arn):
    return _error(404, "NotFoundException", f"{kind} {arn} not found.")


def _describe_flow_source_metadata(arn):
    flow = _flows.get(arn)
    if not flow:
        return _error(404, "NotFoundException", f"Flow {arn} not found.")
    return json_response({"flowArn": arn, "messages": []})


def _describe_flow_source_thumbnail(arn):
    flow = _flows.get(arn)
    if not flow:
        return _error(404, "NotFoundException", f"Flow {arn} not found.")
    return json_response({"thumbnailDetails": {"flowArn": arn, "thumbnailMessages": []}})


# ---------------------------------------------------------------------------
# Request Router
# ---------------------------------------------------------------------------

_FLOW_ARN_RE = re.compile(r"^/v1/flows/(arn:aws:mediaconnect:[^/]+:[^/]+:flow:[^/]+:[^/]+)$")
_FLOW_SOURCE_METADATA_RE = re.compile(
    r"^/v1/flows/(arn:aws:mediaconnect:[^/]+:[^/]+:flow:[^/]+:[^/]+)/source-metadata$"
)
_FLOW_SOURCE_THUMBNAIL_RE = re.compile(
    r"^/v1/flows/(arn:aws:mediaconnect:[^/]+:[^/]+:flow:[^/]+:[^/]+)/source-thumbnail$"
)
_TAGS_ARN_RE = re.compile(r"^/tags/(.+)$")

# Read-only collection endpoints (all GET, no request body needed).
_LIST_ROUTES = {
    "/v1/bridges": _list_bridges,
    "/v1/entitlements": _list_entitlements,
    "/v1/gateways": _list_gateways,
    "/v1/gateway-instances": _list_gateway_instances,
    "/v1/offerings": _list_offerings,
    "/v1/reservations": _list_reservations,
}

# Describe-by-ARN endpoints for resources this stub never owns: {prefix: (kind,)}.
# Each returns NotFoundException for any ARN, matching an account with none of
# these resources. Ordered longest-prefix-first at match time.
_DESCRIBE_BY_ARN_ROUTES = (
    ("/v1/gateway-instances/", "GatewayInstance"),
    ("/v1/bridges/", "Bridge"),
    ("/v1/gateways/", "Gateway"),
    ("/v1/reservations/", "Reservation"),
    ("/v1/offerings/", "Offering"),
)


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

    # GET /v1/{bridges,entitlements,gateways,gateway-instances,offerings,reservations}
    if method == "GET" and path in _LIST_ROUTES:
        return _LIST_ROUTES[path](query)

    # GET /v1/flows/{FlowArn}/source-metadata -- DescribeFlowSourceMetadata
    m = _FLOW_SOURCE_METADATA_RE.match(path)
    if m and method == "GET":
        return _describe_flow_source_metadata(urllib.parse.unquote(m.group(1)))

    # GET /v1/flows/{FlowArn}/source-thumbnail -- DescribeFlowSourceThumbnail
    m = _FLOW_SOURCE_THUMBNAIL_RE.match(path)
    if m and method == "GET":
        return _describe_flow_source_thumbnail(urllib.parse.unquote(m.group(1)))

    # /v1/flows/{FlowArn} -- DescribeFlow / UpdateFlow
    m = _FLOW_ARN_RE.match(path)
    if m:
        arn = urllib.parse.unquote(m.group(1))
        if method == "GET":
            return _describe_flow(arn)
        if method == "PUT":
            return _update_flow(arn, body)

    # GET /v1/{bridges,gateways,gateway-instances,reservations,offerings}/{Arn}
    if method == "GET":
        for prefix, kind in _DESCRIBE_BY_ARN_ROUTES:
            if path.startswith(prefix) and path[len(prefix):]:
                arn = urllib.parse.unquote(path[len(prefix):])
                return _not_found_by_arn(kind, arn)

    # GET /tags/{ResourceArn}
    m = _TAGS_ARN_RE.match(path)
    if m and method == "GET":
        arn = urllib.parse.unquote(m.group(1))
        return _list_tags(arn)

    return _error(400, "BadRequestException",
                  f"No route for {method} {path}")
