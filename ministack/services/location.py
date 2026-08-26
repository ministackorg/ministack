"""
Amazon Location Service emulator — trackers only.
REST/JSON protocol — /tracking/v0/* paths.

Routing note: the SDK client is named ``location`` but signs with credential
scope ``geo`` (botocore signingName), and the modeled endpoint host prefixes
(``cp.tracking.`` for the tracker control plane, ``tracking.`` for the device
position data plane) sit in front of the ``geo.{region}`` endpoint — so a
request arrives with host ``cp.tracking.geo.{region}.{host}`` or, under an
endpoint override, with only the ``geo`` credential scope to go by. See
``SERVICE_PATTERNS["location"]`` and the ``"geo"`` scope-map entry in
``core/router.py``.

Wire traps, verified against the botocore ``location`` 2020-11-19 model:
  * ``SampleTime`` / ``ReceivedTime`` / ``CreateTime`` / ``UpdateTime`` are
    ``timestamp`` shapes with an explicit ``timestampFormat: iso8601`` — the
    SDK SENDS ISO8601 strings, and responses here emit ISO8601 UTC strings,
    which botocore's lenient timestamp parser turns back into datetimes (it
    also accepts epoch numbers, so both are tolerated on the way in; the
    store keeps a canonical epoch-seconds float).
  * ``Position`` is a modeled double list of exactly two members,
    ``[longitude, latitude]`` — not an object.

Supports:
  Trackers:  CreateTracker, DescribeTracker, UpdateTracker, ListTrackers,
             DeleteTracker
  Positions: BatchUpdateDevicePosition, GetDevicePosition,
             BatchGetDevicePosition, GetDevicePositionHistory

Scope boundaries (metadata-only control plane + in-memory position store):
  * ``PricingPlan``, ``KmsKeyId``, ``PositionFiltering``, ``EventBridgeEnabled``
    and ``Tags`` are stored and echoed but have no behavior — no KMS
    encryption and no EventBridge position events. Position filtering is a
    stated divergence: real ``TimeBased`` filtering stores at most one
    position per 30 seconds per device and retains position data for 30
    days; MiniStack stores every accepted sample and instead bounds
    per-device history at the newest 100 samples (oldest dropped first).
  * ``MaxResults`` is honored on ``ListTrackers`` and
    ``GetDevicePositionHistory``, but no ``NextToken`` is ever emitted —
    with the 100-sample history bound there is never more than one page.
  * No geofence-collection consumers (``AssociateTrackerConsumer`` et al.),
    and no maps / places / routes APIs.
  * ``GetDevicePositionHistory`` returns samples in ascending ``SampleTime``
    order.
"""

import copy
import datetime
import json
import logging
import re
import time
import urllib.parse

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)

logger = logging.getLogger("location")

# Newest samples kept per device for GetDevicePositionHistory (oldest dropped).
_HISTORY_LIMIT = 100

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

# TrackerName -> tracker record. Each record carries its own device-position
# store under "positions": {DeviceId: {"latest": <position>, "history": [...]}}
# where a position is {"DeviceId", "SampleTime": float, "ReceivedTime": float,
# "Position": [lon, lat], "Accuracy"?, "PositionProperties"?}.
_trackers = AccountRegionScopedDict()


def reset():
    _trackers.clear()


def get_state():
    return {
        "trackers": copy.deepcopy(_trackers),
    }


def restore_state(data):
    if not data:
        return
    _trackers.update(data.get("trackers", {}))


try:
    _restored = load_state("location")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted location state; continuing fresh")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now():
    return time.time()


def _tracker_arn(name):
    return f"arn:aws:geo:{get_region()}:{get_account_id()}:tracker/{name}"


def _error(status, code, message):
    return error_response_json(code, message, status)


def _not_found(name):
    # Live-service wording, probed 2026-08-26.
    return _error(404, "ResourceNotFoundException", f"Tracker not found: {name}.")


def _parse_timestamp(value):
    """Accept the model's ISO8601 string or an epoch number; return an
    epoch-seconds float, or None when the value is absent or unparseable."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return parsed.timestamp()
    return None


def _iso(ts):
    """Epoch-seconds float -> the model's iso8601 wire form (millisecond
    precision, `Z` suffix — what real Location emits)."""
    return (
        datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
        .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    )


# The model's ResourceName shape: pattern [-._\w]+, length 1-100.
_TRACKER_NAME_RE = re.compile(r"[-._\w]{1,100}")

# CreateTracker request members stored verbatim and echoed by DescribeTracker.
_STORED_OPTIONALS = (
    "PricingPlan", "PricingPlanDataSource", "Description", "Tags",
    "PositionFiltering", "EventBridgeEnabled", "KmsKeyId",
    "KmsKeyEnableGeospatialQueries",
)

# Response members the live service always materializes (probed 2026-08-26):
# DescribeTracker carries these defaults even when CreateTracker omitted them.
_CREATE_DEFAULTS = {
    "Description": "",
    "Tags": {},
    "PositionFiltering": "TimeBased",
    "EventBridgeEnabled": False,
}

# UpdateTracker's in-place-mutable members (the emulator's supported subset).
_MUTABLE_FIELDS = ("Description", "PositionFiltering", "EventBridgeEnabled")


def _tracker_view(rec):
    """DescribeTracker response — required members plus stored optionals."""
    view = {
        "TrackerName": rec["TrackerName"],
        "TrackerArn": rec["TrackerArn"],
        "Description": rec.get("Description", ""),
        "CreateTime": _iso(rec["CreateTime"]),
        "UpdateTime": _iso(rec["UpdateTime"]),
    }
    for field in _STORED_OPTIONALS:
        if field not in ("Description",) and field in rec:
            view[field] = rec[field]
    return view


def _position_view(pos):
    view = {
        "DeviceId": pos["DeviceId"],
        "SampleTime": _iso(pos["SampleTime"]),
        "ReceivedTime": _iso(pos["ReceivedTime"]),
        "Position": pos["Position"],
    }
    if "Accuracy" in pos:
        view["Accuracy"] = pos["Accuracy"]
    if "PositionProperties" in pos:
        view["PositionProperties"] = pos["PositionProperties"]
    return view


# ---------------------------------------------------------------------------
# Tracker handlers
# ---------------------------------------------------------------------------

def _create_tracker(body):
    name = body.get("TrackerName", "")
    if not name:
        return _error(400, "ValidationException", "TrackerName is required.")
    if len(name) > 100:
        return _error(
            400, "ValidationException",
            f"1 validation error detected: Value '{name}' at 'trackerName' "
            "failed to satisfy constraint: Member must have length less than "
            "or equal to 100",
        )
    if not _TRACKER_NAME_RE.fullmatch(name):
        # An unvalidated name with e.g. "/" would be unaddressable through
        # the path-parameter routes below.
        return _error(
            400, "ValidationException",
            f"1 validation error detected: Value '{name}' at 'trackerName' "
            "failed to satisfy constraint: Member must satisfy regular "
            "expression pattern: [-._\\w]+",
        )
    if name in _trackers:
        # Live-service wording, probed 2026-08-26.
        return _error(409, "ConflictException",
                      f"Tracker already exists: {name}.")
    now = _now()
    rec = {
        "TrackerName": name,
        "TrackerArn": _tracker_arn(name),
        "CreateTime": now,
        "UpdateTime": now,
        "positions": {},
    }
    rec.update(copy.deepcopy(_CREATE_DEFAULTS))
    for field in _STORED_OPTIONALS:
        if field in body:
            rec[field] = body[field]
    _trackers[name] = rec
    return json_response({
        "TrackerName": name,
        "TrackerArn": rec["TrackerArn"],
        "CreateTime": _iso(now),
    })


def _describe_tracker(name):
    rec = _trackers.get(name)
    if rec is None:
        return _not_found(name)
    return json_response(_tracker_view(rec))


def _update_tracker(name, body):
    rec = _trackers.get(name)
    if rec is None:
        return _not_found(name)
    for field in _MUTABLE_FIELDS:
        if field in body:
            rec[field] = body[field]
    rec["UpdateTime"] = _now()
    return json_response({
        "TrackerName": name,
        "TrackerArn": rec["TrackerArn"],
        "UpdateTime": _iso(rec["UpdateTime"]),
    })


def _list_trackers(body):
    max_results = int(body.get("MaxResults") or 100)
    entries = []
    for rec in _trackers.values():
        entry = {
            "TrackerName": rec["TrackerName"],
            "Description": rec.get("Description", ""),
            "CreateTime": _iso(rec["CreateTime"]),
            "UpdateTime": _iso(rec["UpdateTime"]),
        }
        for field in ("PricingPlan", "PricingPlanDataSource"):
            if field in rec:
                entry[field] = rec[field]
        entries.append(entry)
    return json_response({"Entries": entries[:max_results]})


def _delete_tracker(name):
    if name not in _trackers:
        return _not_found(name)
    del _trackers[name]
    return json_response({})


# ---------------------------------------------------------------------------
# Device position handlers
# ---------------------------------------------------------------------------

def _batch_update_positions(name, body):
    rec = _trackers.get(name)
    if rec is None:
        return _not_found(name)
    errors = []
    for update in body.get("Updates") or []:
        device_id = update.get("DeviceId") or ""
        sample_time = _parse_timestamp(update.get("SampleTime"))
        position = update.get("Position")
        problem = None
        if not device_id:
            problem = "Missing required member: DeviceId"
        elif sample_time is None:
            problem = "Missing or unparseable required member: SampleTime"
        elif (
            not isinstance(position, list) or len(position) != 2
            or not all(isinstance(c, (int, float)) and not isinstance(c, bool)
                       for c in position)
        ):
            problem = "Position must be a [longitude, latitude] double pair"
        # Range wording below is the live service's, probed 2026-08-26.
        elif not -180.0 <= position[0] <= 180.0:
            problem = "longitude must be between -180 and 180 degrees"
        elif not -90.0 <= position[1] <= 90.0:
            problem = "latitude must be between -90 and 90 degrees"
        if problem:
            errors.append({
                "DeviceId": device_id,
                "SampleTime": _iso(sample_time if sample_time is not None else _now()),
                "Error": {"Code": "ValidationError", "Message": problem},
            })
            continue
        pos = {
            "DeviceId": device_id,
            "SampleTime": sample_time,
            "ReceivedTime": _now(),
            "Position": [float(position[0]), float(position[1])],
        }
        if "Accuracy" in update:
            pos["Accuracy"] = update["Accuracy"]
        if "PositionProperties" in update:
            pos["PositionProperties"] = update["PositionProperties"]
        device = rec["positions"].setdefault(device_id, {"latest": None, "history": []})
        history = device["history"]
        history.append(pos)
        history.sort(key=lambda p: (p["SampleTime"], p["ReceivedTime"]))
        del history[:-_HISTORY_LIMIT]
        device["latest"] = history[-1]
    return json_response({"Errors": errors})


def _get_device_position(name, device_id):
    rec = _trackers.get(name)
    if rec is None:
        return _not_found(name)
    device = rec["positions"].get(device_id)
    if device is None or device.get("latest") is None:
        # Live-service wording, probed 2026-08-26.
        return _error(404, "ResourceNotFoundException",
                      "records not found for given deviceId")
    return json_response(_position_view(device["latest"]))


def _batch_get_positions(name, body):
    rec = _trackers.get(name)
    if rec is None:
        return _not_found(name)
    device_ids = body.get("DeviceIds")
    if not device_ids:
        return _error(400, "ValidationException", "DeviceIds is required.")
    found = []
    errors = []
    for device_id in device_ids:
        device = rec["positions"].get(device_id)
        if device is None or device.get("latest") is None:
            # The API model documents ResourceNotFoundError entries for
            # missing devices; the live service was observed (2026-08-26)
            # returning Errors [] and simply omitting the device instead.
            # We keep the documented shape.
            errors.append({
                "DeviceId": device_id,
                "Error": {
                    "Code": "ResourceNotFoundError",
                    "Message": f"Device {device_id} has no position on tracker {name}.",
                },
            })
            continue
        found.append(_position_view(device["latest"]))
    return json_response({"DevicePositions": found, "Errors": errors})


def _get_position_history(name, device_id, body):
    rec = _trackers.get(name)
    if rec is None:
        return _not_found(name)
    device = rec["positions"].get(device_id, {})
    # Documented defaults when the members are omitted: the 24 hours up to now.
    start = _parse_timestamp(body.get("StartTimeInclusive"))
    if start is None:
        start = _now() - 24 * 3600
    end = _parse_timestamp(body.get("EndTimeExclusive"))
    if end is None:
        end = _now()
    max_results = int(body.get("MaxResults") or 100)
    positions = [
        _position_view(pos)
        for pos in device.get("history", [])
        if start <= pos["SampleTime"] < end
    ]
    return json_response({"DevicePositions": positions[:max_results]})


# ---------------------------------------------------------------------------
# Request Router
# ---------------------------------------------------------------------------

_TRACKER_RE = re.compile(r"^/tracking/v0/trackers/([^/]+)$")
_POSITIONS_RE = re.compile(r"^/tracking/v0/trackers/([^/]+)/positions$")
_GET_POSITIONS_RE = re.compile(r"^/tracking/v0/trackers/([^/]+)/get-positions$")
_DEVICE_LATEST_RE = re.compile(
    r"^/tracking/v0/trackers/([^/]+)/devices/([^/]+)/positions/latest$"
)
_DEVICE_HISTORY_RE = re.compile(
    r"^/tracking/v0/trackers/([^/]+)/devices/([^/]+)/list-positions$"
)


async def handle_request(method, path, headers, body_bytes, query_params):
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        return _error(400, "ValidationException",
                      "Could not deserialize the request body as JSON.")

    # POST /tracking/v0/trackers -- CreateTracker
    if path == "/tracking/v0/trackers" and method == "POST":
        return _create_tracker(body)

    # POST /tracking/v0/list-trackers -- ListTrackers
    if path == "/tracking/v0/list-trackers" and method == "POST":
        return _list_trackers(body)

    # POST /tracking/v0/trackers/{TrackerName}/positions -- BatchUpdateDevicePosition
    m = _POSITIONS_RE.match(path)
    if m and method == "POST":
        return _batch_update_positions(urllib.parse.unquote(m.group(1)), body)

    # POST /tracking/v0/trackers/{TrackerName}/get-positions -- BatchGetDevicePosition
    m = _GET_POSITIONS_RE.match(path)
    if m and method == "POST":
        return _batch_get_positions(urllib.parse.unquote(m.group(1)), body)

    # GET /tracking/v0/trackers/{T}/devices/{D}/positions/latest -- GetDevicePosition
    m = _DEVICE_LATEST_RE.match(path)
    if m and method == "GET":
        return _get_device_position(
            urllib.parse.unquote(m.group(1)), urllib.parse.unquote(m.group(2))
        )

    # POST /tracking/v0/trackers/{T}/devices/{D}/list-positions -- GetDevicePositionHistory
    m = _DEVICE_HISTORY_RE.match(path)
    if m and method == "POST":
        return _get_position_history(
            urllib.parse.unquote(m.group(1)), urllib.parse.unquote(m.group(2)), body
        )

    # /tracking/v0/trackers/{TrackerName}
    #   -- DescribeTracker / UpdateTracker / DeleteTracker
    m = _TRACKER_RE.match(path)
    if m:
        name = urllib.parse.unquote(m.group(1))
        if method == "GET":
            return _describe_tracker(name)
        if method == "PATCH":
            return _update_tracker(name, body)
        if method == "DELETE":
            return _delete_tracker(name)

    return _error(400, "ValidationException", f"No route for {method} {path}")
