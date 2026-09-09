# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
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
  * ``KmsKeyId``, ``KmsKeyEnableGeospatialQueries``, ``PositionFiltering``,
    ``EventBridgeEnabled`` and ``Tags`` are stored and echoed but have no
    behavior — no KMS encryption and no EventBridge position events. The
    deprecated ``PricingPlan`` always reads ``RequestBasedUsage`` (documented,
    and on the live DescribeTracker of 2026-08-26); ``PricingPlanDataSource``
    is not returned, because it was absent from that live describe although
    the reference says "Always returns an empty string". Position
    filtering is a stated divergence: real ``TimeBased`` filtering stores at
    most one position per 30 seconds per device and retains position data
    for 30 days; MiniStack stores every accepted sample and instead bounds
    per-device history at the newest 100 samples (oldest dropped first).
  * ``ListTrackers`` and ``GetDevicePositionHistory`` page at ``MaxResults``
    (1..100, default 100) with a ``NextToken`` carrying the sort position of
    the last item returned.
  * The documented member constraints are refused with a
    ``ValidationException`` before the request is processed — before the
    tracker is looked up, so an invalid request naming a tracker that does
    not exist answers 400, the way a real request-validation layer sitting in
    front of the operation does, not 404: ``Description``
    0..1000, ``KmsKeyId`` 1..2048, ``Tags`` at most 50 entries with keys
    1..128 and values 0..256 on the tag pattern, ``PositionProperties`` at
    most 4 entries with keys 1..20 and values 1..150. The message wordings
    are unmeasured.
  * ``BatchUpdateDevicePosition`` answers a structurally invalid entry (a
    required member missing, ``Position`` not two numbers) with a
    request-level ``ValidationException``; the per-entry ``Errors`` list is
    kept for the measured value-range case. ``BatchGetDevicePosition`` omits
    a device without a position and returns ``Errors: []``, as the live
    service did (2026-08-26).
  * No geofence-collection consumers (``AssociateTrackerConsumer`` et al.),
    and no maps / places / routes APIs.
  * ``GetDevicePositionHistory`` returns samples in ascending ``SampleTime``
    order.
"""

import base64
import copy
import datetime
import json
import logging
import math
import re
import time
import unicodedata
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
# "Position": [lon, lat], "Seq": int, "Accuracy"?, "PositionProperties"?}.
_trackers = AccountRegionScopedDict()

# A monotonic counter stamped on every stored sample as "Seq". It is the last
# component of the history sort key, so two samples that share SampleTime and
# ReceivedTime still have a total order. Without it a NextToken pointing at one
# of them would resume with a strict ">" that also excludes the other, and the
# sample behind the page boundary would never be returned.
_sequence = 0


def _next_sequence():
    global _sequence
    _sequence += 1
    return _sequence


def reset():
    global _sequence
    _trackers.clear()
    _sequence = 0


def get_state():
    return {
        "trackers": copy.deepcopy(_trackers),
    }


def restore_state(data):
    global _sequence
    if not data:
        return
    _trackers.update(data.get("trackers", {}))
    # Keep the counter above every restored sample, so a sample stored after
    # the restore still sorts behind the restored ones.
    for rec in _trackers.all_values():
        for device in rec.get("positions", {}).values():
            for pos in device.get("history", []):
                _sequence = max(_sequence, pos.get("Seq", 0))


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


def _validation(message):
    return _error(400, "ValidationException", message)


def _constraint(value, field, constraint):
    """The request-surface ValidationException shape: one violated member
    constraint. The exact wordings are unmeasured on Amazon Location; the
    shape is the one the AWS SDKs render for every service."""
    return _validation(
        f"1 validation error detected: Value '{value}' at '{field}' failed to "
        f"satisfy constraint: {constraint}"
    )


def _not_found(name):
    # This wording was measured on GetDevicePosition against a missing tracker
    # (2026-08-26). The other tracker operations reuse it; their texts were not
    # measured.
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


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _max_results(body):
    """``MaxResults`` (1..100, default 100) as ``(value, error_response)``."""
    value = body.get("MaxResults")
    if value is None:
        return 100, None
    if not _is_number(value) or int(value) != value:
        return None, _validation("MaxResults must be an integer")
    value = int(value)
    if value > 100:
        return None, _constraint(
            value, "maxResults", "Member must have value less than or equal to 100"
        )
    if value < 1:
        return None, _constraint(
            value, "maxResults", "Member must have value greater than or equal to 1"
        )
    return value, None


def _encode_token(key):
    """Tokens carry the sort position of the last item returned, not an
    index: an offset would skip an item whenever one was deleted between
    pages and repeat one whenever one was created."""
    raw = json.dumps(list(key)).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_token(token, casts):
    """The sort key a token carries, or None when the token is not ours."""
    try:
        raw = json.loads(base64.urlsafe_b64decode(token.encode("ascii")).decode("utf-8"))
        if not isinstance(raw, list) or len(raw) != len(casts):
            return None
        return tuple(cast(item) for cast, item in zip(casts, raw))
    except Exception:
        return None


_INVALID_TOKEN = "The NextToken that you provided is invalid."


# The model's ResourceName shape: pattern [-._\w]+, length 1-100 (the length
# is checked separately so the two constraints report distinctly). The pattern
# is a Java regex, where \w is [a-zA-Z0-9_]; Python's \w also matches every
# Unicode letter and digit, so re.ASCII is what makes the two agree. The
# CreateTracker reference spells the same thing out: a tracker name may
# "contain only alphanumeric characters (A-Z, a-z, 0-9), hyphens (-), periods
# (.), and underscores (_)".
_TRACKER_NAME_RE = re.compile(r"[-._\w]+", re.ASCII)

_POSITION_FILTERING = ("TimeBased", "DistanceBased", "AccuracyBased")

# CreateTracker request members stored verbatim and echoed by DescribeTracker.
# The deprecated PricingPlan / PricingPlanDataSource are not among them:
# DescribeTracker documents PricingPlan as "Always returns RequestBasedUsage"
# and PricingPlanDataSource as "No longer used. Always returns an empty
# string" — but the live DescribeTracker of 2026-08-26 had no
# PricingPlanDataSource member at all, and the measurement is what is followed.
_STORED_OPTIONALS = (
    "Description", "Tags", "PositionFiltering", "EventBridgeEnabled",
    "KmsKeyId", "KmsKeyEnableGeospatialQueries",
)

# Response members the live service always materializes (probed 2026-08-26):
# DescribeTracker carries these even when CreateTracker omitted them.
_CREATE_DEFAULTS = {
    "Description": "",
    "PricingPlan": "RequestBasedUsage",
    "Tags": {},
    "PositionFiltering": "TimeBased",
    "EventBridgeEnabled": False,
}

# UpdateTracker's request members that the emulator stores (PricingPlan and
# PricingPlanDataSource are validated and ignored, as deprecated on AWS).
_MUTABLE_FIELDS = (
    "Description", "PositionFiltering", "EventBridgeEnabled",
    "KmsKeyEnableGeospatialQueries",
)


# The tag key/value pattern of the CreateTracker reference,
# ``([\p{L}\p{Z}\p{N}_.,:/=+\-@]*)``. The stdlib ``re`` has no Unicode property
# classes, so the three categories are checked through unicodedata instead:
# letters (L*), separators (Z*) and numbers (N*), plus these literals.
_TAG_LITERALS = frozenset("_.,:/=+-@")


def _tag_text_ok(value):
    return all(
        unicodedata.category(ch)[0] in ("L", "Z", "N") or ch in _TAG_LITERALS
        for ch in value
    )


def _validate_string_length(body, member, field, minimum, maximum):
    """A documented string length constraint, checked before the request is
    processed. The wordings are unmeasured (see ``_constraint``)."""
    value = body.get(member)
    if value is None:
        return None
    if not isinstance(value, str):
        return _constraint(value, field, "Member must be a string")
    if len(value) > maximum:
        return _constraint(
            value, field, f"Member must have length less than or equal to {maximum}"
        )
    if len(value) < minimum:
        return _constraint(
            value, field, f"Member must have length greater than or equal to {minimum}"
        )
    return None


def _validate_string_map(value, field, max_entries, key_len, value_len, pattern=False):
    """A documented string-to-string map constraint (Tags on CreateTracker,
    PositionProperties on a device position update). Wordings unmeasured."""
    if value is None:
        return None
    if not isinstance(value, dict):
        return _constraint(value, field, "Member must be a map")
    if len(value) > max_entries:
        return _constraint(
            len(value), field,
            f"Member must have length less than or equal to {max_entries}",
        )
    for key, item in value.items():
        for text, kind, (minimum, maximum) in (
            (key, "keys", key_len), (item, "values", value_len)
        ):
            if not isinstance(text, str):
                return _constraint(text, field, f"Map {kind} must be a string")
            if len(text) > maximum:
                return _constraint(
                    text, field,
                    f"Map {kind} must have length less than or equal to {maximum}",
                )
            if len(text) < minimum:
                return _constraint(
                    text, field,
                    f"Map {kind} must have length greater than or equal to {minimum}",
                )
            if pattern and not _tag_text_ok(text):
                return _constraint(
                    text, field,
                    f"Map {kind} must satisfy regular expression pattern: "
                    r"([\p{L}\p{Z}\p{N}_.,:/=+\-@]*)",
                )
    return None


def _validate_settings(body):
    """The members CreateTracker and UpdateTracker share. botocore enforces
    only the minimum lengths client-side, so a maximum length, an enum value
    or a pattern from a real SDK call reaches this. Everything here runs
    before the request is processed."""
    err = _validate_string_length(body, "Description", "description", 0, 1000)
    if err is not None:
        return err
    filtering = body.get("PositionFiltering")
    if filtering is not None and filtering not in _POSITION_FILTERING:
        return _constraint(
            filtering, "positionFiltering",
            "Member must satisfy enum value set: [TimeBased, DistanceBased, AccuracyBased]",
        )
    plan = body.get("PricingPlan")
    if plan is not None and plan != "RequestBasedUsage":
        # The model's enum still carries MobileAssetTracking and
        # MobileAssetManagement, but the member is deprecated: "No longer used.
        # If included, the only allowed value is RequestBasedUsage."
        # (CreateTracker reference.) An enum-set message naming one member
        # would be misleading, so the refusal says what happened to the rest.
        # Wording unmeasured.
        return _validation(
            f"PricingPlan '{plan}' is no longer accepted. The only allowed "
            f"value is RequestBasedUsage."
        )
    return None


def _tracker_view(rec):
    """DescribeTracker response — required members plus stored optionals."""
    view = {
        "TrackerName": rec["TrackerName"],
        "TrackerArn": rec["TrackerArn"],
        "Description": rec["Description"],
        "PricingPlan": rec["PricingPlan"],
        "CreateTime": _iso(rec["CreateTime"]),
        "UpdateTime": _iso(rec["UpdateTime"]),
    }
    for field in _STORED_OPTIONALS:
        if field != "Description" and field in rec:
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
        return _validation("TrackerName is required.")
    if len(name) > 100:
        return _constraint(
            name, "trackerName", "Member must have length less than or equal to 100"
        )
    if not _TRACKER_NAME_RE.fullmatch(name):
        # An unvalidated name with e.g. "/" would be unaddressable through
        # the path-parameter routes below.
        return _constraint(
            name, "trackerName",
            "Member must satisfy regular expression pattern: [-._\\w]+",
        )
    err = _validate_settings(body)
    if err is not None:
        return err
    # CreateTracker-only members (UpdateTracker takes neither).
    err = _validate_string_length(body, "KmsKeyId", "kmsKeyId", 1, 2048)
    if err is not None:
        return err
    err = _validate_string_map(
        body.get("Tags"), "tags", 50, (1, 128), (0, 256), pattern=True
    )
    if err is not None:
        return err
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
    err = _validate_settings(body)
    if err is not None:
        return err
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
    max_results, err = _max_results(body)
    if err is not None:
        return err
    cursor = None
    token = body.get("NextToken")
    if token:
        cursor = _decode_token(token, (str,))
        if cursor is None:
            return _validation(_INVALID_TOKEN)
    # Sorted by name: a total order the token can resume from.
    records = sorted(_trackers.values(), key=lambda r: r["TrackerName"])
    if cursor is not None:
        records = [r for r in records if r["TrackerName"] > cursor[0]]
    page = records[:max_results]
    result = {"Entries": [
        {
            "TrackerName": rec["TrackerName"],
            "Description": rec["Description"],
            "PricingPlan": rec["PricingPlan"],
            "CreateTime": _iso(rec["CreateTime"]),
            "UpdateTime": _iso(rec["UpdateTime"]),
        }
        for rec in page
    ]}
    if len(records) > max_results:
        result["NextToken"] = _encode_token((page[-1]["TrackerName"],))
    return json_response(result)


def _delete_tracker(name):
    if name not in _trackers:
        return _not_found(name)
    del _trackers[name]
    return json_response({})


# ---------------------------------------------------------------------------
# Device position handlers
# ---------------------------------------------------------------------------

def _parse_update(index, update):
    """A DevicePositionUpdate's required members, or the ValidationException
    for a structurally invalid entry. botocore refuses to send one, so the
    raw-wire answer of the live service is unmeasured; the request-level
    400 follows the model (DeviceId, SampleTime and Position are required)."""
    member = f"updates.{index + 1}.member"
    if not isinstance(update, dict):
        return None, _constraint(update, member, "Member must be a structure")
    device_id = update.get("DeviceId")
    if not isinstance(device_id, str) or not device_id:
        return None, _constraint(device_id, f"{member}.deviceId", "Member must not be null")
    sample_time = _parse_timestamp(update.get("SampleTime"))
    if sample_time is None:
        return None, _constraint(
            update.get("SampleTime"), f"{member}.sampleTime", "Member must not be null"
        )
    position = update.get("Position")
    if (not isinstance(position, list) or len(position) != 2
            or not all(_is_number(c) for c in position)):
        return None, _constraint(
            position, f"{member}.position", "Member must have length equal to 2"
        )
    # PositionProperties: at most 4 entries, keys 1..20, values 1..150
    # (DevicePositionUpdate reference).
    err = _validate_string_map(
        update.get("PositionProperties"), f"{member}.positionProperties",
        4, (1, 20), (1, 150),
    )
    if err is not None:
        return None, err
    return (device_id, sample_time, position), None


# The filtering thresholds the CreateTracker reference documents: TimeBased
# stores "only one update per 30 seconds ... for each unique device ID",
# DistanceBased ignores an update when "the device has moved less than 30 m
# (98.4 ft)", and AccuracyBased ignores it when the device "has moved less
# than the measured accuracy" (the reference's own example adds the two
# horizontal accuracies: 5 m and 10 m ignore a move under 15 m).
_TIME_FILTER_SECONDS = 30.0
_DISTANCE_FILTER_METRES = 30.0
_EARTH_RADIUS_METRES = 6371008.8


def _metres_between(a, b):
    """Great-circle distance between two [lon, lat] pairs, in metres."""
    lon1, lat1 = math.radians(a[0]), math.radians(a[1])
    lon2, lat2 = math.radians(b[0]), math.radians(b[1])
    dlon, dlat = lon2 - lon1, lat2 - lat1
    h = (math.sin(dlat / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2)
    return 2 * _EARTH_RADIUS_METRES * math.asin(min(1.0, math.sqrt(h)))


def _horizontal_accuracy(accuracy):
    """The Horizontal member of a PositionalAccuracy, or None."""
    if isinstance(accuracy, dict) and _is_number(accuracy.get("Horizontal")):
        return float(accuracy["Horizontal"])
    return None


def _is_filtered_out(mode, previous, sample_time, position, accuracy):
    """Whether the tracker's PositionFiltering drops this update.

    The comparison is against the newest sample the device already has. A
    dropped update is not an error: the reference says such updates are
    "neither evaluated against linked geofence collections, nor stored", and
    BatchUpdateDevicePosition reports nothing for them. Unmeasured: an update
    whose SampleTime is older than the stored one, which the reference does
    not describe; MiniStack treats it by the same distance in time.
    """
    if previous is None:
        return False
    if mode == "TimeBased":
        # SampleTime is epoch seconds here (see _parse_timestamp).
        return abs(sample_time - previous["SampleTime"]) < _TIME_FILTER_SECONDS
    moved = _metres_between(previous["Position"], position)
    if mode == "DistanceBased":
        return moved < _DISTANCE_FILTER_METRES
    if mode == "AccuracyBased":
        previous_accuracy = _horizontal_accuracy(previous.get("Accuracy"))
        current_accuracy = _horizontal_accuracy(accuracy)
        if previous_accuracy is None and current_accuracy is None:
            # Nothing measured the accuracy, so nothing can be below it.
            return False
        threshold = (previous_accuracy or 0.0) + (current_accuracy or 0.0)
        return moved < threshold
    return False


def _batch_update_positions(name, body):
    updates = body.get("Updates")
    if not isinstance(updates, list) or not updates:
        return _constraint(
            updates, "updates", "Member must have length greater than or equal to 1"
        )
    if len(updates) > 10:
        return _constraint(
            len(updates), "updates", "Member must have length less than or equal to 10"
        )
    parsed = []
    for index, update in enumerate(updates):
        members, err = _parse_update(index, update)
        if err is not None:
            return err
        parsed.append((update, *members))
    rec = _trackers.get(name)
    if rec is None:
        return _not_found(name)
    errors = []
    # One receive time for the whole batch: the request arrived once, and a
    # per-entry clock read would make the order of two samples that share a
    # SampleTime depend on the clock resolution. Unmeasured.
    received = _now()
    for update, device_id, sample_time, position in parsed:
        problem = None
        if not -90.0 <= position[1] <= 90.0:
            # Live-service wording, probed 2026-08-26.
            problem = "latitude must be between -90 and 90 degrees"
        elif not -180.0 <= position[0] <= 180.0:
            # Unmeasured: the latitude text with the longitude range.
            problem = "longitude must be between -180 and 180 degrees"
        if problem:
            errors.append({
                "DeviceId": device_id,
                "SampleTime": _iso(sample_time),
                "Error": {"Code": "ValidationError", "Message": problem},
            })
            continue
        device = rec["positions"].setdefault(device_id, {"latest": None, "history": []})
        if _is_filtered_out(rec.get("PositionFiltering", "TimeBased"),
                            device["latest"], sample_time, position,
                            update.get("Accuracy")):
            continue
        pos = {
            "DeviceId": device_id,
            "SampleTime": sample_time,
            "ReceivedTime": received,
            "Position": [float(position[0]), float(position[1])],
            "Seq": _next_sequence(),
        }
        if "Accuracy" in update:
            pos["Accuracy"] = update["Accuracy"]
        if "PositionProperties" in update:
            pos["PositionProperties"] = update["PositionProperties"]
        history = device["history"]
        history.append(pos)
        history.sort(key=_history_key)
        del history[:-_HISTORY_LIMIT]
        device["latest"] = history[-1]
    return json_response({"Errors": errors})


def _history_key(pos):
    # "Seq" makes the key a total order: samples that share both timestamps
    # would otherwise be one point for a NextToken, and the second one would
    # be lost across a page boundary.
    return (pos["SampleTime"], pos["ReceivedTime"], pos["Seq"])


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
    device_ids = body.get("DeviceIds")
    if not isinstance(device_ids, list) or not device_ids:
        return _constraint(
            device_ids, "deviceIds", "Member must have length greater than or equal to 1"
        )
    if len(device_ids) > 10:
        return _constraint(
            len(device_ids), "deviceIds", "Member must have length less than or equal to 10"
        )
    rec = _trackers.get(name)
    if rec is None:
        return _not_found(name)
    # A device without a position is left out and Errors stays empty, as the
    # live service answered (probed 2026-08-26, one found + one missing).
    found = []
    for device_id in device_ids:
        device = rec["positions"].get(device_id)
        if device is not None and device.get("latest") is not None:
            found.append(_position_view(device["latest"]))
    return json_response({"DevicePositions": found, "Errors": []})


def _get_position_history(name, device_id, body):
    max_results, err = _max_results(body)
    if err is not None:
        return err
    cursor = None
    token = body.get("NextToken")
    if token:
        cursor = _decode_token(token, (float, float, int))
        if cursor is None:
            return _validation(_INVALID_TOKEN)
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
    positions = [
        pos for pos in device.get("history", [])
        if start <= pos["SampleTime"] < end
    ]
    if cursor is not None:
        positions = [pos for pos in positions if _history_key(pos) > cursor]
    page = positions[:max_results]
    result = {"DevicePositions": [_position_view(pos) for pos in page]}
    if len(positions) > max_results:
        result["NextToken"] = _encode_token(_history_key(page[-1]))
    return json_response(result)


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
        return _validation("Could not deserialize the request body as JSON.")

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

    return _validation(f"No route for {method} {path}")
