"""
Integration tests for the Amazon Location Service emulator (trackers only).

The SDK signs these requests with credential scope `geo` (botocore
signingName), not `location` — the tests going through the boto3 client also
exercise that routing path, since the `location` fixture disables the modeled
`cp.tracking.` / `tracking.` host-prefix injection.
"""
import datetime
import json
import os
import urllib.request
import uuid

import pytest
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")


def _uid(prefix="trk"):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# Millisecond-aligned (the wire form truncates to ms) and one hour in the
# past, so samples land inside GetDevicePositionHistory's default window
# (the 24 hours up to now) while offsets stay deterministic within a run.
_TS_BASE = datetime.datetime.now(datetime.timezone.utc).replace(
    microsecond=0
) - datetime.timedelta(hours=1)


def _ts(offset_seconds=0):
    return _TS_BASE + datetime.timedelta(seconds=offset_seconds)


def _raw_post(path, payload):
    """POST over the raw wire (for shapes botocore refuses to send), signed
    with the `geo` credential scope the real SDK uses."""
    req = urllib.request.Request(
        f"{ENDPOINT}{path}",
        data=json.dumps(payload).encode(),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                "Credential=test/20260826/us-east-1/geo/aws4_request, "
                "SignedHeaders=host, Signature=fake"
            ),
        },
    )
    with urllib.request.urlopen(req) as resp:
        return resp.status, json.loads(resp.read() or b"{}")


# ---------------------------------------------------------------------------
# Tracker CRUD
# ---------------------------------------------------------------------------

def test_location_tracker_crud_round_trip(location):
    name = _uid()
    created = location.create_tracker(
        TrackerName=name,
        Description="fleet trackers",
        PositionFiltering="TimeBased",
        EventBridgeEnabled=False,
        Tags={"team": "iot"},
    )
    assert created["TrackerName"] == name
    assert created["TrackerArn"].startswith("arn:aws:geo:us-east-1:")
    assert created["TrackerArn"].endswith(f":tracker/{name}")
    # Timestamps must be wire-correct: botocore parses them into datetimes.
    assert isinstance(created["CreateTime"], datetime.datetime)

    described = location.describe_tracker(TrackerName=name)
    assert described["TrackerName"] == name
    assert described["TrackerArn"] == created["TrackerArn"]
    assert described["Description"] == "fleet trackers"
    assert described["PositionFiltering"] == "TimeBased"
    assert described["EventBridgeEnabled"] is False
    assert described["Tags"] == {"team": "iot"}
    assert isinstance(described["CreateTime"], datetime.datetime)
    assert isinstance(described["UpdateTime"], datetime.datetime)

    entries = location.list_trackers()["Entries"]
    entry = next(e for e in entries if e["TrackerName"] == name)
    assert entry["Description"] == "fleet trackers"
    assert isinstance(entry["CreateTime"], datetime.datetime)
    assert isinstance(entry["UpdateTime"], datetime.datetime)

    location.delete_tracker(TrackerName=name)
    with pytest.raises(ClientError) as excinfo:
        location.describe_tracker(TrackerName=name)
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_location_describe_materializes_defaults(location):
    name = _uid()
    location.create_tracker(TrackerName=name)
    # The live service (probed 2026-08-26) materializes these members even
    # when CreateTracker omitted them; Description is a required response
    # member — empty string when unset.
    described = location.describe_tracker(TrackerName=name)
    assert described["Description"] == ""
    assert described["Tags"] == {}
    assert described["EventBridgeEnabled"] is False
    assert described["PositionFiltering"] == "TimeBased"
    location.delete_tracker(TrackerName=name)


def test_location_tracker_name_validated(location):
    # A name with "/" would be unaddressable via the path routes.
    with pytest.raises(ClientError) as excinfo:
        location.create_tracker(TrackerName="bad/name")
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "trackerName" in err["Message"]
    assert "[-._\\w]+" in err["Message"]
    with pytest.raises(ClientError) as excinfo:
        location.create_tracker(TrackerName="x" * 101)
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "length less than or equal to 100" in err["Message"]


def test_location_duplicate_tracker_conflicts(location):
    name = _uid()
    location.create_tracker(TrackerName=name)
    with pytest.raises(ClientError) as excinfo:
        location.create_tracker(TrackerName=name)
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ConflictException"
    # Live-service wording, probed 2026-08-26.
    assert err["Message"] == f"Tracker already exists: {name}."
    location.delete_tracker(TrackerName=name)


def test_location_unknown_tracker_operations_404(location):
    with pytest.raises(ClientError) as excinfo:
        location.describe_tracker(TrackerName="no-such-tracker")
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ResourceNotFoundException"
    # Live-service wording, probed 2026-08-26.
    assert err["Message"] == "Tracker not found: no-such-tracker."
    with pytest.raises(ClientError) as excinfo:
        location.delete_tracker(TrackerName="no-such-tracker")
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"
    with pytest.raises(ClientError) as excinfo:
        location.batch_update_device_position(
            TrackerName="no-such-tracker",
            Updates=[{"DeviceId": "d1", "SampleTime": _ts(), "Position": [0.0, 0.0]}],
        )
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_location_update_tracker_round_trip(location):
    name = _uid()
    location.create_tracker(TrackerName=name, Description="v1")
    updated = location.update_tracker(
        TrackerName=name,
        Description="v2",
        PositionFiltering="DistanceBased",
        EventBridgeEnabled=True,
    )
    assert updated["TrackerName"] == name
    assert updated["TrackerArn"].endswith(f":tracker/{name}")
    assert isinstance(updated["UpdateTime"], datetime.datetime)

    described = location.describe_tracker(TrackerName=name)
    assert described["Description"] == "v2"
    assert described["PositionFiltering"] == "DistanceBased"
    assert described["EventBridgeEnabled"] is True
    assert described["UpdateTime"] >= described["CreateTime"]

    # Omitted members keep their values (PATCH semantics, not replacement).
    location.update_tracker(TrackerName=name, Description="v3")
    described = location.describe_tracker(TrackerName=name)
    assert described["Description"] == "v3"
    assert described["PositionFiltering"] == "DistanceBased"

    with pytest.raises(ClientError) as excinfo:
        location.update_tracker(TrackerName="no-such-tracker", Description="x")
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"
    location.delete_tracker(TrackerName=name)


# ---------------------------------------------------------------------------
# Device positions
# ---------------------------------------------------------------------------

def test_location_batch_update_then_get_position(location):
    name = _uid()
    location.create_tracker(TrackerName=name)
    resp = location.batch_update_device_position(
        TrackerName=name,
        Updates=[{
            "DeviceId": "veh-1",
            "SampleTime": _ts(0),
            "Position": [11.5761, 48.1371],
            "Accuracy": {"Horizontal": 5.0},
            "PositionProperties": {"speed": "12.5"},
        }],
    )
    assert resp["Errors"] == []

    pos = location.get_device_position(TrackerName=name, DeviceId="veh-1")
    assert pos["DeviceId"] == "veh-1"
    # Position round-trips as the modeled [lon, lat] double list.
    assert pos["Position"] == [11.5761, 48.1371]
    assert pos["PositionProperties"] == {"speed": "12.5"}
    assert pos["Accuracy"] == {"Horizontal": 5.0}
    # Timestamps are wire-correct iso8601: boto3 parses them into datetimes,
    # and SampleTime survives the round-trip exactly.
    assert pos["SampleTime"] == _ts(0)
    assert isinstance(pos["ReceivedTime"], datetime.datetime)
    location.delete_tracker(TrackerName=name)


def test_location_get_position_unknown_device_404(location):
    name = _uid()
    location.create_tracker(TrackerName=name)
    with pytest.raises(ClientError) as excinfo:
        location.get_device_position(TrackerName=name, DeviceId="never-reported")
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ResourceNotFoundException"
    # Live-service wording, probed 2026-08-26.
    assert err["Message"] == "records not found for given deviceId"
    location.delete_tracker(TrackerName=name)


def test_location_position_history_in_order(location):
    name = _uid()
    location.create_tracker(TrackerName=name)
    # Delivered out of order — history must come back ascending by SampleTime.
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[
            {"DeviceId": "veh-1", "SampleTime": _ts(30), "Position": [3.0, 3.0]},
            {"DeviceId": "veh-1", "SampleTime": _ts(10), "Position": [1.0, 1.0]},
        ],
    )
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[{"DeviceId": "veh-1", "SampleTime": _ts(20), "Position": [2.0, 2.0]}],
    )

    history = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1"
    )["DevicePositions"]
    assert [p["SampleTime"] for p in history] == [_ts(10), _ts(20), _ts(30)]

    # The latest position is the newest sample, not the last write.
    latest = location.get_device_position(TrackerName=name, DeviceId="veh-1")
    assert latest["SampleTime"] == _ts(30)
    assert latest["Position"] == [3.0, 3.0]

    # Start is inclusive, End exclusive.
    window = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1",
        StartTimeInclusive=_ts(10), EndTimeExclusive=_ts(30),
    )["DevicePositions"]
    assert [p["SampleTime"] for p in window] == [_ts(10), _ts(20)]
    location.delete_tracker(TrackerName=name)


def test_location_batch_get_mixes_found_and_missing(location):
    name = _uid()
    location.create_tracker(TrackerName=name)
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[
            {"DeviceId": "veh-1", "SampleTime": _ts(0), "Position": [1.0, 1.0]},
            {"DeviceId": "veh-2", "SampleTime": _ts(0), "Position": [2.0, 2.0]},
        ],
    )
    resp = location.batch_get_device_position(
        TrackerName=name, DeviceIds=["veh-1", "ghost", "veh-2"]
    )
    assert {p["DeviceId"] for p in resp["DevicePositions"]} == {"veh-1", "veh-2"}
    assert len(resp["Errors"]) == 1
    assert resp["Errors"][0]["DeviceId"] == "ghost"
    assert resp["Errors"][0]["Error"]["Code"] == "ResourceNotFoundError"
    location.delete_tracker(TrackerName=name)


def test_location_batch_update_reports_malformed_entries(location):
    """Per-entry validation errors. botocore refuses to send an update missing
    a required member, so this goes over the raw wire — which also exercises
    the numeric epoch SampleTime spelling the lenient timestamp parser accepts
    alongside the model's iso8601 strings."""
    name = _uid()
    location.create_tracker(TrackerName=name)
    status, body = _raw_post(
        f"/tracking/v0/trackers/{name}/positions",
        {"Updates": [
            {"DeviceId": "veh-ok", "SampleTime": _ts(0).timestamp(),
             "Position": [11.0, 48.0]},
            {"DeviceId": "veh-bad", "SampleTime": _ts(0).timestamp()},
        ]},
    )
    assert status == 200
    assert len(body["Errors"]) == 1
    assert body["Errors"][0]["DeviceId"] == "veh-bad"
    assert body["Errors"][0]["Error"]["Code"] == "ValidationError"
    # The well-formed sibling entry still landed.
    pos = location.get_device_position(TrackerName=name, DeviceId="veh-ok")
    assert pos["Position"] == [11.0, 48.0]
    assert pos["SampleTime"] == _ts(0)
    location.delete_tracker(TrackerName=name)


def test_location_batch_update_rejects_out_of_range_positions(location):
    """Out-of-range coordinates become per-entry Errors items while the call
    itself stays 200 — with the live service's wording (probed 2026-08-26)."""
    name = _uid()
    location.create_tracker(TrackerName=name)
    resp = location.batch_update_device_position(
        TrackerName=name,
        Updates=[
            {"DeviceId": "veh-ok", "SampleTime": _ts(0), "Position": [11.0, 48.0]},
            {"DeviceId": "veh-lat", "SampleTime": _ts(0), "Position": [0.0, 91.0]},
            {"DeviceId": "veh-lon", "SampleTime": _ts(0), "Position": [-181.0, 0.0]},
        ],
    )
    errors = {e["DeviceId"]: e for e in resp["Errors"]}
    assert set(errors) == {"veh-lat", "veh-lon"}
    assert errors["veh-lat"]["Error"] == {
        "Code": "ValidationError",
        "Message": "latitude must be between -90 and 90 degrees",
    }
    assert errors["veh-lon"]["Error"] == {
        "Code": "ValidationError",
        "Message": "longitude must be between -180 and 180 degrees",
    }
    assert isinstance(errors["veh-lat"]["SampleTime"], datetime.datetime)
    # The in-range sibling entry still landed; the rejected ones did not.
    assert location.get_device_position(
        TrackerName=name, DeviceId="veh-ok"
    )["Position"] == [11.0, 48.0]
    with pytest.raises(ClientError):
        location.get_device_position(TrackerName=name, DeviceId="veh-lat")
    location.delete_tracker(TrackerName=name)


def test_location_history_bounded_to_newest_100(location):
    """Per-device history keeps the newest 100 samples (a stated divergence:
    real TimeBased filtering stores at most one position per 30 s per device
    and retains 30 days; MiniStack keeps every sample but only the newest
    100)."""
    name = _uid()
    location.create_tracker(TrackerName=name)
    # The modeled Updates list caps at 10 entries per call.
    for chunk_start in range(0, 105, 10):
        location.batch_update_device_position(
            TrackerName=name,
            Updates=[
                {"DeviceId": "veh-1", "SampleTime": _ts(i), "Position": [1.0, 1.0]}
                for i in range(chunk_start, min(chunk_start + 10, 105))
            ],
        )
    history = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1"
    )["DevicePositions"]
    assert len(history) == 100
    # The oldest five were dropped.
    assert history[0]["SampleTime"] == _ts(5)
    assert history[-1]["SampleTime"] == _ts(104)
    location.delete_tracker(TrackerName=name)


def test_location_history_default_window_is_last_24_hours(location):
    """With StartTimeInclusive/EndTimeExclusive omitted, the documented
    defaults apply: the 24 hours up to now."""
    name = _uid()
    location.create_tracker(TrackerName=name)
    old = _ts(-24 * 3600)  # 25 hours ago — outside the default window
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[
            {"DeviceId": "veh-1", "SampleTime": old, "Position": [1.0, 1.0]},
            {"DeviceId": "veh-1", "SampleTime": _ts(0), "Position": [2.0, 2.0]},
        ],
    )
    history = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1"
    )["DevicePositions"]
    assert [p["SampleTime"] for p in history] == [_ts(0)]
    # An explicit window reaches the older sample.
    history = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1",
        StartTimeInclusive=_ts(-26 * 3600), EndTimeExclusive=_ts(1),
    )["DevicePositions"]
    assert [p["SampleTime"] for p in history] == [old, _ts(0)]
    location.delete_tracker(TrackerName=name)
