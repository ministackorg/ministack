# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
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
import urllib.error
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


def _raw_post(path, payload=None, raw=None):
    """POST over the raw wire (for shapes botocore refuses to send), signed
    with the `geo` credential scope the real SDK uses. `raw` sends the bytes
    unchanged, for a body that is not JSON at all."""
    req = urllib.request.Request(
        f"{ENDPOINT}{path}",
        data=raw if raw is not None else json.dumps(payload).encode(),
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
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


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
    # The deprecated PricingPlan "Always returns RequestBasedUsage" (API
    # reference) and was present on the live describe (probed 2026-08-26).
    assert described["PricingPlan"] == "RequestBasedUsage"
    assert "PricingPlanDataSource" not in described
    entry = next(e for e in location.list_trackers()["Entries"] if e["TrackerName"] == name)
    assert entry["PricingPlan"] == "RequestBasedUsage"
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


def test_location_tracker_name_is_ascii(location):
    """The pattern [-._\\w]+ is a Java regex service-side, where \\w is ASCII.
    The CreateTracker reference spells it out — a name may "contain only
    alphanumeric characters (A-Z, a-z, 0-9), hyphens (-), periods (.), and
    underscores (_)" — so a Unicode letter is refused, not stored."""
    for name in ("Trackeré", "トラッカー", "trk-µ"):
        with pytest.raises(ClientError) as excinfo:
            location.create_tracker(TrackerName=name)
        err = excinfo.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "[-._\\w]+" in err["Message"]
        with pytest.raises(ClientError):
            location.describe_tracker(TrackerName=name)


def test_location_path_tracker_name_validated(location):
    """The name in the URL is the same modeled ResourceName as the one in a
    request body — "Length Constraints: Minimum length of 1. Maximum length
    of 100. Pattern: [-._\\w]+" on every operation page. A name that breaks
    it is a ValidationException, not the 404 of a lookup that could never
    have matched."""
    sample = {"DeviceId": "veh-1", "SampleTime": _ts(0), "Position": [1.0, 1.0]}
    for name in ("x" * 101, "bad?name"):
        calls = (
            lambda n: location.describe_tracker(TrackerName=n),
            lambda n: location.update_tracker(TrackerName=n, Description="x"),
            lambda n: location.delete_tracker(TrackerName=n),
            lambda n: location.batch_update_device_position(
                TrackerName=n, Updates=[sample]),
            lambda n: location.batch_get_device_position(
                TrackerName=n, DeviceIds=["veh-1"]),
            lambda n: location.get_device_position(TrackerName=n, DeviceId="veh-1"),
            lambda n: location.get_device_position_history(
                TrackerName=n, DeviceId="veh-1"),
        )
        for call in calls:
            with pytest.raises(ClientError) as excinfo:
                call(name)
            err = excinfo.value.response["Error"]
            assert err["Code"] == "ValidationException", (name, err)
            assert "trackerName" in err["Message"]


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
    # "Tracker not found: X." was measured on GetDevicePosition only
    # (2026-08-26), so only that call pins the text.
    with pytest.raises(ClientError) as excinfo:
        location.get_device_position(TrackerName="no-such-tracker", DeviceId="d1")
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ResourceNotFoundException"
    assert err["Message"] == "Tracker not found: no-such-tracker."
    # The wording of the other operations is unmeasured: code only.
    with pytest.raises(ClientError) as excinfo:
        location.describe_tracker(TrackerName="no-such-tracker")
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"
    with pytest.raises(ClientError) as excinfo:
        location.update_tracker(TrackerName="no-such-tracker", Description="x")
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"
    with pytest.raises(ClientError) as excinfo:
        location.delete_tracker(TrackerName="no-such-tracker")
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"
    with pytest.raises(ClientError) as excinfo:
        location.batch_update_device_position(
            TrackerName="no-such-tracker",
            Updates=[{"DeviceId": "d1", "SampleTime": _ts(), "Position": [0.0, 0.0]}],
        )
    assert excinfo.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_location_invalid_request_beats_the_missing_tracker(location):
    """An invalid member is refused before the tracker is looked up: AWS
    validates the request in front of the operation, so a bad request naming
    a tracker that does not exist answers 400, not the 404 of the lookup."""
    missing = "no-such-tracker"
    with pytest.raises(ClientError) as excinfo:
        location.update_tracker(TrackerName=missing, PositionFiltering="Bogus")
    assert excinfo.value.response["Error"]["Code"] == "ValidationException"
    with pytest.raises(ClientError) as excinfo:
        location.batch_update_device_position(
            TrackerName=missing,
            Updates=[{
                "DeviceId": "veh-1", "SampleTime": _ts(0), "Position": [1.0, 1.0],
                "PositionProperties": {f"p{i}": "v" for i in range(5)},
            }],
        )
    assert excinfo.value.response["Error"]["Code"] == "ValidationException"
    with pytest.raises(ClientError) as excinfo:
        location.batch_get_device_position(
            TrackerName=missing, DeviceIds=[f"d{i}" for i in range(11)]
        )
    assert excinfo.value.response["Error"]["Code"] == "ValidationException"
    with pytest.raises(ClientError) as excinfo:
        location.get_device_position_history(
            TrackerName=missing, DeviceId="veh-1", MaxResults=101
        )
    assert excinfo.value.response["Error"]["Code"] == "ValidationException"


def test_location_update_tracker_round_trip(location):
    name = _uid()
    location.create_tracker(TrackerName=name, Description="v1")
    updated = location.update_tracker(
        TrackerName=name,
        Description="v2",
        PositionFiltering="DistanceBased",
        EventBridgeEnabled=True,
        KmsKeyEnableGeospatialQueries=True,
    )
    assert updated["TrackerName"] == name
    assert updated["TrackerArn"].endswith(f":tracker/{name}")
    assert isinstance(updated["UpdateTime"], datetime.datetime)

    described = location.describe_tracker(TrackerName=name)
    assert described["Description"] == "v2"
    assert described["PositionFiltering"] == "DistanceBased"
    assert described["EventBridgeEnabled"] is True
    assert described["KmsKeyEnableGeospatialQueries"] is True
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


def test_location_time_based_filtering_stores_one_sample_per_30_seconds(location):
    """TimeBased is the default: "If your update frequency is more often than
    30 seconds, only one update per 30 seconds is stored for each unique
    device ID"."""
    name = _uid()
    location.create_tracker(TrackerName=name)
    assert location.describe_tracker(
        TrackerName=name)["PositionFiltering"] == "TimeBased"

    for offset in (0, 10, 29, 30, 61):
        resp = location.batch_update_device_position(
            TrackerName=name,
            Updates=[{"DeviceId": "veh-1", "SampleTime": _ts(offset),
                      "Position": [11.5761 + offset / 1000.0, 48.1371]}],
        )
        # A filtered update is not an error: it is simply not stored.
        assert resp["Errors"] == []

    history = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1")["DevicePositions"]
    assert [p["SampleTime"] for p in history] == [_ts(0), _ts(30), _ts(61)]
    location.delete_tracker(TrackerName=name)


def test_location_distance_based_filtering_ignores_moves_under_30_m(location):
    """DistanceBased: "If the device has moved less than 30 m (98.4 ft),
    location updates are ignored"."""
    name = _uid()
    location.create_tracker(TrackerName=name, PositionFiltering="DistanceBased")
    # 0.0001 degrees of latitude is about 11 m, 0.0005 about 56 m.
    for offset, lat in ((0, 48.1371), (60, 48.1372), (120, 48.1376)):
        location.batch_update_device_position(
            TrackerName=name,
            Updates=[{"DeviceId": "veh-1", "SampleTime": _ts(offset),
                      "Position": [11.5761, lat]}],
        )

    history = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1")["DevicePositions"]
    assert [p["SampleTime"] for p in history] == [_ts(0), _ts(120)]
    assert [p["Position"][1] for p in history] == [48.1371, 48.1376]
    location.delete_tracker(TrackerName=name)


def test_location_accuracy_based_filtering_ignores_moves_under_the_accuracy(location):
    """AccuracyBased, with the reference's own example: two consecutive
    updates of 5 m and 10 m horizontal accuracy ignore the second when the
    device has moved less than 15 m."""
    name = _uid()
    location.create_tracker(TrackerName=name, PositionFiltering="AccuracyBased")
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[{"DeviceId": "veh-1", "SampleTime": _ts(0),
                  "Position": [11.5761, 48.1371],
                  "Accuracy": {"Horizontal": 5.0}}],
    )
    # About 11 m north, under the 15 m the two accuracies add up to.
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[{"DeviceId": "veh-1", "SampleTime": _ts(60),
                  "Position": [11.5761, 48.1372],
                  "Accuracy": {"Horizontal": 10.0}}],
    )
    # About 56 m north, above it.
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[{"DeviceId": "veh-1", "SampleTime": _ts(120),
                  "Position": [11.5761, 48.1376],
                  "Accuracy": {"Horizontal": 10.0}}],
    )

    history = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1")["DevicePositions"]
    assert [p["SampleTime"] for p in history] == [_ts(0), _ts(120)]
    location.delete_tracker(TrackerName=name)


def test_location_filtering_is_per_device_and_survives_an_update_tracker(location):
    """The 30 second window is per unique device id, and UpdateTracker can
    switch the mode of an existing tracker."""
    name = _uid()
    location.create_tracker(TrackerName=name)
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[
            {"DeviceId": "veh-1", "SampleTime": _ts(0), "Position": [11.5761, 48.1371]},
            {"DeviceId": "veh-2", "SampleTime": _ts(1), "Position": [11.5761, 48.1371]},
        ],
    )
    for device in ("veh-1", "veh-2"):
        assert location.get_device_position(
            TrackerName=name, DeviceId=device)["SampleTime"] in (_ts(0), _ts(1))

    location.update_tracker(TrackerName=name, PositionFiltering="DistanceBased")
    assert location.describe_tracker(
        TrackerName=name)["PositionFiltering"] == "DistanceBased"
    # One second later but 56 m away: TimeBased would have dropped it,
    # DistanceBased keeps it.
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[{"DeviceId": "veh-1", "SampleTime": _ts(1),
                  "Position": [11.5761, 48.1376]}],
    )
    history = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1")["DevicePositions"]
    assert len(history) == 2
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


# The history tests below need every sample they send to be stored. All three
# filtering modes drop something, and the only configuration that keeps every
# sample is AccuracyBased on updates that carry no Accuracy: the reference
# ignores an update when the device "has moved less than the measured
# accuracy", and nothing measured it. TimeBased, the default, would collapse
# samples closer together than 30 seconds.
_KEEP_EVERY_SAMPLE = {"PositionFiltering": "AccuracyBased"}


def test_location_position_history_in_order(location):
    name = _uid()
    location.create_tracker(TrackerName=name, **_KEEP_EVERY_SAMPLE)
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


def test_location_batch_get_omits_missing_devices(location):
    """A device without a position is left out and Errors stays empty: the
    live service's answer to one found + one missing (probed 2026-08-26)."""
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
    assert resp["Errors"] == []
    location.delete_tracker(TrackerName=name)


def test_location_batch_update_rejects_malformed_entries(location):
    """An entry missing a required member fails the whole request with a
    ValidationException (the model marks DeviceId, SampleTime and Position
    required); nothing of the batch is stored. botocore refuses to send such
    an entry, so this goes over the raw wire, which also exercises the
    numeric epoch SampleTime spelling the lenient timestamp parser accepts
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
    assert status == 400
    assert body["__type"].endswith("ValidationException") or body.get("code") == "ValidationException" \
        or "ValidationException" in json.dumps(body)
    assert "updates.2.member.position" in json.dumps(body)
    with pytest.raises(ClientError):
        location.get_device_position(TrackerName=name, DeviceId="veh-ok")

    # The epoch spelling alone is accepted.
    status, body = _raw_post(
        f"/tracking/v0/trackers/{name}/positions",
        {"Updates": [{"DeviceId": "veh-ok", "SampleTime": _ts(0).timestamp(),
                      "Position": [11.0, 48.0]}]},
    )
    assert status == 200 and body["Errors"] == []
    pos = location.get_device_position(TrackerName=name, DeviceId="veh-ok")
    assert pos["SampleTime"] == _ts(0)

    # Batch size: 1..10 entries, checked before any entry is stored.
    for updates in ([], [{"DeviceId": f"v{i}", "SampleTime": _ts(0).timestamp(),
                          "Position": [1.0, 1.0]} for i in range(11)]):
        status, body = _raw_post(f"/tracking/v0/trackers/{name}/positions",
                                 {"Updates": updates})
        assert status == 400, body
    with pytest.raises(ClientError):
        location.get_device_position(TrackerName=name, DeviceId="v0")
    location.delete_tracker(TrackerName=name)


def test_location_batch_update_rejects_out_of_range_positions(location):
    """Out-of-range coordinates become per-entry Errors items while the call
    itself stays 200 — with the live service's wording for latitude (probed
    2026-08-26)."""
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
    # Only the latitude text was measured; the longitude wording is the
    # emulator's, so only the code is pinned.
    assert errors["veh-lon"]["Error"]["Code"] == "ValidationError"
    assert isinstance(errors["veh-lat"]["SampleTime"], datetime.datetime)
    # The in-range sibling entry still landed; the rejected ones did not.
    assert location.get_device_position(
        TrackerName=name, DeviceId="veh-ok"
    )["Position"] == [11.0, 48.0]
    with pytest.raises(ClientError):
        location.get_device_position(TrackerName=name, DeviceId="veh-lat")
    location.delete_tracker(TrackerName=name)


def test_location_history_bounded_to_newest_100(location):
    """Per-device history keeps the newest 100 samples. A stated divergence:
    the service retains 30 days of positions, MiniStack the newest 100 per
    device."""
    name = _uid()
    location.create_tracker(TrackerName=name, **_KEEP_EVERY_SAMPLE)
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
    location.create_tracker(TrackerName=name, **_KEEP_EVERY_SAMPLE)
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


# ---------------------------------------------------------------------------
# Request validation and paging
# ---------------------------------------------------------------------------

def test_location_tracker_settings_validated(location):
    """PositionFiltering is an enum and PricingPlan may only be
    RequestBasedUsage (API reference, CreateTracker and UpdateTracker);
    botocore does not check either client-side."""
    name = _uid()
    with pytest.raises(ClientError) as excinfo:
        location.create_tracker(TrackerName=name, PositionFiltering="Bogus")
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "positionFiltering" in err["Message"]
    # MobileAssetTracking is still in the model's enum but no longer allowed,
    # so the message says that instead of naming a one-member enum set. The
    # wording is the emulator's, not measured.
    with pytest.raises(ClientError) as excinfo:
        location.create_tracker(TrackerName=name, PricingPlan="MobileAssetTracking")
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "no longer accepted" in err["Message"]
    assert "RequestBasedUsage" in err["Message"]
    with pytest.raises(ClientError):
        location.describe_tracker(TrackerName=name)

    location.create_tracker(TrackerName=name, PricingPlan="RequestBasedUsage")
    with pytest.raises(ClientError) as excinfo:
        location.update_tracker(TrackerName=name, PositionFiltering="Bogus")
    assert excinfo.value.response["Error"]["Code"] == "ValidationException"
    assert location.describe_tracker(TrackerName=name)["PositionFiltering"] == "TimeBased"
    location.delete_tracker(TrackerName=name)


def test_location_description_length_validated(location):
    """Description is 0..1000 (CreateTracker reference). It is refused before
    the tracker is created. The message wording is unmeasured."""
    name = _uid()
    with pytest.raises(ClientError) as excinfo:
        location.create_tracker(TrackerName=name, Description="x" * 1001)
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "description" in err["Message"]
    with pytest.raises(ClientError):
        location.describe_tracker(TrackerName=name)

    # The bound itself passes, and UpdateTracker checks it too.
    location.create_tracker(TrackerName=name, Description="x" * 1000)
    with pytest.raises(ClientError) as excinfo:
        location.update_tracker(TrackerName=name, Description="x" * 1001)
    assert excinfo.value.response["Error"]["Code"] == "ValidationException"
    assert location.describe_tracker(TrackerName=name)["Description"] == "x" * 1000
    location.delete_tracker(TrackerName=name)


def test_location_kms_key_id_length_validated(location):
    """KmsKeyId is 1..2048 (CreateTracker reference). botocore enforces the
    minimum lengths client-side but no maximum, so the empty value goes over
    the raw wire. Wording unmeasured."""
    name = _uid()
    with pytest.raises(ClientError) as excinfo:
        location.create_tracker(TrackerName=name, KmsKeyId="k" * 2049)
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "kmsKeyId" in err["Message"]
    status, body = _raw_post(
        "/tracking/v0/trackers", {"TrackerName": name, "KmsKeyId": ""}
    )
    assert status == 400, body
    assert "kmsKeyId" in json.dumps(body)
    with pytest.raises(ClientError):
        location.describe_tracker(TrackerName=name)

    location.create_tracker(TrackerName=name, KmsKeyId="k" * 2048)
    assert location.describe_tracker(TrackerName=name)["KmsKeyId"] == "k" * 2048
    location.delete_tracker(TrackerName=name)


def test_location_tags_validated(location):
    """Tags: at most 50 entries, keys 1..128, values 0..256, both on the
    pattern ([\\p{L}\\p{Z}\\p{N}_.,:/=+\\-@]*) (CreateTracker reference).
    Wordings unmeasured."""
    name = _uid()
    cases = (
        {f"k{i}": "v" for i in range(51)},  # more than 50 entries
        {"k" * 129: "v"},                   # key longer than 128
        {"k?": "v"},                        # key off the pattern
        {"k": "v" * 257},                   # value longer than 256
        {"k": "v#"},                        # value off the pattern
    )
    for tags in cases:
        with pytest.raises(ClientError) as excinfo:
            location.create_tracker(TrackerName=name, Tags=tags)
        err = excinfo.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "tags" in err["Message"]
        with pytest.raises(ClientError):
            location.describe_tracker(TrackerName=name)

    # A key shorter than 1 is a minimum, which botocore refuses to send.
    status, body = _raw_post(
        "/tracking/v0/trackers", {"TrackerName": name, "Tags": {"": "v"}}
    )
    assert status == 400, body
    assert "tags" in json.dumps(body)
    with pytest.raises(ClientError):
        location.describe_tracker(TrackerName=name)

    # The bounds themselves pass: 50 entries, a 128-character key, an empty
    # value, and the separators and literals the pattern allows.
    tags = {f"k{i}": "v" for i in range(48)}
    tags["k" * 128] = ""
    tags["fleet name"] = "iot-rocket.local/eu=1,2:3+4@5_6"
    location.create_tracker(TrackerName=name, Tags=tags)
    assert location.describe_tracker(TrackerName=name)["Tags"] == tags
    location.delete_tracker(TrackerName=name)


def test_location_position_properties_validated(location):
    """PositionProperties: at most 4 entries, keys 1..20, values 1..150
    (DevicePositionUpdate reference). The whole batch is refused before
    anything is stored. Wordings unmeasured."""
    name = _uid()
    location.create_tracker(TrackerName=name)
    cases = (
        {f"p{i}": "v" for i in range(5)},  # more than 4 entries
        {"p" * 21: "v"},                   # key longer than 20
        {"p": "v" * 151},                  # value longer than 150
    )
    for properties in cases:
        with pytest.raises(ClientError) as excinfo:
            location.batch_update_device_position(
                TrackerName=name,
                Updates=[{
                    "DeviceId": "veh-1", "SampleTime": _ts(0),
                    "Position": [1.0, 1.0], "PositionProperties": properties,
                }],
            )
        err = excinfo.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "positionProperties" in err["Message"]
        with pytest.raises(ClientError):
            location.get_device_position(TrackerName=name, DeviceId="veh-1")

    # The minimum lengths are the ones botocore refuses to send: raw wire.
    for properties in ({"": "v"}, {"p": ""}):
        status, body = _raw_post(
            f"/tracking/v0/trackers/{name}/positions",
            {"Updates": [{"DeviceId": "veh-1", "SampleTime": _ts(0).timestamp(),
                          "Position": [1.0, 1.0], "PositionProperties": properties}]},
        )
        assert status == 400, body
        assert "positionProperties" in json.dumps(body)
        with pytest.raises(ClientError):
            location.get_device_position(TrackerName=name, DeviceId="veh-1")

    properties = {"p" * 20: "v" * 150, "a": "1", "b": "2", "c": "3"}
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[{"DeviceId": "veh-1", "SampleTime": _ts(0),
                  "Position": [1.0, 1.0], "PositionProperties": properties}],
    )
    stored = location.get_device_position(TrackerName=name, DeviceId="veh-1")
    assert stored["PositionProperties"] == properties
    location.delete_tracker(TrackerName=name)


def test_location_batch_get_device_ids_bounded(location):
    name = _uid()
    location.create_tracker(TrackerName=name)
    with pytest.raises(ClientError) as excinfo:
        location.batch_get_device_position(
            TrackerName=name, DeviceIds=[f"d{i}" for i in range(11)]
        )
    err = excinfo.value.response["Error"]
    assert err["Code"] == "ValidationException"
    assert "deviceIds" in err["Message"]
    location.delete_tracker(TrackerName=name)


def test_location_list_trackers_pages(location):
    """ListTrackers pages at MaxResults with a NextToken; MaxResults outside
    1..100 and a foreign token are ValidationExceptions."""
    prefix = _uid("page")
    names = sorted(f"{prefix}-{i}" for i in range(3))
    for name in names:
        location.create_tracker(TrackerName=name)
    try:
        seen, token, pages = [], None, 0
        while True:
            kwargs = {"MaxResults": 2}
            if token:
                kwargs["NextToken"] = token
            resp = location.list_trackers(**kwargs)
            assert len(resp["Entries"]) <= 2
            seen.extend(e["TrackerName"] for e in resp["Entries"])
            pages += 1
            token = resp.get("NextToken")
            if not token:
                break
            assert pages < 500
        assert len(seen) == len(set(seen))
        assert [n for n in seen if n.startswith(prefix)] == names

        with pytest.raises(ClientError) as excinfo:
            location.list_trackers(MaxResults=500)
        err = excinfo.value.response["Error"]
        assert err["Code"] == "ValidationException"
        assert "maxResults" in err["Message"]
        status, body = _raw_post("/tracking/v0/list-trackers", {"MaxResults": 0})
        assert status == 400
        with pytest.raises(ClientError) as excinfo:
            location.list_trackers(NextToken="not-a-token")
        assert excinfo.value.response["Error"]["Code"] == "ValidationException"
    finally:
        for name in names:
            location.delete_tracker(TrackerName=name)


def test_location_position_history_pages(location):
    """GetDevicePositionHistory pages at MaxResults, ascending across pages,
    and refuses a foreign token."""
    name = _uid()
    location.create_tracker(TrackerName=name, **_KEEP_EVERY_SAMPLE)
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[{"DeviceId": "veh-1", "SampleTime": _ts(i), "Position": [1.0, 1.0]}
                 for i in range(5)],
    )
    first = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1", MaxResults=2)
    assert [p["SampleTime"] for p in first["DevicePositions"]] == [_ts(0), _ts(1)]
    second = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1", MaxResults=2, NextToken=first["NextToken"])
    assert [p["SampleTime"] for p in second["DevicePositions"]] == [_ts(2), _ts(3)]
    third = location.get_device_position_history(
        TrackerName=name, DeviceId="veh-1", MaxResults=2, NextToken=second["NextToken"])
    assert [p["SampleTime"] for p in third["DevicePositions"]] == [_ts(4)]
    assert "NextToken" not in third
    with pytest.raises(ClientError) as excinfo:
        location.get_device_position_history(
            TrackerName=name, DeviceId="veh-1", NextToken="not-a-token")
    assert excinfo.value.response["Error"]["Code"] == "ValidationException"
    with pytest.raises(ClientError):
        location.get_device_position_history(
            TrackerName=name, DeviceId="veh-1", MaxResults=101)
    location.delete_tracker(TrackerName=name)


def test_location_position_history_pages_over_equal_timestamps(location):
    """Two samples of one device can share SampleTime and ReceivedTime: they
    are sent in one batch, and the batch is received once. A NextToken resumes
    with a strict `>`, so a sort key of the two timestamps alone would drop the
    second one whenever the page boundary falls between them."""
    name = _uid()
    location.create_tracker(TrackerName=name, **_KEEP_EVERY_SAMPLE)
    location.batch_update_device_position(
        TrackerName=name,
        Updates=[
            {"DeviceId": "veh-1", "SampleTime": _ts(0), "Position": [1.0, 1.0]},
            {"DeviceId": "veh-1", "SampleTime": _ts(0), "Position": [2.0, 2.0]},
            {"DeviceId": "veh-1", "SampleTime": _ts(1), "Position": [3.0, 3.0]},
        ],
    )
    seen, token, pages = [], None, 0
    while True:
        kwargs = {"TrackerName": name, "DeviceId": "veh-1", "MaxResults": 1}
        if token:
            kwargs["NextToken"] = token
        resp = location.get_device_position_history(**kwargs)
        seen.extend(resp["DevicePositions"])
        pages += 1
        token = resp.get("NextToken")
        if not token:
            break
        assert pages < 10
    assert [p["SampleTime"] for p in seen] == [_ts(0), _ts(0), _ts(1)]
    # The two tied samples really do share both timestamps, so only the
    # sequence number separates them; their order between each other is not
    # asserted.
    assert seen[0]["ReceivedTime"] == seen[1]["ReceivedTime"]
    assert {tuple(p["Position"]) for p in seen[:2]} == {(1.0, 1.0), (2.0, 2.0)}
    location.delete_tracker(TrackerName=name)


def test_location_raw_wire_requests_rejected(location):
    """Request shapes botocore refuses to send, over the raw wire. Each is a
    400 and nothing is stored. The wordings are unmeasured."""
    name = _uid()
    location.create_tracker(TrackerName=name)
    positions = f"/tracking/v0/trackers/{name}/positions"

    # MaxResults is a modeled integer.
    status, body = _raw_post("/tracking/v0/list-trackers", {"MaxResults": 2.5})
    assert status == 400, body
    assert "MaxResults" in json.dumps(body)

    # An Updates entry that is not a structure.
    status, body = _raw_post(positions, {"Updates": ["veh-1"]})
    assert status == 400, body
    assert "updates.1.member" in json.dumps(body)

    # An Updates entry without the required DeviceId.
    status, body = _raw_post(positions, {"Updates": [
        {"SampleTime": _ts(0).timestamp(), "Position": [1.0, 1.0]}]})
    assert status == 400, body
    assert "updates.1.member.deviceId" in json.dumps(body)

    # A body that is not JSON at all.
    status, body = _raw_post(positions, raw=b"{not json")
    assert status == 400, body
    assert "deserialize" in json.dumps(body)

    # A path that maps to no operation.
    status, body = _raw_post(f"/tracking/v0/trackers/{name}/bogus", {})
    assert status == 400, body
    assert "No route for" in json.dumps(body)

    with pytest.raises(ClientError):
        location.get_device_position(TrackerName=name, DeviceId="veh-1")
    location.delete_tracker(TrackerName=name)
