# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""IoT Wireless GetPositionEstimate tests.

The response is NOT a JSON envelope: the output shape declares
``"payload": "GeoJsonPayload"``, so the HTTP body is the raw GeoJSON blob and
boto3 hands back a ``StreamingBody`` under the ``GeoJsonPayload`` key. Every
happy-path test here reads that stream and parses it, pinning the wire shape.

Happy-path addresses must be globally routable: the resolver mirrors the
real service's refusal of an address its geo database cannot place (measured
with TEST-NET 203.0.113.7) by refusing every non-global address, so the
documentation ranges (TEST-NET, 10/8, ::1) are exactly the ones that DON'T
resolve.
"""
import json
import urllib.error
import urllib.request
from datetime import datetime, timezone

import pytest
from botocore.exceptions import ClientError
from conftest import ENDPOINT

# One instant, so the rendered `properties.timestamp` is a fixed string.
_TIMESTAMP = datetime(2026, 8, 26, 14, 6, 11, tzinfo=timezone.utc)
_TIMESTAMP_RENDERED = "2026-08-26T14:06:11Z"

# The five members that are accepted but never resolve the position, with the
# sample values of the AWS developer guide's payload. `CellTowers` is an
# object keyed by radio type, not an array.
_NON_IP_MEMBERS = {
    "WiFiAccessPoints": [{"MacAddress": "A0:EC:F9:1E:32:C1", "Rss": -75}],
    "CellTowers": {
        "Gsm": [{"Mcc": 262, "Mnc": 1, "Lac": 5126, "GeranCid": 16504}]
    },
    "Gnss": {"Payload": "8295D1B1B1B1B1B1B1"},
    "Timestamp": _TIMESTAMP,
    "AdvancedConfiguration": {"WiFiCellular": {"ConfidencePercent": 90}},
}


def _raw(path, body, method="POST"):
    """Unsigned raw HTTP (no Authorization header, like curl); returns
    (status, content-type, body bytes)."""
    req = urllib.request.Request(
        f"{ENDPOINT}{path}", data=body, method=method,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.headers.get("content-type"), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("content-type"), e.read()


def _estimate_bytes(client, ip):
    response = client.get_position_estimate(Ip={"IpAddress": ip})
    return response["GeoJsonPayload"].read()


def test_iotwireless_position_estimate_is_geojson_point_blob(iotwireless):
    """The streamed blob parses as a GeoJSON Point with in-range coordinates."""
    document = json.loads(_estimate_bytes(iotwireless, "1.2.3.4"))
    assert document["type"] == "Point"
    lon, lat = document["coordinates"]
    assert -180 <= lon < 180
    assert -60 <= lat < 60


def test_iotwireless_accuracy_properties_carry_the_measured_values(iotwireless):
    """The two accuracy properties carry the values of the one live call
    (eu-west-1, 2026-08-26)."""
    properties = json.loads(_estimate_bytes(iotwireless, "1.2.3.4"))["properties"]
    assert properties["horizontalAccuracy"] == 1000000
    assert properties["horizontalConfidenceLevel"] == 0.67


def test_iotwireless_same_ip_answers_identical_bytes(iotwireless):
    """The estimate is deterministic, so a consumer test can assert on it."""
    first = _estimate_bytes(iotwireless, "5.6.7.8")
    second = _estimate_bytes(iotwireless, "5.6.7.8")
    assert first == second


def test_iotwireless_equivalent_ip_spellings_answer_identical_bytes(iotwireless):
    """The hash runs over the canonical address, not the raw string."""
    assert _estimate_bytes(iotwireless, "2600::1") == _estimate_bytes(
        iotwireless, "2600:0:0:0:0:0:0:1"
    )
    # An IPv4-mapped IPv6 address is the IPv4 address.
    assert _estimate_bytes(iotwireless, "::ffff:1.2.3.4") == _estimate_bytes(
        iotwireless, "1.2.3.4"
    )


def test_iotwireless_different_ips_answer_different_coordinates(iotwireless):
    first = json.loads(_estimate_bytes(iotwireless, "5.6.7.8"))
    second = json.loads(_estimate_bytes(iotwireless, "1.2.3.4"))
    assert first["coordinates"] != second["coordinates"]


@pytest.mark.parametrize("member", sorted(_NON_IP_MEMBERS))
def test_iotwireless_non_ip_members_do_not_move_the_estimate(iotwireless, member):
    """Each non-Ip member is accepted and none of them resolves the position.
    `Timestamp` is the one that leaves a trace: AWS documents it as the time
    the position is resolved, so it is echoed into `properties.timestamp` and
    dropped here before the comparison. Nothing else may differ."""
    document = json.loads(
        iotwireless.get_position_estimate(
            Ip={"IpAddress": "5.6.7.8"}, **{member: _NON_IP_MEMBERS[member]}
        )["GeoJsonPayload"].read()
    )
    document["properties"].pop("timestamp", None)
    assert document == json.loads(_estimate_bytes(iotwireless, "5.6.7.8"))


def test_iotwireless_timestamp_is_echoed_into_the_properties(iotwireless):
    """AWS documents `Timestamp` as the time at which the position is
    resolved, and the live payload reports that time in `properties`. The
    request's value is echoed, so the blob stays a function of the input."""
    document = json.loads(
        iotwireless.get_position_estimate(
            Ip={"IpAddress": "5.6.7.8"}, Timestamp=_TIMESTAMP
        )["GeoJsonPayload"].read()
    )
    assert document["properties"]["timestamp"] == _TIMESTAMP_RENDERED


def test_iotwireless_timestamp_absent_leaves_the_property_out(iotwireless):
    """No `Timestamp` in the request, no `timestamp` in the payload."""
    document = json.loads(_estimate_bytes(iotwireless, "5.6.7.8"))
    assert "timestamp" not in document["properties"]


def test_iotwireless_coordinates_clamped_to_half_open_intervals():
    """Hashes landing in [179.99995, 180) / [59.99995, 60) would round to
    exactly 180.0 / 60.0, so the clamp keeps the documented half-open
    intervals. Both addresses were searched to hit the raw rounding edge."""
    from ministack.services.iotwireless import _coordinates_for

    lon, _ = _coordinates_for("166.199.146.1")
    assert lon == 179.9999
    _, lat = _coordinates_for("66.72.68.1")
    assert lat == 59.9999


@pytest.mark.parametrize(
    "ip", ["203.0.113.7", "10.0.0.1", "::1", "224.0.0.1", "ff02::1"]
)
def test_iotwireless_unresolvable_ip_is_resource_not_found(iotwireless, ip):
    """A valid but non-global (or multicast) address is refused the way the
    real service refuses an address its geo database cannot place (measured
    verbatim with TEST-NET 203.0.113.7 in eu-west-1)."""
    with pytest.raises(ClientError) as excinfo:
        iotwireless.get_position_estimate(Ip={"IpAddress": ip})
    error = excinfo.value.response["Error"]
    assert error["Code"] == "ResourceNotFoundException"
    assert error["Message"] == f"Cannot find position for the IP address {ip}"


def test_iotwireless_invalid_ip_is_validation_exception(iotwireless):
    """Wording measured verbatim on the live service."""
    with pytest.raises(ClientError) as excinfo:
        iotwireless.get_position_estimate(Ip={"IpAddress": "not-an-ip"})
    error = excinfo.value.response["Error"]
    assert error["Code"] == "ValidationException"
    assert error["Message"] == (
        "1 validation error detected: IP Address is not valid."
    )


def test_iotwireless_empty_input_is_validation_exception(iotwireless):
    """No resolver hint at all. Wording measured verbatim on the live
    service."""
    with pytest.raises(ClientError) as excinfo:
        iotwireless.get_position_estimate()
    error = excinfo.value.response["Error"]
    assert error["Code"] == "ValidationException"
    assert error["Message"] == (
        "1 validation error detected: Request must have at least 1 valid "
        "position measurement."
    )


def test_iotwireless_timestamp_only_input_is_validation_exception(iotwireless):
    """The developer guide lists "the payload contains only the timestamp
    information" as a cause of the documented 400, so `Timestamp` is not a
    measurement. Only the class is pinned; the live wording for this input
    is unmeasured."""
    with pytest.raises(ClientError) as excinfo:
        iotwireless.get_position_estimate(Timestamp=_TIMESTAMP)
    assert excinfo.value.response["Error"]["Code"] == "ValidationException"


def test_iotwireless_non_ip_hints_alone_are_validation_exception(iotwireless):
    """Documented divergence: MiniStack resolves from Ip only, so an input
    carrying nothing but WLAN measurements is refused with a message naming
    the IP-only scope (the real service would run the third-party solver)."""
    with pytest.raises(ClientError) as excinfo:
        iotwireless.get_position_estimate(
            WiFiAccessPoints=[{"MacAddress": "A0:EC:F9:1E:32:C1", "Rss": -75}]
        )
    error = excinfo.value.response["Error"]
    assert error["Code"] == "ValidationException"
    assert "Ip only" in error["Message"]


# ---------------------------------------------------------------------------
# Raw wire (unsigned, like curl)
# ---------------------------------------------------------------------------

def test_iotwireless_unsigned_raw_post_answers_the_bare_geojson_blob(iotwireless):
    """No Authorization header: the POST-only path rule routes it, and the
    body is the raw GeoJSON Point with no envelope key, byte-identical to
    what the SDK client streams."""
    status, content_type, body = _raw(
        "/position-estimate", json.dumps({"Ip": {"IpAddress": "1.2.3.4"}}).encode()
    )
    assert status == 200
    assert content_type == "application/octet-stream"
    document = json.loads(body)
    assert document["type"] == "Point"
    assert "GeoJsonPayload" not in document
    assert body == _estimate_bytes(iotwireless, "1.2.3.4")


@pytest.mark.parametrize("ip_member", [{}, {"IpAddress": ""}])
def test_iotwireless_raw_ip_without_an_address_is_not_valid(ip_member):
    """An `Ip` member that carries no usable address is a present hint, so it
    reaches the address validator rather than the no-measurement branch. The
    SDK cannot send this (`IpAddress` is a required member), only raw HTTP
    can."""
    status, _content_type, raw = _raw(
        "/position-estimate", json.dumps({"Ip": ip_member}).encode()
    )
    assert status == 400
    document = json.loads(raw)
    assert document["__type"] == "ValidationException"
    assert document["message"] == (
        "1 validation error detected: IP Address is not valid."
    )


@pytest.mark.parametrize("body", [
    b"[]",                                  # a JSON array, not an object
    b'{"Ip": "1.2.3.4"}',                    # Ip is not an object
    b"not json",
])
def test_iotwireless_raw_malformed_input_is_validation_exception(body):
    status, _content_type, raw = _raw("/position-estimate", body)
    assert status == 400
    assert json.loads(raw)["__type"] == "ValidationException"


def test_iotwireless_unknown_route_is_validation_exception():
    """An unknown path is 400 ValidationException "No route for ...", like
    the neighbouring REST-JSON services. The request carries the
    `iotwireless` credential scope so the router hands it to the service."""
    req = urllib.request.Request(
        f"{ENDPOINT}/no-such-operation", data=b"{}", method="POST",
        headers={
            "content-type": "application/json",
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                "Credential=test/20260811/us-east-1/iotwireless/aws4_request, "
                "SignedHeaders=host, Signature=fake"
            ),
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            status, raw = resp.status, resp.read()
    except urllib.error.HTTPError as e:
        status, raw = e.code, e.read()
    assert status == 400
    assert json.loads(raw)["__type"] == "ValidationException"
