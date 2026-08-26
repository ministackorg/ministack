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

import pytest
from botocore.exceptions import ClientError


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


def test_iotwireless_same_ip_answers_identical_bytes(iotwireless):
    """The estimate is deterministic — a consumer test can assert on it."""
    first = _estimate_bytes(iotwireless, "5.6.7.8")
    second = _estimate_bytes(iotwireless, "5.6.7.8")
    assert first == second


def test_iotwireless_equivalent_ip_spellings_answer_identical_bytes(iotwireless):
    """The hash runs over the canonical address, not the raw string."""
    assert _estimate_bytes(iotwireless, "2600::1") == _estimate_bytes(
        iotwireless, "2600:0:0:0:0:0:0:1"
    )


def test_iotwireless_different_ips_answer_different_coordinates(iotwireless):
    first = json.loads(_estimate_bytes(iotwireless, "5.6.7.8"))
    second = json.loads(_estimate_bytes(iotwireless, "1.2.3.4"))
    assert first["coordinates"] != second["coordinates"]


def test_iotwireless_other_hints_accepted_and_ignored(iotwireless):
    """WiFiAccessPoints riding along with Ip never change the response."""
    with_wifi = iotwireless.get_position_estimate(
        Ip={"IpAddress": "5.6.7.8"},
        WiFiAccessPoints=[{"MacAddress": "A0:EC:F9:1E:32:C1", "Rss": -75}],
    )["GeoJsonPayload"].read()
    assert with_wifi == _estimate_bytes(iotwireless, "5.6.7.8")


def test_iotwireless_advanced_configuration_accepted_and_ignored(iotwireless):
    """The documented `AdvancedConfiguration` member is accepted like the
    other non-Ip hints and never changes the response."""
    with_config = iotwireless.get_position_estimate(
        Ip={"IpAddress": "5.6.7.8"},
        AdvancedConfiguration={"WiFiCellular": {"ConfidencePercent": 90}},
    )["GeoJsonPayload"].read()
    assert with_config == _estimate_bytes(iotwireless, "5.6.7.8")


def test_iotwireless_coordinates_clamped_to_half_open_intervals():
    """Hashes landing in [179.99995, 180) / [59.99995, 60) would round to
    exactly 180.0 / 60.0 — the clamp keeps the documented half-open
    intervals. Both addresses were searched to hit the raw rounding edge."""
    from ministack.services.iotwireless import _coordinates_for

    lon, _ = _coordinates_for("166.199.146.1")
    assert lon == 179.9999
    _, lat = _coordinates_for("66.72.68.1")
    assert lat == 59.9999


@pytest.mark.parametrize("ip", ["203.0.113.7", "10.0.0.1"])
def test_iotwireless_unresolvable_ip_is_resource_not_found(iotwireless, ip):
    """A valid but non-global address is refused the way the real service
    refuses an address its geo database cannot place (measured verbatim
    with TEST-NET 203.0.113.7 in eu-west-1)."""
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
    """No resolver hint at all — wording measured verbatim on the live
    service."""
    with pytest.raises(ClientError) as excinfo:
        iotwireless.get_position_estimate()
    error = excinfo.value.response["Error"]
    assert error["Code"] == "ValidationException"
    assert error["Message"] == (
        "1 validation error detected: Request must have at least 1 valid "
        "position measurement."
    )


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
