# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""IoT Wireless HTTP API (``iotwireless`` AWS service).

Implements ``GetPositionEstimate`` (``POST /position-estimate``), the
device-location resolver of AWS IoT Wireless (endpoint prefix
``api.iotwireless``, REST-JSON, SigV4 scope ``iotwireless``).

Wire shape worth stating up front: the output structure declares
``"payload": "GeoJsonPayload"``, so the HTTP response body IS the raw GeoJSON
blob, not a JSON envelope, and boto3 hands the caller a ``StreamingBody``.
Answering ``{"GeoJsonPayload": ...}`` would break every SDK client.

Resolver scope: the real service resolves WLAN, cell-tower, GNSS and IP
measurements through third-party solvers. MiniStack resolves from ``Ip``
only. ``WiFiAccessPoints``, ``CellTowers`` and ``Gnss`` are accepted and
ignored, and none of them moves the estimate. The estimate is synthetic
and deterministic: the canonical
form of ``Ip.IpAddress`` is hashed (SHA-256) onto lon [-180, 180) / lat
[-60, 60), 4 decimals, so the same address always answers the same estimate
and a consumer test can assert on it.

The blob's ``properties`` carry the two accuracy fields with the values of
the single live call recorded for this work (eu-west-1, 2026-08-26):
``horizontalAccuracy`` 1000000 and ``horizontalConfidenceLevel`` 0.67. That
call asked for nothing, so 0.67 is the default level;
``AdvancedConfiguration.WiFiCellular.ConfidencePercent`` is the caller's own
say over it and drives the reported level, over 100, within the 50 to 99 the
model allows. The
``timestamp`` property is always there: AWS documents the request's
``Timestamp`` as the time at which the position is resolved and says "if not
specified, the time at which the request was received will be used", so the
caller's value is echoed when there is one and the receive time stands in
otherwise. That is the one part of the payload that is not a function of the
request; send a ``Timestamp`` and the same request answers byte-identical
GeoJSON. The live payload's ``country`` is left out for determinism, as are
the ``city`` / ``state`` / ``postalCode`` properties the developer guide
lists for IP lookups (documented divergence).

Two refusals are recorded from the live service (eu-west-1 2026-08-26) and
are marked as such at their call sites. EVERY OTHER message in this module
is unverified: no message string is documented for this operation, so the
wording is this emulator's own and must not be read as AWS's.
An input with no resolver hint answers ``ValidationException`` ``"1
validation error detected: Request must have at least 1 valid position
measurement."``, and an ``IpAddress`` that ``ipaddress.ip_address()``
refuses answers ``ValidationException`` ``"1 validation error detected: IP
Address is not valid."``. A valid address the resolver cannot place answers
``ResourceNotFoundException`` ``"Cannot find position for the IP address
<ip>"``, which is how the real service refuses an address its geo database
has no entry for (measured with TEST-NET 203.0.113.7). MiniStack
approximates that refusal class deterministically by refusing every address
that is not globally routable or is multicast and resolving every other one
(documented divergence), following the running interpreter's ``ipaddress``
view: private, loopback, link-local, reserved, TEST-NET and multicast
addresses do not resolve.

An input whose only hints are WLAN/cell/GNSS measurements is well formed and
unresolvable, so it answers ``ResourceNotFoundException`` too, with a
message naming the IP-only scope: AWS documents that class for "no location
information was found or solved ... insufficient data in the measurement
data input", where the real service would run the third-party solvers.

An IPv4-mapped IPv6 address hashes as its IPv4 form. A malformed JSON body
is a ``ValidationException``, as is a ``Timestamp`` that is not a Unix
timestamp (a raw HTTP caller can send one; an SDK cannot) and a
``ConfidencePercent`` outside the range the model allows.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import logging
from datetime import datetime, timezone

from ministack.core.responses import REST_JSON_CONTENT_TYPE, error_response_json

logger = logging.getLogger("iotwireless")


# ---------------------------------------------------------------------------
# Persistence (no state: the estimate is a pure function of the request)
# ---------------------------------------------------------------------------


def get_state() -> dict:
    return {}


def restore_state(data: dict | None) -> None:
    return None


def reset() -> None:
    return None


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


async def handle_request(
    method: str, path: str, headers: dict, body: bytes, query_params: dict
) -> tuple:
    if method == "POST" and path == "/position-estimate":
        return _get_position_estimate(body)
    return _validation(f"No route for {method} {path}")


# ---------------------------------------------------------------------------
# GetPositionEstimate
# ---------------------------------------------------------------------------

# The members the real service treats as resolver input. At least one must be
# present: the developer guide lists "the payload contains only the timestamp
# information" as one of the causes of the documented 400, so `Timestamp` and
# `AdvancedConfiguration` alone do not count. Presence is what matters, not
# the value, so an empty `Ip` reaches the address validator below.
_HINT_MEMBERS = ("WiFiAccessPoints", "CellTowers", "Ip", "Gnss")

# The values of the one live call recorded for this work (eu-west-1,
# 2026-08-26). Fixed, because the estimate is synthetic: there is no solver
# behind it whose accuracy could vary per address. That call sent no
# `AdvancedConfiguration`, so 0.67 is what the service reports when the
# caller asks for nothing; a caller that does ask drives the level itself.
_HORIZONTAL_ACCURACY = 1000000
_HORIZONTAL_CONFIDENCE_LEVEL = 0.67

# `AdvancedConfiguration.WiFiCellular.ConfidencePercent`, as the model
# constrains it: an integer, 50 to 99 inclusive.
_CONFIDENCE_PERCENT_MIN = 50
_CONFIDENCE_PERCENT_MAX = 99

# What a member reader answers when the request's value is not one the member
# accepts, so the caller gets the documented 400 instead of the value.
_INVALID = object()


def _error(code: str, message: str, status: int) -> tuple:
    """An iotwireless error body.

    The exception shapes of iotwireless/2020-11-22 declare the message member
    as ``Message`` with no ``locationName``, so ``Message`` is what goes on the
    wire — not the ``message`` most json-protocol services model. The protocol
    is ``rest-json``, so the body is ``application/json``.

    ``ResourceNotFoundException`` and ``ConflictException`` also model
    ``ResourceId`` / ``ResourceType``. Both are optional and nothing here knows
    what the live service puts in them for this operation, so they are omitted
    rather than filled with a guess.
    """
    return error_response_json(code, message, status,
                               message_key="Message",
                               content_type=REST_JSON_CONTENT_TYPE)


def _validation(message: str) -> tuple:
    return _error("ValidationException", message, 400)


def _get_position_estimate(body: bytes) -> tuple:
    if body:
        try:
            payload = json.loads(body)
        except ValueError:
            return _validation("Request body is not valid JSON")
        if not isinstance(payload, dict):
            return _validation("Request body must be a JSON object")
    else:
        payload = {}

    if not any(member in payload for member in _HINT_MEMBERS):
        # Measured verbatim on the live service.
        return _validation(
            "1 validation error detected: Request must have at least 1 valid "
            "position measurement."
        )
    resolved_at = _resolved_timestamp(payload)
    if resolved_at is _INVALID:
        # The input is checked before the resolver runs, so a Timestamp that
        # is not a timestamp is the documented 400 whatever the address does.
        return _validation(
            "Timestamp must be a Unix timestamp, in seconds since the epoch"
        )
    confidence_level = _confidence_level(payload)
    if confidence_level is _INVALID:
        return _validation(
            "AdvancedConfiguration.WiFiCellular.ConfidencePercent must be an "
            f"integer from {_CONFIDENCE_PERCENT_MIN} to "
            f"{_CONFIDENCE_PERCENT_MAX}"
        )
    if "Ip" not in payload:
        # A WLAN/cell/GNSS-only request is well formed; there is no solver
        # behind it here, so it takes the same unresolvable-measurement 404 as
        # an address the IP resolver cannot place. The wording is NOT the live
        # service's: no message string is documented for this operation
        # (botocore carries only "Resource does not exist." on the shape), and
        # the developer-guide sentence this branch was written against could
        # not be retrieved to verify. Nothing here may name the emulator.
        return _error("ResourceNotFoundException",
                      "Cannot find position for the supplied measurements", 404)
    ip_member = payload["Ip"]
    raw = ip_member.get("IpAddress") if isinstance(ip_member, dict) else None
    try:
        address = ipaddress.ip_address(str(raw))
    except ValueError:
        # Measured verbatim on the live service.
        return _validation("1 validation error detected: IP Address is not valid.")
    # ::ffff:1.2.3.4 is the same address as 1.2.3.4; hash the IPv4 form.
    address = getattr(address, "ipv4_mapped", None) or address
    canonical = str(address)
    if not address.is_global or address.is_multicast:
        # The real service refuses an address its geo database cannot place
        # (measured with TEST-NET 203.0.113.7); MiniStack approximates that
        # refusal class deterministically with the global-routability test
        # (multicast groups are global in `ipaddress` but have no place).
        return _error("ResourceNotFoundException",
                      f"Cannot find position for the IP address {canonical}", 404)

    lon, lat = _coordinates_for(canonical)
    properties = {
        "horizontalAccuracy": _HORIZONTAL_ACCURACY,
        "horizontalConfidenceLevel": confidence_level,
        "timestamp": resolved_at,
    }
    geojson = {
        "coordinates": [lon, lat],
        "type": "Point",
        "properties": properties,
    }
    logger.info(
        "IoT Wireless: resolved %s to (%s, %s)", canonical, lon, lat
    )
    # Raw payload blob: the output shape's `payload` trait, see module
    # docstring. No JSON envelope.
    blob = json.dumps(geojson, ensure_ascii=False).encode("utf-8")
    return 200, {"Content-Type": "application/octet-stream"}, blob


def _resolved_timestamp(payload: dict):
    """The value for ``properties.timestamp``, or ``_INVALID``.

    AWS documents the request's ``Timestamp`` as "the time when the position
    information will be resolved", in Unix timestamp format, and adds "if not
    specified, the time at which the request was received will be used". The
    developer guide lists the payload as carrying "the timestamp information,
    which corresponds to the date and time at which the location was
    resolved" and both documented sample payloads have it, as did the one
    live call recorded for this work, so the property is never left out.

    The member is modelled as a timestamp, and rest-json puts one on the wire
    as the Unix number, so a value that is not that number is not a timestamp
    and the request is refused. A string is read as the same number spelled
    the JSON way, which is all a hand-written request can add; an ISO-8601
    string is not a Unix timestamp and does not pass.

    The caller's value is rendered the way the live payload reports the
    resolve time, as an ISO-8601 string. The substituted receive time stands
    in for a member in Unix timestamp format, so it is taken at whole-second
    resolution.
    """
    if "Timestamp" not in payload:
        return _render_timestamp(datetime.now(timezone.utc).replace(microsecond=0))
    value = payload["Timestamp"]
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            return _INVALID
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return _INVALID
    try:
        return _render_timestamp(datetime.fromtimestamp(value, timezone.utc))
    except (OSError, OverflowError, ValueError):
        # Out of the range a date can be built from, NaN, an infinity.
        return _INVALID


def _render_timestamp(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _confidence_level(payload: dict):
    """The value for ``properties.horizontalConfidenceLevel``, or ``_INVALID``.

    ``AdvancedConfiguration.WiFiCellular.ConfidencePercent`` is documented as
    "confidence level for WiFi and cellular position estimates, expressed as
    a percentage", so a value the caller supplies IS the level the payload
    reports, over 100. The model constrains it to an integer from 50 to 99
    inclusive and a value outside that is refused rather than clamped.

    Omitted, the level stays the 0.67 of the one live call, which sent no
    ``AdvancedConfiguration`` at all. AWS documents the member's own default
    as 68, and a caller who sends ``ConfidencePercent: 68`` gets 0.68 here;
    what 0.67 records is what the service answered a request that asked for
    nothing, which is the thing an emulator has to reproduce.
    """
    advanced = payload.get("AdvancedConfiguration")
    if advanced is None:
        return _HORIZONTAL_CONFIDENCE_LEVEL
    if not isinstance(advanced, dict):
        return _INVALID
    wifi_cellular = advanced.get("WiFiCellular")
    if wifi_cellular is None:
        return _HORIZONTAL_CONFIDENCE_LEVEL
    if not isinstance(wifi_cellular, dict):
        return _INVALID
    percent = wifi_cellular.get("ConfidencePercent")
    if percent is None:
        return _HORIZONTAL_CONFIDENCE_LEVEL
    if isinstance(percent, bool) or not isinstance(percent, int):
        return _INVALID
    if not _CONFIDENCE_PERCENT_MIN <= percent <= _CONFIDENCE_PERCENT_MAX:
        return _INVALID
    return percent / 100


def _coordinates_for(canonical_ip: str) -> tuple[float, float]:
    """Synthetic deterministic estimate: SHA-256 of the canonical IP mapped
    onto lon [-180, 180) / lat [-60, 60), rounded to 4 decimals. The upper
    bounds are clamped: a raw value in [179.99995, 180) (or [59.99995, 60))
    would otherwise round to exactly 180.0 / 60.0 and break the documented
    half-open interval."""
    digest = hashlib.sha256(canonical_ip.encode("utf-8")).digest()
    lon = int.from_bytes(digest[:8], "big") / 2**64 * 360.0 - 180.0
    lat = int.from_bytes(digest[8:16], "big") / 2**64 * 120.0 - 60.0
    return min(round(lon, 4), 179.9999), min(round(lat, 4), 59.9999)
