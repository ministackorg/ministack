"""IoT Wireless HTTP API (``iotwireless`` AWS service).

Implements ``GetPositionEstimate`` (``POST /position-estimate``), the
device-location resolver of AWS IoT Wireless (endpoint prefix
``api.iotwireless``, REST-JSON, SigV4 scope ``iotwireless``).

Wire shape worth stating up front: the output structure declares
``"payload": "GeoJsonPayload"`` — the HTTP response body IS the raw GeoJSON
blob, not a JSON envelope, and boto3 hands the caller a ``StreamingBody``.
Answering ``{"GeoJsonPayload": ...}`` would break every SDK client.

Resolver scope: the real service resolves WLAN, cell-tower, GNSS and IP
measurements through third-party solvers. MiniStack resolves from ``Ip``
only — ``WiFiAccessPoints``, ``CellTowers``, ``Gnss``, ``Timestamp`` and
``AdvancedConfiguration`` are accepted and ignored, and their presence never
changes the response. The estimate is synthetic and deterministic: the
canonical form of ``Ip.IpAddress`` is hashed (SHA-256) onto lon [-180, 180)
/ lat [-60, 60), 4 decimals, so the same IP always answers byte-identical
GeoJSON and a consumer test can assert on it. The blob's ``properties``
carry only the two deterministic accuracy fields; the live service's
properties are ``country``, ``horizontalAccuracy``,
``horizontalConfidenceLevel`` and ``timestamp`` (plus ``city`` / ``state`` /
``postalCode`` on IP lookups), of which ``country``, ``timestamp`` and the
city/state/postal fields are omitted here so the payload stays a pure
function of the IP (documented divergence).

Refusals mirror the live service verbatim (measured eu-west-1 2026-08-26):
an input with no resolver hint answers ``ValidationException`` ``"1
validation error detected: Request must have at least 1 valid position
measurement."``, an ``IpAddress`` that ``ipaddress.ip_address()`` refuses
answers ``ValidationException`` ``"1 validation error detected: IP Address
is not valid."``, and a valid address the resolver cannot place answers
``ResourceNotFoundException`` ``"Cannot find position for the IP address
<ip>"`` — the real service refuses when its geo database has no entry for
the address (measured with TEST-NET 203.0.113.7); MiniStack approximates
that refusal class deterministically by refusing every address that is not
globally routable (``ipaddress.ip_address(...).is_global`` false: private,
loopback, link-local, reserved, TEST-NET) and resolving every global one
(documented divergence). A malformed JSON body is a ``ValidationException``
too, and an input whose only hints are WLAN/cell/GNSS measurements is
answered ``ValidationException`` with a message naming the IP-only scope
(documented divergence — the real service would run the third-party
solvers).
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import logging

from ministack.core.responses import error_response_json

logger = logging.getLogger("iotwireless")


# ---------------------------------------------------------------------------
# Persistence (no state — the estimate is a pure function of the request)
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
    return error_response_json(
        "ResourceNotFoundException",
        f"Unsupported iotwireless path: {method} {path}",
        404,
    )


# ---------------------------------------------------------------------------
# GetPositionEstimate
# ---------------------------------------------------------------------------

# The members the real service treats as resolver input — at least one must
# be present (`Timestamp` and `AdvancedConfiguration` alone do not count).
_HINT_MEMBERS = ("WiFiAccessPoints", "CellTowers", "Ip", "Gnss")


def _validation(message: str) -> tuple:
    return error_response_json("ValidationException", message, 400)


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

    if not any(payload.get(member) for member in _HINT_MEMBERS):
        # Measured verbatim on the live service.
        return _validation(
            "1 validation error detected: Request must have at least 1 valid "
            "position measurement."
        )
    ip_member = payload.get("Ip")
    if not ip_member:
        return _validation(
            "MiniStack resolves position from Ip only — provide Ip.IpAddress "
            "(WiFiAccessPoints/CellTowers/Gnss are accepted but not resolved)"
        )
    raw = ip_member.get("IpAddress") if isinstance(ip_member, dict) else None
    try:
        address = ipaddress.ip_address(str(raw))
    except ValueError:
        # Measured verbatim on the live service.
        return _validation("1 validation error detected: IP Address is not valid.")
    canonical = str(address)
    if not address.is_global:
        # The real service refuses an address its geo database cannot place
        # (measured with TEST-NET 203.0.113.7); MiniStack approximates that
        # refusal class deterministically with the global-routability test.
        return error_response_json(
            "ResourceNotFoundException",
            f"Cannot find position for the IP address {canonical}",
            404,
        )

    lon, lat = _coordinates_for(canonical)
    geojson = {
        "coordinates": [lon, lat],
        "type": "Point",
        "properties": {
            "horizontalAccuracy": 5000,
            "horizontalConfidenceLevel": 0.68,
        },
    }
    logger.info(
        "IoT Wireless: resolved %s to (%s, %s)", canonical, lon, lat
    )
    # Raw payload blob — the output shape's `payload` trait, see module
    # docstring. No JSON envelope.
    blob = json.dumps(geojson, ensure_ascii=False).encode("utf-8")
    return 200, {"Content-Type": "application/octet-stream"}, blob


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
