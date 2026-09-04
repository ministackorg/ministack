"""Shared AWS Signature Version 4 primitives.

This module owns only deterministic signing mechanics. Services remain
responsible for validating their own credential scope, resolving secrets, and
mapping verification failures to service-specific responses.
"""

import datetime as dt
import hashlib
import hmac
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"


def uri_encode(value: str, encode_slash: bool = True) -> str:
    """Encode a SigV4 URI component using the RFC 3986 unreserved set."""
    safe = "-_.~" + ("" if encode_slash else "/")
    return quote(value, safe=safe)


def build_canonical_query_string(
    query_params: Mapping[str, Any],
    *,
    exclude: tuple[str, ...] = ("x-amz-signature",),
) -> str:
    """Encode and sort query parameters for a SigV4 canonical request."""
    excluded = {name.lower() for name in exclude}
    pairs = []
    for name, values in query_params.items():
        if name.lower() in excluded:
            continue
        value_list = values if isinstance(values, list) else [values]
        for value in value_list:
            pairs.append((uri_encode(name), uri_encode(value)))
    pairs.sort()
    return "&".join(f"{key}={value}" for key, value in pairs)


def build_canonical_headers(headers: Mapping[str, Any], signed_headers: str) -> str:
    """Normalize the headers named by a SigV4 signed-headers value."""
    canonical = ""
    for name in (name for name in signed_headers.split(";") if name):
        raw = headers.get(name, headers.get(name.lower(), ""))
        canonical += f"{name.lower()}:{' '.join(str(raw).split())}\n"
    return canonical


def build_canonical_request(
    method: str,
    path: str,
    headers: Mapping[str, Any],
    query_params: Mapping[str, Any],
    signed_headers: str,
    *,
    payload_hash: str = UNSIGNED_PAYLOAD,
    exclude_query_params: tuple[str, ...] = ("x-amz-signature",),
) -> str:
    """Build the canonical request hashed by SigV4."""
    return "\n".join(
        [
            method,
            uri_encode(path, encode_slash=False),
            build_canonical_query_string(query_params, exclude=exclude_query_params),
            build_canonical_headers(headers, signed_headers),
            signed_headers,
            payload_hash,
        ]
    )


def build_string_to_sign(
    amz_date: str,
    date_stamp: str,
    region: str,
    service: str,
    canonical_request: str,
) -> str:
    """Build the SigV4 string to sign for a canonical request."""
    return "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            f"{date_stamp}/{region}/{service}/aws4_request",
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )


def derive_signing_key(secret: str, date_stamp: str, region: str, service: str) -> bytes:
    """Derive the SigV4 signing key for one date, region, and service."""

    def _sign(key: bytes, value: str) -> bytes:
        return hmac.new(key, value.encode("utf-8"), hashlib.sha256).digest()

    date_key = _sign(("AWS4" + secret).encode("utf-8"), date_stamp)
    region_key = _sign(date_key, region)
    service_key = _sign(region_key, service)
    return _sign(service_key, "aws4_request")


def calculate_signature(
    secret: str,
    date_stamp: str,
    region: str,
    service: str,
    string_to_sign: str,
) -> str:
    """Calculate the lowercase hexadecimal SigV4 signature."""
    signing_key = derive_signing_key(secret, date_stamp, region, service)
    return hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()


def presigned_request_is_expired(
    amz_date: str,
    expires: str | int,
    *,
    now: dt.datetime | None = None,
) -> bool:
    """Return whether a presigned request is past its exclusive expiry time.

    Invalid timestamps and durations raise ``ValueError`` so each service can
    apply its own validation or compatibility behavior.
    """
    signed_at = dt.datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.timezone.utc)
    current_time = now if now is not None else dt.datetime.now(dt.timezone.utc)
    return current_time > signed_at + dt.timedelta(seconds=int(expires))


def signatures_match(computed: str, provided: str) -> bool:
    """Compare signatures without leaking a mismatch position through timing."""
    return hmac.compare_digest(computed, provided)
