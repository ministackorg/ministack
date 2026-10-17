"""Shared AWS Signature Version 4 primitives.

This module owns only deterministic signing mechanics. Services remain
responsible for validating their own credential scope, resolving secrets, and
mapping verification failures to service-specific responses.
"""

import base64
import datetime as dt
import functools
import hashlib
import hmac
from collections.abc import Mapping
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote

UNSIGNED_PAYLOAD = "UNSIGNED-PAYLOAD"


def decode_header_value(value: bytes) -> str:
    """Decode a raw header value as UTF-8, or as Latin-1 when it is not valid UTF-8."""
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return value.decode("latin-1")


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


SIGNATURE_MISMATCH = (
    "The request signature we calculated does not match the signature you provided. "
    "Check your AWS Secret Access Key and signing method. Consult the service documentation for details."
)


@dataclass(frozen=True)
class HeaderSignature:
    """The parts of an ``Authorization: AWS4-HMAC-SHA256`` header and the request date."""

    access_key_id: str
    date_stamp: str
    region: str
    service: str
    signed_headers: str
    signature: str
    amz_date: str


def _incomplete(authorization: str, reason: str, code: str = "IncompleteSignature") -> tuple[str, str]:
    digest = base64.b64encode(hashlib.sha256(authorization.encode("utf-8")).digest()).decode()
    return code, f"{reason} (Hashed with SHA-256 and encoded with Base64) Authorization={digest}"


def _missing_parts(values: Mapping[str, Any], names: tuple[str, ...]) -> tuple[list[str], str]:
    """The names without a value, and the code: MissingAuthorizationParameter when one is left out entirely."""
    missing = [name for name in names if not values.get(name)]
    absent = any(name not in values for name in missing)
    return missing, "MissingAuthorizationParameter" if absent else "IncompleteSignature"


def parse_header_signature(headers: Mapping[str, Any]) -> HeaderSignature | tuple[str, str]:
    """Split a header signature into its parts, or name every part that is missing."""
    authorization = headers.get("authorization", "")
    fields = dict(part.strip().partition("=")[::2] for part in authorization.split(" ", 1)[-1].split(","))
    amz_date = headers.get("x-amz-date", "")
    if not amz_date and headers.get("date"):
        try:
            signed_at = parsedate_to_datetime(headers["date"]).astimezone(dt.timezone.utc)
            amz_date = signed_at.strftime("%Y%m%dT%H%M%SZ")
        except (TypeError, ValueError):
            pass
    no_date = "Authorization header requires existence of either a 'X-Amz-Date' or a 'Date' header."
    missing, code = _missing_parts(fields, ("Credential", "Signature", "SignedHeaders"))
    if missing:
        reasons = [f"Authorization header requires '{name}' parameter." for name in missing]
        return _incomplete(authorization, " ".join(reasons + ([] if amz_date else [no_date])), code)
    scope = fields["Credential"].split("/")
    if len(scope) != 5:
        return "IncompleteSignature", (
            "Credential must have exactly 5 slash-delimited elements, e.g. keyid/date/region/service/term, "
            f"got '{fields['Credential']}'"
        )
    if not amz_date:
        return _incomplete(authorization, no_date)
    return HeaderSignature(*scope[:4], fields["SignedHeaders"], fields["Signature"], amz_date)


def _query_value(query_params: Mapping[str, Any], name: str) -> str:
    value = query_params.get(name, query_params.get(name.lower(), ""))
    return (value[0] if value else "") if isinstance(value, list) else value


def parse_query_signature(query_params: Mapping[str, Any]) -> HeaderSignature | tuple[str, str]:
    """Split a query-string (presigned) signature into its parts, or name every part that is missing."""
    names = ("X-Amz-Credential", "X-Amz-Signature", "X-Amz-SignedHeaders", "X-Amz-Date")
    missing, code = _missing_parts(
        {name: _query_value(query_params, name) for name in names
         if name in query_params or name.lower() in query_params}, names)
    if missing:
        return code, " ".join(
            [f"AWS query-string parameters must include '{name}'." for name in missing]
            + ["Re-examine the query-string parameters."])
    credential = _query_value(query_params, "X-Amz-Credential")
    scope = credential.split("/")
    if len(scope) != 5:
        return "IncompleteSignature", (
            "Credential must have exactly 5 slash-delimited elements, e.g. keyid/date/region/service/term, "
            f"got '{credential}'"
        )
    return HeaderSignature(*scope[:4], _query_value(query_params, "X-Amz-SignedHeaders"),
                           _query_value(query_params, "X-Amz-Signature"), _query_value(query_params, "X-Amz-Date"))


# Services that accept a request signed up to 15 minutes from their clock; the others allow 5.
_FIFTEEN_MINUTE_SERVICES = frozenset({"dynamodb", "ecr", "elasticache", "iam", "rds", "s3", "sns", "sqs", "sts"})


def _amz_datetime(value: str) -> dt.datetime | None:
    """Parse ``YYYYMMDDTHHMMSSZ`` by slicing; strptime is slow on the request path."""
    digits = value[:8] + value[9:15]
    if len(value) != 16 or value[8] != "T" or value[15] != "Z" or not (digits.isascii() and digits.isdigit()):
        return None
    try:
        return dt.datetime(int(value[:4]), int(value[4:6]), int(value[6:8]), int(value[9:11]), int(value[11:13]),
                           int(value[13:15]), tzinfo=dt.timezone.utc)
    except ValueError:
        return None


def request_time_failure(signature: HeaderSignature, *, now: dt.datetime | None = None) -> tuple[str, str] | None:
    """Refuse a request signed further from the current time than its service allows."""
    signed_at = _amz_datetime(signature.amz_date)
    if signed_at is None:
        return None
    now = now or dt.datetime.now(dt.timezone.utc)
    minutes = 15 if signature.service in _FIFTEEN_MINUTE_SERVICES else 5
    window = dt.timedelta(minutes=minutes)

    def stamp(value: dt.datetime) -> str:
        return value.strftime("%Y%m%dT%H%M%SZ")

    if signed_at < now - window:
        return "RequestTimeTooSkewed", (
            f"Signature expired: {signature.amz_date} is now earlier than {stamp(now - window)} "
            f"({stamp(now)} - {minutes} min.)"
        )
    if signed_at > now + window:
        return "RequestTimeTooSkewed", (
            f"Signature not yet current: {signature.amz_date} is still later than {stamp(now + window)} "
            f"({stamp(now)} + {minutes} min.)"
        )
    return None


def _remove_dot_segments(path: str) -> str:
    segments = []
    for segment in path.split("/"):
        if segment == "..":
            if segments:
                segments.pop()
        elif segment and segment != ".":
            segments.append(segment)
    return "/" + "/".join(segments) + ("/" if path.endswith("/") and segments else "")


_cached_signing_key = functools.lru_cache(maxsize=256)(derive_signing_key)


def header_signature_failure(
    signature: HeaderSignature,
    secret: str,
    method: str,
    raw_path: str,
    query_params: Mapping[str, Any],
    headers: Mapping[str, Any],
    body: bytes,
    *,
    presigned: bool = False,
    explain: bool = False,
) -> tuple[str, str] | None:
    """Recompute a header (or, ``presigned``, query-string) signature; S3 signs its path as sent, other services
    the normalized path encoded again."""
    if signature.date_stamp != signature.amz_date[:8]:
        return "CredentialDateMismatch", (
            "Date in Credential scope does not match YYYYMMDD from ISO-8601 version of date from HTTP. ")
    path = raw_path if signature.service == "s3" else uri_encode(_remove_dot_segments(raw_path), encode_slash=False)
    payload_hash = headers.get("x-amz-content-sha256") or hashlib.sha256(body).hexdigest()
    excluded = ("x-amz-signature",) if presigned else ()
    if presigned and signature.service == "iotdevicegateway":
        # AWS IoT signs the URL without the session token and appends the token afterwards.
        excluded += ("x-amz-security-token",)
    canonical_request = "\n".join([
        method, path, build_canonical_query_string(query_params, exclude=excluded),
        build_canonical_headers(headers, signature.signed_headers), signature.signed_headers, payload_hash,
    ])
    string_to_sign = build_string_to_sign(
        signature.amz_date, signature.date_stamp, signature.region, signature.service, canonical_request
    )
    key = _cached_signing_key(secret, signature.date_stamp, signature.region, signature.service)
    computed = hmac.new(key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    if not signatures_match(computed, signature.signature) or (
        payload_hash.startswith("STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
        and not _chunk_signatures_match(body, computed, signature, key)
    ):
        if explain:  # API Gateway names the strings it expected
            return "SignatureDoesNotMatch", (
                f"{SIGNATURE_MISMATCH}\n\nThe Canonical String for this request should have been\n'{canonical_request}'"
                f"\n\nThe String-to-Sign should have been\n'{string_to_sign}'\n"
            )
        return "SignatureDoesNotMatch", SIGNATURE_MISMATCH
    return None


def _chunk_signatures_match(body: bytes, seed: str, signature: HeaderSignature, key: bytes) -> bool:
    """Check each aws-chunked chunk signature, chained from the seed signature."""
    scope = f"{signature.date_stamp}/{signature.region}/{signature.service}/aws4_request"
    empty = hashlib.sha256(b"").hexdigest()
    previous, pos = seed, 0
    while (end := body.find(b"\r\n", pos)) >= 0:
        size, _, provided = body[pos:end].decode("ascii", "replace").partition(";chunk-signature=")
        try:
            size = int(size, 16)
        except ValueError:
            return False
        chunk = body[end + 2:end + 2 + size]
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256-PAYLOAD", signature.amz_date, scope, previous, empty,
            hashlib.sha256(chunk).hexdigest(),
        ])
        previous = hmac.new(key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        if not signatures_match(previous, provided.strip()):
            return False
        if size == 0:
            return True
        pos = end + 2 + size + 2
    return False
