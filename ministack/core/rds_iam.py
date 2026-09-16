# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Strict verification of SDK-generated RDS IAM database tokens.

This module has no runtime callers and does not consult AUTH. Later integration
must explicitly gate authentication/authorization at the call site and define
the AUTH=false behavior; adding this primitive changes no existing request path.
Resource enablement, rds-db:connect policies, and TLS are separate checks.
"""

import datetime as dt
import hashlib
import math
import re
import time
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlsplit

from ministack.core.iam_evaluator import CredentialResolutionError, resolve_credential
from ministack.core.sigv4 import build_canonical_request, build_string_to_sign, calculate_signature, signatures_match

_MAX_TOKEN_LENGTH = 65536
_MAX_FUTURE_SKEW_SECONDS = 300
_REQUIRED_PARAMS = frozenset({
    "Action", "DBUser", "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date",
    "X-Amz-Expires", "X-Amz-SignedHeaders", "X-Amz-Signature",
})
_OPTIONAL_PARAMS = frozenset({"X-Amz-Security-Token"})
_EMPTY_PAYLOAD_HASH = hashlib.sha256(b"").hexdigest()


@dataclass(frozen=True)
class RdsIamTokenError:
    """A bounded reason code; never includes the token, signature, or secret."""

    code: str


@dataclass(frozen=True)
class VerifiedRdsToken:
    """Verified identity, not an authorization decision. Contains no secrets."""

    access_key_id: str
    account_id: str
    principal_arn: str
    principal_type: str
    principal_name: str
    db_user: str
    expires_at: float


def verify_rds_iam_token(
    token: str,
    *,
    hostname: str,
    port: int,
    db_user: str,
    region: str,
    account_id: str,
) -> VerifiedRdsToken | RdsIamTokenError:
    """Verify an RDS token against a caller-supplied, trusted target.

    Accept the scheme-less token returned by generate_db_auth_token. The caller
    supplies the advertised endpoint/port, exact database username, region, and
    credential account from trusted context, never from token claims. No tenant
    context is changed. Future clock skew is limited to five minutes; token
    lifetime is at most 900 seconds and cannot outlive temporary credentials.

    This verifies authentication only. A success does not imply IAM database
    auth is enabled, TLS is in use, or rds-db:connect is allowed.
    """
    invalid = RdsIamTokenError("InvalidToken")
    if (
        not isinstance(token, str) or not token or len(token) > _MAX_TOKEN_LENGTH
        or not token.isascii() or any(ord(c) <= 32 or ord(c) == 127 for c in token)
        or re.search(r"%(?![0-9a-fA-F]{2})", token)
    ):
        return invalid
    if (
        not isinstance(hostname, str) or not re.fullmatch(r"[a-zA-Z0-9.-]+", hostname)
        or type(port) is not int or not 1 <= port <= 65535
        or not isinstance(db_user, str) or not db_user
        or not isinstance(region, str) or not region
        or not isinstance(account_id, str) or not re.fullmatch(r"[0-9]{12}", account_id)
    ):
        return RdsIamTokenError("InvalidTarget")
    try:
        parsed = urlsplit("//" + token)
        if (
            parsed.path not in ("", "/") or parsed.fragment or "#" in token
            or parsed.netloc.lower() != f"{hostname.lower()}:{port}"
        ):
            return invalid
        pairs = parse_qsl(
            parsed.query, keep_blank_values=True, strict_parsing=True,
            encoding="utf-8", errors="strict", max_num_fields=16,
        )
    except (ValueError, UnicodeError):
        return invalid
    params = dict(pairs)
    if (
        len(params) != len(pairs) or not _REQUIRED_PARAMS.issubset(params)
        or params.keys() - _REQUIRED_PARAMS - _OPTIONAL_PARAMS
        or any(not value for value in params.values())
    ):
        return invalid
    if (
        params["Action"] != "connect" or params["DBUser"] != db_user
        or params["X-Amz-Algorithm"] != "AWS4-HMAC-SHA256"
        or params["X-Amz-SignedHeaders"] != "host"
        or not re.fullmatch(r"[0-9a-f]{64}", params["X-Amz-Signature"])
    ):
        return invalid
    scope = params["X-Amz-Credential"].split("/")
    amz_date = params["X-Amz-Date"]
    if (
        len(scope) != 5 or not scope[0] or scope[1] != amz_date[:8]
        or scope[2:] != [region, "rds-db", "aws4_request"]
        or not re.fullmatch(r"[0-9]{8}T[0-9]{6}Z", amz_date)
        or not re.fullmatch(r"[0-9]{1,3}", params["X-Amz-Expires"])
    ):
        return invalid
    try:
        signed_at = dt.datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=dt.timezone.utc,
        ).timestamp()
    except ValueError:
        return invalid
    lifetime = int(params["X-Amz-Expires"])
    if not 1 <= lifetime <= 900:
        return invalid
    now = time.time()
    expires_at = signed_at + lifetime
    if signed_at > now + _MAX_FUTURE_SKEW_SECONDS:
        return RdsIamTokenError("TokenNotYetValid")
    if now >= expires_at:
        return RdsIamTokenError("ExpiredToken")

    credential = resolve_credential(scope[0], account_id, params.get("X-Amz-Security-Token", ""))
    if isinstance(credential, CredentialResolutionError):
        return RdsIamTokenError("InvalidCredentials")
    # Legacy/incomplete STS records are unsuitable for strict database login,
    # even though identity-only API lookups may still use them.
    from ministack.services import sts as sts_svc

    if scope[0] in sts_svc._sessions:
        if (
            not credential.session_token or credential.expiration is None
            or not math.isfinite(credential.expiration) or now >= credential.expiration
        ):
            return RdsIamTokenError("InvalidCredentials")
        expires_at = min(expires_at, credential.expiration)

    # Go preserves endpoint casing in its signed Host; botocore normalizes
    # it. Both forms have already been bound to the same trusted DNS target.
    hosts = [parsed.netloc, parsed.netloc.lower()]
    # The RDS SDK presigns an HTTPS URL. Botocore omits its default port
    # from Host, while the token still explicitly carries ":443". Accept
    # that convention only for target port 443, preserving port binding.
    if port == 443:
        hosts.extend((parsed.netloc.rsplit(":", 1)[0], hostname.lower()))
    matched = False
    for host in dict.fromkeys(hosts):
        canonical = build_canonical_request(
            "GET", "/", {"host": host}, params, "host",
            payload_hash=_EMPTY_PAYLOAD_HASH,
        )
        string_to_sign = build_string_to_sign(amz_date, scope[1], region, "rds-db", canonical)
        signature = calculate_signature(
            credential.secret_access_key, scope[1], region, "rds-db", string_to_sign,
        )
        matched |= signatures_match(signature, params["X-Amz-Signature"])
    if not matched:
        return RdsIamTokenError("SignatureDoesNotMatch")
    return VerifiedRdsToken(
        access_key_id=credential.access_key_id,
        account_id=credential.account_id,
        principal_arn=credential.principal_arn,
        principal_type=credential.principal_type,
        principal_name=credential.principal_name,
        db_user=db_user,
        expires_at=expires_at,
    )
