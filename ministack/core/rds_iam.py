# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Strict verification of SDK-generated RDS IAM database tokens.

This module has no runtime callers and does not consult AUTH. Later integration
must explicitly gate authentication/authorization at the call site and define
the AUTH=false behavior; adding this primitive changes no existing request path.
Resource enablement, rds-db:connect policies, and TLS are separate checks.

The broker half serves POST /_ministack/rds/iam-auth for the MySQL plugin.
Capabilities are process-local, never persisted, never issued over HTTP; AUTH
gates IAM enforcement only, not capability checks or resource enablement.
"""

import asyncio
import datetime as dt
import hashlib
import json
import math
import re
import secrets
import threading
import time
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlsplit

from ministack.core.iam_evaluator import (
    AuthError,
    CredentialResolutionError,
    EvalContext,
    evaluate,
    resolve_credential,
    resolve_principal,
)
from ministack.core.responses import request_scope
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


# Stage 5 of #1744: resource-bound authorization. No runtime callers, no AUTH
# lookup; integration gates AUTH at its call site.

@dataclass(frozen=True)
class RdsIamAuthorizationError:
    """A bounded failure code, with no client token or credential material."""

    code: str


@dataclass(frozen=True)
class AuthorizedRdsConnection:
    identity: VerifiedRdsToken
    resource_arn: str


def authorize_rds_iam_token(
    token: str,
    *,
    account_id: str,
    region: str,
    resource_kind: str,
    resource_identifier: str,
    db_user: str,
    reader_endpoint: bool = False,
) -> AuthorizedRdsConnection | RdsIamAuthorizationError | RdsIamTokenError:
    """Verify and authorize against a trusted instance or cluster identifier.

    The future broker must bind these arguments to its resource capability,
    not accept a target/account claimed by the client. Endpoint/port, enablement,
    and the stable dbuser resource ID are read from that resource's scoped state.
    Aurora members use the parent cluster's enablement and resource ID, but the
    member's endpoint. Cluster callers may explicitly select the reader endpoint.
    Cross-account credentials must first assume a role in the resource account.
    A success is a point-in-time decision, not a reusable authorization cache.
    """
    from ministack.services import rds

    if (
        not isinstance(account_id, str) or not re.fullmatch(r"[0-9]{12}", account_id)
        or not isinstance(region, str) or not re.fullmatch(r"[a-z0-9-]+", region)
        or resource_kind not in ("instance", "cluster")
        or not isinstance(resource_identifier, str) or not resource_identifier
        or type(reader_endpoint) is not bool
        or (reader_endpoint and resource_kind != "cluster")
    ):
        return RdsIamAuthorizationError("InvalidTarget")

    store = rds._instances if resource_kind == "instance" else rds._clusters
    target = store.get_scoped(account_id, region, resource_identifier)
    if target is None:
        return RdsIamAuthorizationError("ResourceNotFound")
    engines = ("mysql", "aurora", "aurora-mysql") if resource_kind == "instance" else ("aurora", "aurora-mysql")
    if target.get("Engine") not in engines:
        return RdsIamAuthorizationError("UnsupportedEngine")
    if resource_kind == "instance" and target.get("Engine") != "mysql" and not target.get("DBClusterIdentifier"):
        return RdsIamAuthorizationError("InvalidTarget")

    owner = target
    resource_id_key = "DbiResourceId" if resource_kind == "instance" else "DbClusterResourceId"
    if resource_kind == "instance" and target.get("DBClusterIdentifier"):
        owner = rds._clusters.get_scoped(account_id, region, target["DBClusterIdentifier"])
        if owner is None:
            return RdsIamAuthorizationError("ResourceNotFound")
        if owner.get("Engine") not in ("aurora", "aurora-mysql"):
            return RdsIamAuthorizationError("UnsupportedEngine")
        resource_id_key = "DbClusterResourceId"
    if owner.get("IAMDatabaseAuthenticationEnabled") is not True:
        return RdsIamAuthorizationError("IAMDatabaseAuthenticationDisabled")
    resource_id = owner.get(resource_id_key)
    prefix = "db-" if resource_id_key == "DbiResourceId" else "cluster-"
    if not isinstance(resource_id, str) or not re.fullmatch(prefix + r"[A-Za-z0-9]+", resource_id):
        return RdsIamAuthorizationError("InvalidTarget")

    endpoint = target.get("ReaderEndpoint" if reader_endpoint else "Endpoint")
    if isinstance(endpoint, dict):
        hostname, port = endpoint.get("Address"), endpoint.get("Port")
    else:
        hostname, port = endpoint, target.get("Port")
    verified = verify_rds_iam_token(
        token, hostname=hostname, port=port, db_user=db_user, region=region, account_id=account_id,
    )
    if isinstance(verified, RdsIamTokenError):
        return verified

    # MiniStack's credential and RDS stores currently issue arn:aws identities.
    resource_arn = f"arn:aws:rds-db:{region}:{account_id}:dbuser:{resource_id}/{db_user}"
    # Policy gathering includes ambient-scoped user/group/managed-policy stores.
    # Pin the explicit resource account, restoring both contextvars on all exits.
    with request_scope(account_id, region):
        principal = resolve_principal(verified.access_key_id, account_id)
        if (
            isinstance(principal, AuthError) or principal.account != account_id
            or principal.arn != verified.principal_arn or principal.type != verified.principal_type
        ):
            return RdsIamAuthorizationError("InvalidCredentials")
        if principal.policies is not None:
            result = evaluate(EvalContext(
                principal_arn=principal.arn, principal_type=principal.type,
                principal_account=account_id, action="rds-db:connect",
                resource_arn=resource_arn, region=region,
            ), principal.policies)
            if result.decision != "Allow":
                return RdsIamAuthorizationError(result.decision)
    return AuthorizedRdsConnection(verified, resource_arn)


PATH = "/_ministack/rds/iam-auth"
MAX_BODY = 70 * 1024
BODY_TIMEOUT = 5
_lock = threading.RLock()
_bindings = {}


@dataclass(frozen=True, repr=False)
class _Binding:
    account_id: str
    region: str
    resource_kind: str
    resource_identifier: str
    reader_endpoint: bool
    target: dict
    owner: dict
    resource_id: str


def _resources(account_id, region, resource_kind, resource_identifier):
    from ministack.services import rds

    store = rds._instances if resource_kind == "instance" else rds._clusters
    target = store.get_scoped(account_id, region, resource_identifier)
    if not target or target.get("Engine") not in ("mysql", "aurora", "aurora-mysql"):
        return None, None
    owner = target
    if resource_kind == "instance" and target.get("DBClusterIdentifier"):
        owner = rds._clusters.get_scoped(account_id, region, target["DBClusterIdentifier"])
        if not owner or owner.get("Engine") not in ("aurora", "aurora-mysql"):
            return None, None
    elif target.get("Engine") != "mysql" and resource_kind == "instance":
        return None, None
    if resource_kind == "cluster" and target.get("Engine") == "mysql":
        return None, None
    return target, owner


def _resource_id(target, owner, kind):
    return owner.get("DbClusterResourceId" if kind == "cluster" or owner is not target else "DbiResourceId")


def issue_capability(*, account_id, region, resource_kind, resource_identifier, reader_endpoint=False):
    """In-process only; rotates this binding's capability. Re-issue after a
    restart or a resource replacement."""
    if (resource_kind not in ("instance", "cluster") or not account_id or not region
            or type(reader_endpoint) is not bool or (reader_endpoint and resource_kind != "cluster")):
        raise ValueError("Invalid RDS target")
    with _lock:
        target, owner = _resources(account_id, region, resource_kind, resource_identifier)
        resource_id = _resource_id(target, owner, resource_kind) if owner is not None else None
        if not resource_id:
            raise ValueError("Invalid RDS target")
        for digest, binding in list(_bindings.items()):
            if (binding.account_id, binding.region, binding.resource_kind, binding.resource_identifier,
                binding.reader_endpoint) == (account_id, region, resource_kind, resource_identifier, reader_endpoint):
                del _bindings[digest]
        capability = secrets.token_hex(32)
        _bindings[hashlib.sha256(capability.encode()).digest()] = _Binding(
            account_id, region, resource_kind, resource_identifier, reader_endpoint, target, owner, resource_id,
        )
        return capability


def revoke_capability(capability):
    with _lock:
        _bindings.pop(hashlib.sha256(capability.encode()).digest(), None)


def reset():
    with _lock:
        _bindings.clear()


def _binding_current(binding):
    target, owner = _resources(binding.account_id, binding.region, binding.resource_kind,
                               binding.resource_identifier)
    return (target is binding.target and owner is binding.owner
            and _resource_id(target, owner, binding.resource_kind) == binding.resource_id
            and owner.get("IAMDatabaseAuthenticationEnabled") is True)


def _decision(capability, payload, auth_enabled):
    with _lock:
        binding = _bindings.get(hashlib.sha256(capability).digest())
        if binding is None:
            return False
        if not _binding_current(binding):
            return False
        if not auth_enabled:
            return True
        result = authorize_rds_iam_token(
            payload["token"], account_id=binding.account_id, region=binding.region,
            resource_kind=binding.resource_kind, resource_identifier=binding.resource_identifier,
            reader_endpoint=binding.reader_endpoint, db_user=payload["username"],
        )
        # RDS lifecycle workers do not hold the broker lock. Authorization
        # looks up state independently, so discard its result if the resource
        # or its owner was replaced or disabled while that lookup ran.
        return isinstance(result, AuthorizedRdsConnection) and _binding_current(binding)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate field")
        result[key] = value
    return result


async def _read_body(receive):
    body = bytearray()
    while True:
        message = await receive()
        if message["type"] != "http.request":
            raise ValueError("Disconnected")
        chunk = message.get("body", b"")
        if len(body) + len(chunk) > MAX_BODY:
            raise ValueError("Oversized request")
        body.extend(chunk)
        if not message.get("more_body", False):
            return bytes(body)


async def handle(scope, receive, send, *, auth_enabled):
    """POST {username, token} with X-Ministack-RDS-Capability. Never logs request
    data: transport or schema failures deny, even in permissive mode."""
    status, allowed = 403, False
    try:
        headers = {}
        for name, value in scope.get("headers", []):
            name = name.lower()
            if name in headers:
                raise ValueError("Duplicate header")
            headers[name] = value
        capability = headers.get(b"x-ministack-rds-capability", b"")
        if (scope["method"] != "POST" or scope.get("query_string")
                or len(capability) != 64 or any(c not in b"0123456789abcdef" for c in capability)
                or headers.get(b"content-type", b"").split(b";", 1)[0].strip().lower() != b"application/json"
                or b"content-encoding" in headers):
            raise ValueError("Invalid request")
        with _lock:
            if hashlib.sha256(capability).digest() not in _bindings:
                raise ValueError("Invalid capability")
        body = await asyncio.wait_for(_read_body(receive), timeout=BODY_TIMEOUT)
        payload = json.loads(body.decode("utf-8"), object_pairs_hook=_unique_object)
        if (not isinstance(payload, dict) or payload.keys() != {"username", "token"}
                or not isinstance(payload["username"], str) or not 1 <= len(payload["username"]) <= 256
                or "\x00" in payload["username"] or not isinstance(payload["token"], str)
                or len(payload["token"]) > 65536):
            raise ValueError("Invalid payload")
        allowed = _decision(capability, payload, auth_enabled)
        status = 200 if allowed else 403
    except Exception:
        # Even unexpected verifier failures must not leak tokens through logs
        # or turn a failed request into an allow response.
        pass
    await send({"type": "http.response.start", "status": status, "headers": [
        (b"content-type", b"application/json"), (b"cache-control", b"no-store"),
    ]})
    await send({"type": "http.response.body", "body": b'{"allowed":true}' if allowed else b'{"allowed":false}'})
