# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Resolve AWS access keys to the credentials and principals MiniStack issued."""

import hmac
import os
import re
import time
from dataclasses import dataclass

from ministack.core.responses import get_account_id

_ACCOUNT_ID_RE = re.compile(r"^\d{12}$")
_SESSION_TOKEN_NOT_CHECKED = object()


@dataclass(frozen=True)
class ResolvedCredential:
    access_key_id: str
    secret_access_key: str
    account_id: str
    principal_arn: str
    principal_id: str
    principal_type: str
    principal_name: str = ""
    session_token: str | None = None
    expiration: float | None = None
    source_access_key_id: str | None = None


@dataclass(frozen=True)
class CredentialResolutionError:
    code: str
    message: str


class AmbiguousAccessKeyError(ValueError):
    """Raised when more than one tenant owns the same IAM access key."""


def is_root_access_key(access_key_id: str) -> bool:
    """Return whether a key represents MiniStack's configured root caller."""
    configured_key = os.environ.get("AWS_ACCESS_KEY_ID", "test")
    return (
        not access_key_id
        or access_key_id == "test"
        or access_key_id == configured_key
        or bool(_ACCOUNT_ID_RE.fullmatch(access_key_id))
    )


def _unknown_access_key() -> CredentialResolutionError:
    return CredentialResolutionError(
        "UnrecognizedClientException",
        "The security token included in the request is invalid.",
    )


def _invalid_session_token() -> CredentialResolutionError:
    return CredentialResolutionError(
        "InvalidToken",
        "The provided token is malformed or otherwise invalid.",
    )


def _account_from_arn(arn: str) -> str | None:
    parts = arn.split(":")
    if len(parts) > 4 and _ACCOUNT_ID_RE.fullmatch(parts[4]):
        return parts[4]
    return None


def _principal_type(arn: str, recorded_type: str) -> str:
    if recorded_type:
        return recorded_type
    if ":assumed-role/" in arn:
        return "AssumedRole"
    if ":user/" in arn:
        return "User"
    if arn.endswith(":root"):
        return "Root"
    return ""


def _validate_session_token(expected: str | None, supplied) -> CredentialResolutionError | None:
    if supplied is _SESSION_TOKEN_NOT_CHECKED:
        return None
    supplied = supplied or ""
    if expected is None:
        return _invalid_session_token() if supplied else None
    if not isinstance(expected, str) or not isinstance(supplied, str) or not hmac.compare_digest(
        expected.encode("utf-8"), supplied.encode("utf-8")
    ):
        return _invalid_session_token()
    return None


def find_iam_access_key_account(access_key_id: str) -> str | None:
    """Return the sole account that owns an IAM access key, if known.

    AWS access key IDs are globally unique. Duplicate emulator records raise
    instead of selecting either tenant.
    """
    if is_root_access_key(access_key_id):
        return None

    from ministack.services import sts as sts_svc

    if access_key_id in sts_svc._sessions:
        return None

    from ministack.services import iam as iam_svc

    accounts = {
        account_id
        for (account_id, stored_key) in iam_svc._access_keys.to_dict()
        if stored_key == access_key_id
    }
    if len(accounts) > 1:
        raise AmbiguousAccessKeyError(access_key_id)
    if accounts:
        return accounts.pop()
    return None


def resolve_credential(
    access_key_id: str,
    account_id: str | None = None,
    session_token=_SESSION_TOKEN_NOT_CHECKED,
) -> ResolvedCredential | CredentialResolutionError:
    """Resolve a root, IAM, or STS key in an explicit account.

    Passing ``session_token`` also validates whether the caller supplied the
    exact token attached to temporary credentials. Omitting it leaves token
    validation to the caller, which keeps policy-only identity lookups usable.
    """
    account_id = account_id or get_account_id()

    if is_root_access_key(access_key_id):
        if _ACCOUNT_ID_RE.fullmatch(access_key_id):
            if account_id != access_key_id:
                return _unknown_access_key()
            account_id = access_key_id
        configured_key = os.environ.get("AWS_ACCESS_KEY_ID", "test")
        configured_token = os.environ.get("AWS_SESSION_TOKEN") or None
        numeric_token_supplied = (
            bool(_ACCOUNT_ID_RE.fullmatch(access_key_id))
            and isinstance(session_token, str)
            and bool(session_token)
        )
        expected_token = (
            configured_token
            if access_key_id == configured_key or numeric_token_supplied
            else None
        )
        token_error = _validate_session_token(expected_token, session_token)
        if token_error:
            return token_error
        return ResolvedCredential(
            access_key_id=access_key_id,
            secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
            session_token=expected_token,
            account_id=account_id,
            principal_arn=f"arn:aws:iam::{account_id}:root",
            principal_id=account_id,
            principal_type="Root",
            source_access_key_id=access_key_id,
        )

    from ministack.services import sts as sts_svc

    session = sts_svc._sessions.get(access_key_id)
    if session is not None:
        principal_arn = str(session.get("Arn", ""))
        session_account = session.get("AccountId") or _account_from_arn(principal_arn)
        if session_account and session_account != account_id:
            return _unknown_access_key()
        expiration = session.get("Expiration")
        if isinstance(expiration, (int, float)) and time.time() > expiration:
            return CredentialResolutionError(
                "ExpiredTokenException",
                "The security token included in the request is expired",
            )
        expected_token = session.get("SessionToken")
        token_error = _validate_session_token(expected_token, session_token)
        if token_error:
            return token_error
        secret = session.get("SecretAccessKey")
        principal_type = _principal_type(principal_arn, session.get("PrincipalType", ""))
        if not secret or not principal_type:
            return _unknown_access_key()
        principal_name = ""
        if principal_type == "User" and ":user/" in principal_arn:
            principal_name = str(session.get("PrincipalName") or "")
            if not principal_name:
                principal_name = principal_arn.rsplit("/", 1)[-1]
        return ResolvedCredential(
            access_key_id=access_key_id,
            secret_access_key=secret,
            session_token=expected_token,
            expiration=expiration if isinstance(expiration, (int, float)) else None,
            account_id=session_account or account_id,
            principal_arn=principal_arn,
            principal_id=str(session.get("UserId", "")),
            principal_type=principal_type,
            principal_name=principal_name,
            source_access_key_id=session.get("SourceAccessKeyId"),
        )

    from ministack.services import iam as iam_svc

    key_record = iam_svc._access_keys.get_scoped(account_id, None, access_key_id)
    if key_record is None:
        return _unknown_access_key()
    if key_record.get("Status") != "Active":
        return CredentialResolutionError(
            "InvalidClientTokenId",
            "The security token included in the request is invalid.",
        )
    token_error = _validate_session_token(None, session_token)
    if token_error:
        return token_error
    secret = key_record.get("SecretAccessKey")
    if not secret:
        return _unknown_access_key()
    user_name = str(key_record.get("UserName", ""))
    user = iam_svc._users.get_scoped(account_id, None, user_name) or {}
    principal_arn = str(
        user.get("Arn") or f"arn:aws:iam::{account_id}:user/{user_name}"
    )
    return ResolvedCredential(
        access_key_id=access_key_id,
        secret_access_key=secret,
        account_id=account_id,
        principal_arn=principal_arn,
        principal_id=str(user.get("UserId", "")),
        principal_type="User",
        principal_name=user_name,
        source_access_key_id=access_key_id,
    )
