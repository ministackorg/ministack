# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).

import asyncio
import time

import pytest

from ministack.core.aws_credentials import (
    AmbiguousAccessKeyError,
    CredentialResolutionError,
    ResolvedCredential,
    find_iam_access_key_account,
    resolve_credential,
)
from ministack.core.iam_evaluator import PrincipalInfo, resolve_principal
from ministack.services import iam as iam_svc
from ministack.services import sts as sts_svc


def test_resolve_root_credential_from_environment(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "configured-root")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "configured-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "configured-token")

    credential = resolve_credential(
        "configured-root", "123456789012", "configured-token"
    )

    assert isinstance(credential, ResolvedCredential)
    assert credential.secret_access_key == "configured-secret"
    assert credential.session_token == "configured-token"
    assert credential.principal_arn == "arn:aws:iam::123456789012:root"


def test_resolve_root_credential_treats_empty_environment_token_as_absent(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "configured-root")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "")

    credential = resolve_credential("configured-root", "123456789012", "")

    assert isinstance(credential, ResolvedCredential)
    assert credential.session_token is None


def test_resolve_numeric_root_accepts_optional_ambient_session_token(monkeypatch):
    account_id = "123456789012"
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "configured-root")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "configured-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "ambient-token")

    with_token = resolve_credential(account_id, account_id, "ambient-token")
    without_token = resolve_credential(account_id, account_id, "")
    wrong_token = resolve_credential(account_id, account_id, "wrong-token")

    assert isinstance(with_token, ResolvedCredential)
    assert with_token.session_token == "ambient-token"
    assert isinstance(without_token, ResolvedCredential)
    assert without_token.session_token is None
    assert isinstance(wrong_token, CredentialResolutionError)
    assert wrong_token.code == "InvalidToken"


def test_resolve_iam_credential_is_account_scoped_and_requires_active_status():
    access_key = "AKIATESTSCOPED00001"
    first_account = "111111111111"
    second_account = "222222222222"
    iam_svc._access_keys.set_scoped(first_account, None, access_key, {
        "AccessKeyId": access_key,
        "SecretAccessKey": "first-secret",
        "Status": "Active",
        "UserName": "first-user",
    })
    iam_svc._access_keys.set_scoped(second_account, None, access_key, {
        "AccessKeyId": access_key,
        "SecretAccessKey": "second-secret",
        "Status": "Inactive",
        "UserName": "second-user",
    })
    try:
        first = resolve_credential(access_key, first_account, "")
        second = resolve_credential(access_key, second_account, "")

        assert isinstance(first, ResolvedCredential)
        assert first.secret_access_key == "first-secret"
        assert first.principal_arn == (
            "arn:aws:iam::111111111111:user/first-user"
        )
        assert isinstance(second, CredentialResolutionError)
        assert second.code == "InvalidClientTokenId"
        with pytest.raises(AmbiguousAccessKeyError):
            find_iam_access_key_account(access_key)
    finally:
        iam_svc._access_keys.pop_scoped(first_account, None, access_key, None)
        iam_svc._access_keys.pop_scoped(second_account, None, access_key, None)


def test_resolve_sts_credential_checks_token_expiry_and_origin():
    access_key = "ASIATESTSESSION0001"
    account_id = "123456789012"
    sts_svc._sessions[access_key] = {
        "Arn": f"arn:aws:iam::{account_id}:user/alice",
        "UserId": "AIDAALICE",
        "SecretAccessKey": "session-secret",
        "SessionToken": "session-token",
        "Expiration": time.time() + 60,
        "AccountId": account_id,
        "PrincipalType": "User",
        "SourceAccessKeyId": "AKIAALICE",
    }
    try:
        credential = resolve_credential(access_key, account_id, "session-token")
        wrong = resolve_credential(access_key, account_id, "wrong-token")
        missing = resolve_credential(access_key, account_id, "")
        non_ascii = resolve_credential(access_key, account_id, "not-valid-☃")

        assert isinstance(credential, ResolvedCredential)
        assert credential.principal_type == "User"
        assert credential.principal_name == "alice"
        assert credential.source_access_key_id == "AKIAALICE"
        assert isinstance(wrong, CredentialResolutionError)
        assert wrong.code == "InvalidToken"
        assert isinstance(missing, CredentialResolutionError)
        assert missing.code == "InvalidToken"
        assert isinstance(non_ascii, CredentialResolutionError)
        assert non_ascii.code == "InvalidToken"

        sts_svc._sessions[access_key]["Expiration"] = time.time() - 1
        expired = resolve_credential(access_key, account_id, "session-token")
        assert isinstance(expired, CredentialResolutionError)
        assert expired.code == "ExpiredTokenException"
    finally:
        sts_svc._sessions.pop(access_key, None)


def test_find_iam_access_key_account_returns_unique_owner():
    access_key = "test-account-lookup-key"
    account_id = "123456789012"
    iam_svc._access_keys.set_scoped(account_id, None, access_key, {
        "AccessKeyId": access_key,
        "SecretAccessKey": "secret",
        "Status": "Active",
        "UserName": "alice",
    })
    try:
        assert find_iam_access_key_account(access_key) == account_id
    finally:
        iam_svc._access_keys.pop_scoped(account_id, None, access_key, None)




def test_ambiguous_iam_access_key_is_rejected_before_http_routing(monkeypatch):
    from ministack import app as app_mod
    from ministack.core.responses import get_account_id, set_request_account_id

    monkeypatch.setattr(app_mod, "AUTH", True)
    access_key = "test-ambiguous-http-key"
    accounts = ("000000000000", "123456789012")
    original_account = get_account_id()
    sent = []
    for account_id in accounts:
        iam_svc._access_keys.set_scoped(account_id, None, access_key, {
            "AccessKeyId": access_key,
            "SecretAccessKey": f"secret-{account_id}",
            "Status": "Active",
            "UserName": f"user-{account_id}",
        })
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [(b"host", b"s3.localhost")],
        "query_string": (
            f"X-Amz-Credential={access_key}/20260908/us-east-1/s3/aws4_request"
        ).encode(),
    }

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    try:
        asyncio.run(app_mod.app(scope, receive, send))

        assert sent[0]["type"] == "http.response.start"
        assert sent[0]["status"] == 403
        assert b"InvalidClientTokenId" in sent[1]["body"]
    finally:
        for account_id in accounts:
            iam_svc._access_keys.pop_scoped(account_id, None, access_key, None)
        set_request_account_id(original_account)




@pytest.mark.parametrize("auth_enabled", [False, True])
@pytest.mark.parametrize("ambiguous", [False, True])
def test_http_iam_routing_respects_auth_mode(monkeypatch, auth_enabled, ambiguous):
    from ministack import app as app_mod
    from ministack.core.responses import get_account_id, request_scope

    key = "test-routing-mode-key"
    owner = "123456789012"
    accounts = [owner, "234567890123"] if ambiguous else [owner]
    monkeypatch.setattr(app_mod, "AUTH", auth_enabled)
    monkeypatch.setenv("MINISTACK_ACCOUNT_ID", "000000000000")
    routed = []
    sent = []

    async def capture(*args):
        routed.append(get_account_id())
        return 200, {}, b"ok"

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    monkeypatch.setattr(app_mod, "_handle_pre_body_request", capture)
    for account in accounts:
        iam_svc._access_keys.set_scoped(account, None, key, {"UserName": "alice"})
    try:
        with request_scope("000000000000", "us-east-1"):
            asyncio.run(app_mod.app({
                "type": "http", "method": "GET", "path": "/",
                "headers": [(b"host", b"sts.localhost"), (
                    b"authorization",
                    f"AWS4-HMAC-SHA256 Credential={key}/20260911/us-east-1/sts/aws4_request".encode(),
                )],
                "query_string": b"",
            }, receive, send))
        assert sent[0]["status"] == (403 if auth_enabled and ambiguous else 200)
        assert routed == ([] if auth_enabled and ambiguous else [
            owner if auth_enabled else "000000000000"
        ])
    finally:
        for account in accounts:
            iam_svc._access_keys.pop_scoped(account, None, key, None)


@pytest.mark.parametrize("auth_enabled", [False, True])
@pytest.mark.parametrize("bad_token", [False, True])
def test_s3_presign_verifies_in_both_auth_modes(monkeypatch, auth_enabled, bad_token):
    from urllib.parse import parse_qs, urlsplit

    import boto3
    from botocore.config import Config

    from ministack import app as app_mod
    from ministack.core.responses import get_account_id, request_scope
    from ministack.services import s3 as s3_svc

    key = "test-presign-mode-key"
    owner = "123456789012"
    monkeypatch.setattr(app_mod, "AUTH", auth_enabled)
    iam_svc._access_keys.set_scoped(owner, None, key, {
        "UserName": "alice", "Status": "Active", "SecretAccessKey": "secret",
    })
    client = boto3.client(
        "s3", endpoint_url="http://localhost:4566", region_name="us-east-1",
        aws_access_key_id=key, aws_secret_access_key="secret",
        aws_session_token="unexpected-token" if bad_token else None,
        config=Config(signature_version="s3v4"),
    )
    url = urlsplit(client.generate_presigned_url(
        "get_object", Params={"Bucket": "test-bucket", "Key": "object"},
    ))
    try:
        with request_scope("000000000000", "us-east-1"):
            error = s3_svc._verify_presigned_sigv4(
                "GET", url.path, {"host": url.netloc}, parse_qs(url.query),
            )
            assert get_account_id() == owner
            if bad_token:
                assert error[0] == 403
            else:
                assert error is None
    finally:
        iam_svc._access_keys.pop_scoped(owner, None, key, None)


@pytest.mark.parametrize("auth_enabled", [False, True])
@pytest.mark.parametrize("action", ["GetSessionToken", "AssumeRole"])
@pytest.mark.parametrize("credential_kind", ["unknown", "inactive", "expired"])
def test_sts_credential_rejections_require_auth(monkeypatch, auth_enabled, action, credential_kind):
    from ministack import app as app_mod
    from ministack.core.responses import request_scope

    key = "test-sts-mode-key"
    account = "000000000000"
    monkeypatch.setattr(app_mod, "AUTH", auth_enabled)
    if credential_kind == "inactive":
        iam_svc._access_keys.set_scoped(account, None, key, {
            "UserName": "alice", "Status": "Inactive", "SecretAccessKey": "secret",
        })
    if credential_kind == "expired":
        sts_svc._sessions[key] = {
            "Arn": f"arn:aws:sts::{account}:assumed-role/role/session",
            "UserId": "role:session", "SecretAccessKey": "secret",
            "Expiration": time.time() - 60,
        }
    previous = set(sts_svc._sessions)
    try:
        with request_scope(account, "us-east-1"):
            status, _, _ = asyncio.run(sts_svc.handle_request(
                "GET", "/", {
                    "authorization": f"AWS4-HMAC-SHA256 Credential={key}/20260911/us-east-1/sts/aws4_request",
                }, b"", {
                    "Action": [action], "RoleArn": [f"arn:aws:iam::{account}:role/role"],
                    "RoleSessionName": ["session"],
                },
            ))
        assert status == (403 if auth_enabled else 200)
    finally:
        iam_svc._access_keys.pop_scoped(account, None, key, None)
        for created in set(sts_svc._sessions) - previous:
            sts_svc._sessions.pop(created, None)
        sts_svc._sessions.pop(key, None)


def test_presigned_mrap_resolves_alias_in_iam_owner_account(monkeypatch):
    from urllib.parse import parse_qs, urlsplit

    from botocore.auth import S3SigV4QueryAuth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    from ministack import app as app_mod
    from ministack.core.responses import get_account_id, request_scope
    from ministack.services import s3 as s3_svc

    key, owner, alias = "test-mrap-owner-key", "123456789012", "testalias.mrap"
    host = f"{alias}.accesspoint.s3-global.amazonaws.com"
    monkeypatch.setattr(app_mod, "AUTH", False)
    request = AWSRequest(method="GET", url=f"http://{host}/object")
    S3SigV4QueryAuth(Credentials(key, "secret"), "s3", "us-east-1").add_auth(request)
    url = urlsplit(request.url)
    routed = []

    async def capture(method, path, headers, body, query_params, **kwargs):
        routed.append((get_account_id(), path))
        return 200, {}, b"object"

    monkeypatch.setattr(s3_svc, "handle_request", capture)
    iam_svc._access_keys.set_scoped(owner, None, key, {
        "UserName": "alice", "Status": "Active", "SecretAccessKey": "secret",
    })
    s3_svc._mraps.set_scoped(owner, None, alias, {"Regions": ["member-bucket"]})
    try:
        with request_scope("000000000000", "us-east-1"):
            result = asyncio.run(app_mod._handle_s3_vhost_request(
                host, url.path, "GET", {"host": host}, b"", parse_qs(url.query),
            ))
        assert result[0] == 200
        assert routed == [(owner, "/member-bucket/object")]
    finally:
        iam_svc._access_keys.pop_scoped(owner, None, key, None)
        s3_svc._mraps.pop_scoped(owner, None, alias, None)


def test_resolve_get_session_token_principal_retains_user_policies():
    access_key = "test-session-access-key"
    account_id = "123456789012"
    user_name = "alice"
    iam_svc._users.set_scoped(account_id, None, user_name, {
        "UserName": user_name,
        "UserId": "AIDAALICE",
        "AttachedPolicies": [],
    })
    iam_svc._user_inline_policies[user_name] = {
        "allow-s3": {
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "*",
            }],
        },
    }
    sts_svc._sessions[access_key] = {
        "Arn": f"arn:aws:iam::{account_id}:user/team/{user_name}",
        "UserId": "AIDAALICE",
        "SecretAccessKey": "session-secret",
        "SessionToken": "session-token",
        "Expiration": time.time() + 60,
        "AccountId": account_id,
        "PrincipalType": "User",
        "PrincipalName": user_name,
        "SourceAccessKeyId": "AKIAALICE",
    }
    try:
        principal = resolve_principal(access_key, account_id)

        assert isinstance(principal, PrincipalInfo)
        assert principal.type == "User"
        assert principal.arn == (
            f"arn:aws:iam::{account_id}:user/team/{user_name}"
        )
        assert principal.policies
        assert principal.policies[0][0].actions == ["s3:GetObject"]
    finally:
        sts_svc._sessions.pop(access_key, None)
        iam_svc._user_inline_policies.pop(user_name, None)
        iam_svc._users.pop_scoped(account_id, None, user_name, None)
