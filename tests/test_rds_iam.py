# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Offline RDS tokens signed by boto3/botocore, not the verifier's helpers."""

import datetime as dt
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlencode, urlsplit

import boto3
import pytest
from botocore.auth import SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from ministack.core import rds_iam
from ministack.core.rds_iam import RdsIamTokenError, VerifiedRdsToken, verify_rds_iam_token
from ministack.core.responses import get_account_id
from ministack.services import iam, sts

ACCOUNT = "123456789012"
KEY = "AKIARDSVERIFIERTEST"
SECRET = "rds-verifier-test-secret"
HOST = "database.example.us-east-1.rds.amazonaws.com"
REGION = "us-east-1"
USER = "db-user"
TARGET = dict(hostname=HOST, port=3306, db_user=USER, region=REGION, account_id=ACCOUNT)


@pytest.fixture
def clock(monkeypatch):
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    monkeypatch.setattr("botocore.auth.get_current_datetime", lambda: now.replace(tzinfo=None))
    monkeypatch.setattr(rds_iam, "time", SimpleNamespace(time=lambda: now.timestamp()))
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "root-secret")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    iam._users.set_scoped(ACCOUNT, None, "alice", {
        "UserName": "alice", "UserId": "test-user-id",
        "Arn": f"arn:aws:iam::{ACCOUNT}:user/team/alice",
    })
    iam._access_keys.set_scoped(ACCOUNT, None, KEY, {
        "UserName": "alice", "Status": "Active", "SecretAccessKey": SECRET,
    })
    try:
        yield now.timestamp()
    finally:
        iam._access_keys.pop_scoped(ACCOUNT, None, KEY, None)
        iam._users.pop_scoped(ACCOUNT, None, "alice", None)
        sts._sessions.pop("ASIARDSVERIFIERTEST", None)


def sdk_token(*, key=KEY, secret=SECRET, session_token=None, user=USER, host=HOST, port=3306):
    client = boto3.client(
        "rds", region_name=REGION, aws_access_key_id=key,
        aws_secret_access_key=secret, aws_session_token=session_token,
    )
    return client.generate_db_auth_token(
        DBHostname=host, Port=port, DBUsername=user, Region=REGION,
    )


def signed_token(*, params=None, expires=900, service="rds-db", region=REGION, path="/", headers=None):
    query = {"Action": "connect", "DBUser": USER, **(params or {})}
    request = AWSRequest(
        method="GET", url=f"https://{HOST}:3306{path}?{urlencode(query)}", headers=headers,
    )
    SigV4QueryAuth(Credentials(KEY, SECRET), service, region, expires=expires).add_auth(request)
    return request.url.removeprefix("https://")


def change_query(token, name, value):
    parsed = urlsplit("//" + token)
    params = dict(parse_qsl(parsed.query))
    if value is None:
        params.pop(name, None)
    else:
        params[name] = value
    return f"{parsed.netloc}{parsed.path}?{urlencode(params)}"


def test_boto3_iam_token_resolves_identity_without_changing_tenant(clock):
    before = get_account_id()
    result = verify_rds_iam_token(sdk_token(), **TARGET)
    assert isinstance(result, VerifiedRdsToken)
    assert result.principal_arn == f"arn:aws:iam::{ACCOUNT}:user/team/alice"
    assert result.principal_name == "alice"
    assert result.account_id == ACCOUNT
    assert result.db_user == USER
    assert result.expires_at == clock + 900
    assert SECRET not in repr(result)
    assert get_account_id() == before


def test_root_path_may_be_implicit(clock):
    # Go's RDS signer can serialize an empty URL path. SigV4 canonicalizes
    # both this form and boto3's explicit slash to "/".
    token = sdk_token().replace("/?", "?")
    assert isinstance(verify_rds_iam_token(token, **TARGET), VerifiedRdsToken)


@pytest.mark.parametrize("port", [80, 443, 3306, 65535])
def test_sdk_port_binding_including_https_default(clock, port):
    token = sdk_token(port=port)
    assert isinstance(verify_rds_iam_token(
        token, **{**TARGET, "port": port},
    ), VerifiedRdsToken)
    other_port = 80 if port == 443 else 443
    tampered = token.replace(f":{port}/", f":{other_port}/", 1)
    assert isinstance(verify_rds_iam_token(
        tampered, **{**TARGET, "port": other_port},
    ), RdsIamTokenError)


@pytest.mark.parametrize("preserve_host_case", [False, True])
@pytest.mark.parametrize("port", [80, 443, 3306, 65535])
def test_sdk_host_case_variants(clock, preserve_host_case, port):
    if preserve_host_case:
        # The Go v2 signer signs the original Host. Supplying Host explicitly
        # gives botocore that same canonical request without reusing our code.
        request = AWSRequest(
            method="GET", url=f"https://{HOST.upper()}:{port}/?Action=connect&DBUser={USER}",
            headers={"host": HOST.upper() + ("" if port == 443 else f":{port}")},
        )
        SigV4QueryAuth(Credentials(KEY, SECRET), "rds-db", REGION, expires=900).add_auth(request)
        token = request.url.removeprefix("https://")
    else:
        token = sdk_token(host=HOST.upper(), port=port)
    assert isinstance(verify_rds_iam_token(token, **{**TARGET, "port": port}), VerifiedRdsToken)
    assert isinstance(verify_rds_iam_token(
        token, **{**TARGET, "hostname": "other.example.com", "port": port},
    ), RdsIamTokenError)


@pytest.mark.parametrize("user", ["MixedCase", "space + slash/percent%", "caf\u00e9"])
def test_sdk_username_encoding(clock, user):
    assert isinstance(verify_rds_iam_token(
        sdk_token(user=user), **{**TARGET, "db_user": user},
    ), VerifiedRdsToken)


@pytest.mark.parametrize("key", ["test", ACCOUNT])
def test_configured_and_numeric_root_credentials(clock, key):
    result = verify_rds_iam_token(sdk_token(key=key, secret="root-secret"), **TARGET)
    assert isinstance(result, VerifiedRdsToken)
    assert result.principal_type == "Root"


@pytest.mark.parametrize("principal", ["assumed-role/role/session", "user/team/alice"])
def test_sts_token_and_expiration_limit(clock, principal):
    arn_service = "sts" if principal.startswith("assumed-role/") else "iam"
    sts._sessions["ASIARDSVERIFIERTEST"] = {
        "Arn": f"arn:aws:{arn_service}::{ACCOUNT}:{principal}",
        "SecretAccessKey": SECRET, "SessionToken": "session+/=token",
        "Expiration": clock + 600, "AccountId": ACCOUNT,
    }
    result = verify_rds_iam_token(sdk_token(
        key="ASIARDSVERIFIERTEST", session_token="session+/=token",
    ), **TARGET)
    assert isinstance(result, VerifiedRdsToken)
    assert result.expires_at == clock + 600
    assert "session+/=token" not in repr(result)
    for token in (None, "wrong-token"):
        assert isinstance(verify_rds_iam_token(sdk_token(
            key="ASIARDSVERIFIERTEST", session_token=token,
        ), **TARGET), RdsIamTokenError)


@pytest.mark.parametrize("expiration", [None, "invalid", float("nan"), float("inf"), 0])
def test_incomplete_or_expired_sts_record_is_rejected(clock, expiration):
    sts._sessions["ASIARDSVERIFIERTEST"] = {
        "Arn": f"arn:aws:sts::{ACCOUNT}:assumed-role/role/session",
        "SecretAccessKey": SECRET, "SessionToken": "token", "Expiration": expiration,
    }
    assert isinstance(verify_rds_iam_token(sdk_token(
        key="ASIARDSVERIFIERTEST", session_token="token",
    ), **TARGET), RdsIamTokenError)


def test_unknown_inactive_and_wrong_account_credentials(clock):
    assert isinstance(verify_rds_iam_token(sdk_token(key="unknown"), **TARGET), RdsIamTokenError)
    assert isinstance(verify_rds_iam_token(
        sdk_token(), **{**TARGET, "account_id": "234567890123"},
    ), RdsIamTokenError)
    iam._access_keys.get_scoped(ACCOUNT, None, KEY)["Status"] = "Inactive"
    assert isinstance(verify_rds_iam_token(sdk_token(), **TARGET), RdsIamTokenError)


@pytest.mark.parametrize("field, value", [
    ("hostname", "different.example.com"), ("port", 3307), ("db_user", "DB-USER"),
    ("region", "us-west-2"), ("account_id", ""), ("port", 0),
])
def test_target_binding(clock, field, value):
    assert isinstance(verify_rds_iam_token(
        sdk_token(), **{**TARGET, field: value},
    ), RdsIamTokenError)


@pytest.mark.parametrize("options", [
    {"params": {"Action": "other"}}, {"params": {"DBUser": "other"}},
    {"params": {"Extra": "value"}}, {"expires": 0}, {"expires": 901},
    {"expires": -1}, {"service": "s3"}, {"region": "us-west-2"},
    {"path": "/other"}, {"headers": {"x-extra": "value"}},
])
def test_even_correct_signatures_must_obey_rds_contract(clock, options):
    assert isinstance(verify_rds_iam_token(signed_token(**options), **TARGET), RdsIamTokenError)


@pytest.mark.parametrize("name, value", [
    ("X-Amz-Algorithm", "AWS4-ECDSA-P256-SHA256"),
    ("X-Amz-SignedHeaders", "Host"), ("X-Amz-Date", "20260230T000000Z"),
    ("X-Amz-Date", "2026091T000000Z"), ("X-Amz-Expires", "1.0"),
    ("X-Amz-Expires", "+900"), ("X-Amz-Signature", "z" * 64),
    ("X-Amz-Signature", "\u00e9" * 64), ("X-Amz-Signature", "0" * 64),
    ("X-Amz-Credential", f"{KEY}/20200101/us-east-1/rds-db/aws4_request"),
    ("X-Amz-Credential", f"{KEY}/20260914/us-east-1/rds-db/other"),
])
def test_malformed_or_tampered_query(clock, name, value):
    result = verify_rds_iam_token(change_query(sdk_token(), name, value), **TARGET)
    assert isinstance(result, RdsIamTokenError)
    assert SECRET not in repr(result)


@pytest.mark.parametrize("name", sorted(rds_iam._REQUIRED_PARAMS))
def test_missing_or_duplicate_required_parameter(clock, name):
    token = sdk_token()
    assert isinstance(verify_rds_iam_token(change_query(token, name, None), **TARGET), RdsIamTokenError)
    assert isinstance(verify_rds_iam_token(token + f"&{name}=duplicate", **TARGET), RdsIamTokenError)


@pytest.mark.parametrize("transform", [
    lambda token: "https://" + token, lambda token: "user@" + token,
    lambda token: token + "#", lambda token: token + "#fragment",
    lambda token: "\n" + token, lambda token: token + "\x00",
    lambda token: token + "&Extra=%ZZ", lambda token: token + "&Extra=%FF",
    lambda token: token + "&Action", lambda token: token + "&dbuser=other",
    lambda token: token + "&X-Amz-Security-Token=",
    lambda token: token.replace("/?", "/../?"),
    lambda token: token.replace(":3306/", ":03306/"),
    lambda token: token + "&Extra=" + "a" * 65536,
])
def test_rejects_ambiguous_token_syntax(clock, transform):
    assert isinstance(verify_rds_iam_token(transform(sdk_token()), **TARGET), RdsIamTokenError)


@pytest.mark.parametrize("offset, expected", [
    (899, VerifiedRdsToken), (900, RdsIamTokenError), (901, RdsIamTokenError),
    (-300, VerifiedRdsToken), (-301, RdsIamTokenError),
])
def test_expiry_and_future_skew_boundaries(clock, monkeypatch, offset, expected):
    token = sdk_token()
    monkeypatch.setattr(rds_iam, "time", SimpleNamespace(time=lambda: clock + offset))
    assert isinstance(verify_rds_iam_token(token, **TARGET), expected)


@pytest.mark.parametrize("auth", ["false", "true"])
def test_verifier_is_strict_independently_of_auth_setting(clock, monkeypatch, auth):
    monkeypatch.setenv("AUTH", auth)
    assert isinstance(verify_rds_iam_token(sdk_token(), **TARGET), VerifiedRdsToken)
    assert isinstance(verify_rds_iam_token(sdk_token(secret="wrong"), **TARGET), RdsIamTokenError)
