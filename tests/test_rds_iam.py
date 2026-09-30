# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Offline RDS tokens signed by boto3/botocore, not the verifier's helpers."""

import asyncio
import datetime as dt
import importlib
import json
import time
from types import SimpleNamespace
from urllib.parse import parse_qsl, urlencode, urlsplit

import boto3
import pytest
from botocore.auth import SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials

from ministack.core import rds_iam
from ministack.core.rds_iam import (
    AuthorizedRdsConnection,
    RdsIamTokenError,
    VerifiedRdsToken,
    authorize_rds_iam_token,
    verify_rds_iam_token,
)
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    get_account_id,
    get_region,
    request_scope,
)
from ministack.services import iam, rds, sts

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


# Stage 5 of #1744: resource-bound authorization.

OTHER_ACCOUNT = "210987654321"
AUTHZ_KEY = "test-rds-authorization-key"
AUTHZ_SECRET = "authorization-test-secret"
AUTHZ_USER = "AppUser"
AUTHZ_HOST = "db.example.com"
AUTHZ_ARN = f"arn:aws:rds-db:{REGION}:{ACCOUNT}:dbuser:db-TEST/{AUTHZ_USER}"
AUTHZ_ARGS = dict(account_id=ACCOUNT, region=REGION, resource_kind="instance",
                  resource_identifier="database", db_user=AUTHZ_USER)


def _authz_policy(resource=AUTHZ_ARN, effect="Allow", **extra):
    return {"Statement": [{"Effect": effect, "Action": "rds-db:connect", "Resource": resource, **extra}]}


def _authz_token(*, host=AUTHZ_HOST, port=3306, user=AUTHZ_USER, key=AUTHZ_KEY, secret=AUTHZ_SECRET, session_token=None):
    return boto3.client("rds", region_name=REGION, aws_access_key_id=key,
                        aws_secret_access_key=secret, aws_session_token=session_token).generate_db_auth_token(
        DBHostname=host, Port=port, DBUsername=user,
    )


class TestResourceBoundAuthorization:
    @pytest.fixture(autouse=True)
    def state(self, monkeypatch):
        for name in ("_users", "_access_keys", "_user_inline_policies", "_roles", "_groups",
                     "_group_inline_policies", "_policies"):
            monkeypatch.setattr(iam, name, AccountScopedDict())
        monkeypatch.setattr(rds, "_instances", AccountRegionScopedDict())
        monkeypatch.setattr(rds, "_clusters", AccountRegionScopedDict())
        monkeypatch.setattr(sts, "_sessions", {})
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "root-secret")
        monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
        iam._users.set_scoped(ACCOUNT, None, "alice", {
            "UserName": "alice", "Arn": f"arn:aws:iam::{ACCOUNT}:user/alice",
        })
        iam._access_keys.set_scoped(ACCOUNT, None, AUTHZ_KEY, {
            "UserName": "alice", "Status": "Active", "SecretAccessKey": AUTHZ_SECRET,
        })
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"connect": _authz_policy()})
        instance = dict(Engine="mysql", DbiResourceId="db-TEST", IAMDatabaseAuthenticationEnabled=True,
                        Endpoint={"Address": AUTHZ_HOST, "Port": 3306})
        rds._instances.set_scoped(ACCOUNT, REGION, "database", instance)
        return instance


    @pytest.mark.parametrize("auth", [None, "false", "true"])
    @pytest.mark.parametrize("enabled", [False, True])
    def test_strict_primitive_independent_of_auth(self, state, monkeypatch, auth, enabled):
        if auth is None:
            monkeypatch.delenv("AUTH", raising=False)
        else:
            monkeypatch.setenv("AUTH", auth)
        state["IAMDatabaseAuthenticationEnabled"] = enabled
        with request_scope(OTHER_ACCOUNT, "eu-west-1"):
            result = authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS)
            assert (get_account_id(), get_region()) == (OTHER_ACCOUNT, "eu-west-1")
        if enabled:
            assert isinstance(result, AuthorizedRdsConnection)
            assert result.resource_arn == AUTHZ_ARN
            assert AUTHZ_SECRET not in repr(result)
        else:
            assert result.code == "IAMDatabaseAuthenticationDisabled"


    @pytest.mark.parametrize("resource", [AUTHZ_ARN, AUTHZ_ARN.replace(AUTHZ_USER, "*"), "*"])
    def test_exact_and_wildcard_grants(self, resource):
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"grant": _authz_policy(resource)})
        assert isinstance(authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS), AuthorizedRdsConnection)


    @pytest.mark.parametrize("resource", [AUTHZ_ARN.replace(AUTHZ_USER, AUTHZ_USER.lower()), AUTHZ_ARN.replace("db-TEST", "db-OTHER_ACCOUNT"),
                                          AUTHZ_ARN.replace(ACCOUNT, OTHER_ACCOUNT), AUTHZ_ARN.replace(REGION, "eu-west-1"),
                                          AUTHZ_ARN.replace("db-TEST", "database")])
    def test_wrong_policy_resource_is_denied(self, resource):
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"grant": _authz_policy(resource)})
        assert authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS).code == "ImplicitDeny"


    def test_explicit_deny_overrides_allow(self):
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"allow": _authz_policy("*"), "deny": _authz_policy(effect="Deny")})
        assert authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS).code == "Deny"


    @pytest.mark.parametrize("exception,allowed", [(AUTHZ_ARN, True), (AUTHZ_ARN.replace(AUTHZ_USER, AUTHZ_USER.lower()), False)])
    def test_not_resource_respects_case(self, exception, allowed):
        deny = {"Statement": [{"Effect": "Deny", "Action": "rds-db:connect", "NotResource": exception}]}
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"allow": _authz_policy("*"), "deny": deny})
        result = authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS)
        assert isinstance(result, AuthorizedRdsConnection) is allowed


    def test_missing_policy_and_wrong_action_deny(self):
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {})
        assert authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS).code == "ImplicitDeny"
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"rds": _authz_policy(Action="rds:*")})
        assert authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS).code == "ImplicitDeny"


    @pytest.mark.parametrize("kind", ["group", "managed"])
    def test_policies_use_explicit_account_and_restore_context(self, kind):
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {})
        iam._user_inline_policies.set_scoped(OTHER_ACCOUNT, None, "alice", {"deny": _authz_policy("*", "Deny")})
        if kind == "group":
            iam._groups.set_scoped(ACCOUNT, None, "team", {"Users": ["alice"]})
            iam._group_inline_policies.set_scoped(ACCOUNT, None, "team", {"grant": _authz_policy()})
        else:
            arn = f"arn:aws:iam::{ACCOUNT}:policy/connect"
            iam._users.get_scoped(ACCOUNT, None, "alice")["AttachedPolicies"] = [arn]
            iam._policies.set_scoped(ACCOUNT, None, "connect", {
                "Arn": arn, "DefaultVersionId": "v1", "Versions": {"v1": {"Document": _authz_policy()}},
            })
        with request_scope(OTHER_ACCOUNT, "eu-west-1"):
            assert isinstance(authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS), AuthorizedRdsConnection)
            assert (get_account_id(), get_region()) == (OTHER_ACCOUNT, "eu-west-1")


    @pytest.mark.parametrize("override", [dict(host="other.example.com"), dict(port=3307), dict(user="appuser"),
                                         dict(secret="wrong"), dict(key="unknown")])
    def test_invalid_token_never_reaches_allow(self, override):
        assert not isinstance(authorize_rds_iam_token(_authz_token(**override), **AUTHZ_ARGS), AuthorizedRdsConnection)


    def test_inactive_key(self):
        iam._access_keys.get_scoped(ACCOUNT, None, AUTHZ_KEY)["Status"] = "Inactive"
        assert authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS).code == "InvalidCredentials"


    @pytest.mark.parametrize("field,value", [("account_id", OTHER_ACCOUNT), ("region", "eu-west-1"),
                                            ("resource_identifier", "missing")])
    def test_resource_lookup_does_not_fall_back_to_ambient_tenant(self, field, value):
        with request_scope(ACCOUNT, REGION):
            assert authorize_rds_iam_token(_authz_token(), **{**AUTHZ_ARGS, field: value}).code == "ResourceNotFound"


    @pytest.mark.parametrize("field,value", [("DbiResourceId", ""), ("DbiResourceId", "db-TEST/*"),
                                            ("Endpoint", {}), ("Endpoint", {"Address": AUTHZ_HOST})])
    def test_incomplete_resource_fails_closed(self, state, field, value):
        state[field] = value
        assert not isinstance(authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS), AuthorizedRdsConnection)


    @pytest.mark.parametrize("kind", ["instance", "cluster"])
    def test_unsupported_engine(self, state, kind):
        state["Engine"] = "postgres"
        rds._clusters.set_scoped(ACCOUNT, REGION, "database", state)
        assert authorize_rds_iam_token(_authz_token(), **{**AUTHZ_ARGS, "resource_kind": kind}).code == "UnsupportedEngine"


    @pytest.mark.parametrize("kind,reader", [("instance", False), ("cluster", False), ("cluster", True)])
    @pytest.mark.parametrize("enabled", [False, True])
    def test_aurora_uses_cluster_resource_and_flag(self, state, kind, reader, enabled):
        state.update(Engine="aurora-mysql", DBClusterIdentifier="database", IAMDatabaseAuthenticationEnabled=not enabled)
        cluster_arn = AUTHZ_ARN.replace("db-TEST", "cluster-TEST")
        rds._clusters.set_scoped(ACCOUNT, REGION, "database", {
            "Engine": "aurora-mysql", "DbClusterResourceId": "cluster-TEST",
            "IAMDatabaseAuthenticationEnabled": enabled, "Endpoint": AUTHZ_HOST,
            "ReaderEndpoint": "reader.example.com", "Port": 3306,
        })
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"grant": _authz_policy(cluster_arn)})
        result = authorize_rds_iam_token(_authz_token(host="reader.example.com" if reader else AUTHZ_HOST),
                                        **{**AUTHZ_ARGS, "resource_kind": kind}, reader_endpoint=reader)
        if enabled:
            assert result.resource_arn == cluster_arn
        else:
            assert result.code == "IAMDatabaseAuthenticationDisabled"


    def test_missing_parent_cluster_is_not_standalone(self, state):
        state.update(Engine="aurora-mysql", DBClusterIdentifier="missing")
        assert authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS).code == "ResourceNotFound"


    @pytest.mark.parametrize("session_account", [ACCOUNT, OTHER_ACCOUNT])
    def test_role_session_must_belong_to_resource_account(self, session_account):
        key = "test-rds-authorization-session-key"
        sts._sessions[key] = dict(Arn=f"arn:aws:sts::{session_account}:assumed-role/db-role/session",
                                 AccountId=session_account, SecretAccessKey=AUTHZ_SECRET, SessionToken="session-token",
                                 Expiration=time.time() + 600, PrincipalType="AssumedRole")
        iam._roles.set_scoped(session_account, None, "db-role", {"InlinePolicies": {"grant": _authz_policy()}})
        result = authorize_rds_iam_token(_authz_token(key=key, session_token="session-token"), **AUTHZ_ARGS)
        assert isinstance(result, AuthorizedRdsConnection) is (session_account == ACCOUNT)


    def test_root_still_requires_enabled_resource(self, state):
        signed = _authz_token(key="test", secret="root-secret")
        assert isinstance(authorize_rds_iam_token(signed, **AUTHZ_ARGS), AuthorizedRdsConnection)
        state["IAMDatabaseAuthenticationEnabled"] = False
        assert authorize_rds_iam_token(signed, **AUTHZ_ARGS).code == "IAMDatabaseAuthenticationDisabled"


    def test_ambient_policy_cannot_grant_access(self):
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {})
        iam._user_inline_policies.set_scoped(OTHER_ACCOUNT, None, "alice", {"grant": _authz_policy("*")})
        with request_scope(OTHER_ACCOUNT, "eu-west-1"):
            assert authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS).code == "ImplicitDeny"
            assert (get_account_id(), get_region()) == (OTHER_ACCOUNT, "eu-west-1")


    @pytest.mark.parametrize("region,allowed", [(REGION, True), ("eu-west-1", False)])
    def test_policy_conditions_use_target_region(self, region, allowed):
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {
            "grant": _authz_policy(Condition={"StringEquals": {"aws:RequestedRegion": region}}),
        })
        assert isinstance(authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS), AuthorizedRdsConnection) is allowed


    def test_cluster_dictionary_endpoint_uses_advertised_port(self):
        rds._clusters.set_scoped(ACCOUNT, REGION, "database", {
            "Engine": "aurora-mysql", "DbClusterResourceId": "cluster-TEST",
            "IAMDatabaseAuthenticationEnabled": True, "Endpoint": {"Address": AUTHZ_HOST, "Port": 13306},
            "Port": 3306,
        })
        iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"grant": _authz_policy(AUTHZ_ARN.replace("db-", "cluster-"))})
        args = {**AUTHZ_ARGS, "resource_kind": "cluster"}
        assert isinstance(authorize_rds_iam_token(_authz_token(port=13306), **args), AuthorizedRdsConnection)
        assert not isinstance(authorize_rds_iam_token(_authz_token(), **args), AuthorizedRdsConnection)


    def test_aurora_without_parent_cannot_use_instance_arn(self, state):
        state["Engine"] = "aurora-mysql"
        assert authorize_rds_iam_token(_authz_token(), **AUTHZ_ARGS).code == "InvalidTarget"


    @pytest.mark.parametrize("override", [dict(account_id=""), dict(region=""), dict(resource_kind="proxy"),
                                         dict(resource_identifier=""), dict(reader_endpoint=True),
                                         dict(reader_endpoint="false")])
    def test_invalid_target(self, override):
        assert authorize_rds_iam_token(_authz_token(), **{**AUTHZ_ARGS, **override}).code == "InvalidTarget"


app_module = importlib.import_module("ministack.app")


@pytest.fixture
def ep_state(monkeypatch):
    """Isolate the rds_iam's resource and IAM ep_state using the shared _authz_token inputs."""
    for name in ("_users", "_access_keys", "_user_inline_policies", "_roles", "_groups",
                 "_group_inline_policies", "_policies"):
        monkeypatch.setattr(iam, name, AccountScopedDict())
    monkeypatch.setattr(rds, "_instances", AccountRegionScopedDict())
    monkeypatch.setattr(rds, "_clusters", AccountRegionScopedDict())
    monkeypatch.setattr(sts, "_sessions", {})
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "root-secret")
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    iam._users.set_scoped(ACCOUNT, None, "alice", {
        "UserName": "alice", "Arn": f"arn:aws:iam::{ACCOUNT}:user/alice",
    })
    iam._access_keys.set_scoped(ACCOUNT, None, AUTHZ_KEY, {
        "UserName": "alice", "Status": "Active", "SecretAccessKey": AUTHZ_SECRET,
    })
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"connect": _authz_policy()})
    instance = dict(Engine="mysql", DbiResourceId="db-TEST", IAMDatabaseAuthenticationEnabled=True,
                    Endpoint={"Address": AUTHZ_HOST, "Port": 3306})
    rds._instances.set_scoped(ACCOUNT, REGION, "database", instance)
    return instance


@pytest.fixture(autouse=True)
def clean_broker():
    rds_iam.reset()
    yield
    rds_iam.reset()


def capability():
    return rds_iam.issue_capability(**{k: v for k, v in AUTHZ_ARGS.items() if k != "db_user"})


def request(cap, *, payload=None, body=None, headers=None, method="POST", query=b"", chunks=None):
    if body is None:
        body = json.dumps(payload if payload is not None else {"username": AUTHZ_USER, "token": _authz_token()}).encode()
    messages = iter(chunks or [{"type": "http.request", "body": body}])
    sent = []

    async def receive():
        return next(messages)

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "method": method, "path": rds_iam.PATH, "query_string": query,
             "headers": headers if headers is not None else [
                 (b"content-type", b"application/json"), (b"x-ministack-rds-capability", cap.encode()),
             ]}
    asyncio.run(app_module.app(scope, receive, send))
    return sent[0]["status"], json.loads(sent[1]["body"])


@pytest.mark.parametrize("auth", [False, True])
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("valid_token", [False, True])
def test_auth_and_enablement_matrix(ep_state, monkeypatch, auth, enabled, valid_token):
    monkeypatch.setattr(app_module, "AUTH", auth)
    ep_state["IAMDatabaseAuthenticationEnabled"] = enabled
    before = get_account_id(), get_region()
    result = request(capability(), payload={"username": AUTHZ_USER, "token": _authz_token() if valid_token else ""})
    allowed = enabled and (not auth or valid_token)
    assert result == (200 if allowed else 403, {"allowed": allowed})
    assert (get_account_id(), get_region()) == before


def test_permissive_never_calls_authorizer(ep_state, monkeypatch):
    monkeypatch.setattr(app_module, "AUTH", False)

    def unexpected(*args, **kwargs):
        pytest.fail("Permissive mode called IAM authorizer")

    monkeypatch.setattr(rds_iam, "authorize_rds_iam_token", unexpected)
    assert request(capability(), payload={"username": AUTHZ_USER, "token": "not-a-token"})[0] == 200


@pytest.mark.parametrize("auth", [False, True])
def test_missing_invalid_rotated_revoked_capabilities(ep_state, monkeypatch, auth):
    monkeypatch.setattr(app_module, "AUTH", auth)
    old = capability()
    new = capability()
    for cap in ("", "f" * 64, old):
        assert request(cap)[0] == 403
    assert request(new)[0] == 200
    rds_iam.revoke_capability(new)
    assert request(new)[0] == 403


@pytest.mark.parametrize("change", ["replace", "delete", "id", "reset"])
def test_stale_binding_cannot_authorize(ep_state, monkeypatch, change):
    monkeypatch.setattr(app_module, "AUTH", False)
    cap = capability()
    if change == "replace":
        rds._instances.set_scoped(ACCOUNT, REGION, "database", dict(ep_state))
    elif change == "delete":
        rds._instances.pop_scoped(ACCOUNT, REGION, "database")
    elif change == "id":
        ep_state["DbiResourceId"] = "db-NEW"
    else:
        rds_iam.reset()
    assert request(cap)[0] == 403


@pytest.mark.parametrize("body", [b"null", b"[]", b"{", b'{}', b'\xff',
    b'{"username":"x","username":"y","token":""}',
    b'{"username":"x","token":"","account_id":"123456789012"}',
    b'{"username":"","token":""}', b'{"username":"x","token":null}',
    b'{"username":"x","token":"","resource_identifier":"other"}'])
def test_malformed_payloads_denied_in_permissive_mode(ep_state, monkeypatch, body):
    monkeypatch.setattr(app_module, "AUTH", False)
    assert request(capability(), body=body) == (403, {"allowed": False})


def test_chunked_body_limit_and_disconnect(ep_state, monkeypatch):
    monkeypatch.setattr(app_module, "AUTH", False)
    cap = capability()
    assert request(cap, chunks=[{"type": "http.request", "body": b" " * rds_iam.MAX_BODY, "more_body": True},
                                {"type": "http.request", "body": b"x"}])[0] == 403
    assert request(cap, chunks=[{"type": "http.disconnect"}])[0] == 403
    assert request(cap, chunks=[{"type": "http.request", "body": b'{"username":"x",', "more_body": True},
                                {"type": "http.request", "body": b'"token":""}'}])[0] == 200


@pytest.mark.parametrize("kind", ["query", "get", "duplicate", "gzip"])
def test_transport_constraints(ep_state, monkeypatch, kind):
    monkeypatch.setattr(app_module, "AUTH", False)
    cap = capability()
    headers = [(b"content-type", b"application/json"), (b"x-ministack-rds-capability", cap.encode())]
    if kind == "duplicate":
        headers.append(headers[-1])
    if kind == "gzip":
        headers.append((b"content-encoding", b"gzip"))
    assert request(cap, headers=headers, query=b"token=secret" if kind == "query" else b"",
                   method="GET" if kind == "get" else "POST")[0] == 403


def test_timeout(ep_state, monkeypatch):
    monkeypatch.setattr(rds_iam, "BODY_TIMEOUT", 0.001)
    sent = []

    async def receive():
        await asyncio.sleep(1)

    async def send(message):
        sent.append(message)

    asyncio.run(rds_iam.handle({"method": "POST", "headers": [
        (b"content-type", b"application/json"), (b"x-ministack-rds-capability", capability().encode()),
    ]}, receive, send, auth_enabled=False))
    assert sent[0]["status"] == 403


def test_authorizer_failure_does_not_log_secrets(ep_state, monkeypatch, caplog):
    monkeypatch.setattr(app_module, "AUTH", True)
    cap = capability()

    def broken(*args, **kwargs):
        raise RuntimeError("secret-_authz_token " + cap)

    monkeypatch.setattr(rds_iam, "authorize_rds_iam_token", broken)
    assert request(cap, payload={"username": AUTHZ_USER, "token": "secret-token"}) == (403, {"allowed": False})
    assert cap not in caplog.text
    assert "secret-token" not in caplog.text


def test_aurora_parent_flag_and_replacement(ep_state, monkeypatch):
    monkeypatch.setattr(app_module, "AUTH", False)
    ep_state.update(Engine="aurora-mysql", DBClusterIdentifier="cluster")
    owner = {"Engine": "aurora-mysql", "DbClusterResourceId": "cluster-TEST",
             "IAMDatabaseAuthenticationEnabled": False}
    rds._clusters.set_scoped(ACCOUNT, REGION, "cluster", owner)
    cap = capability()
    assert request(cap)[0] == 403
    owner["IAMDatabaseAuthenticationEnabled"] = True
    assert request(cap)[0] == 200
    rds._clusters.set_scoped(ACCOUNT, REGION, "cluster", dict(owner))
    assert request(cap)[0] == 403


@pytest.mark.parametrize("auth", [False, True])
def test_policy_denial_only_enforced_when_auth_enabled(ep_state, monkeypatch, auth):
    monkeypatch.setattr(app_module, "AUTH", auth)
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {})
    assert request(capability())[0] == (403 if auth else 200)


def test_capability_selects_resource_even_with_identical_endpoint(ep_state, monkeypatch):
    monkeypatch.setattr(app_module, "AUTH", True)
    rds._instances.set_scoped(ACCOUNT, REGION, "other", {**ep_state, "DbiResourceId": "db-OTHER"})
    cap = rds_iam.issue_capability(account_id=ACCOUNT, region=REGION, resource_kind="instance",
                                  resource_identifier="other")
    assert request(cap)[0] == 403  # Token's user policy grants only db-TEST.


def test_revocation_during_body_read_fails_closed(ep_state, monkeypatch):
    monkeypatch.setattr(app_module, "AUTH", False)
    cap = capability()
    sent = []

    async def receive():
        rds_iam.revoke_capability(cap)
        return {"type": "http.request", "body": b'{"username":"x","token":""}'}

    async def send(message):
        sent.append(message)

    asyncio.run(rds_iam.handle({"method": "POST", "headers": [
        (b"content-type", b"application/json"), (b"x-ministack-rds-capability", cap.encode()),
    ]}, receive, send, auth_enabled=False))
    assert sent[0]["status"] == 403


@pytest.mark.parametrize("aurora", [False, True])
@pytest.mark.parametrize("change", ["target", "owner", "id", "disable", "delete"])
def test_binding_changed_during_authorization_fails_closed(ep_state, monkeypatch, aurora, change):
    monkeypatch.setattr(app_module, "AUTH", True)
    owner = ep_state
    if aurora:
        ep_state.update(Engine="aurora-mysql", DBClusterIdentifier="cluster")
        owner = {"Engine": "aurora-mysql", "DbClusterResourceId": "cluster-TEST",
                 "IAMDatabaseAuthenticationEnabled": True}
        rds._clusters.set_scoped(ACCOUNT, REGION, "cluster", owner)
    cap = capability()
    authorize = rds_iam.authorize_rds_iam_token
    decisions = []

    def mutate_then_authorize(*args, **kwargs):
        if change == "target":
            rds._instances.set_scoped(ACCOUNT, REGION, "database", dict(ep_state))
        elif change == "owner":
            store, identifier = (rds._clusters, "cluster") if aurora else (rds._instances, "database")
            store.set_scoped(ACCOUNT, REGION, identifier, dict(owner))
        elif change == "id":
            owner["DbClusterResourceId" if aurora else "DbiResourceId"] = "cluster-NEW" if aurora else "db-NEW"
        # For replacement/id mutations, the real authorizer can allow the new
        # resource. For disable/delete, mutate just after a real allow result.
        result = authorize(*args, **kwargs)
        decisions.append(result)
        if change == "disable":
            owner["IAMDatabaseAuthenticationEnabled"] = False
        elif change == "delete":
            rds._instances.pop_scoped(ACCOUNT, REGION, "database")
        return result

    monkeypatch.setattr(rds_iam, "authorize_rds_iam_token", mutate_then_authorize)
    assert request(cap, payload={"username": AUTHZ_USER, "token": _authz_token(key="test", secret="root-secret")}) == (
        403, {"allowed": False},
    )
    assert len(decisions) == 1
    assert isinstance(decisions[0], rds_iam.AuthorizedRdsConnection)
