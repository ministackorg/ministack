# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Offline stage-5 authorization using SDK tokens and real scoped policy stores."""

import time

import boto3
import pytest

from ministack.core.rds_iam_authorization import AuthorizedRdsConnection, authorize_rds_iam_token
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    get_account_id,
    get_region,
    request_scope,
)
from ministack.services import iam, rds, sts

ACCOUNT = "123456789012"
OTHER = "210987654321"
REGION = "us-east-1"
KEY = "test-rds-authorization-key"
SECRET = "authorization-test-secret"
USER = "AppUser"
HOST = "db.example.com"
ARN = f"arn:aws:rds-db:{REGION}:{ACCOUNT}:dbuser:db-TEST/{USER}"
ARGS = dict(account_id=ACCOUNT, region=REGION, resource_kind="instance",
            resource_identifier="database", db_user=USER)


def policy(resource=ARN, effect="Allow", **extra):
    return {"Statement": [{"Effect": effect, "Action": "rds-db:connect", "Resource": resource, **extra}]}


def token(*, host=HOST, port=3306, user=USER, key=KEY, secret=SECRET, session_token=None):
    return boto3.client("rds", region_name=REGION, aws_access_key_id=key,
                        aws_secret_access_key=secret, aws_session_token=session_token).generate_db_auth_token(
        DBHostname=host, Port=port, DBUsername=user,
    )


@pytest.fixture(autouse=True)
def state(monkeypatch):
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
    iam._access_keys.set_scoped(ACCOUNT, None, KEY, {
        "UserName": "alice", "Status": "Active", "SecretAccessKey": SECRET,
    })
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"connect": policy()})
    instance = dict(Engine="mysql", DbiResourceId="db-TEST", IAMDatabaseAuthenticationEnabled=True,
                    Endpoint={"Address": HOST, "Port": 3306})
    rds._instances.set_scoped(ACCOUNT, REGION, "database", instance)
    return instance


@pytest.mark.parametrize("auth", [None, "false", "true"])
@pytest.mark.parametrize("enabled", [False, True])
def test_strict_primitive_independent_of_auth(state, monkeypatch, auth, enabled):
    if auth is None:
        monkeypatch.delenv("AUTH", raising=False)
    else:
        monkeypatch.setenv("AUTH", auth)
    state["IAMDatabaseAuthenticationEnabled"] = enabled
    with request_scope(OTHER, "eu-west-1"):
        result = authorize_rds_iam_token(token(), **ARGS)
        assert (get_account_id(), get_region()) == (OTHER, "eu-west-1")
    if enabled:
        assert isinstance(result, AuthorizedRdsConnection)
        assert result.resource_arn == ARN
        assert SECRET not in repr(result)
    else:
        assert result.code == "IAMDatabaseAuthenticationDisabled"


@pytest.mark.parametrize("resource", [ARN, ARN.replace(USER, "*"), "*"])
def test_exact_and_wildcard_grants(resource):
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"grant": policy(resource)})
    assert isinstance(authorize_rds_iam_token(token(), **ARGS), AuthorizedRdsConnection)


@pytest.mark.parametrize("resource", [ARN.replace(USER, USER.lower()), ARN.replace("db-TEST", "db-OTHER"),
                                      ARN.replace(ACCOUNT, OTHER), ARN.replace(REGION, "eu-west-1"),
                                      ARN.replace("db-TEST", "database")])
def test_wrong_policy_resource_is_denied(resource):
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"grant": policy(resource)})
    assert authorize_rds_iam_token(token(), **ARGS).code == "ImplicitDeny"


def test_explicit_deny_overrides_allow():
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"allow": policy("*"), "deny": policy(effect="Deny")})
    assert authorize_rds_iam_token(token(), **ARGS).code == "Deny"


@pytest.mark.parametrize("exception,allowed", [(ARN, True), (ARN.replace(USER, USER.lower()), False)])
def test_not_resource_respects_case(exception, allowed):
    deny = {"Statement": [{"Effect": "Deny", "Action": "rds-db:connect", "NotResource": exception}]}
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"allow": policy("*"), "deny": deny})
    result = authorize_rds_iam_token(token(), **ARGS)
    assert isinstance(result, AuthorizedRdsConnection) is allowed


def test_missing_policy_and_wrong_action_deny():
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {})
    assert authorize_rds_iam_token(token(), **ARGS).code == "ImplicitDeny"
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"rds": policy(Action="rds:*")})
    assert authorize_rds_iam_token(token(), **ARGS).code == "ImplicitDeny"


@pytest.mark.parametrize("kind", ["group", "managed"])
def test_policies_use_explicit_account_and_restore_context(kind):
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {})
    iam._user_inline_policies.set_scoped(OTHER, None, "alice", {"deny": policy("*", "Deny")})
    if kind == "group":
        iam._groups.set_scoped(ACCOUNT, None, "team", {"Users": ["alice"]})
        iam._group_inline_policies.set_scoped(ACCOUNT, None, "team", {"grant": policy()})
    else:
        arn = f"arn:aws:iam::{ACCOUNT}:policy/connect"
        iam._users.get_scoped(ACCOUNT, None, "alice")["AttachedPolicies"] = [arn]
        iam._policies.set_scoped(ACCOUNT, None, "connect", {
            "Arn": arn, "DefaultVersionId": "v1", "Versions": {"v1": {"Document": policy()}},
        })
    with request_scope(OTHER, "eu-west-1"):
        assert isinstance(authorize_rds_iam_token(token(), **ARGS), AuthorizedRdsConnection)
        assert (get_account_id(), get_region()) == (OTHER, "eu-west-1")


@pytest.mark.parametrize("override", [dict(host="other.example.com"), dict(port=3307), dict(user="appuser"),
                                     dict(secret="wrong"), dict(key="unknown")])
def test_invalid_token_never_reaches_allow(override):
    assert not isinstance(authorize_rds_iam_token(token(**override), **ARGS), AuthorizedRdsConnection)


def test_inactive_key():
    iam._access_keys.get_scoped(ACCOUNT, None, KEY)["Status"] = "Inactive"
    assert authorize_rds_iam_token(token(), **ARGS).code == "InvalidCredentials"


@pytest.mark.parametrize("field,value", [("account_id", OTHER), ("region", "eu-west-1"),
                                        ("resource_identifier", "missing")])
def test_resource_lookup_does_not_fall_back_to_ambient_tenant(field, value):
    with request_scope(ACCOUNT, REGION):
        assert authorize_rds_iam_token(token(), **{**ARGS, field: value}).code == "ResourceNotFound"


@pytest.mark.parametrize("field,value", [("DbiResourceId", ""), ("DbiResourceId", "db-TEST/*"),
                                        ("Endpoint", {}), ("Endpoint", {"Address": HOST})])
def test_incomplete_resource_fails_closed(state, field, value):
    state[field] = value
    assert not isinstance(authorize_rds_iam_token(token(), **ARGS), AuthorizedRdsConnection)


@pytest.mark.parametrize("kind", ["instance", "cluster"])
def test_unsupported_engine(state, kind):
    state["Engine"] = "postgres"
    rds._clusters.set_scoped(ACCOUNT, REGION, "database", state)
    assert authorize_rds_iam_token(token(), **{**ARGS, "resource_kind": kind}).code == "UnsupportedEngine"


@pytest.mark.parametrize("kind,reader", [("instance", False), ("cluster", False), ("cluster", True)])
@pytest.mark.parametrize("enabled", [False, True])
def test_aurora_uses_cluster_resource_and_flag(state, kind, reader, enabled):
    state.update(Engine="aurora-mysql", DBClusterIdentifier="database", IAMDatabaseAuthenticationEnabled=not enabled)
    cluster_arn = ARN.replace("db-TEST", "cluster-TEST")
    rds._clusters.set_scoped(ACCOUNT, REGION, "database", {
        "Engine": "aurora-mysql", "DbClusterResourceId": "cluster-TEST",
        "IAMDatabaseAuthenticationEnabled": enabled, "Endpoint": HOST,
        "ReaderEndpoint": "reader.example.com", "Port": 3306,
    })
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"grant": policy(cluster_arn)})
    result = authorize_rds_iam_token(token(host="reader.example.com" if reader else HOST),
                                    **{**ARGS, "resource_kind": kind}, reader_endpoint=reader)
    if enabled:
        assert result.resource_arn == cluster_arn
    else:
        assert result.code == "IAMDatabaseAuthenticationDisabled"


def test_missing_parent_cluster_is_not_standalone(state):
    state.update(Engine="aurora-mysql", DBClusterIdentifier="missing")
    assert authorize_rds_iam_token(token(), **ARGS).code == "ResourceNotFound"


@pytest.mark.parametrize("session_account", [ACCOUNT, OTHER])
def test_role_session_must_belong_to_resource_account(session_account):
    key = "test-rds-authorization-session-key"
    sts._sessions[key] = dict(Arn=f"arn:aws:sts::{session_account}:assumed-role/db-role/session",
                             AccountId=session_account, SecretAccessKey=SECRET, SessionToken="session-token",
                             Expiration=time.time() + 600, PrincipalType="AssumedRole")
    iam._roles.set_scoped(session_account, None, "db-role", {"InlinePolicies": {"grant": policy()}})
    result = authorize_rds_iam_token(token(key=key, session_token="session-token"), **ARGS)
    assert isinstance(result, AuthorizedRdsConnection) is (session_account == ACCOUNT)


def test_root_still_requires_enabled_resource(state):
    signed = token(key="test", secret="root-secret")
    assert isinstance(authorize_rds_iam_token(signed, **ARGS), AuthorizedRdsConnection)
    state["IAMDatabaseAuthenticationEnabled"] = False
    assert authorize_rds_iam_token(signed, **ARGS).code == "IAMDatabaseAuthenticationDisabled"


def test_ambient_policy_cannot_grant_access():
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {})
    iam._user_inline_policies.set_scoped(OTHER, None, "alice", {"grant": policy("*")})
    with request_scope(OTHER, "eu-west-1"):
        assert authorize_rds_iam_token(token(), **ARGS).code == "ImplicitDeny"
        assert (get_account_id(), get_region()) == (OTHER, "eu-west-1")


@pytest.mark.parametrize("region,allowed", [(REGION, True), ("eu-west-1", False)])
def test_policy_conditions_use_target_region(region, allowed):
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {
        "grant": policy(Condition={"StringEquals": {"aws:RequestedRegion": region}}),
    })
    assert isinstance(authorize_rds_iam_token(token(), **ARGS), AuthorizedRdsConnection) is allowed


def test_cluster_dictionary_endpoint_uses_advertised_port():
    rds._clusters.set_scoped(ACCOUNT, REGION, "database", {
        "Engine": "aurora-mysql", "DbClusterResourceId": "cluster-TEST",
        "IAMDatabaseAuthenticationEnabled": True, "Endpoint": {"Address": HOST, "Port": 13306},
        "Port": 3306,
    })
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {"grant": policy(ARN.replace("db-", "cluster-"))})
    args = {**ARGS, "resource_kind": "cluster"}
    assert isinstance(authorize_rds_iam_token(token(port=13306), **args), AuthorizedRdsConnection)
    assert not isinstance(authorize_rds_iam_token(token(), **args), AuthorizedRdsConnection)


def test_aurora_without_parent_cannot_use_instance_arn(state):
    state["Engine"] = "aurora-mysql"
    assert authorize_rds_iam_token(token(), **ARGS).code == "InvalidTarget"


@pytest.mark.parametrize("override", [dict(account_id=""), dict(region=""), dict(resource_kind="proxy"),
                                     dict(resource_identifier=""), dict(reader_endpoint=True),
                                     dict(reader_endpoint="false")])
def test_invalid_target(override):
    assert authorize_rds_iam_token(token(), **{**ARGS, **override}).code == "InvalidTarget"
