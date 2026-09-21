# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Offline ASGI broker tests; no database or Docker resources required."""

import asyncio
import importlib
import json

import pytest
from test_rds_iam import ACCOUNT, AUTHZ_HOST, AUTHZ_KEY, AUTHZ_SECRET, REGION, _authz_policy
from test_rds_iam import AUTHZ_ARGS as ARGS
from test_rds_iam import AUTHZ_USER as USER
from test_rds_iam import _authz_token as token

from ministack.core import rds_iam_broker as broker
from ministack.core.responses import AccountRegionScopedDict, AccountScopedDict, get_account_id, get_region
from ministack.services import iam, rds, sts

app_module = importlib.import_module("ministack.app")


@pytest.fixture
def state(monkeypatch):
    """Isolate the broker's resource and IAM state using the shared token inputs."""
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
    broker.reset()
    yield
    broker.reset()


def capability():
    return broker.issue_capability(**{k: v for k, v in ARGS.items() if k != "db_user"})


def request(cap, *, payload=None, body=None, headers=None, method="POST", query=b"", chunks=None):
    if body is None:
        body = json.dumps(payload if payload is not None else {"username": USER, "token": token()}).encode()
    messages = iter(chunks or [{"type": "http.request", "body": body}])
    sent = []

    async def receive():
        return next(messages)

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "method": method, "path": broker.PATH, "query_string": query,
             "headers": headers if headers is not None else [
                 (b"content-type", b"application/json"), (b"x-ministack-rds-capability", cap.encode()),
             ]}
    asyncio.run(app_module.app(scope, receive, send))
    return sent[0]["status"], json.loads(sent[1]["body"])


@pytest.mark.parametrize("auth", [False, True])
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("valid_token", [False, True])
def test_auth_and_enablement_matrix(state, monkeypatch, auth, enabled, valid_token):
    monkeypatch.setattr(app_module, "AUTH", auth)
    state["IAMDatabaseAuthenticationEnabled"] = enabled
    before = get_account_id(), get_region()
    result = request(capability(), payload={"username": USER, "token": token() if valid_token else ""})
    allowed = enabled and (not auth or valid_token)
    assert result == (200 if allowed else 403, {"allowed": allowed})
    assert (get_account_id(), get_region()) == before


def test_permissive_never_calls_authorizer(state, monkeypatch):
    monkeypatch.setattr(app_module, "AUTH", False)

    def unexpected(*args, **kwargs):
        pytest.fail("Permissive mode called IAM authorizer")

    monkeypatch.setattr(broker, "authorize_rds_iam_token", unexpected)
    assert request(capability(), payload={"username": USER, "token": "not-a-token"})[0] == 200


@pytest.mark.parametrize("auth", [False, True])
def test_missing_invalid_rotated_revoked_capabilities(state, monkeypatch, auth):
    monkeypatch.setattr(app_module, "AUTH", auth)
    old = capability()
    new = capability()
    for cap in ("", "f" * 64, old):
        assert request(cap)[0] == 403
    assert request(new)[0] == 200
    broker.revoke_capability(new)
    assert request(new)[0] == 403


@pytest.mark.parametrize("change", ["replace", "delete", "id", "reset"])
def test_stale_binding_cannot_authorize(state, monkeypatch, change):
    monkeypatch.setattr(app_module, "AUTH", False)
    cap = capability()
    if change == "replace":
        rds._instances.set_scoped(ACCOUNT, REGION, "database", dict(state))
    elif change == "delete":
        rds._instances.pop_scoped(ACCOUNT, REGION, "database")
    elif change == "id":
        state["DbiResourceId"] = "db-NEW"
    else:
        broker.reset()
    assert request(cap)[0] == 403


@pytest.mark.parametrize("body", [b"null", b"[]", b"{", b'{}', b'\xff',
    b'{"username":"x","username":"y","token":""}',
    b'{"username":"x","token":"","account_id":"123456789012"}',
    b'{"username":"","token":""}', b'{"username":"x","token":null}',
    b'{"username":"x","token":"","resource_identifier":"other"}'])
def test_malformed_payloads_denied_in_permissive_mode(state, monkeypatch, body):
    monkeypatch.setattr(app_module, "AUTH", False)
    assert request(capability(), body=body) == (403, {"allowed": False})


def test_chunked_body_limit_and_disconnect(state, monkeypatch):
    monkeypatch.setattr(app_module, "AUTH", False)
    cap = capability()
    assert request(cap, chunks=[{"type": "http.request", "body": b" " * broker.MAX_BODY, "more_body": True},
                                {"type": "http.request", "body": b"x"}])[0] == 403
    assert request(cap, chunks=[{"type": "http.disconnect"}])[0] == 403
    assert request(cap, chunks=[{"type": "http.request", "body": b'{"username":"x",', "more_body": True},
                                {"type": "http.request", "body": b'"token":""}'}])[0] == 200


@pytest.mark.parametrize("kind", ["query", "get", "duplicate", "gzip"])
def test_transport_constraints(state, monkeypatch, kind):
    monkeypatch.setattr(app_module, "AUTH", False)
    cap = capability()
    headers = [(b"content-type", b"application/json"), (b"x-ministack-rds-capability", cap.encode())]
    if kind == "duplicate":
        headers.append(headers[-1])
    if kind == "gzip":
        headers.append((b"content-encoding", b"gzip"))
    assert request(cap, headers=headers, query=b"token=secret" if kind == "query" else b"",
                   method="GET" if kind == "get" else "POST")[0] == 403


def test_timeout(state, monkeypatch):
    monkeypatch.setattr(broker, "BODY_TIMEOUT", 0.001)
    sent = []

    async def receive():
        await asyncio.sleep(1)

    async def send(message):
        sent.append(message)

    asyncio.run(broker.handle({"method": "POST", "headers": [
        (b"content-type", b"application/json"), (b"x-ministack-rds-capability", capability().encode()),
    ]}, receive, send, auth_enabled=False))
    assert sent[0]["status"] == 403


def test_authorizer_failure_does_not_log_secrets(state, monkeypatch, caplog):
    monkeypatch.setattr(app_module, "AUTH", True)
    cap = capability()

    def broken(*args, **kwargs):
        raise RuntimeError("secret-token " + cap)

    monkeypatch.setattr(broker, "authorize_rds_iam_token", broken)
    assert request(cap, payload={"username": USER, "token": "secret-token"}) == (403, {"allowed": False})
    assert cap not in caplog.text
    assert "secret-token" not in caplog.text


def test_aurora_parent_flag_and_replacement(state, monkeypatch):
    monkeypatch.setattr(app_module, "AUTH", False)
    state.update(Engine="aurora-mysql", DBClusterIdentifier="cluster")
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
def test_policy_denial_only_enforced_when_auth_enabled(state, monkeypatch, auth):
    monkeypatch.setattr(app_module, "AUTH", auth)
    iam._user_inline_policies.set_scoped(ACCOUNT, None, "alice", {})
    assert request(capability())[0] == (403 if auth else 200)


def test_capability_selects_resource_even_with_identical_endpoint(state, monkeypatch):
    monkeypatch.setattr(app_module, "AUTH", True)
    rds._instances.set_scoped(ACCOUNT, REGION, "other", {**state, "DbiResourceId": "db-OTHER"})
    cap = broker.issue_capability(account_id=ACCOUNT, region=REGION, resource_kind="instance",
                                  resource_identifier="other")
    assert request(cap)[0] == 403  # Token's user policy grants only db-TEST.


def test_revocation_during_body_read_fails_closed(state, monkeypatch):
    monkeypatch.setattr(app_module, "AUTH", False)
    cap = capability()
    sent = []

    async def receive():
        broker.revoke_capability(cap)
        return {"type": "http.request", "body": b'{"username":"x","token":""}'}

    async def send(message):
        sent.append(message)

    asyncio.run(broker.handle({"method": "POST", "headers": [
        (b"content-type", b"application/json"), (b"x-ministack-rds-capability", cap.encode()),
    ]}, receive, send, auth_enabled=False))
    assert sent[0]["status"] == 403


@pytest.mark.parametrize("aurora", [False, True])
@pytest.mark.parametrize("change", ["target", "owner", "id", "disable", "delete"])
def test_binding_changed_during_authorization_fails_closed(state, monkeypatch, aurora, change):
    monkeypatch.setattr(app_module, "AUTH", True)
    owner = state
    if aurora:
        state.update(Engine="aurora-mysql", DBClusterIdentifier="cluster")
        owner = {"Engine": "aurora-mysql", "DbClusterResourceId": "cluster-TEST",
                 "IAMDatabaseAuthenticationEnabled": True}
        rds._clusters.set_scoped(ACCOUNT, REGION, "cluster", owner)
    cap = capability()
    authorize = broker.authorize_rds_iam_token
    decisions = []

    def mutate_then_authorize(*args, **kwargs):
        if change == "target":
            rds._instances.set_scoped(ACCOUNT, REGION, "database", dict(state))
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

    monkeypatch.setattr(broker, "authorize_rds_iam_token", mutate_then_authorize)
    assert request(cap, payload={"username": USER, "token": token(key="test", secret="root-secret")}) == (
        403, {"allowed": False},
    )
    assert len(decisions) == 1
    assert isinstance(decisions[0], broker.AuthorizedRdsConnection)
