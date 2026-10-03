"""Local role trust regressions using exact IAM user principals and role grants."""

import asyncio
import json
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

import pytest

from ministack.core.iam_evaluator import evaluate_trust_policy

ACCOUNT = "123456789012"
CALLER = f"arn:aws:iam::{ACCOUNT}:user/trust-deployer"
ROLE_NAME = "trust-deployment"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/{ROLE_NAME}"
ALLOW = {"Effect": "Allow", "Principal": {"AWS": CALLER}, "Action": "sts:AssumeRole"}
REQUIRED = {
    "StringEquals": {"sts:ExternalId": "deployment-id"},
    "StringLike": {"sts:RoleSessionName": ["deploy-?*", "release-?*"]},
}


def _document(*statements):
    return json.dumps({"Version": "2012-10-17", "Statement": list(statements)})


@pytest.mark.parametrize("deny_first", [False, True])
def test_trust_explicit_deny_overrides_allow_without_context(deny_first):
    deny = dict(ALLOW, Effect="Deny")
    statements = [deny, ALLOW] if deny_first else [ALLOW, deny]
    assert not evaluate_trust_policy(_document(*statements), CALLER)


def test_trust_required_external_id_without_context_is_denied():
    assert not evaluate_trust_policy(
        _document(dict(ALLOW, Condition={"StringEquals": {"sts:ExternalId": "deployment-id"}})),
        CALLER,
    )


@pytest.fixture(params=["query", "json"])
def trust_api(request, monkeypatch):
    from ministack import app as app_mod
    from ministack.core.responses import AccountScopedDict, set_request_account_id
    from ministack.services import iam, sts

    monkeypatch.setattr(app_mod, "AUTH", True)
    monkeypatch.setattr(sts, "_sessions", {})
    monkeypatch.setattr(iam, "_users", AccountScopedDict())
    monkeypatch.setattr(iam, "_roles", AccountScopedDict())
    monkeypatch.setattr(iam, "_access_keys", AccountScopedDict())
    set_request_account_id(ACCOUNT)
    protocol = request.param
    source_key = ""

    def call(service, action, **params):
        headers = {}
        if source_key and service == "sts":
            headers["authorization"] = (
                f"AWS4-HMAC-SHA256 Credential={source_key}/20260929/us-east-1/sts/aws4_request, "
                "SignedHeaders=host, Signature=unused"
            )
        if protocol == "json":
            target = "IAMService" if service == "iam" else "AWSSecurityTokenServiceV20110615"
            headers.update({"content-type": "application/x-amz-json-1.1", "x-amz-target": f"{target}.{action}"})
            body = json.dumps(params).encode()
        else:
            headers["content-type"] = "application/x-www-form-urlencoded"
            body = urlencode({"Action": action, **params}).encode()
        if service == "sts":
            return asyncio.run(app_mod._dispatch_service_request(
                "POST", "/", headers, body, {}, "trust-test-request",
            ))
        return asyncio.run(iam.handle_request("POST", "/", headers, body, {}))

    try:
        assert call("iam", "CreateUser", UserName="trust-deployer")[0] == 200
        status, _, body = call("iam", "CreateAccessKey", UserName="trust-deployer")
        assert status == 200
        source_key = ET.fromstring(body).findtext(".//{*}AccessKeyId")
        assert source_key
        assert call("iam", "PutUserPolicy", UserName="trust-deployer", PolicyName="assume",
                    PolicyDocument=_document({
                        "Effect": "Allow", "Action": "sts:AssumeRole", "Resource": ROLE_ARN,
                    }))[0] == 200
        yield call, sts._sessions, protocol
    finally:
        call("iam", "DeleteRole", RoleName=ROLE_NAME)
        if source_key:
            call("iam", "DeleteAccessKey", UserName="trust-deployer", AccessKeyId=source_key)
        call("iam", "DeleteUserPolicy", UserName="trust-deployer", PolicyName="assume")
        call("iam", "DeleteUser", UserName="trust-deployer")


def _assume(api, expected_status, **params):
    call, sessions, protocol = api
    before = dict(sessions)
    status, headers, body = call("sts", "AssumeRole", RoleArn=ROLE_ARN, **params)
    assert status == expected_status, body
    if status == 403:
        if protocol == "json":
            assert headers["Content-Type"].startswith("application/x-amz-json")
            assert headers["x-amzn-errortype"] == "AccessDenied"
            error = json.loads(body)
            assert error["__type"] == "AccessDenied"
            assert "sts:AssumeRole" in error["message"]
        else:
            assert "xml" in headers["Content-Type"]
            assert ET.fromstring(body).findtext(".//{*}Code") == "AccessDenied"
        assert b"Credentials" not in body
        assert sessions == before
    else:
        if protocol == "json":
            assert headers["Content-Type"].startswith("application/x-amz-json")
            access_key = json.loads(body)["Credentials"]["AccessKeyId"]
        else:
            access_key = ET.fromstring(body).findtext(".//{*}Credentials/{*}AccessKeyId")
        assert set(sessions) - set(before) == {access_key}
        assert sessions[access_key]["SourcePrincipalArn"] == CALLER
        assert sessions[access_key]["Arn"].endswith("/" + params["RoleSessionName"])


def _install_policy(api, operation, policy):
    call, _, _ = api
    initial = policy if operation == "CreateRole" else _document(ALLOW)
    assert call("iam", "CreateRole", RoleName=ROLE_NAME, AssumeRolePolicyDocument=initial)[0] == 200
    if operation == "UpdateAssumeRolePolicy":
        _assume(api, 200, RoleSessionName="before-update")
        assert call("iam", operation, RoleName=ROLE_NAME, PolicyDocument=policy)[0] == 200


@pytest.mark.parametrize("operation", ["CreateRole", "UpdateAssumeRolePolicy"])
@pytest.mark.parametrize("params, expected", [
    ({"ExternalId": "deployment-id", "RoleSessionName": "deploy-123"}, 200),
    ({"ExternalId": "deployment-id", "RoleSessionName": "release-123"}, 200),
    ({"ExternalId": "wrong", "RoleSessionName": "deploy-123"}, 403),
    ({"RoleSessionName": "deploy-123"}, 403),
    ({"ExternalId": "deployment-id", "RoleSessionName": "other-123"}, 403),
])
def test_trust_requires_external_id_and_session_name(trust_api, operation, params, expected):
    _install_policy(trust_api, operation, _document(dict(ALLOW, Condition=REQUIRED)))
    _assume(trust_api, expected, **params)


@pytest.mark.parametrize("operation", ["CreateRole", "UpdateAssumeRolePolicy"])
@pytest.mark.parametrize("deny_first", [False, True])
@pytest.mark.parametrize("deny_changes, expected", [
    ({}, 403),
    ({"Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:user/other"}}, 200),
    ({"Action": "sts:AssumeRoleWithSAML"}, 200),
    ({"Condition": {"StringEquals": {"sts:ExternalId": "deployment-id"}}}, 403),
    ({"Condition": {"StringEquals": {"sts:ExternalId": "other-id"}}}, 200),
    ({"Condition": {"StringLike": {"sts:RoleSessionName": "deploy-*"}}}, 403),
])
def test_trust_only_matching_denies_override_allow(trust_api, operation, deny_first, deny_changes, expected):
    deny = dict(ALLOW, Effect="Deny", **deny_changes)
    statements = [deny, ALLOW] if deny_first else [ALLOW, deny]
    _install_policy(trust_api, operation, _document(*statements))
    _assume(trust_api, expected, ExternalId="deployment-id", RoleSessionName="deploy-123")


def test_trust_update_replaces_conditions_and_deny(trust_api):
    call, _, _ = trust_api
    _install_policy(trust_api, "CreateRole", _document(dict(ALLOW, Condition=REQUIRED)))
    _assume(trust_api, 200, ExternalId="deployment-id", RoleSessionName="deploy-123")
    assert call("iam", "UpdateAssumeRolePolicy", RoleName=ROLE_NAME,
                PolicyDocument=_document(ALLOW, dict(ALLOW, Effect="Deny")))[0] == 200
    _assume(trust_api, 403, ExternalId="deployment-id", RoleSessionName="deploy-123")
    assert call("iam", "UpdateAssumeRolePolicy", RoleName=ROLE_NAME,
                PolicyDocument=_document(ALLOW))[0] == 200
    _assume(trust_api, 200, RoleSessionName="unrestricted")


def test_trust_condition_key_presence(trust_api):
    # Missing request values must remain absent, rather than becoming "".
    _install_policy(trust_api, "CreateRole", _document(dict(
        ALLOW, Condition={"Null": {"sts:ExternalId": "true"}},
    )))
    _assume(trust_api, 200, RoleSessionName="without-id")
    _assume(trust_api, 403, ExternalId="deployment-id", RoleSessionName="with-id")


@pytest.mark.parametrize("operation", ["CreateRole", "UpdateAssumeRolePolicy"])
@pytest.mark.parametrize("deny_first", [False, True])
@pytest.mark.parametrize("excluded, condition, expected", [
    ("sts:AssumeRoleWithSAML", {}, 403),
    (["sts:AssumeRoleWithSAML", "sts:AssumeRoleWithWebIdentity"], {}, 403),
    (["sts:AssumeRoleWithSAML", "STS:Assume*"], {}, 200),
    ("sts:AssumeRoleWithSAML", {"StringEquals": {"sts:ExternalId": "other-id"}}, 200),
])
def test_trust_not_action_denies(trust_api, operation, deny_first, excluded, condition, expected):
    deny = {"Effect": "Deny", "Principal": {"AWS": CALLER},
            "NotAction": excluded, "Condition": condition}
    statements = [deny, ALLOW] if deny_first else [ALLOW, deny]
    _install_policy(trust_api, operation, _document(*statements))
    _assume(trust_api, expected, ExternalId="deployment-id", RoleSessionName="deploy-123")


@pytest.mark.parametrize("operation", ["CreateRole", "UpdateAssumeRolePolicy"])
@pytest.mark.parametrize("policy", [
    _document(ALLOW, dict(ALLOW, Effect="Deny")),
    _document(dict(ALLOW, Condition=REQUIRED)),
])
def test_trust_denies_and_conditions_remain_permissive_without_auth(trust_api, monkeypatch, operation, policy):
    from ministack import app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", False)
    _install_policy(trust_api, operation, policy)
    _assume(trust_api, 200, RoleSessionName="unrestricted")
