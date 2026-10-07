"""Exercise Logs tag conditions through the ASGI authorization boundary."""

import asyncio
import json

import pytest

from ministack import app as app_mod
from ministack.services import cloudwatch_logs as logs
from ministack.services import iam


@pytest.fixture
def logs_auth(monkeypatch):
    monkeypatch.setattr(app_mod, "AUTH", True)
    account, region = "123456789012", "us-east-1"
    user, key = "logs-tag-context", "AKIALOGSTAGCONTEXT"
    iam._users.set_scoped(account, None, user, {"UserName": user, "AttachedPolicies": []})
    iam._access_keys.set_scoped(account, None, key, {
        "AccessKeyId": key, "SecretAccessKey": "test", "Status": "Active", "UserName": user,
    })
    created = []

    def call(action, *, root=False, request_region=region, with_headers=False, raw_body=None, **payload):
        body = json.dumps(payload).encode() if raw_body is None else raw_body
        sent = []

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message):
            sent.append(message)

        scope = {
            "type": "http", "method": "POST", "path": "/", "query_string": b"",
            "headers": [
                (b"host", f"logs.{request_region}.amazonaws.com".encode()),
                (b"x-amz-target", f"Logs_20140328.{action}".encode()),
                (b"content-type", b"application/x-amz-json-1.1"),
                (b"authorization", (
                    f"AWS4-HMAC-SHA256 Credential={account if root else key}"
                    f"/20261006/{request_region}/logs/aws4_request"
                ).encode()),
            ],
        }
        asyncio.run(app_mod.app(scope, receive, send))
        response = sent[0]["status"], json.loads(sent[1]["body"])
        if with_headers:
            return *response, {name.lower(): value for name, value in sent[0]["headers"]}
        return response

    def create(name, tags=None, request_region=region):
        assert call("CreateLogGroup", root=True, request_region=request_region,
                    logGroupName=name, tags=tags or {})[0] == 200
        created.append((request_region, name))
        return f"arn:aws:logs:{request_region}:{account}:log-group:{name}"

    def policy(*statements):
        iam._user_inline_policies.set_scoped(account, None, user, {
            "context": {"Version": "2012-10-17", "Statement": list(statements)},
        })

    try:
        yield call, create, policy, account, region
    finally:
        iam._user_inline_policies.pop_scoped(account, None, user, None)
        iam._access_keys.pop_scoped(account, None, key, None)
        iam._users.pop_scoped(account, None, user, None)
        for request_region, name in created:
            logs._log_groups.pop_scoped(account, request_region, name, None)
        logs._log_groups.pop_scoped(account, region, "logs-context-create", None)


def assert_denied(response, *, explicit=False):
    status, body = response
    assert status == 400 and body["__type"] == "AccessDeniedException"
    if explicit:
        assert "explicit deny" in body["Message"]


@pytest.mark.parametrize("explicit", [False, True])
def test_authorization_denial_matches_live_logs_error_envelope(logs_auth, explicit):
    call, create, policy, _, _ = logs_auth
    arn = create("logs-context-denial-envelope", {"protect": "true"})
    if explicit:
        policy(
            {"Effect": "Allow", "Action": "logs:UntagResource", "Resource": arn},
            {"Effect": "Deny", "Action": "logs:UntagResource", "Resource": arn},
        )
    else:
        policy()
    status, body, headers = call("UntagResource", resourceArn=arn, tagKeys=["protect"], with_headers=True)
    assert_denied((status, body), explicit=explicit)
    assert f"on resource: {arn}" in body["Message"]
    if not explicit:
        assert "because no identity-based policy allows the logs:UntagResource action" in body["Message"]
    assert headers[b"content-type"] == b"application/x-amz-json-1.1"
    assert b"x-amzn-errortype" not in headers
    assert "message" not in body


@pytest.mark.parametrize("action", ["PutRetentionPolicy", "ListTagsLogGroup", "ListTagsForResource"])
def test_owner_tag_allows_only_matching_existing_group(logs_auth, action):
    call, create, policy, _, _ = logs_auth
    for owner in ("team-a", "team-b", None):
        name = f"logs-context-owner-{owner}"
        arn = create(name, {"owner": owner} if owner else {})
        policy({"Effect": "Allow", "Action": f"logs:{action}", "Resource": arn + "*",
                "Condition": {"StringEquals": {"aws:ResourceTag/owner": "team-a"}}})
        params = {"resourceArn": arn} if action == "ListTagsForResource" else {"logGroupName": name}
        if action == "PutRetentionPolicy":
            params["retentionInDays"] = 7
        response = call(action, **params)
        if owner == "team-a":
            assert response[0] == 200
        else:
            assert_denied(response)


@pytest.mark.parametrize("legacy", [False, True])
def test_protect_tag_deny_prevents_removal_without_blocking_other_tags(logs_auth, legacy):
    call, create, policy, _, _ = logs_auth
    name = "logs-context-protected"
    arn = create(name, {"protect": "true", "harmless": "yes"})
    action = "UntagLogGroup" if legacy else "UntagResource"
    policy(
        {"Effect": "Allow", "Action": f"logs:{action}", "Resource": "*"},
        {"Effect": "Deny", "Action": f"logs:{action}", "Resource": "*",
         "Condition": {"StringEquals": {"aws:ResourceTag/protect": "true"},
                       "ForAnyValue:StringEquals": {"aws:TagKeys": ["protect"]}}},
    )
    target = {"logGroupName": name} if legacy else {"resourceArn": arn}
    field = "tags" if legacy else "tagKeys"
    assert_denied(call(action, **target, **{field: ["harmless", "protect"]}), explicit=True)
    assert call("ListTagsForResource", root=True, resourceArn=arn) == (
        200, {"tags": {"protect": "true", "harmless": "yes"}},
    )
    assert call(action, **target, **{field: ["harmless"]})[0] == 200
    assert call("ListTagsForResource", root=True, resourceArn=arn) == (200, {"tags": {"protect": "true"}})


@pytest.mark.parametrize("action", ["CreateLogGroup", "TagLogGroup", "TagResource"])
def test_request_tags_and_keys_use_the_operation_payload(logs_auth, action):
    call, create, policy, _, _ = logs_auth
    name = "logs-context-create"
    arn = f"arn:aws:logs:us-east-1:123456789012:log-group:{name}"
    if action != "CreateLogGroup":
        create(name, {"owner": "team-a"})
    policy({"Effect": "Allow", "Action": f"logs:{action}", "Resource": arn + "*",
            "Condition": {"StringEquals": {"aws:RequestTag/owner": "team-a"},
                          "ForAllValues:StringEquals": {"aws:TagKeys": ["owner"]}}})
    target = {"resourceArn": arn} if action == "TagResource" else {"logGroupName": name}
    for tags in ({"owner": "team-b"}, {}, {"owner": "team-a", "extra": "bad"}):
        assert_denied(call(action, **target, tags=tags))
    assert call(action, **target, tags={"owner": "team-a"})[0] == 200


def test_modern_tag_actions_enforce_exact_target_arn(logs_auth):
    call, create, policy, _, _ = logs_auth
    allowed = create("logs-context-exact-allow")
    denied = create("logs-context-exact-deny", {"protect": "true"})
    policy({"Effect": "Allow", "Action": ["logs:TagResource", "logs:UntagResource", "logs:ListTagsForResource"],
            "Resource": allowed})
    assert call("TagResource", resourceArn=allowed, tags={"owner": "team-a"})[0] == 200
    assert call("ListTagsForResource", resourceArn=allowed) == (200, {"tags": {"owner": "team-a"}})
    assert call("UntagResource", resourceArn=allowed, tagKeys=["owner"])[0] == 200
    for action, extra in (("TagResource", {"tags": {"owner": "wrong"}}),
                          ("UntagResource", {"tagKeys": ["protect"]}), ("ListTagsForResource", {})):
        assert_denied(call(action, resourceArn=denied, logGroupName="logs-context-exact-allow", **extra))
    assert call("ListTagsForResource", root=True, resourceArn=denied) == (200, {"tags": {"protect": "true"}})


@pytest.mark.parametrize("legacy", [False, True])
def test_incoming_owner_tag_cannot_replace_existing_ownership_context(logs_auth, legacy):
    call, create, policy, _, _ = logs_auth
    name = "logs-context-change-owner"
    arn = create(name, {"owner": "team-b"})
    action = "TagLogGroup" if legacy else "TagResource"
    policy({"Effect": "Allow", "Action": f"logs:{action}", "Resource": "*",
            "Condition": {"StringEquals": {"aws:ResourceTag/owner": "team-a",
                                          "aws:RequestTag/owner": "team-a"}}})
    target = {"logGroupName": name} if legacy else {"resourceArn": arn}
    assert_denied(call(action, **target, tags={"owner": "team-a"}))
    assert call("ListTagsForResource", root=True, resourceArn=arn) == (200, {"tags": {"owner": "team-b"}})


def test_disabled_auth_keeps_logs_tag_calls_permissive(logs_auth, monkeypatch):
    call, create, policy, _, _ = logs_auth
    arn = create("logs-context-auth-disabled", {"protect": "true"})
    policy({"Effect": "Deny", "Action": "logs:*", "Resource": "*"})
    monkeypatch.setattr(app_mod, "AUTH", False)
    assert call("UntagResource", root=True, resourceArn=arn, tagKeys=["protect"])[0] == 200
    assert call("ListTagsForResource", root=True, resourceArn=arn) == (200, {"tags": {}})


@pytest.mark.parametrize("resource_type,store", [
    ("delivery-source", logs._delivery_sources),
    ("delivery-destination", logs._delivery_destinations),
    ("delivery", logs._deliveries),
])
def test_modern_tag_context_uses_the_supported_delivery_resource(logs_auth, resource_type, store):
    call, _, policy, account, region = logs_auth
    name = "logs-context-delivery"
    arn = f"arn:aws:logs:{region}:{account}:{resource_type}:{name}"
    store.set_scoped(account, region, name, {"arn": arn, "tags": {"owner": "team-a", "protect": "true"}})
    policy(
        {"Effect": "Allow", "Action": ["logs:ListTagsForResource", "logs:UntagResource"], "Resource": arn,
         "Condition": {"StringEquals": {"aws:ResourceTag/owner": "team-a"}}},
        {"Effect": "Deny", "Action": "logs:UntagResource", "Resource": arn,
         "Condition": {"ForAnyValue:StringEquals": {"aws:TagKeys": ["protect"]}}},
    )
    try:
        assert call("ListTagsForResource", resourceArn=arn) == (
            200, {"tags": {"owner": "team-a", "protect": "true"}},
        )
        assert_denied(call("UntagResource", resourceArn=arn, tagKeys=["protect"]), explicit=True)
        assert store.get_scoped(account, region, name)["tags"]["protect"] == "true"
    finally:
        store.pop_scoped(account, region, name, None)


def test_owner_context_is_scoped_to_resource_account_and_region(logs_auth):
    call, create, policy, account, region = logs_auth
    name = "logs-context-scope"
    local = create(name, {"owner": "team-a"})
    remote_region = create(name, {"owner": "team-b"}, request_region="us-west-2")
    logs._log_groups.set_scoped("111111111111", region, name, {
        "arn": f"arn:aws:logs:{region}:111111111111:log-group:{name}:*", "tags": {"owner": "team-a"},
    })
    policy({"Effect": "Allow", "Action": "logs:ListTagsForResource", "Resource": "*",
            "Condition": {"StringEquals": {"aws:ResourceTag/owner": "team-a"}}})
    try:
        assert call("ListTagsForResource", resourceArn=local)[0] == 200
        assert_denied(call("ListTagsForResource", resourceArn=remote_region, request_region="us-west-2"))
        for invalid in (remote_region, local.replace(account, "111111111111")):
            assert call("ListTagsForResource", resourceArn=invalid) == (
                400, {"__type": "ValidationException", "message": "Invalid resourceArn"},
            )
    finally:
        logs._log_groups.pop_scoped("111111111111", region, name, None)


@pytest.mark.parametrize("auth", [False, True])
@pytest.mark.parametrize("resource_type,store", [
    ("log-group", logs._log_groups),
    ("delivery-source", logs._delivery_sources),
    ("delivery-destination", logs._delivery_destinations),
    ("delivery", logs._deliveries),
])
def test_modern_tag_actions_reject_aliases_to_unrelated_arns(
        logs_auth, monkeypatch, auth, resource_type, store):
    call, create, policy, account, region = logs_auth
    name = "logs-context-resolver"
    tags = {"owner": "team-a", "protect": "true"}
    arn = f"arn:aws:logs:{region}:{account}:{resource_type}:{name}"
    if resource_type == "log-group":
        create(name, tags)
    else:
        store.set_scoped(account, region, name, {"arn": arn, "tags": dict(tags)})
    monkeypatch.setattr(app_mod, "AUTH", auth)
    invalid_arns = [
        arn.replace(account, "111111111111"),
        arn.replace(region, "us-west-2"),
        arn.replace(":logs:", ":s3:"),
        arn.replace("arn:aws:", "arn:aws-cn:"),
        arn + ":**" if resource_type == "log-group" else arn.replace(
            f":{resource_type}:", f":{resource_type}:other:"),
    ]
    if resource_type == "log-group":
        invalid_arns.append(arn + ":*")
    try:
        for invalid in invalid_arns:
            # Permission on the supplied ARN must never authorize a mutation
            # or a read of the same-named local resource.
            policy({"Effect": "Allow", "Action": "logs:*", "Resource": invalid})
            for action, extra in (
                ("TagResource", {"tags": {"protect": "false"}}),
                ("UntagResource", {"tagKeys": ["protect"]}),
                ("ListTagsForResource", {}),
            ):
                assert call(action, root=not auth, resourceArn=invalid, **extra) == (
                    400, {"__type": "ValidationException", "message": "Invalid resourceArn"},
                )
                assert store.get_scoped(account, region, name)["tags"] == tags
    finally:
        if resource_type != "log-group":
            store.pop_scoped(account, region, name, None)


def test_invalid_tag_arn_precedes_policy_denial_but_preserves_credential_errors(logs_auth):
    call, create, policy, account, _ = logs_auth
    arn = create("logs-context-validation", {"protect": "true"})
    policy({"Effect": "Deny", "Action": "logs:*", "Resource": "*"})
    status, body, headers = call("UntagResource", resourceArn=arn + ":*",
                                 tagKeys=["protect"], with_headers=True)
    assert (status, body) == (400, {"__type": "ValidationException", "message": "Invalid resourceArn"})
    assert headers[b"content-type"] == b"application/x-amz-json-1.1"
    assert b"x-amzn-errortype" not in headers
    iam._access_keys.get_scoped(account, None, "AKIALOGSTAGCONTEXT")["Status"] = "Inactive"
    status, body = call("UntagResource", resourceArn=arn + ":*", tagKeys=["protect"])
    assert status == 403 and body["__type"] == "InvalidClientTokenId"


@pytest.mark.parametrize("auth", [False, True])
def test_tag_arn_validation_preserves_malformed_json_error(logs_auth, monkeypatch, auth):
    call, _, _, _, _ = logs_auth
    monkeypatch.setattr(app_mod, "AUTH", auth)
    for action in ("TagResource", "UntagResource", "ListTagsForResource"):
        assert call(action, root=True, raw_body=b"{") == (
            400, {"__type": "SerializationException", "message": "Invalid JSON"},
        )
