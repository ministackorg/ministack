"""ECR registry policy and account setting (issue 2617). Bodies and messages come from the named AWS captures."""

import asyncio
import json

import pytest

from ministack.core.responses import get_account_id
from ministack.services import ecr, ecr_registry_policy

pytestmark = pytest.mark.serial  # shares the registry-wide rule quota, registry policy and service-linked role

ROLE = "arn:aws:iam::000000000000:root"
RES = "arn:aws:ecr:us-east-2:000000000000:repository/msc-2617-r08-registry-policy-pol/*"
POLICY = {"Version": "2012-10-17", "Statement": [{
    "Sid": "msc2617", "Effect": "Allow", "Principal": {"AWS": ROLE},
    "Action": ["ecr:CreateRepository", "ecr:BatchImportUpstreamImage"], "Resource": RES}]}
STORED = (
    '{\n  "Version" : "2012-10-17",\n  "Statement" : [ {\n    "Sid" : "msc2617",\n    "Effect" : "Allow",\n'
    '    "Principal" : {\n      "AWS" : "arn:aws:iam::000000000000:root"\n    },\n'
    '    "Action" : [ "ecr:CreateRepository", "ecr:BatchImportUpstreamImage" ],\n'
    f'    "Resource" : "{RES}"\n  }} ]\n}}')


@pytest.fixture(autouse=True)
def _reset():
    ecr.reset()
    yield
    ecr.reset()


def _action(action, raw):
    body = raw if isinstance(raw, (bytes, str)) else json.dumps(raw)
    status, headers, out = asyncio.run(ecr.handle_request(
        "POST", "/", {"x-amz-target": f"AmazonEC2ContainerRegistry_V20150921.{action}"},
        body.encode() if isinstance(body, str) else body, {}))
    return status, json.loads(out)


def _put(policy):
    return _action("PutRegistryPolicy", {"policyText": policy if isinstance(policy, str) else json.dumps(policy)})


def _invalid(status_body, constraint):
    status, body = status_body
    assert status == 400 and body["__type"] == "InvalidParameterException"
    assert body["message"] == f"Invalid parameter at 'PolicyText' failed to satisfy constraint: '{constraint}'"


def test_get_and_delete_without_policy():
    # r08-get-before, r08-delete-before
    for action in ("GetRegistryPolicy", "DeleteRegistryPolicy"):
        status, body = _action(action, {})
        assert status == 400 and body["__type"] == "RegistryPolicyNotFoundException"
        assert body["message"] == f"Registry policy does not exist in the registry with id '{get_account_id()}'"


def test_put_stores_reserialised_policy_and_delete_returns_it():
    # r08-put-valid, r08-put-valid--after, r08-put-same-again, r08-delete-valid
    for _ in range(2):
        status, body = _put(POLICY)
        assert (status, body) == (200, {"registryId": get_account_id(), "policyText": STORED})
    assert _action("GetRegistryPolicy", {})[1] == {"registryId": get_account_id(), "policyText": STORED}
    assert _action("DeleteRegistryPolicy", {})[1] == {"registryId": get_account_id(), "policyText": STORED}
    assert _action("GetRegistryPolicy", {})[0] == 400


def test_put_normalises_statement_object_version_account_principal_and_single_action():
    # r08-put-statement-object, r08-put-no-version, r08-put-principal-account-id, r08-put-replicate
    statement = {"Effect": "Allow", "Principal": {"AWS": "000000000000"}, "Action": ["ecr:ReplicateImage"],
                 "Resource": "*"}
    status, body = _put({"Statement": statement})
    assert status == 200
    assert body["policyText"] == (
        '{\n  "Version" : "2008-10-17",\n  "Statement" : [ {\n    "Effect" : "Allow",\n    "Principal" : {\n'
        f'      "AWS" : "{ROLE}"\n    }},\n    "Action" : "ecr:ReplicateImage",\n    "Resource" : "*"\n  }} ]\n}}')


@pytest.mark.parametrize("key,patch,expect", [
    ("r08-put-effect-deny", {"Effect": "Deny"}, '"Effect" : "Deny"'),
    ("r08-put-condition", {"Condition": {"StringEquals": {"aws:PrincipalAccount": "000000000000"}}},
     '"Condition" : {\n      "StringEquals" : {\n        "aws:PrincipalAccount" : "000000000000"\n      }\n    }'),
    ("r08-put-not-action", {"NotAction": "ecr:CreateRepository", "Action": None}, '"NotAction" : "ecr:CreateRepository"'),
    ("r08-put-principal-star", {"Principal": "*"}, '"Principal" : "*"'),
    ("r08-put-principal-service", {"Principal": {"Service": "ecr.amazonaws.com"}},
     '"Principal" : {\n      "Service" : "ecr.amazonaws.com"\n    }'),
    ("r08-put-resource-other-account", {"Resource": "arn:aws:ecr:us-east-2:111122223333:repository/*"}, "111122223333"),
])
def test_put_accepts_what_aws_accepts(key, patch, expect):
    statement = {**POLICY["Statement"][0], **patch}
    statement = {k: v for k, v in statement.items() if v is not None}
    status, body = _put({"Version": "2012-10-17", "Statement": [statement]})
    assert status == 200, key
    assert expect in body["policyText"]


def test_put_refusals_leave_the_stored_policy():
    # r08-put-text-null, r08-put-missing-text, r08-put-empty, r08-put-not-json, r08-put-empty-object,
    # r08-put-no-statement, r08-put-bad-version, r08-put-principal-bad, r08-put-resource-bad, r08-put-effect-bad,
    # r08-put-unknown-field, r08-put-action-unknown, r08-put-len-10241, r08-put-om-length+json
    assert _put(POLICY)[0] == 200
    _invalid(_action("PutRegistryPolicy", {"policyText": None}), "Cannot be null")
    _invalid(_action("PutRegistryPolicy", {}), "Cannot be null")
    _invalid(_put(""), "Must not be empty")
    _invalid(_put("x" * 10241), "Must have length less than or equal to 10240")
    statement = POLICY["Statement"][0]
    for bad in ("not json", "{}", "[]", {"Version": "2012-10-17"}, {"Version": "2012-10-17", "Statement": []},
                {**POLICY, "Version": "2020-01-01"},
                {"Statement": [{**statement, "Principal": {"AWS": "not-an-arn"}}]},
                {"Statement": [{**statement, "Resource": "not-an-arn"}]},
                {"Statement": [{**statement, "Effect": "Maybe"}]},
                {"Statement": [{**statement, "Foo": "bar"}]},
                {"Statement": [{**statement, "Action": ["ecr:NotAnAction"]}]}):
        _invalid(_put(bad), "Invalid registry policy provided")
    assert _action("GetRegistryPolicy", {})[1]["policyText"] == STORED


def test_put_wire_errors():
    # r08-put-text-int, r08-put-text-object, r08-put-text-list, r08-put-text-bool, r08-put-repeat-text,
    # r08-put-body-array, r08-put-body-empty, r08-put-body-not-json
    cases = [
        ('{"policyText": 5}', "NUMBER_VALUE cannot be converted to String"),
        ('{"policyText": true}', "TRUE_VALUE cannot be converted to String"),
        ('{"policyText": {"a": 1}}', "Start of structure or map found where not expected"),
        ('{"policyText": ["x"]}', None),
        ('{"policyText": "a", "policyText": "b"}', "Duplicate key found in payload"),
        ("[]", None), ("", None),
        ("not json", "Unexpected Character in input string at byte 1"),
    ]
    for raw, message in cases:
        status, body = _action("PutRegistryPolicy", raw)
        assert status == 400 and body["__type"] == "SerializationException", raw
        if message is None:
            assert "message" not in body and "Message" not in body, raw  # the captures carry no message here
        else:
            assert body["Message"] == message, raw
    assert _action("GetRegistryPolicy", "[]")[1] == {"__type": "SerializationException"}
    assert _action("DeleteRegistryPolicy", "[]")[1] == {"__type": "SerializationException"}


def test_account_setting_registry_policy_scope():
    # r08-account-setting-scope
    assert _action("GetAccountSetting", {"name": "REGISTRY_POLICY_SCOPE"}) == (
        200, {"name": "REGISTRY_POLICY_SCOPE", "value": "V2"})


def test_put_accepts_any_twelve_digit_account_principal():
    # r08-put-principal-account-id, r08-put-principal-fake-account (AWS refuses the fake account; ANSWERS
    # principal-account-existence says MiniStack accepts it)
    for account in ("111122223333", "arn:aws:iam::111122223333:root"):
        policy = json.loads(json.dumps(POLICY))
        policy["Statement"][0]["Principal"] = {"AWS": account}
        status, body = _put(policy)
        assert status == 200
        assert '"AWS" : "arn:aws:iam::111122223333:root"' in body["policyText"]


def _with_action(action):
    return {"Version": "2012-10-17", "Statement": [{**POLICY["Statement"][0], "Action": action}]}


def test_every_ecr_operation_and_wildcards_are_known_actions():
    """r08-put-action-wildcard (ecr:* accepted), r08-put-valid (ecr:CreateRepository), r08-put-replicate
    (ecr:ReplicateImage). MiniStack contract, uncaptured: ledger line 218 (known set = botocore ECR operation names
    plus the IAM-only BatchImportUpstreamImage and ReplicateImage; "*" and wildcards matching a known action)."""
    for action in ("ecr:CreateRepositoryCreationTemplate", "ecr:BatchImportUpstreamImage", "ecr:ReplicateImage",
                   "ecr:CreateRepository", "ecr:*", "ecr:Get*", "ecr:*Image", "ECR:batchgetimage", "*",
                   ["ecr:PutImage", "*"]):
        assert _put(_with_action(action))[0] == 200, action
    assert {"ecr:BatchImportUpstreamImage", "ecr:ReplicateImage"} <= {f"ecr:{a}" for a in ecr_registry_policy.known_actions()}


def test_unknown_actions_are_still_refused():
    """r08-put-action-unknown (ecr:NotAnAction). MiniStack contract, uncaptured: ledger line 218 (a wildcard that
    matches no known action, an action with no service)."""
    for action in ("ecr:NotAnAction", "ecr:Nope*", "NotAnAction", ["ecr:PutImage", "ecr:NotAnAction"], ""):
        _invalid(_put(_with_action(action)), "Invalid registry policy provided")


def test_nan_and_infinity_are_malformed_policy_text():
    """r08-put-not-json (malformed text answers 'Invalid registry policy provided'). MiniStack contract, uncaptured:
    ledger line 218 (Python's json accepts NaN, Infinity and -Infinity; a strict parser does not)."""
    for constant in ("NaN", "Infinity", "-Infinity"):
        text = json.dumps(POLICY).replace('"msc2617"', constant)
        _invalid(_put(text), "Invalid registry policy provided")
        text = json.dumps({**POLICY, "Id": 0}).replace('"Id": 0', f'"Id": {constant}')
        _invalid(_put(text), "Invalid registry policy provided")
