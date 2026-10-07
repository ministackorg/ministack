# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
"""SES receipt metadata through the SDK's Query serializer and XML parser."""

import asyncio
from datetime import datetime
from urllib.parse import urlencode
from uuid import uuid4

import pytest
from botocore import UNSIGNED
from botocore.exceptions import ClientError
from botocore.parsers import create_parser
from botocore.serialize import create_serializer
from botocore.session import Session

from ministack.core import persistence
from ministack.core.responses import (
    AccountRegionScopedDict,
    set_request_account_id,
    set_request_region,
)
from ministack.services import ses


@pytest.fixture
def receipt_api(monkeypatch):
    # Private stores keep in-process reset/persistence tests independent of the
    # running integration server and other services' fixtures on this worker.
    for name in (
        "_identities", "_sent_emails", "_templates", "_configuration_sets",
        "_account_details", "_receipt_rule_sets", "_active_receipt_rule_set",
    ):
        monkeypatch.setattr(ses, name, AccountRegionScopedDict())
    model = Session().get_service_model("ses")
    # Exercise service-side required-field errors instead of SDK validation.
    serializer = create_serializer("query", include_validation=False)
    parser = create_parser("query")

    def call(action, **params):
        operation = model.operation_model(action)
        request = serializer.serialize_to_request(params, operation)
        status, headers, body = asyncio.run(ses.handle_request(
            "POST", "/", {}, urlencode(request["body"]).encode(), {}
        ))
        result = parser.parse(
            {"status_code": status, "headers": headers, "body": body},
            operation.output_shape,
        )
        if status >= 400:
            raise ClientError(result, action)
        assert status == 200
        return {key: value for key, value in result.items() if key != "ResponseMetadata"}

    return call


def assert_error(api, action, code, message, **params):
    with pytest.raises(ClientError) as exc:
        api(action, **params)
    response = exc.value.response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert response["Error"] == {"Code": code, "Message": message, "Type": "Sender"}


def test_receipt_rule_set_and_rule_lifecycle(receipt_api):
    api = receipt_api
    assert api("DescribeActiveReceiptRuleSet") == {}
    api("CreateReceiptRuleSet", RuleSetName="set.with-periods")
    empty = api("DescribeReceiptRuleSet", RuleSetName="set.with-periods")
    assert empty["Metadata"]["Name"] == "set.with-periods"
    assert isinstance(empty["Metadata"]["CreatedTimestamp"], datetime)
    assert empty["Rules"] == []
    assert api("ListReceiptRuleSets")["RuleSets"] == [empty["Metadata"]]

    api("CreateReceiptRule", RuleSetName="set.with-periods", Rule={"Name": "first"})
    assert api("DescribeReceiptRule", RuleSetName="set.with-periods", RuleName="first")["Rule"] == {
        "Name": "first", "Enabled": False, "ScanEnabled": False,
        "TlsPolicy": "Optional", "Actions": [],
    }
    rule = {
        "Name": "second", "Enabled": True, "ScanEnabled": True,
        "TlsPolicy": "Require", "Recipients": ["EXAMPLE.COM", "User+tag@EXAMPLE.ORG", ".EXAMPLE.COM", "foo.example-test.com"],
        "Actions": [
            {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "value<&"}},
            {"StopAction": {"Scope": "RuleSet"}},
        ],
    }
    api("CreateReceiptRule", RuleSetName="set.with-periods", Rule=rule)
    rule["Recipients"] = [".example.com", "example.com", "foo.example-test.com", "user+tag@example.org"]
    assert api("DescribeReceiptRule", RuleSetName="set.with-periods", RuleName="second")["Rule"] == rule
    api("CreateReceiptRule", RuleSetName="set.with-periods", After="second", Rule={"Name": "third"})
    described = api("DescribeReceiptRuleSet", RuleSetName="set.with-periods")
    assert [r["Name"] for r in described["Rules"]] == ["second", "third", "first"]
    api("SetActiveReceiptRuleSet", RuleSetName="set.with-periods")
    assert api("DescribeActiveReceiptRuleSet") == described
    assert_error(api, "DeleteReceiptRuleSet", "CannotDelete",
                 "Cannot delete active rule set: set.with-periods", RuleSetName="set.with-periods")
    api("DeleteReceiptRule", RuleSetName="set.with-periods", RuleName="third")
    assert [r["Name"] for r in api("DescribeActiveReceiptRuleSet")["Rules"]] == ["second", "first"]
    api("SetActiveReceiptRuleSet")
    assert api("DescribeActiveReceiptRuleSet") == {}
    api("DeleteReceiptRuleSet", RuleSetName="set.with-periods")
    api("DeleteReceiptRuleSet", RuleSetName="set.with-periods")
    assert api("ListReceiptRuleSets") == {"RuleSets": []}


def test_receipt_duplicate_and_missing_resource_errors_leave_order_unchanged(receipt_api):
    api = receipt_api
    for action, params in (
        ("DescribeReceiptRuleSet", {}), ("SetActiveReceiptRuleSet", {}),
        ("CreateReceiptRule", {"Rule": {"Name": "first"}}),
        ("DescribeReceiptRule", {"RuleName": "first"}),
        ("DeleteReceiptRule", {"RuleName": "first"}),
    ):
        assert_error(api, action, "RuleSetDoesNotExist", "Rule set does not exist: missing",
                     RuleSetName="missing", **params)
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, "CreateReceiptRuleSet", "AlreadyExists", "Rule set already exists: rules",
                 RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "first"})
    assert_error(api, "CreateReceiptRule", "AlreadyExists", "Rule already exists: first",
                 RuleSetName="rules", Rule={"Name": "first"}, After="missing")
    assert_error(api, "CreateReceiptRule", "RuleDoesNotExist", "Rule does not exist: missing",
                 RuleSetName="rules", Rule={"Name": "second"}, After="missing")
    assert_error(api, "DescribeReceiptRule", "RuleDoesNotExist", "Rule does not exist: missing",
                 RuleSetName="rules", RuleName="missing")
    api("DeleteReceiptRule", RuleSetName="rules", RuleName="missing")
    assert [r["Name"] for r in api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"]] == ["first"]
    assert_error(api, "ListReceiptRuleSets", "InvalidParameterValue", "Invalid token: invalid", NextToken="invalid")


@pytest.mark.parametrize("name", ["-bad", "bad-", "a" * 65])
def test_receipt_invalid_names(receipt_api, name):
    api = receipt_api
    assert_error(api, "CreateReceiptRuleSet", "InvalidParameterValue",
                 f"Not a valid ruleSetName: {name}", RuleSetName=name)
    # AWS validates rule metadata before looking up the parent rule set.
    assert_error(api, "CreateReceiptRule", "InvalidParameterValue",
                 f"Not a valid ruleName: {name}", RuleSetName="missing", Rule={"Name": name})
    assert api("ListReceiptRuleSets") == {"RuleSets": []}


@pytest.mark.parametrize("name", ["-bad", "bad-", "a" * 65])
@pytest.mark.parametrize("action,params,field", [
    ("DescribeReceiptRuleSet", {}, "ruleSetName"),
    ("DeleteReceiptRuleSet", {}, "ruleSetName"),
    ("SetActiveReceiptRuleSet", {}, "ruleSetName"),
    ("CreateReceiptRule", {"Rule": {"Name": "valid"}}, "ruleSetName"),
    ("DescribeReceiptRule", {"RuleName": "valid"}, "ruleSetName"),
    ("DeleteReceiptRule", {"RuleName": "valid"}, "ruleSetName"),
    ("DescribeReceiptRule", {"RuleSetName": "rules"}, "ruleName"),
    ("DeleteReceiptRule", {"RuleSetName": "rules"}, "ruleName"),
])
def test_receipt_lookup_delete_and_activation_validate_names(receipt_api, name, action, params, field):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "valid"})
    api("SetActiveReceiptRuleSet", RuleSetName="rules")
    before = api("DescribeActiveReceiptRuleSet")
    key = "RuleSetName" if field == "ruleSetName" else "RuleName"
    assert_error(api, action, "InvalidParameterValue",
                 f"Not a valid {field}: {name}", **{**params, key: name})
    assert api("DescribeActiveReceiptRuleSet") == before
    assert api("ListReceiptRuleSets")["RuleSets"] == [before["Metadata"]]


@pytest.mark.parametrize("action,params", [
    ("CreateReceiptRuleSet", {}),
    ("DescribeReceiptRuleSet", {}),
    ("DeleteReceiptRuleSet", {}),
    ("CreateReceiptRule", {"Rule": {"Name": "valid"}}),
    ("DescribeReceiptRule", {"RuleName": "valid"}),
    ("DeleteReceiptRule", {"RuleName": "valid"}),
])
def test_receipt_missing_rule_set_name_error(receipt_api, action, params):
    assert_error(receipt_api, action, "ValidationError",
                 "1 validation error detected: Value at 'ruleSetName' failed to satisfy constraint: Member must not be null",
                 **params)
    assert receipt_api("ListReceiptRuleSets") == {"RuleSets": []}


@pytest.mark.parametrize("action,params,field", [
    ("CreateReceiptRule", {}, "rule"),
    ("CreateReceiptRule", {"Rule": {}}, "rule"),
    ("CreateReceiptRule", {"Rule": {"Enabled": True}}, "rule.name"),
    ("DescribeReceiptRule", {}, "ruleName"),
    ("DeleteReceiptRule", {}, "ruleName"),
])
def test_receipt_missing_rule_fields_error(receipt_api, action, params, field):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, action, "ValidationError",
                 f"1 validation error detected: Value at '{field}' failed to satisfy constraint: Member must not be null",
                 RuleSetName="rules", **params)
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


@pytest.mark.parametrize("recipients", [
    ["", "first@example.com", "second@example.com"],
    ["first@example.com", "", "second@example.com"],
    ["first@example.com", "second@example.com", ""],
])
def test_receipt_empty_recipient_is_rejected_without_creating_a_rule(receipt_api, recipients):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, "CreateReceiptRule", "InvalidParameterValue", "Invalid recipient: ",
                 RuleSetName="rules", Rule={"Name": "invalid", "Recipients": recipients})
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


@pytest.mark.parametrize("value", ["a\tb", "café 雪 😀 <&>"])
def test_receipt_header_tab_and_unicode_round_trip(receipt_api, value):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    actions = [{"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": value}}]
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "header", "Actions": actions})
    api("SetActiveReceiptRuleSet", RuleSetName="rules")
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="header")["Rule"]["Actions"] == actions
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"][0]["Actions"] == actions
    assert api("DescribeActiveReceiptRuleSet")["Rules"][0]["Actions"] == actions


def test_receipt_nul_header_is_created_but_readback_fails_until_deleted(receipt_api):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "valid"})
    api("SetActiveReceiptRuleSet", RuleSetName="rules")
    before = api("DescribeActiveReceiptRuleSet")
    assert api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "broken", "Actions": [
        {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "a\x00b"}},
    ]}) == {}
    for action, params in (
        ("DescribeReceiptRule", {"RuleSetName": "rules", "RuleName": "broken"}),
        ("DescribeReceiptRuleSet", {"RuleSetName": "rules"}),
        ("DescribeActiveReceiptRuleSet", {}),
    ):
        with pytest.raises(ClientError) as exc:
            api(action, **params)
        response = exc.value.response
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 500
        assert response["Error"] == {"Type": "Receiver", "Code": "InternalFailure"}
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="valid")["Rule"] == before["Rules"][0]
    assert api("ListReceiptRuleSets")["RuleSets"] == [before["Metadata"]]
    api("DeleteReceiptRule", RuleSetName="rules", RuleName="broken")
    assert api("DescribeReceiptRuleSet", RuleSetName="rules") == before
    assert api("DescribeActiveReceiptRuleSet") == before


@pytest.mark.parametrize("rule,code,message", [
    ({"Name": "tls", "TlsPolicy": "Invalid"}, "ValidationError",
     "1 validation error detected: Value at 'rule.tlsPolicy' failed to satisfy constraint: Member must satisfy enum value set: [Optional, Require]"),
    ({"Name": "recipient", "Recipients": ["not valid"]}, "InvalidParameterValue", "Invalid recipient: not valid"),
    ({"Name": "scope", "Actions": [{"StopAction": {"Scope": "Invalid"}}]}, "ValidationError",
     "1 validation error detected: Value at 'rule.actions.1.member.stopAction.scope' failed to satisfy constraint: Member must satisfy enum value set: [RuleSet]"),
    ({"Name": "ordering", "Actions": [{"StopAction": {"Scope": "RuleSet"}},
      {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "ok"}}]}, "InvalidParameterValue",
     "Stop action, if any, must be placed at the end of the actions list"),
    ({"Name": "multiple", "Actions": [{"StopAction": {"Scope": "RuleSet"},
      "AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "ok"}}]}, "InvalidParameterValue",
     "Exactly one action type must be specified for each ReceiptAction"),
    ({"Name": "header", "Actions": [{"AddHeaderAction": {"HeaderName": "X bad", "HeaderValue": "ok"}}]},
     "InvalidParameterValue", "Invalid header name: X bad"),
    ({"Name": "newline", "Actions": [{"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "a\nb"}}]},
     "InvalidParameterValue", "Invalid header value: a0x000ab"),
])
def test_receipt_invalid_rule_metadata_is_atomic(receipt_api, rule, code, message):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, "CreateReceiptRule", code, message, RuleSetName="rules", Rule=rule)
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


@pytest.mark.parametrize("header,missing", [
    ({"HeaderName": "X-Test"}, "headerValue"),
    ({"HeaderValue": "value"}, "headerName"),
])
def test_receipt_missing_header_fields_are_rejected_without_creating_a_rule(receipt_api, header, missing):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, "CreateReceiptRule", "ValidationError",
                 f"1 validation error detected: Value at 'rule.actions.1.member.addHeaderAction.{missing}' failed to satisfy constraint: Member must not be null",
                 RuleSetName="rules", Rule={"Name": "missing", "Actions": [{"AddHeaderAction": header}]})
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


@pytest.mark.parametrize("recipient", [
    "example..com", "example.-com", "example-.com", "foo..example.com",
    "foo.-example.com", "foo.example-.com", "example.com-",
])
def test_receipt_invalid_domain_labels_are_rejected_without_creating_a_rule(receipt_api, recipient):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    assert_error(api, "CreateReceiptRule", "InvalidParameterValue", f"Invalid recipient: {recipient}",
                 RuleSetName="rules", Rule={"Name": "invalid", "Recipients": [recipient]})
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


def test_receipt_recipients_are_lowercased_deduplicated_and_sorted(receipt_api):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "recipients", "Recipients": [
        "z@EXAMPLE.COM", "A@Example.com", "a@example.com", "EXAMPLE.COM",
        "example.com", ".EXAMPLE.COM", ".example.com",
    ]})
    recipients = [".example.com", "a@example.com", "example.com", "z@example.com"]
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="recipients")["Rule"]["Recipients"] == recipients
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"][0]["Recipients"] == recipients


@pytest.mark.parametrize("action", [
    {"S3Action": {"BucketName": "bucket"}},
    {"SNSAction": {"TopicArn": "arn:aws:sns:us-east-1:111111111111:topic"}},
    {"LambdaAction": {"FunctionArn": "arn:aws:lambda:us-east-1:111111111111:function:test"}},
    {"BounceAction": {"SmtpReplyCode": "550", "Message": "no", "Sender": "sender@example.com"}},
    {"WorkmailAction": {"OrganizationArn": "arn:aws:workmail:us-east-1:111111111111:organization/m-test"}},
    {"ConnectAction": {"InstanceARN": "arn:aws:connect:us-east-1:111111111111:instance/test",
                       "IAMRoleARN": "arn:aws:iam::111111111111:role/test"}},
    {"StopAction": {"Scope": "RuleSet", "TopicArn": "arn:aws:sns:us-east-1:111111111111:topic"}},
])
def test_receipt_external_actions_are_explicitly_unsupported(receipt_api, action):
    # Intentional supported-subset boundary: destination/permission validation
    # and receiving execution have not been implemented for these action types.
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    name = next(iter(action))
    if name == "StopAction":
        name += " with TopicArn"
    assert_error(api, "CreateReceiptRule", "InvalidAction",
                 f"Receipt action {name} is not implemented",
                 RuleSetName="rules", Rule={"Name": "unsupported", "Actions": [action]})
    assert api("DescribeReceiptRuleSet", RuleSetName="rules")["Rules"] == []


def test_receipt_explicit_empty_fields_are_preserved_and_after_is_validated(receipt_api):
    api = receipt_api
    api("CreateReceiptRuleSet", RuleSetName="rules")
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "empty", "Recipients": [], "Actions": [{}]})
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="empty")["Rule"]["Recipients"] == []
    for name, action in (("empty-stop", {"StopAction": {}}), ("empty-header", {"AddHeaderAction": {}})):
        api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": name, "Actions": [action]})
        assert api("DescribeReceiptRule", RuleSetName="rules", RuleName=name)["Rule"]["Actions"] == []
    api("CreateReceiptRule", RuleSetName="rules", Rule={"Name": "header", "Actions": [
        {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": ""}},
    ]})
    assert api("DescribeReceiptRule", RuleSetName="rules", RuleName="header")["Rule"]["Actions"] == [
        {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": ""}},
    ]
    assert_error(api, "CreateReceiptRule", "ValidationError",
                 "2 validation errors detected: Value at 'after' failed to satisfy constraint: Member must have length greater than or equal to 1; Value at 'after' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[a-zA-Z0-9_.-]+$",
                 RuleSetName="rules", After="", Rule={"Name": "after"})


def test_receipt_metadata_and_active_selection_survive_all_tenant_persistence_and_reset(receipt_api, monkeypatch, tmp_path):
    api = receipt_api
    scopes = [("111111111111", "us-east-1"), ("111111111111", "us-west-2"), ("222222222222", "us-east-1")]
    expected = {}
    for i, (account, region) in enumerate(scopes):
        set_request_account_id(account)
        set_request_region(region)
        assert api("ListReceiptRuleSets") == {"RuleSets": []}
        api("CreateReceiptRuleSet", RuleSetName="same-name")
        api("CreateReceiptRule", RuleSetName="same-name", Rule={"Name": f"rule-{i}"})
        api("SetActiveReceiptRuleSet", RuleSetName="same-name")
        expected[account, region] = api("DescribeActiveReceiptRuleSet")
    snapshot = ses.get_state()
    monkeypatch.setattr(persistence, "PERSIST_STATE", True)
    monkeypatch.setattr(persistence, "STATE_DIR", str(tmp_path))
    persistence.save_state("ses", snapshot)
    ses.reset()
    for account, region in scopes:
        set_request_account_id(account)
        set_request_region(region)
        assert api("ListReceiptRuleSets") == {"RuleSets": []}
        assert api("DescribeActiveReceiptRuleSet") == {}
    ses.load_persisted_state(persistence.load_state("ses"))
    for account, region in scopes:
        set_request_account_id(account)
        set_request_region(region)
        assert api("DescribeActiveReceiptRuleSet") == expected[account, region]
    api("SetActiveReceiptRuleSet")
    api("DeleteReceiptRuleSet", RuleSetName="same-name")
    set_request_account_id(scopes[0][0])
    set_request_region(scopes[0][1])
    assert api("DescribeActiveReceiptRuleSet") == expected[scopes[0]]
    assert snapshot["_receipt_rule_sets"].get_scoped(*scopes[-1], "same-name")["Rules"]


@pytest.mark.serial
def test_receipt_unsigned_sdk_lifecycle():
    # The endpoint comes from conftest so this uses the same isolated local
    # server as the integration suite. Activation changes shared server state.
    from conftest import make_client

    client = make_client("ses", additional_config_kwargs={"signature_version": UNSIGNED})
    name = f"unsigned-receipts-{uuid4().hex[:12]}"
    previous = client.describe_active_receipt_rule_set().get("Metadata", {}).get("Name")
    client.create_receipt_rule_set(RuleSetName=name)
    try:
        assert name in {item["Name"] for item in client.list_receipt_rule_sets()["RuleSets"]}
        client.create_receipt_rule(RuleSetName=name, Rule={"Name": "first"})
        client.create_receipt_rule(RuleSetName=name, After="first", Rule={"Name": "second", "Actions": [
            {"AddHeaderAction": {"HeaderName": "X-Test", "HeaderValue": "value<&"}},
            {"StopAction": {"Scope": "RuleSet"}},
        ]})
        described = client.describe_receipt_rule_set(RuleSetName=name)
        assert [rule["Name"] for rule in described["Rules"]] == ["first", "second"]
        assert client.describe_receipt_rule(RuleSetName=name, RuleName="second")["Rule"] == described["Rules"][1]
        client.set_active_receipt_rule_set(RuleSetName=name)
        active = client.describe_active_receipt_rule_set()
        assert active["Metadata"] == described["Metadata"]
        assert active["Rules"] == described["Rules"]
        client.delete_receipt_rule(RuleSetName=name, RuleName="second")
        assert [rule["Name"] for rule in client.describe_active_receipt_rule_set()["Rules"]] == ["first"]
    finally:
        client.set_active_receipt_rule_set(**({"RuleSetName": previous} if previous else {}))
        client.delete_receipt_rule_set(RuleSetName=name)
    assert name not in {item["Name"] for item in client.list_receipt_rule_sets()["RuleSets"]}
