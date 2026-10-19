"""Tests for the IAM authentication and authorization engine.

Unit tests verify the policy evaluator against AWS-documented behavior:
  - AWS IAM policy evaluation logic (deny overrides allow, implicit deny)
  - AWS condition operator semantics (per IAM JSON policy elements reference)
  - AWS trust policy Principal matching
  - AWS error codes (per STS/IAM Common Errors reference)

Integration tests (SimulateCustomPolicy, policy validation) run against
the MiniStack server and work with AUTH=false.
"""

import asyncio
import contextlib
import contextvars
import io
import json
import re
import time
import xml.etree.ElementTree as ET
from urllib.parse import urlencode

import pytest
from botocore.exceptions import ClientError

from ministack.core.iam_actions import extract_iam_action, extract_resource_arn
from ministack.core.iam_evaluator import (
    AmbiguousAccessKeyError,
    AuthError,
    CredentialResolutionError,
    EvalContext,
    EvalResult,
    PrincipalInfo,
    ResolvedCredential,
    enforce,
    evaluate,
    evaluate_trust_policy,
    find_iam_access_key_account,
    fnmatch_iam,
    fnmatch_iam_cs,
    parse_policy_document,
    resolve_credential,
    resolve_principal,
    validate_policy_document,
)

# ---------------------------------------------------------------------------
# Wildcard matching. Action/NotAction is case-insensitive; Resource/NotResource,
# StringLike and the Arn operators are case-sensitive.
# ---------------------------------------------------------------------------

class TestFnmatchIam:
    def test_star_matches_everything(self):
        assert fnmatch_iam("s3:PutObject", "*")

    def test_service_wildcard(self):
        assert fnmatch_iam("s3:PutObject", "s3:*")

    def test_prefix_wildcard(self):
        assert fnmatch_iam("s3:PutObject", "s3:Put*")

    def test_no_match(self):
        assert not fnmatch_iam("s3:PutObject", "s3:Get*")

    def test_exact_match(self):
        assert fnmatch_iam("s3:PutObject", "s3:PutObject")

    def test_case_insensitive(self):
        # AWS IAM: action matching is case-insensitive
        assert fnmatch_iam("s3:PutObject", "s3:putobject")
        assert fnmatch_iam("S3:PUTOBJECT", "s3:PutObject")

    def test_question_mark_single_char(self):
        assert fnmatch_iam("s3:GetObject", "s3:Get?bject")
        assert not fnmatch_iam("s3:GetObject", "s3:Get?ject")

    def test_arn_wildcard(self):
        assert fnmatch_iam("arn:aws:s3:::mybucket/mykey", "arn:aws:s3:::mybucket/*")
        assert not fnmatch_iam("arn:aws:s3:::otherbucket/mykey", "arn:aws:s3:::mybucket/*")

    def test_empty_pattern_matches_empty(self):
        assert fnmatch_iam("", "")
        assert not fnmatch_iam("something", "")


class TestFnmatchIamCaseSensitive:
    """"In the Resource element, the IAM user name is case sensitive."
    (reference_policies_elements_resource)"""

    @pytest.mark.parametrize("pattern", ("*", "arn:aws:iam::1:user/Bob", "arn:aws:iam::1:user/B*",
                                         "arn:aws:iam::1:user/Bo?"))
    def test_matching_case_matches(self, pattern):
        assert fnmatch_iam_cs("arn:aws:iam::1:user/Bob", pattern)

    @pytest.mark.parametrize("pattern", ("arn:aws:iam::1:user/bob", "arn:aws:iam::1:user/BOB",
                                         "arn:aws:iam::1:user/b*", "ARN:AWS:IAM::1:USER/Bob"))
    def test_wrong_case_does_not_match(self, pattern):
        assert not fnmatch_iam_cs("arn:aws:iam::1:user/Bob", pattern)

    def test_the_two_matchers_disagree_only_on_case(self):
        value, pattern = "arn:aws:s3:::MyBucket/Key", "arn:aws:s3:::mybucket/*"
        assert fnmatch_iam(value, pattern)
        assert not fnmatch_iam_cs(value, pattern)

    def test_action_matching_stays_case_insensitive(self):
        assert fnmatch_iam("s3:PutObject", "s3:putobject")


class TestResourceAndConditionCaseSensitivity:
    def _decide(self, resource, request_arn="arn:aws:rds-db:us-east-1:1:dbuser:db-X/AppUser",
                condition=None):
        stmt = {"Effect": "Allow", "Action": "rds-db:connect", "Resource": resource}
        if condition:
            stmt["Condition"] = condition
        doc = {"Version": "2012-10-17", "Statement": [stmt]}
        return evaluate(EvalContext(
            principal_arn="arn:aws:iam::1:user/u", principal_type="user",
            principal_account="1", action="rds-db:connect",
            resource_arn=request_arn, region="us-east-1",
            service_context={"tag": "Prod", "arn": "arn:aws:sns:us-east-1:1:Topic"},
        ), [parse_policy_document(json.dumps(doc))]).decision

    def test_resource_case_decides_the_outcome(self):
        assert self._decide("arn:aws:rds-db:us-east-1:1:dbuser:db-X/AppUser") == "Allow"
        assert self._decide("arn:aws:rds-db:us-east-1:1:dbuser:db-X/appuser") == "ImplicitDeny"

    def test_not_resource_case_decides_the_outcome(self):
        doc = {"Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": "rds-db:connect",
            "NotResource": "arn:aws:rds-db:us-east-1:1:dbuser:db-X/appuser"}]}
        assert evaluate(EvalContext(
            principal_arn="arn:aws:iam::1:user/u", principal_type="user",
            principal_account="1", action="rds-db:connect",
            resource_arn="arn:aws:rds-db:us-east-1:1:dbuser:db-X/AppUser",
            region="us-east-1",
        ), [parse_policy_document(json.dumps(doc))]).decision == "Allow"

    @pytest.mark.parametrize("pattern,expected", (("Prod*", "Allow"), ("prod*", "ImplicitDeny")))
    def test_string_like_is_case_sensitive(self, pattern, expected):
        """"Case-sensitive matching" (reference_policies_elements_condition_operators)."""
        assert self._decide(
            "*", condition={"StringLike": {"tag": pattern}}) == expected

    @pytest.mark.parametrize("value,expected", (
        ("arn:aws:sns:us-east-1:1:Topic", "Allow"),
        ("arn:aws:sns:us-east-1:1:topic", "ImplicitDeny"),
    ))
    def test_arn_like_is_case_sensitive(self, value, expected):
        """"Case-sensitive matching of the ARN" (same page)."""
        assert self._decide("*", condition={"ArnLike": {"arn": value}}) == expected


# ---------------------------------------------------------------------------
# Policy parsing
# ---------------------------------------------------------------------------

class TestParsePolicyDocument:
    def test_basic_allow(self):
        doc = {"Version": "2012-10-17",
               "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        stmts = parse_policy_document(doc)
        assert len(stmts) == 1
        assert stmts[0].effect == "Allow"
        assert stmts[0].actions == ["s3:*"]

    def test_multiple_actions_as_list(self):
        doc = {"Statement": [{"Effect": "Allow",
                              "Action": ["s3:Get*", "s3:List*"], "Resource": "*"}]}
        stmts = parse_policy_document(doc)
        assert stmts[0].actions == ["s3:Get*", "s3:List*"]

    def test_not_action(self):
        doc = {"Statement": [{"Effect": "Allow", "NotAction": "iam:*", "Resource": "*"}]}
        stmts = parse_policy_document(doc)
        assert stmts[0].not_actions == ["iam:*"]
        assert stmts[0].actions == []

    def test_string_json_input(self):
        stmts = parse_policy_document(
            json.dumps({"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}))
        assert len(stmts) == 1

    def test_invalid_json_returns_empty(self):
        assert parse_policy_document("not json") == []

    def test_invalid_effect_skipped(self):
        doc = {"Statement": [{"Effect": "Maybe", "Action": "*", "Resource": "*"}]}
        assert parse_policy_document(doc) == []

    def test_single_statement_dict_not_list(self):
        doc = {"Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}}
        assert len(parse_policy_document(doc)) == 1


# ---------------------------------------------------------------------------
# Policy validation (matches AWS MalformedPolicyDocument errors)
# ---------------------------------------------------------------------------

class TestValidatePolicyDocument:
    def test_valid_policy(self):
        assert validate_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}) is None

    def test_missing_statement(self):
        assert validate_policy_document({}) is not None

    def test_empty_statement_list(self):
        assert validate_policy_document({"Statement": []}) is not None

    def test_invalid_effect(self):
        assert validate_policy_document(
            {"Statement": [{"Effect": "Maybe", "Action": "s3:*", "Resource": "*"}]}) is not None

    def test_missing_action(self):
        assert validate_policy_document(
            {"Statement": [{"Effect": "Allow", "Resource": "*"}]}) is not None

    def test_missing_resource(self):
        assert validate_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "s3:*"}]}) is not None

    def test_both_action_and_not_action(self):
        assert validate_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*", "NotAction": "iam:*", "Resource": "*"
        }]}) is not None

    def test_both_resource_and_not_resource(self):
        assert validate_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*", "NotResource": "arn:aws:s3:::x"
        }]}) is not None


# ---------------------------------------------------------------------------
# Policy evaluation (per AWS evaluation logic documentation)
# ---------------------------------------------------------------------------

def _ctx(action="s3:PutObject", resource="*", region="us-east-1"):
    return EvalContext(
        principal_arn="arn:aws:iam::000000000000:user/testuser",
        principal_type="User",
        principal_account="000000000000",
        action=action, resource_arn=resource, region=region,
    )


class TestEvaluate:
    def test_explicit_allow(self):
        stmts = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]})
        assert evaluate(_ctx(), [stmts]).decision == "Allow"

    def test_implicit_deny_no_matching_allow(self):
        """AWS: by default all requests are implicitly denied."""
        stmts = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]})
        assert evaluate(_ctx(action="ec2:RunInstances"), [stmts]).decision == "ImplicitDeny"

    def test_explicit_deny_overrides_allow(self):
        """AWS: an explicit deny always overrides an explicit allow."""
        stmts = parse_policy_document({"Statement": [
            {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
            {"Effect": "Deny", "Action": "s3:DeleteBucket", "Resource": "*"},
        ]})
        assert evaluate(_ctx(action="s3:DeleteBucket"), [stmts]).decision == "Deny"
        assert evaluate(_ctx(action="s3:PutObject"), [stmts]).decision == "Allow"

    def test_not_action_excludes_specified(self):
        stmts = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "NotAction": "iam:*", "Resource": "*"}]})
        assert evaluate(_ctx(action="s3:PutObject"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(action="iam:CreateUser"), [stmts]).decision == "ImplicitDeny"

    def test_not_resource_excludes_specified(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*",
            "NotResource": "arn:aws:s3:::secret-bucket/*"
        }]})
        assert evaluate(_ctx(resource="arn:aws:s3:::public/key"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(resource="arn:aws:s3:::secret-bucket/key"), [stmts]).decision == "ImplicitDeny"

    def test_multiple_policies_union(self):
        """AWS: identity-based policies are unioned."""
        p1 = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "s3:Get*", "Resource": "*"}]})
        p2 = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "s3:Put*", "Resource": "*"}]})
        assert evaluate(_ctx(action="s3:PutObject"), [p1, p2]).decision == "Allow"
        assert evaluate(_ctx(action="s3:GetObject"), [p1, p2]).decision == "Allow"
        assert evaluate(_ctx(action="s3:DeleteObject"), [p1, p2]).decision == "ImplicitDeny"

    def test_empty_policies_implicit_deny(self):
        assert evaluate(_ctx(), []).decision == "ImplicitDeny"

    def test_admin_policy_allows_everything(self):
        stmts = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]})
        assert evaluate(_ctx(action="iam:CreateUser"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(action="ec2:RunInstances"), [stmts]).decision == "Allow"

    def test_deny_in_one_policy_overrides_allow_in_another(self):
        """AWS: explicit deny in ANY policy overrides allow in ANY other."""
        allow = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]})
        deny = parse_policy_document(
            {"Statement": [{"Effect": "Deny", "Action": "s3:Delete*", "Resource": "*"}]})
        assert evaluate(_ctx(action="s3:DeleteBucket"), [allow, deny]).decision == "Deny"
        assert evaluate(_ctx(action="s3:PutObject"), [allow, deny]).decision == "Allow"

    def test_every_limit_must_allow(self):
        identity = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": ["s3:*", "sqs:*"], "Resource": "*"}]})
        boundary = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": ["s3:*", "ec2:*"], "Resource": "*"}]})
        assert evaluate(_ctx(action="s3:PutObject"), [identity], [boundary]).decision == "Allow"
        assert evaluate(_ctx(action="sqs:SendMessage"), [identity], [boundary]).decision == "ImplicitDeny"
        assert evaluate(_ctx(action="ec2:RunInstances"), [identity], [boundary]).decision == "ImplicitDeny"
        assert evaluate(_ctx(action="s3:PutObject"), [identity], []).decision == "ImplicitDeny"


# ---------------------------------------------------------------------------
# Condition operators (per IAM condition operators reference)
# ---------------------------------------------------------------------------

class TestConditions:
    def test_string_equals(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*", "Resource": "*",
            "Condition": {"StringEquals": {"aws:RequestedRegion": "us-east-1"}}
        }]})
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="eu-west-1"), [stmts]).decision == "ImplicitDeny"

    def test_string_like_with_wildcard(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*", "Resource": "*",
            "Condition": {"StringLike": {"aws:RequestedRegion": "us-*"}}
        }]})
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="us-west-2"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="eu-west-1"), [stmts]).decision == "ImplicitDeny"

    def test_string_not_equals_on_deny(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Deny", "Action": "s3:*", "Resource": "*",
            "Condition": {"StringNotEquals": {"aws:RequestedRegion": "us-east-1"}}
        }]})
        assert evaluate(_ctx(region="eu-west-1"), [stmts]).decision == "Deny"
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "ImplicitDeny"

    def test_string_equals_ignore_case(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"StringEqualsIgnoreCase": {"aws:RequestedRegion": "US-EAST-1"}}
        }]})
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "Allow"

    def test_ip_address_cidr(self):
        """AWS: IpAddress checks if IP is in CIDR range."""
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"IpAddress": {"aws:SourceIp": "127.0.0.0/8"}}
        }]})
        assert evaluate(_ctx(), [stmts]).decision == "Allow"

    def test_not_ip_address(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Deny", "Action": "*", "Resource": "*",
            "Condition": {"NotIpAddress": {"aws:SourceIp": "127.0.0.0/8"}}
        }]})
        # Default source_ip is 127.0.0.1, which IS in 127.0.0.0/8
        # So NotIpAddress is false → condition not met → deny not applied
        assert evaluate(_ctx(), [stmts]).decision == "ImplicitDeny"

    def test_bool_condition(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Deny", "Action": "*", "Resource": "*",
            "Condition": {"Bool": {"aws:SecureTransport": "true"}}
        }]})
        # secure_transport defaults to false → condition not met
        assert evaluate(_ctx(), [stmts]).decision == "ImplicitDeny"

    def test_null_key_present(self):
        """AWS: Null:false = key must exist. PrincipalArn always exists."""
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Deny", "Action": "*", "Resource": "*",
            "Condition": {"Null": {"aws:PrincipalArn": "false"}}
        }]})
        assert evaluate(_ctx(), [stmts]).decision == "Deny"

    def test_null_key_absent(self):
        """AWS: Null:true = key must NOT exist."""
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"Null": {"aws:SourceVpc": "true"}}
        }]})
        # SourceVpc not in our context → absent → Null:true matches
        assert evaluate(_ctx(), [stmts]).decision == "Allow"

    def test_arn_like(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"ArnLike": {
                "aws:PrincipalArn": "arn:aws:iam::000000000000:user/*"
            }}
        }]})
        assert evaluate(_ctx(), [stmts]).decision == "Allow"

    def test_multiple_values_are_ored(self):
        """AWS: multiple values for a condition key are OR'd."""
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*", "Resource": "*",
            "Condition": {"StringEquals": {
                "aws:RequestedRegion": ["us-east-1", "us-west-2"]
            }}
        }]})
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="us-west-2"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="eu-west-1"), [stmts]).decision == "ImplicitDeny"

    def test_multiple_operators_are_anded(self):
        """AWS: multiple condition blocks are AND'd."""
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*", "Resource": "*",
            "Condition": {
                "StringEquals": {"aws:RequestedRegion": "us-east-1"},
                "IpAddress": {"aws:SourceIp": "127.0.0.0/8"},
            }
        }]})
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "Allow"
        # Region doesn't match → AND fails
        assert evaluate(_ctx(region="eu-west-1"), [stmts]).decision == "ImplicitDeny"

    def test_if_exists_key_absent(self):
        """AWS: *IfExists = if key is absent, condition is satisfied."""
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"StringEqualsIfExists": {
                "aws:SourceVpc": "vpc-12345"  # key not in context
            }}
        }]})
        assert evaluate(_ctx(), [stmts]).decision == "Allow"

    def test_if_exists_key_present_must_match(self):
        """AWS: *IfExists with key present = normal check."""
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"StringEqualsIfExists": {
                "aws:RequestedRegion": "eu-west-1"
            }}
        }]})
        # RequestedRegion IS present and is "us-east-1", not "eu-west-1"
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "ImplicitDeny"

    def test_for_all_values_multivalued_key(self):
        """AWS: ForAllValues = all request values must match at least one policy value."""
        ctx = _ctx()
        ctx.tag_keys = ["env", "team"]
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"ForAllValues:StringEquals": {
                "aws:TagKeys": ["env", "team", "project"]
            }}
        }]})
        assert evaluate(ctx, [stmts]).decision == "Allow"

    def test_for_any_value_multivalued_key(self):
        """AWS: ForAnyValue = at least one request value must match."""
        ctx = _ctx()
        ctx.tag_keys = ["env", "cost-center"]
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"ForAnyValue:StringEquals": {
                "aws:TagKeys": ["env", "team"]
            }}
        }]})
        # "env" matches → ForAnyValue satisfied
        assert evaluate(ctx, [stmts]).decision == "Allow"

    def test_for_any_value_no_match(self):
        ctx = _ctx()
        ctx.tag_keys = ["cost-center", "department"]
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"ForAnyValue:StringEquals": {
                "aws:TagKeys": ["env", "team"]
            }}
        }]})
        assert evaluate(ctx, [stmts]).decision == "ImplicitDeny"


# Measured with iam simulate-custom-policy: Allow *, plus a Deny under the one
# condition. D = explicit deny, A = allowed, for the key absent / matching / other.
_ARN = "arn:aws:sns:eu-central-1:111122223333:topic"
_KEY_PRESENCE_CASES = [
    ("StringNotEquals", "blue", "blue", "red", "DAD"),
    ("StringNotEqualsIgnoreCase", "BLUE", "blue", "red", "DAD"),
    ("StringNotLike", "bl*", "blue", "red", "DAD"),
    ("NumericNotEquals", "10", "10", "5", "DAD"),
    ("DateNotEquals", "2030-01-01T00:00:00Z", "2030-01-01T00:00:00Z", "2031-01-01T00:00:00Z", "DAD"),
    ("ArnNotEquals", _ARN, _ARN, _ARN + "-other", "DAD"),
    ("ArnNotLike", "arn:aws:sns:*:111122223333:top*", _ARN, _ARN[:-5] + "other", "DAD"),
    ("NotIpAddress", "10.0.0.0/8", "10.1.2.3", "192.168.1.1", "DAD"),
    ("ForAnyValue:StringNotEquals", ["team"], ["team"], ["other"], "AAD"),
    ("ForAllValues:StringNotEquals", ["team"], ["team"], ["other"], "DAD"),
    ("ForAnyValue:StringNotLike", ["te*"], ["team"], ["other"], "AAD"),
    ("ForAllValues:StringNotLike", ["te*"], ["team"], ["other"], "DAD"),
    ("StringNotEqualsIfExists", "blue", "blue", "red", "DAD"),
    ("StringEquals", "blue", "blue", "red", "ADA"),
    ("StringLike", "bl*", "blue", "red", "ADA"),
    ("NumericLessThan", "10", "5", "20", "ADA"),
    ("DateGreaterThan", "2030-01-01T00:00:00Z", "2031-01-01T00:00:00Z", "2029-01-01T00:00:00Z", "ADA"),
    ("ArnLike", "arn:aws:sns:*:111122223333:top*", _ARN, _ARN[:-5] + "other", "ADA"),
    ("IpAddress", "10.0.0.0/8", "10.1.2.3", "192.168.1.1", "ADA"),
    ("Bool", "true", "true", "false", "ADA"),
    ("ForAnyValue:StringEquals", ["team"], ["team"], ["other"], "ADA"),
    ("ForAllValues:StringEquals", ["team"], ["team"], ["other"], "DDA"),
    ("StringEqualsIfExists", "blue", "blue", "red", "DDA"),
    ("Null", "true", "blue", "blue", "DAA"),
]


@pytest.mark.parametrize("state", range(3), ids=["absent", "matching", "other"])
@pytest.mark.parametrize("operator, policy_value, matching, other, measured", [
    pytest.param(*case, id=case[0]) for case in _KEY_PRESENCE_CASES
])
def test_condition_operator_by_key_presence(operator, policy_value, matching, other, measured, state):
    ctx = _ctx()
    value = (None, matching, other)[state]
    if value is not None:
        ctx.service_context = {"test:key": value}
    stmts = parse_policy_document({"Statement": [
        {"Effect": "Allow", "Action": "*", "Resource": "*"},
        {"Effect": "Deny", "Action": "*", "Resource": "*",
         "Condition": {operator: {"test:key": policy_value}}},
    ]})
    expected = "Deny" if measured[state] == "D" else "Allow"
    assert evaluate(ctx, [stmts]).decision == expected


_NEGATED_VALUE_LIST_CASES = [
    ("StringNotEquals", ["qualification", "recovery"], "qualification", "recovery", "neighbor"),
    ("StringNotEqualsIgnoreCase", ["QUALIFICATION", "RECOVERY"], "qualification", "recovery", "neighbor"),
    ("StringNotLike", ["qual*", "rec*"], "qualification", "recovery", "neighbor"),
    ("NumericNotEquals", ["10", "20"], "10", "20", "30"),
    ("DateNotEquals", ["2030-01-01T00:00:00Z", "2031-01-01T00:00:00Z"],
     "2030-01-01T00:00:00Z", "2031-01-01T00:00:00Z", "2032-01-01T00:00:00Z"),
    ("ArnNotEquals", [_ARN, _ARN + "-recovery"], _ARN, _ARN + "-recovery", _ARN + "-neighbor"),
    ("ArnNotLike", [_ARN + "*", "arn:aws:sqs:*:111122223333:recovery*"],
     _ARN, "arn:aws:sqs:eu-central-1:111122223333:recovery", "arn:aws:sqs:eu-central-1:111122223333:neighbor"),
    ("NotIpAddress", ["10.0.0.0/8", "192.168.0.0/16"], "10.1.2.3", "192.168.1.1", "172.16.1.1"),
]


class TestConditionValueLists:
    @pytest.mark.parametrize("operator, policy_values, first, second, other", [
        pytest.param(*case, id=case[0]) for case in _NEGATED_VALUE_LIST_CASES
    ])
    @pytest.mark.parametrize("state", range(3), ids=["first-listed", "second-listed", "unlisted"])
    @pytest.mark.parametrize("suffix", ["", "IfExists"])
    def test_negated_deny_matches_none_of_the_policy_values(
        self, operator, policy_values, first, second, other, state, suffix
    ):
        # AWS docs: policy values for a negated matching operator use NOR.
        ctx = _ctx()
        ctx.service_context = {"test:key": (first, second, other)[state]}
        stmts = parse_policy_document({"Statement": [
            {"Effect": "Allow", "Action": "*", "Resource": "*"},
            {"Effect": "Deny", "Action": "*", "Resource": "*",
             "Condition": {operator + suffix: {"test:key": policy_values}}},
        ]})
        assert evaluate(ctx, [stmts]).decision == ("Deny" if state == 2 else "Allow")

    @pytest.mark.parametrize("operator", ["StringNotEquals", "StringNotLike"])
    @pytest.mark.parametrize("quantifier", ["ForAnyValue", "ForAllValues"])
    @pytest.mark.parametrize("request_values, any_denies, all_denies", [
        (["qualification", "recovery"], False, False),
        (["qualification", "neighbor"], True, False),
        (["neighbor", "other"], True, True),
        ([], False, True),
        (None, False, True),
    ])
    def test_negated_set_quantifiers(self, operator, quantifier, request_values, any_denies, all_denies):
        ctx = _ctx()
        if request_values is not None:
            ctx.service_context = {"test:key": request_values}
        values = ["qual*", "rec*"] if operator == "StringNotLike" else ["qualification", "recovery"]
        stmts = parse_policy_document({"Statement": [
            {"Effect": "Allow", "Action": "*", "Resource": "*"},
            {"Effect": "Deny", "Action": "*", "Resource": "*",
             "Condition": {f"{quantifier}:{operator}": {"test:key": values}}},
        ]})
        denies = any_denies if quantifier == "ForAnyValue" else all_denies
        assert evaluate(ctx, [stmts]).decision == ("Deny" if denies else "Allow")

    @pytest.mark.parametrize("policy_key", [
        "aws:RequestTag/Environment", "aws:RequestTag/environment", "AWS:REQUESTTAG/ENVIRONMENT",
    ])
    @pytest.mark.parametrize("tag_value, expected", [("production", "Allow"), ("Production", "ImplicitDeny")])
    def test_request_tag_keys_ignore_case_but_string_equals_values_do_not(self, policy_key, tag_value, expected):
        ctx = _ctx()
        ctx.request_tags = {"Environment": tag_value}
        ctx.tag_keys = ["Environment"]
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"StringEquals": {policy_key: "production"}},
        }]})
        assert evaluate(ctx, [stmts]).decision == expected

    @pytest.mark.parametrize("policy_key, expected", [("Environment", "Allow"), ("environment", "ImplicitDeny")])
    def test_tag_keys_string_equals_preserves_case(self, policy_key, expected):
        ctx = _ctx()
        ctx.request_tags = {"Environment": "production"}
        ctx.tag_keys = ["Environment"]
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"ForAllValues:StringEquals": {"aws:TagKeys": [policy_key]}},
        }]})
        assert evaluate(ctx, [stmts]).decision == expected

    @pytest.mark.parametrize("policy_key", ["Environment", "environment", "ENVIRONMENT"])
    @pytest.mark.parametrize("quantifier", ["", "ForAnyValue:", "ForAllValues:"])
    @pytest.mark.parametrize("operator", ["StringEquals", "StringLike", "StringNotEquals", "StringNotLike"])
    @pytest.mark.parametrize("suffix", ["", "IfExists"])
    @pytest.mark.parametrize("tag_values", [
        ("production", "test"), ("production", "production"), ("test", "dev"),
    ], ids=["mixed", "both-match", "none-match"])
    @pytest.mark.parametrize("reverse_tags", [False, True])
    def test_case_colliding_request_tags_match_all_case_variants(
        self, policy_key, quantifier, operator, suffix, tag_values, reverse_tags
    ):
        # Live AWS, us-west-2: signed SQS CreateQueue with a disposable IAM
        # user's policy. An affirmative operator matches any case variant;
        # a negated operator matches only if none of the variants match.
        tags = list(zip(["Environment", "environment"], tag_values))
        ctx = _ctx()
        ctx.request_tags = dict(reversed(tags) if reverse_tags else tags)
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {quantifier + operator + suffix: {f"aws:RequestTag/{policy_key}": "production"}},
        }]})
        matches = "production" in tag_values
        if operator in ("StringNotEquals", "StringNotLike"):
            matches = not matches
        assert evaluate(ctx, [stmts]).decision == ("Allow" if matches else "ImplicitDeny")



class TestResourceAccountCondition:
    """``aws:ResourceAccount`` (and the ``s3:ResourceAccount`` alias) resolve to the
    account that owns the resource. The emulator hosts one account per request and
    models no cross-account access, so that is the requesting account unless the
    resource ARN carries an account field. CDK's bootstrap file-publishing role
    conditions its S3 grant on this key, so an unresolved key denied every
    ``cdk deploy`` under AUTH=true."""

    _S3 = {"Effect": "Allow", "Action": ["s3:GetBucket*", "s3:List*"],
           "Resource": ["arn:aws:s3:::cdk-assets", "arn:aws:s3:::cdk-assets/*"]}

    def _policy(self, condition):
        return parse_policy_document({"Statement": [dict(self._S3, Condition=condition)]})

    def test_same_account_condition_allows(self):
        stmts = self._policy({"StringEquals": {"aws:ResourceAccount": ["000000000000"]}})
        ctx = _ctx(action="s3:GetBucketLocation", resource="arn:aws:s3:::cdk-assets")
        assert evaluate(ctx, [stmts]).decision == "Allow"

    def test_s3_alias_allows(self):
        stmts = self._policy({"StringEquals": {"s3:ResourceAccount": "000000000000"}})
        ctx = _ctx(action="s3:ListBucket", resource="arn:aws:s3:::cdk-assets")
        assert evaluate(ctx, [stmts]).decision == "Allow"

    def test_foreign_account_condition_still_denies(self):
        stmts = self._policy({"StringEquals": {"aws:ResourceAccount": ["111111111111"]}})
        ctx = _ctx(action="s3:GetBucketLocation", resource="arn:aws:s3:::cdk-assets")
        assert evaluate(ctx, [stmts]).decision == "ImplicitDeny"

    def test_resource_arn_account_field_wins(self):
        # An ARN that names a different owning account is that account's resource.
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "sqs:SendMessage", "Resource": "*",
            "Condition": {"StringEquals": {"aws:ResourceAccount": "222222222222"}}}]})
        ctx = _ctx(action="sqs:SendMessage",
                   resource="arn:aws:sqs:us-east-1:222222222222:queue")
        assert evaluate(ctx, [stmts]).decision == "Allow"
        ctx_own = _ctx(action="sqs:SendMessage",
                       resource="arn:aws:sqs:us-east-1:000000000000:queue")
        assert evaluate(ctx_own, [stmts]).decision == "ImplicitDeny"

    def test_unknown_global_key_still_denies(self):
        stmts = self._policy({"StringEquals": {"aws:PrincipalOrgID": "o-abc"}})
        ctx = _ctx(action="s3:ListBucket", resource="arn:aws:s3:::cdk-assets")
        assert evaluate(ctx, [stmts]).decision == "ImplicitDeny"

    @pytest.mark.parametrize("arn,expected", [
        ("*", None),
        ("", None),
        ("arn:aws:s3:::bucket", None),                    # partition-only, no account
        ("arn:aws:s3:::bucket/key", None),
        ("arn:aws:iam::aws:policy/AdministratorAccess", None),  # AWS-owned
        ("arn:aws:sqs:us-east-1:222222222222:q", "222222222222"),
        ("not-an-arn", None),
        ("arn:aws:sqs", None),                            # too short
        ("arn:aws:sqs:us-east-1:12345:q", None),          # not twelve digits
        ("a:b:c:d:123456789012:e", None),                 # twelve digits, not an ARN
    ])
    def test_account_from_arn(self, arn, expected):
        from ministack.core.iam_evaluator import _account_from_arn
        assert _account_from_arn(arn) == expected

    def test_deny_statement_conditioned_on_resource_account(self):
        # A Deny guarded by aws:ResourceAccount fires for the caller's own account.
        stmts = parse_policy_document({"Statement": [
            {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
            {"Effect": "Deny", "Action": "s3:DeleteObject", "Resource": "*",
             "Condition": {"StringEquals": {"s3:ResourceAccount": "000000000000"}}},
        ]})
        ctx = _ctx(action="s3:DeleteObject", resource="arn:aws:s3:::cdk-assets/x")
        assert evaluate(ctx, [stmts]).decision == "Deny"
        ctx = _ctx(action="s3:GetObject", resource="arn:aws:s3:::cdk-assets/x")
        assert evaluate(ctx, [stmts]).decision == "Allow"

    def test_string_not_equals_on_resource_account(self):
        # The CDK bootstrap shape inverted: allow only when the resource is NOT
        # in a foreign account — resolves to the caller's account, so it allows.
        stmts = self._policy({"StringNotEquals": {"aws:ResourceAccount": "111111111111"}})
        ctx = _ctx(action="s3:ListBucket", resource="arn:aws:s3:::cdk-assets")
        assert evaluate(ctx, [stmts]).decision == "Allow"


# ---------------------------------------------------------------------------
# Trust policy evaluation (per AWS AssumeRole documentation)
# ---------------------------------------------------------------------------

class TestTrustPolicy:
    def test_wildcard_principal(self):
        trust = {"Statement": [{"Effect": "Allow", "Principal": "*",
                                "Action": "sts:AssumeRole"}]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::999:user/anyone")

    def test_account_root_allows_all_in_account(self):
        """AWS: arn:aws:iam::ACCT:root trusts all principals in that account."""
        trust = {"Statement": [{"Effect": "Allow",
                                "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                                "Action": "sts:AssumeRole"}]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::123456789012:user/alice")
        assert not evaluate_trust_policy(trust, "arn:aws:iam::999999999999:user/bob")

    def test_specific_user_principal(self):
        trust = {"Statement": [{"Effect": "Allow",
                                "Principal": {"AWS": "arn:aws:iam::123456789012:user/alice"},
                                "Action": "sts:AssumeRole"}]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::123456789012:user/alice")
        assert not evaluate_trust_policy(trust, "arn:aws:iam::123456789012:user/bob")

    def test_service_principal_allows_root(self):
        """Service principals allow root (MiniStack's internal service calls use root)."""
        trust = {"Statement": [{"Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole"}]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::123456789012:root")

    def test_service_principal_denies_user_named_after_service(self):
        trust = {"Statement": [{"Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole"}]}
        assert not evaluate_trust_policy(trust, "arn:aws:iam::123456789012:user/lambda-ops")

    def test_service_principal_denies_unrelated_user(self):
        """A service principal does NOT allow arbitrary IAM users."""
        trust = {"Statement": [{"Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole"}]}
        assert not evaluate_trust_policy(trust, "arn:aws:iam::123:user/anyone")

    def test_account_id_shorthand(self):
        """AWS: account ID alone = arn:aws:iam::ACCT:root."""
        trust = {"Statement": [{"Effect": "Allow",
                                "Principal": {"AWS": "123456789012"},
                                "Action": "sts:AssumeRole"}]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::123456789012:user/alice")
        assert not evaluate_trust_policy(trust, "arn:aws:iam::999999999999:user/bob")

    def test_multiple_principals_list(self):
        trust = {"Statement": [{"Effect": "Allow",
                                "Principal": {"AWS": [
                                    "arn:aws:iam::111111111111:root",
                                    "arn:aws:iam::222222222222:root"]},
                                "Action": "sts:AssumeRole"}]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::111111111111:user/a")
        assert evaluate_trust_policy(trust, "arn:aws:iam::222222222222:user/b")
        assert not evaluate_trust_policy(trust, "arn:aws:iam::333333333333:user/c")

    def test_wrong_action_not_matched(self):
        trust = {"Statement": [{"Effect": "Allow", "Principal": "*",
                                "Action": "s3:GetObject"}]}
        assert not evaluate_trust_policy(trust, "arn:aws:iam::123:user/a")

    def test_deny_effect_not_matched(self):
        trust = {"Statement": [{"Effect": "Deny", "Principal": "*",
                                "Action": "sts:AssumeRole"}]}
        assert not evaluate_trust_policy(trust, "arn:aws:iam::123:user/a")

    def test_federated_principal_matches_only_web_identity(self):
        provider = "arn:aws:iam::123456789012:oidc-provider/issuer.example.com"
        trust = {"Statement": [
            {"Effect": "Allow", "Principal": {"Federated": provider},
             "Action": "sts:AssumeRoleWithWebIdentity"},
            {"Effect": "Allow", "Principal": {"AWS": "123456789012"},
             "Action": "sts:AssumeRoleWithWebIdentity"}]}
        other = "arn:aws:iam::123456789012:oidc-provider/other.example.com"
        for arn, allowed in ((provider, True), (other, False)):
            ctx = EvalContext(principal_arn=arn, principal_type="WebIdentityUser",
                              principal_account="123456789012", action="sts:AssumeRoleWithWebIdentity",
                              resource_arn="*", region="")
            assert evaluate_trust_policy(trust, arn, ctx) is allowed

    def test_string_json_input(self):
        trust_str = json.dumps({"Statement": [{"Effect": "Allow", "Principal": "*",
                                               "Action": "sts:AssumeRole"}]})
        assert evaluate_trust_policy(trust_str, "arn:aws:iam::123:user/a")

    def test_invalid_json_returns_false(self):
        assert not evaluate_trust_policy("not json", "arn:aws:iam::123:user/a")


# ---------------------------------------------------------------------------
# Principal resolution — AWS error codes
# (per AWS STS/IAM Common Errors reference)
# ---------------------------------------------------------------------------

class TestResolvePrincipal:
    def test_empty_key_is_root(self):
        result = resolve_principal("", "000000000000")
        assert isinstance(result, PrincipalInfo)
        assert result.type == "Root"
        assert result.policies is None

    def test_test_key_is_root(self):
        """Default boto3 key 'test' should work as root."""
        result = resolve_principal("test", "000000000000")
        assert isinstance(result, PrincipalInfo)
        assert result.type == "Root"

    def test_twelve_digit_account_id_is_root(self):
        result = resolve_principal("123456789012", "123456789012")
        assert isinstance(result, PrincipalInfo)
        assert result.type == "Root"

    def test_unknown_key_returns_unrecognized_client(self):
        """AWS: unknown access key → UnrecognizedClientException (HTTP 403)."""
        result = resolve_principal("AKIAFAKEKEY1234567", "000000000000")
        assert isinstance(result, AuthError)
        assert result.code == "UnrecognizedClientException"

    def test_inactive_key_returns_invalid_client_token(self):
        """AWS: inactive access key → InvalidClientTokenId (HTTP 403)."""
        from ministack.services import iam as iam_svc
        fake_key = "AKIATESTIACTV00001"
        iam_svc._access_keys[fake_key] = {
            "UserName": "inactive-user", "AccessKeyId": fake_key,
            "SecretAccessKey": "s", "Status": "Inactive", "CreateDate": "2024-01-01",
        }
        try:
            result = resolve_principal(fake_key, "000000000000")
            assert isinstance(result, AuthError)
            assert result.code == "InvalidClientTokenId"
        finally:
            iam_svc._access_keys.pop(fake_key, None)

    def test_expired_session_returns_expired_token(self):
        """AWS: expired session token → ExpiredTokenException (HTTP 403)."""
        from ministack.services import sts as sts_svc
        fake_key = "ASIATESTEXPIRED001"
        sts_svc._sessions[fake_key] = {
            "Arn": "arn:aws:sts::000000000000:assumed-role/r/s",
            "UserId": "AROA123:s", "SecretAccessKey": "secret",
            "Expiration": time.time() - 3600,
        }
        try:
            result = resolve_principal(fake_key, "000000000000")
            assert isinstance(result, AuthError)
            assert result.code == "ExpiredTokenException"
        finally:
            sts_svc._sessions.pop(fake_key, None)

    def test_valid_session_resolves_to_assumed_role(self):
        from ministack.services import sts as sts_svc
        fake_key = "ASIATESTVALID00001"
        sts_svc._sessions[fake_key] = {
            "Arn": "arn:aws:sts::000000000000:assumed-role/testrole/sess",
            "UserId": "AROA123:sess", "SecretAccessKey": "secret",
            "Expiration": time.time() + 3600,
        }
        try:
            result = resolve_principal(fake_key, "000000000000")
            assert isinstance(result, PrincipalInfo)
            assert result.type == "AssumedRole"
            assert "assumed-role/testrole" in result.arn
        finally:
            sts_svc._sessions.pop(fake_key, None)

    def test_active_access_key_resolves_to_user(self):
        from ministack.services import iam as iam_svc
        fake_key = "AKIATESTACTIVE0001"
        iam_svc._access_keys[fake_key] = {
            "UserName": "active-user", "AccessKeyId": fake_key,
            "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
        }
        try:
            result = resolve_principal(fake_key, "000000000000")
            assert isinstance(result, PrincipalInfo)
            assert result.type == "User"
            assert "active-user" in result.arn
        finally:
            iam_svc._access_keys.pop(fake_key, None)


# ---------------------------------------------------------------------------
# End-to-end enforce() flow
# ---------------------------------------------------------------------------

class TestEnforce:
    def test_root_key_always_allowed(self):
        assert enforce("test", "s3:DeleteBucket", "s3", "us-east-1") is None

    def test_unknown_key_returns_auth_error(self):
        result = enforce("AKIAFAKEUNKNOWN123", "s3:ListBuckets", "s3", "us-east-1")
        assert isinstance(result, AuthError)
        assert result.code == "UnrecognizedClientException"

    def test_user_with_matching_policy_allowed(self):
        from ministack.services import iam as iam_svc
        fake_key = "AKIATESTENFRC00001"
        iam_svc._access_keys[fake_key] = {
            "UserName": "enforce-user", "AccessKeyId": fake_key,
            "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
        }
        iam_svc._users["enforce-user"] = {
            "UserName": "enforce-user",
            "Arn": "arn:aws:iam::000000000000:user/enforce-user",
            "UserId": "AIDA123", "CreateDate": "2024-01-01", "Path": "/",
            "AttachedPolicies": [], "Tags": [],
        }
        # User inline policies live in the separate _user_inline_policies dict
        iam_svc._user_inline_policies["enforce-user"] = {
            "p": json.dumps({
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
            })
        }
        try:
            assert enforce(fake_key, "s3:PutObject", "s3", "us-east-1") is None
            result = enforce(fake_key, "ec2:RunInstances", "ec2", "us-east-1")
            assert isinstance(result, EvalResult)
            assert result.decision == "ImplicitDeny"
        finally:
            iam_svc._access_keys.pop(fake_key, None)
            iam_svc._users.pop("enforce-user", None)
            iam_svc._user_inline_policies.pop("enforce-user", None)

    def test_secretsmanager_suffix_grant_matches_the_stored_arn(self):
        """The grant shape the CDK writes for a secret looked up by name.

        Measured on AWS: a policy on "secret:<name>-??????" allows a request
        against "secret:<name>-0ac6da". The resolved resource therefore has to
        carry the stored suffix, or a correctly scoped policy denies.
        """
        from ministack.core.iam_actions import extract_resource_arn
        from ministack.core.responses import get_account_id
        from ministack.services import iam as iam_svc
        from ministack.services import secretsmanager as sm

        fake_key = "AKIATESTSECRET0001"
        stored = sm.create_secret_in_process("enforce-suffix-secret", "v")
        prefix = stored.rsplit("-", 1)[0]
        iam_svc._access_keys[fake_key] = {
            "UserName": "secret-user", "AccessKeyId": fake_key,
            "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
        }
        iam_svc._users["secret-user"] = {
            "UserName": "secret-user",
            "Arn": "arn:aws:iam::000000000000:user/secret-user",
            "UserId": "AIDA789", "CreateDate": "2024-01-01", "Path": "/",
            "AttachedPolicies": [], "Tags": [],
        }
        iam_svc._user_inline_policies["secret-user"] = {
            "p": json.dumps({"Statement": [{
                "Effect": "Allow",
                "Action": "secretsmanager:GetSecretValue",
                "Resource": f"{prefix}-??????",
            }]})
        }

        def decide(secret_id):
            resource = extract_resource_arn(
                "secretsmanager", "POST", "/", {},
                json.dumps({"SecretId": secret_id}).encode(), {},
                "us-east-1", get_account_id(),
            )
            return enforce(
                fake_key, "secretsmanager:GetSecretValue", "secretsmanager",
                "us-east-1", resource_arn=resource,
            )

        try:
            assert decide("enforce-suffix-secret") is None
            # The same grant on a different secret still denies.
            assert decide("some-other-secret").decision == "ImplicitDeny"
            # An explicit Deny on the stored ARN now matches as well.
            iam_svc._user_inline_policies["secret-user"]["d"] = json.dumps({"Statement": [{
                "Effect": "Deny",
                "Action": "secretsmanager:GetSecretValue",
                "Resource": f"{prefix}-*",
            }]})
            assert decide("enforce-suffix-secret").decision == "Deny"
        finally:
            sm._secrets.pop("enforce-suffix-secret", None)
            iam_svc._access_keys.pop(fake_key, None)
            iam_svc._users.pop("secret-user", None)
            iam_svc._user_inline_policies.pop("secret-user", None)

    def test_explicit_deny_in_policy_blocks(self):
        from ministack.services import iam as iam_svc
        fake_key = "AKIATESTDENY000001"
        iam_svc._access_keys[fake_key] = {
            "UserName": "deny-user", "AccessKeyId": fake_key,
            "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
        }
        iam_svc._users["deny-user"] = {
            "UserName": "deny-user",
            "Arn": "arn:aws:iam::000000000000:user/deny-user",
            "UserId": "AIDA456", "CreateDate": "2024-01-01", "Path": "/",
            "AttachedPolicies": [], "Tags": [],
        }
        iam_svc._user_inline_policies["deny-user"] = {
            "p": json.dumps({"Statement": [
                {"Effect": "Allow", "Action": "*", "Resource": "*"},
                {"Effect": "Deny", "Action": "s3:DeleteBucket", "Resource": "*"},
            ]})
        }
        try:
            assert enforce(fake_key, "s3:PutObject", "s3", "us-east-1") is None
            result = enforce(fake_key, "s3:DeleteBucket", "s3", "us-east-1")
            assert isinstance(result, EvalResult)
            assert result.decision == "Deny"
        finally:
            iam_svc._access_keys.pop(fake_key, None)
            iam_svc._users.pop("deny-user", None)
            iam_svc._user_inline_policies.pop("deny-user", None)

    def test_user_inherits_group_inline_policy(self):
        """User policies include inline policies from their groups."""
        from ministack.services import iam as iam_svc
        fake_key = "AKIATESTGROUP00001"
        iam_svc._access_keys[fake_key] = {
            "UserName": "group-user", "AccessKeyId": fake_key,
            "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
        }
        iam_svc._users["group-user"] = {
            "UserName": "group-user",
            "Arn": "arn:aws:iam::000000000000:user/group-user",
            "UserId": "AIDA789", "CreateDate": "2024-01-01", "Path": "/",
            "AttachedPolicies": [], "Tags": [],
        }
        iam_svc._groups["dev-team"] = {
            "GroupName": "dev-team", "GroupId": "AGPA123",
            "Arn": "arn:aws:iam::000000000000:group/dev-team",
            "Users": ["group-user"], "AttachedPolicies": [],
        }
        iam_svc._group_inline_policies["dev-team"] = {
            "s3-access": json.dumps({
                "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
            })
        }
        try:
            # s3 allowed via group policy
            assert enforce(fake_key, "s3:PutObject", "s3", "us-east-1") is None
            # ec2 not in any policy
            result = enforce(fake_key, "ec2:RunInstances", "ec2", "us-east-1")
            assert isinstance(result, EvalResult)
            assert result.decision == "ImplicitDeny"
        finally:
            iam_svc._access_keys.pop(fake_key, None)
            iam_svc._users.pop("group-user", None)
            iam_svc._groups.pop("dev-team", None)
            iam_svc._group_inline_policies.pop("dev-team", None)

    def test_user_inherits_user_inline_policy(self):
        """User inline policies are stored in _user_inline_policies, not on the user object."""
        from ministack.services import iam as iam_svc
        fake_key = "AKIATESTUSRPOL0001"
        iam_svc._access_keys[fake_key] = {
            "UserName": "inline-user", "AccessKeyId": fake_key,
            "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
        }
        iam_svc._users["inline-user"] = {
            "UserName": "inline-user",
            "Arn": "arn:aws:iam::000000000000:user/inline-user",
            "UserId": "AIDA012", "CreateDate": "2024-01-01", "Path": "/",
            "AttachedPolicies": [], "Tags": [],
        }
        iam_svc._user_inline_policies["inline-user"] = {
            "my-policy": json.dumps({
                "Statement": [{"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"}]
            })
        }
        try:
            assert enforce(fake_key, "dynamodb:PutItem", "dynamodb", "us-east-1") is None
            result = enforce(fake_key, "s3:GetObject", "s3", "us-east-1")
            assert isinstance(result, EvalResult)
            assert result.decision == "ImplicitDeny"
        finally:
            iam_svc._access_keys.pop(fake_key, None)
            iam_svc._users.pop("inline-user", None)
            iam_svc._user_inline_policies.pop("inline-user", None)


_PROBES = {"S": "sqs:ListQueues", "D": "dynamodb:ListTables",
           "B": "s3:ListAllMyBuckets", "L": "logs:DescribeLogGroups"}
_IDENTITY = {"Statement": [{"Effect": "Allow", "Resource": "*",
                            "Action": ["sqs:ListQueues", "dynamodb:ListTables", "logs:DescribeLogGroups"]}]}
_BOUNDARY = {"Statement": [
    {"Effect": "Allow", "Resource": "*",
     "Action": ["sqs:ListQueues", "dynamodb:ListTables", "s3:ListAllMyBuckets"]},
    {"Effect": "Deny", "Action": "dynamodb:ListTables", "Resource": "*"},
]}


def _allowed(access_key):
    """The probe actions ``enforce`` lets through, as letters."""
    return "".join(letter for letter, action in _PROBES.items()
                   if enforce(access_key, action, action.split(":")[0], "us-east-1") is None)


def _iam_call(action, **params):
    from ministack.services import iam as iam_svc
    status, _headers, body = iam_svc._IAM_HANDLERS[action](
        {"Action": [action], **{k: [v] for k, v in params.items()}})
    assert status == 200, body
    return body


@pytest.fixture
def boundary_principals():
    """A user and a role session with the probe identity policy, and a boundary policy."""
    from ministack.services import iam as iam_svc
    from ministack.services import sts as sts_svc
    boundary = "arn:aws:iam::000000000000:policy/probe-boundary"
    iam_svc.store_policy(boundary, "probe-boundary", "/", json.dumps(_BOUNDARY))
    _iam_call("CreateUser", UserName="probe-user")
    _iam_call("PutUserPolicy", UserName="probe-user", PolicyName="identity",
              PolicyDocument=json.dumps(_IDENTITY))
    iam_svc._access_keys["AKIATESTBOUNDARY01"] = {
        "UserName": "probe-user", "AccessKeyId": "AKIATESTBOUNDARY01",
        "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
    }
    _iam_call("CreateRole", RoleName="probe-role", AssumeRolePolicyDocument=json.dumps({"Statement": [
        {"Effect": "Allow", "Principal": {"AWS": "000000000000"}, "Action": "sts:AssumeRole"}]}))
    _iam_call("PutRolePolicy", RoleName="probe-role", PolicyName="identity",
              PolicyDocument=json.dumps(_IDENTITY))
    sts_svc._sessions["ASIATESTBOUNDARY01"] = {
        "Arn": "arn:aws:sts::000000000000:assumed-role/probe-role/s",
        "UserId": "AROA123:s", "SecretAccessKey": "s", "PrincipalType": "AssumedRole",
        "AccountId": "000000000000", "Expiration": time.time() + 3600,
    }
    try:
        yield {"user": "AKIATESTBOUNDARY01", "role": "ASIATESTBOUNDARY01", "boundary": boundary}
    finally:
        sts_svc._sessions.pop("ASIATESTBOUNDARY01", None)
        iam_svc._access_keys.pop("AKIATESTBOUNDARY01", None)
        iam_svc._user_inline_policies.pop("probe-user", None)
        iam_svc._users.pop("probe-user", None)
        iam_svc._roles.pop("probe-role", None)
        iam_svc._policies.pop(boundary, None)


class TestPermissionsBoundary:
    """A boundary limits identity policies: only actions both allow pass, and its Deny denies."""

    @pytest.mark.parametrize("kind", ["user", "role"])
    def test_boundary_limits_identity_policies(self, boundary_principals, kind):
        key, boundary = boundary_principals[kind], boundary_principals["boundary"]
        name = {"UserName": "probe-user"} if kind == "user" else {"RoleName": "probe-role"}
        entity = "User" if kind == "user" else "Role"
        assert _allowed(key) == "SDL"
        _iam_call(f"Put{entity}PermissionsBoundary", PermissionsBoundary=boundary, **name)
        assert _allowed(key) == "S"
        _iam_call(f"Delete{entity}PermissionsBoundary", **name)
        assert _allowed(key) == "SDL"

    def test_boundary_set_at_creation(self, boundary_principals):
        from ministack.services import iam as iam_svc
        _iam_call("CreateUser", UserName="probe-user-2", PermissionsBoundary=boundary_principals["boundary"])
        _iam_call("PutUserPolicy", UserName="probe-user-2", PolicyName="identity",
                  PolicyDocument=json.dumps(_IDENTITY))
        iam_svc._access_keys["AKIATESTBOUNDARY02"] = {
            "UserName": "probe-user-2", "AccessKeyId": "AKIATESTBOUNDARY02",
            "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
        }
        try:
            assert _allowed("AKIATESTBOUNDARY02") == "S"
            denied = enforce("AKIATESTBOUNDARY02", "dynamodb:ListTables", "dynamodb", "us-east-1")
            assert denied.decision == "Deny"
        finally:
            iam_svc._access_keys.pop("AKIATESTBOUNDARY02", None)
            iam_svc._user_inline_policies.pop("probe-user-2", None)
            iam_svc._users.pop("probe-user-2", None)


def _sts_key(access_key, **params):
    """Call STS in-process as ``access_key`` and return the issued access key."""
    import re
    from urllib.parse import urlencode

    from ministack.services import sts as sts_svc
    headers = {
        "authorization": f"AWS4-HMAC-SHA256 Credential={access_key}/20260930/us-east-1/sts/aws4_request",
        "content-type": "application/x-www-form-urlencoded",
    }
    body = urlencode({"Version": "2011-06-15", **params}).encode()
    status, _headers, payload = asyncio.run(sts_svc.handle_request("POST", "/", headers, body, {}))
    assert status == 200, payload
    return re.search(r"<AccessKeyId>(.*?)</AccessKeyId>", payload.decode()).group(1)


def _allow(*actions, deny=None):
    stmts = [{"Effect": "Allow", "Action": list(actions), "Resource": "*"}]
    if deny:
        stmts.append({"Effect": "Deny", "Action": deny, "Resource": "*"})
    return json.dumps({"Version": "2012-10-17", "Statement": stmts})


@pytest.fixture
def issued_sessions():
    from ministack.services import sts as sts_svc
    before = set(sts_svc._sessions)
    yield
    for key in set(sts_svc._sessions) - before:
        sts_svc._sessions.pop(key, None)


class TestSessionPolicies:
    """Session policies narrow a session to what both allow; a federated user without one gets nothing."""

    def _assume(self, **params):
        return _sts_key("test", Action="AssumeRole", RoleSessionName="s",
                        RoleArn="arn:aws:iam::000000000000:role/probe-role", **params)

    def test_session_policy_limits_the_role(self, boundary_principals, issued_sessions):
        from ministack.services import iam as iam_svc
        logs = "arn:aws:iam::000000000000:policy/probe-logs"
        iam_svc.store_policy(logs, "probe-logs", "/", _allow("logs:DescribeLogGroups"))
        try:
            assert _allowed(self._assume()) == "SDL"
            assert _allowed(self._assume(Policy=_allow("sqs:ListQueues", "s3:ListAllMyBuckets"))) == "S"
            assert _allowed(self._assume(**{"PolicyArns.member.1.arn": logs})) == "L"
            assert _allowed(self._assume(Policy=_allow("*", deny="dynamodb:ListTables"))) == "SL"
        finally:
            iam_svc._policies.pop(logs, None)

    def test_session_policy_and_boundary_both_limit(self, boundary_principals, issued_sessions):
        _iam_call("PutRolePermissionsBoundary", RoleName="probe-role",
                  PermissionsBoundary=boundary_principals["boundary"])
        assert _allowed(self._assume(Policy=_allow("dynamodb:ListTables", "logs:DescribeLogGroups"))) == ""
        assert _allowed(self._assume(Policy=_allow("*"))) == "S"

    def test_federated_user_is_limited_by_user_and_session_policies(self, boundary_principals, issued_sessions):
        user = boundary_principals["user"]
        fed = _sts_key(user, Action="GetFederationToken", Name="fed",
                       Policy=_allow("sqs:ListQueues", "s3:ListAllMyBuckets"))
        assert _allowed(fed) == "S"
        assert _allowed(_sts_key(user, Action="GetFederationToken", Name="fed")) == ""
        _iam_call("PutUserPermissionsBoundary", UserName="probe-user",
                  PermissionsBoundary=boundary_principals["boundary"])
        assert _allowed(_sts_key(user, Action="GetFederationToken", Name="fed", Policy=_allow("*"))) == "S"

    def test_federated_user_principal_tags_are_the_session_tags(self, boundary_principals, issued_sessions):
        from ministack.services import iam as iam_svc
        iam_svc._users["probe-user"]["Tags"] = [{"Key": "team", "Value": "blue"}, {"Key": "env", "Value": "dev"}]
        fed = _sts_key(boundary_principals["user"], Action="GetFederationToken", Name="fed", Policy=_allow("*"),
                       **{"Tags.member.1.Key": "TEAM", "Tags.member.1.Value": "green"})
        keys = resolve_principal(fed, "000000000000").keys
        assert keys["aws:principaltag/team"] == "green" and "aws:principaltag/env" not in keys
        assert keys["aws:userid"] == "000000000000:fed" and "aws:username" not in keys

    def test_root_federation_is_limited_by_the_session_policy(self, issued_sessions):
        fed = _sts_key("test", Action="GetFederationToken", Name="fed", Policy=_allow("sqs:ListQueues"))
        assert _allowed(fed) == "S"
        assert resolve_principal(fed, "000000000000").arn == "arn:aws:sts::000000000000:federated-user/fed"


class TestCustomerManagedPolicyResolution:
    """Customer policies belong to the explicit account and use full ARNs."""

    OWNER = "111111111111"
    OTHER = "222222222222"
    ARN = f"arn:aws:iam::{OWNER}:policy/team/connect"
    DOCUMENT = {"Statement": [{"Effect": "Allow", "Action": "rds-db:connect", "Resource": "*"}]}

    @pytest.fixture
    def policy(self, monkeypatch):
        from ministack.core.responses import AccountScopedDict, request_scope
        from ministack.services import iam as iam_svc

        monkeypatch.setattr(iam_svc, "_policies", AccountScopedDict())
        with request_scope(self.OWNER, "us-east-1"):
            return iam_svc.store_policy(self.ARN, "connect", "/team/", self.DOCUMENT)

    @pytest.mark.parametrize("requested,ambient,found", [
        (OWNER, OTHER, True),
        (OTHER, OWNER, False),
    ])
    def test_uses_explicit_account_and_preserves_context(self, policy, requested, ambient, found):
        from ministack.core.iam_evaluator import _resolve_managed_policy_document
        from ministack.core.responses import get_account_id, get_region, request_scope

        with request_scope(ambient, "eu-west-1"):
            document = _resolve_managed_policy_document(self.ARN, requested)
            assert document == (policy["Versions"]["v1"]["Document"] if found else None)
            assert (get_account_id(), get_region()) == (ambient, "eu-west-1")

    @pytest.mark.parametrize("arn", [
        ARN.replace("policy/team/", "policy/"),
        ARN.replace("policy/team/", "policy/other/"),
        ARN.replace(OWNER, OTHER),
        ARN + "-missing",
    ])
    def test_requires_complete_matching_arn(self, policy, arn):
        from ministack.core.iam_evaluator import _resolve_managed_policy_document
        from ministack.core.responses import request_scope

        with request_scope(self.OWNER, "us-east-1"):
            assert _resolve_managed_policy_document(arn, self.OWNER) is None

    def test_current_default_version_supplies_role_permissions(self, policy, monkeypatch):
        from ministack.core.iam_evaluator import _gather_role_policies
        from ministack.core.responses import AccountScopedDict, get_account_id, get_region, request_scope
        from ministack.services import iam as iam_svc

        monkeypatch.setattr(iam_svc, "_roles", AccountScopedDict())
        iam_svc._roles.set_scoped(self.OWNER, None, "app", {"AttachedPolicies": [self.ARN]})
        policy["Versions"]["v2"] = {"Document": json.dumps({"Statement": [
            {"Effect": "Deny", "Action": "rds-db:connect", "Resource": "*"},
        ]})}
        ctx = EvalContext(
            principal_arn=f"arn:aws:iam::{self.OWNER}:role/app", principal_type="AssumedRole",
            principal_account=self.OWNER, action="rds-db:connect", resource_arn="*", region="us-east-1",
        )
        with request_scope(self.OTHER, "eu-west-1"):
            assert evaluate(ctx, _gather_role_policies("app", self.OWNER)).decision == "Allow"
            policy["DefaultVersionId"] = "v2"
            assert evaluate(ctx, _gather_role_policies("app", self.OWNER)).decision == "Deny"
            policy["DefaultVersionId"] = "v1"
            assert evaluate(ctx, _gather_role_policies("app", self.OWNER)).decision == "Allow"
            assert (get_account_id(), get_region()) == (self.OTHER, "eu-west-1")


class TestCombineResourcePolicy:
    """An identity decision combined with the policy of the resource the request addresses."""

    _USER = "arn:aws:iam::000000000000:user/u"

    def _decide(self, identity, principal, effect="Allow", owner="000000000000", caller=_USER, limits=()):
        from ministack.core.iam_evaluator import combine_resource_policy

        ctx = EvalContext(principal_arn=caller, principal_type="User", principal_account="000000000000",
                          action="sqs:SendMessage", resource_arn=f"arn:aws:sqs:us-east-1:{owner}:q",
                          region="us-east-1")
        policy = {"Statement": [{"Effect": effect, "Principal": {"AWS": principal},
                                 "Action": "sqs:*", "Resource": "*"}]}
        return combine_resource_policy(EvalResult(identity, caller), ctx, policy, owner, limits=limits).decision

    @pytest.mark.parametrize("identity,principal,effect,owner,expected", [
        ("ImplicitDeny", _USER, "Allow", "000000000000", "Allow"),
        ("ImplicitDeny", "*", "Allow", "000000000000", "Allow"),
        ("ImplicitDeny", "arn:aws:iam::000000000000:root", "Allow", "000000000000", "ImplicitDeny"),
        ("ImplicitDeny", "000000000000", "Allow", "000000000000", "ImplicitDeny"),
        ("Allow", "arn:aws:iam::000000000000:root", "Allow", "000000000000", "Allow"),
        ("Allow", _USER, "Deny", "000000000000", "Deny"),
        ("Allow", "arn:aws:iam::000000000000:user/other", "Deny", "000000000000", "Allow"),
        ("ImplicitDeny", _USER, "Allow", "111111111111", "ImplicitDeny"),
        ("Allow", "000000000000", "Allow", "111111111111", "Allow"),
        ("Allow", "arn:aws:iam::111111111111:root", "Allow", "111111111111", "ImplicitDeny"),
    ])
    def test_decision(self, identity, principal, effect, owner, expected):
        assert self._decide(identity, principal, effect, owner) == expected

    def test_a_statement_naming_the_role_names_its_sessions(self):
        session = "arn:aws:sts::000000000000:assumed-role/r/s"
        assert self._decide("ImplicitDeny", "arn:aws:iam::000000000000:role/app/r", caller=session) == "Allow"
        assert self._decide("ImplicitDeny", "arn:aws:iam::000000000000:role/x", caller=session) == "ImplicitDeny"

    _SESSION = "arn:aws:sts::000000000000:assumed-role/r/s"

    @pytest.mark.parametrize("caller,principal,limit_action,expected", [
        (_USER, _USER, "sqs:ListQueues", "Allow"),
        (_USER, "*", "sqs:ListQueues", "ImplicitDeny"),
        (_SESSION, _SESSION, "sqs:ListQueues", "Allow"),
        (_SESSION, "arn:aws:iam::000000000000:role/r", "sqs:ListQueues", "ImplicitDeny"),
        (_SESSION, "arn:aws:iam::000000000000:role/r", "sqs:SendMessage", "Allow"),
        ("arn:aws:sts::000000000000:federated-user/f", "arn:aws:sts::000000000000:federated-user/f",
         "sqs:ListQueues", "ImplicitDeny"),
    ])
    def test_limits_narrow_a_resource_allow_unless_it_names_the_user_or_session(
            self, caller, principal, limit_action, expected):
        from ministack.core.iam_evaluator import parse_policy_document

        limit = [parse_policy_document({"Statement": [
            {"Effect": "Allow", "Action": limit_action, "Resource": "*"}]})]
        assert self._decide("ImplicitDeny", principal, caller=caller, limits=(limit,)) == expected
@contextlib.contextmanager
def _iam_user(name, tags=()):
    """A stored IAM user with one access key; yields the key and a setter for its inline policy."""
    from ministack.core.responses import get_account_id
    from ministack.services import iam as iam_svc
    key = "AKIATEST" + name.upper().replace("-", "")
    iam_svc._access_keys[key] = {"UserName": name, "AccessKeyId": key, "SecretAccessKey": "s",
                                 "Status": "Active", "CreateDate": "2024-01-01"}
    iam_svc._users[name] = {"UserName": name, "Arn": f"arn:aws:iam::{get_account_id()}:user/{name}",
                            "UserId": "AIDA" + name.upper().replace("-", ""), "Path": "/",
                            "AttachedPolicies": [], "Tags": list(tags)}
    try:
        yield key, lambda doc: iam_svc._user_inline_policies.__setitem__(name, {"p": doc})
    finally:
        iam_svc._access_keys.pop(key, None)
        iam_svc._users.pop(name, None)
        iam_svc._user_inline_policies.pop(name, None)


def _allows(access_key, put_policy, condition):
    """Whether *access_key* may call sqs:ListQueues under a policy gated on *condition*."""
    put_policy(json.dumps({"Statement": [{
        "Effect": "Allow", "Action": "sqs:ListQueues", "Resource": "*", "Condition": condition}]}))
    return enforce(access_key, "sqs:ListQueues", "sqs", "us-east-1") is None


class TestGlobalConditionKeys:
    """The caller and transport condition keys."""

    @pytest.fixture
    def user(self):
        with _iam_user("ck-user", [{"Key": "Team", "Value": "blue"}]) as user:
            yield user

    @pytest.fixture
    def role_session(self):
        from ministack.core.responses import get_account_id
        from ministack.services import iam as iam_svc
        from ministack.services import sts as sts_svc
        trust = {"Statement": [{"Effect": "Allow", "Principal": {"AWS": get_account_id()},
                                "Action": ["sts:AssumeRole", "sts:TagSession"]}]}
        iam_svc._roles["ck-role"] = {"RoleName": "ck-role", "RoleId": "AROACKROLE", "AttachedPolicies": [],
                                     "Arn": f"arn:aws:iam::{get_account_id()}:role/ck-role", "InlinePolicies": {},
                                     "AssumeRolePolicyDocument": json.dumps(trust),
                                     "Tags": [{"Key": "team", "Value": "red"}, {"Key": "dept", "Value": "ops"}]}
        body = (f"Action=AssumeRole&RoleArn=arn:aws:iam::{get_account_id()}:role/ck-role&RoleSessionName=s1"
                "&Tags.member.1.Key=team&Tags.member.1.Value=green").encode()
        status, _, out = asyncio.run(sts_svc.handle_request("POST", "/", {}, body, {}))
        assert status == 200
        key = re.search(r"<AccessKeyId>([^<]+)", out.decode()).group(1)
        yield key, lambda doc: iam_svc._roles["ck-role"]["InlinePolicies"].__setitem__("p", doc)
        sts_svc._sessions.pop(key, None)
        iam_svc._roles.pop("ck-role", None)

    @pytest.mark.parametrize("condition,allowed", [
        ({"StringEquals": {"aws:username": "ck-user"}}, True),
        ({"StringEquals": {"aws:userid": "AIDACKUSER"}}, True),
        ({"StringEquals": {"aws:PrincipalTag/team": "blue"}}, True),
        ({"StringEquals": {"aws:PrincipalTag/Team": "red"}}, False),
        ({"Null": {"aws:MultiFactorAuthPresent": "false"}}, False),
        ({"Null": {"aws:TokenIssueTime": "false"}}, False),
    ])
    def test_iam_user_keys(self, user, condition, allowed):
        assert _allows(*user, condition) is allowed

    def test_assumed_role_id_starts_with_the_role_id(self, role_session):
        from ministack.services import sts as sts_svc
        assert sts_svc._sessions[role_session[0]]["UserId"] == "AROACKROLE:s1"

    @pytest.mark.parametrize("condition,allowed", [
        ({"Null": {"aws:username": "true"}}, True),
        ({"StringEquals": {"aws:userid": "AROACKROLE:s1"}}, True),
        ({"StringEquals": {"aws:PrincipalTag/team": "green"}}, True),
        ({"StringEquals": {"aws:PrincipalTag/dept": "ops"}}, True),
        ({"Bool": {"aws:MultiFactorAuthPresent": "false"}}, True),
        ({"DateGreaterThan": {"aws:TokenIssueTime": "2020-01-01T00:00:00Z"}}, True),
    ])
    def test_assumed_role_session_keys(self, role_session, condition, allowed):
        assert _allows(*role_session, condition) is allowed

    def test_assume_role_json_protocol_keeps_session_tags(self, role_session):
        from ministack.core.responses import get_account_id
        from ministack.services import sts as sts_svc
        body = json.dumps({"RoleArn": f"arn:aws:iam::{get_account_id()}:role/ck-role", "RoleSessionName": "s2",
                           "Tags": [{"Key": "team", "Value": "white"}]}).encode()
        headers = {"content-type": "application/x-amz-json-1.1",
                   "x-amz-target": "AWSSecurityTokenServiceV20110615.AssumeRole"}
        status, _, out = asyncio.run(sts_svc.handle_request("POST", "/", headers, body, {}))
        key = json.loads(out)["Credentials"]["AccessKeyId"]
        try:
            assert _allows(key, role_session[1], {"StringEquals": {"aws:PrincipalTag/team": "white"}})
        finally:
            sts_svc._sessions.pop(key, None)

    @pytest.mark.parametrize("condition,allowed", [
        ({"IpAddress": {"aws:SourceIp": "10.1.2.0/24"}}, True),
        ({"IpAddress": {"aws:SourceIp": "127.0.0.1/32"}}, False),
        ({"Bool": {"aws:SecureTransport": "true"}}, True),
    ])
    def test_request_origin_keys(self, user, condition, allowed):
        from ministack.core.iam_evaluator import pin_request_origin

        def decide():
            pin_request_origin("10.1.2.3", True)
            return _allows(*user, condition)
        assert contextvars.copy_context().run(decide) is allowed

    def test_policy_simulation_ignores_the_request_origin(self):
        from ministack.core.iam_evaluator import pin_request_origin
        from ministack.services import iam as iam_svc
        doc = json.dumps({"Statement": [
            {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"},
            {"Effect": "Deny", "Action": "s3:GetObject", "Resource": "*",
             "Condition": {"Bool": {"aws:SecureTransport": "false"}}}]})
        body = urlencode({"Action": "SimulateCustomPolicy", "PolicyInputList.member.1": doc,
                          "ActionNames.member.1": "s3:GetObject"}).encode()

        def simulate():
            pin_request_origin("10.1.2.3", True)
            return asyncio.run(iam_svc.handle_request("POST", "/", {}, body, {}))[2]
        assert b"<EvalDecision>explicitDeny</EvalDecision>" in contextvars.copy_context().run(simulate)

    @pytest.mark.parametrize("auth,scheme,headers,origin", [
        (True, "https", [], ("10.9.8.7", True)),
        (True, "http", [], ("10.9.8.7", False)),
        (True, "http", [(b"x-forwarded-proto", b"https")], ("10.9.8.7", True)),
        (False, "https", [], ("127.0.0.1", False)),
    ])
    def test_http_request_pins_the_client_address_and_scheme(self, monkeypatch, auth, scheme, headers, origin):
        from ministack import app as app_mod
        seen = []

        async def capture(*args):
            ctx = EvalContext("", "", "", "", "*", "us-east-1")
            seen.append((ctx.source_ip, ctx.secure_transport))
            return 200, {}, b"ok"

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            pass

        monkeypatch.setattr(app_mod, "_handle_pre_body_request", capture)
        monkeypatch.setattr(app_mod, "AUTH", auth)
        asyncio.run(app_mod.app({"type": "http", "method": "GET", "path": "/", "scheme": scheme,
                                 "client": ("10.9.8.7", 50000), "headers": [(b"host", b"sts.localhost"), *headers],
                                 "query_string": b""}, receive, send))
        assert seen == [origin]


class TestRequestTagContext:
    """aws:RequestTag/* and aws:TagKeys of create, tag and untag calls."""

    @pytest.mark.parametrize("service,action,body,params,context", [
        ("sqs", "sqs:CreateQueue", b'{"QueueName":"q","tags":{"env":"dev"}}', {},
         {"aws:requesttag/env": "dev", "aws:TagKeys": ["env"]}),
        ("sqs", "sqs:CreateQueue", b"", {"Tag.1.Key": ["env"], "Tag.1.Value": ["dev"]},
         {"aws:requesttag/env": "dev", "aws:TagKeys": ["env"]}),
        ("dynamodb", "dynamodb:TagResource", b'{"Tags":[{"Key":"env","Value":"dev"},{"Key":"t","Value":"1"}]}', {},
         {"aws:requesttag/env": "dev", "aws:requesttag/t": "1", "aws:TagKeys": ["env", "t"]}),
        ("kms", "kms:CreateKey", b'{"Tags":[{"TagKey":"env","TagValue":"dev"}]}', {},
         {"aws:requesttag/env": "dev", "aws:TagKeys": ["env"]}),
        ("iam", "iam:CreateUser", b"", {"Tags.member.1.Key": ["env"], "Tags.member.1.Value": ["dev"]},
         {"aws:requesttag/env": "dev", "aws:TagKeys": ["env"]}),
        ("sqs", "sqs:CreateQueue", b'{"QueueName":"q","tags":{"Env":"a","env":"b"}}', {},
         {"aws:requesttag/env": ["a", "b"], "aws:TagKeys": ["Env", "env"]}),
        ("sns", "sns:UntagResource", b"", {"TagKeys.member.1": ["env"]}, {"aws:TagKeys": ["env"]}),
        ("ssm", "ssm:RemoveTagsFromResource", b'{"TagKeys":["env"]}', {}, {"aws:TagKeys": ["env"]}),
        ("lambda", "lambda:UntagResource", b"", {"tagKeys": ["env", "temp"]}, {"aws:TagKeys": ["env", "temp"]}),
        ("sqs", "sqs:CreateQueue", b'{"QueueName":"q"}', {}, {}),
        ("lambda", "lambda:InvokeFunction", b'{"Tags":{"env":"dev"}}', {}, {}),
        ("sqs", "sqs:SendMessage", b'{"MessageBody":"x","tags":{"env":"dev"}}', {}, {}),
    ])
    def test_wire_shapes(self, service, action, body, params, context):
        from ministack.core.iam_actions import request_tag_context
        assert request_tag_context(service, action, body, params) == context

    @pytest.mark.parametrize("tags,allowed", [
        ({"env": "dev"}, True),
        ({"env": "prod"}, False),
        ({"env": "dev", "blocked": "1"}, False),
        ({}, False),
    ])
    def test_create_is_decided_by_the_request_tags(self, tags, allowed):
        from ministack.core.iam_actions import request_tag_context
        body = json.dumps({"QueueName": "q", "tags": tags}).encode()
        context = request_tag_context("sqs", "sqs:CreateQueue", body, {})
        with _iam_user("rt-user") as (key, put_policy):
            put_policy(json.dumps({"Statement": [
                {"Effect": "Allow", "Action": "sqs:CreateQueue", "Resource": "*",
                 "Condition": {"StringEquals": {"aws:RequestTag/env": "dev"}}},
                {"Effect": "Deny", "Action": "sqs:CreateQueue", "Resource": "*",
                 "Condition": {"ForAnyValue:StringEquals": {"aws:TagKeys": "blocked"}}},
            ]}))
            denied = enforce(key, "sqs:CreateQueue", "sqs", "us-east-1", service_context=context)
        assert (denied is None) is allowed

    @pytest.mark.parametrize("tags,allowed", [({}, True), ({"env": "dev"}, False)])
    def test_tag_keys_is_absent_without_tags(self, tags, allowed):
        from ministack.core.iam_actions import request_tag_context
        body = json.dumps({"QueueName": "q", "tags": tags}).encode()
        context = request_tag_context("sqs", "sqs:CreateQueue", body, {})
        with _iam_user("rt-null") as (key, put_policy):
            put_policy(json.dumps({"Statement": [{"Effect": "Allow", "Action": "sqs:CreateQueue", "Resource": "*",
                                                  "Condition": {"Null": {"aws:TagKeys": "true"}}}]}))
            assert (enforce(key, "sqs:CreateQueue", "sqs", "us-east-1", service_context=context) is None) is allowed

    @pytest.mark.parametrize("method,grant,allowed", [
        ("POST", "function:f", True), ("GET", "function:f*", True), ("DELETE", "function:g", False)])
    def test_lambda_tag_calls_match_a_function_scoped_grant(self, method, grant, allowed):
        from ministack.core.iam_actions import extract_iam_action, extract_resource_arn
        path = "/2017-03-31/tags/arn:aws:lambda:us-east-1:000000000000:function:f"
        action = extract_iam_action("lambda", method, path, {}, b"", {})
        arn = extract_resource_arn("lambda", method, path, {}, b"", {}, "us-east-1", "000000000000")
        with _iam_user("rt-fn") as (key, put_policy):
            put_policy(json.dumps({"Statement": [{
                "Effect": "Allow", "Action": ["lambda:TagResource", "lambda:UntagResource", "lambda:ListTags"],
                "Resource": f"arn:aws:lambda:us-east-1:000000000000:{grant}"}]}))
            denied = enforce(key, action, "lambda", "us-east-1", resource_arn=arn)
        assert (denied is None) is allowed


class TestResourceTagCondition:
    """aws:ResourceTag/* from the tags of the resource a request names."""

    def test_resource_tags_come_from_each_store(self):
        from ministack.core.responses import get_account_id, get_region
        from ministack.services import iam as iam_svc
        from ministack.services import secretsmanager as sm
        from ministack.services import sns, ssm
        from ministack.services.tagging import resource_tags

        acct, region = get_account_id(), get_region()
        topic = f"arn:aws:sns:{region}:{acct}:rt-topic"
        param = f"arn:aws:ssm:{region}:{acct}:parameter/rt-param"
        sns._topics[topic] = {"tags": {"env": "dev"}}
        ssm._tags[param] = {"env": "dev"}
        secret = sm.create_secret_in_process("rt-secret", "v")
        sm._secrets["rt-secret"]["Tags"] = [{"Key": "env", "Value": "dev"}]
        iam_svc._roles["rt-role"] = {"RoleName": "rt-role", "Arn": f"arn:aws:iam::{acct}:role/rt-role",
                                     "Tags": [{"Key": "env", "Value": "dev"}]}
        try:
            for arn in (topic, param, secret, f"arn:aws:iam::{acct}:role/rt-role"):
                assert resource_tags(arn) == {"env": "dev"}, arn
            assert resource_tags(f"arn:aws:sns:{region}:{acct}:missing") is None
            assert resource_tags("arn:aws:s3:::bucket") is None
            assert resource_tags("*") is None
        finally:
            sns._topics.pop(topic, None)
            ssm._tags.pop(param, None)
            sm._secrets.pop("rt-secret", None)
            iam_svc._roles.pop("rt-role", None)

    @pytest.mark.parametrize("condition,resource,allowed", [
        ({"StringEquals": {"aws:ResourceTag/env": "dev"}}, "rt-dev", True),
        ({"StringEquals": {"aws:ResourceTag/ENV": "dev"}}, "rt-dev", True),
        ({"StringEquals": {"aws:ResourceTag/env": "dev"}}, "rt-prod", False),
        ({"StringEquals": {"aws:ResourceTag/env": "dev"}}, "rt-missing", False),
        ({"Null": {"aws:ResourceTag/env": "true"}}, "rt-missing", True),
    ])
    def test_read_is_decided_by_the_resource_tags(self, condition, resource, allowed):
        from ministack.core.responses import get_account_id
        with _iam_user("rt-reader") as (key, put_policy), _iam_user("rt-dev", [{"Key": "env", "Value": "dev"}]), \
                _iam_user("rt-prod", [{"Key": "env", "Value": "prod"}]):
            put_policy(json.dumps({"Statement": [
                {"Effect": "Allow", "Action": "iam:GetUser", "Resource": "*", "Condition": condition}]}))
            denied = enforce(key, "iam:GetUser", "iam", "us-east-1",
                             resource_arn=f"arn:aws:iam::{get_account_id()}:user/{resource}")
        assert (denied is None) is allowed

    def test_tags_a_service_supplies_keep_precedence(self):
        from ministack.core.responses import get_account_id
        with _iam_user("rt-ctx") as (key, put_policy), _iam_user("rt-prod", [{"Key": "env", "Value": "prod"}]):
            put_policy(json.dumps({"Statement": [{"Effect": "Allow", "Action": "iam:GetUser", "Resource": "*",
                                                  "Condition": {"StringEquals": {"aws:ResourceTag/env": "dev"}}}]}))
            denied = enforce(key, "iam:GetUser", "iam", "us-east-1", service_context={"aws:ResourceTag/env": "dev"},
                             resource_arn=f"arn:aws:iam::{get_account_id()}:user/rt-prod")
        assert denied is None


class TestSeededAwsManagedPolicies:
    """The AWS-managed policies CDK, SAM and Serverless attach by their real ARNs
    resolve to a document: the service-role/* path is the only one AWS has for the
    Lambda execution roles, and a CDK deploy role reads stacks through
    AWSCloudFormationReadOnlyAccess."""

    @pytest.mark.parametrize("name,action", [
        ("service-role/AWSLambdaBasicExecutionRole", "logs:PutLogEvents"),
        ("service-role/AWSLambdaVPCAccessExecutionRole", "ec2:CreateNetworkInterface"),
        ("service-role/AmazonAPIGatewayPushToCloudWatchLogs", "logs:CreateLogGroup"),
        ("service-role/AWSIoTThingsRegistration", "iot:RegisterThing"),
        ("AWSCloudFormationReadOnlyAccess", "cloudformation:DescribeStacks"),
        ("AWSCloudFormationReadOnlyAccess", "cloudformation:BatchDescribeTypeConfigurations"),
        ("CloudWatchLambdaInsightsExecutionRolePolicy", "logs:CreateLogGroup"),
        ("service-role/AWSLambdaSQSQueueExecutionRole", "sqs:ReceiveMessage"),
        ("service-role/AWSLambdaKinesisExecutionRole", "kinesis:GetRecords"),
        ("service-role/AWSLambdaDynamoDBExecutionRole", "dynamodb:GetShardIterator"),
    ])
    def test_real_arn_resolves_and_grants(self, name, action):
        from ministack.core.iam_evaluator import _resolve_managed_policy_document
        doc = _resolve_managed_policy_document(f"arn:aws:iam::aws:policy/{name}", "000000000000")
        assert doc, name
        stmts = parse_policy_document(doc)
        assert evaluate(_ctx(action=action), [stmts]).decision == "Allow", action

    def test_lambda_insights_log_writes_are_scoped_to_its_log_group(self):
        from ministack.core.iam_evaluator import _resolve_managed_policy_document
        stmts = parse_policy_document(_resolve_managed_policy_document(
            "arn:aws:iam::aws:policy/CloudWatchLambdaInsightsExecutionRolePolicy", "000000000000"))
        insights = "arn:aws:logs:us-east-1:000000000000:log-group:/aws/lambda-insights:log-stream:x"
        assert evaluate(_ctx(action="logs:PutLogEvents", resource=insights), [stmts]).decision == "Allow"
        other = "arn:aws:logs:us-east-1:000000000000:log-group:/aws/lambda/f:log-stream:x"
        assert evaluate(_ctx(action="logs:PutLogEvents", resource=other), [stmts]).decision == "ImplicitDeny"

    @pytest.mark.parametrize("name", [
        "service-role/AWSLambdaBasicExecutionRole",
        "service-role/AmazonAPIGatewayPushToCloudWatchLogs",
    ])
    def test_get_policy_reports_path_and_bare_name(self, name):
        from ministack.services.iam import _get_policy
        status, _, body = _get_policy({"PolicyArn": [f"arn:aws:iam::aws:policy/{name}"]})
        body = body.decode()
        assert status == 200
        path, _, policy_name = name.rpartition("/")
        assert f"<PolicyName>{policy_name}</PolicyName>" in body
        assert f"<Path>/{path}/</Path>" in body
        assert f"<Arn>arn:aws:iam::aws:policy/{name}</Arn>" in body

    def test_list_policies_by_path_prefix_finds_service_role_policies(self):
        from ministack.services.iam import _list_policies
        body = _list_policies({"Scope": ["AWS"], "PathPrefix": ["/service-role/"]})[2].decode()
        assert "<Arn>arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole</Arn>" in body
        assert "<Arn>arn:aws:iam::aws:policy/AdministratorAccess</Arn>" not in body

    def test_bare_arn_of_a_service_role_policy_answers_no_such_entity(self):
        # AWS publishes the Lambda execution-role policies only under
        # service-role/; the path-less spelling does not exist there and does
        # not exist here.
        from ministack.services.iam import _get_policy, _list_policies
        bare = "arn:aws:iam::aws:policy/AWSLambdaBasicExecutionRole"
        real = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
        status, _, body = _get_policy({"PolicyArn": [bare]})
        assert status == 404 and "NoSuchEntity" in body.decode()
        listing = _list_policies({"Scope": ["AWS"]})[2].decode()
        assert f"<Arn>{real}</Arn>" in listing
        assert f"<Arn>{bare}</Arn>" not in listing

    def test_cloudformation_readonly_does_not_grant_writes(self):
        from ministack.core.iam_evaluator import _resolve_managed_policy_document
        doc = _resolve_managed_policy_document(
            "arn:aws:iam::aws:policy/AWSCloudFormationReadOnlyAccess", "000000000000")
        stmts = parse_policy_document(doc)
        assert evaluate(_ctx(action="cloudformation:CreateStack"), [stmts]).decision == "ImplicitDeny"

    def test_autocreate_applies_to_enforcement(self, monkeypatch):
        from ministack.core.iam_evaluator import _resolve_managed_policy_document
        from ministack.services import iam as iam_svc
        arn = "arn:aws:iam::aws:policy/SomethingNobodySeeded"
        monkeypatch.delenv("MINISTACK_AUTOCREATE_AWS_MANAGED", raising=False)
        assert _resolve_managed_policy_document(arn, "000000000000") is None
        monkeypatch.setenv("MINISTACK_AUTOCREATE_AWS_MANAGED", "1")
        monkeypatch.setattr(iam_svc, "_aws_managed_policies", dict(iam_svc._aws_managed_policies))
        doc = _resolve_managed_policy_document(arn, "000000000000")
        assert doc and evaluate(_ctx(action="sqs:SendMessage"),
                                [parse_policy_document(doc)]).decision == "Allow"




# ---------------------------------------------------------------------------
# Action extraction
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Resource ARN construction
# ---------------------------------------------------------------------------

_FORM = {"content-type": "application/x-www-form-urlencoded"}


def _sigv4_headers(credential_scope: str, host: str) -> dict:
    """Headers shaped like a signed request, which is how the router decides
    the service: the credential scope wins over the host and the path."""
    return {
        "authorization": (
            f"AWS4-HMAC-SHA256 Credential=test/20260101/eu-central-1/{credential_scope}"
            "/aws4_request, SignedHeaders=host, Signature=deadbeef"
        ),
        "host": host,
    }


def _capture_enforced_arn(monkeypatch) -> dict:
    """Record the action and resource ARN the data-plane check was handed.

    Always allows, so the call carries on into dispatch.
    """
    import ministack.app as app

    seen: dict[str, str] = {}

    def _capture(service, iam_action, headers, query_params, request_id, resource_arn="*",
                 service_context=None, auth_error_response=None):
        seen["action"] = iam_action
        seen["arn"] = resource_arn
        seen["context"] = service_context
        return None

    monkeypatch.setattr(app, "_enforce_data_plane", _capture)
    monkeypatch.setattr(app, "extract_region", lambda h, q=None: "eu-central-1")
    return seen


class _NoApiModule:
    """Stands in for the apigateway modules so dispatch stops at the 404."""

    @staticmethod
    def find_api_scope(api_id):
        return None

    @staticmethod
    async def handle_connections_api(method, api_id, stage, connection_id, body, headers):
        return 200, {}, b""


_NOTIFY_ARN = "arn:aws:lambda:eu-central-1:000000000000:function:notify"


def _grant_invoke_url_statements():
    """The two statements ``grantInvokeUrl`` writes, as aws-cdk-lib 2.260.0 does."""
    return parse_policy_document({"Statement": [
        {
            "Effect": "Allow", "Action": "lambda:InvokeFunctionUrl",
            "Resource": _NOTIFY_ARN,
            "Condition": {"StringEquals": {"lambda:FunctionUrlAuthType": "AWS_IAM"}},
        },
        {
            "Effect": "Allow", "Action": "lambda:InvokeFunction",
            "Resource": _NOTIFY_ARN,
            "Condition": {"Bool": {"lambda:InvokedViaFunctionUrl": "true"}},
        },
    ]})


def _function_url_eval_context(service_context: dict) -> EvalContext:
    """A Function URL invoke as the evaluator sees it, keys lowered the way
    ``enforce`` lowers what a handler passes."""
    return EvalContext(
        principal_arn="arn:aws:sts::000000000000:assumed-role/caller/session",
        principal_type="AssumedRole", principal_account="000000000000",
        action="lambda:InvokeFunctionUrl", resource_arn=_NOTIFY_ARN,
        region="eu-central-1",
        service_context={k.lower(): v for k, v in service_context.items()},
    )


def _function_url_module(resolved):
    """Stands in for lambda_svc with one Function URL resolution. ``handled``
    records what the handler was passed."""

    class _Module:
        handled: dict = {}

        @staticmethod
        def resolve_function_url(url_id):
            return resolved

        @staticmethod
        async def handle_function_url_request(*args, **kwargs):
            _Module.handled.update(kwargs)
            return 200, {}, b""

    return _Module


_FUNCTION_URL_HOST = "3f2a1c4d-0b6e-4a58-9c71-8d5e2f0a1b93.lambda-url.eu-central-1.on.aws"


class TestResourceArn:
    def test_s3_bucket(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("s3", "PUT", "/mybucket", {}, b"", {}, "us-east-1", "123") == "arn:aws:s3:::mybucket"

    def test_s3_object(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("s3", "GET", "/mybucket/path/to/key", {}, b"", {}, "us-east-1", "123") == "arn:aws:s3:::mybucket/path/to/key"

    def test_s3_list_buckets(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("s3", "GET", "/", {}, b"", {}, "us-east-1", "123") == "*"

    def test_dynamodb_table(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"TableName": "users"}).encode()
        assert extract_resource_arn("dynamodb", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:dynamodb:us-east-1:123:table/users"

    def test_dynamodb_no_table(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("dynamodb", "POST", "/", {}, b"{}", {}, "us-east-1", "123") == "*"

    def test_dynamodb_index(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps(
            {"TableName": "users", "IndexName": "email-index"}
        ).encode()
        assert extract_resource_arn(
            "dynamodb", "POST", "/", {}, body, {}, "us-east-1", "123"
        ) == "arn:aws:dynamodb:us-east-1:123:table/users/index/email-index"

    def test_dynamodb_batch_request_uses_first_table(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"RequestItems": {"events": [], "snapshots": []}}).encode()
        assert extract_resource_arn(
            "dynamodb", "POST", "/", {}, body, {}, "us-east-1", "123"
        ) == "arn:aws:dynamodb:us-east-1:123:table/events"

    def test_dynamodb_batch_request_returns_every_table(self):
        from ministack.core.iam_actions import dynamodb_resource_arns
        body = json.dumps({"RequestItems": {"events": [], "snapshots": []}}).encode()
        assert dynamodb_resource_arns(body, "us-east-1", "123") == [
            "arn:aws:dynamodb:us-east-1:123:table/events",
            "arn:aws:dynamodb:us-east-1:123:table/snapshots",
        ]

    def test_dynamodb_table_name_may_be_the_table_arn(self):
        from ministack.core.iam_actions import dynamodb_resource_arns, dynamodb_transaction_checks
        arn = "arn:aws:dynamodb:us-east-1:123:table/users"
        assert dynamodb_resource_arns(json.dumps({"TableName": arn, "IndexName": "by-email"}).encode(),
                                      "us-east-1", "123") == [f"{arn}/index/by-email"]
        assert dynamodb_resource_arns(json.dumps({"RequestItems": {arn: []}}).encode(),
                                      "us-east-1", "123") == [arn]
        body = json.dumps({"TransactItems": [{"Put": {"TableName": arn, "Item": {}}}]}).encode()
        assert dynamodb_transaction_checks("dynamodb:TransactWriteItems", body, "us-east-1", "123") == [
            ("dynamodb:PutItem", arn)]

    def test_eventbridge_put_events_returns_every_bus(self):
        from ministack.core.iam_actions import eventbridge_resource_arns
        body = json.dumps({"Entries": [
            {"EventBusName": "orders"},
            {"Source": "example"},
            {"EventBusName": "orders"},
            {"EventBusName": "arn:aws:events:us-east-1:123:event-bus/audit"},
        ]}).encode()
        assert eventbridge_resource_arns(body, "us-east-1", "123") == [
            "arn:aws:events:us-east-1:123:event-bus/orders",
            "arn:aws:events:us-east-1:123:event-bus/default",
            "arn:aws:events:us-east-1:123:event-bus/audit",
        ]

    def test_eventbridge_put_events_survives_a_malformed_entry(self):
        from ministack.core.iam_actions import eventbridge_resource_arns
        body = json.dumps({"Entries": ["junk", {"EventBusName": 7}]}).encode()
        assert eventbridge_resource_arns(body, "us-east-1", "123") == [
            "arn:aws:events:us-east-1:123:event-bus/default",
        ]

    def test_dynamodb_attributes_are_top_level_only(self):
        """AWS resolves a ProjectionExpression of "Name, Address.City" to
        ["Name", "Address"], substituting a placeholder per path segment."""
        from ministack.core.iam_actions import dynamodb_service_context
        body = json.dumps({
            "TableName": "users",
            "ProjectionExpression": "Name, Address.City, #a.#c, Items[0]",
            "ExpressionAttributeNames": {"#a": "Addr", "#c": "City"},
        }).encode()
        context = dynamodb_service_context(body)
        assert context["dynamodb:Attributes"] == ["Name", "Address", "Addr", "Items"]
        assert context["dynamodb:Select"] == "SPECIFIC_ATTRIBUTES"

    def test_dynamodb_select_is_always_resolved(self):
        """Select always has a value on AWS, so a policy conditioning on it
        with StringEqualsIfExists must not pass a projection-less request."""
        from ministack.core.iam_actions import dynamodb_service_context
        assert dynamodb_service_context(
            json.dumps({"TableName": "users"}).encode()
        ) == {"dynamodb:Select": "ALL_ATTRIBUTES"}
        assert dynamodb_service_context(
            json.dumps({"TableName": "users", "Select": "COUNT"}).encode()
        )["dynamodb:Select"] == "COUNT"
        assert dynamodb_service_context(
            json.dumps({"TableName": "users", "AttributesToGet": ["a", "b"]}).encode()
        ) == {"dynamodb:Attributes": ["a", "b"], "dynamodb:Select": "SPECIFIC_ATTRIBUTES"}
        assert dynamodb_service_context(b"not json") == {}

    def test_eventbridge_put_events_defaults_to_default_bus(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"Entries": [{"Source": "example"}]}).encode()
        assert extract_resource_arn(
            "events", "POST", "/", {}, body, {}, "us-east-1", "123"
        ) == "arn:aws:events:us-east-1:123:event-bus/default"

    def test_lambda_function(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("lambda", "GET", "/2015-03-31/functions/my-func", {}, b"", {}, "us-east-1", "123") == "arn:aws:lambda:us-east-1:123:function:my-func"

    def test_lambda_invoke(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("lambda", "POST", "/2015-03-31/functions/my-func/invocations", {}, b"", {}, "us-east-1", "123") == "arn:aws:lambda:us-east-1:123:function:my-func"

    def test_lambda_function_named_by_arn(self):
        from ministack.core.iam_actions import extract_resource_arn
        path = "/2015-03-31/functions/arn%3Aaws%3Alambda%3Aus-east-1%3A123%3Afunction%3Amy-func/invocations"
        assert extract_resource_arn(
            "lambda", "POST", path, {}, b"", {}, "us-east-1", "123"
        ) == "arn:aws:lambda:us-east-1:123:function:my-func"
    @pytest.mark.parametrize("arn", [
        "arn:aws:lambda:us-east-1:123:function:f", "arn%3Aaws%3Alambda%3Aus-east-1%3A123%3Afunction%3Af"])
    @pytest.mark.parametrize("method", ["GET", "POST", "DELETE"])
    def test_lambda_tags(self, method, arn):
        from ministack.core.iam_actions import extract_resource_arn
        resolved = extract_resource_arn("lambda", method, f"/2017-03-31/tags/{arn}", {}, b"", {}, "us-east-1", "123")
        assert resolved == "arn:aws:lambda:us-east-1:123:function:f"

    def test_sqs_queue_url(self):
        from ministack.core.iam_actions import extract_resource_arn
        url = "http://localhost:4566/000000000000/my-queue"
        assert extract_resource_arn(
            "sqs", "POST", "/", {}, b"", {"QueueUrl": [url]}, "us-east-1", "123"
        ) == "arn:aws:sqs:us-east-1:000000000000:my-queue"
        assert extract_resource_arn(
            "sqs", "POST", "/", {}, b"", {"QueueUrl": ["my-queue"]}, "us-east-1", "123"
        ) == "arn:aws:sqs:us-east-1:123:my-queue"

    def test_sqs_create_queue(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("sqs", "POST", "/", {}, b"", {"QueueName": ["my-queue"]}, "us-east-1", "123") == "arn:aws:sqs:us-east-1:123:my-queue"

    def test_sns_topic_arn(self):
        from ministack.core.iam_actions import extract_resource_arn
        topic = "arn:aws:sns:us-east-1:123:my-topic"
        assert extract_resource_arn("sns", "POST", "/", {}, b"", {"TopicArn": [topic]}, "us-east-1", "123") == topic

    def test_kms_key_id(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"KeyId": "abc-123"}).encode()
        assert extract_resource_arn("kms", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:kms:us-east-1:123:key/abc-123"

    def test_kms_alias(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"KeyId": "alias/my-key"}).encode()
        assert extract_resource_arn("kms", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:kms:us-east-1:123:alias/my-key"

    def test_secretsmanager(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"SecretId": "my-secret"}).encode()
        assert extract_resource_arn("secretsmanager", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:secretsmanager:us-east-1:123:secret:my-secret"

    @pytest.mark.parametrize("form", ["name", "arn", "partial_arn"])
    def test_secretsmanager_resolves_the_stored_arn(self, form):
        # AWS evaluates against the stored ARN with its six random characters.
        # A request may name the secret by name, full ARN or ARN without them.
        from ministack.core.iam_actions import extract_resource_arn
        from ministack.core.responses import get_account_id
        from ministack.services import secretsmanager as sm

        stored = sm.create_secret_in_process("suffix-secret", "v")
        try:
            secret_id = {
                "name": "suffix-secret",
                "arn": stored,
                "partial_arn": stored.rsplit("-", 1)[0],
            }[form]
            body = json.dumps({"SecretId": secret_id}).encode()
            assert extract_resource_arn(
                "secretsmanager", "POST", "/", {}, body, {}, "us-east-1", get_account_id()
            ) == stored
        finally:
            sm._secrets.pop("suffix-secret", None)

    def test_secretsmanager_resolves_the_stored_arn_when_routed_through_detect_service(self):
        """A signed GetSecretValue reaches this branch through detect_service."""
        from ministack.core.iam_actions import extract_resource_arn
        from ministack.core.responses import get_account_id
        from ministack.core.router import detect_service
        from ministack.services import secretsmanager as sm

        stored = sm.create_secret_in_process("router-secret", "v")
        try:
            headers = {
                **_sigv4_headers("secretsmanager", "secretsmanager.us-east-1.amazonaws.com"),
                "x-amz-target": "secretsmanager.GetSecretValue",
                "content-type": "application/x-amz-json-1.1",
            }
            body = json.dumps({"SecretId": "router-secret"}).encode()
            service = detect_service("POST", "/", headers, {})
            assert service == "secretsmanager"
            assert extract_resource_arn(
                service, "POST", "/", headers, body, {}, "us-east-1", get_account_id()
            ) == stored
        finally:
            sm._secrets.pop("router-secret", None)

    @pytest.mark.parametrize("owner_region,owner_account", [
        ("eu-central-1", None),
        ("us-east-1", "111122223333"),
    ])
    def test_secretsmanager_secret_in_another_scope_is_not_resolved(self, owner_region, owner_account):
        """The lookup is scoped to the request's account and region, like the
        handlers'. A name held only in another region or account keeps the
        name-derived ARN, and that secret's full ARN passes through unchanged."""
        from ministack.core.iam_actions import extract_resource_arn
        from ministack.core.responses import (
            _request_account_id,
            get_account_id,
            get_region,
            set_request_region,
        )
        from ministack.services import secretsmanager as sm

        account_id = get_account_id()
        owner = owner_account or account_id
        prev_region = get_region()
        token = _request_account_id.set(owner)
        set_request_region(owner_region)
        try:
            stored = sm.create_secret_in_process("scoped-secret", "v")
        finally:
            _request_account_id.reset(token)
            set_request_region("us-east-1")
        try:
            name_arn = f"arn:aws:secretsmanager:us-east-1:{account_id}:secret:scoped-secret"
            for secret_id, expected in (("scoped-secret", name_arn), (stored, stored)):
                body = json.dumps({"SecretId": secret_id}).encode()
                assert extract_resource_arn(
                    "secretsmanager", "POST", "/", {}, body, {}, "us-east-1", account_id
                ) == expected
        finally:
            set_request_region(prev_region)
            sm._secrets.pop_scoped(owner, owner_region, "scoped-secret", None)

    def test_createsecret_on_an_existing_name_resolves_the_stored_arn(self):
        """CreateSecret on a name the store already holds resolves the stored
        ARN; a new name keeps the name-derived ARN. Which ARN AWS evaluates
        here is unmeasured."""
        from ministack.core.iam_actions import extract_resource_arn
        from ministack.core.responses import get_account_id
        from ministack.services import secretsmanager as sm

        account_id = get_account_id()
        stored = sm.create_secret_in_process("existing-createsecret-name", "v")
        try:
            existing_body = json.dumps({"Name": "existing-createsecret-name"}).encode()
            assert extract_resource_arn(
                "secretsmanager", "POST", "/", {}, existing_body, {}, "us-east-1", account_id
            ) == stored

            new_body = json.dumps({"Name": "brand-new-createsecret-name"}).encode()
            assert extract_resource_arn(
                "secretsmanager", "POST", "/", {}, new_body, {}, "us-east-1", account_id
            ) == f"arn:aws:secretsmanager:us-east-1:{account_id}:secret:brand-new-createsecret-name"
        finally:
            sm._secrets.pop("existing-createsecret-name", None)

    def test_iam_role(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("iam", "POST", "/", {}, b"", {"RoleName": ["my-role"]}, "us-east-1", "123") == "arn:aws:iam::123:role/my-role"

    def test_iam_user(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("iam", "POST", "/", {}, b"", {"UserName": ["alice"]}, "us-east-1", "123") == "arn:aws:iam::123:user/alice"

    def test_unknown_service(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("unknown", "GET", "/", {}, b"", {}, "us-east-1", "123") == "*"

    def test_events_rule(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"Name": "my-rule"}).encode()
        arn = extract_resource_arn("events", "POST", "/", {}, body, {}, "us-east-1", "123")
        assert "events" in arn and "my-rule" in arn

    def test_kinesis_stream(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"StreamName": "my-stream"}).encode()
        assert extract_resource_arn("kinesis", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:kinesis:us-east-1:123:stream/my-stream"

    def test_logs_group(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"logGroupName": "/app/logs"}).encode()
        assert extract_resource_arn("logs", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:logs:us-east-1:123:log-group:/app/logs"

    def test_states_machine(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"stateMachineArn": "arn:aws:states:us-east-1:123:stateMachine:my-sm"}).encode()
        assert extract_resource_arn("states", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:states:us-east-1:123:stateMachine:my-sm"

    def test_ecs_cluster(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"cluster": "my-cluster"}).encode()
        assert extract_resource_arn("ecs", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:ecs:us-east-1:123:cluster/my-cluster"

    def test_eks_cluster_from_path(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("eks", "GET", "/clusters/prod", {}, b"", {}, "us-east-1", "123") == "arn:aws:eks:us-east-1:123:cluster/prod"

    def test_rds_instance(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("rds", "POST", "/", {}, b"", {"DBInstanceIdentifier": ["mydb"]}, "us-east-1", "123") == "arn:aws:rds:us-east-1:123:db:mydb"

    def test_cloudformation_stack(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("cloudformation", "POST", "/", {}, b"", {"StackName": ["my-stack"]}, "us-east-1", "123") == "arn:aws:cloudformation:us-east-1:123:stack/my-stack/*"

    def test_ssm_parameter(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("ssm", "POST", "/", {}, b"", {"Name": ["/app/config"]}, "us-east-1", "123") == "arn:aws:ssm:us-east-1:123:parameter/app/config"

    def test_signer_profile_and_job(self):
        from ministack.core.iam_actions import extract_resource_arn
        job_body = b'{"profileName": "fleet", "source": {"s3": {}}}'
        assert extract_resource_arn("signer", "POST", "/signing-jobs", {}, job_body, {}, "us-east-1", "123") == "arn:aws:signer:us-east-1:123:/signing-profiles/fleet"
        assert extract_resource_arn("signer", "GET", "/signing-profiles/fleet", {}, b"", {}, "us-east-1", "123") == "arn:aws:signer:us-east-1:123:/signing-profiles/fleet"
        assert extract_resource_arn("signer", "GET", "/signing-jobs/9a1f", {}, b"", {}, "us-east-1", "123") == "arn:aws:signer:us-east-1:123:/signing-jobs/9a1f"
        assert extract_resource_arn("signer", "PUT", "/signing-profiles/fleet", {}, b"{}", {}, "us-east-1", "123") == "*"
        assert extract_resource_arn("signer", "GET", "/signing-jobs", {}, b"", {"status": "Succeeded"}, "us-east-1", "123") == "*"
        assert extract_resource_arn("signer", "POST", "/signing-jobs", {}, b"not json", {}, "us-east-1", "123") == "*"

    def test_signer_cancel_and_profile_permissions(self):
        """CancelSigningProfile and the three profile-permission actions
        are scoped to the signing profile."""
        from ministack.core.iam_actions import extract_resource_arn
        arn = "arn:aws:signer:us-east-1:123:/signing-profiles/fleet"
        for method, path in (("DELETE", "/signing-profiles/fleet"),
                             ("POST", "/signing-profiles/fleet/permissions"),
                             ("GET", "/signing-profiles/fleet/permissions"),
                             ("DELETE", "/signing-profiles/fleet/permissions/s1")):
            assert extract_resource_arn("signer", method, path, {}, b"{}", {}, "us-east-1", "123") == arn

    def test_elb_passthrough_arn(self):
        from ministack.core.iam_actions import extract_resource_arn
        lb_arn = "arn:aws:elasticloadbalancing:us-east-1:123:loadbalancer/app/my-lb/abc"
        assert extract_resource_arn("elasticloadbalancing", "POST", "/", {}, b"", {"LoadBalancerArn": [lb_arn]}, "us-east-1", "123") == lb_arn

    def test_cloudfront_distribution(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("cloudfront", "GET", "/2020-05-31/distribution/E123ABC", {}, b"", {}, "us-east-1", "123") == "arn:aws:cloudfront::123:distribution/E123ABC"

    def test_route53_hostedzone(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("route53", "GET", "/2013-04-01/hostedzone/Z123", {}, b"", {}, "us-east-1", "123") == "arn:aws:route53:::hostedzone/Z123"

    def test_cognito_userpool(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"UserPoolId": "us-east-1_abc123"}).encode()
        assert extract_resource_arn("cognito-idp", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:cognito-idp:us-east-1:123:userpool/us-east-1_abc123"

    def test_scheduler(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("scheduler", "GET", "/schedules/my-sched", {}, b"", {}, "us-east-1", "123") == "arn:aws:scheduler:us-east-1:123:schedule/default/my-sched"

    def test_pipes(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("pipes", "GET", "/v1/pipes/my-pipe", {}, b"", {}, "us-east-1", "123") == "arn:aws:pipes:us-east-1:123:pipe/my-pipe"

    def test_location_tracker(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("location", "GET", "/tracking/v0/trackers/fleet", {}, b"", {}, "us-east-1", "123") == "arn:aws:geo:us-east-1:123:tracker/fleet"
        assert extract_resource_arn("location", "POST", "/tracking/v0/trackers/fleet/positions", {}, b"", {}, "us-east-1", "123") == "arn:aws:geo:us-east-1:123:tracker/fleet"
        assert extract_resource_arn("location", "POST", "/tracking/v0/list-trackers", {}, b"", {}, "us-east-1", "123") == "*"

    def test_mq_broker(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert "mq" in extract_resource_arn("mq", "GET", "/v1/brokers/my-broker", {}, b"", {}, "us-east-1", "123")
        assert "my-broker" in extract_resource_arn("mq", "GET", "/v1/brokers/my-broker", {}, b"", {}, "us-east-1", "123")

    def test_kafka_cluster(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert "kafka" in extract_resource_arn("kafka", "GET", "/v1/clusters/my-cluster", {}, b"", {}, "us-east-1", "123")

    def test_dsql_cluster(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("dsql", "GET", "/clusters/cl-123", {}, b"", {}, "us-east-1", "123") == "arn:aws:dsql:us-east-1:123:cluster/cl-123"

    def test_efs_filesystem(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("elasticfilesystem", "GET", "/2015-02-01/file-systems/fs-123", {}, b"", {}, "us-east-1", "123") == "arn:aws:elasticfilesystem:us-east-1:123:file-system/fs-123"

    def test_cloudtrail(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"Name": "my-trail"}).encode()
        assert extract_resource_arn("cloudtrail", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:cloudtrail:us-east-1:123:trail/my-trail"

    def test_appconfig_application(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("appconfig", "GET", "/applications/app-123", {}, b"", {}, "us-east-1", "123") == "arn:aws:appconfig:us-east-1:123:application/app-123"

    def test_appconfig_environment(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("appconfig", "GET", "/applications/app-1/environments/env-1", {}, b"", {}, "us-east-1", "123") == "arn:aws:appconfig:us-east-1:123:application/app-1/environment/env-1"

    def test_resource_groups(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("resource-groups", "GET", "/groups/my-group", {}, b"", {}, "us-east-1", "123") == "arn:aws:resource-groups:us-east-1:123:group/my-group"

    def test_rds_data_passthrough(self):
        from ministack.core.iam_actions import extract_resource_arn
        body = json.dumps({"resourceArn": "arn:aws:rds:us-east-1:123:cluster:my-db"}).encode()
        assert extract_resource_arn("rds-data", "POST", "/", {}, body, {}, "us-east-1", "123") == "arn:aws:rds:us-east-1:123:cluster:my-db"

    def test_s3tables(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("s3tables", "GET", "/buckets/my-tb", {}, b"", {}, "us-east-1", "123") == "arn:aws:s3tables:us-east-1:123:bucket/my-tb"

    def test_mediaconnect(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert "mediaconnect" in extract_resource_arn("mediaconnect", "GET", "/v1/flows/flow-123", {}, b"", {}, "us-east-1", "123")

    # --- EC2 ---
    def test_ec2_instance(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("ec2", "POST", "/", {}, b"", {"InstanceId.1": ["i-abc123"]}, "us-east-1", "123") == "arn:aws:ec2:us-east-1:123:instance/i-abc123"

    def test_ec2_vpc(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("ec2", "POST", "/", {}, b"", {"VpcId": ["vpc-123"]}, "us-east-1", "123") == "arn:aws:ec2:us-east-1:123:vpc/vpc-123"

    def test_ec2_security_group(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("ec2", "POST", "/", {}, b"", {"GroupId": ["sg-123"]}, "us-east-1", "123") == "arn:aws:ec2:us-east-1:123:security-group/sg-123"

    def test_ec2_no_resource(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("ec2", "POST", "/", {}, b"", {"Action": ["DescribeInstances"]}, "us-east-1", "123") == "*"

    # --- IoT ---
    def test_iot_thing(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("iot", "GET", "/things/my-thing", {}, b"", {}, "us-east-1", "123") == "arn:aws:iot:us-east-1:123:thing/my-thing"

    def test_iot_policy(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("iot", "GET", "/policies/my-policy", {}, b"", {}, "us-east-1", "123") == "arn:aws:iot:us-east-1:123:policy/my-policy"

    def test_iot_rule(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("iot", "GET", "/rules/my-rule", {}, b"", {}, "us-east-1", "123") == "arn:aws:iot:us-east-1:123:rule/my-rule"

    def test_iot_job(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("iot", "PUT", "/jobs/rollout-2024", {}, b"", {}, "us-east-1", "123") == "arn:aws:iot:us-east-1:123:job/rollout-2024"

    def test_iot_provisioning_template(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("iot", "GET", "/provisioning-templates/fleet", {}, b"", {}, "us-east-1", "123") == "arn:aws:iot:us-east-1:123:provisioningtemplate/fleet"

    def test_iot_provisioning_claim(self):
        from ministack.core.iam_actions import extract_iam_action, extract_resource_arn
        path = "/provisioning-templates/fleet/provisioning-claim"
        assert extract_iam_action("iot", "POST", path, {}, b"", {}) == "iot:CreateProvisioningClaim"
        assert extract_resource_arn(
            "iot", "POST", path, {}, b"", {}, "us-east-1", "123"
        ) == "arn:aws:iot:us-east-1:123:provisioningtemplate/fleet"

    def test_iot_job_template(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn(
            "iot", "PUT", "/job-templates/ota", {}, b"", {}, "us-east-1", "123"
        ) == "arn:aws:iot:us-east-1:123:jobtemplate/ota"

    def test_iot_publish_topic_keeps_every_level(self):
        """A topic ARN carries the whole topic, not its first segment:
        arn:aws:iot:...:topic/sensors/rack-1/temperature. Truncating it to
        topic/sensors would authorize a publish against the wrong resource."""
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn(
            "iot", "POST", "/topics/sensors/rack-1/temperature", {}, b"", {}, "us-east-1", "123"
        ) == "arn:aws:iot:us-east-1:123:topic/sensors/rack-1/temperature"

    def test_iot_shadow_resolves_to_its_thing(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn(
            "iot", "GET", "/things/my-thing/shadow", {}, b"", {}, "us-east-1", "123"
        ) == "arn:aws:iot:us-east-1:123:thing/my-thing"

    def test_iot_collection_call_has_no_resource(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("iot", "GET", "/jobs", {}, b"", {}, "us-east-1", "123") == "*"

    def test_iot_tagging_calls_authorize_on_the_named_resource(self):
        from ministack.core.iam_actions import extract_resource_arn
        group = "arn:aws:iot:us-east-1:123:thinggroup/fleet"
        tag = json.dumps({"resourceArn": group, "tags": [{"Key": "k", "Value": "v"}]}).encode()
        untag = json.dumps({"resourceArn": group, "tagKeys": ["k"]}).encode()
        assert extract_resource_arn("iot", "POST", "/tags", {}, tag, {}, "us-east-1", "123") == group
        assert extract_resource_arn("iot", "POST", "/untag", {}, untag, {}, "us-east-1", "123") == group
        assert extract_resource_arn(
            "iot", "GET", "/tags", {}, b"", {"resourceArn": [group]}, "us-east-1", "123"
        ) == group
        assert extract_resource_arn("iot", "GET", "/tags", {}, b"", {}, "us-east-1", "123") == "*"
        # Only where the handler reads it: a second resourceArn elsewhere is not the target.
        other = {"resourceArn": ["arn:aws:iot:us-east-1:123:thinggroup/other"]}
        assert extract_resource_arn("iot", "POST", "/tags", {}, tag, other, "us-east-1", "123") == group
        assert extract_resource_arn(
            "iot", "GET", "/tags", {}, tag, other, "us-east-1", "123"
        ) == "arn:aws:iot:us-east-1:123:thinggroup/other"
        bad = json.dumps({"resourceArn": [group]}).encode()
        assert extract_resource_arn("iot", "POST", "/untag", {}, bad, {}, "us-east-1", "123") == "*"

    def test_iot_job_policy_from_the_cdk_actually_allows_the_call(self):
        """The two halves together, which is where this showed up. The CDK's
        AwsCustomResource emits exactly this policy for an IoT job, and every
        such stack failed to deploy under AUTH because the request carried no
        job ARN to match it against: only Resource "*" got through."""
        from ministack.core.iam_actions import extract_resource_arn

        arn = extract_resource_arn(
            "iot", "PUT", "/jobs/hawkbit_rollout-2024", {}, b"", {}, "eu-central-1", "000000000000"
        )
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow",
            "Action": "iot:CreateJob",
            "Resource": [
                "arn:aws:iot:eu-central-1:000000000000:job/hawkbit_rollout-2024",
                "arn:aws:iot:eu-central-1:000000000000:thinggroup/hawkbit_rollout",
            ],
        }]})
        ctx = EvalContext(
            principal_arn="arn:aws:sts::000000000000:assumed-role/deployer/session",
            principal_type="AssumedRole",
            principal_account="000000000000",
            action="iot:CreateJob", resource_arn=arn, region="eu-central-1",
        )
        assert evaluate(ctx, [stmts]).decision == "Allow"

    def test_iot_retained_message_is_its_topic(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn(
            "iot-data", "GET", "/retainedMessage/sensors/rack-1", {}, b"", {}, "us-east-1", "123"
        ) == "arn:aws:iot:us-east-1:123:topic/sensors/rack-1"

    # --- The data planes route by credential scope, not as "iot" ---
    def test_publish_resolves_through_the_router(self):
        """The branch above is reached only if the router agrees. A boto3
        iot-data client signs with the iotdata scope, so detect_service answers
        "iot-data" and not "iot": a branch keyed on "iot" alone is dead code
        that a test calling extract_resource_arn("iot", ...) cannot see."""
        from ministack.core.iam_actions import extract_iam_action, extract_resource_arn
        from ministack.core.router import detect_service

        path = "/topics/sensors/rack-1/temperature"
        headers = _sigv4_headers("iotdata", "data-ats.iot.eu-central-1.amazonaws.com")
        service = detect_service("POST", path, headers, {})
        assert service == "iot-data"
        assert extract_iam_action(service, "POST", path, headers, b"", {}) == "iot:Publish"
        assert extract_resource_arn(
            service, "POST", path, headers, b"", {}, "eu-central-1", "123"
        ) == "arn:aws:iot:eu-central-1:123:topic/sensors/rack-1/temperature"

    def test_publish_topic_arn_keeps_a_literal_percent_sequence(self):
        """A %41 or %2F left in the decoded Publish path is part of the topic name and stays in the ARN."""
        from ministack.core.iam_actions import extract_iam_action, extract_resource_arn
        from ministack.core.router import detect_service

        headers = _sigv4_headers("iotdata", "data-ats.iot.eu-central-1.amazonaws.com")
        for method, path, action, topic in (
            ("POST", "/topics/sensors/p%41q", "iot:Publish", "sensors/p%41q"),
            ("POST", "/topics/sensors/p%2Fq", "iot:Publish", "sensors/p%2Fq"),
            ("GET", "/retainedMessage/sensors/p%25q", "iot:GetRetainedMessage", "sensors/p%25q"),
        ):
            service = detect_service(method, path, headers, {})
            assert service == "iot-data"
            assert extract_iam_action(service, method, path, headers, b"", {}) == action
            assert extract_resource_arn(
                service, method, path, headers, b"", {}, "eu-central-1", "123"
            ) == f"arn:aws:iot:eu-central-1:123:topic/{topic}"

    def test_create_job_resolves_through_the_router(self):
        from ministack.core.iam_actions import extract_iam_action, extract_resource_arn
        from ministack.core.router import detect_service

        headers = _sigv4_headers("iot", "iot.eu-central-1.amazonaws.com")
        service = detect_service("PUT", "/jobs/rollout-2024", headers, {})
        assert service == "iot"
        assert extract_iam_action(service, "PUT", "/jobs/rollout-2024", headers, b"", {}) == "iot:CreateJob"
        assert extract_resource_arn(
            service, "PUT", "/jobs/rollout-2024", headers, b"", {}, "eu-central-1", "123"
        ) == "arn:aws:iot:eu-central-1:123:job/rollout-2024"

    def test_job_execution_on_the_jobs_data_plane_is_its_thing(self):
        """AWS scopes the jobs data plane on the thing, not the job."""
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn(
            "iot-jobs-data", "GET", "/things/dev-01/jobs/j1", {}, b"", {}, "us-east-1", "123"
        ) == "arn:aws:iot:us-east-1:123:thing/dev-01"

    # --- Query-protocol services put their parameters in the body ---
    def test_ec2_reads_a_form_encoded_body(self):
        """botocore POSTs EC2 as application/x-www-form-urlencoded, so nothing
        reaches the query string and the branch below resolved nothing. The
        router merges that body in, which is why this goes through it."""
        from ministack.app import _routing_params
        from ministack.core.iam_actions import extract_resource_arn

        body = b"Action=AuthorizeSecurityGroupIngress&GroupId=sg-0abc&Version=2016-11-15"
        params = _routing_params("POST", "/", _FORM, body, {})
        assert extract_resource_arn(
            "ec2", "POST", "/", _FORM, body, params, "eu-central-1", "123"
        ) == "arn:aws:ec2:eu-central-1:123:security-group/sg-0abc"

    def test_cloudformation_reads_a_form_encoded_body(self):
        from ministack.app import _routing_params
        from ministack.core.iam_actions import extract_resource_arn

        body = b"Action=DescribeStacks&StackName=my-stack"
        params = _routing_params("POST", "/", _FORM, body, {})
        assert extract_resource_arn(
            "cloudformation", "POST", "/", _FORM, body, params, "us-east-1", "123",
        ) == "arn:aws:cloudformation:us-east-1:123:stack/my-stack/*"

    def test_query_params_win_over_the_body(self):
        from ministack.app import _routing_params
        from ministack.core.iam_actions import extract_resource_arn

        body = b"GroupId=sg-body"
        params = _routing_params("POST", "/", _FORM, body, {"GroupId": ["sg-query"]})
        assert extract_resource_arn(
            "ec2", "POST", "/", _FORM, body, params, "us-east-1", "123",
        ) == "arn:aws:ec2:us-east-1:123:security-group/sg-query"

    def test_body_is_read_even_when_the_action_was_lifted_out_of_it(self):
        """The router already lifted Action out of this same body, so a merge
        that fired only on an empty dict would be a no-op on exactly the
        requests it exists for."""
        from ministack.app import _routing_params
        from ministack.core.iam_actions import extract_resource_arn

        body = b"Action=AuthorizeSecurityGroupIngress&GroupId=sg-0abc&Version=2016-11-15"
        params = _routing_params(
            "POST", "/", _FORM, body, {"Action": ["AuthorizeSecurityGroupIngress"]}
        )
        assert params["GroupId"] == ["sg-0abc"]
        assert extract_resource_arn(
            "ec2", "POST", "/", _FORM, body, params, "eu-central-1", "123",
        ) == "arn:aws:ec2:eu-central-1:123:security-group/sg-0abc"

    @pytest.mark.parametrize("content_type, body", [
        ("application/x-amz-json-1.1", b'{"TableName":"users"}'),
        ("application/xml", b"<Response>a=b</Response>"),
        ("application/x-www-form-urlencoded", b""),
        ("", b"Action=DescribeStacks&StackName=my-stack"),
    ])
    def test_only_a_form_encoded_body_is_merged(self, content_type, body):
        """A body of any other content type is left alone, which is what keeps
        this off the S3 upload path: extract_resource_arn runs on every
        authenticated request, and a PutObject body can be very large."""
        from ministack.app import _routing_params
        assert _routing_params("POST", "/", {"content-type": content_type}, body, {}) == {}

    def test_a_json_body_still_resolves_its_own_resource(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn(
            "dynamodb", "POST", "/", {}, b'{"TableName":"users"}', {}, "us-east-1", "123"
        ) == "arn:aws:dynamodb:us-east-1:123:table/users"

    # --- No false allows ---
    def test_a_scoped_grant_denies_the_resource_it_does_not_name(self):
        """The other half of the fix: the request now resolves to its own ARN,
        so a grant naming a different one has to stop matching."""
        from ministack.core.iam_actions import extract_resource_arn

        arn = extract_resource_arn(
            "iot", "PUT", "/jobs/rollout-2025", {}, b"", {}, "eu-central-1", "000000000000"
        )
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "iot:CreateJob",
            "Resource": "arn:aws:iot:eu-central-1:000000000000:job/rollout-2024",
        }]})
        ctx = EvalContext(
            principal_arn="arn:aws:sts::000000000000:assumed-role/deployer/session",
            principal_type="AssumedRole", principal_account="000000000000",
            action="iot:CreateJob", resource_arn=arn, region="eu-central-1",
        )
        assert evaluate(ctx, [stmts]).decision == "ImplicitDeny"

    def test_a_wildcard_grant_still_matches_a_resolved_arn(self):
        from ministack.core.iam_actions import extract_resource_arn

        arn = extract_resource_arn(
            "iot", "PUT", "/jobs/rollout-2024", {}, b"", {}, "eu-central-1", "000000000000"
        )
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "iot:CreateJob", "Resource": "*",
        }]})
        ctx = EvalContext(
            principal_arn="arn:aws:sts::000000000000:assumed-role/deployer/session",
            principal_type="AssumedRole", principal_account="000000000000",
            action="iot:CreateJob", resource_arn=arn, region="eu-central-1",
        )
        assert evaluate(ctx, [stmts]).decision == "Allow"

    # --- execute-api ---
    def test_execute_api_invoke_is_authorized_against_its_own_arn(self, monkeypatch):
        """Every invoke used to be authorized against "*", so a grant scoped to
        one API and stage — what the CDK's grantExecute and every hand-written
        service-to-service policy produce — never matched."""
        import ministack.app as app

        seen = _capture_enforced_arn(monkeypatch)
        app._enforce_execute_api("d9506af4", "dev", "POST", "/commands/delete", {}, {})
        assert seen["arn"] == (
            "arn:aws:execute-api:eu-central-1:000000000000:d9506af4/dev/POST/commands/delete"
        )

    def test_execute_api_default_stage_is_not_taken_from_the_path(self, monkeypatch):
        """Why the call sits after the stage is resolved. A v2 API on $default
        serves from the root, so the first path segment is a path segment, and
        authorizing on it would name a resource that does not exist."""
        import ministack.app as app

        seen = _capture_enforced_arn(monkeypatch)
        monkeypatch.setattr(app, "_parse_execute_api_url",
                            lambda host, path: ("d9506af4", "commands", "/delete"))
        monkeypatch.setattr(app, "_resolve_stage_and_path",
                            lambda api_id, tentative, path: ("$default", f"/{tentative}{path}"))
        monkeypatch.setattr(app, "_get_module", lambda name: _NoApiModule)

        asyncio.run(app._handle_execute_api_request(
            "d9506af4.execute-api.eu-central-1.amazonaws.com",
            "/commands/delete", "POST", {}, b"", {},
        ))
        assert seen["arn"] == (
            "arn:aws:execute-api:eu-central-1:000000000000:d9506af4/$default/POST/commands/delete"
        )

    # --- The WebSocket @connections API is its own action ---
    @pytest.mark.parametrize("via_mapping", [False, True])
    def test_connections_api_asks_for_manage_connections(self, monkeypatch, via_mapping):
        """AWS authorizes @connections under execute-api:ManageConnections. It
        asked for execute-api:Invoke, so a grantManageConnections policy, which
        names only that action, could not match. Also through a base-path
        mapping that names the stage."""
        import ministack.app as app

        seen = _capture_enforced_arn(monkeypatch)
        target = ("d9506af4", "dev", "/@connections/cid-1")
        if via_mapping:
            monkeypatch.setattr(app, "_parse_execute_api_url", lambda host, path: None)
            monkeypatch.setattr(app, "_resolve_custom_domain_request", lambda host, path: target)
        else:
            monkeypatch.setattr(app, "_parse_execute_api_url", lambda host, path: target)
        monkeypatch.setattr(app, "_get_module", lambda name: _NoApiModule)

        asyncio.run(app._handle_execute_api_request(
            "d9506af4.execute-api.eu-central-1.amazonaws.com",
            "/dev/@connections/cid-1", "POST", {}, b"", {},
        ))
        assert seen["action"] == "execute-api:ManageConnections"
        assert seen["arn"] == (
            "arn:aws:execute-api:eu-central-1:000000000000:d9506af4/dev/POST/@connections/cid-1"
        )

    @pytest.mark.parametrize("granted,status", [
        ("execute-api:ManageConnections", 200),
        ("execute-api:Invoke", 403),
    ])
    def test_a_manage_connections_grant_is_enforced_end_to_end(self, monkeypatch, granted, status):
        """The grant grantManageConnections writes, through the real
        _enforce_data_plane and enforce: it allows @connections, and an Invoke
        grant on the same resource does not."""
        import ministack.app as app
        from ministack.services import iam as iam_svc

        fake_key = "AKIATESTMANAGECONN1"
        iam_svc._access_keys[fake_key] = {
            "UserName": "conn-user", "AccessKeyId": fake_key,
            "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
        }
        iam_svc._users["conn-user"] = {
            "UserName": "conn-user",
            "Arn": "arn:aws:iam::000000000000:user/conn-user",
            "UserId": "AIDACONN1", "CreateDate": "2024-01-01", "Path": "/",
            "AttachedPolicies": [], "Tags": [],
        }
        iam_svc._user_inline_policies["conn-user"] = {"p": json.dumps({"Statement": [{
            "Effect": "Allow", "Action": granted,
            "Resource": "arn:aws:execute-api:eu-central-1:000000000000:d9506af4/*/*/@connections/*",
        }]})}
        monkeypatch.setattr(app, "AUTH", True)
        monkeypatch.setattr(app, "_parse_execute_api_url",
                            lambda host, path: ("d9506af4", "dev", "/@connections/cid-1"))
        monkeypatch.setattr(app, "_get_module", lambda name: _NoApiModule)
        headers = {"authorization": (
            f"AWS4-HMAC-SHA256 Credential={fake_key}/20260101/eu-central-1/execute-api"
            "/aws4_request, SignedHeaders=host, Signature=deadbeef"
        )}
        try:
            response = asyncio.run(app._handle_execute_api_request(
                "d9506af4.execute-api.eu-central-1.amazonaws.com",
                "/dev/@connections/cid-1", "POST", headers, b"", {},
            ))
        finally:
            iam_svc._access_keys.pop(fake_key, None)
            iam_svc._users.pop("conn-user", None)
            iam_svc._user_inline_policies.pop("conn-user", None)
        assert response[0] == status

    def test_a_plain_invoke_still_asks_for_invoke(self, monkeypatch):
        """Nothing but @connections moves off execute-api:Invoke. Through the
        handler, because that is where the branch is."""
        import ministack.app as app

        seen = _capture_enforced_arn(monkeypatch)
        monkeypatch.setattr(app, "_parse_execute_api_url",
                            lambda host, path: ("d9506af4", "dev", "/commands/delete"))
        monkeypatch.setattr(app, "_resolve_stage_and_path",
                            lambda api_id, tentative, path: (tentative, path))
        monkeypatch.setattr(app, "_get_module", lambda name: _NoApiModule)

        asyncio.run(app._handle_execute_api_request(
            "d9506af4.execute-api.eu-central-1.amazonaws.com",
            "/dev/commands/delete", "POST", {}, b"", {},
        ))
        assert seen["action"] == "execute-api:Invoke"

    @pytest.mark.parametrize("granted,refusal", [
        ("d9506af4/prod/$connect", None),
        ("d9506af4/prod/$default", (403, (
            "User: arn:aws:iam::000000000000:user/ws-user is not authorized to perform: execute-api:Invoke "
            "on resource: arn:aws:execute-api:eu-central-1:000000000000:d9506af4/prod/$connect "
            "because no identity-based policy allows the execute-api:Invoke action"))),
    ], ids=["connect", "other-route"])
    def test_websocket_aws_iam_connect_asks_for_invoke_on_connect(self, monkeypatch, granted, refusal):
        """A handshake on an AWS_IAM $connect route is authorized as execute-api:Invoke on <api>/<stage>/$connect."""
        import ministack.app as app
        from ministack.services import apigateway as apigw_svc
        from ministack.services import iam as iam_svc

        fake_key = "AKIATESTWSCONNECT01"
        iam_svc._access_keys[fake_key] = {
            "UserName": "ws-user", "AccessKeyId": fake_key,
            "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
        }
        iam_svc._users["ws-user"] = {
            "UserName": "ws-user", "Arn": "arn:aws:iam::000000000000:user/ws-user",
            "UserId": "AIDAWSCONN1", "CreateDate": "2024-01-01", "Path": "/",
            "AttachedPolicies": [], "Tags": [],
        }
        iam_svc._user_inline_policies["ws-user"] = {"p": json.dumps({"Statement": [{
            "Effect": "Allow", "Action": "execute-api:Invoke",
            "Resource": f"arn:aws:execute-api:eu-central-1:000000000000:{granted}",
        }]})}
        monkeypatch.setattr(app, "AUTH", True)
        monkeypatch.setattr(apigw_svc, "_api_owner", lambda api_id: ("WEBSOCKET", "000000000000", "eu-central-1"))
        monkeypatch.setattr(apigw_svc, "_match_ws_route",
                            lambda api_id, key: {"routeKey": "$connect", "authorizationType": "AWS_IAM"})
        stages = apigw_svc.AccountRegionScopedDict()
        stages.set_scoped("000000000000", "eu-central-1", "d9506af4", {"prod": {}})
        monkeypatch.setattr(apigw_svc, "_stages", stages)
        connected = []

        async def connect_integration(*args, **kwargs):
            connected.append(args[4])
            return {"statusCode": 403}

        monkeypatch.setattr(apigw_svc, "_invoke_ws_lambda", connect_integration)

        async def receive():
            return {"type": "websocket.connect"}

        def handshake(query):
            sent = []

            async def send(message):
                sent.append(message)

            scope = {"type": "websocket", "path": "/prod", "query_string": query.encode(), "headers": [],
                     "extensions": {"websocket.http.response": {}}}
            asyncio.run(apigw_svc.handle_websocket(scope, receive, send, "d9506af4"))
            return sent[0].get("status"), json.loads(sent[1]["body"])["message"] if len(sent) > 1 else None

        try:
            unsigned = handshake("")
            signed = handshake(f"X-Amz-Credential={fake_key}%2F20260101%2Feu-central-1%2Fexecute-api%2Faws4_request")
        finally:
            iam_svc._access_keys.pop(fake_key, None)
            iam_svc._users.pop("ws-user", None)
            iam_svc._user_inline_policies.pop("ws-user", None)
        assert unsigned == (403, "Missing Authentication Token")
        if refusal is None:
            assert connected == ["prod"]
        else:
            assert (connected, signed) == ([], refusal)

    # --- Lambda Function URLs ---
    @pytest.mark.parametrize("resolved,arn,context", [
        (("000000000000", "eu-central-1", "notify", None, {"AuthType": "AWS_IAM"}),
         _NOTIFY_ARN, {"lambda:FunctionUrlAuthType": "AWS_IAM"}),
        (("000000000000", "eu-central-1", "notify", "prod", {"AuthType": "NONE"}),
         f"{_NOTIFY_ARN}:prod", {"lambda:FunctionUrlAuthType": "NONE"}),
        (None, "*", {}),
    ], ids=["function", "alias", "unknown"])
    def test_function_url_is_authorized_against_its_function(self, monkeypatch, resolved, arn, context):
        """lambda:InvokeFunctionUrl ran against "*", so a grant naming the
        function, which is what grantInvokeUrl writes, never matched. A URL on
        an alias carries the qualifier. The URL's AuthType is supplied as
        lambda:FunctionUrlAuthType and lambda:InvokedViaFunctionUrl is not. An
        id that resolves to nothing keeps "*" and no keys, so the lookup,
        which runs before the caller is authorized, reports nothing about which
        URLs exist. The handler reuses the resolution."""
        import ministack.app as app

        seen = _capture_enforced_arn(monkeypatch)
        module = _function_url_module(resolved)
        monkeypatch.setattr(app, "_get_module", lambda name: module)

        asyncio.run(app._handle_lambda_url_request(_FUNCTION_URL_HOST, "/", "GET", {}, b"", {}))
        assert seen["action"] == "lambda:InvokeFunctionUrl"
        assert seen["arn"] == arn
        assert seen["context"] == context
        assert module.handled["resolved"] == resolved

    def test_the_resolved_resource_and_key_reach_the_evaluator(self, monkeypatch):
        """The tests above stub _enforce_data_plane, so they pin what the handler
        resolved. _enforce_data_plane has to hand both on to enforce. With AUTH
        off nothing reaches enforce and the URL is still served."""
        import ministack.app as app
        from ministack.core import iam_evaluator

        seen: dict = {}

        def enforce_stub(access_key_id, iam_action, service, region, resource_arn="*",
                         service_context=None):
            seen.update(action=iam_action, arn=resource_arn, context=service_context)
            return None

        monkeypatch.setattr(app, "AUTH", True)
        monkeypatch.setattr(iam_evaluator, "enforce", enforce_stub)
        monkeypatch.setattr(app, "_get_module", lambda name: _function_url_module(
            ("000000000000", "eu-central-1", "notify", None, {"AuthType": "NONE"})))

        asyncio.run(app._handle_lambda_url_request(_FUNCTION_URL_HOST, "/", "GET", {}, b"", {}))
        assert seen == {"action": "lambda:InvokeFunctionUrl", "arn": _NOTIFY_ARN,
                        "context": {"lambda:FunctionUrlAuthType": "NONE"}}

        seen.clear()
        monkeypatch.setattr(app, "AUTH", False)
        response = asyncio.run(app._handle_lambda_url_request(_FUNCTION_URL_HOST, "/", "GET", {}, b"", {}))
        assert response[0] == 200
        assert seen == {}

    def test_the_function_url_handler_does_not_resolve_a_passed_resolution_again(self, monkeypatch):
        from ministack.services import lambda_svc

        def fail(url_id):
            raise AssertionError("resolve_function_url must not run when resolved= is passed")

        monkeypatch.setattr(lambda_svc, "resolve_function_url", fail)
        status, _, body = asyncio.run(lambda_svc.handle_function_url_request(
            "url-id", "GET", "/", {}, b"", {},
            resolved=("000000000000", "eu-central-1", "no-such-fn", None, {"AuthType": "NONE"}),
        ))
        assert status == 404
        assert b"no-such-fn" in body

    def test_a_grant_invoke_url_policy_matches_an_aws_iam_url(self):
        """End of the chain: the resource and the condition key together are
        what make the policy the CDK writes match."""
        ctx = _function_url_eval_context({"lambda:FunctionUrlAuthType": "AWS_IAM"})
        assert evaluate(ctx, [_grant_invoke_url_statements()]).decision == "Allow"

    def test_a_grant_invoke_url_policy_does_not_match_a_none_url(self):
        """The negative case. A URL whose AuthType is NONE supplies NONE, the
        StringEquals fails, and the statement does not match, which is what AWS
        answers for the same pair."""
        ctx = _function_url_eval_context({"lambda:FunctionUrlAuthType": "NONE"})
        assert evaluate(ctx, [_grant_invoke_url_statements()]).decision == "ImplicitDeny"

    def test_the_same_policy_without_the_key_still_does_not_match(self):
        """What the behaviour was before, and what it still is for a key this
        does not supply: an unresolved key makes the condition false. The
        evaluator's fallback is unchanged; only the key became resolvable."""
        ctx = _function_url_eval_context({})
        assert evaluate(ctx, [_grant_invoke_url_statements()]).decision == "ImplicitDeny"

    # --- API Gateway ---
    def test_apigateway_v2(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("apigateway", "GET", "/v2/apis/abc123", {}, b"", {}, "us-east-1", "123") == "arn:aws:apigateway:us-east-1::/apis/abc123"

    def test_apigateway_v1(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("apigateway", "GET", "/restapis/xyz789", {}, b"", {}, "us-east-1", "123") == "arn:aws:apigateway:us-east-1::/restapis/xyz789"

    # --- Bedrock ---
    def test_bedrock_model(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("bedrock-runtime", "POST", "/model/anthropic.claude-v2/invoke", {}, b"", {}, "us-east-1", "123") == "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2"

    def test_bedrock_agent(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("bedrock-agent", "GET", "/agents/AGENT123", {}, b"", {}, "us-east-1", "123") == "arn:aws:bedrock:us-east-1:123:agent/AGENT123"

    def test_bedrock_knowledge_base(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("bedrock-agent", "GET", "/knowledgebases/KB123", {}, b"", {}, "us-east-1", "123") == "arn:aws:bedrock:us-east-1:123:knowledge-base/KB123"

    # --- AppSync ---
    def test_appsync_api(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("appsync", "GET", "/v1/apis/abc123", {}, b"", {}, "us-east-1", "123") == "arn:aws:appsync:us-east-1:123:apis/abc123"

    def test_appsync_datasource(self):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("appsync", "GET", "/v1/apis/abc/datasources/myds", {}, b"", {}, "us-east-1", "123") == "arn:aws:appsync:us-east-1:123:apis/abc/datasources/myds"

    def test_resource_scoped_policy_enforcement(self):
        """A policy allowing s3:GetObject only on mybucket/* should deny access to otherbucket."""
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::mybucket/*"
        }]})
        ctx_allowed = _ctx(action="s3:GetObject", resource="arn:aws:s3:::mybucket/key")
        assert evaluate(ctx_allowed, [stmts]).decision == "Allow"
        ctx_denied = _ctx(action="s3:GetObject", resource="arn:aws:s3:::otherbucket/key")
        assert evaluate(ctx_denied, [stmts]).decision == "ImplicitDeny"

    def test_resource_wildcard_in_arn(self):
        """Wildcards in resource ARNs work."""
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "dynamodb:*",
            "Resource": "arn:aws:dynamodb:*:*:table/users*"
        }]})
        ctx = _ctx(action="dynamodb:PutItem",
                   resource="arn:aws:dynamodb:us-east-1:123:table/users")
        assert evaluate(ctx, [stmts]).decision == "Allow"
        ctx2 = _ctx(action="dynamodb:PutItem",
                    resource="arn:aws:dynamodb:us-east-1:123:table/orders")
        assert evaluate(ctx2, [stmts]).decision == "ImplicitDeny"


class TestActionExtraction:
    def test_query_protocol(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("sqs", "POST", "/", {}, b"", {"Action": ["CreateQueue"]}) == "sqs:CreateQueue"
        assert extract_iam_action("monitoring", "POST", "/", {}, b"", {"Action": ["PutMetricData"]}) == "cloudwatch:PutMetricData"

    def test_budgets_target_protocol(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("budgets", "POST", "/", {"x-amz-target": "AWSBudgetServiceGateway.CreateBudget"}, b"", {}) == "budgets:CreateBudget"

    def test_target_protocol(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("dynamodb", "POST", "/", {"x-amz-target": "DynamoDB_20120810.PutItem"}, b"", {}) == "dynamodb:PutItem"
        assert extract_iam_action("kms", "POST", "/", {"x-amz-target": "TrentService.Encrypt"}, b"", {}) == "kms:Encrypt"

    def test_opensearchserverless_target_protocol(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action(
            "opensearchserverless", "POST", "/", {"x-amz-target": "OpenSearchServerless.CreateCollection"}, b"", {}
        ) == "aoss:CreateCollection"

    def test_s3_rest(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("s3", "GET", "/", {}, b"", {}) == "s3:ListAllMyBuckets"
        assert extract_iam_action("s3", "PUT", "/bucket/key", {}, b"", {}) == "s3:PutObject"
        assert extract_iam_action("s3", "GET", "/bucket", {}, b"", {"versioning": [""]}) == "s3:GetBucketVersioning"
        assert extract_iam_action("s3", "POST", "/bucket/key", {}, b"", {"restore": [""]}) == "s3:RestoreObject"

    def test_lambda_rest(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("lambda", "POST", "/2015-03-31/functions", {}, b"", {}) == "lambda:CreateFunction"
        assert extract_iam_action("lambda", "POST", "/2015-03-31/functions/f/invocations", {}, b"", {}) == "lambda:InvokeFunction"

    def test_lambda_tags_rest(self):
        from ministack.core.iam_actions import extract_iam_action
        path = "/2017-03-31/tags/arn:aws:lambda:us-east-1:000000000000:function:f"
        assert extract_iam_action("lambda", "POST", path, {}, b"", {}) == "lambda:TagResource"
        assert extract_iam_action("lambda", "DELETE", path, {}, b"", {"tagKeys": ["a"]}) == "lambda:UntagResource"
        assert extract_iam_action("lambda", "GET", path, {}, b"", {}) == "lambda:ListTags"

    def test_unknown_service_returns_none(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("unknown_svc", "GET", "/", {}, b"", {}) is None

    def test_the_jobs_data_plane_is_the_iotjobsdata_namespace(self):
        """The four job-execution operations are iotjobsdata: actions on AWS.
        They were mapped to iot:, which a grant of the documented action cannot
        match. StartCommandExecution is the fifth operation the same botocore
        model declares, and AWS keeps that one on iot:."""
        from ministack.core.iam_actions import extract_iam_action

        def act(method, path):
            return extract_iam_action("iot-jobs-data", method, path, {}, b"", {})

        assert act("GET", "/things/dev-01/jobs") == "iotjobsdata:GetPendingJobExecutions"
        assert act("PUT", "/things/dev-01/jobs/$next") == "iotjobsdata:StartNextPendingJobExecution"
        assert act("GET", "/things/dev-01/jobs/j1") == "iotjobsdata:DescribeJobExecution"
        assert act("POST", "/things/dev-01/jobs/j1/") == "iotjobsdata:UpdateJobExecution"
        assert act("POST", "/command-executions") == "iot:StartCommandExecution"

    def test_the_jobs_data_plane_resolves_through_the_router(self):
        """The branch above is reached only if the router agrees. The AWS IoT
        Jobs SDK signs with credential scope iot-jobs-data and hits the
        data.jobs.iot... host family; a test calling extract_iam_action with
        the literal string "iot-jobs-data" cannot see whether the router
        actually routes a real signed request there, the way
        test_publish_resolves_through_the_router does for the message plane."""
        from ministack.core.iam_actions import extract_iam_action
        from ministack.core.router import detect_service

        headers = _sigv4_headers("iot-jobs-data", "data.jobs.iot.eu-central-1.localhost")

        path = "/things/dev-01/jobs"
        service = detect_service("GET", path, headers, {})
        assert service == "iot-jobs-data"
        assert extract_iam_action(service, "GET", path, headers, b"", {}) == "iotjobsdata:GetPendingJobExecutions"

        service = detect_service("POST", "/command-executions", headers, {})
        assert service == "iot-jobs-data"
        assert extract_iam_action(
            service, "POST", "/command-executions", headers, b"", {}
        ) == "iot:StartCommandExecution"

    def test_the_iot_control_plane_and_message_plane_keep_the_iot_namespace(self):
        """Only the jobs data plane moves. iot: is still the namespace for the
        control plane and for publish."""
        from ministack.core.iam_actions import extract_iam_action

        assert extract_iam_action("iot", "PUT", "/jobs/rollout-2024", {}, b"", {}) == "iot:CreateJob"
        assert extract_iam_action("iot-data", "POST", "/topics/a/b/c", {}, b"", {}) == "iot:Publish"

    def test_agentcore_invoke_resolves_through_the_router(self):
        """The AgentCore signing scope and InvokeAgentRuntime path are recognized."""
        from ministack.core.iam_actions import extract_iam_action, extract_resource_arn
        from ministack.core.router import detect_service

        headers = _sigv4_headers(
            "bedrock-agentcore", "bedrock-agentcore.us-east-1.amazonaws.com"
        )
        path = (
            "/runtimes/arn%3Aaws%3Abedrock-agentcore%3Aus-east-1%3A"
            "000000000000%3Aruntime%2Frt-example/invocations"
        )
        service = detect_service("POST", path, headers, {})
        assert service == "bedrock-agentcore"
        assert extract_iam_action(service, "POST", path, headers, b"{}", {}) == (
            "bedrock-agentcore:InvokeAgentRuntime"
        )
        decoded_path = (
            "/runtimes/arn:aws:bedrock-agentcore:us-east-1:000000000000:"
            "runtime/rt-example/invocations"
        )
        assert extract_iam_action(service, "POST", decoded_path, headers, b"{}", {}) == (
            "bedrock-agentcore:InvokeAgentRuntime"
        )
        assert extract_resource_arn(
            service, "POST", path, headers, b"{}", {}, "us-east-1", "000000000000"
        ) == "arn:aws:bedrock-agentcore:us-east-1:000000000000:runtime/rt-example"
        assert extract_resource_arn(
            service, "POST", decoded_path, headers, b"{}", {}, "us-east-1", "000000000000"
        ) == "arn:aws:bedrock-agentcore:us-east-1:000000000000:runtime/rt-example"
        assert extract_iam_action(service, "GET", decoded_path, headers, b"{}", {}) is None
        assert extract_iam_action(
            service, "POST", "/runtimes//invocations", headers, b"{}", {}
        ) is None

    def test_agentcore_resource_policy_routes_use_control_plane_actions(self):
        from ministack.core.iam_actions import extract_iam_action, extract_resource_arn

        headers = _sigv4_headers(
            "bedrock-agentcore", "bedrock-agentcore.us-east-1.amazonaws.com"
        )
        resource = "arn:aws:bedrock-agentcore:us-east-1:000000000000:runtime/rt-example"
        path = "/resourcepolicy/" + resource.replace(":", "%3A").replace("/", "%2F")
        assert extract_iam_action("bedrock-agentcore", "PUT", path, headers, b"{}", {}) == (
            "bedrock-agentcore:PutResourcePolicy"
        )
        assert extract_iam_action("bedrock-agentcore", "GET", path, headers, b"", {}) == (
            "bedrock-agentcore:GetResourcePolicy"
        )
        assert extract_iam_action("bedrock-agentcore", "DELETE", path, headers, b"", {}) == (
            "bedrock-agentcore:DeleteResourcePolicy"
        )
        assert extract_resource_arn(
            "bedrock-agentcore", "PUT", path, headers, b"{}", {},
            "us-east-1", "000000000000",
        ) == resource


class TestBedrockAgentCoreAuthorization:
    _RUNTIME_ARN = "arn:aws:bedrock-agentcore:us-east-1:000000000000:runtime/rt-example"
    _OTHER_RUNTIME_ARN = "arn:aws:bedrock-agentcore:us-east-1:000000000000:runtime/rt-other"

    @staticmethod
    def _invoke(path, monkeypatch, policy, query=None):
        import asyncio

        import ministack.app as app_mod
        from ministack.core import iam_evaluator

        statements = parse_policy_document(policy)
        seen = []

        def enforce_stub(access_key_id, iam_action, service, region, resource_arn="*",
                         service_context=None):
            seen.append((iam_action, service, region, resource_arn))
            result = evaluate(_ctx(action=iam_action, resource=resource_arn), [statements])
            if result.decision == "Allow":
                return None
            result.principal_arn = "arn:aws:iam::000000000000:user/testuser"
            return result

        async def invoke_handler(method, request_path, headers, body, query):
            return 200, {"Content-Type": "application/json"}, b"{}"

        monkeypatch.setattr(app_mod, "AUTH", True, raising=False)
        monkeypatch.setattr(iam_evaluator, "enforce", enforce_stub)
        monkeypatch.setitem(app_mod.SERVICE_HANDLERS, "bedrock-agentcore", invoke_handler)
        headers = {
            **_sigv4_headers(
                "bedrock-agentcore", "bedrock-agentcore.us-east-1.amazonaws.com"
            ),
            "host": "bedrock-agentcore.us-east-1.amazonaws.com",
        }
        headers["authorization"] = headers["authorization"].replace(
            "20260101/eu-central-1/", "20260101/us-east-1/"
        )
        response = asyncio.run(
            app_mod._dispatch_service_request("POST", path, headers, b"{}", query or {}, "req-agentcore")
        )
        return response, seen

    def test_invoke_allows_the_exact_runtime_resource(self, monkeypatch):
        path = "/runtimes/" + self._RUNTIME_ARN.replace(":", "%3A").replace("/", "%2F") + "/invocations"
        endpoint = self._RUNTIME_ARN + "/runtime-endpoint/DEFAULT"
        response, seen = self._invoke(
            path,
            monkeypatch,
            {"Statement": [{
                "Effect": "Allow",
                "Action": "bedrock-agentcore:InvokeAgentRuntime",
                "Resource": [self._RUNTIME_ARN, endpoint],
            }]},
        )

        assert response[0] == 200
        assert [arn for *_, arn in seen] == [self._RUNTIME_ARN, endpoint]

    @pytest.mark.parametrize("query,endpoint_name", [({}, "DEFAULT"), ({"qualifier": ["prod"]}, "prod")])
    def test_invoke_also_needs_the_runtime_endpoint(self, monkeypatch, query, endpoint_name):
        """runtime and runtime-endpoint are both required resources of InvokeAgentRuntime."""
        path = "/runtimes/" + self._RUNTIME_ARN.replace(":", "%3A").replace("/", "%2F") + "/invocations"
        response, seen = self._invoke(
            path,
            monkeypatch,
            {"Statement": [{
                "Effect": "Allow",
                "Action": "bedrock-agentcore:InvokeAgentRuntime",
                "Resource": self._RUNTIME_ARN,
            }]},
            query,
        )

        assert response[0] == 403
        assert seen[-1][3] == f"{self._RUNTIME_ARN}/runtime-endpoint/{endpoint_name}"

    def test_invoke_denies_a_different_runtime_resource(self, monkeypatch):
        path = "/runtimes/" + self._OTHER_RUNTIME_ARN.replace(":", "%3A").replace("/", "%2F") + "/invocations"
        response, seen = self._invoke(
            path,
            monkeypatch,
            {"Statement": [{
                "Effect": "Allow",
                "Action": "bedrock-agentcore:InvokeAgentRuntime",
                "Resource": self._RUNTIME_ARN,
            }]},
        )

        assert response[0] == 403
        assert b"AccessDenied" in response[2]
        assert seen == [(
            "bedrock-agentcore:InvokeAgentRuntime",
            "bedrock-agentcore",
            "us-east-1",
            self._OTHER_RUNTIME_ARN,
        )]

    @pytest.mark.parametrize("granted,decision", [
        ("iot:*", "ImplicitDeny"),
        ("iotjobsdata:UpdateJobExecution", "Allow"),
    ])
    def test_a_jobs_data_plane_grant_needs_the_iotjobsdata_name(self, granted, decision):
        """A policy written for the old iot: name stops matching, as on AWS, and
        the name the documentation tells a device policy to grant matches."""
        from ministack.core.iam_actions import extract_iam_action

        action = extract_iam_action("iot-jobs-data", "POST", "/things/dev-01/jobs/j1/", {}, b"", {})
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": granted,
            "Resource": "arn:aws:iot:eu-central-1:000000000000:thing/dev-01",
        }]})
        ctx = EvalContext(
            principal_arn="arn:aws:sts::000000000000:assumed-role/device/session",
            principal_type="AssumedRole", principal_account="000000000000",
            action=action, resource_arn="arn:aws:iot:eu-central-1:000000000000:thing/dev-01",
            region="eu-central-1",
        )
        assert evaluate(ctx, [stmts]).decision == decision


class TestDynamoDBTransactionAuthorization:
    """TransactWriteItems / TransactGetItems are authorised per item."""

    _TABLE = "arn:aws:dynamodb:us-east-1:000000000000:table/repro"

    @staticmethod
    def _transact(monkeypatch, target, items_key, items, actions, resource="*", policy=None):
        import asyncio

        import ministack.app as app_mod
        from ministack.core import iam_evaluator

        statements = parse_policy_document(policy or {"Statement": [{
            "Effect": "Allow", "Action": actions, "Resource": resource,
        }]})
        seen = []
        handled = []

        def enforce_stub(access_key_id, iam_action, service, region, resource_arn="*",
                         service_context=None):
            seen.append((iam_action, resource_arn))
            ctx = _ctx(action=iam_action, resource=resource_arn)
            ctx.service_context = {k.lower(): v for k, v in (service_context or {}).items()}
            result = evaluate(ctx, [statements])
            if result.decision == "Allow":
                return None
            result.principal_arn = "arn:aws:iam::000000000000:user/testuser"
            return result

        async def handler(method, request_path, headers, body, query):
            handled.append(True)
            return 200, {"Content-Type": "application/x-amz-json-1.0"}, b"{}"

        monkeypatch.setattr(app_mod, "AUTH", True, raising=False)
        monkeypatch.setattr(iam_evaluator, "enforce", enforce_stub)
        monkeypatch.setitem(app_mod.SERVICE_HANDLERS, "dynamodb", handler)
        headers = {
            **_sigv4_headers("dynamodb", "dynamodb.us-east-1.amazonaws.com"),
            "host": "dynamodb.us-east-1.amazonaws.com",
            "x-amz-target": f"DynamoDB_20120810.{target}",
            "content-type": "application/x-amz-json-1.0",
        }
        headers["authorization"] = headers["authorization"].replace(
            "20260101/eu-central-1/", "20260101/us-east-1/"
        )
        body = json.dumps({items_key: items}).encode()
        response = asyncio.run(
            app_mod._dispatch_service_request("POST", "/", headers, body, {}, "req-ddb-tx")
        )
        return response, seen, handled

    _WRITE = [
        {"ConditionCheck": {"TableName": "repro", "Key": {"pk": {"S": "guard"}},
                            "ConditionExpression": "attribute_exists(pk)"}},
        {"Put": {"TableName": "repro", "Item": {"pk": {"S": "written"}}}},
    ]

    def test_write_is_allowed_by_the_per_item_actions(self, monkeypatch):
        response, seen, handled = self._transact(
            monkeypatch, "TransactWriteItems", "TransactItems", self._WRITE,
            ["dynamodb:ConditionCheckItem", "dynamodb:PutItem"],
        )
        assert response[0] == 200
        assert handled
        assert seen == [
            ("dynamodb:ConditionCheckItem", self._TABLE),
            ("dynamodb:PutItem", self._TABLE),
        ]

    def test_write_without_condition_check_permission_is_denied(self, monkeypatch):
        response, _, handled = self._transact(
            monkeypatch, "TransactWriteItems", "TransactItems", self._WRITE,
            ["dynamodb:TransactWriteItems", "dynamodb:PutItem"],
        )
        assert response[0] == 403
        assert b"AccessDenied" in response[2]
        assert b"dynamodb:ConditionCheckItem" in response[2]
        assert not handled

    def test_update_and_delete_map_to_their_own_actions(self, monkeypatch):
        items = [
            {"Update": {"TableName": "repro", "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET x = :x"}},
            {"Delete": {"TableName": "repro", "Key": {"pk": {"S": "b"}}}},
        ]
        response, seen, _ = self._transact(
            monkeypatch, "TransactWriteItems", "TransactItems", items,
            ["dynamodb:UpdateItem", "dynamodb:DeleteItem"],
        )
        assert response[0] == 200
        assert [a for a, _ in seen] == ["dynamodb:UpdateItem", "dynamodb:DeleteItem"]

    def test_item_naming_several_members_is_checked_for_each(self, monkeypatch):
        # AWS rejects an item with two members; the handler does not, so the
        # second member must not run unchecked behind an allowed first one.
        items = [{
            "Update": {"TableName": "repro", "Key": {"pk": {"S": "a"}}, "UpdateExpression": "SET x = :x"},
            "Delete": {"TableName": "repro", "Key": {"pk": {"S": "a"}}},
        }]
        response, seen, handled = self._transact(
            monkeypatch, "TransactWriteItems", "TransactItems", items, ["dynamodb:UpdateItem"],
        )
        assert [a for a, _ in seen] == ["dynamodb:UpdateItem", "dynamodb:DeleteItem"]
        assert response[0] == 403
        assert b"dynamodb:DeleteItem" in response[2]
        assert not handled

    def test_denial_on_a_later_table_refuses_the_whole_transaction(self, monkeypatch):
        items = [
            {"Put": {"TableName": "repro", "Item": {"pk": {"S": "a"}}}},
            {"Put": {"TableName": "other", "Item": {"pk": {"S": "b"}}}},
        ]
        response, seen, handled = self._transact(
            monkeypatch, "TransactWriteItems", "TransactItems", items, ["dynamodb:PutItem"],
            resource=self._TABLE,
        )
        assert [r for _, r in seen] == [self._TABLE, self._TABLE.replace("repro", "other")]
        assert response[0] == 403
        assert b"AccessDenied" in response[2]
        assert not handled

    def test_get_is_authorised_per_item_on_its_own_table(self, monkeypatch):
        items = [
            {"Get": {"TableName": "repro", "Key": {"pk": {"S": "a"}}}},
            {"Get": {"TableName": "other", "Key": {"pk": {"S": "b"}}}},
        ]
        response, seen, _ = self._transact(
            monkeypatch, "TransactGetItems", "TransactItems", items, ["dynamodb:GetItem"],
        )
        assert response[0] == 200
        assert seen == [
            ("dynamodb:GetItem", self._TABLE),
            ("dynamodb:GetItem", self._TABLE.replace("repro", "other")),
        ]

    _ITEM_ACTIONS = ["dynamodb:ConditionCheckItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
                     "dynamodb:DeleteItem", "dynamodb:GetItem"]
    _ENCLOSING = {"ForAnyValue:StringEquals": {
        "dynamodb:EnclosingOperation": ["TransactWriteItems", "TransactGetItems"]}}

    def test_allow_only_transactional_operations(self, monkeypatch):
        """Example 2 of the DynamoDB transactions IAM guide."""
        policy = {"Statement": [{"Effect": "Allow", "Action": self._ITEM_ACTIONS,
                                 "Resource": "*", "Condition": self._ENCLOSING}]}
        response, _, handled = self._transact(
            monkeypatch, "TransactWriteItems", "TransactItems", self._WRITE, [], policy=policy)
        assert response[0] == 200 and handled

    def test_deny_transactional_operations(self, monkeypatch):
        """Example 3 of the DynamoDB transactions IAM guide."""
        policy = {"Statement": [
            {"Effect": "Deny", "Action": self._ITEM_ACTIONS, "Resource": "*",
             "Condition": self._ENCLOSING},
            {"Effect": "Allow", "Action": self._ITEM_ACTIONS, "Resource": "*"},
        ]}
        response, _, handled = self._transact(
            monkeypatch, "TransactWriteItems", "TransactItems", self._WRITE, [], policy=policy)
        assert response[0] == 403 and not handled

    def test_get_with_only_the_whole_operation_action_is_denied(self, monkeypatch):
        items = [{"Get": {"TableName": "repro", "Key": {"pk": {"S": "a"}}}}]
        response, _, handled = self._transact(
            monkeypatch, "TransactGetItems", "TransactItems", items,
            ["dynamodb:TransactGetItems"],
        )
        assert response[0] == 403
        assert b"AccessDenied" in response[2]
        assert b"dynamodb:GetItem" in response[2]
        assert not handled


class TestS3ActionMapping:
    """S3 authorizes by the IAM actions its documentation lists, not by API
    operation name: every multipart operation except abort is ``s3:PutObject``."""

    def _act(self, method, path, query):
        from ministack.core.iam_actions import extract_iam_action
        return extract_iam_action("s3", method, path, {}, b"", query)

    @pytest.mark.parametrize("method,path,query,expected", [
        ("POST", "/bucket/key", {"uploads": ""}, "s3:PutObject"),          # CreateMultipartUpload
        ("PUT", "/bucket/key", {"uploadId": "u", "partNumber": "1"}, "s3:PutObject"),  # UploadPart
        ("POST", "/bucket/key", {"uploadId": "u"}, "s3:PutObject"),        # CompleteMultipartUpload
        ("DELETE", "/bucket/key", {"uploadId": "u"}, "s3:AbortMultipartUpload"),
        ("GET", "/bucket/key", {"uploadId": "u"}, "s3:ListMultipartUploadParts"),
        ("GET", "/bucket", {"uploads": ""}, "s3:ListBucketMultipartUploads"),
        ("GET", "/bucket", {"versions": ""}, "s3:ListBucketVersions"),
        ("GET", "/bucket/key", {"tagging": ""}, "s3:GetObjectTagging"),
        ("PUT", "/bucket/key", {"acl": ""}, "s3:PutObjectAcl"),
        ("GET", "/bucket", {"tagging": ""}, "s3:GetBucketTagging"),
        ("DELETE", "/bucket", {"tagging": ""}, "s3:PutBucketTagging"),     # no s3:DeleteBucketTagging
        ("DELETE", "/bucket/key", {"tagging": ""}, "s3:DeleteObjectTagging"),
        ("GET", "/bucket", {"acl": ""}, "s3:GetBucketAcl"),
        ("DELETE", "/bucket", {"lifecycle": ""}, "s3:PutLifecycleConfiguration"),
        ("DELETE", "/bucket", {"encryption": ""}, "s3:PutEncryptionConfiguration"),
        ("DELETE", "/bucket", {"replication": ""}, "s3:PutReplicationConfiguration"),
        ("DELETE", "/bucket", {"cors": ""}, "s3:PutBucketCORS"),
        ("DELETE", "/bucket", {"policy": ""}, "s3:DeleteBucketPolicy"),   # this one exists
        ("GET", "/bucket/key", {"versions": ""}, "s3:GetObject"),         # bucket-only sub-resource
        # a request that names a version authorizes as the version action
        ("GET", "/bucket/key", {"versionId": "v1"}, "s3:GetObjectVersion"),
        ("HEAD", "/bucket/key", {"versionId": "v1"}, "s3:GetObjectVersion"),
        ("DELETE", "/bucket/key", {"versionId": "v1"}, "s3:DeleteObjectVersion"),
        ("GET", "/bucket/key", {"versionId": "v1", "tagging": ""}, "s3:GetObjectVersionTagging"),
        ("PUT", "/bucket/key", {"versionId": "v1", "tagging": ""}, "s3:PutObjectVersionTagging"),
        ("DELETE", "/bucket/key", {"versionId": "v1", "tagging": ""}, "s3:DeleteObjectVersionTagging"),
        ("GET", "/bucket/key", {"versionId": "v1", "acl": ""}, "s3:GetObjectVersionAcl"),
        ("PUT", "/bucket/key", {"versionId": "v1", "acl": ""}, "s3:PutObjectVersionAcl"),
        ("GET", "/bucket/key", {"versionId": "v1", "attributes": ""}, "s3:GetObjectVersionAttributes"),
        ("GET", "/bucket/key", {"versionId": ["v1"]}, "s3:GetObjectVersion"),  # parse_qs list form
        ("GET", "/bucket/key", {"versionId": ""}, "s3:GetObject"),        # an empty versionId names nothing
        ("PUT", "/bucket/key", {"versionId": "v1", "retention": ""}, "s3:PutObjectRetention"),  # no version action
        ("GET", "/bucket", {"versionId": "v1"}, "s3:ListBucket"),         # bucket level: no version action
        # a POST on the bucket is the browser form upload unless it says ?delete
        ("POST", "/bucket", {}, "s3:PutObject"),                          # POST Object
        ("POST", "/bucket", {"delete": ""}, "s3:DeleteObject"),           # DeleteObjects
        # object sub-resources
        ("POST", "/bucket/key", {"select": "", "select-type": "2"}, "s3:GetObject"),  # SelectObjectContent
        ("GET", "/bucket/key", {"attributes": ""}, "s3:GetObjectAttributes"),
        ("GET", "/bucket/key", {"retention": ""}, "s3:GetObjectRetention"),
        ("PUT", "/bucket/key", {"retention": ""}, "s3:PutObjectRetention"),
        ("GET", "/bucket/key", {"legal-hold": ""}, "s3:GetObjectLegalHold"),
        ("PUT", "/bucket/key", {"legal-hold": ""}, "s3:PutObjectLegalHold"),
        ("GET", "/bucket/key", {"torrent": ""}, "s3:GetObject"),
        ("GET", "/bucket/key", {"object-lock": ""}, "s3:GetObject"),      # bucket-only sub-resource
        # bucket sub-resources; a DELETE of a configuration is its Put action
        ("GET", "/bucket", {"accelerate": ""}, "s3:GetAccelerateConfiguration"),
        ("PUT", "/bucket", {"accelerate": ""}, "s3:PutAccelerateConfiguration"),
        ("GET", "/bucket", {"requestPayment": ""}, "s3:GetBucketRequestPayment"),
        ("PUT", "/bucket", {"requestPayment": ""}, "s3:PutBucketRequestPayment"),
        ("GET", "/bucket", {"publicAccessBlock": ""}, "s3:GetBucketPublicAccessBlock"),
        ("PUT", "/bucket", {"publicAccessBlock": ""}, "s3:PutBucketPublicAccessBlock"),
        ("DELETE", "/bucket", {"publicAccessBlock": ""}, "s3:PutBucketPublicAccessBlock"),
        ("GET", "/bucket", {"ownershipControls": ""}, "s3:GetBucketOwnershipControls"),
        ("PUT", "/bucket", {"ownershipControls": ""}, "s3:PutBucketOwnershipControls"),
        ("DELETE", "/bucket", {"ownershipControls": ""}, "s3:PutBucketOwnershipControls"),
        ("GET", "/bucket", {"intelligent-tiering": "", "id": "x"}, "s3:GetIntelligentTieringConfiguration"),
        ("PUT", "/bucket", {"intelligent-tiering": "", "id": "x"}, "s3:PutIntelligentTieringConfiguration"),
        ("DELETE", "/bucket", {"intelligent-tiering": "", "id": "x"}, "s3:PutIntelligentTieringConfiguration"),
        ("GET", "/bucket", {"metrics": "", "id": "x"}, "s3:GetMetricsConfiguration"),
        ("PUT", "/bucket", {"metrics": "", "id": "x"}, "s3:PutMetricsConfiguration"),
        ("DELETE", "/bucket", {"metrics": "", "id": "x"}, "s3:PutMetricsConfiguration"),
        ("GET", "/bucket", {"analytics": "", "id": "x"}, "s3:GetAnalyticsConfiguration"),
        ("PUT", "/bucket", {"analytics": "", "id": "x"}, "s3:PutAnalyticsConfiguration"),
        ("DELETE", "/bucket", {"analytics": "", "id": "x"}, "s3:PutAnalyticsConfiguration"),
        ("GET", "/bucket", {"inventory": "", "id": "x"}, "s3:GetInventoryConfiguration"),
        ("PUT", "/bucket", {"inventory": "", "id": "x"}, "s3:PutInventoryConfiguration"),
        ("DELETE", "/bucket", {"inventory": "", "id": "x"}, "s3:PutInventoryConfiguration"),
        ("GET", "/bucket", {"object-lock": ""}, "s3:GetBucketObjectLockConfiguration"),
        ("PUT", "/bucket", {"object-lock": ""}, "s3:PutBucketObjectLockConfiguration"),
        ("GET", "/bucket", {"policyStatus": ""}, "s3:GetBucketPolicyStatus"),
        ("PUT", "/bucket/key", {}, "s3:PutObject"),
        ("HEAD", "/bucket/key", {}, "s3:GetObject"),
        ("GET", "/bucket", {}, "s3:ListBucket"),
    ])
    def test_operation_maps_to_documented_iam_action(self, method, path, query, expected):
        assert self._act(method, path, query) == expected

    def test_cdk_publishing_role_grant_covers_multipart(self):
        # The CDK bootstrap file-publishing role grants s3:PutObject* and
        # s3:Abort*; a multipart asset upload must be authorized by those.
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject*", "s3:GetBucket*", "s3:List*", "s3:PutObject*", "s3:Abort*"],
            "Resource": ["arn:aws:s3:::cdk-assets", "arn:aws:s3:::cdk-assets/*"]}]})
        for method, query in (("POST", {"uploads": ""}), ("PUT", {"uploadId": "u", "partNumber": "1"}),
                              ("POST", {"uploadId": "u"}), ("DELETE", {"uploadId": "u"})):
            action = self._act(method, "/cdk-assets/asset.zip", query)
            ctx = _ctx(action=action, resource="arn:aws:s3:::cdk-assets/asset.zip")
            assert evaluate(ctx, [stmts]).decision == "Allow", action


class TestS3AdditionalChecks:
    """A copy reads its source, an attributes call is a pair, a batch delete
    is authorized per key and a governance bypass is its own action, so those
    requests carry checks beyond the (action, resource) pair the extractors
    return."""

    _NS = "http://s3.amazonaws.com/doc/2006-03-01/"   # what boto3 sends
    _DELETE_BODY = (f'<Delete xmlns="{_NS}"><Object><Key>a.txt</Key></Object>'
                    '<Object><Key>dir/b.txt</Key><VersionId>v7</VersionId></Object>'
                    '<Quiet>true</Quiet></Delete>').encode()
    _BYPASS = {"x-amz-bypass-governance-retention": "true"}

    @staticmethod
    def _checks(method, path, headers=None, body=b"", query=None):
        from ministack.core.iam_actions import s3_additional_checks
        return s3_additional_checks(method, path, headers or {}, body, query or {})

    # -- copy source --------------------------------------------------------

    def test_copy_object_reads_the_source(self):
        assert self._checks("PUT", "/dst/key", {"x-amz-copy-source": "/src/a%20b.txt"}) == [
            ("s3:GetObject", "arn:aws:s3:::src/a b.txt")]

    def test_copy_source_may_omit_the_leading_slash_and_name_a_version(self):
        checks = self._checks("PUT", "/dst/key", {"x-amz-copy-source": "src/dir/a.txt?versionId=v3"},
                              query={"uploadId": "u", "partNumber": "1"})  # UploadPartCopy
        assert checks == [("s3:GetObjectVersion", "arn:aws:s3:::src/dir/a.txt")]

    def test_plain_put_and_malformed_source_add_nothing(self):
        assert self._checks("PUT", "/dst/key") == []
        assert self._checks("PUT", "/dst/key", {"x-amz-copy-source": "nokey"}) == []
        assert self._checks("PUT", "/dst", {"x-amz-copy-source": "/src/k"}) == []   # CreateBucket
        assert self._checks("GET", "/") == []

    def test_copy_needs_read_on_the_source(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:PutObject", "Resource": "arn:aws:s3:::dst/*"}]})
        (act, arn), = self._checks("PUT", "/dst/k", {"x-amz-copy-source": "/src/k"})
        assert evaluate(_ctx(action=act, resource=arn), [stmts]).decision == "ImplicitDeny"

    # -- attributes pair ----------------------------------------------------

    def test_attributes_also_need_get_object(self):
        assert self._checks("GET", "/b/dir/sub/k.txt", query={"attributes": ""}) == [
            ("s3:GetObject", "arn:aws:s3:::b/dir/sub/k.txt")]

    def test_versioned_attributes_pair_with_get_object_version(self):
        assert self._checks("GET", "/b/k", query={"attributes": "", "versionId": "v1"}) == [
            ("s3:GetObjectVersion", "arn:aws:s3:::b/k")]

    def test_a_plain_get_or_head_is_a_single_check(self):
        assert self._checks("GET", "/b/k") == []
        assert self._checks("HEAD", "/b/k", query={"versionId": "v1"}) == []

    # -- governance bypass --------------------------------------------------

    def test_bypass_header_adds_the_bypass_action_on_the_object(self):
        assert self._checks("DELETE", "/b/k", self._BYPASS) == [
            ("s3:BypassGovernanceRetention", "arn:aws:s3:::b/k")]
        assert self._checks("DELETE", "/b/k", self._BYPASS, query={"versionId": "v1"}) == [
            ("s3:BypassGovernanceRetention", "arn:aws:s3:::b/k")]
        assert self._checks("PUT", "/b/k", self._BYPASS, query={"retention": ""}) == [
            ("s3:BypassGovernanceRetention", "arn:aws:s3:::b/k")]

    def test_bypass_header_is_ignored_where_the_operation_has_none(self):
        assert self._checks("DELETE", "/b/k", {"x-amz-bypass-governance-retention": "false"}) == []
        assert self._checks("DELETE", "/b/k", self._BYPASS, query={"tagging": ""}) == []
        assert self._checks("PUT", "/b/k", self._BYPASS) == []
        assert self._checks("DELETE", "/b", self._BYPASS) == []

    def test_batch_delete_bypass_covers_every_key(self):
        checks = self._checks("POST", "/b", self._BYPASS, self._DELETE_BODY, {"delete": ""})
        assert checks == [
            ("s3:DeleteObjectVersion", "arn:aws:s3:::b/dir/b.txt"),
            ("s3:BypassGovernanceRetention", "arn:aws:s3:::b/a.txt"),
            ("s3:BypassGovernanceRetention", "arn:aws:s3:::b/dir/b.txt"),
        ]

    # -- batch delete -------------------------------------------------------

    def test_batch_delete_is_one_check_per_key(self):
        from ministack.core.iam_actions import extract_resource_arn
        primary = extract_resource_arn("s3", "POST", "/b", {}, self._DELETE_BODY, {"delete": ""}, "", "")
        assert primary == "arn:aws:s3:::b/a.txt"
        assert self._checks("POST", "/b", body=self._DELETE_BODY, query={"delete": ""}) == [
            ("s3:DeleteObjectVersion", "arn:aws:s3:::b/dir/b.txt"),
        ]

    def test_batch_delete_body_without_a_namespace_parses_too(self):
        body = b"<Delete><Object><Key>x</Key></Object><Object><Key>y</Key></Object></Delete>"
        assert self._checks("POST", "/b", body=body, query={"delete": ""}) == [
            ("s3:DeleteObject", "arn:aws:s3:::b/y")]

    def test_batch_delete_skips_an_object_without_a_key(self):
        body = (b"<Delete><Object><VersionId>v1</VersionId></Object>"
                b"<Object><Key></Key></Object><Object><Key>z</Key></Object></Delete>")
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("s3", "POST", "/b", {}, body, {"delete": ""}, "", "") == "arn:aws:s3:::b/z"
        assert self._checks("POST", "/b", body=body, query={"delete": ""}) == []

    @pytest.mark.parametrize("body", [b"", b"<Delete>", b"<!DOCTYPE d [<!ENTITY e 'x'>]><Delete>&e;</Delete>"])
    def test_batch_delete_with_no_usable_body_keeps_the_bucket(self, body):
        from ministack.core.iam_actions import extract_resource_arn
        assert extract_resource_arn("s3", "POST", "/b", {}, body, {"delete": ""}, "", "") == "arn:aws:s3:::b"
        assert self._checks("POST", "/b", body=body, query={"delete": ""}) == []

    def test_a_plain_bucket_post_is_not_a_batch_delete(self):
        assert self._checks("POST", "/b", body=self._DELETE_BODY) == []   # POST Object

    def test_object_scoped_policy_allows_a_batch_delete(self):
        # The case from the report: a grant on arn:aws:s3:::b/* used to be
        # denied because the batch was evaluated against the bucket ARN.
        from ministack.core.iam_actions import extract_iam_action, extract_resource_arn
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
            "Resource": "arn:aws:s3:::b/*"}]})
        action = extract_iam_action("s3", "POST", "/b", {}, self._DELETE_BODY, {"delete": ""})
        primary = extract_resource_arn("s3", "POST", "/b", {}, self._DELETE_BODY, {"delete": ""}, "", "")
        checks = [(action, primary)] + self._checks("POST", "/b", body=self._DELETE_BODY, query={"delete": ""})
        for act, arn in checks:
            assert evaluate(_ctx(action=act, resource=arn), [stmts]).decision == "Allow", (act, arn)


class TestS3EnforcementSites:
    """Both S3 enforcement sites (virtual-hosted and path-style) run the
    additional checks after the primary one. The evaluator is stubbed with a
    policy so the tests drive the app functions in-process."""

    _NS = "http://s3.amazonaws.com/doc/2006-03-01/"
    _BATCH = (f'<Delete xmlns="{_NS}"><Object><Key>a.txt</Key></Object>'
              '<Object><Key>b.txt</Key></Object></Delete>').encode()

    @staticmethod
    def _stub_evaluator(monkeypatch, policy):
        """Route enforce() through a fixed policy; return the checks it saw."""
        import ministack.app as app_mod
        from ministack.core import iam_evaluator

        stmts = parse_policy_document(policy)
        seen = []

        def enforce_stub(access_key_id, iam_action, service, region, resource_arn="*",
                         service_context=None):
            seen.append((iam_action, resource_arn))
            result = evaluate(_ctx(action=iam_action, resource=resource_arn), [stmts])
            if result.decision == "Allow":
                return None
            result.principal_arn = "arn:aws:iam::000000000000:user/testuser"
            return result

        monkeypatch.setattr(app_mod, "AUTH", True, raising=False)
        monkeypatch.setattr(iam_evaluator, "enforce", enforce_stub)
        return seen

    @staticmethod
    def _vhost(bucket, path, method, headers, body, query):
        import asyncio

        import ministack.app as app_mod
        return asyncio.run(app_mod._handle_s3_vhost_request(
            f"{bucket}.localhost:4566", path, method, headers, body, query))

    @staticmethod
    def _path_style(method, path, headers, body, query):
        import asyncio

        import ministack.app as app_mod
        headers = {"host": "localhost:4566", **headers}
        return asyncio.run(app_mod._dispatch_service_request(method, path, headers, body, query, "req-1"))

    _ONLY_FIRST_KEY = {"Statement": [{
        "Effect": "Allow", "Action": "s3:DeleteObject", "Resource": "arn:aws:s3:::iam-sites-b/a.txt"}]}
    _EVERY_KEY = {"Statement": [{
        "Effect": "Allow", "Action": "s3:DeleteObject", "Resource": "arn:aws:s3:::iam-sites-b/*"}]}

    def test_vhost_batch_delete_is_denied_on_the_second_key(self, monkeypatch):
        seen = self._stub_evaluator(monkeypatch, self._ONLY_FIRST_KEY)
        status, _headers, body = self._vhost("iam-sites-b", "/", "POST", {}, self._BATCH, {"delete": ""})
        assert status == 403
        assert b"AccessDenied" in body
        assert seen == [("s3:DeleteObject", "arn:aws:s3:::iam-sites-b/a.txt"),
                        ("s3:DeleteObject", "arn:aws:s3:::iam-sites-b/b.txt")]

    def test_vhost_batch_delete_with_an_object_scoped_grant_passes(self, monkeypatch):
        seen = self._stub_evaluator(monkeypatch, self._EVERY_KEY)
        status, _headers, _body = self._vhost("iam-sites-b", "/", "POST", {}, self._BATCH, {"delete": ""})
        assert status != 403
        assert [a for a, _ in seen] == ["s3:DeleteObject", "s3:DeleteObject"]

    def test_path_style_batch_delete_is_denied_on_the_second_key(self, monkeypatch):
        seen = self._stub_evaluator(monkeypatch, self._ONLY_FIRST_KEY)
        status, _headers, body = self._path_style("POST", "/iam-sites-b", {}, self._BATCH, {"delete": ""})
        assert status == 403
        assert b"AccessDenied" in body
        assert seen == [("s3:DeleteObject", "arn:aws:s3:::iam-sites-b/a.txt"),
                        ("s3:DeleteObject", "arn:aws:s3:::iam-sites-b/b.txt")]

    def test_path_style_batch_delete_with_an_object_scoped_grant_passes(self, monkeypatch):
        seen = self._stub_evaluator(monkeypatch, self._EVERY_KEY)
        status, _headers, _body = self._path_style("POST", "/iam-sites-b", {}, self._BATCH, {"delete": ""})
        assert status != 403
        assert [a for a, _ in seen] == ["s3:DeleteObject", "s3:DeleteObject"]

    def test_a_primary_denial_stops_before_the_extra_checks(self, monkeypatch):
        seen = self._stub_evaluator(monkeypatch, {"Statement": [
            {"Effect": "Allow", "Action": "s3:GetObject", "Resource": "*"}]})
        status, _headers, _body = self._path_style(
            "PUT", "/iam-sites-dst/k", {"x-amz-copy-source": "/iam-sites-src/k"}, b"", {})
        assert status == 403
        assert seen == [("s3:PutObject", "arn:aws:s3:::iam-sites-dst/k")]


# ---------------------------------------------------------------------------
# AccessDenied response formatting
# ---------------------------------------------------------------------------

class TestAccessDeniedResponse:
    def test_s3_rest_xml(self):
        from ministack.core.iam_actions import access_denied_response
        s, h, b = access_denied_response("s3", "s3:PutObject", "arn:aws:iam::123:user/a", "r1")
        assert s == 403
        assert b"<Error>" in b
        assert b"AccessDenied" in b

    def test_ec2_unauthorized_operation(self):
        from ministack.core.iam_actions import access_denied_response
        s, h, b = access_denied_response("ec2", "ec2:RunInstances", "arn:aws:iam::123:user/a", "r1")
        assert s == 403
        assert b"UnauthorizedOperation" in b

    def test_json_protocol(self):
        from ministack.core.iam_actions import access_denied_response
        s, h, b = access_denied_response("dynamodb", "dynamodb:PutItem", "arn:aws:iam::123:user/a", "r1")
        assert s == 403
        body = json.loads(b)
        assert body["__type"] == "AccessDeniedException"

    def test_query_xml_protocol(self):
        from ministack.core.iam_actions import access_denied_response
        s, h, b = access_denied_response("iam", "iam:CreateRole", "arn:aws:iam::123:user/a", "r1")
        assert s == 403
        assert b"<ErrorResponse" in b

    def test_custom_error_code_preserved(self):
        from ministack.core.iam_actions import access_denied_response
        s, h, b = access_denied_response("dynamodb", "dynamodb:PutItem", "", "r1",
                                          error_code="ExpiredTokenException",
                                          message="Token expired")
        assert b"ExpiredTokenException" in b

    def test_ssm_denial_shape_applies_to_every_ssm_action(self):
        """Not only the tag actions: AWS answers e.g. GetParameters denials with
        HTTP 400 and the resource in the message."""
        from ministack.core.iam_actions import access_denied_response

        arn = "arn:aws:ssm:us-east-1:123456789012:parameter/app/db"
        status, headers, body = access_denied_response(
            "ssm", "ssm:GetParameter", "arn:aws:iam::123456789012:user/a", "r1", resource_arn=arn)
        assert status == 400
        assert headers["Content-Type"] == "application/x-amz-json-1.1"
        denied = json.loads(body)
        assert denied["__type"] == "AccessDeniedException"
        assert f"ssm:GetParameter on resource: {arn} because" in denied["Message"]

    @pytest.mark.parametrize("action", (
        "AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource",
    ))
    def test_ssm_tag_credential_error_keeps_authentication_status(self, action):
        from ministack.core.iam_actions import access_denied_response

        status, _, body = access_denied_response(
            "ssm", f"ssm:{action}", "", "r1",
            error_code="UnrecognizedClientException", message="Invalid signing credentials",
        )
        assert status == 403
        assert json.loads(body) == {
            "__type": "UnrecognizedClientException", "message": "Invalid signing credentials",
        }


# ---------------------------------------------------------------------------
# Integration: SimulateCustomPolicy (runs against server, AUTH=false OK)
# ---------------------------------------------------------------------------

def test_simulate_custom_policy_allow(iam):
    policy = json.dumps({"Version": "2012-10-17",
                         "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]})
    resp = iam.simulate_custom_policy(PolicyInputList=[policy],
                                      ActionNames=["s3:GetObject", "s3:PutObject"])
    results = resp["EvaluationResults"]
    assert len(results) == 2
    for r in results:
        assert r["EvalDecision"] == "allowed"


def test_simulate_custom_policy_implicit_deny(iam):
    policy = json.dumps({"Version": "2012-10-17",
                         "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]})
    resp = iam.simulate_custom_policy(PolicyInputList=[policy],
                                      ActionNames=["ec2:RunInstances"])
    assert resp["EvaluationResults"][0]["EvalDecision"] == "implicitDeny"


def test_simulate_custom_policy_explicit_deny(iam):
    policy = json.dumps({"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "*", "Resource": "*"},
        {"Effect": "Deny", "Action": "s3:DeleteBucket", "Resource": "*"},
    ]})
    resp = iam.simulate_custom_policy(PolicyInputList=[policy],
                                      ActionNames=["s3:PutObject", "s3:DeleteBucket"])
    results = {r["EvalActionName"]: r["EvalDecision"] for r in resp["EvaluationResults"]}
    assert results["s3:PutObject"] == "allowed"
    assert results["s3:DeleteBucket"] == "explicitDeny"


# ---------------------------------------------------------------------------
# Integration: Policy document validation (runs against server)
# ---------------------------------------------------------------------------

def test_create_policy_rejects_malformed_json(iam):
    with pytest.raises(ClientError) as exc:
        iam.create_policy(PolicyName="bad-policy-json", PolicyDocument="not valid json")
    assert exc.value.response["Error"]["Code"] == "MalformedPolicyDocument"


def test_create_policy_rejects_missing_action(iam):
    with pytest.raises(ClientError) as exc:
        iam.create_policy(PolicyName="bad-policy-no-action",
                          PolicyDocument=json.dumps({"Statement": [{"Effect": "Allow", "Resource": "*"}]}))
    assert exc.value.response["Error"]["Code"] == "MalformedPolicyDocument"


def test_put_role_policy_rejects_malformed_document(iam):
    iam.create_role(
        RoleName="validation-test-role-2",
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "sts:AssumeRole"}]
        }),
    )
    try:
        with pytest.raises(ClientError) as exc:
            iam.put_role_policy(RoleName="validation-test-role-2",
                                PolicyName="bad", PolicyDocument="not json")
        assert exc.value.response["Error"]["Code"] == "MalformedPolicyDocument"
    finally:
        iam.delete_role(RoleName="validation-test-role-2")


def test_put_group_policy_rejects_malformed_document(iam):
    """PutGroupPolicy validates its document like PutRolePolicy and
    PutUserPolicy. Measured: a real account answers MalformedPolicyDocument for
    a document that is not JSON, one without a Statement, one whose Effect is
    neither Allow nor Deny, and one whose statement has no Resource."""
    iam.create_group(GroupName="validation-test-group")
    try:
        for document in ("not json",
                         json.dumps({"Version": "2012-10-17"}),
                         json.dumps({"Statement": [{"Effect": "Maybe",
                                                    "Action": "s3:GetObject",
                                                    "Resource": "*"}]}),
                         json.dumps({"Statement": [{"Effect": "Allow",
                                                    "Action": "s3:GetObject"}]})):
            with pytest.raises(ClientError) as exc:
                iam.put_group_policy(GroupName="validation-test-group",
                                     PolicyName="bad", PolicyDocument=document)
            assert exc.value.response["Error"]["Code"] == "MalformedPolicyDocument"
        assert iam.list_group_policies(
            GroupName="validation-test-group")["PolicyNames"] == []
    finally:
        iam.delete_group(GroupName="validation-test-group")


def test_put_inline_policy_reports_the_missing_entity_before_the_document(iam):
    """A call that is wrong in both ways at once is answered NoSuchEntity, not
    MalformedPolicyDocument.

    Measured per kind on a real account, with a document whose Effect is
    neither Allow nor Deny on a group, a role and a user, and again, on a role
    and a user, with one that is not JSON: the missing entity is reported
    before the document is read."""
    for call, kind, kwargs in (
        (iam.put_group_policy, "group", {"GroupName": "validation-absent-group"}),
        (iam.put_role_policy, "role", {"RoleName": "validation-absent-role"}),
        (iam.put_user_policy, "user", {"UserName": "validation-absent-user"}),
    ):
        for document in ("not json",
                         json.dumps({"Statement": [{"Effect": "Maybe",
                                                    "Action": "s3:GetObject",
                                                    "Resource": "*"}]})):
            with pytest.raises(ClientError) as exc:
                call(PolicyName="bad", PolicyDocument=document, **kwargs)
            assert exc.value.response["Error"]["Code"] == "NoSuchEntity"
            assert exc.value.response["Error"]["Message"] == \
                f"The {kind} with name validation-absent-{kind} cannot be found."


# ---------------------------------------------------------------------------
# Resource ARN extraction (#1504, #1505)
#
# Several services moved to the JSON protocol, so their parameters arrive in
# the body and never reach query_params. A branch that reads only query params
# falls back to "*", which no resource-scoped statement matches, and every
# least-privilege policy for that service denies.
# ---------------------------------------------------------------------------

class TestExtractResourceArn:
    ACCOUNT = "000000000000"
    REGION = "us-east-1"

    def _arn(self, service, body=b"", query=None, path="/", method="POST"):
        from ministack.core.iam_actions import extract_resource_arn
        return extract_resource_arn(
            service, method, path, {}, body, query or {},
            self.REGION, self.ACCOUNT,
        )

    def test_sqs_queue_url_from_json_body(self):
        # SQS is a JSON-protocol service: current SDKs send QueueUrl in the body.
        body = json.dumps({
            "QueueUrl": "http://localhost:4566/000000000000/my-queue",
            "MessageBody": "x",
        }).encode()
        assert self._arn("sqs", body=body) == (
            "arn:aws:sqs:us-east-1:000000000000:my-queue")

    def test_sqs_queue_name_from_json_body(self):
        body = json.dumps({"QueueName": "my-queue"}).encode()
        assert self._arn("sqs", body=body) == (
            "arn:aws:sqs:us-east-1:000000000000:my-queue")

    def test_sqs_queue_url_from_query_form_still_works(self):
        query = {"QueueUrl": ["http://localhost:4566/000000000000/my-queue"]}
        assert self._arn("sqs", query=query) == (
            "arn:aws:sqs:us-east-1:000000000000:my-queue")

    def test_sqs_queue_from_request_path(self):
        # Query-protocol callers address the queue by path.
        assert self._arn("sqs", path="/000000000000/my-queue") == (
            "arn:aws:sqs:us-east-1:000000000000:my-queue")

    def test_sqs_queue_in_another_account_keeps_its_owner(self):
        body = json.dumps({"QueueUrl": "http://localhost:4566/111111111111/my-queue"}).encode()
        assert self._arn("sqs", body=body) == "arn:aws:sqs:us-east-1:111111111111:my-queue"
        assert self._arn("sqs", path="/111111111111/my-queue") == "arn:aws:sqs:us-east-1:111111111111:my-queue"

    def test_sqs_without_a_queue_falls_back_to_star(self):
        assert self._arn("sqs", body=json.dumps({"MaxResults": 10}).encode()) == "*"

    def test_kms_key_id_from_body(self):
        body = json.dumps({"KeyId": "1234abcd-12ab-34cd-56ef-1234567890ab"}).encode()
        assert self._arn("kms", body=body) == (
            "arn:aws:kms:us-east-1:000000000000:key/"
            "1234abcd-12ab-34cd-56ef-1234567890ab")

    def test_kms_decrypt_resolves_the_key_from_the_ciphertext(self):
        # Decrypt carries no KeyId for a symmetric key: "AWS KMS can get this
        # information from metadata that it adds to the symmetric ciphertext
        # blob." IAM still evaluates against that key's ARN.
        import base64
        key_id = "1234abcd-12ab-34cd-56ef-1234567890ab"
        blob = key_id.encode() + b"\x00" * 32 + b"\x00" * 16 + b"payload"
        body = json.dumps({
            "CiphertextBlob": base64.b64encode(blob).decode(),
        }).encode()
        assert self._arn("kms", body=body) == (
            f"arn:aws:kms:us-east-1:000000000000:key/{key_id}")

    def test_kms_unparseable_ciphertext_falls_back_to_star(self):
        import base64
        body = json.dumps({
            "CiphertextBlob": base64.b64encode(b"too-short").decode(),
        }).encode()
        assert self._arn("kms", body=body) == "*"

    def test_kms_explicit_key_id_wins_over_the_ciphertext(self):
        import base64
        blob = ("1234abcd-12ab-34cd-56ef-1234567890ab".encode()
                + b"\x00" * 48 + b"payload")
        body = json.dumps({
            "KeyId": "alias/my-key",
            "CiphertextBlob": base64.b64encode(blob).decode(),
        }).encode()
        assert self._arn("kms", body=body) == (
            "arn:aws:kms:us-east-1:000000000000:alias/my-key")

    def test_acm_certificate_arn_from_json_body(self):
        arn = "arn:aws:acm:us-east-1:000000000000:certificate/abc"
        body = json.dumps({"CertificateArn": arn}).encode()
        assert self._arn("acm", body=body) == arn

    def test_ssm_parameter_name_from_json_body(self):
        body = json.dumps({"Name": "/app/db"}).encode()
        assert self._arn("ssm", body=body) == (
            "arn:aws:ssm:us-east-1:000000000000:parameter/app/db")

    @pytest.mark.parametrize("action", (
        "AddTagsToResource", "RemoveTagsFromResource", "ListTagsForResource",
    ))
    @pytest.mark.parametrize("resource_id", ("app/db", "/app/db"))
    def test_ssm_parameter_tags_use_resource_id(self, action, resource_id):
        from ministack.core.iam_actions import extract_resource_arn

        body = json.dumps({
            "ResourceType": "Parameter", "ResourceId": resource_id,
            "Name": "/wrong/parameter",
        }).encode()
        assert extract_resource_arn(
            "ssm", "POST", "/", {"x-amz-target": f"AmazonSSM.{action}"},
            body, {}, self.REGION, self.ACCOUNT,
        ) == "arn:aws:ssm:us-east-1:000000000000:parameter/app/db"

    def test_ssm_tag_arn_input_and_other_resource_types(self):
        from ministack.core.iam_actions import extract_resource_arn

        arn = "arn:aws:ssm:us-west-2:111111111111:parameter/app/db"
        headers = {"x-amz-target": "AmazonSSM.ListTagsForResource"}
        for local_alias in (
            "arn:aws:ssm:us-east-1:000000000000:parameter//app/db",
            "arn:aws:ssm:us-east-1:000000000000:parameterapp/db",
        ):
            assert extract_resource_arn(
                "ssm", "POST", "/", headers,
                json.dumps({"ResourceId": local_alias}).encode(),
                {}, self.REGION, self.ACCOUNT,
            ) == "arn:aws:ssm:us-east-1:000000000000:parameter/app/db"
        assert extract_resource_arn(
            "ssm", "POST", "/", headers,
            json.dumps({"ResourceType": "Parameter", "ResourceId": arn}).encode(),
            {}, self.REGION, self.ACCOUNT,
        ) == arn
        for foreign_arn in (
            "arn:aws:ssm:us-west-2:000000000000:parameter/app/db",
            "arn:aws:ssm:us-east-1:111111111111:parameter/app/db",
            "arn:aws:ssm:us-east-1:000000000000:document/app/db",
        ):
            assert extract_resource_arn(
                "ssm", "POST", "/", headers,
                json.dumps({"ResourceType": "Parameter", "ResourceId": foreign_arn}).encode(),
                {}, self.REGION, self.ACCOUNT,
            ) == foreign_arn
        assert extract_resource_arn(
            "ssm", "POST", "/", headers,
            json.dumps({
                "ResourceType": "Document", "ResourceId": "/app/db", "Name": "/app/db",
            }).encode(), {}, self.REGION, self.ACCOUNT,
        ) == "*"
        assert extract_resource_arn(
            "ssm", "POST", "/", headers,
            json.dumps({"ResourceType": "Parameter", "Name": "/app/db"}).encode(),
            {}, self.REGION, self.ACCOUNT,
        ) == "*"

    def test_cloudwatch_alarm_name_from_json_body(self):
        body = json.dumps({"AlarmName": "cpu-high"}).encode()
        assert self._arn("monitoring", body=body) == (
            "arn:aws:cloudwatch:us-east-1:000000000000:alarm:cpu-high")


@pytest.mark.parametrize("policy_mode", ("exact", "explicit-deny", "action-only"))
def test_ssm_parameter_tag_actions_enforce_exact_resource_and_preserve_denied_tags(
    monkeypatch, policy_mode,
):
    """Run the real ASGI route, IAM evaluator, and SSM tag handlers."""
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    from ministack import app as app_mod
    from ministack.services import iam as iam_svc
    from ministack.services import ssm as ssm_svc

    monkeypatch.setattr(app_mod, "AUTH", True)
    account, region = "123456789012", "us-east-1"
    key, user = "AKIASSMTAGAUTHCASE", "ssm-tag-auth-case"
    name_a, name_b = "/iam-ssm-tag/a", "/iam-ssm-tag/b"
    legacy_name = "iam-ssm-tag/legacy"
    arn_a = f"arn:aws:ssm:{region}:{account}:parameter/iam-ssm-tag/a"
    arn_b = f"arn:aws:ssm:{region}:{account}:parameter/iam-ssm-tag/b"
    legacy_arn = f"arn:aws:ssm:{region}:{account}:parameteriam-ssm-tag/legacy"
    canonical_legacy_arn = f"arn:aws:ssm:{region}:{account}:parameter/iam-ssm-tag/legacy"
    iam_svc._users.set_scoped(account, None, user, {"UserName": user, "AttachedPolicies": []})
    iam_svc._access_keys.set_scoped(account, None, key, {
        "AccessKeyId": key, "SecretAccessKey": "test-secret", "Status": "Active",
        "UserName": user,
    })
    for name, arn in ((name_a, arn_a), (name_b, arn_b), (legacy_name, legacy_arn)):
        ssm_svc._parameters.set_scoped(account, region, name, {"ARN": arn})
    ssm_svc._tags.set_scoped(account, region, arn_b, {"existing": "before"})
    ssm_svc._tags.set_scoped(account, region, legacy_arn, {"existing": "legacy"})

    def call(action, resource_id, *, with_headers=False, **extra):
        payload = {"ResourceType": "Parameter", "ResourceId": resource_id, **extra}
        body = json.dumps(payload).encode()
        sent = []

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message):
            sent.append(message)

        request = AWSRequest(method="POST", url=f"https://ssm.{region}.amazonaws.com/", data=body, headers={
            "x-amz-target": f"AmazonSSM.{action}", "content-type": "application/x-amz-json-1.1",
        })
        SigV4Auth(Credentials(key, "test-secret"), "ssm", region).add_auth(request)
        scope = {
            "type": "http", "method": "POST", "path": "/", "query_string": b"",
            "headers": [(b"host", b"ssm.us-east-1.amazonaws.com")] + [
                (name.lower().encode(), value.encode()) for name, value in request.headers.items()
            ],
        }
        asyncio.run(app_mod.app(scope, receive, send))
        response = sent[0]["status"], json.loads(sent[1]["body"])
        if with_headers:
            return *response, {name.lower(): value for name, value in sent[0]["headers"]}
        return response

    def policy(*statements):
        iam_svc._user_inline_policies.set_scoped(account, None, user, {
            "tag-access": {"Version": "2012-10-17", "Statement": list(statements)},
        })

    actions = ["ssm:AddTagsToResource", "ssm:RemoveTagsFromResource", "ssm:ListTagsForResource"]
    try:
        if policy_mode == "exact":
            policy({"Effect": "Allow", "Action": actions, "Resource": arn_a})
            assert call("AddTagsToResource", name_a, Tags=[{"Key": "team", "Value": "a"}])[0] == 200
            assert call("ListTagsForResource", "iam-ssm-tag/a") == (
                200, {"TagList": [{"Key": "team", "Value": "a"}]},
            )
            assert call("RemoveTagsFromResource", arn_a, TagKeys=["team"])[0] == 200
            assert call("ListTagsForResource", name_a) == (200, {"TagList": []})
            for action, extra in (
                ("AddTagsToResource", {"Tags": [{"Key": "new", "Value": "bad"}]}),
                ("RemoveTagsFromResource", {"TagKeys": ["existing"]}),
                ("ListTagsForResource", {}),
            ):
                status, denied, response_headers = call(
                    action, name_b, Name=name_a, with_headers=True, **extra,
                )
                assert status == 400 and denied["__type"] == "AccessDeniedException"
                assert f"on resource: {arn_b}" in denied["Message"]
                assert f"because no identity-based policy allows the ssm:{action} action" in denied["Message"]
                assert response_headers[b"content-type"] == b"application/x-amz-json-1.1"
                assert b"x-amzn-errortype" not in response_headers
                assert ssm_svc._tags.get_scoped(account, region, arn_b) == {"existing": "before"}
            for foreign_arn in (
                f"arn:aws:ssm:us-west-2:{account}:parameter/iam-ssm-tag/a",
                f"arn:aws:ssm:{region}:111111111111:parameter/iam-ssm-tag/a",
            ):
                status, denied = call("ListTagsForResource", foreign_arn)
                assert status == 400 and denied["__type"] == "AccessDeniedException"
            status, denied = call(
                "AddTagsToResource", name_a, ResourceType="Document",
                Tags=[{"Key": "new", "Value": "bad"}],
            )
            assert status == 400 and denied["__type"] == "AccessDeniedException"
            assert ssm_svc._tags.get_scoped(account, region, name_a) is None
        elif policy_mode == "explicit-deny":
            policy(
                {"Effect": "Allow", "Action": actions, "Resource": "*"},
                {"Effect": "Deny", "Action": actions, "Resource": arn_b},
            )
            for action, extra in (
                ("AddTagsToResource", {"Tags": [{"Key": "new", "Value": "bad"}]}),
                ("RemoveTagsFromResource", {"TagKeys": ["existing"]}),
                ("ListTagsForResource", {}),
            ):
                for resource_id in (
                    name_b, "iam-ssm-tag/b", arn_b,
                    f"arn:aws:ssm:{region}:{account}:parameter//iam-ssm-tag/b",
                ):
                    status, denied = call(action, resource_id, **extra)
                    assert status == 400 and denied["__type"] == "AccessDeniedException"
                    assert f"on resource: {arn_b}" in denied["Message"]
                    assert "with an explicit deny in an identity-based policy" in denied["Message"]
                    assert ssm_svc._tags.get_scoped(account, region, arn_b) == {"existing": "before"}
            policy(
                {"Effect": "Allow", "Action": actions, "Resource": "*"},
                {"Effect": "Deny", "Action": actions, "Resource": canonical_legacy_arn},
            )
            status, denied = call(
                "RemoveTagsFromResource", legacy_arn, TagKeys=["existing"],
            )
            assert status == 400 and denied["__type"] == "AccessDeniedException"
            assert ssm_svc._tags.get_scoped(account, region, legacy_arn) == {"existing": "legacy"}
        else:
            policy({"Effect": "Allow", "Action": "ssm:ListTagsForResource", "Resource": arn_a})
            assert call("ListTagsForResource", name_a) == (200, {"TagList": []})
            for action, extra in (
                ("AddTagsToResource", {"Tags": [{"Key": "new", "Value": "bad"}]}),
                ("RemoveTagsFromResource", {"TagKeys": ["existing"]}),
            ):
                status, denied = call(action, name_a, **extra)
                assert status == 400 and denied["__type"] == "AccessDeniedException"
                assert f"ssm:{action}" in denied["Message"]
            assert ssm_svc._tags.get_scoped(account, region, arn_a) is None
    finally:
        iam_svc._user_inline_policies.pop_scoped(account, None, user, None)
        iam_svc._access_keys.pop_scoped(account, None, key, None)
        iam_svc._users.pop_scoped(account, None, user, None)
        for name in (name_a, name_b, legacy_name):
            ssm_svc._parameters.pop_scoped(account, region, name, None)
        for arn in (arn_a, arn_b, legacy_arn):
            ssm_svc._tags.pop_scoped(account, region, arn, None)


def test_ssm_parameter_tag_authorization_is_scoped_to_request_account_and_region(monkeypatch):
    from botocore.auth import SigV4Auth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    from ministack import app as app_mod
    from ministack.services import iam as iam_svc
    from ministack.services import ssm as ssm_svc

    monkeypatch.setattr(app_mod, "AUTH", True)
    name = "/iam-ssm-tag/scoped"
    tenants = (
        ("123456789012", "us-east-1", "AKIASSMTAGEAST", "ssm-tag-east"),
        ("123456789012", "us-west-2", "AKIASSMTAGWEST", "ssm-tag-west"),
        ("210987654321", "us-east-1", "AKIASSMTAGOTHER", "ssm-tag-other"),
    )

    def arn(account, region):
        return f"arn:aws:ssm:{region}:{account}:parameter/iam-ssm-tag/scoped"

    def call(account, region, key, resource_id):
        body = json.dumps({
            "ResourceType": "Parameter", "ResourceId": resource_id,
            "Tags": [{"Key": "owner", "Value": key}],
        }).encode()
        sent = []

        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}

        async def send(message):
            sent.append(message)

        request = AWSRequest(method="POST", url=f"https://ssm.{region}.amazonaws.com/", data=body, headers={
            "x-amz-target": "AmazonSSM.AddTagsToResource", "content-type": "application/x-amz-json-1.1",
        })
        SigV4Auth(Credentials(key, "test-secret"), "ssm", region).add_auth(request)
        scope = {
            "type": "http", "method": "POST", "path": "/", "query_string": b"",
            "headers": [(b"host", f"ssm.{region}.amazonaws.com".encode())] + [
                (name.lower().encode(), value.encode()) for name, value in request.headers.items()
            ],
        }
        asyncio.run(app_mod.app(scope, receive, send))
        return sent[0]["status"]

    try:
        for account, region, key, user in tenants:
            parameter_arn = arn(account, region)
            iam_svc._users.set_scoped(account, None, user, {
                "UserName": user, "AttachedPolicies": [],
            })
            iam_svc._access_keys.set_scoped(account, None, key, {
                "AccessKeyId": key, "SecretAccessKey": "test-secret", "Status": "Active",
                "UserName": user,
            })
            iam_svc._user_inline_policies.set_scoped(account, None, user, {
                "tag-access": {"Statement": [{
                    "Effect": "Allow", "Action": "ssm:AddTagsToResource", "Resource": parameter_arn,
                }]},
            })
            ssm_svc._parameters.set_scoped(account, region, name, {"ARN": parameter_arn})
            ssm_svc._tags.set_scoped(account, region, parameter_arn, {"owner": "before"})

        for account, region, key, _ in tenants:
            assert call(account, region, key, name) == 200
            assert ssm_svc._tags.get_scoped(account, region, arn(account, region)) == {
                "owner": key,
            }

        for account, region, key, _ in tenants:
            for other_account, other_region, _, _ in tenants:
                if (account, region) == (other_account, other_region):
                    continue
                before = {
                    (a, r): ssm_svc._tags.get_scoped(a, r, arn(a, r)).copy()
                    for a, r, _, _ in tenants
                }
                assert call(account, region, key, arn(other_account, other_region)) == 400
                assert all(
                    ssm_svc._tags.get_scoped(a, r, arn(a, r)) == before[a, r]
                    for a, r, _, _ in tenants
                )
    finally:
        for account, region, key, user in tenants:
            iam_svc._user_inline_policies.pop_scoped(account, None, user, None)
            iam_svc._access_keys.pop_scoped(account, None, key, None)
            iam_svc._users.pop_scoped(account, None, user, None)
            ssm_svc._parameters.pop_scoped(account, region, name, None)
            ssm_svc._tags.pop_scoped(account, region, arn(account, region), None)


# ---------------------------------------------------------------------------
# CreateFunction with an unresolvable role (#1506)
# ---------------------------------------------------------------------------

def test_lambda_create_function_with_a_missing_role_answers_400(monkeypatch):
    """The role check must reach the client, not blow up inside _build_config."""
    import io
    import zipfile

    import ministack.app as app_mod
    from ministack.services import lambda_svc

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("index.js", "exports.handler=async()=>({})")

    monkeypatch.setattr(app_mod, "AUTH", True, raising=False)
    name = "iam-auth-missing-role-fn"
    status, _headers, body = lambda_svc._create_function({
        "FunctionName": name,
        "Runtime": "nodejs22.x",
        "Handler": "index.handler",
        "Role": "arn:aws:iam::000000000000:role/does-not-exist",
        "Code": {"ZipFile": __import__("base64").b64encode(buf.getvalue()).decode()},
    })
    assert status == 400
    payload = json.loads(body)
    assert payload["__type"] == "InvalidParameterValueException"
    assert "does-not-exist" in payload["message"]
    # Nothing may be persisted for a function that was refused.
    assert name not in lambda_svc._functions


def test_lambda_build_config_returns_a_config_not_an_error(monkeypatch):
    """_build_config is annotated -> dict; it must never return an error tuple."""
    import ministack.app as app_mod
    from ministack.services import lambda_svc

    monkeypatch.setattr(app_mod, "AUTH", True, raising=False)
    config = lambda_svc._build_config("f", {
        "Runtime": "nodejs22.x",
        "Handler": "index.handler",
        "Role": "arn:aws:iam::000000000000:role/does-not-exist",
    })
    assert isinstance(config, dict)
    assert config["FunctionName"] == "f"


def test_lambda_execution_credentials_resolve_to_configured_role(monkeypatch):
    """SDK calls from Lambda are evaluated against its execution role."""
    import ministack.app as app_mod
    from ministack.core.responses import _request_account_id
    from ministack.services import iam as iam_svc
    from ministack.services import lambda_svc
    from ministack.services import sts as sts_svc

    monkeypatch.setattr(app_mod, "AUTH", True, raising=False)
    token = _request_account_id.set("000000000000")
    role_name = "appointment-mark-canceled"
    events_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/events"
    snapshots_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/snapshots"
    iam_svc._roles[role_name] = {
        "AttachedPolicies": [],
        "InlinePolicies": {
            "events": json.dumps({
                "Statement": [{
                    "Effect": "Allow",
                    "Action": ["dynamodb:PutItem", "dynamodb:BatchWriteItem"],
                    "Resource": events_arn,
                }]
            })
        },
    }
    try:
        credentials = lambda_svc.execution_credentials({
            "FunctionName": "appointment-mark-canceled",
            "FunctionArn": (
                "arn:aws:lambda:us-east-1:000000000000:function:"
                "appointment-mark-canceled"
            ),
            "Role": f"arn:aws:iam::000000000000:role/{role_name}",
        })
        access_key = credentials["AWS_ACCESS_KEY_ID"]
        assert access_key.startswith("ASIA")
        assert credentials["AWS_SESSION_TOKEN"]
        assert sts_svc._sessions[access_key]["Arn"].startswith(
            f"arn:aws:sts::000000000000:assumed-role/{role_name}/"
        )
        assert enforce(
            access_key,
            "dynamodb:BatchWriteItem",
            "dynamodb",
            "us-east-1",
            resource_arn=events_arn,
        ) is None
        denied = enforce(
            access_key,
            "dynamodb:BatchWriteItem",
            "dynamodb",
            "us-east-1",
            resource_arn=snapshots_arn,
        )
        assert isinstance(denied, EvalResult)
        assert denied.decision == "ImplicitDeny"
    finally:
        iam_svc._roles.pop(role_name, None)
        sts_svc._sessions.clear()
        _request_account_id.reset(token)


def test_role_session_uses_account_from_session_arn():
    from ministack.core.iam_evaluator import resolve_principal
    from ministack.core.responses import _request_account_id
    from ministack.services import iam as iam_svc
    from ministack.services import sts as sts_svc

    role_name = "cross-account-request-context"
    account_id = "957398953894"
    token = _request_account_id.set(account_id)
    iam_svc._roles[role_name] = {
        "AttachedPolicies": [],
        "InlinePolicies": {"allow": json.dumps({"Statement": [{
            "Effect": "Allow", "Action": "states:StartExecution", "Resource": "*",
        }]})},
    }
    _request_account_id.reset(token)
    sts_svc._sessions["ASIASESSIONACCOUNT"] = {
        "Arn": f"arn:aws:sts::{account_id}:assumed-role/{role_name}/lambda",
        "SecretAccessKey": "test-session-secret",
    }
    try:
        principal = resolve_principal("ASIASESSIONACCOUNT", "000000000000")
        assert principal.account == account_id
        assert principal.policies
    finally:
        token = _request_account_id.set(account_id)
        iam_svc._roles.pop(role_name, None)
        _request_account_id.reset(token)
        sts_svc._sessions.clear()


def test_lambda_execution_role_explicit_deny_overrides_allow(monkeypatch):
    import ministack.app as app_mod
    from ministack.core.responses import _request_account_id
    from ministack.services import iam as iam_svc
    from ministack.services import lambda_svc
    from ministack.services import sts as sts_svc

    monkeypatch.setattr(app_mod, "AUTH", True, raising=False)
    token = _request_account_id.set("000000000000")
    role_name = "denied-writer"
    table_arn = "arn:aws:dynamodb:us-east-1:000000000000:table/events"
    iam_svc._roles[role_name] = {
        "AttachedPolicies": [],
        "InlinePolicies": {
            "allow-and-deny": json.dumps({
                "Statement": [
                    {"Effect": "Allow", "Action": "dynamodb:*", "Resource": "*"},
                    {
                        "Effect": "Deny",
                        "Action": "dynamodb:BatchWriteItem",
                        "Resource": table_arn,
                    },
                ]
            })
        },
    }
    try:
        access_key = lambda_svc.execution_credentials({
            "FunctionName": "denied-writer",
            "FunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:denied-writer",
            "Role": f"arn:aws:iam::000000000000:role/{role_name}",
        })["AWS_ACCESS_KEY_ID"]
        denied = enforce(
            access_key,
            "dynamodb:BatchWriteItem",
            "dynamodb",
            "us-east-1",
            resource_arn=table_arn,
        )
        assert isinstance(denied, EvalResult)
        assert denied.decision == "Deny"
    finally:
        iam_svc._roles.pop(role_name, None)
        sts_svc._sessions.clear()
        _request_account_id.reset(token)


# ---------------------------------------------------------------------------
# Access-key resolution (root / IAM user / STS session)
# ---------------------------------------------------------------------------


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
    from ministack.services import iam as iam_svc

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
    from ministack.services import sts as sts_svc

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
    from ministack.services import iam as iam_svc

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


def test_resolve_get_session_token_principal_retains_user_policies():
    from ministack.services import iam as iam_svc
    from ministack.services import sts as sts_svc

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


def test_ambiguous_iam_access_key_is_rejected_before_http_routing(monkeypatch):
    from ministack import app as app_mod
    from ministack.core.responses import get_account_id, set_request_account_id
    from ministack.services import iam as iam_svc

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
    from ministack.services import iam as iam_svc

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


# ---------------------------------------------------------------------------
# Credential checks over HTTP: boto3 clients whose transport is the app itself
# ---------------------------------------------------------------------------

class _Body(io.BytesIO):
    def stream(self, **_):
        yield self.getvalue()


def _asgi_send(request, extra_headers=(), **_):
    """Serve a botocore request with the in-process app instead of the network."""
    from urllib.parse import unquote, urlsplit

    from botocore.awsrequest import AWSResponse

    from ministack import app as app_mod

    url = urlsplit(request.url)
    body = request.body.read() if hasattr(request.body, "read") else request.body or b""
    body = body.encode() if isinstance(body, str) else body
    headers = {"host": url.netloc.encode()}
    headers.update((k.lower(), v if isinstance(v, bytes) else str(v).encode("latin-1"))
                   for k, v in request.headers.items())
    scope = {"type": "http", "method": request.method, "path": unquote(url.path) or "/",
             "raw_path": url.path.encode(), "query_string": url.query.encode(),
             "headers": [(k.encode("latin-1"), v) for k, v in headers.items()] + list(extra_headers)}
    sent = []

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message):
        sent.append(message)

    asyncio.run(app_mod.app(scope, receive, send))
    response_headers = {k.decode(): v.decode() for k, v in sent[0].get("headers", [])}
    return AWSResponse(request.url, sent[0]["status"], response_headers,
                       _Body(b"".join(m.get("body", b"") for m in sent[1:])))


def _local_client(service, key="test", secret="test", token=None, **s3_config):
    import boto3
    from botocore.config import Config

    client = boto3.client(
        service, endpoint_url="http://localhost:4566", region_name="us-east-1",
        aws_access_key_id=key, aws_secret_access_key=secret, aws_session_token=token,
        config=Config(retries={"max_attempts": 1}, s3={"addressing_style": "path", **s3_config}),
    )
    client.meta.events.register("before-send", _asgi_send)
    return client


def _failure(call):
    with pytest.raises(ClientError) as exc:
        call()
    return exc.value.response["Error"]["Code"], exc.value.response["ResponseMetadata"]["HTTPStatusCode"]


_LIST_CALLS = {
    "iam": lambda c: c.list_users(),
    "sqs": lambda c: c.list_queues(),
    "dynamodb": lambda c: c.list_tables(),
    "lambda": lambda c: c.list_functions(),
    "s3": lambda c: c.list_buckets(),
    "ec2": lambda c: c.describe_regions(),
    "ssm": lambda c: c.describe_parameters(),
    "logs": lambda c: c.describe_log_groups(),
}


class TestCredentialErrors:
    @pytest.mark.parametrize("service,code,status", [
        ("iam", "InvalidClientTokenId", 403),
        ("sqs", "InvalidClientTokenId", 403),
        ("dynamodb", "UnrecognizedClientException", 400),
        ("lambda", "UnrecognizedClientException", 403),
        ("s3", "InvalidAccessKeyId", 403),
        ("ec2", "AuthFailure", 401),
        ("ssm", "UnrecognizedClientException", 400),
        ("logs", "UnrecognizedClientException", 400),
    ])
    def test_unknown_key_answers_the_protocols_code(self, monkeypatch, service, code, status):
        from ministack import app as app_mod

        monkeypatch.setattr(app_mod, "AUTH", True)
        client = _local_client(service, "AKIAUNKNOWNKEY000001", "secret")
        assert _failure(lambda: _LIST_CALLS[service](client)) == (code, status)

    @pytest.mark.parametrize("service,content_type", [
        ("iam", "text/xml"),
        ("sqs", "application/x-amz-json-1.0"),
        ("dynamodb", "application/x-amz-json-1.0"),
        ("lambda", "application/json"),
        ("s3", "application/xml"),
        ("ec2", "text/xml;charset=UTF-8"),
        ("ssm", "application/x-amz-json-1.1"),
        ("logs", "application/x-amz-json-1.1"),
    ])
    def test_unknown_key_answers_the_protocols_content_type(self, monkeypatch, service, content_type):
        from ministack import app as app_mod

        monkeypatch.setattr(app_mod, "AUTH", True)
        client = _local_client(service, "AKIAUNKNOWNKEY000001", "secret")
        with pytest.raises(ClientError) as exc:
            _LIST_CALLS[service](client)
        assert exc.value.response["ResponseMetadata"]["HTTPHeaders"]["content-type"] == content_type

    def test_unknown_key_is_ignored_without_auth(self, monkeypatch):
        from ministack import app as app_mod

        monkeypatch.setattr(app_mod, "AUTH", False)
        client = _local_client("dynamodb", "AKIAUNKNOWNKEY000001", "secret")
        assert client.list_tables()["ResponseMetadata"]["HTTPStatusCode"] == 200

    @pytest.mark.parametrize("service,code,status", [
        ("iam", "ExpiredToken", 403),
        ("dynamodb", "ExpiredTokenException", 400),
        ("lambda", "ExpiredTokenException", 403),
        ("s3", "ExpiredToken", 400),
        ("ec2", "RequestExpired", 400),
    ])
    def test_expired_session_answers_the_protocols_code(self, monkeypatch, service, code, status):
        from ministack import app as app_mod
        from ministack.services import sts as sts_svc

        monkeypatch.setattr(app_mod, "AUTH", True)
        monkeypatch.setitem(sts_svc._sessions, "ASIAEXPIREDSESSION01", {
            "Arn": "arn:aws:iam::000000000000:user/alice", "UserId": "AIDAALICE",
            "SecretAccessKey": "session-secret", "SessionToken": "session-token",
            "Expiration": time.time() - 1, "AccountId": "000000000000", "PrincipalType": "User",
        })
        client = _local_client(service, "ASIAEXPIREDSESSION01", "session-secret", "session-token")
        assert _failure(lambda: _LIST_CALLS[service](client)) == (code, status)


@pytest.fixture
def user_key(monkeypatch):
    """An IAM user allowed everything, created through the app under AUTH=true."""
    from ministack import app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", True)
    iam = _local_client("iam")
    name = f"signer-{time.time_ns()}"
    iam.create_user(UserName=name)
    iam.put_user_policy(UserName=name, PolicyName="all", PolicyDocument=json.dumps(
        {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]}))
    key = iam.create_access_key(UserName=name)["AccessKey"]
    yield key["AccessKeyId"], key["SecretAccessKey"]
    iam.delete_access_key(UserName=name, AccessKeyId=key["AccessKeyId"])
    iam.delete_user_policy(UserName=name, PolicyName="all")
    iam.delete_user(UserName=name)


def _chunked_put(key, secret, url, data, *, bad_chunk=False):
    """A PutObject streamed as signed aws-chunked chunks, as the Java and Go SDKs send it."""
    import datetime as dt
    import hashlib
    import hmac

    from botocore.awsrequest import AWSRequest

    from ministack.core.sigv4 import derive_signing_key

    amz_date = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    scope = f"{amz_date[:8]}/us-east-1/s3/aws4_request"
    signing_key = derive_signing_key(secret, amz_date[:8], "us-east-1", "s3")
    chunks = [data, b""]
    headers = {
        "content-encoding": "aws-chunked", "host": "localhost:4566", "x-amz-date": amz_date,
        "x-amz-content-sha256": "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
        "x-amz-decoded-content-length": str(len(data)),
    }
    signed = ";".join(sorted(headers))
    path = url.split("localhost:4566", 1)[1]
    canonical = "\n".join(["PUT", path, "", "".join(f"{h}:{headers[h]}\n" for h in sorted(headers)), signed,
                           headers["x-amz-content-sha256"]])
    string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    previous = hmac.new(signing_key, string_to_sign.encode(), hashlib.sha256).hexdigest()
    headers["authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={key}/{scope}, SignedHeaders={signed}, Signature={previous}")
    body = b""
    for chunk in chunks:
        previous = hmac.new(signing_key, "\n".join([
            "AWS4-HMAC-SHA256-PAYLOAD", amz_date, scope, previous, hashlib.sha256(b"").hexdigest(),
            hashlib.sha256(chunk).hexdigest()]).encode(), hashlib.sha256).hexdigest()
        signature = "0" * 64 if bad_chunk and chunk else previous
        body += f"{len(chunk):x};chunk-signature={signature}\r\n".encode() + chunk + b"\r\n"
    return _asgi_send(AWSRequest(method="PUT", url=url, headers=headers, data=body).prepare())


class TestHeaderSignatures:
    @pytest.mark.parametrize("service,code,status", [
        ("iam", "SignatureDoesNotMatch", 403),
        ("sqs", "SignatureDoesNotMatch", 403),
        ("dynamodb", "InvalidSignatureException", 400),
        ("lambda", "InvalidSignatureException", 403),
        ("s3", "SignatureDoesNotMatch", 403),
        ("ec2", "AuthFailure", 401),
    ])
    def test_wrong_secret_answers_the_protocols_code(self, user_key, service, code, status):
        client = _local_client(service, user_key[0], "wrong-secret")
        assert _failure(lambda: _LIST_CALLS[service](client)) == (code, status)

    def test_get_caller_identity_checks_the_signature(self, user_key):
        client = _local_client("sts", user_key[0], "wrong-secret")
        assert _failure(client.get_caller_identity) == ("SignatureDoesNotMatch", 403)
        assert ":user/signer-" in _local_client("sts", *user_key).get_caller_identity()["Arn"]

    @pytest.mark.parametrize("service", sorted(_LIST_CALLS))
    def test_right_secret_passes(self, user_key, service):
        _LIST_CALLS[service](_local_client(service, *user_key))

    def test_paths_and_queries_sign_as_the_sdk_encodes_them(self, user_key):
        s3 = _local_client("s3", *user_key)
        bucket = f"signed-{time.time_ns()}"
        s3.create_bucket(Bucket=bucket)
        try:
            s3.put_object(Bucket=bucket, Key="dir/a b+c=ü.txt", Body=b"x")
            assert s3.get_object(Bucket=bucket, Key="dir/a b+c=ü.txt")["Body"].read() == b"x"
            assert s3.list_objects_v2(Bucket=bucket, Prefix="dir/a b")["KeyCount"] == 1
        finally:
            s3.delete_object(Bucket=bucket, Key="dir/a b+c=ü.txt")
            s3.delete_bucket(Bucket=bucket)
        lam = _local_client("lambda", *user_key)
        function = "arn:aws:lambda:us-east-1:000000000000:function:a b"
        assert _failure(lambda: lam.get_function(FunctionName=function)) != ("InvalidSignatureException", 403)
        lam.list_functions(MaxItems=5, FunctionVersion="ALL")

    @pytest.mark.parametrize("key", ["a%2Fb", "caf%c3%a9", "x%7Ey"])
    def test_s3_signs_the_path_as_sent(self, user_key, key):
        from botocore.auth import S3SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials

        s3 = _local_client("s3", *user_key)
        bucket = f"raw-path-{time.time_ns()}"
        s3.create_bucket(Bucket=bucket)
        try:
            request = AWSRequest(method="PUT", url=f"http://localhost:4566/{bucket}/{key}", data=b"x")
            S3SigV4Auth(Credentials(*user_key), "s3", "us-east-1").add_auth(request)
            assert _asgi_send(request.prepare()).status_code == 200
        finally:
            for item in s3.list_objects_v2(Bucket=bucket).get("Contents", []):
                s3.delete_object(Bucket=bucket, Key=item["Key"])
            s3.delete_bucket(Bucket=bucket)

    def test_pin_holds_no_root_key_request_and_ends_with_the_check(self, user_key):
        from ministack.core import iam_evaluator

        def pin(key):
            iam_evaluator.pin_signed_request("GET", "/", {}, [
                (b"host", b"localhost:4566"),
                (b"authorization", f"AWS4-HMAC-SHA256 Credential={key}/20260101/us-east-1/sts/aws4_request, "
                                   "SignedHeaders=host, Signature=0".encode()),
            ])
            return iam_evaluator._signed_request.get()

        assert pin("test") is None
        assert pin(user_key[0]) is not None
        assert isinstance(iam_evaluator.authenticate(user_key[0], "000000000000"), iam_evaluator.AuthError)
        assert iam_evaluator._signed_request.get() is None

    def test_request_changed_after_signing_is_refused(self, user_key):
        client = _local_client("dynamodb", *user_key)
        client.meta.events.register_first(
            "before-send", lambda request, **_: setattr(request, "body", b'{"Limit": 2}'))
        assert _failure(lambda: client.list_tables(Limit=1)) == ("InvalidSignatureException", 400)

    @pytest.mark.parametrize("service,code,status", [
        ("iam", "IncompleteSignature", 400),
        ("dynamodb", "IncompleteSignatureException", 400),
        ("lambda", "MissingAuthenticationTokenException", 403),
        ("s3", "AuthorizationHeaderMalformed", 400),
    ])
    def test_incomplete_authorization_header_is_refused(self, user_key, service, code, status):
        client = _local_client(service, *user_key)

        def drop_signature(request, **_):
            request.headers["Authorization"] = request.headers["Authorization"].split(b", Signature=")[0]

        client.meta.events.register_first("before-send", drop_signature)
        assert _failure(lambda: _LIST_CALLS[service](client)) == (code, status)

    @pytest.mark.parametrize("service,code,status", [
        ("iam", "SignatureDoesNotMatch", 403),
        ("dynamodb", "InvalidSignatureException", 400),
        ("s3", "AuthorizationHeaderMalformed", 400),
    ])
    def test_credential_date_must_match_the_request_date(self, user_key, service, code, status):
        client = _local_client(service, *user_key)

        def move_scope_date(request, **_):
            request.headers["Authorization"] = re.sub(
                rb"/\d{8}/", b"/20200101/", request.headers["Authorization"], count=1)

        client.meta.events.register_first("before-send", move_scope_date)
        assert _failure(lambda: _LIST_CALLS[service](client)) == (code, status)

    def test_header_sent_on_two_lines_signs_as_its_comma_joined_value(self, user_key):
        from botocore.auth import S3SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials

        s3 = _local_client("s3", *user_key)
        bucket = f"lines-{time.time_ns()}"
        s3.create_bucket(Bucket=bucket)
        try:
            request = AWSRequest(method="PUT", url=f"http://localhost:4566/{bucket}/k", data=b"x",
                                 headers={"x-amz-meta-list": "a,b"})
            S3SigV4Auth(Credentials(*user_key), "s3", "us-east-1").add_auth(request)
            del request.headers["x-amz-meta-list"]
            lines = [(b"x-amz-meta-list", b"a"), (b"x-amz-meta-list", b"b")]
            assert _asgi_send(request.prepare(), lines).status_code == 200
        finally:
            s3.delete_object(Bucket=bucket, Key="k")
            s3.delete_bucket(Bucket=bucket)

    def test_s3_unsigned_and_chunked_payloads(self, user_key):
        s3 = _local_client("s3", *user_key)
        bucket = f"payloads-{time.time_ns()}"
        s3.create_bucket(Bucket=bucket)
        try:
            unsigned = _local_client("s3", *user_key, payload_signing_enabled=False)
            unsigned.put_object(Bucket=bucket, Key="unsigned", Body=b"abc")
            wrong = _local_client("s3", user_key[0], "wrong-secret", payload_signing_enabled=False)
            assert _failure(lambda: wrong.put_object(Bucket=bucket, Key="unsigned", Body=b"abc")) == (
                "SignatureDoesNotMatch", 403)
            url = f"http://localhost:4566/{bucket}/chunked"
            assert _chunked_put(*user_key, url, b"hello chunks").status_code == 200
            assert s3.get_object(Bucket=bucket, Key="chunked")["Body"].read() == b"hello chunks"
            assert _chunked_put(user_key[0], "wrong-secret", url, b"x").status_code == 403
            assert _chunked_put(*user_key, url, b"x", bad_chunk=True).status_code == 403
        finally:
            for name in ("unsigned", "chunked"):
                s3.delete_object(Bucket=bucket, Key=name)
            s3.delete_bucket(Bucket=bucket)

    def test_root_key_signs_with_any_secret(self, user_key):
        assert _local_client("dynamodb", "test", "anything").list_tables()["TableNames"] is not None

    def test_signature_is_not_checked_without_auth(self, user_key, monkeypatch):
        from ministack import app as app_mod

        monkeypatch.setattr(app_mod, "AUTH", False)
        _local_client("dynamodb", user_key[0], "wrong-secret").list_tables()


def _presigned(key, secret, service, query, token=None, edit=None):
    """A GET presigned in its query string, as botocore's SigV4QueryAuth signs it: (code or None, status, body)."""
    from botocore.auth import SigV4QueryAuth
    from botocore.awsrequest import AWSRequest
    from botocore.credentials import Credentials

    request = AWSRequest(method="GET", url=f"http://localhost:4566/?{query}")
    SigV4QueryAuth(Credentials(key, secret, token), service, "us-east-1", 300).add_auth(request)
    url = edit(request.url) if edit else request.url
    response = _asgi_send(AWSRequest(method="GET", url=url).prepare())
    body = response.content.decode()
    code = re.search(r"<Code>([^<]*)</Code>", body) or re.search(r'"__type": "([^"]*)"', body)
    return (code.group(1) if code else None), response.status_code, body


_LIST_USERS = "Action=ListUsers&Version=2010-05-08"
_LIST_QUEUES = "Action=ListQueues&Version=2012-11-05"


def _drop(name):
    return lambda url: re.sub(rf"&?{name}=[^&]*", "", url)


class TestQuerySignatures:
    def test_right_secret_passes(self, user_key):
        assert _presigned(*user_key, "iam", _LIST_USERS)[:2] == (None, 200)

    @pytest.mark.parametrize("service,query", [("iam", _LIST_USERS), ("sqs", _LIST_QUEUES)])
    def test_wrong_or_forged_signature_is_refused(self, user_key, service, query):
        assert _presigned(user_key[0], "wrong-secret", service, query)[:2] == ("SignatureDoesNotMatch", 403)
        forged = _presigned(*user_key, service, query,
                            edit=lambda url: re.sub(r"X-Amz-Signature=\w+", "X-Amz-Signature=" + "0" * 64, url))
        assert forged[:2] == ("SignatureDoesNotMatch", 403)

    def test_query_mode_sqs_failure_is_sqs_query_xml(self, user_key):
        code, status, body = _presigned(user_key[0], "wrong-secret", "sqs", _LIST_QUEUES)
        assert (code, status) == ("SignatureDoesNotMatch", 403)
        assert body.startswith('<?xml version="1.0"?><ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/">'
                               "<Error><Type>Sender</Type><Code>SignatureDoesNotMatch</Code>")
        assert "</Message><Detail/></Error><RequestId>" in body
        assert _failure(lambda: _local_client("sqs", user_key[0], "wrong-secret").list_queues()) == (
            "SignatureDoesNotMatch", 403)

    def test_session_key_needs_its_token_and_signature(self, user_key):
        session = _local_client("sts", *user_key).get_session_token()["Credentials"]
        key, secret, token = session["AccessKeyId"], session["SecretAccessKey"], session["SessionToken"]
        assert _presigned(key, secret, "sqs", _LIST_QUEUES, token)[:2] == (None, 200)
        assert _presigned(key, secret, "sqs", _LIST_QUEUES)[:2] == ("InvalidClientTokenId", 403)
        forged = _presigned(key, secret, "sqs", _LIST_QUEUES, token,
                            edit=lambda url: re.sub(r"X-Amz-Signature=\w+", "X-Amz-Signature=" + "0" * 64, url))
        assert forged[:2] == ("SignatureDoesNotMatch", 403)

    def test_signature_outside_the_time_window_is_refused(self, user_key):
        def to_2020(url):
            url = re.sub(r"X-Amz-Date=\d{8}T\d{6}Z", "X-Amz-Date=20200101T000000Z", url)
            return re.sub(r"%2F\d{8}%2F", "%2F20200101%2F", url)

        code, status, body = _presigned(*user_key, "iam", _LIST_USERS, edit=to_2020)
        assert (code, status) == ("SignatureDoesNotMatch", 403)
        assert "Signature expired: 20200101T000000Z is now earlier than " in body

    @pytest.mark.parametrize("name", ["X-Amz-Date", "X-Amz-SignedHeaders", "X-Amz-Signature"])
    def test_missing_query_parameter_is_refused(self, user_key, name):
        code, status, body = _presigned(*user_key, "sqs", _LIST_QUEUES, edit=_drop(name))
        assert (code, status) == ("IncompleteSignature", 400)
        assert (f"AWS query-string parameters must include &apos;{name}&apos;. "
                "Re-examine the query-string parameters.") in body or (
            f"AWS query-string parameters must include '{name}'. Re-examine the query-string parameters.") in body

    def test_credential_must_have_five_elements(self, user_key):
        code, status, body = _presigned(
            *user_key, "sqs", _LIST_QUEUES,
            edit=lambda url: re.sub(r"X-Amz-Credential=([^%]+)%2F[^&]*", r"X-Amz-Credential=\1%2Fus-east-1", url))
        assert (code, status) == ("IncompleteSignature", 400)
        assert "Credential must have exactly 5 slash-delimited elements" in body

    def test_root_key_signs_with_any_secret(self, user_key):
        assert _presigned("test", "anything", "iam", _LIST_USERS)[:2] == (None, 200)

    def test_signature_is_not_checked_without_auth(self, user_key, monkeypatch):
        from ministack import app as app_mod

        monkeypatch.setattr(app_mod, "AUTH", False)
        assert _presigned(user_key[0], "wrong-secret", "iam", _LIST_USERS)[:2] == (None, 200)


class TestSessionTokens:
    @pytest.fixture
    def session(self, user_key):
        return _local_client("sts", *user_key).get_session_token()["Credentials"]

    @pytest.mark.parametrize("service,missing,wrong", [
        ("sqs", ("InvalidClientTokenId", 403), ("InvalidClientTokenId", 403)),
        ("dynamodb", ("UnrecognizedClientException", 400), ("UnrecognizedClientException", 400)),
        ("lambda", ("UnrecognizedClientException", 403), ("UnrecognizedClientException", 403)),
        ("s3", ("InvalidAccessKeyId", 403), ("InvalidToken", 400)),
        ("ec2", ("AuthFailure", 401), ("AuthFailure", 401)),
    ])
    def test_session_key_needs_its_own_token(self, session, service, missing, wrong):
        key, secret, token = session["AccessKeyId"], session["SecretAccessKey"], session["SessionToken"]
        _LIST_CALLS[service](_local_client(service, key, secret, token))
        assert _failure(lambda: _LIST_CALLS[service](_local_client(service, key, secret))) == missing
        assert _failure(lambda: _LIST_CALLS[service](_local_client(service, key, secret, token + "x"))) == wrong

    def test_wrong_token_message_has_no_full_stop(self, session):
        client = _local_client("dynamodb", session["AccessKeyId"], session["SecretAccessKey"], "other")
        with pytest.raises(ClientError) as exc:
            client.list_tables()
        assert exc.value.response["Error"]["Message"] == "The security token included in the request is invalid"

    def test_long_term_key_refuses_a_token(self, user_key, session):
        client = _local_client("iam", *user_key, session["SessionToken"])
        assert _failure(client.list_users) == ("InvalidClientTokenId", 403)

    def test_tokens_are_not_checked_without_auth(self, session, monkeypatch):
        from ministack import app as app_mod

        monkeypatch.setattr(app_mod, "AUTH", False)
        _local_client("dynamodb", session["AccessKeyId"], session["SecretAccessKey"]).list_tables()


@contextlib.contextmanager
def _signing_clock(seconds):
    """Sign as if the client clock ran ``seconds`` ahead."""
    import datetime as dt
    from unittest import mock

    import botocore.auth

    now = botocore.auth.get_current_datetime() + dt.timedelta(seconds=seconds)
    with mock.patch.object(botocore.auth, "get_current_datetime", return_value=now):
        yield


class TestClockSkew:
    @pytest.mark.parametrize("service,minutes,code,status", [
        ("sts", 15, "SignatureDoesNotMatch", 403),
        ("dynamodb", 15, "InvalidSignatureException", 400),
        ("s3", 15, "RequestTimeTooSkewed", 403),
        ("lambda", 5, "InvalidSignatureException", 403),
        ("ec2", 5, "AuthFailure", 401),
    ])
    def test_request_outside_the_services_window_is_refused(self, user_key, service, minutes, code, status):
        call = _LIST_CALLS.get(service) or (lambda c: c.get_caller_identity())
        client = _local_client(service, *user_key)
        with _signing_clock(minutes * 60 - 60):
            call(client)
        with _signing_clock(-(minutes * 60 - 60)):
            call(client)
        with _signing_clock(minutes * 60 + 60):
            assert _failure(lambda: call(client)) == (code, status)
        with _signing_clock(-(minutes * 60 + 60)):
            assert _failure(lambda: call(client)) == (code, status)

    def test_message_names_both_times(self, user_key):
        with _signing_clock(-1200), pytest.raises(ClientError) as exc:
            _local_client("dynamodb", *user_key).list_tables()
        assert re.fullmatch(r"Signature expired: \d{8}T\d{6}Z is now earlier than \d{8}T\d{6}Z "
                            r"\(\d{8}T\d{6}Z - 15 min\.\)", exc.value.response["Error"]["Message"])

    def test_skew_is_reported_before_an_unknown_key(self, user_key):
        with _signing_clock(1200):
            client = _local_client("dynamodb", "AKIAUNKNOWNKEY000001", "secret")
            assert _failure(client.list_tables) == ("InvalidSignatureException", 400)

    def test_root_key_and_auth_off_ignore_the_clock(self, user_key, monkeypatch):
        from ministack import app as app_mod

        with _signing_clock(1200):
            _local_client("dynamodb").list_tables()
            monkeypatch.setattr(app_mod, "AUTH", False)
            _local_client("dynamodb", *user_key).list_tables()


class TestExecuteApiAuthentication:
    @pytest.fixture
    def invoke(self, user_key):
        from botocore.auth import SigV4Auth
        from botocore.awsrequest import AWSRequest
        from botocore.credentials import Credentials

        apigw = _local_client("apigateway")
        api = apigw.create_rest_api(name="iam-auth")["id"]
        root = apigw.get_resources(restApiId=api)["items"][0]["id"]
        method = {"restApiId": api, "resourceId": root, "httpMethod": "GET"}
        apigw.put_method(**method, authorizationType="AWS_IAM")
        apigw.put_integration(**method, type="MOCK", requestTemplates={"application/json": '{"statusCode": 200}'})
        apigw.put_method_response(**method, statusCode="200")
        apigw.put_integration_response(**method, statusCode="200", responseTemplates={"application/json": '{"ok": 1}'})
        for response_type in ("INVALID_SIGNATURE", "EXPIRED_TOKEN", "DEFAULT_4XX"):
            apigw.put_gateway_response(restApiId=api, responseType=response_type, responseParameters={
                "gatewayresponse.header.x-type": f"'{response_type}'"})
        apigw.create_deployment(restApiId=api, stageName="p")

        def call(key, secret, seconds=0, authorization=None, token=None):
            request = AWSRequest(method="GET", url=f"http://{api}.execute-api.localhost:4566/p/")
            with _signing_clock(seconds):
                SigV4Auth(Credentials(key, secret, token), "execute-api", "us-east-1").add_auth(request)
            if authorization:
                request.headers.replace_header("Authorization", authorization(request.headers["Authorization"]))
            response = _asgi_send(request.prepare())
            return (response.status_code, response.headers.get("x-type"), response.headers.get("x-amzn-ErrorType"),
                    json.loads(response.content).get("message"))

        yield call
        apigw.delete_rest_api(restApiId=api)

    def test_aws_iam_failures_answer_through_gateway_responses(self, user_key, invoke):
        assert invoke(*user_key)[0] == 200
        assert invoke(user_key[0], "wrong-secret")[:3] == (403, "INVALID_SIGNATURE", "InvalidSignatureException")
        status, response_type, error_type, message = invoke(*user_key, seconds=-1200)
        assert (status, response_type, error_type) == (403, "INVALID_SIGNATURE", "InvalidSignatureException")
        assert message.startswith("Signature expired: ") and message.endswith(" - 5 min.)")
        assert invoke("AKIAUNKNOWNKEY000001", "secret") == (
            403, "DEFAULT_4XX", "UnrecognizedClientException", "The security token included in the request is invalid.")
        assert invoke(*user_key, authorization=lambda value: value.split(", Signature=")[0])[:3] == (
            403, "DEFAULT_4XX", "IncompleteSignatureException")
        assert invoke(*user_key, authorization=lambda value: re.sub(r"/\d{8}/", "/20200101/", value, count=1))[:3] == (
            403, "INVALID_SIGNATURE", "InvalidSignatureException")

    def test_expired_session_answers_through_expired_token(self, invoke, monkeypatch):
        from ministack.services import sts as sts_svc

        monkeypatch.setitem(sts_svc._sessions, "ASIAEXPIREDSESSION01", {
            "Arn": "arn:aws:iam::000000000000:user/alice", "UserId": "AIDAALICE",
            "SecretAccessKey": "session-secret", "SessionToken": "session-token",
            "Expiration": time.time() - 1, "AccountId": "000000000000", "PrincipalType": "User",
        })
        assert invoke("ASIAEXPIREDSESSION01", "session-secret", token="session-token") == (
            403, "EXPIRED_TOKEN", "ExpiredTokenException", "The security token included in the request is expired")

    def test_root_key_and_auth_off_are_unchanged(self, user_key, invoke, monkeypatch):
        from ministack import app as app_mod

        assert invoke("test", "anything")[0] == 200
        monkeypatch.setattr(app_mod, "AUTH", False)
        assert invoke(user_key[0], "wrong-secret")[0] == 200


# ---------------------------------------------------------------------------
# AssumeRole trust policy: matching denies and STS request conditions (AUTH=true)
# ---------------------------------------------------------------------------

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


def _tags(protocol, **tags):
    if protocol == "json":
        return {"Tags": [{"Key": key, "Value": value} for key, value in tags.items()]}
    return {f"Tags.member.{n}.{field}": text for n, (key, value) in enumerate(tags.items(), 1)
            for field, text in (("Key", key), ("Value", value))}


def test_trust_conditions_see_the_caller_principal_tags(trust_api):
    call, _, _ = trust_api
    _install_policy(trust_api, "CreateRole", _document(dict(
        ALLOW, Condition={"StringEquals": {"aws:PrincipalTag/team": "blue"}})))
    _assume(trust_api, 403, RoleSessionName="untagged")
    assert call("iam", "TagUser", UserName="trust-deployer", **_tags("query", team="blue"))[0] == 200
    _assume(trust_api, 200, RoleSessionName="tagged")


@pytest.mark.parametrize("actions, tagged, expected", [
    ("sts:AssumeRole", False, 200),
    ("sts:AssumeRole", True, 403),
    (["sts:AssumeRole", "sts:TagSession"], True, 200),
])
def test_trust_session_tags_need_tag_session(trust_api, actions, tagged, expected):
    call, sessions, protocol = trust_api
    _install_policy(trust_api, "CreateRole", _document(dict(ALLOW, Action=actions)))
    status, _, body = call("sts", "AssumeRole", RoleArn=ROLE_ARN, RoleSessionName="tags",
                           **(_tags(protocol, team="blue") if tagged else {}))
    assert status == expected, body
    if expected == 403:
        assert f"{CALLER} is not authorized to perform: sts:TagSession on resource: {ROLE_ARN}".encode() in body
    elif tagged:
        assert [s["SessionTags"] for s in sessions.values()] == [[{"Key": "team", "Value": "blue"}]]


@pytest.mark.parametrize("key, expected", [("team", 200), ("dept", 403)])
def test_trust_conditions_see_session_tags_as_request_tags(trust_api, key, expected):
    _, _, protocol = trust_api
    _install_policy(trust_api, "CreateRole", _document(ALLOW, dict(
        ALLOW, Action="sts:TagSession", Condition={"StringLike": {"aws:RequestTag/team": "*"}})))
    call, _, _ = trust_api
    status, _, body = call("sts", "AssumeRole", RoleArn=ROLE_ARN, RoleSessionName="tags",
                           **_tags(protocol, **{key: "blue"}))
    assert status == expected, body


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


@pytest.mark.parametrize("service, action", [("iam", "iam:ListUsers"), ("sqs", "sqs:ListQueues"), ("sns", "sns:ListTopics")])
@pytest.mark.parametrize("explicit_deny, reason", [
    (True, "with an explicit deny in an identity-based policy"),
    (False, "because no identity-based policy allows the {action} action"),
])
def test_access_denied_message_names_an_explicit_deny(service, action, explicit_deny, reason):
    from ministack.core.iam_actions import access_denied_response

    _status, _headers, body = access_denied_response(
        service, action, "arn:aws:iam::000000000000:user/u", "req-1", explicit_deny=explicit_deny)
    expected = f"User: arn:aws:iam::000000000000:user/u is not authorized to perform: {action} "
    assert (expected + reason.format(action=action)) in body.decode()


# ── API Gateway management actions and resource paths ──


@pytest.mark.parametrize("path", ["/restapis/api/stages/prod", "/v2/apis/api/stages/prod"])
@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE"])
def test_management_actions_use_http_verbs(path, method):
    # Neither query-protocol parameters nor JSON-protocol headers may override
    # the action of this REST-only service.
    assert extract_iam_action(
        "apigateway", method, path,
        {"x-amz-target": "ApiGateway.GetStage"}, b"", {"Action": "GetStage"},
    ) == f"apigateway:{method}"


@pytest.mark.parametrize("path, resource", [
    ("/restapis", "/restapis"),
    ("/restapis/api", "/restapis/api"),
    ("/restapis/api/stages", "/restapis/api/stages"),
    ("/restapis/api/stages/prod", "/restapis/api/stages/prod"),
    ("/restapis/api/stages/prod/", "/restapis/api/stages/prod"),
    ("/restapis/api/stages/", "/restapis/api/stages"),
    ("/restapis/api/resources/root/methods/GET/integration", "/restapis/api/resources/root/methods/GET/integration"),
    ("/v2/apis", "/apis"),
    ("/v2/apis/api", "/apis/api"),
    ("/v2/apis/api/stages", "/apis/api/stages"),
    ("/v2/apis/api/stages/", "/apis/api/stages"),
    ("/v2/apis/api/stages/prod/", "/apis/api/stages/prod"),
    ("/v2/apis/api/stages/%24default", "/apis/api/stages/$default"),
    ("/v2/apis/api/stages/%24default/", "/apis/api/stages/$default"),
    ("/v2/apis/api/routes/route", "/apis/api/routes/route"),
    ("/domainnames/example.com/basepathmappings", "/domainnames/example.com/basepathmappings"),
    ("/v2/domainnames/example.com/apimappings", "/domainnames/example.com/apimappings"),
    ("/account", "/account"),
    ("/tags/arn:aws:apigateway:us-east-2::/restapis/api",
     "/tags/arn%3Aaws%3Aapigateway%3Aus-east-2%3A%3A%2Frestapis%2Fapi"),
    ("/tags/arn%3Aaws%3Aapigateway%3Aus-east-2%3A%3A%2Frestapis%2Fapi",
     "/tags/arn%3Aaws%3Aapigateway%3Aus-east-2%3A%3A%2Frestapis%2Fapi"),
    ("/v2/tags/arn:aws:apigateway:us-east-2::/apis/api",
     "/tags/arn%3Aaws%3Aapigateway%3Aus-east-2%3A%3A%2Fapis%2Fapi"),
])
def test_management_resource_preserves_path(path, resource):
    assert extract_resource_arn(
        "apigateway", "GET", path, {}, b"", {}, "us-east-2", "000000000000",
    ) == f"arn:aws:apigateway:us-east-2::{resource}"


@pytest.mark.parametrize("prefix", ["/restapis/api", "/v2/apis/api"])
@pytest.mark.parametrize("suffix", ["", "/"])
@pytest.mark.parametrize("case, method, stage, expected", [
    ("stage-grant", "GET", "prod", 200),
    ("stage-grant", "PATCH", "prod", 200),
    ("stage-grant", "GET", "dev", 403),
    ("parent-grant", "GET", "prod", 403),
    ("operation-grant", "GET", "prod", 403),
    ("stage-deny", "GET", "prod", 403),
    ("stage-deny", "GET", "dev", 200),
])
def test_stage_policy_is_enforced_on_dispatch(monkeypatch, prefix, suffix, case, method, stage, expected):
    import ministack.app as app
    from ministack.core import iam_evaluator

    parent = f"arn:aws:apigateway:us-east-2::{prefix.removeprefix('/v2')}"
    grants = {
        "stage-grant": [{"Effect": "Allow", "Action": ["apigateway:GET", "apigateway:PATCH"],
                         "Resource": f"{parent}/stages/prod"}],
        "parent-grant": [{"Effect": "Allow", "Action": "apigateway:GET", "Resource": parent}],
        "operation-grant": [{"Effect": "Allow", "Action": "apigateway:GetStage", "Resource": "*"}],
        "stage-deny": [
            {"Effect": "Allow", "Action": "apigateway:*", "Resource": f"{parent}/*"},
            {"Effect": "Deny", "Action": "apigateway:GET", "Resource": f"{parent}/stages/prod"},
        ],
    }
    statements = parse_policy_document({"Statement": grants[case]})
    dispatched = []

    def enforce_policy(access_key, action, service, region, resource_arn="*", service_context=None):
        result = evaluate(EvalContext(
            principal_arn="arn:aws:iam::000000000000:user/caller",
            principal_type="User", principal_account="000000000000",
            action=action, resource_arn=resource_arn, region=region,
        ), [statements])
        return None if result.decision == "Allow" else result

    async def handler(*args):
        dispatched.append(args)
        return 200, {}, b"{}"

    monkeypatch.setattr(app, "AUTH", True)
    monkeypatch.setattr(iam_evaluator, "enforce", enforce_policy)
    monkeypatch.setattr(iam_evaluator, "pin_request_caller", lambda *args: None)
    monkeypatch.setitem(app.SERVICE_HANDLERS, "apigateway", handler)
    headers = {"authorization": "AWS4-HMAC-SHA256 Credential=test/20261006/us-east-2/apigateway/aws4_request"}
    status, _, body = asyncio.run(app._dispatch_service_request(
        method, f"{prefix}/stages/{stage}{suffix}", headers, b"", {}, "request-stage",
    ))
    assert status == expected
    assert bool(dispatched) == (expected == 200)
    if expected == 403:
        assert f"apigateway:{method}" in json.loads(body)["message"]


@pytest.mark.parametrize("method,path,action", [
    ("POST", "/v20180820/async-requests/mrap/create", "s3:CreateMultiRegionAccessPoint"),
    ("POST", "/v20180820/async-requests/mrap/delete", "s3:DeleteMultiRegionAccessPoint"),
    ("POST", "/v20180820/async-requests/mrap/put-policy", "s3:PutMultiRegionAccessPointPolicy"),
    ("GET", "/v20180820/async-requests/mrap/arn%3Aaws%3As3%3Aus-west-2%3A000000000000%3Aasync-request/mrap/create/t",
     "s3:DescribeMultiRegionAccessPointOperation"),
    ("GET", "/v20180820/mrap/instances", "s3:ListMultiRegionAccessPoints"),
    ("GET", "/v20180820/mrap/instances/m1", "s3:GetMultiRegionAccessPoint"),
    ("GET", "/v20180820/mrap/instances/m1/policy", "s3:GetMultiRegionAccessPointPolicy"),
    ("GET", "/v20180820/mrap/instances/arn%3Aaws%3As3%3A%3A000000000000%3Aaccesspoint/a.mrap/routes",
     "s3:GetMultiRegionAccessPointRoutes"),
    ("PATCH", "/v20180820/mrap/instances/arn%3Aaws%3As3%3A%3A000000000000%3Aaccesspoint/a.mrap/routes",
     "s3:SubmitMultiRegionAccessPointRoutes"),
])
def test_s3control_mrap_iam_action(method, path, action):
    from ministack.core.iam_actions import s3control_action

    assert s3control_action(method, path) == action


def test_s3control_mrap_denied_without_the_action(monkeypatch):
    """Under AUTH=true an S3 Control MRAP call needs its own IAM action."""
    import ministack.app as app
    from ministack.services import iam as iam_svc

    fake_key = "AKIATESTMRAPCTL0001"
    iam_svc._access_keys[fake_key] = {
        "UserName": "mrap-user", "AccessKeyId": fake_key,
        "SecretAccessKey": "s", "Status": "Active", "CreateDate": "2024-01-01",
    }
    iam_svc._users["mrap-user"] = {
        "UserName": "mrap-user", "Arn": "arn:aws:iam::000000000000:user/mrap-user",
        "UserId": "AIDAMRAP1", "CreateDate": "2024-01-01", "Path": "/",
        "AttachedPolicies": [], "Tags": [],
    }
    iam_svc._user_inline_policies["mrap-user"] = {"p": json.dumps({"Statement": [{
        "Effect": "Allow", "Action": "s3:ListMultiRegionAccessPoints", "Resource": "*"}]})}
    monkeypatch.setattr(app, "AUTH", True)
    headers = {
        "authorization": f"AWS4-HMAC-SHA256 Credential={fake_key}/20261012/us-west-2/s3/aws4_request, "
                         "SignedHeaders=host, Signature=deadbeef",
        "x-amz-account-id": "000000000000",
    }
    try:
        listed = asyncio.run(app._handle_s3_control_request(
            "/v20180820/mrap/instances", "GET", b"", {}, "rid", headers))
        denied = asyncio.run(app._handle_s3_control_request(
            "/v20180820/mrap/instances/nope", "GET", b"", {}, "rid", headers))
    finally:
        iam_svc._access_keys.pop(fake_key, None)
        iam_svc._users.pop("mrap-user", None)
        iam_svc._user_inline_policies.pop("mrap-user", None)
    assert listed[0] == 200
    assert denied[0] == 403
    assert b"s3:GetMultiRegionAccessPoint" in denied[2]


@pytest.mark.parametrize("addressing", ["path", "virtual"])
def test_s3_list_grants_scoped_by_prefix_condition(monkeypatch, addressing):
    """s3:prefix, s3:delimiter and s3:max-keys come from a ListBucket or ListBucketVersions request."""
    from ministack import app as app_mod

    monkeypatch.setattr(app_mod, "AUTH", True)
    iam, root_s3 = _local_client("iam"), _local_client("s3")
    name = f"prefix-{time.time_ns()}"
    bucket = name
    root_s3.create_bucket(Bucket=bucket)
    iam.create_user(UserName=name)
    iam.put_user_policy(UserName=name, PolicyName="home", PolicyDocument=json.dumps({
        "Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": ["s3:ListBucket", "s3:ListBucketVersions"],
            "Resource": f"arn:aws:s3:::{bucket}",
            "Condition": {"StringLike": {"s3:prefix": ["home/alice/*"]}, "StringEquals": {"s3:delimiter": "/"},
                          "NumericLessThanEquals": {"s3:max-keys": "100"}},
        }]}))
    key = iam.create_access_key(UserName=name)["AccessKey"]
    try:
        s3 = _local_client("s3", key["AccessKeyId"], key["SecretAccessKey"], addressing_style=addressing)
        allowed = {"Prefix": "home/alice/", "Delimiter": "/", "MaxKeys": 100}
        s3.list_objects_v2(Bucket=bucket, **allowed)
        s3.list_objects(Bucket=bucket, **allowed)
        s3.list_object_versions(Bucket=bucket, **allowed)
        for refused in ({**allowed, "Prefix": "home/bob/"}, {**allowed, "Delimiter": "-"},
                        {**allowed, "MaxKeys": 101}, {"Delimiter": "/", "MaxKeys": 100}):
            assert _failure(lambda: s3.list_objects_v2(Bucket=bucket, **refused)) == ("AccessDenied", 403)
        assert _failure(lambda: s3.list_object_versions(
            Bucket=bucket, **{**allowed, "Prefix": "home/bob/"})) == ("AccessDenied", 403)
    finally:
        iam.delete_access_key(UserName=name, AccessKeyId=key["AccessKeyId"])
        iam.delete_user_policy(UserName=name, PolicyName="home")
        iam.delete_user(UserName=name)
        root_s3.delete_bucket(Bucket=bucket)


@pytest.mark.parametrize("method,path,action", [
    ("PUT", "/v20180820/accesspoint/ap1", "s3:CreateAccessPoint"),
    ("GET", "/v20180820/accesspoint/ap1", "s3:GetAccessPoint"),
    ("DELETE", "/v20180820/accesspoint/ap1", "s3:DeleteAccessPoint"),
    ("GET", "/v20180820/accesspoint", "s3:ListAccessPoints"),
    ("PUT", "/v20180820/accesspoint/ap1/policy", "s3:PutAccessPointPolicy"),
    ("GET", "/v20180820/accesspoint/ap1/policy", "s3:GetAccessPointPolicy"),
    ("DELETE", "/v20180820/accesspoint/ap1/policy", "s3:DeleteAccessPointPolicy"),
])
def test_s3control_access_point_iam_action(method, path, action):
    from ministack.core.iam_actions import s3control_action

    assert s3control_action(method, path) == action


def test_s3control_access_point_denied_without_the_action(monkeypatch):
    """Under AUTH=true access point calls need their own action, on the access
    point's regional ARN."""
    import ministack.app as app

    seen = _capture_enforced_arn(monkeypatch)
    headers = {
        "authorization": "AWS4-HMAC-SHA256 Credential=test/20261013/us-east-1/s3/aws4_request, "
                         "SignedHeaders=host, Signature=deadbeef",
        "x-amz-account-id": "000000000000",
    }
    monkeypatch.setattr(app, "AUTH", True)
    asyncio.run(app._handle_s3_control_request("/v20180820/accesspoint/ap1/policy", "GET", b"", {}, "rid", headers))
    assert seen["action"] == "s3:GetAccessPointPolicy"
    assert seen["arn"].endswith(":000000000000:accesspoint/ap1")


@pytest.mark.parametrize("method,path,action", [
    ("PUT", "/v20180820/accesspointforobjectlambda/ol", "s3:CreateAccessPointForObjectLambda"),
    ("GET", "/v20180820/accesspointforobjectlambda/ol", "s3:GetAccessPointForObjectLambda"),
    ("DELETE", "/v20180820/accesspointforobjectlambda/ol", "s3:DeleteAccessPointForObjectLambda"),
    ("GET", "/v20180820/accesspointforobjectlambda", "s3:ListAccessPointsForObjectLambda"),
    ("PUT", "/v20180820/accesspointforobjectlambda/ol/configuration", "s3:PutAccessPointConfigurationForObjectLambda"),
    ("GET", "/v20180820/accesspointforobjectlambda/ol/configuration", "s3:GetAccessPointConfigurationForObjectLambda"),
    ("PUT", "/v20180820/accesspointforobjectlambda/ol/policy", "s3:PutAccessPointPolicyForObjectLambda"),
    ("GET", "/v20180820/accesspointforobjectlambda/ol/policy", "s3:GetAccessPointPolicyForObjectLambda"),
    ("DELETE", "/v20180820/accesspointforobjectlambda/ol/policy", "s3:DeleteAccessPointPolicyForObjectLambda"),
])
def test_s3control_object_lambda_iam_action(method, path, action):
    from ministack.core.iam_actions import s3control_action

    assert s3control_action(method, path) == action


@pytest.mark.parametrize("method,key,query,action", [
    ("GET", "k.txt", {}, "s3-object-lambda:GetObject"),
    ("HEAD", "k.txt", {}, "s3-object-lambda:GetObject"),
    ("PUT", "k.txt", {}, "s3-object-lambda:PutObject"),
    ("DELETE", "k.txt", {}, "s3-object-lambda:DeleteObject"),
    ("GET", "", {"list-type": ["2"]}, "s3-object-lambda:ListBucket"),
])
def test_s3_object_lambda_data_plane_iam_action(method, key, query, action):
    from ministack.core.iam_actions import s3_object_lambda_action

    assert s3_object_lambda_action(method, key, query) == action


def test_write_get_object_response_needs_its_iam_action(monkeypatch):
    import ministack.app as app

    seen = _capture_enforced_arn(monkeypatch)
    monkeypatch.setattr(app, "AUTH", True)
    headers = {"x-amz-request-route": "io-use1-unknown", "x-amz-request-token": "t",
               "authorization": "AWS4-HMAC-SHA256 Credential=test/20261013/us-east-1/s3-object-lambda/"
                                "aws4_request, SignedHeaders=host, Signature=x"}
    status = asyncio.run(app._handle_special_data_plane_request(
        "POST", "/WriteGetObjectResponse", headers, b"", {}, "rid"))[0]
    assert seen["action"] == "s3-object-lambda:WriteGetObjectResponse"
    assert status == 400
