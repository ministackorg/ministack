"""Tests for the IAM authentication and authorization engine.

Unit tests verify the policy evaluator against AWS-documented behavior:
  - AWS IAM policy evaluation logic (deny overrides allow, implicit deny)
  - AWS condition operator semantics (per IAM JSON policy elements reference)
  - AWS trust policy Principal matching
  - AWS error codes (per STS/IAM Common Errors reference)

Integration tests (SimulateCustomPolicy, policy validation) run against
the MiniStack server and work with AUTH=false.
"""

import json
import time

import pytest
from botocore.exceptions import ClientError

from ministack.core.iam_evaluator import (
    AuthError,
    EvalContext,
    EvalResult,
    PrincipalInfo,
    evaluate,
    evaluate_trust_policy,
    enforce,
    fnmatch_iam,
    parse_policy_document,
    resolve_principal,
    validate_policy_document,
)


# ---------------------------------------------------------------------------
# Wildcard matching (IAM spec: case-insensitive, * = any, ? = single char)
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

    def test_service_principal(self):
        trust = {"Statement": [{"Effect": "Allow",
                                "Principal": {"Service": "lambda.amazonaws.com"},
                                "Action": "sts:AssumeRole"}]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::123:user/anyone")

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


# ---------------------------------------------------------------------------
# Action extraction
# ---------------------------------------------------------------------------

class TestActionExtraction:
    def test_query_protocol(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("sqs", "POST", "/", {}, b"", {"Action": ["CreateQueue"]}) == "sqs:CreateQueue"
        assert extract_iam_action("monitoring", "POST", "/", {}, b"", {"Action": ["PutMetricData"]}) == "cloudwatch:PutMetricData"

    def test_target_protocol(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("dynamodb", "POST", "/", {"x-amz-target": "DynamoDB_20120810.PutItem"}, b"", {}) == "dynamodb:PutItem"
        assert extract_iam_action("kms", "POST", "/", {"x-amz-target": "TrentService.Encrypt"}, b"", {}) == "kms:Encrypt"

    def test_s3_rest(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("s3", "GET", "/", {}, b"", {}) == "s3:ListAllMyBuckets"
        assert extract_iam_action("s3", "PUT", "/bucket/key", {}, b"", {}) == "s3:PutObject"
        assert extract_iam_action("s3", "GET", "/bucket", {}, b"", {"versioning": [""]}) == "s3:GetBucketVersioning"

    def test_lambda_rest(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("lambda", "POST", "/2015-03-31/functions", {}, b"", {}) == "lambda:CreateFunction"
        assert extract_iam_action("lambda", "POST", "/2015-03-31/functions/f/invocations", {}, b"", {}) == "lambda:InvokeFunction"

    def test_unknown_service_returns_none(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("unknown_svc", "GET", "/", {}, b"", {}) is None


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
        s, h, b = access_denied_response("s3", "s3:PutObject", "", "r1",
                                          error_code="ExpiredTokenException",
                                          message="Token expired")
        assert b"ExpiredTokenException" in b


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
    assert results["s3:DeleteBucket"] == "deny"


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
