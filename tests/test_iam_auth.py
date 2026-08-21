"""Tests for the IAM authentication and authorization engine.

Unit tests for the policy evaluator, condition operators, principal resolution,
trust policy evaluation, and role ARN validation. These test the engine directly
without requiring a running server or AUTH=true.

Integration tests for SimulateCustomPolicy and policy document validation use
boto3 against the running server (these work with AUTH=false).
"""

import json
import time

import pytest
from botocore.exceptions import ClientError

from ministack.core.iam_evaluator import (
    AuthError,
    EvalContext,
    EvalResult,
    ParsedStatement,
    PrincipalInfo,
    evaluate,
    evaluate_trust_policy,
    fnmatch_iam,
    parse_policy_document,
    resolve_principal,
    validate_policy_document,
    validate_role_arn,
)


# ---------------------------------------------------------------------------
# Wildcard matching
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
        assert fnmatch_iam("s3:PutObject", "s3:putobject")
        assert fnmatch_iam("S3:PUTOBJECT", "s3:PutObject")

    def test_question_mark(self):
        assert fnmatch_iam("s3:GetObject", "s3:Get?bject")
        assert not fnmatch_iam("s3:GetObject", "s3:Get?ject")

    def test_arn_wildcard(self):
        assert fnmatch_iam(
            "arn:aws:s3:::mybucket/mykey",
            "arn:aws:s3:::mybucket/*"
        )
        assert not fnmatch_iam(
            "arn:aws:s3:::otherbucket/mykey",
            "arn:aws:s3:::mybucket/*"
        )


# ---------------------------------------------------------------------------
# Policy parsing
# ---------------------------------------------------------------------------

class TestParsePolicyDocument:
    def test_basic_allow(self):
        doc = {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "s3:*",
                "Resource": "*"
            }]
        }
        stmts = parse_policy_document(doc)
        assert len(stmts) == 1
        assert stmts[0].effect == "Allow"
        assert stmts[0].actions == ["s3:*"]
        assert stmts[0].resources == ["*"]

    def test_multiple_statements(self):
        doc = {
            "Statement": [
                {"Effect": "Allow", "Action": ["s3:Get*", "s3:List*"], "Resource": "*"},
                {"Effect": "Deny", "Action": "s3:DeleteBucket", "Resource": "*"},
            ]
        }
        stmts = parse_policy_document(doc)
        assert len(stmts) == 2
        assert stmts[0].actions == ["s3:Get*", "s3:List*"]
        assert stmts[1].effect == "Deny"

    def test_not_action(self):
        doc = {"Statement": [{"Effect": "Allow", "NotAction": "iam:*", "Resource": "*"}]}
        stmts = parse_policy_document(doc)
        assert stmts[0].not_actions == ["iam:*"]
        assert stmts[0].actions == []

    def test_string_input(self):
        doc_str = json.dumps({"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]})
        stmts = parse_policy_document(doc_str)
        assert len(stmts) == 1

    def test_invalid_json(self):
        assert parse_policy_document("not json") == []

    def test_invalid_effect(self):
        doc = {"Statement": [{"Effect": "Maybe", "Action": "*", "Resource": "*"}]}
        assert parse_policy_document(doc) == []

    def test_single_statement_dict(self):
        doc = {"Statement": {"Effect": "Allow", "Action": "*", "Resource": "*"}}
        stmts = parse_policy_document(doc)
        assert len(stmts) == 1


# ---------------------------------------------------------------------------
# Policy validation
# ---------------------------------------------------------------------------

class TestValidatePolicyDocument:
    def test_valid_policy(self):
        doc = {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]}
        assert validate_policy_document(doc) is None

    def test_missing_statement(self):
        assert validate_policy_document({}) is not None

    def test_empty_statement_list(self):
        assert validate_policy_document({"Statement": []}) is not None

    def test_invalid_effect(self):
        doc = {"Statement": [{"Effect": "Maybe", "Action": "s3:*", "Resource": "*"}]}
        assert validate_policy_document(doc) is not None

    def test_missing_action(self):
        doc = {"Statement": [{"Effect": "Allow", "Resource": "*"}]}
        assert validate_policy_document(doc) is not None

    def test_missing_resource(self):
        doc = {"Statement": [{"Effect": "Allow", "Action": "s3:*"}]}
        assert validate_policy_document(doc) is not None

    def test_both_action_and_not_action(self):
        doc = {"Statement": [{"Effect": "Allow", "Action": "s3:*",
                              "NotAction": "iam:*", "Resource": "*"}]}
        assert validate_policy_document(doc) is not None

    def test_not_json_string(self):
        assert validate_policy_document("not json") is not None

    def test_json_string_input(self):
        doc_str = json.dumps({"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]})
        assert validate_policy_document(doc_str) is None


# ---------------------------------------------------------------------------
# Policy evaluation
# ---------------------------------------------------------------------------

def _ctx(action="s3:PutObject", resource="*", region="us-east-1"):
    return EvalContext(
        principal_arn="arn:aws:iam::000000000000:user/testuser",
        principal_type="User",
        principal_account="000000000000",
        action=action,
        resource_arn=resource,
        region=region,
    )


class TestEvaluate:
    def test_allow(self):
        stmts = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]})
        result = evaluate(_ctx(), [stmts])
        assert result.decision == "Allow"

    def test_implicit_deny(self):
        stmts = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]})
        result = evaluate(_ctx(action="ec2:RunInstances"), [stmts])
        assert result.decision == "ImplicitDeny"

    def test_explicit_deny_overrides_allow(self):
        stmts = parse_policy_document({"Statement": [
            {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
            {"Effect": "Deny", "Action": "s3:DeleteBucket", "Resource": "*"},
        ]})
        result = evaluate(_ctx(action="s3:DeleteBucket"), [stmts])
        assert result.decision == "Deny"

    def test_explicit_deny_does_not_affect_other_actions(self):
        stmts = parse_policy_document({"Statement": [
            {"Effect": "Allow", "Action": "s3:*", "Resource": "*"},
            {"Effect": "Deny", "Action": "s3:DeleteBucket", "Resource": "*"},
        ]})
        result = evaluate(_ctx(action="s3:PutObject"), [stmts])
        assert result.decision == "Allow"

    def test_not_action_allow(self):
        stmts = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "NotAction": "iam:*", "Resource": "*"}]})
        result = evaluate(_ctx(action="s3:PutObject"), [stmts])
        assert result.decision == "Allow"
        result = evaluate(_ctx(action="iam:CreateUser"), [stmts])
        assert result.decision == "ImplicitDeny"

    def test_not_resource(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*",
            "NotResource": "arn:aws:s3:::secret-bucket/*"
        }]})
        result = evaluate(_ctx(resource="arn:aws:s3:::public-bucket/key"), [stmts])
        assert result.decision == "Allow"
        result = evaluate(_ctx(resource="arn:aws:s3:::secret-bucket/key"), [stmts])
        assert result.decision == "ImplicitDeny"

    def test_multiple_policies(self):
        policy1 = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "s3:Get*", "Resource": "*"}]})
        policy2 = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "s3:Put*", "Resource": "*"}]})
        result = evaluate(_ctx(action="s3:PutObject"), [policy1, policy2])
        assert result.decision == "Allow"
        result = evaluate(_ctx(action="s3:GetObject"), [policy1, policy2])
        assert result.decision == "Allow"
        result = evaluate(_ctx(action="s3:DeleteObject"), [policy1, policy2])
        assert result.decision == "ImplicitDeny"

    def test_empty_policies(self):
        result = evaluate(_ctx(), [])
        assert result.decision == "ImplicitDeny"

    def test_admin_policy(self):
        stmts = parse_policy_document(
            {"Statement": [{"Effect": "Allow", "Action": "*", "Resource": "*"}]})
        result = evaluate(_ctx(action="iam:CreateUser"), [stmts])
        assert result.decision == "Allow"
        result = evaluate(_ctx(action="ec2:RunInstances"), [stmts])
        assert result.decision == "Allow"


# ---------------------------------------------------------------------------
# Condition operators
# ---------------------------------------------------------------------------

class TestConditions:
    def test_string_equals(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*", "Resource": "*",
            "Condition": {"StringEquals": {"aws:RequestedRegion": "us-east-1"}}
        }]})
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="eu-west-1"), [stmts]).decision == "ImplicitDeny"

    def test_string_like(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*", "Resource": "*",
            "Condition": {"StringLike": {"aws:RequestedRegion": "us-*"}}
        }]})
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="us-west-2"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="eu-west-1"), [stmts]).decision == "ImplicitDeny"

    def test_string_not_equals(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Deny", "Action": "s3:*", "Resource": "*",
            "Condition": {"StringNotEquals": {"aws:RequestedRegion": "us-east-1"}}
        }]})
        assert evaluate(_ctx(region="eu-west-1"), [stmts]).decision == "Deny"
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "ImplicitDeny"

    def test_ip_address(self):
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
        # source_ip defaults to 127.0.0.1, so NotIpAddress 127.0.0.0/8 is false → deny doesn't apply
        assert evaluate(_ctx(), [stmts]).decision == "ImplicitDeny"

    def test_bool_condition(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Deny", "Action": "*", "Resource": "*",
            "Condition": {"Bool": {"aws:SecureTransport": "true"}}
        }]})
        # secure_transport defaults to false → condition not met → deny doesn't apply
        assert evaluate(_ctx(), [stmts]).decision == "ImplicitDeny"

    def test_null_condition_key_present(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Deny", "Action": "*", "Resource": "*",
            "Condition": {"Null": {"aws:PrincipalArn": "false"}}
        }]})
        # PrincipalArn IS present → Null:false is true → deny applies
        assert evaluate(_ctx(), [stmts]).decision == "Deny"

    def test_null_condition_key_absent(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"Null": {"aws:SourceVpc": "true"}}
        }]})
        # SourceVpc is not in our context → Null:true is true → allow applies
        assert evaluate(_ctx(), [stmts]).decision == "Allow"

    def test_string_equals_ignore_case(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"StringEqualsIgnoreCase": {"aws:RequestedRegion": "US-EAST-1"}}
        }]})
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "Allow"

    def test_arn_like(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "*", "Resource": "*",
            "Condition": {"ArnLike": {
                "aws:PrincipalArn": "arn:aws:iam::000000000000:user/*"
            }}
        }]})
        assert evaluate(_ctx(), [stmts]).decision == "Allow"

    def test_multiple_condition_values_or(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*", "Resource": "*",
            "Condition": {"StringEquals": {
                "aws:RequestedRegion": ["us-east-1", "us-west-2"]
            }}
        }]})
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="us-west-2"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="eu-west-1"), [stmts]).decision == "ImplicitDeny"

    def test_multiple_condition_blocks_and(self):
        stmts = parse_policy_document({"Statement": [{
            "Effect": "Allow", "Action": "s3:*", "Resource": "*",
            "Condition": {
                "StringEquals": {"aws:RequestedRegion": "us-east-1"},
                "IpAddress": {"aws:SourceIp": "127.0.0.0/8"},
            }
        }]})
        assert evaluate(_ctx(region="us-east-1"), [stmts]).decision == "Allow"
        assert evaluate(_ctx(region="eu-west-1"), [stmts]).decision == "ImplicitDeny"


# ---------------------------------------------------------------------------
# Trust policy evaluation
# ---------------------------------------------------------------------------

class TestTrustPolicy:
    def test_wildcard_principal(self):
        trust = {"Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "sts:AssumeRole"
        }]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::999:user/anyone")

    def test_account_root_principal(self):
        trust = {"Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
            "Action": "sts:AssumeRole"
        }]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::123456789012:user/alice")
        assert not evaluate_trust_policy(trust, "arn:aws:iam::999999999999:user/bob")

    def test_specific_user_principal(self):
        trust = {"Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:user/alice"},
            "Action": "sts:AssumeRole"
        }]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::123456789012:user/alice")
        assert not evaluate_trust_policy(trust, "arn:aws:iam::123456789012:user/bob")

    def test_service_principal(self):
        trust = {"Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::123:user/anyone")

    def test_multiple_principals(self):
        trust = {"Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": [
                "arn:aws:iam::111111111111:root",
                "arn:aws:iam::222222222222:root",
            ]},
            "Action": "sts:AssumeRole"
        }]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::111111111111:user/a")
        assert evaluate_trust_policy(trust, "arn:aws:iam::222222222222:user/b")
        assert not evaluate_trust_policy(trust, "arn:aws:iam::333333333333:user/c")

    def test_account_id_shorthand(self):
        trust = {"Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "123456789012"},
            "Action": "sts:AssumeRole"
        }]}
        assert evaluate_trust_policy(trust, "arn:aws:iam::123456789012:user/alice")
        assert not evaluate_trust_policy(trust, "arn:aws:iam::999999999999:user/bob")

    def test_wrong_action(self):
        trust = {"Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject"
        }]}
        assert not evaluate_trust_policy(trust, "arn:aws:iam::123:user/a")

    def test_deny_statement_ignored(self):
        trust = {"Statement": [{
            "Effect": "Deny",
            "Principal": "*",
            "Action": "sts:AssumeRole"
        }]}
        assert not evaluate_trust_policy(trust, "arn:aws:iam::123:user/a")

    def test_string_json_input(self):
        trust_str = json.dumps({"Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "sts:AssumeRole"
        }]})
        assert evaluate_trust_policy(trust_str, "arn:aws:iam::123:user/a")


# ---------------------------------------------------------------------------
# Principal resolution (unit tests — no server needed)
# ---------------------------------------------------------------------------

class TestResolvePrincipal:
    def test_empty_key_is_root(self):
        result = resolve_principal("", "000000000000")
        assert isinstance(result, PrincipalInfo)
        assert result.type == "Root"
        assert result.policies is None

    def test_test_key_is_root(self):
        result = resolve_principal("test", "000000000000")
        assert isinstance(result, PrincipalInfo)
        assert result.type == "Root"

    def test_twelve_digit_is_root(self):
        result = resolve_principal("123456789012", "123456789012")
        assert isinstance(result, PrincipalInfo)
        assert result.type == "Root"

    def test_unknown_key_is_auth_error(self):
        result = resolve_principal("AKIAFAKEKEY1234567", "000000000000")
        assert isinstance(result, AuthError)
        assert result.code == "InvalidClientTokenId"

    def test_expired_session(self):
        from ministack.services import sts as sts_svc
        fake_key = "ASIAFAKEEXPIRED001"
        sts_svc._sessions[fake_key] = {
            "Arn": "arn:aws:sts::000000000000:assumed-role/r/s",
            "UserId": "AROA123:s",
            "SecretAccessKey": "secret",
            "Expiration": time.time() - 3600,  # expired 1 hour ago
        }
        try:
            result = resolve_principal(fake_key, "000000000000")
            assert isinstance(result, AuthError)
            assert result.code == "ExpiredTokenException"
        finally:
            sts_svc._sessions.pop(fake_key, None)

    def test_valid_session(self):
        from ministack.services import sts as sts_svc
        fake_key = "ASIAFAKEVALID00001"
        sts_svc._sessions[fake_key] = {
            "Arn": "arn:aws:sts::000000000000:assumed-role/testrole/sess",
            "UserId": "AROA123:sess",
            "SecretAccessKey": "secret",
            "Expiration": time.time() + 3600,  # valid for 1 more hour
        }
        try:
            result = resolve_principal(fake_key, "000000000000")
            assert isinstance(result, PrincipalInfo)
            assert result.type == "AssumedRole"
        finally:
            sts_svc._sessions.pop(fake_key, None)

    def test_inactive_access_key(self):
        from ministack.services import iam as iam_svc
        fake_key = "AKIAFAKEINACTIVE01"
        iam_svc._access_keys[fake_key] = {
            "UserName": "inactiveuser",
            "AccessKeyId": fake_key,
            "SecretAccessKey": "secret",
            "Status": "Inactive",
            "CreateDate": "2024-01-01T00:00:00Z",
        }
        try:
            result = resolve_principal(fake_key, "000000000000")
            assert isinstance(result, AuthError)
            assert result.code == "InvalidClientTokenId"
        finally:
            iam_svc._access_keys.pop(fake_key, None)


# ---------------------------------------------------------------------------
# Action extraction
# ---------------------------------------------------------------------------

class TestActionExtraction:
    def test_query_protocol(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("sqs", "POST", "/", {}, b"", {"Action": ["CreateQueue"]}) == "sqs:CreateQueue"
        assert extract_iam_action("iam", "POST", "/", {}, b"", {"Action": ["CreateRole"]}) == "iam:CreateRole"
        assert extract_iam_action("monitoring", "POST", "/", {}, b"", {"Action": ["PutMetricData"]}) == "cloudwatch:PutMetricData"

    def test_target_protocol(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("dynamodb", "POST", "/", {"x-amz-target": "DynamoDB_20120810.PutItem"}, b"", {}) == "dynamodb:PutItem"
        assert extract_iam_action("kms", "POST", "/", {"x-amz-target": "TrentService.Encrypt"}, b"", {}) == "kms:Encrypt"
        assert extract_iam_action("logs", "POST", "/", {"x-amz-target": "Logs_20140328.CreateLogGroup"}, b"", {}) == "logs:CreateLogGroup"

    def test_s3_rest(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("s3", "GET", "/", {}, b"", {}) == "s3:ListAllMyBuckets"
        assert extract_iam_action("s3", "PUT", "/bucket/key", {}, b"", {}) == "s3:PutObject"
        assert extract_iam_action("s3", "GET", "/bucket", {}, b"", {"versioning": [""]}) == "s3:GetBucketVersioning"

    def test_lambda_rest(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("lambda", "POST", "/2015-03-31/functions", {}, b"", {}) == "lambda:CreateFunction"
        assert extract_iam_action("lambda", "GET", "/2015-03-31/functions/f", {}, b"", {}) == "lambda:GetFunction"
        assert extract_iam_action("lambda", "POST", "/2015-03-31/functions/f/invocations", {}, b"", {}) == "lambda:InvokeFunction"

    def test_unknown_service(self):
        from ministack.core.iam_actions import extract_iam_action
        assert extract_iam_action("unknown_svc", "GET", "/", {}, b"", {}) is None


# ---------------------------------------------------------------------------
# AccessDenied response formatting
# ---------------------------------------------------------------------------

class TestAccessDeniedResponse:
    def test_s3_xml(self):
        from ministack.core.iam_actions import access_denied_response
        s, h, b = access_denied_response("s3", "s3:PutObject", "arn:aws:iam::123:user/a", "r1")
        assert s == 403
        assert b"<Error>" in b
        assert b"AccessDenied" in b

    def test_ec2_xml(self):
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

    def test_custom_error_code(self):
        from ministack.core.iam_actions import access_denied_response
        s, h, b = access_denied_response("s3", "s3:PutObject", "", "r1",
                                          error_code="InvalidClientTokenId",
                                          message="Token is invalid")
        assert s == 403
        assert b"InvalidClientTokenId" in b

    def test_query_xml(self):
        from ministack.core.iam_actions import access_denied_response
        s, h, b = access_denied_response("iam", "iam:CreateRole", "arn:aws:iam::123:user/a", "r1")
        assert s == 403
        assert b"<ErrorResponse" in b
        assert b"AccessDenied" in b


# ---------------------------------------------------------------------------
# Integration: SimulateCustomPolicy (against running server, AUTH=false OK)
# ---------------------------------------------------------------------------

def test_simulate_custom_policy_allow(iam):
    policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
    })
    resp = iam.simulate_custom_policy(
        PolicyInputList=[policy],
        ActionNames=["s3:GetObject", "s3:PutObject"],
    )
    results = resp["EvaluationResults"]
    assert len(results) == 2
    for r in results:
        assert r["EvalDecision"] == "allowed"


def test_simulate_custom_policy_deny(iam):
    policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": "*"}]
    })
    resp = iam.simulate_custom_policy(
        PolicyInputList=[policy],
        ActionNames=["ec2:RunInstances"],
    )
    results = resp["EvaluationResults"]
    assert len(results) == 1
    assert results[0]["EvalDecision"] == "implicitDeny"


def test_simulate_custom_policy_explicit_deny(iam):
    policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Action": "*", "Resource": "*"},
            {"Effect": "Deny", "Action": "s3:DeleteBucket", "Resource": "*"},
        ]
    })
    resp = iam.simulate_custom_policy(
        PolicyInputList=[policy],
        ActionNames=["s3:PutObject", "s3:DeleteBucket"],
    )
    results = {r["EvalActionName"]: r["EvalDecision"] for r in resp["EvaluationResults"]}
    assert results["s3:PutObject"] == "allowed"
    assert results["s3:DeleteBucket"] == "deny"


# ---------------------------------------------------------------------------
# Integration: Policy document validation (against running server)
# ---------------------------------------------------------------------------

def test_create_policy_rejects_malformed_document(iam):
    with pytest.raises(ClientError) as exc:
        iam.create_policy(
            PolicyName="bad-policy",
            PolicyDocument="not valid json",
        )
    assert exc.value.response["Error"]["Code"] == "MalformedPolicyDocument"


def test_create_policy_rejects_missing_action(iam):
    with pytest.raises(ClientError) as exc:
        iam.create_policy(
            PolicyName="no-action-policy",
            PolicyDocument=json.dumps({
                "Statement": [{"Effect": "Allow", "Resource": "*"}]
            }),
        )
    assert exc.value.response["Error"]["Code"] == "MalformedPolicyDocument"


def test_put_role_policy_rejects_malformed_document(iam):
    iam.create_role(
        RoleName="validation-test-role",
        AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "sts:AssumeRole"}]
        }),
    )
    try:
        with pytest.raises(ClientError) as exc:
            iam.put_role_policy(
                RoleName="validation-test-role",
                PolicyName="bad",
                PolicyDocument="not json",
            )
        assert exc.value.response["Error"]["Code"] == "MalformedPolicyDocument"
    finally:
        iam.delete_role(RoleName="validation-test-role")
