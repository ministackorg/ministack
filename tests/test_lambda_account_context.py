"""
Integration test: Lambda Account Context — Non-Default Account AWS_ACCESS_KEY_ID.

**Validates: Requirements 1.1–1.6, 2.1–2.6, 3.1–3.3**

Bug Condition:
    A Lambda function deployed under a non-default account should receive
    AWS_ACCESS_KEY_ID set to the owning account's 12-digit ID (derived from
    the function ARN), NOT the host process's AWS_ACCESS_KEY_ID.

This test deploys a stub Lambda that calls STS GetCallerIdentity and returns
the account ID. It verifies:
  - Non-default account functions get the correct account context (bug condition)
  - Default account functions still work correctly (preservation)
  - Explicit AWS_ACCESS_KEY_ID overrides take precedence (preservation)
"""

import io
import json
import os
import zipfile

import boto3
import pytest
from botocore.config import Config
from unittest.mock import patch

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
REGION = "us-east-1"


def _client(service, access_key="test"):
    """Create a boto3 client with a specific access key."""
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key="test",
        region_name=REGION,
        config=Config(region_name=REGION, retries={"max_attempts": 0}),
    )


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


# Lambda code that calls STS GetCallerIdentity and returns the account + env vars
_STS_CALLER_CODE = """\
import json
import os
import urllib.request

def handler(event, context):
    # Call STS GetCallerIdentity via the ministack endpoint
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "unknown")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
    region = os.environ.get("AWS_REGION", "us-east-1")

    # Return the raw env vars so the test can verify them
    return {
        "aws_access_key_id": access_key,
        "aws_region": region,
        "function_arn": os.environ.get("_LAMBDA_FUNCTION_ARN", ""),
    }
"""


# ---------------------------------------------------------------------------
# Bug Condition Tests: Non-default account should get ARN-derived account ID
# ---------------------------------------------------------------------------


class TestBugCondition:
    """Validates that Lambda functions under non-default accounts receive
    the correct AWS_ACCESS_KEY_ID derived from their function ARN."""

    def test_non_default_account_gets_arn_account_id(self):
        """
        **Validates: Requirements 1.1, 1.5, 2.1, 2.5**

        Deploy a function under account 000000000001, invoke it, and verify
        AWS_ACCESS_KEY_ID is set to '000000000001' (not the host's key).
        """
        lam = _client("lambda", access_key="000000000001")

        func_name = "account-context-test-nondefault"
        try:
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role="arn:aws:iam::000000000001:role/lambda-role",
                Handler="index.handler",
                Code={"ZipFile": _make_zip(_STS_CALLER_CODE)},
            )

            resp = lam.invoke(FunctionName=func_name, Payload=json.dumps({}))
            payload = json.loads(resp["Payload"].read())

            assert payload["aws_access_key_id"] == "000000000001", (
                f"Expected AWS_ACCESS_KEY_ID='000000000001' (from ARN), "
                f"got '{payload['aws_access_key_id']}'"
            )
        finally:
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass

    def test_another_non_default_account(self):
        """
        **Validates: Requirements 1.1, 2.1**

        Deploy under a different non-default account (123456789012) to confirm
        the fix works for arbitrary 12-digit account IDs.
        """
        lam = _client("lambda", access_key="123456789012")

        func_name = "account-context-test-123"
        try:
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role="arn:aws:iam::123456789012:role/lambda-role",
                Handler="index.handler",
                Code={"ZipFile": _make_zip(_STS_CALLER_CODE)},
            )

            resp = lam.invoke(FunctionName=func_name, Payload=json.dumps({}))
            payload = json.loads(resp["Payload"].read())

            assert payload["aws_access_key_id"] == "123456789012", (
                f"Expected AWS_ACCESS_KEY_ID='123456789012' (from ARN), "
                f"got '{payload['aws_access_key_id']}'"
            )
        finally:
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Preservation Tests: Default account and explicit overrides unchanged
# ---------------------------------------------------------------------------


class TestPreservation:
    """Validates that existing behavior is preserved for default account
    functions and explicit environment variable overrides."""

    def test_default_account_still_works(self):
        """
        **Validates: Requirement 3.1**

        Deploy a function under the default account (000000000000) and verify
        AWS_ACCESS_KEY_ID is '000000000000' (derived from the ARN).
        Note: On unfixed code with host key 'test', this also fails — the fix
        ensures the ARN-derived account is always used.
        """
        lam = _client("lambda", access_key="000000000000")

        func_name = "account-context-test-default"
        try:
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role="arn:aws:iam::000000000000:role/lambda-role",
                Handler="index.handler",
                Code={"ZipFile": _make_zip(_STS_CALLER_CODE)},
            )

            resp = lam.invoke(FunctionName=func_name, Payload=json.dumps({}))
            payload = json.loads(resp["Payload"].read())

            assert payload["aws_access_key_id"] == "000000000000", (
                f"Expected AWS_ACCESS_KEY_ID='000000000000' for default account, "
                f"got '{payload['aws_access_key_id']}'"
            )
        finally:
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass

    def test_explicit_env_override_takes_precedence(self):
        """
        **Validates: Requirement 3.2**

        Deploy a function with an explicit AWS_ACCESS_KEY_ID in Environment.Variables.
        The explicit value should take precedence over the ARN-derived account.
        """
        lam = _client("lambda", access_key="000000000001")

        func_name = "account-context-test-override"
        try:
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role="arn:aws:iam::000000000001:role/lambda-role",
                Handler="index.handler",
                Code={"ZipFile": _make_zip(_STS_CALLER_CODE)},
                Environment={
                    "Variables": {
                        "AWS_ACCESS_KEY_ID": "999999999999",
                    }
                },
            )

            resp = lam.invoke(FunctionName=func_name, Payload=json.dumps({}))
            payload = json.loads(resp["Payload"].read())

            assert payload["aws_access_key_id"] == "999999999999", (
                f"Expected AWS_ACCESS_KEY_ID='999999999999' (explicit override), "
                f"got '{payload['aws_access_key_id']}'"
            )
        finally:
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass

    def test_other_env_vars_unchanged(self):
        """
        **Validates: Requirement 3.3**

        Verify that AWS_REGION and _LAMBDA_FUNCTION_ARN are still set correctly
        regardless of the account context fix.
        """
        lam = _client("lambda", access_key="000000000001")

        func_name = "account-context-test-other-env"
        try:
            lam.create_function(
                FunctionName=func_name,
                Runtime="python3.12",
                Role="arn:aws:iam::000000000001:role/lambda-role",
                Handler="index.handler",
                Code={"ZipFile": _make_zip(_STS_CALLER_CODE)},
            )

            resp = lam.invoke(FunctionName=func_name, Payload=json.dumps({}))
            payload = json.loads(resp["Payload"].read())

            # AWS_REGION should be set
            assert payload["aws_region"] == REGION, (
                f"Expected AWS_REGION='{REGION}', got '{payload['aws_region']}'"
            )
            # _LAMBDA_FUNCTION_ARN should contain the function name and account
            assert "000000000001" in payload["function_arn"], (
                f"Expected account '000000000001' in function ARN, "
                f"got '{payload['function_arn']}'"
            )
            assert func_name in payload["function_arn"], (
                f"Expected function name in ARN, got '{payload['function_arn']}'"
            )
        finally:
            try:
                lam.delete_function(FunctionName=func_name)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Unit Tests: _account_from_arn helper
# ---------------------------------------------------------------------------


class TestAccountFromArn:
    """Unit tests for the _account_from_arn helper function."""

    def test_valid_arn_extracts_account(self):
        """Valid ARN returns the 12-digit account ID."""
        from ministack.services.lambda_svc import _account_from_arn

        result = _account_from_arn("arn:aws:lambda:us-east-1:123456789012:function:myFunc")
        assert result == "123456789012"

    def test_various_valid_accounts(self):
        """Various valid 12-digit account IDs are extracted correctly."""
        from ministack.services.lambda_svc import _account_from_arn

        assert _account_from_arn("arn:aws:lambda:us-east-1:000000000000:function:f") == "000000000000"
        assert _account_from_arn("arn:aws:lambda:eu-west-1:000000000001:function:f") == "000000000001"
        assert _account_from_arn("arn:aws:lambda:ap-southeast-1:999999999999:function:f") == "999999999999"

    def test_empty_string_falls_back(self):
        """Empty string falls back to host env var."""
        from ministack.services.lambda_svc import _account_from_arn

        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "fallback_key"}):
            result = _account_from_arn("")
            assert result == "fallback_key"

    def test_too_few_segments_falls_back(self):
        """ARN with too few segments falls back to host env var."""
        from ministack.services.lambda_svc import _account_from_arn

        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "fallback_key"}):
            result = _account_from_arn("arn:aws:lambda")
            assert result == "fallback_key"

    def test_non_numeric_account_falls_back(self):
        """ARN with non-numeric account segment falls back to host env var."""
        from ministack.services.lambda_svc import _account_from_arn

        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "fallback_key"}):
            result = _account_from_arn("arn:aws:lambda:us-east-1:not-a-number:function:f")
            assert result == "fallback_key"

    def test_none_input_falls_back(self):
        """None input falls back to host env var without crashing."""
        from ministack.services.lambda_svc import _account_from_arn

        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "fallback_key"}):
            result = _account_from_arn(None)
            assert result == "fallback_key"

    def test_no_env_var_falls_back_to_test(self):
        """When AWS_ACCESS_KEY_ID is not set, falls back to 'test'."""
        from ministack.services.lambda_svc import _account_from_arn

        with patch.dict(os.environ, {}, clear=True):
            result = _account_from_arn("")
            assert result == "test"

    def test_lambda_runtime_helper_matches(self):
        """The lambda_runtime.py local helper produces the same results."""
        from ministack.core.lambda_runtime import _account_from_arn as runtime_helper

        assert runtime_helper("arn:aws:lambda:us-east-1:123456789012:function:f") == "123456789012"
        assert runtime_helper("arn:aws:lambda:us-east-1:000000000001:function:f") == "000000000001"

        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "fallback_key"}):
            assert runtime_helper("") == "fallback_key"
            assert runtime_helper(None) == "fallback_key"
            assert runtime_helper("arn:aws:lambda") == "fallback_key"
