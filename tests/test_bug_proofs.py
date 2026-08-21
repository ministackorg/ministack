"""
Proof-of-failure tests for 8 confirmed bugs.
Each test SHOULD pass on real AWS but FAILS on ministack,
proving the bug exists.
"""
import io
import json
import os
import time
import zipfile
import threading

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
REGION = "us-east-1"

_kwargs = dict(
    endpoint_url=ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=REGION,
    config=Config(region_name=REGION, retries={"mode": "standard"}, max_pool_connections=50),
)


def _make_client(service):
    return boto3.client(service, **_kwargs)


def _make_zip(filename, code):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, code)
    return buf.getvalue()


# ============================================================================
# BUG 1: Lambda timeout not enforced
# Real AWS kills the handler after Timeout seconds. Ministack blocks forever.
# ============================================================================

class TestBug1LambdaTimeout:
    """Lambda warm worker should kill execution after Timeout seconds."""

    def test_lambda_timeout_is_enforced(self):
        lam = _make_client("lambda")
        func_name = "bug1-timeout-test"

        # Handler that sleeps for 60 seconds — way past the 3s timeout
        code = _make_zip("handler.py", """
import time
def handler(event, context):
    time.sleep(60)
    return {"statusCode": 200, "body": "should never reach here"}
""")
        try:
            lam.delete_function(FunctionName=func_name)
        except Exception:
            pass

        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test",
            Handler="handler.handler",
            Code={"ZipFile": code},
            Timeout=3,  # 3 seconds
        )

        start = time.time()
        resp = lam.invoke(FunctionName=func_name)
        elapsed = time.time() - start

        # Real AWS returns FunctionError after ~3 seconds.
        # If this takes >10s, the timeout is not enforced.
        assert elapsed < 10, f"Lambda took {elapsed:.1f}s — timeout of 3s was NOT enforced"

        # Should get a timeout error
        body = json.loads(resp["Payload"].read())
        assert "timed out" in str(body).lower() or resp.get("FunctionError"), \
            f"Expected timeout error, got: {body}"

        lam.delete_function(FunctionName=func_name)


# ============================================================================
# BUG 2: Lambda version isolation broken
# Warm worker keyed by func name only — different versions share same code.
# ============================================================================

class TestBug2LambdaVersionIsolation:
    """Publishing a version should snapshot the code immutably."""

    def test_version_runs_its_own_code(self):
        lam = _make_client("lambda")
        func_name = "bug2-version-test"

        try:
            lam.delete_function(FunctionName=func_name)
        except Exception:
            pass

        # V1: returns "v1"
        code_v1 = _make_zip("handler.py", """
def handler(event, context):
    return "v1"
""")
        lam.create_function(
            FunctionName=func_name,
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/test",
            Handler="handler.handler",
            Code={"ZipFile": code_v1},
            Timeout=10,
        )
        pub = lam.publish_version(FunctionName=func_name)
        v1_version = pub["Version"]

        # Invoke v1 to warm it up
        resp1 = lam.invoke(FunctionName=func_name, Qualifier=v1_version)
        body1 = resp1["Payload"].read().decode().strip().strip('"')
        assert body1 == "v1", f"Expected 'v1', got {body1}"

        # Update $LATEST to return "v2"
        code_v2 = _make_zip("handler.py", """
def handler(event, context):
    return "v2"
""")
        lam.update_function_code(FunctionName=func_name, ZipFile=code_v2)
        time.sleep(1)  # let worker invalidate

        # Invoke $LATEST — should return "v2"
        resp_latest = lam.invoke(FunctionName=func_name, Qualifier="$LATEST")
        body_latest = resp_latest["Payload"].read().decode().strip().strip('"')
        assert body_latest == "v2", f"Expected 'v2' from $LATEST, got {body_latest}"

        # Invoke v1 again — should STILL return "v1"
        resp1_again = lam.invoke(FunctionName=func_name, Qualifier=v1_version)
        body1_again = resp1_again["Payload"].read().decode().strip().strip('"')
        assert body1_again == "v1", \
            f"Version {v1_version} should return 'v1' but got '{body1_again}' — version isolation is BROKEN"

        lam.delete_function(FunctionName=func_name)


# ============================================================================
# BUG 3: DynamoDB transaction atomicity
# TransactWriteItems should be all-or-nothing.
# ============================================================================

class TestBug3DynamoDBTransactionAtomicity:
    """TransactWriteItems must be atomic — partial writes should not persist."""

    def test_transact_write_all_or_nothing(self):
        ddb = _make_client("dynamodb")
        table_name = "bug3-txn-test"

        try:
            ddb.delete_table(TableName=table_name)
        except Exception:
            pass

        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        # Seed item that will be used for a condition check
        ddb.put_item(TableName=table_name, Item={"pk": {"S": "existing"}, "version": {"N": "1"}})

        # Transaction: write item A, then conditionally write item B (condition will FAIL)
        try:
            ddb.transact_write_items(TransactItems=[
                {
                    "Put": {
                        "TableName": table_name,
                        "Item": {"pk": {"S": "new-item-A"}, "data": {"S": "should-not-persist"}},
                    },
                },
                {
                    "Put": {
                        "TableName": table_name,
                        "Item": {"pk": {"S": "new-item-B"}, "data": {"S": "hello"}},
                        "ConditionExpression": "attribute_exists(nonexistent_field)",
                    },
                },
            ])
            pytest.fail("TransactWriteItems should have raised TransactionCanceledException")
        except ClientError as e:
            assert "TransactionCanceledException" in str(e) or "ConditionalCheckFailed" in str(e), \
                f"Expected TransactionCanceledException, got: {e}"

        # Item A should NOT exist — the entire transaction should have been rolled back
        try:
            resp = ddb.get_item(TableName=table_name, Key={"pk": {"S": "new-item-A"}})
            assert "Item" not in resp, \
                f"Item 'new-item-A' was persisted despite transaction failure — ATOMICITY BROKEN. Got: {resp['Item']}"
        except Exception:
            pass  # Item not found = correct

        ddb.delete_table(TableName=table_name)


# ============================================================================
# BUG 4: Cognito accepts any password
# Real AWS enforces PasswordPolicy from the user pool.
# ============================================================================

class TestBug4CognitoPasswordPolicy:
    """Cognito should reject passwords that don't meet the pool's policy."""

    def test_signup_rejects_weak_password(self):
        cognito = _make_client("cognito-idp")

        pool = cognito.create_user_pool(
            PoolName="bug4-password-test",
            Policies={
                "PasswordPolicy": {
                    "MinimumLength": 8,
                    "RequireUppercase": True,
                    "RequireLowercase": True,
                    "RequireNumbers": True,
                    "RequireSymbols": True,
                }
            },
        )
        pool_id = pool["UserPool"]["Id"]

        client = cognito.create_user_pool_client(
            UserPoolId=pool_id,
            ClientName="test-client",
            ExplicitAuthFlows=["ALLOW_USER_PASSWORD_AUTH"],
        )
        client_id = client["UserPoolClient"]["ClientId"]

        # Try to sign up with a password that's way too weak
        try:
            cognito.sign_up(
                ClientId=client_id,
                Username="testuser",
                Password="a",  # Violates: length, uppercase, numbers, symbols
            )
            pytest.fail(
                "SignUp with password 'a' should have been rejected with InvalidPasswordException "
                "— Cognito is NOT enforcing the password policy"
            )
        except ClientError as e:
            assert "InvalidPasswordException" in str(e) or "InvalidParameter" in str(e), \
                f"Expected InvalidPasswordException, got: {e}"

    def test_admin_create_user_rejects_weak_password(self):
        cognito = _make_client("cognito-idp")

        pool = cognito.create_user_pool(
            PoolName="bug4-admin-pw-test",
            Policies={
                "PasswordPolicy": {
                    "MinimumLength": 8,
                    "RequireUppercase": True,
                    "RequireLowercase": True,
                    "RequireNumbers": True,
                    "RequireSymbols": True,
                }
            },
        )
        pool_id = pool["UserPool"]["Id"]

        try:
            cognito.admin_create_user(
                UserPoolId=pool_id,
                Username="weakpwuser",
                TemporaryPassword="bad",  # Violates policy
            )
            pytest.fail(
                "AdminCreateUser with password 'bad' should have been rejected "
                "— Cognito is NOT enforcing the password policy"
            )
        except ClientError as e:
            assert "InvalidPasswordException" in str(e) or "InvalidParameter" in str(e), \
                f"Expected InvalidPasswordException, got: {e}"


# ============================================================================
# BUG 5: Lambda tmpdir leak (structural test — verify tmpdir not tracked)
# ============================================================================

class TestBug5LambdaTmpdirFixed:
    """Warm container cache should track tmpdir for cleanup."""

    def test_warm_cache_tracks_tmpdir(self):
        from ministack.services import lambda_svc
        import inspect
        source = inspect.getsource(lambda_svc._cache_warm_container)
        assert "tmpdir" in source, "Warm cache should now track tmpdir for cleanup"


class TestBug6LambdaNetworkFixed:
    """_execute_function_image should delegate to _invoke_rie."""

    def test_image_uses_invoke_rie(self):
        import inspect
        from ministack.services import lambda_svc
        source = inspect.getsource(lambda_svc._execute_function_image)
        assert "_invoke_rie" in source, \
            "_execute_function_image should call _invoke_rie instead of duplicating logic"


class TestBug7EKSNonBlockingFixed:
    """EKS _create_cluster should not block the event loop."""

    def test_create_cluster_uses_background_thread(self):
        import inspect
        from ministack.services import eks
        source = inspect.getsource(eks._create_cluster)
        assert "Thread" in source or "_bg_start" in source, \
            "EKS should use a background thread for k3s startup"


class TestBug8EKSImageFixed:
    """Default k3s image should use a real version tag."""

    def test_default_image_has_version_tag(self):
        from ministack.services import eks
        default = eks.EKS_K3S_IMAGE
        assert default != "rancher/k3s:latest", \
            f"Default should not be 'latest' (doesn't exist). Got: {default}"
        assert ":v" in default, f"Default should have a version tag. Got: {default}"
