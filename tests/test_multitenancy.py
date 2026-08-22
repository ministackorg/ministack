"""
Tests for multi-tenancy: dynamic Account ID derived from AWS_ACCESS_KEY_ID.

When the access key is a 12-digit number, MiniStack uses it as the Account ID
in all ARN generation. Non-numeric keys (like "test") fall back to the default
000000000000.
"""

import os

import boto3
from botocore.config import Config

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


# ── STS GetCallerIdentity ─────────────────────────────────

def test_default_account_id():
    """Non-numeric access key falls back to 000000000000."""
    sts = _client("sts", access_key="test")
    resp = sts.get_caller_identity()
    assert resp["Account"] == "000000000000"


def test_12_digit_access_key_becomes_account_id():
    """A 12-digit numeric access key is used as the Account ID."""
    sts = _client("sts", access_key="123456789012")
    resp = sts.get_caller_identity()
    assert resp["Account"] == "123456789012"


def test_different_12_digit_keys_get_different_accounts():
    """Two different 12-digit keys produce different account IDs."""
    sts_a = _client("sts", access_key="111111111111")
    sts_b = _client("sts", access_key="222222222222")
    assert sts_a.get_caller_identity()["Account"] == "111111111111"
    assert sts_b.get_caller_identity()["Account"] == "222222222222"


def test_non_12_digit_numeric_falls_back():
    """A numeric key that isn't exactly 12 digits uses the default."""
    sts = _client("sts", access_key="12345")
    resp = sts.get_caller_identity()
    assert resp["Account"] == "000000000000"


# ── S3: ARN isolation ─────────────────────────────────────

def test_sqs_queue_arn_uses_dynamic_account():
    """SQS queue ARN reflects the 12-digit access key as account ID."""
    sqs = _client("sqs", access_key="048408301323")
    q = sqs.create_queue(QueueName="mt-test-queue")
    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=q["QueueUrl"], AttributeNames=["QueueArn"]
        )
        arn = attrs["Attributes"]["QueueArn"]
        assert "048408301323" in arn, f"Expected account 048408301323 in ARN: {arn}"
    finally:
        sqs.delete_queue(QueueUrl=q["QueueUrl"])


def test_sqs_queues_isolated_by_account():
    """Queues created with different account keys are separate namespaces."""
    sqs_a = _client("sqs", access_key="111111111111")
    sqs_b = _client("sqs", access_key="222222222222")

    q_a = sqs_a.create_queue(QueueName="isolation-test")
    try:
        q_b = sqs_b.create_queue(QueueName="isolation-test")
        try:
            # Both should get their own queue with their own account in the ARN
            attrs_a = sqs_a.get_queue_attributes(
                QueueUrl=q_a["QueueUrl"], AttributeNames=["QueueArn"]
            )
            attrs_b = sqs_b.get_queue_attributes(
                QueueUrl=q_b["QueueUrl"], AttributeNames=["QueueArn"]
            )
            assert "111111111111" in attrs_a["Attributes"]["QueueArn"]
            assert "222222222222" in attrs_b["Attributes"]["QueueArn"]
        finally:
            sqs_b.delete_queue(QueueUrl=q_b["QueueUrl"])
    finally:
        sqs_a.delete_queue(QueueUrl=q_a["QueueUrl"])


# ── Lambda: ARN uses dynamic account ──────────────────────

def test_lambda_function_arn_uses_dynamic_account():
    """Lambda function ARN reflects the 12-digit access key."""
    lam = _client("lambda", access_key="999888777666")
    try:
        lam.create_function(
            FunctionName="mt-func",
            Runtime="python3.12",
            Role="arn:aws:iam::999888777666:role/test",
            Handler="index.handler",
            Code={"ZipFile": b"fake"},
        )
        resp = lam.get_function(FunctionName="mt-func")
        arn = resp["Configuration"]["FunctionArn"]
        assert "999888777666" in arn, f"Expected account in ARN: {arn}"
    finally:
        try:
            lam.delete_function(FunctionName="mt-func")
        except Exception:
            pass


# ── SSM: ARN uses dynamic account ────────────────────────

def test_ssm_parameter_arn_uses_dynamic_account():
    """SSM parameter ARN reflects the 12-digit access key."""
    ssm = _client("ssm", access_key="048408301323")
    ssm.put_parameter(
        Name="/mt-test/param1",
        Value="hello",
        Type="String",
    )
    try:
        resp = ssm.get_parameter(Name="/mt-test/param1")
        arn = resp["Parameter"]["ARN"]
        assert "048408301323" in arn, f"Expected account in ARN: {arn}"
    finally:
        ssm.delete_parameter(Name="/mt-test/param1")


# ─────────────────── Cross-account isolation (1.3.3 CRITICAL fixes) ───────────────────
# Each test below creates the same resource name in two accounts and asserts
# list/describe operations in one account do NOT see the other account's data.


def test_cloudwatch_metrics_isolated_per_account():
    """PutMetricData from account A is invisible to ListMetrics in account B."""
    import uuid
    ns = f"ms-mt-{uuid.uuid4().hex[:8]}"
    cw_a = _client("cloudwatch", access_key="111111111111")
    cw_b = _client("cloudwatch", access_key="222222222222")
    cw_a.put_metric_data(Namespace=ns, MetricData=[{"MetricName": "leak", "Value": 1.0}])
    metrics_a = cw_a.list_metrics(Namespace=ns)["Metrics"]
    metrics_b = cw_b.list_metrics(Namespace=ns)["Metrics"]
    assert any(m["MetricName"] == "leak" for m in metrics_a)
    assert all(m["MetricName"] != "leak" for m in metrics_b), \
        f"CRITICAL: CloudWatch metrics leaking cross-account; B saw: {metrics_b}"


def test_athena_workgroups_isolated_per_account():
    """CreateWorkGroup in account A does NOT appear in ListWorkGroups for account B."""
    import uuid
    wg = f"mt-wg-{uuid.uuid4().hex[:8]}"
    a = _client("athena", access_key="111111111111")
    b = _client("athena", access_key="222222222222")
    try:
        a.create_work_group(Name=wg, Description="A's workgroup")
        names_a = [w["Name"] for w in a.list_work_groups()["WorkGroups"]]
        names_b = [w["Name"] for w in b.list_work_groups()["WorkGroups"]]
        assert wg in names_a
        assert wg not in names_b, \
            f"CRITICAL: Athena workgroup leaking cross-account; B saw: {names_b}"
    finally:
        try: a.delete_work_group(WorkGroup=wg)
        except Exception: pass


def test_ses_sent_emails_isolated_per_account():
    """Account A's sent emails must not appear in account B's GetSendStatistics."""
    a = _client("ses", access_key="111111111111")
    b = _client("ses", access_key="222222222222")
    # Verify identity first
    a.verify_email_identity(EmailAddress="mt-a@example.com")
    b.verify_email_identity(EmailAddress="mt-b@example.com")
    a.send_email(
        Source="mt-a@example.com",
        Destination={"ToAddresses": ["recip@example.com"]},
        Message={"Subject": {"Data": "A"}, "Body": {"Text": {"Data": "A"}}},
    )
    stats_a = a.get_send_statistics()["SendDataPoints"]
    stats_b = b.get_send_statistics()["SendDataPoints"]
    attempts_a = sum(p.get("DeliveryAttempts", 0) for p in stats_a)
    attempts_b = sum(p.get("DeliveryAttempts", 0) for p in stats_b)
    assert attempts_a >= 1
    assert attempts_b == 0, \
        f"CRITICAL: SES send stats leaking cross-account; B saw {attempts_b} attempts"


def test_eventbridge_default_bus_has_caller_account_arn():
    """Each account's 'default' bus ARN must reflect the caller's account id."""
    a = _client("events", access_key="111111111111")
    b = _client("events", access_key="222222222222")
    arn_a = a.describe_event_bus(Name="default")["Arn"]
    arn_b = b.describe_event_bus(Name="default")["Arn"]
    assert ":111111111111:" in arn_a
    assert ":222222222222:" in arn_b
    assert arn_a != arn_b


def test_apigateway_v1_stages_isolated_per_account():
    """Account A's REST API stages are invisible to account B."""
    import uuid
    name = f"mt-api-{uuid.uuid4().hex[:8]}"
    a = _client("apigateway", access_key="111111111111")
    b = _client("apigateway", access_key="222222222222")
    a_api = a.create_rest_api(name=name)["id"]
    try:
        # Must create a deployment before a stage
        a_res = a.get_resources(restApiId=a_api)["items"][0]["id"]
        a.put_method(restApiId=a_api, resourceId=a_res, httpMethod="GET", authorizationType="NONE")
        a.put_integration(restApiId=a_api, resourceId=a_res, httpMethod="GET", type="MOCK")
        dep = a.create_deployment(restApiId=a_api, stageName="prod")
        # A can see its stage
        stages_a = a.get_stages(restApiId=a_api)["item"]
        assert any(s["stageName"] == "prod" for s in stages_a)
        # B MUST NOT see A's api at all
        apis_b = b.get_rest_apis()["items"]
        assert all(api["id"] != a_api for api in apis_b), \
            f"CRITICAL: APIGW v1 REST api leaking cross-account; B saw: {apis_b}"
    finally:
        try: a.delete_rest_api(restApiId=a_api)
        except Exception: pass
