import time

import boto3
import pytest
import requests
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs


@pytest.fixture(scope="module")
def ministack():
    """Start a MiniStack container and wait for it to be healthy."""
    container = DockerContainer("ministackorg/ministack:latest").with_exposed_ports(4566)
    container.start()

    host = container.get_container_host_ip()
    port = container.get_exposed_port(4566)
    endpoint = f"http://{host}:{port}"

    # Wait for health endpoint to be ready
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            resp = requests.get(f"{endpoint}/_ministack/health", timeout=2)
            if resp.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(0.5)
    else:
        raise RuntimeError("MiniStack container did not become healthy within 30s")

    yield endpoint

    container.stop()


def _client(service: str, endpoint: str):
    """Create a boto3 client pointing at the MiniStack container."""
    return boto3.client(
        service,
        endpoint_url=endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

class TestS3:
    def test_create_bucket_put_get_object(self, ministack):
        s3 = _client("s3", ministack)

        bucket = "test-bucket"
        s3.create_bucket(Bucket=bucket)

        s3.put_object(Bucket=bucket, Key="hello.txt", Body=b"hello world")

        resp = s3.get_object(Bucket=bucket, Key="hello.txt")
        body = resp["Body"].read()
        assert body == b"hello world"


# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------

class TestSQS:
    def test_create_queue_send_receive(self, ministack):
        sqs = _client("sqs", ministack)

        queue = sqs.create_queue(QueueName="test-queue")
        queue_url = queue["QueueUrl"]

        sqs.send_message(QueueUrl=queue_url, MessageBody="ping")

        messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
        assert len(messages["Messages"]) == 1
        assert messages["Messages"][0]["Body"] == "ping"


# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------

class TestDynamoDB:
    def test_create_table_put_get_item(self, ministack):
        ddb = _client("dynamodb", ministack)

        table_name = "test-table"
        ddb.create_table(
            TableName=table_name,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )

        ddb.put_item(
            TableName=table_name,
            Item={"pk": {"S": "key1"}, "data": {"S": "value1"}},
        )

        resp = ddb.get_item(
            TableName=table_name,
            Key={"pk": {"S": "key1"}},
        )
        assert resp["Item"]["data"]["S"] == "value1"
