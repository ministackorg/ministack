"""
Pytest fixtures for MiniStack integration tests.
"""
import os
import urllib.request
import pytest
import boto3
from botocore.config import Config

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
REGION = "us-east-1"

_kwargs = dict(
    endpoint_url=ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=REGION,
    config=Config(region_name=REGION, retries={"max_attempts": 0}),
)


def make_client(service):
    return boto3.client(service, **_kwargs)


@pytest.fixture(scope="session", autouse=True)
def reset_server():
    """Reset all server state once before the test session starts."""
    req = urllib.request.Request(
        f"{ENDPOINT}/_ministack/reset",
        data=b"",
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # server may not be up yet; individual tests will fail naturally


@pytest.fixture(scope="session")
def s3():
    return make_client("s3")

@pytest.fixture(scope="session")
def sqs():
    return make_client("sqs")

@pytest.fixture(scope="session")
def sns():
    return make_client("sns")

@pytest.fixture(scope="session")
def ddb():
    return make_client("dynamodb")

@pytest.fixture(scope="session")
def sts():
    return make_client("sts")

@pytest.fixture(scope="session")
def sm():
    return make_client("secretsmanager")

@pytest.fixture(scope="session")
def logs():
    return make_client("logs")

@pytest.fixture(scope="session")
def lam():
    return make_client("lambda")

@pytest.fixture(scope="session")
def iam():
    return make_client("iam")

@pytest.fixture(scope="session")
def ssm():
    return make_client("ssm")

@pytest.fixture(scope="session")
def eb():
    return make_client("events")

@pytest.fixture(scope="session")
def kin():
    return make_client("kinesis")

@pytest.fixture(scope="session")
def cw():
    return make_client("cloudwatch")

@pytest.fixture(scope="session")
def ses():
    return make_client("ses")

@pytest.fixture(scope="session")
def sfn():
    return make_client("stepfunctions")

@pytest.fixture(scope="session")
def ecs():
    return make_client("ecs")

@pytest.fixture(scope="session")
def rds():
    return make_client("rds")

@pytest.fixture(scope="session")
def ec():
    return make_client("elasticache")

@pytest.fixture(scope="session")
def glue():
    return make_client("glue")

@pytest.fixture(scope="session")
def athena():
    return make_client("athena")

@pytest.fixture(scope="session")
def apigw():
    return make_client("apigatewayv2")

@pytest.fixture(scope="session")
def sfn_sync():
    """SFN client for StartSyncExecution — forces same endpoint (boto3 normally prefixes sync-)."""
    from botocore.config import Config as BotoConfig
    return boto3.client(
        "stepfunctions",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
        config=BotoConfig(
            region_name=REGION,
            retries={"max_attempts": 0},
            inject_host_prefix=False,
        ),
    )
