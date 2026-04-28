"""
Pytest fixtures for MiniStack integration tests.
"""
import os
import urllib.request

import boto3
import pytest
from botocore.config import Config

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
REGION = "us-east-1"

_kwargs = dict(
    endpoint_url=ENDPOINT,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name=REGION,
    # Hardcoded retry and pool settings to reduce transient connection flakes
    config=Config(
        region_name=REGION,
        retries={"mode": "standard"},
        max_pool_connections=50,
    ),
)


def make_client(service):
    return boto3.client(service, **_kwargs)


_SERIAL_TESTS = {
    "tests/test_athena.py::test_athena_engine_mock_via_config",
    "tests/test_ec2.py::test_ec2_create_default_vpc",
    "tests/test_eks.py::test_eks_cfn_cluster",
    "tests/test_eks.py::test_eks_create_describe_delete_cluster",
    "tests/test_lambda.py::test_lambda_reset_terminates_workers",
    "tests/test_ministack.py::test_ministack_config_invalid_key_ignored",
    "tests/test_ses.py::test_ses_messages_endpoint_reset",
    "tests/test_ses.py::test_ses_messages_endpoint_account_filter",
    "tests/test_sfn.py::test_sfn_mock_config_return",
    "tests/test_sfn.py::test_sfn_mock_config_throw",
    "tests/test_sfn.py::test_sfn_wait_scale_zero_does_not_timeout_lambda_tasks",
    "tests/test_sfn.py::test_sfn_wait_scale_zero_skips_wait",
    "tests/test_rds_lambda_network.py::test_rds_lambda_network_connectivity",
    "tests/test_elasticache_lambda_network.py::test_elasticache_lambda_network_connectivity",
    # API Gateway execute-api → Lambda invoke under tight urlopen / WS recv
    # timeouts. These pass cleanly when run serially but are sensitive to
    # xdist parallel load on shared CI runners (cold-start time bursts past
    # the client timeout). Pre-warming + the WS warm-pool key fix covered
    # the deterministic cases; these remaining ones tip over only under
    # sustained parallel pressure. Run them in the dedicated serial phase.
    "tests/test_apigatewayv2.py::test_apigwv2_path_based_execute_api_http",
    "tests/test_apigatewayv2.py::test_apigwv2_path_based_websocket",
    "tests/test_apigatewayv2.py::test_apigwv2_default_stage_serves_from_root",
    "tests/test_apigatewayv2.py::test_apigwv1_path_based_restapi_legacy_user_request",
    "tests/test_apigatewayv2.py::test_apigwv2_named_stage_still_requires_prefix",
    "tests/test_apigatewayv2.py::test_apigwv2_integration_wrapped_function_arn",
}


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "serial: test must run in a dedicated sequential phase",
    )


def pytest_collection_modifyitems(config, items):
    for item in items:
        nodeid = item.nodeid.split("[", 1)[0]
        if nodeid in _SERIAL_TESTS:
            item.add_marker("serial")


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

@pytest.fixture
def sts_as_role(sts):
    def _make(role_arn, session_name="test-session"):
        creds = sts.assume_role(RoleArn=role_arn, RoleSessionName=session_name)["Credentials"]
        return boto3.client(
            "sts",
            endpoint_url=ENDPOINT,
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
            region_name=REGION,
            config=Config(retries={"mode": "standard"}),
        )
    return _make

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
def ecr():
    return make_client("ecr")

@pytest.fixture(scope="session")
def ec():
    return make_client("elasticache")

@pytest.fixture(scope="session")
def glue():
    return make_client("glue")

@pytest.fixture(scope="session")
def athena():
    return make_client("athena")


def _ministack_config(settings):
    """Set runtime config on the running server via POST /_ministack/config."""
    import json
    req = urllib.request.Request(
        f"{ENDPOINT}/_ministack/config",
        data=json.dumps(settings).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=5)


@pytest.fixture(scope="session")
def fh():
    return make_client("firehose")

@pytest.fixture(scope="session")
def apigw():
    return make_client("apigatewayv2")

@pytest.fixture(scope="session")
def apigw_v1():
    return make_client("apigateway")

@pytest.fixture(scope="session")
def r53():
    return make_client("route53")

@pytest.fixture(scope="session")
def cognito_idp():
    return make_client("cognito-idp")

@pytest.fixture(scope="session")
def cognito_identity():
    return make_client("cognito-identity")

@pytest.fixture(scope="session")
def ec2():
    return make_client("ec2")

@pytest.fixture(scope="session")
def emr():
    return make_client("emr")

@pytest.fixture(scope="session")
def elbv2():
    return make_client("elbv2")
                                                                                                          
@pytest.fixture(scope="session")
def ebs():
    return make_client("ec2")

@pytest.fixture(scope="session")
def efs():
    return make_client("efs")

@pytest.fixture(scope="session")
def acm_client():
    return make_client("acm")

@pytest.fixture(scope="session")
def wafv2():
    return make_client("wafv2")

@pytest.fixture(scope="session")
def sesv2():
    return make_client("sesv2")

@pytest.fixture(scope="session")
def cfn():
    return make_client("cloudformation")

@pytest.fixture(scope="session")
def kms_client():
    return make_client("kms")

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
            retries={"mode": "standard"},
            max_pool_connections=50,
            inject_host_prefix=False,
        ),
    )

@pytest.fixture(scope="session")
def cloudfront():
    return make_client("cloudfront")

@pytest.fixture(scope="session")
def rds_data():
    return make_client("rds-data")

@pytest.fixture(scope="session")
def appconfig_client():
    return make_client("appconfig")

@pytest.fixture(scope="session")
def appconfigdata_client():
    return make_client("appconfigdata")

@pytest.fixture(scope="session")
def sd():
    """SD client for DiscoverInstances — forces same endpoint (boto3 normally prefixes data-)."""
    from botocore.config import Config as BotoConfig
    return boto3.client(
        "servicediscovery",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=REGION,
        config=BotoConfig(
            region_name=REGION,
            retries={"mode": "standard"},
            max_pool_connections=50,
            inject_host_prefix=False,
        ),
    )

@pytest.fixture(scope="session")
def codebuild():
    return make_client("codebuild")

@pytest.fixture(scope="session")
def autoscaling():
    return make_client("autoscaling")

@pytest.fixture(scope="session")
def transfer():
    return make_client("transfer")

@pytest.fixture(scope="session")
def eks():
    return make_client("eks")

@pytest.fixture(scope="session")
def appsync():
    return make_client("appsync")

@pytest.fixture(scope="session")
def scheduler():
    return make_client("scheduler")

@pytest.fixture(scope="session")
def tagging():
    return make_client("resourcegroupstaggingapi")
