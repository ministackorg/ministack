
import pytest

_ministack_installed = True

_requires_package = pytest.mark.skipif(
    not _ministack_installed,
    reason="ministack not installed locally (runs in CI via pip install -e .)",
)

@_requires_package
def test_package_core_importable():
    """ministack.core modules must all be importable."""
    from ministack.core.lambda_runtime import get_or_create_worker
    from ministack.core.persistence import save_all
    from ministack.core.responses import json_response
    from ministack.core.router import detect_service

    assert callable(json_response)
    assert callable(detect_service)
    assert callable(get_or_create_worker)
    assert callable(save_all)

@_requires_package
def test_package_services_importable():
    """All 25 ministack.services modules must be importable and expose handle_request."""
    from ministack.services import (
        apigateway,
        athena,
        cloudwatch,
        cloudwatch_logs,
        cognito,
        dynamodb,
        ecs,
        elasticache,
        eventbridge,
        firehose,
        glue,
        iam,
        kinesis,
        lambda_svc,
        rds,
        route53,
        s3,
        secretsmanager,
        ses,
        sns,
        sqs,
        ssm,
        stepfunctions,
        sts,
    )

    for mod in [
        s3,
        sqs,
        sns,
        dynamodb,
        lambda_svc,
        secretsmanager,
        cloudwatch_logs,
        ssm,
        eventbridge,
        kinesis,
        cloudwatch,
        ses,
        stepfunctions,
        ecs,
        rds,
        elasticache,
        glue,
        athena,
        apigateway,
        firehose,
        route53,
        cognito,
        iam,
        sts,
    ]:
        assert callable(getattr(mod, "handle_request", None)), f"{mod.__name__} missing handle_request"
