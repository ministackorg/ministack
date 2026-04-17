import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

_ministack_installed = True

_requires_package = pytest.mark.skipif(
    not _ministack_installed,
    reason="ministack not installed locally (runs in CI via pip install -e .)",
)

@_requires_package
def test_package_core_importable():
    """ministack.core modules must all be importable."""
    from ministack.core.lambda_runtime import get_or_create_worker
    from ministack.core.lambda_runtime import reset as lr_reset
    from ministack.core.persistence import load_state, save_all
    from ministack.core.responses import error_response_json, json_response, new_uuid
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
        apigateway_v1,
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
    )
    from ministack.services import iam, sts

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
