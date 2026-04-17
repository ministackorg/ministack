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
def test_minstack_app_asgi_callable():
    """ministack.app:app must be an async callable (ASGI entry point)."""
    import inspect

    from ministack import app as app_module

    assert callable(app_module.app)
    assert inspect.iscoroutinefunction(app_module.app)
    assert callable(app_module.main)


def test_ministack_config_invalid_key_ignored():
    """/_ministack/config silently ignores unknown keys and only applies valid ones."""
    import json as _json
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        f"{endpoint}/_ministack/config",
        data=_json.dumps(
            {
                "nonexistent_module.VAR": "val",
                "athena.ATHENA_ENGINE": "auto",
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    resp = _json.loads(urllib.request.urlopen(req, timeout=5).read())
    assert "nonexistent_module.VAR" not in resp["applied"]
    assert resp["applied"].get("athena.ATHENA_ENGINE") == "auto"

def test_ministack_health_endpoints():
    import urllib.request

    resp_health = urllib.request.urlopen("http://localhost:4566/health")
    assert resp_health.status == 200
    data_health = json.loads(resp_health.read())
    assert "services" in data_health
    assert "s3" in data_health["services"]
    assert data_health["edition"] == "light"

    resp_ministack = urllib.request.urlopen("http://localhost:4566/_ministack/health")
    data_ministack = json.loads(resp_ministack.read())
    assert data_health == data_ministack

    resp_localstack = urllib.request.urlopen("http://localhost:4566/_localstack/health")
    data_localstack = json.loads(resp_localstack.read())
    assert data_health == data_localstack

@_requires_package
def test_ministack_package_core_importable():
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
def test_ministack_package_services_importable():
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
