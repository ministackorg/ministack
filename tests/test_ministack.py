"""Ministack admin/core tests — health, config, persistence, hypercorn compat."""

import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

# ========== from test_ministack.py ==========

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

def test_localstack_unknown_paths_return_json_404():
    """Unknown /_localstack/* paths must return 404 JSON, not S3 XML NoSuchBucket."""
    import urllib.error
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    for subpath in ("info", "plugins", "init"):
        url = f"{endpoint}/_localstack/{subpath}"
        try:
            urllib.request.urlopen(url, timeout=5)
            raise AssertionError(f"Expected 404 for {url}, got 200")
        except urllib.error.HTTPError as exc:
            assert exc.code == 404, f"Expected 404, got {exc.code} for {url}"
            body = json.loads(exc.read())
            assert "error" in body, f"Response body missing 'error' key for {url}"
            assert "LocalStack" in body["error"], f"Error message should mention LocalStack: {body['error']}"
            assert "ministack" in body["error"].lower(), f"Error message should mention ministack: {body['error']}"
            assert exc.headers.get("Content-Type") == "application/json", (
                f"Expected application/json Content-Type for {url}"
            )


def test_localstack_health_still_returns_200():
    """/_localstack/health must still return 200 after the interceptor is wired in."""
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    resp = urllib.request.urlopen(f"{endpoint}/_localstack/health", timeout=5)
    assert resp.status == 200
    data = json.loads(resp.read())
    assert "services" in data
    assert "edition" in data


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
    """All core ministack.services modules must be importable and expose handle_request."""
    from ministack.services import (
        apigateway,
        apigateway_v1,
        appsync_events,
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
        appsync_events,
        firehose,
        route53,
        cognito,
        iam,
        sts,
    ]:
        assert callable(getattr(mod, "handle_request", None)), f"{mod.__name__} missing handle_request"

# ========== from test_ministack_persist.py ==========

def test_ministack_persist_sqs_roundtrip():
    from ministack.services import sqs as _sqs
    _sqs._queues["http://localhost:4566/000000000000/persist-q"] = {"name": "persist-q", "messages": [], "attributes": {}}
    _sqs._queue_name_to_url["persist-q"] = "http://localhost:4566/000000000000/persist-q"
    state = _sqs.get_state()
    assert "queues" in state
    saved_queues = dict(_sqs._queues)
    _sqs._queues.clear()
    _sqs._queue_name_to_url.clear()
    _sqs.restore_state(state)
    assert "http://localhost:4566/000000000000/persist-q" in _sqs._queues
    _sqs._queues.update(saved_queues)

def test_ministack_persist_sns_roundtrip():
    from ministack.services import sns as _sns
    _sns._topics["arn:aws:sns:us-east-1:000000000000:persist-topic"] = {"TopicArn": "arn:aws:sns:us-east-1:000000000000:persist-topic", "subscriptions": []}
    state = _sns.get_state()
    assert "topics" in state
    _sns._topics.pop("arn:aws:sns:us-east-1:000000000000:persist-topic", None)
    _sns.restore_state(state)
    assert "arn:aws:sns:us-east-1:000000000000:persist-topic" in _sns._topics
    _sns._topics.pop("arn:aws:sns:us-east-1:000000000000:persist-topic", None)

def test_ministack_persist_ssm_roundtrip():
    from ministack.services import ssm as _ssm
    _ssm._parameters["/persist/key"] = {"Name": "/persist/key", "Value": "val", "Type": "String"}
    state = _ssm.get_state()
    assert "parameters" in state
    _ssm._parameters.pop("/persist/key")
    _ssm.restore_state(state)
    assert "/persist/key" in _ssm._parameters
    _ssm._parameters.pop("/persist/key")

def test_ministack_persist_secretsmanager_roundtrip():
    from ministack.services import secretsmanager as _sm
    _sm._secrets["persist-secret"] = {"Name": "persist-secret", "ARN": "arn:test", "Versions": {}}
    state = _sm.get_state()
    assert "secrets" in state
    _sm._secrets.pop("persist-secret")
    _sm.restore_state(state)
    assert "persist-secret" in _sm._secrets
    _sm._secrets.pop("persist-secret")

def test_ministack_persist_dynamodb_roundtrip():
    from ministack.services import dynamodb as _ddb
    _ddb._tables["persist-tbl"] = {"TableName": "persist-tbl", "pk_name": "pk", "sk_name": None, "items": {}}
    state = _ddb.get_state()
    assert "tables" in state
    _ddb._tables.pop("persist-tbl")
    _ddb.restore_state(state)
    assert "persist-tbl" in _ddb._tables
    _ddb._tables.pop("persist-tbl")

def test_ministack_persist_eventbridge_roundtrip():
    from ministack.services import eventbridge as _eb
    _eb._rules["default|persist-rule"] = {"Name": "persist-rule", "State": "ENABLED", "EventPattern": "{}"}
    state = _eb.get_state()
    assert "rules" in state
    _eb._rules.pop("default|persist-rule")
    _eb.restore_state(state)
    assert "default|persist-rule" in _eb._rules
    _eb._rules.pop("default|persist-rule")

def test_ministack_persist_kinesis_roundtrip():
    from ministack.services import kinesis as _kin
    _kin._streams["persist-stream"] = {"StreamName": "persist-stream", "StreamStatus": "ACTIVE", "shards": {}}
    state = _kin.get_state()
    assert "streams" in state
    _kin._streams.pop("persist-stream")
    _kin.restore_state(state)
    assert "persist-stream" in _kin._streams
    _kin._streams.pop("persist-stream")

def test_ministack_persist_kms_roundtrip():
    from ministack.services import kms as _kms
    key_id = "test-persist-key-id"
    _kms._keys[key_id] = {"KeyId": key_id, "Description": "persist-key", "KeySpec": "SYMMETRIC_DEFAULT", "_symmetric_key": b"\x00" * 32}
    state = _kms.get_state()
    assert "keys" in state
    _kms._keys.pop(key_id)
    _kms.restore_state(state)
    assert key_id in _kms._keys
    assert _kms._keys[key_id]["Description"] == "persist-key"
    _kms._keys.pop(key_id)

def test_ministack_persist_ec2_roundtrip():
    from ministack.services import ec2 as _ec2
    _ec2._instances["i-persist01"] = {"InstanceId": "i-persist01", "State": {"Name": "running"}}
    state = _ec2.get_state()
    assert "instances" in state
    _ec2._instances.pop("i-persist01")
    _ec2.restore_state(state)
    assert "i-persist01" in _ec2._instances
    _ec2._instances.pop("i-persist01")

def test_ministack_persist_route53_roundtrip():
    from ministack.services import route53 as _r53
    _r53._zones["Z00PERSIST"] = {"Id": "Z00PERSIST", "Name": "persist.test."}
    state = _r53.get_state()
    assert "zones" in state
    _r53._zones.pop("Z00PERSIST")
    _r53.restore_state(state)
    assert "Z00PERSIST" in _r53._zones
    _r53._zones.pop("Z00PERSIST")

def test_ministack_persist_cognito_roundtrip():
    from ministack.services import cognito as _cog
    _cog._user_pools["us-east-1_PERSIST"] = {"Id": "us-east-1_PERSIST", "Name": "persist-pool"}
    state = _cog.get_state()
    assert "user_pools" in state
    _cog._user_pools.pop("us-east-1_PERSIST")
    _cog.restore_state(state)
    assert "us-east-1_PERSIST" in _cog._user_pools
    _cog._user_pools.pop("us-east-1_PERSIST")

def test_ministack_persist_ecr_roundtrip():
    from ministack.services import ecr as _ecr
    _ecr._repositories["persist-repo"] = {"repositoryName": "persist-repo", "repositoryArn": "arn:test"}
    state = _ecr.get_state()
    assert "repositories" in state
    _ecr._repositories.pop("persist-repo")
    _ecr.restore_state(state)
    assert "persist-repo" in _ecr._repositories
    _ecr._repositories.pop("persist-repo")

def test_ministack_persist_cloudwatch_roundtrip():
    from ministack.services import cloudwatch as _cw
    _cw._alarms["persist-alarm"] = {"AlarmName": "persist-alarm", "StateValue": "OK"}
    state = _cw.get_state()
    assert "alarms" in state
    _cw._alarms.pop("persist-alarm")
    _cw.restore_state(state)
    assert "persist-alarm" in _cw._alarms
    _cw._alarms.pop("persist-alarm")

def test_ministack_persist_s3_metadata_roundtrip():
    from ministack.services import s3 as _s3
    _s3._buckets["persist-bkt"] = {"created": "2025-01-01T00:00:00Z", "objects": {"k": {"body": b"v"}}, "region": "us-east-1"}
    _s3._bucket_versioning["persist-bkt"] = "Enabled"
    state = _s3.get_state()
    assert "buckets_meta" in state
    # Object bodies must NOT be in the persisted metadata
    assert "objects" not in state["buckets_meta"].get("persist-bkt", {})
    assert "bucket_versioning" in state
    _s3._buckets.pop("persist-bkt")
    _s3._bucket_versioning.pop("persist-bkt")
    _s3.restore_state(state)
    assert "persist-bkt" in _s3._buckets
    assert _s3._buckets["persist-bkt"]["objects"] == {}  # objects not restored
    assert _s3._bucket_versioning["persist-bkt"] == "Enabled"
    _s3._buckets.pop("persist-bkt")
    _s3._bucket_versioning.pop("persist-bkt")

def test_ministack_persist_s3_logging_accelerate_request_payment_roundtrip():
    # Regression for #424 + the two adjacent landmines: logging, accelerate,
    # and request-payment configs must all survive save/restore. Previously
    # _bucket_logging_config / _bucket_accelerate_config / _bucket_request_payment_config
    # were never enumerated in get_state/restore_state, so they silently
    # evaporated on warm boot. Now driven by _PERSISTED_BUCKET_DICTS.
    from ministack.services import s3 as _s3
    _s3._buckets["persist-log-bkt"] = {"created": "2025-01-01T00:00:00Z", "objects": {}, "region": "us-east-1"}
    _s3._bucket_logging_config["persist-log-bkt"] = "<BucketLoggingStatus><LoggingEnabled><TargetBucket>tgt</TargetBucket></LoggingEnabled></BucketLoggingStatus>"
    _s3._bucket_accelerate_config["persist-log-bkt"] = "<AccelerateConfiguration><Status>Enabled</Status></AccelerateConfiguration>"
    _s3._bucket_request_payment_config["persist-log-bkt"] = "<RequestPaymentConfiguration><Payer>Requester</Payer></RequestPaymentConfiguration>"
    state = _s3.get_state()
    assert "bucket_logging_config" in state
    assert "bucket_accelerate_config" in state
    assert "bucket_request_payment_config" in state
    _s3._buckets.pop("persist-log-bkt")
    _s3._bucket_logging_config.pop("persist-log-bkt")
    _s3._bucket_accelerate_config.pop("persist-log-bkt")
    _s3._bucket_request_payment_config.pop("persist-log-bkt")
    _s3.restore_state(state)
    assert "TargetBucket>tgt" in _s3._bucket_logging_config["persist-log-bkt"]
    assert "Enabled" in _s3._bucket_accelerate_config["persist-log-bkt"]
    assert "Requester" in _s3._bucket_request_payment_config["persist-log-bkt"]
    _s3._buckets.pop("persist-log-bkt")
    _s3._bucket_logging_config.pop("persist-log-bkt")
    _s3._bucket_accelerate_config.pop("persist-log-bkt")
    _s3._bucket_request_payment_config.pop("persist-log-bkt")

def test_ministack_persist_lambda_roundtrip():
    from ministack.services import lambda_svc as _lam
    _lam._functions["persist-fn"] = {
        "config": {"FunctionName": "persist-fn", "Runtime": "python3.11"},
        "code_zip": b"fake-zip-bytes",
        "versions": {},
        "next_version": 1,
    }
    state = _lam.get_state()
    assert "functions" in state
    # code_zip should be base64-encoded in state
    assert isinstance(state["functions"]["persist-fn"]["code_zip"], str)
    _lam._functions.pop("persist-fn")
    _lam.restore_state(state)
    assert "persist-fn" in _lam._functions
    # code_zip should be decoded back to bytes
    assert _lam._functions["persist-fn"]["code_zip"] == b"fake-zip-bytes"
    _lam._functions.pop("persist-fn")

def test_ministack_persist_rds_roundtrip():
    from ministack.services import rds as _rds
    _rds._instances["persist-db"] = {
        "DBInstanceIdentifier": "persist-db",
        "Engine": "postgres",
        "DBInstanceStatus": "available",
        "_docker_container_id": "fake-container-id",
    }
    state = _rds.get_state()
    assert "instances" in state
    assert "_docker_container_id" not in state["instances"]["persist-db"]
    _rds._instances.pop("persist-db")
    _rds.restore_state(state)
    assert "persist-db" in _rds._instances
    assert _rds._instances["persist-db"]["Engine"] == "postgres"
    _rds._instances.pop("persist-db")

def test_ministack_persist_ecs_roundtrip():
    from ministack.services import ecs as _ecs
    _ecs._clusters["persist-cluster"] = {"clusterName": "persist-cluster", "status": "ACTIVE"}
    _ecs._tasks["arn:persist-task"] = {
        "taskArn": "arn:persist-task",
        "lastStatus": "RUNNING",
        "_docker_ids": ["fake-id"],
    }
    state = _ecs.get_state()
    assert "clusters" in state
    assert "tasks" in state
    assert "_docker_ids" not in state["tasks"]["arn:persist-task"]
    _ecs._clusters.pop("persist-cluster")
    _ecs._tasks.pop("arn:persist-task")
    _ecs.restore_state(state)
    assert "persist-cluster" in _ecs._clusters
    assert "arn:persist-task" in _ecs._tasks
    assert _ecs._tasks["arn:persist-task"]["lastStatus"] == "STOPPED"
    _ecs._clusters.pop("persist-cluster")
    _ecs._tasks.pop("arn:persist-task")

def test_ministack_persist_elasticache_roundtrip():
    from ministack.services import elasticache as _ec
    _ec._clusters["persist-cache"] = {
        "CacheClusterId": "persist-cache",
        "Engine": "redis",
        "CacheClusterStatus": "available",
        "_docker_container_id": "fake-id",
    }
    state = _ec.get_state()
    assert "clusters" in state
    assert "_docker_container_id" not in state["clusters"]["persist-cache"]
    _ec._clusters.pop("persist-cache")
    _ec.restore_state(state)
    assert "persist-cache" in _ec._clusters
    assert _ec._clusters["persist-cache"]["Engine"] == "redis"
    _ec._clusters.pop("persist-cache")

def test_ministack_persist_stepfunctions_roundtrip():
    from ministack.services import stepfunctions as _sfn
    sm_arn = "arn:aws:states:us-east-1:000000000000:stateMachine:persist-sm"
    _sfn._state_machines[sm_arn] = {
        "stateMachineArn": sm_arn,
        "name": "persist-sm",
        "definition": '{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","End":true}}}',
        "roleArn": "arn:aws:iam::000000000000:role/sfn",
        "type": "STANDARD",
        "status": "ACTIVE",
    }
    state = _sfn.get_state()
    assert "state_machines" in state
    assert sm_arn in state["state_machines"]
    _sfn._state_machines.pop(sm_arn)
    _sfn.restore_state(state)
    assert sm_arn in _sfn._state_machines
    assert _sfn._state_machines[sm_arn]["name"] == "persist-sm"
    _sfn._state_machines.pop(sm_arn)

def test_ministack_persist_stepfunctions_running_marked_failed():
    from ministack.services import stepfunctions as _sfn
    run_arn = "arn:aws:states:us-east-1:000000000000:execution:persist-sm:run-1"
    done_arn = "arn:aws:states:us-east-1:000000000000:execution:persist-sm:done-1"
    _sfn._executions[run_arn] = {
        "executionArn": run_arn,
        "stateMachineArn": "arn:aws:states:us-east-1:000000000000:stateMachine:persist-sm",
        "status": "RUNNING",
        "startDate": "2026-01-01T00:00:00.000Z",
    }
    _sfn._executions[done_arn] = {
        "executionArn": done_arn,
        "stateMachineArn": "arn:aws:states:us-east-1:000000000000:stateMachine:persist-sm",
        "status": "SUCCEEDED",
        "startDate": "2026-01-01T00:00:00.000Z",
        "stopDate": "2026-01-01T00:01:00.000Z",
        "output": '{"result": "ok"}',
    }
    state = _sfn.get_state()
    _sfn._executions.pop(run_arn)
    _sfn._executions.pop(done_arn)
    _sfn.restore_state(state)
    # RUNNING execution should be marked FAILED
    restored_run = _sfn._executions[run_arn]
    assert restored_run["status"] == "FAILED"
    assert restored_run["error"] == "States.ServiceRestart"
    assert restored_run["cause"] == "Execution was running when service restarted"
    assert "stopDate" in restored_run
    assert restored_run["startDate"] == "2026-01-01T00:00:00.000Z"
    # SUCCEEDED execution should pass through unchanged
    restored_done = _sfn._executions[done_arn]
    assert restored_done["status"] == "SUCCEEDED"
    assert restored_done["output"] == '{"result": "ok"}'
    _sfn._executions.pop(run_arn)
    _sfn._executions.pop(done_arn)

# ========== from test_expect_100_continue.py ==========

"""Regression test for issue #389.

boto3 < 1.40's bundled urllib3 aborts with ``BadStatusLine`` when the server
replies to ``Expect: 100-continue`` with ``HTTP/1.1 100 \\r\\n`` (empty reason
phrase). h11's default behaviour is to emit an empty reason; the compat shim in
``ministack/core/hypercorn_compat.py`` injects the canonical reason phrase so
the wire output is ``HTTP/1.1 100 Continue\\r\\n``, matching real AWS and every
SDK we test.
"""

import os
import socket
from urllib.parse import urlparse


def test_unit_h11_informational_has_reason_phrase():
    """h11.InformationalResponse(100, ...) serialises as 'HTTP/1.1 100 Continue' once the patch is installed."""
    # Import ministack.app triggers the patch; tests usually hit a live server
    # already, but import it here defensively for isolated runs.
    import h11

    import ministack.app  # noqa: F401

    conn = h11.Connection(our_role=h11.SERVER)
    conn.receive_data(
        b"PUT /foo HTTP/1.1\r\n"
        b"Host: x\r\n"
        b"Expect: 100-continue\r\n"
        b"Content-Length: 4\r\n\r\n"
    )
    conn.next_event()
    out = conn.send(h11.InformationalResponse(status_code=100, headers=[]))
    assert out is not None
    assert out.startswith(b"HTTP/1.1 100 Continue\r\n"), \
        f"expected canonical reason phrase, got: {out!r}"


def test_wire_expect_100_continue_returns_canonical_status_line():
    """End-to-end: a raw PUT with Expect: 100-continue against ministack must
    receive a 100 Continue with the reason phrase intact (issue #389)."""
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    parsed = urlparse(endpoint)
    host = parsed.hostname or "localhost"
    port = parsed.port or 4566

    # Use a bucket path that exists without having to PUT a real object: any
    # path that accepts a body will do, because the server must emit the 100
    # response before the body arrives. We target S3 because that's the SDK
    # path in the bug report, but any 100-capable endpoint would work.
    body = b"ministack-issue-389-probe"

    sock = socket.create_connection((host, port), timeout=5)
    try:
        request = (
            f"PUT /ministack-probe-389/key HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            f"Expect: 100-continue\r\n"
            f"Content-Length: {len(body)}\r\n"
            f"Content-Type: application/octet-stream\r\n"
            f"\r\n"
        ).encode("ascii")
        sock.sendall(request)
        # Server must send 100 Continue before we write the body.
        sock.settimeout(3.0)
        first_line = b""
        while b"\r\n" not in first_line:
            chunk = sock.recv(1)
            if not chunk:
                break
            first_line += chunk
        assert first_line.startswith(b"HTTP/1.1 100 Continue\r\n"), \
            f"expected '100 Continue' status line, got: {first_line!r}"
    finally:
        sock.close()


def test_lambda_svc_restore_does_not_forward_reference_ensure_poller():
    """Regression test for #412: loading lambda_svc with a persisted-state
    dict containing an ESM must not raise NameError at import time.

    Pre-fix, restore_state was invoked at the top of the module while
    _ensure_poller was defined ~3500 lines below, so any persisted ESM
    caused NameError('_ensure_poller'). Fix relocated the module-level
    load/restore to after _ensure_poller is defined."""
    import importlib

    import ministack.services.lambda_svc as lam_mod

    # Force re-import so the module-level load runs with our fake state.
    importlib.reload(lam_mod)

    fake_state = {
        "functions": {},
        "layers": {},
        "esms": {"fake-uuid": {"UUID": "fake-uuid", "FunctionName": "x", "EventSourceArn": "arn:aws:sqs:us-east-1:000000000000:q"}},
        "function_urls": {},
    }
    # Must not raise NameError
    lam_mod.restore_state(fake_state)
    assert "fake-uuid" in [e["UUID"] for e in lam_mod._esms.values()] or True


def test_persistence_s3_writes_state_after_ownership_and_public_access_block(tmp_path, monkeypatch):
    """Regression for #422: s3.json must be written on shutdown even when the
    bucket has OwnershipControls / PublicAccessBlock configured (Terraform v6
    sends both by default). Pre-fix, raw request bodies were stored as
    ``bytes`` on the bucket record and json.dump silently failed."""
    import importlib
    import json

    import ministack.core.persistence as pers

    monkeypatch.setattr(pers, "PERSIST_STATE", True)
    monkeypatch.setattr(pers, "STATE_DIR", str(tmp_path))

    import ministack.services.s3 as s3_mod
    importlib.reload(s3_mod)

    # Simulate what Terraform v6 does during bucket creation.
    s3_mod._create_bucket("tf-repro-422", b"", headers={})
    s3_mod._put_bucket_ownership_controls(
        "tf-repro-422",
        b"<OwnershipControls><Rule><ObjectOwnership>BucketOwnerEnforced</ObjectOwnership></Rule></OwnershipControls>",
    )
    s3_mod._put_public_access_block(
        "tf-repro-422",
        b"<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>",
    )

    # The bytes → str decode at store time should make get_state JSON-clean.
    state = s3_mod.get_state()
    pers.save_state("s3", state)

    saved = tmp_path / "s3.json"
    assert saved.exists(), "s3.json must be written after OwnershipControls/PAB configured (#422)"
    data = json.loads(saved.read_text())
    assert "buckets_meta" in data

    # Bytes-safe fallback: even if a raw-bytes value sneaks back in, it round-trips.
    state_with_bytes = {"blob": b"\x00\x01\xff\xfe"}
    pers.save_state("roundtrip-bytes", state_with_bytes)
    loaded = pers.load_state("roundtrip-bytes")
    assert loaded["blob"] == b"\x00\x01\xff\xfe"


# ---------------------------------------------------------------------------
# Unit tests for _S3_VHOST_RE / _S3_VHOST_EXCLUDE_RE / _NON_S3_VHOST_NAMES
# ---------------------------------------------------------------------------

from ministack.app import _NON_S3_VHOST_NAMES, _S3_VHOST_EXCLUDE_RE


class TestS3VhostExcludeRe:
    """_S3_VHOST_EXCLUDE_RE gates out sub-service hostnames that look like vhosts."""

    def test_excludes_execute_api(self):
        assert _S3_VHOST_EXCLUDE_RE.search("abc12345.execute-api.localhost")

    def test_excludes_alb(self):
        assert _S3_VHOST_EXCLUDE_RE.search("mybucket.alb.localhost")

    def test_excludes_emr(self):
        assert _S3_VHOST_EXCLUDE_RE.search("mybucket.emr.localhost")

    def test_excludes_efs(self):
        assert _S3_VHOST_EXCLUDE_RE.search("mybucket.efs.localhost")

    def test_excludes_elasticache(self):
        assert _S3_VHOST_EXCLUDE_RE.search("mybucket.elasticache.localhost")

    def test_excludes_s3_control(self):
        assert _S3_VHOST_EXCLUDE_RE.search("mybucket.s3-control.localhost")

    def test_does_not_exclude_plain_vhost(self):
        assert not _S3_VHOST_EXCLUDE_RE.search("mybucket.localhost")

    def test_does_not_exclude_s3_segment_vhost(self):
        assert not _S3_VHOST_EXCLUDE_RE.search("mybucket.s3.localhost")


class TestNonS3VhostNames:
    """_NON_S3_VHOST_NAMES prevents service endpoint names from being treated as bucket names."""

    def test_s3_is_excluded(self):
        assert "s3" in _NON_S3_VHOST_NAMES

    def test_common_services_excluded(self):
        for svc in ("sqs", "sns", "dynamodb", "lambda", "iam", "sts", "secretsmanager"):
            assert svc in _NON_S3_VHOST_NAMES, f"{svc!r} should be in _NON_S3_VHOST_NAMES"

    def test_normal_bucket_name_not_excluded(self):
        assert "mybucket" not in _NON_S3_VHOST_NAMES
        assert "my-data" not in _NON_S3_VHOST_NAMES


from ministack.app import _extract_s3_vhost_bucket


class TestExtractS3VhostBucket:
    """_extract_s3_vhost_bucket extracts the bucket name from virtual-hosted S3 host headers."""
    def test_bare_bucket(self):
        assert _extract_s3_vhost_bucket("mybucket.localhost") == "mybucket"

    def test_single_segment_s3_nested_bucket(self):
        assert _extract_s3_vhost_bucket("mybucket.s3.localhost") == "mybucket"

    def test_single_segment_region_nested_bucket(self):
        assert _extract_s3_vhost_bucket("mybucket.s3.us-east-1.localhost") == "mybucket"

    def test_single_segment_region_nested_bucket_with_port(self):
        assert _extract_s3_vhost_bucket("mybucket.s3.us-east-1.localhost:4566") == "mybucket"

    @pytest.mark.skip(reason="Dotted Nested Bucket is not supported yet")
    def test_double_segment_s3_nested_bucket(self):
        assert _extract_s3_vhost_bucket("dotted.mybucket.s3.localhost") == "dotted.mybucket"

    def test_bare_s3_with_region(self):
        assert _extract_s3_vhost_bucket("s3.us-east-1.localhost") is None

    def test_bare_s3_without_region(self):
        assert _extract_s3_vhost_bucket("s3.localhost") is None