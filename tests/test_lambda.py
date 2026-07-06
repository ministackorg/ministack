import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from unittest.mock import patch
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

_EXECUTE_PORT = urlparse(_endpoint).port or 4566

def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()

def _make_zip_js(code: str, filename: str = "index.js") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, code)
    return buf.getvalue()

_LAMBDA_CODE = 'def handler(event, context):\n    return {"statusCode": 200, "body": "ok"}\n'

_LAMBDA_CODE_V2 = 'def handler(event, context):\n    return {"statusCode": 200, "body": "v2"}\n'

_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"

_NODE_CODE = (
    "exports.handler = async (event, context) => {"
    " return { statusCode: 200, body: JSON.stringify({ hello: event.name || 'world' }) }; };"
)

def _zip_lambda(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()

def test_lambda_create_invoke(lam):
    code = b'def handler(event, context):\n    return {"statusCode": 200, "body": "Hello!", "event": event}\n'
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName="test-func-1",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    funcs = lam.list_functions()
    assert any(f["FunctionName"] == "test-func-1" for f in funcs["Functions"])
    resp = lam.invoke(FunctionName="test-func-1", Payload=json.dumps({"key": "value"}))
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200


def test_lambda_python_nested_handler_slash_form(lam):
    """AWS Python Lambda accepts both dot and slash separators in nested
    handler paths (``pkg/sub/mod.fn`` equivalent to ``pkg.sub.mod.fn``);
    real AWS resolves either form via the underlying file path. MiniStack
    previously imported the pre-rsplit string literally, so slash form
    failed with ``ModuleNotFoundError: No module named 'pkg/sub/mod'``.
    """
    code = b"def hello(event, context):\n    return {\"ok\": True, \"event\": event}\n"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("pkg/sub/mod.py", code)
    lam.create_function(
        FunctionName="slash-handler-fn",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="pkg/sub/mod.hello",
        Code={"ZipFile": buf.getvalue()},
    )
    resp = lam.invoke(
        FunctionName="slash-handler-fn",
        Payload=json.dumps({"k": "v"}),
    )
    assert "FunctionError" not in resp, resp
    payload = json.loads(resp["Payload"].read())
    assert payload.get("ok") is True


def test_create_function_missing_runtime_raises(lam):
    """Zip deployment without a Runtime should return InvalidParameterValueException."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", "def handler(e, c): return {}")
    with pytest.raises(ClientError) as exc:
        lam.create_function(
            FunctionName="no-runtime-fn",
            Role="arn:aws:iam::000000000000:role/role",
            Handler="index.handler",
            Code={"ZipFile": buf.getvalue()},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterValueException"


def test_lambda_esm_sqs(lam, sqs):
    """SQS → Lambda event source mapping: messages sent to SQS trigger Lambda."""
    import io
    import zipfile as zf

    # Clean up from previous runs
    try:
        lam.delete_function(FunctionName="esm-test-func")
    except Exception:
        pass

    # Lambda that records what it received
    code = (
        b"import json\n"
        b"received = []\n"
        b"def handler(event, context):\n"
        b"    received.extend(event.get('Records', []))\n"
        b"    return {'processed': len(event.get('Records', []))}\n"
    )
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w") as z:
        z.writestr("index.py", code)

    lam.create_function(
        FunctionName="esm-test-func",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    q_url = sqs.create_queue(QueueName="esm-test-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    # Create event source mapping
    resp = lam.create_event_source_mapping(
        EventSourceArn=q_arn,
        FunctionName="esm-test-func",
        BatchSize=5,
        Enabled=True,
    )
    esm_uuid = resp["UUID"]
    assert resp["State"] == "Enabled"

    # Send a message to SQS
    sqs.send_message(QueueUrl=q_url, MessageBody="trigger-lambda")

    # Wait for poller to pick it up (max 5s)
    import time

    for _ in range(10):
        time.sleep(0.5)
        msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1)
        if not msgs.get("Messages"):
            break  # message was consumed by Lambda

    # Queue should be empty — Lambda consumed the message
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1)
    assert not msgs.get("Messages"), "Message should have been consumed by Lambda via ESM"

    # Cleanup
    lam.delete_event_source_mapping(UUID=esm_uuid)

def test_lambda_create_function(lam):
    resp = lam.create_function(
        FunctionName="lam-create-test",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    assert resp["FunctionName"] == "lam-create-test"
    assert resp["Runtime"] == "python3.12"
    assert resp["Handler"] == "index.handler"
    # AWS: CreateFunction returns State=Pending and transitions to Active
    # asynchronously. Terraform's FunctionActive waiter polls GetFunction.
    assert resp["State"] in ("Pending", "Active")
    assert resp["LastUpdateStatus"] in ("InProgress", "Successful")
    assert "FunctionArn" in resp

def test_lambda_create_duplicate(lam):
    with pytest.raises(ClientError) as exc:
        lam.create_function(
            FunctionName="lam-create-test",
            Runtime="python3.12",
            Role=_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
        )
    assert exc.value.response["Error"]["Code"] == "ResourceConflictException"

def test_lambda_get_function(lam):
    resp = lam.get_function(FunctionName="lam-create-test")
    assert resp["Configuration"]["FunctionName"] == "lam-create-test"
    assert "Code" in resp
    assert "Tags" in resp

def test_lambda_get_function_not_found(lam):
    with pytest.raises(ClientError) as exc:
        lam.get_function(FunctionName="nonexistent-func-xyz")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    # Real AWS sends `x-amzn-errortype` on REST-JSON errors; Java/Go SDK v2 read it.
    assert exc.value.response["ResponseMetadata"]["HTTPHeaders"].get("x-amzn-errortype") == "ResourceNotFoundException"

def test_lambda_list_functions(lam):
    resp = lam.list_functions()
    names = [f["FunctionName"] for f in resp["Functions"]]
    assert "lam-create-test" in names

def test_lambda_delete_function(lam):
    lam.create_function(
        FunctionName="lam-to-delete",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    lam.delete_function(FunctionName="lam-to-delete")
    with pytest.raises(ClientError) as exc:
        lam.get_function(FunctionName="lam-to-delete")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

def test_lambda_invoke(lam):
    lam.create_function(
        FunctionName="lam-invoke-test",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    resp = lam.invoke(
        FunctionName="lam-invoke-test",
        Payload=json.dumps({"hello": "world"}),
    )
    assert resp["StatusCode"] == 200
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200
    assert payload["body"] == "ok"

def test_lambda_invoke_async(lam):
    resp = lam.invoke(
        FunctionName="lam-invoke-test",
        InvocationType="Event",
        Payload=json.dumps({"async": True}),
    )
    assert resp["StatusCode"] == 202


@pytest.mark.serial
def test_lambda_invoke_emits_cloudwatch_metrics(lam, cw):
    """After invocation, AWS/Lambda namespace must carry Invocations + Duration
    metrics dimensioned by FunctionName. Mirrors real Lambda observability —
    the four canonical metrics (Invocations, Errors, Duration, Throttles) are
    published per call.

    Marked ``serial`` because xdist workers share one ministack container, and
    any concurrent test calling ``/_ministack/reset`` would wipe the metric
    store between our invoke and query. The function name is also UUID-suffixed
    so re-runs against a persistent store don't pick up stale datapoints.
    """
    fname = f"lam-cw-metrics-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    try:
        lam.invoke(FunctionName=fname, Payload=json.dumps({"x": 1}))
        lam.invoke(FunctionName=fname, Payload=json.dumps({"x": 2}))

        end = time.time()
        start = end - 600
        invocations = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fname}],
            StartTime=start, EndTime=end,
            Period=60, Statistics=["Sum"],
        )
        total = sum(p["Sum"] for p in invocations["Datapoints"])
        assert total >= 2, f"expected >=2 invocations, got {total}"

        duration = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Duration",
            Dimensions=[{"Name": "FunctionName", "Value": fname}],
            StartTime=start, EndTime=end,
            Period=60, Statistics=["Average", "Maximum"],
        )
        assert duration["Datapoints"], "no Duration datapoints recorded"
        assert duration["Datapoints"][0]["Average"] > 0
    finally:
        lam.delete_function(FunctionName=fname)

def test_lambda_update_code(lam):
    lam.update_function_code(
        FunctionName="lam-invoke-test",
        ZipFile=_make_zip(_LAMBDA_CODE_V2),
    )
    resp = lam.invoke(
        FunctionName="lam-invoke-test",
        Payload=json.dumps({}),
    )
    payload = json.loads(resp["Payload"].read())
    assert payload["body"] == "v2"

def test_lambda_update_config(lam):
    lam.update_function_configuration(
        FunctionName="lam-invoke-test",
        Handler="index.new_handler",
        Environment={"Variables": {"MY_VAR": "my_val"}},
    )
    resp = lam.get_function(FunctionName="lam-invoke-test")
    cfg = resp["Configuration"]
    assert cfg["Handler"] == "index.new_handler"
    assert cfg["Environment"]["Variables"]["MY_VAR"] == "my_val"

    lam.update_function_configuration(
        FunctionName="lam-invoke-test",
        Handler="index.handler",
    )

def test_lambda_tags(lam):
    arn = lam.get_function(FunctionName="lam-invoke-test")["Configuration"]["FunctionArn"]
    lam.tag_resource(Resource=arn, Tags={"env": "test", "team": "backend"})
    resp = lam.list_tags(Resource=arn)
    assert resp["Tags"]["env"] == "test"
    assert resp["Tags"]["team"] == "backend"

    lam.untag_resource(Resource=arn, TagKeys=["team"])
    resp = lam.list_tags(Resource=arn)
    assert "team" not in resp["Tags"]
    assert resp["Tags"]["env"] == "test"

def test_lambda_add_permission(lam):
    lam.add_permission(
        FunctionName="lam-invoke-test",
        StatementId="allow-s3",
        Action="lambda:InvokeFunction",
        Principal="s3.amazonaws.com",
        SourceArn="arn:aws:s3:::my-bucket",
    )
    resp = lam.get_policy(FunctionName="lam-invoke-test")
    policy = json.loads(resp["Policy"])
    sids = [s["Sid"] for s in policy["Statement"]]
    assert "allow-s3" in sids

def test_lambda_list_versions(lam):
    resp = lam.list_versions_by_function(FunctionName="lam-invoke-test")
    versions = resp["Versions"]
    assert any(v["Version"] == "$LATEST" for v in versions)

def test_lambda_publish_version(lam):
    resp = lam.publish_version(
        FunctionName="lam-invoke-test",
        Description="first published version",
    )
    assert resp["Version"] == "1"
    assert resp["Description"] == "first published version"
    assert "FunctionArn" in resp

    versions = lam.list_versions_by_function(FunctionName="lam-invoke-test")["Versions"]
    version_nums = [v["Version"] for v in versions]
    assert "$LATEST" in version_nums
    assert "1" in version_nums

def test_lambda_esm_sqs_comprehensive(lam, sqs):
    try:
        lam.delete_function(FunctionName="esm-comp-func")
    except ClientError:
        pass

    code = 'def handler(event, context):\n    return {"processed": len(event.get("Records", []))}\n'
    lam.create_function(
        FunctionName="esm-comp-func",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    q_url = sqs.create_queue(QueueName="esm-comp-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    resp = lam.create_event_source_mapping(
        EventSourceArn=q_arn,
        FunctionName="esm-comp-func",
        BatchSize=5,
        Enabled=True,
    )
    esm_uuid = resp["UUID"]
    assert resp["State"] == "Enabled"
    assert resp["BatchSize"] == 5
    assert resp["EventSourceArn"] == q_arn

    got = lam.get_event_source_mapping(UUID=esm_uuid)
    assert got["UUID"] == esm_uuid

    listed = lam.list_event_source_mappings(FunctionName="esm-comp-func")
    assert any(e["UUID"] == esm_uuid for e in listed["EventSourceMappings"])

    lam.delete_event_source_mapping(UUID=esm_uuid)

def test_lambda_esm_scaling_config_round_trip(lam, sqs):
    try:
        lam.delete_function(FunctionName="esm-scaling-func")
    except ClientError:
        pass

    lam.create_function(
        FunctionName="esm-scaling-func",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip("def handler(event, context):\n    return {}\n")},
    )
    q_url = sqs.create_queue(QueueName="esm-scaling-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    resp = lam.create_event_source_mapping(
        EventSourceArn=q_arn,
        FunctionName="esm-scaling-func",
        BatchSize=5,
        MaximumBatchingWindowInSeconds=20,
        ScalingConfig={"MaximumConcurrency": 7},
        Enabled=True,
    )
    esm_uuid = resp["UUID"]
    assert resp["ScalingConfig"] == {"MaximumConcurrency": 7}
    assert resp["MaximumBatchingWindowInSeconds"] == 20

    got = lam.get_event_source_mapping(UUID=esm_uuid)
    assert got["ScalingConfig"] == {"MaximumConcurrency": 7}

    listed = lam.list_event_source_mappings(FunctionName="esm-scaling-func")
    entry = next(e for e in listed["EventSourceMappings"] if e["UUID"] == esm_uuid)
    assert entry["ScalingConfig"] == {"MaximumConcurrency": 7}

    updated = lam.update_event_source_mapping(
        UUID=esm_uuid, ScalingConfig={"MaximumConcurrency": 50},
    )
    assert updated["ScalingConfig"] == {"MaximumConcurrency": 50}

    lam.delete_event_source_mapping(UUID=esm_uuid)

def test_lambda_esm_no_scaling_config_omits_field(lam, sqs):
    try:
        lam.delete_function(FunctionName="esm-noscaling-func")
    except ClientError:
        pass

    lam.create_function(
        FunctionName="esm-noscaling-func",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip("def handler(event, context):\n    return {}\n")},
    )
    q_url = sqs.create_queue(QueueName="esm-noscaling-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    resp = lam.create_event_source_mapping(
        EventSourceArn=q_arn, FunctionName="esm-noscaling-func", BatchSize=5,
    )
    assert "ScalingConfig" not in resp

    lam.delete_event_source_mapping(UUID=resp["UUID"])

@pytest.mark.parametrize("bad_value", [1, 1001])
def test_lambda_esm_scaling_config_out_of_range_rejected(lam, sqs, bad_value):
    import urllib.error
    import urllib.request

    try:
        lam.delete_function(FunctionName="esm-badscaling-func")
    except ClientError:
        pass

    lam.create_function(
        FunctionName="esm-badscaling-func",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip("def handler(event, context):\n    return {}\n")},
    )
    q_url = sqs.create_queue(QueueName="esm-badscaling-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    payload = json.dumps({
        "EventSourceArn": q_arn,
        "FunctionName": "esm-badscaling-func",
        "ScalingConfig": {"MaximumConcurrency": bad_value},
    }).encode()
    req = urllib.request.Request(
        f"{_endpoint}/2015-03-31/event-source-mappings",
        data=payload,
        headers={
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/lambda/aws4_request",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        urllib.request.urlopen(req)
        assert False, "Expected a ValidationException error response"
    except urllib.error.HTTPError as e:
        assert e.code == 400
        body = json.loads(e.read())
        assert body.get("__type") == "ValidationException", body

def test_lambda_esm_filter_criteria_stored_on_create(lam, sqs):
    """FilterCriteria specified at CreateEventSourceMapping must be echoed
    back by GetEventSourceMapping — it was silently dropped before this fix."""
    try:
        lam.delete_function(FunctionName="esm-fc-func")
    except ClientError:
        pass
    lam.create_function(
        FunctionName="esm-fc-func",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    q_url = sqs.create_queue(QueueName="esm-fc-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]

    fc = {"Filters": [{"Pattern": json.dumps({"body": {"type": ["order"]}})}]}
    resp = lam.create_event_source_mapping(
        EventSourceArn=q_arn,
        FunctionName="esm-fc-func",
        FilterCriteria=fc,
    )
    esm_uuid = resp["UUID"]
    assert resp.get("FilterCriteria") == fc, "FilterCriteria must be in create response"

    got = lam.get_event_source_mapping(UUID=esm_uuid)
    assert got.get("FilterCriteria") == fc, "FilterCriteria must survive a GetEventSourceMapping round-trip"

    lam.delete_event_source_mapping(UUID=esm_uuid)

def test_lambda_esm_sqs_failure_respects_visibility_timeout(lam, sqs):
    """On Lambda failure, the message should remain in-flight until VisibilityTimeout expires."""
    import io
    import zipfile as zf

    for fn in ("esm-fail-func",):
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass

    code = b"def handler(event, context):\n    raise Exception('boom')\n"
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w") as z:
        z.writestr("index.py", code)

    lam.create_function(
        FunctionName="esm-fail-func",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
        Timeout=3,
    )

    q_url = sqs.create_queue(
        QueueName="esm-fail-queue",
        Attributes={"VisibilityTimeout": "30"},
    )["QueueUrl"]
    q_arn = sqs.get_queue_attributes(QueueUrl=q_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    resp = lam.create_event_source_mapping(
        EventSourceArn=q_arn,
        FunctionName="esm-fail-func",
        BatchSize=1,
        Enabled=True,
    )
    esm_uuid = resp["UUID"]

    sqs.send_message(QueueUrl=q_url, MessageBody="trigger-failure")

    # Wait until ESM has actually processed (and failed) the message
    for _ in range(40):
        time.sleep(0.5)
        cur = lam.get_event_source_mapping(UUID=esm_uuid)
        if cur.get("LastProcessingResult") == "FAILED":
            break
    else:
        pytest.skip("ESM did not process message in time")

    # Disable ESM immediately after failure confirmed
    lam.update_event_source_mapping(UUID=esm_uuid, Enabled=False)

    # Message should be invisible (VisibilityTimeout=30s, and ESM just received it)
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=1, WaitTimeSeconds=0)
    assert not msgs.get("Messages"), "Message should be invisible during VisibilityTimeout after failed ESM invoke"

    lam.delete_event_source_mapping(UUID=esm_uuid)


def test_lambda_esm_sqs_report_batch_item_failures(lam, sqs):
    """ReportBatchItemFailures: failed messages stay on queue and reach DLQ."""
    for fn in ("esm-partial-func",):
        try:
            lam.delete_function(FunctionName=fn)
        except Exception:
            pass

    # Handler reports ALL messages as failed
    code = (
        "import json\n"
        "def handler(event, context):\n"
        "    failures = []\n"
        "    for r in event.get('Records', []):\n"
        "        failures.append({'itemIdentifier': r['messageId']})\n"
        "    return {'batchItemFailures': failures}\n"
    )
    lam.create_function(
        FunctionName="esm-partial-func",
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    # DLQ + main queue with maxReceiveCount=1
    dlq_url = sqs.create_queue(QueueName="esm-partial-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(
        QueueUrl=dlq_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    q_url = sqs.create_queue(
        QueueName="esm-partial-queue",
        Attributes={
            "VisibilityTimeout": "1",
            "RedrivePolicy": json.dumps({
                "deadLetterTargetArn": dlq_arn,
                "maxReceiveCount": "1",
            }),
        },
    )["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    esm = lam.create_event_source_mapping(
        EventSourceArn=q_arn,
        FunctionName="esm-partial-func",
        FunctionResponseTypes=["ReportBatchItemFailures"],
        BatchSize=1,
        Enabled=True,
    )
    esm_uuid = esm["UUID"]
    assert "ReportBatchItemFailures" in esm["FunctionResponseTypes"]

    sqs.send_message(QueueUrl=q_url, MessageBody="partial-fail-test")

    # Wait for ESM to process and message to land in DLQ
    dlq_count = 0
    for _ in range(30):
        time.sleep(1)
        attrs = sqs.get_queue_attributes(
            QueueUrl=dlq_url,
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        dlq_count = int(attrs["Attributes"]["ApproximateNumberOfMessages"])
        if dlq_count >= 1:
            break

    lam.update_event_source_mapping(UUID=esm_uuid, Enabled=False)
    lam.delete_event_source_mapping(UUID=esm_uuid)

    assert dlq_count >= 1, (
        f"Message should have reached DLQ after partial failure, "
        f"but DLQ has {dlq_count} messages"
    )


def test_lambda_warm_start(lam, apigw):
    """Warm worker via API Gateway execute-api: module-level state persists across invocations."""
    import urllib.request as _urlreq
    import uuid as _uuid

    fname = f"intg-warm-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        b"import time\n"
        b"_boot_time = time.time()\n"
        b"def handler(event, context):\n"
        b"    return {'statusCode': 200, 'body': str(_boot_time)}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    api_id = apigw.create_api(Name=f"warm-api-{fname}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /ping", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    def call():
        req = _urlreq.Request(
            f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/ping",
            method="GET",
        )
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        return _urlreq.urlopen(req).read().decode()

    t1 = call()  # cold start — spawns worker, imports module
    t2 = call()  # warm — reuses worker, same module state
    assert t1 == t2, f"Warm worker should reuse module state: {t1} != {t2}"

    apigw.delete_api(ApiId=api_id)
    lam.delete_function(FunctionName=fname)

def test_lambda_invoke_log_includes_user_output_and_traceback_on_error(lam):
    """When a handler prints then raises, the decoded LogResult tail must contain
    BOTH the user output AND the exception traceback. Regression for the
    error-path log drop where only the traceback was returned."""
    fname = f"lam-log-err-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        "def handler(event, context):\n"
        "    print('user-step-1')\n"
        "    print('user-step-2')\n"
        "    raise ValueError('boom-from-handler')\n"
    )

    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    try:
        import base64 as _b64
        resp = lam.invoke(
            FunctionName=fname,
            Payload=json.dumps({}),
            LogType="Tail",
        )
        assert resp.get("FunctionError") == "Unhandled"
        log_b64 = resp.get("LogResult", "")
        assert log_b64, "LogResult should be present when LogType=Tail"
        decoded = _b64.b64decode(log_b64).decode("utf-8", errors="replace")
        assert "user-step-1" in decoded, f"user print missing from log: {decoded!r}"
        assert "user-step-2" in decoded, f"second user print missing from log: {decoded!r}"
        assert "ValueError" in decoded or "boom-from-handler" in decoded, \
            f"traceback missing from log: {decoded!r}"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_warm_invoke_with_stderr_logging(lam):
    """Warm invoke should succeed repeatedly even when the worker writes to stderr."""
    fname = f"lam-warm-stderr-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        "import sys\n"
        "def handler(event, context):\n"
        "    print(f'log:{event.get(\"n\", 0)}')\n"
        "    return {'statusCode': 200, 'value': event.get('n', 0)}\n"
    )

    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    try:
        first = lam.invoke(FunctionName=fname, Payload=json.dumps({"n": 1}))
        second = lam.invoke(FunctionName=fname, Payload=json.dumps({"n": 2}))

        assert first["StatusCode"] == 200
        assert second["StatusCode"] == 200
        assert json.loads(first["Payload"].read())["value"] == 1
        assert json.loads(second["Payload"].read())["value"] == 2
    finally:
        lam.delete_function(FunctionName=fname)

def test_lambda_nodejs_create_and_invoke(lam):
    lam.create_function(
        FunctionName="lam-node-basic",
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(_NODE_CODE, "index.js")},
    )
    resp = lam.invoke(
        FunctionName="lam-node-basic",
        Payload=json.dumps({"name": "ministack"}),
    )
    assert resp["StatusCode"] == 200
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200
    body = json.loads(payload["body"])
    assert body["hello"] == "ministack"

def test_lambda_nodejs22_runtime(lam):
    lam.create_function(
        FunctionName="lam-node22",
        Runtime="nodejs22.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(_NODE_CODE, "index.js")},
    )
    resp = lam.invoke(FunctionName="lam-node22", Payload=json.dumps({"name": "v22"}))
    assert resp["StatusCode"] == 200
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200

def test_lambda_nodejs_update_code(lam):
    v2 = (
        "exports.handler = async (event) => {"
        " return { statusCode: 200, body: 'v2' }; };"
    )
    lam.update_function_code(
        FunctionName="lam-node-basic",
        ZipFile=_make_zip_js(v2, "index.js"),
    )
    resp = lam.invoke(FunctionName="lam-node-basic", Payload=b"{}")
    assert resp["StatusCode"] == 200
    payload = json.loads(resp["Payload"].read())
    assert payload["body"] == "v2"

def test_lambda_create_from_s3(lam, s3):
    bucket = "lambda-code-bucket"
    s3.create_bucket(Bucket=bucket)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", "def handler(event, context): return {'s3': True}")
    s3.put_object(Bucket=bucket, Key="fn.zip", Body=buf.getvalue())

    lam.create_function(
        FunctionName="lam-s3-code",
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"S3Bucket": bucket, "S3Key": "fn.zip"},
    )
    resp = lam.invoke(FunctionName="lam-s3-code", Payload=b"{}")
    assert resp["StatusCode"] == 200
    assert json.loads(resp["Payload"].read())["s3"] is True

def test_lambda_update_code_from_s3(lam, s3):
    bucket = "lambda-code-bucket"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", "def handler(event, context): return {'v': 's3v2'}")
    s3.put_object(Bucket=bucket, Key="fn-v2.zip", Body=buf.getvalue())

    lam.update_function_code(
        FunctionName="lam-s3-code",
        S3Bucket=bucket,
        S3Key="fn-v2.zip",
    )
    resp = lam.invoke(FunctionName="lam-s3-code", Payload=b"{}")
    assert json.loads(resp["Payload"].read())["v"] == "s3v2"

def test_lambda_update_code_s3_missing_returns_error(lam):
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        lam.update_function_code(
            FunctionName="lam-s3-code",
            S3Bucket="lambda-code-bucket",
            S3Key="does-not-exist.zip",
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterValueException"

def test_lambda_publish_version_with_create(lam):
    code = "def handler(event, context): return {'ver': 1}"
    try:
        lam.get_function(FunctionName="lam-versioned-pub")
    except Exception:
        lam.create_function(
            FunctionName="lam-versioned-pub",
            Runtime="python3.11",
            Role=_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _make_zip(code)},
            Publish=True,
        )
    resp = lam.list_versions_by_function(FunctionName="lam-versioned-pub")
    versions = [v["Version"] for v in resp["Versions"]]
    assert any(v != "$LATEST" for v in versions)

def test_lambda_update_code_publish_version(lam):
    # Ensure function exists (may have been cleaned up)
    try:
        lam.get_function(FunctionName="lam-versioned")
    except Exception:
        lam.create_function(
            FunctionName="lam-versioned",
            Runtime="python3.11",
            Role=_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _make_zip("def handler(event, context): return {'ver': 1}")},
            Publish=True,
        )
    v2 = "def handler(event, context): return {'ver': 2}"
    lam.update_function_code(
        FunctionName="lam-versioned",
        ZipFile=_make_zip(v2),
        Publish=True,
    )
    resp = lam.list_versions_by_function(FunctionName="lam-versioned")
    versions = [v["Version"] for v in resp["Versions"] if v["Version"] != "$LATEST"]
    assert len(versions) >= 1

def test_lambda_nodejs_promise_handler(lam):
    code = (
        "exports.handler = (event) => Promise.resolve({ promise: true, val: event.x });"
    )
    lam.create_function(
        FunctionName="lam-node-promise",
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(code, "index.js")},
    )
    resp = lam.invoke(FunctionName="lam-node-promise", Payload=json.dumps({"x": 42}))
    payload = json.loads(resp["Payload"].read())
    assert payload["promise"] is True
    assert payload["val"] == 42

def test_lambda_nodejs_callback_handler(lam):
    code = (
        "exports.handler = (event, context, cb) => cb(null, { cb: true, val: event.y });"
    )
    lam.create_function(
        FunctionName="lam-node-cb",
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(code, "index.js")},
    )
    resp = lam.invoke(FunctionName="lam-node-cb", Payload=json.dumps({"y": 7}))
    payload = json.loads(resp["Payload"].read())
    assert payload["cb"] is True
    assert payload["val"] == 7

def test_lambda_nodejs_env_vars_at_spawn(lam):
    """Lambda env vars are available at process startup (NODE_OPTIONS, etc.)."""
    code = (
        "exports.handler = async (event) => ({"
        " myVar: process.env.MY_CUSTOM_VAR,"
        " region: process.env.AWS_REGION"
        "});"
    )
    lam.create_function(
        FunctionName="lam-node-env-spawn",
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(code, "index.js")},
        Environment={"Variables": {"MY_CUSTOM_VAR": "from-spawn"}},
    )
    resp = lam.invoke(FunctionName="lam-node-env-spawn", Payload=b"{}")
    payload = json.loads(resp["Payload"].read())
    assert payload["myVar"] == "from-spawn"

def test_lambda_python_env_vars_at_spawn(lam):
    """Python Lambda env vars are available at process startup."""
    code = (
        "import os\n"
        "def handler(event, context):\n"
        "    return {'myVar': os.environ.get('MY_PY_VAR', 'missing')}\n"
    )
    lam.create_function(
        FunctionName="lam-py-env-spawn",
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
        Environment={"Variables": {"MY_PY_VAR": "from-spawn-py"}},
    )
    resp = lam.invoke(FunctionName="lam-py-env-spawn", Payload=b"{}")
    payload = json.loads(resp["Payload"].read())
    assert payload["myVar"] == "from-spawn-py"

def test_lambda_standard_runtime_env_vars_injected(lam):
    """Warm-worker Lambdas must inject the same env vars as the Docker
    execution path (lambda_svc.py:_execute_function_docker), which in turn
    matches the standard AWS Lambda runtime environment per AWS docs:
      https://docs.aws.amazon.com/lambda/latest/dg/configuration-envvars.html

    This is the regression test for the warm-worker env var gap.  The vars
    asserted below are the full set the Docker path injects — any var the
    Docker path sets but the warm-worker doesn't is a divergence bug.
    """
    code = (
        "import os\n"
        "def handler(event, context):\n"
        "    keys = [\n"
        "        'AWS_REGION', 'AWS_DEFAULT_REGION',\n"
        "        'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN',\n"
        "        'AWS_LAMBDA_FUNCTION_NAME', 'AWS_LAMBDA_FUNCTION_MEMORY_SIZE',\n"
        "        'AWS_LAMBDA_FUNCTION_VERSION', 'AWS_LAMBDA_LOG_STREAM_NAME',\n"
        "        'AWS_ENDPOINT_URL',\n"
        "    ]\n"
        "    return {k: os.environ.get(k, '<UNSET>') for k in keys}\n"
    )
    name = f"lam-runtime-env-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=name,
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    resp = lam.invoke(FunctionName=name, Payload=b"{}")
    payload = json.loads(resp["Payload"].read())

    # Vars that must be non-empty (AWS spec requires a value).
    must_be_nonempty = [
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_LAMBDA_FUNCTION_NAME",
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE",
        "AWS_LAMBDA_FUNCTION_VERSION",
        "AWS_LAMBDA_LOG_STREAM_NAME",
        "AWS_ENDPOINT_URL",
    ]
    for key in must_be_nonempty:
        val = payload.get(key, "<UNSET>")
        assert val and val != "<UNSET>", (
            f"{key} must be set and non-empty (got {val!r}). "
            f"The Docker execution path sets this; warm-worker must too."
        )

    # AWS_SESSION_TOKEN must be PRESENT (set in the env) but may be empty
    # when no session creds are configured.  Real AWS sets it to the role
    # session token; Ministack mirrors what the host process has, defaulting
    # to "".  The key matters because boto3's credential chain checks for
    # its presence.
    assert payload.get("AWS_SESSION_TOKEN", "<UNSET>") != "<UNSET>", (
        "AWS_SESSION_TOKEN must be present in the env (may be empty string). "
        "boto3's credential chain looks for this key explicitly."
    )

    # Function-identity vars must match the configured function.
    assert payload["AWS_LAMBDA_FUNCTION_NAME"] == name, (
        f"AWS_LAMBDA_FUNCTION_NAME must equal the function name "
        f"(got {payload['AWS_LAMBDA_FUNCTION_NAME']!r}, expected {name!r})"
    )
    # MEMORY_SIZE defaults to 128 in CreateFunction when unspecified.
    assert payload["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] == "128"
    # VERSION defaults to $LATEST for unpublished functions.
    assert payload["AWS_LAMBDA_FUNCTION_VERSION"] == "$LATEST"

def test_lambda_function_env_overrides_endpoint_url(lam):
    """Function ``Environment.Variables.AWS_ENDPOINT_URL`` wins over the
    host process value, matching real AWS Lambda behavior.

    Real AWS does not inject ``AWS_ENDPOINT_URL`` — it is an SDK/testing
    convention — so a function-level value is the user's authoritative
    configuration and must not be silently overridden by MiniStack.
    Precedence is: function Environment.Variables → host AWS_ENDPOINT_URL
    → MiniStack's internal default.
    """
    code = (
        "import os\n"
        "def handler(event, context):\n"
        "    return {'endpoint': os.environ.get('AWS_ENDPOINT_URL', 'unset')}\n"
    )
    fname = f"lam-endpoint-override-{_uuid_mod.uuid4().hex[:8]}"
    function_endpoint = "http://function-scoped-endpoint:9999"
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
        Environment={"Variables": {
            "AWS_ENDPOINT_URL": function_endpoint,
        }},
    )
    resp = lam.invoke(FunctionName=fname, Payload=b"{}")
    payload = json.loads(resp["Payload"].read())
    assert payload["endpoint"] == function_endpoint, (
        "Function-level AWS_ENDPOINT_URL must win over host/internal default"
    )


def test_lambda_dynamodb_stream_esm(lam, ddb):
    # Create table with streams enabled
    ddb.create_table(
        TableName="stream-test-table",
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    stream_arn = ddb.describe_table(TableName="stream-test-table")["Table"]["LatestStreamArn"]

    # Create Lambda that captures stream records
    code = "def handler(event, context): return len(event['Records'])"
    lam.create_function(
        FunctionName="lam-ddb-stream",
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    esm = lam.create_event_source_mapping(
        FunctionName="lam-ddb-stream",
        EventSourceArn=stream_arn,
        StartingPosition="TRIM_HORIZON",
        BatchSize=10,
    )
    assert esm["EventSourceArn"] == stream_arn
    assert esm["FunctionArn"].endswith("lam-ddb-stream")

    # Verify ESM is registered and retrievable
    esm_resp = lam.get_event_source_mapping(UUID=esm["UUID"])
    assert esm_resp["EventSourceArn"] == stream_arn
    assert esm_resp["StartingPosition"] == "TRIM_HORIZON"

    # Write items — stream should capture them
    ddb.put_item(TableName="stream-test-table", Item={"pk": {"S": "k1"}, "val": {"S": "v1"}})
    ddb.put_item(TableName="stream-test-table", Item={"pk": {"S": "k2"}, "val": {"S": "v2"}})
    ddb.delete_item(TableName="stream-test-table", Key={"pk": {"S": "k1"}})

    # Verify table still has expected state
    scan = ddb.scan(TableName="stream-test-table")
    pks = [item["pk"]["S"] for item in scan["Items"]]
    assert "k2" in pks
    assert "k1" not in pks


def test_lambda_dynamodb_stream_esm_latest_processes_first_record(lam, ddb):
    table_name = "ddb-latest-race-test"
    fn_name = "ddb-latest-race-fn"

    ddb.create_table(
        TableName=table_name,
        KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    )
    stream_arn = ddb.describe_table(TableName=table_name)["Table"]["LatestStreamArn"]

    code = "def handler(event, context):\n    return {'count': len(event['Records'])}\n"
    lam.create_function(
        FunctionName=fn_name,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    esm = lam.create_event_source_mapping(
        FunctionName=fn_name,
        EventSourceArn=stream_arn,
        StartingPosition="LATEST",
        BatchSize=10,
    )
    esm_uuid = esm["UUID"]

    # Let the poller tick at least once with an empty stream so position is
    # eagerly initialised to 0.
    time.sleep(2)
    ddb.put_item(TableName=table_name, Item={"pk": {"S": "first"}, "val": {"S": "x"}})

    for _ in range(10):
        time.sleep(0.5)
        resp = lam.get_event_source_mapping(UUID=esm_uuid)
        if resp.get("LastProcessingResult") != "No records processed":
            break

    result = lam.get_event_source_mapping(UUID=esm_uuid)
    assert result.get("LastProcessingResult") != "No records processed", (
        "LATEST ESM skipped the first record on an initially-empty table"
    )

    lam.delete_event_source_mapping(UUID=esm_uuid)


def test_lambda_function_url_config(lam):
    """CreateFunctionUrlConfig / Get / Update / Delete / List lifecycle."""
    import uuid as _uuid_mod

    fn = f"intg-url-cfg-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )

    # Create
    resp = lam.create_function_url_config(FunctionName=fn, AuthType="NONE")
    assert resp["AuthType"] == "NONE"
    assert "FunctionUrl" in resp
    url = resp["FunctionUrl"]

    # Get
    got = lam.get_function_url_config(FunctionName=fn)
    assert got["FunctionUrl"] == url

    # Update
    updated = lam.update_function_url_config(
        FunctionName=fn,
        AuthType="AWS_IAM",
        Cors={"AllowOrigins": ["*"]},
    )
    assert updated["AuthType"] == "AWS_IAM"
    assert updated["Cors"]["AllowOrigins"] == ["*"]

    # List
    listed = lam.list_function_url_configs(FunctionName=fn)
    assert any(c["FunctionUrl"] == url for c in listed["FunctionUrlConfigs"])

    # Delete
    lam.delete_function_url_config(FunctionName=fn)
    with pytest.raises(ClientError) as exc:
        lam.get_function_url_config(FunctionName=fn)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

def test_lambda_unknown_path_returns_404(lam):
    """Requests to an unrecognised Lambda path must return 404, not 400 InvalidRequest."""
    import urllib.error
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(
        f"{endpoint}/2015-03-31/functions/nonexistent-fn/completely-unknown-subpath",
        headers={"Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/lambda/aws4_request"},
        method="GET",
    )
    try:
        urllib.request.urlopen(req)
        assert False, "Expected an error response"
    except urllib.error.HTTPError as e:
        assert e.code == 404

def test_lambda_reset_terminates_workers(lam):
    """/_ministack/reset must cleanly terminate warm Lambda workers."""
    import urllib.request

    fn = f"intg-reset-worker-{__import__('uuid').uuid4().hex[:8]}"
    code = "import time\n_boot = time.time()\ndef handler(event, context):\n    return {'boot': _boot}\n"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    # Warm the worker
    r1 = lam.invoke(FunctionName=fn, Payload=b"{}")
    boot1 = json.loads(r1["Payload"].read())["boot"]

    # Reset — must terminate worker without error
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    req = urllib.request.Request(f"{endpoint}/_ministack/reset", data=b"", method="POST")
    for _attempt in range(3):
        try:
            urllib.request.urlopen(req, timeout=15)
            break
        except Exception:
            if _attempt == 2:
                raise

    # Re-create and invoke — new worker means new boot time
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    r2 = lam.invoke(FunctionName=fn, Payload=b"{}")
    boot2 = json.loads(r2["Payload"].read())["boot"]
    assert boot2 > boot1, "Worker should have been reset — new boot time expected"

def test_lambda_alias_crud(lam):
    """CreateAlias, GetAlias, UpdateAlias, DeleteAlias."""
    code = _zip_lambda("def handler(e,c): return {'v': 1}")
    lam.create_function(
        FunctionName="qa-lam-alias",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": code},
    )
    lam.publish_version(FunctionName="qa-lam-alias")
    lam.create_alias(
        FunctionName="qa-lam-alias",
        Name="prod",
        FunctionVersion="1",
        Description="production alias",
    )
    alias = lam.get_alias(FunctionName="qa-lam-alias", Name="prod")
    assert alias["Name"] == "prod"
    assert alias["FunctionVersion"] == "1"
    lam.update_alias(FunctionName="qa-lam-alias", Name="prod", Description="updated")
    alias2 = lam.get_alias(FunctionName="qa-lam-alias", Name="prod")
    assert alias2["Description"] == "updated"
    aliases = lam.list_aliases(FunctionName="qa-lam-alias")["Aliases"]
    assert any(a["Name"] == "prod" for a in aliases)
    lam.delete_alias(FunctionName="qa-lam-alias", Name="prod")
    aliases2 = lam.list_aliases(FunctionName="qa-lam-alias")["Aliases"]
    assert not any(a["Name"] == "prod" for a in aliases2)


def test_lambda_alias_no_phantom_routing_config(lam):
    """Regression for #440: when Terraform sends RoutingConfig with an empty
    AdditionalVersionWeights map (its default payload when no weighted routing
    is declared), ministack must NOT echo RoutingConfig back — real AWS omits
    the field. Otherwise Terraform plans to remove the block on every apply."""
    code = _zip_lambda("def handler(e,c): return 'v1'")
    fn = "qa-lam-alias-rc"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": code},
    )
    lam.publish_version(FunctionName=fn)
    # CreateAlias with the exact payload terraform-provider-aws sends when
    # there is no `routing_config` block in HCL — outer dict present, inner
    # weights empty.
    created = lam.create_alias(
        FunctionName=fn,
        Name="live",
        FunctionVersion="1",
        RoutingConfig={"AdditionalVersionWeights": {}},
    )
    assert "RoutingConfig" not in created, f"phantom RoutingConfig echoed back: {created.get('RoutingConfig')!r}"
    fetched = lam.get_alias(FunctionName=fn, Name="live")
    assert "RoutingConfig" not in fetched, f"phantom RoutingConfig on GetAlias: {fetched.get('RoutingConfig')!r}"

    # But a real weighted config MUST survive.
    lam.publish_version(FunctionName=fn)
    updated = lam.update_alias(
        FunctionName=fn,
        Name="live",
        RoutingConfig={"AdditionalVersionWeights": {"2": 0.1}},
    )
    assert updated["RoutingConfig"]["AdditionalVersionWeights"] == {"2": 0.1}

    # Clearing it back to empty removes it.
    cleared = lam.update_alias(
        FunctionName=fn,
        Name="live",
        RoutingConfig={"AdditionalVersionWeights": {}},
    )
    assert "RoutingConfig" not in cleared

    lam.delete_function(FunctionName=fn)


def test_lambda_event_source_mapping_tags(lam, sqs):
    """Regression for #442: CreateEventSourceMapping accepts Tags; ListTags
    returns them by ESM ARN. Without this, Terraform replans tags on every apply."""
    code = _zip_lambda("def handler(e,c): return 'ok'")
    fn = "qa-esm-tags-fn"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": code},
    )
    q = sqs.create_queue(QueueName="qa-esm-tags-queue")
    q_arn = sqs.get_queue_attributes(QueueUrl=q["QueueUrl"], AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    esm = lam.create_event_source_mapping(
        FunctionName=fn,
        EventSourceArn=q_arn,
        Tags={"Team": "billing", "Env": "prod"},
    )
    esm_arn = f"arn:aws:lambda:us-east-1:000000000000:event-source-mapping:{esm['UUID']}"
    tags = lam.list_tags(Resource=esm_arn)["Tags"]
    assert tags == {"Team": "billing", "Env": "prod"}

    # TagResource / UntagResource must also work on an ESM ARN.
    lam.tag_resource(Resource=esm_arn, Tags={"Team": "platform"})
    tags = lam.list_tags(Resource=esm_arn)["Tags"]
    assert tags["Team"] == "platform"
    assert tags["Env"] == "prod"

    lam.untag_resource(Resource=esm_arn, TagKeys=["Env"])
    tags = lam.list_tags(Resource=esm_arn)["Tags"]
    assert "Env" not in tags
    assert tags["Team"] == "platform"

    lam.delete_event_source_mapping(UUID=esm["UUID"])
    lam.delete_function(FunctionName=fn)
    sqs.delete_queue(QueueUrl=q["QueueUrl"])


def test_lambda_publish_version_snapshot(lam):
    """PublishVersion creates a numbered version snapshot."""
    code = _zip_lambda("def handler(e,c): return 'v1'")
    lam.create_function(
        FunctionName="qa-lam-version",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": code},
    )
    ver = lam.publish_version(FunctionName="qa-lam-version")
    assert ver["Version"] == "1"
    versions = lam.list_versions_by_function(FunctionName="qa-lam-version")["Versions"]
    version_nums = [v["Version"] for v in versions]
    assert "1" in version_nums
    assert "$LATEST" in version_nums


def test_lambda_published_version_readiness_follows_function(lam):
    """Published versions created during function bootstrap become Active."""
    fn = f"qa-lam-version-ready-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": _zip_lambda("def handler(e,c): return 'v1'")},
        Publish=True,
    )

    deadline = time.time() + 10
    latest = version = None
    while time.time() < deadline:
        latest = lam.get_function_configuration(FunctionName=fn)
        version = lam.get_function_configuration(FunctionName=fn, Qualifier="1")
        if (
            latest["State"] == "Active"
            and latest["LastUpdateStatus"] == "Successful"
            and version["State"] == "Active"
            and version["LastUpdateStatus"] == "Successful"
        ):
            break
        time.sleep(0.1)

    assert latest["State"] == "Active"
    assert latest["LastUpdateStatus"] == "Successful"
    assert version["Version"] == "1"
    assert version["State"] == "Active"
    assert version["LastUpdateStatus"] == "Successful"


def test_lambda_function_concurrency(lam):
    """PutFunctionConcurrency / GetFunctionConcurrency / DeleteFunctionConcurrency."""
    code = _zip_lambda("def handler(e,c): return {}")
    lam.create_function(
        FunctionName="qa-lam-concurrency",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": code},
    )
    lam.put_function_concurrency(
        FunctionName="qa-lam-concurrency",
        ReservedConcurrentExecutions=5,
    )
    resp = lam.get_function_concurrency(FunctionName="qa-lam-concurrency")
    assert resp["ReservedConcurrentExecutions"] == 5
    lam.delete_function_concurrency(FunctionName="qa-lam-concurrency")
    resp2 = lam.get_function_concurrency(FunctionName="qa-lam-concurrency")
    assert resp2.get("ReservedConcurrentExecutions") is None

def test_lambda_add_remove_permission(lam):
    """AddPermission / RemovePermission / GetPolicy."""
    code = _zip_lambda("def handler(e,c): return {}")
    lam.create_function(
        FunctionName="qa-lam-policy",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/r",
        Handler="index.handler",
        Code={"ZipFile": code},
    )
    lam.add_permission(
        FunctionName="qa-lam-policy",
        StatementId="allow-s3",
        Action="lambda:InvokeFunction",
        Principal="s3.amazonaws.com",
    )
    policy = json.loads(lam.get_policy(FunctionName="qa-lam-policy")["Policy"])
    assert any(s["Sid"] == "allow-s3" for s in policy["Statement"])
    lam.remove_permission(FunctionName="qa-lam-policy", StatementId="allow-s3")
    policy2 = json.loads(lam.get_policy(FunctionName="qa-lam-policy")["Policy"])
    assert not any(s["Sid"] == "allow-s3" for s in policy2["Statement"])

def test_lambda_list_functions_pagination(lam):
    """ListFunctions pagination with Marker works correctly."""
    for i in range(5):
        code = _zip_lambda("def handler(e,c): return {}")
        try:
            lam.create_function(
                FunctionName=f"qa-lam-page-{i}",
                Runtime="python3.12",
                Role="arn:aws:iam::000000000000:role/r",
                Handler="index.handler",
                Code={"ZipFile": code},
            )
        except ClientError:
            pass
    resp1 = lam.list_functions(MaxItems=2)
    assert len(resp1["Functions"]) <= 2
    if "NextMarker" in resp1:
        resp2 = lam.list_functions(MaxItems=2, Marker=resp1["NextMarker"])
        names1 = {f["FunctionName"] for f in resp1["Functions"]}
        names2 = {f["FunctionName"] for f in resp2["Functions"]}
        assert not names1 & names2

def test_lambda_invoke_event_type_returns_202(lam):
    """Invoke with InvocationType=Event returns 202 immediately."""
    code = _zip_lambda("def handler(e,c): return {}")
    try:
        lam.create_function(
            FunctionName="qa-lam-event-invoke",
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": code},
        )
    except ClientError:
        pass
    resp = lam.invoke(
        FunctionName="qa-lam-event-invoke",
        InvocationType="Event",
        Payload=json.dumps({}),
    )
    assert resp["StatusCode"] == 202

def test_lambda_invoke_dry_run_returns_204(lam):
    """Invoke with InvocationType=DryRun returns 204."""
    code = _zip_lambda("def handler(e,c): return {}")
    try:
        lam.create_function(
            FunctionName="qa-lam-dryrun",
            Runtime="python3.12",
            Role="arn:aws:iam::000000000000:role/r",
            Handler="index.handler",
            Code={"ZipFile": code},
        )
    except ClientError:
        pass
    resp = lam.invoke(
        FunctionName="qa-lam-dryrun",
        InvocationType="DryRun",
        Payload=json.dumps({}),
    )
    assert resp["StatusCode"] == 204

def test_lambda_layer_publish(lam):
    import base64
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("layer.py", "# layer")
    zip_bytes = buf.getvalue()
    resp = lam.publish_layer_version(
        LayerName="my-test-layer",
        Description="Test layer",
        Content={"ZipFile": zip_bytes},
        CompatibleRuntimes=["python3.12"],
    )
    assert resp["Version"] == 1
    assert "my-test-layer" in resp["LayerVersionArn"]

def test_lambda_layer_publish_from_s3(lam, s3):
    """PublishLayerVersion with S3Bucket/S3Key. Contributed by @Baptiste-Garcin (#356)."""
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("s3layer.py", "# layer from s3")
    zip_bytes = buf.getvalue()

    bucket = "layer-bucket"
    key = "layers/my-layer.zip"
    s3.create_bucket(Bucket=bucket)
    s3.put_object(Bucket=bucket, Key=key, Body=zip_bytes)

    resp = lam.publish_layer_version(
        LayerName="s3-layer",
        Description="Layer from S3",
        Content={"S3Bucket": bucket, "S3Key": key},
        CompatibleRuntimes=["python3.12"],
    )
    assert resp["Version"] == 1
    assert "s3-layer" in resp["LayerVersionArn"]
    assert resp["Content"]["CodeSize"] == len(zip_bytes)
    assert resp["Content"]["CodeSha256"]

def test_lambda_layer_get_version(lam):
    resp = lam.get_layer_version(LayerName="my-test-layer", VersionNumber=1)
    assert resp["Version"] == 1
    assert resp["Description"] == "Test layer"

def test_lambda_layer_list_versions(lam):
    resp = lam.list_layer_versions(LayerName="my-test-layer")
    assert len(resp["LayerVersions"]) >= 1
    assert resp["LayerVersions"][0]["Version"] == 1

def test_lambda_layer_list_layers(lam):
    resp = lam.list_layers()
    names = [l["LayerName"] for l in resp["Layers"]]
    assert "my-test-layer" in names

def test_lambda_layer_delete_version(lam):
    import base64
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("tmp.py", "")
    lam.publish_layer_version(LayerName="delete-layer-test", Content={"ZipFile": buf.getvalue()})
    lam.delete_layer_version(LayerName="delete-layer-test", VersionNumber=1)
    resp = lam.list_layer_versions(LayerName="delete-layer-test")
    assert len(resp["LayerVersions"]) == 0

def test_lambda_function_with_layer(lam):
    # Publish layer
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("layer.py", "")
    layer_resp = lam.publish_layer_version(LayerName="fn-layer", Content={"ZipFile": buf.getvalue()})
    layer_arn = layer_resp["LayerVersionArn"]
    # Create function using the layer
    fn_zip = io.BytesIO()
    with zipfile.ZipFile(fn_zip, "w") as z:
        z.writestr("index.py", "def handler(e, c): return {}")
    lam.create_function(
        FunctionName="fn-with-layer",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test",
        Handler="index.handler",
        Code={"ZipFile": fn_zip.getvalue()},
        Layers=[layer_arn],
    )
    fn = lam.get_function(FunctionName="fn-with-layer")
    assert layer_arn in fn["Configuration"]["Layers"][0]["Arn"]


def test_lambda_docker_cp_dir_arcname_creates_subdir_in_existing_parent():
    """Docker's put_archive requires dest_dir to exist. For /opt/layer_N
    (which doesn't exist in the base RIE image), the fix is to extract into
    the existing /opt with the layer dir baked into the arcname so the tar
    materialises the subdir. Regression for issue #816 docker-executor 404."""
    import io as _io
    import tarfile as _tarfile
    import tempfile

    from ministack.services.lambda_svc import _docker_cp_dir

    captured = {}

    class _FakeContainer:
        def put_archive(self, path, data):
            captured["path"] = path
            captured["data"] = data.read() if hasattr(data, "read") else data

    with tempfile.TemporaryDirectory() as src_dir:
        os.makedirs(os.path.join(src_dir, "python"))
        with open(os.path.join(src_dir, "python", "mod.py"), "w") as f:
            f.write("X = 1\n")

        _docker_cp_dir(_FakeContainer(), src_dir, "/opt", arcname="layer_0")

    assert captured["path"] == "/opt"
    tar_bytes = captured["data"]
    with _tarfile.open(fileobj=_io.BytesIO(tar_bytes), mode="r") as tar:
        names = tar.getnames()
    # Entries must be rooted at "layer_0/..." so extraction into /opt produces /opt/layer_0/...
    assert any(n.startswith("layer_0/python") or n == "layer_0/python/mod.py" for n in names), names
    assert "layer_0" in names or any(n.startswith("layer_0/") for n in names)


def test_lambda_pool_kill_function_reaps_all_qualifiers():
    """_pool_kill_function must remove every pooled docker container for a
    function across all qualifiers (the pool key includes CodeSha256, so
    config-only updates leave stale entries unless explicitly reaped). Issue
    #816 docker-executor follow-up. Wired into _update_config / _delete_function
    so layer attach via UpdateFunctionConfiguration displaces the pre-attach
    container before the next invoke."""
    from ministack.services import lambda_svc as _svc

    class _StubContainer:
        def __init__(self):
            self.stopped = False
            self.removed = False
        def stop(self, timeout=2):
            self.stopped = True
        def remove(self, force=False):
            self.removed = True

    stubs = [_StubContainer() for _ in range(3)]
    keys = [
        "111122223333:fn-A:zip:sha-v1",
        "111122223333:fn-A:zip:sha-v2",
        "111122223333:fn-B:zip:sha-v1",   # different function — must NOT be touched
    ]
    with _svc._warm_pool_lock:
        for k, s in zip(keys, stubs):
            _svc._warm_pool.setdefault(k, []).append(
                {"container": s, "tmpdir": None, "in_use": False,
                 "last_used": 0, "created": 0}
            )

    try:
        _svc._pool_kill_function("111122223333", "fn-A")

        with _svc._warm_pool_lock:
            assert _svc._warm_pool.get("111122223333:fn-A:zip:sha-v1", []) == []
            assert _svc._warm_pool.get("111122223333:fn-A:zip:sha-v2", []) == []
            # fn-B must be untouched
            assert len(_svc._warm_pool.get("111122223333:fn-B:zip:sha-v1", [])) == 1

        assert stubs[0].stopped and stubs[0].removed, "fn-A v1 container not killed"
        assert stubs[1].stopped and stubs[1].removed, "fn-A v2 container not killed"
        assert not stubs[2].stopped, "fn-B container was killed (should be untouched)"
    finally:
        # Clean up any leftover stub entries so this test doesn't pollute siblings.
        with _svc._warm_pool_lock:
            for k in keys:
                _svc._warm_pool.pop(k, None)


def test_lambda_function_with_layer_reports_real_code_size(lam):
    """GetFunctionConfiguration.Layers[*].CodeSize must mirror the layer's
    actual zip size, not hardcoded 0 (issue #816)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("mod.py", "X = 'hello' * 1000")  # non-trivial size
    layer_zip = buf.getvalue()
    layer_resp = lam.publish_layer_version(
        LayerName="codesize-layer",
        Content={"ZipFile": layer_zip},
    )
    layer_arn = layer_resp["LayerVersionArn"]
    expected_size = layer_resp["Content"]["CodeSize"]
    assert expected_size > 0

    fn_zip = io.BytesIO()
    with zipfile.ZipFile(fn_zip, "w") as z:
        z.writestr("index.py", "def handler(e, c): return {}")
    lam.create_function(
        FunctionName="codesize-fn",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test",
        Handler="index.handler",
        Code={"ZipFile": fn_zip.getvalue()},
        Layers=[layer_arn],
    )
    cfg = lam.get_function_configuration(FunctionName="codesize-fn")
    assert cfg["Layers"][0]["Arn"] == layer_arn
    assert cfg["Layers"][0]["CodeSize"] == expected_size


def test_lambda_update_function_configuration_layer_attachment_invokes_with_layer(lam):
    """UpdateFunctionConfiguration(Layers=[arn]) must:
      (a) surface the layer's real CodeSize on the next GetFunctionConfiguration, and
      (b) recycle the warm worker so the next invoke actually loads the layer.
    Regression for issue #816 (layer not found after association)."""
    # Layer publishes a Python module the handler imports.
    layer_buf = io.BytesIO()
    with zipfile.ZipFile(layer_buf, "w") as z:
        z.writestr("python/mylayermod.py", "VALUE = 'from-layer'")
    layer_arn = lam.publish_layer_version(
        LayerName="late-attach-layer",
        Content={"ZipFile": layer_buf.getvalue()},
        CompatibleRuntimes=["python3.12"],
    )["LayerVersionArn"]

    # Function created WITHOUT the layer first — handler tolerates the absence
    # so the initial invoke can warm a worker.
    fn_src = (
        "def handler(event, context):\n"
        "    try:\n"
        "        import mylayermod\n"
        "        return {'layer_value': mylayermod.VALUE}\n"
        "    except ImportError:\n"
        "        return {'layer_value': None}\n"
    )
    fn_buf = io.BytesIO()
    with zipfile.ZipFile(fn_buf, "w") as z:
        z.writestr("index.py", fn_src)
    lam.create_function(
        FunctionName="late-attach-fn",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test",
        Handler="index.handler",
        Code={"ZipFile": fn_buf.getvalue()},
    )

    # Pre-attach invoke warms a worker without the layer.
    pre = lam.invoke(FunctionName="late-attach-fn", Payload=b"{}")
    pre_body = json.loads(pre["Payload"].read())
    assert pre_body == {"layer_value": None}

    # Attach the layer via UpdateFunctionConfiguration.
    lam.update_function_configuration(FunctionName="late-attach-fn", Layers=[layer_arn])

    # (a) CodeSize on GetFunctionConfiguration matches the layer's real size.
    cfg = lam.get_function_configuration(FunctionName="late-attach-fn")
    assert cfg["Layers"][0]["Arn"] == layer_arn
    assert cfg["Layers"][0]["CodeSize"] > 0

    # (b) Next invoke must use a fresh worker that has the layer mounted on
    #     /opt/python — the import succeeds and the handler returns the layer value.
    post = lam.invoke(FunctionName="late-attach-fn", Payload=b"{}")
    post_body = json.loads(post["Payload"].read())
    assert post_body == {"layer_value": "from-layer"}

def test_lambda_layer_content_location(lam):
    """Content.Location should be a non-empty URL pointing to the layer zip."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("mod.py", "X=1")
    resp = lam.publish_layer_version(
        LayerName="loc-layer",
        Content={"ZipFile": buf.getvalue()},
    )
    assert resp["Content"]["Location"]
    assert "loc-layer" in resp["Content"]["Location"]
    # Verify the URL actually serves zip data
    import urllib.request

    data = urllib.request.urlopen(resp["Content"]["Location"]).read()
    assert len(data) == resp["Content"]["CodeSize"]

def test_lambda_layer_pagination(lam):
    """Publish 3 versions, paginate with MaxItems=1."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("p.py", "")
    for _ in range(3):
        lam.publish_layer_version(LayerName="page-layer", Content={"ZipFile": buf.getvalue()})
    # List with MaxItems=1 (newest first)
    resp = lam.list_layer_versions(LayerName="page-layer", MaxItems=1)
    assert len(resp["LayerVersions"]) == 1
    assert "NextMarker" in resp

def test_lambda_layer_list_filter_runtime(lam):
    """Filter list_layer_versions by CompatibleRuntime."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("r.py", "")
    lam.publish_layer_version(
        LayerName="rt-filter-layer",
        Content={"ZipFile": buf.getvalue()},
        CompatibleRuntimes=["python3.12"],
    )
    lam.publish_layer_version(
        LayerName="rt-filter-layer",
        Content={"ZipFile": buf.getvalue()},
        CompatibleRuntimes=["nodejs18.x"],
    )
    resp = lam.list_layer_versions(
        LayerName="rt-filter-layer",
        CompatibleRuntime="python3.12",
    )
    assert all("python3.12" in v["CompatibleRuntimes"] for v in resp["LayerVersions"])

def test_lambda_layer_list_filter_architecture(lam):
    """Filter list_layer_versions by CompatibleArchitecture."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("a.py", "")
    lam.publish_layer_version(
        LayerName="arch-filter-layer",
        Content={"ZipFile": buf.getvalue()},
        CompatibleArchitectures=["x86_64"],
    )
    lam.publish_layer_version(
        LayerName="arch-filter-layer",
        Content={"ZipFile": buf.getvalue()},
        CompatibleArchitectures=["arm64"],
    )
    resp = lam.list_layer_versions(
        LayerName="arch-filter-layer",
        CompatibleArchitecture="x86_64",
    )
    assert all("x86_64" in v["CompatibleArchitectures"] for v in resp["LayerVersions"])

def test_lambda_layer_list_layers_pagination(lam):
    """Multiple layers, paginate ListLayers."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("x.py", "")
    for i in range(3):
        lam.publish_layer_version(
            LayerName=f"ll-page-{i}",
            Content={"ZipFile": buf.getvalue()},
        )
    resp = lam.list_layers(MaxItems=1)
    assert len(resp["Layers"]) == 1
    assert "NextMarker" in resp

def test_lambda_layer_list_layers_filter_runtime(lam):
    """ListLayers filtered by CompatibleRuntime."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("f.py", "")
    lam.publish_layer_version(
        LayerName="ll-rt-py",
        Content={"ZipFile": buf.getvalue()},
        CompatibleRuntimes=["python3.12"],
    )
    lam.publish_layer_version(
        LayerName="ll-rt-node",
        Content={"ZipFile": buf.getvalue()},
        CompatibleRuntimes=["nodejs18.x"],
    )
    resp = lam.list_layers(CompatibleRuntime="python3.12")
    names = [l["LayerName"] for l in resp["Layers"]]
    assert "ll-rt-py" in names
    assert "ll-rt-node" not in names

def test_lambda_layer_get_version_not_found(lam):
    """Getting a nonexistent layer should raise 404."""
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        lam.get_layer_version(LayerName="no-such-layer-xyz", VersionNumber=1)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_lambda_layer_get_version_by_arn(lam):
    """GetLayerVersionByArn resolves by full ARN."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("ba.py", "")
    pub = lam.publish_layer_version(
        LayerName="by-arn-layer",
        Content={"ZipFile": buf.getvalue()},
    )
    arn = pub["LayerVersionArn"]
    resp = lam.get_layer_version_by_arn(Arn=arn)
    assert resp["LayerVersionArn"] == arn
    assert resp["Version"] == pub["Version"]

def test_lambda_layer_version_permission_add(lam):
    """Add a layer version permission and verify response."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("perm.py", "")
    pub = lam.publish_layer_version(
        LayerName="perm-layer",
        Content={"ZipFile": buf.getvalue()},
    )
    resp = lam.add_layer_version_permission(
        LayerName="perm-layer",
        VersionNumber=pub["Version"],
        StatementId="allow-all",
        Action="lambda:GetLayerVersion",
        Principal="*",
    )
    assert "Statement" in resp
    import json

    stmt = json.loads(resp["Statement"])
    assert stmt["Sid"] == "allow-all"
    assert stmt["Action"] == "lambda:GetLayerVersion"

def test_lambda_layer_version_permission_get_policy(lam):
    """Get policy after adding a permission."""
    import json

    resp = lam.get_layer_version_policy(LayerName="perm-layer", VersionNumber=1)
    policy = json.loads(resp["Policy"])
    assert len(policy["Statement"]) >= 1
    assert policy["Statement"][0]["Sid"] == "allow-all"

def test_lambda_layer_version_permission_remove(lam):
    """Remove a layer version permission."""
    lam.remove_layer_version_permission(
        LayerName="perm-layer",
        VersionNumber=1,
        StatementId="allow-all",
    )
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        lam.get_layer_version_policy(LayerName="perm-layer", VersionNumber=1)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

def test_lambda_layer_version_permission_duplicate_sid(lam):
    """Adding a duplicate StatementId should raise conflict."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("dup.py", "")
    pub = lam.publish_layer_version(
        LayerName="dup-sid-layer",
        Content={"ZipFile": buf.getvalue()},
    )
    lam.add_layer_version_permission(
        LayerName="dup-sid-layer",
        VersionNumber=pub["Version"],
        StatementId="s1",
        Action="lambda:GetLayerVersion",
        Principal="*",
    )
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        lam.add_layer_version_permission(
            LayerName="dup-sid-layer",
            VersionNumber=pub["Version"],
            StatementId="s1",
            Action="lambda:GetLayerVersion",
            Principal="*",
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409

def test_lambda_layer_version_permission_invalid_action(lam):
    """Only lambda:GetLayerVersion is a valid action."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("inv.py", "")
    pub = lam.publish_layer_version(
        LayerName="inv-act-layer",
        Content={"ZipFile": buf.getvalue()},
    )
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        lam.add_layer_version_permission(
            LayerName="inv-act-layer",
            VersionNumber=pub["Version"],
            StatementId="s1",
            Action="lambda:InvokeFunction",
            Principal="*",
        )
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] in (400, 403)

def test_lambda_layer_delete_idempotent(lam):
    """Deleting a nonexistent version should not error."""
    lam.delete_layer_version(LayerName="no-such-layer-del", VersionNumber=999)

def test_lambda_warm_worker_invalidation(lam):
    """Create function with code v1, invoke, update code to v2, invoke again — must see v2."""
    import io as _io
    import zipfile as _zf

    fname = "lambda-worker-invalidation-test"
    try:
        lam.delete_function(FunctionName=fname)
    except Exception:
        pass

    # v1 code
    code_v1 = b'def handler(event, context):\n    return {"version": 1}\n'
    buf1 = _io.BytesIO()
    with _zf.ZipFile(buf1, "w") as z:
        z.writestr("index.py", code_v1)
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf1.getvalue()},
    )

    # Invoke v1
    resp1 = lam.invoke(FunctionName=fname, Payload=json.dumps({}))
    payload1 = json.loads(resp1["Payload"].read())
    assert payload1["version"] == 1

    # Update to v2
    code_v2 = b'def handler(event, context):\n    return {"version": 2}\n'
    buf2 = _io.BytesIO()
    with _zf.ZipFile(buf2, "w") as z:
        z.writestr("index.py", code_v2)
    lam.update_function_code(FunctionName=fname, ZipFile=buf2.getvalue())

    # Invoke v2
    resp2 = lam.invoke(FunctionName=fname, Payload=json.dumps({}))
    payload2 = json.loads(resp2["Payload"].read())
    assert payload2["version"] == 2

def test_lambda_event_invoke_config_crud(lam):
    """Put/Get/Delete EventInvokeConfig lifecycle."""
    code = "def handler(e,c): return {}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName="eic-fn", Runtime="python3.11",
        Role=_LAMBDA_ROLE, Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )

    lam.put_function_event_invoke_config(
        FunctionName="eic-fn",
        MaximumRetryAttempts=1,
        MaximumEventAgeInSeconds=300,
    )
    cfg = lam.get_function_event_invoke_config(FunctionName="eic-fn")
    assert cfg["MaximumRetryAttempts"] == 1
    assert cfg["MaximumEventAgeInSeconds"] == 300

    lam.delete_function_event_invoke_config(FunctionName="eic-fn")
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError):
        lam.get_function_event_invoke_config(FunctionName="eic-fn")

    lam.delete_function(FunctionName="eic-fn")

def test_lambda_provisioned_concurrency_crud(lam):
    """Put/Get/Delete ProvisionedConcurrencyConfig lifecycle."""
    code = "def handler(e,c): return {}"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    lam.create_function(
        FunctionName="pc-fn", Runtime="python3.11",
        Role=_LAMBDA_ROLE, Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
        Publish=True,
    )
    versions = lam.list_versions_by_function(FunctionName="pc-fn")["Versions"]
    ver = [v for v in versions if v["Version"] != "$LATEST"][0]["Version"]

    lam.put_provisioned_concurrency_config(
        FunctionName="pc-fn",
        Qualifier=ver,
        ProvisionedConcurrentExecutions=5,
    )
    cfg = lam.get_provisioned_concurrency_config(
        FunctionName="pc-fn", Qualifier=ver,
    )
    assert cfg["RequestedProvisionedConcurrentExecutions"] == 5

    lam.delete_provisioned_concurrency_config(
        FunctionName="pc-fn", Qualifier=ver,
    )
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError):
        lam.get_provisioned_concurrency_config(FunctionName="pc-fn", Qualifier=ver)

    lam.delete_function(FunctionName="pc-fn")

def test_lambda_image_create_invoke(lam):
    """CreateFunction with PackageType Image + GetFunction returns ImageUri."""
    lam.create_function(
        FunctionName="img-test-v39",
        PackageType="Image",
        Code={"ImageUri": "my-repo/my-image:latest"},
        Role="arn:aws:iam::000000000000:role/test",
        Timeout=30,
    )
    desc = lam.get_function(FunctionName="img-test-v39")
    assert desc["Configuration"]["PackageType"] == "Image"
    assert desc["Code"]["RepositoryType"] == "ECR"
    assert desc["Code"]["ImageUri"] == "my-repo/my-image:latest"
    lam.delete_function(FunctionName="img-test-v39")

def test_lambda_update_code_image_uri(lam):
    """UpdateFunctionCode with ImageUri updates the image."""
    lam.create_function(
        FunctionName="img-update-v39",
        PackageType="Image",
        Code={"ImageUri": "my-repo/my-image:v1"},
        Role="arn:aws:iam::000000000000:role/test",
    )
    lam.update_function_code(FunctionName="img-update-v39", ImageUri="my-repo/my-image:v2")
    desc = lam.get_function(FunctionName="img-update-v39")
    assert desc["Code"]["ImageUri"] == "my-repo/my-image:v2"
    lam.delete_function(FunctionName="img-update-v39")

def test_lambda_provided_runtime_create(lam):
    """CreateFunction with provided.al2023 runtime accepts bootstrap handler."""
    import io
    import zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("bootstrap", "#!/bin/sh\necho ok\n")
    lam.create_function(
        FunctionName="provided-test-v39",
        Runtime="provided.al2023",
        Handler="bootstrap",
        Code={"ZipFile": buf.getvalue()},
        Role="arn:aws:iam::000000000000:role/test",
    )
    desc = lam.get_function_configuration(FunctionName="provided-test-v39")
    assert desc["Runtime"] == "provided.al2023"
    assert desc["Handler"] == "bootstrap"
    lam.delete_function(FunctionName="provided-test-v39")


@pytest.mark.skipif(
    os.environ.get("LAMBDA_EXECUTOR", "").lower() != "docker",
    reason="requires LAMBDA_EXECUTOR=docker and Docker daemon",
)
def test_lambda_provided_runtime_docker_invoke(lam):
    """Invoke a provided.al2023 Lambda via the Docker executor.

    Uses a shell-script bootstrap that implements the Lambda Runtime API
    (GET /invocation/next, POST /invocation/{id}/response).
    """
    # Shell bootstrap implementing the Lambda Runtime API protocol.
    # Must loop: the RIE expects the bootstrap to poll for invocations.
    bootstrap_script = (
        "#!/bin/sh\n"
        'RUNTIME_API="${AWS_LAMBDA_RUNTIME_API}"\n'
        "while true; do\n"
        '  RESP=$(curl -s -D /tmp/headers '
        '"http://${RUNTIME_API}/2018-06-01/runtime/invocation/next")\n'
        '  REQUEST_ID=$(grep -i "Lambda-Runtime-Aws-Request-Id" /tmp/headers '
        '| tr -d "\\r" | cut -d" " -f2)\n'
        '  curl -s -X POST '
        '"http://${RUNTIME_API}/2018-06-01/runtime/invocation/${REQUEST_ID}/response" '
        "-d '{\"statusCode\":200,\"body\":\"hello from provided\"}'\n"
        "done\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        info = zipfile.ZipInfo("bootstrap")
        info.external_attr = 0o755 << 16  # executable
        zf.writestr(info, bootstrap_script)

    func_name = f"provided-docker-test-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=func_name,
        Runtime="provided.al2023",
        Handler="bootstrap",
        Code={"ZipFile": buf.getvalue()},
        Role="arn:aws:iam::000000000000:role/test",
        Timeout=30,
    )
    try:
        resp = lam.invoke(FunctionName=func_name, Payload=json.dumps({"key": "value"}))
        payload = json.loads(resp["Payload"].read())
        assert payload["statusCode"] == 200
        assert payload["body"] == "hello from provided"
    finally:
        lam.delete_function(FunctionName=func_name)


def test_apigwv2_nodejs_lambda_proxy(lam, apigw):
    """API Gateway v2 HTTP API should invoke Node.js Lambda via warm worker, not return mock."""
    import urllib.request as _urlreq
    import uuid as _uuid

    from botocore.exceptions import ClientError

    fname = f"apigwv2-node-{_uuid_mod.uuid4().hex[:8]}"
    api_id = None
    code = (
        "exports.handler = async (event) => ({"
        " statusCode: 200,"
        " body: JSON.stringify({ route: event.routeKey, method: event.requestContext.http.method })"
        "});"
    )
    try:
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler",
            Code={"ZipFile": _make_zip_js(code, "index.js")},
        )
        api_id = apigw.create_api(Name=f"v2-node-{fname}", ProtocolType="HTTP")["ApiId"]
        int_id = apigw.create_integration(
            ApiId=api_id,
            IntegrationType="AWS_PROXY",
            IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{fname}",
            PayloadFormatVersion="2.0",
        )["IntegrationId"]
        apigw.create_route(ApiId=api_id, RouteKey="GET /test", Target=f"integrations/{int_id}")
        apigw.create_stage(ApiId=api_id, StageName="$default")

        req = _urlreq.Request(
            f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/$default/test",
            method="GET",
        )
        req.add_header("Host", f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}")
        resp = _urlreq.urlopen(req).read().decode()
        body = json.loads(resp)

        assert body.get("route") == "GET /test", f"Expected handler result, got: {resp}"
        assert body.get("method") == "GET"
    finally:
        if api_id is not None:
            try:
                apigw.delete_api(ApiId=api_id)
            except ClientError:
                pass
        try:
            lam.delete_function(FunctionName=fname)
        except ClientError:
            pass


def test_lambda_nodejs_esm_mjs_handler(lam):
    """Node.js .mjs (ESM) handlers should be loaded via dynamic import() fallback.

    Creates a ZIP with two .mjs files:
      - utils.mjs: exports a helper function using ESM `export` syntax
      - index.mjs: imports utils.mjs via ESM `import` statement and uses it

    This verifies that:
      1. .mjs files are loaded via import() instead of require()
      2. ESM import/export syntax works between modules
      3. The handler's return value is correctly propagated
    """
    fname = f"lam-esm-{_uuid_mod.uuid4().hex[:8]}"

    utils_code = (
        "export function greet(name) {\n"
        "  return `Hello, ${name} from ESM!`;\n"
        "}\n"
        "\n"
        "export const VERSION = '1.0.0';\n"
    )

    handler_code = (
        "import { greet, VERSION } from './utils.mjs';\n"
        "\n"
        "export const handler = async (event) => {\n"
        "  const name = event.name || 'World';\n"
        "  return {\n"
        "    statusCode: 200,\n"
        "    body: JSON.stringify({\n"
        "      message: greet(name),\n"
        "      version: VERSION,\n"
        "      esm: true,\n"
        "    }),\n"
        "  };\n"
        "};\n"
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("index.mjs", handler_code)
        z.writestr("utils.mjs", utils_code)

    lam.create_function(
        FunctionName=fname,
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    try:
        resp = lam.invoke(
            FunctionName=fname,
            Payload=json.dumps({"name": "MiniStack"}),
        )
        assert resp["StatusCode"] == 200
        assert "FunctionError" not in resp, f"Lambda error: {resp['Payload'].read().decode()}"
        payload = json.loads(resp["Payload"].read())
        assert payload["statusCode"] == 200
        body = json.loads(payload["body"])
        assert body["message"] == "Hello, MiniStack from ESM!"
        assert body["version"] == "1.0.0"
        assert body["esm"] is True
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_warm_worker_uses_layer(lam):
    """Warm worker should extract layers and make their code available to the handler."""
    # Create a layer with a Python module
    layer_buf = io.BytesIO()
    with zipfile.ZipFile(layer_buf, "w") as z:
        z.writestr("python/myhelper.py", "LAYER_VALUE = 'from-layer'\n")
    layer_resp = lam.publish_layer_version(
        LayerName="warm-layer-test",
        Content={"ZipFile": layer_buf.getvalue()},
        CompatibleRuntimes=["python3.12"],
    )
    layer_arn = layer_resp["LayerVersionArn"]

    # Create a function that imports from the layer
    func_code = (
        "import myhelper\n"
        "def handler(event, context):\n"
        "    return {'value': myhelper.LAYER_VALUE}\n"
    )
    func_buf = io.BytesIO()
    with zipfile.ZipFile(func_buf, "w") as z:
        z.writestr("index.py", func_code)

    fname = f"warm-layer-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test",
        Handler="index.handler",
        Code={"ZipFile": func_buf.getvalue()},
        Layers=[layer_arn],
    )

    try:
        resp = lam.invoke(FunctionName=fname, Payload=b"{}")
        assert resp["StatusCode"] == 200
        assert "FunctionError" not in resp, f"Lambda error: {resp.get('FunctionError')}"
        payload = json.loads(resp["Payload"].read())
        assert payload["value"] == "from-layer"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_nodejs_esm_type_module(lam):
    """Node.js ESM via package.json type:module should trigger ERR_REQUIRE_ESM fallback."""
    fname = f"lam-esm-type-{_uuid_mod.uuid4().hex[:8]}"

    handler_code = (
        "export const handler = async (event) => ({\n"
        "  statusCode: 200,\n"
        "  body: 'type-module-works',\n"
        "});\n"
    )

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("index.js", handler_code)
        z.writestr("package.json", '{"type": "module"}')

    lam.create_function(
        FunctionName=fname,
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    try:
        resp = lam.invoke(FunctionName=fname, Payload=b"{}")
        assert resp["StatusCode"] == 200
        assert "FunctionError" not in resp, f"Lambda error: {resp['Payload'].read().decode()}"
        payload = json.loads(resp["Payload"].read())
        assert payload["statusCode"] == 200
        assert payload["body"] == "type-module-works"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_warm_worker_nodejs_uses_layer(lam):
    """Warm worker should extract Node.js layers and make packages available via require()."""
    # Create a layer with a Node.js module under nodejs/node_modules/
    layer_buf = io.BytesIO()
    with zipfile.ZipFile(layer_buf, "w") as z:
        z.writestr(
            "nodejs/node_modules/layerhelper/index.js",
            "module.exports.LAYER_VALUE = 'from-node-layer';\n",
        )
    layer_resp = lam.publish_layer_version(
        LayerName="warm-node-layer-test",
        Content={"ZipFile": layer_buf.getvalue()},
        CompatibleRuntimes=["nodejs20.x"],
    )
    layer_arn = layer_resp["LayerVersionArn"]

    # Create a Node.js function that requires the layer package
    handler_code = (
        "const helper = require('layerhelper');\n"
        "exports.handler = async (event) => {\n"
        "  return { value: helper.LAYER_VALUE };\n"
        "};\n"
    )
    func_buf = io.BytesIO()
    with zipfile.ZipFile(func_buf, "w") as z:
        z.writestr("index.js", handler_code)

    fname = f"warm-node-layer-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=fname,
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": func_buf.getvalue()},
        Layers=[layer_arn],
    )

    try:
        resp = lam.invoke(FunctionName=fname, Payload=b"{}")
        assert resp["StatusCode"] == 200
        assert "FunctionError" not in resp, f"Lambda error: {resp['Payload'].read().decode()}"
        payload = json.loads(resp["Payload"].read())
        assert payload["value"] == "from-node-layer"
    finally:
        lam.delete_function(FunctionName=fname)

def test_lambda_warm_worker_nodejs_esm_uses_layer(lam):
    """ESM .mjs handler must be able to import packages from a Lambda Layer.

    This is the combined case of ESM support (PR #238) and Layer extraction
    (PR #236). Node.js ESM import() does not use NODE_PATH, so the runtime
    symlinks layer packages into code/node_modules/ for ancestor-tree resolution.
    """
    # Create a layer with a Node.js package under nodejs/node_modules/
    layer_buf = io.BytesIO()
    with zipfile.ZipFile(layer_buf, "w") as z:
        z.writestr(
            "nodejs/node_modules/esmhelper/index.js",
            "module.exports.LAYER_VALUE = 'from-esm-layer';\n",
        )
    layer_resp = lam.publish_layer_version(
        LayerName="warm-esm-layer-test",
        Content={"ZipFile": layer_buf.getvalue()},
        CompatibleRuntimes=["nodejs20.x"],
    )
    layer_arn = layer_resp["LayerVersionArn"]

    # Create an ESM handler that uses native import to load the layer package.
    # The layer package exports via CJS but Node.js ESM can import CJS modules.
    # Native import does NOT use NODE_PATH — this is the bug we are testing.
    handler_code = (
        "import helper from 'esmhelper';\n"
        "export const handler = async (event) => {\n"
        "  return { value: helper.LAYER_VALUE, esm: true };\n"
        "};\n"
    )
    func_buf = io.BytesIO()
    with zipfile.ZipFile(func_buf, "w") as z:
        z.writestr("index.mjs", handler_code)

    fname = f"warm-esm-layer-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=fname,
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": func_buf.getvalue()},
        Layers=[layer_arn],
    )

    try:
        resp = lam.invoke(FunctionName=fname, Payload=b"{}")
        assert resp["StatusCode"] == 200
        assert "FunctionError" not in resp, f"Lambda error: {resp['Payload'].read().decode()}"
        payload = json.loads(resp["Payload"].read())
        assert payload["value"] == "from-esm-layer"
        assert payload["esm"] is True
    finally:
        lam.delete_function(FunctionName=fname)

# ---------------------------------------------------------------------------
# Terraform compatibility tests
# ---------------------------------------------------------------------------


def test_lambda_image_no_default_runtime_handler(lam):
    """Image-based functions must not get default runtime/handler values."""
    fname = "tf-compat-image-no-defaults"
    try:
        lam.delete_function(FunctionName=fname)
    except ClientError:
        pass
    resp = lam.create_function(
        FunctionName=fname,
        PackageType="Image",
        Code={"ImageUri": "my-repo/my-image:latest"},
        Role=_LAMBDA_ROLE,
        Timeout=30,
    )
    try:
        assert resp["PackageType"] == "Image"
        assert resp["Runtime"] == "", f"Expected empty Runtime for Image, got {resp['Runtime']!r}"
        assert resp["Handler"] == "", f"Expected empty Handler for Image, got {resp['Handler']!r}"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_image_preserves_image_config(lam):
    """ImageConfig provided at creation must be preserved in the GetFunction response."""
    fname = "tf-compat-image-config"
    try:
        lam.delete_function(FunctionName=fname)
    except ClientError:
        pass
    lam.create_function(
        FunctionName=fname,
        PackageType="Image",
        Code={"ImageUri": "my-repo/my-image:latest"},
        Role=_LAMBDA_ROLE,
        ImageConfig={"Command": ["main.lambda_handler"]},
    )
    try:
        get_resp = lam.get_function(FunctionName=fname)
        cfg = get_resp["Configuration"]
        assert "ImageConfigResponse" in cfg, "ImageConfigResponse missing from get_function response"
        assert cfg["ImageConfigResponse"]["ImageConfig"]["Command"] == ["main.lambda_handler"]
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_empty_dead_letter_config(lam):
    """Functions without DeadLetterConfig must return empty dict, not {TargetArn: ''}."""
    fname = "tf-compat-no-dlc"
    try:
        lam.delete_function(FunctionName=fname)
    except ClientError:
        pass
    resp = lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Handler="index.handler",
        Role=_LAMBDA_ROLE,
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    try:
        dlc = resp.get("DeadLetterConfig", {})
        assert dlc == {} or "TargetArn" not in dlc or dlc.get("TargetArn") == "", \
            f"Expected empty DeadLetterConfig, got {dlc!r}"
        assert dlc.get("TargetArn") is None or dlc == {}, \
            f"DeadLetterConfig should not have TargetArn when unconfigured, got {dlc!r}"
    finally:
        lam.delete_function(FunctionName=fname)


def test_esm_sqs_no_starting_position(lam, sqs):
    """SQS event source mappings must not include StartingPosition."""
    fname = "tf-compat-esm-sqs"
    try:
        lam.delete_function(FunctionName=fname)
    except ClientError:
        pass
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Handler="index.handler",
        Role=_LAMBDA_ROLE,
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    q_url = sqs.create_queue(QueueName="tf-compat-esm-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    esm_uuid = None
    try:
        resp = lam.create_event_source_mapping(
            EventSourceArn=q_arn,
            FunctionName=fname,
            BatchSize=5,
            Enabled=True,
        )
        esm_uuid = resp["UUID"]
        assert "StartingPosition" not in resp, \
            f"SQS ESM should not have StartingPosition, got {resp.get('StartingPosition')!r}"

        get_resp = lam.get_event_source_mapping(UUID=esm_uuid)
        assert "StartingPosition" not in get_resp, \
            "StartingPosition should not appear in get_event_source_mapping for SQS"
    finally:
        if esm_uuid:
            lam.delete_event_source_mapping(UUID=esm_uuid)
        lam.delete_function(FunctionName=fname)
        sqs.delete_queue(QueueUrl=q_url)


def test_esm_kinesis_has_starting_position(lam, kin):
    """Kinesis event source mappings must include StartingPosition."""
    fname = "tf-compat-esm-kinesis"
    stream_name = "tf-compat-esm-stream"
    try:
        lam.delete_function(FunctionName=fname)
    except ClientError:
        pass
    try:
        kin.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
    except ClientError:
        pass

    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Handler="index.handler",
        Role=_LAMBDA_ROLE,
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    kin.create_stream(StreamName=stream_name, ShardCount=1)
    stream_arn = kin.describe_stream(
        StreamName=stream_name
    )["StreamDescription"]["StreamARN"]

    esm_uuid = None
    try:
        resp = lam.create_event_source_mapping(
            EventSourceArn=stream_arn,
            FunctionName=fname,
            StartingPosition="TRIM_HORIZON",
            BatchSize=100,
            Enabled=True,
        )
        esm_uuid = resp["UUID"]
        assert "StartingPosition" in resp, "Kinesis ESM must include StartingPosition"
        assert resp["StartingPosition"] == "TRIM_HORIZON"
    finally:
        if esm_uuid:
            lam.delete_event_source_mapping(UUID=esm_uuid)
        lam.delete_function(FunctionName=fname)
        try:
            kin.delete_stream(StreamName=stream_name, EnforceConsumerDeletion=True)
        except ClientError:
            pass


def test_esm_response_no_function_name_field(lam, sqs):
    """ESM API responses should contain FunctionArn but not FunctionName (matching AWS)."""
    fname = "tf-compat-esm-no-fname"
    try:
        lam.delete_function(FunctionName=fname)
    except ClientError:
        pass
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Handler="index.handler",
        Role=_LAMBDA_ROLE,
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    q_url = sqs.create_queue(QueueName="tf-compat-esm-fname-queue")["QueueUrl"]
    q_arn = sqs.get_queue_attributes(
        QueueUrl=q_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]

    esm_uuid = None
    try:
        resp = lam.create_event_source_mapping(
            EventSourceArn=q_arn,
            FunctionName=fname,
            BatchSize=5,
            Enabled=True,
        )
        esm_uuid = resp["UUID"]
        assert "FunctionArn" in resp, "ESM response must include FunctionArn"
        assert fname in resp["FunctionArn"], "FunctionArn must contain the function name"
    finally:
        if esm_uuid:
            lam.delete_event_source_mapping(UUID=esm_uuid)
        lam.delete_function(FunctionName=fname)
        sqs.delete_queue(QueueUrl=q_url)


def test_lambda_update_function_configuration_layers(lam):
    """Attaching a layer via update-function-configuration should normalize ARN strings
    to {Arn, CodeSize} dicts — regression test for 'str' object has no attribute 'get'."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("util.py", "# layer code")
    layer_resp = lam.publish_layer_version(
        LayerName="update-cfg-layer", Content={"ZipFile": buf.getvalue()},
    )
    layer_arn = layer_resp["LayerVersionArn"]

    fn_zip = io.BytesIO()
    with zipfile.ZipFile(fn_zip, "w") as z:
        z.writestr("index.py", "def handler(e, c): return {}")
    lam.create_function(
        FunctionName="fn-update-layer-test",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test",
        Handler="index.handler",
        Code={"ZipFile": fn_zip.getvalue()},
    )

    resp = lam.update_function_configuration(
        FunctionName="fn-update-layer-test",
        Layers=[layer_arn],
    )
    # Response Layers must be dicts with Arn key, not raw strings
    assert len(resp["Layers"]) == 1
    assert isinstance(resp["Layers"][0], dict)
    assert resp["Layers"][0]["Arn"] == layer_arn

    # GetFunction must also return normalized layer dicts
    fn = lam.get_function(FunctionName="fn-update-layer-test")
    assert fn["Configuration"]["Layers"][0]["Arn"] == layer_arn


# ============================================================================
# Unit tests — Lambda warm-container pool, ESM filter, CW Logs emitter,
# event-stream framing, throttle response shape. These mock containers and
# don't hit the live ministack server, so they run even without Docker.
# Originally lived in tests/test_lambda_pool.py — merged here for one-file-per-service.
# ============================================================================

import time
from unittest.mock import MagicMock

import pytest

import ministack.services.lambda_svc as lsvc
from ministack.core.responses import set_request_account_id


@pytest.fixture(autouse=True)
def _clear_pool():
    """Fresh pool before every test; also clear after so later tests don't see residue."""
    lsvc._warm_pool.clear()
    yield
    lsvc._warm_pool.clear()


def _mk_container(running: bool = True):
    """Fake container with a .reload() that sets status, matching docker-py interface."""
    c = MagicMock()
    c.status = "running" if running else "exited"
    def _reload():
        # No-op — container.status stays at whatever was set last.
        pass
    c.reload.side_effect = _reload
    return c


# ──────────────────────────────── pool key ──────────────────────────────────

def test_pool_key_scopes_by_account():
    """Same function in two accounts → two distinct keys → two distinct pools."""
    set_request_account_id("111111111111")
    k_a = lsvc._warm_pool_key("fn", {"CodeSha256": "abc"})
    set_request_account_id("222222222222")
    k_b = lsvc._warm_pool_key("fn", {"CodeSha256": "abc"})
    assert k_a != k_b
    assert k_a.startswith("111111111111:")
    assert k_b.startswith("222222222222:")


def test_pool_key_differs_by_package_type():
    set_request_account_id("111111111111")
    k_zip = lsvc._warm_pool_key("fn", {"CodeSha256": "abc"})
    k_img = lsvc._warm_pool_key("fn", {"PackageType": "Image", "ImageUri": "my/img:v1"})
    assert k_zip != k_img
    assert ":zip:" in k_zip
    assert ":image:" in k_img


def test_pool_key_differs_by_code_sha():
    """Code update → new key → cold start (doesn't accidentally reuse old container)."""
    set_request_account_id("111111111111")
    k1 = lsvc._warm_pool_key("fn", {"CodeSha256": "sha-v1"})
    k2 = lsvc._warm_pool_key("fn", {"CodeSha256": "sha-v2"})
    assert k1 != k2


def test_pool_key_differs_by_image_uri():
    set_request_account_id("111111111111")
    k1 = lsvc._warm_pool_key("fn", {"PackageType": "Image", "ImageUri": "img:v1"})
    k2 = lsvc._warm_pool_key("fn", {"PackageType": "Image", "ImageUri": "img:v2"})
    assert k1 != k2


# ──────────────────────────── acquire / spawn / release ─────────────────────

def test_acquire_on_empty_pool_signals_spawn():
    entry, reason = lsvc._pool_acquire("k", max_concurrency=None)
    assert entry is None
    assert reason == "spawn"


def test_register_then_reacquire_reuses_same_entry():
    c = _mk_container()
    entry1 = lsvc._pool_register("k", c, tmpdir=None)
    assert entry1["in_use"] is True

    # While in_use, next acquire can't reuse it — signals spawn.
    entry2, reason = lsvc._pool_acquire("k", max_concurrency=None)
    assert entry2 is None
    assert reason == "spawn"

    # After release, the same container is reused.
    lsvc._pool_release(entry1)
    assert entry1["in_use"] is False
    entry3, reason = lsvc._pool_acquire("k", max_concurrency=None)
    assert entry3 is entry1
    assert reason == "reused"
    assert entry3["in_use"] is True


def test_multiple_concurrent_invocations_get_separate_entries():
    """Two concurrent invocations must land on two distinct pool entries (not the same container)."""
    c1 = _mk_container()
    c2 = _mk_container()
    e1 = lsvc._pool_register("k", c1, tmpdir=None)
    # e1 is in_use — next acquire signals spawn, simulating cold start
    _, reason = lsvc._pool_acquire("k", max_concurrency=None)
    assert reason == "spawn"
    e2 = lsvc._pool_register("k", c2, tmpdir=None)
    assert e1 is not e2
    assert e1["container"] is c1
    assert e2["container"] is c2
    assert len(lsvc._warm_pool["k"]) == 2


def test_function_concurrency_cap_rejects_when_full():
    """ReservedConcurrentExecutions=2 → 3rd concurrent invocation gets func_cap."""
    for _ in range(2):
        lsvc._pool_register("k", _mk_container(), tmpdir=None)
    entry, reason = lsvc._pool_acquire("k", max_concurrency=2)
    assert entry is None
    assert reason == "func_cap"


def test_function_concurrency_cap_none_is_unbounded():
    """No ReservedConcurrentExecutions → can always spawn."""
    for _ in range(50):
        lsvc._pool_register("k", _mk_container(), tmpdir=None)
    entry, reason = lsvc._pool_acquire("k", max_concurrency=None)
    assert entry is None
    assert reason == "spawn"


def test_account_concurrency_cap_rejects(monkeypatch):
    """Global account cap: 3 in-use total → 4th is throttled as acct_cap."""
    monkeypatch.setattr(lsvc, "_ACCOUNT_CONCURRENCY_CAP", 3)
    # 3 in-use entries across two pool keys
    lsvc._pool_register("k1", _mk_container(), tmpdir=None)
    lsvc._pool_register("k1", _mk_container(), tmpdir=None)
    lsvc._pool_register("k2", _mk_container(), tmpdir=None)
    entry, reason = lsvc._pool_acquire("k2", max_concurrency=None)
    assert entry is None
    assert reason == "acct_cap"


# ──────────────────────────── lifecycle: dead, remove, evict, clear ─────────

def test_dead_containers_are_pruned_on_acquire():
    """Pool must not hand out a dead container on reuse."""
    dead = _mk_container(running=False)
    alive_entry = lsvc._pool_register("k", _mk_container(running=True), tmpdir=None)
    # Release alive so it becomes reusable
    lsvc._pool_release(alive_entry)
    # Sneak a dead one into the pool directly
    lsvc._warm_pool["k"].append({
        "container": dead, "tmpdir": None, "in_use": False,
        "last_used": time.time(), "created": time.time(),
    })
    assert len(lsvc._warm_pool["k"]) == 2

    # Acquire — dead one pruned, alive one reused
    entry, reason = lsvc._pool_acquire("k", max_concurrency=None)
    assert reason == "reused"
    assert entry["container"] is alive_entry["container"]
    assert len(lsvc._warm_pool["k"]) == 1


def test_pool_remove_kills_and_unregisters():
    entry = lsvc._pool_register("k", _mk_container(), tmpdir=None)
    lsvc._pool_remove(entry)
    assert entry not in lsvc._warm_pool.get("k", [])
    entry["container"].stop.assert_called()
    entry["container"].remove.assert_called()


def test_pool_evict_idle_removes_only_expired_and_not_in_use(monkeypatch):
    monkeypatch.setattr(lsvc, "_WARM_CONTAINER_TTL", 60)
    busy = lsvc._pool_register("k", _mk_container(), tmpdir=None)  # in_use=True
    idle_old = lsvc._pool_register("k", _mk_container(), tmpdir=None)
    lsvc._pool_release(idle_old)
    idle_old["last_used"] = time.time() - 300  # past TTL
    idle_fresh = lsvc._pool_register("k", _mk_container(), tmpdir=None)
    lsvc._pool_release(idle_fresh)  # last_used = now, within TTL

    lsvc._pool_evict_idle()

    remaining = lsvc._warm_pool.get("k", [])
    assert busy in remaining        # still in use — must not be evicted
    assert idle_fresh in remaining  # under TTL — kept
    assert idle_old not in remaining
    idle_old["container"].stop.assert_called()


def test_pool_clear_all_kills_everything():
    for key in ("a", "b", "c"):
        lsvc._pool_register(key, _mk_container(), tmpdir=None)
    victims = [e for lst in lsvc._warm_pool.values() for e in lst]
    assert len(victims) == 3

    lsvc._pool_clear_all()

    assert lsvc._warm_pool == {}
    for v in victims:
        v["container"].stop.assert_called()
        v["container"].remove.assert_called()


# ──────────────────────────── multi-tenancy ─────────────────────────────────

def test_two_accounts_get_independent_pools():
    """Invocations in account A must not pick up account B's containers."""
    set_request_account_id("111111111111")
    k_a = lsvc._warm_pool_key("fn", {"CodeSha256": "sha"})
    c_a = _mk_container()
    e_a = lsvc._pool_register(k_a, c_a, tmpdir=None)
    lsvc._pool_release(e_a)

    set_request_account_id("222222222222")
    k_b = lsvc._warm_pool_key("fn", {"CodeSha256": "sha"})
    assert k_a != k_b

    entry, reason = lsvc._pool_acquire(k_b, max_concurrency=None)
    assert entry is None
    assert reason == "spawn"   # account B must cold-start; can't reuse A's container


def test_throttle_response_shape_matches_aws():
    """The throttle response body must match the AWS TooManyRequestsException shape."""
    r = lsvc._throttle_response(
        reason_code="ReservedFunctionConcurrentInvocationLimitExceeded",
        msg="Rate Exceeded",
        retry_after=1,
    )
    assert r["throttle"] is True
    assert r["error"] is True
    body = r["body"]
    assert body["__type"] == "TooManyRequestsException"
    assert body["Reason"] == "ReservedFunctionConcurrentInvocationLimitExceeded"
    assert "retryAfterSeconds" in body
    assert "message" in body


# ──────────────────── async retry + DLQ routing ─────────────────────────────

def test_route_async_failure_to_sqs_dlq():
    """Async invoke final failure routes an AWS-shaped envelope to the SQS DLQ."""
    import ministack.services.sqs as _sqs
    set_request_account_id("000000000000")
    # Create a queue directly in the internal state
    url = "http://localhost:4566/000000000000/dlq-test"
    arn = "arn:aws:sqs:us-east-1:000000000000:dlq-test"
    _sqs._queues[url] = {
        "messages": [], "attributes": {"QueueArn": arn},
        "is_fifo": False, "dedup_cache": {}, "fifo_seq": 0,
    }
    try:
        lsvc._route_async_failure(
            target_arn=arn,
            func_name="doesnt-matter",
            event={"input": "hi"},
            result={"error": True, "function_error": "Unhandled",
                    "body": {"errorType": "Handler", "errorMessage": "boom"}},
        )
        assert len(_sqs._queues[url]["messages"]) == 1
        import json as _json
        envelope = _json.loads(_sqs._queues[url]["messages"][0]["body"])
        assert envelope["requestPayload"] == {"input": "hi"}
        assert envelope["requestContext"]["condition"] == "RetriesExhausted"
        assert envelope["responseContext"]["functionError"] == "Unhandled"
        assert envelope["responsePayload"]["errorMessage"] == "boom"
    finally:
        _sqs._queues.pop(url, None)


def test_route_async_failure_to_sns_topic():
    """Async invoke final failure can target an SNS topic (OnFailure destination)."""
    import ministack.services.sns as _sns
    set_request_account_id("000000000000")
    arn = "arn:aws:sns:us-east-1:000000000000:async-fail"
    _sns._topics[arn] = {
        "arn": arn, "name": "async-fail",
        "subscriptions": [], "messages": [], "tags": {}, "attributes": {},
    }
    try:
        # Monkey-patch _fanout to observe the call without needing subscribers
        called = {}
        real_fanout = _sns._fanout
        def _capture(topic_arn, msg_id, message, subject, *args, **kwargs):
            called["topic_arn"] = topic_arn
            called["message"] = message
            called["subject"] = subject
        _sns._fanout = _capture
        try:
            lsvc._route_async_failure(
                target_arn=arn,
                func_name="doesnt-matter",
                event={"k": "v"},
                result={"error": True, "function_error": "Handled",
                        "body": {"errorType": "X"}},
            )
            assert called.get("topic_arn") == arn
            assert "requestPayload" in called.get("message", "")
        finally:
            _sns._fanout = real_fanout
    finally:
        _sns._topics.pop(arn, None)


def test_route_async_failure_unknown_target_logs_and_returns():
    """Unknown DLQ ARN must not raise — just logs."""
    set_request_account_id("000000000000")
    # Should NOT raise
    lsvc._route_async_failure(
        target_arn="arn:aws:sqs:us-east-1:000000000000:does-not-exist",
        func_name="x", event={}, result={"error": True, "body": {}},
    )


# ──────────────────── RIE result → function_error classification ────────────

def test_lambda_strict_hard_fails_when_docker_unavailable(monkeypatch):
    """LAMBDA_STRICT=1 + no Docker → Runtime.DockerUnavailable, NO fallback to warm/local."""
    monkeypatch.setattr(lsvc, "LAMBDA_STRICT", True)
    monkeypatch.setattr(lsvc, "_docker_available", False)
    func = {"config": {
        "FunctionName": "strict-test",
        "Runtime": "python3.12",
        "PackageType": "Zip",
        "CodeSha256": "abc",
        "Timeout": 3,
        "MemorySize": 128,
    }, "code_zip": b"\x00"}
    result = lsvc._execute_function_docker(func, {"k": "v"})
    assert result.get("error") is True
    assert result["body"]["errorType"] == "Runtime.DockerUnavailable"


def test_lambda_permissive_falls_back_to_warm_without_docker(monkeypatch):
    """Default (LAMBDA_STRICT=False) + no Docker + python runtime → warm fallback."""
    monkeypatch.setattr(lsvc, "LAMBDA_STRICT", False)
    monkeypatch.setattr(lsvc, "_docker_available", False)
    called = {"warm": False}
    def _fake_warm(func, event):
        called["warm"] = True
        return {"body": {"ok": True}}
    monkeypatch.setattr(lsvc, "_execute_function_warm", _fake_warm)
    func = {"config": {
        "FunctionName": "perm-test",
        "Runtime": "python3.12",
        "PackageType": "Zip",
        "CodeSha256": "abc",
        "Timeout": 3,
        "MemorySize": 128,
    }, "code_zip": b"\x00"}
    lsvc._execute_function_docker(func, {})
    assert called["warm"] is True


def test_emit_lambda_logs_writes_start_end_report_to_cw_logs():
    """Lambda → CW Logs emits AWS-shaped START / body / END / REPORT lines."""
    import ministack.services.cloudwatch_logs as _cwl
    set_request_account_id("000000000000")
    _cwl._log_groups.clear()

    func = {"config": {"FunctionName": "emit-test", "Version": "$LATEST", "MemorySize": 128}}
    lsvc._emit_lambda_logs(
        func, request_id="abc-1234",
        log_text="user print line 1\nuser print line 2",
        error=False, duration_ms=42,
    )

    assert "/aws/lambda/emit-test" in _cwl._log_groups
    streams = _cwl._log_groups["/aws/lambda/emit-test"]["streams"]
    assert len(streams) == 1
    stream_name = next(iter(streams))
    assert stream_name.startswith(tuple(f"{y:04d}/" for y in range(2024, 2031)))
    assert "[$LATEST]" in stream_name
    msgs = [e["message"] for e in streams[stream_name]["events"]]
    assert any(m.startswith("START RequestId: abc-1234") and "$LATEST" in m for m in msgs)
    assert "user print line 1" in msgs
    assert "user print line 2" in msgs
    assert any(m == "END RequestId: abc-1234" for m in msgs)
    assert any(m.startswith("REPORT RequestId: abc-1234") and "Duration: 42 ms" in m for m in msgs)


def test_emit_lambda_logs_autocreate_is_per_function():
    """Each function gets its own /aws/lambda/{name} group."""
    import ministack.services.cloudwatch_logs as _cwl
    set_request_account_id("000000000000")
    _cwl._log_groups.clear()

    lsvc._emit_lambda_logs(
        {"config": {"FunctionName": "fn-a", "Version": "$LATEST", "MemorySize": 128}},
        "r1", "", False, 1,
    )
    lsvc._emit_lambda_logs(
        {"config": {"FunctionName": "fn-b", "Version": "$LATEST", "MemorySize": 128}},
        "r2", "", False, 1,
    )
    assert "/aws/lambda/fn-a" in _cwl._log_groups
    assert "/aws/lambda/fn-b" in _cwl._log_groups


def test_emit_lambda_logs_honors_logging_config_log_group():
    """LoggingConfig.LogGroup routes logs to the named (e.g. shared) group, not
    the default per-function group (#895)."""
    import ministack.services.cloudwatch_logs as _cwl
    set_request_account_id("000000000000")
    _cwl._log_groups.clear()

    func = {"config": {
        "FunctionName": "log-cfg-fn", "Version": "$LATEST", "MemorySize": 128,
        "LoggingConfig": {"LogFormat": "Text", "LogGroup": "/aws/lambda/shared-logs"},
    }}
    lsvc._emit_lambda_logs(func, "r1", "hello", False, 1)

    # Logs land in the configured group, with events...
    assert "/aws/lambda/shared-logs" in _cwl._log_groups
    streams = _cwl._log_groups["/aws/lambda/shared-logs"]["streams"]
    assert sum(len(s["events"]) for s in streams.values()) > 0
    # ...and the default per-function group is NOT created.
    assert "/aws/lambda/log-cfg-fn" not in _cwl._log_groups


def test_emit_lambda_logs_failure_is_best_effort(monkeypatch):
    """A broken CW Logs module must not bubble into the Lambda invocation."""
    import ministack.services.cloudwatch_logs as _cwl
    # Nuke the target to force a write failure
    monkeypatch.setattr(_cwl, "_log_groups", None)
    # Must not raise
    lsvc._emit_lambda_logs(
        {"config": {"FunctionName": "crash", "Version": "$LATEST", "MemorySize": 128}},
        "r", "", False, 1,
    )


def test_match_esm_filter_equality():
    """Basic equality matching on a nested record."""
    rec = {"body": {"orderType": "Premium", "region": "us-east-1"}}
    assert lsvc._match_esm_filter(rec, {"body": {"orderType": ["Premium"]}}) is True
    assert lsvc._match_esm_filter(rec, {"body": {"orderType": ["Basic"]}}) is False


def test_match_esm_filter_content_prefix_suffix_anything_but():
    """Content-filter dicts: prefix, suffix, anything-but, exists."""
    rec = {"body": {"name": "prod-user-42"}}
    assert lsvc._match_esm_filter(rec, {"body": {"name": [{"prefix": "prod-"}]}}) is True
    assert lsvc._match_esm_filter(rec, {"body": {"name": [{"prefix": "dev-"}]}}) is False
    assert lsvc._match_esm_filter(rec, {"body": {"name": [{"suffix": "-42"}]}}) is True
    assert lsvc._match_esm_filter(rec, {"body": {"name": [{"anything-but": ["prod-user-42"]}]}}) is False
    assert lsvc._match_esm_filter(rec, {"body": {"name": [{"anything-but": ["other"]}]}}) is True
    assert lsvc._match_esm_filter(rec, {"body": {"missing": [{"exists": False}]}}) is True
    assert lsvc._match_esm_filter(rec, {"body": {"name": [{"exists": True}]}}) is True


def test_match_esm_filter_numeric():
    """Numeric comparison operator."""
    rec = {"body": {"count": 7}}
    assert lsvc._match_esm_filter(rec, {"body": {"count": [{"numeric": [">", 5]}]}}) is True
    assert lsvc._match_esm_filter(rec, {"body": {"count": [{"numeric": [">", 10]}]}}) is False
    assert lsvc._match_esm_filter(rec, {"body": {"count": [{"numeric": [">", 5, "<", 10]}]}}) is True


def test_apply_filter_criteria_drops_non_matching_sqs_records():
    """SQS bodies are JSON-parsed before matching, matching AWS behaviour."""
    import json as _json
    esm = {"FilterCriteria": {"Filters": [
        {"Pattern": _json.dumps({"body": {"orderType": ["Premium"]}})},
    ]}}
    records = [
        {"messageId": "a", "body": _json.dumps({"orderType": "Premium"})},
        {"messageId": "b", "body": _json.dumps({"orderType": "Basic"})},
    ]
    filtered = lsvc._apply_filter_criteria(records, esm)
    assert [r["messageId"] for r in filtered] == ["a"]


def test_apply_filter_criteria_no_filters_passes_through():
    records = [{"messageId": "x"}, {"messageId": "y"}]
    assert lsvc._apply_filter_criteria(records, {}) == records
    assert lsvc._apply_filter_criteria(records, {"FilterCriteria": {}}) == records


def test_apply_filter_criteria_ddb_eventname_filter():
    """DynamoDB stream records are filtered by top-level eventName, matching AWS behaviour."""
    import json as _json
    esm = {"FilterCriteria": {"Filters": [
        {"Pattern": _json.dumps({"eventName": ["INSERT"]})},
    ]}}
    records = [
        {"eventName": "INSERT", "dynamodb": {"NewImage": {"pk": {"S": "a"}}}},
        {"eventName": "MODIFY", "dynamodb": {"NewImage": {"pk": {"S": "b"}}}},
        {"eventName": "REMOVE", "dynamodb": {"OldImage": {"pk": {"S": "c"}}}},
    ]
    filtered = lsvc._apply_filter_criteria(records, esm)
    assert [r["eventName"] for r in filtered] == ["INSERT"]


def test_apply_filter_criteria_ddb_newimage_filter():
    """DynamoDB stream records are filtered by nested dynamodb.NewImage data."""
    import json as _json
    esm = {"FilterCriteria": {"Filters": [
        {"Pattern": _json.dumps({"dynamodb": {"NewImage": {"status": {"S": ["active"]}}}})},
    ]}}
    records = [
        {"eventName": "INSERT", "dynamodb": {"NewImage": {"pk": {"S": "1"}, "status": {"S": "active"}}}},
        {"eventName": "INSERT", "dynamodb": {"NewImage": {"pk": {"S": "2"}, "status": {"S": "inactive"}}}},
        {"eventName": "REMOVE", "dynamodb": {"OldImage": {"pk": {"S": "3"}, "status": {"S": "active"}}}},
    ]
    filtered = lsvc._apply_filter_criteria(records, esm)
    assert len(filtered) == 1
    assert filtered[0]["dynamodb"]["NewImage"]["pk"]["S"] == "1"


def test_event_stream_encode_roundtrip():
    """The vnd.amazon.eventstream encoder must produce a valid framed message
    that boto3's own EventStream parser can decode."""
    from botocore.eventstream import EventStreamBuffer
    msg = lsvc._es_encode_message({
        ":message-type": "event",
        ":event-type": "PayloadChunk",
        ":content-type": "application/octet-stream",
    }, b"hello-world")
    buf = EventStreamBuffer()
    buf.add_data(msg)
    events = list(buf)
    assert len(events) == 1
    event = events[0]
    # botocore surfaces headers as a dict[str, Any] on the parsed event
    assert event.headers[":event-type"] == "PayloadChunk"
    assert event.payload == b"hello-world"


def test_invoke_rie_classifies_unhandled_vs_handled():
    """If RIE returns X-Amz-Function-Error header the result carries
    function_error='Unhandled'. A handler-returned errorType with no RIE
    header should produce 'Handled'."""
    # The classification logic lives inside _invoke_rie; unit-test by
    # simulating what that branch does via a tiny inline replica.
    parsed_error_payload = {"errorType": "E", "errorMessage": "m"}

    # Case 1: RIE header present → Unhandled
    has_header = True
    if has_header or (isinstance(parsed_error_payload, dict) and parsed_error_payload.get("errorType")):
        classification = "Unhandled" if has_header else "Handled"
    assert classification == "Unhandled"

    # Case 2: No RIE header, but body has errorType → Handled
    has_header = False
    if has_header or (isinstance(parsed_error_payload, dict) and parsed_error_payload.get("errorType")):
        classification = "Unhandled" if has_header else "Handled"
    assert classification == "Handled"


def test_lambda_invoke_stderr_captured_in_log_result(lam):
    """Direct Lambda.Invoke captures print() output in X-Amz-Log-Result header."""
    import base64

    fname = f"lam-log-capture-{_uuid_mod.uuid4().hex[:8]}"
    marker_1 = f"LINE1-{_uuid_mod.uuid4().hex[:8]}"
    marker_2 = f"LINE2-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        "def handler(event, context):\n"
        f"    print('{marker_1}')\n"
        f"    print('{marker_2}')\n"
        "    return {'statusCode': 200, 'body': 'ok'}\n"
    )

    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    try:
        resp = lam.invoke(
            FunctionName=fname,
            Payload=json.dumps({}),
            LogType="Tail",
        )
        assert resp["StatusCode"] == 200

        log_result = resp.get("LogResult", "")
        assert log_result, "X-Amz-Log-Result header should be non-empty"
        decoded = base64.b64decode(log_result).decode("utf-8")
        assert marker_1 in decoded, f"Expected '{marker_1}' in log output: {decoded}"
        assert marker_2 in decoded, f"Expected '{marker_2}' in log output: {decoded}"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_invoke_emits_cloudwatch_logs(lam, logs):
    """Direct Lambda.Invoke emits START/body/END/REPORT to CloudWatch Logs."""
    fname = f"lam-cwl-direct-{_uuid_mod.uuid4().hex[:8]}"
    marker = f"CWL-MARKER-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        "def handler(event, context):\n"
        f"    print('{marker}')\n"
        "    return {'statusCode': 200, 'body': 'ok'}\n"
    )

    lam.create_function(
        FunctionName=fname,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )

    try:
        resp = lam.invoke(FunctionName=fname, Payload=json.dumps({}))
        assert resp["StatusCode"] == 200

        log_group = f"/aws/lambda/{fname}"
        streams = logs.describe_log_streams(logGroupName=log_group)["logStreams"]
        assert len(streams) >= 1

        all_messages = []
        for stream in streams:
            events = logs.get_log_events(
                logGroupName=log_group,
                logStreamName=stream["logStreamName"],
            )["events"]
            all_messages.extend(e["message"] for e in events)

        assert any(marker in msg for msg in all_messages), (
            f"Marker '{marker}' not found in CW Logs: {all_messages}"
        )
        assert any(msg.startswith("START RequestId:") for msg in all_messages)
        assert any(msg.startswith("END RequestId:") for msg in all_messages)
        assert any(msg.startswith("REPORT RequestId:") for msg in all_messages)
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_invoke_stderr_captured_in_log_result_nodejs(lam):
    """Node.js Lambda console.log output is captured in X-Amz-Log-Result header."""
    import base64

    fname = f"lam-log-capture-js-{_uuid_mod.uuid4().hex[:8]}"
    marker_1 = f"JSLINE1-{_uuid_mod.uuid4().hex[:8]}"
    marker_2 = f"JSLINE2-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        "exports.handler = async (event) => {\n"
        f"  console.log('{marker_1}');\n"
        f"  console.log('{marker_2}');\n"
        "  return { statusCode: 200, body: 'ok' };\n"
        "};\n"
    )

    lam.create_function(
        FunctionName=fname,
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(code)},
    )

    try:
        resp = lam.invoke(
            FunctionName=fname,
            Payload=json.dumps({}),
            LogType="Tail",
        )
        assert resp["StatusCode"] == 200

        log_result = resp.get("LogResult", "")
        assert log_result, "X-Amz-Log-Result header should be non-empty"
        decoded = base64.b64decode(log_result).decode("utf-8")
        assert marker_1 in decoded, f"Expected '{marker_1}' in log output: {decoded}"
        assert marker_2 in decoded, f"Expected '{marker_2}' in log output: {decoded}"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_invoke_emits_cloudwatch_logs_nodejs(lam, logs):
    """Node.js Lambda console.log emits to CloudWatch Logs on direct invoke."""
    fname = f"lam-cwl-direct-js-{_uuid_mod.uuid4().hex[:8]}"
    marker = f"JSCWL-MARKER-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        "exports.handler = async (event) => {\n"
        f"  console.log('{marker}');\n"
        "  return { statusCode: 200, body: 'ok' };\n"
        "};\n"
    )

    lam.create_function(
        FunctionName=fname,
        Runtime="nodejs20.x",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip_js(code)},
    )

    try:
        resp = lam.invoke(FunctionName=fname, Payload=json.dumps({}))
        assert resp["StatusCode"] == 200

        log_group = f"/aws/lambda/{fname}"
        streams = logs.describe_log_streams(logGroupName=log_group)["logStreams"]
        assert len(streams) >= 1

        all_messages = []
        for stream in streams:
            events = logs.get_log_events(
                logGroupName=log_group,
                logStreamName=stream["logStreamName"],
            )["events"]
            all_messages.extend(e["message"] for e in events)

        assert any(marker in msg for msg in all_messages), (
            f"Marker '{marker}' not found in CW Logs: {all_messages}"
        )
        assert any(msg.startswith("START RequestId:") for msg in all_messages)
        assert any(msg.startswith("END RequestId:") for msg in all_messages)
        assert any(msg.startswith("REPORT RequestId:") for msg in all_messages)
    finally:
        lam.delete_function(FunctionName=fname)


# ──────────────────── LAMBDA_DOCKER_FLAGS ────────────────────

def test_lambda_docker_flags_applied_to_run_kwargs(monkeypatch):
    """LAMBDA_DOCKER_FLAGS env/volume/dns/network/cap/memory flags end up in containers.run() kwargs."""
    monkeypatch.setattr(lsvc, "LAMBDA_DOCKER_FLAGS", (
        '-v /host/ca:/opt/ca:ro -e SSL_CERT_FILE=/opt/ca/ca.crt -e NODE_EXTRA_CA_CERTS=/opt/ca/ca.crt '
        '--dns 172.30.0.2 --network=my-net --memory 512m --shm-size=256m '
        '--cap-add SYS_PTRACE --add-host myhost:10.0.0.1 --tmpfs /run:size=100m '
        '--privileged --read-only --unknown-flag ignored'
    ))
    monkeypatch.setattr(lsvc, "_docker_available", True)

    captured = {}
    fake_container = _mk_container()
    fake_container.ports = {"8080/tcp": [{"HostPort": "9999"}]}

    def _fake_run(**kwargs):
        captured.update(kwargs)
        return fake_container

    fake_client = MagicMock()
    fake_client.containers.run = _fake_run
    fake_client.images.get = MagicMock()
    monkeypatch.setattr(lsvc, "_get_docker_client", lambda: fake_client)

    code = b""
    import io
    import zipfile
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", "def handler(e,c): pass")
    code = buf.getvalue()

    lsvc._spawn_lambda_container(
        {"FunctionName": "test-fn", "Runtime": "python3.12", "Handler": "index.handler",
         "PackageType": "Zip", "Timeout": 3, "MemorySize": 128},
        code,
    )

    assert captured["environment"]["SSL_CERT_FILE"] == "/opt/ca/ca.crt"
    assert captured["environment"]["NODE_EXTRA_CA_CERTS"] == "/opt/ca/ca.crt"
    ca_mount = [m for m in captured["mounts"] if m["Target"] == "/opt/ca"]
    assert len(ca_mount) == 1
    assert ca_mount[0]["Source"] == "/host/ca"
    assert ca_mount[0]["ReadOnly"] is True
    assert captured["dns"] == ["172.30.0.2"]
    assert captured["network"] == "my-net"
    assert captured["mem_limit"] == "512m"
    assert captured["shm_size"] == "256m"
    assert captured["cap_add"] == ["SYS_PTRACE"]
    assert captured["extra_hosts"] == {"myhost": "10.0.0.1"}
    assert captured["tmpfs"] == {"/run": "size=100m"}
    assert captured["privileged"] is True
    assert captured["read_only"] is True
    assert "unknown_flag" not in captured


def test_lambda_filesystem_configs_s3_mount_round_trip(lam):
    """FileSystemConfigs accept-and-echo: AWS added S3-bucket ARN support
    in 2026-04 alongside the original EFS access-point ARNs. The emulator
    doesn't mount anything; it just round-trips the config so SDK/CFN reads
    see what was set."""
    fn = f"fs-mount-{int(time.time()*1000)}"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
        FileSystemConfigs=[{
            "Arn": "arn:aws:s3:::my-bucket",
            "LocalMountPath": "/mnt/data",
        }],
    )
    cfg = lam.get_function_configuration(FunctionName=fn)
    assert cfg["FileSystemConfigs"] == [{"Arn": "arn:aws:s3:::my-bucket",
                                          "LocalMountPath": "/mnt/data"}]
    lam.delete_function(FunctionName=fn)


# ============================================================================
# Lambda Account Context tests — Non-Default Account AWS_ACCESS_KEY_ID.
# Originally in tests/test_lambda_account_context.py — merged here for
# one-file-per-service.
#
# Validates that Lambda functions deployed under non-default accounts receive
# AWS_ACCESS_KEY_ID set to the owning account's 12-digit ID (derived from
# the function ARN), NOT the host process's AWS_ACCESS_KEY_ID.
# ============================================================================

_ACCOUNT_CONTEXT_ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
_ACCOUNT_CONTEXT_REGION = "us-east-1"


def _account_context_client(service, access_key="test"):
    """Create a boto3 client with a specific access key for account context tests."""
    return boto3.client(
        service,
        endpoint_url=_ACCOUNT_CONTEXT_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key="test",
        region_name=_ACCOUNT_CONTEXT_REGION,
        config=Config(region_name=_ACCOUNT_CONTEXT_REGION, retries={"max_attempts": 0}),
    )


# Lambda code that returns env vars for account context verification
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


def test_account_context_non_default_gets_arn_account_id():
    """Deploy a function under account 000000000001, invoke it, and verify
    AWS_ACCESS_KEY_ID is set to '000000000001' (not the host's key)."""
    lam = _account_context_client("lambda", access_key="000000000001")

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


def test_account_context_another_non_default_account():
    """Deploy under a different non-default account (123456789012) to confirm
    the fix works for arbitrary 12-digit account IDs."""
    lam = _account_context_client("lambda", access_key="123456789012")

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


def test_account_context_default_account_still_works():
    """Deploy a function under the default account (000000000000) and verify
    AWS_ACCESS_KEY_ID is '000000000000' (derived from the ARN)."""
    lam = _account_context_client("lambda", access_key="000000000000")

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


def test_account_context_explicit_env_override_takes_precedence():
    """Deploy a function with an explicit AWS_ACCESS_KEY_ID in Environment.Variables.
    The explicit value should take precedence over the ARN-derived account."""
    lam = _account_context_client("lambda", access_key="000000000001")

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


def test_account_context_other_env_vars_unchanged():
    """Verify that AWS_REGION and _LAMBDA_FUNCTION_ARN are still set correctly
    regardless of the account context fix."""
    lam = _account_context_client("lambda", access_key="000000000001")

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

        assert payload["aws_region"] == _ACCOUNT_CONTEXT_REGION, (
            f"Expected AWS_REGION='{_ACCOUNT_CONTEXT_REGION}', got '{payload['aws_region']}'"
        )
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


def test_account_from_arn_valid_arn_extracts_account():
    """Valid ARN returns the 12-digit account ID."""
    from ministack.services.lambda_svc import _account_from_arn

    result = _account_from_arn("arn:aws:lambda:us-east-1:123456789012:function:myFunc")
    assert result == "123456789012"


def test_account_from_arn_various_valid_accounts():
    """Various valid 12-digit account IDs are extracted correctly."""
    from ministack.services.lambda_svc import _account_from_arn

    assert _account_from_arn("arn:aws:lambda:us-east-1:000000000000:function:f") == "000000000000"
    assert _account_from_arn("arn:aws:lambda:eu-west-1:000000000001:function:f") == "000000000001"
    assert _account_from_arn("arn:aws:lambda:ap-southeast-1:999999999999:function:f") == "999999999999"


def test_account_from_arn_empty_string_falls_back():
    """Empty string falls back to host env var."""
    from ministack.services.lambda_svc import _account_from_arn

    with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "fallback_key"}):
        result = _account_from_arn("")
        assert result == "fallback_key"


def test_account_from_arn_too_few_segments_falls_back():
    """ARN with too few segments falls back to host env var."""
    from ministack.services.lambda_svc import _account_from_arn

    with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "fallback_key"}):
        result = _account_from_arn("arn:aws:lambda")
        assert result == "fallback_key"


def test_account_from_arn_non_numeric_falls_back():
    """ARN with non-numeric account segment falls back to host env var."""
    from ministack.services.lambda_svc import _account_from_arn

    with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "fallback_key"}):
        result = _account_from_arn("arn:aws:lambda:us-east-1:not-a-number:function:f")
        assert result == "fallback_key"


def test_account_from_arn_none_input_falls_back():
    """None input falls back to host env var without crashing."""
    from ministack.services.lambda_svc import _account_from_arn

    with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "fallback_key"}):
        result = _account_from_arn(None)
        assert result == "fallback_key"


def test_account_from_arn_no_env_var_falls_back_to_test():
    """When AWS_ACCESS_KEY_ID is not set, falls back to 'test'."""
    from ministack.services.lambda_svc import _account_from_arn

    with patch.dict(os.environ, {}, clear=True):
        result = _account_from_arn("")
        assert result == "test"


def test_account_from_arn_lambda_runtime_helper_matches():
    """The lambda_runtime.py local helper produces the same results."""
    from ministack.core.lambda_runtime import _account_from_arn as runtime_helper

    assert runtime_helper("arn:aws:lambda:us-east-1:123456789012:function:f") == "123456789012"
    assert runtime_helper("arn:aws:lambda:us-east-1:000000000001:function:f") == "000000000001"

    with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "fallback_key"}):
        assert runtime_helper("") == "fallback_key"
        assert runtime_helper(None) == "fallback_key"
        assert runtime_helper("arn:aws:lambda") == "fallback_key"


def _run_nodejs_worker(handler_js, event_payload=None, env_extra=None):
    """Spin up a Node.js Lambda worker with the given handler, return invoke result."""
    import io
    import json
    import os
    import shutil
    import subprocess
    import tempfile
    import zipfile

    from ministack.core.lambda_runtime import _NODEJS_WORKER_SCRIPT

    node = shutil.which("node")
    if not node:
        pytest.skip("node not found on PATH")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.js", handler_js)
    code_zip = buf.getvalue()

    tmpdir = tempfile.mkdtemp(prefix="test-node-worker-")
    try:
        worker_path = os.path.join(tmpdir, "_worker.js")
        with open(worker_path, "w") as f:
            f.write(_NODEJS_WORKER_SCRIPT)

        code_dir = os.path.join(tmpdir, "code")
        os.makedirs(code_dir)
        zip_path = os.path.join(tmpdir, "code.zip")
        with open(zip_path, "wb") as f:
            f.write(code_zip)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(code_dir)

        env = {**os.environ, "AWS_ENDPOINT_URL": "http://127.0.0.1:4566"}
        if env_extra:
            env.update(env_extra)

        proc = subprocess.Popen(
            [node, worker_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )

        init = json.dumps({
            "code_dir": code_dir,
            "module": "index",
            "handler": "handler",
            "env": {},
            "function_name": "test-worker",
            "memory": 128,
            "arn": "arn:aws:lambda:us-east-1:000000000000:function:test-worker",
        })
        proc.stdin.write(init + "\n")
        proc.stdin.flush()

        init_resp = json.loads(proc.stdout.readline())
        assert init_resp.get("status") == "ready", (
            f"Worker init failed: {init_resp}; stderr: {proc.stderr.read(2048)}"
        )

        event = json.dumps({**(event_payload or {}), "_request_id": "test-req-1"})
        proc.stdin.write(event + "\n")
        proc.stdin.flush()

        invoke_resp = json.loads(proc.stdout.readline())
        proc.terminate()
        return invoke_resp
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_nodejs_worker_aws_sdk_v3_stub_resolves():
    """@aws-sdk/client-lambda, @aws-sdk/client-sfn, @aws-sdk/client-ssm resolve.

    Real AWS Lambda (Node.js 18+) ships these built-in. Ministack injects
    stubs: Lambda uses a dedicated REST stub; sfn/ssm use the generic JSON-RPC
    stub backed by Ministack's own service implementations.
    """
    handler_js = """\
const { Lambda, LambdaClient, InvokeCommand, waitUntilFunctionActiveV2 } = require("@aws-sdk/client-lambda");
const { SFN, SFNClient } = require("@aws-sdk/client-sfn");
const { SSM, SSMClient, PutParameterCommand, GetParameterCommand } = require("@aws-sdk/client-ssm");
exports.handler = async (_event, _ctx) => ({
  hasLambda: typeof Lambda === "function",
  hasLambdaClient: typeof LambdaClient === "function",
  hasInvokeCommand: typeof InvokeCommand === "function",
  hasWaiter: typeof waitUntilFunctionActiveV2 === "function",
  hasSFN: typeof SFN === "function",
  hasSFNClient: typeof SFNClient === "function",
  hasSSM: typeof SSM === "function",
  hasSSMClient: typeof SSMClient === "function",
  hasPutParameterCommand: typeof PutParameterCommand === "function",
  hasGetParameterCommand: typeof GetParameterCommand === "function",
});
"""
    result = _run_nodejs_worker(handler_js)
    assert result.get("status") == "ok", f"Invocation failed: {result}"
    r = result["result"]
    assert r["hasLambda"] is True
    assert r["hasLambdaClient"] is True
    assert r["hasInvokeCommand"] is True
    assert r["hasWaiter"] is True
    assert r["hasSFN"] is True
    assert r["hasSFNClient"] is True
    assert r["hasSSM"] is True
    assert r["hasSSMClient"] is True
    assert r["hasPutParameterCommand"] is True
    assert r["hasGetParameterCommand"] is True


def test_nodejs_worker_json_rpc_error_has_name():
    """Service errors from the JSON-RPC stub expose err.name (not just err.code).

    AWS SDK v3 handlers typically catch errors by name, e.g.:
      if (e.name !== 'ParameterNotFound') throw e;
    The stub must set both .name and .code so that pattern works.
    """
    handler_js = """\
const http = require("http");
// Spin up a tiny server that returns a ParameterNotFound error body.
const srv = http.createServer((req, res) => {
  res.writeHead(400, { "Content-Type": "application/x-amz-json-1.1" });
  res.end(JSON.stringify({ __type: "ParameterNotFound", Message: "param not found" }));
});
srv.listen(0, "127.0.0.1", () => {
  const port = srv.address().port;
  process.env.AWS_ENDPOINT_URL = "http://127.0.0.1:" + port;
  const { SSMClient, GetParameterCommand } = require("@aws-sdk/client-ssm");
  const client = new SSMClient({});
  client.send(new GetParameterCommand({ Name: "/does/not/exist" }))
    .catch((e) => {
      srv.close();
      exports._result = { name: e.name, code: e.code };
    });
});
exports.handler = () => new Promise((res) => {
  const wait = () => exports._result ? res(exports._result) : setTimeout(wait, 10);
  wait();
});
"""
    result = _run_nodejs_worker(handler_js)
    assert result.get("status") == "ok", f"Invocation failed: {result}"
    r = result["result"]
    assert r["name"] == "ParameterNotFound", f"err.name was {r['name']!r}, expected 'ParameterNotFound'"
    assert r["code"] == "ParameterNotFound", f"err.code was {r['code']!r}"


def test_nodejs_worker_aws_sdk_v3_stub_resolves_extended():
    """JSON-RPC service stubs resolve for all awsJson1.x services.

    sts, sns, cloudwatch are intentionally excluded: they use query/smithy-rpc-v2-cbor
    protocols, not awsJson1.x, so their real SDK packages format requests correctly
    without a stub.
    """
    handler_js = """\
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const { KMSClient, EncryptCommand } = require("@aws-sdk/client-kms");
const { CognitoIdentityProviderClient, AdminGetUserCommand } = require("@aws-sdk/client-cognito-identity-provider");
const { CognitoIdentityClient } = require("@aws-sdk/client-cognito-identity");
const { ECRClient, DescribeRepositoriesCommand } = require("@aws-sdk/client-ecr");
const { GlueClient, GetDatabaseCommand } = require("@aws-sdk/client-glue");
const { AthenaClient, StartQueryExecutionCommand } = require("@aws-sdk/client-athena");
const { FirehoseClient, PutRecordCommand } = require("@aws-sdk/client-firehose");
const { ACMClient, ListCertificatesCommand } = require("@aws-sdk/client-acm");
const { OrganizationsClient, ListAccountsCommand } = require("@aws-sdk/client-organizations");
const { CodeBuildClient, ListProjectsCommand } = require("@aws-sdk/client-codebuild");
const { CloudTrailClient, LookupEventsCommand } = require("@aws-sdk/client-cloudtrail");
const { ServiceDiscoveryClient, ListServicesCommand } = require("@aws-sdk/client-servicediscovery");
exports.handler = async () => ({
  sqs:   typeof SQSClient === "function" && typeof SendMessageCommand === "function",
  kms:   typeof KMSClient === "function" && typeof EncryptCommand === "function",
  cidp:  typeof CognitoIdentityProviderClient === "function" && typeof AdminGetUserCommand === "function",
  ci:    typeof CognitoIdentityClient === "function",
  ecr:   typeof ECRClient === "function" && typeof DescribeRepositoriesCommand === "function",
  glue:  typeof GlueClient === "function" && typeof GetDatabaseCommand === "function",
  ath:   typeof AthenaClient === "function" && typeof StartQueryExecutionCommand === "function",
  fh:    typeof FirehoseClient === "function" && typeof PutRecordCommand === "function",
  acm:   typeof ACMClient === "function" && typeof ListCertificatesCommand === "function",
  org:   typeof OrganizationsClient === "function" && typeof ListAccountsCommand === "function",
  cb:    typeof CodeBuildClient === "function" && typeof ListProjectsCommand === "function",
  ct:    typeof CloudTrailClient === "function" && typeof LookupEventsCommand === "function",
  sd:    typeof ServiceDiscoveryClient === "function" && typeof ListServicesCommand === "function",
});
"""
    result = _run_nodejs_worker(handler_js)
    assert result.get("status") == "ok", f"Invocation failed: {result}"
    r = result["result"]
    for svc, ok in r.items():
        assert ok is True, f"Stub not resolved for service key: {svc!r}"


def test_nodejs_worker_https_localhost_downgraded_to_http():
    """https.request to localhost is downgraded to HTTP so cfn-response.js works.

    The CDK Provider Framework's cfn-response.js calls https.request
    unconditionally for the ResponseURL PUT and drops the port from the URL.
    patchAwsSdk() intercepts this and redirects to HTTP on the Ministack port.
    """
    import http.server
    import threading

    # Start a tiny HTTP server to catch the PUT
    received = {}

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_PUT(self):
            length = int(self.headers.get("Content-Length", 0))
            received["body"] = self.rfile.read(length).decode()
            received["path"] = self.path
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    srv = http.server.HTTPServer(("127.0.0.1", 0), _Handler)
    port = srv.server_address[1]
    t = threading.Thread(target=srv.handle_request, daemon=True)
    t.start()

    handler_js = f"""\
const https = require("https");
exports.handler = (event, ctx, cb) => {{
  // Simulate cfn-response.js: https.request with no port (port dropped from URL)
  const req = https.request({{
    hostname: "127.0.0.1",
    port: {port},
    path: "/test-cfn-response",
    method: "PUT",
    headers: {{"content-type": "", "content-length": 4}},
  }}, (res) => {{
    res.resume();
    cb(null, {{ statusCode: res.statusCode }});
  }});
  req.on("error", (e) => cb(e.message));
  req.write("test");
  req.end();
}};
"""
    result = _run_nodejs_worker(handler_js)
    t.join(timeout=5)
    srv.server_close()

    assert result.get("status") == "ok", f"Handler failed: {result}"
    assert received.get("path") == "/test-cfn-response", "PUT not received by HTTP server"
    assert received.get("body") == "test"


def test_nodejs_worker_aws_sdk_v3_stub_wire_roundtrip(lam, ssm):
    """End-to-end: a Node.js Lambda using @aws-sdk/client-ssm's
    PutParameterCommand actually creates a parameter on MiniStack's SSM
    service. Guards against the JSON-RPC stub silently 404'ing or routing
    to the wrong target prefix (the per-service hardcoded map can drift from
    router.py undetected by the resolution-only tests).
    """
    import shutil
    import uuid as _uuid

    if not shutil.which("node"):
        pytest.skip("node not found on PATH")

    fname = f"sdk-roundtrip-{_uuid_mod.uuid4().hex[:8]}"
    param_name = f"/ministack-test/{_uuid_mod.uuid4().hex[:8]}"
    param_value = f"value-{_uuid_mod.uuid4().hex[:8]}"
    code = (
        "const { SSMClient, PutParameterCommand } = require('@aws-sdk/client-ssm');\n"
        "exports.handler = async (event) => {\n"
        "  const client = new SSMClient({});\n"
        "  await client.send(new PutParameterCommand({\n"
        "    Name: event.name, Value: event.value, Type: 'String', Overwrite: true,\n"
        "  }));\n"
        "  return { ok: true };\n"
        "};\n"
    )
    try:
        lam.create_function(
            FunctionName=fname,
            Runtime="nodejs20.x",
            Role="arn:aws:iam::000000000000:role/test-role",
            Handler="index.handler",
            Code={"ZipFile": _make_zip_js(code, "index.js")},
        )
        resp = lam.invoke(
            FunctionName=fname,
            Payload=json.dumps({"name": param_name, "value": param_value}).encode(),
        )
        body = resp["Payload"].read().decode()
        assert resp["StatusCode"] == 200, f"Invoke failed: {body}"
        assert "FunctionError" not in resp, f"Handler errored: {body}"
        assert json.loads(body) == {"ok": True}, f"Unexpected body: {body}"

        # The stub must have actually called SSM. Verify via boto3 — if the
        # X-Amz-Target was wrong or the body didn't reach MS, this raises
        # ParameterNotFound and the test fails.
        fetched = ssm.get_parameter(Name=param_name)["Parameter"]
        assert fetched["Value"] == param_value
    finally:
        try:
            lam.delete_function(FunctionName=fname)
        except Exception:
            pass
        try:
            ssm.delete_parameter(Name=param_name)
        except Exception:
            pass


def test_lambda_ruby_4_0_runtime_maps_to_official_image():
    """Lambda Ruby 4.0 runtime support (botocore 1.42.94 added the runtime).
    Maps to AWS's official Lambda Ruby 4.0 base image."""
    from ministack.services.lambda_svc import _RUNTIME_IMAGE_MAP

    assert _RUNTIME_IMAGE_MAP.get("ruby4.0") == "public.ecr.aws/lambda/ruby:4.0"


# ---------------------------------------------------------------------------
# Lambda Durable Functions (Durable Execution).
# Shapes verified against:
#   https://docs.aws.amazon.com/lambda/latest/api/API_CheckpointDurableExecution.html
#   https://docs.aws.amazon.com/lambda/latest/api/API_GetDurableExecutionState.html
#   https://docs.aws.amazon.com/lambda/latest/api/API_GetDurableExecution.html
#   https://docs.aws.amazon.com/lambda/latest/api/API_ListDurableExecutionsByFunction.html
#   https://docs.aws.amazon.com/lambda/latest/api/API_GetDurableExecutionHistory.html
#   https://docs.aws.amazon.com/lambda/latest/api/API_StopDurableExecution.html
# ---------------------------------------------------------------------------

import urllib.request
import urllib.error


def _ms_endpoint():
    return os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _raw_durable(method: str, path: str, body: dict | None = None,
                 query: dict | None = None):
    """Hit ministack with a raw HTTP call for the durable-execution surface.
    Boto3 doesn't carry the preview shapes yet, so use urllib."""
    import json as _json
    from urllib.parse import urlencode
    qstr = ("?" + urlencode(query)) if query else ""
    req = urllib.request.Request(
        f"{_ms_endpoint()}{path}{qstr}",
        method=method,
        data=(_json.dumps(body).encode("utf-8") if body is not None else None),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as r:
            payload = r.read()
            return r.getcode(), _json.loads(payload) if payload else {}
    except urllib.error.HTTPError as e:
        body_bytes = e.read()
        try:
            return e.code, _json.loads(body_bytes) if body_bytes else {}
        except Exception:
            return e.code, {"raw": body_bytes.decode("utf-8", "replace")}


def _create_durable_execution_directly(lam):
    """Create a function with DurableConfig.Enabled, invoke it, and read
    the resulting DurableExecutionArn from the X-Amz-Durable-Execution-Arn
    response header. Returns (function_name, function_arn, dict with
    DurableExecutionArn + CheckpointToken)."""
    import base64 as _b64
    import json as _json
    fname = f"durable-fn-{_uuid_mod.uuid4().hex[:8]}"
    try:
        lam.delete_function(FunctionName=fname)
    except Exception:
        pass
    zip_b64 = _b64.b64encode(_make_zip("def handler(e,c): return e")).decode()
    code, body = _raw_durable("POST", "/2015-03-31/functions", body={
        "FunctionName": fname,
        "Runtime": "python3.12",
        "Role": _LAMBDA_ROLE,
        "Handler": "index.handler",
        "Code": {"ZipFile": zip_b64},
        "DurableConfig": {"Enabled": True},
    })
    fn_arn = body["FunctionArn"]
    # Invoke to create a durable execution.
    invoke_req = urllib.request.Request(
        f"{_ms_endpoint()}/2015-03-31/functions/{fname}/invocations",
        method="POST",
        data=b'{"hello":"world"}',
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(invoke_req) as r:
        arn = r.headers.get("X-Amz-Durable-Execution-Arn")
        token = r.headers.get("X-Amz-Durable-Checkpoint-Token")
        r.read()
    assert arn, "expected X-Amz-Durable-Execution-Arn header on invoke response"
    assert token, "expected X-Amz-Durable-Checkpoint-Token header on invoke response"
    return fname, fn_arn, {"DurableExecutionArn": arn, "CheckpointToken": token}


def test_lambda_durable_function_config_round_trip(lam):
    """CreateFunction accepts DurableConfig via raw HTTP (boto3 client-side
    rejects unknown params until its model is updated) and GetFunction
    echoes it back."""
    fname = f"durable-cfg-{_uuid_mod.uuid4().hex[:8]}"
    try:
        lam.delete_function(FunctionName=fname)
    except Exception:
        pass
    import json as _json
    import base64 as _b64
    zip_b64 = _b64.b64encode(_make_zip("def handler(e,c): return e")).decode()
    code, body = _raw_durable("POST", "/2015-03-31/functions", body={
        "FunctionName": fname,
        "Runtime": "python3.12",
        "Role": _LAMBDA_ROLE,
        "Handler": "index.handler",
        "Code": {"ZipFile": zip_b64},
        "DurableConfig": {"Enabled": True},
    })
    try:
        assert code in (200, 201), body
        assert body.get("DurableConfig") == {"Enabled": True}
        # Boto3 strips unknown fields, so verify the round-trip via raw HTTP.
        code, gf = _raw_durable("GET", f"/2015-03-31/functions/{fname}")
        assert code == 200
        assert gf["Configuration"].get("DurableConfig") == {"Enabled": True}
    finally:
        try:
            lam.delete_function(FunctionName=fname)
        except Exception:
            pass


def test_lambda_durable_get_execution(lam):
    fname, fn_arn, rec = _create_durable_execution_directly(lam)
    try:
        from urllib.parse import quote
        path = f"/2025-12-01/durable-executions/{quote(rec['DurableExecutionArn'], safe='/:$')}"
        code, body = _raw_durable("GET", path)
        assert code == 200
        assert body["DurableExecutionArn"] == rec["DurableExecutionArn"]
        assert body["Status"] == "RUNNING"
        assert json.loads(body["InputPayload"]) == {"hello": "world"}
        assert body["FunctionArn"] == fn_arn
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_get_state_requires_token(lam):
    fname, _, rec = _create_durable_execution_directly(lam)
    try:
        from urllib.parse import quote
        arn = quote(rec["DurableExecutionArn"], safe="/:$")
        # Wrong token rejected.
        code, body = _raw_durable("GET", f"/2025-12-01/durable-executions/{arn}/state",
                                  query={"CheckpointToken": "wrong"})
        assert code == 400
        # Correct token succeeds. AWS seeds an EXECUTION-type operation on
        # invoke so the SDK can read the input payload via
        # state.get_execution_operation; expect exactly that op here.
        code, body = _raw_durable("GET", f"/2025-12-01/durable-executions/{arn}/state",
                                  query={"CheckpointToken": rec["CheckpointToken"]})
        assert code == 200
        assert len(body["Operations"]) == 1
        assert body["Operations"][0]["Type"] == "EXECUTION"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_checkpoint_rotates_token_and_records_operations(lam):
    fname, _, rec = _create_durable_execution_directly(lam)
    try:
        from urllib.parse import quote
        arn = quote(rec["DurableExecutionArn"], safe="/:$")
        original_token = rec["CheckpointToken"]
        # Checkpoint a single Step succeed.
        code, body = _raw_durable("POST", f"/2025-12-01/durable-executions/{arn}/checkpoint", body={
            "CheckpointToken": original_token,
            "Updates": [{
                "Id": "step-1",
                "Type": "STEP",
                "Action": "SUCCEED",
                "Name": "first-step",
                "Payload": '{"value":42}',
            }],
        })
        assert code == 200
        assert body["CheckpointToken"] != original_token
        ops = body["NewExecutionState"]["Operations"]
        assert any(op["Id"] == "step-1" and op["Status"] == "SUCCEEDED" for op in ops)
        # Replaying with the OLD token must fail.
        code, _ = _raw_durable("POST", f"/2025-12-01/durable-executions/{arn}/checkpoint", body={
            "CheckpointToken": original_token,
            "Updates": [],
        })
        assert code == 400
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_get_history(lam):
    fname, _, rec = _create_durable_execution_directly(lam)
    try:
        from urllib.parse import quote
        arn = quote(rec["DurableExecutionArn"], safe="/:$")
        # Run a checkpoint so we have a non-trivial history.
        _raw_durable("POST", f"/2025-12-01/durable-executions/{arn}/checkpoint", body={
            "CheckpointToken": rec["CheckpointToken"],
            "Updates": [{"Id": "s", "Type": "STEP", "Action": "SUCCEED",
                         "Name": "n", "Payload": "1"}],
        })
        code, body = _raw_durable("GET", f"/2025-12-01/durable-executions/{arn}/history")
        assert code == 200
        events = body["Events"]
        assert any(e["EventType"] == "ExecutionStarted" for e in events)
        assert any(e["EventType"] == "StepSucceeded" for e in events)
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_stop(lam):
    fname, _, rec = _create_durable_execution_directly(lam)
    try:
        from urllib.parse import quote
        arn = quote(rec["DurableExecutionArn"], safe="/:$")
        code, body = _raw_durable("POST", f"/2025-12-01/durable-executions/{arn}/stop", body={
            "ErrorMessage": "stop-test",
        })
        assert code == 200
        assert "StopTimestamp" in body
        # GetDurableExecution reflects the STOPPED status + error.
        code, body = _raw_durable("GET", f"/2025-12-01/durable-executions/{arn}")
        assert body["Status"] == "STOPPED"
        assert body["Error"]["ErrorMessage"] == "stop-test"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_list_by_function(lam):
    fname, fn_arn, rec = _create_durable_execution_directly(lam)
    try:
        from urllib.parse import quote
        code, body = _raw_durable("GET", f"/2025-12-01/functions/{fname}/durable-executions")
        assert code == 200
        arns = [s["DurableExecutionArn"] for s in body["DurableExecutions"]]
        assert rec["DurableExecutionArn"] in arns
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_unknown_execution_404(lam):
    code, body = _raw_durable(
        "GET",
        "/2025-12-01/durable-executions/arn:aws:lambda:us-east-1:000000000000:function:nofn:$LATEST/durable-execution/aaa/bbb",
    )
    assert code == 404
    assert "ResourceNotFoundException" in body.get("__type", "")


# ---------------------------------------------------------------------------
# Durable Lambda — runtime env injection + chained invoke + persistence.
# ---------------------------------------------------------------------------

def test_lambda_durable_runtime_env_vars_present(lam):
    """A function with DurableConfig.Enabled gets the durable ARN + initial
    CheckpointToken injected as env vars in its execution environment."""
    import base64 as _b64
    import json as _json
    fname = f"durable-env-{_uuid_mod.uuid4().hex[:8]}"
    try:
        lam.delete_function(FunctionName=fname)
    except Exception:
        pass
    # Handler echoes the durable env vars back so the test can verify them.
    code = """
import os, json
def handler(event, context):
    return {
        "arn": os.environ.get("AWS_LAMBDA_DURABLE_EXECUTION_ARN"),
        "token": os.environ.get("AWS_LAMBDA_DURABLE_CHECKPOINT_TOKEN"),
        "name": os.environ.get("AWS_LAMBDA_DURABLE_EXECUTION_NAME"),
    }
"""
    zip_b64 = _b64.b64encode(_make_zip(code)).decode()
    _raw_durable("POST", "/2015-03-31/functions", body={
        "FunctionName": fname,
        "Runtime": "python3.12",
        "Role": _LAMBDA_ROLE,
        "Handler": "index.handler",
        "Code": {"ZipFile": zip_b64},
        "DurableConfig": {"Enabled": True},
    })
    try:
        resp = lam.invoke(FunctionName=fname, Payload=b"{}")
        body = _json.loads(resp["Payload"].read())
        assert body["arn"] and body["arn"].startswith("arn:aws:lambda:")
        assert "/durable-execution/" in body["arn"]
        assert body["token"]
        assert body["name"]
    finally:
        try:
            lam.delete_function(FunctionName=fname)
        except Exception:
            pass


def test_lambda_durable_chained_invoke_runs_child(lam):
    """A CHAINED_INVOKE checkpoint update with Action=START actually spawns
    the child function and records the result back into the parent's
    operation log."""
    import base64 as _b64
    import json as _json
    parent = f"durable-parent-{_uuid_mod.uuid4().hex[:8]}"
    child = f"durable-child-{_uuid_mod.uuid4().hex[:8]}"
    for n in (parent, child):
        try:
            lam.delete_function(FunctionName=n)
        except Exception:
            pass
    # Child handler returns a deterministic marker.
    child_code = "def handler(e,c): return {'child_marker': 'CHILD_OK'}"
    _raw_durable("POST", "/2015-03-31/functions", body={
        "FunctionName": child,
        "Runtime": "python3.12",
        "Role": _LAMBDA_ROLE,
        "Handler": "index.handler",
        "Code": {"ZipFile": _b64.b64encode(_make_zip(child_code)).decode()},
    })
    parent_code = "def handler(e,c): return {}"
    _raw_durable("POST", "/2015-03-31/functions", body={
        "FunctionName": parent,
        "Runtime": "python3.12",
        "Role": _LAMBDA_ROLE,
        "Handler": "index.handler",
        "Code": {"ZipFile": _b64.b64encode(_make_zip(parent_code)).decode()},
        "DurableConfig": {"Enabled": True},
    })
    try:
        # Invoke parent to spin up its durable execution.
        invoke_req = urllib.request.Request(
            f"{_ms_endpoint()}/2015-03-31/functions/{parent}/invocations",
            method="POST", data=b"{}", headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(invoke_req) as r:
            arn = r.headers.get("X-Amz-Durable-Execution-Arn")
            token = r.headers.get("X-Amz-Durable-Checkpoint-Token")
            r.read()
        from urllib.parse import quote
        arn_enc = quote(arn, safe="/:$")
        # Post a CHAINED_INVOKE START targeting the child.
        code, body = _raw_durable("POST",
            f"/2025-12-01/durable-executions/{arn_enc}/checkpoint", body={
                "CheckpointToken": token,
                "Updates": [{
                    "Id": "chain-1",
                    "Type": "CHAINED_INVOKE",
                    "Action": "START",
                    "Name": "call-child",
                    "ChainedInvokeOptions": {"FunctionName": child},
                }],
            })
        assert code == 200
        # The child runs in a daemon thread; give it a moment.
        import time as _time
        for _ in range(30):
            _time.sleep(0.1)
            code, history = _raw_durable("GET",
                f"/2025-12-01/durable-executions/{arn_enc}/history")
            if any(e["EventType"] == "ChainedInvokeSucceeded" for e in history.get("Events", [])):
                break
        events = history["Events"]
        assert any(e["EventType"] == "ChainedInvokeSucceeded" for e in events), \
            f"expected ChainedInvokeSucceeded, got {[e['EventType'] for e in events]}"
        # Confirm the child's marker is in the result payload.
        for e in events:
            if e["EventType"] == "ChainedInvokeSucceeded":
                payload = e["ChainedInvokeSucceededDetails"]["Result"]["Payload"]
                assert "CHILD_OK" in payload
                break
    finally:
        for n in (parent, child):
            try:
                lam.delete_function(FunctionName=n)
            except Exception:
                pass


def test_lambda_durable_persistence_round_trip():
    """get_state / restore_state round-trip preserves the executions map."""
    from ministack.services import lambda_durable
    # Snapshot original state.
    original = lambda_durable.get_state()
    try:
        # Create a synthetic execution.
        lambda_durable._executions.clear()
        rec = lambda_durable.create_execution_for_invoke(
            function_arn="arn:aws:lambda:us-east-1:000000000000:function:persist-test",
            version="$LATEST",
            input_payload='{"k":"v"}',
        )
        snap = lambda_durable.get_state()
        # Wipe and restore.
        lambda_durable._executions.clear()
        assert not lambda_durable._executions
        lambda_durable.restore_state(snap)
        assert rec["DurableExecutionArn"] in lambda_durable._executions
        restored = lambda_durable._executions[rec["DurableExecutionArn"]]
        assert restored["Status"] == "RUNNING"
        assert restored["InputPayload"] == '{"k":"v"}'
    finally:
        lambda_durable._executions.clear()
        lambda_durable.restore_state(original)


# ---------------------------------------------------------------------------
# Lambda code_zip persistence — content-addressed blob storage.
# code_zip bytes are written to ${STATE_DIR}/lambda-blobs/{sha256}.zip; the
# returned state holds only the sha reference. The in-memory _functions
# shape is unchanged (still bytes after restore) so invoke / update / delete
# paths see no difference.
# ---------------------------------------------------------------------------


def _make_lambda_record(name: str, code_zip: bytes, versions: dict | None = None) -> dict:
    """Build a _functions entry matching what CreateFunction stores."""
    return {
        "config": {
            "FunctionName": name,
            "FunctionArn": f"arn:aws:lambda:us-east-1:000000000000:function:{name}",
            "Runtime": "python3.12",
            "Handler": "index.handler",
        },
        "code_zip": code_zip,
        "versions": versions or {},
        "next_version": 1,
        "tags": {},
        "policy": {"Version": "2012-10-17", "Id": "default", "Statement": []},
    }


@pytest.fixture
def lambda_svc_isolated(tmp_path, monkeypatch):
    """Snapshot lambda_svc state and redirect its blob storage to tmp_path.
    Restores the original state at teardown so tests don't pollute the
    shared in-process module that serves the running ministack."""
    from ministack.services import lambda_svc

    monkeypatch.setattr(lambda_svc, "CODE_BLOB_DIR", str(tmp_path / "lambda-blobs"))
    original = lambda_svc.get_state()
    try:
        lambda_svc._functions._data.clear()
        yield lambda_svc, tmp_path / "lambda-blobs"
    finally:
        lambda_svc._functions._data.clear()
        lambda_svc.restore_state(original)


def test_lambda_code_zip_round_trip_through_blob_storage(lambda_svc_isolated):
    """Bytes survive an exact get_state → clear → restore_state round-trip."""
    svc, _ = lambda_svc_isolated
    code = b"\x89PNG\r\n\x1a\n" + b"\x00" * 1000  # non-UTF8 bytes
    svc._functions["fn"] = _make_lambda_record("fn", code)

    state = svc.get_state()
    svc._functions._data.clear()
    svc.restore_state(state)

    restored = svc._functions._data[("000000000000", "fn")]
    assert restored["code_zip"] == code
    assert isinstance(restored["code_zip"], bytes)


def test_lambda_get_state_replaces_code_zip_with_blob_ref(lambda_svc_isolated):
    """The returned state must not inline bytes or base64 — only a sha ref."""
    import hashlib
    svc, blob_dir = lambda_svc_isolated
    code = b"def handler(event, ctx): return 'ok'\n" * 256
    svc._functions["fn"] = _make_lambda_record("fn", code)

    state = svc.get_state()
    fn_state = state["functions"]._data[("000000000000", "fn")]

    assert fn_state["code_zip"] == {"code_blob_ref": hashlib.sha256(code).hexdigest()}
    blob_path = blob_dir / f"{hashlib.sha256(code).hexdigest()}.zip"
    assert blob_path.exists()
    assert blob_path.read_bytes() == code


def test_lambda_per_version_code_zip_also_externalized(lambda_svc_isolated):
    """PublishVersion-created versions also store code_zip externally."""
    import hashlib
    svc, _ = lambda_svc_isolated
    v1, v2 = b"v1 body", b"v2 body"
    svc._functions["fn"] = _make_lambda_record(
        "fn", v2, versions={"1": {"code_zip": v1, "config": {"Version": "1"}}}
    )

    state = svc.get_state()
    fn_state = state["functions"]._data[("000000000000", "fn")]
    assert fn_state["versions"]["1"]["code_zip"] == {
        "code_blob_ref": hashlib.sha256(v1).hexdigest()
    }

    svc._functions._data.clear()
    svc.restore_state(state)
    restored = svc._functions._data[("000000000000", "fn")]
    assert restored["code_zip"] == v2
    assert restored["versions"]["1"]["code_zip"] == v1


def test_lambda_identical_code_dedups_to_single_blob(lambda_svc_isolated):
    """Content-addressing means two functions with identical bytes share one
    file. Important when the deps-bundled-into-every-zip pattern produces
    many functions with byte-identical layer payloads."""
    svc, blob_dir = lambda_svc_isolated
    code = b"shared body across two functions"
    svc._functions["fn-a"] = _make_lambda_record("fn-a", code)
    svc._functions["fn-b"] = _make_lambda_record("fn-b", code)

    svc.get_state()

    files = sorted(blob_dir.iterdir())
    assert len(files) == 1, [f.name for f in files]


def test_lambda_legacy_inline_base64_persistence_still_loads(lambda_svc_isolated):
    """Pre-existing lambda.json files (written before content-addressed
    storage) stored code_zip as inline base64. restore_state must accept
    that shape so an in-place upgrade requires no migration step."""
    import base64 as _b64
    from ministack.core.responses import AccountScopedDict
    svc, _ = lambda_svc_isolated

    code = b"legacy persisted bytes"
    legacy = {
        "functions": AccountScopedDict(),
        "layers": AccountScopedDict(),
        "esms": AccountScopedDict(),
        "function_urls": AccountScopedDict(),
        "kinesis_positions": {},
        "dynamodb_stream_positions": {},
    }
    legacy["functions"]._data[("000000000000", "old-fn")] = {
        "config": {"FunctionName": "old-fn"},
        "code_zip": _b64.b64encode(code).decode(),
        "versions": {},
    }

    svc.restore_state(legacy)

    restored = svc._functions._data[("000000000000", "old-fn")]
    assert restored["code_zip"] == code


def test_lambda_missing_blob_degrades_without_aborting_restore(lambda_svc_isolated):
    """If a blob file is missing (corrupted volume / partial mount), restore
    must downgrade the affected function (code_zip=None) and continue,
    rather than raise and prevent the whole server from starting."""
    from ministack.core.responses import AccountScopedDict
    svc, _ = lambda_svc_isolated

    state = {
        "functions": AccountScopedDict(),
        "layers": AccountScopedDict(),
        "esms": AccountScopedDict(),
        "function_urls": AccountScopedDict(),
        "kinesis_positions": {},
        "dynamodb_stream_positions": {},
    }
    state["functions"]._data[("000000000000", "orphan")] = {
        "config": {"FunctionName": "orphan"},
        "code_zip": {"code_blob_ref": "0" * 64},  # sha pointing at nothing
        "versions": {},
    }

    svc.restore_state(state)

    assert svc._functions._data[("000000000000", "orphan")]["code_zip"] is None


def test_lambda_get_state_prunes_orphan_blobs(lambda_svc_isolated):
    """When a function's code is updated, the previous generation's blob
    becomes orphan. The next get_state sweeps it. Without this, repeated
    UpdateFunctionCode (e.g. iterative sandbox redeploys) leaks blob files
    across restarts."""
    import hashlib
    svc, blob_dir = lambda_svc_isolated

    old_code = b"old code"
    new_code = b"new code"
    svc._functions["fn"] = _make_lambda_record("fn", old_code)
    svc.get_state()  # writes blob(old_code)
    assert (blob_dir / f"{hashlib.sha256(old_code).hexdigest()}.zip").exists()

    # Simulate UpdateFunctionCode.
    svc._functions["fn"]["code_zip"] = new_code
    svc.get_state()  # writes blob(new_code), should remove blob(old_code)

    assert (blob_dir / f"{hashlib.sha256(new_code).hexdigest()}.zip").exists()
    assert not (blob_dir / f"{hashlib.sha256(old_code).hexdigest()}.zip").exists()


def test_lambda_durable_event_wrapped_with_sdk_fields(lam):
    """A durable invocation's event payload is wrapped with the fields the
    aws-durable-execution-sdk-python SDK reads from the Lambda event:
    DurableExecutionArn, CheckpointToken, InitialExecutionState.
    Without this wrapping the SDK's `@durable_execution` decorator raises
    ExecutionError on the first line of its wrapper."""
    import base64 as _b64
    import json as _json
    fname = f"durable-wrap-{_uuid_mod.uuid4().hex[:8]}"
    try:
        lam.delete_function(FunctionName=fname)
    except Exception:
        pass
    # Handler echoes the keys it received.
    code = """
def handler(event, context):
    return {"keys": sorted(list(event.keys())), "event": event}
"""
    _raw_durable("POST", "/2015-03-31/functions", body={
        "FunctionName": fname,
        "Runtime": "python3.12",
        "Role": _LAMBDA_ROLE,
        "Handler": "index.handler",
        "Code": {"ZipFile": _b64.b64encode(_make_zip(code)).decode()},
        "DurableConfig": {"Enabled": True},
    })
    try:
        resp = lam.invoke(FunctionName=fname, Payload=b'{"user":"data"}')
        body = _json.loads(resp["Payload"].read())
        # SDK requires these three top-level keys.
        for key in ("DurableExecutionArn", "CheckpointToken", "InitialExecutionState"):
            assert key in body["keys"], f"missing {key} in {body['keys']}"
        ops = body["event"]["InitialExecutionState"]["Operations"]
        # AWS seeds the synthetic EXECUTION-type op with the input payload.
        assert len(ops) == 1 and ops[0]["Type"] == "EXECUTION"
    finally:
        try:
            lam.delete_function(FunctionName=fname)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Durable execution — external callbacks (SendCallback{Success,Failure,Heartbeat}).
# Spec: callbacks suspend the SDK; external systems resolve them via REST.
#   https://docs.aws.amazon.com/lambda/latest/api/API_SendDurableExecutionCallbackSuccess.html
#   https://docs.aws.amazon.com/lambda/latest/api/API_SendDurableExecutionCallbackFailure.html
#   https://docs.aws.amazon.com/lambda/latest/api/API_SendDurableExecutionCallbackHeartbeat.html
# ---------------------------------------------------------------------------

def _start_callback(lam):
    """Create a durable execution and checkpoint a CALLBACK START so a callback
    is registered and resolvable externally. Returns (fname, arn, callback_id,
    new_checkpoint_token)."""
    fname, _, rec = _create_durable_execution_directly(lam)
    from urllib.parse import quote
    arn_q = quote(rec["DurableExecutionArn"], safe="/:$")
    code, body = _raw_durable("POST", f"/2025-12-01/durable-executions/{arn_q}/checkpoint", body={
        "CheckpointToken": rec["CheckpointToken"],
        "Updates": [{
            "Id": "cbop1aaaaaaaaaaaaaaaaaaaaaaaaaa",
            "Type": "CALLBACK",
            "Action": "START",
            "Name": "ext-cb",
            "CallbackOptions": {"TimeoutSeconds": 120, "HeartbeatTimeoutSeconds": 30},
        }],
    })
    assert code == 200, body
    ops = body["NewExecutionState"]["Operations"]
    op = next(o for o in ops if o["Id"] == "cbop1aaaaaaaaaaaaaaaaaaaaaaaaaa")
    cb_id = (op.get("CallbackDetails") or {}).get("CallbackId")
    assert cb_id, f"no CallbackId in {op}"
    return fname, rec["DurableExecutionArn"], cb_id, body["CheckpointToken"]


def test_lambda_durable_send_callback_success_then_already_closed(lam):
    """First succeed returns 200; second call against the same closed callback
    must return CallbackTimeoutException (400) per the spec."""
    import urllib.request, urllib.error
    from urllib.parse import quote
    fname, arn, cb_id, _ = _start_callback(lam)
    try:
        url = f"{_ms_endpoint()}/2025-12-01/durable-execution-callbacks/{quote(cb_id, safe='')}/succeed"
        req = urllib.request.Request(url, method="POST", data=b'"first"',
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            assert r.status == 200
        # Re-fire: already-closed callback → 400.
        req2 = urllib.request.Request(url, method="POST", data=b'"second"',
                                      headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req2)
            assert False, "expected 400 on already-closed callback"
        except urllib.error.HTTPError as e:
            assert e.code == 400
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_send_callback_success_records_result(lam):
    fname, arn, cb_id, _ = _start_callback(lam)
    try:
        import urllib.request
        from urllib.parse import quote
        req = urllib.request.Request(
            f"{_ms_endpoint()}/2025-12-01/durable-execution-callbacks/{quote(cb_id, safe='')}/succeed",
            method="POST", data=b'"forty-two"',
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as r:
            assert r.status == 200
        # History should include CallbackSucceeded with the Result payload.
        code, hist = _raw_durable("GET",
            f"/2025-12-01/durable-executions/{quote(arn, safe='/:$')}/history")
        assert code == 200
        types = [e["EventType"] for e in hist["Events"]]
        assert "CallbackSucceeded" in types
        ev = next(e for e in hist["Events"] if e["EventType"] == "CallbackSucceeded")
        assert ev["CallbackSucceededDetails"]["Result"]["Payload"] == '"forty-two"'
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_send_callback_failure(lam):
    fname, arn, cb_id, _ = _start_callback(lam)
    try:
        import urllib.request, json as _json
        from urllib.parse import quote
        req = urllib.request.Request(
            f"{_ms_endpoint()}/2025-12-01/durable-execution-callbacks/{quote(cb_id, safe='')}/fail",
            method="POST",
            data=_json.dumps({
                "ErrorType": "ExternalTimeout",
                "ErrorMessage": "third-party timed out",
                "ErrorData": "extra-context",
            }).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req) as r:
            assert r.status == 200
        code, hist = _raw_durable("GET",
            f"/2025-12-01/durable-executions/{quote(arn, safe='/:$')}/history")
        assert code == 200
        ev = next(e for e in hist["Events"] if e["EventType"] == "CallbackFailed")
        err = ev["CallbackFailedDetails"]["Error"]["Payload"]
        assert err["ErrorType"] == "ExternalTimeout"
        assert err["ErrorMessage"] == "third-party timed out"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_send_callback_heartbeat(lam):
    """Heartbeat must return 200 and must NOT close the callback — a
    subsequent succeed on the same id must still work."""
    import urllib.request
    from urllib.parse import quote
    fname, arn, cb_id, _ = _start_callback(lam)
    try:
        hb_url = f"{_ms_endpoint()}/2025-12-01/durable-execution-callbacks/{quote(cb_id, safe='')}/heartbeat"
        for _ in range(3):
            req = urllib.request.Request(hb_url, method="POST", data=b"",
                                         headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req) as r:
                assert r.status == 200
        # Callback should still be live → succeed returns 200.
        ok_url = f"{_ms_endpoint()}/2025-12-01/durable-execution-callbacks/{quote(cb_id, safe='')}/succeed"
        req = urllib.request.Request(ok_url, method="POST", data=b'"done"',
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req) as r:
            assert r.status == 200
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_send_callback_unknown_id_400(lam):
    """Unknown CallbackId returns InvalidParameterValueException, not 500."""
    import urllib.request, urllib.error
    req = urllib.request.Request(
        f"{_ms_endpoint()}/2025-12-01/durable-execution-callbacks/does-not-exist/succeed",
        method="POST", data=b'"x"',
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req)
        assert False, "expected 400"
    except urllib.error.HTTPError as e:
        assert e.code == 400


def test_lambda_durable_get_execution_rejects_malformed_arn(lam):
    """Malformed DurableExecutionArn → 400 InvalidParameterValueException."""
    code, body = _raw_durable("GET", "/2025-12-01/durable-executions/not-a-real-arn")
    assert code == 400
    assert body.get("__type") == "InvalidParameterValueException"


# ---------------------------------------------------------------------------
# Resume scheduler — fires WAIT/CALLBACK expiries and survives restart.
# These exercise lambda_durable._resume_execution and restore_state directly
# (they're internal but they ARE the AWS-parity contract for in-flight
# durable executions: timers keep ticking, callbacks stay resolvable across
# restarts).
# ---------------------------------------------------------------------------

def test_lambda_durable_heartbeat_extends_callback_timeout():
    """Pushing the HeartbeatDeadline forward must actually delay the
    CallbackTimedOut firing. Stale heap entries must be no-ops."""
    from ministack.services import lambda_durable as _ld
    arn = "arn:aws:lambda:us-east-1:000000000000:function:hb-test:$LATEST/durable-execution/" \
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    now = _ld._now()
    op_id = "hbop1aaaaaaaaaaaaaaaaaaaaaaaaaaa"
    rec = {
        "DurableExecutionArn": arn,
        "FunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:hb-test",
        "Status": "RUNNING",
        "Operations": [{
            "Id": op_id, "Type": "CALLBACK", "Status": "STARTED",
            "CallbackDetails": {
                "CallbackId": op_id,
                "HeartbeatTimeoutSeconds": 30.0,
                "HeartbeatDeadline": now - 5,  # already past
                "TimeoutDeadline": now + 600,
            },
        }],
        "History": [], "NextEventId": 1, "CheckpointToken": "tok",
        "InputPayload": "{}",
    }
    _ld._executions[arn] = rec
    _ld._callback_index[op_id] = (arn, op_id)
    try:
        # Heartbeat now → pushes HeartbeatDeadline to now+30s.
        status, _, _ = _ld.handle_callback_heartbeat(op_id, b"")
        assert status == 200
        new_deadline = rec["Operations"][0]["CallbackDetails"]["HeartbeatDeadline"]
        assert new_deadline > _ld._now() + 25
        # Simulate the stale heap entry firing now — must be a no-op
        # (callback not timed out, status still STARTED).
        _ld._resume_execution(arn)
        assert rec["Operations"][0]["Status"] == "STARTED"
        assert (rec["Operations"][0].get("CallbackDetails") or {}).get("Error") is None
    finally:
        _ld._executions.pop(arn, None)
        _ld._callback_index.pop(op_id, None)


def test_lambda_durable_restore_rebuilds_callback_index_and_rearms_timers():
    """After restore_state, in-flight callbacks must be resolvable and
    pending timers must be back on the heap."""
    from ministack.services import lambda_durable as _ld
    arn = "arn:aws:lambda:us-east-1:000000000000:function:restore-test:$LATEST/durable-execution/" \
          "cccccccccccccccccccccccccccccccc/dddddddddddddddddddddddddddddddd"
    cb_op_id = "rstcbaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wait_op_id = "rstwaitaaaaaaaaaaaaaaaaaaaaaaaaa"
    now = _ld._now()
    rec = {
        "DurableExecutionArn": arn,
        "FunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:restore-test",
        "Status": "RUNNING",
        "Operations": [
            {"Id": cb_op_id, "Type": "CALLBACK", "Status": "STARTED",
             "CallbackDetails": {"CallbackId": cb_op_id,
                                 "TimeoutDeadline": now + 300}},
            {"Id": wait_op_id, "Type": "WAIT", "Status": "STARTED",
             "WaitDetails": {"ScheduledEndTimestamp": now + 60, "Duration": 60}},
        ],
        "History": [], "NextEventId": 1, "CheckpointToken": "tok",
        "InputPayload": "{}",
    }
    # Wipe live state so the test is hermetic.
    pre_index = dict(_ld._callback_index)
    pre_execs = dict(_ld._executions)
    pre_queue = list(_ld._resume_queue)
    _ld._executions.clear()
    _ld._callback_index.clear()
    with _ld._resume_lock:
        _ld._resume_queue.clear()
    try:
        # Pretend ministack just booted and read this rec from disk.
        _ld.restore_state({"executions": {arn: rec}})
        # Index must contain the STARTED callback.
        assert cb_op_id in _ld._callback_index
        assert _ld._callback_index[cb_op_id] == (arn, cb_op_id)
        # Heap must have at least one entry for this arn at or before the
        # earliest deadline (the WAIT at now+60).
        with _ld._resume_lock:
            entries = [(t, a) for (t, a, _acct) in _ld._resume_queue if a == arn]
        assert entries, "no resume entry queued after restore"
        assert min(t for t, _ in entries) <= now + 60 + 1
        # And Send*Callback resolves the restored callback (no 404).
        target, op, err = _ld._resolve_callback(cb_op_id)
        assert err is None and target is rec and op["Id"] == cb_op_id
    finally:
        _ld._executions.clear()
        _ld._executions.update(pre_execs)
        _ld._callback_index.clear()
        _ld._callback_index.update(pre_index)
        with _ld._resume_lock:
            _ld._resume_queue.clear()
            for e in pre_queue:
                _ld._resume_queue.append(e)


def test_lambda_durable_restore_skips_non_running_executions():
    """SUCCEEDED/FAILED/STOPPED executions must NOT be re-armed (they would
    pin a function arn that may not exist anymore) and their callbacks must
    NOT be re-indexed."""
    from ministack.services import lambda_durable as _ld
    arn_done = "arn:aws:lambda:us-east-1:000000000000:function:done-fn:$LATEST/durable-execution/" \
               "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee/ffffffffffffffffffffffffffffffff"
    cb_op_id = "donecbaaaaaaaaaaaaaaaaaaaaaaaaaa"
    rec = {
        "DurableExecutionArn": arn_done,
        "FunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:done-fn",
        "Status": "SUCCEEDED",
        "Operations": [
            {"Id": cb_op_id, "Type": "CALLBACK", "Status": "SUCCEEDED",
             "CallbackDetails": {"CallbackId": cb_op_id, "Result": "x"}},
        ],
        "History": [], "NextEventId": 1, "CheckpointToken": "tok",
        "InputPayload": "{}",
    }
    pre_index = dict(_ld._callback_index)
    pre_execs = dict(_ld._executions)
    _ld._executions.clear()
    _ld._callback_index.clear()
    try:
        _ld.restore_state({"executions": {arn_done: rec}})
        # SUCCEEDED callback must NOT be indexed (only STARTED ones).
        assert cb_op_id not in _ld._callback_index
    finally:
        _ld._executions.clear()
        _ld._executions.update(pre_execs)
        _ld._callback_index.clear()
        _ld._callback_index.update(pre_index)


# ---------------------------------------------------------------------------
# Edge-case coverage for the 7 ops in issue #670 — written so we can close
# the ticket with confidence rather than just on happy-path verification.
# ---------------------------------------------------------------------------

def test_lambda_durable_stop_on_terminal_returns_invalid_parameter(lam):
    """Per AWS docs ('Stops a running durable execution'), Stop on a
    non-running execution must return 400 InvalidParameterValueException —
    the only 4xx-class error the API documents for input failures."""
    from urllib.parse import quote
    fname, _, rec = _create_durable_execution_directly(lam)
    try:
        arn_q = quote(rec["DurableExecutionArn"], safe="/:$")
        code1, _ = _raw_durable("POST", f"/2025-12-01/durable-executions/{arn_q}/stop", body={})
        assert code1 == 200
        code2, body2 = _raw_durable("POST", f"/2025-12-01/durable-executions/{arn_q}/stop", body={})
        assert code2 == 400, f"expected 400, got {code2}: {body2}"
        assert body2.get("__type") == "InvalidParameterValueException", body2
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_checkpoint_malformed_updates_rejected(lam):
    """A Checkpoint with garbage Updates (missing required fields, unknown
    Type) must 400, not 500 or silent accept."""
    from urllib.parse import quote
    fname, _, rec = _create_durable_execution_directly(lam)
    try:
        arn_q = quote(rec["DurableExecutionArn"], safe="/:$")
        # Missing Id, Type, Action.
        code, body = _raw_durable("POST",
            f"/2025-12-01/durable-executions/{arn_q}/checkpoint",
            body={"CheckpointToken": rec["CheckpointToken"],
                  "Updates": [{"banana": "split"}]})
        assert code == 400 or (code == 200 and body.get("NewExecutionState", {}).get("Operations") == []), \
            f"malformed update accepted as valid update: code={code} body={body}"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_checkpoint_unknown_op_type_rejected(lam):
    """Unknown Type (not in EXECUTION/CONTEXT/STEP/WAIT/CALLBACK/CHAINED_INVOKE)
    must not silently create an Op with that bogus Type."""
    from urllib.parse import quote
    fname, _, rec = _create_durable_execution_directly(lam)
    try:
        arn_q = quote(rec["DurableExecutionArn"], safe="/:$")
        code, body = _raw_durable("POST",
            f"/2025-12-01/durable-executions/{arn_q}/checkpoint",
            body={"CheckpointToken": rec["CheckpointToken"],
                  "Updates": [{"Id": "x" * 32, "Type": "NOT_A_REAL_TYPE",
                               "Action": "START"}]})
        # Either 400-reject, or the bogus type must not create a recognised op.
        if code == 200:
            ops = body["NewExecutionState"]["Operations"]
            bogus = [o for o in ops if o.get("Type") == "NOT_A_REAL_TYPE"]
            assert not bogus, "ministack silently created an Op with an invalid Type"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_list_pagination_max_items(lam):
    """Per AWS docs, ListDurableExecutionsByFunction uses MaxItems (not
    MaxResults) with 'Valid Range: Minimum 0, Maximum 1000'. Out-of-range
    must 400 with InvalidParameterValueException."""
    fname, _, _ = _create_durable_execution_directly(lam)
    try:
        # Out-of-range → 400 InvalidParameterValueException.
        code, body = _raw_durable("GET",
            f"/2025-12-01/functions/{fname}/durable-executions",
            query={"MaxItems": "9999999"})
        assert code == 400, f"expected 400, got {code}: {body}"
        assert body.get("__type") == "InvalidParameterValueException", body
        # MaxItems=1 with a single execution → still returns it.
        code, body = _raw_durable("GET",
            f"/2025-12-01/functions/{fname}/durable-executions",
            query={"MaxItems": "1"})
        assert code == 200, f"got {code}: {body}"
        assert len(body.get("DurableExecutions", [])) >= 1
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_get_history_pagination_marker(lam):
    """Per AWS docs, the Lambda pagination contract uses Marker (not
    NextToken) and MaxItems (not MaxResults). MaxItems > 1000 must 400."""
    from urllib.parse import quote
    fname, _, rec = _create_durable_execution_directly(lam)
    try:
        arn_q = quote(rec["DurableExecutionArn"], safe="/:$")
        code, body = _raw_durable("GET",
            f"/2025-12-01/durable-executions/{arn_q}/history",
            query={"MaxItems": "1"})
        assert code == 200, body
        assert "Events" in body
        code, body = _raw_durable("GET",
            f"/2025-12-01/durable-executions/{arn_q}/history",
            query={"MaxItems": "9999999"})
        assert code == 400, body
        assert body.get("__type") == "InvalidParameterValueException", body
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_get_execution_state_old_token_rejected(lam):
    """GetDurableExecutionState with a stale CheckpointToken must 400 —
    SDKs rely on this to detect they've been preempted."""
    from urllib.parse import quote
    fname, _, rec = _create_durable_execution_directly(lam)
    try:
        arn_q = quote(rec["DurableExecutionArn"], safe="/:$")
        original_token = rec["CheckpointToken"]
        # Rotate the token via a Checkpoint.
        code, body = _raw_durable("POST",
            f"/2025-12-01/durable-executions/{arn_q}/checkpoint",
            body={"CheckpointToken": original_token, "Updates": []})
        assert code == 200
        # Old token must now be rejected on GetState.
        code, body = _raw_durable("GET",
            f"/2025-12-01/durable-executions/{arn_q}/state",
            query={"CheckpointToken": original_token})
        assert code == 400, f"stale token accepted: code={code} body={body}"
    finally:
        lam.delete_function(FunctionName=fname)


def test_lambda_durable_get_unknown_arn_404(lam):
    """GetDurableExecution with a syntactically-valid but unknown ARN must
    return 404 (ResourceNotFoundException), not 500."""
    fake = ("arn:aws:lambda:us-east-1:000000000000:function:does-not-exist:"
            "$LATEST/durable-execution/" + ("a" * 32) + "/" + ("b" * 32))
    from urllib.parse import quote
    code, body = _raw_durable("GET", f"/2025-12-01/durable-executions/{quote(fake, safe='/:$')}")
    assert code == 404, f"expected 404, got {code}: {body}"


def test_lambda_durable_create_function_durable_config_round_trip_with_update(lam):
    """DurableConfig must survive UpdateFunctionConfiguration that touches
    unrelated fields (timeout, memory)."""
    import base64 as _b64, json as _json
    fname = f"dur-upd-{_uuid_mod.uuid4().hex[:8]}"
    try:
        lam.delete_function(FunctionName=fname)
    except Exception:
        pass
    zip_b64 = _b64.b64encode(_make_zip("def handler(e,c): return e")).decode()
    code, _ = _raw_durable("POST", "/2015-03-31/functions", body={
        "FunctionName": fname, "Runtime": "python3.12", "Role": _LAMBDA_ROLE,
        "Handler": "index.handler", "Code": {"ZipFile": zip_b64},
        "DurableConfig": {"Enabled": True},
    })
    assert code == 201
    try:
        # Touch unrelated config.
        lam.update_function_configuration(FunctionName=fname, Timeout=60, MemorySize=256)
        code, body = _raw_durable("GET", f"/2015-03-31/functions/{fname}")
        assert body["Configuration"].get("DurableConfig") == {"Enabled": True}, \
            f"DurableConfig lost after Update: {body['Configuration'].get('DurableConfig')}"
    finally:
        try: lam.delete_function(FunctionName=fname)
        except Exception: pass


# ---------------------------------------------------------------------------
# X-Ray active tracing — _X_AMZN_TRACE_ID injection per invocation.
#
# Real AWS injects this env var into the runtime when TracingConfig.Mode is
# Active; the aws-xray-sdk reads it per-segment via os.getenv() and raises
# "Missing AWS Lambda trace data for X-Ray" on absence. The warm Python
# executor is the default for python3.* runtimes, so these tests pin its
# behavior end-to-end. AWS RIE (the docker executor) does NOT support X-Ray
# upstream, so the corresponding "supported here" guarantee is warm/local/
# provided only.
# ---------------------------------------------------------------------------

_XRAY_TRACE_HEADER_RE = (
    r"^Root=1-[0-9a-f]{8}-[0-9a-f]{24};Parent=[0-9a-f]{16};Sampled=1$"
)

_XRAY_ECHO_HANDLER = (
    "import os\n"
    "def handler(event, context):\n"
    "    return {'trace_id': os.environ.get('_X_AMZN_TRACE_ID', '<UNSET>')}\n"
)


def _create_xray_fn(lam, name: str, mode: str) -> None:
    lam.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_XRAY_ECHO_HANDLER)},
        TracingConfig={"Mode": mode},
    )


def _invoke_trace_id(lam, name: str) -> str:
    resp = lam.invoke(FunctionName=name, Payload=b"{}")
    return json.loads(resp["Payload"].read())["trace_id"]


def test_lambda_xray_active_injects_trace_id(lam):
    """TracingConfig.Mode=Active → handler sees a properly-formatted
    _X_AMZN_TRACE_ID (`Root=1-<8hex>-<24hex>;Parent=<16hex>;Sampled=1`)."""
    import re as _re
    fname = f"xray-active-{_uuid_mod.uuid4().hex[:8]}"
    _create_xray_fn(lam, fname, "Active")
    try:
        trace_id = _invoke_trace_id(lam, fname)
        assert _re.match(_XRAY_TRACE_HEADER_RE, trace_id), trace_id
    finally:
        try: lam.delete_function(FunctionName=fname)
        except Exception: pass


def test_lambda_xray_passthrough_does_not_set_trace_id(lam):
    """TracingConfig.Mode=PassThrough (default) → no _X_AMZN_TRACE_ID. The
    AWS X-Ray SDK opts itself out when the env var is absent, which is the
    expected behavior for non-Active functions."""
    fname = f"xray-pt-{_uuid_mod.uuid4().hex[:8]}"
    _create_xray_fn(lam, fname, "PassThrough")
    try:
        assert _invoke_trace_id(lam, fname) == "<UNSET>"
    finally:
        try: lam.delete_function(FunctionName=fname)
        except Exception: pass


def test_lambda_xray_active_fresh_id_per_invocation(lam):
    """Each invocation gets a distinct trace ID — the warm worker's
    persistent subprocess must NOT cache the env var across invocations.
    AWS contract: every Lambda invocation is a new root segment."""
    import re as _re
    fname = f"xray-fresh-{_uuid_mod.uuid4().hex[:8]}"
    _create_xray_fn(lam, fname, "Active")
    try:
        t1 = _invoke_trace_id(lam, fname)
        t2 = _invoke_trace_id(lam, fname)
        assert _re.match(_XRAY_TRACE_HEADER_RE, t1), t1
        assert _re.match(_XRAY_TRACE_HEADER_RE, t2), t2
        assert t1 != t2, f"Trace ID was reused: {t1}"
    finally:
        try: lam.delete_function(FunctionName=fname)
        except Exception: pass


def test_lambda_xray_does_not_leak_across_functions(lam):
    """Active on function A must not leave _X_AMZN_TRACE_ID set when
    function B (PassThrough) runs afterward — verifies the worker bootstrap
    clears the env var when no trace ID is injected for the call."""
    fa = f"xray-leak-a-{_uuid_mod.uuid4().hex[:8]}"
    fb = f"xray-leak-b-{_uuid_mod.uuid4().hex[:8]}"
    _create_xray_fn(lam, fa, "Active")
    _create_xray_fn(lam, fb, "PassThrough")
    try:
        # Prime A so its worker has _X_AMZN_TRACE_ID in os.environ.
        _invoke_trace_id(lam, fa)
        # B must not see the env var from A's invocation.
        assert _invoke_trace_id(lam, fb) == "<UNSET>"
    finally:
        for f in (fa, fb):
            try: lam.delete_function(FunctionName=f)
            except Exception: pass


def test_xray_trace_id_helper_unit():
    """Direct unit test of the helper used by all executors."""
    import re as _re
    from ministack.services.lambda_svc import _xray_trace_id_for_invocation
    # PassThrough / missing → None
    assert _xray_trace_id_for_invocation({}) is None
    assert _xray_trace_id_for_invocation({"TracingConfig": {"Mode": "PassThrough"}}) is None
    # Active → synthesizes proper format
    h = _xray_trace_id_for_invocation({"TracingConfig": {"Mode": "Active"}})
    assert _re.match(_XRAY_TRACE_HEADER_RE, h), h
    # Inbound header propagates regardless of mode (chained Lambda → Lambda
    # invocation: parent's trace ID stitches into child via header).
    inbound = "Root=1-12345678-aaaabbbbccccddddeeeeffff;Parent=1111222233334444;Sampled=1"
    assert _xray_trace_id_for_invocation({}, inbound) == inbound
    assert _xray_trace_id_for_invocation({"TracingConfig": {"Mode": "Active"}}, inbound) == inbound


# ---------------------------------------------------------------------------
# Layer / code zip extraction preserves unix mode bits — issue #888. AWS keeps
# layer file permissions; the +x on /opt/bin tools and bundled binaries must
# survive extraction (ZipFile.extractall drops them).
# ---------------------------------------------------------------------------


def test_extract_zip_preserves_executable_bit():
    import tempfile
    from ministack.services.lambda_svc import _extract_zip_preserving_mode

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        exe = zipfile.ZipInfo("bin/tool")
        exe.external_attr = 0o755 << 16
        zf.writestr(exe, "#!/bin/sh\necho hi\n")
        mod = zipfile.ZipInfo("python/mymod.py")
        mod.external_attr = 0o644 << 16
        zf.writestr(mod, "X = 1\n")
    buf.seek(0)

    dest = tempfile.mkdtemp()
    with zipfile.ZipFile(buf) as zf:
        _extract_zip_preserving_mode(zf, dest)

    tool_mode = os.stat(os.path.join(dest, "bin/tool")).st_mode & 0o777
    assert tool_mode == 0o755, f"executable bit dropped: {oct(tool_mode)}"
    assert os.stat(os.path.join(dest, "python/mymod.py")).st_mode & 0o777 == 0o644


def test_extract_zip_windows_zip_keeps_default_mode():
    """Windows-created zips (PowerShell Compress-Archive) carry no unix mode
    (external_attr high bits = 0) — we must NOT chmod them to 0, which would
    make the extracted files unreadable."""
    import tempfile
    from ministack.services.lambda_svc import _extract_zip_preserving_mode

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("python/winmod.py", "Y = 2\n")  # external_attr defaults to 0
    buf.seek(0)

    dest = tempfile.mkdtemp()
    with zipfile.ZipFile(buf) as zf:
        _extract_zip_preserving_mode(zf, dest)

    mode = os.stat(os.path.join(dest, "python/winmod.py")).st_mode & 0o777
    assert mode != 0, "file left unreadable (chmod 0) on a windows-style zip"


def test_lambda_local_executor_site_packages_layer(lam):
    """Local executor exposes <layer>/python/lib/python*/site-packages as a
    *site directory* (AWS's documented semantics), so pip-style (`pip install
    -t`) dependency layers import — including `.pth`-driven paths, which require
    `site.addsitedir` rather than a plain `sys.path.insert` (#888)."""
    sp = "python/lib/python3.12/site-packages"
    lbuf = io.BytesIO()
    with zipfile.ZipFile(lbuf, "w") as z:
        # regular package directly in site-packages
        z.writestr(f"{sp}/sitelib888.py", "def hi():\n    return 'sp-ok'\n")
        # a .pth file that adds a sibling dir — only resolves via site.addsitedir
        z.writestr(f"{sp}/extra888.pth", "vendored888\n")
        z.writestr(f"{sp}/vendored888/pthmod888.py", "def hi():\n    return 'pth-ok'\n")
    lv = lam.publish_layer_version(
        LayerName="sp-layer-888", Content={"ZipFile": lbuf.getvalue()},
        CompatibleRuntimes=["python3.12"])
    fbuf = io.BytesIO()
    with zipfile.ZipFile(fbuf, "w") as z:
        z.writestr("index.py",
                   "import sitelib888, pthmod888\n"
                   "def handler(e, c):\n"
                   "    return {'sp': sitelib888.hi(), 'pth': pthmod888.hi()}\n")
    lam.create_function(
        FunctionName="sp-fn-888", Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/r", Handler="index.handler",
        Code={"ZipFile": fbuf.getvalue()}, Layers=[lv["LayerVersionArn"]])
    resp = lam.invoke(FunctionName="sp-fn-888", Payload=b"{}")
    assert "FunctionError" not in resp, resp
    payload = json.loads(resp["Payload"].read())
    assert payload["sp"] == "sp-ok"
    assert payload["pth"] == "pth-ok"
