import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

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
        Runtime="python3.9",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
    )
    funcs = lam.list_functions()
    assert any(f["FunctionName"] == "test-func-1" for f in funcs["Functions"])
    resp = lam.invoke(FunctionName="test-func-1", Payload=json.dumps({"key": "value"}))
    payload = json.loads(resp["Payload"].read())
    assert payload["statusCode"] == 200

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
        Runtime="python3.9",
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
        Runtime="python3.9",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(_LAMBDA_CODE)},
    )
    assert resp["FunctionName"] == "lam-create-test"
    assert resp["Runtime"] == "python3.9"
    assert resp["Handler"] == "index.handler"
    assert resp["State"] == "Active"
    assert "FunctionArn" in resp

def test_lambda_create_duplicate(lam):
    with pytest.raises(ClientError) as exc:
        lam.create_function(
            FunctionName="lam-create-test",
            Runtime="python3.9",
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

def test_lambda_list_functions(lam):
    resp = lam.list_functions()
    names = [f["FunctionName"] for f in resp["Functions"]]
    assert "lam-create-test" in names

def test_lambda_delete_function(lam):
    lam.create_function(
        FunctionName="lam-to-delete",
        Runtime="python3.9",
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
        Runtime="python3.9",
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
        Runtime="python3.9",
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
        Runtime="python3.9",
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
        Runtime="python3.9",
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

    fname = f"intg-warm-{_uuid.uuid4().hex[:8]}"
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
        Runtime="python3.9",
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

def test_lambda_endpoint_url_not_overridden_by_function_env(lam):
    """AWS_ENDPOINT_URL from function env vars must not override the
    process-level value.  When MiniStack runs in Docker, the host-mapped
    port (e.g. 4568) is unreachable from inside the container — the
    Lambda binary must always use MiniStack's internal endpoint.

    This test verifies that the MiniStack server's AWS_ENDPOINT_URL takes
    precedence over function-level env vars.  It requires the server to
    have AWS_ENDPOINT_URL set (as it does when running via docker-compose).
    """
    # Verify the MiniStack server has AWS_ENDPOINT_URL set by checking
    # a baseline Lambda.  If the server doesn't have it, the override
    # logic has nothing to restore and this test is not meaningful.
    probe_code = (
        "import os\n"
        "def handler(event, context):\n"
        "    return {'endpoint': os.environ.get('AWS_ENDPOINT_URL', '')}\n"
    )
    probe_name = f"lam-endpoint-probe-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=probe_name,
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(probe_code)},
    )
    resp = lam.invoke(FunctionName=probe_name, Payload=b"{}")
    server_endpoint = json.loads(resp["Payload"].read()).get("endpoint", "")
    if not server_endpoint:
        pytest.skip("MiniStack server does not have AWS_ENDPOINT_URL set "
                     "(run with docker-compose to test endpoint override)")

    # Now test with a function that sets a conflicting AWS_ENDPOINT_URL.
    code = (
        "import os\n"
        "def handler(event, context):\n"
        "    return {'endpoint': os.environ.get('AWS_ENDPOINT_URL', 'unset')}\n"
    )
    fname = f"lam-endpoint-override-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=fname,
        Runtime="python3.11",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
        Environment={"Variables": {
            "AWS_ENDPOINT_URL": "http://should-be-overridden:9999",
        }},
    )
    resp = lam.invoke(FunctionName=fname, Payload=b"{}")
    payload = json.loads(resp["Payload"].read())
    # The Lambda must see the server's endpoint, not the function env var.
    assert payload["endpoint"] != "http://should-be-overridden:9999", (
        "Function-level AWS_ENDPOINT_URL must not override internal endpoint"
    )
    assert payload["endpoint"] == server_endpoint


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

def test_lambda_function_url_config(lam):
    """CreateFunctionUrlConfig / Get / Update / Delete / List lifecycle."""
    import uuid as _uuid_mod

    fn = f"intg-url-cfg-{_uuid_mod.uuid4().hex[:8]}"
    lam.create_function(
        FunctionName=fn,
        Runtime="python3.9",
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
        Runtime="python3.9",
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
        Runtime="python3.9",
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
        Runtime="python3.9",
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

def test_lambda_publish_version_snapshot(lam):
    """PublishVersion creates a numbered version snapshot."""
    code = _zip_lambda("def handler(e,c): return 'v1'")
    lam.create_function(
        FunctionName="qa-lam-version",
        Runtime="python3.9",
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

def test_lambda_function_concurrency(lam):
    """PutFunctionConcurrency / GetFunctionConcurrency / DeleteFunctionConcurrency."""
    code = _zip_lambda("def handler(e,c): return {}")
    lam.create_function(
        FunctionName="qa-lam-concurrency",
        Runtime="python3.9",
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
        Runtime="python3.9",
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
                Runtime="python3.9",
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
            Runtime="python3.9",
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
            Runtime="python3.9",
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
    import base64, zipfile, io

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
    import base64, zipfile, io

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
        Runtime="python3.9",
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
    import zipfile, io
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

    fname = f"apigwv2-node-{_uuid.uuid4().hex[:8]}"
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
        Runtime="python3.9",
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
        Runtime="python3.9",
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
        Runtime="python3.9",
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
        Runtime="python3.9",
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
