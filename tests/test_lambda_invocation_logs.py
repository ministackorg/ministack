"""Invocation IDs and platform logs: AWS docs and local RIE observations.

Docker tests use real RIE containers; no live AWS account validation is claimed.
"""
import asyncio
import base64
import io
import json
import re
import shutil
import time
import urllib.error
import uuid
import zipfile
from unittest.mock import Mock

import pytest

from ministack.core import lambda_runtime
from ministack.core.responses import AccountRegionScopedDict
from ministack.services import cloudwatch_logs as cwl
from ministack.services import lambda_svc as svc


@pytest.fixture
def log_state(monkeypatch):
    monkeypatch.setattr(cwl, "_log_groups", AccountRegionScopedDict())
    monkeypatch.setattr(cwl, "_fanout_to_subscription_filters", Mock())
    monkeypatch.setattr(svc, "_probe_peak_memory_mb", lambda func: 12)
    monkeypatch.setattr(svc, "_ensure_reaper_thread", lambda: None)
    monkeypatch.setattr(svc, "_proxy_url_for", lambda config: None)
    monkeypatch.setattr(svc, "LAMBDA_STRICT", False)
    monkeypatch.setattr(svc, "LAMBDA_EXECUTOR", "local")


def _function(runtime="python3.12", code_zip=None):
    name = "invocation-logs-" + uuid.uuid4().hex
    return {"config": {
        "FunctionName": name,
        "FunctionArn": svc._func_arn(name),
        "Runtime": runtime, "Handler": "index.handler", "Version": "$LATEST",
        "MemorySize": 128, "Timeout": 10, "PackageType": "Zip",
    }, "code_zip": code_zip}


def _messages(func):
    group = cwl._log_groups["/aws/lambda/" + func["config"]["FunctionName"]]
    return [e["message"] for s in group["streams"].values() for e in s["events"]]


def _assert_platform_lines(messages, request_id):
    for kind in ("START", "END", "REPORT"):
        ids = [m.group(1) for line in messages
               if (m := re.match(rf"^{kind} RequestId: (\S+)", line))]
        assert ids == [request_id]


@pytest.mark.parametrize("mode,target", [
    ("python", "_execute_function_warm"), ("node", "_execute_function_warm"),
    ("docker", "_execute_function_docker"), ("strict", "_execute_function_docker"),
    ("image", "_execute_function_docker"), ("other", "_execute_function_docker"),
    ("provided", "_execute_function_provided_warm"),
    ("durable", "_execute_function_provided"), ("proxy", "_execute_function_proxy"),
])
def test_execute_function_generates_one_request_id(monkeypatch, log_state, mode, target):
    func = _function()
    if mode == "node":
        func["config"]["Runtime"] = "nodejs22.x"
    elif mode == "other":
        func["config"]["Runtime"] = "ruby3.3"
    elif mode in ("provided", "durable"):
        func["config"]["Runtime"] = "provided.al2023"
    elif mode == "image":
        func["config"].update(PackageType="Image", ImageUri="example:latest")
    monkeypatch.setattr(svc, "LAMBDA_EXECUTOR", "docker" if mode == "docker" else "local")
    monkeypatch.setattr(svc, "LAMBDA_STRICT", mode == "strict")
    if mode == "proxy":
        monkeypatch.setattr(svc, "_proxy_url_for", lambda config: "http://proxy")
    generated = Mock(return_value="invocation-id")
    executor = Mock(return_value={"body": {"ok": True}, "log": "user output"})
    emit = Mock()
    monkeypatch.setattr(svc, "new_uuid", generated)
    monkeypatch.setattr(svc, target, executor)
    monkeypatch.setattr(svc, "_emit_lambda_logs", emit)
    token = svc._durable_ctx.set({"arn": "execution"} if mode == "durable" else None)
    try:
        svc._execute_function(func, {"input": 1})
    finally:
        svc._durable_ctx.reset(token)
    generated.assert_called_once_with()
    args = (func, {"input": 1}, "http://proxy", "invocation-id") if mode == "proxy" else (
        func, {"input": 1}, "invocation-id")
    executor.assert_called_once_with(*args)
    assert emit.call_args.args[1] == "invocation-id"


@pytest.mark.parametrize("unavailable", ["sdk", "daemon"])
@pytest.mark.parametrize("runtime,target", [
    ("python3.12", "_execute_function_warm"),
    ("nodejs22.x", "_execute_function_warm"),
    ("ruby3.3", "_execute_function_local"),
])
def test_docker_fallback_preserves_id_and_synthetic_logs(
        monkeypatch, log_state, unavailable, runtime, target):
    func = _function(runtime, b"zip")
    monkeypatch.setattr(svc, "_docker_available", unavailable != "sdk")
    monkeypatch.setattr(svc, "_get_docker_client", lambda: None)
    executor = Mock(return_value={"body": {}, "log": "USER_PRINT fallback"})
    monkeypatch.setattr(svc, target, executor)
    monkeypatch.setattr(svc, "LAMBDA_EXECUTOR", "docker")
    svc._execute_function_dispatch(func, func["config"], {}, "same-id", time.time())
    executor.assert_called_once_with(func, {}, "same-id")
    _assert_platform_lines(_messages(func), "same-id")


@pytest.mark.parametrize("log_source,error", [(None, False), ("rie", False), ("rie", True)])
def test_invoke_preserves_log_provenance_and_tail(monkeypatch, log_state, log_source, error):
    func = _function()
    raw = "START RequestId: same-id Version: $LATEST\nUSER_PRINT once\nEND RequestId: same-id\nREPORT RequestId: same-id\tDuration: 1.25 ms"
    monkeypatch.setitem(svc._functions, func["config"]["FunctionName"], func)
    monkeypatch.setattr(svc, "LAMBDA_EXECUTOR", "docker")
    monkeypatch.setattr(svc, "_execute_function_docker", Mock(return_value={
        "body": {}, "log": raw, "log_source": log_source, "error": error}))
    monkeypatch.setattr(svc, "_emit_lambda_metrics", lambda *args, **kwargs: None)
    monkeypatch.setattr(svc, "new_uuid", lambda: "same-id")
    status, headers, _ = asyncio.run(svc._invoke(func["config"]["FunctionName"], {}, {"x-amz-log-type": "Tail"}))
    assert status == 200
    assert ("X-Amz-Function-Error" in headers) == error
    tail = base64.b64decode(headers["X-Amz-Log-Result"]).decode().splitlines()
    assert tail == raw.splitlines()
    messages = _messages(func)
    # User-written platform-looking lines remain user logs without RIE metadata.
    assert (messages if log_source == "rie" else messages[1:-2]) == tail
    if log_source == "rie":
        _assert_platform_lines(messages, "same-id")
    else:
        _assert_platform_lines([messages[0], *messages[-2:]], "same-id")
        assert len(messages) == len(tail) + 3
    forwarded = cwl._fanout_to_subscription_filters.call_args.args[2]
    assert [e["message"] for e in forwarded] == messages


@pytest.mark.parametrize("raw", ["", "partial runtime output"])
def test_rie_empty_and_partial_logs_are_handled_without_text_parsing(log_state, raw):
    func = _function()
    svc._emit_lambda_logs(func, "same-id", raw, True, 1, log_source="rie")
    if raw:
        assert _messages(func) == [raw]
    else:
        _assert_platform_lines(_messages(func), "same-id")


@pytest.mark.parametrize("outcome", ["ok", "handler-error", "init-error", "read-timeout", "not-running"])
def test_invoke_rie_passes_request_id_and_marks_actual_rie_output(monkeypatch, outcome):
    container = Mock()
    container.status = "exited" if outcome == "not-running" else "running"
    container.attrs = {"NetworkSettings": {"Networks": {}}}
    container.ports = {"8080/tcp": [{"HostPort": "12345"}]}
    container.logs.return_value = b"opaque output"
    monkeypatch.setattr(svc, "_running_in_container", lambda: False)
    monkeypatch.setattr(svc, "LAMBDA_DOCKER_NETWORK", "")
    response = Mock(headers={})
    response.read.return_value = json.dumps(
        {"errorType": "ValueError", "errorMessage": "failed"} if outcome == "handler-error" else {"ok": True}
    ).encode()
    seen = []

    def post(req, timeout):
        seen.append(req)
        if outcome == "init-error":
            raise urllib.error.HTTPError(req.full_url, 502, "Bad Gateway", {}, io.BytesIO(b"{}"))
        if outcome == "read-timeout":
            raise TimeoutError("timed out")
        return response

    monkeypatch.setattr(svc.urllib.request, "urlopen", post)
    result = svc._invoke_rie(container, {"input": 1}, 3, "same-id")
    assert result["log"] == "opaque output"
    if outcome == "not-running":
        assert not seen
        assert "log_source" not in result
    else:
        assert len(seen) == 1
        assert dict(seen[0].header_items())["X-amzn-requestid"] == "same-id"
        assert json.loads(seen[0].data) == {"input": 1}
        assert result["log_source"] == "rie"


_PYTHON_HANDLER = """import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
def handler(event, context):
    print('USER_PRINT ' + event['marker'], flush=True)
    logger.info('USER_INFO %s', event['marker'])
    return {'request_id': context.aws_request_id, 'stream': context.log_stream_name}
"""
_NODE_HANDLER = """exports.handler = async (event, context) => {
    return {request_id: context.awsRequestId, stream: context.logStreamName};
};
"""
_PROVIDED_BOOTSTRAP = """#!/usr/bin/env python3
import json, os, urllib.request
base = 'http://' + os.environ['AWS_LAMBDA_RUNTIME_API'] + '/2018-06-01/runtime/'
while True:
    response = urllib.request.urlopen(base + 'invocation/next')
    request_id = response.headers['Lambda-Runtime-Aws-Request-Id']
    response.read()
    payload = json.dumps({'request_id': request_id}).encode()
    urllib.request.urlopen(urllib.request.Request(base + 'invocation/' + request_id + '/response', data=payload)).read()
    if os.environ.get('PROBE_ONE_SHOT'):
        break
"""


def _code_zip():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("index.py", _PYTHON_HANDLER)
        archive.writestr("index.js", _NODE_HANDLER)
        bootstrap = zipfile.ZipInfo("bootstrap")
        bootstrap.external_attr = 0o100755 << 16
        archive.writestr(bootstrap, _PROVIDED_BOOTSTRAP)
    return output.getvalue()


def _assert_two_invocations(func, *, rie=False):
    previous_id = None
    for seq in range(2):
        marker = str(seq)
        before = len(_messages(func)) if seq else 0
        result = svc._execute_function(func, {"marker": marker})
        assert not result.get("error"), result
        request_id = result["body"]["request_id"]
        assert request_id != previous_id
        if func["config"]["Runtime"].startswith(("python", "nodejs")):
            assert request_id != result["body"]["stream"]
        messages = _messages(func)[before:]
        _assert_platform_lines(messages, request_id)
        if rie:
            assert result["log_source"] == "rie"
            assert messages == result["log"].splitlines()
        if func["config"]["Runtime"].startswith("python"):
            for kind in ("USER_PRINT", "USER_INFO"):
                assert sum(kind + " " + marker in line for line in messages) == 1
        previous_id = request_id


@pytest.mark.parametrize("runtime", ["python3.12", "nodejs22.x"])
@pytest.mark.parametrize("executor", ["warm", "local"])
def test_real_python_and_node_invocations_share_platform_request_id(monkeypatch, log_state, runtime, executor):
    if runtime.startswith("nodejs") and not shutil.which("node"):
        pytest.skip("Node.js is required")
    if executor == "local" and runtime.startswith("python") and not shutil.which("python3"):
        pytest.skip("local executor requires python3")
    if executor == "local":
        monkeypatch.setattr(svc, "_execute_function_warm", svc._execute_function_local)
    func = _function(runtime, _code_zip())
    try:
        _assert_two_invocations(func)
    finally:
        lambda_runtime.invalidate_worker(func["config"]["FunctionName"])


@pytest.mark.parametrize("one_shot", [False, True])
def test_real_provided_runtime_receives_platform_request_id(monkeypatch, log_state, one_shot):
    if not shutil.which("python3"):
        pytest.skip("provided bootstrap requires /usr/bin/env python3")
    func = _function("provided.al2023", _code_zip())
    if one_shot:
        func["config"]["Environment"] = {"Variables": {"PROBE_ONE_SHOT": "1"}}
    token = svc._durable_ctx.set({"arn": "execution"} if one_shot else None)
    try:
        _assert_two_invocations(func)
    finally:
        svc._durable_ctx.reset(token)
        lambda_runtime.invalidate_worker(func["config"]["FunctionName"])


@pytest.fixture(scope="module")
def python_lambda_image(tmp_path_factory):
    docker = pytest.importorskip("docker")
    try:
        client = docker.from_env()
        client.ping()
    except docker.errors.DockerException as exc:
        pytest.skip(f"Docker daemon unavailable: {exc}")
    base = "public.ecr.aws/lambda/python:3.12"
    try:
        client.images.get(base)
    except docker.errors.ImageNotFound:
        client.images.pull(base)
    tag = "ministack-log-test:" + uuid.uuid4().hex
    build_dir = tmp_path_factory.mktemp("lambda-image")
    (build_dir / "Dockerfile").write_text(
        'FROM ' + base + '\nCOPY index.py /var/task/index.py\nCMD ["index.handler"]\n', encoding="utf-8")
    (build_dir / "index.py").write_text(_PYTHON_HANDLER, encoding="utf-8")
    client.images.build(path=str(build_dir), tag=tag, rm=True)
    try:
        yield tag
    finally:
        client.images.remove(tag)
        client.close()


@pytest.mark.data_plane
@pytest.mark.parametrize("package_type", ["Zip", "Image"])
def test_real_docker_invocations_have_one_platform_log_sequence(monkeypatch, log_state, request, package_type):
    if svc._get_docker_client() is None:
        pytest.skip("Docker daemon unavailable")
    monkeypatch.setattr(svc, "LAMBDA_EXECUTOR", "docker")
    monkeypatch.setattr(svc, "_KEEPALIVE_KILL_ON_RELEASE", False)
    spawn = Mock(wraps=svc._spawn_lambda_container)
    monkeypatch.setattr(svc, "_spawn_lambda_container", spawn)
    func = _function(code_zip=_code_zip())
    if package_type == "Image":
        func["config"].update(PackageType="Image", ImageUri=request.getfixturevalue("python_lambda_image"))
    try:
        _assert_two_invocations(func, rie=True)
        spawn.assert_called_once()  # The second invocation must reuse the environment.
    finally:
        svc._pool_kill_function("000000000000", func["config"]["FunctionName"])
