"""Invoke identity follows the caller's qualifier, not alias resolution.

AWS contract: https://docs.aws.amazon.com/lambda/latest/dg/python-context.html
"""
import asyncio
import io
import json
import uuid
import zipfile
from copy import deepcopy

import pytest

from ministack.core.lambda_runtime import invalidate_worker
from ministack.services import lambda_svc as svc


@pytest.mark.parametrize("runtime", ["python3.12", "nodejs20.x"])
@pytest.mark.parametrize("asynchronous", [False, True])
def test_invocation_retains_alias_without_mutating_version(monkeypatch, asynchronous, runtime):
    name = "identity-" + uuid.uuid4().hex
    arn = svc._func_arn(name)
    config = {"FunctionName": name, "FunctionArn": arn, "Runtime": runtime,
              "Handler": "index.handler", "Timeout": 10, "Version": "$LATEST"}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("index.py", "def handler(event, context):\n"
                         "    return [context.invoked_function_arn, context.function_version, event]\n")
        archive.writestr("index.js", "exports.handler = async (event, context) => [context.invokedFunctionArn, context.functionVersion, event];")
    version = {"config": {**config, "FunctionArn": arn + ":1", "Version": "1"},
               "code_zip": buf.getvalue()}
    function = {"config": config, "code_zip": buf.getvalue(), "versions": {"1": version},
                "aliases": {"worker": {"FunctionVersion": "1"}, "other": {"FunctionVersion": "1"}}}
    original = deepcopy(function)
    monkeypatch.setitem(svc._functions, name, function)
    monkeypatch.setattr(svc, "_emit_lambda_metrics", lambda *args, **kwargs: None)
    monkeypatch.setattr(svc, "_execute_function_with_config_scope", svc._execute_function_warm)
    captured = []
    monkeypatch.setattr(svc, "invoke_async_with_retry", lambda f, e: captured.append(svc._execute_function_warm(f, e)))
    try:
        # Alternate aliases sharing one published version, including warm reuse.
        for qualifier in ["worker", "other", "1", "worker", None, "$LATEST"]:
            headers = {"x-amz-invocation-type": "Event"} if asynchronous else {}
            status, response_headers, body = asyncio.run(svc._invoke(name, {"value": 1}, headers, qualifier))
            expected_version = "1" if qualifier in ("worker", "other", "1") else "$LATEST"
            assert response_headers["X-Amz-Executed-Version"] == expected_version
            assert status == (202 if asynchronous else 200)
            result = captured[-1]["body"] if asynchronous else json.loads(body)
            assert result == [arn + (":" + qualifier if qualifier else ""), expected_version, {"value": 1}]
            assert function == original
    finally:
        invalidate_worker(name)
