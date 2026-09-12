"""Local custom-runtime routing and pool integration; no running server required."""

import time
from unittest.mock import Mock

import pytest

from ministack.core import lambda_runtime
from ministack.services import lambda_svc


def _config(account="111122223333", region="us-east-1", version="$LATEST"):
    return {
        "FunctionName": "provided-pool-test",
        "FunctionArn": f"arn:aws:lambda:{region}:{account}:function:provided-pool-test",
        "Runtime": "provided.al2023",
        "Version": version,
        "Timeout": 2,
        "Handler": "bootstrap",
    }


@pytest.fixture
def isolated_pool(monkeypatch):
    monkeypatch.setattr(lambda_runtime, "_workers", {})
    monkeypatch.setattr(lambda_svc, "_ensure_reaper_thread", lambda: None)
    try:
        yield
    finally:
        lambda_runtime.reset()


@pytest.mark.parametrize("event", [
    {"_x_amzn_trace_id": "user data", "value": 1}, [1, 2], "text", None,
])
def test_provided_metadata_does_not_mutate_payload(monkeypatch, event):
    worker = Mock()
    worker.invoke.return_value = {"status": "ok", "result": event, "log": "handler log"}
    monkeypatch.setattr(lambda_svc, "_ensure_reaper_thread", lambda: None)
    monkeypatch.setattr(lambda_svc, "acquire_worker", Mock(return_value=(worker, "spawn")))
    release = Mock()
    monkeypatch.setattr(lambda_svc, "release_worker", release)
    monkeypatch.setattr(lambda_svc, "_xray_trace_id_for_invocation", lambda config: "trace-1")
    result = lambda_svc._execute_function_provided_warm(
        {"config": _config(), "code_zip": b"zip"}, event, "request-1",
    )
    worker.invoke.assert_called_once_with(event, "request-1", trace_id="trace-1")
    release.assert_called_once_with(worker)
    assert result == {"body": event, "log": "handler log"}
    if isinstance(event, dict):
        assert event["_x_amzn_trace_id"] == "user data"


def test_provided_failure_is_scoped_and_not_retried(monkeypatch, isolated_pool):
    config = _config()
    failed, _ = lambda_runtime.acquire_worker(config["FunctionName"], config, b"zip")
    lambda_runtime.release_worker(failed)
    fail = Mock(side_effect=RuntimeError("bootstrap crashed"))
    monkeypatch.setattr(failed, "invoke", fail)
    unrelated = []
    for account, region, version in [
        ("999900001111", "us-east-1", "$LATEST"),
        ("111122223333", "eu-west-1", "$LATEST"),
        ("111122223333", "us-east-1", "1"),
    ]:
        other = _config(account, region, version)
        worker, _ = lambda_runtime.acquire_worker(
            other["FunctionName"], other, b"zip", qualifier=version,
        )
        unrelated.append(worker)
    fallback = Mock()
    monkeypatch.setattr(lambda_svc, "_execute_function_provided", fallback)
    result = lambda_svc._execute_function_provided_warm({"config": config, "code_zip": b"zip"}, {})
    assert result["error"] is True
    assert result["body"]["errorMessage"] == "bootstrap crashed"
    assert fail.call_count == 1
    fallback.assert_not_called()
    remaining = [w for entries in lambda_runtime._workers.values() for w in entries]
    assert remaining == unrelated
    assert all(w.in_use for w in unrelated)


def test_provided_concurrent_leases_and_reuse(isolated_pool):
    config = _config()
    first, _ = lambda_runtime.acquire_worker(config["FunctionName"], config, b"zip")
    second, _ = lambda_runtime.acquire_worker(config["FunctionName"], config, b"zip")
    assert isinstance(first, lambda_runtime.ProvidedWorker)
    assert isinstance(second, lambda_runtime.ProvidedWorker)
    assert first is not second
    lambda_runtime.release_worker(first)
    reused, reason = lambda_runtime.acquire_worker(config["FunctionName"], config, b"zip")
    assert reused is first
    assert reason == "reused"
    lambda_runtime.release_worker(reused)
    lambda_runtime.release_worker(second)


@pytest.mark.parametrize("mode,target", [
    ("local", "_execute_function_provided_warm"),
    ("durable", "_execute_function_provided"),
    ("docker", "_execute_function_docker"),
    ("strict", "_execute_function_docker"),
    ("image", "_execute_function_docker"),
    ("proxy", "_execute_function_proxy"),
])
def test_provided_dispatch_preserves_other_executors(monkeypatch, mode, target):
    monkeypatch.setattr(lambda_svc, "LAMBDA_EXECUTOR", "docker" if mode == "docker" else "local")
    monkeypatch.setattr(lambda_svc, "LAMBDA_STRICT", mode == "strict")
    monkeypatch.setattr(lambda_svc, "_proxy_url_for", lambda config: "http://proxy" if mode == "proxy" else None)
    monkeypatch.setattr(lambda_svc, "_emit_lambda_logs", Mock())
    names = ["_execute_function_provided_warm", "_execute_function_provided",
             "_execute_function_docker", "_execute_function_proxy"]
    executors = {name: Mock(return_value={"body": name}) for name in names}
    for name, executor in executors.items():
        monkeypatch.setattr(lambda_svc, name, executor)
    config = _config()
    if mode == "image":
        config.update(PackageType="Image", ImageUri="example:latest")
    func = {"config": config, "code_zip": b"zip"}
    token = lambda_svc._durable_ctx.set({"test": True} if mode == "durable" else None)
    try:
        result = lambda_svc._execute_function_dispatch(func, config, {}, "request-1", time.time())
    finally:
        lambda_svc._durable_ctx.reset(token)
    assert result == {"body": target}
    for name, executor in executors.items():
        assert executor.call_count == (1 if name == target else 0)
    if mode == "local":
        executors[target].assert_called_once_with(func, {}, "request-1")


def test_provided_env_keeps_function_vars_and_endpoint_precedence(monkeypatch):
    config = _config()
    config.update(MemorySize=256, Environment={"Variables": {
        "CUSTOM": "value", "AWS_ENDPOINT_URL": "http://wrong:4566",
    }})
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://ministack:4566")
    monkeypatch.setattr(lambda_svc, "get_region", lambda: "us-east-1")
    token = lambda_svc._durable_ctx.set(None)
    try:
        env = lambda_svc._provided_worker_env(config, "/code", 12345)
    finally:
        lambda_svc._durable_ctx.reset(token)
    assert env["AWS_LAMBDA_RUNTIME_API"] == "127.0.0.1:12345"
    assert env["AWS_LAMBDA_FUNCTION_NAME"] == config["FunctionName"]
    assert env["AWS_LAMBDA_FUNCTION_MEMORY_SIZE"] == "256"
    assert env["AWS_ACCESS_KEY_ID"] == "111122223333"
    assert env["AWS_REGION"] == "us-east-1"
    assert env["LAMBDA_TASK_ROOT"] == "/code"
    assert env["CUSTOM"] == "value"
    assert env["AWS_ENDPOINT_URL"] == "http://ministack:4566"


def test_provided_env_uses_execution_role_credentials(monkeypatch):
    config = _config()
    credentials = {
        "AWS_ACCESS_KEY_ID": "ASIATESTROLE",
        "AWS_SECRET_ACCESS_KEY": "role-secret",
        "AWS_SESSION_TOKEN": "role-session",
    }
    resolve = Mock(return_value=credentials)
    monkeypatch.setattr(lambda_svc, "execution_credentials", resolve)
    env = lambda_svc._provided_worker_env(config, "/code", 12345)
    resolve.assert_called_once_with(config)
    assert {key: env[key] for key in credentials} == credentials


@pytest.mark.parametrize("operation", ["code", "configuration", "delete"])
def test_function_changes_invalidate_provided_workers(monkeypatch, isolated_pool, operation):
    config = _config()
    name = config["FunctionName"]
    monkeypatch.setattr(lambda_svc, "_functions", {name: {"config": config, "code_zip": b"zip"}})
    monkeypatch.setattr(lambda_svc, "get_account_id", lambda: "111122223333")
    monkeypatch.setattr(lambda_svc, "get_region", lambda: "us-east-1")
    monkeypatch.setattr(lambda_svc, "_pool_kill_function", Mock())
    monkeypatch.setattr(lambda_svc, "_sweep_extract_cache", Mock())
    monkeypatch.setattr(lambda_svc, "_schedule_state_transition", Mock())
    worker, _ = lambda_runtime.acquire_worker(name, config, b"zip")
    lambda_runtime.release_worker(worker)
    if operation == "code":
        result = lambda_svc._update_code(name, {})
    elif operation == "configuration":
        result = lambda_svc._update_config(name, {"Environment": {"Variables": {"UPDATED": "yes"}}})
    else:
        result = lambda_svc._delete_function(name, {})
    assert result[0] in (200, 204)
    assert not lambda_runtime._workers
