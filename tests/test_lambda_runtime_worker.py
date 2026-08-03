"""
Unit tests for the Lambda warm-worker runtime:

  test_tmpdir_cleaned_before_respawn
    -- _spawn() must shutil.rmtree the old tmpdir before mkdtemp on re-spawn.

  test_process_terminated_on_error_response
    -- invoke() must call proc.terminate() when the handler returns status=error.

The cleanup/protocol tests mock subprocesses; context coverage runs a real local
Python worker. No Docker or running Ministack instance is required.
"""

import io
import json
import time
import zipfile
from unittest.mock import MagicMock, mock_open, patch

from ministack.core import lambda_runtime
from ministack.core.lambda_runtime import Worker

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config():
    return {
        "Runtime": "python3.12",
        "Handler": "index.handler",
        "FunctionName": "test-fn",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:test-fn",
        "Timeout": 30,
    }


def _spawn_proc():
    """Minimal Popen mock sufficient for one _spawn() call."""
    proc = MagicMock()
    # stdout: return the init-ready JSON then EOF
    ready = json.dumps({"status": "ready"}) + "\n"
    proc.stdout.readline.return_value = ready
    # stderr: empty iterator so the daemon thread exits immediately
    proc.stderr = iter([])
    proc.poll.return_value = None
    return proc


def _protocol_line(status, **fields):
    return json.dumps({"status": status, **fields}) + "\n"


def _mock_worker(stdout_lines):
    worker = Worker("test-fn", _config(), b"ignored-zip")
    proc = MagicMock()
    proc.poll.return_value = None
    proc.stdout.readline.side_effect = list(stdout_lines)
    proc.stdin = MagicMock()
    proc.stderr = iter([])
    worker._proc = proc
    return worker, proc


def _invoke_worker(stdout_lines, request_id="req"):
    worker, proc = _mock_worker(stdout_lines)
    return worker.invoke({}, request_id=request_id), proc, worker


# ---------------------------------------------------------------------------
# Test 1: tmpdir is cleaned up on respawn
# ---------------------------------------------------------------------------


def test_tmpdir_cleaned_before_respawn():
    """_spawn() must rmtree the previous tmpdir before mkdtemp on re-spawn.

    Verifies the fix: shutil.rmtree(self._tmpdir) is called inside _spawn()
    before tempfile.mkdtemp() creates the replacement directory.
    """
    worker = Worker("test-fn", _config(), b"ignored-zip")

    first_dir = "/fake/ministack-lambda-test-fn-FIRST"
    second_dir = "/fake/ministack-lambda-test-fn-SECOND"
    dirs = iter([first_dir, second_dir])

    proc1, proc2 = _spawn_proc(), _spawn_proc()
    procs = iter([proc1, proc2])

    # Record the call sequence so we can assert ordering
    call_log: list = []

    def fake_mkdtemp(**kw):
        d = next(dirs)
        call_log.append(("mkdtemp", d))
        return d

    def fake_rmtree(path, **kw):
        call_log.append(("rmtree", path))

    with (
        patch("ministack.core.lambda_runtime.tempfile.mkdtemp", side_effect=fake_mkdtemp),
        patch("ministack.core.lambda_runtime.shutil.rmtree", side_effect=fake_rmtree),
        patch("ministack.core.lambda_runtime.os.path.exists", return_value=True),
        patch("ministack.core.lambda_runtime.os.makedirs"),
        patch("ministack.core.lambda_runtime.zipfile.ZipFile"),
        patch("builtins.open", mock_open()),
        patch(
            "ministack.core.lambda_runtime.subprocess.Popen",
            side_effect=lambda *a, **k: next(procs),
        ),
    ):
        worker._spawn()
        assert worker._tmpdir == first_dir

        worker._spawn()
        assert worker._tmpdir == second_dir

    # Verify exactly one rmtree call, targeting the first directory
    rmtree_events = [(op, p) for op, p in call_log if op == "rmtree"]
    mkdtemp_events = [(op, p) for op, p in call_log if op == "mkdtemp"]

    assert rmtree_events == [("rmtree", first_dir)], (
        "shutil.rmtree should be called exactly once, for the first tmpdir"
    )
    assert len(mkdtemp_events) == 2, "mkdtemp should be called once per spawn"

    # rmtree(first_dir) must appear BEFORE the second mkdtemp in the call sequence
    rmtree_pos = call_log.index(("rmtree", first_dir))
    mkdtemp2_pos = call_log.index(("mkdtemp", second_dir))
    assert rmtree_pos < mkdtemp2_pos, (
        "rmtree(first_dir) must precede mkdtemp() for the replacement directory"
    )


# ---------------------------------------------------------------------------
# Test 2: process terminated on error response
# ---------------------------------------------------------------------------


def test_process_terminated_on_error_response():
    """invoke() must call proc.terminate() when the handler returns status=error."""
    error_line = _protocol_line("error", error="handler blew up")
    result, proc, worker = _invoke_worker([error_line], request_id="req-001")

    assert result["status"] == "error", "invoke() should surface the error status"
    proc.terminate.assert_called_once_with()
    assert worker._proc is None, "_proc must be cleared after an error response"


def test_python_worker_exposes_standard_lambda_context_fields():
    """Warm Python workers expose the context fields documented by Lambda."""
    code = """\
import os

def handler(event, context):
    return {
        "function_name": context.function_name,
        "function_version": context.function_version,
        "memory_limit_in_mb": context.memory_limit_in_mb,
        "invoked_function_arn": context.invoked_function_arn,
        "aws_request_id": context.aws_request_id,
        "log_group_name": context.log_group_name,
        "log_stream_name": context.log_stream_name,
        "env_log_stream_name": os.environ["AWS_LAMBDA_LOG_STREAM_NAME"],
        "identity": context.identity,
        "client_context": context.client_context,
        "remaining_time": context.get_remaining_time_in_millis(),
    }
"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("index.py", code)

    config = _config()
    config["Version"] = "$LATEST"
    worker = Worker("test-fn", config, buf.getvalue())
    try:
        result = worker.invoke({}, request_id="context-request-id")
    finally:
        worker.kill()

    assert result["status"] == "ok", result
    context = result["result"]
    assert context["function_name"] == "test-fn"
    assert context["function_version"] == "$LATEST"
    assert context["memory_limit_in_mb"] == 128
    assert context["invoked_function_arn"] == config["FunctionArn"]
    assert context["aws_request_id"] == "context-request-id"
    assert context["log_group_name"] == "/aws/lambda/test-fn"
    assert context["log_stream_name"]
    assert context["log_stream_name"] == context["env_log_stream_name"]
    assert context["identity"] is None
    assert context["client_context"] is None
    assert 0 < context["remaining_time"] <= 30_000


def test_invoke_ignores_json_logs_on_stdout():
    """Pino-style JSON on fd 1 must not be mistaken for the protocol response."""
    ok_line = _protocol_line("ok", result={"ok": True})
    log_line = json.dumps({"level": 30, "msg": "hi"}) + "\n"
    result, _, _ = _invoke_worker([log_line, ok_line])

    assert result["status"] == "ok"
    assert result["result"] == {"ok": True}


def test_invoke_ignores_raw_text_on_stdout():
    """Plain fd-1 writes must not prevent reading the protocol response."""
    ok_line = _protocol_line("ok", result={"ok": True})
    result, _, _ = _invoke_worker(["hi\n", ok_line])

    assert result["status"] == "ok"
    assert result["result"] == {"ok": True}


def test_invoke_ignores_json_with_unrelated_status_key():
    """HTTP-style JSON logs with a status code must not end the read loop."""
    junk = json.dumps({"status": 200, "message": "ok"}) + "\n"
    ok_line = _protocol_line("ok", result={"n": 1})
    result, _, _ = _invoke_worker([junk, ok_line])

    assert result["status"] == "ok"
    assert result["result"] == {"n": 1}


def test_invoke_ignores_many_log_lines_before_protocol():
    """A burst of structured logs must not hide the real protocol line."""
    logs = [
        json.dumps({"level": 30, "msg": f"line-{i}"}) + "\n"
        for i in range(8)
    ]
    ok_line = _protocol_line("ok", result={"done": True})
    result, _, _ = _invoke_worker(logs + [ok_line])

    assert result["status"] == "ok"
    assert result["result"] == {"done": True}


def test_invoke_skips_malformed_json_lines():
    """Broken JSON on stdout must be ignored, not treated as the response."""
    ok_line = _protocol_line("ok", result={})
    result, _, _ = _invoke_worker(['{"truncated":\n', ok_line])

    assert result["status"] == "ok"


def test_invoke_skips_empty_lines():
    """Blank lines between junk output and the protocol line are ignored."""
    ok_line = _protocol_line("ok", result={"x": 1})
    result, _, _ = _invoke_worker(["\n", "noise\n", "\n", ok_line])

    assert result["status"] == "ok"
    assert result["result"] == {"x": 1}


def test_invoke_skips_ready_status_during_invoke():
    """Init-only ready messages must not satisfy an invocation read."""
    ready = _protocol_line("ready", cold=False)
    ok_line = _protocol_line("ok", result={"v": 2})
    result, _, _ = _invoke_worker([ready, ok_line])

    assert result["status"] == "ok"
    assert result["result"] == {"v": 2}


def test_invoke_still_surfaces_protocol_error():
    """Protocol error lines must still fail the invocation."""
    error_line = _protocol_line("error", error="boom")
    result, proc, _ = _invoke_worker([error_line])

    assert result["status"] == "error"
    assert result["error"] == "boom"
    proc.terminate.assert_called_once_with()


def test_invoke_error_after_junk_stdout():
    """Handler errors must win even when stdout already has log noise."""
    junk = json.dumps({"level": 50, "msg": "warn"}) + "\n"
    err_line = _protocol_line("error", error="fail")
    result, proc, _ = _invoke_worker([junk, "oops\n", err_line])

    assert result["status"] == "error"
    assert result["error"] == "fail"
    proc.terminate.assert_called_once_with()


def test_invoke_gives_up_after_max_lines():
    """Stop after 200 non-protocol lines instead of hanging forever."""
    worker, proc = _mock_worker(["noise\n"] * 201)
    result = worker.invoke({}, request_id="req-max")

    assert result["status"] == "error"
    assert "No JSON response" in result["error"]


# ---------------------------------------------------------------------------
# Warm pool bounds: idle release and LRU ceiling
# ---------------------------------------------------------------------------


def _warm_worker(name="test-fn", last_used=None):
    """Pooled worker with a live process mock, as after a cold start."""
    worker = Worker(name, _config(), b"ignored-zip")
    proc = MagicMock()
    proc.poll.return_value = None
    proc.stderr = iter([])
    worker._proc = proc
    worker._cold = False
    if last_used is not None:
        worker.last_used = last_used
    return worker, proc


def test_release_if_idle_reclaims_subprocess_but_keeps_worker_poolable():
    worker, proc = _warm_worker()

    assert worker.is_warm
    assert worker.release_if_idle() is True

    proc.terminate.assert_called_once_with()
    assert worker._proc is None, "released worker must drop its process handle"
    assert not worker.is_warm
    assert worker.release_if_idle() is False, "second pass has nothing to reclaim"


def test_release_if_idle_skips_worker_with_invocation_in_flight():
    worker, proc = _warm_worker()
    worker._lock.acquire()
    try:
        assert worker.release_if_idle() is False
        proc.terminate.assert_not_called()
        assert worker.is_warm
    finally:
        worker._lock.release()


def test_invoke_refreshes_last_used():
    worker, _proc = _mock_worker([_protocol_line("ok", result={})])
    worker.last_used = time.time() - 3600

    worker.invoke({}, request_id="req-idle")

    assert time.time() - worker.last_used < 5, "invoke() must refresh idle age"


def test_release_idle_workers_only_reaps_past_the_ttl():
    fresh, fresh_proc = _warm_worker("fresh")
    stale, stale_proc = _warm_worker("stale", last_used=time.time() - 10_000)

    with patch.dict(
        lambda_runtime._workers, {"a:fresh": fresh, "a:stale": stale}, clear=True
    ):
        assert lambda_runtime.release_idle_workers() == 1

    stale_proc.terminate.assert_called_once_with()
    fresh_proc.terminate.assert_not_called()
    assert fresh.is_warm


def test_warm_cap_releases_least_recently_used():
    now = time.time()
    oldest, oldest_proc = _warm_worker("oldest", last_used=now - 300)
    middle, middle_proc = _warm_worker("middle", last_used=now - 200)
    newest, newest_proc = _warm_worker("newest", last_used=now - 100)
    spawning, spawning_proc = _warm_worker("spawning", last_used=now)

    pool = {
        "a:oldest": oldest, "a:middle": middle,
        "a:newest": newest, "a:spawning": spawning,
    }
    # Cap 2, one slot taken by `spawning` — the two oldest must go.
    with (
        patch.dict(lambda_runtime._workers, pool, clear=True),
        patch.object(lambda_runtime, "_MAX_WARM_WORKERS", 2),
    ):
        assert lambda_runtime._enforce_warm_cap(exclude=spawning) == 2

    oldest_proc.terminate.assert_called_once_with()
    middle_proc.terminate.assert_called_once_with()
    newest_proc.terminate.assert_not_called()
    spawning_proc.terminate.assert_not_called()
    assert newest.is_warm and spawning.is_warm


def test_warm_cap_disabled_by_zero():
    worker, proc = _warm_worker("only", last_used=time.time() - 300)

    with (
        patch.dict(lambda_runtime._workers, {"a:only": worker}, clear=True),
        patch.object(lambda_runtime, "_MAX_WARM_WORKERS", 0),
    ):
        assert lambda_runtime._enforce_warm_cap() == 0

    proc.terminate.assert_not_called()
