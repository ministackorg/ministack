"""
Unit tests for Worker resource-cleanup fixes:

  test_tmpdir_cleaned_before_respawn
    -- _spawn() must shutil.rmtree the old tmpdir before mkdtemp on re-spawn.

  test_process_terminated_on_error_response
    -- invoke() must call proc.terminate() when the handler returns status=error.

Both tests mock subprocess so no running Docker/Ministack instance is required.
"""

import json
from unittest.mock import MagicMock, mock_open, patch

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
