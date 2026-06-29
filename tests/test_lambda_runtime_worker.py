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
    """invoke() must call proc.terminate() when the handler returns status=error.

    Verifies the fix: the worker subprocess is terminated rather than silently
    orphaned when _read_response() receives {"status": "error"}.
    """
    worker = Worker("test-fn", _config(), b"ignored-zip")

    proc = MagicMock()
    proc.poll.return_value = None  # process appears alive when invoke() checks it

    error_line = json.dumps({"status": "error", "error": "handler blew up"}) + "\n"
    proc.stdout.readline.side_effect = [error_line]
    proc.stdin = MagicMock()
    proc.stderr = iter([])

    # Pre-set _proc so invoke() skips _spawn() entirely
    worker._proc = proc

    result = worker.invoke({"key": "val"}, request_id="req-001")

    assert result["status"] == "error", "invoke() should surface the error status"
    proc.terminate.assert_called_once_with()
    assert worker._proc is None, "_proc must be cleared after an error response"
