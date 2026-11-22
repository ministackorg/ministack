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
import os
import shutil
import time
import zipfile
from unittest.mock import MagicMock, mock_open, patch

import pytest

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


def _node_worker(code, extra_files=None):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("index.js", code)
        for path, contents in (extra_files or {}).items():
            archive.writestr(path, contents)
    config = _config()
    config["Runtime"] = "nodejs20.x"
    return Worker("test-fn", config, buf.getvalue())


def _spawn_proc():
    """Minimal Popen mock sufficient for one _spawn() call."""
    proc = MagicMock()
    # The log: an empty iterator, so the daemon thread exits immediately.
    proc.stdout = iter([])
    proc.poll.return_value = None
    return proc


def _fake_open(file, *args, **kwargs):
    """Files that _spawn() writes are mocks; its reply pipe holds one ready line."""
    if not isinstance(file, int):
        return mock_open()()
    os.close(file)
    return io.StringIO(json.dumps({"status": "ready"}) + "\n")


def _protocol_line(status, **fields):
    return json.dumps({"status": status, **fields}) + "\n"


def _mock_worker(stdout_lines):
    worker = Worker("test-fn", _config(), b"ignored-zip")
    proc = MagicMock()
    proc.poll.return_value = None
    proc.stdin = MagicMock()
    worker._proc = proc
    worker._responses = io.StringIO("".join(stdout_lines))
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
        patch("builtins.open", side_effect=_fake_open),
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


def test_invoke_still_surfaces_protocol_error():
    """Protocol error lines must still fail the invocation."""
    error_line = _protocol_line("error", error="boom")
    result, proc, _ = _invoke_worker([error_line])

    assert result["status"] == "error"
    assert result["error"] == "boom"
    proc.terminate.assert_called_once_with()


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not installed")
def test_node_worker_resolves_hyphenated_sdk_client_stub():
    worker = _node_worker(
        """
const sdk = require("@aws-sdk/client-secrets-manager");
exports.handler = async () => ({
  client: typeof sdk.SecretsManagerClient,
  command: typeof sdk.GetSecretValueCommand,
});
"""
    )
    try:
        result = worker.invoke({}, request_id="sdk-stub")
    finally:
        worker.kill()

    assert result["status"] == "ok"
    assert result["result"] == {"client": "function", "command": "function"}


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not installed")
def test_node_worker_resolves_expected_sdk_client_stubs():
    worker = _node_worker(
        """
const packages = [
  "@aws-sdk/client-secrets-manager",
  "@aws-sdk/client-dynamodb",
  "@aws-sdk/client-sqs",
  "@aws-sdk/client-cloudwatch-logs",
  "@aws-sdk/client-eventbridge",
  "@aws-sdk/client-cognito-identity-provider",
  "@aws-sdk/client-dynamodb-streams",
  "@aws-sdk/client-events",
  "@aws-sdk/client-logs",
];
exports.handler = async () => Object.fromEntries(
  packages.map((id) => [id, typeof require(id).ContractClient])
);
"""
    )
    try:
        result = worker.invoke({}, request_id="sdk-stub-contract")
    finally:
        worker.kill()

    assert result["status"] == "ok"
    assert result["result"] == {
        "@aws-sdk/client-secrets-manager": "function",
        "@aws-sdk/client-dynamodb": "function",
        "@aws-sdk/client-sqs": "function",
        "@aws-sdk/client-cloudwatch-logs": "function",
        "@aws-sdk/client-eventbridge": "function",
        "@aws-sdk/client-cognito-identity-provider": "function",
        "@aws-sdk/client-dynamodb-streams": "function",
        "@aws-sdk/client-events": "function",
        "@aws-sdk/client-logs": "function",
    }


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not installed")
def test_node_worker_prefers_bundled_sdk_client_module():
    worker = _node_worker(
        """
const sdk = require("@aws-sdk/client-custom");
exports.handler = async () => ({ source: sdk.source });
""",
        {
            "node_modules/@aws-sdk/client-custom/index.js": (
                'module.exports = { source: "bundle" };\n'
            )
        },
    )
    try:
        result = worker.invoke({}, request_id="bundled-sdk")
    finally:
        worker.kill()

    assert result["status"] == "ok"
    assert result["result"] == {"source": "bundle"}


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not installed")
def test_node_worker_reports_missing_sdk_client_stub():
    worker = _node_worker(
        """
require("@aws-sdk/client-no-local-stub");
exports.handler = async () => ({ ok: true });
"""
    )
    try:
        with pytest.raises(RuntimeError) as exc:
            worker.invoke({}, request_id="missing-sdk")
    finally:
        worker.kill()

    message = str(exc.value)
    assert "@aws-sdk/client-no-local-stub" in message
    assert "LAMBDA_EXECUTOR=docker" in message
    assert "Cannot find module" not in message


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not installed")
def test_node_worker_cwd_is_code_dir():
    """process.cwd() must be the extracted code root, as on AWS (LAMBDA_TASK_ROOT)."""
    worker = _node_worker(
        "exports.handler = async () => ({ cwd: process.cwd(), root: process.env.LAMBDA_TASK_ROOT });"
    )
    try:
        result = worker.invoke({}, request_id="cwd-request-id")
    finally:
        worker.kill()

    assert result["status"] == "ok", result
    # macOS resolves /var to /private/var in process.cwd().
    assert os.path.realpath(result["result"]["cwd"]) == os.path.realpath(result["result"]["root"])


def test_python_worker_cwd_is_code_dir():
    """os.getcwd() must be the extracted code root, as on AWS (LAMBDA_TASK_ROOT)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("index.py", "import os\n\ndef handler(event, context):\n"
                                     "    return {'cwd': os.getcwd(), 'root': os.environ['LAMBDA_TASK_ROOT']}\n")
    worker = Worker("test-fn", _config(), buf.getvalue())
    try:
        result = worker.invoke({}, request_id="cwd-request-id")
    finally:
        worker.kill()

    assert result["status"] == "ok", result
    assert os.path.realpath(result["result"]["cwd"]) == os.path.realpath(result["result"]["root"])
    assert result["result"]["cwd"] == result["result"]["root"]



def test_python_worker_child_process_output_is_log():
    """A child process that prints 300 lines at init and in the handler (`aws s3 sync`
    in the CDK BucketDeployment handler) writes to the log, not into the replies."""
    code = (
        "import subprocess, sys\n"
        "def burst():\n"
        "    subprocess.run([sys.executable, '-c', 'for i in range(300): print(\"line\", i)'], check=True)\n"
        "burst()\n"
        "def handler(event, context):\n"
        "    burst()\n"
        "    return {'ok': True}\n"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("index.py", code)
    worker = Worker("test-fn", _config(), buf.getvalue())
    try:
        result = worker.invoke({}, request_id="child-output")
    finally:
        worker.kill()
    assert result["status"] == "ok", result
    assert result["result"] == {"ok": True}
    assert "line 299" in result["log"]


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not installed")
def test_node_worker_child_process_output_is_log():
    worker = _node_worker(
        """
const burst = () => require("child_process").spawnSync(
  process.execPath, ["-e", "for (let i = 0; i < 300; i++) console.log('line', i)"], { stdio: "inherit" });
burst();
exports.handler = async () => { burst(); return { ok: true }; };
"""
    )
    try:
        result = worker.invoke({}, request_id="child-output")
    finally:
        worker.kill()
    assert result["status"] == "ok", result
    assert result["result"] == {"ok": True}
    assert "line 299" in result["log"]


@pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not installed")
def test_node_worker_crash_with_a_detached_child_is_an_exit():
    worker = _node_worker(
        """
const { spawn } = require("child_process");
exports.handler = async () => {
  spawn(process.execPath, ["-e", "setTimeout(() => {}, 20000)"], { detached: true, stdio: "ignore" }).unref();
  process.exit(3);
};
"""
    )
    worker.config["Timeout"] = 15
    started = time.monotonic()
    try:
        result = worker.invoke({}, request_id="crash")
    finally:
        worker.kill()
    assert time.monotonic() - started < 10
    assert result.get("error_type") == "Runtime.ExitError", result
