"""
Unit tests for the warm ``provided.*`` execution environment
(``ministack.core.lambda_runtime.ProvidedWorker``).

Every test drives a *real* bootstrap: a stdlib-only Python script that speaks
the Lambda Runtime API over ``AWS_LAMBDA_RUNTIME_API`` exactly as a Go or Rust
binary would. No Go toolchain, no Docker and no running MiniStack are needed.

Coverage:
  * environment reuse across invocations, with per-invocation trace context
  * the caller's event is never mutated to carry the trace ID
  * continuous draining of stdout *and* stderr (a pipe that fills would
    otherwise wedge the bootstrap), plus the bounded log buffer
  * a crashed bootstrap is replaced, and its server/socket/threads go with it
  * init errors and instant exits fail fast instead of burning the init timeout
  * a timed-out invocation tears the environment down and the next one recovers
  * stale / duplicate / cross-generation request IDs are rejected (400)
  * teardown leaves no process, listening socket or thread behind
"""

import socket
import sys
import threading
import time
import zipfile
from io import BytesIO

import pytest

from ministack.core import lambda_runtime
from ministack.core.lambda_runtime import (
    ProvidedWorker,
    acquire_worker,
    kill_workers,
    release_worker,
    reset,
)

# ---------------------------------------------------------------------------
# Bootstrap fixtures (stdlib only, run by the host Python interpreter)
# ---------------------------------------------------------------------------

_PREAMBLE = '''#!{python}
import json, os, sys, time, urllib.request, urllib.error

API = os.environ["AWS_LAMBDA_RUNTIME_API"]
PORT = int(API.rsplit(":", 1)[1])


def _call(path, data=None, method=None):
    url = "http://%s/2018-06-01%s" % (API, path)
    req = urllib.request.Request(url, data=data, method=method)
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        return resp.getcode(), resp.headers, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers, exc.read()


def next_invocation():
    code, headers, body = _call("/runtime/invocation/next")
    if code != 200:
        raise SystemExit(0)
    return headers, json.loads(body or b"null")


def post_response(rid, payload):
    return _call("/runtime/invocation/%s/response" % rid,
                 data=json.dumps(payload).encode(), method="POST")[0]


def post_error(rid, payload):
    return _call("/runtime/invocation/%s/error" % rid,
                 data=json.dumps(payload).encode(), method="POST")[0]


def post_init_error(payload):
    return _call("/runtime/init/error",
                 data=json.dumps(payload).encode(), method="POST")[0]
'''


def _bootstrap(body: str) -> bytes:
    """Zip a bootstrap script whose body follows the Runtime API preamble."""
    script = _PREAMBLE.format(python=sys.executable) + "\n" + body
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("bootstrap", script)
    return buf.getvalue()


ECHO_BOOTSTRAP = _bootstrap('''
n = 0
while True:
    headers, event = next_invocation()
    n += 1
    rid = headers["Lambda-Runtime-Aws-Request-Id"]
    post_response(rid, {
        "pid": os.getpid(),
        "n": n,
        "port": PORT,
        "event": event,
        "request_id": rid,
        "trace": headers.get("Lambda-Runtime-Trace-Id"),
        "arn": headers.get("Lambda-Runtime-Invoked-Function-Arn"),
        "deadline": headers.get("Lambda-Runtime-Deadline-Ms"),
    })
''')


def _config(**overrides):
    config = {
        "Runtime": "provided.al2023",
        "Handler": "bootstrap",
        "FunctionName": "provided-fn",
        "FunctionArn": "arn:aws:lambda:us-east-1:123456789012:function:provided-fn",
        "Timeout": 10,
        "MemorySize": 128,
    }
    config.update(overrides)
    return config


@pytest.fixture(autouse=True)
def isolated_runtime(monkeypatch, tmp_path):
    from ministack.services import lambda_svc

    monkeypatch.setattr(lambda_runtime, "_workers", {})
    monkeypatch.setattr(lambda_svc, "_PROVIDED_CODE_CACHE", str(tmp_path / "code"))
    monkeypatch.setattr(lambda_svc, "_provided_code_dirs", {})
    yield
    reset()


@pytest.fixture
def worker_factory():
    """Build ProvidedWorkers and guarantee they are torn down."""
    built: list = []

    def _make(code_zip: bytes, **config_overrides) -> ProvidedWorker:
        worker = ProvidedWorker("provided-fn", _config(**config_overrides), code_zip)
        built.append(worker)
        return worker

    yield _make
    kill_workers(built)
    reset()


def _port_is_closed(port: int) -> bool:
    for _ in range(50):
        sock = socket.socket()
        sock.settimeout(0.5)
        try:
            sock.connect(("127.0.0.1", port))
        except OSError:
            return True
        finally:
            sock.close()
        time.sleep(0.05)
    return False


# ---------------------------------------------------------------------------
# Reuse + per-invocation trace context
# ---------------------------------------------------------------------------


def test_environment_is_reused_and_trace_is_per_invocation(worker_factory):
    """Second invocation lands on the same process, with its own trace ID."""
    worker = worker_factory(ECHO_BOOTSTRAP)

    first = worker.invoke({"k": 1}, "req-1", trace_id="Root=1-aaa;Sampled=1")
    second = worker.invoke({"k": 2}, "req-2", trace_id="Root=1-bbb;Sampled=1")
    third = worker.invoke({"k": 3}, "req-3")

    assert first["status"] == "ok" and second["status"] == "ok"
    assert first["cold_start"] is True
    assert second["cold_start"] is False and third["cold_start"] is False

    assert first["result"]["pid"] == second["result"]["pid"] == third["result"]["pid"]
    assert [r["result"]["n"] for r in (first, second, third)] == [1, 2, 3]

    # Request IDs and trace IDs are per invocation, never carried over.
    assert first["result"]["request_id"] == "req-1"
    assert second["result"]["request_id"] == "req-2"
    assert first["result"]["trace"] == "Root=1-aaa;Sampled=1"
    assert second["result"]["trace"] == "Root=1-bbb;Sampled=1"
    assert third["result"]["trace"] is None
    assert first["result"]["arn"] == worker.config["FunctionArn"]


def test_trace_id_does_not_touch_the_event_payload(worker_factory):
    """The event is the caller's; trace context rides the Runtime API header.

    A user event that happens to contain the key the old implementation
    reserved must reach the handler untouched.
    """
    worker = worker_factory(ECHO_BOOTSTRAP)
    event = {"_x_amzn_trace_id": "user-supplied", "hello": "world"}

    result = worker.invoke(event, "req-1", trace_id="Root=1-ccc")

    assert result["status"] == "ok"
    # Delivered verbatim ...
    assert result["result"]["event"] == {
        "_x_amzn_trace_id": "user-supplied", "hello": "world"}
    # ... and the header carries the real trace ID.
    assert result["result"]["trace"] == "Root=1-ccc"
    # The caller's dict was not mutated.
    assert event == {"_x_amzn_trace_id": "user-supplied", "hello": "world"}


# ---------------------------------------------------------------------------
# Logs: both pipes drained continuously, buffer bounded
# ---------------------------------------------------------------------------


CHATTY_BOOTSTRAP = _bootstrap('''
LINE = "x" * 120
while True:
    headers, event = next_invocation()
    rid = headers["Lambda-Runtime-Aws-Request-Id"]
    count = event["lines"]
    for i in range(count):
        sys.stdout.write("OUT-%d %s\\n" % (i, LINE))
        sys.stderr.write("ERR-%d %s\\n" % (i, LINE))
    sys.stdout.flush()
    sys.stderr.flush()
    time.sleep(0.2)
    post_response(rid, {"logged": count})
''')


def test_large_log_volume_does_not_wedge_the_bootstrap(worker_factory):
    """~500KB across both pipes: an undrained pipe would block the child.

    stdout is drained as logs too — a custom runtime writes there — and both
    streams are merged into one log buffer.
    """
    worker = worker_factory(CHATTY_BOOTSTRAP, Timeout=30)

    result = worker.invoke({"lines": 2000}, "req-1")

    assert result["status"] == "ok"
    assert result["result"] == {"logged": 2000}
    log = result["log"]
    assert "OUT-0 " in log and "ERR-0 " in log
    assert "OUT-1999 " in log and "ERR-1999 " in log

    # And the environment survives it.
    again = worker.invoke({"lines": 1}, "req-2")
    assert again["status"] == "ok"
    assert again["cold_start"] is False


def test_log_buffer_is_bounded_and_keeps_the_newest_lines(worker_factory, monkeypatch):
    monkeypatch.setattr(lambda_runtime, "_PROVIDED_LOG_MAX_LINES", 50)
    worker = worker_factory(CHATTY_BOOTSTRAP, Timeout=30)

    result = worker.invoke({"lines": 400}, "req-1")

    assert result["status"] == "ok"
    lines = [line for line in result["log"].splitlines() if line]
    assert len(lines) <= 50
    # Oldest dropped, newest kept.
    assert "OUT-0 " not in result["log"]
    assert any(line.startswith("ERR-399 ") or line.startswith("OUT-399 ")
               for line in lines)


# ---------------------------------------------------------------------------
# Crash / restart
# ---------------------------------------------------------------------------


CRASH_BOOTSTRAP = _bootstrap('''
headers, event = next_invocation()
rid = headers["Lambda-Runtime-Aws-Request-Id"]
post_response(rid, {"pid": os.getpid(), "port": PORT})
sys.stderr.write("bye\\n")
sys.stderr.flush()
os._exit(9)
''')


def test_crashed_bootstrap_is_replaced_without_leaking_its_server(worker_factory):
    worker = worker_factory(CRASH_BOOTSTRAP)
    threads_before = threading.active_count()

    first = worker.invoke({}, "req-1")
    assert first["status"] == "ok"
    first_port = first["result"]["port"]
    first_pid = first["result"]["pid"]

    # The bootstrap exits right after responding; the next invocation gets a
    # brand new environment, and the dead one's HTTP server is gone.
    second = worker.invoke({}, "req-2")
    assert second["status"] == "ok"
    assert second["cold_start"] is True
    assert second["result"]["pid"] != first_pid
    assert second["result"]["port"] != first_port
    assert _port_is_closed(first_port), "leaked Runtime API server from dead generation"

    worker.kill()
    assert _port_is_closed(second["result"]["port"])
    # Two generations, no accumulated threads.
    for _ in range(50):
        if threading.active_count() <= threads_before + 1:
            break
        time.sleep(0.1)
    assert threading.active_count() <= threads_before + 1


# ---------------------------------------------------------------------------
# Init failures are detected promptly
# ---------------------------------------------------------------------------


INIT_ERROR_BOOTSTRAP = _bootstrap('''
sys.stderr.write("init blew up\\n")
sys.stderr.flush()
post_init_error({"errorMessage": "cannot load config", "errorType": "Init.Error"})
sys.exit(1)
''')

INSTANT_EXIT_BOOTSTRAP = _bootstrap('''
sys.stderr.write("missing shared library\\n")
sys.stderr.flush()
sys.exit(3)
''')


def test_init_error_fails_fast_and_leaves_nothing_running(worker_factory):
    worker = worker_factory(INIT_ERROR_BOOTSTRAP)

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="init error"):
        worker.invoke({}, "req-1")
    elapsed = time.monotonic() - started

    assert elapsed < lambda_runtime._PROVIDED_INIT_TIMEOUT / 2
    assert worker._proc is None
    assert worker._server is None
    assert worker._server_thread is None


def test_bootstrap_that_exits_immediately_fails_fast(worker_factory):
    worker = worker_factory(INSTANT_EXIT_BOOTSTRAP)

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="exited during init with code 3"):
        worker.invoke({}, "req-1")
    elapsed = time.monotonic() - started

    assert elapsed < lambda_runtime._PROVIDED_INIT_TIMEOUT / 2
    assert worker._proc is None and worker._server is None


def test_init_timeout_is_bounded(worker_factory, monkeypatch):
    """A binary that starts but never polls /next is given up on."""
    monkeypatch.setattr(lambda_runtime, "_PROVIDED_INIT_TIMEOUT", 1.0)
    silent = _bootstrap('time.sleep(60)\n')
    worker = worker_factory(silent)

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="did not reach the Runtime API"):
        worker.invoke({}, "req-1")
    elapsed = time.monotonic() - started

    assert 1.0 <= elapsed < 8.0
    assert worker._proc is None and worker._server is None


# ---------------------------------------------------------------------------
# Timeout + recovery
# ---------------------------------------------------------------------------


SLOW_ONCE_BOOTSTRAP_TEMPLATE = '''
MARKER = {marker!r}
while True:
    headers, event = next_invocation()
    rid = headers["Lambda-Runtime-Aws-Request-Id"]
    if not os.path.exists(MARKER):
        open(MARKER, "w").close()
        sys.stderr.write("going to sleep\\n")
        sys.stderr.flush()
        time.sleep(30)
    post_response(rid, {{"pid": os.getpid(), "port": PORT}})
'''


def test_timeout_kills_environment_and_next_invocation_recovers(worker_factory, tmp_path):
    marker = str(tmp_path / "slept")
    worker = worker_factory(
        _bootstrap(SLOW_ONCE_BOOTSTRAP_TEMPLATE.format(marker=marker)),
        Timeout=1,
    )

    started = time.monotonic()
    timed_out = worker.invoke({}, "req-1")
    elapsed = time.monotonic() - started

    assert timed_out["status"] == "error"
    assert timed_out["error"] == "Task timed out after 1.00 seconds"
    assert "going to sleep" in timed_out["log"]
    assert elapsed < 8.0
    assert worker._proc is None and worker._server is None

    # A fresh environment serves the next invocation, and the marker makes the
    # replacement take the fast path.
    recovered = worker.invoke({}, "req-2")
    assert recovered["status"] == "ok"
    assert recovered["cold_start"] is True


# ---------------------------------------------------------------------------
# Request-ID validation and stale-response isolation
# ---------------------------------------------------------------------------


BAD_REQUEST_ID_BOOTSTRAP = _bootstrap('''
while True:
    headers, event = next_invocation()
    rid = headers["Lambda-Runtime-Aws-Request-Id"]
    codes = {
        "bogus": post_response("not-a-real-request-id", {"stolen": True}),
        "empty": post_error("", {"errorMessage": "nope"}),
    }
    codes["real"] = post_response(rid, {"codes": codes, "request_id": rid})
    if codes["real"] != 202:
        post_error(rid, {"errorMessage": "response rejected: %s" % codes})
''')


def test_unknown_request_ids_are_rejected_not_delivered(worker_factory):
    worker = worker_factory(BAD_REQUEST_ID_BOOTSTRAP)

    result = worker.invoke({}, "req-1")

    assert result["status"] == "ok"
    # The forged IDs got 400s; only the real one was accepted.
    assert result["result"]["codes"] == {"bogus": 400, "empty": 404}
    assert result["result"]["request_id"] == "req-1"


DUPLICATE_RESPONSE_BOOTSTRAP = _bootstrap('''
while True:
    headers, event = next_invocation()
    rid = headers["Lambda-Runtime-Aws-Request-Id"]
    first = post_response(rid, {"which": "first"})
    second = post_response(rid, {"which": "second", "first_code": first})
    sys.stderr.write("dup-code=%s\\n" % second)
    sys.stderr.flush()
''')


def test_duplicate_response_for_same_request_id_is_rejected(worker_factory):
    worker = worker_factory(DUPLICATE_RESPONSE_BOOTSTRAP)

    first = worker.invoke({}, "req-1")
    assert first["status"] == "ok"
    assert first["result"] == {"which": "first"}

    # The duplicate got a 400 and, crucially, did not become the *next*
    # invocation's result.
    second = worker.invoke({}, "req-2")
    assert second["status"] == "ok"
    assert second["result"] == {"which": "first"}
    assert "dup-code=400" in "\n".join([first["log"], second["log"]])


def test_results_from_a_dead_generation_are_ignored():
    """A late POST from a torn-down environment cannot satisfy its successor."""
    worker = ProvidedWorker("provided-fn", _config(), b"")
    worker._generation = 7
    worker._current_request_id = "req-live"

    # Same request ID, previous generation.
    assert worker._record_result(6, "req-live", "response", {"stale": True}) is False
    assert worker._response_ready.is_set() is False
    assert worker._result == {}

    # Wrong request ID, current generation.
    assert worker._record_result(7, "req-old", "response", {"stale": True}) is False
    assert worker._response_ready.is_set() is False

    # The live one is accepted exactly once.
    assert worker._record_result(7, "req-live", "response", {"ok": True}) is True
    assert worker._result == {"response": {"ok": True}}
    assert worker._record_result(7, "req-live", "response", {"again": True}) is False
    assert worker._result == {"response": {"ok": True}}


def test_init_error_from_a_dead_generation_is_ignored():
    worker = ProvidedWorker("provided-fn", _config(), b"")
    worker._generation = 3

    worker._record_init_error(2, {"errorMessage": "stale"})
    assert worker._init_error is None
    assert worker._init_settled.is_set() is False

    worker._record_init_error(3, {"errorMessage": "live"})
    assert worker._init_error == {"errorMessage": "live"}
    assert worker._init_settled.is_set() is True


# ---------------------------------------------------------------------------
# Cleanup / pool contract
# ---------------------------------------------------------------------------


def test_kill_reaps_process_server_and_threads(worker_factory):
    worker = worker_factory(ECHO_BOOTSTRAP)
    threads_before = threading.active_count()

    result = worker.invoke({}, "req-1")
    port = result["result"]["port"]
    proc = worker._proc
    log_thread = worker._log_thread
    assert proc.poll() is None

    worker.kill()

    assert proc.poll() is not None, "bootstrap process not reaped"
    assert worker._proc is None
    assert worker._server is None and worker._server_thread is None
    assert worker._log_thread is None
    assert _port_is_closed(port), "Runtime API socket still listening after kill"
    log_thread.join(timeout=2.0)
    assert not log_thread.is_alive()
    for _ in range(50):
        if threading.active_count() <= threads_before:
            break
        time.sleep(0.1)
    assert threading.active_count() <= threads_before


def test_pool_contract_lease_reuse_and_reset():
    """provided.* functions use the same acquire/release pool as the rest."""
    config = _config()
    try:
        worker, reason = acquire_worker("provided-fn", config, ECHO_BOOTSTRAP)
        assert isinstance(worker, ProvidedWorker)
        assert reason == "spawn"
        assert worker.in_use is True

        first = worker.invoke({}, "req-1")
        assert first["status"] == "ok"
        port = first["result"]["port"]

        # A second lease while the first is held gets a *separate* environment.
        other, other_reason = acquire_worker("provided-fn", config, ECHO_BOOTSTRAP)
        assert other is not worker and other_reason == "spawn"
        release_worker(other)

        release_worker(worker)
        again, reason = acquire_worker("provided-fn", config, ECHO_BOOTSTRAP)
        assert reason == "reused"
        assert again is worker
        assert again.invoke({}, "req-2")["result"]["port"] == port
        release_worker(again)
    finally:
        reset()

    assert _port_is_closed(port)


def test_concurrent_invocations_use_separate_leased_environments():
    """Two leases run at the same time on two processes, results not crossed."""
    config = _config()
    barrier = threading.Barrier(2, timeout=30)
    results: dict = {}

    try:
        workers = []
        for _ in range(2):
            worker, reason = acquire_worker("provided-fn", config, ECHO_BOOTSTRAP)
            assert reason == "spawn"
            workers.append(worker)

        def _run(index, worker):
            barrier.wait()
            results[index] = worker.invoke({"i": index}, f"req-{index}",
                                           trace_id=f"Root=1-{index}")

        threads = [threading.Thread(target=_run, args=(i, w))
                   for i, w in enumerate(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
            assert not thread.is_alive()

        assert set(results) == {0, 1}
        assert all(r["status"] == "ok" for r in results.values())
        assert results[0]["result"]["pid"] != results[1]["result"]["pid"]
        for index, result in results.items():
            assert result["result"]["event"] == {"i": index}
            assert result["result"]["request_id"] == f"req-{index}"
            assert result["result"]["trace"] == f"Root=1-{index}"
        for worker in workers:
            release_worker(worker)
    finally:
        reset()

    for result in results.values():
        assert _port_is_closed(result["result"]["port"])


def test_idle_reaper_and_reset_release_provided_environments():
    config = _config()
    try:
        first, _ = acquire_worker("provided-fn", config, ECHO_BOOTSTRAP)
        second, _ = acquire_worker("provided-fn", config, ECHO_BOOTSTRAP)
        ports = [w.invoke({}, "req-1")["result"]["port"] for w in (first, second)]
        procs = [first._proc, second._proc]
        release_worker(first)
        release_worker(second)

        # ttl=0 reaps every surplus environment; the first per key stays warm.
        assert lambda_runtime.reap_idle_workers(ttl=0) == 1
        assert procs[1].poll() is not None
        assert _port_is_closed(ports[1])
        assert second._server is None
        assert procs[0].poll() is None

        reset()
        assert procs[0].poll() is not None
        assert _port_is_closed(ports[0])
        assert first._server is None and first._log_thread is None
    finally:
        reset()


def test_invoke_signature_keeps_trace_out_of_the_positional_contract():
    """``trace_id`` is keyword-only, so no caller can pass it as the event."""
    import inspect

    sig = inspect.signature(ProvidedWorker.invoke)
    assert list(sig.parameters) == ["self", "event", "request_id", "trace_id"]
    assert sig.parameters["trace_id"].kind is inspect.Parameter.KEYWORD_ONLY
    assert sig.parameters["trace_id"].default is None


def test_json_only_bootstrap_output_is_not_parsed_as_protocol(worker_factory):
    """stdout is logs, not a protocol channel: JSON there must not confuse us."""
    noisy = _bootstrap('''
while True:
    headers, event = next_invocation()
    rid = headers["Lambda-Runtime-Aws-Request-Id"]
    sys.stdout.write(json.dumps({"status": "ok", "result": "from-stdout"}) + "\\n")
    sys.stdout.flush()
    post_response(rid, {"real": True})
''')
    worker = worker_factory(noisy)

    result = worker.invoke({}, "req-1")

    assert result["status"] == "ok"
    assert result["result"] == {"real": True}
    assert "from-stdout" in result["log"]


def test_handler_error_does_not_poison_warm_environment(worker_factory):
    code = _bootstrap('''
n = 0
while True:
    headers, event = next_invocation()
    n += 1
    rid = headers["Lambda-Runtime-Aws-Request-Id"]
    if event.get("fail"):
        post_error(rid, {"errorMessage": "handler failed", "errorType": "Test.Error"})
    else:
        post_response(rid, {"n": n})
''')
    worker = worker_factory(code)
    failed = worker.invoke({"fail": True}, "req-1")
    assert failed["status"] == "error"
    assert failed["error_payload"]["errorType"] == "Test.Error"
    recovered = worker.invoke({}, "req-2")
    assert recovered["status"] == "ok"
    assert recovered["cold_start"] is False
    assert recovered["result"] == {"n": 2}


def test_exit_during_invocation_fails_without_waiting_for_timeout(worker_factory):
    worker = worker_factory(_bootstrap('''
next_invocation()
sys.stderr.write("crashed during handler\\n")
sys.stderr.flush()
sys.exit(9)
'''), Timeout=30)
    started = time.monotonic()
    result = worker.invoke({}, "req-1")
    assert time.monotonic() - started < 8
    assert result["status"] == "error"
    assert result["error_payload"]["errorType"] == "Runtime.ExitError"
    assert "crashed during handler" in result["log"]
    assert worker._proc is None and worker._server is None


def test_response_recorded_just_after_process_exit_is_preserved(worker_factory, monkeypatch):
    from unittest.mock import Mock

    worker = worker_factory(ECHO_BOOTSTRAP)
    proc = Mock()
    statuses = iter([None, 0])
    proc.poll.side_effect = lambda: next(statuses, 0)
    worker._proc = proc
    ready = Mock()
    waits = iter([False, True])

    def wait(timeout):
        settled = next(waits)
        if settled:
            worker._result = {"response": {"ok": True}}
        return settled

    ready.wait.side_effect = wait
    worker._response_ready = ready
    monkeypatch.setattr(worker, "_drain_stderr_bounded", lambda: "")
    result = worker.invoke({}, "req-1")
    assert result["status"] == "ok"
    assert result["result"] == {"ok": True}
    assert ready.wait.call_count == 2
    assert worker._proc is None


def test_spawn_failure_closes_the_already_started_server(worker_factory, monkeypatch):
    worker = worker_factory(ECHO_BOOTSTRAP)
    resources = []

    def failed_popen(*args, **kwargs):
        resources.extend([worker._server, worker._server_thread])
        raise OSError("cannot execute bootstrap")

    monkeypatch.setattr(lambda_runtime.subprocess, "Popen", failed_popen)
    with pytest.raises(OSError, match="cannot execute bootstrap"):
        worker.invoke({}, "req-1")
    server, thread = resources
    assert server.fileno() == -1
    assert not thread.is_alive()
    assert worker._proc is None and worker._server is None
