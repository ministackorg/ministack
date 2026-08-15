"""Concurrency regression tests for the re-entrancy model.

MiniStack classifies every blocking call by whether it can re-enter the server:
work that can (a Lambda execution, an ECS dispatch, a Step Functions sync
execution) gets a dedicated thread; work that cannot uses a bounded shared pool.
Put re-entrant work on the shared pool and it deadlocks — the nested call queues
behind the call that is waiting for it.

The rest of the suite cannot catch that. Every other test issues one request at
a time, and a misclassified service passes serially and wedges only under
concurrency. These tests fire N callers at once and assert two things:

- every caller completes (no thread-pool starvation)
- the event loop keeps serving during the burst (nothing blocked it)

Kept deliberately small: N is modest so this runs in CI in seconds and stays
well under any account concurrency cap.
"""

import concurrent.futures
import io
import json
import os
import statistics
import threading
import time
import urllib.request
import uuid
import zipfile

import pytest

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
ROLE = "arn:aws:iam::000000000000:role/lambda-role"

# Enough callers to exhaust a misclassified bounded pool without being slow.
N = 12
# The nested-invoke probe has to exceed the shared worker pool (default 64) or
# starvation never shows: below the pool size every caller gets a slot and a
# broken build passes. Verified — at N=12 this test passes against 1.4.17,
# which has the bug.
N_NESTED = 70
# A wedged server fails these by timing out, so the bar only has to separate
# "served promptly" from "blocked for seconds".
MAX_LOOP_STALL_MS = 5000


def _zip(src: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("index.py", src)
    return buf.getvalue()


def _make_lambda(lam, src: str, timeout: int = 30) -> str:
    name = f"conc-{uuid.uuid4().hex[:10]}"
    lam.create_function(
        FunctionName=name, Runtime="python3.11", Role=ROLE, Handler="index.handler",
        Code={"ZipFile": _zip(src)}, Timeout=timeout,
    )
    return name


class LoopProbe:
    """Polls /_ministack/health throughout a burst to detect a blocked loop."""

    def __init__(self):
        self.samples = []
        self._stop = threading.Event()
        self._thread = None

    def __enter__(self):
        def poll():
            while not self._stop.is_set():
                t0 = time.perf_counter()
                try:
                    urllib.request.urlopen(f"{ENDPOINT}/_ministack/health", timeout=15).read()
                    self.samples.append((time.perf_counter() - t0) * 1000)
                except Exception:
                    self.samples.append(None)
                time.sleep(0.1)

        self._thread = threading.Thread(target=poll, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def assert_responsive(self, what: str):
        served = [s for s in self.samples if s is not None]
        assert served, f"{what}: health never responded — the event loop was blocked"
        worst = max(served)
        assert worst < MAX_LOOP_STALL_MS, (
            f"{what}: event loop stalled {worst:.0f}ms (median "
            f"{statistics.median(served):.0f}ms) — a blocking call is running on the loop"
        )


def _burst(fn, items=None, n=N):
    """Run `fn` concurrently over `items` (or over range(n)); return all results."""
    work = list(items) if items is not None else list(range(n))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(work))) as ex:
        return list(ex.map(fn, work))


# --------------------------------------------------------------------------
# Lambda invoking Lambda — the original starvation case
# --------------------------------------------------------------------------

def test_concurrent_nested_lambda_invocations_do_not_starve(lam):
    """Callers that each invoke another function must never wedge.

    Three details make this discriminate; without any one of them a build with
    the bug still passes:

    - **distinct caller functions.** One function invoked N times serialises on
      its own worker lock and never reaches the shared pool.
    - **a hold before the nested call**, so every caller is in flight at once
      rather than completing before the next starts.
    - **N above the pool size**, since starvation begins at exactly the pool
      bound.

    A throttle is a correct answer and is allowed — the account concurrency cap
    is deliberately below N on small hosts. What must never happen is a caller
    getting no answer at all, which is what starvation looks like.
    """
    callee = _make_lambda(lam, "def handler(event, context):\n    return {'pong': True}\n")
    caller_src = f"""
import json, os, time, urllib.request

def handler(event, context):
    time.sleep(float(event.get("hold", 1.5)))
    ep = os.environ["AWS_ENDPOINT_URL"]
    req = urllib.request.Request(
        ep + "/2015-03-31/functions/{callee}/invocations",
        data=json.dumps({{}}).encode(),
        headers={{"Content-Type": "application/json"}},
    )
    return {{"nested": json.loads(urllib.request.urlopen(req, timeout=25).read().decode())}}
"""
    callers = [_make_lambda(lam, caller_src, timeout=40) for _ in range(N_NESTED)]

    def invoke(name):
        try:
            payload = lam.invoke(FunctionName=name, Payload=json.dumps({"hold": 1.5}).encode())
            body = payload["Payload"].read().decode() or "{}"
            if payload.get("FunctionError"):
                # A timeout is the starvation signature: the nested call never
                # got a slot. Anything else is the handler's own failure.
                return "wedged" if "timed out" in body.lower() else "error"
            return "ok"
        except Exception as exc:
            return "throttled" if "TooManyRequests" in str(exc) else "wedged"

    with LoopProbe() as probe:
        results = _burst(invoke, items=callers)
    probe.assert_responsive("nested lambda burst")

    wedged = results.count("wedged")
    assert wedged == 0, (
        f"{wedged}/{N_NESTED} nested invocations never completed — re-entrant work is "
        f"queueing behind itself (ok={results.count('ok')}, "
        f"throttled={results.count('throttled')}, error={results.count('error')})"
    )
    assert results.count("ok") > 0, "no invocation succeeded at all"


# --------------------------------------------------------------------------
# Step Functions StartSyncExecution — dispatched off the loop
# --------------------------------------------------------------------------

def test_concurrent_sync_executions_do_not_block_the_loop(sfn, sfn_sync):
    """StartSyncExecution runs off the loop; N at once must not stall the server."""
    definition = json.dumps({
        "StartAt": "Wait",
        "States": {"Wait": {"Type": "Wait", "Seconds": 1, "Next": "Done"},
                   "Done": {"Type": "Pass", "End": True}},
    })
    arn = sfn.create_state_machine(
        name=f"conc-sync-{uuid.uuid4().hex[:8]}", definition=definition,
        roleArn="arn:aws:iam::000000000000:role/R",
    )["stateMachineArn"]

    def run(i):
        return sfn_sync.start_sync_execution(
            stateMachineArn=arn, input=json.dumps({"i": i}))["status"]

    with LoopProbe() as probe:
        statuses = _burst(run)
    probe.assert_responsive("sync execution burst")

    assert all(s == "SUCCEEDED" for s in statuses), f"sync executions returned {set(statuses)}"


# --------------------------------------------------------------------------
# API Gateway v2 -> Lambda — proxy integration re-enters the server
# --------------------------------------------------------------------------

def test_concurrent_apigw_lambda_proxy_requests(apigw, lam):
    """N simultaneous HTTP-API requests, each executing a Lambda, must all serve."""
    fn = _make_lambda(lam, (
        "import time\n"
        "def handler(event, context):\n"
        "    time.sleep(0.4)\n"
        "    return {'statusCode': 200, 'body': 'ok'}\n"
    ))
    api = apigw.create_api(Name=f"conc-{uuid.uuid4().hex[:8]}", ProtocolType="HTTP",
                           Target=f"arn:aws:lambda:us-east-1:000000000000:function:{fn}")
    api_id = api["ApiId"]

    def call(_):
        try:
            r = urllib.request.urlopen(f"{ENDPOINT}/_apigw/{api_id}/", timeout=30)
            return r.status
        except urllib.error.HTTPError as e:
            return e.code
        except Exception:
            return None

    with LoopProbe() as probe:
        codes = _burst(call)
    probe.assert_responsive("apigw proxy burst")

    # The route wiring differs by MiniStack version; what this test guards is
    # that the server answered every caller rather than wedging.
    assert all(c is not None for c in codes), (
        f"{codes.count(None)}/{N} API Gateway requests got no response at all — "
        f"the Lambda dispatch wedged"
    )


# --------------------------------------------------------------------------
# The loop must stay live while Docker-backed work runs
# --------------------------------------------------------------------------

@pytest.mark.skipif(
    os.environ.get("MINISTACK_SKIP_DOCKER_TESTS") == "1",
    reason="Docker-backed concurrency probe disabled",
)
def test_ecs_run_task_does_not_block_the_loop(ecs):
    """RunTask talks to the Docker daemon; that must never happen on the loop."""
    cluster = f"conc-{uuid.uuid4().hex[:8]}"
    ecs.create_cluster(clusterName=cluster)
    ecs.register_task_definition(
        family=f"{cluster}-td",
        containerDefinitions=[{"name": "app", "image": "alpine:latest",
                               "command": ["sleep", "3600"], "memory": 64, "essential": True}],
    )
    with LoopProbe() as probe:
        try:
            ecs.run_task(cluster=cluster, taskDefinition=f"{cluster}-td", count=1)
        except Exception as exc:                      # no daemon / image pull refused
            pytest.skip(f"ECS RunTask unavailable in this environment: {exc}")
        finally:
            for arn in ecs.list_tasks(cluster=cluster).get("taskArns", []):
                try:
                    ecs.stop_task(cluster=cluster, task=arn)
                except Exception:
                    pass
            try:
                ecs.delete_cluster(cluster=cluster)
            except Exception:
                pass
    probe.assert_responsive("ECS RunTask")
