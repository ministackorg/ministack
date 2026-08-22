"""Off-loop execution primitives.

MiniStack is a single ASGI process, so every blocking call has to be placed
deliberately. The rule is not "which service is this" but **can this work
re-enter the server**:

- Work that cannot re-enter (an outbound HTTP call to a real endpoint, a
  DuckDB query, a zip extraction) is safe on a bounded shared pool. Use
  :func:`run_offloop`.
- Work that CAN re-enter (anything that runs a Lambda handler, a Docker
  container that talks back to 4566, a CloudFormation custom resource
  blocking on its ResponseURL callback) must never take a slot from a bounded
  pool: the nested request needs a slot of its own, and it queues behind the
  very call waiting on it. Deadlock arrives at exactly the pool size. Use
  :func:`run_reentrant`, which gives each such call its own thread.
- Fire-and-forget work (delivery loops, reapers, background provisioning) uses
  :func:`spawn_background` so it is named, its exceptions are logged, and it is
  visible to :func:`active_counts` instead of being an anonymous
  ``threading.Thread``.

Nothing blocking belongs on the event loop itself. A blocking call there
freezes every service at once, which is strictly worse than exhausting a pool.

Bounding in-flight re-entrant work is a *policy* question that belongs to the
caller (Lambda enforces AWS's concurrency model and throttles), not to this
module: a blocking gate here would re-create the deadlock it exists to remove.

Moving handlers off the loop also removed the serialisation they were silently
relying on. Under the loop, read-modify-write on a service record could not
interleave; off it, every ``if record still exists: publish into it`` became a
race, and a delete landing in the gap leaves a container nothing references. Use
:func:`resource_lock` for those, under one rule: **hold it across a record
mutation, never across a Docker call or a re-entrant dispatch.** A lock held
across either is how #1277's per-service locking deadlocked.
"""

import asyncio
import contextvars
import logging
import threading

logger = logging.getLogger("concurrency")

# Live counters, for diagnostics via /_ministack/health.
_counts_lock = threading.Lock()
_reentrant_threads = 0
_background_threads = 0


def active_counts() -> dict:
    """Snapshot of threads this module currently has running."""
    with _counts_lock:
        return {"reentrant": _reentrant_threads, "background": _background_threads}


_resource_locks: dict = {}
_resource_locks_guard = threading.Lock()


def resource_lock(scope: str, key: str) -> threading.Lock:
    """A lock for one resource — e.g. ``resource_lock("elasticache", cluster_id)``.

    For making a read-modify-write on a service record atomic now that handlers
    no longer run serialised on the event loop. Typical use is a deferred
    provisioner publishing what it started, versus a delete removing the record:

        with resource_lock("elasticache", cluster_id):     # short, no I/O
            rec = _clusters.get(cluster_id)
            stale = rec is not record or rec.get("_deleting")
            if not stale:
                rec["_docker_container_id"] = container_id
        if stale:
            tear_down(container_id)                        # outside the lock

    Two rules keep this safe:

    - **Never hold it across a Docker call or anything that can re-enter
      MiniStack.** That is what makes per-service locking deadlock: a container
      MiniStack starts is handed ``AWS_ENDPOINT_URL`` pointing back at MiniStack,
      so it calls in while the starter still holds the lock.
    - **Compare record identity, not truthiness.** ``if rec:`` cannot tell a
      surviving record from a same-named one created after a delete; ``rec is
      record`` can.

    Locks are kept for the process lifetime. They are one mutex per resource id
    ever seen, which is far cheaper than the bookkeeping to reclaim them safely.
    """
    with _resource_locks_guard:
        return _resource_locks.setdefault((scope, key), threading.Lock())


def _bump(kind: str, delta: int) -> None:
    global _reentrant_threads, _background_threads
    with _counts_lock:
        if kind == "reentrant":
            _reentrant_threads += delta
        else:
            _background_threads += delta


async def run_offloop(fn, *args, **kwargs):
    """Run blocking work that cannot re-enter the server, on the shared pool.

    Thin alias for :func:`asyncio.to_thread` so call sites state their
    classification explicitly and an audit can distinguish "reviewed, cannot
    re-enter" from "nobody thought about it".
    """
    return await asyncio.to_thread(fn, *args, **kwargs)


async def run_reentrant(fn, *args, thread_name: str = "ministack-reentrant"):
    """Run blocking work that may call back into this server, on its own thread.

    A dedicated thread per call, not a pooled worker: nested requests draw from
    the same pool, so a bound of any size only moves the deadlock edge rather
    than removing it.

    ContextVars propagate as ``asyncio.to_thread`` would.

    Cancellation defers to completion. A thread cannot be cancelled once it is
    running, so if a disconnected client let the awaiter unwind immediately, the
    caller would proceed — releasing a lease, taking a reset barrier, tearing
    down request state — while the worker is still mutating that same state. So
    the wait is shielded, repeated cancellation is absorbed until the worker
    finishes, and the original ``CancelledError`` is re-raised afterwards. The
    awaiting task is suspended, not the loop, so this costs nothing but the
    task's own latency.
    """
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    ctx = contextvars.copy_context()

    def _deliver(result, exc):
        if not future.done():
            if exc is not None:
                future.set_exception(exc)
            else:
                future.set_result(result)

    def _post(result, exc):
        try:
            loop.call_soon_threadsafe(_deliver, result, exc)
        except RuntimeError:
            pass  # loop already closed (shutdown) — nobody left to deliver to

    def _run():
        result, exc = None, None
        try:
            result = ctx.run(fn, *args)
        except Exception as e:  # noqa: BLE001 — delivered to the awaiter
            exc = e
        except BaseException as e:
            # SystemExit / KeyboardInterrupt: hand it to the awaiter so the
            # request fails instead of hanging on a future nothing resolves,
            # then let it unwind this thread rather than swallowing it.
            _post(None, e)
            _bump("reentrant", -1)
            raise
        _post(result, exc)
        _bump("reentrant", -1)

    _bump("reentrant", 1)
    try:
        threading.Thread(target=_run, daemon=True, name=thread_name).start()
    except RuntimeError as exc:
        _bump("reentrant", -1)
        # OS thread limit. Fail the request loudly instead of awaiting a future
        # nothing will ever resolve.
        raise RuntimeError(f"cannot start {thread_name} thread: {exc}") from exc

    try:
        return await asyncio.shield(future)
    except asyncio.CancelledError as cancellation:
        while not future.done():
            try:
                await asyncio.shield(future)
            except asyncio.CancelledError:
                continue  # absorb repeated cancellation; the worker is still live
            except Exception:
                break     # the worker failed; that is a completion too
        # Consume a failed worker's exception so asyncio does not report it as
        # never-retrieved after the awaiting request has already gone away.
        if future.done() and not future.cancelled():
            try:
                future.result()
            except Exception:
                pass
        raise cancellation


def spawn_background(fn, *args, thread_name: str = "ministack-background", **kwargs):
    """Start named fire-and-forget work with its exceptions logged.

    Replaces bare ``threading.Thread(target=..., daemon=True).start()`` so that
    background failures surface in the log instead of dying silently, and so
    the thread is countable.
    """
    ctx = contextvars.copy_context()

    def _run():
        try:
            ctx.run(lambda: fn(*args, **kwargs))
        except Exception:
            logger.exception("background task %s failed", thread_name)
        finally:
            _bump("background", -1)

    _bump("background", 1)
    thread = threading.Thread(target=_run, daemon=True, name=thread_name)
    try:
        thread.start()
    except RuntimeError:
        # OS thread limit. `_run` never executes, so its `finally` never runs
        # and the counter would leak — permanently, and precisely when someone
        # is reading it to diagnose thread exhaustion. Callers that can fall
        # back to running inline (firehose's Iceberg delivery) catch this, so
        # it must still propagate. `run_reentrant` does the same.
        _bump("background", -1)
        raise
    return thread
