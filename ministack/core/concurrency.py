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

    ContextVars propagate as ``asyncio.to_thread`` would. A cancelled awaiter
    (client disconnected mid-call) does not stop the thread — it runs to
    completion so that resource release, warm-worker return and container
    recycle still happen — but the awaiter unwinds immediately rather than
    blocking on work whose result nobody wants.
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
    return await future


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
    thread.start()
    return thread
