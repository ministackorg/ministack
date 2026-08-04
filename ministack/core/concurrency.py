"""Concurrency helpers shared by service routers."""

import asyncio
import contextvars
import threading
import weakref


class LoopLocal:
    """Lazily create weakly cached values for the running event loop.

    Both keys and values must be weak: asyncio synchronization primitives bind
    themselves to a loop after contention, so a strong value would keep its
    weak-key loop alive through that back-reference.
    """

    def __init__(self, factory):
        self._factory = factory
        self._values = weakref.WeakKeyDictionary()

    def get(self):
        loop = asyncio.get_running_loop()
        value_ref = self._values.get(loop)
        value = value_ref() if value_ref is not None else None
        if value is None:
            value = self._factory()
            self._values[loop] = weakref.ref(value)
        return value


async def run_in_thread_to_completion(func, *args, **kwargs):
    """Run blocking work off-loop while deferring cancellation until it ends.

    ``asyncio.to_thread`` workers cannot be cancelled once running. Letting the
    awaiting task unwind immediately would release admission/reset guards while
    the worker still mutates service state. Shield the worker, absorb repeated
    cancellation while it finishes, then re-raise the original cancellation.
    """
    worker = asyncio.create_task(asyncio.to_thread(func, *args, **kwargs))
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError as cancellation:
        while not worker.done():
            try:
                await asyncio.shield(worker)
            except asyncio.CancelledError:
                continue
            except Exception:
                break

        # Retrieve a failed worker's exception so asyncio does not report an
        # unobserved task failure after the HTTP request has been cancelled.
        if worker.done() and not worker.cancelled():
            try:
                worker.result()
            except Exception:
                pass
        raise cancellation


def _complete_thread_future(future, result=None, error=None):
    if future.done():
        return
    if error is not None:
        future.set_exception(error)
    else:
        future.set_result(result)


async def run_in_dedicated_thread_to_completion(
    func, *args, thread_name=None, context_setup=None
):
    """Run blocking work in its own context-preserving daemon thread.

    This has the same cancellation contract as ``run_in_thread_to_completion``
    without consuming the shared executor. ``context_setup`` runs only in the
    copied worker context before ``func`` and can install per-operation loop
    handles without retaining them globally.
    """
    loop = asyncio.get_running_loop()
    result_future = loop.create_future()
    ctx_snapshot = contextvars.copy_context()

    def run():
        try:
            if context_setup is not None:
                ctx_snapshot.run(context_setup)
            result = ctx_snapshot.run(func, *args)
        except BaseException as exc:
            loop.call_soon_threadsafe(
                _complete_thread_future,
                result_future,
                None,
                exc,
            )
        else:
            loop.call_soon_threadsafe(
                _complete_thread_future,
                result_future,
                result,
                None,
            )

    threading.Thread(
        target=run,
        name=thread_name,
        daemon=True,
    ).start()

    try:
        return await asyncio.shield(result_future)
    except asyncio.CancelledError as cancellation:
        while not result_future.done():
            try:
                await asyncio.shield(result_future)
            except asyncio.CancelledError:
                continue
            except Exception:
                break

        if result_future.done() and not result_future.cancelled():
            try:
                result_future.result()
            except Exception:
                pass
        raise cancellation
