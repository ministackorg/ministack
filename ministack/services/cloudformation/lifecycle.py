"""CloudFormation stack-task admission and reset lifecycle."""

import asyncio

from ministack.core.concurrency import LoopLocal


class StackTaskAdmissionClosed(RuntimeError):
    """Raised when code tries to spawn stack work during reset."""


class _StackTaskLifecycle:
    """Track stack lifecycle tasks and close their admission during reset."""

    def __init__(self):
        self._accepting = True
        self._tasks = set()

    def create_task(self, coro):
        # Check and insertion are synchronous on the server loop: reset cannot
        # close admission between them.
        if not self._accepting:
            coro.close()
            raise StackTaskAdmissionClosed(
                "CloudFormation stack task admission is closed for reset"
            )
        task = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(task)
        # Keep this lifecycle alive for as long as one of its tasks exists;
        # LoopLocal intentionally stores only weak values for loop collection.
        task.add_done_callback(self._task_finished)
        return task

    def is_accepting(self):
        return self._accepting

    def _task_finished(self, task):
        self._tasks.discard(task)

    async def begin_reset(self):
        """Close admission, cancel active tasks, and await full unwinding."""
        self._accepting = False
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        if not tasks:
            return

        waiter = asyncio.ensure_future(asyncio.gather(*tasks, return_exceptions=True))
        try:
            await asyncio.shield(waiter)
        except asyncio.CancelledError as cancellation:
            # Stack tasks may be inside an uncancellable worker. Repeated reset
            # cancellation must not let the state wipe overtake that worker.
            while not waiter.done():
                try:
                    await asyncio.shield(waiter)
                except asyncio.CancelledError:
                    continue
            raise cancellation

    def finish_reset(self):
        self._accepting = True


_stack_task_lifecycles = LoopLocal(_StackTaskLifecycle)


def _get_stack_task_lifecycle() -> _StackTaskLifecycle:
    return _stack_task_lifecycles.get()
