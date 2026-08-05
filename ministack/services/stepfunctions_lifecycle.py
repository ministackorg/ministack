"""Step Functions synchronous-action admission and reset lifecycle."""

import asyncio

from ministack.core.concurrency import LoopLocal


class SyncActionAdmissionClosed(RuntimeError):
    """Raised when synchronous Step Functions work starts during reset."""


class _SyncActionLifecycle:
    """Track dedicated-thread actions and close their admission during reset."""

    def __init__(self):
        self._accepting = True
        self._active_actions = 0
        self._actions_drained = asyncio.Event()
        self._actions_drained.set()

    def enter_action(self):
        """Synchronously register an action before its thread starts."""
        if not self._accepting:
            raise SyncActionAdmissionClosed(
                "Step Functions synchronous-action admission is closed for reset"
            )
        self._active_actions += 1
        if self._active_actions == 1:
            self._actions_drained.clear()

    def leave_action(self):
        """Release an action only after its dedicated thread has finished."""
        self._active_actions -= 1
        if self._active_actions == 0:
            self._actions_drained.set()

    async def begin_reset(self):
        """Close admission and await every active dedicated-thread action."""
        self._accepting = False
        waiter = asyncio.create_task(self._actions_drained.wait())
        try:
            await asyncio.shield(waiter)
        except asyncio.CancelledError as cancellation:
            while not waiter.done():
                try:
                    await asyncio.shield(waiter)
                except asyncio.CancelledError:
                    continue
            raise cancellation

    def finish_reset(self):
        self._accepting = True


_sync_action_lifecycles = LoopLocal(_SyncActionLifecycle)


def _get_sync_action_lifecycle() -> _SyncActionLifecycle:
    return _sync_action_lifecycles.get()
