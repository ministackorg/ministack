# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
CloudFormation WaitCondition / WaitConditionHandle — the signal store.

A ``WaitConditionHandle`` registers a token and returns the URL a signal is
PUT to; a ``WaitCondition`` blocks its stack until ``Count`` SUCCESS signals
arrived, a FAILURE signal arrived, or its timeout passed. A wait condition
with a ``CreationPolicy`` has no handle; its signals come through
``SignalResource``, which delivers to the wait condition that is waiting
under the stack id and logical id. Every valid signal is published to the
stack events, as the CreationPolicy reference documents.
"""

import logging
import re
import threading
import time
from urllib.parse import urlparse

from ministack.core.responses import new_uuid

from .custom_resource import _HOST, _PORT

logger = logging.getLogger("cloudformation")

SIGNAL_PATH = "/_ministack/cfn-signal/"
MAX_TIMEOUT_SECONDS = 43200
_ISO8601_DURATION_RE = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$")

_lock = threading.Lock()
_changed = threading.Condition(_lock)
_generation = 0
# token → {"stack_id": owning stack, "url": bool, "signals": {UniqueId: {"Status", "Reason", "Data"}}}
_handles: dict = {}
# (stack id, logical id) → {"token", "stack_name", "resource_type"}, while that wait condition waits
_waiting: dict = {}
# physical id of a completed wait condition → its attributes (updates are not
# supported on AWS, so an update returns what the create produced)
_results: dict = {}


# --- validation of the template values -------------------------------------


def validate_count(value, logical_id: str) -> int:
    """``Count`` of a WaitCondition or of ``CreationPolicy.ResourceSignal``:
    an integer of at least 1, quoted or not; absent means 1."""
    if value in (None, ""):
        return 1
    try:
        count = int(str(value).strip())
    except (TypeError, ValueError):
        count = 0
    if count < 1:
        raise ValueError(f"WaitCondition {logical_id!r}: Count must be an integer of at least 1")
    return count


def validate_timeout_seconds(value, logical_id: str) -> int:
    """The WaitCondition ``Timeout`` property: seconds, 1 to 43200."""
    try:
        timeout_s = int(str(value).strip())
    except (TypeError, ValueError):
        timeout_s = -1
    if not 0 < timeout_s <= MAX_TIMEOUT_SECONDS:
        raise ValueError(
            f"WaitCondition {logical_id!r}: Timeout must be a number of seconds between 1 and {MAX_TIMEOUT_SECONDS}"
        )
    return timeout_s


def validate_resource_signal_timeout(value, logical_id: str) -> int:
    """``CreationPolicy.ResourceSignal.Timeout``: an ISO 8601 duration of the
    form ``PT#H#M#S``, default PT5M, at least one second, at most 12 hours."""
    if value in (None, ""):
        value = "PT5M"
    match = _ISO8601_DURATION_RE.match(value.strip()) if isinstance(value, str) else None
    if not match or not any(match.groups()):
        raise ValueError(
            f"WaitCondition {logical_id!r}: CreationPolicy ResourceSignal Timeout must be an "
            "ISO 8601 duration of the form PT#H#M#S"
        )
    hours, minutes, seconds = (int(g) if g else 0 for g in match.groups())
    timeout_s = hours * 3600 + minutes * 60 + seconds
    if timeout_s < 1:
        raise ValueError(
            f"WaitCondition {logical_id!r}: CreationPolicy ResourceSignal Timeout must be at least one second"
        )
    if timeout_s > MAX_TIMEOUT_SECONDS:
        raise ValueError(
            f"WaitCondition {logical_id!r}: CreationPolicy ResourceSignal Timeout must be at most 12 hours"
        )
    return timeout_s


# --- handles ------------------------------------------------------------------


def handle_url(token: str) -> str:
    return f"http://{_HOST}:{_PORT}{SIGNAL_PATH}{token}"


def _token_of(url) -> str | None:
    if not isinstance(url, str):
        return None
    path = urlparse(url).path
    if not path.startswith(SIGNAL_PATH):
        return None
    return path[len(SIGNAL_PATH) :] or None


def token_from_url(url) -> str | None:
    """The token of a handle URL this store issued, else None."""
    token = _token_of(url)
    with _lock:
        return token if token in _handles else None


def has_handle(token: str) -> bool:
    with _lock:
        return token in _handles


def handle_owner(token: str) -> str | None:
    with _lock:
        entry = _handles.get(token)
        return entry["stack_id"] if entry else None


def register_slot(stack_id: str, url: bool = False) -> str:
    """A signal slot for a stack. Without ``url`` only SignalResource reaches
    it (the CreationPolicy form) and it is discarded when its wait ends."""
    token = new_uuid()
    with _lock:
        _handles[token] = {"stack_id": stack_id, "url": url, "signals": {}}
    return token


def register_handle(stack_id: str) -> tuple:
    """Create a handle for a stack: ``(URL, token)``. The URL is the handle's
    physical id and ``Ref``, the token its ``Id`` attribute."""
    token = register_slot(stack_id, url=True)
    return handle_url(token), token


def discard_handle(url) -> None:
    token = _token_of(url)
    with _lock:
        _handles.pop(token, None)


# --- signals ------------------------------------------------------------------


def deliver_signal(token: str, payload: dict) -> bool:
    """Store one signal and publish it to the events of the stack that waits
    on the token. Returns False for an unknown token.

    Raises ValueError for a body without a valid ``Status`` or ``UniqueId``.
    A repeated ``UniqueId`` is a retransmission and is ignored, as on AWS.
    """
    status = payload.get("Status") if isinstance(payload, dict) else None
    if status not in ("SUCCESS", "FAILURE"):
        raise ValueError("Status must be SUCCESS or FAILURE")
    unique_id = payload.get("UniqueId")
    if not isinstance(unique_id, str) or not 1 <= len(unique_id) <= 64:
        raise ValueError("UniqueId must be a string of 1 to 64 characters")
    with _changed:
        entry = _handles.get(token)
        if entry is None:
            return False
        if unique_id in entry["signals"]:
            return True
        entry["signals"][unique_id] = {
            "Status": status,
            "Reason": payload.get("Reason", ""),
            "Data": payload.get("Data", ""),
        }
        waiters = [(key, w) for key, w in _waiting.items() if w["token"] == token]
        _changed.notify_all()
    from .stacks import _add_event

    for (stack_id, logical_id), waiter in waiters:
        _add_event(
            stack_id,
            waiter["stack_name"],
            logical_id,
            waiter["resource_type"],
            "CREATE_IN_PROGRESS",
            f"Received {status} signal with UniqueId {unique_id}",
        )
    return True


def signal_resource(stack_id: str, logical_id: str, unique_id: str, status: str) -> bool:
    """SignalResource: deliver to the wait condition currently waiting under
    that stack id and logical id. Returns False when nothing waits there."""
    with _lock:
        waiter = _waiting.get((stack_id, logical_id))
    if waiter is None:
        return False
    return deliver_signal(waiter["token"], {"Status": status, "UniqueId": unique_id})


def wait_for(
    token: str,
    stack_id: str,
    stack_name: str,
    logical_id: str,
    resource_type: str,
    count: int,
    timeout_s: float,
) -> dict:
    """Block until ``count`` SUCCESS signals arrived on ``token``.

    CALLER CONTRACT: must run in a worker thread, never on the event loop; the
    loop has to stay free to receive the signal PUT or the SignalResource call.

    Returns the ``{UniqueId: Data}`` map of the SUCCESS signals.
    Raises RuntimeError on a FAILURE signal or a state reset, TimeoutError
    when the timeout passes first.
    """
    deadline = time.monotonic() + timeout_s
    key = (stack_id, logical_id)
    with _changed:
        generation = _generation
        _waiting[key] = {"token": token, "stack_name": stack_name, "resource_type": resource_type}
        try:
            while True:
                if _generation != generation:
                    raise RuntimeError("WaitCondition aborted: the emulator state was reset")
                signals = _handles.get(token, {}).get("signals", {})
                failed = next((s for s in signals.items() if s[1]["Status"] == "FAILURE"), None)
                if failed is not None:
                    raise RuntimeError(
                        f"WaitCondition received failed message: '{failed[1]['Reason']}' for uniqueId: {failed[0]}"
                    )
                successes = {uid: s["Data"] for uid, s in signals.items() if s["Status"] == "SUCCESS"}
                if len(successes) >= count:
                    return successes
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"WaitCondition timed out. Received {len(successes)} conditions when expecting {count}"
                    )
                _changed.wait(timeout=min(remaining, 1.0))
        finally:
            if _waiting.get(key, {}).get("token") == token:
                _waiting.pop(key, None)
            entry = _handles.get(token)
            if entry is not None and not entry["url"]:
                _handles.pop(token, None)


def remember_result(physical_id: str, attrs: dict) -> None:
    with _lock:
        _results[physical_id] = dict(attrs)


def recall_result(physical_id: str) -> dict:
    with _lock:
        return dict(_results.get(physical_id, {}))


def reset():
    global _generation
    with _changed:
        _generation += 1
        _handles.clear()
        _waiting.clear()
        _results.clear()
        _changed.notify_all()
