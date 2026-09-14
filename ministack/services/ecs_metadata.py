# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""ECS Task Metadata V4 emulator.

Real ECS injects ECS_CONTAINER_METADATA_URI_V4=http://169.254.170.2/v4/<token>
per container; ministack instead serves the same routes off the gateway port,
keyed by tokens registered from services/ecs.py. State is volatile by design
(stripped on persistence; reset() called by /_ministack/reset).
"""

import logging
import re
import threading

from ministack.core.responses import json_response

logger = logging.getLogger("ecs_metadata")

# task_arn -> task_payload (Containers list shared by all sibling tokens)
_TASKS: dict[str, dict] = {}
# token -> task_arn
_TOKEN_TO_TASK: dict[str, str] = {}
# token -> container_payload (an entry inside _TASKS[arn]["Containers"])
_TOKEN_TO_CONTAINER: dict[str, dict] = {}
_LOCK = threading.Lock()

_PATH_RE = re.compile(r"^/v4/(?P<token>[A-Za-z0-9_-]{8,})(?P<rest>/.*)?$")


def register_container(
    token: str, task_arn: str, task_payload: dict, container_payload: dict
) -> None:
    """Register a container under a task. The first call for a task_arn seeds
    the task payload; subsequent calls just append to its Containers list, so
    /task returns every sibling container regardless of which token was used.
    """
    with _LOCK:
        task = _TASKS.get(task_arn)
        if task is None:
            task = dict(task_payload)
            task.setdefault("Containers", [])
            _TASKS[task_arn] = task
        task["Containers"].append(container_payload)
        _TOKEN_TO_TASK[token] = task_arn
        _TOKEN_TO_CONTAINER[token] = container_payload
    logger.debug(
        "registered metadata token for task=%s container=%s",
        task_arn,
        container_payload.get("Name"),
    )


def unregister_token(token: str) -> None:
    with _LOCK:
        arn = _TOKEN_TO_TASK.pop(token, None)
        container = _TOKEN_TO_CONTAINER.pop(token, None)
        if not arn:
            return
        task = _TASKS.get(arn)
        if task and container is not None:
            try:
                task["Containers"].remove(container)
            except ValueError:
                pass
        if task is not None and not task.get("Containers"):
            _TASKS.pop(arn, None)
    logger.debug("unregistered metadata token for task=%s", arn)


def set_docker_id(token: str, docker_id: str) -> None:
    with _LOCK:
        if container := _TOKEN_TO_CONTAINER.get(token):
            container["DockerId"] = docker_id


def reset() -> None:
    with _LOCK:
        _TASKS.clear()
        _TOKEN_TO_TASK.clear()
        _TOKEN_TO_CONTAINER.clear()


def _live_status(task_arn: str) -> tuple[str, str, dict] | None:
    """``(DesiredStatus, KnownStatus, {container name: status})``, or None.

    Imported lazily: services/ecs.py imports this module, so a module-level
    import back would be circular.
    """
    from ministack.services import ecs

    return ecs.metadata_task_status(task_arn)


def _with_status(payload: dict, status, container: bool = False) -> dict:
    """A copy of ``payload`` carrying the current status.

    `DesiredStatus` is the task's everywhere, which is how a container learns
    it is being shut down. `KnownStatus` is the task's on the task payload and
    the container's own on a container payload: on AWS the two differ while a
    task starts, where the container is already RUNNING and the task is not.

    A copy because this writes the two members, and the dicts it is handed are
    the live registry entries: the task payload every sibling container's
    `/task` view is built from, and the container payloads inside it. Writing
    the overlay into those would drift the registry away from what was
    registered, and the fallback for a task whose record is gone would then
    serve a value nobody registered.
    """
    out = dict(payload)
    if status is None:
        return out
    desired, known, per_container = status
    out["DesiredStatus"] = desired
    if container:
        out["KnownStatus"] = per_container.get(out.get("Name")) or out.get("KnownStatus") or known
    else:
        out["KnownStatus"] = known
    return out


async def handle_request(method, path, headers, body, query_params):
    m = _PATH_RE.match(path)
    if not m:
        return json_response({"message": "not found"}, status=404)
    token = m.group("token")
    with _LOCK:
        arn = _TOKEN_TO_TASK.get(token)
        if not arn:
            return json_response({"message": "unknown token"}, status=404)
        container = _TOKEN_TO_CONTAINER[token]
        task = _TASKS[arn]
        containers = list(task.get("Containers", []))

    # Resolved outside _LOCK: the ecs service takes its own per-task locks, and
    # holding this one across that call would be a lock-order inversion.
    status = _live_status(arn)

    rest = (m.group("rest") or "").rstrip("/")
    if rest == "":
        return json_response(_with_status(container, status, container=True))
    if rest == "/task":
        task = _with_status(task, status)
        task["Containers"] = [
            _with_status(c, status, container=True) for c in containers
        ]
        return json_response(task)
    if rest in ("/stats", "/task/stats"):
        return json_response({})
    return json_response({"message": "not found"}, status=404)
