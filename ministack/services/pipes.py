"""
Minimal EventBridge Pipes runtime for local CloudFormation use.

Scope intentionally limited to:
- Source: DynamoDB Streams
- Target: SNS
- Lifecycle: create/delete (via CloudFormation handlers)
"""

import copy
import json
import logging
import os
import threading
import time

from ministack.core.responses import AccountScopedDict, get_account_id, new_uuid, get_region
from ministack.core.persistence import load_state

logger = logging.getLogger("pipes")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_pipes = AccountScopedDict()       # pipe_name -> pipe record
_positions = AccountScopedDict()   # pipe_arn -> next stream record index
_poller_started = False
_poller_lock = threading.Lock()


def get_state():
    return {
        "pipes": copy.deepcopy(_pipes),
        "positions": copy.deepcopy(_positions),
    }


def restore_state(data):
    if data:
        _pipes.update(data.get("pipes", {}))
        _positions.update(data.get("positions", {}))
        # Restored RUNNING pipes need the background poller — register_pipe
        # is the only other place that starts it, and it isn't called on
        # warm-boot. Without this, persisted pipes would silently stop
        # forwarding events until a new pipe is registered.
        if any(p.get("CurrentState") == "RUNNING" for p in _pipes.values()):
            _ensure_poller()


try:
    _restored = load_state("pipes")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted pipes state; continuing fresh")


def reset():
    _pipes.clear()
    _positions.clear()


def register_pipe(
    *,
    name: str,
    source: str,
    target: str,
    role_arn: str = "",
    desired_state: str = "RUNNING",
    starting_position: str = "LATEST",
    tags: dict | None = None,
):
    arn = f"arn:aws:pipes:{get_region()}:{get_account_id()}:pipe/{name}"
    state = "STOPPED" if str(desired_state).upper() == "STOPPED" else "RUNNING"
    start = str(starting_position or "LATEST").upper()
    if start not in ("LATEST", "TRIM_HORIZON"):
        start = "LATEST"

    _pipes[name] = {
        "Name": name,
        "Arn": arn,
        "RoleArn": role_arn,
        "Source": source,
        "Target": target,
        "DesiredState": state,
        "CurrentState": state,
        "StartingPosition": start,
        "Tags": tags or {},
        "CreationTime": int(time.time()),
    }
    _positions[arn] = _initial_position(_pipes[name])

    _ensure_poller()
    return _pipes[name]


def delete_pipe(name: str):
    pipe = _pipes.pop(name, None)
    if pipe:
        _positions.pop(pipe["Arn"], None)


def _ensure_poller():
    global _poller_started
    with _poller_lock:
        if not _poller_started:
            t = threading.Thread(target=_poll_loop, daemon=True)
            t.start()
            _poller_started = True


def _poll_loop():
    while True:
        try:
            _poll_once()
        except Exception as e:
            logger.error("Pipes poller error: %s", e)
        time.sleep(1 if _pipes else 5)


def _poll_once():
    from ministack.services import dynamodb as _ddb

    stream_records = getattr(_ddb, "_stream_records", None)
    if stream_records is None:
        return

    for pipe in list(_pipes.values()):
        if pipe.get("CurrentState") != "RUNNING":
            continue

        source_arn = pipe.get("Source", "")
        target_arn = pipe.get("Target", "")
        if ":dynamodb:" not in source_arn or "/stream/" not in source_arn:
            continue
        if ":sns:" not in target_arn:
            continue

        table_name = _table_name_from_stream_arn(source_arn)
        if not table_name:
            continue

        records = stream_records.get(table_name, [])
        pos = int(_positions.get(pipe["Arn"], 0))
        if pos < 0:
            pos = 0
        if pos >= len(records):
            continue

        batch = records[pos:]
        for rec in batch:
            _publish_record_to_sns(target_arn, pipe, rec)
        _positions[pipe["Arn"]] = pos + len(batch)


def _publish_record_to_sns(topic_arn: str, pipe: dict, record: dict):
    from ministack.services import sns as _sns

    topic = _sns._topics.get(topic_arn)
    if not topic:
        logger.warning("Pipes %s: SNS topic not found %s", pipe.get("Name"), topic_arn)
        return

    msg_id = new_uuid()
    message = json.dumps(record)
    subject = f"Pipes {pipe.get('Name', '')}"

    topic["messages"].append({
        "id": msg_id,
        "message": message,
        "subject": subject,
        "message_structure": "",
        "message_attributes": {},
        "timestamp": int(time.time()),
    })
    _sns._fanout(topic_arn, msg_id, message, subject, "", {})


def _table_name_from_stream_arn(stream_arn: str) -> str:
    if "/stream/" not in stream_arn:
        return ""
    return stream_arn.split("/stream/", 1)[0].rsplit("/", 1)[-1]


def _initial_position(pipe: dict) -> int:
    from ministack.services import dynamodb as _ddb

    table_name = _table_name_from_stream_arn(pipe.get("Source", ""))
    if not table_name:
        return 0

    records = getattr(_ddb, "_stream_records", {}).get(table_name, [])
    if pipe.get("StartingPosition") == "TRIM_HORIZON":
        return 0
    return len(records)
