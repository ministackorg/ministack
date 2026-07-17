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

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    _request_account_id,
    _request_region,
    get_account_id,
    get_region,
    new_uuid,
)

logger = logging.getLogger("pipes")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
CROSS_REGION_PIPE_ERROR = "Creating cross-region pipe is not permitted."

_pipes = AccountRegionScopedDict()       # pipe_name -> pipe record
_positions = AccountRegionScopedDict()   # pipe_arn -> next stream record index
_poller_started = False
_poller_lock = threading.Lock()


def get_state():
    return {
        "pipes": copy.deepcopy(_pipes),
        "positions": copy.deepcopy(_positions),
    }


def restore_state(data):
    if data:
        _restore_pipe_store(data.get("pipes", {}))
        _restore_position_store(data.get("positions", {}))
        # Restored RUNNING pipes need the background poller — register_pipe
        # is the only other place that starts it, and it isn't called on
        # warm-boot. Without this, persisted pipes would silently stop
        # forwarding events until a new pipe is registered.
        if any(p.get("CurrentState") == "RUNNING" for p in _pipes.all_values()):
            _ensure_poller()


def _pipe_arn_scope(pipe_arn: str, default_account_id: str | None = None) -> tuple[str, str]:
    try:
        spec = parse_arn(pipe_arn)
    except ArnParseError:
        return default_account_id or get_account_id(), get_region()
    if spec.service != "pipes":
        return default_account_id or get_account_id(), get_region()
    return spec.account_id or default_account_id or get_account_id(), spec.region or get_region()


def _pipe_record_scope(pipe: dict, default_account_id: str | None = None) -> tuple[str, str]:
    return _pipe_arn_scope(pipe.get("Arn", ""), default_account_id)


def _restore_pipe_store(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _pipes.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, name), pipe in data._data.items():
            restored_account_id, region = _pipe_record_scope(pipe, account_id)
            _pipes.set_scoped(restored_account_id, region, name, copy.deepcopy(pipe))
        return
    if isinstance(data, dict):
        for key, pipe in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, name = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, name = key
                account_id, region = _pipe_record_scope(pipe, account_id)
            else:
                name = key
                account_id, region = _pipe_record_scope(pipe)
            _pipes.set_scoped(account_id, region, name, copy.deepcopy(pipe))


def _restore_position_store(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _positions.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, pipe_arn), position in data._data.items():
            restored_account_id, region = _pipe_arn_scope(pipe_arn, account_id)
            _positions.set_scoped(restored_account_id, region, pipe_arn, position)
        return
    if isinstance(data, dict):
        for key, position in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, pipe_arn = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, pipe_arn = key
                account_id, region = _pipe_arn_scope(pipe_arn, account_id)
            else:
                pipe_arn = key
                account_id, region = _pipe_arn_scope(pipe_arn)
            _positions.set_scoped(account_id, region, pipe_arn, position)


def _iter_all_pipes():
    for scoped_key, pipe in list(_pipes.all_items()):
        account_id, region, _name = scoped_key
        yield account_id, region, pipe


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
    pipe_region = get_region()
    # AWS rejects cross-region source/target ARNs before role validation.
    for component_arn in (source, target):
        try:
            component_region = parse_arn(component_arn).region
        except ArnParseError:
            continue
        if component_region and component_region != pipe_region:
            raise ValueError(CROSS_REGION_PIPE_ERROR)

    arn = f"arn:aws:pipes:{pipe_region}:{get_account_id()}:pipe/{name}"
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
        time.sleep(1 if _pipes.has_any() else 5)


def _poll_once():
    from ministack.services import dynamodb as _ddb

    stream_records = getattr(_ddb, "_stream_records", None)
    if stream_records is None:
        return

    for pipe_account_id, pipe_region, pipe in _iter_all_pipes():
        account_token = _request_account_id.set(pipe_account_id)
        region_token = _request_region.set(pipe_region)
        try:
            if pipe.get("CurrentState") != "RUNNING":
                continue

            source_arn = pipe.get("Source", "")
            target_arn = pipe.get("Target", "")
            if _arn_service(source_arn) != "dynamodb" or _arn_service(target_arn) != "sns":
                continue

            source = _dynamodb_stream_source(source_arn)
            if source is None:
                continue
            source_spec, table_name = source
            if source_spec.account_id != pipe_account_id:
                continue

            records = stream_records.get_scoped(
                pipe_account_id, source_spec.region, table_name, []
            )
            pos = int(_positions.get(pipe["Arn"], 0))
            if pos < 0:
                pos = 0
            if pos >= len(records):
                continue

            batch = records[pos:]
            for rec in batch:
                _publish_record_to_sns(target_arn, pipe, rec)
            _positions[pipe["Arn"]] = pos + len(batch)
        finally:
            _request_region.reset(region_token)
            _request_account_id.reset(account_token)


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


def _arn_service(arn: str) -> str:
    """Classify a target ARN for dispatch; invalid stored targets are ignored."""
    try:
        return parse_arn(arn).service
    except ArnParseError:
        return ""


def _table_name_from_stream_arn(stream_arn: str) -> str:
    """Return a DynamoDB table name for Pipes runtime dispatch, or empty string."""
    source = _dynamodb_stream_source(stream_arn)
    return "" if source is None else source[1]


def _dynamodb_stream_source(stream_arn: str):
    """Return the parsed source ARN and table name for a DynamoDB stream."""
    try:
        spec = parse_arn(stream_arn)
    except ArnParseError:
        return None
    if spec.service != "dynamodb":
        return None
    parts = spec.resource.split("/")
    if (
        len(parts) < 4
        or parts[0] != "table"
        or parts[2] != "stream"
        or not parts[1]
        or not parts[3]
    ):
        return None
    return spec, parts[1]


def _pipe_account_id(pipe: dict) -> str:
    try:
        spec = parse_arn(pipe.get("Arn", ""))
    except ArnParseError:
        return get_account_id()
    if spec.service != "pipes" or not spec.account_id:
        return get_account_id()
    return spec.account_id


def _initial_position(pipe: dict) -> int:
    from ministack.services import dynamodb as _ddb

    source = _dynamodb_stream_source(pipe.get("Source", ""))
    if source is None:
        return 0
    source_spec, table_name = source
    pipe_account_id = _pipe_account_id(pipe)
    if source_spec.account_id != pipe_account_id:
        return 0

    stream_records = getattr(_ddb, "_stream_records", None)
    if stream_records is None:
        return 0
    records = stream_records.get_scoped(
        pipe_account_id, source_spec.region, table_name, []
    )
    if pipe.get("StartingPosition") == "TRIM_HORIZON":
        return 0
    return len(records)
