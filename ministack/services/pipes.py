"""
EventBridge Pipes service emulator.

REST/JSON protocol — /v1/pipes/* and /tags/* paths.

SDK surface:
  ListPipes, CreatePipe, DescribePipe, UpdatePipe, DeletePipe,
  StartPipe, StopPipe, ListTagsForResource, TagResource, UntagResource

Runtime (background poller + CloudFormation) scope is intentionally limited to:
- Source: DynamoDB Streams
- Target: SNS, Step Functions state machine
"""

import copy
import json
import logging
import os
import threading
import time
from urllib.parse import unquote

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
    description: str = "",
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

    now = int(time.time())
    _pipes[name] = {
        "Name": name,
        "Arn": arn,
        "RoleArn": role_arn,
        "Description": description or "",
        "Source": source,
        "Target": target,
        "DesiredState": state,
        "CurrentState": state,
        "StartingPosition": start,
        "Tags": tags or {},
        "CreationTime": now,
        "LastModifiedTime": now,
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
            _poll_pipe(_ddb, pipe, pipe_account_id)
        finally:
            _request_region.reset(region_token)
            _request_account_id.reset(account_token)


def _poll_pipe(_ddb, pipe: dict, pipe_account_id: str) -> None:
    if pipe.get("CurrentState") != "RUNNING":
        return
    if _arn_service(pipe.get("Source", "")) != "dynamodb":
        return

    source = _dynamodb_stream_source(pipe.get("Source", ""))
    if source is None:
        return
    source_spec, table_name = source
    if source_spec.account_id != pipe_account_id:
        return

    scope = {"account_id": pipe_account_id, "region": source_spec.region}
    # Positions are absolute stream positions: records expiring off the
    # front of the stream must not shift a pipe's read position.
    horizon = _ddb.stream_start_position(table_name, **scope)
    end = _ddb.stream_end_position(table_name, **scope)
    pos = max(int(_positions.get(pipe["Arn"], 0)), horizon)
    if pos >= end:
        return

    batch = _ddb.stream_records_since(table_name, pos, end - pos, **scope)
    if _deliver_batch(pipe, batch):
        _positions[pipe["Arn"]] = pos + len(batch)


def _deliver_batch(pipe: dict, batch: list) -> bool:
    """True once every record in the batch has reached the target. A batch that
    did not is left on the stream: the position stays on it and the next poll
    retries it, until the records age out of the retention window."""
    target_arn = pipe.get("Target", "")
    target_service = _arn_service(target_arn)
    if target_service == "states":
        return _start_state_machine_from_records(target_arn, pipe, batch)
    if target_service == "sns":
        for rec in batch:
            _publish_record_to_sns(target_arn, pipe, rec)
        return True
    logger.warning(
        "Pipes %s: holding %d record(s); MiniStack delivers to sns and states, "
        "not to %s", pipe.get("Name"), len(batch), target_service or target_arn)
    return False


def _start_state_machine_from_records(sm_arn: str, pipe: dict, records: list) -> bool:
    """Start one execution carrying the batch as the JSON array Pipes delivers.

    `_start_execution` answers the `(status, headers, body)` triple every
    MiniStack handler answers; anything from 400 up means no execution started.
    """
    from ministack.services import stepfunctions as _sfn

    status, _headers, body = _sfn._start_execution({
        "stateMachineArn": sm_arn,
        "input": json.dumps(records),
    })
    if status >= 400:
        logger.warning("Pipes %s: StartExecution on %s failed (%s): %s",
                       pipe.get("Name"), sm_arn, status, body)
        return False
    return True


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
    scope = {"account_id": pipe_account_id, "region": source_spec.region}
    if pipe.get("StartingPosition") == "TRIM_HORIZON":
        return _ddb.stream_start_position(table_name, **scope)
    return _ddb.stream_end_position(table_name, **scope)


# ---------------------------------------------------------------------------
# REST/JSON request handler (endpointPrefix "pipes", protocol rest-json).
#
# Op / method / requestUri (botocore pipes/2015-10-07/service-2.json):
#   ListPipes             GET    /v1/pipes
#   CreatePipe            POST   /v1/pipes/{Name}
#   DescribePipe          GET    /v1/pipes/{Name}
#   UpdatePipe            PUT    /v1/pipes/{Name}
#   DeletePipe            DELETE /v1/pipes/{Name}
#   StartPipe             POST   /v1/pipes/{Name}/start
#   StopPipe              POST   /v1/pipes/{Name}/stop
#   ListTagsForResource   GET    /tags/{resourceArn}
#   TagResource           POST   /tags/{resourceArn}
#   UntagResource         DELETE /tags/{resourceArn}
#
# JSON-protocol timestamps are int epoch seconds (Timestamp shape).
# ---------------------------------------------------------------------------

_VALID_REQUESTED_STATE = ("RUNNING", "STOPPED")


def _json_resp(status, body):
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _error(status, code, message):
    return (
        status,
        {"Content-Type": "application/json", "x-amzn-errortype": code},
        json.dumps({"__type": code, "message": message}).encode(),
    )


def _not_found(name):
    # Matches the real AWS NotFoundException message for DescribePipe/DeletePipe
    # etc. (member: "message").
    return _error(404, "NotFoundException", f"Pipe {name} does not exist.")


def _single(v):
    return v[0] if isinstance(v, list) else v


def _lifecycle_response(pipe):
    """Shape shared by CreatePipe/UpdatePipe/DeletePipe/StartPipe/StopPipe."""
    return {
        "Arn": pipe.get("Arn", ""),
        "Name": pipe.get("Name", ""),
        "DesiredState": pipe.get("DesiredState", "RUNNING"),
        "CurrentState": pipe.get("CurrentState", "RUNNING"),
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
    }


def _summary(pipe):
    """ListPipes Pipe summary member shape."""
    out = {
        "Name": pipe.get("Name", ""),
        "Arn": pipe.get("Arn", ""),
        "DesiredState": pipe.get("DesiredState", "RUNNING"),
        "CurrentState": pipe.get("CurrentState", "RUNNING"),
        "StateReason": pipe.get("StateReason", ""),
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
        "Source": pipe.get("Source", ""),
        "Target": pipe.get("Target", ""),
    }
    if pipe.get("Enrichment"):
        out["Enrichment"] = pipe["Enrichment"]
    return out


def _describe_response(pipe):
    return {
        "Arn": pipe.get("Arn", ""),
        "Name": pipe.get("Name", ""),
        "Description": pipe.get("Description", ""),
        "DesiredState": pipe.get("DesiredState", "RUNNING"),
        "CurrentState": pipe.get("CurrentState", "RUNNING"),
        "StateReason": pipe.get("StateReason", ""),
        "Source": pipe.get("Source", ""),
        "Target": pipe.get("Target", ""),
        "RoleArn": pipe.get("RoleArn", ""),
        "Tags": pipe.get("Tags", {}) or {},
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
    }


def _find_pipe_by_arn(arn):
    for pipe in _pipes.values():
        if pipe.get("Arn") == arn:
            return pipe
    return None


def _create_pipe(name, body):
    if name in _pipes:
        return _error(409, "ConflictException", f"Pipe {name} already exists.")
    source = body.get("Source", "")
    target = body.get("Target", "")
    role_arn = body.get("RoleArn", "")
    desired = str(body.get("DesiredState", "RUNNING")).upper()
    if desired not in _VALID_REQUESTED_STATE:
        return _error(
            400,
            "ValidationException",
            f"DesiredState must be one of {list(_VALID_REQUESTED_STATE)}.",
        )
    try:
        pipe = register_pipe(
            name=name,
            source=source,
            target=target,
            role_arn=role_arn,
            desired_state=desired,
            tags=body.get("Tags") or {},
            description=body.get("Description", "") or "",
        )
    except ValueError as e:
        return _error(400, "ValidationException", str(e))
    return _json_resp(200, _lifecycle_response(pipe))


def _describe_pipe(name):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    return _json_resp(200, _describe_response(pipe))


def _update_pipe(name, body):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    if "Description" in body:
        pipe["Description"] = body.get("Description", "") or ""
    if "RoleArn" in body:
        pipe["RoleArn"] = body.get("RoleArn", "") or ""
    if "Target" in body and body.get("Target"):
        pipe["Target"] = body["Target"]
    if "DesiredState" in body:
        desired = str(body.get("DesiredState", "")).upper()
        if desired not in _VALID_REQUESTED_STATE:
            return _error(
                400,
                "ValidationException",
                f"DesiredState must be one of {list(_VALID_REQUESTED_STATE)}.",
            )
        pipe["DesiredState"] = desired
        pipe["CurrentState"] = desired
    pipe["LastModifiedTime"] = int(time.time())
    _pipes[name] = pipe
    return _json_resp(200, _lifecycle_response(pipe))


def _delete_pipe_op(name):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    resp = {
        "Arn": pipe.get("Arn", ""),
        "Name": pipe.get("Name", ""),
        "DesiredState": "DELETED",
        "CurrentState": "DELETING",
        "CreationTime": int(pipe.get("CreationTime", 0)),
        "LastModifiedTime": int(pipe.get("LastModifiedTime", pipe.get("CreationTime", 0))),
    }
    delete_pipe(name)
    return _json_resp(200, resp)


def _set_state(name, desired):
    pipe = _pipes.get(name)
    if pipe is None:
        return _not_found(name)
    pipe["DesiredState"] = desired
    pipe["CurrentState"] = desired
    pipe["LastModifiedTime"] = int(time.time())
    _pipes[name] = pipe
    return _json_resp(200, _lifecycle_response(pipe))


def _list_pipes(query):
    name_prefix = _single(query.get("NamePrefix"))
    desired_state = _single(query.get("DesiredState"))
    current_state = _single(query.get("CurrentState"))
    source_prefix = _single(query.get("SourcePrefix"))
    target_prefix = _single(query.get("TargetPrefix"))
    limit = _single(query.get("Limit"))

    pipes = sorted(_pipes.values(), key=lambda p: p.get("Name", ""))
    result = []
    for pipe in pipes:
        if name_prefix and not pipe.get("Name", "").startswith(name_prefix):
            continue
        if desired_state and pipe.get("DesiredState") != desired_state:
            continue
        if current_state and pipe.get("CurrentState") != current_state:
            continue
        if source_prefix and not pipe.get("Source", "").startswith(source_prefix):
            continue
        if target_prefix and not pipe.get("Target", "").startswith(target_prefix):
            continue
        result.append(_summary(pipe))

    next_token = None
    if limit:
        try:
            n = int(limit)
            if 0 < n < len(result):
                next_token = result[n]["Name"]
                result = result[:n]
        except (TypeError, ValueError):
            pass

    body = {"Pipes": result}
    if next_token is not None:
        body["NextToken"] = next_token
    return _json_resp(200, body)


def _list_tags(arn):
    pipe = _find_pipe_by_arn(arn)
    if pipe is None:
        return _not_found(arn)
    return _json_resp(200, {"tags": pipe.get("Tags", {}) or {}})


def _tag_resource(arn, body):
    pipe = _find_pipe_by_arn(arn)
    if pipe is None:
        return _not_found(arn)
    pipe.setdefault("Tags", {}).update(body.get("tags", {}) or {})
    return _json_resp(200, {})


def _untag_resource(arn, query):
    pipe = _find_pipe_by_arn(arn)
    if pipe is None:
        return _not_found(arn)
    keys = query.get("tagKeys", [])
    if not isinstance(keys, list):
        keys = [keys]
    tags = pipe.setdefault("Tags", {})
    for k in keys:
        tags.pop(k, None)
    return _json_resp(200, {})


async def handle_request(method, path, headers, body_bytes, query_params):
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except (json.JSONDecodeError, TypeError):
        body = {}
    if not isinstance(body, dict):
        body = {}

    # Pipe lifecycle sub-actions: /v1/pipes/{Name}/start | /stop
    if path.startswith("/v1/pipes/"):
        rest = path[len("/v1/pipes/"):]
        if rest.endswith("/start"):
            return _set_state(unquote(rest[: -len("/start")]), "RUNNING")
        if rest.endswith("/stop"):
            return _set_state(unquote(rest[: -len("/stop")]), "STOPPED")
        name = unquote(rest)
        if name:
            if method == "POST":
                return _create_pipe(name, body)
            if method == "GET":
                return _describe_pipe(name)
            if method == "PUT":
                return _update_pipe(name, body)
            if method == "DELETE":
                return _delete_pipe_op(name)

    if path == "/v1/pipes" and method == "GET":
        return _list_pipes(query_params)

    # Tag routes: /tags/{resourceArn+}
    if path.startswith("/tags/"):
        arn = unquote(path[len("/tags/"):])
        if method == "GET":
            return _list_tags(arn)
        if method == "POST":
            return _tag_resource(arn, body)
        if method == "DELETE":
            return _untag_resource(arn, query_params)

    return _error(400, "ValidationException", f"No route for {method} {path}")
