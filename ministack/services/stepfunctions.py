"""
Step Functions Service Emulator with ASL execution engine.
JSON-based API via X-Amz-Target (AWSStepFunctions).

Supports: CreateStateMachine, DeleteStateMachine, DescribeStateMachine,
          UpdateStateMachine, ListStateMachines,
          StartExecution, StartSyncExecution, StopExecution,
          DescribeExecution, DescribeStateMachineForExecution, ListExecutions,
          GetExecutionHistory,
          SendTaskSuccess, SendTaskFailure, SendTaskHeartbeat,
          CreateActivity, DeleteActivity, DescribeActivity, ListActivities,
          GetActivityTask,
          TagResource, UntagResource, ListTagsForResource.

ASL state types: Pass, Task, Choice, Wait, Succeed, Fail, Parallel, Map.
Task states invoke Lambda functions via services.lambda_svc when available.
Executions run in background threads and transition through RUNNING ->
SUCCEEDED / FAILED / TIMED_OUT / ABORTED.
"""

import asyncio
import copy
import json
import logging
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait as futures_wait
from datetime import datetime, timezone

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountScopedDict,
    get_account_id,
    error_response_json,
    json_response,
    new_uuid,
    now_iso,
)

logger = logging.getLogger("states")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# SFN mock config — compatible with LocalStack's SFN_MOCK_CONFIG / LOCALSTACK_SFN_MOCK_CONFIG
_sfn_mock_config = AccountScopedDict()
_sfn_mock_config_path = (
    os.environ.get("SFN_MOCK_CONFIG")
    or os.environ.get("LOCALSTACK_SFN_MOCK_CONFIG")
    or ""
)
if _sfn_mock_config_path:
    try:
        with open(_sfn_mock_config_path) as f:
            _sfn_mock_config = json.load(f)
        logger.info("SFN mock config loaded from %s", _sfn_mock_config_path)
    except Exception as e:
        logger.warning("Failed to load SFN mock config from %s: %s", _sfn_mock_config_path, e)


def _get_mock_response(sm_name: str, test_case: str, state_name: str, attempt: int) -> dict | None:
    """Look up a mock response for a state using the AWS SFN Local mock config format.

    Format: StateMachines.<SM>.TestCases.<TC>.<State> -> response name
            MockedResponses.<name>.<attempt> -> {Return: ...} or {Throw: ...}
    Attempt keys can be "0", "1", "1-3", etc.
    """
    sm_cfg = _sfn_mock_config.get("StateMachines", {}).get(sm_name, {})
    if not test_case or not sm_cfg:
        return None
    tc = sm_cfg.get("TestCases", {}).get(test_case, {})
    response_name = tc.get(state_name)
    if not response_name:
        return None
    mocked = _sfn_mock_config.get("MockedResponses", {}).get(response_name, {})
    if not mocked:
        return None
    # Match attempt: exact ("0") or range ("1-3")
    str_attempt = str(attempt)
    if str_attempt in mocked:
        return mocked[str_attempt]
    for key, val in mocked.items():
        if "-" in key:
            parts = key.split("-", 1)
            try:
                lo, hi = int(parts[0]), int(parts[1])
                if lo <= attempt <= hi:
                    return val
            except ValueError:
                continue
    return None

_state_machines = AccountScopedDict()
_executions = AccountScopedDict()
_task_tokens = AccountScopedDict()
_tags = AccountScopedDict()
_activities = AccountScopedDict()
_activity_tasks = AccountScopedDict()

# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "state_machines": copy.deepcopy(_state_machines),
        "executions": copy.deepcopy(_executions),
        "tags": copy.deepcopy(_tags),
        "activities": copy.deepcopy(_activities),
    }


def restore_state(data):
    if not data:
        return
    _state_machines.update(data.get("state_machines", {}))
    _executions.update(data.get("executions", {}))
    _tags.update(data.get("tags", {}))
    _activities.update(data.get("activities", {}))
    # Executions that were RUNNING when the process died cannot resume —
    # mark them FAILED, following the ECS precedent (tasks → STOPPED).
    for exc in _executions.values():
        if exc.get("status") == "RUNNING":
            exc["status"] = "FAILED"
            exc["stopDate"] = now_iso()
            exc["error"] = "States.ServiceRestart"
            exc["cause"] = "Execution was running when service restarted"


_restored = load_state("stepfunctions")
if _restored:
    restore_state(_restored)


_TIMESTAMP_RESPONSE_FIELDS = {
    "creationDate",
    "redriveDate",
    "startDate",
    "stopDate",
    "timestamp",
    "updateDate",
}


def _timestamp_response_value(value):
    """Step Functions models timestamps as JSON numbers, not ISO strings."""
    if not isinstance(value, str):
        return value
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return value


def _normalize_timestamp_response(payload, field_name=None):
    if isinstance(payload, dict):
        return {
            key: _normalize_timestamp_response(value, key)
            for key, value in payload.items()
        }
    if isinstance(payload, list):
        return [_normalize_timestamp_response(item, field_name) for item in payload]
    if field_name in _TIMESTAMP_RESPONSE_FIELDS:
        return _timestamp_response_value(payload)
    return payload


def _finalize_response(response):
    """Serialize Step Functions timestamps in the format AWS SDKs expect."""
    status, headers, body = response
    if not body:
        return response
    try:
        payload = json.loads(body)
    except (TypeError, ValueError):
        return response

    normalized = _normalize_timestamp_response(payload)
    if normalized == payload:
        return response
    return json_response(normalized, status)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateStateMachine": _create_state_machine,
        "DeleteStateMachine": _delete_state_machine,
        "DescribeStateMachine": _describe_state_machine,
        "UpdateStateMachine": _update_state_machine,
        "ListStateMachines": _list_state_machines,
        "StartExecution": _start_execution,
        "StopExecution": _stop_execution,
        "DescribeExecution": _describe_execution,
        "ListExecutions": _list_executions,
        "GetExecutionHistory": _get_execution_history,
        "SendTaskSuccess": _send_task_success,
        "SendTaskFailure": _send_task_failure,
        "SendTaskHeartbeat": _send_task_heartbeat,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsForResource": _list_tags_for_resource,
        "StartSyncExecution": _start_sync_execution,
        "DescribeStateMachineForExecution": _describe_state_machine_for_execution,
        "CreateActivity": _create_activity,
        "DeleteActivity": _delete_activity,
        "DescribeActivity": _describe_activity,
        "ListActivities": _list_activities,
        "GetActivityTask": _get_activity_task,
        "TestState": _test_state,
        "ValidateStateMachineDefinition": _validate_state_machine_definition,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    if action == "GetActivityTask":
        return _finalize_response(await _get_activity_task(data))
    return _finalize_response(handler(data))


# ---------------------------------------------------------------------------
# State machine CRUD
# ---------------------------------------------------------------------------

def _create_state_machine(data):
    name = data.get("name")
    if not name:
        return error_response_json("ValidationException", "name is required", 400)

    arn = f"arn:aws:states:{REGION}:{get_account_id()}:stateMachine:{name}"
    if arn in _state_machines:
        return error_response_json(
            "StateMachineAlreadyExists",
            f"State machine {name} already exists", 400)

    ts = now_iso()
    _state_machines[arn] = {
        "stateMachineArn": arn,
        "name": name,
        "definition": data.get("definition", "{}"),
        "roleArn": data.get("roleArn",
                            f"arn:aws:iam::{get_account_id()}:role/StepFunctionsRole"),
        "type": data.get("type", "STANDARD"),
        "creationDate": ts,
        "status": "ACTIVE",
        "loggingConfiguration": data.get(
            "loggingConfiguration",
            {"level": "OFF", "includeExecutionData": False}),
    }

    tags = data.get("tags", [])
    if tags:
        _tags[arn] = list(tags)

    return json_response({"stateMachineArn": arn, "creationDate": ts})


def _delete_state_machine(data):
    arn = data.get("stateMachineArn")
    if arn not in _state_machines:
        return error_response_json(
            "StateMachineDoesNotExist",
            f"State machine {arn} not found", 400)
    del _state_machines[arn]
    _tags.pop(arn, None)
    # Clean up executions for this state machine
    stale = [k for k, v in _executions.items() if v.get("stateMachineArn") == arn]
    for k in stale:
        _executions.pop(k, None)
    return json_response({})


def _describe_state_machine(data):
    arn = data.get("stateMachineArn")
    sm = _state_machines.get(arn)
    if not sm:
        return error_response_json(
            "StateMachineDoesNotExist",
            f"State machine {arn} not found", 400)
    return json_response(sm)


def _update_state_machine(data):
    arn = data.get("stateMachineArn")
    sm = _state_machines.get(arn)
    if not sm:
        return error_response_json(
            "StateMachineDoesNotExist",
            f"State machine {arn} not found", 400)
    if "definition" in data:
        sm["definition"] = data["definition"]
    if "roleArn" in data:
        sm["roleArn"] = data["roleArn"]
    if "loggingConfiguration" in data:
        sm["loggingConfiguration"] = data["loggingConfiguration"]
    return json_response({"updateDate": now_iso()})


def _list_state_machines(data):
    all_machines = [
        {"stateMachineArn": sm["stateMachineArn"], "name": sm["name"],
         "type": sm["type"], "creationDate": sm["creationDate"]}
        for sm in _state_machines.values()
    ]
    max_results = int(data.get("maxResults", 1000))
    next_token = data.get("nextToken")
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = all_machines[start:start + max_results]
    resp = {"stateMachines": page}
    if start + max_results < len(all_machines):
        resp["nextToken"] = str(start + max_results)
    return json_response(resp)


# ---------------------------------------------------------------------------
# Execution lifecycle
# ---------------------------------------------------------------------------

def _start_execution(data):
    sm_arn_raw = data.get("stateMachineArn", "")
    # Support #TestCaseName suffix for mock config
    test_case = ""
    if "#" in sm_arn_raw:
        sm_arn, test_case = sm_arn_raw.rsplit("#", 1)
    else:
        sm_arn = sm_arn_raw
    if sm_arn not in _state_machines:
        return error_response_json(
            "StateMachineDoesNotExist",
            f"State machine {sm_arn} not found", 400)

    sm = _state_machines[sm_arn]
    name = data.get("name") or new_uuid()
    exec_arn = (f"arn:aws:states:{REGION}:{get_account_id()}"
                f":execution:{sm['name']}:{name}")

    start_date = now_iso()
    input_str = data.get("input", "{}")

    _executions[exec_arn] = {
        "executionArn": exec_arn,
        "stateMachineArn": sm_arn,
        "name": name,
        "status": "RUNNING",
        "startDate": start_date,
        "stopDate": None,
        "input": input_str,
        "inputDetails": {"included": True},
        "output": None,
        "outputDetails": {"included": True},
        "testCase": test_case,
        "mockAttempts": {},
        "events": [
            {"id": 1, "type": "ExecutionStarted", "timestamp": start_date,
             "executionStartedEventDetails": {
                 "input": input_str, "roleArn": sm["roleArn"]}},
        ],
    }

    threading.Thread(
        target=_run_execution, args=(exec_arn,), daemon=True).start()

    logger.info("Step Functions execution started: %s", exec_arn)
    return json_response({"executionArn": exec_arn, "startDate": start_date})


def _stop_execution(data):
    exec_arn = data.get("executionArn")
    execution = _executions.get(exec_arn)
    if not execution:
        return error_response_json(
            "ExecutionDoesNotExist",
            f"Execution {exec_arn} not found", 400)
    if execution["status"] != "RUNNING":
        return error_response_json(
            "ValidationException", "Execution is not running", 400)

    stop_date = now_iso()
    execution["status"] = "ABORTED"
    execution["stopDate"] = stop_date
    _add_event(execution, "ExecutionAborted", {
        "executionAbortedEventDetails": {
            "error": data.get("error", ""),
            "cause": data.get("cause", ""),
        },
    })
    return json_response({"stopDate": stop_date})


def _describe_execution(data):
    exec_arn = data.get("executionArn")
    execution = _executions.get(exec_arn)
    if not execution:
        return error_response_json(
            "ExecutionDoesNotExist",
            f"Execution {exec_arn} not found", 400)
    result = {
        "executionArn": execution["executionArn"],
        "stateMachineArn": execution["stateMachineArn"],
        "name": execution["name"],
        "status": execution["status"],
        "startDate": execution["startDate"],
        "stopDate": execution["stopDate"],
        "input": execution["input"],
        "inputDetails": execution.get("inputDetails", {"included": True}),
        "output": execution["output"],
        "outputDetails": execution.get("outputDetails", {"included": True}),
    }
    return json_response(result)


def _list_executions(data):
    sm_arn = data.get("stateMachineArn")
    status_filter = data.get("statusFilter")
    all_execs = []
    for ex in _executions.values():
        if sm_arn and ex["stateMachineArn"] != sm_arn:
            continue
        if status_filter and ex["status"] != status_filter:
            continue
        all_execs.append({
            "executionArn": ex["executionArn"],
            "stateMachineArn": ex["stateMachineArn"],
            "name": ex["name"],
            "status": ex["status"],
            "startDate": ex["startDate"],
            "stopDate": ex.get("stopDate"),
        })
    max_results = int(data.get("maxResults", 1000))
    next_token = data.get("nextToken")
    start = 0
    if next_token:
        try:
            start = int(next_token)
        except ValueError:
            start = 0
    page = all_execs[start:start + max_results]
    resp = {"executions": page}
    if start + max_results < len(all_execs):
        resp["nextToken"] = str(start + max_results)
    return json_response(resp)


def _get_execution_history(data):
    exec_arn = data.get("executionArn")
    execution = _executions.get(exec_arn)
    if not execution:
        return error_response_json(
            "ExecutionDoesNotExist",
            f"Execution {exec_arn} not found", 400)
    events = list(execution["events"])
    if data.get("reverseOrder", False):
        events = list(reversed(events))
    max_results = data.get("maxResults", 1000)
    return json_response({"events": events[:max_results]})


def _start_sync_execution(data):
    sm_arn_raw = data.get("stateMachineArn", "")
    test_case = ""
    if "#" in sm_arn_raw:
        sm_arn, test_case = sm_arn_raw.rsplit("#", 1)
    else:
        sm_arn = sm_arn_raw
    if sm_arn not in _state_machines:
        return error_response_json(
            "StateMachineDoesNotExist",
            f"State machine {sm_arn} not found", 400)

    sm = _state_machines[sm_arn]
    name = data.get("name") or new_uuid()
    exec_arn = (f"arn:aws:states:{REGION}:{get_account_id()}"
                f":execution:{sm['name']}:{name}")

    start_date = now_iso()
    input_str = data.get("input", "{}")

    _executions[exec_arn] = {
        "executionArn": exec_arn,
        "stateMachineArn": sm_arn,
        "name": name,
        "status": "RUNNING",
        "startDate": start_date,
        "stopDate": None,
        "input": input_str,
        "inputDetails": {"included": True},
        "output": None,
        "outputDetails": {"included": True},
        "testCase": test_case,
        "mockAttempts": {},
        "events": [
            {"id": 1, "type": "ExecutionStarted", "timestamp": start_date,
             "executionStartedEventDetails": {
                 "input": input_str, "roleArn": sm["roleArn"]}},
        ],
    }

    _run_execution(exec_arn)

    execution = _executions[exec_arn]
    resp = {
        "executionArn": exec_arn,
        "stateMachineArn": sm_arn,
        "name": name,
        "startDate": start_date,
        "stopDate": execution.get("stopDate") or now_iso(),
        "status": execution["status"],
        "input": input_str,
        "inputDetails": {"included": True},
        "output": execution.get("output") or "{}",
        "outputDetails": {"included": True},
    }
    # Include error/cause for failed executions (matches AWS SFN behaviour)
    if execution["status"] == "FAILED":
        failed_events = [
            e for e in execution.get("events", [])
            if e.get("type") == "ExecutionFailed"
        ]
        if failed_events:
            details = failed_events[-1].get("executionFailedEventDetails", {})
            resp["error"] = details.get("error", "")
            resp["cause"] = details.get("cause", "")
    return json_response(resp)


def _describe_state_machine_for_execution(data):
    exec_arn = data.get("executionArn")
    execution = _executions.get(exec_arn)
    if not execution:
        return error_response_json(
            "ExecutionDoesNotExist",
            f"Execution {exec_arn} not found", 400)

    sm_arn = execution["stateMachineArn"]
    sm = _state_machines.get(sm_arn)
    if not sm:
        return error_response_json(
            "StateMachineDoesNotExist",
            f"State machine {sm_arn} not found", 400)

    return json_response({
        "stateMachineArn": sm["stateMachineArn"],
        "name": sm["name"],
        "definition": sm["definition"],
        "roleArn": sm["roleArn"],
        "updateDate": sm.get("creationDate", now_iso()),
    })


# ---------------------------------------------------------------------------
# Callback pattern — SendTask*
# ---------------------------------------------------------------------------

def _send_task_success(data):
    token = data.get("taskToken")
    output = data.get("output", "{}")
    info = _task_tokens.get(token)
    if not info:
        return error_response_json(
            "TaskDoesNotExist", "Task token not found", 400)
    info["result"] = output
    info["event"].set()
    return json_response({})


def _send_task_failure(data):
    token = data.get("taskToken")
    info = _task_tokens.get(token)
    if not info:
        return error_response_json(
            "TaskDoesNotExist", "Task token not found", 400)
    info["error"] = {
        "Error": data.get("error", "TaskFailed"),
        "Cause": data.get("cause", ""),
    }
    info["event"].set()
    return json_response({})


def _send_task_heartbeat(data):
    token = data.get("taskToken")
    info = _task_tokens.get(token)
    if not info:
        return error_response_json(
            "TaskDoesNotExist", "Task token not found", 400)
    info["heartbeat"] = now_iso()
    return json_response({})


# ---------------------------------------------------------------------------
# Activity CRUD
# ---------------------------------------------------------------------------

def _create_activity(data):
    name = data.get("name")
    if not name:
        return error_response_json("ValidationException", "name is required", 400)

    arn = f"arn:aws:states:{REGION}:{get_account_id()}:activity:{name}"
    if arn in _activities:
        return error_response_json(
            "ActivityAlreadyExists", f"Activity already exists: {arn}", 400)

    ts = now_iso()
    _activities[arn] = {"activityArn": arn, "name": name, "creationDate": ts}
    _activity_tasks[arn] = []

    tags = data.get("tags", [])
    if tags:
        _tags[arn] = list(tags)

    return json_response({"activityArn": arn, "creationDate": ts})


def _delete_activity(data):
    arn = data.get("activityArn")
    if arn not in _activities:
        return error_response_json(
            "ActivityDoesNotExist", f"Activity {arn} not found", 400)
    del _activities[arn]
    _activity_tasks.pop(arn, None)
    _tags.pop(arn, None)
    return json_response({})


def _describe_activity(data):
    arn = data.get("activityArn")
    act = _activities.get(arn)
    if not act:
        return error_response_json(
            "ActivityDoesNotExist", f"Activity {arn} not found", 400)
    return json_response(act)


def _list_activities(data):
    acts = [
        {"activityArn": a["activityArn"], "name": a["name"], "creationDate": a["creationDate"]}
        for a in _activities.values()
    ]
    return json_response({"activities": acts})


async def _get_activity_task(data):
    arn = data.get("activityArn")
    if arn not in _activities:
        return error_response_json(
            "ActivityDoesNotExist", f"Activity {arn} not found", 400)

    queue = _activity_tasks.get(arn, [])
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        if queue:
            task = queue.pop(0)
            return json_response({"taskToken": task["taskToken"], "input": task["input"]})
        await asyncio.sleep(0.5)

    return json_response({})


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------

def _tag_resource(data):
    arn = data.get("resourceArn")
    new_tags = data.get("tags", [])
    existing = _tags.setdefault(arn, [])
    existing_map = {t["key"]: i for i, t in enumerate(existing)}
    for tag in new_tags:
        idx = existing_map.get(tag["key"])
        if idx is not None:
            existing[idx] = tag
        else:
            existing.append(tag)
            existing_map[tag["key"]] = len(existing) - 1
    return json_response({})


def _untag_resource(data):
    arn = data.get("resourceArn")
    keys_to_remove = set(data.get("tagKeys", []))
    existing = _tags.get(arn, [])
    _tags[arn] = [t for t in existing if t["key"] not in keys_to_remove]
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("resourceArn")
    return json_response({"tags": _tags.get(arn, [])})


# ---------------------------------------------------------------------------
# Event helper
# ---------------------------------------------------------------------------

def _add_event(execution, event_type, details=None):
    event = {
        "id": len(execution["events"]) + 1,
        "type": event_type,
        "timestamp": now_iso(),
    }
    if details:
        event.update(details)
    execution["events"].append(event)
    return event


# ---------------------------------------------------------------------------
# TestState API
# ---------------------------------------------------------------------------

def _test_state(data):
    """Execute a single state in isolation — AWS TestState API."""
    definition_str = data.get("definition")
    if not definition_str:
        return error_response_json("InvalidDefinition", "definition is required", 400)

    try:
        definition = json.loads(definition_str) if isinstance(definition_str, str) else definition_str
    except json.JSONDecodeError:
        return error_response_json("InvalidDefinition", "Invalid JSON in definition", 400)

    input_str = data.get("input", "{}")
    try:
        input_data = json.loads(input_str) if isinstance(input_str, str) else input_str
    except json.JSONDecodeError:
        return error_response_json("InvalidExecutionInput", "Invalid JSON in input", 400)

    inspection_level = data.get("inspectionLevel", "INFO")
    state_name = data.get("stateName")
    mock_raw = data.get("mock")
    if isinstance(mock_raw, str):
        try:
            mock = json.loads(mock_raw)
        except json.JSONDecodeError:
            mock = None
    else:
        mock = mock_raw

    # If definition has States (full SM definition), extract the target state
    if "States" in definition:
        if not state_name:
            state_name = definition.get("StartAt")
        states = definition.get("States", {})
        if state_name not in states:
            return error_response_json("InvalidDefinition",
                f"State '{state_name}' not found in definition", 400)
        state_def = states[state_name]
    else:
        # Single state definition
        state_def = definition
        if not state_name:
            state_name = "TestState"

    state_type = state_def.get("Type")
    if not state_type:
        return error_response_json("InvalidDefinition", "State must have a Type", 400)

    # Build context
    user_ctx = data.get("context")
    if user_ctx:
        try:
            ctx = json.loads(user_ctx) if isinstance(user_ctx, str) else user_ctx
        except json.JSONDecodeError:
            ctx = {}
    else:
        ctx = {}
    ctx.setdefault("Execution", {"Id": f"arn:aws:states:{REGION}:{get_account_id()}:execution:test:{new_uuid()}", "Name": "test", "StartTime": now_iso()})
    ctx.setdefault("StateMachine", {"Id": "test", "Name": "test"})
    ctx["State"] = {"Name": state_name, "EnteredTime": now_iso()}

    inspection_data = {}
    if inspection_level in ("DEBUG", "TRACE"):
        inspection_data["input"] = json.dumps(input_data)

    result = {}
    try:
        if state_type == "Pass":
            output, next_state = _execute_pass(state_def, input_data)
            result = {"status": "SUCCEEDED", "output": json.dumps(output)}
            if next_state:
                result["nextState"] = next_state

        elif state_type == "Choice":
            output, next_state = _execute_choice(state_def, input_data)
            result = {"status": "SUCCEEDED", "output": json.dumps(output)}
            if next_state:
                result["nextState"] = next_state

        elif state_type == "Wait":
            output, next_state = _execute_wait(state_def, input_data)
            result = {"status": "SUCCEEDED", "output": json.dumps(output)}
            if next_state:
                result["nextState"] = next_state

        elif state_type == "Succeed":
            output = _apply_input_path(state_def, input_data)
            output = _apply_output_path(state_def, output)
            result = {"status": "SUCCEEDED", "output": json.dumps(output)}

        elif state_type == "Fail":
            result = {
                "status": "FAILED",
                "error": state_def.get("Error", "States.Fail"),
                "cause": state_def.get("Cause", ""),
            }

        elif state_type == "Task":
            effective = _apply_input_path(state_def, input_data)
            effective = _apply_parameters(state_def, effective, ctx)

            if inspection_level in ("DEBUG", "TRACE"):
                inspection_data["afterInputPath"] = json.dumps(_apply_input_path(state_def, input_data))
                inspection_data["afterParameters"] = json.dumps(effective)

            # Mock support
            if mock:
                if "errorOutput" in mock:
                    err = mock["errorOutput"]
                    error_code = err.get("error", "MockError")
                    cause = err.get("cause", "Mocked failure")
                    # Check Catch
                    catchers = state_def.get("Catch", [])
                    catcher = _find_matching_catcher(catchers, error_code)
                    if catcher:
                        error_output = {"Error": error_code, "Cause": cause}
                        output = _apply_result_path_raw(
                            catcher.get("ResultPath", "$"), input_data, error_output)
                        result = {
                            "status": "CAUGHT_ERROR",
                            "output": json.dumps(output),
                            "error": error_code,
                            "cause": cause,
                            "nextState": catcher["Next"],
                        }
                    else:
                        # Check Retry
                        retriers = state_def.get("Retry", [])
                        retrier, _ = _find_matching_retrier(retriers, error_code, {})
                        if retrier is not None:
                            retry_config = data.get("stateConfiguration", {})
                            retry_count = retry_config.get("retrierRetryCount", 0)
                            max_attempts = retrier.get("MaxAttempts", 3)
                            if retry_count < max_attempts:
                                interval = retrier.get("IntervalSeconds", 1)
                                backoff = retrier.get("BackoffRate", 2.0)
                                result = {
                                    "status": "RETRIABLE",
                                    "error": error_code,
                                    "cause": cause,
                                }
                                if inspection_level in ("DEBUG", "TRACE"):
                                    inspection_data["errorDetails"] = {
                                        "retryBackoffIntervalSeconds": interval * (backoff ** retry_count),
                                        "retryIndex": 0,
                                    }
                            else:
                                result = {"status": "FAILED", "error": error_code, "cause": cause}
                        else:
                            result = {"status": "FAILED", "error": error_code, "cause": cause}
                elif "result" in mock:
                    try:
                        mock_result = json.loads(mock["result"]) if isinstance(mock["result"], str) else mock["result"]
                    except json.JSONDecodeError:
                        mock_result = mock["result"]
                    task_result = _apply_result_selector(state_def, mock_result)
                    output = _apply_result_path(state_def, input_data, task_result)
                    output = _apply_output_path(state_def, output)
                    result = {"status": "SUCCEEDED", "output": json.dumps(output)}
                    next_state = _next_or_end(state_def)
                    if next_state:
                        result["nextState"] = next_state
            else:
                # Real execution
                resource = state_def.get("Resource", "")
                try:
                    task_result = _invoke_resource(resource, effective)
                    task_result = _apply_result_selector(state_def, task_result)

                    if inspection_level in ("DEBUG", "TRACE"):
                        inspection_data["result"] = json.dumps(task_result)
                        inspection_data["afterResultSelector"] = json.dumps(task_result)

                    output = _apply_result_path(state_def, input_data, task_result)

                    if inspection_level in ("DEBUG", "TRACE"):
                        inspection_data["afterResultPath"] = json.dumps(output)

                    output = _apply_output_path(state_def, output)
                    result = {"status": "SUCCEEDED", "output": json.dumps(output)}
                    next_state = _next_or_end(state_def)
                    if next_state:
                        result["nextState"] = next_state
                except _ExecutionError as err:
                    catchers = state_def.get("Catch", [])
                    catcher = _find_matching_catcher(catchers, err.error)
                    if catcher:
                        error_output = {"Error": err.error, "Cause": err.cause}
                        output = _apply_result_path_raw(
                            catcher.get("ResultPath", "$"), input_data, error_output)
                        result = {
                            "status": "CAUGHT_ERROR",
                            "output": json.dumps(output),
                            "error": err.error,
                            "cause": err.cause,
                            "nextState": catcher["Next"],
                        }
                    else:
                        result = {"status": "FAILED", "error": err.error, "cause": err.cause}
        else:
            return error_response_json("InvalidDefinition", f"Unsupported state type: {state_type}", 400)

    except _ExecutionError as err:
        result = {"status": "FAILED", "error": err.error, "cause": err.cause}

    if inspection_level in ("DEBUG", "TRACE") and inspection_data:
        result["inspectionData"] = inspection_data

    return json_response(result)


# ---------------------------------------------------------------------------
# ValidateStateMachineDefinition API
# ---------------------------------------------------------------------------

def _validate_state_machine_definition(data):
    return json_response({"result": "OK", "diagnostics": []})


# ===================================================================
# ASL Execution Engine
# ===================================================================

class _ExecutionError(Exception):
    def __init__(self, error, cause):
        self.error = error
        self.cause = cause
        super().__init__(f"{error}: {cause}")


def _run_execution(exec_arn):
    """Background thread: walk the ASL definition to completion."""
    execution = _executions.get(exec_arn)
    if not execution:
        return

    time.sleep(0.15)

    sm = _state_machines.get(execution["stateMachineArn"])
    if not sm:
        _fail_execution(execution, "StateMachineDeleted",
                        "State machine no longer exists")
        return

    try:
        definition = json.loads(sm["definition"])
    except json.JSONDecodeError:
        _fail_execution(execution, "InvalidDefinition",
                        "Could not parse state machine definition")
        return

    all_states = definition.get("States", {})
    current_name = definition.get("StartAt")
    if not current_name or current_name not in all_states:
        _fail_execution(execution, "InvalidDefinition",
                        f"StartAt state '{current_name}' not found")
        return

    try:
        current_input = json.loads(execution["input"])
    except json.JSONDecodeError:
        current_input = {}

    ctx = {
        "Execution": {
            "Id": exec_arn,
            "Input": current_input,
            "Name": execution["name"],
            "StartTime": execution["startDate"],
        },
        "StateMachine": {
            "Id": execution["stateMachineArn"],
            "Name": sm["name"],
        },
    }

    try:
        while current_name and execution["status"] == "RUNNING":
            state_def = all_states.get(current_name)
            if not state_def:
                raise _ExecutionError(
                    "States.Runtime",
                    f"State '{current_name}' not found in definition")

            ctx["State"] = {"Name": current_name, "EnteredTime": now_iso()}
            state_type = state_def.get("Type")

            _add_event(execution, f"{state_type}StateEntered", {
                "stateEnteredEventDetails": {
                    "name": current_name,
                    "input": json.dumps(current_input),
                },
            })

            if state_type == "Succeed":
                current_input = _apply_input_path(state_def, current_input)
                current_input = _apply_output_path(state_def, current_input)
                _add_event(execution, "SucceedStateExited", {
                    "stateExitedEventDetails": {
                        "name": current_name,
                        "output": json.dumps(current_input),
                    },
                })
                current_name = None
                continue

            if state_type == "Fail":
                raise _ExecutionError(
                    state_def.get("Error", "States.Fail"),
                    state_def.get("Cause", ""))

            handler_fn = {
                "Pass": _execute_pass,
                "Task": _execute_task,
                "Choice": _execute_choice,
                "Wait": _execute_wait,
                "Parallel": _execute_parallel,
                "Map": _execute_map,
            }.get(state_type)

            if not handler_fn:
                raise _ExecutionError(
                    "States.Runtime", f"Unknown state type: {state_type}")

            if state_type in ("Task", "Parallel", "Map"):
                current_input, next_name = handler_fn(
                    state_def, current_input, execution, ctx)
            else:
                current_input, next_name = handler_fn(
                    state_def, current_input)

            _add_event(execution, f"{state_type}StateExited", {
                "stateExitedEventDetails": {
                    "name": current_name,
                    "output": json.dumps(current_input),
                },
            })
            current_name = next_name

        if execution["status"] == "RUNNING":
            output_json = json.dumps(current_input)
            execution["status"] = "SUCCEEDED"
            execution["output"] = output_json
            execution["stopDate"] = now_iso()
            _add_event(execution, "ExecutionSucceeded", {
                "executionSucceededEventDetails": {"output": output_json},
            })

    except _ExecutionError as err:
        _fail_execution(execution, err.error, err.cause)
    except Exception as exc:
        logger.exception("Unexpected error in execution %s", exec_arn)
        _fail_execution(execution, "States.Runtime", str(exc))


def _fail_execution(execution, error, cause):
    execution["status"] = "FAILED"
    execution["output"] = json.dumps({"Error": error, "Cause": cause})
    execution["stopDate"] = now_iso()
    _add_event(execution, "ExecutionFailed", {
        "executionFailedEventDetails": {"error": error, "cause": cause},
    })


# ---------------------------------------------------------------------------
# Pass state
# ---------------------------------------------------------------------------

def _execute_pass(state_def, raw_input):
    effective = _apply_input_path(state_def, raw_input)
    effective = _apply_parameters(state_def, effective)

    result = state_def.get("Result", effective)
    result = _apply_result_selector(state_def, result)
    output = _apply_result_path(state_def, raw_input, result)
    output = _apply_output_path(state_def, output)
    return output, _next_or_end(state_def)


# ---------------------------------------------------------------------------
# Task state (with Retry / Catch)
# ---------------------------------------------------------------------------

def _execute_task(state_def, raw_input, execution, ctx):
    resource = state_def.get("Resource", "")
    is_callback = ".waitForTaskToken" in resource

    # SFN mock config — return canned response if configured (AWS SFN Local format)
    if _sfn_mock_config and execution:
        test_case = execution.get("testCase", "")
        sm_name = ctx.get("StateMachine", {}).get("Name", "")
        state_name = ctx.get("State", {}).get("Name", "")
        attempts = execution.get("mockAttempts", {})
        attempt = attempts.get(state_name, 0)
        mock = _get_mock_response(sm_name, test_case, state_name, attempt)
        if mock is not None:
            attempts[state_name] = attempt + 1
            if "Throw" in mock:
                raise _ExecutionError(
                    mock["Throw"].get("Error", "MockError"),
                    mock["Throw"].get("Cause", "Mocked failure"))
            mock_result = mock.get("Return", {})
            result = _apply_result_selector(state_def, mock_result)
            output = _apply_result_path(state_def, raw_input, result)
            output = _apply_output_path(state_def, output)
            return output, _next_or_end(state_def)

    if is_callback:
        ctx["Task"] = {"Token": new_uuid()}

    effective = _apply_input_path(state_def, raw_input)
    effective = _apply_parameters(state_def, effective, ctx)

    retriers = state_def.get("Retry", [])
    catchers = state_def.get("Catch", [])
    retry_counts: dict = {}
    last_error: _ExecutionError | None = None

    while True:
        try:
            _add_event(execution, "TaskScheduled", {
                "taskScheduledEventDetails": {
                    "resourceType": "lambda" if "lambda" in resource else "states",
                    "resource": resource,
                },
            })

            if is_callback:
                task_result = _invoke_with_callback(
                    resource, effective, ctx["Task"]["Token"], state_def)
            else:
                task_result = _invoke_resource(resource, effective)

            _add_event(execution, "TaskSucceeded", {
                "taskSucceededEventDetails": {
                    "output": json.dumps(task_result),
                    "resource": resource,
                },
            })

            result = _apply_result_selector(state_def, task_result)
            output = _apply_result_path(state_def, raw_input, result)
            output = _apply_output_path(state_def, output)
            return output, _next_or_end(state_def)

        except _ExecutionError as err:
            last_error = err
            _add_event(execution, "TaskFailed", {
                "taskFailedEventDetails": {
                    "error": err.error, "cause": err.cause,
                    "resource": resource,
                },
            })

            retrier, retrier_idx = _find_matching_retrier(
                retriers, err.error, retry_counts)
            if retrier is not None:
                count = retry_counts.get(retrier_idx, 0)
                interval = retrier.get("IntervalSeconds", 1)
                backoff = retrier.get("BackoffRate", 2.0)
                sleep_sec = interval * (backoff ** count)
                time.sleep(min(sleep_sec, 60))
                retry_counts[retrier_idx] = count + 1
                continue
            break

    if last_error:
        catcher = _find_matching_catcher(catchers, last_error.error)
        if catcher:
            error_output = {"Error": last_error.error, "Cause": last_error.cause}
            output = _apply_result_path_raw(
                catcher.get("ResultPath", "$"), raw_input, error_output)
            return output, catcher["Next"]
        raise last_error

    raise _ExecutionError("States.Runtime", "Task failed with no error captured")


def _invoke_resource(resource, input_data):
    """Dispatch to Lambda or return a mock/passthrough."""
    if "states:::lambda:invoke" in resource:
        func_name = input_data.get("FunctionName", "")
        payload = input_data.get("Payload", input_data)
        if ":function:" in func_name:
            func_name = func_name.split(":function:")[-1].split(":")[0]
        result = _call_lambda(func_name, payload)
        return {"StatusCode": 200, "Payload": result}

    func_name = _extract_lambda_name(resource)
    if func_name:
        return _call_lambda(func_name, input_data)

    # Activity resource — enqueue task and wait for worker to call GetActivityTask + SendTask*
    if ":activity:" in resource:
        return _invoke_activity(resource, input_data)

    if resource.startswith("arn:aws:states:::states:startExecution.sync"):
        return _invoke_nested_start_execution(resource, input_data)

    # Service integration dispatch
    clean = resource.replace(".sync", "").replace(".waitForTaskToken", "")
    for prefix, handler in _SERVICE_DISPATCH.items():
        if clean.startswith(prefix):
            return handler(resource, input_data)

    # Generic aws-sdk:* service integration
    if "aws-sdk:" in resource:
        return _invoke_aws_sdk_integration(resource, input_data)

    return input_data


def _invoke_activity(resource, input_data):
    """Enqueue a task for the activity worker and block until SendTaskSuccess/Failure."""
    arn = resource
    if arn not in _activities:
        raise _ExecutionError(
            "ActivityDoesNotExist", f"Activity {arn} not found")

    token = new_uuid()
    evt = threading.Event()
    _task_tokens[token] = {"event": evt, "result": None, "error": None, "heartbeat": None}

    _activity_tasks[arn].append({
        "taskToken": token,
        "input": json.dumps(input_data),
    })

    timeout = 99999
    if not evt.wait(timeout=timeout):
        _task_tokens.pop(token, None)
        raise _ExecutionError("States.Timeout", "Activity task timed out waiting for worker")

    info = _task_tokens.pop(token, {})
    if info.get("error"):
        e = info["error"]
        raise _ExecutionError(e.get("Error", "TaskFailed"), e.get("Cause", ""))
    result_raw = info.get("result", "{}")
    try:
        return json.loads(result_raw) if isinstance(result_raw, str) else result_raw
    except json.JSONDecodeError:
        return result_raw


def _invoke_with_callback(resource, input_data, token, state_def):
    """waitForTaskToken pattern: invoke then block until callback."""
    evt = threading.Event()
    _task_tokens[token] = {
        "event": evt, "result": None, "error": None, "heartbeat": None}

    clean_resource = resource.replace(".waitForTaskToken", "")
    func_name = _extract_lambda_name(clean_resource)
    if not func_name and "states:::lambda:invoke" in clean_resource:
        func_name = input_data.get("FunctionName", "")
        if ":function:" in func_name:
            func_name = func_name.split(":function:")[-1].split(":")[0]

    if func_name:
        try:
            _call_lambda(func_name, input_data)
        except _ExecutionError:
            pass

    timeout = state_def.get("TimeoutSeconds", 99999)
    if not evt.wait(timeout=timeout):
        _task_tokens.pop(token, None)
        raise _ExecutionError("States.Timeout",
                              "Task timed out waiting for callback")

    info = _task_tokens.pop(token, {})
    if info.get("error"):
        e = info["error"]
        raise _ExecutionError(e.get("Error", "TaskFailed"),
                              e.get("Cause", ""))
    result_raw = info.get("result", "{}")
    try:
        return json.loads(result_raw) if isinstance(result_raw, str) else result_raw
    except json.JSONDecodeError:
        return result_raw


def _call_lambda(func_name, event):
    """Invoke a Lambda via the co-located lambda_svc module (synchronous)."""
    try:
        from ministack.services import lambda_svc
    except ImportError:
        logger.warning("lambda_svc unavailable; returning passthrough for %s", func_name)
        return event

    func = lambda_svc._functions.get(func_name)
    if not func:
        raise _ExecutionError(
            "Lambda.ResourceNotFoundException",
            f"Function not found: {func_name}")

    result = lambda_svc._execute_function(func, event)

    if result.get("error"):
        body = result.get("body", {})
        if isinstance(body, dict):
            raise _ExecutionError(
                body.get("errorType", "Lambda.Unknown"),
                body.get("errorMessage", str(body)))
        raise _ExecutionError("Lambda.Unknown", str(body))

    body = result.get("body")
    if body is None:
        return {}
    if isinstance(body, (dict, list)):
        return body
    try:
        return json.loads(body) if isinstance(body, (str, bytes)) else body
    except (json.JSONDecodeError, TypeError):
        return body


# ---------------------------------------------------------------------------
# Choice state
# ---------------------------------------------------------------------------

def _execute_choice(state_def, raw_input):
    effective = _apply_input_path(state_def, raw_input)

    for choice in state_def.get("Choices", []):
        if _evaluate_rule(choice, effective):
            return _apply_output_path(state_def, effective), choice["Next"]

    default = state_def.get("Default")
    if default:
        return _apply_output_path(state_def, effective), default

    raise _ExecutionError("States.NoChoiceMatched",
                          "No choice rule matched and no Default")


def _evaluate_rule(rule, data):
    if "And" in rule:
        return all(_evaluate_rule(r, data) for r in rule["And"])
    if "Or" in rule:
        return any(_evaluate_rule(r, data) for r in rule["Or"])
    if "Not" in rule:
        return not _evaluate_rule(rule["Not"], data)

    variable = rule.get("Variable")
    if not variable:
        return False
    value = _resolve_path(variable, data)

    # --- type checks ---
    if "IsPresent" in rule:
        return (value is not None) == rule["IsPresent"]
    if "IsNull" in rule:
        return (value is None) == rule["IsNull"]
    if "IsNumeric" in rule:
        return isinstance(value, (int, float)) == rule["IsNumeric"]
    if "IsString" in rule:
        return isinstance(value, str) == rule["IsString"]
    if "IsBoolean" in rule:
        return isinstance(value, bool) == rule["IsBoolean"]
    if "IsTimestamp" in rule:
        return _is_timestamp(value) == rule["IsTimestamp"]

    # --- string ---
    if "StringEquals" in rule:
        return value == rule["StringEquals"]
    if "StringEqualsPath" in rule:
        return value == _resolve_path(rule["StringEqualsPath"], data)
    if "StringLessThan" in rule:
        return isinstance(value, str) and value < rule["StringLessThan"]
    if "StringGreaterThan" in rule:
        return isinstance(value, str) and value > rule["StringGreaterThan"]
    if "StringLessThanEquals" in rule:
        return isinstance(value, str) and value <= rule["StringLessThanEquals"]
    if "StringGreaterThanEquals" in rule:
        return isinstance(value, str) and value >= rule["StringGreaterThanEquals"]
    if "StringMatches" in rule:
        pattern = re.escape(rule["StringMatches"]).replace(r"\*", ".*")
        return isinstance(value, str) and bool(re.fullmatch(pattern, value))

    # --- numeric ---
    if "NumericEquals" in rule:
        return _is_num(value) and value == rule["NumericEquals"]
    if "NumericEqualsPath" in rule:
        return _is_num(value) and value == _resolve_path(rule["NumericEqualsPath"], data)
    if "NumericLessThan" in rule:
        return _is_num(value) and value < rule["NumericLessThan"]
    if "NumericGreaterThan" in rule:
        return _is_num(value) and value > rule["NumericGreaterThan"]
    if "NumericLessThanEquals" in rule:
        return _is_num(value) and value <= rule["NumericLessThanEquals"]
    if "NumericGreaterThanEquals" in rule:
        return _is_num(value) and value >= rule["NumericGreaterThanEquals"]

    # --- boolean ---
    if "BooleanEquals" in rule:
        return value is rule["BooleanEquals"] or value == rule["BooleanEquals"]
    if "BooleanEqualsPath" in rule:
        return value == _resolve_path(rule["BooleanEqualsPath"], data)

    # --- timestamp ---
    for op, cmp_fn in [("TimestampEquals", lambda a, b: a == b),
                       ("TimestampLessThan", lambda a, b: a < b),
                       ("TimestampGreaterThan", lambda a, b: a > b),
                       ("TimestampLessThanEquals", lambda a, b: a <= b),
                       ("TimestampGreaterThanEquals", lambda a, b: a >= b)]:
        if op in rule:
            a, b = _parse_ts(value), _parse_ts(rule[op])
            return a is not None and b is not None and cmp_fn(a, b)

    return False


# ---------------------------------------------------------------------------
# Wait state
# ---------------------------------------------------------------------------

def _execute_wait(state_def, raw_input):
    effective = _apply_input_path(state_def, raw_input)

    if "Seconds" in state_def:
        time.sleep(state_def["Seconds"])
    elif "Timestamp" in state_def:
        _sleep_until(state_def["Timestamp"])
    elif "SecondsPath" in state_def:
        secs = _resolve_path(state_def["SecondsPath"], effective)
        if isinstance(secs, (int, float)) and secs > 0:
            time.sleep(secs)
    elif "TimestampPath" in state_def:
        ts_str = _resolve_path(state_def["TimestampPath"], effective)
        if isinstance(ts_str, str):
            _sleep_until(ts_str)

    output = _apply_output_path(state_def, effective)
    return output, _next_or_end(state_def)


def _sleep_until(iso_ts):
    try:
        target = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        delta = (target - datetime.now(timezone.utc)).total_seconds()
        if delta > 0:
            time.sleep(delta)
    except (ValueError, TypeError):
        pass


# ---------------------------------------------------------------------------
# Parallel state
# ---------------------------------------------------------------------------

def _execute_parallel(state_def, raw_input, execution, ctx):
    effective = _apply_input_path(state_def, raw_input)
    effective = _apply_parameters(state_def, effective, ctx)

    branches = state_def.get("Branches", [])
    results = [None] * len(branches)
    errors = [None] * len(branches)

    def run_branch(idx, branch):
        try:
            results[idx] = _run_sub_machine(
                branch.get("States", {}),
                branch.get("StartAt"),
                effective, execution, ctx)
        except Exception as exc:
            errors[idx] = exc

    threads = [threading.Thread(target=run_branch, args=(i, b), daemon=True)
               for i, b in enumerate(branches)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    for err in errors:
        if err is not None:
            raise err if isinstance(err, _ExecutionError) else _ExecutionError(
                "States.BranchFailed", str(err))

    result = _apply_result_selector(state_def, results)
    output = _apply_result_path(state_def, raw_input, result)
    output = _apply_output_path(state_def, output)
    return output, _next_or_end(state_def)


# ---------------------------------------------------------------------------
# Map state
# ---------------------------------------------------------------------------

def _execute_map(state_def, raw_input, execution, ctx):
    effective = _apply_input_path(state_def, raw_input)
    effective = _apply_parameters(state_def, effective, ctx)

    items_path = state_def.get("ItemsPath", "$")
    items = _resolve_path(items_path, effective)
    if not isinstance(items, list):
        items = [items]

    iterator = state_def.get("Iterator") or state_def.get("ItemProcessor", {})
    iter_states = iterator.get("States", {})
    iter_start = iterator.get("StartAt")
    max_conc = state_def.get("MaxConcurrency", 0)

    results = [None] * len(items)
    errors = [None] * len(items)

    def run_item(idx, item):
        try:
            item_ctx = copy.deepcopy(ctx)
            item_ctx["Map"] = {"Item": {"Index": idx, "Value": item}}
            item_params = state_def.get("ItemSelector") or state_def.get("Parameters")
            # ItemSelector $ paths resolve against the Map state's effective input,
            # not the individual item. $$.Map.Item.Value provides the item.
            item_input = (_resolve_params_obj(item_params, effective, item_ctx)
                          if item_params else item)
            results[idx] = _run_sub_machine(
                iter_states, iter_start, item_input, execution, item_ctx)
        except Exception as exc:
            errors[idx] = exc

    workers = max_conc if max_conc > 0 else (len(items) or 1)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = [pool.submit(run_item, i, item) for i, item in enumerate(items)]
        futures_wait(futs)

    for err in errors:
        if err is not None:
            raise err if isinstance(err, _ExecutionError) else _ExecutionError(
                "States.MapFailed", str(err))

    result = _apply_result_selector(state_def, results)
    output = _apply_result_path(state_def, raw_input, result)
    output = _apply_output_path(state_def, output)
    return output, _next_or_end(state_def)


# ---------------------------------------------------------------------------
# Sub-machine runner (Parallel branches / Map iterations)
# ---------------------------------------------------------------------------

def _run_sub_machine(states, start_at, input_data, execution, ctx):
    current_name = start_at
    current_input = copy.deepcopy(input_data)

    while current_name:
        state_def = states.get(current_name)
        if not state_def:
            raise _ExecutionError(
                "States.Runtime", f"State '{current_name}' not found")

        state_type = state_def.get("Type")
        ctx["State"] = {"Name": current_name, "EnteredTime": now_iso()}

        if state_type == "Succeed":
            return _apply_output_path(state_def,
                                      _apply_input_path(state_def, current_input))
        if state_type == "Fail":
            raise _ExecutionError(
                state_def.get("Error", "States.Fail"),
                state_def.get("Cause", ""))

        handler_fn = {
            "Pass": _execute_pass,
            "Task": _execute_task,
            "Choice": _execute_choice,
            "Wait": _execute_wait,
            "Parallel": _execute_parallel,
            "Map": _execute_map,
        }.get(state_type)

        if not handler_fn:
            raise _ExecutionError(
                "States.Runtime", f"Unknown state type: {state_type}")

        if state_type in ("Task", "Parallel", "Map"):
            current_input, current_name = handler_fn(
                state_def, current_input, execution, ctx)
        else:
            current_input, current_name = handler_fn(
                state_def, current_input)

    return current_input


# ===================================================================
# Path / Parameter processing
# ===================================================================

def _apply_input_path(state_def, data):
    ip = state_def.get("InputPath", "$")
    if ip is None:
        return {}
    return _resolve_path(ip, data)


def _apply_output_path(state_def, data):
    op = state_def.get("OutputPath", "$")
    if op is None:
        return {}
    return _resolve_path(op, data)


def _apply_parameters(state_def, data, ctx=None):
    params = state_def.get("Parameters")
    if not params:
        return data
    return _resolve_params_obj(params, data, ctx)


def _apply_result_selector(state_def, data):
    sel = state_def.get("ResultSelector")
    if not sel:
        return data
    return _resolve_params_obj(sel, data)


def _apply_result_path(state_def, original, result):
    return _apply_result_path_raw(
        state_def.get("ResultPath", "$"), original, result)


def _apply_result_path_raw(result_path, original, result):
    if result_path is None:
        return copy.deepcopy(original)
    if result_path == "$":
        return result

    output = copy.deepcopy(original) if isinstance(original, dict) else {}
    parts = result_path.lstrip("$.").split(".")
    cur = output
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur.get(p), dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = result
    return output


def _resolve_path(path, data):
    if path == "$" or not path:
        return data
    if not path.startswith("$"):
        return data

    parts = path[2:].split(".") if path.startswith("$.") else []
    cur = data
    for part in parts:
        if not part:
            continue
        m = re.match(r"(\w+)\[(\d+)]", part)
        if m:
            field, idx = m.group(1), int(m.group(2))
            if isinstance(cur, dict) and field in cur:
                cur = cur[field]
                if isinstance(cur, list) and idx < len(cur):
                    cur = cur[idx]
                else:
                    return None
            else:
                return None
        elif isinstance(cur, dict) and part in cur:
            cur = cur[part]
        else:
            return None
    return cur


def _parse_intrinsic_args(s, pos):
    """Recursive descent parser for intrinsic function arguments.

    Returns (list_of_args, next_pos) where next_pos is after the closing ')'.
    """
    args = []
    pos = _skip_ws(s, pos)
    if pos < len(s) and s[pos] == ")":
        return args, pos + 1

    while pos < len(s):
        pos = _skip_ws(s, pos)
        if pos >= len(s):
            break

        ch = s[pos]

        if s[pos:].startswith("States."):
            arg, pos = _parse_intrinsic_call(s, pos)
            args.append(arg)
        elif ch == "'":
            end = s.index("'", pos + 1)
            args.append(("str", s[pos + 1 : end]))
            pos = end + 1
        elif ch == "$":
            end = pos
            while end < len(s) and s[end] not in (",", ")"):
                end += 1
            args.append(("path", s[pos:end].strip()))
            pos = end
        elif ch in "0123456789-":
            end = pos + 1
            while end < len(s) and s[end] not in (",", ")"):
                end += 1
            tok = s[pos:end].strip()
            if "." in tok:
                args.append(("num", float(tok)))
            else:
                args.append(("num", int(tok)))
            pos = end
        elif s[pos : pos + 4] == "true":
            args.append(("bool", True))
            pos += 4
        elif s[pos : pos + 5] == "false":
            args.append(("bool", False))
            pos += 5
        elif s[pos : pos + 4] == "null":
            args.append(("null", None))
            pos += 4
        else:
            pos += 1
            continue

        pos = _skip_ws(s, pos)
        if pos < len(s) and s[pos] == ",":
            pos += 1
        elif pos < len(s) and s[pos] == ")":
            return args, pos + 1

    return args, pos


def _skip_ws(s, pos):
    while pos < len(s) and s[pos] in " \t\n\r":
        pos += 1
    return pos


def _parse_intrinsic_call(s, pos):
    """Parse a States.Xxx(...) call starting at pos. Returns (('call', name, args), next_pos)."""
    paren = s.index("(", pos)
    name = s[pos:paren].strip()
    args, end = _parse_intrinsic_args(s, paren + 1)
    return ("call", name, args), end


def _eval_intrinsic_arg(arg, data, ctx):
    """Evaluate a single parsed argument node."""
    kind = arg[0]
    if kind == "str":
        return arg[1]
    elif kind == "num" or kind == "bool" or kind == "null":
        return arg[1]
    elif kind == "path":
        path = arg[1]
        if path.startswith("$$."):
            return _resolve_ctx_path(path, ctx or {})
        return _resolve_path(path, data)
    elif kind == "call":
        return _exec_intrinsic(arg, data, ctx)
    return None


def _exec_intrinsic(node, data, ctx):
    """Execute a parsed intrinsic call node ('call', name, args)."""
    _, name, raw_args = node
    args = [_eval_intrinsic_arg(a, data, ctx) for a in raw_args]

    if name == "States.StringToJson":
        return json.loads(args[0])
    elif name == "States.JsonToString":
        return json.dumps(args[0], separators=(",", ":"))
    elif name == "States.JsonMerge":
        merged = {}
        merged.update(args[0])
        merged.update(args[1])
        return merged
    elif name == "States.Format":
        template = args[0]
        parts = template.split("{}")
        result_parts = []
        for i, part in enumerate(parts):
            result_parts.append(part)
            if i < len(parts) - 1 and i < len(args) - 1:
                val = args[i + 1]
                result_parts.append(str(val) if not isinstance(val, str) else val)
        return "".join(result_parts)
    elif name == "States.ArrayGetItem":
        return args[0][int(args[1])]
    elif name == "States.Array":
        return list(args)
    elif name == "States.ArrayLength":
        return len(args[0])

    raise ValueError(f"Unsupported intrinsic function: {name}")


def _evaluate_intrinsic(expression, data, ctx):
    """Parse and evaluate a States.* intrinsic function expression."""
    node, _ = _parse_intrinsic_call(expression, 0)
    return _exec_intrinsic(node, data, ctx)


def _resolve_params_obj(template, data, ctx=None):
    if not isinstance(template, dict):
        return template
    result = {}
    for key, value in template.items():
        if key.endswith(".$"):
            real_key = key[:-2]
            if isinstance(value, str):
                if value.startswith("States."):
                    result[real_key] = _evaluate_intrinsic(value, data, ctx)
                elif value.startswith("$$."):
                    result[real_key] = _resolve_ctx_path(value, ctx or {})
                else:
                    result[real_key] = _resolve_path(value, data)
            else:
                result[real_key] = value
        elif isinstance(value, dict):
            result[key] = _resolve_params_obj(value, data, ctx)
        elif isinstance(value, list):
            result[key] = [
                _resolve_params_obj(v, data, ctx) if isinstance(v, dict) else v
                for v in value
            ]
        else:
            result[key] = value
    return result


def _resolve_ctx_path(path, ctx):
    if not path.startswith("$$."):
        return None
    parts = path[3:].split(".")
    cur = ctx
    for p in parts:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return None
    return cur


# ===================================================================
# Retry / Catch helpers
# ===================================================================

def _find_matching_retrier(retriers, error, retry_counts):
    for idx, retrier in enumerate(retriers):
        equals = retrier.get("ErrorEquals", [])
        max_attempts = retrier.get("MaxAttempts", 3)
        if retry_counts.get(idx, 0) >= max_attempts:
            continue
        if "States.ALL" in equals or "States.TaskFailed" in equals or error in equals:
            return retrier, idx
    return None, -1


def _find_matching_catcher(catchers, error):
    for catcher in catchers:
        equals = catcher.get("ErrorEquals", [])
        if "States.ALL" in equals or "States.TaskFailed" in equals or error in equals:
            return catcher
    return None


# ===================================================================
# Misc helpers
# ===================================================================

def _extract_lambda_name(resource):
    if not resource:
        return None
    if ":function:" in resource:
        return resource.split(":function:")[-1].split(":")[0]
    return None


def _next_or_end(state_def):
    if state_def.get("End"):
        return None
    return state_def.get("Next")


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_timestamp(v):
    if not isinstance(v, str):
        return False
    try:
        datetime.fromisoformat(v.replace("Z", "+00:00"))
        return True
    except (ValueError, TypeError):
        return False


def _parse_ts(v):
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            pass
    return None


# ===================================================================
# Service integrations (Task state dispatch)
# ===================================================================


def _invoke_nested_start_execution(resource, input_data):
    """Run a nested Step Functions execution and wait for the child result."""
    request = _nested_start_execution_request(input_data)
    status, _, body = _start_sync_execution(request)
    payload = json.loads(body) if body else {}

    if status >= 400:
        raise _ExecutionError(
            payload.get("__type", "States.Runtime"),
            payload.get("message", "Nested execution failed to start"),
        )

    if payload.get("status") != "SUCCEEDED":
        error, cause = _nested_execution_failure(payload)
        raise _ExecutionError(error, cause)

    output_value = payload.get("output") or "{}"
    if resource.endswith(".sync:2") and isinstance(output_value, str):
        try:
            output_value = json.loads(output_value)
        except json.JSONDecodeError:
            pass

    return {
        "ExecutionArn": payload.get("executionArn"),
        "Input": payload.get("input", "{}"),
        "InputDetails": payload.get("inputDetails", {"included": True}),
        "Name": payload.get("name"),
        "Output": output_value,
        "OutputDetails": payload.get("outputDetails", {"included": True}),
        "StartDate": payload.get("startDate"),
        "StateMachineArn": payload.get("stateMachineArn"),
        "Status": payload.get("status"),
        "StopDate": payload.get("stopDate"),
    }


def _nested_start_execution_request(input_data):
    state_machine_arn = input_data.get("StateMachineArn") or input_data.get("stateMachineArn")
    if not state_machine_arn:
        raise _ExecutionError("ValidationException", "StateMachineArn is required")

    nested_input = input_data.get("Input", input_data.get("input", {}))
    if isinstance(nested_input, str):
        input_str = nested_input
    else:
        input_str = json.dumps(nested_input)

    request = {
        "stateMachineArn": state_machine_arn,
        "input": input_str,
    }
    name = input_data.get("Name") or input_data.get("name")
    if name:
        request["name"] = name
    return request


def _nested_execution_failure(payload):
    output = payload.get("output")
    if isinstance(output, str):
        try:
            decoded = json.loads(output)
        except json.JSONDecodeError:
            decoded = None
        if isinstance(decoded, dict) and decoded.get("Error"):
            return decoded["Error"], decoded.get("Cause", "")

    execution_arn = payload.get("executionArn", "")
    status = payload.get("status", "FAILED")
    return "States.TaskFailed", f"Nested execution {execution_arn} ended with status {status}"


def _invoke_sqs_send_message(resource, input_data):
    """arn:aws:states:::sqs:sendMessage"""
    try:
        from ministack.services import sqs
    except ImportError:
        logger.warning("sqs module unavailable; returning passthrough")
        return input_data
    try:
        url = input_data.get("QueueUrl", "")
        result = sqs._act_send_message(input_data, url)
        return result
    except sqs._Err as e:
        raise _ExecutionError(f"SQS.{e.code}", e.message)


def _invoke_sns_publish(resource, input_data):
    """arn:aws:states:::sns:publish"""
    try:
        from ministack.services import sns
    except ImportError:
        logger.warning("sns module unavailable; returning passthrough")
        return input_data
    status, _, body = sns._publish(input_data)
    if status >= 400:
        raise _ExecutionError(
            "SNS.PublishFailed",
            body.decode("utf-8", "replace") if isinstance(body, bytes) else str(body),
        )
    decoded = body.decode() if isinstance(body, bytes) else body
    m = re.search(r"<MessageId>(.+?)</MessageId>", decoded)
    msg_id = m.group(1) if m else new_uuid()
    return {"MessageId": msg_id}


def _invoke_dynamodb(op_name, input_data):
    """arn:aws:states:::dynamodb:{putItem,getItem,deleteItem,updateItem}"""
    try:
        from ministack.services import dynamodb
    except ImportError:
        logger.warning("dynamodb module unavailable; returning passthrough")
        return input_data
    fn_map = {
        "putItem": dynamodb._put_item,
        "getItem": dynamodb._get_item,
        "deleteItem": dynamodb._delete_item,
        "updateItem": dynamodb._update_item,
    }
    fn = fn_map.get(op_name)
    if not fn:
        raise _ExecutionError(
            "States.Runtime", f"Unsupported DynamoDB operation: {op_name}"
        )
    status, _, body = fn(input_data)
    result = json.loads(body) if body else {}
    if status >= 400:
        error_type = result.get("__type", "DynamoDB.AmazonDynamoDBException")
        raise _ExecutionError(error_type, result.get("message", ""))
    return result


def _invoke_ecs_run_task(resource, input_data):
    """arn:aws:states:::ecs:runTask[.sync]"""
    try:
        from ministack.services import ecs
    except ImportError:
        logger.warning("ecs module unavailable; returning passthrough")
        return input_data
    ecs_data = _pascal_to_camel(input_data)
    status, _, body = ecs._run_task(ecs_data)
    result = json.loads(body) if body else {}
    if status >= 400:
        raise _ExecutionError("ECS.RunTaskFailed", result.get("message", str(result)))

    is_sync = resource.rstrip("/").endswith(".sync")
    if is_sync and result.get("tasks"):
        task_arns = [t["taskArn"] for t in result["tasks"]]
        cluster = ecs_data.get("cluster", "default")
        result = _poll_ecs_tasks(cluster, task_arns)

    return result


def _poll_ecs_tasks(cluster, task_arns):
    """Poll DescribeTasks until all tasks are STOPPED (max 10 min).

    Returns the full DescribeTasks result including exit codes — the state
    machine definition decides how to handle success/failure via Choice or Catch.
    """
    from ministack.services import ecs

    for _ in range(600):
        time.sleep(1)
        status, _, body = ecs._describe_tasks({"cluster": cluster, "tasks": task_arns})
        result = json.loads(body) if body else {}
        tasks = result.get("tasks", [])
        if tasks and all(t.get("lastStatus") == "STOPPED" for t in tasks):
            return result
    raise _ExecutionError("States.Timeout", "ECS tasks did not complete in time")


def _pascal_to_camel(d):
    """Convert top-level PascalCase keys to camelCase for ECS internals."""
    if not isinstance(d, dict):
        return d
    out = {}
    for k, v in d.items():
        new_key = k[0].lower() + k[1:] if k else k
        out[new_key] = v
    return out


# ---------------------------------------------------------------------------
# Generic aws-sdk:* task dispatcher
# ---------------------------------------------------------------------------

# Map aws-sdk service names to MiniStack internal routing info.
# service_key overrides the key used in app.SERVICE_HANDLERS when it differs
# from the sdk service name.
_AWS_SDK_SERVICE_MAP = {
    # JSON-protocol services: use X-Amz-Target header
    "dynamodb": {"target_prefix": "DynamoDB_20120810", "protocol": "json"},
    "secretsmanager": {"target_prefix": "secretsmanager", "protocol": "json"},
    "sfn": {"target_prefix": "AWSStepFunctions", "protocol": "json", "service_key": "states"},
    "logs": {"target_prefix": "Logs_20140328", "protocol": "json"},
    "ssm": {"target_prefix": "AmazonSSM", "protocol": "json"},
    "eventbridge": {"target_prefix": "AWSEvents", "protocol": "json", "service_key": "events"},
    "kinesis": {"target_prefix": "Kinesis_20131202", "protocol": "json"},
    "glue": {"target_prefix": "AWSGlue", "protocol": "json"},
    "athena": {"target_prefix": "AmazonAthena", "protocol": "json"},
    "ecs": {"target_prefix": "AmazonEC2ContainerServiceV20141113", "protocol": "json"},
    "ecr": {"target_prefix": "AmazonEC2ContainerRegistry_V20150921", "protocol": "json"},
    "kms": {"target_prefix": "TrentService", "protocol": "json"},
    # Query-protocol services
    "sqs": {"protocol": "query"},
    "sns": {"protocol": "query"},
    "rds": {"protocol": "query"},
    "elasticache": {"protocol": "query"},
    "ec2": {"protocol": "query"},
    "iam": {"protocol": "query"},
    "sts": {"protocol": "query"},
    "cloudwatch": {"protocol": "query", "service_key": "monitoring"},
    # REST services (not yet supported via aws-sdk dispatcher)
    "s3": {"protocol": "rest"},
    "lambda": {"protocol": "rest"},
}


def _dispatch_aws_sdk_json(service_info, service_name, action, input_data):
    """Dispatch an aws-sdk integration call to a JSON-protocol MiniStack service."""
    from ministack import app

    target_prefix = service_info["target_prefix"]
    # SFN ARNs use camelCase (e.g. getRandomPassword) but service handlers
    # expect PascalCase (GetRandomPassword).
    pascal_action = action[0].upper() + action[1:] if action else action
    target = f"{target_prefix}.{pascal_action}"
    service_key = service_info.get("service_key", service_name)

    handler = app.SERVICE_HANDLERS.get(service_key)
    if not handler:
        raise _ExecutionError(
            "States.Runtime",
            f"Service '{service_key}' is not available in MiniStack",
        )

    body = json.dumps(input_data)
    headers = {
        "x-amz-target": target,
        "content-type": "application/x-amz-json-1.0",
        "host": f"{service_key}.{REGION}.amazonaws.com",
        "authorization": (
            f"AWS4-HMAC-SHA256 Credential=test/20260101/{REGION}/{service_key}/aws4_request"
        ),
    }

    # Service handlers are async def but perform no real I/O, so we can
    # drive the coroutine synchronously — this avoids conflicts with the
    # already-running uvicorn event loop.
    coro = handler("POST", "/", headers, body, {})
    try:
        coro.send(None)
    except StopIteration as stop:
        status, resp_headers, resp_body = stop.value
    else:
        # If the coroutine didn't finish in one step it truly needs async;
        # fall back to the event loop (only reachable if a handler awaits).
        coro.close()
        loop = asyncio.new_event_loop()
        try:
            status, resp_headers, resp_body = loop.run_until_complete(
                handler("POST", "/", headers, body, {})
            )
        finally:
            loop.close()

    decoded = resp_body.decode("utf-8") if isinstance(resp_body, bytes) else resp_body
    result = json.loads(decoded) if decoded else {}

    if status >= 400:
        error_type = result.get("__type", result.get("Error", {}).get("Code", f"{service_name}.ServiceException"))
        error_msg = result.get("message", result.get("Message", str(result)))
        raise _ExecutionError(error_type, error_msg)

    # For JSON-protocol services, only convert top-level keys to avoid
    # mangling user-defined data (e.g. DynamoDB attribute names).
    if isinstance(result, dict):
        return {_api_name_to_sfn_key(k): v for k, v in result.items()}
    return result


def _flatten_query_params(data, prefix=""):
    """Flatten a JSON dict into AWS query-protocol form params.

    Handles nested dicts, lists (Member.N convention), and scalar values.
    """
    params = {}
    if not isinstance(data, dict):
        return params
    for key, value in data.items():
        full_key = f"{prefix}{key}" if not prefix else f"{prefix}.{key}"
        if isinstance(value, dict):
            params.update(_flatten_query_params(value, full_key))
        elif isinstance(value, list):
            for i, item in enumerate(value, 1):
                member_key = f"{full_key}.member.{i}"
                if isinstance(item, dict):
                    params.update(_flatten_query_params(item, member_key))
                else:
                    params[member_key] = str(item)
        elif isinstance(value, bool):
            params[full_key] = "true" if value else "false"
        else:
            params[full_key] = str(value)
    return params


def _xml_element_to_dict(element):
    """Convert an XML element tree to a JSON-friendly dict.

    Strips namespace prefixes.  Repeated child tags become lists.
    Leaf text nodes become strings.
    """
    # Strip namespace
    tag = element.tag.split("}")[-1] if "}" in element.tag else element.tag

    children = list(element)
    if not children:
        # Leaf node
        return tag, (element.text or "")

    result = {}
    for child in children:
        child_tag, child_val = _xml_element_to_dict(child)
        if child_tag in result:
            existing = result[child_tag]
            if not isinstance(existing, list):
                result[child_tag] = [existing]
            result[child_tag].append(child_val)
        else:
            result[child_tag] = child_val
    return tag, result


# Known AWS acronyms that appear as uppercase runs in wire-format names.
# Used by _sfn_key_to_api_name to reverse the Java SDK V2 naming convention.
# Excludes Arn/Id (single uppercase in wire format) and Http/Https/Ec2
# (contain digits or are mixed-case in wire format, not pure acronym runs).
_AWS_ACRONYMS = frozenset({
    "Db", "Iam", "Vpc", "Ssl", "Kms", "Ttl", "Io", "Az",
    "Ebs", "Ssh", "Mfa", "Dns", "Acl",
    "Tcp", "Udp", "Iops", "Ca", "Sg",
})


def _sfn_key_to_api_name(name):
    """Convert SFN SDK key name to AWS wire-format name.

    Reverses _api_name_to_sfn_key: expands known acronyms back to uppercase.
    Examples: DbClusters -> DBClusters, KmsKeyId -> KMSKeyId,
              VpcSecurityGroupIds -> VPCSecurityGroupIds
    """
    if not name:
        return name
    import re
    tokens = re.findall(r"[A-Z][a-z]*|[a-z]+|[0-9]+", name)
    return "".join(t.upper() if t in _AWS_ACRONYMS else t for t in tokens)


def _convert_params_to_api_names(data):
    """Recursively convert SFN SDK-style param names to AWS wire-format names."""
    if isinstance(data, dict):
        return {_sfn_key_to_api_name(k): _convert_params_to_api_names(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_convert_params_to_api_names(item) for item in data]
    return data


def _api_name_to_sfn_key(name):
    """Convert an AWS API member name to SFN SDK integration key name.

    SFN uses the Java SDK V2 naming convention: consecutive uppercase characters
    (acronyms) are lowered except the last one when followed by a lowercase char.
    Examples: DBClusters -> DbClusters, DBClusterArn -> DbClusterArn,
              IAMDatabaseAuthenticationEnabled -> IamDatabaseAuthenticationEnabled
    """
    if not name:
        return name
    result = []
    i = 0
    while i < len(name):
        if i == 0:
            result.append(name[i].upper())
            i += 1
            continue
        if name[i].isupper():
            j = i
            while j < len(name) and name[j].isupper():
                j += 1
            run_len = j - i
            if run_len == 1:
                result.append(name[i])
                i += 1
            else:
                if j < len(name) and name[j].islower():
                    result.append(name[i:j - 1].lower())
                    result.append(name[j - 1])
                else:
                    result.append(name[i:j].lower())
                i = j
        else:
            result.append(name[i])
            i += 1
    return "".join(result)


def _convert_keys_to_sfn_convention(obj):
    """Recursively convert dict keys from AWS API naming to SFN/Java SDK V2 naming.

    Also converts datetime objects to epoch seconds (AWS SFN convention).
    """
    import datetime
    if isinstance(obj, dict):
        return {_api_name_to_sfn_key(k): _convert_keys_to_sfn_convention(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_keys_to_sfn_convention(item) for item in obj]
    if isinstance(obj, datetime.datetime):
        return obj.timestamp()
    return obj


def _dispatch_aws_sdk_query(service_info, service_name, action, input_data):
    """Dispatch an aws-sdk integration call to a query-protocol MiniStack service."""
    import xml.etree.ElementTree as ET
    from urllib.parse import urlencode
    from ministack import app

    service_key = service_info.get("service_key", service_name)
    handler = app.SERVICE_HANDLERS.get(service_key)
    if not handler:
        raise _ExecutionError(
            "States.Runtime",
            f"Service '{service_key}' is not available in MiniStack",
        )

    # SFN ARNs use camelCase (e.g. createDBSubnetGroup) but query-protocol
    # services expect PascalCase (CreateDBSubnetGroup).
    pascal_action = action[0].upper() + action[1:] if action else action
    # Convert SFN SDK-style param names (DbSubnetGroupName) to wire-format
    # names (DBSubnetGroupName) before flattening to query params.
    wire_data = _convert_params_to_api_names(input_data)
    form_params = {"Action": pascal_action}
    form_params.update(_flatten_query_params(wire_data))
    body = urlencode(form_params)

    headers = {
        "content-type": "application/x-www-form-urlencoded",
        "host": f"{service_key}.{REGION}.amazonaws.com",
        "authorization": (
            f"AWS4-HMAC-SHA256 Credential=test/20260101/{REGION}/{service_key}/aws4_request"
        ),
    }

    coro = handler("POST", "/", headers, body, {})
    try:
        coro.send(None)
    except StopIteration as stop:
        status, resp_headers, resp_body = stop.value
    else:
        coro.close()
        loop = asyncio.new_event_loop()
        try:
            status, resp_headers, resp_body = loop.run_until_complete(
                handler("POST", "/", headers, body, {})
            )
        finally:
            loop.close()

    decoded = resp_body.decode("utf-8") if isinstance(resp_body, bytes) else resp_body

    # Parse XML response to JSON
    if status >= 400:
        # Try to extract error from XML
        try:
            root = ET.fromstring(decoded)
            err_el = root.find(".//{http://rds.amazonaws.com/doc/2014-10-31/}Error")
            if err_el is None:
                # Try without namespace
                err_el = root.find(".//Error")
            if err_el is not None:
                code = err_el.findtext("{http://rds.amazonaws.com/doc/2014-10-31/}Code")
                if code is None:
                    code = err_el.findtext("Code")
                msg = err_el.findtext("{http://rds.amazonaws.com/doc/2014-10-31/}Message")
                if msg is None:
                    msg = err_el.findtext("Message")
                raise _ExecutionError(code or f"{service_name}.ServiceException", msg or decoded)
        except _ExecutionError:
            raise
        except Exception:
            pass
        raise _ExecutionError(f"{service_name}.ServiceException", decoded)

    # Convert successful XML response to dict, then apply SFN key naming convention
    try:
        root = ET.fromstring(decoded)
        _, result = _xml_element_to_dict(root)
        if isinstance(result, dict):
            # Unwrap the <ActionResult> wrapper if present
            result_key = f"{pascal_action}Result"
            if result_key in result:
                result = result[result_key]
            # Drop ResponseMetadata
            result.pop("ResponseMetadata", None)
        return _convert_keys_to_sfn_convention(result)
    except ET.ParseError:
        raise _ExecutionError("States.Runtime", f"Failed to parse {service_name} XML response")


def _invoke_aws_sdk_integration(resource, input_data):
    """Dispatch arn:aws:states:::aws-sdk:<service>:<action> to the target MiniStack service."""
    # Parse service and action from ARN
    parts = resource.replace(".sync", "").replace(".waitForTaskToken", "").split(":")
    # arn:aws:states:::aws-sdk:<service>:<action>
    # parts after split: ['arn', 'aws', 'states', '', '', 'aws-sdk', '<service>', '<action>']
    if len(parts) < 8 or parts[5] != "aws-sdk":
        raise _ExecutionError("States.Runtime", f"Invalid aws-sdk resource ARN: {resource}")
    service_name = parts[6].lower()
    action = parts[7]

    service_info = _AWS_SDK_SERVICE_MAP.get(service_name)
    if not service_info:
        raise _ExecutionError(
            "States.Runtime",
            f"Service '{service_name}' is not supported in MiniStack aws-sdk integrations",
        )

    protocol = service_info["protocol"]
    if protocol == "json":
        return _dispatch_aws_sdk_json(service_info, service_name, action, input_data)
    elif protocol == "query":
        return _dispatch_aws_sdk_query(service_info, service_name, action, input_data)
    else:
        raise _ExecutionError(
            "States.Runtime",
            f"aws-sdk integration for {protocol}-protocol service '{service_name}' "
            "is not yet implemented; use native service integrations instead",
        )


_SERVICE_DISPATCH = {
    "arn:aws:states:::sqs:sendMessage": _invoke_sqs_send_message,
    "arn:aws:states:::sns:publish": _invoke_sns_publish,
    "arn:aws:states:::dynamodb:putItem": lambda r, d: _invoke_dynamodb("putItem", d),
    "arn:aws:states:::dynamodb:getItem": lambda r, d: _invoke_dynamodb("getItem", d),
    "arn:aws:states:::dynamodb:deleteItem": lambda r, d: _invoke_dynamodb(
        "deleteItem", d
    ),
    "arn:aws:states:::dynamodb:updateItem": lambda r, d: _invoke_dynamodb(
        "updateItem", d
    ),
    "arn:aws:states:::ecs:runTask": _invoke_ecs_run_task,
}


def reset():
    _state_machines.clear()
    _executions.clear()
    _task_tokens.clear()
    _tags.clear()
    _activities.clear()
    _activity_tasks.clear()
