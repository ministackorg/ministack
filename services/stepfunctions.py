"""
Step Functions Service Emulator with ASL execution engine.
JSON-based API via X-Amz-Target (AWSStepFunctions).

Supports: CreateStateMachine, DeleteStateMachine, DescribeStateMachine,
          UpdateStateMachine, ListStateMachines,
          StartExecution, StartSyncExecution, StopExecution,
          DescribeExecution, DescribeStateMachineForExecution, ListExecutions,
          GetExecutionHistory,
          SendTaskSuccess, SendTaskFailure, SendTaskHeartbeat,
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
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait as futures_wait
from datetime import datetime, timezone

from core.responses import json_response, error_response_json, new_uuid, now_iso

logger = logging.getLogger("states")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"

_state_machines: dict = {}
_executions: dict = {}
_task_tokens: dict = {}
_tags: dict = {}


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
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


# ---------------------------------------------------------------------------
# State machine CRUD
# ---------------------------------------------------------------------------

def _create_state_machine(data):
    name = data.get("name")
    if not name:
        return error_response_json("ValidationException", "name is required", 400)

    arn = f"arn:aws:states:{REGION}:{ACCOUNT_ID}:stateMachine:{name}"
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
                            f"arn:aws:iam::{ACCOUNT_ID}:role/StepFunctionsRole"),
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
    machines = [
        {"stateMachineArn": sm["stateMachineArn"], "name": sm["name"],
         "type": sm["type"], "creationDate": sm["creationDate"]}
        for sm in _state_machines.values()
    ]
    return json_response({"stateMachines": machines})


# ---------------------------------------------------------------------------
# Execution lifecycle
# ---------------------------------------------------------------------------

def _start_execution(data):
    sm_arn = data.get("stateMachineArn")
    if sm_arn not in _state_machines:
        return error_response_json(
            "StateMachineDoesNotExist",
            f"State machine {sm_arn} not found", 400)

    sm = _state_machines[sm_arn]
    name = data.get("name") or new_uuid()
    exec_arn = (f"arn:aws:states:{REGION}:{ACCOUNT_ID}"
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
        "events": [
            {"id": 1, "type": "ExecutionStarted", "timestamp": start_date,
             "executionStartedEventDetails": {
                 "input": input_str, "roleArn": sm["roleArn"]}},
        ],
    }

    threading.Thread(
        target=_run_execution, args=(exec_arn,), daemon=True).start()

    logger.info(f"Step Functions execution started: {exec_arn}")
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
    execs = []
    for ex in _executions.values():
        if sm_arn and ex["stateMachineArn"] != sm_arn:
            continue
        if status_filter and ex["status"] != status_filter:
            continue
        execs.append({
            "executionArn": ex["executionArn"],
            "stateMachineArn": ex["stateMachineArn"],
            "name": ex["name"],
            "status": ex["status"],
            "startDate": ex["startDate"],
            "stopDate": ex.get("stopDate"),
        })
    return json_response({"executions": execs})


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
    sm_arn = data.get("stateMachineArn")
    if sm_arn not in _state_machines:
        return error_response_json(
            "StateMachineDoesNotExist",
            f"State machine {sm_arn} not found", 400)

    sm = _state_machines[sm_arn]
    name = data.get("name") or new_uuid()
    exec_arn = (f"arn:aws:states:{REGION}:{ACCOUNT_ID}"
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
        "events": [
            {"id": 1, "type": "ExecutionStarted", "timestamp": start_date,
             "executionStartedEventDetails": {
                 "input": input_str, "roleArn": sm["roleArn"]}},
        ],
    }

    _run_execution(exec_arn)

    execution = _executions[exec_arn]
    return json_response({
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
    })


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
            "TaskDoesNotExist", f"Task token not found", 400)
    info["result"] = output
    info["event"].set()
    return json_response({})


def _send_task_failure(data):
    token = data.get("taskToken")
    info = _task_tokens.get(token)
    if not info:
        return error_response_json(
            "TaskDoesNotExist", f"Task token not found", 400)
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
            "TaskDoesNotExist", f"Task token not found", 400)
    info["heartbeat"] = now_iso()
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
        logger.exception(f"Unexpected error in execution {exec_arn}")
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

    # Service integration dispatch
    clean = resource.replace(".sync", "").replace(".waitForTaskToken", "")
    for prefix, handler in _SERVICE_DISPATCH.items():
        if clean.startswith(prefix):
            return handler(resource, input_data)

    return input_data


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
    """Invoke a Lambda via the co-located lambda_svc module."""
    try:
        from services import lambda_svc
    except ImportError:
        logger.warning("lambda_svc unavailable; returning passthrough for %s", func_name)
        return event

    status, headers, body = asyncio.run(lambda_svc._invoke(func_name, event, {}))

    if headers.get("X-Amz-Function-Error"):
        try:
            err = json.loads(body)
            raise _ExecutionError(
                err.get("errorType", "Lambda.Unknown"),
                err.get("errorMessage", body.decode("utf-8", errors="replace")
                         if isinstance(body, bytes) else str(body)))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise _ExecutionError(
                "Lambda.Unknown",
                body.decode("utf-8", errors="replace")
                if isinstance(body, bytes) else str(body))

    try:
        return json.loads(body) if body else {}
    except (json.JSONDecodeError, UnicodeDecodeError):
        return body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body


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
            item_input = (_resolve_params_obj(item_params, item, item_ctx)
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


def _resolve_params_obj(template, data, ctx=None):
    if not isinstance(template, dict):
        return template
    result = {}
    for key, value in template.items():
        if key.endswith(".$"):
            real_key = key[:-2]
            if isinstance(value, str):
                if value.startswith("$$."):
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
        if "States.ALL" in equals or error in equals:
            return retrier, idx
    return None, -1


def _find_matching_catcher(catchers, error):
    for catcher in catchers:
        equals = catcher.get("ErrorEquals", [])
        if "States.ALL" in equals or error in equals:
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


def _invoke_sqs_send_message(resource, input_data):
    """arn:aws:states:::sqs:sendMessage"""
    try:
        from services import sqs
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
        from services import sns
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
        from services import dynamodb
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
        from services import ecs
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
    from services import ecs

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
