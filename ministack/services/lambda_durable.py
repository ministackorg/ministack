"""AWS Lambda Durable Functions / Durable Execution emulator.

Implements the seven management-plane operations of the Lambda Durable Execution
API (preview, Dec 2025) plus the function-level DurableConfig field on
CreateFunction / GetFunction. The shapes here are derived from the canonical
AWS public docs:

  - https://docs.aws.amazon.com/lambda/latest/api/API_CheckpointDurableExecution.html
  - https://docs.aws.amazon.com/lambda/latest/api/API_GetDurableExecutionState.html
  - https://docs.aws.amazon.com/lambda/latest/api/API_GetDurableExecution.html
  - https://docs.aws.amazon.com/lambda/latest/api/API_GetDurableExecutionHistory.html
  - https://docs.aws.amazon.com/lambda/latest/api/API_ListDurableExecutionsByFunction.html
  - https://docs.aws.amazon.com/lambda/latest/api/API_StopDurableExecution.html

DurableExecution ARN format per the docs' Pattern field:

    arn:aws:lambda:<region>:<account>:function:<NAME>:<VERSION>/durable-execution/<token>/<id>
"""
from __future__ import annotations

import base64
import copy
import json
import secrets
import time
import uuid
from urllib.parse import unquote

from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)
from ministack.core.persistence import load_state


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

# DurableExecutionArn -> execution record:
#   {
#     "DurableExecutionArn": str,
#     "DurableExecutionName": str,
#     "FunctionArn": str,
#     "Version": str,
#     "InputPayload": str,
#     "Status": "RUNNING"|"SUCCEEDED"|"FAILED"|"TIMED_OUT"|"STOPPED",
#     "StartTimestamp": float (epoch seconds),
#     "EndTimestamp": float | None,
#     "Result": str | None,
#     "Error": dict | None,
#     "TraceHeader": dict | None,
#     "CheckpointToken": str,           # current valid token
#     "Operations": list[dict],         # the operation log (mutated by Checkpoint)
#     "History": list[dict],            # append-only event log
#     "NextEventId": int,
#   }
_executions = AccountScopedDict()
# Function-level DurableConfig is stored on the function config in lambda_svc;
# we expose helpers here for serialization parity.


# ---------------------------------------------------------------------------
# Persistence hooks
# ---------------------------------------------------------------------------

def get_state():
    return {"executions": copy.deepcopy(_executions)}


def restore_state(data):
    if data:
        _executions.update(data.get("executions", {}))


try:
    _restored = load_state("lambda_durable")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception("Failed to restore lambda_durable state")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# AWS uses the function's qualifier (version / "$LATEST") embedded in the ARN.
# Token + ID follow the "/durable-execution/{token}/{id}" suffix per the docs.
_VALID_STATUS = {"RUNNING", "SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"}


def new_checkpoint_token() -> str:
    # AWS pattern: [A-Za-z0-9+/]+={0,2} — base64. 32 random bytes -> 44 chars.
    return base64.b64encode(secrets.token_bytes(32)).decode("ascii")


def build_durable_execution_arn(function_arn: str, version: str = "$LATEST",
                                name: str | None = None) -> tuple[str, str, str]:
    """Build a fully-qualified DurableExecutionArn from a function ARN.

    Returns (arn, name, token-uuid) where the suffix `/durable-execution/<token>/<id>`
    embeds two opaque UUIDs that the SDK echoes back unchanged.
    """
    token_id = uuid.uuid4().hex[:24]
    inner_id = uuid.uuid4().hex[:24]
    # Strip any pre-existing qualifier from the function ARN.
    base = function_arn
    if base.count(":") >= 7:
        base = ":".join(base.split(":")[:7])
    qualifier = version or "$LATEST"
    arn = f"{base}:{qualifier}/durable-execution/{token_id}/{inner_id}"
    return arn, (name or token_id), token_id


def _parse_execution_arn(path_arn: str) -> str:
    """Decode + normalize the URL-embedded ARN (boto3 may URL-encode colons)."""
    return unquote(path_arn)


def _require_execution(arn: str):
    arn = _parse_execution_arn(arn)
    rec = _executions.get(arn)
    if not rec:
        return None, error_response_json(
            "ResourceNotFoundException",
            f"Durable execution not found: {arn}",
            404,
        )
    return rec, None


def _now() -> float:
    return time.time()


def _execution_summary(rec: dict) -> dict:
    out = {
        "DurableExecutionArn": rec["DurableExecutionArn"],
        "DurableExecutionName": rec["DurableExecutionName"],
        "FunctionArn": rec["FunctionArn"],
        "StartTimestamp": rec["StartTimestamp"],
        "Status": rec["Status"],
    }
    if rec.get("EndTimestamp") is not None:
        out["EndTimestamp"] = rec["EndTimestamp"]
    return out


def _emit_history_event(rec: dict, event_type: str, details_key: str, details: dict,
                        name: str | None = None, parent_id: str | None = None,
                        sub_type: str | None = None, event_id: str | None = None) -> None:
    """Append an event to the execution's history log."""
    rec["NextEventId"] = int(rec.get("NextEventId", 0)) + 1
    ev = {
        "EventId": rec["NextEventId"],
        "EventTimestamp": _now(),
        "EventType": event_type,
        details_key: details,
    }
    if event_id is not None:
        ev["Id"] = event_id
    if name is not None:
        ev["Name"] = name
    if parent_id is not None:
        ev["ParentId"] = parent_id
    if sub_type is not None:
        ev["SubType"] = sub_type
    rec["History"].append(ev)


# ---------------------------------------------------------------------------
# Cross-module: invoked by lambda_svc on Invoke when DurableConfig.Enabled.
# ---------------------------------------------------------------------------

def create_execution_for_invoke(function_arn: str, version: str,
                                input_payload: str,
                                name: str | None = None,
                                trace_id: str | None = None) -> dict:
    """Spin up a new durable execution and return its record. The Lambda
    runtime is expected to read the ARN from the AWS_DURABLE_EXECUTION_ARN
    env var and call Checkpoint/GetState through the regular Lambda endpoint."""
    arn, exec_name, _ = build_durable_execution_arn(function_arn, version, name)
    rec = {
        "DurableExecutionArn": arn,
        "DurableExecutionName": exec_name,
        "FunctionArn": function_arn,
        "Version": version or "$LATEST",
        "InputPayload": input_payload or "",
        "Status": "RUNNING",
        "StartTimestamp": _now(),
        "EndTimestamp": None,
        "Result": None,
        "Error": None,
        "TraceHeader": {"XAmznTraceId": trace_id} if trace_id else None,
        "CheckpointToken": new_checkpoint_token(),
        "Operations": [],
        "History": [],
        "NextEventId": 0,
    }
    _executions[arn] = rec
    _emit_history_event(rec, "ExecutionStarted", "ExecutionStartedDetails", {
        "Input": {"Payload": input_payload or "", "Truncated": False},
    })
    return rec


def mark_execution_completed(arn: str, result_payload: str | None,
                             error: dict | None) -> None:
    rec = _executions.get(arn)
    if not rec:
        return
    rec["EndTimestamp"] = _now()
    if error:
        rec["Status"] = "FAILED"
        rec["Error"] = error
        _emit_history_event(rec, "ExecutionFailed", "ExecutionFailedDetails", {
            "Error": {"Payload": error, "Truncated": False},
        })
    else:
        rec["Status"] = "SUCCEEDED"
        rec["Result"] = result_payload
        _emit_history_event(rec, "ExecutionSucceeded", "ExecutionSucceededDetails", {
            "Result": {"Payload": result_payload or "", "Truncated": False},
        })


# ---------------------------------------------------------------------------
# Handlers — wired in from lambda_svc.handle_request based on path matching.
# ---------------------------------------------------------------------------

def handle_checkpoint(arn_path: str, body: bytes) -> tuple:
    rec, err = _require_execution(arn_path)
    if err:
        return err
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("InvalidParameterValueException",
            "Request body is not valid JSON", 400)
    checkpoint_token = data.get("CheckpointToken")
    if not checkpoint_token:
        return error_response_json("InvalidParameterValueException",
            "CheckpointToken is required", 400)
    if checkpoint_token != rec["CheckpointToken"]:
        return error_response_json("InvalidParameterValueException",
            "CheckpointToken does not match the current state of the execution", 400)
    updates = data.get("Updates") or []
    if rec["Status"] != "RUNNING":
        return error_response_json("InvalidParameterValueException",
            f"Cannot checkpoint a durable execution in status {rec['Status']}", 400)

    for upd in updates:
        _apply_update(rec, upd)

    new_token = new_checkpoint_token()
    rec["CheckpointToken"] = new_token
    return json_response({
        "CheckpointToken": new_token,
        "NewExecutionState": {
            "NextMarker": "",
            "Operations": copy.deepcopy(rec["Operations"]),
        },
    })


def _apply_update(rec: dict, upd: dict) -> None:
    """Translate one OperationUpdate into both an Operation log entry and a
    matching history event. The mapping mirrors the AWS docs Event types."""
    op_id = upd.get("Id")
    op_type = upd.get("Type")
    action = upd.get("Action")
    sub_type = upd.get("SubType")
    name = upd.get("Name")
    parent_id = upd.get("ParentId")
    payload = upd.get("Payload")
    err = upd.get("Error")
    now = _now()

    existing = next((o for o in rec["Operations"] if o.get("Id") == op_id), None)
    if existing is None:
        op = {
            "Id": op_id,
            "Type": op_type,
            "ParentId": parent_id,
            "Name": name,
            "StartTimestamp": now,
            "Status": "STARTED",
        }
        if sub_type:
            op["SubType"] = sub_type
        rec["Operations"].append(op)
        existing = op

    if action == "START":
        existing["Status"] = "STARTED"
    elif action == "SUCCEED":
        existing["Status"] = "SUCCEEDED"
        existing["EndTimestamp"] = now
    elif action == "FAIL":
        existing["Status"] = "FAILED"
        existing["EndTimestamp"] = now
    elif action == "CANCEL":
        existing["Status"] = "CANCELLED"
        existing["EndTimestamp"] = now
    elif action == "RETRY":
        existing["Status"] = "STARTED"

    # Attach type-specific details onto the Operation.
    if op_type == "STEP":
        details = existing.setdefault("StepDetails", {})
        if payload is not None and action == "SUCCEED":
            details["Result"] = payload
        if err is not None:
            details["Error"] = err
        if upd.get("StepOptions", {}).get("NextAttemptDelaySeconds") is not None:
            details["NextAttemptTimestamp"] = now + upd["StepOptions"]["NextAttemptDelaySeconds"]
        details["Attempt"] = details.get("Attempt", 0) + (1 if action in ("SUCCEED", "FAIL") else 0)
    elif op_type == "WAIT":
        details = existing.setdefault("WaitDetails", {})
        wait_secs = upd.get("WaitOptions", {}).get("WaitSeconds")
        if wait_secs is not None:
            details["ScheduledEndTimestamp"] = now + wait_secs
    elif op_type == "CALLBACK":
        details = existing.setdefault("CallbackDetails", {})
        if payload is not None and action == "SUCCEED":
            details["Result"] = payload
        if err is not None:
            details["Error"] = err
    elif op_type == "CONTEXT":
        details = existing.setdefault("ContextDetails", {})
        if payload is not None and action == "SUCCEED":
            details["Result"] = payload
        if err is not None:
            details["Error"] = err
        if upd.get("ContextOptions", {}).get("ReplayChildren") is not None:
            details["ReplayChildren"] = upd["ContextOptions"]["ReplayChildren"]
    elif op_type == "CHAINED_INVOKE":
        details = existing.setdefault("ChainedInvokeDetails", {})
        if payload is not None and action == "SUCCEED":
            details["Result"] = payload
        if err is not None:
            details["Error"] = err
    elif op_type == "EXECUTION":
        details = existing.setdefault("ExecutionDetails", {})
        if rec.get("InputPayload"):
            details["InputPayload"] = rec["InputPayload"]

    # History event mirror.
    event_type_map = {
        ("STEP", "START"): ("StepStarted", "StepStartedDetails", {}),
        ("STEP", "SUCCEED"): ("StepSucceeded", "StepSucceededDetails",
                              {"Result": {"Payload": payload or "", "Truncated": False}}),
        ("STEP", "FAIL"): ("StepFailed", "StepFailedDetails",
                           {"Error": {"Payload": err or {}, "Truncated": False}}),
        ("WAIT", "START"): ("WaitStarted", "WaitStartedDetails",
                            {"Duration": upd.get("WaitOptions", {}).get("WaitSeconds", 0)}),
        ("WAIT", "SUCCEED"): ("WaitSucceeded", "WaitSucceededDetails",
                              {"Duration": upd.get("WaitOptions", {}).get("WaitSeconds", 0)}),
        ("WAIT", "CANCEL"): ("WaitCancelled", "WaitCancelledDetails",
                             {"Error": {"Payload": err or {}, "Truncated": False}}),
        ("CALLBACK", "START"): ("CallbackStarted", "CallbackStartedDetails",
                                {"CallbackId": op_id or ""}),
        ("CALLBACK", "SUCCEED"): ("CallbackSucceeded", "CallbackSucceededDetails",
                                  {"Result": {"Payload": payload or "", "Truncated": False}}),
        ("CALLBACK", "FAIL"): ("CallbackFailed", "CallbackFailedDetails",
                               {"Error": {"Payload": err or {}, "Truncated": False}}),
        ("CONTEXT", "START"): ("ContextStarted", "ContextStartedDetails", {}),
        ("CONTEXT", "SUCCEED"): ("ContextSucceeded", "ContextSucceededDetails",
                                 {"Result": {"Payload": payload or "", "Truncated": False}}),
        ("CONTEXT", "FAIL"): ("ContextFailed", "ContextFailedDetails",
                              {"Error": {"Payload": err or {}, "Truncated": False}}),
    }
    key = (op_type, action)
    if key in event_type_map:
        ev_type, details_key, details = event_type_map[key]
        _emit_history_event(rec, ev_type, details_key, details,
                            name=name, parent_id=parent_id, sub_type=sub_type,
                            event_id=op_id)


def handle_get_state(arn_path: str, query_params: dict) -> tuple:
    rec, err = _require_execution(arn_path)
    if err:
        return err
    checkpoint_token = _qp_first(query_params, "CheckpointToken")
    if not checkpoint_token:
        return error_response_json("InvalidParameterValueException",
            "CheckpointToken is required", 400)
    if checkpoint_token != rec["CheckpointToken"]:
        return error_response_json("InvalidParameterValueException",
            "CheckpointToken does not match the current state of the execution", 400)
    marker = _qp_first(query_params, "Marker", "")
    max_items_raw = _qp_first(query_params, "MaxItems", "100")
    try:
        max_items = int(max_items_raw) or 100
    except ValueError:
        max_items = 100
    max_items = min(max(1, max_items), 1000)

    ops = list(rec["Operations"])
    start = 0
    if marker:
        try:
            start = int(marker)
        except ValueError:
            start = 0
    page = ops[start:start + max_items]
    resp = {"Operations": copy.deepcopy(page)}
    if start + max_items < len(ops):
        resp["NextMarker"] = str(start + max_items)
    return json_response(resp)


def handle_get_execution(arn_path: str) -> tuple:
    rec, err = _require_execution(arn_path)
    if err:
        return err
    out = {
        "DurableExecutionArn": rec["DurableExecutionArn"],
        "DurableExecutionName": rec["DurableExecutionName"],
        "FunctionArn": rec["FunctionArn"],
        "Version": rec["Version"],
        "InputPayload": rec["InputPayload"],
        "Status": rec["Status"],
        "StartTimestamp": rec["StartTimestamp"],
    }
    if rec.get("EndTimestamp") is not None:
        out["EndTimestamp"] = rec["EndTimestamp"]
    if rec.get("Result") is not None:
        out["Result"] = rec["Result"]
    if rec.get("Error") is not None:
        out["Error"] = rec["Error"]
    if rec.get("TraceHeader") is not None:
        out["TraceHeader"] = rec["TraceHeader"]
    return json_response(out)


def handle_get_history(arn_path: str, query_params: dict) -> tuple:
    rec, err = _require_execution(arn_path)
    if err:
        return err
    marker = _qp_first(query_params, "Marker", "")
    reverse = _qp_first(query_params, "ReverseOrder", "false").lower() == "true"
    max_items_raw = _qp_first(query_params, "MaxItems", "100")
    try:
        max_items = int(max_items_raw) or 100
    except ValueError:
        max_items = 100
    max_items = min(max(1, max_items), 1000)

    events = list(rec["History"])
    if reverse:
        events = list(reversed(events))
    start = 0
    if marker:
        try:
            start = int(marker)
        except ValueError:
            start = 0
    page = events[start:start + max_items]
    resp = {"Events": copy.deepcopy(page)}
    if start + max_items < len(events):
        resp["NextMarker"] = str(start + max_items)
    return json_response(resp)


def handle_list_by_function(function_name: str, query_params: dict,
                            function_arn_lookup) -> tuple:
    """`function_arn_lookup` is a callable from lambda_svc that resolves a
    name-or-ARN to the canonical function ARN, returning None if unknown."""
    fn_arn = function_arn_lookup(function_name)
    if not fn_arn:
        return error_response_json("ResourceNotFoundException",
            f"Function not found: {function_name}", 404)
    qualifier = _qp_first(query_params, "Qualifier", "$LATEST")
    status_filter = query_params.get("Statuses") or query_params.get("Status")
    if isinstance(status_filter, list):
        status_filter = status_filter[0] if status_filter else None
    name_filter = _qp_first(query_params, "DurableExecutionName")
    started_after = _qp_first(query_params, "StartedAfter")
    started_before = _qp_first(query_params, "StartedBefore")
    reverse = _qp_first(query_params, "ReverseOrder", "false").lower() == "true"
    marker = _qp_first(query_params, "Marker", "")
    max_items_raw = _qp_first(query_params, "MaxItems", "100")
    try:
        max_items = int(max_items_raw) or 100
    except ValueError:
        max_items = 100
    max_items = min(max(1, max_items), 1000)

    summaries = []
    for rec in _executions.values():
        if rec["FunctionArn"] != fn_arn:
            continue
        if rec["Version"] != qualifier:
            continue
        if status_filter and rec["Status"] != status_filter:
            continue
        if name_filter and rec["DurableExecutionName"] != name_filter:
            continue
        if started_after:
            try:
                if rec["StartTimestamp"] < float(started_after):
                    continue
            except ValueError:
                pass
        if started_before:
            try:
                if rec["StartTimestamp"] > float(started_before):
                    continue
            except ValueError:
                pass
        summaries.append(_execution_summary(rec))
    summaries.sort(key=lambda s: s["StartTimestamp"], reverse=not reverse)
    start = 0
    if marker:
        try:
            start = int(marker)
        except ValueError:
            start = 0
    page = summaries[start:start + max_items]
    resp = {"DurableExecutions": page}
    if start + max_items < len(summaries):
        resp["NextMarker"] = str(start + max_items)
    return json_response(resp)


def handle_stop(arn_path: str, body: bytes) -> tuple:
    rec, err = _require_execution(arn_path)
    if err:
        return err
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}
    if rec["Status"] != "RUNNING":
        return error_response_json("InvalidParameterValueException",
            f"Cannot stop a durable execution in status {rec['Status']}", 400)
    rec["Status"] = "STOPPED"
    rec["EndTimestamp"] = _now()
    rec["Error"] = {
        "ErrorType": data.get("ErrorType") or "DurableExecutionStopped",
        "ErrorMessage": data.get("ErrorMessage") or "Stopped by caller",
        "ErrorData": data.get("ErrorData") or "",
        "StackTrace": data.get("StackTrace") or [],
    }
    _emit_history_event(rec, "ExecutionStopped", "ExecutionStoppedDetails", {
        "Error": {"Payload": rec["Error"], "Truncated": False},
    })
    return json_response({"StopTimestamp": rec["EndTimestamp"]})


# ---------------------------------------------------------------------------
# Internal util — mirror of lambda_svc._qp_first to avoid a circular import.
# ---------------------------------------------------------------------------

def _qp_first(query_params: dict, key: str, default: str = "") -> str:
    v = query_params.get(key, default)
    if isinstance(v, list):
        return v[0] if v else default
    return v


# ---------------------------------------------------------------------------
# Path-matching entry point — exposed for lambda_svc.handle_request.
# ---------------------------------------------------------------------------

# AWS API version date prefix for the durable-execution surface per the spec.
_DURABLE_API_VERSION = "2025-12-01"


def try_route(method: str, path: str, body: bytes, query_params: dict,
              function_arn_lookup) -> tuple | None:
    """Returns a `(status, headers, body)` triple if the path is a durable-
    execution route, or None when the caller should fall through to the
    normal Lambda router."""
    path = unquote(path)
    parts = path.lstrip("/").split("/")
    if len(parts) < 3 or parts[0] != _DURABLE_API_VERSION:
        return None

    # /2025-12-01/functions/{name}/durable-executions
    if parts[1] == "functions" and len(parts) >= 4 and parts[3] == "durable-executions":
        if method != "GET":
            return None
        function_name = parts[2]
        return handle_list_by_function(function_name, query_params, function_arn_lookup)

    if parts[1] != "durable-executions" or len(parts) < 3:
        return None

    # The DurableExecutionArn embeds slashes ("/durable-execution/<token>/<id>").
    # Reconstruct it from the path segments — the suffix after the ARN
    # contains exactly the trailing action keyword (state, history, stop,
    # checkpoint) OR nothing (GetDurableExecution).
    tail_keywords = {"state", "history", "stop", "checkpoint"}
    if parts[-1] in tail_keywords:
        action = parts[-1]
        arn = "/".join(parts[2:-1])
    else:
        action = None
        arn = "/".join(parts[2:])

    if action == "checkpoint" and method == "POST":
        return handle_checkpoint(arn, body)
    if action == "state" and method == "GET":
        return handle_get_state(arn, query_params)
    if action == "history" and method == "GET":
        return handle_get_history(arn, query_params)
    if action == "stop" and method == "POST":
        return handle_stop(arn, body)
    if action is None and method == "GET":
        return handle_get_execution(arn)
    return None


# ---------------------------------------------------------------------------
# Reset hook for ministack's /_ministack/reset.
# ---------------------------------------------------------------------------

def reset() -> None:
    _executions.clear()
