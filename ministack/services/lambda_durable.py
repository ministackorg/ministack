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

import contextvars

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    _request_account_id,
    _request_region,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)

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

# Resume scheduler — heapq of (resume_at_epoch, durable_arn, account_id).
# When a durable invocation returns Status=PENDING with WAIT operations still
# scheduled, ministack schedules a re-invocation at the earliest WAIT expiry.
import heapq
import threading as _threading

_resume_queue: list[tuple[float, str, str]] = []
_resume_lock = _threading.Lock()
_resume_event = _threading.Event()
_resume_thread_started = False

# Maps CallbackId → (DurableExecutionArn, OperationId) so external
# SendCallback{Success,Failure,Heartbeat} can find their target without
# scanning every execution. Defined HERE (above restore_state) because
# restore_state rebuilds this index from persisted executions at module
# import time — if the name were defined later, restore_state would
# NameError on cold start.
_callback_index: dict[str, tuple[str, str]] = {}

# Function-level DurableConfig is stored on the function config in lambda_svc;
# we expose helpers here for serialization parity.


# ---------------------------------------------------------------------------
# Persistence hooks
# ---------------------------------------------------------------------------

def get_state():
    return {"executions": copy.deepcopy(_executions)}


def restore_state(data):
    if not data:
        return
    _executions.update(data.get("executions", {}))
    # AWS contract: callbacks and timers survive process restarts. The live
    # in-memory `_callback_index` and resume heap are NOT persisted (the
    # heap entries are wall-clock-relative, the index is derivable), so
    # rebuild both from the restored execution records.
    # `_executions` is an AccountScopedDict; iterating yields composite
    # (account, arn) keys, so always read the bare arn off the record itself.
    for _key, rec in _executions.items():
        if not isinstance(rec, dict):
            continue
        arn = rec.get("DurableExecutionArn")
        if not arn:
            continue
        for op in rec.get("Operations", []):
            if op.get("Type") == "CALLBACK" and op.get("Status") == "STARTED":
                op_id = op.get("Id")
                if op_id:
                    _callback_index[op_id] = (arn, op_id)
        # Re-arm WAIT and CALLBACK timers for executions that were still
        # RUNNING when the process went down. Without this, restored
        # executions stall forever — timers never fire.
        if rec.get("Status") == "RUNNING":
            try:
                schedule_resume(arn)
            except Exception:
                import logging
                logging.getLogger(__name__).exception(
                    "Failed to re-arm timer on restore for %s", arn,
                )


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


import re as _re

# AWS spec Pattern for DurableExecutionArn (verified against
# https://docs.aws.amazon.com/lambda/latest/api/API_GetDurableExecution.html
# and the AWS SDK for Java v2 2.44.13 model).
_DURABLE_EXEC_ARN_RE = _re.compile(
    r"^arn:([a-zA-Z0-9-]+):lambda:([a-zA-Z0-9-]+):(\d{12})"
    r":function:([a-zA-Z0-9_-]+):(\$LATEST(?:\.PUBLISHED)?|[0-9]+)"
    r"/durable-execution/([a-zA-Z0-9_-]+)/([a-zA-Z0-9_-]+)$"
)

# CallbackId pattern from API_SendDurableExecutionCallback* (base64).
_CALLBACK_ID_RE = _re.compile(r"^[A-Za-z0-9+/]+={0,2}$")


def _parse_execution_arn(path_arn: str) -> str:
    """Decode + normalize the URL-embedded ARN (boto3 may URL-encode colons)."""
    return unquote(path_arn)


def _validate_execution_arn(arn: str) -> tuple | None:
    """AWS returns InvalidParameterValueException 400 for a structurally
    malformed DurableExecutionArn (separate from ResourceNotFound 404 for a
    well-formed but unknown one)."""
    if not isinstance(arn, str) or not _DURABLE_EXEC_ARN_RE.match(arn):
        return error_response_json(
            "InvalidParameterValueException",
            f"Invalid DurableExecutionArn: {arn}",
            400,
        )
    return None


def _require_execution(arn: str):
    arn = _parse_execution_arn(arn)
    err = _validate_execution_arn(arn)
    if err:
        return None, err
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


_TIMESTAMP_FIELDS_ON_OP = ("StartTimestamp", "EndTimestamp")
_TIMESTAMP_FIELDS_NESTED = {
    "StepDetails": ("NextAttemptTimestamp",),
    "WaitDetails": ("ScheduledEndTimestamp",),
}


def _to_unix_millis(v) -> int | None:
    """Float-seconds → int Unix-millis. Used on the EVENT path only
    (InitialExecutionState.Operations) because the durable SDK reads that path
    via `Operation.from_json_dict` which calls `TimestampConverter.from_unix_millis`.
    The boto3 API-response path uses `_to_unix_seconds` instead."""
    if v is None:
        return None
    try:
        return int(float(v) * 1000)
    except (TypeError, ValueError):
        return None


def _to_unix_seconds(v) -> int | None:
    """Float-seconds → int Unix-seconds. Used on every boto3 API-RESPONSE
    timestamp because botocore's `parse_timestamp` interprets numeric values
    as seconds via `datetime.fromtimestamp(value, tzinfo())`. Ministack-wide
    JSON convention is int (not float) so the strict-deserializing Java SDK v2
    and Go SDK v2 don't reject it — see feedback_timestamps_int_epoch."""
    if v is None:
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _serialize_operations(operations: list[dict], *, for_event: bool = False) -> list[dict]:
    """Return the wire-shape list of Operation objects.

    Two paths, two timestamp units (the wire shape really is different):
      - `for_event=True`  → InitialExecutionState.Operations on the Lambda
        event payload. The durable SDK reads this via `Operation.from_json_dict`
        which expects int Unix-millis.
      - `for_event=False` → Checkpoint / GetState API response bodies. boto3
        parses these via `botocore.utils.parse_timestamp` which expects
        Unix-seconds (the protocol-default for restJson1 timestamps).
    """
    convert = _to_unix_millis if for_event else _to_unix_seconds
    out = []
    for op in (operations or []):
        clone = copy.deepcopy(op)
        for f in _TIMESTAMP_FIELDS_ON_OP:
            if f in clone:
                c = convert(clone[f])
                if c is not None:
                    clone[f] = c
        for sub_key, fields in _TIMESTAMP_FIELDS_NESTED.items():
            sub = clone.get(sub_key)
            if isinstance(sub, dict):
                for f in fields:
                    if f in sub:
                        c = convert(sub[f])
                        if c is not None:
                            sub[f] = c
        out.append(clone)
    return out


def _execution_summary(rec: dict) -> dict:
    out = {
        "DurableExecutionArn": rec["DurableExecutionArn"],
        "DurableExecutionName": rec["DurableExecutionName"],
        "FunctionArn": rec["FunctionArn"],
        "StartTimestamp": _to_unix_seconds(rec["StartTimestamp"]),
        "Status": rec["Status"],
    }
    if rec.get("EndTimestamp") is not None:
        out["EndTimestamp"] = _to_unix_seconds(rec["EndTimestamp"])
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
    # AWS seeds a synthetic EXECUTION-type operation into the operations log
    # so the SDK can read the original user input via
    # execution_state.get_input_payload() on every invocation (including
    # replays). The SDK looks this operation up by `arn.split("/")[-1]`
    # (state.py:get_execution_operation), so its Id must match the trailing
    # ARN segment exactly.
    invocation_id = arn.split("/")[-1]
    execution_op = {
        "Id": invocation_id,
        "Type": "EXECUTION",
        "Status": "STARTED",
        "StartTimestamp": _now(),
        "ExecutionDetails": {"InputPayload": input_payload or ""},
    }
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
        "Operations": [execution_op],
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

def _fire_chained_invoke(parent_rec: dict, op_id: str, ci_opts: dict,
                         payload: str | None) -> None:
    """Asynchronously invoke a child durable function as part of a chain.
    The child runs via the existing lambda_svc executor; ministack records
    its result back onto the parent's ChainedInvoke operation when the child
    completes."""
    import threading

    from ministack.services import lambda_svc

    target = ci_opts.get("FunctionName")
    tenant_id = ci_opts.get("TenantId")

    def _run():
        # A ChainedInvoke may target a specific tenant; ministack scopes tenants
        # by account id, so honour TenantId by switching the account for this
        # child invocation (region stays the parent's, from the copied context).
        if tenant_id:
            _request_account_id.set(tenant_id)
        try:
            event_str = payload or "{}"
            try:
                event = json.loads(event_str) if isinstance(event_str, str) else event_str
            except (TypeError, ValueError):
                event = {}
            func_record = lambda_svc._functions.get(lambda_svc._resolve_name(target))
            if not func_record:
                # Surface failure back onto the parent's operation log.
                _append_chained_result(parent_rec, op_id, success=False, result=None,
                                       err={"ErrorType": "ResourceNotFoundException",
                                            "ErrorMessage": f"Function not found: {target}",
                                            "StackTrace": []})
                return
            # Run via the existing executor. The child runs synchronously here
            # but in a worker thread so we don't block ministack's ASGI loop.
            result = lambda_svc._execute_function(func_record, event)
            if result.get("error"):
                _append_chained_result(parent_rec, op_id, success=False, result=None,
                                       err={"ErrorType": str(result.get("function_error") or "Unhandled"),
                                            "ErrorMessage": str(result.get("body") or ""),
                                            "StackTrace": []})
            else:
                _append_chained_result(parent_rec, op_id, success=True,
                                       result=json.dumps(result.get("body"))
                                       if not isinstance(result.get("body"), str)
                                       else result.get("body"), err=None)
        except Exception as exc:
            _append_chained_result(parent_rec, op_id, success=False, result=None,
                                   err={"ErrorType": type(exc).__name__,
                                        "ErrorMessage": str(exc),
                                        "StackTrace": []})

    # Capture the caller's tenant scope (account + region); contextvars are not
    # inherited by a bare Thread, so the child's _functions lookup would
    # otherwise resolve in the default scope (B1).
    ctx = contextvars.copy_context()
    threading.Thread(target=lambda: ctx.run(_run), daemon=True,
                     name=f"chained-invoke-{op_id}").start()


def _append_chained_result(parent_rec: dict, op_id: str, success: bool,
                           result: str | None, err: dict | None) -> None:
    """Apply the child invocation's outcome onto the parent ChainedInvoke
    operation in the operation log + emit a history event."""
    for op in parent_rec["Operations"]:
        if op.get("Id") == op_id and op.get("Type") == "CHAINED_INVOKE":
            op["Status"] = "SUCCEEDED" if success else "FAILED"
            op["EndTimestamp"] = _now()
            details = op.setdefault("ChainedInvokeDetails", {})
            if success and result is not None:
                details["Result"] = result
            if err:
                details["Error"] = err
            break
    if success:
        _emit_history_event(parent_rec, "ChainedInvokeSucceeded",
                            "ChainedInvokeSucceededDetails",
                            {"Result": {"Payload": result or "", "Truncated": False}},
                            event_id=op_id)
    else:
        _emit_history_event(parent_rec, "ChainedInvokeFailed",
                            "ChainedInvokeFailedDetails",
                            {"Error": {"Payload": err or {}, "Truncated": False}},
                            event_id=op_id)


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
        try:
            _apply_update(rec, upd)
        except _InvalidUpdate as e:
            return error_response_json("InvalidParameterValueException",
                str(e), 400)

    new_token = new_checkpoint_token()
    rec["CheckpointToken"] = new_token
    return json_response({
        "CheckpointToken": new_token,
        "NewExecutionState": {
            "NextMarker": "",
            "Operations": _serialize_operations(rec["Operations"]),
        },
    })


_VALID_OP_TYPES = frozenset({
    "EXECUTION", "CONTEXT", "STEP", "WAIT", "CALLBACK", "CHAINED_INVOKE",
})
_VALID_OP_ACTIONS = frozenset({
    "START", "SUCCEED", "FAIL", "CANCEL", "RETRY",
})


class _InvalidUpdate(ValueError):
    """Signal to handle_checkpoint that an OperationUpdate failed validation
    and the whole Checkpoint should be rejected with InvalidParameterValueException."""


def _apply_update(rec: dict, upd: dict) -> None:
    """Translate one OperationUpdate into both an Operation log entry and a
    matching history event. The mapping mirrors the AWS docs Event types.

    Raises _InvalidUpdate when the update is structurally invalid (missing
    Id/Type/Action, unknown Type or Action) so the caller surfaces a 400
    rather than silently storing a garbage op."""
    op_id = upd.get("Id")
    op_type = upd.get("Type")
    action = upd.get("Action")
    if not op_id or not isinstance(op_id, str):
        raise _InvalidUpdate("OperationUpdate.Id is required and must be a string")
    if op_type not in _VALID_OP_TYPES:
        raise _InvalidUpdate(f"OperationUpdate.Type must be one of {sorted(_VALID_OP_TYPES)}, got {op_type!r}")
    if action not in _VALID_OP_ACTIONS:
        raise _InvalidUpdate(f"OperationUpdate.Action must be one of {sorted(_VALID_OP_ACTIONS)}, got {action!r}")
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
        # AWS-spec CallbackOptions per API_CheckpointDurableExecution:
        # TimeoutSeconds (overall expiry) + HeartbeatTimeoutSeconds (heartbeat
        # interval cap). Real AWS fires "Callback.Timeout" when either elapses
        # without resolution. Store both as absolute epoch deadlines so the
        # resume scheduler can poll them alongside WAIT expiries.
        cb_opts = upd.get("CallbackOptions") or {}
        if action == "START":
            details["CallbackId"] = op_id  # SDK uses Operation.Id as CallbackId
            timeout_s = cb_opts.get("TimeoutSeconds")
            if timeout_s is not None:
                details["TimeoutDeadline"] = now + float(timeout_s)
            hb_s = cb_opts.get("HeartbeatTimeoutSeconds")
            if hb_s is not None:
                details["HeartbeatTimeoutSeconds"] = float(hb_s)
                details["HeartbeatDeadline"] = now + float(hb_s)
            # Index so Send*Callback handlers can look us up by the bare id.
            _callback_index[op_id] = (rec["DurableExecutionArn"], op_id)
        elif action in ("SUCCEED", "FAIL", "CANCEL"):
            # Callback resolved internally — drop from the live index.
            _callback_index.pop(op_id, None)
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
        # On START, kick off the child function invocation asynchronously so
        # downstream durable workflows actually run (item #3 in the parity gap).
        ci_opts = upd.get("ChainedInvokeOptions") or {}
        if action == "START" and ci_opts.get("FunctionName"):
            _fire_chained_invoke(rec, op_id, ci_opts, payload)
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
        max_items = int(max_items_raw)
    except ValueError:
        return error_response_json("InvalidParameterValueException",
            f"MaxItems must be an integer, got {max_items_raw!r}", 400)
    # AWS docs: "Valid Range: Minimum value of 0. Maximum value of 1000."
    if max_items < 0 or max_items > 1000:
        return error_response_json("InvalidParameterValueException",
            "MaxItems must be between 0 and 1000", 400)
    if max_items == 0:
        max_items = 100

    ops = list(rec["Operations"])
    start = 0
    if marker:
        try:
            start = int(marker)
        except ValueError:
            start = 0
    page = ops[start:start + max_items]
    resp = {"Operations": _serialize_operations(page)}
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
        "StartTimestamp": _to_unix_seconds(rec["StartTimestamp"]),
    }
    if rec.get("EndTimestamp") is not None:
        out["EndTimestamp"] = _to_unix_seconds(rec["EndTimestamp"])
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
        max_items = int(max_items_raw)
    except ValueError:
        return error_response_json("InvalidParameterValueException",
            f"MaxItems must be an integer, got {max_items_raw!r}", 400)
    if max_items < 0 or max_items > 1000:
        return error_response_json("InvalidParameterValueException",
            "MaxItems must be between 0 and 1000", 400)
    if max_items == 0:
        max_items = 100

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
    # Convert EventTimestamp from float-seconds to int-seconds for boto3.
    serialized = []
    for ev in page:
        clone = copy.deepcopy(ev)
        if "EventTimestamp" in clone:
            clone["EventTimestamp"] = _to_unix_seconds(clone["EventTimestamp"])
        serialized.append(clone)
    resp = {"Events": serialized}
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
        max_items = int(max_items_raw)
    except ValueError:
        return error_response_json("InvalidParameterValueException",
            f"MaxItems must be an integer, got {max_items_raw!r}", 400)
    # AWS docs: "Valid Range: Minimum value of 0. Maximum value of 1000."
    if max_items < 0 or max_items > 1000:
        return error_response_json("InvalidParameterValueException",
            "MaxItems must be between 0 and 1000", 400)
    if max_items == 0:
        max_items = 100

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
    return json_response({"StopTimestamp": _to_unix_seconds(rec["EndTimestamp"])})


# ---------------------------------------------------------------------------
# Internal util — mirror of lambda_svc._qp_first to avoid a circular import.
# ---------------------------------------------------------------------------

def _qp_first(query_params: dict, key: str, default: str = "") -> str:
    v = query_params.get(key, default)
    if isinstance(v, list):
        return v[0] if v else default
    return v


# ---------------------------------------------------------------------------
# Send*Callback handlers — POST /2025-12-01/durable-execution-callbacks/{id}/{action}
# Sources verified against:
#   API_SendDurableExecutionCallbackSuccess.html
#   API_SendDurableExecutionCallbackFailure.html
#   API_SendDurableExecutionCallbackHeartbeat.html
# Each returns HTTP 200 with empty body on success.
# ---------------------------------------------------------------------------

def _resolve_callback(callback_id: str):
    """Look up a callback by its id. Returns (rec, callback_op, err)."""
    if not isinstance(callback_id, str) or not _CALLBACK_ID_RE.match(callback_id):
        return None, None, error_response_json(
            "InvalidParameterValueException",
            f"Invalid CallbackId: {callback_id}",
            400,
        )
    entry = _callback_index.get(callback_id)
    if not entry:
        return None, None, error_response_json(
            "ResourceNotFoundException",
            f"Callback not found: {callback_id}",
            404,
        )
    arn, op_id = entry
    rec = _executions.get(arn)
    if not rec:
        return None, None, error_response_json(
            "ResourceNotFoundException",
            f"Durable execution not found for callback: {callback_id}",
            404,
        )
    op = next((o for o in rec["Operations"] if o.get("Id") == op_id and o.get("Type") == "CALLBACK"), None)
    if not op or op.get("Status") != "STARTED":
        # AWS docs: "callback associated with the token has already been closed"
        return None, None, error_response_json(
            "CallbackTimeoutException",
            "The callback ID token has either expired or the callback associated with the token has already been closed.",
            400,
        )
    return rec, op, None


def handle_callback_success(callback_id: str, body: bytes) -> tuple:
    rec, op, err = _resolve_callback(callback_id)
    if err:
        return err
    # AWS: body is the raw Result blob (max 256 KB). Surface as a UTF-8 string
    # for the operation details; the SDK reads it back via the operation log.
    result_payload = ""
    if body:
        if len(body) > 262144:
            return error_response_json("InvalidParameterValueException",
                "Result exceeds the 256 KB maximum size", 400)
        try:
            result_payload = body.decode("utf-8")
        except UnicodeDecodeError:
            import base64 as _b64
            result_payload = _b64.b64encode(body).decode("ascii")
    op["Status"] = "SUCCEEDED"
    op["EndTimestamp"] = _now()
    details = op.setdefault("CallbackDetails", {})
    details["Result"] = result_payload
    _emit_history_event(rec, "CallbackSucceeded", "CallbackSucceededDetails",
                        {"Result": {"Payload": result_payload, "Truncated": False}},
                        event_id=op.get("Id"), name=op.get("Name"))
    # Leave the entry in _callback_index — a subsequent call against the same
    # id must hit _resolve_callback's "not STARTED" branch and return
    # CallbackTimeoutException (400), matching the AWS contract.
    # Wake the execution so it can resume the SDK handler.
    schedule_resume(rec["DurableExecutionArn"])
    return 200, {"Content-Type": "application/json"}, b""


def handle_callback_failure(callback_id: str, body: bytes) -> tuple:
    rec, op, err = _resolve_callback(callback_id)
    if err:
        return err
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        data = {}
    error_obj = {
        "ErrorType": data.get("ErrorType") or "CallbackFailure",
        "ErrorMessage": data.get("ErrorMessage") or "Callback failed",
        "ErrorData": data.get("ErrorData") or "",
        "StackTrace": data.get("StackTrace") or [],
    }
    op["Status"] = "FAILED"
    op["EndTimestamp"] = _now()
    details = op.setdefault("CallbackDetails", {})
    details["Error"] = error_obj
    _emit_history_event(rec, "CallbackFailed", "CallbackFailedDetails",
                        {"Error": {"Payload": error_obj, "Truncated": False}},
                        event_id=op.get("Id"), name=op.get("Name"))
    # Index entry retained — see handle_callback_success for the rationale.
    schedule_resume(rec["DurableExecutionArn"])
    return 200, {"Content-Type": "application/json"}, b""


def handle_callback_heartbeat(callback_id: str, body: bytes) -> tuple:
    """Heartbeat resets the heartbeat deadline so a long-running external
    operation doesn't trip its HeartbeatTimeoutSeconds. Overall TimeoutSeconds
    is unaffected."""
    rec, op, err = _resolve_callback(callback_id)
    if err:
        return err
    details = op.setdefault("CallbackDetails", {})
    hb_s = details.get("HeartbeatTimeoutSeconds")
    if hb_s is not None:
        details["HeartbeatDeadline"] = _now() + float(hb_s)
        # The previously-queued resume entry still points at the OLD
        # deadline. We do NOT push a fresh entry here — heartbeats can fire
        # frequently and each extra entry would trigger a spurious re-invoke
        # when it pops. Instead, _resume_execution re-arms the heap from
        # current deadlines when nothing has actually elapsed (see the
        # `nothing_elapsed` path below).
    return 200, {"Content-Type": "application/json"}, b""


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

    # /2025-12-01/durable-execution-callbacks/{CallbackId}/{succeed|fail|heartbeat}
    if parts[1] == "durable-execution-callbacks" and len(parts) >= 4 and method == "POST":
        callback_id = unquote(parts[2])
        action = parts[3]
        if action == "succeed":
            return handle_callback_success(callback_id, body)
        if action == "fail":
            return handle_callback_failure(callback_id, body)
        if action == "heartbeat":
            return handle_callback_heartbeat(callback_id, body)
        return None

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
    with _resume_lock:
        _resume_queue.clear()


# ---------------------------------------------------------------------------
# Resume scheduler — fires re-invocations when WAIT operations expire so
# paused durable executions actually resume the way they do on real AWS.
# ---------------------------------------------------------------------------

def _next_expiry(rec: dict) -> float | None:
    """Return the earliest scheduled expiry across all STARTED WAIT and
    CALLBACK ops, or None when nothing is pending. Callback ops contribute
    BOTH their HeartbeatDeadline (resets on incoming heartbeats) and their
    overall TimeoutDeadline; whichever fires first cancels the callback as
    `Callback.Timeout` per AWS docs."""
    soonest = None
    for op in rec.get("Operations", []):
        if op.get("Status") != "STARTED":
            continue
        if op.get("Type") == "WAIT":
            ts = (op.get("WaitDetails") or {}).get("ScheduledEndTimestamp")
            if ts is None:
                continue
            if soonest is None or ts < soonest:
                soonest = ts
        elif op.get("Type") == "CALLBACK":
            details = op.get("CallbackDetails") or {}
            for ts in (details.get("HeartbeatDeadline"), details.get("TimeoutDeadline")):
                if ts is None:
                    continue
                if soonest is None or ts < soonest:
                    soonest = ts
        elif op.get("Type") == "STEP":
            # SDK's RETRY checkpoint records NextAttemptTimestamp = now+delay
            # and returns PENDING expecting a re-invoke once the delay elapses.
            # See aws_durable_execution_sdk_python/operation/step.py:336 (the
            # create_step_retry path) — without scanning STEP ops here, retries
            # would never fire and the execution would hang at STARTED.
            ts = (op.get("StepDetails") or {}).get("NextAttemptTimestamp")
            if ts is None:
                continue
            if soonest is None or ts < soonest:
                soonest = ts
    return soonest


def schedule_resume(arn: str, account_id: str | None = None,
                    region: str | None = None) -> bool:
    """Inspect the execution's operations log; if any WAIT or CALLBACK op
    has a pending expiry, enqueue a re-invocation at the earliest one. Also
    fires immediately when a callback has already been resolved externally
    (Send*Callback handlers call us). Returns True when scheduled."""
    rec = _executions.get(arn)
    if not rec or rec.get("Status") != "RUNNING":
        return False
    expiry = _next_expiry(rec)
    # When called from a Send*Callback handler we want to wake the execution
    # immediately so the SDK observes the resolution on its next replay.
    has_resolved_callback = any(
        op.get("Type") == "CALLBACK" and op.get("Status") in ("SUCCEEDED", "FAILED")
        and not (op.get("CallbackDetails") or {}).get("_replayed")
        for op in rec.get("Operations", [])
    )
    if expiry is None and not has_resolved_callback:
        return False
    when = _now() if expiry is None else expiry
    if has_resolved_callback:
        when = min(when, _now())
    # Capture the caller's tenant scope. The resume thread has no request
    # contextvars, and both _executions (account-scoped) and _functions
    # (account+region-scoped) lookups would otherwise fall back to the default
    # scope — durable functions in a non-default region/account never resume (B1).
    account_id = account_id or get_account_id()
    region = region or get_region()
    with _resume_lock:
        heapq.heappush(_resume_queue, (when, arn, account_id, region))
    _resume_event.set()
    _ensure_resume_thread()
    return True


def _ensure_resume_thread() -> None:
    global _resume_thread_started
    if _resume_thread_started:
        return
    _resume_thread_started = True
    t = _threading.Thread(target=_resume_loop, daemon=True, name="durable-resume")
    t.start()


def _resume_loop() -> None:
    """Forever: pop the earliest entry from the resume queue, sleep until
    its expiry, then ask lambda_svc to re-invoke the function with the
    accumulated operations log so the SDK can replay completed steps."""
    while True:
        with _resume_lock:
            head = _resume_queue[0] if _resume_queue else None
        if head is None:
            _resume_event.wait(timeout=1.0)
            _resume_event.clear()
            continue
        wait_for = max(0.0, head[0] - time.time())
        if wait_for > 0:
            if _resume_event.wait(timeout=wait_for):
                _resume_event.clear()
                continue
        # Time elapsed — pop and resume.
        with _resume_lock:
            if not _resume_queue or _resume_queue[0] != head:
                continue
            _, arn, account_id, region = heapq.heappop(_resume_queue)
        # Re-establish the execution's tenant scope before touching account- or
        # region-scoped state (contextvars don't cross the thread boundary).
        tok_a = _request_account_id.set(account_id)
        tok_r = _request_region.set(region)
        try:
            _resume_execution(arn, account_id, region)
        except Exception:
            import logging
            logging.getLogger(__name__).exception(
                "Failed to resume durable execution %s", arn,
            )
        finally:
            _request_account_id.reset(tok_a)
            _request_region.reset(tok_r)


def _resume_execution(arn: str, account_id: str = "000000000000",
                      region: str | None = None) -> None:
    """Settle elapsed timers (WAIT expiries, CALLBACK heartbeat/timeout
    deadlines) then re-invoke the function so the SDK picks up where it
    left off. When called with a stale heap entry (heartbeat pushed the
    deadline forward after the entry was queued) we detect that nothing
    elapsed and simply re-enqueue at the current earliest deadline rather
    than triggering a spurious re-invoke."""
    rec = _executions.get(arn)
    if not rec or rec.get("Status") != "RUNNING":
        return
    now = _now()
    anything_elapsed = False
    has_resolved_callback = any(
        op.get("Type") == "CALLBACK" and op.get("Status") in ("SUCCEEDED", "FAILED")
        and not (op.get("CallbackDetails") or {}).get("_replayed")
        for op in rec.get("Operations", [])
    )
    for op in rec.get("Operations", []):
        if op.get("Status") != "STARTED":
            continue
        if op.get("Type") == "WAIT":
            ts = (op.get("WaitDetails") or {}).get("ScheduledEndTimestamp")
            if ts is not None and ts <= now:
                op["Status"] = "SUCCEEDED"
                op["EndTimestamp"] = now
                anything_elapsed = True
                _emit_history_event(rec, "WaitSucceeded", "WaitSucceededDetails", {
                    "Duration": (op.get("WaitDetails") or {}).get("Duration", 0),
                }, event_id=op.get("Id"), name=op.get("Name"))
        elif op.get("Type") == "STEP":
            # STEP RETRY: the SDK will re-execute the step body on replay; we
            # just need to wake the function once the recorded delay elapses.
            # The STEP stays Status=STARTED across attempts (the SDK uses
            # StepDetails.Attempt for counting), so this branch only marks
            # "something elapsed" so _resume_execution invokes rather than
            # taking the stale-entry no-op path.
            ts = (op.get("StepDetails") or {}).get("NextAttemptTimestamp")
            if ts is not None and ts <= now:
                anything_elapsed = True
        elif op.get("Type") == "CALLBACK":
            details = op.get("CallbackDetails") or {}
            t_overall = details.get("TimeoutDeadline")
            t_heartbeat = details.get("HeartbeatDeadline")
            fired = None
            if t_overall is not None and t_overall <= now:
                fired = "Callback.Timeout"
            elif t_heartbeat is not None and t_heartbeat <= now:
                fired = "Callback.Heartbeat"
            if fired is not None:
                op["Status"] = "TIMED_OUT"
                op["EndTimestamp"] = now
                anything_elapsed = True
                err_obj = {
                    "ErrorType": fired,
                    "ErrorMessage": f"Callback timed out: {fired}",
                    "ErrorData": "",
                    "StackTrace": [],
                }
                details["Error"] = err_obj
                _emit_history_event(rec, "CallbackTimedOut", "CallbackTimedOutDetails",
                                    {"Error": {"Payload": err_obj, "Truncated": False}},
                                    event_id=op.get("Id"), name=op.get("Name"))
                # Index entry stays so a late Send*Callback returns 400
                # CallbackTimeoutException rather than 404.
    # Stale heap entry (e.g. heartbeat extended the deadline after we were
    # queued, or schedule_resume queued an immediate wake that lost a race
    # with a checkpoint): nothing to settle and no external resolution to
    # replay. Re-arm at the current earliest deadline and skip the invoke.
    if not anything_elapsed and not has_resolved_callback:
        expiry = _next_expiry(rec)
        if expiry is not None:
            with _resume_lock:
                heapq.heappush(
                    _resume_queue,
                    (expiry, arn, account_id, region or get_region()),
                )
            _resume_event.set()
        return
    # Mark externally-resolved callbacks as replayed so the next schedule_resume
    # doesn't loop.
    for op in rec.get("Operations", []):
        if op.get("Type") == "CALLBACK" and op.get("Status") in ("SUCCEEDED", "FAILED"):
            details = op.setdefault("CallbackDetails", {})
            details["_replayed"] = True
    # Rotate token so the resumed invocation gets a fresh one.
    rec["CheckpointToken"] = new_checkpoint_token()
    # Trigger a fresh invocation through lambda_svc with the populated
    # operations log — the SDK's `InitialExecutionState` will replay the
    # completed steps and continue from the WAIT.
    from ministack.services import lambda_svc
    function_name = rec["FunctionArn"].rsplit(":", 1)[-1]
    try:
        try:
            event = json.loads(rec.get("InputPayload") or "{}")
        except (TypeError, ValueError):
            event = {}
        lambda_svc.invoke_durable_resume(function_name, arn, event)
    except Exception:
        import logging
        logging.getLogger(__name__).exception("Resume invocation for %s failed", arn)

