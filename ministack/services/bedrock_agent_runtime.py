"""
Bedrock Agent Runtime Service Emulator.
JSON REST API + eventstream — signing name: bedrock. Endpoint prefix: bedrock-agent-runtime.

All 31 operations verified against botocore bedrock-agent-runtime-2023-07-26.
camelCase path segments per AWS spec (`/agentAliases`) — distinct from
bedrock-agent (lowercase).

Operation families:
  Agent runtime: InvokeAgent, InvokeInlineAgent
  Agent memory: GetAgentMemory, DeleteAgentMemory
  Knowledge base: Retrieve, RetrieveAndGenerate, RetrieveAndGenerateStream
  Reranking: Rerank
  Sessions: CreateSession, GetSession, UpdateSession, DeleteSession, EndSession,
            ListSessions, CreateInvocation, ListInvocations, PutInvocationStep,
            GetInvocationStep, ListInvocationSteps
  Flow runtime: InvokeFlow, StartFlowExecution, StopFlowExecution,
                GetFlowExecution, ListFlowExecutions, ListFlowExecutionEvents,
                GetExecutionFlowSnapshot
  Prompt optimization: OptimizePrompt
  Query generation: GenerateQuery
  Tags
"""

import base64
import copy
import hashlib
import json
import logging
import re
import uuid
import zlib
from datetime import datetime, timezone
from urllib.parse import unquote

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    get_account_id,
    get_region,
)

logger = logging.getLogger("bedrock-agent-runtime")


# ===========================================================================
# Camelize + JSON helpers
# ===========================================================================


def _to_camel(key: str) -> str:
    return key[0].lower() + key[1:] if key else key


def _camelize(obj):
    if isinstance(obj, dict):
        return {_to_camel(k): _camelize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_camelize(v) for v in obj]
    return obj


def _json(payload: dict, status: int = 200, extra_headers: dict | None = None) -> tuple:
    headers = {"Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    return status, headers, json.dumps(_camelize(payload)).encode()


def _empty(status: int = 200) -> tuple:
    return status, {"Content-Type": "application/json"}, b"{}"


# ===========================================================================
# Errors
# ===========================================================================


def _error(code: str, message: str, status: int) -> tuple:
    body = json.dumps({"message": message, "__type": code}).encode()
    return status, {"Content-Type": "application/json"}, body


def _not_found(msg: str) -> tuple:
    return _error("ResourceNotFoundException", msg, 404)


def _validation(msg: str) -> tuple:
    return _error("ValidationException", msg, 400)


def _conflict(msg: str) -> tuple:
    return _error("ConflictException", msg, 409)


# ===========================================================================
# Helpers
# ===========================================================================


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _arn(rt: str, path: str) -> str:
    return f"arn:aws:bedrock:{get_region()}:{get_account_id()}:{rt}/{path}"


def _parse_body(body) -> tuple:
    if not body:
        return {}, None
    try:
        obj = json.loads(body)
    except json.JSONDecodeError:
        return None, _validation("Body is not valid JSON.")
    if not isinstance(obj, dict):
        return None, _validation("Body must be a JSON object.")
    return obj, None


def _mock_reply(model_or_agent: str, prompt: str) -> str:
    digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:8]
    return f"[ministack mock {model_or_agent}] reply for prompt#{digest}"


# ===========================================================================
# Eventstream encoder (vnd.amazon.eventstream)
# ===========================================================================


def _es_encode_message(headers: dict, payload: bytes) -> bytes:
    hdr = bytearray()
    for name, value in headers.items():
        nb = name.encode()
        vb = value.encode()
        hdr.append(len(nb))
        hdr.extend(nb)
        hdr.append(7)
        hdr.extend(len(vb).to_bytes(2, "big"))
        hdr.extend(vb)
    headers_length = len(hdr)
    total_length = 12 + headers_length + len(payload) + 4
    prelude = total_length.to_bytes(4, "big") + headers_length.to_bytes(4, "big")
    prelude_crc = zlib.crc32(prelude).to_bytes(4, "big")
    msg_head = prelude + prelude_crc + bytes(hdr) + payload
    return msg_head + zlib.crc32(msg_head).to_bytes(4, "big")


def _es_event(event_type: str, payload: dict) -> bytes:
    return _es_encode_message({
        ":message-type": "event",
        ":event-type": event_type,
        ":content-type": "application/json",
    }, json.dumps(payload).encode())


# ===========================================================================
# State
# ===========================================================================

_sessions = AccountRegionScopedDict()             # session_id -> session dict
_invocations = AccountRegionScopedDict()          # f"{session_id}/{inv_id}" -> invocation
_invocation_steps = AccountRegionScopedDict()     # f"{session_id}/{inv_id}/{step_id}" -> step
_agent_memories = AccountRegionScopedDict()       # f"{agent_id}/{alias_id}" -> memory list
_flow_executions = AccountRegionScopedDict()      # f"{flow_id}/{alias_id}/{exec_id}" -> exec
_tags = AccountRegionScopedDict()


def reset():
    for s in (_sessions, _invocations, _invocation_steps, _agent_memories,
               _flow_executions, _tags):
        s.clear()


def get_state():
    return copy.deepcopy({
        "sessions": _sessions, "invocations": _invocations,
        "invocation_steps": _invocation_steps, "agent_memories": _agent_memories,
        "flow_executions": _flow_executions, "tags": _tags,
    })


def restore_state(data):
    if not data:
        return
    _sessions.update(data.get("sessions", {}))
    _invocations.update(data.get("invocations", {}))
    _invocation_steps.update(data.get("invocation_steps", {}))
    _agent_memories.update(data.get("agent_memories", {}))
    _flow_executions.update(data.get("flow_executions", {}))
    _tags.update(data.get("tags", {}))


try:
    _restored = load_state("bedrock_agent_runtime")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore bedrock_agent_runtime state; continuing fresh")


# ===========================================================================
# InvokeAgent (eventstream response)
# ===========================================================================


def _invoke_agent(agent_id: str, alias_id: str, session_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    prompt = body_obj.get("inputText", "")
    end_session = bool(body_obj.get("endSession"))
    reply = _mock_reply(agent_id, prompt)
    # Stream: trace -> chunk(s) -> (optional end-of-session)
    stream = b""
    if body_obj.get("enableTrace"):
        stream += _es_event("trace", {
            "agentId": agent_id,
            "agentAliasId": alias_id,
            "sessionId": session_id,
            "trace": {"orchestrationTrace": {
                "modelInvocationInput": {"text": prompt, "type": "ORCHESTRATE"},
            }},
        })
    # Chunk(s) — body is base64 of bytes per AWS, but for InvokeAgent the inner
    # `bytes` field is the raw response text bytes encoded as a Blob (boto3
    # handles base64 transparently in protocol layer for blob members).
    stream += _es_event("chunk", {"bytes": base64.b64encode(reply.encode()).decode()})
    return 200, {
        "Content-Type": "application/vnd.amazon.eventstream",
        "x-amz-bedrock-agent-session-id": session_id,
        "x-amzn-bedrock-agent-content-type": "application/json",
    }, stream


def _invoke_inline_agent(session_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    prompt = body_obj.get("inputText", "")
    reply = _mock_reply(f"inline-{body_obj.get('foundationModel', 'unknown')}", prompt)
    stream = _es_event("chunk", {"bytes": base64.b64encode(reply.encode()).decode()})
    return 200, {
        "Content-Type": "application/vnd.amazon.eventstream",
        "x-amz-bedrock-agent-session-id": session_id,
    }, stream


# ===========================================================================
# Agent memory
# ===========================================================================


def _get_agent_memory(agent_id: str, alias_id: str, query_params) -> tuple:
    key = f"{agent_id}/{alias_id}"
    memories = _agent_memories.get(key, [])
    return _json({"MemoryContents": memories, "NextToken": None})


def _delete_agent_memory(agent_id: str, alias_id: str, query_params) -> tuple:
    key = f"{agent_id}/{alias_id}"
    _agent_memories.pop(key, None)
    return _empty(status=202)


# ===========================================================================
# Retrieve / RetrieveAndGenerate / Rerank / GenerateQuery / OptimizePrompt
# ===========================================================================


def _retrieve(kb_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("retrievalQuery"):
        return _validation("retrievalQuery is required.")
    # Return empty results — shape-correct
    return _json({"RetrievalResults": [], "NextToken": None})


def _retrieve_and_generate(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    input_text = body_obj.get("input", {}).get("text", "")
    if not input_text:
        return _validation("input.text is required.")
    session_id = body_obj.get("sessionId") or uuid.uuid4().hex
    reply = _mock_reply("rag", input_text)
    return _json({
        "Output": {"Text": reply},
        "Citations": [],
        "SessionId": session_id,
    })


def _retrieve_and_generate_stream(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    input_text = body_obj.get("input", {}).get("text", "")
    if not input_text:
        return _validation("input.text is required.")
    session_id = body_obj.get("sessionId") or uuid.uuid4().hex
    reply = _mock_reply("rag-stream", input_text)
    stream = b""
    stream += _es_event("output", {"text": reply})
    stream += _es_event("citation", {"citation": {"generatedResponsePart":
                                                      {"textResponsePart": {"text": reply,
                                                                              "span": {"start": 0,
                                                                                        "end": len(reply)}}},
                                                    "retrievedReferences": []}})
    return 200, {"Content-Type": "application/vnd.amazon.eventstream",
                  "x-amz-bedrock-agent-session-id": session_id}, stream


def _rerank(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("queries"):
        return _validation("queries is required.")
    sources = body_obj.get("sources", [])
    results = [{"Index": i, "RelevanceScore": 1.0 - (i * 0.1)}
                for i in range(len(sources))]
    return _json({"Results": results, "NextToken": None})


def _generate_query(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("queryGenerationInput"):
        return _validation("queryGenerationInput is required.")
    text = body_obj["queryGenerationInput"].get("text", "")
    return _json({"Queries": [{"Type": "REDSHIFT", "Sql": f"-- mock SQL for: {text}"}]})


def _optimize_prompt(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("input"):
        return _validation("input is required.")
    if not body_obj.get("targetModelId"):
        return _validation("targetModelId is required.")
    input_text = body_obj["input"].get("textPrompt", {}).get("text", "")
    optimized = f"[ministack optimized] {input_text}"
    stream = _es_event("optimizedPromptEvent", {
        "optimizedPrompt": {"textPrompt": {"text": optimized}},
    })
    return 200, {"Content-Type": "application/vnd.amazon.eventstream"}, stream


# ===========================================================================
# Sessions
# ===========================================================================


def _session_arn(session_id: str) -> str:
    return _arn("session", session_id)


def _create_session(body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    session_id = uuid.uuid4().hex
    now = _now_iso()
    rec = {
        "SessionArn": _session_arn(session_id),
        "SessionId": session_id,
        "SessionStatus": "ACTIVE",
        "CreatedAt": now,
        "LastUpdatedAt": now,
        "EncryptionKeyArn": body_obj.get("encryptionKeyArn"),
        "SessionMetadata": body_obj.get("sessionMetadata", {}),
    }
    _sessions[session_id] = rec
    if body_obj.get("tags"):
        _tags[rec["SessionArn"]] = dict(body_obj["tags"])
    return _json({
        "SessionArn": rec["SessionArn"],
        "SessionId": session_id,
        "SessionStatus": "ACTIVE",
        "CreatedAt": now,
    }, status=201)


def _get_session(session_id: str) -> tuple:
    rec = _sessions.get(session_id)
    if rec is None:
        return _not_found(f"Session {session_id} not found.")
    return _json(rec)


def _list_sessions(body) -> tuple:
    summaries = [{"SessionId": r["SessionId"], "SessionArn": r["SessionArn"],
                   "SessionStatus": r["SessionStatus"],
                   "CreatedAt": r["CreatedAt"],
                   "LastUpdatedAt": r["LastUpdatedAt"]}
                  for r in _sessions.values()]
    return _json({"SessionSummaries": summaries})


def _update_session(session_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    rec = _sessions.get(session_id)
    if rec is None:
        return _not_found(f"Session {session_id} not found.")
    if "sessionMetadata" in body_obj:
        rec["SessionMetadata"] = body_obj["sessionMetadata"]
    rec["LastUpdatedAt"] = _now_iso()
    return _json({"SessionArn": rec["SessionArn"], "SessionId": session_id,
                   "LastUpdatedAt": rec["LastUpdatedAt"]})


def _delete_session(session_id: str) -> tuple:
    if session_id not in _sessions:
        return _not_found(f"Session {session_id} not found.")
    del _sessions[session_id]
    return _empty()


def _end_session(session_id: str) -> tuple:
    rec = _sessions.get(session_id)
    if rec is None:
        return _not_found(f"Session {session_id} not found.")
    rec["SessionStatus"] = "ENDED"
    rec["LastUpdatedAt"] = _now_iso()
    return _json({"SessionArn": rec["SessionArn"], "SessionId": session_id,
                   "SessionStatus": "ENDED"})


def _create_invocation(session_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if session_id not in _sessions:
        return _not_found(f"Session {session_id} not found.")
    inv_id = uuid.uuid4().hex
    now = _now_iso()
    rec = {
        "InvocationId": inv_id,
        "SessionId": session_id,
        "CreatedAt": now,
        "Description": body_obj.get("description", ""),
    }
    _invocations[f"{session_id}/{inv_id}"] = rec
    return _json(rec, status=201)


def _list_invocations(session_id: str, body) -> tuple:
    if session_id not in _sessions:
        return _not_found(f"Session {session_id} not found.")
    summaries = [r for k, r in _invocations.items() if k.startswith(f"{session_id}/")]
    return _json({"InvocationSummaries": summaries})


def _put_invocation_step(session_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if session_id not in _sessions:
        return _not_found(f"Session {session_id} not found.")
    inv_id = body_obj.get("invocationIdentifier")
    if not inv_id:
        return _validation("invocationIdentifier is required.")
    step_id = body_obj.get("invocationStepId") or uuid.uuid4().hex
    rec = {
        "InvocationStepId": step_id,
        "InvocationId": inv_id,
        "SessionId": session_id,
        "InvocationStepTime": body_obj.get("invocationStepTime") or _now_iso(),
        "Payload": body_obj.get("payload", {}),
    }
    _invocation_steps[f"{session_id}/{inv_id}/{step_id}"] = rec
    return _json({"InvocationStepId": step_id}, status=201)


def _get_invocation_step(session_id: str, step_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    inv_id = body_obj.get("invocationIdentifier")
    if not inv_id:
        return _validation("invocationIdentifier is required.")
    rec = _invocation_steps.get(f"{session_id}/{inv_id}/{step_id}")
    if rec is None:
        return _not_found(f"Invocation step {step_id} not found.")
    return _json({"InvocationStep": rec})


def _list_invocation_steps(session_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    inv_id = body_obj.get("invocationIdentifier")
    summaries = []
    for key, r in _invocation_steps.items():
        if not key.startswith(f"{session_id}/"):
            continue
        if inv_id and r["InvocationId"] != inv_id:
            continue
        summaries.append({
            "InvocationStepId": r["InvocationStepId"],
            "InvocationId": r["InvocationId"],
            "SessionId": session_id,
            "InvocationStepTime": r["InvocationStepTime"],
        })
    return _json({"InvocationStepSummaries": summaries})


# ===========================================================================
# Flow runtime
# ===========================================================================


def _invoke_flow(flow_id: str, alias_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    if not body_obj.get("inputs"):
        return _validation("inputs is required.")
    exec_id = body_obj.get("executionId") or uuid.uuid4().hex
    stream = _es_event("flowOutputEvent", {
        "content": {"document": "[ministack mock flow output]"},
        "nodeName": "OutputNode",
        "nodeType": "FlowOutputNode",
    })
    stream += _es_event("flowCompletionEvent", {"completionReason": "SUCCESS"})
    return 200, {
        "Content-Type": "application/vnd.amazon.eventstream",
        "x-amz-bedrock-flow-execution-id": exec_id,
    }, stream


def _flow_exec_arn(flow_id: str, alias_id: str, exec_id: str) -> str:
    return _arn("flow", f"{flow_id}/aliases/{alias_id}/executions/{exec_id}")


def _start_flow_execution(flow_id: str, alias_id: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    exec_id = uuid.uuid4().hex
    now = _now_iso()
    rec = {
        "ExecutionArn": _flow_exec_arn(flow_id, alias_id, exec_id),
        "ExecutionId": exec_id,
        "FlowId": flow_id,
        "FlowAliasId": alias_id,
        "FlowVersion": body_obj.get("flowExecutionRoleArn", ""),
        "Status": "Running",
        "CreatedAt": now,
        "EndedAt": now,
        "Inputs": body_obj.get("inputs", []),
        "ModelPerformanceConfiguration": body_obj.get("modelPerformanceConfiguration"),
    }
    _flow_executions[f"{flow_id}/{alias_id}/{exec_id}"] = rec
    return _json({"ExecutionArn": rec["ExecutionArn"]})


def _get_flow_execution(flow_id: str, alias_id: str, exec_id: str) -> tuple:
    rec = _flow_executions.get(f"{flow_id}/{alias_id}/{exec_id}")
    if rec is None:
        return _not_found(f"Flow execution {exec_id} not found.")
    return _json(rec)


def _list_flow_executions(flow_id: str, query_params) -> tuple:
    summaries = [r for k, r in _flow_executions.items()
                  if k.startswith(f"{flow_id}/")]
    return _json({"FlowExecutionSummaries": summaries})


def _list_flow_execution_events(flow_id: str, alias_id: str, exec_id: str,
                                  query_params) -> tuple:
    # Empty event list — shape-correct
    return _json({"FlowExecutionEvents": []})


def _get_execution_flow_snapshot(flow_id: str, alias_id: str, exec_id: str) -> tuple:
    if f"{flow_id}/{alias_id}/{exec_id}" not in _flow_executions:
        return _not_found(f"Flow execution {exec_id} not found.")
    return _json({
        "Definition": "{}",
        "ExecutionRoleArn": "",
        "FlowAliasIdentifier": alias_id,
        "FlowIdentifier": flow_id,
        "FlowVersion": "1",
    })


def _stop_flow_execution(flow_id: str, alias_id: str, exec_id: str) -> tuple:
    rec = _flow_executions.get(f"{flow_id}/{alias_id}/{exec_id}")
    if rec is None:
        return _not_found(f"Flow execution {exec_id} not found.")
    rec["Status"] = "Aborted"
    return _json({"ExecutionArn": rec["ExecutionArn"], "Status": "Aborted"})


# ===========================================================================
# Tags
# ===========================================================================


def _tag_resource(arn: str, body) -> tuple:
    body_obj, err = _parse_body(body)
    if err:
        return err
    tags = body_obj.get("tags", {})
    current = dict(_tags.get(arn, {}))
    current.update(tags)
    _tags[arn] = current
    return _empty()


def _untag_resource(arn: str, query_params) -> tuple:
    keys = query_params.get("tagKeys", []) if isinstance(query_params, dict) else []
    if isinstance(keys, str):
        keys = [keys]
    current = dict(_tags.get(arn, {}))
    for k in keys:
        current.pop(k, None)
    _tags[arn] = current
    return _empty()


def _list_tags(arn: str) -> tuple:
    return 200, {"Content-Type": "application/json"}, json.dumps({
        "tags": dict(_tags.get(arn, {})),
    }).encode()


# ===========================================================================
# Dispatcher (31 ops)
# ===========================================================================


_INVOKE_AGENT_RE = re.compile(
    r"^/agents/([^/]+)/agentAliases/([^/]+)/sessions/([^/]+)/text$"
)
_INVOKE_INLINE_RE = re.compile(r"^/agents/([^/]+)$")
_MEMORY_RE = re.compile(r"^/agents/([^/]+)/agentAliases/([^/]+)/memories$")

_RETRIEVE_RE = re.compile(r"^/knowledgebases/([^/]+)/retrieve$")

_SESSION_RE = re.compile(r"^/sessions/([^/]+)/?$")
_INVOCATIONS_RE = re.compile(r"^/sessions/([^/]+)/invocations/?$")
_INV_STEPS_RE = re.compile(r"^/sessions/([^/]+)/invocationSteps/?$")
_INV_STEP_RE = re.compile(r"^/sessions/([^/]+)/invocationSteps/([^/]+)$")

_INVOKE_FLOW_RE = re.compile(r"^/flows/([^/]+)/aliases/([^/]+)$")
_FLOW_EXECS_RE = re.compile(r"^/flows/([^/]+)/aliases/([^/]+)/executions$")
_FLOW_EXEC_RE = re.compile(r"^/flows/([^/]+)/aliases/([^/]+)/executions/([^/]+)$")
_FLOW_EXEC_STOP_RE = re.compile(r"^/flows/([^/]+)/aliases/([^/]+)/executions/([^/]+)/stop$")
_FLOW_EXEC_EVENTS_RE = re.compile(r"^/flows/([^/]+)/aliases/([^/]+)/executions/([^/]+)/events$")
_FLOW_EXEC_SNAPSHOT_RE = re.compile(r"^/flows/([^/]+)/aliases/([^/]+)/executions/([^/]+)/flowsnapshot$")
_FLOW_LIST_EXECS_RE = re.compile(r"^/flows/([^/]+)/executions$")

_TAGS_RE = re.compile(r"^/tags/(.+)$")


async def handle_request(method, path, headers, body, query_params):
    logger.debug("%s %s", method, path)

    # --- Agent runtime ---
    m = _INVOKE_AGENT_RE.match(path)
    if m and method == "POST":
        return _invoke_agent(unquote(m.group(1)), unquote(m.group(2)),
                              unquote(m.group(3)), body)
    m = _MEMORY_RE.match(path)
    if m:
        aid, alid = unquote(m.group(1)), unquote(m.group(2))
        if method == "GET":
            return _get_agent_memory(aid, alid, query_params)
        if method == "DELETE":
            return _delete_agent_memory(aid, alid, query_params)
    # InvokeInlineAgent matches POST /agents/{sessionId} (no trailing slash)
    m = _INVOKE_INLINE_RE.match(path)
    if m and method == "POST" and not path.endswith("/") and "/agentAliases/" not in path:
        return _invoke_inline_agent(unquote(m.group(1)), body)

    # --- Retrieve ---
    m = _RETRIEVE_RE.match(path)
    if m and method == "POST":
        return _retrieve(unquote(m.group(1)), body)
    if path == "/retrieveAndGenerate" and method == "POST":
        return _retrieve_and_generate(body)
    if path == "/retrieveAndGenerateStream" and method == "POST":
        return _retrieve_and_generate_stream(body)
    if path == "/rerank" and method == "POST":
        return _rerank(body)
    if path == "/generateQuery" and method == "POST":
        return _generate_query(body)
    if path == "/optimize-prompt" and method == "POST":
        return _optimize_prompt(body)

    # --- Sessions ---
    if path == "/sessions/":
        if method == "PUT":
            return _create_session(body)
        if method == "POST":
            return _list_sessions(body)
    m = _INV_STEP_RE.match(path)
    if m and method == "POST":
        return _get_invocation_step(unquote(m.group(1)), unquote(m.group(2)), body)
    m = _INV_STEPS_RE.match(path)
    if m:
        sid = unquote(m.group(1))
        if method == "PUT":
            return _put_invocation_step(sid, body)
        if method == "POST":
            return _list_invocation_steps(sid, body)
    m = _INVOCATIONS_RE.match(path)
    if m:
        sid = unquote(m.group(1))
        if method == "PUT":
            return _create_invocation(sid, body)
        if method == "POST":
            return _list_invocations(sid, body)
    m = _SESSION_RE.match(path)
    if m:
        sid = unquote(m.group(1))
        if method == "GET":
            return _get_session(sid)
        if method == "PUT":
            return _update_session(sid, body)
        if method == "DELETE":
            return _delete_session(sid)
        if method == "PATCH":
            return _end_session(sid)

    # --- Flow runtime ---
    m = _FLOW_EXEC_SNAPSHOT_RE.match(path)
    if m and method == "GET":
        return _get_execution_flow_snapshot(unquote(m.group(1)),
                                              unquote(m.group(2)),
                                              unquote(m.group(3)))
    m = _FLOW_EXEC_EVENTS_RE.match(path)
    if m and method == "GET":
        return _list_flow_execution_events(unquote(m.group(1)),
                                              unquote(m.group(2)),
                                              unquote(m.group(3)),
                                              query_params)
    m = _FLOW_EXEC_STOP_RE.match(path)
    if m and method == "POST":
        return _stop_flow_execution(unquote(m.group(1)), unquote(m.group(2)),
                                       unquote(m.group(3)))
    m = _FLOW_EXEC_RE.match(path)
    if m and method == "GET":
        return _get_flow_execution(unquote(m.group(1)), unquote(m.group(2)),
                                      unquote(m.group(3)))
    m = _FLOW_EXECS_RE.match(path)
    if m and method == "POST":
        return _start_flow_execution(unquote(m.group(1)), unquote(m.group(2)), body)
    m = _FLOW_LIST_EXECS_RE.match(path)
    if m and method == "GET":
        return _list_flow_executions(unquote(m.group(1)), query_params)
    m = _INVOKE_FLOW_RE.match(path)
    if m and method == "POST":
        return _invoke_flow(unquote(m.group(1)), unquote(m.group(2)), body)

    # --- Tags ---
    m = _TAGS_RE.match(path)
    if m:
        arn = unquote(m.group(1))
        if method == "POST":
            return _tag_resource(arn, body)
        if method == "DELETE":
            return _untag_resource(arn, query_params)
        if method == "GET":
            return _list_tags(arn)

    return _validation(f"No route for {method} {path}.")
