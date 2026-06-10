"""
Bedrock Runtime Service Emulator.
JSON REST API — signing name: bedrock-runtime.

Operations (all 9 ops verified against botocore bedrock-runtime-2023-09-30):
  Converse                       POST /model/{modelId}/converse                                    200
  ConverseStream                 POST /model/{modelId}/converse-stream                             200 (eventstream)
  InvokeModel                    POST /model/{modelId}/invoke                                      200
  InvokeModelWithResponseStream  POST /model/{modelId}/invoke-with-response-stream                 200 (eventstream)
  ApplyGuardrail                 POST /guardrail/{guardrailIdentifier}/version/{guardrailVersion}/apply  200
  StartAsyncInvoke               POST /async-invoke                                                200
  GetAsyncInvoke                 GET  /async-invoke/{invocationArn}                                200
  ListAsyncInvokes               GET  /async-invoke                                                200
  InvokeModelWithBidirectionalStream  — not implemented (bidirectional eventstream over HTTP/2,
                                        out of scope for non-streaming HTTP servers)

Behavior:
  Default: deterministic mock, family-aware response shape selected by modelId
    prefix (anthropic.*, amazon.titan*, amazon.nova*, meta.llama*, mistral.*,
    cohere.*, ai21.*). Wire-shape parity with AWS in every field; generated
    text is a deterministic canned reply that echoes a hash of the prompt so
    observability tools don't flag it as cached.
  Optional proxy: MINISTACK_BEDROCK_PROXY_URL points at any OpenAI-compatible
    /chat/completions endpoint (Ollama, llama.cpp, vLLM). When set and
    reachable, the prompt is translated to OpenAI shape, forwarded, and the
    response translated back to Converse shape. Falls back to mock silently
    on connection error.

Token counts are a heuristic (chars/4) — shape-correct, absolute counts are
approximate. Documented in release notes; users asserting on exact counts
should not assert against a mock.
"""

import asyncio
import base64
import hashlib
import json
import logging
import os
import re
import struct
import time
import urllib.error
import urllib.request
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

logger = logging.getLogger("bedrock-runtime")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_PROXY_URL = os.environ.get("MINISTACK_BEDROCK_PROXY_URL", "").rstrip("/")
_PROXY_TIMEOUT_S = float(os.environ.get("MINISTACK_BEDROCK_PROXY_TIMEOUT_SECONDS", "30"))

# ---------------------------------------------------------------------------
# Path regexes
# ---------------------------------------------------------------------------

# modelId can be a plain id ("anthropic.claude-3-5-sonnet-20240620-v1:0"),
# an inference profile id ("us.anthropic.claude-3-5-sonnet-20240620-v1:0"),
# or a full ARN ("arn:aws:bedrock:us-east-1:123:foundation-model/...").
# It's URL-encoded by SDKs because of the ":" — match the encoded form too.
_CONVERSE_RE = re.compile(r"^/model/(.+?)/converse$")
_CONVERSE_STREAM_RE = re.compile(r"^/model/(.+?)/converse-stream$")

# ---------------------------------------------------------------------------
# Model family detection
# ---------------------------------------------------------------------------

_FAMILY_PATTERNS = [
    ("anthropic", re.compile(r"(^|[./])anthropic\.", re.I)),
    ("nova", re.compile(r"(^|[./])amazon\.nova", re.I)),
    ("titan", re.compile(r"(^|[./])amazon\.titan", re.I)),
    ("llama", re.compile(r"(^|[./])meta\.llama", re.I)),
    ("mistral", re.compile(r"(^|[./])mistral\.", re.I)),
    ("cohere", re.compile(r"(^|[./])cohere\.", re.I)),
    ("ai21", re.compile(r"(^|[./])ai21\.", re.I)),
    ("deepseek", re.compile(r"(^|[./])deepseek\.", re.I)),
]


def _family(model_id: str) -> str:
    for name, pat in _FAMILY_PATTERNS:
        if pat.search(model_id):
            return name
    return "generic"


# ---------------------------------------------------------------------------
# Token estimation (heuristic — chars/4, shape-correct only)
# ---------------------------------------------------------------------------


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, len(text) // 4)


def _messages_text(messages) -> str:
    chunks = []
    for m in messages or []:
        for c in m.get("content") or []:
            if isinstance(c, dict) and "text" in c:
                chunks.append(c["text"])
    return "\n".join(chunks)


def _system_text(system) -> str:
    chunks = []
    for s in system or []:
        if isinstance(s, dict) and "text" in s:
            chunks.append(s["text"])
    return "\n".join(chunks)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def _error(code: str, message: str, status: int) -> tuple:
    body = json.dumps({"message": message, "__type": code}).encode()
    return status, {"Content-Type": "application/json"}, body


# ---------------------------------------------------------------------------
# Mock response generator
# ---------------------------------------------------------------------------


def _mock_reply(model_id: str, messages, system) -> str:
    """Deterministic canned reply that echoes a hash of the input so
    consecutive calls with different inputs produce distinguishable output
    (observability tools flag identical outputs as broken caching)."""
    family = _family(model_id)
    prompt = _system_text(system) + "\n" + _messages_text(messages)
    digest = hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:8]
    return f"[ministack mock {family} {model_id}] reply for prompt#{digest}"


def _build_converse_response(model_id: str, messages, system, started_at_ms: int, text: str,
                              input_tokens: int | None = None,
                              output_tokens: int | None = None) -> dict:
    if input_tokens is None:
        input_tokens = _estimate_tokens(_system_text(system) + _messages_text(messages))
    if output_tokens is None:
        output_tokens = _estimate_tokens(text)
    latency_ms = max(1, int(time.time() * 1000) - started_at_ms)
    return {
        "output": {
            "message": {
                "role": "assistant",
                "content": [{"text": text}],
            }
        },
        "stopReason": "end_turn",
        "usage": {
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "totalTokens": input_tokens + output_tokens,
        },
        "metrics": {"latencyMs": latency_ms},
    }


# ---------------------------------------------------------------------------
# Optional proxy to OpenAI-compatible runtime (Ollama / llama.cpp / vLLM)
# ---------------------------------------------------------------------------


def _proxy_to_openai_chat(model_id: str, messages, system, inference_config) -> str | None:
    """Forward to MINISTACK_BEDROCK_PROXY_URL using OpenAI chat-completions
    shape. Returns assistant text on success, None on any failure (caller falls
    back to mock)."""
    if not _PROXY_URL:
        return None
    openai_messages = []
    for s in system or []:
        if isinstance(s, dict) and "text" in s:
            openai_messages.append({"role": "system", "content": s["text"]})
    for m in messages or []:
        role = m.get("role", "user")
        parts = []
        for c in m.get("content") or []:
            if isinstance(c, dict) and "text" in c:
                parts.append(c["text"])
        if parts:
            openai_messages.append({"role": role, "content": "\n".join(parts)})
    payload = {
        "model": model_id,
        "messages": openai_messages,
        "stream": False,
    }
    if inference_config:
        if "maxTokens" in inference_config:
            payload["max_tokens"] = inference_config["maxTokens"]
        if "temperature" in inference_config:
            payload["temperature"] = inference_config["temperature"]
        if "topP" in inference_config:
            payload["top_p"] = inference_config["topP"]
        if "stopSequences" in inference_config:
            payload["stop"] = inference_config["stopSequences"]
    url = f"{_PROXY_URL}/v1/chat/completions"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=_PROXY_TIMEOUT_S) as resp:
            data = json.load(resp)
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
        logger.debug("bedrock proxy unreachable, falling back to mock: %s", e)
        return None
    except Exception:
        logger.exception("bedrock proxy returned malformed response, falling back to mock")
        return None
    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Converse (non-streaming)
# ---------------------------------------------------------------------------


def _validate_converse_request(body_obj) -> tuple | None:
    if not isinstance(body_obj, dict):
        return _error("ValidationException", "Request body must be a JSON object.", 400)
    messages = body_obj.get("messages")
    if messages is not None and not isinstance(messages, list):
        return _error("ValidationException", "messages must be an array.", 400)
    if messages:
        for i, m in enumerate(messages):
            if not isinstance(m, dict):
                return _error("ValidationException", f"messages[{i}] must be an object.", 400)
            if m.get("role") not in ("user", "assistant"):
                return _error("ValidationException",
                              f"messages[{i}].role must be 'user' or 'assistant'.", 400)
            content = m.get("content")
            if not isinstance(content, list) or not content:
                return _error("ValidationException",
                              f"messages[{i}].content must be a non-empty array.", 400)
    return None


def _converse(model_id: str, headers, body) -> tuple:
    try:
        body_obj = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _error("ValidationException", "Body is not valid JSON.", 400)
    err = _validate_converse_request(body_obj)
    if err:
        return err
    messages = body_obj.get("messages", [])
    system = body_obj.get("system", [])
    inference_config = body_obj.get("inferenceConfig") or {}
    started = int(time.time() * 1000)
    text = _proxy_to_openai_chat(model_id, messages, system, inference_config)
    if text is None:
        text = _mock_reply(model_id, messages, system)
    response = _build_converse_response(model_id, messages, system, started, text)
    resp_headers = {
        "Content-Type": "application/json",
        "x-amzn-bedrock-input-token-count": str(response["usage"]["inputTokens"]),
        "x-amzn-bedrock-output-token-count": str(response["usage"]["outputTokens"]),
    }
    return 200, resp_headers, json.dumps(response).encode()


# ---------------------------------------------------------------------------
# Eventstream encoder (vnd.amazon.eventstream)
# ---------------------------------------------------------------------------


def _es_encode_message(headers: dict, payload: bytes) -> bytes:
    hdr_bytes = bytearray()
    for name, value in headers.items():
        name_b = name.encode("utf-8")
        val_b = value.encode("utf-8")
        hdr_bytes.append(len(name_b))
        hdr_bytes.extend(name_b)
        hdr_bytes.append(7)  # type 7 = string
        hdr_bytes.extend(len(val_b).to_bytes(2, "big"))
        hdr_bytes.extend(val_b)
    headers_length = len(hdr_bytes)
    total_length = 12 + headers_length + len(payload) + 4
    prelude = total_length.to_bytes(4, "big") + headers_length.to_bytes(4, "big")
    prelude_crc = zlib.crc32(prelude).to_bytes(4, "big")
    msg_head = prelude + prelude_crc + bytes(hdr_bytes) + payload
    message_crc = zlib.crc32(msg_head).to_bytes(4, "big")
    return msg_head + message_crc


def _es_event(event_type: str, payload: dict) -> bytes:
    return _es_encode_message(
        {
            ":message-type": "event",
            ":event-type": event_type,
            ":content-type": "application/json",
        },
        json.dumps(payload).encode("utf-8"),
    )


def _build_converse_stream(model_id: str, messages, system, started_at_ms: int, text: str) -> bytes:
    """Emit the AWS ConverseStream event sequence:
      messageStart -> contentBlockDelta* -> contentBlockStop -> messageStop -> metadata
    All events under :event-type, payload is application/json per AWS wire trace.
    """
    stream = b""
    stream += _es_event("messageStart", {"role": "assistant"})
    # Chunk text into ~20-char deltas so streaming consumers see multiple events
    chunk_size = max(1, len(text) // 5) if len(text) > 20 else len(text)
    pos = 0
    while pos < len(text):
        delta = text[pos:pos + chunk_size]
        stream += _es_event("contentBlockDelta", {
            "contentBlockIndex": 0,
            "delta": {"text": delta},
        })
        pos += chunk_size
    if not text:
        stream += _es_event("contentBlockDelta", {
            "contentBlockIndex": 0,
            "delta": {"text": ""},
        })
    stream += _es_event("contentBlockStop", {"contentBlockIndex": 0})
    stream += _es_event("messageStop", {"stopReason": "end_turn"})
    input_tokens = _estimate_tokens(_system_text(system) + _messages_text(messages))
    output_tokens = _estimate_tokens(text)
    latency_ms = max(1, int(time.time() * 1000) - started_at_ms)
    stream += _es_event("metadata", {
        "usage": {
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "totalTokens": input_tokens + output_tokens,
        },
        "metrics": {"latencyMs": latency_ms},
    })
    return stream


def _converse_stream(model_id: str, headers, body) -> tuple:
    try:
        body_obj = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _error("ValidationException", "Body is not valid JSON.", 400)
    err = _validate_converse_request(body_obj)
    if err:
        return err
    messages = body_obj.get("messages", [])
    system = body_obj.get("system", [])
    inference_config = body_obj.get("inferenceConfig") or {}
    started = int(time.time() * 1000)
    text = _proxy_to_openai_chat(model_id, messages, system, inference_config)
    if text is None:
        text = _mock_reply(model_id, messages, system)
    stream_bytes = _build_converse_stream(model_id, messages, system, started, text)
    resp_headers = {
        "Content-Type": "application/vnd.amazon.eventstream",
        "x-amzn-bedrock-content-type": "application/json",
    }
    return 200, resp_headers, stream_bytes


# ---------------------------------------------------------------------------
# InvokeModel — raw body in/out, family-specific request/response shapes
# ---------------------------------------------------------------------------
#
# Per AWS docs, the request and response bodies for /model/{id}/invoke vary by
# model family. We accept the family-specific request, generate a deterministic
# mock reply (or proxy), then emit a family-shaped response so consumers
# parsing the JSON body see the right keys.


def _extract_prompt_from_invoke_body(family: str, body_obj: dict) -> str:
    """Pull the prompt text out of a family-specific InvokeModel body."""
    if family == "anthropic":
        # Anthropic Messages API
        if "messages" in body_obj:
            chunks = []
            for m in body_obj["messages"]:
                c = m.get("content")
                if isinstance(c, str):
                    chunks.append(c)
                elif isinstance(c, list):
                    for part in c:
                        if isinstance(part, dict) and part.get("type") == "text":
                            chunks.append(part.get("text", ""))
            return "\n".join(chunks)
        # Anthropic legacy text completion
        return body_obj.get("prompt", "")
    if family == "titan":
        return body_obj.get("inputText", "")
    if family == "nova":
        # Nova uses messages-style under "messages"
        chunks = []
        for m in body_obj.get("messages", []):
            for c in m.get("content") or []:
                if isinstance(c, dict) and "text" in c:
                    chunks.append(c["text"])
        return "\n".join(chunks)
    if family in ("llama", "mistral", "cohere", "ai21", "deepseek", "generic"):
        return body_obj.get("prompt", "")
    return body_obj.get("prompt", "")


def _build_invoke_response_body(family: str, model_id: str, prompt: str, reply: str) -> dict:
    """Return family-specific response body matching the format AWS emits."""
    in_tok = _estimate_tokens(prompt)
    out_tok = _estimate_tokens(reply)
    if family == "anthropic":
        # Anthropic Messages API response shape
        return {
            "id": "msg_" + uuid.uuid4().hex[:24],
            "type": "message",
            "role": "assistant",
            "model": model_id,
            "content": [{"type": "text", "text": reply}],
            "stop_reason": "end_turn",
            "stop_sequence": None,
            "usage": {"input_tokens": in_tok, "output_tokens": out_tok},
        }
    if family == "titan":
        return {
            "inputTextTokenCount": in_tok,
            "results": [{
                "tokenCount": out_tok,
                "outputText": reply,
                "completionReason": "FINISH",
            }],
        }
    if family == "nova":
        return {
            "output": {"message": {"role": "assistant",
                                     "content": [{"text": reply}]}},
            "stopReason": "end_turn",
            "usage": {"inputTokens": in_tok, "outputTokens": out_tok,
                       "totalTokens": in_tok + out_tok},
        }
    if family == "llama":
        return {
            "generation": reply,
            "prompt_token_count": in_tok,
            "generation_token_count": out_tok,
            "stop_reason": "stop",
        }
    if family == "mistral":
        return {
            "outputs": [{"text": reply, "stop_reason": "stop"}],
        }
    if family == "cohere":
        return {
            "generations": [{
                "id": uuid.uuid4().hex,
                "text": reply,
                "finish_reason": "COMPLETE",
            }],
            "id": uuid.uuid4().hex,
            "prompt": prompt,
        }
    if family == "ai21":
        # AI21 Jamba uses OpenAI-like shape
        return {
            "id": "chat-" + uuid.uuid4().hex[:24],
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": reply},
                "finish_reason": "stop",
            }],
            "usage": {"prompt_tokens": in_tok, "completion_tokens": out_tok,
                       "total_tokens": in_tok + out_tok},
            "model": model_id,
        }
    # generic / deepseek fallback
    return {
        "outputs": [{"text": reply}],
        "usage": {"input_tokens": in_tok, "output_tokens": out_tok},
    }


def _invoke_model(model_id: str, headers, body) -> tuple:
    """POST /model/{modelId}/invoke — body is raw JSON in model-family shape."""
    if not body:
        return _error("ValidationException", "Request body is required.", 400)
    try:
        body_obj = json.loads(body)
    except json.JSONDecodeError:
        return _error("ValidationException", "Body is not valid JSON.", 400)
    if not isinstance(body_obj, dict):
        return _error("ValidationException", "Body must be a JSON object.", 400)
    family = _family(model_id)
    prompt = _extract_prompt_from_invoke_body(family, body_obj)
    reply = _proxy_to_openai_chat(
        model_id,
        [{"role": "user", "content": [{"text": prompt}]}] if prompt else [],
        [],
        body_obj.get("inferenceConfig"),
    )
    if reply is None:
        reply = _mock_reply(model_id, [
            {"role": "user", "content": [{"text": prompt}]},
        ], [])
    response_body = _build_invoke_response_body(family, model_id, prompt, reply)
    in_tok = _estimate_tokens(prompt)
    out_tok = _estimate_tokens(reply)
    return 200, {
        "Content-Type": "application/json",
        "x-amzn-bedrock-input-token-count": str(in_tok),
        "x-amzn-bedrock-output-token-count": str(out_tok),
    }, json.dumps(response_body).encode()


def _build_invoke_stream(family: str, model_id: str, prompt: str, reply: str) -> bytes:
    """Emit vnd.amazon.eventstream chunks for InvokeModelWithResponseStream.

    AWS frames each chunk as `:event-type=chunk` with a JSON payload that
    contains the family-specific delta wrapped in `bytes` field (base64).
    Real wire trace: `{"bytes": "<base64 of family chunk JSON>"}`.
    """
    stream = b""
    # Split reply into ~5 deltas
    chunk_size = max(1, len(reply) // 5) if len(reply) > 20 else max(1, len(reply))
    pos = 0
    chunks = []
    while pos < len(reply):
        chunks.append(reply[pos:pos + chunk_size])
        pos += chunk_size
    if not chunks:
        chunks = [""]
    for i, piece in enumerate(chunks):
        is_last = i == len(chunks) - 1
        if family == "anthropic":
            if i == 0:
                # message_start
                inner = {
                    "type": "message_start",
                    "message": {
                        "id": "msg_" + uuid.uuid4().hex[:24],
                        "type": "message",
                        "role": "assistant",
                        "model": model_id,
                        "content": [],
                        "stop_reason": None,
                        "stop_sequence": None,
                        "usage": {"input_tokens": _estimate_tokens(prompt),
                                   "output_tokens": 0},
                    },
                }
                stream += _es_event_chunk(inner)
                # content_block_start
                stream += _es_event_chunk({
                    "type": "content_block_start",
                    "index": 0,
                    "content_block": {"type": "text", "text": ""},
                })
            stream += _es_event_chunk({
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": piece},
            })
            if is_last:
                stream += _es_event_chunk({"type": "content_block_stop", "index": 0})
                stream += _es_event_chunk({
                    "type": "message_delta",
                    "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                    "usage": {"output_tokens": _estimate_tokens(reply)},
                })
                stream += _es_event_chunk({"type": "message_stop"})
        elif family == "titan":
            stream += _es_event_chunk({
                "outputText": piece,
                "index": i,
                "totalOutputTextTokenCount": _estimate_tokens(reply) if is_last else None,
                "completionReason": "FINISH" if is_last else None,
                "inputTextTokenCount": _estimate_tokens(prompt) if i == 0 else None,
            })
        elif family == "llama":
            stream += _es_event_chunk({
                "generation": piece,
                "prompt_token_count": _estimate_tokens(prompt) if i == 0 else None,
                "generation_token_count": _estimate_tokens(reply) if is_last else None,
                "stop_reason": "stop" if is_last else None,
            })
        elif family == "mistral":
            stream += _es_event_chunk({
                "outputs": [{"text": piece,
                              "stop_reason": "stop" if is_last else None}],
            })
        elif family == "cohere":
            stream += _es_event_chunk({
                "is_finished": is_last,
                "event_type": "text-generation",
                "text": piece,
                "finish_reason": "COMPLETE" if is_last else None,
            })
        else:
            stream += _es_event_chunk({"outputs": [{"text": piece}],
                                          "stop_reason": "stop" if is_last else None})
    return stream


def _es_event_chunk(inner: dict) -> bytes:
    """AWS InvokeModelWithResponseStream wraps each family-chunk JSON in a
    `{"bytes": "<base64>"}` envelope inside an eventstream `chunk` event."""
    inner_bytes = json.dumps(inner, separators=(",", ":")).encode("utf-8")
    payload = {"bytes": base64.b64encode(inner_bytes).decode("ascii")}
    return _es_encode_message({
        ":message-type": "event",
        ":event-type": "chunk",
        ":content-type": "application/json",
    }, json.dumps(payload).encode("utf-8"))


def _invoke_model_with_response_stream(model_id: str, headers, body) -> tuple:
    if not body:
        return _error("ValidationException", "Request body is required.", 400)
    try:
        body_obj = json.loads(body)
    except json.JSONDecodeError:
        return _error("ValidationException", "Body is not valid JSON.", 400)
    if not isinstance(body_obj, dict):
        return _error("ValidationException", "Body must be a JSON object.", 400)
    family = _family(model_id)
    prompt = _extract_prompt_from_invoke_body(family, body_obj)
    reply = _proxy_to_openai_chat(
        model_id,
        [{"role": "user", "content": [{"text": prompt}]}] if prompt else [],
        [],
        body_obj.get("inferenceConfig"),
    )
    if reply is None:
        reply = _mock_reply(model_id, [
            {"role": "user", "content": [{"text": prompt}]},
        ], [])
    stream_bytes = _build_invoke_stream(family, model_id, prompt, reply)
    return 200, {
        "Content-Type": "application/vnd.amazon.eventstream",
        "x-amzn-bedrock-content-type": "application/json",
    }, stream_bytes


# ---------------------------------------------------------------------------
# ApplyGuardrail
# ---------------------------------------------------------------------------


def _apply_guardrail(guardrail_id: str, version: str, body) -> tuple:
    try:
        body_obj = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _error("ValidationException", "Body is not valid JSON.", 400)
    if not isinstance(body_obj, dict):
        return _error("ValidationException", "Body must be a JSON object.", 400)
    source = body_obj.get("source")
    content = body_obj.get("content")
    if source not in ("INPUT", "OUTPUT"):
        return _error("ValidationException",
                      "source must be one of: INPUT, OUTPUT.", 400)
    if not isinstance(content, list):
        return _error("ValidationException", "content must be an array.", 400)
    # Mock evaluation: pass everything through unchanged.
    outputs = []
    total_chars = 0
    for block in content:
        if isinstance(block, dict) and "text" in block:
            text = block["text"].get("text", "")
            outputs.append({"text": text})
            total_chars += len(text)
    response = {
        "usage": {
            "topicPolicyUnits": 0,
            "contentPolicyUnits": 0,
            "wordPolicyUnits": 0,
            "sensitiveInformationPolicyUnits": 0,
            "sensitiveInformationPolicyFreeUnits": 0,
            "contextualGroundingPolicyUnits": 0,
        },
        "action": "NONE",
        "outputs": outputs,
        "assessments": [{
            "topicPolicy": None,
            "contentPolicy": None,
            "wordPolicy": None,
            "sensitiveInformationPolicy": None,
            "contextualGroundingPolicy": None,
        }] if outputs else [],
        "guardrailCoverage": {
            "textCharacters": {"guarded": total_chars, "total": total_chars},
        },
    }
    return 200, {"Content-Type": "application/json"}, json.dumps(response).encode()


# ---------------------------------------------------------------------------
# Async invoke (state-only — mocks run to COMPLETED instantly)
# ---------------------------------------------------------------------------

_async_invokes = AccountRegionScopedDict()  # invocationArn -> dict


try:
    _restored = load_state("bedrock_runtime")
    if _restored:
        _async_invokes.update(_restored.get("async_invokes", {}))
except Exception:
    logger.exception("Failed to restore bedrock_runtime state; continuing fresh")


def _invocation_arn(invocation_id: str) -> str:
    return (f"arn:aws:bedrock:{get_region()}:{get_account_id()}:"
            f"async-invoke/{invocation_id}")


def _start_async_invoke(body) -> tuple:
    try:
        body_obj = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _error("ValidationException", "Body is not valid JSON.", 400)
    if not isinstance(body_obj, dict):
        return _error("ValidationException", "Body must be a JSON object.", 400)
    if not body_obj.get("modelId"):
        return _error("ValidationException", "modelId is required.", 400)
    if "modelInput" not in body_obj:
        return _error("ValidationException", "modelInput is required.", 400)
    if "outputDataConfig" not in body_obj:
        return _error("ValidationException", "outputDataConfig is required.", 400)
    inv_id = uuid.uuid4().hex
    arn = _invocation_arn(inv_id)
    now = datetime.now(timezone.utc).isoformat()
    record = {
        "invocationArn": arn,
        "modelArn": f"arn:aws:bedrock:{get_region()}::foundation-model/{body_obj['modelId']}",
        "clientRequestToken": body_obj.get("clientRequestToken", uuid.uuid4().hex),
        "status": "Completed",
        "submitTime": now,
        "lastModifiedTime": now,
        "endTime": now,
        "outputDataConfig": body_obj["outputDataConfig"],
    }
    _async_invokes[arn] = record
    return 200, {"Content-Type": "application/json"}, json.dumps({
        "invocationArn": arn,
    }).encode()


def _get_async_invoke(arn: str) -> tuple:
    record = _async_invokes.get(arn)
    if record is None:
        return _error("ResourceNotFoundException",
                      f"Async invocation {arn} not found.", 404)
    return 200, {"Content-Type": "application/json"}, json.dumps(record).encode()


def _list_async_invokes(query_params) -> tuple:
    status_eq = (query_params.get("statusEquals") or [None])[0] if isinstance(query_params, dict) else None
    summaries = []
    for rec in _async_invokes.values():
        if status_eq and rec["status"] != status_eq:
            continue
        summaries.append(rec)
    return 200, {"Content-Type": "application/json"}, json.dumps({
        "nextToken": None,
        "asyncInvokeSummaries": summaries,
    }).encode()


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


_INVOKE_STREAM_RE = re.compile(r"^/model/(.+?)/invoke-with-response-stream$")
_INVOKE_RE = re.compile(r"^/model/(.+?)/invoke$")
_APPLY_GUARDRAIL_RE = re.compile(
    r"^/guardrail/([^/]+)/version/([^/]+)/apply$"
)
_ASYNC_INVOKE_GET_RE = re.compile(r"^/async-invoke/(.+)$")


# ---------------------------------------------------------------------------
# OpenAI-compatible Chat Completions
# Bedrock's OpenAI-shape inference surface, served on the bedrock-runtime host
# at /v1/chat/completions (the byte-shape contract is OpenAI's, not AWS's, since
# the clients are the OpenAI SDKs pointed at Bedrock via base_url). Mock by
# default; forwards to MINISTACK_BEDROCK_PROXY_URL when set, same as Converse.
# ---------------------------------------------------------------------------


def _openai_flatten_content(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for p in content:
            if isinstance(p, dict) and p.get("type") == "text":
                parts.append(p.get("text", ""))
            elif isinstance(p, str):
                parts.append(p)
        return "\n".join(parts)
    return ""


def _openai_messages_text(messages) -> str:
    return "\n".join(_openai_flatten_content(m.get("content")) for m in (messages or []))


def _openai_error(message, code="invalid_request_error", status=400) -> tuple:
    body = json.dumps({"error": {"message": message, "type": code, "param": None, "code": None}}).encode()
    return status, {"Content-Type": "application/json"}, body


def _chatcmpl_id() -> str:
    return "chatcmpl-" + uuid.uuid4().hex[:24]


def _openai_mock_reply(model_id, messages) -> str:
    digest = hashlib.sha256(_openai_messages_text(messages).encode()).hexdigest()[:8]
    return f"[ministack mock {_family(model_id)} {model_id}] reply for prompt#{digest}"


def _proxy_openai_passthrough(payload: dict):
    """Forward a raw OpenAI chat-completions payload to the configured proxy.
    Returns the assistant text, or None on no-proxy / failure (caller mocks)."""
    if not _PROXY_URL:
        return None
    fwd = dict(payload)
    fwd["stream"] = False
    req = urllib.request.Request(
        f"{_PROXY_URL}/v1/chat/completions",
        data=json.dumps(fwd).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=_PROXY_TIMEOUT_S) as resp:
            data = json.load(resp)
        return data["choices"][0]["message"]["content"]
    except Exception:
        logger.debug("openai proxy unreachable/malformed, falling back to mock")
        return None


def _build_chat_completion(model_id, messages, text) -> dict:
    pt = _estimate_tokens(_openai_messages_text(messages))
    ct = _estimate_tokens(text)
    return {
        "id": _chatcmpl_id(),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model_id,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": text},
            "finish_reason": "stop",
            "logprobs": None,
        }],
        "usage": {"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct},
        "system_fingerprint": "ministack",
    }


def _validate_openai_chat(body_obj):
    if not isinstance(body_obj, dict):
        return _openai_error("Request body must be a JSON object.")
    if "model" not in body_obj or not isinstance(body_obj["model"], str):
        return _openai_error("'model' is a required field.")
    messages = body_obj.get("messages")
    if not isinstance(messages, list) or not messages:
        return _openai_error("'messages' must be a non-empty array.")
    for i, m in enumerate(messages):
        if not isinstance(m, dict):
            return _openai_error(f"messages[{i}] must be an object.")
        if m.get("role") not in ("system", "user", "assistant", "tool", "developer"):
            return _openai_error(f"messages[{i}].role must be one of system/user/assistant/tool/developer.")
        if "content" not in m:
            return _openai_error(f"messages[{i}].content is required.")
    return None


def _openai_sse(data) -> bytes:
    if data == "[DONE]":
        return b"data: [DONE]\n\n"
    return f"data: {json.dumps(data)}\n\n".encode("utf-8")


def _build_chat_stream(model_id, messages, text) -> bytes:
    base = {
        "id": _chatcmpl_id(),
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model_id,
        "system_fingerprint": "ministack",
    }
    out = _openai_sse({**base, "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""}, "finish_reason": None, "logprobs": None}]})
    if text:
        chunk_size = max(1, len(text) // 5) if len(text) > 20 else len(text)
        pos = 0
        while pos < len(text):
            out += _openai_sse({**base, "choices": [{"index": 0, "delta": {"content": text[pos:pos + chunk_size]}, "finish_reason": None, "logprobs": None}]})
            pos += chunk_size
    out += _openai_sse({**base, "choices": [{"index": 0, "delta": {}, "finish_reason": "stop", "logprobs": None}]})
    out += _openai_sse("[DONE]")
    return out


def _openai_chat_completion(headers, body) -> tuple:
    try:
        body_obj = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _openai_error("Body is not valid JSON.")
    err = _validate_openai_chat(body_obj)
    if err:
        return err
    model_id = body_obj["model"]
    messages = body_obj["messages"]
    text = _proxy_openai_passthrough(body_obj)
    if text is None:
        text = _openai_mock_reply(model_id, messages)
    if body_obj.get("stream") is True:
        return 200, {
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }, _build_chat_stream(model_id, messages, text)
    return 200, {"Content-Type": "application/json"}, json.dumps(_build_chat_completion(model_id, messages, text)).encode()


async def handle_request(method, path, headers, body, query_params):
    logger.debug("%s %s", method, path)

    # OpenAI-compatible chat completions — AWS serves this on the bedrock-runtime
    # surface, so it's handled here rather than as a separate service.
    if path == "/v1/chat/completions":
        if method != "POST":
            return _openai_error(f"Unsupported method {method}.", status=405)
        return await asyncio.to_thread(_openai_chat_completion, headers, body)

    # Proxy-capable handlers run in a worker thread: a slow
    # MINISTACK_BEDROCK_PROXY_URL must never block the single-port event loop.
    if method == "POST":
        m = _CONVERSE_STREAM_RE.match(path)
        if m:
            return await asyncio.to_thread(_converse_stream, unquote(m.group(1)), headers, body)
        m = _CONVERSE_RE.match(path)
        if m:
            return await asyncio.to_thread(_converse, unquote(m.group(1)), headers, body)
        m = _INVOKE_STREAM_RE.match(path)
        if m:
            return await asyncio.to_thread(_invoke_model_with_response_stream, unquote(m.group(1)), headers, body)
        m = _INVOKE_RE.match(path)
        if m:
            return await asyncio.to_thread(_invoke_model, unquote(m.group(1)), headers, body)
        m = _APPLY_GUARDRAIL_RE.match(path)
        if m:
            return _apply_guardrail(unquote(m.group(1)), unquote(m.group(2)), body)
        if path == "/async-invoke":
            return _start_async_invoke(body)

    if method == "GET":
        if path == "/async-invoke":
            return _list_async_invokes(query_params)
        m = _ASYNC_INVOKE_GET_RE.match(path)
        if m:
            return _get_async_invoke(unquote(m.group(1)))

    return _error("ValidationException", f"No route for {method} {path}", 400)


# Persistence — async invoke records persist; everything else stateless
def get_state():
    import copy
    return copy.deepcopy({"async_invokes": _async_invokes})


def restore_state(data):
    if not data:
        return
    _async_invokes.update(data.get("async_invokes", {}))


def reset():
    _async_invokes.clear()
