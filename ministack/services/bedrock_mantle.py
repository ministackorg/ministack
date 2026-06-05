"""
Bedrock Mantle Service Emulator.
OpenAI-shape inference API — signing name: bedrock-mantle.

AWS shipped bedrock-mantle as a service that exposes OpenAI-compatible
endpoints (and Anthropic-compatible). It lets clients written against the
OpenAI / Anthropic SDKs target Bedrock by overriding base_url. The byte-shape
contract is OpenAI's / Anthropic's, not AWS's — verified against the OpenAI
Chat Completions and Anthropic Messages OpenAPI/JSON specs.

Operations in v1:
  POST /v1/chat/completions      (OpenAI Chat Completions)

Behavior:
  Default: deterministic mock, family-aware reply.
  Optional proxy: MINISTACK_BEDROCK_PROXY_URL forwards to any OpenAI-compatible
    /v1/chat/completions endpoint (Ollama, llama.cpp, vLLM). Falls back to
    mock silently on failure. Same env var as bedrock-runtime so users config
    once.
  Streaming: Server-Sent Events (text/event-stream), `data: {...}\\n\\n`
    chunks, terminated with `data: [DONE]\\n\\n`. Matches OpenAI wire format
    byte-for-byte so openai-python's stream iterator drives unchanged.
"""

import hashlib
import json
import logging
import os
import time
import urllib.error
import urllib.request
import uuid

logger = logging.getLogger("bedrock-mantle")

_PROXY_URL = os.environ.get("MINISTACK_BEDROCK_PROXY_URL", "").rstrip("/")
_PROXY_TIMEOUT_S = float(os.environ.get("MINISTACK_BEDROCK_PROXY_TIMEOUT_SECONDS", "30"))


# ---------------------------------------------------------------------------
# Token / family heuristics
# ---------------------------------------------------------------------------


def _estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, len(text) // 4)


def _family(model_id: str) -> str:
    m = model_id.lower()
    if "claude" in m or "anthropic" in m:
        return "anthropic"
    if "nova" in m:
        return "nova"
    if "titan" in m:
        return "titan"
    if "llama" in m or "meta" in m:
        return "llama"
    if "mistral" in m:
        return "mistral"
    if "cohere" in m or "command" in m:
        return "cohere"
    if "gpt" in m:
        return "openai"
    return "generic"


def _flatten_content(content):
    """OpenAI accepts str or list-of-parts (e.g. [{'type':'text','text':...}])."""
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


def _messages_text(messages) -> str:
    return "\n".join(
        _flatten_content(m.get("content"))
        for m in (messages or [])
    )


# ---------------------------------------------------------------------------
# Errors (OpenAI shape — not AWS shape — since clients are OpenAI SDKs)
# ---------------------------------------------------------------------------


def _openai_error(message: str, code: str = "invalid_request_error",
                   status: int = 400) -> tuple:
    body = json.dumps({
        "error": {
            "message": message,
            "type": code,
            "param": None,
            "code": None,
        }
    }).encode()
    return status, {"Content-Type": "application/json"}, body


# ---------------------------------------------------------------------------
# Mock reply
# ---------------------------------------------------------------------------


def _mock_reply(model_id: str, messages) -> str:
    family = _family(model_id)
    digest = hashlib.sha256(_messages_text(messages).encode()).hexdigest()[:8]
    return f"[ministack mock {family} {model_id}] reply for prompt#{digest}"


# ---------------------------------------------------------------------------
# Proxy
# ---------------------------------------------------------------------------


def _proxy_forward(payload: dict) -> str | None:
    if not _PROXY_URL:
        return None
    url = f"{_PROXY_URL}/v1/chat/completions"
    forward_payload = dict(payload)
    forward_payload["stream"] = False
    req = urllib.request.Request(
        url,
        data=json.dumps(forward_payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=_PROXY_TIMEOUT_S) as resp:
            data = json.load(resp)
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
        logger.debug("mantle proxy unreachable, falling back to mock: %s", e)
        return None
    except Exception:
        logger.exception("mantle proxy malformed response, falling back to mock")
        return None
    try:
        return data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Chat Completions response builders
# ---------------------------------------------------------------------------


def _chatcmpl_id() -> str:
    return "chatcmpl-" + uuid.uuid4().hex[:24]


def _build_completion(model_id: str, messages, text: str) -> dict:
    prompt_tokens = _estimate_tokens(_messages_text(messages))
    completion_tokens = _estimate_tokens(text)
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
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
        "system_fingerprint": "ministack",
    }


# ---------------------------------------------------------------------------
# Chat Completions: validation
# ---------------------------------------------------------------------------


def _validate(body_obj) -> tuple | None:
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
            return _openai_error(
                f"messages[{i}].role must be one of system/user/assistant/tool/developer."
            )
        if "content" not in m:
            return _openai_error(f"messages[{i}].content is required.")
    return None


# ---------------------------------------------------------------------------
# Non-streaming
# ---------------------------------------------------------------------------


def _chat_completion(headers, body) -> tuple:
    try:
        body_obj = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _openai_error("Body is not valid JSON.")
    err = _validate(body_obj)
    if err:
        return err
    model_id = body_obj["model"]
    messages = body_obj["messages"]
    text = _proxy_forward(body_obj)
    if text is None:
        text = _mock_reply(model_id, messages)
    completion = _build_completion(model_id, messages, text)
    return 200, {"Content-Type": "application/json"}, json.dumps(completion).encode()


# ---------------------------------------------------------------------------
# Streaming (SSE — OpenAI chat.completion.chunk events)
# ---------------------------------------------------------------------------


def _sse(data: dict | str) -> bytes:
    if data == "[DONE]":
        return b"data: [DONE]\n\n"
    return f"data: {json.dumps(data)}\n\n".encode("utf-8")


def _build_chat_stream(model_id: str, messages, text: str) -> bytes:
    cid = _chatcmpl_id()
    created = int(time.time())
    base = {
        "id": cid,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model_id,
        "system_fingerprint": "ministack",
    }
    stream = b""
    # Role chunk
    stream += _sse({
        **base,
        "choices": [{"index": 0, "delta": {"role": "assistant", "content": ""},
                      "finish_reason": None, "logprobs": None}],
    })
    # Content chunks (split into ~5 deltas for non-trivial streams)
    if text:
        chunk_size = max(1, len(text) // 5) if len(text) > 20 else len(text)
        pos = 0
        while pos < len(text):
            piece = text[pos:pos + chunk_size]
            stream += _sse({
                **base,
                "choices": [{"index": 0, "delta": {"content": piece},
                              "finish_reason": None, "logprobs": None}],
            })
            pos += chunk_size
    # Final chunk with finish_reason
    stream += _sse({
        **base,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop",
                      "logprobs": None}],
    })
    stream += _sse("[DONE]")
    return stream


def _chat_completion_stream(headers, body) -> tuple:
    try:
        body_obj = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _openai_error("Body is not valid JSON.")
    err = _validate(body_obj)
    if err:
        return err
    model_id = body_obj["model"]
    messages = body_obj["messages"]
    text = _proxy_forward(body_obj)
    if text is None:
        text = _mock_reply(model_id, messages)
    stream_bytes = _build_chat_stream(model_id, messages, text)
    return 200, {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }, stream_bytes


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


async def handle_request(method, path, headers, body, query_params):
    logger.debug("%s %s", method, path)
    if path == "/v1/chat/completions":
        if method != "POST":
            return _openai_error(f"Unsupported method {method}.", status=405)
        # Stream flag in body decides SSE vs JSON.
        try:
            preview = json.loads(body or b"{}")
        except json.JSONDecodeError:
            return _openai_error("Body is not valid JSON.")
        if preview.get("stream") is True:
            return _chat_completion_stream(headers, body)
        return _chat_completion(headers, body)
    return _openai_error(f"No route for {method} {path}.", status=404)


# Persistence — stateless
def get_state():
    return {}


def restore_state(data):
    return None


def reset():
    return None
