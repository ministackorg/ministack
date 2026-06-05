"""
Bedrock Mantle (OpenAI Chat Completions) integration tests.

Mantle is an OpenAI-shape API (not AWS-shape), so tests drive it with raw HTTP
mirroring what openai-python emits. Validates wire-shape parity against the
OpenAI Chat Completions OpenAPI: required response fields, SSE stream framing,
error envelope, role/content validation.
"""

import json
import urllib.error
import urllib.request

from conftest import ENDPOINT


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _post_chat(body: dict) -> tuple:
    req = urllib.request.Request(
        f"{ENDPOINT}/v1/chat/completions",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _post_chat_json(body: dict) -> tuple:
    status, headers, raw = _post_chat(body)
    return status, headers, json.loads(raw)


def _post_chat_stream_lines(body: dict) -> list:
    status, headers, raw = _post_chat({**body, "stream": True})
    assert status == 200
    assert headers.get("Content-Type") == "text/event-stream"
    lines = []
    for chunk in raw.decode().split("\n\n"):
        chunk = chunk.strip()
        if not chunk:
            continue
        assert chunk.startswith("data: "), f"non-SSE chunk: {chunk!r}"
        payload = chunk[len("data: "):]
        if payload == "[DONE]":
            lines.append("[DONE]")
        else:
            lines.append(json.loads(payload))
    return lines


# ---------------------------------------------------------------------------
# Non-streaming chat completion: required shape
# ---------------------------------------------------------------------------


def test_mantle_chat_completion_required_response_fields():
    status, _, body = _post_chat_json({
        "model": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "messages": [{"role": "user", "content": "hello"}],
    })
    assert status == 200
    # OpenAI Chat Completions required fields
    for k in ("id", "object", "created", "model", "choices", "usage"):
        assert k in body, f"missing {k}"
    assert body["object"] == "chat.completion"
    assert body["id"].startswith("chatcmpl-")
    assert isinstance(body["created"], int)
    assert body["model"] == "anthropic.claude-3-5-sonnet-20241022-v2:0"
    # Choice shape
    choice = body["choices"][0]
    assert choice["index"] == 0
    assert choice["message"]["role"] == "assistant"
    assert isinstance(choice["message"]["content"], str)
    assert choice["finish_reason"] in ("stop", "length", "tool_calls",
                                        "content_filter", "function_call")
    # Usage shape (OpenAI keys, not AWS keys)
    for k in ("prompt_tokens", "completion_tokens", "total_tokens"):
        assert isinstance(body["usage"][k], int)
    assert body["usage"]["total_tokens"] == (
        body["usage"]["prompt_tokens"] + body["usage"]["completion_tokens"]
    )


def test_mantle_chat_completion_accepts_list_content_parts():
    """OpenAI accepts content as either str or [{'type':'text','text':'...'}]."""
    status, _, body = _post_chat_json({
        "model": "amazon.nova-pro-v1:0",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "part one"},
                {"type": "text", "text": "part two"},
            ],
        }],
    })
    assert status == 200
    assert body["choices"][0]["message"]["role"] == "assistant"


def test_mantle_chat_completion_multi_turn():
    status, _, body = _post_chat_json({
        "model": "meta.llama3-1-70b-instruct-v1:0",
        "messages": [
            {"role": "system", "content": "be brief"},
            {"role": "user", "content": "alpha?"},
            {"role": "assistant", "content": "beta"},
            {"role": "user", "content": "gamma?"},
        ],
    })
    assert status == 200
    assert body["usage"]["prompt_tokens"] >= 1


def test_mantle_distinct_prompts_produce_distinct_replies():
    _, _, a = _post_chat_json({
        "model": "anthropic.claude-3-haiku-20240307-v1:0",
        "messages": [{"role": "user", "content": "alpha"}],
    })
    _, _, b = _post_chat_json({
        "model": "anthropic.claude-3-haiku-20240307-v1:0",
        "messages": [{"role": "user", "content": "beta"}],
    })
    assert a["choices"][0]["message"]["content"] != b["choices"][0]["message"]["content"]


# ---------------------------------------------------------------------------
# Streaming SSE
# ---------------------------------------------------------------------------


def test_mantle_chat_completion_stream_emits_role_then_content_then_done():
    events = _post_chat_stream_lines({
        "model": "amazon.nova-lite-v1:0",
        "messages": [{"role": "user", "content": "stream"}],
    })
    # First event: role delta
    assert events[0]["choices"][0]["delta"]["role"] == "assistant"
    # Last is [DONE], penultimate has finish_reason
    assert events[-1] == "[DONE]"
    assert events[-2]["choices"][0]["finish_reason"] == "stop"
    # Middle events carry content deltas
    content = ""
    for e in events[1:-2]:
        if "content" in e["choices"][0]["delta"]:
            content += e["choices"][0]["delta"]["content"]
    assert content  # reconstructed text


def test_mantle_chat_completion_stream_all_chunks_are_completion_chunk_object():
    events = _post_chat_stream_lines({
        "model": "mistral.mistral-large-2407-v1:0",
        "messages": [{"role": "user", "content": "x"}],
    })
    for e in events[:-1]:  # everything except [DONE]
        assert e["object"] == "chat.completion.chunk"
        assert e["id"].startswith("chatcmpl-")


def test_mantle_chat_completion_stream_finish_reason_only_on_last_data_chunk():
    events = _post_chat_stream_lines({
        "model": "cohere.command-r-plus-v1:0",
        "messages": [{"role": "user", "content": "x"}],
    })
    # All events with finish_reason set should be exactly the penultimate one
    with_finish = [
        i for i, e in enumerate(events)
        if isinstance(e, dict) and e["choices"][0]["finish_reason"] is not None
    ]
    assert with_finish == [len(events) - 2]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _expect_openai_error(body: dict, expected_status: int = 400):
    status, _, parsed = _post_chat_json(body)
    assert status == expected_status, parsed
    assert "error" in parsed
    assert "message" in parsed["error"]
    assert "type" in parsed["error"]


def test_mantle_chat_completion_missing_model_field():
    _expect_openai_error({"messages": [{"role": "user", "content": "x"}]})


def test_mantle_chat_completion_empty_messages():
    _expect_openai_error({"model": "x", "messages": []})


def test_mantle_chat_completion_invalid_role():
    _expect_openai_error({
        "model": "x",
        "messages": [{"role": "bogus", "content": "y"}],
    })


def test_mantle_chat_completion_message_missing_content():
    _expect_openai_error({
        "model": "x",
        "messages": [{"role": "user"}],
    })


def test_mantle_chat_completion_malformed_json_body():
    req = urllib.request.Request(
        f"{ENDPOINT}/v1/chat/completions",
        data=b"{nope",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req):
            assert False, "expected error"
    except urllib.error.HTTPError as e:
        assert e.code == 400
        body = json.loads(e.read())
        assert body["error"]["type"] == "invalid_request_error"


def test_mantle_chat_completion_method_not_allowed_for_get():
    req = urllib.request.Request(
        f"{ENDPOINT}/v1/chat/completions",
        method="GET",
    )
    try:
        with urllib.request.urlopen(req):
            assert False, "expected error"
    except urllib.error.HTTPError as e:
        assert e.code == 405


# ---------------------------------------------------------------------------
# System fingerprint + roles
# ---------------------------------------------------------------------------


def test_mantle_chat_completion_system_fingerprint_present():
    _, _, body = _post_chat_json({
        "model": "anthropic.claude-3-haiku-20240307-v1:0",
        "messages": [{"role": "user", "content": "x"}],
    })
    assert body["system_fingerprint"] == "ministack"


def test_mantle_chat_completion_accepts_system_developer_tool_roles():
    """OpenAI added 'developer' and accepts 'tool' role messages."""
    for role in ("system", "developer", "tool"):
        status, _, body = _post_chat_json({
            "model": "x",
            "messages": [
                {"role": role, "content": f"x-{role}"},
                {"role": "user", "content": "go"},
            ],
        })
        assert status == 200, f"role {role} rejected: {body}"
