"""
AWS Bedrock Runtime Service Emulator.
REST-based API for model inference, guardrails, token counting, and async invocations.
Supports: Converse, InvokeModel, ApplyGuardrail, CountTokens,
          StartAsyncInvoke, GetAsyncInvoke, ListAsyncInvokes.

Proxies inference requests to LiteLLM (which routes to Ollama or GitHub Copilot).
Requires LiteLLM to be running — returns ServiceUnavailableException if unavailable.
"""

import json
import logging
import os
import re
import threading
import time

from ministack.core.responses import error_response_json, json_response, new_uuid, now_iso

logger = logging.getLogger("bedrock-runtime")

LITELLM_BASE_URL = os.environ.get("LITELLM_BASE_URL", "http://litellm:4000")
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://ollama:11434")
ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# In-memory state
_guardrails: dict = {}
_async_invocations: dict = {}  # invocation_arn -> metadata
_async_lock = threading.Lock()


async def _call_llm(model: str, messages: list, max_tokens: int = 1024,
                    temperature: float = 0.7, top_p: float = 1.0) -> dict:
    """Call the LLM backend. Uses Ollama directly for qwen3 (to disable thinking mode),
    LiteLLM for everything else."""
    import aiohttp

    if "qwen3" in model:
        # Ollama direct — with think=false to disable thinking mode
        ollama_messages = [{"role": m["role"], "content": m["content"]} for m in messages]
        payload = {
            "model": model,
            "messages": ollama_messages,
            "stream": False,
            "think": False,
            "options": {"num_predict": max_tokens, "temperature": temperature, "top_p": top_p},
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{OLLAMA_BASE_URL}/api/chat",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=300),
            ) as resp:
                if resp.status != 200:
                    error_body = await resp.text()
                    raise RuntimeError(f"Ollama returned {resp.status}: {error_body}")
                data = await resp.json()
        return {
            "choices": [{"message": {"content": data.get("message", {}).get("content", ""),
                                     "role": "assistant"},
                         "finish_reason": "stop" if data.get("done") else "length"}],
            "usage": {"prompt_tokens": data.get("prompt_eval_count", 0),
                      "completion_tokens": data.get("eval_count", 0),
                      "total_tokens": data.get("prompt_eval_count", 0) + data.get("eval_count", 0)},
        }
    else:
        # LiteLLM proxy
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "top_p": top_p,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{LITELLM_BASE_URL}/v1/chat/completions",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=300),
            ) as resp:
                if resp.status != 200:
                    error_body = await resp.text()
                    raise RuntimeError(f"LiteLLM returned {resp.status}: {error_body}")
                return await resp.json()

# ---------------------------------------------------------------------------
# Path routing patterns
# ---------------------------------------------------------------------------

_RE_CONVERSE = re.compile(r"^/model/([^/]+)/converse$")
_RE_INVOKE = re.compile(r"^/model/([^/]+)/invoke$")
_RE_COUNT_TOKENS = re.compile(r"^/model/([^/]+)/count-tokens$")
_RE_GUARDRAIL = re.compile(r"^/guardrail/([^/]+)/version/([^/]+)/apply$")
_RE_ASYNC_INVOKE_ID = re.compile(r"^/async-invoke/(.+)$")
_RE_ASYNC_INVOKE = re.compile(r"^/async-invoke/?$")


async def handle_request(method, path, headers, body, query_params):
    """Main entry point for Bedrock Runtime requests."""
    # Converse
    m = _RE_CONVERSE.match(path)
    if m and method == "POST":
        return await _converse(m.group(1), body)

    # InvokeModel
    m = _RE_INVOKE.match(path)
    if m and method == "POST":
        return await _invoke_model(m.group(1), body, headers)

    # CountTokens
    m = _RE_COUNT_TOKENS.match(path)
    if m and method == "POST":
        return await _count_tokens(m.group(1), body)

    # ApplyGuardrail
    m = _RE_GUARDRAIL.match(path)
    if m and method == "POST":
        return await _apply_guardrail(m.group(1), m.group(2), body)

    # GetAsyncInvoke (must match before ASYNC_INVOKE base path)
    m = _RE_ASYNC_INVOKE_ID.match(path)
    if m and method == "GET":
        return _get_async_invoke(m.group(1))

    # StartAsyncInvoke / ListAsyncInvokes
    if _RE_ASYNC_INVOKE.match(path):
        if method == "POST":
            return await _start_async_invoke(body)
        elif method == "GET":
            return _list_async_invokes(query_params)

    return error_response_json("UnrecognizedClientException",
                               f"Unrecognized operation: {method} {path}", 400)


async def _converse(model_id: str, body: bytes):
    """
    Converse API — proxy to LiteLLM for inference.
    Transforms Bedrock Converse request format to OpenAI chat completion format,
    then transforms the response back.
    """
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    # Import bedrock module to resolve model mapping
    from ministack.services.bedrock import resolve_model
    local_model = resolve_model(model_id)

    # Transform Bedrock messages to OpenAI format
    bedrock_messages = data.get("messages", [])
    openai_messages = []
    for msg in bedrock_messages:
        role = msg.get("role", "user")
        content_blocks = msg.get("content", [])
        text_parts = []
        for block in content_blocks:
            if isinstance(block, dict) and "text" in block:
                text_parts.append(block["text"])
            elif isinstance(block, str):
                text_parts.append(block)
        openai_messages.append({"role": role, "content": " ".join(text_parts) if text_parts else ""})

    # Add system prompt if present
    system_prompts = data.get("system", [])
    if system_prompts:
        system_text = " ".join(
            s.get("text", s) if isinstance(s, dict) else str(s)
            for s in system_prompts
        )
        openai_messages.insert(0, {"role": "system", "content": system_text})

    # Inference config
    inference_config = data.get("inferenceConfig", {})
    temperature = inference_config.get("temperature", 0.7)
    max_tokens = inference_config.get("maxTokens", 1024)
    top_p = inference_config.get("topP", 1.0)

    # Call LLM backend (Ollama direct for qwen3, LiteLLM for others)
    try:
        result = await _call_llm(local_model, openai_messages, max_tokens, temperature, top_p)
    except Exception as e:
        logger.error("LLM call failed for model %s: %s", local_model, e)
        return error_response_json("ServiceUnavailableException",
                                   f"Inference backend is unavailable: {e}", 503)

    # Transform OpenAI response to Bedrock Converse format
    choice = result.get("choices", [{}])[0]
    message = choice.get("message", {})
    response_text = message.get("content", "") or ""

    finish_reason = choice.get("finish_reason", "end_turn")

    # Map OpenAI finish reasons to Bedrock stop reasons
    stop_reason_map = {
        "stop": "end_turn",
        "length": "max_tokens",
        "content_filter": "content_filtered",
    }
    stop_reason = stop_reason_map.get(finish_reason, "end_turn")

    usage = result.get("usage", {})
    bedrock_response = {
        "output": {
            "message": {
                "role": "assistant",
                "content": [{"text": response_text}],
            }
        },
        "stopReason": stop_reason,
        "usage": {
            "inputTokens": usage.get("prompt_tokens", 0),
            "outputTokens": usage.get("completion_tokens", 0),
            "totalTokens": usage.get("total_tokens", 0),
        },
        "metrics": {
            "latencyMs": int((time.time() % 1) * 1000),
        },
        "ResponseMetadata": {
            "RequestId": new_uuid(),
            "HTTPStatusCode": 200,
        },
    }

    return json_response(bedrock_response)


async def _invoke_model(model_id: str, body: bytes, headers: dict):
    """
    InvokeModel — legacy model invocation API.
    Supports Anthropic Messages format, Amazon Titan, and generic text completion.
    Proxies to LiteLLM and transforms the response to the provider-specific format.
    """
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    from ministack.services.bedrock import resolve_model
    local_model = resolve_model(model_id)

    # Build OpenAI-format messages from the provider-specific input
    openai_messages = []
    max_tokens = 1024
    temperature = 0.7

    if "messages" in data:
        # Anthropic Messages API format
        system = data.get("system", "")
        if system:
            openai_messages.append({"role": "system", "content": system})
        for msg in data["messages"]:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if isinstance(content, list):
                text_parts = [b.get("text", "") for b in content if isinstance(b, dict) and "text" in b]
                content = " ".join(text_parts)
            openai_messages.append({"role": role, "content": content})
        max_tokens = data.get("max_tokens", 1024)
        temperature = data.get("temperature", 0.7)
    elif "inputText" in data:
        # Amazon Titan format
        openai_messages.append({"role": "user", "content": data["inputText"]})
        tc = data.get("textGenerationConfig", {})
        max_tokens = tc.get("maxTokenCount", 1024)
        temperature = tc.get("temperature", 0.7)
    elif "prompt" in data:
        # Generic / Llama / Mistral format
        openai_messages.append({"role": "user", "content": data["prompt"]})
        max_tokens = data.get("max_gen_len", data.get("max_tokens", 1024))
        temperature = data.get("temperature", 0.7)
    else:
        openai_messages.append({"role": "user", "content": json.dumps(data)})

    try:
        result = await _call_llm(local_model, openai_messages, max_tokens, temperature)
    except Exception as e:
        return error_response_json("ServiceUnavailableException",
                                   f"Inference backend unavailable: {e}", 503)

    choice = result.get("choices", [{}])[0]
    response_text = choice.get("message", {}).get("content", "")
    finish_reason = choice.get("finish_reason", "stop")
    usage = result.get("usage", {})

    # Format response based on model provider
    if "anthropic" in model_id.lower():
        # Anthropic Messages response format
        response_body = {
            "id": f"msg_{new_uuid()[:24]}",
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": response_text}],
            "model": model_id,
            "stop_reason": "end_turn" if finish_reason == "stop" else finish_reason,
            "stop_sequence": None,
            "usage": {
                "input_tokens": usage.get("prompt_tokens", 0),
                "output_tokens": usage.get("completion_tokens", 0),
            },
        }
    elif "titan" in model_id.lower():
        # Amazon Titan response format
        response_body = {
            "inputTextTokenCount": usage.get("prompt_tokens", 0),
            "results": [{
                "tokenCount": usage.get("completion_tokens", 0),
                "outputText": response_text,
                "completionReason": "FINISH",
            }],
        }
    else:
        # Generic format (Llama, Mistral, etc.)
        response_body = {
            "generation": response_text,
            "prompt_token_count": usage.get("prompt_tokens", 0),
            "generation_token_count": usage.get("completion_tokens", 0),
            "stop_reason": finish_reason,
        }

    resp_body = json.dumps(response_body).encode("utf-8")
    return 200, {
        "Content-Type": "application/json",
        "x-amzn-bedrock-input-token-count": str(usage.get("prompt_tokens", 0)),
        "x-amzn-bedrock-output-token-count": str(usage.get("completion_tokens", 0)),
    }, resp_body


async def _apply_guardrail(guardrail_id: str, version: str, body: bytes):
    """
    ApplyGuardrail — checks content against configured guardrail patterns.
    Uses regex-based content filtering from bedrock_models.yaml config.
    """
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    source = data.get("source", "INPUT")
    content = data.get("content", [])

    # Load guardrail config — merge YAML defaults with dynamic guardrails
    from ministack.services.bedrock import get_models_config, get_guardrail_config
    config = get_models_config()
    guardrail_config = config.get("guardrails", {})
    blocked_patterns = list(guardrail_config.get("blocked_patterns", []))

    # Also check dynamically created guardrails for word policies
    dynamic = get_guardrail_config(guardrail_id)
    if dynamic:
        word_policy = dynamic.get("wordPolicy", {})
        for w in word_policy.get("wordsConfig", []):
            if isinstance(w, dict) and "text" in w:
                blocked_patterns.append(re.escape(w["text"]))

    # Check each content block against blocked patterns
    assessments = []
    action = "NONE"

    for block in content:
        text = ""
        if isinstance(block, dict):
            if "text" in block:
                text = block["text"].get("text", "") if isinstance(block["text"], dict) else block["text"]
        elif isinstance(block, str):
            text = block

        block_assessment = {
            "contentPolicy": {"filters": []},
            "wordPolicy": {"customWords": [], "managedWordLists": []},
            "sensitiveInformationPolicy": {"piiEntities": [], "regexes": []},
            "topicPolicy": {"topics": []},
        }

        for pattern in blocked_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                action = "GUARDRAIL_INTERVENED"
                block_assessment["wordPolicy"]["customWords"].append({
                    "match": pattern,
                    "action": "BLOCKED",
                })

        assessments.append(block_assessment)

    # Build output — if blocked, replace content
    outputs = []
    if action == "GUARDRAIL_INTERVENED":
        outputs.append({
            "text": "Sorry, the model cannot answer this question. The guardrail blocked the content.",
        })
    else:
        for block in content:
            if isinstance(block, dict) and "text" in block:
                text_val = block["text"].get("text", "") if isinstance(block["text"], dict) else block["text"]
                outputs.append({"text": text_val})
            elif isinstance(block, str):
                outputs.append({"text": block})

    response = {
        "action": action,
        "outputs": outputs,
        "assessments": assessments,
        "usage": {
            "topicPolicyUnits": 1,
            "contentPolicyUnits": 1,
            "wordPolicyUnits": 1,
            "sensitiveInformationPolicyUnits": 1,
            "sensitiveInformationPolicyFreeUnits": 0,
        },
    }

    return json_response(response)


# ---------------------------------------------------------------------------
# CountTokens
# ---------------------------------------------------------------------------

async def _count_tokens(model_id: str, body: bytes):
    """CountTokens — estimate token count for input without running inference."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    # Extract text from input — supports converse format or invokeModel format
    text = ""
    input_data = data.get("input", {})
    if "converse" in input_data:
        messages = input_data["converse"].get("messages", [])
        for msg in messages:
            for block in msg.get("content", []):
                if isinstance(block, dict) and "text" in block:
                    text += block["text"] + " "
        system = input_data["converse"].get("system", [])
        for s in system:
            if isinstance(s, dict) and "text" in s:
                text += s["text"] + " "
    elif "invokeModel" in input_data:
        text = json.dumps(input_data["invokeModel"])
    else:
        text = json.dumps(data)

    # Rough token estimate: ~4 chars per token (good approximation for most models)
    input_tokens = max(1, len(text) // 4)

    return json_response({"inputTokens": input_tokens})


# ---------------------------------------------------------------------------
# Async Invocations
# ---------------------------------------------------------------------------

async def _start_async_invoke(body: bytes):
    """StartAsyncInvoke — queue an async model invocation."""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("ValidationException", "Invalid JSON body", 400)

    model_id = data.get("modelId", "")
    if not model_id:
        return error_response_json("ValidationException", "modelId is required", 400)

    invocation_id = new_uuid()
    invocation_arn = f"arn:aws:bedrock:{REGION}:{ACCOUNT_ID}:async-invoke/{invocation_id}"
    now = now_iso()

    invocation = {
        "invocationArn": invocation_arn,
        "modelArn": f"arn:aws:bedrock:{REGION}::foundation-model/{model_id}",
        "status": "InProgress",
        "submitTime": now,
        "lastModifiedTime": now,
        "clientRequestToken": data.get("clientRequestToken", invocation_id),
        "outputDataConfig": data.get("outputDataConfig", {}),
        "modelInput": data.get("modelInput", {}),
    }

    with _async_lock:
        _async_invocations[invocation_arn] = invocation

    # Run inference in background thread
    thread = threading.Thread(
        target=_run_async_invoke_sync,
        args=(invocation_arn, model_id, data.get("modelInput", {})),
        daemon=True,
    )
    thread.start()

    return json_response({"invocationArn": invocation_arn}, 202)


def _run_async_invoke_sync(invocation_arn: str, model_id: str, model_input: dict):
    """Background thread for async invocation."""
    import asyncio
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_run_async_invoke(invocation_arn, model_id, model_input))
    except Exception as e:
        logger.error("Async invocation %s failed: %s", invocation_arn, e)
        with _async_lock:
            inv = _async_invocations.get(invocation_arn, {})
            inv["status"] = "Failed"
            inv["failureMessage"] = str(e)
            inv["lastModifiedTime"] = now_iso()
            inv["endTime"] = now_iso()
    finally:
        loop.close()


async def _run_async_invoke(invocation_arn: str, model_id: str, model_input: dict):
    """Async inference execution."""
    import aiohttp
    from ministack.services.bedrock import resolve_model

    local_model = resolve_model(model_id)
    messages = [{"role": "user", "content": json.dumps(model_input) if model_input else "Hello"}]

    # Try to extract messages from model input
    if "messages" in model_input:
        messages = []
        for msg in model_input["messages"]:
            role = msg.get("role", "user")
            content = msg.get("content", "")
            if isinstance(content, list):
                content = " ".join(b.get("text", "") for b in content if isinstance(b, dict))
            messages.append({"role": role, "content": content})

    result = await _call_llm(local_model, messages, max_tokens=1024)

    response_text = result.get("choices", [{}])[0].get("message", {}).get("content", "")

    with _async_lock:
        inv = _async_invocations.get(invocation_arn, {})
        inv["status"] = "Completed"
        inv["endTime"] = now_iso()
        inv["lastModifiedTime"] = now_iso()
        inv["_result"] = response_text


def _get_async_invoke(invocation_arn: str):
    """GetAsyncInvoke — return status of an async invocation."""
    with _async_lock:
        inv = _async_invocations.get(invocation_arn)
    if not inv:
        # Try matching by ID suffix
        for arn, data in _async_invocations.items():
            if arn.endswith(invocation_arn) or invocation_arn in arn:
                inv = data
                break
    if not inv:
        return error_response_json("ResourceNotFoundException",
                                   f"Async invocation {invocation_arn} not found", 404)
    result = {k: v for k, v in inv.items() if not k.startswith("_")}
    return json_response(result)


def _list_async_invokes(query_params):
    """ListAsyncInvokes — list all async invocations."""
    max_results = int(query_params.get("maxResults", [10])[0]) if isinstance(
        query_params.get("maxResults"), list) else int(query_params.get("maxResults", 10))
    status_filter = query_params.get("statusEquals", [None])[0] if isinstance(
        query_params.get("statusEquals"), list) else query_params.get("statusEquals")

    with _async_lock:
        items = list(_async_invocations.values())

    if status_filter:
        items = [i for i in items if i.get("status") == status_filter]

    summaries = [{k: v for k, v in i.items() if not k.startswith("_")}
                 for i in items[:max_results]]

    result = {"asyncInvokeSummaries": summaries}
    if len(items) > max_results:
        result["nextToken"] = str(max_results)
    return json_response(result)


def reset():
    """Clear all in-memory state."""
    _guardrails.clear()
    with _async_lock:
        _async_invocations.clear()


def get_state():
    """Return serializable state for persistence."""
    return {"async_invocations": {k: {kk: vv for kk, vv in v.items() if not kk.startswith("_")}
                                  for k, v in _async_invocations.items()}}
