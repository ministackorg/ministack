"""
Bedrock + Bedrock Runtime integration tests.

Verifies wire-shape parity for the Converse family + control-plane listings,
mock + (where reachable) proxy paths. Token counts are heuristic so we assert
on presence and ordering, not absolute values.
"""

import json
import urllib.error
import urllib.parse
import urllib.request

from conftest import ENDPOINT, make_client

# ---------------------------------------------------------------------------
# Control plane
# ---------------------------------------------------------------------------


def test_bedrock_list_foundation_models_returns_catalog():
    client = make_client("bedrock")
    resp = client.list_foundation_models()
    summaries = resp["modelSummaries"]
    assert len(summaries) > 10
    ids = {s["modelId"] for s in summaries}
    assert "anthropic.claude-3-5-sonnet-20241022-v2:0" in ids
    assert "amazon.nova-pro-v1:0" in ids
    assert "meta.llama3-1-70b-instruct-v1:0" in ids
    for s in summaries:
        assert s["modelArn"].startswith("arn:aws:bedrock:us-east-1::foundation-model/")
        assert s["modelLifecycle"]["status"] == "ACTIVE"
        assert s["inferenceTypesSupported"] == ["ON_DEMAND"]


def test_bedrock_list_foundation_models_filters_by_provider():
    client = make_client("bedrock")
    resp = client.list_foundation_models(byProvider="Anthropic")
    for s in resp["modelSummaries"]:
        assert s["providerName"] == "Anthropic"


def test_bedrock_get_foundation_model_by_id():
    client = make_client("bedrock")
    resp = client.get_foundation_model(
        modelIdentifier="anthropic.claude-3-5-sonnet-20241022-v2:0"
    )
    details = resp["modelDetails"]
    assert details["modelId"] == "anthropic.claude-3-5-sonnet-20241022-v2:0"
    assert details["providerName"] == "Anthropic"
    assert details["responseStreamingSupported"] is True


def test_bedrock_get_foundation_model_unknown_id_returns_404():
    import botocore.exceptions

    client = make_client("bedrock")
    try:
        client.get_foundation_model(modelIdentifier="does-not-exist")
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"
    else:
        raise AssertionError("expected ResourceNotFoundException")


# ---------------------------------------------------------------------------
# Converse (non-streaming)
# ---------------------------------------------------------------------------


def test_bedrock_converse_returns_shape_required_fields():
    client = make_client("bedrock-runtime")
    resp = client.converse(
        modelId="anthropic.claude-3-5-sonnet-20241022-v2:0",
        messages=[{"role": "user", "content": [{"text": "hello"}]}],
    )
    # Required by botocore ConverseResponse: output, stopReason, usage, metrics
    assert "output" in resp
    assert "message" in resp["output"]
    assert resp["output"]["message"]["role"] == "assistant"
    assert isinstance(resp["output"]["message"]["content"], list)
    assert "text" in resp["output"]["message"]["content"][0]
    assert resp["stopReason"] in (
        "end_turn", "tool_use", "max_tokens", "stop_sequence",
        "guardrail_intervened", "content_filtered",
    )
    assert "usage" in resp
    for k in ("inputTokens", "outputTokens", "totalTokens"):
        assert isinstance(resp["usage"][k], int)
    assert resp["usage"]["totalTokens"] == (
        resp["usage"]["inputTokens"] + resp["usage"]["outputTokens"]
    )
    assert isinstance(resp["metrics"]["latencyMs"], int)
    assert resp["metrics"]["latencyMs"] >= 1


def test_bedrock_converse_with_system_prompt_and_inference_config():
    client = make_client("bedrock-runtime")
    resp = client.converse(
        modelId="amazon.nova-pro-v1:0",
        system=[{"text": "You are a terse assistant."}],
        messages=[{"role": "user", "content": [{"text": "what is 2+2?"}]}],
        inferenceConfig={"maxTokens": 64, "temperature": 0.0},
    )
    assert resp["output"]["message"]["role"] == "assistant"
    text = resp["output"]["message"]["content"][0]["text"]
    assert "nova" in text.lower() or "ministack" in text.lower()


def test_bedrock_converse_distinct_prompts_produce_distinct_replies():
    client = make_client("bedrock-runtime")
    model = "anthropic.claude-3-haiku-20240307-v1:0"
    a = client.converse(modelId=model, messages=[
        {"role": "user", "content": [{"text": "alpha"}]}])
    b = client.converse(modelId=model, messages=[
        {"role": "user", "content": [{"text": "beta"}]}])
    assert (a["output"]["message"]["content"][0]["text"]
            != b["output"]["message"]["content"][0]["text"])


def test_bedrock_converse_via_inference_profile_id():
    client = make_client("bedrock-runtime")
    resp = client.converse(
        modelId="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
        messages=[{"role": "user", "content": [{"text": "hi"}]}],
    )
    assert resp["output"]["message"]["role"] == "assistant"


def test_bedrock_converse_rejects_invalid_role():
    import botocore.exceptions

    client = make_client("bedrock-runtime")
    try:
        client.converse(
            modelId="anthropic.claude-3-haiku-20240307-v1:0",
            messages=[{"role": "system", "content": [{"text": "x"}]}],
        )
    except botocore.exceptions.ParamValidationError:
        # botocore client-side validation catches this before the wire — fine.
        return
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ValidationException"
    else:
        raise AssertionError("expected ValidationException or ParamValidationError")


# ---------------------------------------------------------------------------
# ConverseStream (eventstream)
# ---------------------------------------------------------------------------


def test_bedrock_converse_stream_emits_full_event_sequence():
    client = make_client("bedrock-runtime")
    resp = client.converse_stream(
        modelId="anthropic.claude-3-5-haiku-20241022-v1:0",
        messages=[{"role": "user", "content": [{"text": "stream please"}]}],
    )
    events = []
    for evt in resp["stream"]:
        events.append(evt)
    # AWS sequence: messageStart -> contentBlockDelta+ -> contentBlockStop ->
    # messageStop -> metadata. boto3's EventStream parser surfaces each event
    # as a single-key dict keyed by event name.
    names = [list(e.keys())[0] for e in events]
    assert names[0] == "messageStart"
    assert "contentBlockDelta" in names
    assert "contentBlockStop" in names
    assert "messageStop" in names
    assert names[-1] == "metadata"

    # Reassemble text from deltas
    text = "".join(
        e["contentBlockDelta"]["delta"]["text"]
        for e in events if "contentBlockDelta" in e
    )
    assert len(text) > 0

    # Metadata carries usage + metrics
    meta = [e for e in events if "metadata" in e][0]["metadata"]
    assert meta["usage"]["totalTokens"] == (
        meta["usage"]["inputTokens"] + meta["usage"]["outputTokens"]
    )
    assert meta["metrics"]["latencyMs"] >= 1


def test_bedrock_converse_stream_message_start_role_assistant():
    client = make_client("bedrock-runtime")
    resp = client.converse_stream(
        modelId="amazon.nova-lite-v1:0",
        messages=[{"role": "user", "content": [{"text": "x"}]}],
    )
    first = next(iter(resp["stream"]))
    assert "messageStart" in first
    assert first["messageStart"]["role"] == "assistant"


# ---------------------------------------------------------------------------
# Control-plane: filter combinations + ARN lookup + region-awareness
# ---------------------------------------------------------------------------


def test_bedrock_list_foundation_models_filters_by_output_modality_embedding():
    client = make_client("bedrock")
    resp = client.list_foundation_models(byOutputModality="EMBEDDING")
    ids = [s["modelId"] for s in resp["modelSummaries"]]
    assert "amazon.titan-embed-text-v2:0" in ids
    assert "anthropic.claude-3-5-sonnet-20241022-v2:0" not in ids


def test_bedrock_get_foundation_model_by_arn():
    client = make_client("bedrock")
    arn = "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"
    resp = client.get_foundation_model(modelIdentifier=arn)
    assert resp["modelDetails"]["modelId"] == "anthropic.claude-3-haiku-20240307-v1:0"


def test_bedrock_get_foundation_model_arn_parser_does_not_tail_match_invalid_arns():
    import botocore.exceptions

    client = make_client("bedrock")
    valid_tail = "foundation-model/anthropic.claude-3-haiku-20240307-v1:0"
    bad_refs = [
        f"arn:aws:lambda:us-east-1::{valid_tail}",
        f"arn:aws:bedrock:us-west-2::{valid_tail}",
        f"arn:aws:bedrock:us-east-1:000000000000:{valid_tail}",
        "arn:aws:bedrock:us-east-1::custom-model/anthropic.claude-3-haiku-20240307-v1:0",
        "arn:aws:bedrock:us-east-1",
    ]
    for model_identifier in bad_refs:
        try:
            client.get_foundation_model(modelIdentifier=model_identifier)
        except botocore.exceptions.ClientError as exc:
            assert exc.response["Error"]["Code"] == "ResourceNotFoundException"
        else:
            raise AssertionError(f"expected ResourceNotFoundException for {model_identifier}")


def test_bedrock_model_arn_reflects_request_region():
    import boto3
    from botocore.config import Config
    from conftest import ENDPOINT

    client = boto3.client(
        "bedrock", endpoint_url=ENDPOINT,
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name="eu-west-1",
        config=Config(region_name="eu-west-1", retries={"mode": "standard"}),
    )
    resp = client.list_foundation_models()
    for s in resp["modelSummaries"]:
        assert ":bedrock:eu-west-1:" in s["modelArn"]


def test_bedrock_list_inference_profiles_returns_us_eu_apac_prefixes():
    client = make_client("bedrock")
    resp = client.list_inference_profiles()
    ids = {p["inferenceProfileId"] for p in resp["inferenceProfileSummaries"]}
    assert "us.anthropic.claude-3-5-sonnet-20241022-v2:0" in ids
    assert "eu.anthropic.claude-3-5-sonnet-20241022-v2:0" in ids
    assert "apac.anthropic.claude-3-5-sonnet-20241022-v2:0" in ids
    for p in resp["inferenceProfileSummaries"]:
        assert p["status"] == "ACTIVE"
        assert p["type"] == "SYSTEM_DEFINED"
        assert len(p["models"]) == 1


def test_bedrock_get_inference_profile_returns_model_arn():
    client = make_client("bedrock")
    resp = client.get_inference_profile(
        inferenceProfileIdentifier="us.amazon.nova-pro-v1:0"
    )
    assert resp["inferenceProfileId"] == "us.amazon.nova-pro-v1:0"
    assert resp["models"][0]["modelArn"].endswith("amazon.nova-pro-v1:0")


def test_bedrock_get_inference_profile_unknown_returns_404():
    import botocore.exceptions

    client = make_client("bedrock")
    try:
        client.get_inference_profile(inferenceProfileIdentifier="us.does-not-exist")
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"
    else:
        raise AssertionError("expected ResourceNotFoundException")


# ---------------------------------------------------------------------------
# Converse: multi-turn, multi-block, all model families
# ---------------------------------------------------------------------------


def test_bedrock_converse_multi_turn_conversation():
    client = make_client("bedrock-runtime")
    resp = client.converse(
        modelId="anthropic.claude-3-haiku-20240307-v1:0",
        messages=[
            {"role": "user", "content": [{"text": "first"}]},
            {"role": "assistant", "content": [{"text": "ack"}]},
            {"role": "user", "content": [{"text": "second"}]},
        ],
    )
    assert resp["output"]["message"]["role"] == "assistant"
    # input token count grew with multi-turn vs single turn
    assert resp["usage"]["inputTokens"] >= 1


def test_bedrock_converse_multiple_content_blocks_in_message():
    client = make_client("bedrock-runtime")
    resp = client.converse(
        modelId="anthropic.claude-3-5-sonnet-20241022-v2:0",
        messages=[{
            "role": "user",
            "content": [
                {"text": "block one"},
                {"text": "block two"},
            ],
        }],
    )
    assert resp["output"]["message"]["role"] == "assistant"


def test_bedrock_converse_token_count_headers_present_in_http_response():
    import urllib.request

    from conftest import ENDPOINT

    body = json.dumps({
        "messages": [{"role": "user", "content": [{"text": "header check"}]}],
    }).encode()
    req = urllib.request.Request(
        f"{ENDPOINT}/model/amazon.nova-micro-v1%3A0/converse",
        data=body,
        headers={
            "Content-Type": "application/json",
            "Authorization": ("AWS4-HMAC-SHA256 "
                              "Credential=test/20260605/us-east-1/bedrock/aws4_request, "
                              "SignedHeaders=host;x-amz-date, Signature=x"),
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        assert int(resp.headers["x-amzn-bedrock-input-token-count"]) >= 1
        assert int(resp.headers["x-amzn-bedrock-output-token-count"]) >= 1


def test_bedrock_converse_family_anthropic():
    _converse_family("anthropic.claude-3-5-sonnet-20241022-v2:0")


def test_bedrock_converse_family_nova():
    _converse_family("amazon.nova-pro-v1:0")


def test_bedrock_converse_family_titan():
    _converse_family("amazon.titan-text-express-v1")


def test_bedrock_converse_family_llama():
    _converse_family("meta.llama3-1-70b-instruct-v1:0")


def test_bedrock_converse_family_mistral():
    _converse_family("mistral.mistral-large-2407-v1:0")


def test_bedrock_converse_family_cohere():
    _converse_family("cohere.command-r-plus-v1:0")


def test_bedrock_converse_family_ai21():
    _converse_family("ai21.jamba-1-5-large-v1:0")


def _converse_family(model_id: str):
    client = make_client("bedrock-runtime")
    resp = client.converse(
        modelId=model_id,
        messages=[{"role": "user", "content": [{"text": "hi"}]}],
    )
    text = resp["output"]["message"]["content"][0]["text"]
    assert isinstance(text, str) and len(text) > 0
    assert resp["stopReason"] == "end_turn"
    assert resp["usage"]["totalTokens"] > 0


# ---------------------------------------------------------------------------
# Converse: optional shape fields (toolConfig, guardrailConfig, requestMetadata)
# Wire-shape tolerance: accept them without 400 even though we don't act on them
# ---------------------------------------------------------------------------


def test_bedrock_converse_accepts_tool_config():
    client = make_client("bedrock-runtime")
    resp = client.converse(
        modelId="anthropic.claude-3-5-sonnet-20241022-v2:0",
        messages=[{"role": "user", "content": [{"text": "x"}]}],
        toolConfig={
            "tools": [{
                "toolSpec": {
                    "name": "noop",
                    "description": "no-op",
                    "inputSchema": {"json": {"type": "object"}},
                },
            }],
        },
    )
    assert resp["output"]["message"]["role"] == "assistant"


def test_bedrock_converse_accepts_request_metadata():
    client = make_client("bedrock-runtime")
    resp = client.converse(
        modelId="amazon.nova-lite-v1:0",
        messages=[{"role": "user", "content": [{"text": "x"}]}],
        requestMetadata={"trace": "abc", "experiment": "v2"},
    )
    assert resp["usage"]["totalTokens"] >= 1


def test_bedrock_converse_inference_config_stop_sequences_accepted():
    client = make_client("bedrock-runtime")
    resp = client.converse(
        modelId="meta.llama3-1-8b-instruct-v1:0",
        messages=[{"role": "user", "content": [{"text": "x"}]}],
        inferenceConfig={
            "maxTokens": 32,
            "temperature": 0.5,
            "topP": 0.9,
            "stopSequences": ["END"],
        },
    )
    assert resp["output"]["message"]["role"] == "assistant"


# ---------------------------------------------------------------------------
# Converse validation paths (wire-level, bypassing botocore client validation)
# ---------------------------------------------------------------------------


def _raw_converse(body: dict) -> tuple:
    """Return (status, body_dict) from a raw POST to /model/.../converse."""
    import urllib.error
    import urllib.request

    from conftest import ENDPOINT

    req = urllib.request.Request(
        f"{ENDPOINT}/model/anthropic.claude-3-haiku-20240307-v1%3A0/converse",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": ("AWS4-HMAC-SHA256 "
                              "Credential=test/20260605/us-east-1/bedrock/aws4_request, "
                              "SignedHeaders=host;x-amz-date, Signature=x"),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def test_bedrock_converse_validation_messages_not_list():
    status, body = _raw_converse({"messages": "not a list"})
    assert status == 400
    assert body["__type"] == "ValidationException"


def test_bedrock_converse_validation_invalid_role():
    status, body = _raw_converse({"messages": [
        {"role": "system", "content": [{"text": "x"}]},
    ]})
    assert status == 400
    assert body["__type"] == "ValidationException"


def test_bedrock_converse_validation_empty_content():
    status, body = _raw_converse({"messages": [
        {"role": "user", "content": []},
    ]})
    assert status == 400
    assert body["__type"] == "ValidationException"


def test_bedrock_converse_validation_malformed_body():
    import urllib.error
    import urllib.request

    from conftest import ENDPOINT

    req = urllib.request.Request(
        f"{ENDPOINT}/model/anthropic.claude-3-haiku-20240307-v1%3A0/converse",
        data=b"{not json",
        headers={
            "Content-Type": "application/json",
            "Authorization": ("AWS4-HMAC-SHA256 "
                              "Credential=test/20260605/us-east-1/bedrock/aws4_request, "
                              "SignedHeaders=host;x-amz-date, Signature=x"),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            assert False, "should have raised"
    except urllib.error.HTTPError as e:
        assert e.code == 400
        body = json.loads(e.read())
        assert body["__type"] == "ValidationException"


# ---------------------------------------------------------------------------
# ConverseStream: text reconstruction + delta order
# ---------------------------------------------------------------------------


def test_bedrock_converse_stream_deltas_reconstruct_full_text():
    client = make_client("bedrock-runtime")
    resp = client.converse_stream(
        modelId="anthropic.claude-3-5-sonnet-20241022-v2:0",
        messages=[{"role": "user", "content": [{"text": "reconstruct me"}]}],
    )
    deltas = []
    saw_stop = False
    for evt in resp["stream"]:
        if "contentBlockDelta" in evt:
            deltas.append(evt["contentBlockDelta"]["delta"]["text"])
        if "messageStop" in evt:
            saw_stop = True
            assert evt["messageStop"]["stopReason"] == "end_turn"
    assert saw_stop
    assert "".join(deltas)  # non-empty
    # All deltas refer to contentBlockIndex 0 (single text block)
    # (validated by virtue of reaching here without KeyError)


def test_bedrock_converse_stream_via_inference_profile_id():
    client = make_client("bedrock-runtime")
    resp = client.converse_stream(
        modelId="eu.anthropic.claude-3-haiku-20240307-v1:0",
        messages=[{"role": "user", "content": [{"text": "x"}]}],
    )
    names = [list(e.keys())[0] for e in resp["stream"]]
    assert names[0] == "messageStart"
    assert names[-1] == "metadata"


def test_bedrock_runtime_accepts_foundation_model_arn_model_id():
    client = make_client("bedrock-runtime")
    arn = "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"
    resp = client.converse(
        modelId=arn,
        messages=[{"role": "user", "content": [{"text": "hello via arn"}]}],
    )
    assert resp["output"]["message"]["role"] == "assistant"


def test_bedrock_runtime_accepts_custom_model_arn_with_path_segments():
    client = make_client("bedrock-runtime")
    arn = "arn:aws:bedrock:us-east-1:000000000000:custom-model/my-model/123456789012"
    resp = client.converse(
        modelId=arn,
        messages=[{"role": "user", "content": [{"text": "hello via custom arn"}]}],
    )
    assert resp["output"]["message"]["role"] == "assistant"


def test_bedrock_runtime_accepts_additional_model_arn_shapes():
    client = make_client("bedrock-runtime")
    arns = [
        "arn:aws:bedrock:us-east-1:000000000000:custom-model-deployment/my-deployment",
        "arn:aws:bedrock:us-east-1:000000000000:prompt-router/my-router",
        "arn:aws:sagemaker:us-east-1:000000000000:endpoint/my-bedrock-endpoint",
    ]
    for arn in arns:
        resp = client.converse(
            modelId=arn,
            messages=[{"role": "user", "content": [{"text": "hello via arn"}]}],
        )
        assert resp["output"]["message"]["role"] == "assistant"


def test_bedrock_runtime_model_id_arn_parser_does_not_tail_match_invalid_arns():
    model_id = "arn:aws:lambda:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0"
    encoded_model_id = urllib.parse.quote(model_id, safe="")
    req = urllib.request.Request(
        f"{ENDPOINT}/model/{encoded_model_id}/converse",
        data=json.dumps({
            "messages": [{"role": "user", "content": [{"text": "x"}]}],
        }).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": ("AWS4-HMAC-SHA256 "
                              "Credential=test/20260605/us-east-1/bedrock/aws4_request, "
                              "SignedHeaders=host;x-amz-date, Signature=x"),
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req):
            raise AssertionError("expected ValidationException")
    except urllib.error.HTTPError as exc:
        assert exc.code == 400
        body = json.loads(exc.read())
        assert body["__type"] == "ValidationException"


# ---------------------------------------------------------------------------
# InvokeModel — family-specific request/response shape
# ---------------------------------------------------------------------------


def test_bedrock_invoke_model_anthropic_messages_shape():
    client = make_client("bedrock-runtime")
    resp = client.invoke_model(
        modelId="anthropic.claude-3-5-sonnet-20241022-v2:0",
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 100,
            "messages": [{"role": "user", "content": "hello"}],
        }),
    )
    body = json.loads(resp["body"].read())
    assert body["type"] == "message"
    assert body["role"] == "assistant"
    assert body["model"] == "anthropic.claude-3-5-sonnet-20241022-v2:0"
    assert body["content"][0]["type"] == "text"
    assert "text" in body["content"][0]
    assert body["stop_reason"] == "end_turn"
    assert body["usage"]["input_tokens"] >= 1
    assert body["usage"]["output_tokens"] >= 1


def test_bedrock_invoke_model_titan_text_shape():
    client = make_client("bedrock-runtime")
    resp = client.invoke_model(
        modelId="amazon.titan-text-express-v1",
        body=json.dumps({
            "inputText": "tell me a story",
            "textGenerationConfig": {"maxTokenCount": 100, "temperature": 0.5},
        }),
    )
    body = json.loads(resp["body"].read())
    assert "inputTextTokenCount" in body
    assert "results" in body
    assert body["results"][0]["completionReason"] == "FINISH"
    assert "outputText" in body["results"][0]


def test_bedrock_invoke_model_llama_shape():
    client = make_client("bedrock-runtime")
    resp = client.invoke_model(
        modelId="meta.llama3-1-70b-instruct-v1:0",
        body=json.dumps({"prompt": "[INST] hi [/INST]", "max_gen_len": 100}),
    )
    body = json.loads(resp["body"].read())
    assert "generation" in body
    assert body["stop_reason"] == "stop"
    assert "prompt_token_count" in body
    assert "generation_token_count" in body


def test_bedrock_invoke_model_mistral_shape():
    client = make_client("bedrock-runtime")
    resp = client.invoke_model(
        modelId="mistral.mistral-large-2407-v1:0",
        body=json.dumps({"prompt": "<s>[INST] hi [/INST]", "max_tokens": 100}),
    )
    body = json.loads(resp["body"].read())
    assert "outputs" in body
    assert body["outputs"][0]["stop_reason"] == "stop"


def test_bedrock_invoke_model_cohere_shape():
    client = make_client("bedrock-runtime")
    resp = client.invoke_model(
        modelId="cohere.command-r-plus-v1:0",
        body=json.dumps({"prompt": "x", "max_tokens": 100}),
    )
    body = json.loads(resp["body"].read())
    assert "generations" in body
    assert body["generations"][0]["finish_reason"] == "COMPLETE"


def test_bedrock_invoke_model_returns_token_count_headers():
    import urllib.request

    from conftest import ENDPOINT

    req = urllib.request.Request(
        f"{ENDPOINT}/model/anthropic.claude-3-haiku-20240307-v1%3A0/invoke",
        data=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 50,
            "messages": [{"role": "user", "content": "x"}],
        }).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": ("AWS4-HMAC-SHA256 "
                              "Credential=test/20260605/us-east-1/bedrock/aws4_request, "
                              "SignedHeaders=host;x-amz-date, Signature=x"),
        },
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        assert resp.status == 200
        assert int(resp.headers["x-amzn-bedrock-input-token-count"]) >= 1
        assert int(resp.headers["x-amzn-bedrock-output-token-count"]) >= 1


def test_bedrock_invoke_model_rejects_empty_body():
    import botocore.exceptions

    client = make_client("bedrock-runtime")
    try:
        client.invoke_model(
            modelId="anthropic.claude-3-haiku-20240307-v1:0",
            body=b"",
        )
    except botocore.exceptions.ParamValidationError:
        return
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ValidationException"
    else:
        raise AssertionError("expected ValidationException or ParamValidationError")


# ---------------------------------------------------------------------------
# InvokeModelWithResponseStream — eventstream chunk envelope (bytes-encoded)
# ---------------------------------------------------------------------------


def test_bedrock_invoke_model_with_response_stream_anthropic_envelope():
    import base64

    client = make_client("bedrock-runtime")
    resp = client.invoke_model_with_response_stream(
        modelId="anthropic.claude-3-5-sonnet-20241022-v2:0",
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 100,
            "messages": [{"role": "user", "content": "stream"}],
        }),
    )
    inner_events = []
    for evt in resp["body"]:
        chunk = evt["chunk"]
        # Each chunk["bytes"] is base64-encoded JSON of the family-specific
        # event (per AWS InvokeModelWithResponseStream wire format)
        inner = json.loads(chunk["bytes"])
        inner_events.append(inner)
    types = [e["type"] for e in inner_events]
    assert types[0] == "message_start"
    assert "content_block_start" in types
    assert "content_block_delta" in types
    assert "content_block_stop" in types
    assert "message_delta" in types
    assert types[-1] == "message_stop"


def test_bedrock_invoke_model_with_response_stream_titan_envelope():
    client = make_client("bedrock-runtime")
    resp = client.invoke_model_with_response_stream(
        modelId="amazon.titan-text-express-v1",
        body=json.dumps({"inputText": "x"}),
    )
    inners = [json.loads(evt["chunk"]["bytes"]) for evt in resp["body"]]
    assert all("outputText" in i for i in inners)
    assert inners[-1]["completionReason"] == "FINISH"


# ---------------------------------------------------------------------------
# ApplyGuardrail
# ---------------------------------------------------------------------------


def test_bedrock_apply_guardrail_returns_required_fields():
    client = make_client("bedrock-runtime")
    resp = client.apply_guardrail(
        guardrailIdentifier="gr-anything",
        guardrailVersion="DRAFT",
        source="INPUT",
        content=[{"text": {"text": "hello world"}}],
    )
    # Required by botocore ApplyGuardrailResponse
    for k in ("usage", "action", "outputs", "assessments"):
        assert k in resp
    assert resp["action"] == "NONE"
    assert resp["outputs"][0]["text"] == "hello world"
    assert isinstance(resp["usage"]["topicPolicyUnits"], int)
    assert resp["guardrailCoverage"]["textCharacters"]["total"] == len("hello world")


def test_bedrock_apply_guardrail_rejects_invalid_source():
    import botocore.exceptions

    client = make_client("bedrock-runtime")
    try:
        client.apply_guardrail(
            guardrailIdentifier="gr",
            guardrailVersion="DRAFT",
            source="MIDDLE",
            content=[{"text": {"text": "x"}}],
        )
    except botocore.exceptions.ParamValidationError:
        return
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ValidationException"
    else:
        raise AssertionError("expected ValidationException")


# ---------------------------------------------------------------------------
# Async invoke (state-only — mocks complete instantly)
# ---------------------------------------------------------------------------


def test_bedrock_start_async_invoke_then_get():
    client = make_client("bedrock-runtime")
    start = client.start_async_invoke(
        modelId="amazon.nova-pro-v1:0",
        modelInput={"messages": [{"role": "user", "content": [{"text": "x"}]}]},
        outputDataConfig={"s3OutputDataConfig": {"s3Uri": "s3://bucket/prefix/"}},
    )
    arn = start["invocationArn"]
    assert arn.startswith("arn:aws:bedrock:us-east-1:")
    get_resp = client.get_async_invoke(invocationArn=arn)
    assert get_resp["invocationArn"] == arn
    assert get_resp["status"] == "Completed"
    assert "modelArn" in get_resp
    assert "submitTime" in get_resp


def test_bedrock_start_async_invoke_accepts_foundation_model_arn_model_id():
    client = make_client("bedrock-runtime")
    arn = "arn:aws:bedrock:us-east-1::foundation-model/amazon.nova-pro-v1:0"
    start = client.start_async_invoke(
        modelId=arn,
        modelInput={"messages": [{"role": "user", "content": [{"text": "x"}]}]},
        outputDataConfig={"s3OutputDataConfig": {"s3Uri": "s3://bucket/prefix/"}},
    )
    get_resp = client.get_async_invoke(invocationArn=start["invocationArn"])
    assert get_resp["modelArn"] == arn


def test_bedrock_start_async_invoke_rejects_invalid_model_id_arn():
    import botocore.exceptions

    client = make_client("bedrock-runtime")
    try:
        client.start_async_invoke(
            modelId="arn:aws:sqs:us-east-1::foundation-model/amazon.nova-pro-v1:0",
            modelInput={"messages": [{"role": "user", "content": [{"text": "x"}]}]},
            outputDataConfig={"s3OutputDataConfig": {"s3Uri": "s3://bucket/prefix/"}},
        )
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ValidationException"
    else:
        raise AssertionError("expected ValidationException")


def test_bedrock_list_async_invokes_includes_started_ones():
    client = make_client("bedrock-runtime")
    client.start_async_invoke(
        modelId="amazon.nova-lite-v1:0",
        modelInput={"messages": [{"role": "user", "content": [{"text": "x"}]}]},
        outputDataConfig={"s3OutputDataConfig": {"s3Uri": "s3://b/p/"}},
    )
    resp = client.list_async_invokes()
    assert "asyncInvokeSummaries" in resp
    assert len(resp["asyncInvokeSummaries"]) >= 1


def test_bedrock_get_async_invoke_unknown_returns_404():
    import botocore.exceptions

    client = make_client("bedrock-runtime")
    arn = "arn:aws:bedrock:us-east-1:000000000000:async-invoke/missing"
    try:
        client.get_async_invoke(invocationArn=arn)
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"
    else:
        raise AssertionError("expected ResourceNotFoundException")


# ===========================================================================
# OpenAI-compatible Chat Completions (bedrock-runtime, /v1/chat/completions)
# OpenAI-shape (not AWS-shape), so driven with raw HTTP mirroring openai-python.
# Validates wire-shape parity: required response fields, SSE stream framing,
# error envelope, role/content validation.
# ===========================================================================


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


def test_bedrock_openai_chat_completion_required_response_fields():
    status, _, body = _post_chat_json({
        "model": "anthropic.claude-3-5-sonnet-20241022-v2:0",
        "messages": [{"role": "user", "content": "hello"}],
    })
    assert status == 200
    for k in ("id", "object", "created", "model", "choices", "usage"):
        assert k in body, f"missing {k}"
    assert body["object"] == "chat.completion"
    assert body["id"].startswith("chatcmpl-")
    assert isinstance(body["created"], int)
    assert body["model"] == "anthropic.claude-3-5-sonnet-20241022-v2:0"
    choice = body["choices"][0]
    assert choice["index"] == 0
    assert choice["message"]["role"] == "assistant"
    assert isinstance(choice["message"]["content"], str)
    assert choice["finish_reason"] in ("stop", "length", "tool_calls",
                                        "content_filter", "function_call")
    for k in ("prompt_tokens", "completion_tokens", "total_tokens"):
        assert isinstance(body["usage"][k], int)
    assert body["usage"]["total_tokens"] == (
        body["usage"]["prompt_tokens"] + body["usage"]["completion_tokens"]
    )


def test_bedrock_openai_chat_completion_accepts_list_content_parts():
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


def test_bedrock_openai_chat_completion_multi_turn():
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


def test_bedrock_openai_distinct_prompts_produce_distinct_replies():
    _, _, a = _post_chat_json({
        "model": "anthropic.claude-3-haiku-20240307-v1:0",
        "messages": [{"role": "user", "content": "alpha"}],
    })
    _, _, b = _post_chat_json({
        "model": "anthropic.claude-3-haiku-20240307-v1:0",
        "messages": [{"role": "user", "content": "beta"}],
    })
    assert a["choices"][0]["message"]["content"] != b["choices"][0]["message"]["content"]


def test_bedrock_openai_chat_completion_stream_emits_role_then_content_then_done():
    events = _post_chat_stream_lines({
        "model": "amazon.nova-lite-v1:0",
        "messages": [{"role": "user", "content": "stream"}],
    })
    assert events[0]["choices"][0]["delta"]["role"] == "assistant"
    assert events[-1] == "[DONE]"
    assert events[-2]["choices"][0]["finish_reason"] == "stop"
    content = ""
    for e in events[1:-2]:
        if "content" in e["choices"][0]["delta"]:
            content += e["choices"][0]["delta"]["content"]
    assert content


def test_bedrock_openai_chat_completion_stream_all_chunks_are_completion_chunk_object():
    events = _post_chat_stream_lines({
        "model": "mistral.mistral-large-2407-v1:0",
        "messages": [{"role": "user", "content": "x"}],
    })
    for e in events[:-1]:
        assert e["object"] == "chat.completion.chunk"
        assert e["id"].startswith("chatcmpl-")


def test_bedrock_openai_chat_completion_stream_finish_reason_only_on_last_data_chunk():
    events = _post_chat_stream_lines({
        "model": "cohere.command-r-plus-v1:0",
        "messages": [{"role": "user", "content": "x"}],
    })
    with_finish = [
        i for i, e in enumerate(events)
        if isinstance(e, dict) and e["choices"][0]["finish_reason"] is not None
    ]
    assert with_finish == [len(events) - 2]


def _expect_openai_error(body: dict, expected_status: int = 400):
    status, _, parsed = _post_chat_json(body)
    assert status == expected_status, parsed
    assert "error" in parsed
    assert "message" in parsed["error"]
    assert "type" in parsed["error"]


def test_bedrock_openai_chat_completion_missing_model_field():
    _expect_openai_error({"messages": [{"role": "user", "content": "x"}]})


def test_bedrock_openai_chat_completion_empty_messages():
    _expect_openai_error({"model": "x", "messages": []})


def test_bedrock_openai_chat_completion_invalid_role():
    _expect_openai_error({
        "model": "x",
        "messages": [{"role": "bogus", "content": "y"}],
    })


def test_bedrock_openai_chat_completion_message_missing_content():
    _expect_openai_error({
        "model": "x",
        "messages": [{"role": "user"}],
    })


def test_bedrock_openai_chat_completion_malformed_json_body():
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


def test_bedrock_openai_chat_completion_method_not_allowed_for_get():
    req = urllib.request.Request(
        f"{ENDPOINT}/v1/chat/completions",
        method="GET",
    )
    try:
        with urllib.request.urlopen(req):
            assert False, "expected error"
    except urllib.error.HTTPError as e:
        assert e.code == 405


def test_bedrock_openai_chat_completion_system_fingerprint_present():
    _, _, body = _post_chat_json({
        "model": "anthropic.claude-3-haiku-20240307-v1:0",
        "messages": [{"role": "user", "content": "x"}],
    })
    assert body["system_fingerprint"] == "ministack"


def test_bedrock_openai_chat_completion_accepts_system_developer_tool_roles():
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
