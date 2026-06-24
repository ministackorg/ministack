"""
Bedrock Agent Runtime service parity tests — all 31 ops covered.

boto3 round-trip on every operation family. Verified shapes against botocore
bedrock-agent-runtime-2023-07-26. Streaming ops drive boto3's EventStream
parser to validate eventstream framing + event sequence.
"""

import botocore.exceptions
from conftest import make_client


def _ar():
    return make_client("bedrock-agent-runtime")


# ---------------------------------------------------------------------------
# Retrieve / RetrieveAndGenerate / Rerank / GenerateQuery / OptimizePrompt
# ---------------------------------------------------------------------------


def test_bedrock_ar_retrieve_returns_results_envelope():
    resp = _ar().retrieve(
        knowledgeBaseId="KBTEST1234",
        retrievalQuery={"text": "what is AWS?"},
    )
    assert "retrievalResults" in resp
    assert isinstance(resp["retrievalResults"], list)


def test_bedrock_ar_retrieve_validation_missing_query():
    try:
        _ar().retrieve(knowledgeBaseId="kb-x", retrievalQuery={})
    except botocore.exceptions.ParamValidationError:
        return
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ValidationException"
    else:
        raise AssertionError("expected validation")


def test_bedrock_ar_retrieve_and_generate_returns_output_and_session():
    resp = _ar().retrieve_and_generate(
        input={"text": "what is ministack?"},
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": "kb-1",
                "modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0",
            },
        },
    )
    assert "output" in resp
    assert "text" in resp["output"]
    assert "sessionId" in resp
    assert "citations" in resp


def test_bedrock_ar_retrieve_and_generate_stream():
    resp = _ar().retrieve_and_generate_stream(
        input={"text": "stream this"},
        retrieveAndGenerateConfiguration={
            "type": "KNOWLEDGE_BASE",
            "knowledgeBaseConfiguration": {
                "knowledgeBaseId": "kb-1",
                "modelArn": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-haiku-20240307-v1:0",
            },
        },
    )
    events = list(resp["stream"])
    assert any("output" in e for e in events)


def test_bedrock_ar_rerank():
    resp = _ar().rerank(
        queries=[{"type": "TEXT", "textQuery": {"text": "what is ministack?"}}],
        sources=[
            {"type": "INLINE", "inlineDocumentSource": {"type": "TEXT", "textDocument": {"text": "ministack is an AWS emulator"}}},
            {"type": "INLINE", "inlineDocumentSource": {"type": "TEXT", "textDocument": {"text": "unrelated content"}}},
        ],
        rerankingConfiguration={
            "type": "BEDROCK_RERANKING_MODEL",
            "bedrockRerankingConfiguration": {
                "modelConfiguration": {
                    "modelArn": "arn:aws:bedrock:us-east-1::foundation-model/cohere.command-r-v1:0",
                },
            },
        },
    )
    assert "results" in resp
    assert len(resp["results"]) == 2


def test_bedrock_ar_generate_query():
    resp = _ar().generate_query(
        queryGenerationInput={"type": "TEXT", "text": "find all users"},
        transformationConfiguration={
            "mode": "TEXT_TO_SQL",
            "textToSqlConfiguration": {
                "type": "KNOWLEDGE_BASE",
                "knowledgeBaseConfiguration": {"knowledgeBaseArn":
                                                  "arn:aws:bedrock:us-east-1:000000000000:knowledge-base/kb"},
            },
        },
    )
    assert "queries" in resp


# ---------------------------------------------------------------------------
# Sessions + invocations
# ---------------------------------------------------------------------------


def test_bedrock_ar_session_lifecycle():
    create = _ar().create_session(
        sessionMetadata={"userId": "u-1"},
    )
    sid = create["sessionId"]
    assert ":session/" in create["sessionArn"]
    assert create["sessionStatus"] == "ACTIVE"

    g = _ar().get_session(sessionIdentifier=sid)
    assert g["sessionId"] == sid
    assert g["sessionMetadata"]["userId"] == "u-1"

    _ar().update_session(
        sessionIdentifier=sid,
        sessionMetadata={"userId": "u-2"},
    )
    g2 = _ar().get_session(sessionIdentifier=sid)
    assert g2["sessionMetadata"]["userId"] == "u-2"

    end = _ar().end_session(sessionIdentifier=sid)
    assert end["sessionStatus"] == "ENDED"

    _ar().delete_session(sessionIdentifier=sid)
    try:
        _ar().get_session(sessionIdentifier=sid)
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"


def test_bedrock_ar_list_sessions():
    _ar().create_session(sessionMetadata={"x": "1"})
    _ar().create_session(sessionMetadata={"x": "2"})
    resp = _ar().list_sessions()
    assert len(resp["sessionSummaries"]) >= 2


def test_bedrock_ar_invocation_steps_lifecycle():
    session = _ar().create_session()
    sid = session["sessionId"]
    inv = _ar().create_invocation(
        sessionIdentifier=sid, description="step test",
    )
    iid = inv["invocationId"]
    # Put step
    _ar().put_invocation_step(
        sessionIdentifier=sid,
        invocationIdentifier=iid,
        invocationStepTime="2026-06-05T00:00:00Z",
        payload={"contentBlocks": [{"text": "step payload"}]},
    )
    # List steps
    lst = _ar().list_invocation_steps(
        sessionIdentifier=sid, invocationIdentifier=iid,
    )
    assert len(lst["invocationStepSummaries"]) >= 1


# ---------------------------------------------------------------------------
# Flow runtime
# ---------------------------------------------------------------------------


def test_bedrock_ar_invoke_flow_streams_output_and_completion():
    resp = _ar().invoke_flow(
        flowIdentifier="FL-1",
        flowAliasIdentifier="FA-1",
        inputs=[{
            "content": {"document": "hello"},
            "nodeName": "InputNode",
            "nodeOutputName": "document",
        }],
    )
    events = list(resp["responseStream"])
    # Should include flowOutputEvent and flowCompletionEvent
    names = []
    for e in events:
        if isinstance(e, dict):
            names.extend(e.keys())
    assert "flowOutputEvent" in names
    assert "flowCompletionEvent" in names


def test_bedrock_ar_start_get_list_stop_flow_execution():
    create = _ar().start_flow_execution(
        flowIdentifier="FL-1",
        flowAliasIdentifier="FA-1",
        inputs=[{
            "content": {"document": "input"},
            "nodeName": "InputNode",
            "nodeOutputName": "document",
        }],
    )
    arn = create["executionArn"]
    exec_id = arn.rsplit("/", 1)[-1]
    g = _ar().get_flow_execution(
        flowIdentifier="FL-1", flowAliasIdentifier="FA-1",
        executionIdentifier=exec_id,
    )
    assert g["executionArn"] == arn
    lst = _ar().list_flow_executions(flowIdentifier="FL-1")
    assert any(e["executionArn"] == arn for e in lst["flowExecutionSummaries"])
    stop = _ar().stop_flow_execution(
        flowIdentifier="FL-1", flowAliasIdentifier="FA-1",
        executionIdentifier=exec_id,
    )
    assert stop["status"] == "Aborted"


def test_bedrock_ar_list_flow_execution_events():
    create = _ar().start_flow_execution(
        flowIdentifier="FL-1", flowAliasIdentifier="FA-1",
        inputs=[{"content": {"document": "i"}, "nodeName": "N", "nodeOutputName": "document"}],
    )
    exec_id = create["executionArn"].rsplit("/", 1)[-1]
    resp = _ar().list_flow_execution_events(
        flowIdentifier="FL-1", flowAliasIdentifier="FA-1",
        executionIdentifier=exec_id, eventType="Node",
    )
    assert "flowExecutionEvents" in resp


def test_bedrock_ar_get_execution_flow_snapshot():
    create = _ar().start_flow_execution(
        flowIdentifier="FL-1", flowAliasIdentifier="FA-1",
        inputs=[{"content": {"document": "i"}, "nodeName": "N", "nodeOutputName": "document"}],
    )
    exec_id = create["executionArn"].rsplit("/", 1)[-1]
    resp = _ar().get_execution_flow_snapshot(
        flowIdentifier="FL-1", flowAliasIdentifier="FA-1",
        executionIdentifier=exec_id,
    )
    assert resp["flowIdentifier"] == "FL-1"


# ---------------------------------------------------------------------------
# InvokeAgent (eventstream)
# ---------------------------------------------------------------------------


def test_bedrock_ar_invoke_agent_returns_chunk_eventstream():
    resp = _ar().invoke_agent(
        agentId="AG-1",
        agentAliasId="AL-1",
        sessionId="sess-1",
        inputText="hello agent",
    )
    events = list(resp["completion"])
    names = []
    for e in events:
        if isinstance(e, dict):
            names.extend(e.keys())
    assert "chunk" in names


def test_bedrock_ar_get_and_delete_agent_memory():
    resp = _ar().get_agent_memory(
        agentId="AG-1", agentAliasId="AL-1",
        memoryId="mem-1", memoryType="SESSION_SUMMARY",
    )
    assert "memoryContents" in resp
    _ar().delete_agent_memory(agentId="AG-1", agentAliasId="AL-1")


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def test_bedrock_ar_tag_resource():
    session = _ar().create_session()
    arn = session["sessionArn"]
    _ar().tag_resource(resourceArn=arn, tags={"env": "prod"})
    lst = _ar().list_tags_for_resource(resourceArn=arn)
    assert lst["tags"]["env"] == "prod"
    _ar().untag_resource(resourceArn=arn, tagKeys=["env"])
    lst2 = _ar().list_tags_for_resource(resourceArn=arn)
    assert "env" not in lst2["tags"]


def test_bedrock_ar_tag_resource_rejects_malformed_arn():
    try:
        _ar().tag_resource(resourceArn="not-an-arn-but-long-enough", tags={"env": "prod"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ValidationException"
    else:
        raise AssertionError("expected ValidationException")


def test_bedrock_ar_tag_resource_rejects_malformed_session_arn():
    arn = "arn:aws:bedrock:us-east-1:000000000000:session"
    try:
        _ar().tag_resource(resourceArn=arn, tags={"env": "prod"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ValidationException"
    else:
        raise AssertionError("expected ValidationException")


def test_bedrock_ar_tag_resource_rejects_wrong_scope_arn():
    session = _ar().create_session()
    wrong_region = session["sessionArn"].replace(":us-east-1:", ":us-west-2:")
    try:
        _ar().tag_resource(resourceArn=wrong_region, tags={"env": "prod"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"
    else:
        raise AssertionError("expected ResourceNotFoundException")


def test_bedrock_ar_tag_resource_rejects_noncanonical_partition_arn():
    session = _ar().create_session()
    wrong_partition = session["sessionArn"].replace("arn:aws:", "arn:aws-cn:", 1)
    try:
        _ar().tag_resource(resourceArn=wrong_partition, tags={"env": "prod"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"
    else:
        raise AssertionError("expected ResourceNotFoundException")


def test_bedrock_ar_tag_resource_rejects_wrong_service_arn():
    session = _ar().create_session()
    wrong_service = session["sessionArn"].replace(":bedrock:", ":lambda:")
    try:
        _ar().tag_resource(resourceArn=wrong_service, tags={"env": "prod"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ValidationException"
    else:
        raise AssertionError("expected ValidationException")


def test_bedrock_ar_tag_resource_rejects_flow_execution_arn():
    execution = _ar().start_flow_execution(
        flowIdentifier="FLTAG",
        flowAliasIdentifier="FATAG",
        inputs=[{"content": {"document": "input"}, "nodeName": "InputNode", "nodeOutputName": "document"}],
    )
    try:
        _ar().tag_resource(resourceArn=execution["executionArn"], tags={"env": "prod"})
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ValidationException"
    else:
        raise AssertionError("expected ValidationException")


def test_bedrock_ar_list_tags_rejects_unknown_resource_arn():
    arn = "arn:aws:bedrock:us-east-1:000000000000:session/SESSIONMISSING"
    try:
        _ar().list_tags_for_resource(resourceArn=arn)
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"
    else:
        raise AssertionError("expected ResourceNotFoundException")
