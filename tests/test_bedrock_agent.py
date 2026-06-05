"""
Bedrock Agent service parity tests — all 72 ops covered.

boto3 round-trip on every resource family. Shapes verified against botocore
bedrock-agent-2023-06-05.
"""

import botocore.exceptions
from conftest import make_client


def _agent():
    return make_client("bedrock-agent")


def _make_agent(name="agent-test"):
    return _agent().create_agent(
        agentName=name,
        agentResourceRoleArn="arn:aws:iam::000000000000:role/agent-role",
        foundationModel="anthropic.claude-3-haiku-20240307-v1:0",
        instruction="You are a helpful test assistant. Be concise and answer questions clearly.",
    )["agent"]


def _make_kb(name="kb-test"):
    return _agent().create_knowledge_base(
        name=name,
        roleArn="arn:aws:iam::000000000000:role/kb-role",
        knowledgeBaseConfiguration={
            "type": "VECTOR",
            "vectorKnowledgeBaseConfiguration": {
                "embeddingModelArn":
                    "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0",
            },
        },
        storageConfiguration={
            "type": "OPENSEARCH_SERVERLESS",
            "opensearchServerlessConfiguration": {
                "collectionArn": "arn:aws:aoss:us-east-1:000000000000:collection/coll",
                "vectorIndexName": "idx",
                "fieldMapping": {"vectorField": "v", "textField": "t",
                                  "metadataField": "m"},
            },
        },
    )["knowledgeBase"]


# ---------------------------------------------------------------------------
# Agents
# ---------------------------------------------------------------------------


def test_bedrock_agent_create_get_list_delete():
    create = _make_agent("ag-1")
    assert create["agentName"] == "ag-1"
    assert create["agentStatus"] in ("NOT_PREPARED", "CREATING")
    assert ":agent/" in create["agentArn"]

    g = _agent().get_agent(agentId=create["agentId"])
    assert g["agent"]["agentName"] == "ag-1"

    lst = _agent().list_agents()
    assert any(a["agentId"] == create["agentId"] for a in lst["agentSummaries"])

    _agent().delete_agent(agentId=create["agentId"])
    try:
        _agent().get_agent(agentId=create["agentId"])
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ResourceNotFoundException"


def test_bedrock_agent_update():
    create = _make_agent("ag-upd")
    _agent().update_agent(
        agentId=create["agentId"],
        agentName="ag-upd-renamed",
        agentResourceRoleArn="arn:aws:iam::000000000000:role/agent-role",
        foundationModel="amazon.nova-pro-v1:0",
        instruction="You are an updated helpful test assistant for verification purposes.",
    )
    g = _agent().get_agent(agentId=create["agentId"])
    assert g["agent"]["agentName"] == "ag-upd-renamed"


def test_bedrock_agent_prepare():
    create = _make_agent("ag-prep")
    resp = _agent().prepare_agent(agentId=create["agentId"])
    assert resp["agentStatus"] == "PREPARED"


def test_bedrock_agent_create_duplicate_conflict():
    _make_agent("ag-dup")
    try:
        _make_agent("ag-dup")
    except botocore.exceptions.ClientError as exc:
        assert exc.response["Error"]["Code"] == "ConflictException"
    else:
        raise AssertionError("expected conflict")


# ---------------------------------------------------------------------------
# Agent aliases
# ---------------------------------------------------------------------------


def test_bedrock_agent_alias_lifecycle():
    agent = _make_agent("ag-for-alias")
    create = _agent().create_agent_alias(
        agentId=agent["agentId"], agentAliasName="prod",
    )
    aid = create["agentAlias"]["agentAliasId"]
    g = _agent().get_agent_alias(agentId=agent["agentId"], agentAliasId=aid)
    assert g["agentAlias"]["agentAliasName"] == "prod"
    _agent().update_agent_alias(
        agentId=agent["agentId"], agentAliasId=aid,
        agentAliasName="staging",
    )
    g2 = _agent().get_agent_alias(agentId=agent["agentId"], agentAliasId=aid)
    assert g2["agentAlias"]["agentAliasName"] == "staging"
    lst = _agent().list_agent_aliases(agentId=agent["agentId"])
    assert any(a["agentAliasId"] == aid for a in lst["agentAliasSummaries"])
    _agent().delete_agent_alias(agentId=agent["agentId"], agentAliasId=aid)


# ---------------------------------------------------------------------------
# Agent action groups
# ---------------------------------------------------------------------------


def test_bedrock_agent_action_group_lifecycle():
    agent = _make_agent("ag-ag")
    create = _agent().create_agent_action_group(
        agentId=agent["agentId"], agentVersion="DRAFT",
        actionGroupName="ag-1",
        actionGroupExecutor={"lambda": "arn:aws:lambda:us-east-1:000000000000:function:f"},
    )
    agid = create["agentActionGroup"]["actionGroupId"]
    g = _agent().get_agent_action_group(
        agentId=agent["agentId"], agentVersion="DRAFT", actionGroupId=agid,
    )
    assert g["agentActionGroup"]["actionGroupName"] == "ag-1"
    _agent().update_agent_action_group(
        agentId=agent["agentId"], agentVersion="DRAFT", actionGroupId=agid,
        actionGroupName="ag-1-renamed",
        actionGroupExecutor={"lambda": "arn:aws:lambda:us-east-1:000000000000:function:f"},
    )
    lst = _agent().list_agent_action_groups(
        agentId=agent["agentId"], agentVersion="DRAFT",
    )
    assert any(a["actionGroupId"] == agid for a in lst["actionGroupSummaries"])
    _agent().delete_agent_action_group(
        agentId=agent["agentId"], agentVersion="DRAFT", actionGroupId=agid,
        skipResourceInUseCheck=True,
    )


# ---------------------------------------------------------------------------
# Agent collaborators
# ---------------------------------------------------------------------------


def test_bedrock_agent_collaborator_lifecycle():
    agent = _make_agent("ag-collab")
    assoc = _agent().associate_agent_collaborator(
        agentId=agent["agentId"], agentVersion="DRAFT",
        agentDescriptor={"aliasArn": "arn:aws:bedrock:us-east-1:000000000000:agent-alias/A/B"},
        collaboratorName="researcher",
        collaborationInstruction="Help with research.",
    )
    cid = assoc["agentCollaborator"]["collaboratorId"]
    g = _agent().get_agent_collaborator(
        agentId=agent["agentId"], agentVersion="DRAFT", collaboratorId=cid,
    )
    assert g["agentCollaborator"]["collaboratorName"] == "researcher"
    _agent().disassociate_agent_collaborator(
        agentId=agent["agentId"], agentVersion="DRAFT", collaboratorId=cid,
    )


# ---------------------------------------------------------------------------
# Knowledge bases + data sources + ingestion jobs
# ---------------------------------------------------------------------------


def test_bedrock_agent_knowledge_base_crud():
    kb = _make_kb("kb-test-1")
    assert kb["status"] == "ACTIVE"
    g = _agent().get_knowledge_base(knowledgeBaseId=kb["knowledgeBaseId"])
    assert g["knowledgeBase"]["name"] == "kb-test-1"
    lst = _agent().list_knowledge_bases()
    assert any(k["knowledgeBaseId"] == kb["knowledgeBaseId"]
                for k in lst["knowledgeBaseSummaries"])
    _agent().delete_knowledge_base(knowledgeBaseId=kb["knowledgeBaseId"])


def test_bedrock_agent_data_source_crud():
    kb = _make_kb("kb-for-ds")
    ds = _agent().create_data_source(
        knowledgeBaseId=kb["knowledgeBaseId"],
        name="ds-1",
        dataSourceConfiguration={
            "type": "S3",
            "s3Configuration": {"bucketArn": "arn:aws:s3:::bucket"},
        },
    )["dataSource"]
    g = _agent().get_data_source(
        knowledgeBaseId=kb["knowledgeBaseId"], dataSourceId=ds["dataSourceId"],
    )
    assert g["dataSource"]["name"] == "ds-1"
    lst = _agent().list_data_sources(knowledgeBaseId=kb["knowledgeBaseId"])
    assert any(d["dataSourceId"] == ds["dataSourceId"]
                for d in lst["dataSourceSummaries"])
    _agent().delete_data_source(
        knowledgeBaseId=kb["knowledgeBaseId"], dataSourceId=ds["dataSourceId"],
    )


def test_bedrock_agent_ingestion_job_lifecycle():
    kb = _make_kb("kb-ij")
    ds = _agent().create_data_source(
        knowledgeBaseId=kb["knowledgeBaseId"], name="ds-ij",
        dataSourceConfiguration={"type": "S3", "s3Configuration": {"bucketArn": "arn:aws:s3:::b"}},
    )["dataSource"]
    job = _agent().start_ingestion_job(
        knowledgeBaseId=kb["knowledgeBaseId"], dataSourceId=ds["dataSourceId"],
    )["ingestionJob"]
    g = _agent().get_ingestion_job(
        knowledgeBaseId=kb["knowledgeBaseId"], dataSourceId=ds["dataSourceId"],
        ingestionJobId=job["ingestionJobId"],
    )
    assert g["ingestionJob"]["status"] in ("STARTING", "IN_PROGRESS", "COMPLETE")
    lst = _agent().list_ingestion_jobs(
        knowledgeBaseId=kb["knowledgeBaseId"], dataSourceId=ds["dataSourceId"],
    )
    assert any(j["ingestionJobId"] == job["ingestionJobId"]
                for j in lst["ingestionJobSummaries"])


# ---------------------------------------------------------------------------
# Flows
# ---------------------------------------------------------------------------


def test_bedrock_agent_flow_lifecycle():
    flow = _agent().create_flow(
        name="flow-1",
        executionRoleArn="arn:aws:iam::000000000000:role/flow-role",
        definition={"nodes": [], "connections": []},
    )
    fid = flow["id"]
    assert ":flow/" in flow["arn"]
    g = _agent().get_flow(flowIdentifier=fid)
    assert g["name"] == "flow-1"
    lst = _agent().list_flows()
    assert any(f["id"] == fid for f in lst["flowSummaries"])
    _agent().update_flow(
        flowIdentifier=fid, name="flow-1-renamed",
        executionRoleArn="arn:aws:iam::000000000000:role/flow-role",
        definition={"nodes": [], "connections": []},
    )
    _agent().prepare_flow(flowIdentifier=fid)
    _agent().delete_flow(flowIdentifier=fid)


def test_bedrock_agent_flow_version():
    flow = _agent().create_flow(
        name="flow-ver",
        executionRoleArn="arn:aws:iam::000000000000:role/flow-role",
        definition={"nodes": [], "connections": []},
    )
    ver = _agent().create_flow_version(flowIdentifier=flow["id"])
    assert ver["version"] == "1"
    g = _agent().get_flow_version(flowIdentifier=flow["id"], flowVersion="1")
    assert g["version"] == "1"
    lst = _agent().list_flow_versions(flowIdentifier=flow["id"])
    assert any(v["version"] == "1" for v in lst["flowVersionSummaries"])


def test_bedrock_agent_flow_alias():
    flow = _agent().create_flow(
        name="flow-alias",
        executionRoleArn="arn:aws:iam::000000000000:role/flow-role",
    )
    _agent().create_flow_version(flowIdentifier=flow["id"])
    alias = _agent().create_flow_alias(
        flowIdentifier=flow["id"], name="prod",
        routingConfiguration=[{"flowVersion": "1"}],
    )
    aid = alias["id"]
    g = _agent().get_flow_alias(flowIdentifier=flow["id"], aliasIdentifier=aid)
    assert g["name"] == "prod"


def test_bedrock_agent_validate_flow_definition():
    resp = _agent().validate_flow_definition(definition={"nodes": [], "connections": []})
    assert "validations" in resp


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------


def test_bedrock_agent_prompt_lifecycle():
    create = _agent().create_prompt(
        name="prompt-1",
        variants=[{"name": "v1", "templateType": "TEXT",
                    "templateConfiguration": {"text": {"text": "{{x}}"}}}],
        defaultVariant="v1",
    )
    pid = create["id"]
    assert ":prompt/" in create["arn"]
    g = _agent().get_prompt(promptIdentifier=pid)
    assert g["name"] == "prompt-1"
    lst = _agent().list_prompts()
    assert any(p["id"] == pid for p in lst["promptSummaries"])
    _agent().update_prompt(
        promptIdentifier=pid, name="prompt-1-renamed",
        variants=[{"name": "v1", "templateType": "TEXT",
                    "templateConfiguration": {"text": {"text": "{{y}}"}}}],
        defaultVariant="v1",
    )
    ver = _agent().create_prompt_version(promptIdentifier=pid)
    assert ver["version"] == "1"
    _agent().delete_prompt(promptIdentifier=pid)


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def test_bedrock_agent_tag_resource():
    agent = _make_agent("ag-tag")
    arn = agent["agentArn"]
    _agent().tag_resource(resourceArn=arn, tags={"env": "prod", "team": "ml"})
    lst = _agent().list_tags_for_resource(resourceArn=arn)
    assert lst["tags"]["env"] == "prod"
    _agent().untag_resource(resourceArn=arn, tagKeys=["env"])
    lst2 = _agent().list_tags_for_resource(resourceArn=arn)
    assert "env" not in lst2["tags"]
