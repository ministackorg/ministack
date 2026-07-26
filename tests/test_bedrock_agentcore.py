"""Amazon Bedrock AgentCore emulator tests.

Covers the v1 surface: agent-runtime CRUD, runtime-endpoint CRUD,
InvokeAgentRuntime (deterministic echo), region isolation, and validation.
"""
import json
import os
import uuid as _uuid_mod

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

_ARTIFACT = {"containerConfiguration": {"containerUri": "0.dkr.ecr.us-east-1.amazonaws.com/agent:latest"}}
_ROLE = "arn:aws:iam::000000000000:role/agentcore"
_NET = {"networkMode": "PUBLIC"}


def _client(service, region="us-east-1"):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


def _create(ctl, name):
    return ctl.create_agent_runtime(
        agentRuntimeName=name, agentRuntimeArtifact=_ARTIFACT,
        roleArn=_ROLE, networkConfiguration=_NET,
    )


def test_agentcore_runtime_lifecycle():
    ctl = _client("bedrock-agentcore-control")
    name = f"rt_{_uuid_mod.uuid4().hex[:8]}"
    created = _create(ctl, name)
    rid = created["agentRuntimeId"]
    assert created["status"] == "CREATING"
    assert created["agentRuntimeVersion"] == "1"
    assert created["agentRuntimeArn"].startswith(
        "arn:aws:bedrock-agentcore:us-east-1:000000000000:agent/")
    assert created["workloadIdentityDetails"]["workloadIdentityArn"]
    try:
        got = ctl.get_agent_runtime(agentRuntimeId=rid)
        assert got["status"] == "READY"
        assert got["agentRuntimeName"] == name
        assert got["roleArn"] == _ROLE

        ids = [r["agentRuntimeId"] for r in ctl.list_agent_runtimes()["agentRuntimes"]]
        assert rid in ids

        updated = ctl.update_agent_runtime(
            agentRuntimeId=rid, agentRuntimeArtifact=_ARTIFACT,
            roleArn=_ROLE, networkConfiguration=_NET,
        )
        assert updated["agentRuntimeVersion"] == "2"
        assert updated["status"] == "UPDATING"
        assert ctl.get_agent_runtime(agentRuntimeId=rid)["agentRuntimeVersion"] == "2"

        versions = ctl.list_agent_runtime_versions(agentRuntimeId=rid)["agentRuntimes"]
        assert versions and versions[0]["agentRuntimeId"] == rid
    finally:
        deleted = ctl.delete_agent_runtime(agentRuntimeId=rid)
        assert deleted["status"] == "DELETING"

    with pytest.raises(ClientError) as exc:
        ctl.get_agent_runtime(agentRuntimeId=rid)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_agentcore_endpoint_lifecycle():
    ctl = _client("bedrock-agentcore-control")
    name = f"rt_{_uuid_mod.uuid4().hex[:8]}"
    rid = _create(ctl, name)["agentRuntimeId"]
    try:
        ep = ctl.create_agent_runtime_endpoint(agentRuntimeId=rid, name="prod")
        assert ep["endpointName"] == "prod"
        assert ep["status"] == "CREATING"
        assert ep["agentRuntimeEndpointArn"].startswith(
            "arn:aws:bedrock-agentcore:us-east-1:000000000000:agentEndpoint/")

        got = ctl.get_agent_runtime_endpoint(agentRuntimeId=rid, endpointName="prod")
        assert got["status"] == "READY"
        assert got["name"] == "prod"

        names = [e["name"] for e in
                 ctl.list_agent_runtime_endpoints(agentRuntimeId=rid)["runtimeEndpoints"]]
        assert names == ["prod"]

        upd = ctl.update_agent_runtime_endpoint(
            agentRuntimeId=rid, endpointName="prod", description="live")
        assert upd["status"] == "UPDATING"

        assert ctl.delete_agent_runtime_endpoint(
            agentRuntimeId=rid, endpointName="prod")["status"] == "DELETING"
        with pytest.raises(ClientError):
            ctl.get_agent_runtime_endpoint(agentRuntimeId=rid, endpointName="prod")
    finally:
        ctl.delete_agent_runtime(agentRuntimeId=rid)


def test_agentcore_invoke_returns_deterministic_echo():
    ctl = _client("bedrock-agentcore-control")
    rt = _client("bedrock-agentcore")
    rid_resp = _create(ctl, f"rt_{_uuid_mod.uuid4().hex[:8]}")
    arn = rid_resp["agentRuntimeArn"]
    try:
        resp = rt.invoke_agent_runtime(
            agentRuntimeArn=arn, payload=json.dumps({"prompt": "hello"}).encode())
        assert resp["contentType"] == "application/json"
        body = json.loads(resp["response"].read())
        assert body["agentRuntimeArn"] == arn
        assert body["input"] == {"prompt": "hello"}

        with pytest.raises(ClientError) as exc:
            rt.invoke_agent_runtime(
                agentRuntimeArn="arn:aws:bedrock-agentcore:us-east-1:000000000000:"
                                "agent/00000000-0000-0000-0000-000000000000:1",
                payload=b"{}")
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        ctl.delete_agent_runtime(agentRuntimeId=rid_resp["agentRuntimeId"])


def test_agentcore_runtimes_are_region_scoped():
    east = _client("bedrock-agentcore-control", "us-east-1")
    west = _client("bedrock-agentcore-control", "us-west-2")
    name = f"shared_{_uuid_mod.uuid4().hex[:8]}"
    e = _create(east, name)
    w = _create(west, name)
    try:
        east_ids = {r["agentRuntimeId"] for r in east.list_agent_runtimes()["agentRuntimes"]}
        west_ids = {r["agentRuntimeId"] for r in west.list_agent_runtimes()["agentRuntimes"]}
        assert e["agentRuntimeId"] in east_ids and e["agentRuntimeId"] not in west_ids
        assert w["agentRuntimeId"] in west_ids and w["agentRuntimeId"] not in east_ids
        assert ":us-east-1:" in e["agentRuntimeArn"]
        assert ":us-west-2:" in w["agentRuntimeArn"]
        with pytest.raises(ClientError):
            west.get_agent_runtime(agentRuntimeId=e["agentRuntimeId"])
    finally:
        east.delete_agent_runtime(agentRuntimeId=e["agentRuntimeId"])
        west.delete_agent_runtime(agentRuntimeId=w["agentRuntimeId"])


def test_agentcore_create_validation():
    ctl = _client("bedrock-agentcore-control")
    with pytest.raises(ClientError) as exc:
        ctl.create_agent_runtime(
            agentRuntimeName="bad name!", agentRuntimeArtifact=_ARTIFACT,
            roleArn=_ROLE, networkConfiguration=_NET)
    assert exc.value.response["Error"]["Code"] == "ValidationException"
