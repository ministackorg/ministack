"""Amazon Bedrock AgentCore emulator tests.

Covers the v1 surface: agent-runtime CRUD, runtime-endpoint CRUD,
InvokeAgentRuntime (deterministic echo), region isolation, and validation.
"""
import asyncio
import json
import os
import sys
import threading
import types
import urllib.request
import uuid as _uuid_mod
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.utils import parse_timestamp

from ministack.core.responses import StreamingResponse
from ministack.services import bedrock_agentcore as agentcore

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

_ARTIFACT = {"containerConfiguration": {"containerUri": "0.dkr.ecr.us-east-1.amazonaws.com/agent:latest"}}
_ROLE = "arn:aws:iam::000000000000:role/agentcore"
_NET = {"networkMode": "PUBLIC"}
_AUTH_ENABLED = os.environ.get("AUTH", "").lower() == "true"


def _client(service, region="us-east-1", access_key="test"):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


_CODE_ARTIFACT = {"codeConfiguration": {
    "code": {"s3": {"bucket": "agent-code", "prefix": "agent.zip"}},
    "runtime": "PYTHON_3_12", "entryPoint": ["main.py"]}}


def _create(ctl, name, artifact=_ARTIFACT):
    return ctl.create_agent_runtime(
        agentRuntimeName=name, agentRuntimeArtifact=artifact,
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
    # A code artifact never starts a container, so this echoes with or without Docker.
    rid_resp = _create(ctl, f"rt_{_uuid_mod.uuid4().hex[:8]}", _CODE_ARTIFACT)
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


@pytest.mark.skipif(
    not _AUTH_ENABLED,
    reason="resource-policy authorization requires a MiniStack server with AUTH=true",
)
def test_agentcore_resource_policy_cross_account_requires_runtime_and_endpoint():
    owner = "111111111111"
    caller = "222222222222"
    ctl = _client("bedrock-agentcore-control", access_key=owner)
    caller_iam = _client("iam", access_key=caller)
    caller_sts = _client("sts", access_key=caller)
    created = _create(ctl, f"policy_{_uuid_mod.uuid4().hex[:8]}", _CODE_ARTIFACT)
    runtime_arn = created["agentRuntimeArn"]
    runtime_id = created["agentRuntimeId"]
    role_name = f"worker_{_uuid_mod.uuid4().hex[:8]}"
    role_arn = f"arn:aws:iam::{caller}:role/{role_name}"
    worker_rt = None
    try:
        endpoint = ctl.create_agent_runtime_endpoint(
            agentRuntimeId=runtime_id, name="prod"
        )
        endpoint_arn = endpoint["agentRuntimeEndpointArn"]

        caller_iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": f"arn:aws:iam::{caller}:root"},
                    "Action": "sts:AssumeRole",
                }],
            }),
        )

        def identity_policy(resources):
            return json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Action": "bedrock-agentcore:InvokeAgentRuntime",
                    "Resource": resources,
                }],
            })

        caller_iam.put_role_policy(
            RoleName=role_name,
            PolicyName="invoke-runtime",
            PolicyDocument=identity_policy([runtime_arn, endpoint_arn]),
        )
        credentials = caller_sts.assume_role(
            RoleArn=role_arn, RoleSessionName="investigator"
        )["Credentials"]
        worker_rt = boto3.client(
            "bedrock-agentcore",
            endpoint_url=ENDPOINT,
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name="us-east-1",
            config=Config(region_name="us-east-1", retries={"mode": "standard"}),
        )

        def policy(resource):
            return json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"AWS": role_arn},
                    "Action": "bedrock-agentcore:InvokeAgentRuntime",
                    "Resource": resource,
                }],
            })

        assert ctl.put_resource_policy(
            resourceArn=runtime_arn, policy=policy(runtime_arn)
        )["policy"] == policy(runtime_arn)
        ctl.put_resource_policy(
            resourceArn=endpoint_arn, policy=policy(endpoint_arn)
        )

        response = worker_rt.invoke_agent_runtime(
            agentRuntimeArn=runtime_arn,
            qualifier="prod",
            payload=b"{}",
        )
        assert json.loads(response["response"].read())["agentRuntimeArn"] == runtime_arn

        ctl.delete_resource_policy(resourceArn=endpoint_arn)
        with pytest.raises(ClientError) as exc:
            worker_rt.invoke_agent_runtime(
                agentRuntimeArn=runtime_arn, qualifier="prod", payload=b"{}"
            )
        assert exc.value.response["Error"]["Code"] == "AccessDeniedException"

        ctl.put_resource_policy(
            resourceArn=endpoint_arn, policy=policy(endpoint_arn)
        )
        caller_iam.put_role_policy(
            RoleName=role_name,
            PolicyName="invoke-runtime",
            PolicyDocument=identity_policy([runtime_arn]),
        )
        with pytest.raises(ClientError) as exc:
            worker_rt.invoke_agent_runtime(
                agentRuntimeArn=runtime_arn, qualifier="prod", payload=b"{}"
            )
        assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
        caller_iam.put_role_policy(
            RoleName=role_name,
            PolicyName="invoke-runtime",
            PolicyDocument=identity_policy([runtime_arn, endpoint_arn]),
        )
        assert ctl.get_resource_policy(resourceArn=runtime_arn)["policy"]
        ctl.delete_resource_policy(resourceArn=runtime_arn)
        with pytest.raises(ClientError) as exc:
            worker_rt.invoke_agent_runtime(
                agentRuntimeArn=runtime_arn, qualifier="prod", payload=b"{}"
            )
        assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
    finally:
        for resource_arn in (
            locals().get("endpoint_arn"),
            locals().get("runtime_arn"),
        ):
            if resource_arn:
                try:
                    ctl.delete_resource_policy(resourceArn=resource_arn)
                except ClientError:
                    pass
        try:
            caller_iam.delete_role_policy(
                RoleName=role_name, PolicyName="invoke-runtime"
            )
            caller_iam.delete_role(RoleName=role_name)
        except ClientError:
            pass
        ctl.delete_agent_runtime(agentRuntimeId=runtime_id)


@pytest.mark.skipif(
    not _AUTH_ENABLED,
    reason="resource-policy authorization requires a MiniStack server with AUTH=true",
)
def test_agentcore_resource_policy_rejects_wildcard_resource():
    ctl = _client("bedrock-agentcore-control")
    created = _create(ctl, f"invalid_policy_{_uuid_mod.uuid4().hex[:8]}", _CODE_ARTIFACT)
    runtime_arn = created["agentRuntimeArn"]
    try:
        policy = json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "bedrock-agentcore:InvokeAgentRuntime",
                "Resource": "*",
            }],
        })
        with pytest.raises(ClientError) as exc:
            ctl.put_resource_policy(resourceArn=runtime_arn, policy=policy)
        assert exc.value.response["Error"]["Code"] == "ValidationException"
    finally:
        ctl.delete_agent_runtime(agentRuntimeId=created["agentRuntimeId"])


@pytest.mark.skipif(
    not _AUTH_ENABLED,
    reason="resource-policy authorization requires a MiniStack server with AUTH=true",
)
def test_agentcore_resource_policy_explicit_deny_overrides_identity_allow():
    ctl = _client("bedrock-agentcore-control")
    rt = _client("bedrock-agentcore")
    created = _create(ctl, f"deny_policy_{_uuid_mod.uuid4().hex[:8]}", _CODE_ARTIFACT)
    runtime_arn = created["agentRuntimeArn"]
    policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": "*",
            "Action": "bedrock-agentcore:InvokeAgentRuntime",
            "Resource": runtime_arn,
        }],
    })
    try:
        ctl.put_resource_policy(resourceArn=runtime_arn, policy=policy)
        with pytest.raises(ClientError) as exc:
            rt.invoke_agent_runtime(agentRuntimeArn=runtime_arn, payload=b"{}")
        assert exc.value.response["Error"]["Code"] == "AccessDeniedException"
    finally:
        ctl.delete_resource_policy(resourceArn=runtime_arn)
        ctl.delete_agent_runtime(agentRuntimeId=created["agentRuntimeId"])


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


def _raw(method, path, body=None):
    """The wire bytes, not boto3's parse: botocore accepts a number for an
    iso8601 timestamp, so only the raw JSON shows what the SDKs actually get."""
    request = urllib.request.Request(
        f"{ENDPOINT}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Content-Type": "application/json",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20260101/"
                             "us-east-1/bedrock-agentcore/aws4_request",
        },
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read() or b"{}")


def test_agentcore_timestamps_are_iso8601_strings_on_the_wire():
    """DateTimestamp carries timestampFormat iso8601, so every timestamp this
    service answers is an RFC 3339 string. A JSON number fails the Go SDK's
    deserializer outright, so Terraform cannot record the resource."""
    ctl = _client("bedrock-agentcore-control")
    name = f"rt_{_uuid_mod.uuid4().hex[:8]}"
    created = _raw("PUT", "/runtimes", {
        "agentRuntimeName": name, "roleArn": _ROLE,
        "networkConfiguration": _NET, "agentRuntimeArtifact": _ARTIFACT,
    })
    rid = created["agentRuntimeId"]
    try:
        def check(payload, *fields, where=""):
            for field in fields:
                value = payload[field]
                assert isinstance(value, str), f"{where}{field} is {type(value).__name__}"
                # botocore parses what it is handed; a float would also parse,
                # so the type assertion above is the one that matters.
                assert parse_timestamp(value).tzinfo is not None, f"{where}{field}"

        check(created, "createdAt", where="CreateAgentRuntime.")
        check(_raw("GET", f"/runtimes/{rid}"), "createdAt", "lastUpdatedAt",
              where="GetAgentRuntime.")
        listed = _raw("POST", "/runtimes?maxResults=10", {})["agentRuntimes"]
        check(next(r for r in listed if r["agentRuntimeId"] == rid),
              "lastUpdatedAt", where="ListAgentRuntimes.")
        updated = _raw("PUT", f"/runtimes/{rid}", {
            "roleArn": _ROLE, "networkConfiguration": _NET,
            "agentRuntimeArtifact": _ARTIFACT,
        })
        check(updated, "createdAt", "lastUpdatedAt", where="UpdateAgentRuntime.")

        endpoint = _raw("PUT", f"/runtimes/{rid}/runtime-endpoints", {"name": "ep1"})
        check(endpoint, "createdAt", where="CreateAgentRuntimeEndpoint.")
        check(_raw("GET", f"/runtimes/{rid}/runtime-endpoints/ep1"),
              "createdAt", "lastUpdatedAt", where="GetAgentRuntimeEndpoint.")
        eps = _raw("POST", f"/runtimes/{rid}/runtime-endpoints?maxResults=10", {})
        check(eps["runtimeEndpoints"][0], "createdAt", "lastUpdatedAt",
              where="ListAgentRuntimeEndpoints.")
    finally:
        ctl.delete_agent_runtime(agentRuntimeId=rid)


# --- InvokeAgentRuntime against the runtime container (in-process) ---

def _runtime(name="container_test", image="example.local/worker:1"):
    status, _, body = agentcore._create_agent_runtime(json.dumps({
        "agentRuntimeName": name,
        "agentRuntimeArtifact": {"containerConfiguration": {"containerUri": image}},
        "roleArn": "arn:aws:iam::000000000000:role/agentcore",
        "networkConfiguration": {"networkMode": "PUBLIC"},
        "environmentVariables": {"WORKER_MODE": "test"},
    }).encode())
    assert status == 200
    return json.loads(body)


def _invoke(arn, headers=None, body=b"{}"):
    return asyncio.run(agentcore.handle_request(
        "POST", f"/runtimes/{arn}/invocations",
        headers or {"content-type": "application/json"}, body, {},
    ))


def _fake_docker(monkeypatch, port, *, fail=False, network=None,
                 fail_on_start=False, fail_after_create=False, missing_image=False):
    started = []
    removed = []
    created = []
    pulled = []

    class ImageNotFound(Exception):
        pass

    class Container:
        status = "running"
        attrs = {"NetworkSettings": {
            "Ports": {"8080/tcp": [{"HostPort": str(port)}]},
            "Networks": {network: {"IPAddress": "127.0.0.1"}} if network else {},
        }}

        def reload(self):
            pass

        def start(self):
            if fail_on_start:
                raise TimeoutError("Docker start timed out")

        def remove(self, force=False):
            removed.append(force)

    class Containers:
        def get(self, _name):
            if not network:
                raise RuntimeError("MiniStack is not in Docker")
            return Container()

        def create(self, image, **kwargs):
            if fail:
                raise RuntimeError("image unavailable")
            if missing_image and not pulled:
                raise ImageNotFound(image)
            started.append((image, kwargs))
            container = Container()
            created.append(container)
            if fail_after_create:
                raise TimeoutError("Docker create timed out after allocation")
            return container

        def list(self, **_kwargs):
            return created

    fake_docker = types.SimpleNamespace(
        errors=types.SimpleNamespace(ImageNotFound=ImageNotFound),
        from_env=lambda **_kwargs: types.SimpleNamespace(
            containers=Containers(),
            images=types.SimpleNamespace(pull=lambda image: pulled.append(image)),
            ping=lambda: True,
        ),
    )
    monkeypatch.setitem(sys.modules, "docker", fake_docker)
    from ministack.services import bedrock_agentcore
    monkeypatch.setattr(bedrock_agentcore, "_docker", None)
    return started, removed


def _worker(response_status=200):
    requests = []

    class Worker(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200 if self.path == "/ping" else 404)
            self.end_headers()

        def do_POST(self):
            requests.append((self.path, dict(self.headers),
                             self.rfile.read(int(self.headers["Content-Length"]))))
            payload = b'{"evidence":[{"id":"deployment-1"}]}'
            self.send_response(response_status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Worker)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, requests


async def _discard(_message):
    pass


def test_container_image_invoked_and_removed(monkeypatch):
    server, thread, requests = _worker()
    started, removed = _fake_docker(monkeypatch, server.server_port)
    runtime = _runtime()
    try:
        headers = {"content-type": "application/json", "authorization": "secret",
                   "x-amzn-bedrock-agentcore-runtime-session-id": "session-1"}
        status, response_headers, body = _invoke(runtime["agentRuntimeArn"], headers,
                                                 b'{"prompt":"why?"}')
        assert status == 200
        assert isinstance(body, StreamingResponse)
        messages = []

        async def send(message):
            messages.append(message)

        asyncio.run(body.runner(send, None))
        assert json.loads(b"".join(m["body"] for m in messages)) == {
            "evidence": [{"id": "deployment-1"}]}
        assert response_headers["x-amzn-bedrock-agentcore-runtime-session-id"] == "session-1"
        assert started[0][0] == "example.local/worker:1"
        assert started[0][1]["ports"] == {"8080/tcp": ("127.0.0.1", None)}
        assert started[0][1]["environment"] == {"WORKER_MODE": "test"}
        from ministack.core.container_reaper import INSTANCE_LABEL
        assert started[0][1]["labels"]["ministack"] == "agentcore"
        assert INSTANCE_LABEL in started[0][1]["labels"]
        path, forwarded, payload = requests[0]
        assert path == "/invocations" and payload == b'{"prompt":"why?"}'
        assert "authorization" not in {key.lower() for key in forwarded}
        _, _, second_body = _invoke(runtime["agentRuntimeArn"])
        asyncio.run(second_body.runner(_discard, None))
        assert len(started) == 1
    finally:
        agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
        server.shutdown()
        server.server_close()
        thread.join()
    assert removed == [True]


def test_container_joins_ministack_network(monkeypatch):
    started, _ = _fake_docker(monkeypatch, 8080, network="ministack-net")
    class Healthy:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

    monkeypatch.setattr(agentcore, "_local_open", lambda *_args, **_kwargs: Healthy())
    runtime = _runtime("network_test")
    try:
        url = agentcore._container_invocations_url(agentcore._runtimes[runtime["agentRuntimeId"]])
        assert url == "http://127.0.0.1:8080/invocations"
        assert started[0][1]["network"] == "ministack-net"
        assert "ports" not in started[0][1]
    finally:
        agentcore._delete_agent_runtime(runtime["agentRuntimeId"])


def test_worker_error_is_runtime_client_error(monkeypatch):
    server, thread, _ = _worker(400)
    _fake_docker(monkeypatch, server.server_port)
    runtime = _runtime("worker_error")
    try:
        status, headers, body = _invoke(runtime["agentRuntimeArn"])
        assert status == 424
        assert headers["x-amzn-errortype"] == "RuntimeClientError"
        assert json.loads(body)["__type"] == "RuntimeClientError"
    finally:
        agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
        server.shutdown()
        server.server_close()
        thread.join()


def test_container_start_failure_is_not_echo(monkeypatch):
    _fake_docker(monkeypatch, 1, fail=True)
    runtime = _runtime("image_failure")
    try:
        status, headers, body = _invoke(runtime["agentRuntimeArn"])
        assert status == 424
        assert headers["x-amzn-errortype"] == "RuntimeClientError"
        assert b"image unavailable" in body
    finally:
        agentcore._delete_agent_runtime(runtime["agentRuntimeId"])


def test_container_is_removed_if_start_times_out(monkeypatch):
    _, removed = _fake_docker(monkeypatch, 1, fail_on_start=True)
    runtime = _runtime("start_timeout")
    try:
        status, _, _ = _invoke(runtime["agentRuntimeArn"])
        assert status == 424
        assert removed == [True]
    finally:
        agentcore._delete_agent_runtime(runtime["agentRuntimeId"])


def test_missing_image_is_pulled_then_started(monkeypatch):
    server, thread, _ = _worker()
    started, _ = _fake_docker(monkeypatch, server.server_port, missing_image=True)
    runtime = _runtime("pull_image")
    try:
        status, _, response = _invoke(runtime["agentRuntimeArn"])
        assert status == 200
        asyncio.run(response.runner(_discard, None))
        assert [image for image, _ in started] == ["example.local/worker:1"]
    finally:
        agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
        server.shutdown()
        server.server_close()
        thread.join()


def test_orphan_is_removed_if_create_times_out(monkeypatch):
    _, removed = _fake_docker(monkeypatch, 1, fail_after_create=True)
    runtime = _runtime("create_timeout")
    try:
        status, _, _ = _invoke(runtime["agentRuntimeArn"])
        assert status == 424
        assert removed == [True]
    finally:
        agentcore._delete_agent_runtime(runtime["agentRuntimeId"])


def test_connection_reset_maps_to_runtime_client_error(monkeypatch):
    server, thread, _ = _worker()
    _fake_docker(monkeypatch, server.server_port)
    runtime = _runtime("connection_reset")
    try:
        agentcore._container_invocations_url(agentcore._runtimes[runtime["agentRuntimeId"]])
        monkeypatch.setattr(agentcore, "_local_open", lambda *_args, **_kwargs:
                            (_ for _ in ()).throw(ConnectionResetError("peer reset")))
        status, headers, _ = _invoke(runtime["agentRuntimeArn"])
        assert status == 424
        assert headers["x-amzn-errortype"] == "RuntimeClientError"
    finally:
        agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
        server.shutdown()
        server.server_close()
        thread.join()


def test_stream_failure_leaves_response_truncated(monkeypatch):
    class BrokenResponse:
        status = 200
        headers = {"Content-Type": "text/event-stream"}
        closed = False
        reads = 0

        def read1(self, _size):
            self.reads += 1
            if self.reads == 1:
                return b"first\n"
            raise ConnectionResetError("stream interrupted")

        def close(self):
            self.closed = True

    response = BrokenResponse()
    monkeypatch.setattr(agentcore, "_local_open", lambda *_args, **_kwargs: response)
    status, _, stream = agentcore._invoke_container(
        "http://127.0.0.1:8080/invocations", b"{}", {}, "application/json", "session-1")
    assert status == 200
    frames = []

    async def send(frame):
        frames.append(frame)

    asyncio.run(stream.runner(send, None))
    assert frames == [{"type": "http.response.body", "body": b"first\n", "more_body": True}]
    assert response.closed


def test_update_restarts_image(monkeypatch):
    server, thread, _ = _worker()
    started, removed = _fake_docker(monkeypatch, server.server_port)
    runtime = _runtime("update_image")
    try:
        status, _, response = _invoke(runtime["agentRuntimeArn"])
        assert status == 200
        asyncio.run(response.runner(_discard, None))
        agentcore._update_agent_runtime(runtime["agentRuntimeId"], json.dumps({
            "agentRuntimeArtifact": {"containerConfiguration": {"containerUri": "example.local/worker:2"}},
            "roleArn": "arn:aws:iam::000000000000:role/agentcore",
            "networkConfiguration": {"networkMode": "PUBLIC"},
        }).encode())
        assert removed == [True]
        current = agentcore._runtimes[runtime["agentRuntimeId"]]
        status, _, response = _invoke(current["agentRuntimeArn"])
        assert status == 200
        assert [image for image, _ in started] == ["example.local/worker:1", "example.local/worker:2"]
        asyncio.run(response.runner(_discard, None))
    finally:
        agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
        server.shutdown()
        server.server_close()
        thread.join()


def test_without_docker_the_invocation_echoes(monkeypatch):
    monkeypatch.setitem(sys.modules, "docker", None)
    monkeypatch.setattr(agentcore, "_docker", None)
    runtime = _runtime(name="no_docker_echo")
    try:
        status, _, body = _invoke(runtime["agentRuntimeArn"], body=b'{"prompt":"hi"}')
        assert status == 200
        assert json.loads(body)["input"] == {"prompt": "hi"}
    finally:
        agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
