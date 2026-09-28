"""Opt-in AgentCore HTTP proxy tests; no AWS account or model is involved."""

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ministack.core.responses import get_account_id, get_region
from ministack.services import bedrock_agentcore


def _create_runtime(name):
    status, _, body = bedrock_agentcore._create_agent_runtime(json.dumps({
        "agentRuntimeName": name,
        "agentRuntimeArtifact": {"containerConfiguration": {"containerUri": "example.invalid/agent"}},
        "roleArn": "arn:aws:iam::000000000000:role/agentcore",
        "networkConfiguration": {"networkMode": "PUBLIC"},
    }).encode())
    assert status == 200
    return json.loads(body)


def test_agentcore_proxy_forwards_payload_and_session_without_credentials(monkeypatch):
    received = []

    class Worker(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append((self.path, dict(self.headers), self.rfile.read(int(self.headers["Content-Length"]))))
            payload = b'{"evidence":[{"id":"deployment-1"}]}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Worker)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    name = "agentcore_proxy_test"
    runtime = _create_runtime(name)
    key = f"{get_account_id()}:{get_region()}:{name}"
    monkeypatch.setenv("MINISTACK_AGENTCORE_PROXY_URLS", json.dumps({
        key: f"http://127.0.0.1:{server.server_port}/invocations",
    }))
    try:
        status, headers, body = asyncio.run(bedrock_agentcore.handle_request(
            "POST", f"/runtimes/{runtime['agentRuntimeArn']}/invocations",
            {
                "content-type": "application/json",
                "x-amzn-bedrock-agentcore-runtime-session-id": "session-123",
                "authorization": "AWS4-HMAC-SHA256 secret",
            }, b'{"prompt":"why?"}', {},
        ))
        assert status == 200
        assert json.loads(body) == {"evidence": [{"id": "deployment-1"}]}
        assert headers["Content-Type"] == "application/json"
        assert headers["x-amzn-bedrock-agentcore-runtime-session-id"] == "session-123"
        path, forwarded_headers, payload = received[0]
        forwarded_headers = {key.lower(): value for key, value in forwarded_headers.items()}
        assert path == "/invocations"
        assert payload == b'{"prompt":"why?"}'
        assert forwarded_headers["x-amzn-bedrock-agentcore-runtime-session-id"] == "session-123"
        assert "authorization" not in forwarded_headers
    finally:
        bedrock_agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
        server.shutdown()
        server.server_close()
        thread.join()


def test_agentcore_proxy_failure_does_not_return_echo(monkeypatch):
    runtime = _create_runtime("agentcore_proxy_failure")
    key = f"{get_account_id()}:{get_region()}:agentcore_proxy_failure"
    server = ThreadingHTTPServer(("127.0.0.1", 0), BaseHTTPRequestHandler)
    port = server.server_port
    server.server_close()
    monkeypatch.setenv("MINISTACK_AGENTCORE_PROXY_URLS", json.dumps({
        key: f"http://127.0.0.1:{port}/invocations",
    }))
    try:
        status, _, body = asyncio.run(bedrock_agentcore.handle_request(
            "POST", f"/runtimes/{runtime['agentRuntimeArn']}/invocations",
            {"content-type": "application/json"}, b'{"prompt":"why?"}', {},
        ))
        assert status == 500
        assert json.loads(body)["__type"] == "InternalServerException"
    finally:
        bedrock_agentcore._delete_agent_runtime(runtime["agentRuntimeId"])


def test_agentcore_unconfigured_runtime_keeps_echo(monkeypatch):
    runtime = _create_runtime("agentcore_echo_test")
    monkeypatch.setenv("MINISTACK_AGENTCORE_PROXY_URLS", json.dumps({
        f"{get_account_id()}:{get_region()}:another_runtime": "http://127.0.0.1:1/invocations",
    }))
    try:
        status, _, body = asyncio.run(bedrock_agentcore.handle_request(
            "POST", f"/runtimes/{runtime['agentRuntimeArn']}/invocations",
            {"content-type": "application/json"}, b'{"prompt":"hello"}', {},
        ))
        assert status == 200
        assert json.loads(body) == {
            "agentRuntimeArn": runtime["agentRuntimeArn"], "input": {"prompt": "hello"},
        }
    finally:
        bedrock_agentcore._delete_agent_runtime(runtime["agentRuntimeId"])


def test_agentcore_invalid_proxy_configuration_fails_closed(monkeypatch):
    runtime = _create_runtime("agentcore_bad_config")
    monkeypatch.setenv("MINISTACK_AGENTCORE_PROXY_URLS", "[]")
    try:
        status, _, body = asyncio.run(bedrock_agentcore.handle_request(
            "POST", f"/runtimes/{runtime['agentRuntimeArn']}/invocations",
            {"content-type": "application/json"}, b"{}", {},
        ))
        assert status == 500
        assert json.loads(body)["__type"] == "InternalServerException"
    finally:
        bedrock_agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
