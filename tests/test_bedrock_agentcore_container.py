"""AgentCore container invocation contract, without a live AWS account."""

import asyncio
import json
import sys
import threading
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from ministack.core.responses import StreamingResponse
from ministack.services import bedrock_agentcore as agentcore


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


def _fake_docker(monkeypatch, port, *, fail=False, network=None):
    started = []
    removed = []

    class Container:
        status = "running"
        attrs = {"NetworkSettings": {
            "Ports": {"8080/tcp": [{"HostPort": str(port)}]},
            "Networks": {network: {"IPAddress": "127.0.0.1"}} if network else {},
        }}

        def reload(self):
            pass

        def remove(self, force=False):
            removed.append(force)

    class Containers:
        def get(self, _name):
            if not network:
                raise RuntimeError("MiniStack is not in Docker")
            return Container()

        def run(self, image, **kwargs):
            if fail:
                raise RuntimeError("image unavailable")
            started.append((image, kwargs))
            return Container()

    monkeypatch.setitem(sys.modules, "docker", types.SimpleNamespace(
        from_env=lambda **_kwargs: types.SimpleNamespace(containers=Containers())))
    monkeypatch.setenv("MINISTACK_AGENTCORE_DOCKER", "1")
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
