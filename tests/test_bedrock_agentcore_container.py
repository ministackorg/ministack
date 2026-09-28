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
        ),
    )
    monkeypatch.setitem(sys.modules, "docker", fake_docker)
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
