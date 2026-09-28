"""Opt-in AgentCore HTTP proxy tests; no AWS account or model is involved."""

import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import boto3

from ministack.core.responses import StreamingResponse, get_account_id, get_region
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


async def _collect_stream(stream):
    messages = []

    async def send(message):
        messages.append(message)

    await stream.runner(send, None)
    return b"".join(message["body"] for message in messages), messages


def test_agentcore_proxy_forwards_payload_and_session_without_credentials(monkeypatch):
    received = []

    class Worker(BaseHTTPRequestHandler):
        def do_POST(self):
            received.append((self.path, dict(self.headers), self.rfile.read(int(self.headers["Content-Length"]))))
            payload = b'{"evidence":[{"id":"deployment-1"}]}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("traceparent", "00-response-trace-01")
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
                "accept": "application/json",
                "traceparent": "00-request-trace-01",
                "x-amzn-bedrock-agentcore-runtime-session-id": "session-123",
                "authorization": "AWS4-HMAC-SHA256 secret",
            }, b'{"prompt":"why?"}', {},
        ))
        assert status == 200
        assert isinstance(body, StreamingResponse)
        streamed, messages = asyncio.run(_collect_stream(body))
        assert json.loads(streamed) == {"evidence": [{"id": "deployment-1"}]}
        assert messages[-1]["more_body"] is False
        assert headers["Content-Type"] == "application/json"
        assert headers["traceparent"] == "00-response-trace-01"
        assert headers["x-amzn-bedrock-agentcore-runtime-session-id"] == "session-123"
        path, forwarded_headers, payload = received[0]
        forwarded_headers = {key.lower(): value for key, value in forwarded_headers.items()}
        assert path == "/invocations"
        assert payload == b'{"prompt":"why?"}'
        assert forwarded_headers["x-amzn-bedrock-agentcore-runtime-session-id"] == "session-123"
        assert forwarded_headers["accept"] == "application/json"
        assert forwarded_headers["traceparent"] == "00-request-trace-01"
        assert "authorization" not in forwarded_headers
    finally:
        bedrock_agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
        server.shutdown()
        server.server_close()
        thread.join()


def test_agentcore_worker_http_error_maps_to_runtime_client_error(monkeypatch):
    class Worker(BaseHTTPRequestHandler):
        def do_POST(self):
            self.send_error(400, "bad request")

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Worker)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    runtime = _create_runtime("agentcore_worker_error")
    key = f"{get_account_id()}:{get_region()}:agentcore_worker_error"
    monkeypatch.setenv("MINISTACK_AGENTCORE_PROXY_URLS", json.dumps({
        key: f"http://127.0.0.1:{server.server_port}/invocations",
    }))
    try:
        status, headers, body = asyncio.run(bedrock_agentcore.handle_request(
            "POST", f"/runtimes/{runtime['agentRuntimeArn']}/invocations",
            {"content-type": "application/json"}, b"{}", {},
        ))
        assert status == 424
        assert headers["x-amzn-errortype"] == "RuntimeClientError"
        assert json.loads(body)["__type"] == "RuntimeClientError"
    finally:
        bedrock_agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
        server.shutdown()
        server.server_close()
        thread.join()


def test_agentcore_proxy_streams_before_worker_finishes(monkeypatch):
    release_second_chunk = threading.Event()

    class Worker(BaseHTTPRequestHandler):
        def do_POST(self):
            self.rfile.read(int(self.headers["Content-Length"]))
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(b"first\n")
            self.wfile.flush()
            if release_second_chunk.wait(timeout=5):
                self.wfile.write(b"second\n")
                self.wfile.flush()

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Worker)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    runtime = _create_runtime("agentcore_stream_test")
    key = f"{get_account_id()}:{get_region()}:agentcore_stream_test"
    monkeypatch.setenv("MINISTACK_AGENTCORE_PROXY_URLS", json.dumps({
        key: f"http://127.0.0.1:{server.server_port}/invocations",
    }))
    try:
        status, _, body = asyncio.run(bedrock_agentcore.handle_request(
            "POST", f"/runtimes/{runtime['agentRuntimeArn']}/invocations",
            {"content-type": "application/json", "accept": "text/event-stream"}, b"{}", {},
        ))
        assert status == 200
        assert isinstance(body, StreamingResponse)
        frames = []

        async def send(message):
            frames.append(message)
            if message["body"] == b"first\n":
                release_second_chunk.set()

        asyncio.run(body.runner(send, None))
        assert [frame["body"] for frame in frames] == [b"first\n", b"second\n", b""]
        assert frames[-1]["more_body"] is False
    finally:
        release_second_chunk.set()
        bedrock_agentcore._delete_agent_runtime(runtime["agentRuntimeId"])
        server.shutdown()
        server.server_close()
        thread.join()


def test_agentcore_proxy_through_boto3():
    class Worker(BaseHTTPRequestHandler):
        def do_POST(self):
            assert self.path == "/invocations"
            assert json.loads(self.rfile.read(int(self.headers["Content-Length"]))) == {"prompt": "hello"}
            payload = b'{"source":"local-worker"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            pass

    worker = ThreadingHTTPServer(("127.0.0.1", 0), Worker)
    worker_thread = threading.Thread(target=worker.serve_forever, daemon=True)
    worker_thread.start()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    endpoint = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env["IOT_MTLS_ENABLED"] = "0"
    env["MINISTACK_AGENTCORE_PROXY_URLS"] = json.dumps({
        "000000000000:us-east-1:agentcore_boto3_proxy":
            f"http://127.0.0.1:{worker.server_port}/invocations",
    })
    ministack = subprocess.Popen(
        [sys.executable, "-m", "hypercorn", "ministack.app:app", "--bind", f"127.0.0.1:{port}"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        for _ in range(100):
            if ministack.poll() is not None:
                raise AssertionError("MiniStack server exited before becoming healthy")
            try:
                urllib.request.urlopen(f"{endpoint}/_ministack/health", timeout=1).close()
                break
            except (urllib.error.URLError, TimeoutError):
                time.sleep(0.1)
        else:
            raise AssertionError("MiniStack server did not become healthy")

        client_args = dict(
            endpoint_url=endpoint, region_name="us-east-1",
            aws_access_key_id="test", aws_secret_access_key="test",
        )
        control = boto3.client("bedrock-agentcore-control", **client_args)
        runtime = boto3.client("bedrock-agentcore", **client_args)
        created = control.create_agent_runtime(
            agentRuntimeName="agentcore_boto3_proxy",
            agentRuntimeArtifact={"containerConfiguration": {"containerUri": "example.invalid/agent"}},
            roleArn="arn:aws:iam::000000000000:role/agentcore",
            networkConfiguration={"networkMode": "PUBLIC"},
        )
        try:
            result = runtime.invoke_agent_runtime(
                agentRuntimeArn=created["agentRuntimeArn"], payload=b'{"prompt":"hello"}',
            )
            assert result["contentType"] == "application/json"
            assert json.loads(result["response"].read()) == {"source": "local-worker"}
        finally:
            control.delete_agent_runtime(agentRuntimeId=created["agentRuntimeId"])
    finally:
        ministack.terminate()
        try:
            ministack.wait(timeout=5)
        except subprocess.TimeoutExpired:
            ministack.kill()
            ministack.wait()
        worker.shutdown()
        worker.server_close()
        worker_thread.join()


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
