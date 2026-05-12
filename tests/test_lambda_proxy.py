"""BYO-container Lambda proxy: forward Invoke to a user-managed HTTP server.

The proxy URL is configured per-function via Environment.Variables.MINISTACK_LAMBDA_PROXY_URL,
which means tests don't need to restart the server — they create a function with
the proxy URL in its environment and invoke it.
"""
import io
import json
import os
import threading
import time
import zipfile
from http.server import BaseHTTPRequestHandler, HTTPServer

import boto3
import pytest

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _make_zip(code: str = "def handler(event,context):\n    return event\n") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


class _ProxyHandler(BaseHTTPRequestHandler):
    received: list[dict] = []
    response: tuple[int, bytes, dict] = (200, b'{"ok":true}', {"Content-Type": "application/json"})
    sleep_seconds: float = 0.0

    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length) if length else b""
        type(self).received.append({
            "path": self.path,
            "headers": {k: v for k, v in self.headers.items()},
            "body": body,
        })
        if type(self).sleep_seconds:
            time.sleep(type(self).sleep_seconds)
        status, payload, headers = type(self).response
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_args, **_kwargs):
        pass


@pytest.fixture
def proxy_server():
    server = HTTPServer(("127.0.0.1", 0), _ProxyHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _ProxyHandler.received = []
    _ProxyHandler.response = (200, b'{"ok":true}', {"Content-Type": "application/json"})
    _ProxyHandler.sleep_seconds = 0.0
    try:
        yield port
    finally:
        server.shutdown()


def _make_client():
    return boto3.client(
        "lambda",
        endpoint_url=_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _create_proxy_function(client, name: str, url: str | None):
    env = {"Variables": {"MINISTACK_LAMBDA_PROXY_URL": url}} if url else None
    kwargs = dict(
        FunctionName=name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/x",
        Handler="index.handler",
        Code={"ZipFile": _make_zip()},
    )
    if env is not None:
        kwargs["Environment"] = env
    client.create_function(**kwargs)


def test_proxy_invoke_forwards_event_and_returns_response(proxy_server):
    name = "proxy_echo"
    client = _make_client()
    _create_proxy_function(client, name, f"http://127.0.0.1:{proxy_server}/invoke")

    event = {"hello": "world", "n": 42}
    resp = client.invoke(FunctionName=name, Payload=json.dumps(event).encode())
    body = json.loads(resp["Payload"].read())

    assert body == {"ok": True}
    assert _ProxyHandler.received, "container never received the invocation"
    forwarded = _ProxyHandler.received[-1]
    assert forwarded["path"] == "/invoke"
    assert json.loads(forwarded["body"]) == event
    h = forwarded["headers"]
    assert h.get("X-Amzn-Lambda-Function-Name") == name
    assert h.get("X-Amzn-Lambda-Request-Id")
    assert h.get("X-Amzn-Lambda-Function-Arn", "").endswith(f":function:{name}")


def test_proxy_invoke_passes_through_response_payload(proxy_server):
    name = "proxy_payload"
    client = _make_client()
    _create_proxy_function(client, name, f"http://127.0.0.1:{proxy_server}/invoke")
    _ProxyHandler.response = (200, b'{"statusCode":201,"body":"created"}', {"Content-Type": "application/json"})

    resp = client.invoke(FunctionName=name, Payload=b"{}")
    body = json.loads(resp["Payload"].read())
    assert body == {"statusCode": 201, "body": "created"}


def test_proxy_invoke_non_2xx_returns_lambda_error_shape(proxy_server):
    name = "proxy_500"
    client = _make_client()
    _create_proxy_function(client, name, f"http://127.0.0.1:{proxy_server}/invoke")
    _ProxyHandler.response = (500, b"boom", {"Content-Type": "text/plain"})

    resp = client.invoke(FunctionName=name, Payload=b"{}")
    assert resp.get("FunctionError") == "Unhandled"
    body = json.loads(resp["Payload"].read())
    assert body.get("errorType") == "Runtime.HandlerError"
    assert "HTTP 500" in body.get("errorMessage", "")


def test_proxy_invoke_unreachable_returns_lambda_error_shape():
    name = "proxy_unreachable"
    client = _make_client()
    _create_proxy_function(client, name, "http://127.0.0.1:1/invoke")

    resp = client.invoke(FunctionName=name, Payload=b"{}")
    assert resp.get("FunctionError") == "Unhandled"
    body = json.loads(resp["Payload"].read())
    assert body.get("errorType") in ("Runtime.HandlerError", "Sandbox.Timedout")
    assert "errorMessage" in body


def test_proxy_unset_falls_back_to_normal_executor():
    name = "proxy_unset"
    client = _make_client()
    _create_proxy_function(client, name, None)
    resp = client.invoke(FunctionName=name, Payload=b'{"a":1}')
    body = json.loads(resp["Payload"].read())
    assert body == {"a": 1}


def test_proxy_via_apigw_aws_proxy_integration(proxy_server):
    """API Gateway HTTP API routes through to a proxy Lambda's container and returns its response."""
    import urllib.request as _urlreq
    from urllib.parse import urlparse

    name = "proxy_apigw_fn"
    lam = _make_client()
    _create_proxy_function(lam, name, f"http://127.0.0.1:{proxy_server}/invoke")

    # Container replies with a Lambda Proxy response shape, which APIGW unwraps.
    _ProxyHandler.response = (
        200,
        b'{"statusCode":200,"headers":{"Content-Type":"application/json"},"body":"{\\"hi\\":\\"from-php\\"}"}',
        {"Content-Type": "application/json"},
    )

    apigw = boto3.client(
        "apigatewayv2",
        endpoint_url=_endpoint,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    api_id = apigw.create_api(Name=f"proxy-api-{name}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id,
        IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:{name}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="GET /hello", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")

    port = urlparse(_endpoint).port or 4566
    url = f"http://{api_id}.execute-api.localhost:{port}/$default/hello"
    req = _urlreq.Request(url, method="GET")
    req.add_header("Host", f"{api_id}.execute-api.localhost:{port}")
    resp = _urlreq.urlopen(req)
    assert resp.status == 200
    assert json.loads(resp.read()) == {"hi": "from-php"}

    # The container should have received an APIGW v2 event JSON, not a raw HTTP request.
    forwarded = json.loads(_ProxyHandler.received[-1]["body"])
    assert forwarded.get("rawPath") == "/hello"
    assert forwarded.get("requestContext", {}).get("http", {}).get("method") == "GET"
