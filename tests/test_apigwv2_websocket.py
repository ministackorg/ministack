"""API Gateway v2 WebSocket — end-to-end tests.

Covers:
  - CreateApi(protocolType=WEBSOCKET) control-plane defaults
  - Route/Integration CRUD for WS API
  - RouteResponse / IntegrationResponse CRUD
  - Live $connect / $default / $disconnect dispatch via a Lambda
  - @connections runtime API: PostToConnection, GetConnection, DeleteConnection
  - Client isolation when two sockets connect to the same API
  - Accept/reject of $connect based on Lambda statusCode

Uses a hand-rolled WebSocket client (stdlib only) to keep the project
dependency-free.
"""

import base64
import hashlib
import io
import json
import os
import socket
import struct
import time
import uuid
import zipfile
from urllib.parse import urlparse

import pytest

_endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
_EXECUTE_PORT = urlparse(_endpoint).port or 4566
_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


# ── Minimal stdlib WebSocket client ──────────────────────────────────────────
class _WSClient:
    """Blocking WebSocket client — just enough to drive tests."""

    def __init__(self, host: str, port: int, path: str, headers: dict | None = None):
        self._sock = socket.create_connection((host, port), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        request_headers = {
            "Host": f"{host}:{port}",
            "Upgrade": "websocket",
            "Connection": "Upgrade",
            "Sec-WebSocket-Key": key,
            "Sec-WebSocket-Version": "13",
        }
        if headers:
            request_headers.update(headers)
        lines = [f"GET {path} HTTP/1.1"]
        for k, v in request_headers.items():
            lines.append(f"{k}: {v}")
        lines.append("")
        lines.append("")
        self._sock.sendall("\r\n".join(lines).encode())
        self._buf = b""
        self._read_handshake(key)

    def _read_handshake(self, key: str) -> None:
        while b"\r\n\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise RuntimeError(f"handshake closed, got: {self._buf!r}")
            self._buf += chunk
        header_blob, self._buf = self._buf.split(b"\r\n\r\n", 1)
        first_line = header_blob.split(b"\r\n", 1)[0]
        if b"101" not in first_line:
            raise RuntimeError(f"WS handshake failed: {header_blob!r}")
        expected = base64.b64encode(hashlib.sha1((key + _WS_GUID).encode()).digest()).decode()
        if expected.encode() not in header_blob:
            raise RuntimeError("Sec-WebSocket-Accept mismatch")

    def send(self, text: str) -> None:
        payload = text.encode()
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        length = len(payload)
        if length < 126:
            header = struct.pack("!BB", 0x81, 0x80 | length)
        elif length < 65536:
            header = struct.pack("!BBH", 0x81, 0x80 | 126, length)
        else:
            header = struct.pack("!BBQ", 0x81, 0x80 | 127, length)
        self._sock.sendall(header + mask + masked)

    def recv(self, timeout: float = 3.0) -> str | None:
        """Return the next text or binary frame's payload as a string."""
        self._sock.settimeout(timeout)
        try:
            while True:
                frame = self._recv_frame()
                if frame is None:
                    return None
                opcode, payload = frame
                if opcode in (0x1, 0x2):   # text or binary
                    return payload.decode("utf-8", errors="replace")
                if opcode == 0x8:   # close
                    return None
                # ignore ping/pong for test purposes
        except socket.timeout:
            return None

    def _recv_all(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self._sock.recv(max(4096, n - len(self._buf)))
            if not chunk:
                return b""
            self._buf += chunk
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def _recv_frame(self):
        hdr = self._recv_all(2)
        if len(hdr) < 2:
            return None
        b1, b2 = hdr[0], hdr[1]
        opcode = b1 & 0x0F
        masked = (b2 & 0x80) != 0
        length = b2 & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._recv_all(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._recv_all(8))[0]
        mask = self._recv_all(4) if masked else b""
        payload = self._recv_all(length)
        if masked:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return opcode, payload

    def close(self) -> None:
        try:
            # close frame (code 1000)
            self._sock.sendall(b"\x88\x82" + os.urandom(4) + b"\x03\xe8")
        except Exception:
            pass
        try:
            self._sock.close()
        except Exception:
            pass


# ── Fixtures / helpers ───────────────────────────────────────────────────────
_ECHO_CODE = """
import json

def handler(event, context):
    rc = event.get('requestContext', {})
    action = rc.get('routeKey', '$default')
    body_text = event.get('body', '')
    try:
        parsed = json.loads(body_text) if body_text else {}
    except Exception:
        parsed = {}
    # Echo the incoming frame with the connectionId for easy test assertions.
    resp = {
        'connectionId': rc.get('connectionId'),
        'eventType': rc.get('eventType'),
        'action': action,
        'body': parsed,
    }
    return {'statusCode': 200, 'body': json.dumps(resp)}
"""

_CONNECT_REJECT_CODE = """
def handler(event, context):
    # Force $connect rejection.
    return {'statusCode': 401, 'body': 'denied'}
"""


def _make_fn(lam, name: str, code: str) -> str:
    try:
        lam.delete_function(FunctionName=name)
    except Exception:
        pass
    lam.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role=_LAMBDA_ROLE,
        Handler="index.handler",
        Code={"ZipFile": _make_zip(code)},
    )
    return f"arn:aws:lambda:us-east-1:000000000000:function:{name}"


def _wire_ws_api(apigw, lam, *, name_suffix: str,
                 connect_code: str | None = None,
                 default_code: str = _ECHO_CODE,
                 disconnect_code: str | None = None) -> tuple[str, dict]:
    """Create a WS API + routes + integrations and return (apiId, metadata)."""
    api = apigw.create_api(Name=f"ws-{name_suffix}", ProtocolType="WEBSOCKET")
    api_id = api["ApiId"]
    meta = {"created_functions": []}
    assert api.get("RouteSelectionExpression") == "$request.body.action"

    def _route(route_key: str, code: str):
        fn_name = f"ws-{name_suffix}-{route_key.lstrip('$')}-{uuid.uuid4().hex[:6]}"
        arn = _make_fn(lam, fn_name, code)
        meta["created_functions"].append(fn_name)
        integ = apigw.create_integration(
            ApiId=api_id,
            IntegrationType="AWS_PROXY",
            IntegrationUri=arn,
            IntegrationMethod="POST",
        )
        apigw.create_route(
            ApiId=api_id,
            RouteKey=route_key,
            Target=f"integrations/{integ['IntegrationId']}",
        )

    if connect_code is not None:
        _route("$connect", connect_code)
    _route("$default", default_code)
    if disconnect_code is not None:
        _route("$disconnect", disconnect_code)
    apigw.create_stage(ApiId=api_id, StageName="prod")
    return api_id, meta


# ── Control-plane tests ──────────────────────────────────────────────────────
def test_ws_create_api_defaults(apigw):
    """WEBSOCKET APIs default routeSelectionExpression to $request.body.action."""
    resp = apigw.create_api(Name="ws-defaults", ProtocolType="WEBSOCKET")
    assert resp["ProtocolType"] == "WEBSOCKET"
    assert resp["RouteSelectionExpression"] == "$request.body.action"


def test_ws_create_api_custom_rse(apigw):
    resp = apigw.create_api(
        Name="ws-custom-rse", ProtocolType="WEBSOCKET",
        RouteSelectionExpression="$request.body.type",
    )
    assert resp["RouteSelectionExpression"] == "$request.body.type"


def test_ws_route_response_crud(apigw):
    api_id = apigw.create_api(Name="ws-rr", ProtocolType="WEBSOCKET")["ApiId"]
    route = apigw.create_route(ApiId=api_id, RouteKey="sendMessage")
    rr = apigw.create_route_response(
        ApiId=api_id, RouteId=route["RouteId"], RouteResponseKey="$default",
    )
    assert rr["RouteResponseKey"] == "$default"
    assert "RouteResponseId" in rr
    got = apigw.get_route_responses(ApiId=api_id, RouteId=route["RouteId"])
    assert any(i["RouteResponseId"] == rr["RouteResponseId"] for i in got["Items"])
    apigw.delete_route_response(
        ApiId=api_id, RouteId=route["RouteId"], RouteResponseId=rr["RouteResponseId"],
    )


def test_ws_integration_response_crud(apigw):
    api_id = apigw.create_api(Name="ws-ir", ProtocolType="WEBSOCKET")["ApiId"]
    integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="MOCK",
    )
    ir = apigw.create_integration_response(
        ApiId=api_id, IntegrationId=integ["IntegrationId"],
        IntegrationResponseKey="/200/",
    )
    assert ir["IntegrationResponseKey"] == "/200/"
    assert "IntegrationResponseId" in ir
    got = apigw.get_integration_responses(ApiId=api_id, IntegrationId=integ["IntegrationId"])
    assert any(i["IntegrationResponseId"] == ir["IntegrationResponseId"] for i in got["Items"])


# ── Data-plane tests ─────────────────────────────────────────────────────────
def test_ws_connect_and_echo_via_default_route(apigw, lam):
    api_id, meta = _wire_ws_api(
        apigw, lam, name_suffix="echo",
        connect_code=None, default_code=_ECHO_CODE,
    )
    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        ws.send(json.dumps({"action": "sendMessage", "payload": "hi"}))
        resp = ws.recv()
        assert resp is not None, "no reply from Lambda"
        parsed = json.loads(resp)
        assert parsed["eventType"] == "MESSAGE"
        assert parsed["body"]["payload"] == "hi"
        assert parsed["connectionId"]
    finally:
        ws.close()


def test_ws_connect_route_accepts(apigw, lam):
    """$connect Lambda returning 200 accepts the upgrade."""
    api_id, _ = _wire_ws_api(
        apigw, lam, name_suffix="connect-ok",
        connect_code=_ECHO_CODE,
    )
    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        ws.send(json.dumps({"action": "x"}))
        # Should get a normal MESSAGE response — proves the socket is live.
        resp = ws.recv()
        assert resp is not None
    finally:
        ws.close()


def test_ws_connect_route_rejects(apigw, lam):
    """$connect Lambda returning non-2xx rejects the upgrade."""
    api_id, _ = _wire_ws_api(
        apigw, lam, name_suffix="connect-deny",
        connect_code=_CONNECT_REJECT_CODE,
    )
    with pytest.raises(Exception):
        _WSClient("localhost", _EXECUTE_PORT, "/prod",
                  headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})


def test_ws_post_to_connection_from_management_api(apigw, lam):
    """@connections PostToConnection pushes a message to the live socket."""
    import urllib.request

    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="p2c")
    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        # Drive a frame so the Lambda runs and returns the connectionId in its reply.
        ws.send(json.dumps({"action": "sendMessage"}))
        reply = ws.recv()
        conn_id = json.loads(reply)["connectionId"]
        assert conn_id

        # Push a message from a separate HTTP request (simulating server-side push).
        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/@connections/{conn_id}"
        req = urllib.request.Request(
            url, data=b"server-push-payload", method="POST",
            headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
        )
        r = urllib.request.urlopen(req, timeout=5)
        assert r.status == 200

        pushed = ws.recv(timeout=3)
        assert pushed == "server-push-payload"
    finally:
        ws.close()


def test_ws_get_connection_returns_metadata(apigw, lam):
    """@connections GetConnection returns connected-at / identity."""
    import urllib.request

    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="getc")
    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        ws.send(json.dumps({"action": "x"}))
        reply = ws.recv()
        conn_id = json.loads(reply)["connectionId"]

        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/@connections/{conn_id}"
        req = urllib.request.Request(
            url, method="GET",
            headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
        )
        r = urllib.request.urlopen(req, timeout=5)
        meta = json.loads(r.read())
        # Int epoch seconds, per ministack JSON timestamp convention.
        assert isinstance(meta["ConnectedAt"], int)
        assert isinstance(meta["LastActiveAt"], int)
        assert meta["Identity"]["sourceIp"]
    finally:
        ws.close()


def test_ws_delete_connection_closes_socket(apigw, lam):
    """@connections DeleteConnection terminates the WS session."""
    import urllib.request

    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="delc")
    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        ws.send(json.dumps({"action": "x"}))
        reply = ws.recv()
        conn_id = json.loads(reply)["connectionId"]

        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/@connections/{conn_id}"
        req = urllib.request.Request(
            url, method="DELETE",
            headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
        )
        r = urllib.request.urlopen(req, timeout=5)
        assert r.status in (200, 204)

        # Give the server a moment to close, then subsequent recv returns None.
        time.sleep(0.5)
        assert ws.recv(timeout=1.5) is None
    finally:
        ws.close()


def test_ws_post_to_unknown_connection_returns_410(apigw, lam):
    """@connections PostToConnection on an unknown id returns 410 GoneException."""
    import urllib.request
    import urllib.error

    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="gone")
    url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/@connections/{uuid.uuid4().hex}"
    req = urllib.request.Request(
        url, data=b"hi", method="POST",
        headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
    )
    with pytest.raises(urllib.error.HTTPError) as exc_info:
        urllib.request.urlopen(req, timeout=5)
    assert exc_info.value.code == 410


_CAPTURE_QS_CODE = """
import json

def handler(event, context):
    # Echo the $connect event's queryStringParameters so the test can assert on them.
    rc = event.get('requestContext', {})
    return {
        'statusCode': 200,
        'body': json.dumps({
            'qs': event.get('queryStringParameters'),
            'mvqs': event.get('multiValueQueryStringParameters'),
            'eventType': rc.get('eventType'),
        }),
    }
"""


def test_ws_connect_receives_query_string_parameters(apigw, lam):
    """$connect Lambda event exposes queryStringParameters + multiValueQueryStringParameters.

    After accepting, we send a frame and rely on the echo Lambda to confirm the
    socket is live; the test's primary assertion is that the $connect Lambda
    did NOT reject us (so QS params didn't break event validation).
    """
    # Two Lambdas: one on $connect that validates the QS param; one on $default that echoes.
    api_id = apigw.create_api(Name="ws-qs-gate", ProtocolType="WEBSOCKET")["ApiId"]

    gate_code = """
def handler(event, context):
    qs = event.get('queryStringParameters') or {}
    mvqs = event.get('multiValueQueryStringParameters') or {}
    # Reject unless the caller passed ?token=abc
    if qs.get('token') != 'abc':
        return {'statusCode': 401, 'body': 'denied'}
    # Also confirm multi-value came through when a key is repeated.
    if mvqs.get('tag') != ['a', 'b']:
        return {'statusCode': 401, 'body': 'mv missing'}
    return {'statusCode': 200}
"""
    gate_arn = _make_fn(lam, f"ws-qs-gate-connect-{uuid.uuid4().hex[:6]}", gate_code)
    echo_arn = _make_fn(lam, f"ws-qs-gate-default-{uuid.uuid4().hex[:6]}", _ECHO_CODE)

    gate_integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=gate_arn, IntegrationMethod="POST",
    )
    apigw.create_route(ApiId=api_id, RouteKey="$connect",
                       Target=f"integrations/{gate_integ['IntegrationId']}")

    echo_integ = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=echo_arn, IntegrationMethod="POST",
    )
    apigw.create_route(ApiId=api_id, RouteKey="$default",
                       Target=f"integrations/{echo_integ['IntegrationId']}")
    apigw.create_stage(ApiId=api_id, StageName="prod")

    # Without QS params → $connect rejects
    with pytest.raises(Exception):
        _WSClient("localhost", _EXECUTE_PORT, "/prod",
                  headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})

    # With ?token=abc&tag=a&tag=b → accepted, MESSAGE works.
    ws = _WSClient(
        "localhost", _EXECUTE_PORT, "/prod?token=abc&tag=a&tag=b",
        headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
    )
    try:
        ws.send(json.dumps({"action": "ping"}))
        resp = ws.recv()
        assert resp is not None
        assert json.loads(resp)["eventType"] == "MESSAGE"
    finally:
        ws.close()


def test_ws_mock_integration_returns_template_body(apigw):
    """WEBSOCKET routes with MOCK integration + responseTemplates return the template
    body on the socket without any Lambda invocation."""
    api_id = apigw.create_api(Name="ws-mock", ProtocolType="WEBSOCKET")["ApiId"]
    integ = apigw.create_integration(ApiId=api_id, IntegrationType="MOCK")
    apigw.create_route(ApiId=api_id, RouteKey="$default",
                       Target=f"integrations/{integ['IntegrationId']}")
    apigw.create_integration_response(
        ApiId=api_id, IntegrationId=integ["IntegrationId"],
        IntegrationResponseKey="$default",
        ResponseTemplates={"$default": '{"from":"mock"}'},
    )
    apigw.create_stage(ApiId=api_id, StageName="prod")

    ws = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                   headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        ws.send(json.dumps({"action": "anything"}))
        resp = ws.recv()
        assert resp == '{"from":"mock"}'
    finally:
        ws.close()


def test_ws_two_clients_stay_isolated(apigw, lam):
    """Two WS sockets on the same API get distinct connectionIds and
    @connections messages don't cross-deliver."""
    import urllib.request

    api_id, _ = _wire_ws_api(apigw, lam, name_suffix="iso")
    a = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                  headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    b = _WSClient("localhost", _EXECUTE_PORT, "/prod",
                  headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"})
    try:
        a.send(json.dumps({"action": "x"}))
        b.send(json.dumps({"action": "y"}))
        a_reply = json.loads(a.recv())
        b_reply = json.loads(b.recv())
        assert a_reply["connectionId"] != b_reply["connectionId"]

        # Push to A only — B should not receive it.
        url = f"http://{api_id}.execute-api.localhost:{_EXECUTE_PORT}/prod/@connections/{a_reply['connectionId']}"
        urllib.request.urlopen(urllib.request.Request(
            url, data=b"for-a-only", method="POST",
            headers={"Host": f"{api_id}.execute-api.localhost:{_EXECUTE_PORT}"},
        ), timeout=5)

        got_a = a.recv(timeout=3)
        got_b = b.recv(timeout=1)
        assert got_a == "for-a-only"
        assert got_b is None
    finally:
        a.close()
        b.close()
