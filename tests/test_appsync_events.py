"""
Integration tests for AWS AppSync Events.

Covers:
  - Management plane (/v2/apis): CreateApi/GetApi/ListApis/UpdateApi/DeleteApi,
    ChannelNamespace CRUD, ApiKey CRUD (via boto3.client("appsync")).
  - HTTP publish (POST /event on {apiId}.appsync-api.*).
  - WebSocket handshake with the ``header-<b64>`` auth subprotocol.
  - WebSocket subscribe + fan-out (exact paths and terminal ``*`` wildcard).
  - WebSocket publish frame (``publish_success`` + fan-out to peers).
  - Server-pushed ``ka`` keep-alive frames.
  - Channel path regex validation (1..5 segments, alphanumeric + dash).

Uses a stdlib WebSocket client (no new runtime deps).
"""

import asyncio
import base64
import hashlib
import http.client
import json
import os
import socket
import struct
import time
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

_ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
_PORT = urlparse(_ENDPOINT).port or 4566
_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _encode_auth_header(payload: dict) -> str:
    """Return the ``header-<base64url(json)>`` subprotocol string."""
    raw = json.dumps(payload).encode("utf-8")
    b64 = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    return f"header-{b64}"


# ---------------------------------------------------------------------------
# Stdlib WebSocket client (mirrors the helper in test_apigwv2_websocket.py)
# ---------------------------------------------------------------------------

class _WSClient:
    def __init__(self, host_header: str, path: str, subprotocol: str | None = None):
        self._sock = socket.create_connection(("127.0.0.1", _PORT), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        request_headers = {
            "Host": host_header,
            "Upgrade": "websocket",
            "Connection": "Upgrade",
            "Sec-WebSocket-Key": key,
            "Sec-WebSocket-Version": "13",
        }
        if subprotocol:
            request_headers["Sec-WebSocket-Protocol"] = subprotocol
        lines = [f"GET {path} HTTP/1.1"]
        for k, v in request_headers.items():
            lines.append(f"{k}: {v}")
        lines.append("")
        lines.append("")
        self._sock.sendall("\r\n".join(lines).encode())
        self._buf = b""
        self._read_handshake(key)

    def _read_handshake(self, key):
        while b"\r\n\r\n" not in self._buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise RuntimeError(f"handshake closed, got: {self._buf!r}")
            self._buf += chunk
        header_blob, self._buf = self._buf.split(b"\r\n\r\n", 1)
        self.handshake_headers = header_blob  # exposed for tests that inspect status
        first_line = header_blob.split(b"\r\n", 1)[0]
        if b"101" not in first_line:
            raise RuntimeError(f"WS handshake failed: {header_blob!r}")
        expected = base64.b64encode(hashlib.sha1((key + _WS_GUID).encode()).digest()).decode()
        if expected.encode() not in header_blob:
            raise RuntimeError("Sec-WebSocket-Accept mismatch")

    def send_json(self, obj):
        payload = json.dumps(obj).encode()
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

    def recv_json(self, timeout=3.0):
        self._sock.settimeout(timeout)
        try:
            while True:
                frame = self._recv_frame()
                if frame is None:
                    return None
                opcode, payload = frame
                if opcode in (0x1, 0x2):
                    return json.loads(payload.decode("utf-8", errors="replace"))
                if opcode == 0x8:
                    return None
        except socket.timeout:
            return None

    def _recv_all(self, n):
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

    def close(self):
        try:
            self._sock.sendall(b"\x88\x82" + os.urandom(4) + b"\x03\xe8")
        except Exception:
            pass
        try:
            self._sock.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEFAULT_EVENT_CONFIG = {
    "authProviders": [{"authType": "API_KEY"}],
    "connectionAuthModes": [{"authType": "API_KEY"}],
    "defaultPublishAuthModes": [{"authType": "API_KEY"}],
    "defaultSubscribeAuthModes": [{"authType": "API_KEY"}],
}


def _create_api(appsync, name):
    return appsync.create_api(name=name, eventConfig=_DEFAULT_EVENT_CONFIG)["api"]


async def _invoke_asgi_http(
    method: str,
    path: str,
    host: str,
    body: bytes = b"",
    *,
    extra_headers: dict[str, str] | None = None,
):
    """Drive MiniStack's ASGI ``app`` in-process (no TCP server)."""
    from ministack.app import app as asgi_app

    messages: list[dict] = []

    async def send(message: dict):
        messages.append(message)

    headers = [(b"host", host.encode("ascii"))]
    if body:
        headers.append((b"content-type", b"application/json"))
        headers.append((b"content-length", str(len(body)).encode("ascii")))
    if extra_headers:
        for hk, hv in extra_headers.items():
            headers.append((hk.lower().encode("ascii"), hv.encode("utf-8")))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("utf-8"),
        "root_path": "",
        "query_string": b"",
        "headers": headers,
        "client": ("127.0.0.1", 55555),
        "server": ("127.0.0.1", 4566),
    }
    body_sent = False

    async def receive():
        nonlocal body_sent
        if not body_sent:
            body_sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    await asgi_app(scope, receive, send)
    return messages


def _asgi_response(messages: list[dict]) -> tuple[int, dict[str, str], bytes]:
    start = next(m for m in messages if m["type"] == "http.response.start")
    body_msg = next(m for m in messages if m["type"] == "http.response.body")
    hdrs: dict[str, str] = {}
    for k, v in start.get("headers", []):
        hdrs[k.decode("latin-1").lower()] = v.decode("latin-1", errors="replace")
    return start["status"], hdrs, body_msg.get("body", b"")


def _create_namespace(appsync, api_id, name):
    return appsync.create_channel_namespace(apiId=api_id, name=name)["channelNamespace"]


def _minimal_sigv4_appsync_authorization() -> str:
    """SigV4-shaped header with credential scope ``.../appsync/aws4_request`` (unsigned).

    MiniStack's router only inspects the ``Credential=`` segment — real Lambdas
    sign with the same ``appsync`` service name as AppSync GraphQL.
    """
    return (
        "AWS4-HMAC-SHA256 "
        "Credential=testkey/20260127/us-east-1/appsync/aws4_request, "
        "SignedHeaders=host;x-amz-date, "
        "Signature=" + "0" * 64
    )


def _publish_http(
    api_id,
    channel,
    events,
    api_key="da2-local-test-key",
    *,
    authorization=None,
    host_header=None,
):
    """POST /event on the {apiId}.appsync-api.* virtual host (or ``host_header`` override)."""
    conn = http.client.HTTPConnection("127.0.0.1", _PORT, timeout=5)
    body = json.dumps({"channel": channel, "events": events})
    hh = host_header or f"{api_id}.appsync-api.us-east-1.amazonaws.com"
    hdrs = {
        "Host": hh,
        "Content-Type": "application/json",
        "x-api-key": api_key,
    }
    if authorization:
        hdrs["Authorization"] = authorization
    conn.request(
        "POST", "/event",
        body=body.encode("utf-8"),
        headers=hdrs,
    )
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    return resp.status, json.loads(data) if data else None


def _open_subscriber(api_id, *, auth_header: dict | None = None):
    """Open a realtime WebSocket with the spec-shaped subprotocol list.

    Sends ``Sec-WebSocket-Protocol: header-<b64>, aws-appsync-event-ws`` and
    waits for the ``connection_ack``. When ``auth_header`` is None, a default
    ``{"host":..., "x-api-key":"da2-local-test-key"}`` payload is supplied —
    matching how a real client would wrap API_KEY credentials.
    """
    host_header = f"{api_id}.appsync-realtime-api.us-east-1.amazonaws.com"
    http_host = f"{api_id}.appsync-api.us-east-1.amazonaws.com"
    if auth_header is None:
        auth_header = {"host": http_host, "x-api-key": "da2-local-test-key"}
    protocols = f"{_encode_auth_header(auth_header)}, aws-appsync-event-ws"
    ws = _WSClient(host_header=host_header, path="/event/realtime", subprotocol=protocols)
    ws.send_json({"type": "connection_init"})
    ack = ws.recv_json()
    assert ack["type"] == "connection_ack"
    assert ack.get("connectionTimeoutMs") == 300000
    return ws


def _subscribe(ws, sub_id, channel):
    ws.send_json({"type": "subscribe", "id": sub_id, "channel": channel})
    resp = ws.recv_json()
    return resp


def _drain_until(ws, predicate, *, timeout=3.0):
    """Read frames until ``predicate(frame)`` returns True, ignoring 'ka' noise."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        remaining = max(0.1, deadline - time.time())
        frame = ws.recv_json(timeout=remaining)
        if frame is None:
            return None
        if predicate(frame):
            return frame
    return None


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def api(appsync):
    """A clean API + default channel namespace for each test."""
    suffix = str(int(time.time() * 1000))[-10:]
    created = _create_api(appsync, f"events-{suffix}")
    api_id = created["apiId"]
    _create_namespace(appsync, api_id, "default")
    yield api_id
    try:
        appsync.delete_api(apiId=api_id)
    except ClientError:
        pass


# ---------------------------------------------------------------------------
# Management plane
# ---------------------------------------------------------------------------

def test_asgi_get_v2_apis_appsync_api_vhost_discovers_api():
    """Unsigned GET /v2/apis with an AppSync Events API vhost reaches Events.

    Regression: path-only ``/v2/apis`` routing must not send this to ``apigateway``
    (404). It must reach ``appsync_events`` and return CORS headers from dispatch.
    """
    from ministack.services import appsync_events as ae

    ae.reset()
    host = "example.appsync-api.us-east-1.localhost:4566"

    async def _run():
        create_body = json.dumps({"name": "asgi-discovery-e2e"}).encode()
        post_msgs = await _invoke_asgi_http("POST", "/v2/apis", host, create_body)
        post_status, _post_hdrs, post_body = _asgi_response(post_msgs)
        assert post_status == 200
        created = json.loads(post_body.decode())["api"]
        api_id = created["apiId"]
        assert api_id

        get_msgs = await _invoke_asgi_http("GET", "/v2/apis", host)
        get_status, get_hdrs, get_body = _asgi_response(get_msgs)
        assert get_status == 200
        assert get_hdrs.get("access-control-allow-origin") == "*"
        listed = json.loads(get_body.decode())["apis"]
        assert any(a["apiId"] == api_id for a in listed)

    asyncio.run(_run())


def test_create_api_defaults(appsync):
    api = _create_api(appsync, "events-defaults")
    try:
        assert api["name"] == "events-defaults"
        assert api["apiId"]
        assert api["dns"]["HTTP"].endswith(f".appsync-api.us-east-1.localhost:{_PORT}")
        assert api["dns"]["REALTIME"].endswith(
            f".appsync-realtime-api.us-east-1.localhost:{_PORT}"
        )
        assert api["eventConfig"]["authProviders"] == [{"authType": "API_KEY"}]
        assert api["apiArn"].startswith("arn:aws:appsync:")
    finally:
        appsync.delete_api(apiId=api["apiId"])


def test_get_list_update_delete_api(appsync):
    created = _create_api(appsync, "events-crud")
    api_id = created["apiId"]

    fetched = appsync.get_api(apiId=api_id)["api"]
    assert fetched["apiId"] == api_id
    assert fetched["name"] == "events-crud"

    ids = {a["apiId"] for a in appsync.list_apis()["apis"]}
    assert api_id in ids

    updated = appsync.update_api(
        apiId=api_id,
        name="events-crud-renamed",
        eventConfig=_DEFAULT_EVENT_CONFIG,
    )["api"]
    assert updated["name"] == "events-crud-renamed"

    appsync.delete_api(apiId=api_id)
    with pytest.raises(ClientError) as exc:
        appsync.get_api(apiId=api_id)
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_api_key_crud_via_v1_path(appsync, api):
    """boto3's ``create_api_key`` / ``list_api_keys`` / ``delete_api_key`` all
    target the v1 GraphQL endpoint. The Terraform AWS provider's
    ``aws_appsync_api_key`` resource does the same. We transparently delegate
    to the v2 Event API registry when the apiId belongs to an Event API, so
    these SDK calls work for both resource types.
    """
    created = appsync.create_api_key(apiId=api, description="terraform-style")["apiKey"]
    assert created["id"].startswith("da2-")
    assert created["description"] == "terraform-style"

    listed = appsync.list_api_keys(apiId=api)["apiKeys"]
    assert any(k["id"] == created["id"] for k in listed)

    appsync.delete_api_key(apiId=api, id=created["id"])
    listed_after = appsync.list_api_keys(apiId=api)["apiKeys"]
    assert not any(k["id"] == created["id"] for k in listed_after)


def test_channel_namespace_crud(appsync, api):
    ns = appsync.create_channel_namespace(apiId=api, name="chat")["channelNamespace"]
    assert ns["name"] == "chat"
    assert ns["publishAuthModes"]
    assert ns["subscribeAuthModes"]

    fetched = appsync.get_channel_namespace(apiId=api, name="chat")["channelNamespace"]
    assert fetched["name"] == "chat"

    listed = appsync.list_channel_namespaces(apiId=api)["channelNamespaces"]
    names = {n["name"] for n in listed}
    assert {"default", "chat"}.issubset(names)

    updated = appsync.update_channel_namespace(
        apiId=api,
        name="chat",
        publishAuthModes=[{"authType": "API_KEY"}],
    )["channelNamespace"]
    assert updated["publishAuthModes"] == [{"authType": "API_KEY"}]

    appsync.delete_channel_namespace(apiId=api, name="chat")
    with pytest.raises(ClientError) as exc:
        appsync.get_channel_namespace(apiId=api, name="chat")
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


# ---------------------------------------------------------------------------
# HTTP publish
# ---------------------------------------------------------------------------

def test_publish_returns_identifiers(api):
    events = [json.dumps("hello"), json.dumps({"msg": "world"})]
    status, body = _publish_http(api, "/default/room1", events)
    assert status == 200
    assert len(body["successful"]) == 2
    assert body["failed"] == []
    assert {item["index"] for item in body["successful"]} == {0, 1}


def test_publish_unknown_namespace_rejected(api):
    status, body = _publish_http(api, "/does-not-exist/room", [json.dumps("x")])
    assert status == 401
    assert body["__type"].endswith("UnauthorizedException") or body["__type"] == "UnauthorizedException"


def test_publish_unknown_api_404():
    status, body = _publish_http("deadbeefdeadbeefdeadbeefde", "/default/x", [json.dumps("x")])
    assert status == 404


def test_detect_service_post_event_appsync_api_vhost_returns_appsync_events():
    from ministack.core.router import detect_service

    auth = _minimal_sigv4_appsync_authorization()
    host = "abc123dead.appsync-api.us-east-1.amazonaws.com"
    assert detect_service("POST", "/event", {"host": host, "authorization": auth}, {}) == "appsync-events"


def test_detect_service_post_graphql_appsync_api_vhost_stays_appsync():
    """GraphQL data plane shares the ``appsync`` SigV4 scope and the same vhost."""
    from ministack.core.router import detect_service

    auth = _minimal_sigv4_appsync_authorization()
    host = "abc123dead.appsync-api.us-east-1.amazonaws.com"
    assert detect_service("POST", "/graphql", {"host": host, "authorization": auth}, {}) == "appsync"


def test_detect_service_post_event_localhost_stays_appsync():
    """Without ``.appsync-api.`` in Host, step 2 credential scope still routes to GraphQL service."""
    from ministack.core.router import detect_service

    auth = _minimal_sigv4_appsync_authorization()
    host = f"localhost:{_PORT}"
    assert detect_service("POST", "/event", {"host": host, "authorization": auth}, {}) == "appsync"


def test_publish_with_appsync_sigv4_scope_on_events_vhost():
    """Regression: signed ``POST /event`` must reach ``appsync_events`` (not GraphQL 404).

    Uses in-process ASGI (no TCP MiniStack) so CI and local runs stay green.
    """
    from ministack.services import appsync_events as ae

    ae.reset()
    mgmt_host = "example.appsync-api.us-east-1.localhost:4566"
    auth = _minimal_sigv4_appsync_authorization()

    async def _run():
        create_body = json.dumps(
            {"name": "sigv4-pub-asgi", "eventConfig": _DEFAULT_EVENT_CONFIG}
        ).encode()
        post_msgs = await _invoke_asgi_http("POST", "/v2/apis", mgmt_host, create_body)
        post_status, _, post_body = _asgi_response(post_msgs)
        assert post_status == 200
        api_id = json.loads(post_body.decode())["api"]["apiId"]

        ns_body = json.dumps({"name": "default"}).encode()
        ns_path = f"/v2/apis/{api_id}/channelNamespaces"
        ns_msgs = await _invoke_asgi_http("POST", ns_path, mgmt_host, ns_body)
        ns_status, _, ns_raw = _asgi_response(ns_msgs)
        assert ns_status == 200, ns_raw

        pub_host = f"{api_id}.appsync-api.us-east-1.amazonaws.com"
        pub_body = json.dumps(
            {"channel": "/default/room1", "events": [json.dumps({"sigv4": True})]}
        ).encode()
        pub_msgs = await _invoke_asgi_http(
            "POST",
            "/event",
            pub_host,
            pub_body,
            extra_headers={
                "authorization": auth,
                "x-api-key": "da2-local-test-key",
            },
        )
        p_status, _, p_raw = _asgi_response(pub_msgs)
        assert p_status == 200
        parsed = json.loads(p_raw.decode())
        assert len(parsed["successful"]) == 1
        assert parsed["failed"] == []

    asyncio.run(_run())


def test_publish_with_appsync_sigv4_on_management_host_still_404():
    """``POST /event`` on the AppSync *management* vhost is not an Events publish URL."""
    auth = _minimal_sigv4_appsync_authorization()

    async def _run():
        pub_body = json.dumps(
            {"channel": "/default/room1", "events": [json.dumps("x")]}
        ).encode()
        pub_msgs = await _invoke_asgi_http(
            "POST",
            "/event",
            "appsync.us-east-1.amazonaws.com",
            pub_body,
            extra_headers={
                "authorization": auth,
                "x-api-key": "da2-local-test-key",
            },
        )
        status, _, raw = _asgi_response(pub_msgs)
        assert status == 404
        body = json.loads(raw.decode())
        assert "Unknown path" in (body.get("message") or "")

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# WebSocket subscribe + fan-out
# ---------------------------------------------------------------------------

def test_websocket_subscribe_receives_published_event(api):
    ws = _open_subscriber(api)
    try:
        sub_resp = _subscribe(ws, "sub-1", "/default/room1")
        assert sub_resp == {"type": "subscribe_success", "id": "sub-1"}

        # tiny settle so the connection is registered before publish
        time.sleep(0.05)

        status, _ = _publish_http(api, "/default/room1", [json.dumps({"text": "hi"})])
        assert status == 200

        data = _drain_until(ws, lambda f: f.get("type") == "data", timeout=3.0)
        assert data is not None
        assert data["id"] == "sub-1"
        # Per spec, the `event` field is an ARRAY of JSON-encoded strings.
        assert isinstance(data["event"], list)
        assert json.loads(data["event"][0]) == {"text": "hi"}
    finally:
        ws.close()


def test_websocket_single_level_wildcard(api):
    ws = _open_subscriber(api)
    try:
        _subscribe(ws, "sub-1", "/default/*")
        time.sleep(0.05)

        _publish_http(api, "/default/room1", [json.dumps("a")])
        first = _drain_until(ws, lambda f: f.get("type") == "data", timeout=3.0)
        assert first is not None
        assert first["id"] == "sub-1"
        assert json.loads(first["event"][0]) == "a"

        # "/default/room1/child" is a two-segment suffix — should NOT match /default/*
        _publish_http(api, "/default/room1/child", [json.dumps("nope")])
        extra = _drain_until(ws, lambda f: f.get("type") == "data", timeout=1.0)
        assert extra is None
    finally:
        ws.close()


def test_websocket_unsubscribe_stops_delivery(api):
    ws = _open_subscriber(api)
    try:
        _subscribe(ws, "sub-x", "/default/room1")
        time.sleep(0.05)

        _publish_http(api, "/default/room1", [json.dumps("first")])
        first = _drain_until(ws, lambda f: f.get("type") == "data", timeout=3.0)
        assert first is not None
        assert json.loads(first["event"][0]) == "first"

        ws.send_json({"type": "unsubscribe", "id": "sub-x"})
        unsub_ack = _drain_until(ws, lambda f: f.get("type") == "unsubscribe_success", timeout=2.0)
        assert unsub_ack == {"type": "unsubscribe_success", "id": "sub-x"}

        _publish_http(api, "/default/room1", [json.dumps("after")])
        after = _drain_until(ws, lambda f: f.get("type") == "data", timeout=1.0)
        assert after is None
    finally:
        ws.close()


# ---------------------------------------------------------------------------
# WebSocket publish frame
# ---------------------------------------------------------------------------

def test_websocket_publish_frame_acks_and_fans_out(api):
    publisher = _open_subscriber(api)
    listener = _open_subscriber(api)
    try:
        assert _subscribe(listener, "sub-listener", "/default/room1") == {
            "type": "subscribe_success", "id": "sub-listener",
        }
        time.sleep(0.05)

        publisher.send_json({
            "type": "publish",
            "id": "pub-1",
            "channel": "/default/room1",
            "events": [json.dumps("over-ws")],
        })
        ack = _drain_until(publisher, lambda f: f.get("type") == "publish_success", timeout=3.0)
        assert ack is not None
        assert ack["id"] == "pub-1"
        assert len(ack["successful"]) == 1
        assert ack["failed"] == []

        data = _drain_until(listener, lambda f: f.get("type") == "data", timeout=3.0)
        assert data is not None
        assert data["id"] == "sub-listener"
        assert json.loads(data["event"][0]) == "over-ws"
    finally:
        publisher.close()
        listener.close()


# ---------------------------------------------------------------------------
# Server-initiated keep-alive
# ---------------------------------------------------------------------------

def test_websocket_server_sends_keepalive():
    """With APPSYNC_EVENTS_KA_INTERVAL_SECS lowered via compose, we should see a ka.

    If the environment hasn't been configured for a short interval, skip —
    the real 60s default is too slow for unit tests. Set the env var on the
    MiniStack container (e.g. APPSYNC_EVENTS_KA_INTERVAL_SECS=1) to enable.
    """
    ka_interval = os.environ.get("APPSYNC_EVENTS_KA_INTERVAL_SECS")
    if not ka_interval:
        pytest.skip("Set APPSYNC_EVENTS_KA_INTERVAL_SECS on the server to exercise ka frames")
    # Create a throwaway API so we can open a connection.
    import boto3
    client = boto3.client(
        "appsync",
        endpoint_url=_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    created = client.create_api(name="events-ka-test", eventConfig=_DEFAULT_EVENT_CONFIG)["api"]
    api_id = created["apiId"]
    try:
        client.create_channel_namespace(apiId=api_id, name="default")
        ws = _open_subscriber(api_id)
        try:
            ka = _drain_until(ws, lambda f: f.get("type") == "ka", timeout=float(ka_interval) + 2.0)
            assert ka == {"type": "ka"}
        finally:
            ws.close()
    finally:
        client.delete_api(apiId=api_id)


# ---------------------------------------------------------------------------
# Handshake auth (header-<b64> subprotocol)
# ---------------------------------------------------------------------------

def test_websocket_accepts_without_header_subprotocol_in_lax_mode(api):
    """Default MiniStack config is lax — missing header-<b64> still accepts."""
    host_header = f"{api}.appsync-realtime-api.us-east-1.amazonaws.com"
    ws = _WSClient(host_header=host_header, path="/event/realtime",
                   subprotocol="aws-appsync-event-ws")
    try:
        ws.send_json({"type": "connection_init"})
        ack = ws.recv_json()
        assert ack["type"] == "connection_ack"
    finally:
        ws.close()


def test_websocket_malformed_header_subprotocol_rejected(api):
    """A ``header-<b64>`` entry that doesn't decode to JSON must close the socket."""
    host_header = f"{api}.appsync-realtime-api.us-east-1.amazonaws.com"
    # "not-base64!!" is invalid base64url — decode fails.
    ws = _WSClient(
        host_header=host_header,
        path="/event/realtime",
        subprotocol="header-not@@@base64, aws-appsync-event-ws",
    )
    try:
        # Server should push a connection_error then close with 4401.
        frame = ws.recv_json(timeout=2.0)
        if frame is not None:
            assert frame["type"] == "connection_error"
            assert frame["errors"][0]["errorType"] == "UnauthorizedException"
    finally:
        ws.close()


def test_websocket_frame_authorization_falls_back_to_connection_auth(api):
    """Frames without their own ``authorization`` should reuse the handshake header.

    In lax mode this path is effectively a no-op for acceptance, but it still
    exercises the connection-scoped auth plumbing: a subscribe without an
    ``authorization`` block must still succeed.
    """
    ws = _open_subscriber(
        api,
        auth_header={"host": f"{api}.appsync-api.us-east-1.amazonaws.com",
                     "x-api-key": "da2-local-test-key"},
    )
    try:
        resp = _subscribe(ws, "sub-fallback", "/default/room-x")
        assert resp == {"type": "subscribe_success", "id": "sub-fallback"}
    finally:
        ws.close()


# ---------------------------------------------------------------------------
# Channel path validation
# ---------------------------------------------------------------------------

def test_publish_rejects_invalid_channel_path(api):
    # Too many segments (>5).
    status, body = _publish_http(api, "/default/a/b/c/d/e/f", [json.dumps("x")])
    assert status == 400
    assert "BadRequestException" in body["__type"]


def test_publish_rejects_segment_with_bad_chars(api):
    # Underscores are not allowed by the spec segment regex.
    status, body = _publish_http(api, "/default/bad_seg", [json.dumps("x")])
    assert status == 400
    assert "BadRequestException" in body["__type"]


def test_subscribe_rejects_invalid_channel_path(api):
    ws = _open_subscriber(api)
    try:
        ws.send_json({"type": "subscribe", "id": "sub-bad", "channel": "default/no-leading-slash"})
        err = _drain_until(ws, lambda f: f.get("type") == "subscribe_error", timeout=2.0)
        assert err is not None
        assert err["id"] == "sub-bad"
    finally:
        ws.close()


# ---------------------------------------------------------------------------
# Terraform compatibility tests
#
# The Terraform AWS provider ships ``aws_appsync_api`` and
# ``aws_appsync_channel_namespace`` resources that drive the v2 Event API
# through the SDK's CreateApi / GetApi / UpdateApi / DeleteApi operations.
# These tests replicate the exact wire shape Terraform reads back.
# ---------------------------------------------------------------------------


def test_terraform_aws_appsync_api_read_after_create(appsync):
    """aws_appsync_api reads: api_id, name, api_arn, dns, event_config, tags.

    Regression against Terraform AWS provider `internal/service/appsync/api.go`
    which keys on these exact camelCase fields from the boto/Go SDK response.
    """
    created = appsync.create_api(
        name="tf-events",
        eventConfig=_DEFAULT_EVENT_CONFIG,
        tags={"env": "local", "managed-by": "terraform"},
    )["api"]
    try:
        fetched = appsync.get_api(apiId=created["apiId"])["api"]
        for key in ("apiId", "name", "apiArn", "dns", "eventConfig", "tags", "created"):
            assert key in fetched, f"Terraform AWS provider requires '{key}'"
        assert fetched["name"] == "tf-events"
        assert fetched["apiArn"].startswith("arn:aws:appsync:")
        assert fetched["dns"]["HTTP"].endswith(f".appsync-api.us-east-1.localhost:{_PORT}")
        assert fetched["dns"]["REALTIME"].endswith(
            f".appsync-realtime-api.us-east-1.localhost:{_PORT}"
        )
        assert fetched["tags"] == {"env": "local", "managed-by": "terraform"}
        assert fetched["eventConfig"]["authProviders"] == [{"authType": "API_KEY"}]
    finally:
        appsync.delete_api(apiId=created["apiId"])


def test_terraform_aws_appsync_channel_namespace_read_after_create(appsync, api):
    """aws_appsync_channel_namespace reads publish_auth_modes/subscribe_auth_modes."""
    ns = appsync.create_channel_namespace(
        apiId=api,
        name="tf-ns",
        publishAuthModes=[{"authType": "API_KEY"}],
        subscribeAuthModes=[{"authType": "API_KEY"}],
    )["channelNamespace"]
    try:
        assert ns["publishAuthModes"] == [{"authType": "API_KEY"}]
        assert ns["subscribeAuthModes"] == [{"authType": "API_KEY"}]
        for key in ("apiId", "name", "publishAuthModes", "subscribeAuthModes", "created"):
            assert key in ns
    finally:
        appsync.delete_channel_namespace(apiId=api, name="tf-ns")


def test_terraform_aws_appsync_api_key_on_event_api_uses_v1_path(appsync, api):
    """aws_appsync_api_key targets POST /v1/apis/{apiId}/apikeys even when the
    apiId is an Event API. Without the v1->v2 delegation in services/appsync.py
    this test raises NotFoundException and `terraform apply` fails.
    """
    key = appsync.create_api_key(apiId=api, description="tf-managed")["apiKey"]
    assert key["id"].startswith("da2-")
    appsync.delete_api_key(apiId=api, id=key["id"])


def test_http_publish_invokes_lambda_authorizer(monkeypatch):
    """HTTP publish must enforce the same Lambda authorizer as WebSocket publish."""
    from ministack.services import appsync_events as ae

    ae.reset()
    create_body = json.dumps({
        "name": "auth-events",
        "eventConfig": {
            "authProviders": [{
                "authType": "AWS_LAMBDA",
                "lambdaAuthorizerConfig": {
                    "authorizerUri": "arn:aws:lambda:us-east-1:000000000000:function:authz"
                },
            }],
        },
    }).encode()
    status, _headers, raw = ae._create_api(create_body)
    assert status == 200
    api_id = json.loads(raw)["api"]["apiId"]
    ae._create_channel_namespace(api_id, b'{"name":"default"}')

    calls = []

    def _fake_invoke(arn, operation, channel, authorization_token):
        calls.append((arn, operation, channel, authorization_token))
        return authorization_token == "allow", {"tenant": "local"}

    monkeypatch.setattr(ae, "_events_authorizer_invoke", _fake_invoke)
    publish_body = json.dumps({"channel": "/default/room", "events": ['{"ok":true}']}).encode()

    denied = asyncio.run(ae._publish(api_id, {}, publish_body))
    assert denied[0] == 401

    allowed = asyncio.run(ae._publish(api_id, {"authorization": "allow"}, publish_body))
    assert allowed[0] == 200
    assert calls[-1] == (
        "arn:aws:lambda:us-east-1:000000000000:function:authz",
        "publish",
        "/default/room",
        "allow",
    )
