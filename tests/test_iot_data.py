"""Integration tests for the IoT Core data plane (Phase 1b).

Covers the ``iot-data Publish`` HTTP API and the MQTT-over-WebSocket bridge
that powers the original use case from issue #564 (Lambda publishes via
HTTP → browser subscribes via WebSocket).

The bridge implements MQTT 3.1.1 framing internally (no external broker
binary). Multi-tenancy is enforced by transparent topic prefixing in the
bridge layer.
"""

from __future__ import annotations

import base64
import io as _io
import json
import os
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
import zipfile as _zipfile
from urllib.parse import quote, urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import iot_test_ca, patch_endpoint_dns, sqs_policy_allow_sns

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# HTTP Publish (boto3 ``iot-data`` client)
# ---------------------------------------------------------------------------


def test_iot_data_publish_returns_200(iot_data_client):
    resp = iot_data_client.publish(topic=_unique("topic"), payload=b"hello")
    md = resp["ResponseMetadata"]
    assert md["HTTPStatusCode"] == 200


def test_iot_data_publish_accepts_qos_and_retain(iot_data_client):
    topic = _unique("retained")
    resp = iot_data_client.publish(
        topic=topic, qos=1, retain=True, payload=b"sticky"
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def _own_account_iot_data(**config):
    """An iot-data client in a fresh account, so other tests' retained messages stay out of its listings."""
    return boto3.client(
        "iot-data",
        endpoint_url=ENDPOINT,
        aws_access_key_id=f"{uuid.uuid4().int % 10**12:012d}",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(retries={"mode": "standard"}, **config),
    )


def test_get_retained_message_returns_the_retained_publish():
    client = _own_account_iot_data()
    payload = bytes(range(256))
    for topic in ("sensors/bin", "sensors/a b%c/ü"):
        before = int(time.time() * 1000)
        client.publish(topic=topic, qos=1, retain=True, payload=payload)
        after = int(time.time() * 1000)
        resp = client.get_retained_message(topic=topic)
        assert resp["topic"] == topic
        assert resp["payload"] == payload
        assert resp["qos"] == 1
        assert before <= resp["lastModifiedTime"] <= after
        assert "userProperties" not in resp


def test_get_retained_message_returns_http_publish_user_properties():
    """The user properties of a retained HTTP Publish come back as AWS's compact JSON array."""
    client = _own_account_iot_data()
    client.publish(
        topic="sensors/props", retain=True, payload=b"{}",
        userProperties=[{"app": "demo"}, {"k": "v1"}, {"k": "v2"}, {"u": "ü"}],
        contentType="application/json",
    )
    resp = client.get_retained_message(topic="sensors/props")
    assert resp["userProperties"] == '[{"app":"demo"},{"k":"v1"},{"k":"v2"},{"u":"ü"}]'.encode()


def test_retained_topic_keeps_a_literal_percent_sequence():
    """A topic holding %41, %25 or %2F is stored and read back as written, not decoded a second time."""
    client = _own_account_iot_data()
    topics = ("pct/p%41q", "pct/p%25q", "pct/p%2Fq")
    for topic in topics:
        client.publish(topic=topic, retain=True, payload=topic.encode())
    for topic in topics:
        resp = client.get_retained_message(topic=topic)
        assert (resp["topic"], resp["payload"]) == (topic, topic.encode())
    for decoded in ("pct/pAq", "pct/p%q", "pct/p/q"):
        with pytest.raises(ClientError) as ei:
            client.get_retained_message(topic=decoded)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    listed = {e["topic"] for e in client.list_retained_messages()["retainedTopics"]}
    assert listed == set(topics)


def test_get_retained_message_errors():
    client = _own_account_iot_data(parameter_validation=False)
    with pytest.raises(ClientError) as ei:
        client.get_retained_message(topic="sensors/never")
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert ei.value.response["Error"]["Message"] == "Retained message not found"
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
    for topic, message in (
        ("sensors/+", "Invalid topic filter"),
        ("sensors/#", "Invalid topic filter"),
        ("sensors/a+b", "Invalid topic filter"),
        ("", "unknown"),
    ):
        with pytest.raises(ClientError) as ei:
            client.get_retained_message(topic=topic)
        assert ei.value.response["Error"]["Code"] == "InvalidRequestException"
        assert ei.value.response["Error"]["Message"] == message
        assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_retained_message_is_replaced_kept_and_cleared():
    """A live publish keeps the retained message, a retained one replaces it, an empty one clears it."""
    client = _own_account_iot_data()
    client.publish(topic="t", qos=0, retain=True, payload=b"hello")
    first = client.get_retained_message(topic="t")
    client.publish(topic="t", qos=1, payload=b"live")
    client.publish(topic="t", qos=0, payload=b"")
    kept = client.get_retained_message(topic="t")
    assert (kept["payload"], kept["qos"]) == (b"hello", 0)
    assert kept["lastModifiedTime"] == first["lastModifiedTime"]

    time.sleep(0.01)
    client.publish(topic="t", qos=1, retain=True, payload=b"second")
    replaced = client.get_retained_message(topic="t")
    assert (replaced["payload"], replaced["qos"]) == (b"second", 1)
    assert replaced["lastModifiedTime"] > first["lastModifiedTime"]

    client.publish(topic="t", qos=0, retain=True, payload=b"")
    with pytest.raises(ClientError) as ei:
        client.get_retained_message(topic="t")
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert client.list_retained_messages()["retainedTopics"] == []


def test_list_retained_messages_newest_first_in_pages():
    client = _own_account_iot_data()
    for name in ("c", "a", "d", "b"):
        client.publish(topic=f"order/{name}", qos=1, retain=True, payload=name.encode() * 3)
        time.sleep(0.01)
    client.publish(topic="order/a", qos=0, payload=b"live")

    resp = client.list_retained_messages()
    assert resp["nextToken"] == ""
    assert [e["topic"] for e in resp["retainedTopics"]] == [
        "order/b", "order/d", "order/a", "order/c"
    ]
    entry = resp["retainedTopics"][0]
    assert set(entry) == {"topic", "payloadSize", "qos", "lastModifiedTime"}
    assert (entry["payloadSize"], entry["qos"]) == (3, 1)
    assert entry["lastModifiedTime"] == client.get_retained_message(
        topic="order/b")["lastModifiedTime"]

    seen, kwargs = [], {"maxResults": 3}
    while True:
        page = client.list_retained_messages(**kwargs)
        seen.append([e["topic"] for e in page["retainedTopics"]])
        if not page["nextToken"]:
            break
        kwargs["nextToken"] = page["nextToken"]
    assert seen == [["order/b", "order/d", "order/a"], ["order/c"]]


def test_list_retained_messages_pages_by_twenty_without_max_results():
    client = _own_account_iot_data()
    for i in range(21):
        client.publish(topic=f"page/{i:02d}", retain=True, payload=b"p")
    first = client.list_retained_messages()
    assert len(first["retainedTopics"]) == 20
    rest = client.list_retained_messages(nextToken=first["nextToken"])
    assert [e["topic"] for e in rest["retainedTopics"]] == ["page/00"]
    assert rest["nextToken"] == ""


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"maxResults": 0}, "Invalid maxResults value: Invalid max results parameter"),
        ({"maxResults": 201}, "Invalid maxResults value: Invalid max results parameter"),
        ({"maxResults": -1}, "Invalid maxResults value: Invalid max results parameter"),
        ({"maxResults": "abc"}, "Invalid maxResults value: abc"),
        ({"nextToken": "not-a-token"}, "unknown"),
    ],
)
def test_list_retained_messages_rejects_bad_paging(kwargs, message):
    client = _own_account_iot_data(parameter_validation=False)
    with pytest.raises(ClientError) as ei:
        client.list_retained_messages(**kwargs)
    assert ei.value.response["Error"]["Code"] == "InvalidRequestException"
    assert ei.value.response["Error"]["Message"] == message
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_retained_user_properties_and_time_survive_persistence():
    """User properties come back as AWS's JSON array and survive a restore with lastModifiedTime, per region."""
    import asyncio

    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import iot as iot_module
    from ministack.services import iot_data

    account = "123456789012"
    user_props = iot_module._encode_properties([
        (iot_module.PROP_CONTENT_TYPE, "text/plain"),
        (iot_module.PROP_USER_PROPERTY, ("app", "demo")),
        (iot_module.PROP_USER_PROPERTY, ("k", "v1")),
        (iot_module.PROP_USER_PROPERTY, ("k", "v2")),
        (iot_module.PROP_USER_PROPERTY, ("u", "ü")),
    ])
    content_type_only = iot_module._encode_properties([(iot_module.PROP_CONTENT_TYPE, "text/plain")])

    async def _get(region, path):
        set_request_account_id(account)
        set_request_region(region)
        status, _headers, body = await iot_data.handle_request("GET", path, {}, b"", {})
        return status, json.loads(body)

    async def _run():
        await iot_module.broker_publish(
            account, "eu-west-1", "dev/props", b"{}", qos=1, retain=True, properties=user_props)
        await iot_module.broker_publish(
            account, "eu-west-1", "dev/plain", b"x", retain=True, properties=content_type_only)
        status, before = await _get("eu-west-1", "/retainedMessage/dev/props")
        assert status == 200
        assert base64.b64decode(before["userProperties"]) == (
            '[{"app":"demo"},{"k":"v1"},{"k":"v2"},{"u":"ü"}]'.encode())
        assert "userProperties" not in (await _get("eu-west-1", "/retainedMessage/dev/plain"))[1]
        assert (await _get("eu-west-3", "/retainedMessage"))[1]["retainedTopics"] == []

        saved = iot_module.get_state()
        iot_module.reset()
        iot_module.broker_reset()
        assert (await _get("eu-west-1", "/retainedMessage/dev/props"))[0] == 404
        await asyncio.sleep(0.01)
        iot_module.load_persisted_state(saved)
        assert await _get("eu-west-1", "/retainedMessage/dev/props") == (200, before)

    iot_module.broker_reset()
    try:
        asyncio.run(_run())
    finally:
        iot_module.reset()
        iot_module.broker_reset()


def test_iot_reset_clears_retained_messages():
    """The iot module saves retained messages, so its reset drops them even when iot_data was never loaded."""
    import asyncio

    from ministack.services import iot as iot_module

    account = "123456789012"
    asyncio.run(iot_module.broker_publish(account, "eu-west-1", "dev/kept", b"x", retain=True))
    assert iot_module.broker_get_retained(account, "eu-west-1", "dev/kept") is not None
    iot_module.reset()
    assert iot_module.broker_get_retained(account, "eu-west-1", "dev/kept") is None
    assert iot_module.get_state()["mqtt_broker"]["retained"] == []


def test_iot_data_publish_rejects_wildcard_topic():
    """Wildcard characters in publish topics must produce a 400."""
    import urllib.request

    req = urllib.request.Request(
        f"{ENDPOINT}/topics/" + quote("foo/+/bar", safe=""),
        data=b"x",
        method="POST",
        headers={"Authorization": "AWS4-HMAC-SHA256 Credential=test/0/0/iotdata/aws4_request"},
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        pytest.fail("expected HTTP 400")
    except urllib.error.HTTPError as e:
        assert e.code == 400


def test_iot_data_publish_rejects_empty_topic():
    import urllib.request

    req = urllib.request.Request(
        f"{ENDPOINT}/topics/",
        data=b"x",
        method="POST",
        headers={"Authorization": "AWS4-HMAC-SHA256 Credential=test/0/0/iotdata/aws4_request"},
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        pytest.fail("expected error")
    except urllib.error.HTTPError as e:
        # Either 400 (we caught it) or 404 (router didn't match) is acceptable —
        # both signal "this isn't a valid Publish call".
        assert e.code in (400, 404)


def test_iot_data_publish_oversized_topic_400():
    import urllib.request

    long_topic = "a" * 300
    req = urllib.request.Request(
        f"{ENDPOINT}/topics/" + quote(long_topic, safe=""),
        data=b"x",
        method="POST",
        headers={"Authorization": "AWS4-HMAC-SHA256 Credential=test/0/0/iotdata/aws4_request"},
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        pytest.fail("expected HTTP 400")
    except urllib.error.HTTPError as e:
        assert e.code == 400


_NO_DOLLAR = "Topic can't start with $"
_MQTT_ONLY = "Invalid publish to restricted topic using HTTP"


@pytest.mark.parametrize(
    ("topic", "message"),
    [
        ("$aws/events/certificates/registered/x", _NO_DOLLAR),
        ("$AWS/events/certificates/registered/x", _NO_DOLLAR),
        ("$aws/jobs/x", _NO_DOLLAR),
        ("$aws/rules", _NO_DOLLAR),
        ("$foo/bar", _NO_DOLLAR),
        ("$aws/things/t1/jobs/get", _MQTT_ONLY),
        ("$aws/things/t1/defender/metrics/json", _MQTT_ONLY),
        ("$aws/commands/things/t1/executions/1/response/json", _MQTT_ONLY),
        ("$aws/rules/no_such_rule/x", None),
        ("$aws/things/t1/jobs", None),
    ],
)
def test_iot_data_publish_reserved_topics(iot_data_client, topic, message):
    """HTTPS Publish to a "$" topic is refused outside the reserved families
    and for the MQTT-only jobs, Device Defender and commands topics."""
    if message is None:
        resp = iot_data_client.publish(topic=topic, payload=b"{}")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        return
    with pytest.raises(ClientError) as ei:
        iot_data_client.publish(topic=topic, payload=b"{}")
    assert ei.value.response["Error"]["Code"] == "InvalidRequestException"
    assert ei.value.response["Error"]["Message"] == message
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


_BAD_EXPIRY = "Message expiry interval must be an unsigned 4-byte integer"
_BAD_RESPONSE_TOPIC = (
    "This response topic isn't valid. Response topics must not contain MQTT wildcard characters (+ or #)"
)
_BAD_USER_PROPERTIES = "Invalid JSON string for User Properties"


@pytest.mark.parametrize(
    ("kwargs", "user_properties_header", "message"),
    [
        ({"payloadFormatIndicator": "BOGUS"}, None, "Invalid Payload Format Indicator"),
        ({"messageExpiry": -1}, None, _BAD_EXPIRY),
        ({"messageExpiry": 2**32}, None, _BAD_EXPIRY),
        ({"responseTopic": "reply/+/x"}, None, _BAD_RESPONSE_TOPIC),
        ({"responseTopic": "reply/#"}, None, _BAD_RESPONSE_TOPIC),
        ({"correlationData": "not base64!"}, None, "Invalid Base64 string"),
        ({"userProperties": [{"a": "1", "b": "2"}]}, None,
         "Only one User Property is allowed in single JSONObject"),
        ({"userProperties": [{"a": 1}]}, None, _BAD_USER_PROPERTIES),
        ({"userProperties": {"a": "1"}}, None, _BAD_USER_PROPERTIES),
        ({}, base64.b64encode(b'["a"]').decode(), _BAD_USER_PROPERTIES),
        ({}, base64.b64encode(b"not json").decode(), _BAD_USER_PROPERTIES),
        ({}, '[{"a":"1"}]', "Invalid Base64 string"),
    ],
)
def test_iot_data_publish_refuses_invalid_mqtt5_properties(kwargs, user_properties_header, message):
    """The MQTT 5 headers and query parameters of Publish are refused with AWS's messages."""
    client = _own_account_iot_data(parameter_validation=False)
    if user_properties_header is not None:
        def _header(request, **_kw):
            request.headers["x-amz-mqtt5-user-properties"] = user_properties_header
        client.meta.events.register("before-sign.iot-data.Publish", _header)
    with pytest.raises(ClientError) as ei:
        client.publish(topic="props/refused", payload=b"x", **kwargs)
    assert ei.value.response["Error"]["Code"] == "InvalidRequestException"
    assert ei.value.response["Error"]["Message"] == message
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


# IAM contracts, run by CI with AUTH=false and AUTH=true.
@pytest.fixture
def auth_iot_shadow(iot_data_client):
    """A classic shadow to read, removed after the test."""
    thing = _unique("auth-thing")
    iot_data_client.update_thing_shadow(thingName=thing, payload=b'{"state":{"reported":{"a":1}}}')
    yield thing
    iot_data_client.delete_thing_shadow(thingName=thing)


@pytest.mark.auth_contract
@pytest.mark.parametrize("permission", ["none", "allow", "explicit-deny"])
def test_iot_data_permission_contract(auth_enabled, iam_user, auth_iot_shadow, permission):
    """A refused data plane call answers 403 ForbiddenException without a message."""
    statements = []
    if permission != "none":
        statements.append({"Effect": "Allow", "Action": ["iot:Publish", "iot:GetThingShadow"], "Resource": "*"})
    if permission == "explicit-deny":
        statements.append({"Effect": "Deny", "Action": ["iot:Publish", "iot:GetThingShadow"], "Resource": "*"})
    policy = {"Version": "2012-10-17", "Statement": statements} if statements else None
    user = iam_user(policy=policy)
    data = user.client("iot-data")
    calls = (
        lambda: data.publish(topic=_unique("auth/topic"), payload=b"{}"),
        lambda: data.get_thing_shadow(thingName=auth_iot_shadow),
    )
    for call in calls:
        if auth_enabled and permission != "allow":
            with pytest.raises(ClientError) as exc:
                call()
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
            assert exc.value.response["Error"]["Code"] == "ForbiddenException"
            assert exc.value.response["Error"]["Message"] is None
        else:
            assert call()["ResponseMetadata"]["HTTPStatusCode"] == 200
    if auth_enabled and permission == "none":
        # The control plane keeps its own denial.
        with pytest.raises(ClientError) as exc:
            user.client("iot").list_things()
        assert exc.value.response["Error"]["Code"] == "AccessDeniedException"


@pytest.mark.auth_contract
def test_iot_data_unknown_key_contract(auth_enabled):
    """A key IAM does not know answers 403 ForbiddenException with the message Forbidden."""
    data = boto3.client(
        "iot-data", endpoint_url=ENDPOINT, region_name="us-east-1",
        aws_access_key_id="AKIA" + uuid.uuid4().hex[:16].upper(), aws_secret_access_key="unknown-secret",
        config=Config(retries={"total_max_attempts": 1}),
    )
    if auth_enabled:
        with pytest.raises(ClientError) as exc:
            data.publish(topic=_unique("auth/topic"), payload=b"{}")
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403
        assert exc.value.response["Error"]["Code"] == "ForbiddenException"
        assert exc.value.response["Error"]["Message"] == "Forbidden"
    else:
        assert data.publish(topic=_unique("auth/topic"), payload=b"{}")["ResponseMetadata"]["HTTPStatusCode"] == 200


# ---------------------------------------------------------------------------
# MQTT-over-WebSocket round-trip
# ---------------------------------------------------------------------------
#
# The reference test for the unblocking use case from issue #564:
#   1. WebSocket client subscribes to topic T
#   2. Lambda calls iot-data Publish on T over HTTP
#   3. WebSocket subscriber receives the message within 2 seconds.


pytest.importorskip("websockets")
import asyncio  # noqa: E402

import websockets  # noqa: E402

# Minimal MQTT 3.1.1 codec for the test client.


def _enc_remaining(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n > 0:
            b |= 0x80
        out.append(b)
        if n == 0:
            return bytes(out)


def _enc_str(s: str) -> bytes:
    raw = s.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw






def _decode_remaining(buf: bytes, off: int) -> tuple[int, int]:
    multiplier = 1
    value = 0
    pos = off
    while True:
        b = buf[pos]
        pos += 1
        value += (b & 0x7F) * multiplier
        if b & 0x80 == 0:
            return value, pos
        multiplier *= 128


def _parse_packet(buf: bytes) -> tuple[int, int, bytes, int] | None:
    """Try to extract one packet. Returns (type, flags, body, total_consumed) or None."""
    if len(buf) < 2:
        return None
    first = buf[0]
    try:
        remaining, header_end = _decode_remaining(buf, 1)
    except IndexError:
        return None
    total = header_end + remaining
    if len(buf) < total:
        return None
    return (first >> 4) & 0x0F, first & 0x0F, buf[header_end:total], total




def _record_publish(msg, received: list) -> int | None:
    """Append any PUBLISH frame in ``msg`` to ``received``; return its packet type."""
    buf = msg if isinstance(msg, (bytes, bytearray)) else msg.encode("latin-1")
    parsed = _parse_packet(bytes(buf))
    if not parsed:
        return None
    ptype, _flags, body, _ = parsed
    if ptype == 3:  # PUBLISH
        topic_len = struct.unpack_from("!H", body, 0)[0]
        delivered_topic = body[2:2 + topic_len].decode("utf-8")
        payload = body[2 + topic_len:]
        received.append((delivered_topic, payload))
    return ptype


# The handshake budget for a WS subscriber. Generous on purpose: a single
# timeout here kills the thread before it can set `ready`, and every caller
# then reports the same unhelpful "did not become ready" instead of why.
_WS_HANDSHAKE_TIMEOUT = 15.0


async def _ws_subscribe_and_collect(
    ws_url: str, topic: str, ready_event: threading.Event, received: list, stop: threading.Event
):
    async with websockets.connect(ws_url, subprotocols=["mqtt"]) as ws:
        # Unique: a parallel worker connecting with the same client id would take this session over.
        await ws.send(_make_connect(_unique("test-client")))
        # Wait for CONNACK
        await asyncio.wait_for(ws.recv(), timeout=_WS_HANDSHAKE_TIMEOUT)
        # Subscribe
        await ws.send(_make_subscribe(packet_id=1, topic=topic, qos=0))
        # Retained PUBLISH frames may precede SUBACK in the in-process broker.
        while True:
            msg = await asyncio.wait_for(ws.recv(), timeout=_WS_HANDSHAKE_TIMEOUT)
            if _record_publish(msg, received) == 9:  # SUBACK
                break
        ready_event.set()

        # Collect PUBLISH frames until stop or timeout.
        end_at = time.time() + 5
        while not stop.is_set() and time.time() < end_at:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            _record_publish(msg, received)


async def _ws_unsubscribe_one_and_collect(
    ws_url: str,
    topics: list[str],
    drop: str,
    ready_event: threading.Event,
    received: list,
    stop: threading.Event,
):
    """Subscribe to every topic in ``topics``, then UNSUBSCRIBE from ``drop``."""
    async with websockets.connect(ws_url, subprotocols=["mqtt"]) as ws:
        await ws.send(_make_connect("unsub-client"))
        await asyncio.wait_for(ws.recv(), timeout=_WS_HANDSHAKE_TIMEOUT)
        for packet_id, topic in enumerate(topics, start=1):
            await ws.send(_make_subscribe(packet_id=packet_id, topic=topic, qos=0))
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=_WS_HANDSHAKE_TIMEOUT)
                if _record_publish(msg, received) == 9:  # SUBACK
                    break

        await ws.send(_make_unsubscribe(packet_id=len(topics) + 1, topics=[drop]))
        while True:
            msg = await asyncio.wait_for(ws.recv(), timeout=_WS_HANDSHAKE_TIMEOUT)
            if _record_publish(msg, received) == 11:  # UNSUBACK
                break
        ready_event.set()

        end_at = time.time() + 5
        while not stop.is_set() and time.time() < end_at:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            _record_publish(msg, received)


def test_iot_ws_unsubscribe_keeps_the_other_subscriptions(iot_data_client):
    """UNSUBSCRIBE over the real WebSocket bridge drops only the named filter."""
    kept = _unique("unsub/kept")
    dropped = _unique("unsub/dropped")
    parsed = urlparse(ENDPOINT)
    ws_host = parsed.hostname or "localhost"
    ws_port = parsed.port or 4566
    ws_url = f"ws://prefix-ats.iot.us-east-1.{ws_host}:{ws_port}/mqtt"

    ready = threading.Event()
    stop = threading.Event()
    received: list = []

    t = threading.Thread(
        target=lambda: asyncio.run(_ws_unsubscribe_one_and_collect(
            ws_url, [kept, dropped], dropped, ready, received, stop
        )),
        daemon=True,
    )
    t.start()
    assert ready.wait(timeout=_WS_HANDSHAKE_TIMEOUT), "WebSocket subscriber did not become ready"

    iot_data_client.publish(topic=dropped, payload=b"should-not-arrive")
    iot_data_client.publish(topic=kept, payload=b"should-arrive")

    deadline = time.time() + 5
    while time.time() < deadline and not received:
        time.sleep(0.05)
    # Give a message on the unsubscribed topic every chance to show up late.
    time.sleep(0.6)
    stop.set()
    t.join(timeout=2)

    assert received == [(kept, b"should-arrive")]


def test_iot_lambda_publishes_browser_subscribes_e2e(iot_data_client):
    """The unblocking use case from issue #564.

    A WebSocket client subscribes; the iot-data HTTP Publish API delivers
    a message that arrives over the subscribed WebSocket within 5 seconds.
    """
    topic = _unique("e2e/sensor")
    parsed = urlparse(ENDPOINT)
    ws_host = parsed.hostname or "localhost"
    ws_port = parsed.port or 4566
    # Use the IoT data hostname so the ASGI dispatch routes us to the broker.
    ws_url = f"ws://prefix-ats.iot.us-east-1.{ws_host}:{ws_port}/mqtt"

    ready = threading.Event()
    stop = threading.Event()
    received: list = []

    def _runner():
        asyncio.run(_ws_subscribe_and_collect(ws_url, topic, ready, received, stop))

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    assert ready.wait(timeout=_WS_HANDSHAKE_TIMEOUT), "WebSocket subscriber did not become ready"

    payload = b"telemetry-from-lambda"
    iot_data_client.publish(topic=topic, payload=payload)

    deadline = time.time() + 5
    while time.time() < deadline and not received:
        time.sleep(0.05)
    stop.set()
    t.join(timeout=2)

    assert received, "Subscriber did not receive any PUBLISH frames within 5s"
    delivered_topic, delivered_payload = received[0]
    assert delivered_topic == topic
    assert delivered_payload == payload


def test_iot_ws_publish_isolated_between_regions():
    """The same account and topic must remain isolated by IoT data region."""
    import boto3
    from botocore.config import Config

    topic = _unique("regional/sensor")
    parsed = urlparse(ENDPOINT)
    ws_host = parsed.hostname or "localhost"
    ws_port = parsed.port or 4566

    def _ws_url(region):
        credential = quote(
            f"test/20260726/{region}/iotdevicegateway/aws4_request",
            safe="",
        )
        return (
            f"ws://prefix-ats.iot.{region}.{ws_host}:{ws_port}/mqtt"
            f"?X-Amz-Credential={credential}"
        )

    east_ready = threading.Event()
    west_ready = threading.Event()
    stop = threading.Event()
    east_received = []
    west_received = []

    east_thread = threading.Thread(
        target=lambda: asyncio.run(_ws_subscribe_and_collect(
            _ws_url("us-east-1"),
            topic,
            east_ready,
            east_received,
            stop,
        )),
        daemon=True,
    )
    west_thread = threading.Thread(
        target=lambda: asyncio.run(_ws_subscribe_and_collect(
            _ws_url("us-west-2"),
            topic,
            west_ready,
            west_received,
            stop,
        )),
        daemon=True,
    )
    east_thread.start()
    west_thread.start()
    assert east_ready.wait(timeout=_WS_HANDSHAKE_TIMEOUT)
    assert west_ready.wait(timeout=_WS_HANDSHAKE_TIMEOUT)

    east_client = boto3.client(
        "iot-data",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(retries={"mode": "standard"}),
    )
    east_client.publish(topic=topic, payload=b"east-only")

    deadline = time.time() + 5
    while time.time() < deadline and not east_received:
        time.sleep(0.05)
    time.sleep(0.6)
    stop.set()
    east_thread.join(timeout=2)
    west_thread.join(timeout=2)

    assert east_received == [(topic, b"east-only")]
    assert west_received == []


@pytest.mark.parametrize("wildcard_region", ["+", "#"])
def test_iot_ws_credential_region_wildcards_cannot_bypass_isolation(
    wildcard_region,
):
    """Credential-region wildcards cannot receive live or retained messages."""
    import boto3
    from botocore.config import Config

    topic = _unique("wildcard-region/sensor")
    parsed = urlparse(ENDPOINT)
    ws_host = parsed.hostname or "localhost"
    ws_port = parsed.port or 4566
    credential = quote(
        (
            "test/20260726/"
            f"{wildcard_region}/iotdevicegateway/aws4_request"
        ),
        safe="",
    )
    ws_url = (
        f"ws://prefix-ats.iot.us-east-1.{ws_host}:{ws_port}/mqtt"
        f"?X-Amz-Credential={credential}"
    )
    live_ready = threading.Event()
    live_stop = threading.Event()
    live_received = []

    live_thread = threading.Thread(
        target=lambda: asyncio.run(_ws_subscribe_and_collect(
            ws_url,
            topic,
            live_ready,
            live_received,
            live_stop,
        )),
        daemon=True,
    )
    live_thread.start()
    assert live_ready.wait(timeout=_WS_HANDSHAKE_TIMEOUT)

    east_client = boto3.client(
        "iot-data",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(retries={"mode": "standard"}),
    )
    east_client.publish(topic=topic, payload=b"east", retain=True)

    time.sleep(0.6)
    live_stop.set()
    live_thread.join(timeout=2)
    assert live_received == []

    retained_ready = threading.Event()
    retained_stop = threading.Event()
    retained_received = []
    retained_thread = threading.Thread(
        target=lambda: asyncio.run(_ws_subscribe_and_collect(
            ws_url,
            topic,
            retained_ready,
            retained_received,
            retained_stop,
        )),
        daemon=True,
    )
    retained_thread.start()
    assert retained_ready.wait(timeout=_WS_HANDSHAKE_TIMEOUT)
    time.sleep(0.6)
    retained_stop.set()
    retained_thread.join(timeout=2)
    assert retained_received == []


def test_iot_ws_topic_isolation_between_accounts(iot_data_client):
    """A subscriber in account A must NOT see a publish from account B.

    Multi-tenancy via transparent topic prefixing in the bridge.
    """
    import boto3
    from botocore.config import Config

    topic = _unique("isolation/probe")
    parsed = urlparse(ENDPOINT)
    ws_host = parsed.hostname or "localhost"
    ws_port = parsed.port or 4566
    # Subscribe as account A — embed the access key in a SigV4-shaped query
    # parameter the bridge knows how to read.
    cred = quote("111111111111/20240101/us-east-1/iotdevicegateway/aws4_request")
    ws_url = (
        f"ws://prefix-ats.iot.us-east-1.{ws_host}:{ws_port}/mqtt"
        f"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={cred}"
    )

    ready = threading.Event()
    stop = threading.Event()
    received: list = []

    def _runner():
        asyncio.run(_ws_subscribe_and_collect(ws_url, topic, ready, received, stop))

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    assert ready.wait(timeout=_WS_HANDSHAKE_TIMEOUT)

    # Publish from account B using a 12-digit access key.
    client_b = boto3.client(
        "iot-data",
        endpoint_url=ENDPOINT,
        aws_access_key_id="222222222222",
        aws_secret_access_key="x",
        region_name="us-east-1",
        config=Config(retries={"mode": "standard"}),
    )
    client_b.publish(topic=topic, payload=b"from-b")

    # Give the broker time to (not) deliver.
    time.sleep(1.5)
    stop.set()
    t.join(timeout=2)

    assert received == [], (
        "Subscriber in account A should not have received a publish from account B "
        f"(got {received!r})"
    )


def test_iot_ws_same_account_publish_delivers(iot_data_client):
    """Positive case: a subscriber in account A DOES receive a publish from account A.

    Validates that topic prefixing correctly scopes delivery within the same
    account — the counterpart to the negative isolation test above.
    """
    import boto3
    from botocore.config import Config

    topic = _unique("same-acct/sensor")
    parsed = urlparse(ENDPOINT)
    ws_host = parsed.hostname or "localhost"
    ws_port = parsed.port or 4566
    account = "333333333333"
    cred = quote(f"{account}/20240101/us-east-1/iotdevicegateway/aws4_request")
    ws_url = (
        f"ws://prefix-ats.iot.us-east-1.{ws_host}:{ws_port}/mqtt"
        f"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={cred}"
    )

    ready = threading.Event()
    stop = threading.Event()
    received: list = []

    def _runner():
        asyncio.run(_ws_subscribe_and_collect(ws_url, topic, ready, received, stop))

    t = threading.Thread(target=_runner, daemon=True)
    t.start()
    assert ready.wait(timeout=_WS_HANDSHAKE_TIMEOUT)

    # Publish from the SAME account.
    client_a = boto3.client(
        "iot-data",
        endpoint_url=ENDPOINT,
        aws_access_key_id=account,
        aws_secret_access_key="x",
        region_name="us-east-1",
        config=Config(retries={"mode": "standard"}),
    )
    payload = b"hello-from-same-account"
    client_a.publish(topic=topic, payload=payload)

    deadline = time.time() + 5
    while time.time() < deadline and not received:
        time.sleep(0.05)
    stop.set()
    t.join(timeout=2)

    assert received, "Subscriber should have received the publish from the same account"
    delivered_topic, delivered_payload = received[0]
    assert delivered_topic == topic
    assert delivered_payload == payload


# ---------------------------------------------------------------------------
# Device Shadow (GetThingShadow / UpdateThingShadow / DeleteThingShadow)
# ---------------------------------------------------------------------------


def _read_shadow(resp) -> dict:
    return json.loads(resp["payload"].read())


def test_get_thing_shadow_missing_raises_not_found(iot_data_client):
    with pytest.raises(ClientError) as ei:
        iot_data_client.get_thing_shadow(thingName=_unique("nothing"))
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_update_thing_shadow_reported_and_read_back(iot_data_client):
    thing = _unique("dev")
    resp = iot_data_client.update_thing_shadow(
        thingName=thing,
        payload=json.dumps({"state": {"reported": {"temp": 22, "missedReadings": 3}}}).encode(),
    )
    accepted = _read_shadow(resp)
    # The /accepted response echoes only the reported section it received.
    assert accepted["state"] == {"reported": {"temp": 22, "missedReadings": 3}}
    assert accepted["version"] == 1
    assert "reported" in accepted["metadata"]

    got = _read_shadow(iot_data_client.get_thing_shadow(thingName=thing))
    assert got["state"]["reported"] == {"temp": 22, "missedReadings": 3}
    assert got["version"] == 1


def test_update_thing_shadow_merges_and_computes_delta(iot_data_client):
    thing = _unique("dev")
    iot_data_client.update_thing_shadow(
        thingName=thing, payload=json.dumps({"state": {"reported": {"temp": 22}}}).encode()
    )
    iot_data_client.update_thing_shadow(
        thingName=thing, payload=json.dumps({"state": {"desired": {"temp": 25}}}).encode()
    )
    got = _read_shadow(iot_data_client.get_thing_shadow(thingName=thing))
    assert got["state"]["desired"] == {"temp": 25}
    assert got["state"]["reported"] == {"temp": 22}
    # delta = desired fields differing from reported.
    assert got["state"]["delta"] == {"temp": 25}
    assert got["version"] == 2


def test_update_thing_shadow_null_removes_field(iot_data_client):
    thing = _unique("dev")
    iot_data_client.update_thing_shadow(
        thingName=thing,
        payload=json.dumps({"state": {"reported": {"a": 1, "b": 2}}}).encode(),
    )
    iot_data_client.update_thing_shadow(
        thingName=thing, payload=json.dumps({"state": {"reported": {"b": None}}}).encode()
    )
    got = _read_shadow(iot_data_client.get_thing_shadow(thingName=thing))
    assert got["state"]["reported"] == {"a": 1}


def test_named_shadow_is_isolated_from_classic(iot_data_client):
    thing = _unique("dev")
    iot_data_client.update_thing_shadow(
        thingName=thing, payload=json.dumps({"state": {"reported": {"classic": True}}}).encode()
    )
    iot_data_client.update_thing_shadow(
        thingName=thing, shadowName="cfg",
        payload=json.dumps({"state": {"reported": {"named": True}}}).encode(),
    )
    classic = _read_shadow(iot_data_client.get_thing_shadow(thingName=thing))
    named = _read_shadow(iot_data_client.get_thing_shadow(thingName=thing, shadowName="cfg"))
    assert classic["state"]["reported"] == {"classic": True}
    assert named["state"]["reported"] == {"named": True}


def test_delete_thing_shadow(iot_data_client):
    thing = _unique("dev")
    iot_data_client.update_thing_shadow(
        thingName=thing, payload=json.dumps({"state": {"reported": {"x": 1}}}).encode()
    )
    iot_data_client.delete_thing_shadow(thingName=thing)
    with pytest.raises(ClientError) as ei:
        iot_data_client.get_thing_shadow(thingName=thing)
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_update_thing_shadow_version_conflict(iot_data_client):
    thing = _unique("dev")
    iot_data_client.update_thing_shadow(
        thingName=thing, payload=json.dumps({"state": {"reported": {"x": 1}}}).encode()
    )
    # Stale version is rejected.
    with pytest.raises(ClientError) as ei:
        iot_data_client.update_thing_shadow(
            thingName=thing,
            payload=json.dumps({"state": {"reported": {"x": 2}}, "version": 99}).encode(),
        )
    assert ei.value.response["Error"]["Code"] == "ConflictException"


# ---------------------------------------------------------------------------    
# Topic-rule routing (publish → rule → Lambda)
# ---------------------------------------------------------------------------

# Handler forwards the received rule event to the SQS queue named by SINK_URL,
# so the test can observe that the rule fired and with what payload.
_RULE_SINK_HANDLER = (
    "import boto3, json, os\n"
    "def handler(event, context):\n"
    "    s = boto3.client('sqs', endpoint_url=os.environ['AWS_ENDPOINT_URL'])\n"
    "    s.send_message(QueueUrl=os.environ['SINK_URL'], MessageBody=json.dumps(event))\n"
    "    return {'ok': True}\n"
)


def _make_sink_lambda(lam, sink_url):
    buf = _io.BytesIO()
    with _zipfile.ZipFile(buf, "w") as z:
        z.writestr("index.py", _RULE_SINK_HANDLER)
    name = _unique("rulefn")
    lam.create_function(
        FunctionName=name,
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": buf.getvalue()},
        Environment={"Variables": {"SINK_URL": sink_url}},
    )
    return lam.get_function(FunctionName=name)["Configuration"]["FunctionArn"]


def _poll_sink(sqs, url, timeout=12):
    body = _poll_body(sqs, url, timeout)
    return json.loads(body) if body is not None else None


def test_iot_topic_rule_routes_publish_to_lambda(iot_client, iot_data_client, lam, sqs):
    sink = sqs.create_queue(QueueName=_unique("rule-sink"))["QueueUrl"]
    fn_arn = _make_sink_lambda(lam, sink)
    rule = _unique("route").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'sensors/+/telemetry'",
            "actions": [{"lambda": {"functionArn": fn_arn}}],
        },
    )

    iot_data_client.publish(
        topic="sensors/a1/telemetry",
        payload=json.dumps({"temp": 22, "missedReadings": 3}).encode(),
    )
    event = _poll_sink(sqs, sink)
    assert event == {"temp": 22, "missedReadings": 3}

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_basic_ingest_routes_to_lambda(iot_client, iot_data_client, lam, sqs):
    sink = sqs.create_queue(QueueName=_unique("ingest-sink"))["QueueUrl"]
    fn_arn = _make_sink_lambda(lam, sink)
    rule = _unique("ingest").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'unused'",
            "actions": [{"lambda": {"functionArn": fn_arn}}],
        },
    )

    # Basic Ingest: publishing to `$aws/rules/<ruleName>` invokes the rule
    # directly, bypassing the topic filter.
    iot_data_client.publish(
        topic=f"$aws/rules/{rule}",
        payload=json.dumps({"temp": 99, "basic": True}).encode(),
    )
    event = _poll_sink(sqs, sink)
    assert event == {"temp": 99, "basic": True}

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_disabled_rule_does_not_fire(iot_client, iot_data_client, lam, sqs):
    sink = sqs.create_queue(QueueName=_unique("disabled-sink"))["QueueUrl"]
    fn_arn = _make_sink_lambda(lam, sink)
    rule = _unique("disabled").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'sensors/+/telemetry'",
            "ruleDisabled": True,
            "actions": [{"lambda": {"functionArn": fn_arn}}],
        },
    )

    iot_data_client.publish(
        topic="sensors/a1/telemetry", payload=json.dumps({"temp": 1}).encode()
    )
    assert _poll_sink(sqs, sink, timeout=4) is None

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_disable_topic_rule_stops_the_rule(iot_client, iot_data_client, sqs, iam):
    """DisableTopicRule stops both the topic filter and Basic Ingest; EnableTopicRule restarts them."""
    queue = sqs.create_queue(QueueName=_unique("toggled-sink"))["QueueUrl"]
    rule = _unique("toggled").replace("-", "_")
    role = _rule_role(iam)
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'sensors/+/toggled'",
            "actions": [{"sqs": {"queueUrl": queue, "roleArn": role["Arn"]}}],
        },
    )

    def publish(n):
        for topic in ("sensors/a1/toggled", f"$aws/rules/{rule}"):
            iot_data_client.publish(topic=topic, payload=json.dumps({"n": n}).encode())

    try:
        iot_client.disable_topic_rule(ruleName=rule)
        publish(1)
        assert _poll_body(sqs, queue, timeout=2) is None
        iot_client.enable_topic_rule(ruleName=rule)
        publish(2)
        assert _take_bodies(sqs, queue, 2) == [{"n": 2}, {"n": 2}]
    finally:
        iot_client.delete_topic_rule(ruleName=rule)
        iam.delete_role(RoleName=role["RoleName"])


# Every byte value: not valid UTF-8, so it survives the rule path only if the
# payload is never text-decoded.
_BINARY_PAYLOAD = bytes(range(256))


def test_iot_rule_encode_base64_projection_basic_ingest(
    iot_client, iot_data_client, lam, sqs
):
    sink = sqs.create_queue(QueueName=_unique("encode-sink"))["QueueUrl"]
    fn_arn = _make_sink_lambda(lam, sink)
    rule = _unique("encode").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT encode(*, 'base64') AS data FROM 'telemetry'",
            "awsIotSqlVersion": "2016-03-23",
            "actions": [{"lambda": {"functionArn": fn_arn}}],
        },
    )

    iot_data_client.publish(topic=f"$aws/rules/{rule}", payload=_BINARY_PAYLOAD)
    event = _poll_sink(sqs, sink)
    assert list(event) == ["data"]
    assert base64.b64decode(event["data"]) == _BINARY_PAYLOAD

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_rule_encode_base64_projection_topic_filter(
    iot_client, iot_data_client, lam, sqs
):
    sink = sqs.create_queue(QueueName=_unique("encode-filter-sink"))["QueueUrl"]
    fn_arn = _make_sink_lambda(lam, sink)
    rule = _unique("encfilter").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT encode(*, 'base64') AS data FROM 'telemetry'",
            "actions": [{"lambda": {"functionArn": fn_arn}}],
        },
    )

    iot_data_client.publish(topic="telemetry", payload=_BINARY_PAYLOAD)
    event = _poll_sink(sqs, sink)
    assert list(event) == ["data"]
    assert base64.b64decode(event["data"]) == _BINARY_PAYLOAD

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_rule_attribute_projection(iot_client, iot_data_client, lam, sqs):
    sink = sqs.create_queue(QueueName=_unique("project-sink"))["QueueUrl"]
    fn_arn = _make_sink_lambda(lam, sink)
    rule = _unique("project").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT deviceId AS id, topic(2) AS device "
                   "FROM 'sensors/+/telemetry'",
            "actions": [{"lambda": {"functionArn": fn_arn}}],
        },
    )

    iot_data_client.publish(
        topic="sensors/a1/telemetry",
        payload=json.dumps({"deviceId": "d1", "temp": 22}).encode(),
    )
    assert _poll_sink(sqs, sink) == {"id": "d1", "device": "a1"}


def test_iot_rule_where_clause_gates_dispatch(iot_client, iot_data_client, lam, sqs):
    """Only publishes satisfying the WHERE predicate reach the rule's actions."""
    sink = sqs.create_queue(QueueName=_unique("where-sink"))["QueueUrl"]
    fn_arn = _make_sink_lambda(lam, sink)
    rule = _unique("gated").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'alarms/+/state' WHERE severity = 'high' AND count > 1",
            "actions": [{"lambda": {"functionArn": fn_arn}}],
        },
    )

    # Non-matching: predicate false, then attribute missing entirely.
    iot_data_client.publish(
        topic="alarms/a1/state",
        payload=json.dumps({"severity": "low", "count": 5}).encode(),
    )
    iot_data_client.publish(
        topic="alarms/a1/state", payload=json.dumps({"count": 5}).encode()
    )
    # Matching.
    iot_data_client.publish(
        topic="alarms/a1/state",
        payload=json.dumps({"severity": "high", "count": 2, "id": "x"}).encode(),
    )

    event = _poll_sink(sqs, sink)
    assert event == {"severity": "high", "count": 2, "id": "x"}
    # No further deliveries — the non-matching publishes never dispatched.
    assert _poll_sink(sqs, sink, timeout=3) is None

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_rule_where_topic_function_under_basic_ingest(
    iot_client, iot_data_client, lam, sqs
):
    """Under Basic Ingest a WHERE reading `topic(n)` sees the topic *after* the
    `$aws/rules/<name>/` prefix, so the same predicate gates both publish paths."""
    sink = sqs.create_queue(QueueName=_unique("bi-where-sink"))["QueueUrl"]
    fn_arn = _make_sink_lambda(lam, sink)
    rule = _unique("biwhere").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            # The FROM filter is bypassed by Basic Ingest; the WHERE is not.
            "sql": "SELECT * FROM 'unused' WHERE topic(2) = 'a1'",
            "actions": [{"lambda": {"functionArn": fn_arn}}],
        },
    )

    # topic() reports 'sensors/b7/telemetry' here — topic(2) is 'b7', so the
    # predicate is false and nothing dispatches.
    iot_data_client.publish(
        topic=f"$aws/rules/{rule}/sensors/b7/telemetry",
        payload=json.dumps({"temp": 1}).encode(),
    )
    # ...and 'a1' for this one, which matches. If the prefix were not stripped,
    # topic(2) would be 'rules' for both and neither would dispatch.
    iot_data_client.publish(
        topic=f"$aws/rules/{rule}/sensors/a1/telemetry",
        payload=json.dumps({"temp": 2}).encode(),
    )

    assert _poll_sink(sqs, sink) == {"temp": 2}
    assert _poll_sink(sqs, sink, timeout=3) is None

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_rule_where_or_clause_dispatches_either_branch(
    iot_client, iot_data_client, lam, sqs
):
    """An `OR` predicate fires for either branch and stays closed otherwise."""
    sink = sqs.create_queue(QueueName=_unique("or-where-sink"))["QueueUrl"]
    fn_arn = _make_sink_lambda(lam, sink)
    rule = _unique("orwhere").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'alarms/+/state' "
                   "WHERE (severity = 'high' OR severity = 'critical') AND count > 1",
            "actions": [{"lambda": {"functionArn": fn_arn}}],
        },
    )

    # Neither OR branch holds.
    iot_data_client.publish(
        topic="alarms/a1/state",
        payload=json.dumps({"severity": "low", "count": 5}).encode(),
    )
    # A branch holds but the AND-ed clause does not.
    iot_data_client.publish(
        topic="alarms/a1/state",
        payload=json.dumps({"severity": "critical", "count": 1}).encode(),
    )
    # Second OR branch + the AND-ed clause: dispatches.
    iot_data_client.publish(
        topic="alarms/a1/state",
        payload=json.dumps({"severity": "critical", "count": 4, "id": "y"}).encode(),
    )

    assert _poll_sink(sqs, sink) == {"severity": "critical", "count": 4, "id": "y"}
    assert _poll_sink(sqs, sink, timeout=3) is None

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_rule_republish_where_gated_ws_subscriber(iot_client, iot_data_client):
    """A WHERE-gated republish rule: a WebSocket subscriber on the republish
    target observes only the matching publish."""
    from conftest import patch_endpoint_dns

    source = _unique("raw/alerts")
    target = _unique("filtered/alerts")
    rule = _unique("repub").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": f"SELECT * FROM '{source}' WHERE severity = 'high'",
            "actions": [
                {"republish": {"topic": target, "qos": 0, "roleArn": "arn:aws:iam::000000000000:role/rule"}}
            ],
        },
    )

    parsed = urlparse(ENDPOINT)
    ws_host = parsed.hostname or "localhost"
    ws_port = parsed.port or 4566
    ws_url = f"ws://prefix-ats.iot.us-east-1.{ws_host}:{ws_port}/mqtt"

    ready = threading.Event()
    stop = threading.Event()
    received: list = []

    with patch_endpoint_dns():
        t = threading.Thread(
            target=lambda: asyncio.run(
                _ws_subscribe_and_collect(ws_url, target, ready, received, stop)
            ),
            daemon=True,
        )
        t.start()
        assert ready.wait(timeout=_WS_HANDSHAKE_TIMEOUT), "WebSocket subscriber did not become ready"

        iot_data_client.publish(
            topic=source, payload=json.dumps({"severity": "low", "n": 1}).encode()
        )
        iot_data_client.publish(
            topic=source, payload=json.dumps({"severity": "high", "n": 2}).encode()
        )

        deadline = time.time() + 5
        while time.time() < deadline and not received:
            time.sleep(0.05)
        time.sleep(0.5)  # allow a (wrong) second delivery to surface
        stop.set()
        t.join(timeout=2)

    assert len(received) == 1, f"expected exactly the matching publish, got {received}"
    delivered_topic, delivered_payload = received[0]
    assert delivered_topic == target
    assert json.loads(delivered_payload) == {"severity": "high", "n": 2}

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_rule_dynamodbv2_action_puts_item(iot_client, iot_data_client, ddb):
    table = _unique("rule-sink").replace("-", "")
    ddb.create_table(
        TableName=table,
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )
    rule = _unique("ddbrule").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT deviceId AS id, temp, active FROM 'ddb/+/telemetry'",
            "actions": [{"dynamoDBv2": {"putItem": {"tableName": table}, "roleArn": "arn:aws:iam::000000000000:role/rule"}}],
        },
    )

    iot_data_client.publish(
        topic="ddb/a1/telemetry",
        payload=json.dumps({"deviceId": "d1", "temp": 22.5, "active": True}).encode(),
    )

    items = []
    deadline = time.time() + 10
    while time.time() < deadline:
        items = ddb.scan(TableName=table).get("Items", [])
        if items:
            break
        time.sleep(0.2)
    assert items == [
        {"id": {"S": "d1"}, "temp": {"N": "22.5"}, "active": {"BOOL": True}}
    ]

    iot_client.delete_topic_rule(ruleName=rule)
    ddb.delete_table(TableName=table)


def test_iot_rule_sns_action_publishes_to_topic(iot_client, iot_data_client, sns, sqs):
    topic_arn = sns.create_topic(Name=_unique("rule-sns"))["TopicArn"]
    queue_url = sqs.create_queue(QueueName=_unique("sns-sink"))["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(
        QueueUrl=queue_url, AttributeNames=["QueueArn"]
    )["Attributes"]["QueueArn"]
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={"Policy": json.dumps(sqs_policy_allow_sns(queue_arn, topic_arn))})
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)

    rule = _unique("snsrule").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'notify/+/event'",
            "actions": [{"sns": {"targetArn": topic_arn, "roleArn": "arn:aws:iam::000000000000:role/rule"}}],
        },
    )

    iot_data_client.publish(
        topic="notify/a1/event", payload=json.dumps({"kind": "boom"}).encode()
    )

    body = None
    deadline = time.time() + 10
    while time.time() < deadline:
        msgs = sqs.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=1, WaitTimeSeconds=1
        )
        if msgs.get("Messages"):
            body = json.loads(msgs["Messages"][0]["Body"])
            break
    assert body is not None, "SNS → SQS delivery did not arrive"
    # Default (non-raw) delivery wraps the message in the SNS envelope.
    message = json.loads(body["Message"]) if "Message" in body else body
    assert message == {"kind": "boom"}

    iot_client.delete_topic_rule(ruleName=rule)


# ---------------------------------------------------------------------------
# Fleet-index connectivity (a live MQTT session drives connectivity.*)
# ---------------------------------------------------------------------------


def _broker_ws_url() -> str:
    parsed = urlparse(ENDPOINT)
    return (
        f"ws://prefix-ats.iot.us-east-1.{parsed.hostname or 'localhost'}"
        f":{parsed.port or 4566}/mqtt"
    )


def _broker_ws_url_for(account: str, region: str = "us-east-1") -> str:
    """A broker URL whose SigV4 credential scope picks account and region."""
    parsed = urlparse(ENDPOINT)
    cred = quote(f"{account}/20240101/{region}/iotdevicegateway/aws4_request")
    return (
        f"ws://prefix-ats.iot.{region}.{parsed.hostname or 'localhost'}"
        f":{parsed.port or 4566}/mqtt"
        f"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={cred}"
    )


async def _mqtt_connect(client_id: str, url: str | None = None):
    """Open an MQTT session and hold it. The caller closes the socket."""
    ws = await websockets.connect(url or _broker_ws_url(), subprotocols=["mqtt"])
    await ws.send(_make_connect(client_id))
    await asyncio.wait_for(ws.recv(), timeout=_WS_HANDSHAKE_TIMEOUT)  # CONNACK
    return ws


async def _mqtt_disconnect(ws, graceful: bool = True):
    if graceful:
        await ws.send(bytes([0xE0, 0x00]))  # DISCONNECT
    await ws.close()


def _enable_fleet_indexing(client) -> None:
    """Fleet indexing with connectivity status on — the dashboard's config."""
    client.update_indexing_configuration(
        thingIndexingConfiguration={
            "thingIndexingMode": "REGISTRY_AND_SHADOW",
            "thingConnectivityIndexingMode": "STATUS",
        }
    )


def _connectivity_for(iot_client, thing: str) -> dict:
    hits = iot_client.search_index(queryString=f"thingName:{thing}")["things"]
    assert len(hits) == 1, hits
    return hits[0]["connectivity"]


def _await_connected(iot_client, thing: str, want: bool, timeout: float = 5.0):
    """Wait for the index to report the wanted state, then return the group."""
    deadline = time.time() + timeout
    doc = _connectivity_for(iot_client, thing)
    while doc["connected"] is not want and time.time() < deadline:
        time.sleep(0.05)
        doc = _connectivity_for(iot_client, thing)
    assert doc["connected"] is want, doc
    return doc


def test_search_index_finds_connected_thing_and_loses_it_on_disconnect(iot_client):
    """`connectivity.connected:true` is the "which of my things are online" query."""
    _enable_fleet_indexing(iot_client)
    thing = _unique("conn").replace("-", "")
    iot_client.create_thing(thingName=thing)

    async def _run():
        ws = await _mqtt_connect(thing)
        try:
            connected = _await_connected(iot_client, thing, True)
            assert connected["timestamp"] > 0
            online = iot_client.search_index(
                queryString=f"thingName:{thing} AND connectivity.connected:true"
            )["things"]
            assert [t["thingName"] for t in online] == [thing]
        finally:
            await _mqtt_disconnect(ws)

        offline = _await_connected(iot_client, thing, False)
        assert offline["disconnectReason"] == "CLIENT_INITIATED_DISCONNECT"
        assert (
            iot_client.search_index(
                queryString=f"thingName:{thing} AND connectivity.connected:true"
            )["things"]
            == []
        )

    try:
        asyncio.run(_run())
    finally:
        iot_client.delete_thing(thingName=thing)


def test_search_index_connectivity_timestamp_moves_on_disconnect(iot_client):
    _enable_fleet_indexing(iot_client)
    thing = _unique("clock").replace("-", "")
    iot_client.create_thing(thingName=thing)

    async def _run():
        ws = await _mqtt_connect(thing)
        connected_at = _await_connected(iot_client, thing, True)["timestamp"]
        time.sleep(0.05)
        await _mqtt_disconnect(ws)
        assert _await_connected(iot_client, thing, False)["timestamp"] > connected_at

    try:
        asyncio.run(_run())
    finally:
        iot_client.delete_thing(thingName=thing)


def test_search_index_connectivity_is_tied_to_the_client_id(iot_client):
    """A session under another client id is not this thing's connectivity.

    AWS keys connectivity on the MQTT client id, so a device that connects as
    something other than the thing name leaves the thing offline — which is the
    honest answer, since nothing links the two.
    """
    _enable_fleet_indexing(iot_client)
    thing = _unique("assoc").replace("-", "")
    iot_client.create_thing(thingName=thing)

    async def _run():
        ws = await _mqtt_connect(thing + "other")
        try:
            assert _connectivity_for(iot_client, thing)["connected"] is False
        finally:
            await _mqtt_disconnect(ws)

    try:
        asyncio.run(_run())
    finally:
        iot_client.delete_thing(thingName=thing)


def test_search_index_connectivity_reports_a_dropped_transport(iot_client):
    _enable_fleet_indexing(iot_client)
    thing = _unique("drop").replace("-", "")
    iot_client.create_thing(thingName=thing)

    async def _run():
        ws = await _mqtt_connect(thing)
        _await_connected(iot_client, thing, True)
        # Close without sending DISCONNECT: the broker only sees the socket go.
        await _mqtt_disconnect(ws, graceful=False)
        assert _await_connected(iot_client, thing, False)[
            "disconnectReason"
        ] == "CONNECTION_LOST"

    try:
        asyncio.run(_run())
    finally:
        iot_client.delete_thing(thingName=thing)


def test_search_index_reports_duplicate_client_id_after_a_takeover(iot_client):
    """A takeover is visible in the index: the thing stays online, with the
    reason the session it replaced was evicted.

    This is the only surface DUPLICATE_CLIENTID has here, and it is the whole
    point of recording it: the eviction and the winning session's connect are
    one transition, so the reason describes how the current state came about.
    """
    _enable_fleet_indexing(iot_client)
    thing = _unique("takeover").replace("-", "")
    iot_client.create_thing(thingName=thing)

    async def _run():
        first = await _mqtt_connect(thing)
        _await_connected(iot_client, thing, True)
        second = await _mqtt_connect(thing)  # same client id: a takeover
        try:
            deadline = time.time() + 5
            doc = _connectivity_for(iot_client, thing)
            while doc.get("disconnectReason") is None and time.time() < deadline:
                time.sleep(0.05)
                doc = _connectivity_for(iot_client, thing)
            assert doc["connected"] is True, doc
            assert doc["disconnectReason"] == "DUPLICATE_CLIENTID", doc

            found = iot_client.search_index(
                queryString=(
                    f"thingName:{thing} AND "
                    "connectivity.disconnectReason:DUPLICATE_CLIENTID"
                )
            )["things"]
            assert [t["thingName"] for t in found] == [thing]
        finally:
            try:
                await first.close()  # the broker already closed it
            except Exception:
                pass
            await _mqtt_disconnect(second)

    try:
        asyncio.run(_run())
    finally:
        iot_client.delete_thing(thingName=thing)


def test_search_index_connectivity_is_isolated_across_accounts_and_regions():
    """A session in another account or region is not this thing's connectivity.

    The broker keys sessions by (account, region, client id); the index has to
    read the same key, or a device connecting in eu-west-1 would light up a
    same-named thing in us-east-1.
    """
    import boto3
    from botocore.config import Config

    account = f"{uuid.uuid4().int % 10**12:012d}"
    other_account = f"{uuid.uuid4().int % 10**12:012d}"

    def _client(account_id, region="us-east-1"):
        return boto3.client(
            "iot",
            endpoint_url=ENDPOINT,
            aws_access_key_id=account_id,
            aws_secret_access_key="test",
            region_name=region,
            config=Config(retries={"mode": "standard"}),
        )

    owner = _client(account)
    owner_eu = _client(account, "eu-west-1")
    for client in (owner, owner_eu):
        _enable_fleet_indexing(client)

    thing = _unique("iso").replace("-", "")
    owner.create_thing(thingName=thing)
    owner_eu.create_thing(thingName=thing)

    async def _run():
        # Same client id, different account: the owner's thing stays offline.
        ws = await _mqtt_connect(thing, _broker_ws_url_for(other_account))
        try:
            assert _connectivity_for(owner, thing)["connected"] is False
        finally:
            await _mqtt_disconnect(ws)

        # Same account, different region: still not this thing.
        ws = await _mqtt_connect(thing, _broker_ws_url_for(account, "eu-west-1"))
        try:
            assert _await_connected(owner_eu, thing, True)["connected"] is True
            assert _connectivity_for(owner, thing)["connected"] is False
        finally:
            await _mqtt_disconnect(ws)

    try:
        asyncio.run(_run())
    finally:
        owner.delete_thing(thingName=thing)
        owner_eu.delete_thing(thingName=thing)


# ---------------------------------------------------------------------------
# Topic-rule `sqs` action (publish → rule → SQS queue)
# ---------------------------------------------------------------------------


def _poll_message(sqs, url, timeout=10):
    """The first message on the queue, or None."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        if msgs.get("Messages"):
            return msgs["Messages"][0]
    return None


def _poll_body(sqs, url, timeout=10):
    """The raw body of the first message on the queue, or None."""
    msg = _poll_message(sqs, url, timeout)
    return msg["Body"] if msg else None


def test_iot_rule_sqs_action_delivers_payload(iot_client, iot_data_client, sqs):
    queue = sqs.create_queue(QueueName=_unique("rule-sqs"))["QueueUrl"]
    rule = _unique("tosqs").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'sensors/+/telemetry'",
            "actions": [{"sqs": {
                "queueUrl": queue,
                "roleArn": "arn:aws:iam::000000000000:role/aws_iot_sqs",
            }}],
        },
    )

    iot_data_client.publish(
        topic="sensors/a1/telemetry",
        payload=json.dumps({"temp": 22, "missedReadings": 3}).encode(),
    )
    assert json.loads(_poll_body(sqs, queue)) == {"temp": 22, "missedReadings": 3}

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_rule_sqs_action_use_base64_encodes_body(iot_client, iot_data_client, sqs):
    """``useBase64`` means the *body* is Base64 text, not a transport hint."""
    queue = sqs.create_queue(QueueName=_unique("rule-sqs-b64"))["QueueUrl"]
    rule = _unique("sqsb64").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'sensors/+/telemetry'",
            "actions": [{"sqs": {
                "queueUrl": queue,
                "useBase64": True,
                "roleArn": "arn:aws:iam::000000000000:role/aws_iot_sqs",
            }}],
        },
    )

    iot_data_client.publish(
        topic="sensors/a1/telemetry", payload=json.dumps({"temp": 7}).encode()
    )
    body = _poll_body(sqs, queue)
    assert json.loads(base64.b64decode(body)) == {"temp": 7}

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_rule_sqs_action_applies_select_projection(iot_client, iot_data_client, sqs):
    """The SELECT clause is evaluated once and applies to every action alike."""
    queue = sqs.create_queue(QueueName=_unique("rule-sqs-proj"))["QueueUrl"]
    rule = _unique("sqsproj").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT deviceId AS id, topic(2) AS device "
                   "FROM 'sensors/+/telemetry'",
            "actions": [{"sqs": {"queueUrl": queue, "roleArn": "arn:aws:iam::000000000000:role/aws_iot_sqs"}}],
        },
    )

    iot_data_client.publish(
        topic="sensors/a1/telemetry",
        payload=json.dumps({"deviceId": "d1", "temp": 22}).encode(),
    )
    assert json.loads(_poll_body(sqs, queue)) == {"id": "d1", "device": "a1"}

    iot_client.delete_topic_rule(ruleName=rule)


def _take_bodies(sqs, url, count, timeout=10):
    """The JSON bodies of the next ``count`` messages, deleted as they are read."""
    bodies = []
    deadline = time.time() + timeout
    while len(bodies) < count and time.time() < deadline:
        msg = _poll_message(sqs, url, timeout=max(0.0, deadline - time.time()))
        if msg:
            sqs.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
            bodies.append(json.loads(msg["Body"]))
    return bodies


def _rule_role(iam):
    """A role the rules engine can assume, so the rule also creates under AUTH=true."""
    name = _unique("rule-role")
    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": {"Service": "iot.amazonaws.com"}, "Action": "sts:AssumeRole"}
        ],
    }
    return iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]


def _iam_user_key(iam):
    """A fresh IAM user allowed to publish, its access key, and its GetCallerIdentity UserId."""
    user = _unique("publisher")
    iam.create_user(UserName=user)
    iam.put_user_policy(
        UserName=user,
        PolicyName="publish",
        PolicyDocument=json.dumps(
            {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "iot:*", "Resource": "*"}]}
        ),
    )
    key = iam.create_access_key(UserName=user)["AccessKey"]
    caller = boto3.client(
        "sts",
        endpoint_url=ENDPOINT,
        aws_access_key_id=key["AccessKeyId"],
        aws_secret_access_key=key["SecretAccessKey"],
        region_name="us-east-1",
    ).get_caller_identity()
    return user, key, caller["UserId"]


def _drop_iam_user(iam, user, key):
    iam.delete_access_key(UserName=user, AccessKeyId=key["AccessKeyId"])
    iam.delete_user_policy(UserName=user, PolicyName="publish")
    iam.delete_user(UserName=user)


def test_iot_rule_principal_is_the_http_callers_user_id(iot_client, iam, sqs, sts):
    """principal() of an HTTP Publish is the caller's UserId; every message gets its own trace id."""
    queue = sqs.create_queue(QueueName=_unique("rule-principal"))["QueueUrl"]
    topic = _unique("principal/http")
    rule = _unique("principal").replace("-", "_")
    user, key, user_id = _iam_user_key(iam)
    role = _rule_role(iam)
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": f"SELECT principal() AS p, traceid() AS t FROM '{topic}'",
            "actions": [{"sqs": {"queueUrl": queue, "roleArn": role["Arn"]}}],
        },
    )
    try:
        callers = (
            (key["AccessKeyId"], key["SecretAccessKey"], user_id),
            ("test", "test", sts.get_caller_identity()["UserId"]),
        )
        trace_ids = []
        for access_key, secret, expected in callers:
            boto3.client(
                "iot-data",
                endpoint_url=ENDPOINT,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret,
                region_name="us-east-1",
            ).publish(topic=topic, payload=b"{}")
            [event] = _take_bodies(sqs, queue, 1)
            assert event["p"] == expected
            assert str(uuid.UUID(event["t"])) == event["t"]
            trace_ids.append(event["t"])
        assert trace_ids[0] != trace_ids[1]
    finally:
        iot_client.delete_topic_rule(ruleName=rule)
        iam.delete_role(RoleName=role["RoleName"])
        _drop_iam_user(iam, user, key)


def test_iot_rule_principal_is_the_websocket_signers_user_id(iot_client, iam, sqs):
    """principal() and the connected event's principalIdentifier are the WebSocket signer's UserId."""
    queue = sqs.create_queue(QueueName=_unique("rule-ws-principal"))["QueueUrl"]
    topic = _unique("principal/ws")
    client_id = _unique("ws-signed")
    user, key, user_id = _iam_user_key(iam)
    role = _rule_role(iam)
    rules = {
        _unique("wspub").replace("-", "_"): f"SELECT 'publish' AS kind, principal() AS p FROM '{topic}'",
        _unique("wscon").replace("-", "_"): (
            "SELECT 'connected' AS kind, principalIdentifier AS p "
            f"FROM '$aws/events/presence/connected/{client_id}'"
        ),
    }
    for name, sql in rules.items():
        iot_client.create_topic_rule(
            ruleName=name,
            topicRulePayload={"sql": sql, "actions": [{"sqs": {"queueUrl": queue, "roleArn": role["Arn"]}}]},
        )
    parsed = urlparse(ENDPOINT)
    cred = quote(f"{key['AccessKeyId']}/20240101/us-east-1/iotdevicegateway/aws4_request")
    url = (
        f"ws://prefix-ats.iot.us-east-1.{parsed.hostname or 'localhost'}:{parsed.port or 4566}/mqtt"
        f"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={cred}"
    )

    async def _run():
        ws = await _mqtt_connect(client_id, url)
        await ws.send(_make_publish(topic, b"{}"))
        await asyncio.sleep(0.5)
        await _mqtt_disconnect(ws)

    try:
        with patch_endpoint_dns():
            asyncio.run(_run())
        events = {event["kind"]: event["p"] for event in _take_bodies(sqs, queue, 2)}
        assert events == {"publish": user_id, "connected": user_id}
    finally:
        for name in rules:
            iot_client.delete_topic_rule(ruleName=name)
        iam.delete_role(RoleName=role["RoleName"])
        _drop_iam_user(iam, user, key)


def test_iot_rule_sqs_action_missing_queue_does_not_stop_the_rule(
    iot_client, iot_data_client, sqs
):
    """The rule's other actions still run — and the failure is a failure like
    any other, so it reaches the rule's errorAction rather than being swallowed
    where only a log line would show it."""
    queue = sqs.create_queue(QueueName=_unique("rule-sqs-live"))["QueueUrl"]
    dlq = sqs.create_queue(QueueName=_unique("rule-sqs-dlq"))["QueueUrl"]
    gone = queue.rsplit("/", 1)[0] + "/" + _unique("no-such-queue")
    rule = _unique("sqsfail").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'sensors/+/telemetry'",
            "actions": [
                {"sqs": {"queueUrl": gone, "roleArn": "arn:aws:iam::000000000000:role/aws_iot_sqs"}},
                {"sqs": {"queueUrl": queue, "roleArn": "arn:aws:iam::000000000000:role/aws_iot_sqs"}},
            ],
            "errorAction": {
                "sqs": {"queueUrl": dlq, "roleArn": "arn:aws:iam::000000000000:role/aws_iot_sqs"}
            },
        },
    )

    iot_data_client.publish(
        topic="sensors/a1/telemetry", payload=json.dumps({"temp": 1}).encode()
    )
    assert json.loads(_poll_body(sqs, queue)) == {"temp": 1}

    doc = json.loads(_poll_body(sqs, dlq))
    assert doc["ruleName"] == rule
    assert doc["topic"] == "sensors/a1/telemetry"
    assert [f["failedAction"] for f in doc["failures"]] == ["SqsAction"]
    assert "QueueDoesNotExist" in doc["failures"][0]["errorMessage"]

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_rule_sqs_action_message_is_an_ordinary_sqs_message(
    iot_client, iot_data_client, sqs
):
    """Delivery goes through SQS's own SendMessage, so what lands on the queue is
    indistinguishable from any other producer's message — starting with a
    MessageId in the canonical dashed form a consumer may parse or key on."""
    queue = sqs.create_queue(QueueName=_unique("rule-sqs-mid"))["QueueUrl"]
    rule = _unique("sqsmid").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'sensors/+/telemetry'",
            "actions": [{"sqs": {"queueUrl": queue, "roleArn": "arn:aws:iam::000000000000:role/aws_iot_sqs"}}],
        },
    )

    iot_data_client.publish(
        topic="sensors/a1/telemetry", payload=json.dumps({"temp": 11}).encode()
    )
    msg = _poll_message(sqs, queue)
    assert msg is not None
    # uuid.UUID() also accepts the 32-char undashed form, so parsing alone would
    # not notice a hand-minted id; the round-trip is what pins the real shape.
    assert msg["MessageId"] == str(uuid.UUID(msg["MessageId"]))

    iot_client.delete_topic_rule(ruleName=rule)


def test_iot_rule_sqs_action_refuses_fifo_queue(iot_client, iot_data_client, sqs):
    """AWS does not support FIFO queues as an ``sqs`` action destination.

    The refusal is a deliberate skip, not a delivery failure: nothing lands
    on the FIFO queue AND the rule's ``errorAction`` stays quiet. Without the
    DLQ assertion this test could not tell the skip apart from the
    missing-queue case, which does fire the errorAction.
    """
    queue = sqs.create_queue(
        QueueName=_unique("rule-sqs")[:70] + ".fifo",
        Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
    )["QueueUrl"]
    dlq = sqs.create_queue(QueueName=_unique("rule-fifo-dlq"))["QueueUrl"]
    rule = _unique("sqsfifo").replace("-", "_")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM 'sensors/+/telemetry'",
            "actions": [{"sqs": {"queueUrl": queue, "roleArn": "arn:aws:iam::000000000000:role/aws_iot_sqs"}}],
            "errorAction": {
                "sqs": {"queueUrl": dlq, "roleArn": "arn:aws:iam::000000000000:role/aws_iot_sqs"}
            },
        },
    )

    iot_data_client.publish(
        topic="sensors/a1/telemetry", payload=json.dumps({"temp": 3}).encode()
    )
    assert _poll_body(sqs, queue, timeout=4) is None
    assert _poll_body(sqs, dlq, timeout=2) is None, (
        "a FIFO destination is skipped, not failed - the errorAction must not fire"
    )

    iot_client.delete_topic_rule(ruleName=rule)
# Device Shadow over MQTT (reserved $aws/things/.../shadow/... topics)
# ---------------------------------------------------------------------------


def _make_publish_frame(topic: str, payload: bytes) -> bytes:
    body = _enc_str(topic) + payload  # QoS 0: no packet identifier
    return bytes([0x30]) + _enc_remaining(len(body)) + body


def _default_ws_url() -> str:
    parsed = urlparse(ENDPOINT)
    ws_host = parsed.hostname or "localhost"
    ws_port = parsed.port or 4566
    return f"ws://prefix-ats.iot.us-east-1.{ws_host}:{ws_port}/mqtt"


def _collect_shadow_frames(sub_filter, publish_fn, want, timeout=5.0):
    """Subscribe over WS, run ``publish_fn`` once ready, and return the
    collected ``(topic, payload)`` PUBLISH frames."""
    from conftest import patch_endpoint_dns

    ready = threading.Event()
    stop = threading.Event()
    received: list = []

    with patch_endpoint_dns():
        t = threading.Thread(
            target=lambda: asyncio.run(
                _ws_subscribe_and_collect(
                    _default_ws_url(), sub_filter, ready, received, stop
                )
            ),
            daemon=True,
        )
        t.start()
        assert ready.wait(timeout=_WS_HANDSHAKE_TIMEOUT), "WebSocket subscriber did not become ready"

        publish_fn()

        deadline = time.time() + timeout
        while time.time() < deadline and len(received) < want:
            time.sleep(0.05)
        stop.set()
        t.join(timeout=2)
    return received


def test_shadow_update_over_mqtt_emits_accepted_delta_documents(iot_data_client):
    """HTTP `Publish` onto the shadow update topic drives the bridge: a WS
    subscriber sees accepted + delta + documents, and the HTTP data plane
    reads back the same stored state."""
    thing = _unique("shadow-thing")
    base = f"$aws/things/{thing}/shadow"

    received = _collect_shadow_frames(
        f"{base}/update/+",
        lambda: iot_data_client.publish(
            topic=f"{base}/update",
            payload=json.dumps(
                {"state": {"desired": {"led": "on"}}, "clientToken": "tok-a"}
            ).encode(),
        ),
        want=3,
    )

    frames = {topic: json.loads(payload) for topic, payload in received}
    accepted = frames[f"{base}/update/accepted"]
    assert accepted["state"] == {"desired": {"led": "on"}}
    assert accepted["clientToken"] == "tok-a"
    delta = frames[f"{base}/update/delta"]
    assert delta["state"] == {"led": "on"}
    assert delta["version"] == accepted["version"]
    # AWS includes the triggering request's clientToken on the delta, and the
    # metadata for the delta's attributes.
    assert delta["clientToken"] == "tok-a"
    assert set(delta["metadata"]) == {"led"}
    docs = frames[f"{base}/update/documents"]
    assert docs["previous"] is None
    assert docs["current"]["state"]["desired"] == {"led": "on"}

    shadow = _read_shadow(iot_data_client.get_thing_shadow(thingName=thing))
    assert shadow["state"]["desired"] == {"led": "on"}
    assert shadow["version"] == accepted["version"]


def test_shadow_update_over_http_emits_accepted_delta_documents(iot_data_client):
    """An HTTP `UpdateThingShadow` publishes the same state-change
    notifications as an MQTT update: accepted + delta + documents. On AWS the
    Device Shadow service emits these however the update arrived — it is what
    lets a topic rule on `update/accepted` fire for HTTP callers."""
    thing = _unique("shadow-http")
    base = f"$aws/things/{thing}/shadow"

    received = _collect_shadow_frames(
        f"{base}/update/+",
        lambda: iot_data_client.update_thing_shadow(
            thingName=thing,
            payload=json.dumps(
                {"state": {"desired": {"led": "on"}}, "clientToken": "tok-http"}
            ).encode(),
        ),
        want=3,
    )

    frames = {topic: json.loads(payload) for topic, payload in received}
    accepted = frames[f"{base}/update/accepted"]
    assert accepted["state"] == {"desired": {"led": "on"}}
    delta = frames[f"{base}/update/delta"]
    assert delta["state"] == {"led": "on"}
    assert delta["clientToken"] == "tok-http"
    docs = frames[f"{base}/update/documents"]
    # The envelope carries the triggering request's clientToken and the single
    # timestamp; previous/current are state + metadata + version only (a live
    # probe pinned the member keys — no inner timestamp).
    assert set(docs) == {"previous", "current", "timestamp", "clientToken"}
    assert docs["clientToken"] == "tok-http"
    assert docs["previous"] is None
    assert set(docs["current"]) == {"state", "metadata", "version"}
    assert docs["current"]["state"]["desired"] == {"led": "on"}


def test_shadow_delete_over_http_emits_delete_accepted_but_get_nothing(iot_data_client):
    """An HTTP `DeleteThingShadow` publishes `delete/accepted` (a state
    change); an HTTP `GetThingShadow` publishes nothing — `get/accepted` is
    the MQTT request's response channel and the HTTP read has its own
    response body."""
    thing = _unique("shadow-http-del")
    base = f"$aws/things/{thing}/shadow"
    iot_data_client.update_thing_shadow(
        thingName=thing,
        payload=json.dumps({"state": {"reported": {"v": 1}}}).encode(),
    )

    received = _collect_shadow_frames(
        f"{base}/get/+",
        lambda: iot_data_client.get_thing_shadow(thingName=thing),
        want=1,
        timeout=_WS_HANDSHAKE_TIMEOUT,
    )
    assert received == []

    received = _collect_shadow_frames(
        f"{base}/delete/+",
        lambda: iot_data_client.delete_thing_shadow(thingName=thing),
        want=1,
    )
    frames = {topic: json.loads(payload) for topic, payload in received}
    # AWS's delete/accepted body is abbreviated: version + timestamp only.
    assert set(frames[f"{base}/delete/accepted"]) == {"version", "timestamp"}


def test_named_shadow_update_over_http_emits_on_named_topics(iot_data_client):
    """An HTTP update of a named shadow notifies on that shadow's own
    `/name/<n>/update/...` topics, not the classic shadow's."""
    thing = _unique("shadow-http-named")
    base = f"$aws/things/{thing}/shadow/name/cfg"

    received = _collect_shadow_frames(
        f"{base}/update/+",
        lambda: iot_data_client.update_thing_shadow(
            thingName=thing,
            shadowName="cfg",
            payload=json.dumps({"state": {"desired": {"mode": "eco"}}}).encode(),
        ),
        want=3,
    )

    frames = {topic: json.loads(payload) for topic, payload in received}
    accepted = frames[f"{base}/update/accepted"]
    assert accepted["state"] == {"desired": {"mode": "eco"}}
    docs = frames[f"{base}/update/documents"]
    # No clientToken in the request → none on the documents envelope.
    assert set(docs) == {"previous", "current", "timestamp"}
    assert docs["current"]["state"]["desired"] == {"mode": "eco"}


def test_shadow_update_over_http_rejected_emits_no_frames(iot_data_client):
    """A failed HTTP update publishes nothing — no accepted trio and no
    `rejected` envelope, the HTTP 409 already carries the failure."""
    thing = _unique("shadow-http-conflict")
    base = f"$aws/things/{thing}/shadow"
    iot_data_client.update_thing_shadow(
        thingName=thing,
        payload=json.dumps({"state": {"reported": {"x": 1}}}).encode(),
    )

    def _stale_update():
        with pytest.raises(ClientError) as ei:
            iot_data_client.update_thing_shadow(
                thingName=thing,
                payload=json.dumps(
                    {"state": {"reported": {"x": 2}}, "version": 99}
                ).encode(),
            )
        assert ei.value.response["Error"]["Code"] == "ConflictException"

    received = _collect_shadow_frames(
        f"{base}/update/+", _stale_update, want=1, timeout=_WS_HANDSHAKE_TIMEOUT
    )
    assert received == []


def test_shadow_delete_over_http_missing_emits_no_frames(iot_data_client):
    """An HTTP delete of a shadow that does not exist changes no state, so
    nothing is published — the HTTP 404 is the whole answer."""
    thing = _unique("shadow-http-del-miss")
    base = f"$aws/things/{thing}/shadow"

    def _delete_missing():
        with pytest.raises(ClientError) as ei:
            iot_data_client.delete_thing_shadow(thingName=thing)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"

    received = _collect_shadow_frames(
        f"{base}/delete/+", _delete_missing, want=1, timeout=_WS_HANDSHAKE_TIMEOUT
    )
    assert received == []


def test_named_shadow_update_over_mqtt(iot_data_client):
    thing = _unique("shadow-named")
    base = f"$aws/things/{thing}/shadow/name/cfg"

    received = _collect_shadow_frames(
        f"{base}/update/+",
        lambda: iot_data_client.publish(
            topic=f"{base}/update",
            payload=json.dumps({"state": {"desired": {"mode": "eco"}}}).encode(),
        ),
        want=3,
    )

    frames = {topic: json.loads(payload) for topic, payload in received}
    accepted = frames[f"{base}/update/accepted"]
    assert accepted["state"] == {"desired": {"mode": "eco"}}

    named = _read_shadow(
        iot_data_client.get_thing_shadow(thingName=thing, shadowName="cfg")
    )
    assert named["state"]["desired"] == {"mode": "eco"}
    # Only the named shadow was written — the classic one must not exist.
    with pytest.raises(ClientError) as ei:
        iot_data_client.get_thing_shadow(thingName=thing)
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_shadow_get_missing_over_mqtt_rejected_404(iot_data_client):
    thing = _unique("shadow-miss")
    base = f"$aws/things/{thing}/shadow"

    received = _collect_shadow_frames(
        f"{base}/get/+",
        lambda: iot_data_client.publish(topic=f"{base}/get", payload=b""),
        want=1,
    )

    assert received, "expected a get/rejected frame"
    topic, payload = received[0]
    assert topic == f"{base}/get/rejected"
    doc = json.loads(payload)
    assert doc["code"] == 404
    assert thing in doc["message"]


async def _ws_publish_shadow_and_collect(
    ws_url, sub_filter, pub_topic, pub_payload, received, want
):
    """One WS session: subscribe, publish an MQTT PUBLISH frame on the same
    connection, and collect the PUBLISH frames the broker sends back."""

    def _record(msg) -> int | None:
        buf = bytes(msg if isinstance(msg, (bytes, bytearray)) else msg.encode("latin-1"))
        parsed = _parse_packet(buf)
        if not parsed:
            return None
        ptype, _flags, body, _ = parsed
        if ptype == 3:  # PUBLISH
            topic_len = struct.unpack_from("!H", body, 0)[0]
            received.append(
                (body[2:2 + topic_len].decode("utf-8"), body[2 + topic_len:])
            )
        return ptype

    async with websockets.connect(ws_url, subprotocols=["mqtt"]) as ws:
        await ws.send(_make_connect(_unique("shadow-dev")))
        await asyncio.wait_for(ws.recv(), timeout=_WS_HANDSHAKE_TIMEOUT)  # CONNACK
        await ws.send(_make_subscribe(packet_id=1, topic=sub_filter, qos=0))
        while True:
            msg = await asyncio.wait_for(ws.recv(), timeout=_WS_HANDSHAKE_TIMEOUT)
            if _record(msg) == 9:  # SUBACK
                break
        await ws.send(_make_publish_frame(pub_topic, pub_payload))
        end_at = time.time() + 5
        while time.time() < end_at and len(received) < want:
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=0.5)
            except asyncio.TimeoutError:
                continue
            _record(msg)


def test_shadow_update_via_ws_publish_emits_accepted(iot_data_client):
    """The WS PUBLISH path (a device publishing over MQTT) triggers the
    bridge too — the same session receives its update/accepted response."""
    from conftest import patch_endpoint_dns

    thing = _unique("shadow-wspub")
    base = f"$aws/things/{thing}/shadow"
    received: list = []

    with patch_endpoint_dns():
        asyncio.run(
            _ws_publish_shadow_and_collect(
                _default_ws_url(),
                f"{base}/update/accepted",
                f"{base}/update",
                json.dumps({"state": {"reported": {"fw": "9"}}}).encode(),
                received,
                want=1,
            )
        )

    assert received, "expected an update/accepted frame over the same WS session"
    topic, payload = received[0]
    assert topic == f"{base}/update/accepted"
    doc = json.loads(payload)
    assert doc["state"] == {"reported": {"fw": "9"}}

    shadow = _read_shadow(iot_data_client.get_thing_shadow(thingName=thing))
    assert shadow["state"]["reported"] == {"fw": "9"}


def test_shadow_accepted_drives_topic_rule_republish(iot_client, iot_data_client):
    """Rules-engine interplay: a rule on `$aws/things/+/shadow/update/accepted`
    fires on the bridge's own response publish (datasync-style pipelines)."""
    sink = _unique("shadow/sync")
    rule = _unique("shadowsync").replace("-", "_")
    thing = _unique("shadow-rule")
    iot_client.create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": "SELECT * FROM '$aws/things/+/shadow/update/accepted'",
            "actions": [
                {
                    "republish": {
                        "topic": sink,
                        "qos": 0,
                        "roleArn": "arn:aws:iam::000000000000:role/rule",
                    }
                }
            ],
        },
    )

    try:
        received = _collect_shadow_frames(
            sink,
            lambda: iot_data_client.publish(
                topic=f"$aws/things/{thing}/shadow/update",
                payload=json.dumps({"state": {"desired": {"sync": 1}}}).encode(),
            ),
            want=1,
        )

        assert received, "rule republish from update/accepted did not arrive"
        # The `+` thing wildcard means concurrently running shadow tests may
        # hit this rule too — find OUR update's projected event.
        events = [
            json.loads(payload) for topic, payload in received if topic == sink
        ]
        matching = [e for e in events if e.get("state") == {"desired": {"sync": 1}}]
        assert matching, f"expected our accepted-doc projection, got {events}"
        assert matching[0]["version"] >= 1
    finally:
        iot_client.delete_topic_rule(ruleName=rule)


# ============================================================
# MQTT 5.0 wire tests (negotiated per connection). Rolled in from the
# former tests/test_iot_mqtt5.py: same stdlib packet-building style,
# same MQTT-over-WebSocket endpoint. A 3.1.1 client keeps getting the
# exact bytes it got before; a 5.0 client gets property blocks,
# subscription options and reason codes.
# ============================================================


MQTT_311 = 4


MQTT_5 = 5


PKT_CONNACK = 2


PKT_PUBLISH = 3


PKT_PUBACK = 4


PKT_SUBACK = 9


PKT_UNSUBACK = 11


PKT_DISCONNECT = 14


RC5_MALFORMED_PACKET = 0x81


SUB_OPT_NO_LOCAL = 0x04


SUB_OPT_RETAIN_AS_PUBLISHED = 0x08


SUB_OPT_RETAIN_HANDLING_IF_NEW = 0x10


SUB_OPT_RETAIN_HANDLING_NEVER = 0x20


PROP_PAYLOAD_FORMAT_INDICATOR = 0x01


PROP_MESSAGE_EXPIRY_INTERVAL = 0x02


PROP_CONTENT_TYPE = 0x03


PROP_RESPONSE_TOPIC = 0x08


PROP_CORRELATION_DATA = 0x09


PROP_SESSION_EXPIRY_INTERVAL = 0x11


PROP_ASSIGNED_CLIENT_IDENTIFIER = 0x12


PROP_TOPIC_ALIAS_MAXIMUM = 0x22


PROP_MAXIMUM_QOS = 0x24


PROP_RETAIN_AVAILABLE = 0x25


PROP_USER_PROPERTY = 0x26


PROP_SHARED_SUBSCRIPTION_AVAILABLE = 0x2A


def _ws_url(region: str = "us-east-1") -> str:
    parsed = urlparse(ENDPOINT)
    host = parsed.hostname or "localhost"
    port = parsed.port or 4566
    return f"ws://prefix-ats.iot.{region}.{host}:{port}/mqtt"


def _dec_remaining(buf: bytes, off: int) -> tuple[int, int]:
    multiplier = 1
    value = 0
    while True:
        b = buf[off]
        off += 1
        value += (b & 0x7F) * multiplier
        if b & 0x80 == 0:
            return value, off
        multiplier *= 128


def _dec_str(buf: bytes, off: int) -> tuple[str, int]:
    n = struct.unpack_from("!H", buf, off)[0]
    off += 2
    return buf[off:off + n].decode("utf-8"), off + n


def _enc_props(props: list[tuple[int, object]]) -> bytes:
    """Encode a property block, length prefix included."""
    body = bytearray()
    for ident, value in props:
        body.append(ident)
        if ident in (PROP_PAYLOAD_FORMAT_INDICATOR,):
            body.append(int(value))  # type: ignore[arg-type]
        elif ident in (PROP_TOPIC_ALIAS_MAXIMUM,):
            body += struct.pack("!H", int(value))  # type: ignore[arg-type]
        elif ident in (PROP_MESSAGE_EXPIRY_INTERVAL, PROP_SESSION_EXPIRY_INTERVAL):
            body += struct.pack("!I", int(value))  # type: ignore[arg-type]
        elif ident in (PROP_CORRELATION_DATA,):
            body += struct.pack("!H", len(value)) + value  # type: ignore[arg-type]
        elif ident == PROP_USER_PROPERTY:
            name, pair_value = value  # type: ignore[misc]
            body += _enc_str(name) + _enc_str(pair_value)
        else:
            body += _enc_str(str(value))
    return _enc_remaining(len(body)) + bytes(body)


def _dec_props(buf: bytes, off: int) -> tuple[list[tuple[int, object]], int]:
    """Decode a property block, covering every property the broker emits."""
    length, off = _dec_remaining(buf, off)
    end = off + length
    props: list[tuple[int, object]] = []
    while off < end:
        ident = buf[off]
        off += 1
        if ident in (0x01, 0x17, 0x19, 0x24, 0x25, 0x28, 0x29, 0x2A):
            props.append((ident, buf[off]))
            off += 1
        elif ident in (0x13, 0x21, 0x22, 0x23):
            props.append((ident, struct.unpack_from("!H", buf, off)[0]))
            off += 2
        elif ident in (0x02, 0x11, 0x18, 0x27):
            props.append((ident, struct.unpack_from("!I", buf, off)[0]))
            off += 4
        elif ident in (0x09, 0x16):
            n = struct.unpack_from("!H", buf, off)[0]
            off += 2
            props.append((ident, buf[off:off + n]))
            off += n
        elif ident == PROP_USER_PROPERTY:
            name, off = _dec_str(buf, off)
            value, off = _dec_str(buf, off)
            props.append((ident, (name, value)))
        else:
            value, off = _dec_str(buf, off)
            props.append((ident, value))
    return props, end


def _prop(props: list[tuple[int, object]], ident: int, default: object = None) -> object:
    for prop_id, value in props:
        if prop_id == ident:
            return value
    return default


def _make_connect(
    client_id: str,
    protocol_level: int = MQTT_311,
    clean_start: bool = True,
    properties: list[tuple[int, object]] | None = None,
    protocol_name: str = "MQTT",
    raw_properties: bytes | None = None,
) -> bytes:
    body = (
        _enc_str(protocol_name)
        + bytes([protocol_level])
        + bytes([0x02 if clean_start else 0x00])
        + struct.pack("!H", 60)
    )
    if protocol_level == MQTT_5:
        body += raw_properties if raw_properties is not None else _enc_props(properties or [])
    body += _enc_str(client_id)
    return bytes([0x10]) + _enc_remaining(len(body)) + body


def _make_subscribe(
    packet_id: int,
    topic: str,
    qos: int = 0,
    protocol_level: int = MQTT_311,
    options: int | None = None,
) -> bytes:
    body = struct.pack("!H", packet_id)
    if protocol_level == MQTT_5:
        body += _enc_props([])
    body += _enc_str(topic) + bytes([qos if options is None else options])
    return bytes([0x82]) + _enc_remaining(len(body)) + body


def _make_unsubscribe(
    packet_id: int, topics: list[str], protocol_level: int = MQTT_311
) -> bytes:
    body = struct.pack("!H", packet_id)
    if protocol_level == MQTT_5:
        body += _enc_props([])
    body += b"".join(_enc_str(topic) for topic in topics)
    return bytes([0xA2]) + _enc_remaining(len(body)) + body


def _make_publish(
    topic: str,
    payload: bytes,
    qos: int = 0,
    packet_id: int | None = None,
    protocol_level: int = MQTT_311,
    properties: list[tuple[int, object]] | None = None,
    retain: bool = False,
    raw_properties: bytes | None = None,
) -> bytes:
    """Build a PUBLISH. ``raw_properties`` replaces the encoded property block
    verbatim, which is how the malformed-packet tests get a block the encoder
    would never produce."""
    body = _enc_str(topic)
    if qos > 0:
        body += struct.pack("!H", packet_id or 1)
    if protocol_level == MQTT_5:
        body += raw_properties if raw_properties is not None else _enc_props(properties or [])
    body += payload
    fixed = 0x30 | (qos << 1) | (0x01 if retain else 0)
    return bytes([fixed]) + _enc_remaining(len(body)) + body


def _make_disconnect(protocol_level: int = MQTT_311, reason_code: int = 0x00) -> bytes:
    if protocol_level == MQTT_5:
        body = bytes([reason_code]) + _enc_props([])
        return bytes([0xE0]) + _enc_remaining(len(body)) + body
    return bytes([0xE0, 0x00])


def _parse_publish(body: bytes, protocol_level: int, qos: int = 0) -> tuple[str, bytes, list]:
    topic, off = _dec_str(body, 0)
    if qos > 0:
        off += 2
    props: list[tuple[int, object]] = []
    if protocol_level == MQTT_5:
        props, off = _dec_props(body, off)
    return topic, body[off:], props


class _Client:
    """One MQTT connection over the broker's WebSocket endpoint."""

    def __init__(self, ws, protocol_level: int):
        self._ws = ws
        self.protocol_level = protocol_level
        self._buffer = bytearray()

    async def send(self, packet: bytes) -> None:
        await self._ws.send(packet)

    async def packet(self, timeout: float = 5.0) -> tuple[int, int, bytes]:
        """Return the next (type, flags, body); raises on timeout."""
        while True:
            parsed = self._take()
            if parsed is not None:
                return parsed
            frame = await asyncio.wait_for(self._ws.recv(), timeout=timeout)
            if isinstance(frame, str):
                frame = frame.encode("latin-1")
            self._buffer.extend(frame)

    def _take(self) -> tuple[int, int, bytes] | None:
        if len(self._buffer) < 2:
            return None
        remaining, header_end = _dec_remaining(bytes(self._buffer), 1)
        total = header_end + remaining
        if len(self._buffer) < total:
            return None
        first = self._buffer[0]
        body = bytes(self._buffer[header_end:total])
        del self._buffer[:total]
        return (first >> 4) & 0x0F, first & 0x0F, body

    async def await_packet(self, pkt_type: int, timeout: float = 5.0) -> tuple[int, bytes]:
        """Skip packets until ``pkt_type`` arrives (a retained PUBLISH may
        precede a SUBACK), returning its (flags, body)."""
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            left = deadline - asyncio.get_event_loop().time()
            ptype, flags, body = await self.packet(timeout=max(left, 0.1))
            if ptype == pkt_type:
                return flags, body


class _connect:
    """Async context manager yielding a connected ``_Client``."""

    def __init__(self, protocol_level: int = MQTT_311, client_id: str | None = None,
                 region: str = "us-east-1", **connect_kwargs):
        self._protocol_level = protocol_level
        self._client_id = _unique("cli") if client_id is None else client_id
        self._region = region
        self._connect_kwargs = connect_kwargs
        self._dns = None
        self._ws_cm = None

    async def __aenter__(self) -> tuple[_Client, bytes]:
        self._dns = patch_endpoint_dns()
        self._dns.__enter__()
        self._ws_cm = websockets.connect(_ws_url(self._region), subprotocols=["mqtt"])
        ws = await self._ws_cm.__aenter__()
        client = _Client(ws, self._protocol_level)
        await client.send(
            _make_connect(
                self._client_id,
                protocol_level=self._protocol_level,
                **self._connect_kwargs,
            )
        )
        _flags, connack = await client.await_packet(PKT_CONNACK)
        return client, connack

    async def __aexit__(self, *exc) -> None:
        try:
            await self._ws_cm.__aexit__(*exc)
        finally:
            self._dns.__exit__(None, None, None)


def _run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=30))


def test_mqtt5_connect_is_accepted_with_properties():
    """A 5.0 CONNECT gets a 5.0 CONNACK: reason code 0 plus a property block."""

    async def scenario():
        async with _connect(MQTT_5) as (_client, connack):
            return connack

    connack = _run(scenario())
    assert connack[0] == 0x00, "session present must be clear for a fresh session"
    assert connack[1] == 0x00, "reason code Success"
    props, end = _dec_props(connack, 2)
    assert end == len(connack)
    assert props, "a 5.0 CONNACK must carry a property block"
    assert _prop(props, PROP_MAXIMUM_QOS) == 1
    assert _prop(props, PROP_RETAIN_AVAILABLE) == 1
    # Features this broker does not implement are advertised as unavailable
    # rather than left at their permissive defaults.
    assert _prop(props, PROP_SHARED_SUBSCRIPTION_AVAILABLE) == 0
    assert _prop(props, PROP_TOPIC_ALIAS_MAXIMUM) == 0


def test_mqtt311_connack_is_byte_identical():
    """The 3.1.1 CONNACK is still exactly the four bytes it always was."""

    async def scenario():
        async with _connect(MQTT_311) as (client, connack):
            return bytes([0x20]) + _enc_remaining(len(connack)) + connack

    assert _run(scenario()) == b"\x20\x02\x00\x00"


def test_unsupported_protocol_level_is_refused():
    """Protocol level 3 (MQTT 3.1) gets CONNACK 0x01, not a mis-framed reply."""

    async def scenario():
        with patch_endpoint_dns():
            async with websockets.connect(_ws_url(), subprotocols=["mqtt"]) as ws:
                client = _Client(ws, MQTT_311)
                await client.send(
                    _make_connect("legacy", protocol_level=3, protocol_name="MQIsdp")
                )
                _flags, body = await client.await_packet(PKT_CONNACK)
                return body

    body = _run(scenario())
    assert body == b"\x00\x01", "session present 0, return code 0x01"


def test_mqtt5_empty_client_id_gets_assigned_identifier():
    """An empty client id is accepted and the generated one is handed back."""

    async def scenario():
        async with _connect(MQTT_5, client_id="") as (_client, connack):
            return connack

    connack = _run(scenario())
    props, _end = _dec_props(connack, 2)
    assigned = _prop(props, PROP_ASSIGNED_CLIENT_IDENTIFIER)
    assert isinstance(assigned, str) and assigned


def test_mqtt5_unknown_property_is_ignored():
    """A property the broker does not model does not derail the CONNECT parse."""

    async def scenario():
        # 0x7F is not a defined identifier; the parser must skip the rest of
        # the block and still find the client id behind it.
        async with _connect(MQTT_5, properties=[(0x7F, "whatever")]) as (_c, connack):
            return connack

    connack = _run(scenario())
    assert connack[1] == 0x00


def test_mqtt5_subscribe_options_byte_gets_v5_suback():
    """The 5.0 options byte is read for its QoS bits; SUBACK carries properties."""

    async def scenario():
        async with _connect(MQTT_5) as (client, _connack):
            # QoS 1 in bits 0-1, No Local set in bit 2, Retain Handling 1 in
            # bits 4-5 — only the QoS bits are modelled.
            await client.send(
                _make_subscribe(
                    1, _unique("opts/topic"), protocol_level=MQTT_5, options=0x15
                )
            )
            _flags, body = await client.await_packet(PKT_SUBACK)
            return body

    body = _run(scenario())
    assert struct.unpack_from("!H", body, 0)[0] == 1
    props, off = _dec_props(body, 2)
    assert props == []
    assert body[off:] == bytes([0x01]), "reason code Granted QoS 1"


def test_mqtt5_publish_round_trip_forwards_properties():
    """A 5.0 publisher reaches a 5.0 subscriber, properties included."""
    topic = _unique("v5/round/trip")
    payload = b"v5-telemetry"
    sent_props = [
        (PROP_PAYLOAD_FORMAT_INDICATOR, 1),
        (PROP_CONTENT_TYPE, "application/json"),
        (PROP_RESPONSE_TOPIC, f"{topic}/reply"),
        (PROP_CORRELATION_DATA, b"\x01\x02\x03"),
        (PROP_USER_PROPERTY, ("tenant", "acme")),
    ]

    async def scenario():
        async with _connect(MQTT_5) as (sub, _c1):
            await sub.send(_make_subscribe(1, topic, protocol_level=MQTT_5, options=0x00))
            await sub.await_packet(PKT_SUBACK)
            async with _connect(MQTT_5) as (pub, _c2):
                await pub.send(
                    _make_publish(topic, payload, protocol_level=MQTT_5,
                                  properties=sent_props)
                )
                _flags, body = await sub.await_packet(PKT_PUBLISH)
                return body

    body = _run(scenario())
    delivered_topic, delivered_payload, props = _parse_publish(body, MQTT_5)
    assert delivered_topic == topic
    assert delivered_payload == payload
    assert _prop(props, PROP_CONTENT_TYPE) == "application/json"
    assert _prop(props, PROP_RESPONSE_TOPIC) == f"{topic}/reply"
    assert _prop(props, PROP_CORRELATION_DATA) == b"\x01\x02\x03"
    assert _prop(props, PROP_USER_PROPERTY) == ("tenant", "acme")


def test_http_publish_forwards_mqtt5_properties(iot_data_client):
    """HTTP Publish hands its MQTT 5 headers and query parameters to a 5.0 subscriber as properties."""
    topic = _unique("v5/http")
    publishes = [
        {
            "userProperties": [{"app": "demo"}, {"k": "v1"}, {"k": "v2"}, {"u": "ü"}],
            "payloadFormatIndicator": "UTF8_DATA",
            "contentType": "application/json",
            "responseTopic": f"{topic}/reply",
            "correlationData": base64.b64encode(b"\x00\x01req-7").decode(),
            "messageExpiry": 60,
        },
        # Unpadded base64, an empty object, empty strings and an interval above
        # the broker's seven-day limit.
        {
            "payloadFormatIndicator": "UNSPECIFIED_BYTES",
            "correlationData": "cmVxLTc",
            "userProperties": [{}],
            "contentType": "",
            "responseTopic": "",
            "messageExpiry": 604801,
        },
        {},
    ]

    async def scenario():
        async with _connect(MQTT_5) as (sub, _c1):
            await sub.send(_make_subscribe(1, topic, protocol_level=MQTT_5, options=0x00))
            await sub.await_packet(PKT_SUBACK)
            delivered = []
            for kwargs in publishes:
                await asyncio.to_thread(iot_data_client.publish, topic=topic, payload=b"{}", **kwargs)
                _flags, body = await sub.await_packet(PKT_PUBLISH)
                delivered.append(_parse_publish(body, MQTT_5)[2])
            return delivered

    full, edges, none = _run(scenario())
    assert full == [
        (PROP_PAYLOAD_FORMAT_INDICATOR, 1),
        (0x02, 59),
        (PROP_RESPONSE_TOPIC, f"{topic}/reply"),
        (PROP_CORRELATION_DATA, b"\x00\x01req-7"),
        (PROP_CONTENT_TYPE, "application/json"),
        (PROP_USER_PROPERTY, ("app", "demo")),
        (PROP_USER_PROPERTY, ("k", "v1")),
        (PROP_USER_PROPERTY, ("k", "v2")),
        (PROP_USER_PROPERTY, ("u", "ü")),
    ]
    assert edges == [
        (PROP_PAYLOAD_FORMAT_INDICATOR, 0),
        (0x02, 604799),
        (PROP_CORRELATION_DATA, b"req-7"),
    ]
    assert none == []


def test_mqtt311_publisher_reaches_mqtt5_subscriber():
    """Cross-version delivery: the 5.0 subscriber gets an empty property block."""
    topic = _unique("cross/311to5")

    async def scenario():
        async with _connect(MQTT_5) as (sub, _c1):
            await sub.send(_make_subscribe(1, topic, protocol_level=MQTT_5, options=0x00))
            await sub.await_packet(PKT_SUBACK)
            async with _connect(MQTT_311) as (pub, _c2):
                await pub.send(_make_publish(topic, b"from-311"))
                _flags, body = await sub.await_packet(PKT_PUBLISH)
                return body

    body = _run(scenario())
    delivered_topic, payload, props = _parse_publish(body, MQTT_5)
    assert delivered_topic == topic
    assert payload == b"from-311"
    assert props == []


def test_mqtt5_publisher_reaches_mqtt311_subscriber():
    """The 3.1.1 subscriber sees no property field at all — its packet is unchanged."""
    topic = _unique("cross/5to311")

    async def scenario():
        async with _connect(MQTT_311) as (sub, _c1):
            await sub.send(_make_subscribe(1, topic, qos=0))
            await sub.await_packet(PKT_SUBACK)
            async with _connect(MQTT_5) as (pub, _c2):
                await pub.send(
                    _make_publish(
                        topic, b"from-v5", protocol_level=MQTT_5,
                        properties=[(PROP_USER_PROPERTY, ("tenant", "acme"))],
                    )
                )
                _flags, body = await sub.await_packet(PKT_PUBLISH)
                return body

    body = _run(scenario())
    delivered_topic, payload, _props = _parse_publish(body, MQTT_311)
    assert delivered_topic == topic
    assert payload == b"from-v5", "no property block may be spliced in front"


def test_mqtt5_qos1_puback_carries_reason_code():
    """QoS 1 PUBACK reports Success with a subscriber and without one."""
    topic = _unique("v5/qos1")

    async def scenario():
        async with _connect(MQTT_5) as (pub, _c1):
            await pub.send(
                _make_publish(topic, b"nobody-home", qos=1, packet_id=7,
                              protocol_level=MQTT_5)
            )
            _flags, lonely = await pub.await_packet(PKT_PUBACK)
            async with _connect(MQTT_5) as (sub, _c2):
                await sub.send(
                    _make_subscribe(1, topic, protocol_level=MQTT_5, options=0x01)
                )
                await sub.await_packet(PKT_SUBACK)
                await pub.send(
                    _make_publish(topic, b"delivered", qos=1, packet_id=8,
                                  protocol_level=MQTT_5)
                )
                _flags, matched = await pub.await_packet(PKT_PUBACK)
                return lonely, matched

    lonely, matched = _run(scenario())
    assert lonely == b"\x00\x07\x00\x00", "packet id 7, Success, no properties"
    assert matched == b"\x00\x08\x00\x00", "packet id 8, Success, no properties"


@pytest.mark.parametrize("topic,acknowledged", [
    ("$aws/rules/no-such-rule/x", True),
    ("$aws/things/some-thing/shadow/update", True),
    ("$aws/events/certificates/registered/abc", False),
    ("$aws/jobs/abc", False),
    ("$foo/abc", False),
], ids=["rules", "shadow", "events", "jobs", "other-dollar"])
def test_mqtt_publish_to_reserved_topic_closes_the_connection(topic, acknowledged):
    """AWS acknowledges a publish under $aws/rules/ and $aws/things/ and closes
    the connection of a client that publishes to any other $ topic (measured
    over MQTT 3.1.1); the HTTPS Publish applies the same list."""

    async def scenario():
        async with _connect(MQTT_311) as (pub, _c):
            await pub.send(_make_publish(topic, b"x", qos=1, packet_id=5))
            try:
                _flags, body = await pub.await_packet(PKT_PUBACK, timeout=3)
                return ("puback", body)
            except (websockets.exceptions.ConnectionClosed, OSError) as e:
                return ("closed", type(e).__name__)

    kind, detail = _run(scenario())
    assert kind == ("puback" if acknowledged else "closed"), (kind, detail)
    if acknowledged:
        assert detail == b"\x00\x05"


def test_mqtt311_qos1_puback_stays_two_bytes():
    """The 3.1.1 PUBACK gains neither a reason code nor properties."""
    topic = _unique("v311/qos1")

    async def scenario():
        async with _connect(MQTT_311) as (pub, _c1):
            await pub.send(_make_publish(topic, b"x", qos=1, packet_id=3))
            _flags, body = await pub.await_packet(PKT_PUBACK)
            return body

    assert _run(scenario()) == b"\x00\x03"


def test_mqtt5_unsuback_reason_codes_follow_the_actual_removal():
    """A 5.0 UNSUBACK reports one reason code per topic filter, and each one is
    read off what the removal actually did: Success (0x00) for a filter the
    session held, No subscription existed (0x11) for one it never had.

    The same scenario pins the removal itself, because the reason codes alone
    cannot tell an honest per-filter unsubscribe from an unsubscribe-everything
    loop that answers per filter. The filter the packet does not name has to
    keep delivering — the 3.1.1 rule of ``test_unsubscribe_removes_only_the_
    named_filters`` in ``test_iot.py``, seen here through the 5.0 path.
    """
    kept = _unique("v5/unsub/kept")
    dropped = _unique("v5/unsub/dropped")
    unknown = _unique("v5/unsub/never")

    async def scenario():
        async with _connect(MQTT_5) as (sub, _c1):
            for packet_id, topic in enumerate((kept, dropped), start=1):
                await sub.send(
                    _make_subscribe(packet_id, topic, protocol_level=MQTT_5, options=0x00)
                )
                await sub.await_packet(PKT_SUBACK)

            await sub.send(
                _make_unsubscribe(3, [dropped, unknown], protocol_level=MQTT_5)
            )
            _flags, unsuback = await sub.await_packet(PKT_UNSUBACK)

            async with _connect(MQTT_5) as (pub, _c2):
                # The unsubscribed topic goes first, so a delivery that should
                # not happen is the *first* PUBLISH to arrive rather than a
                # timing question.
                await pub.send(
                    _make_publish(dropped, b"should-not-arrive", protocol_level=MQTT_5)
                )
                await pub.send(
                    _make_publish(kept, b"should-arrive", protocol_level=MQTT_5)
                )
                _flags, delivered = await sub.await_packet(PKT_PUBLISH)
                try:
                    extra = await sub.packet(timeout=0.5)
                except (asyncio.TimeoutError, TimeoutError):
                    extra = None
            return unsuback, delivered, extra

    unsuback, delivered, extra = _run(scenario())

    assert struct.unpack_from("!H", unsuback, 0)[0] == 3
    props, off = _dec_props(unsuback, 2)
    assert props == []
    assert unsuback[off:] == bytes([0x00, 0x11]), (
        "Success for the held filter, No subscription existed for the unknown one"
    )

    delivered_topic, payload, _props = _parse_publish(delivered, MQTT_5)
    assert (delivered_topic, payload) == (kept, b"should-arrive")
    assert extra is None, "no further packet — the unsubscribed topic stays silent"


def test_mqtt311_unsuback_stays_two_bytes():
    """The 3.1.1 UNSUBACK gains neither a property block nor reason codes."""
    topic = _unique("v311/unsub")

    async def scenario():
        async with _connect(MQTT_311) as (client, _connack):
            await client.send(_make_subscribe(1, topic, qos=0))
            await client.await_packet(PKT_SUBACK)
            await client.send(_make_unsubscribe(4, [topic, _unique("v311/never")]))
            _flags, body = await client.await_packet(PKT_UNSUBACK)
            return body

    assert _run(scenario()) == b"\x00\x04"


def test_mqtt5_disconnect_with_reason_code_closes_cleanly():
    """A 5.0 DISCONNECT carrying a reason code and properties ends the session."""

    async def scenario():
        async with _connect(MQTT_5) as (client, _connack):
            await client.send(_make_disconnect(MQTT_5, reason_code=0x00))
            try:
                await client.packet(timeout=5.0)
            except websockets.exceptions.ConnectionClosed:
                return "closed"
            return "still open"

    assert _run(scenario()) == "closed"


def test_mqtt5_property_value_running_past_the_block_is_refused():
    """A property whose value is missing gets a reason code, not a bleed-over.

    The block below declares one byte and holds only the Payload Format
    Indicator's identifier: its value would have to come from the byte after
    the block, which is the first byte of the payload. Reading it there costs
    nothing at parse time and shows up as a subscriber receiving a property
    the publisher never sent, on a payload with its first byte eaten — so the
    subscriber here is what proves the block boundary held.
    """
    topic = _unique("malformed/bleed")

    async def scenario():
        async with _connect(MQTT_5) as (subscriber, _connack):
            await subscriber.send(_make_subscribe(1, topic, protocol_level=MQTT_5))
            await subscriber.await_packet(PKT_SUBACK)

            async with _connect(MQTT_5) as (publisher, _c2):
                await publisher.send(
                    _make_publish(
                        topic, b"bleed", protocol_level=MQTT_5,
                        raw_properties=bytes([0x01, PROP_PAYLOAD_FORMAT_INDICATOR]),
                    )
                )
                pkt_type, _flags, disconnect = await publisher.packet(timeout=5.0)

            async with _connect(MQTT_5) as (other, _c3):
                await other.send(
                    _make_publish(topic, b"clean", protocol_level=MQTT_5)
                )
                _flags, delivered = await subscriber.await_packet(PKT_PUBLISH)

            return pkt_type, disconnect, delivered

    pkt_type, disconnect, delivered = _run(scenario())
    assert pkt_type == PKT_DISCONNECT, "a v5 client is told why it is being cut off"
    assert disconnect[0] == RC5_MALFORMED_PACKET
    delivered_topic, payload, props = _parse_publish(delivered, MQTT_5)
    assert (delivered_topic, payload) == (topic, b"clean"), (
        "the malformed publish must not have been delivered at all"
    )
    assert _prop(props, PROP_PAYLOAD_FORMAT_INDICATOR) is None


def test_mqtt5_property_block_truncated_at_the_packet_end_is_refused():
    """The same truncation with nothing after it: the read runs off the packet.

    With no payload behind the block there is no byte to borrow, so this is
    the shape that used to raise out of the parser and take the connection
    down with no packet sent — the client saw only a closed socket.
    """
    topic = _unique("malformed/end")

    async def scenario():
        async with _connect(MQTT_5) as (client, _connack):
            await client.send(
                _make_publish(
                    topic, b"", protocol_level=MQTT_5,
                    raw_properties=bytes([0x01, PROP_PAYLOAD_FORMAT_INDICATOR]),
                )
            )
            pkt_type, _flags, body = await client.packet(timeout=5.0)
            return pkt_type, body

    pkt_type, body = _run(scenario())
    assert pkt_type == PKT_DISCONNECT
    assert body[0] == RC5_MALFORMED_PACKET


def test_mqtt5_malformed_connect_is_refused_with_a_connack_reason_code():
    """Before a CONNACK there is no DISCONNECT to send, so the CONNACK carries it.

    MQTT 5 §4.13.1 lets the server answer a malformed CONNECT with a CONNACK
    reason code and close; a DISCONNECT would be the one packet the client
    cannot be sent yet.
    """

    async def scenario():
        with patch_endpoint_dns():
            async with websockets.connect(_ws_url(), subprotocols=["mqtt"]) as ws:
                client = _Client(ws, MQTT_5)
                await client.send(
                    _make_connect(
                        "malformed-connect", protocol_level=MQTT_5,
                        # Session Expiry Interval is four bytes wide; two are
                        # given, and the client id follows the block.
                        raw_properties=bytes(
                            [0x03, PROP_SESSION_EXPIRY_INTERVAL, 0x00, 0x00]
                        ),
                    )
                )
                pkt_type, _flags, body = await client.packet(timeout=5.0)
                return pkt_type, body

    pkt_type, body = _run(scenario())
    assert pkt_type == PKT_CONNACK
    assert body[0] == 0x00, "session present clear"
    assert body[1] == RC5_MALFORMED_PACKET


def test_mqtt5_property_block_longer_than_127_bytes_round_trips():
    """A block over 127 bytes needs the two-byte varint length on both sides."""
    topic = _unique("props/varint")
    user_properties = [
        (PROP_USER_PROPERTY, (f"key-{i:02d}", f"value-{i:02d}")) for i in range(10)
    ]

    async def scenario():
        async with _connect(MQTT_5) as (subscriber, _connack):
            await subscriber.send(_make_subscribe(1, topic, protocol_level=MQTT_5))
            await subscriber.await_packet(PKT_SUBACK)
            async with _connect(MQTT_5) as (publisher, _c2):
                sent = _make_publish(
                    topic, b"long-props", protocol_level=MQTT_5,
                    properties=user_properties,
                )
                await publisher.send(sent)
                _flags, delivered = await subscriber.await_packet(PKT_PUBLISH)
            return delivered

    delivered = _run(scenario())
    _topic, off = _dec_str(delivered, 0)
    assert delivered[off] & 0x80, (
        "the property length must have spilled into a second varint byte"
    )
    delivered_topic, payload, props = _parse_publish(delivered, MQTT_5)
    assert (delivered_topic, payload) == (topic, b"long-props")
    assert [p for p in props if p[0] == PROP_USER_PROPERTY] == user_properties


def test_mqtt5_no_local_withholds_a_publisher_its_own_message():
    """No Local is defined against the publishing client, not the connection."""
    topic = _unique("options/nolocal")

    async def scenario():
        async with _connect(MQTT_5) as (subscriber, _connack):
            await subscriber.send(
                _make_subscribe(1, topic, protocol_level=MQTT_5,
                                options=SUB_OPT_NO_LOCAL)
            )
            await subscriber.await_packet(PKT_SUBACK)

            await subscriber.send(
                _make_publish(topic, b"mine", qos=1, packet_id=7,
                              protocol_level=MQTT_5)
            )
            # The next packet, not the next PUBACK: a self-delivery would
            # arrive ahead of the acknowledgement and be skipped past.
            own_reply = await subscriber.packet(timeout=5.0)

            async with _connect(MQTT_5) as (other, _c2):
                await other.send(
                    _make_publish(topic, b"theirs", protocol_level=MQTT_5)
                )
                _flags, delivered = await subscriber.await_packet(PKT_PUBLISH)
            return own_reply, delivered

    (pkt_type, _flags, puback), delivered = _run(scenario())
    assert pkt_type == PKT_PUBACK, "the publisher's own message must not come back"
    assert puback == b"\x00\x07\x00\x00", "packet id 7, Success, no properties"
    assert _parse_publish(delivered, MQTT_5)[1] == b"theirs"


def test_mqtt5_retain_as_published_keeps_the_publishers_retain_flag():
    """Two subscribers, one option apart, see different RETAIN bits."""
    topic = _unique("options/rap")

    async def scenario():
        async with _connect(MQTT_5) as (keeps_flag, _c1):
            await keeps_flag.send(
                _make_subscribe(1, topic, protocol_level=MQTT_5,
                                options=SUB_OPT_RETAIN_AS_PUBLISHED)
            )
            await keeps_flag.await_packet(PKT_SUBACK)
            async with _connect(MQTT_5) as (clears_flag, _c2):
                await clears_flag.send(_make_subscribe(1, topic, protocol_level=MQTT_5))
                await clears_flag.await_packet(PKT_SUBACK)
                async with _connect(MQTT_5) as (publisher, _c3):
                    await publisher.send(
                        _make_publish(topic, b"sticky", protocol_level=MQTT_5,
                                      retain=True)
                    )
                    kept, _body = await keeps_flag.await_packet(PKT_PUBLISH)
                    cleared, _body2 = await clears_flag.await_packet(PKT_PUBLISH)
                return kept, cleared

    kept, cleared = _run(scenario())
    assert kept & 0x01 == 1, "Retain As Published forwards the flag as sent"
    assert cleared & 0x01 == 0, "without it the flag is cleared"


def test_mqtt5_retain_handling_2_suppresses_the_retained_replay():
    """Retain Handling 2 means: subscribe me, but keep the retained message."""
    topic = _unique("options/rh2")

    async def scenario():
        async with _connect(MQTT_5) as (publisher, _c1):
            await publisher.send(
                _make_publish(topic, b"stored", qos=1, packet_id=3,
                              protocol_level=MQTT_5, retain=True)
            )
            await publisher.await_packet(PKT_PUBACK)
            async with _connect(MQTT_5) as (subscriber, _c2):
                await subscriber.send(
                    _make_subscribe(1, topic, protocol_level=MQTT_5,
                                    options=SUB_OPT_RETAIN_HANDLING_NEVER)
                )
                pkt_type, _flags, _body = await subscriber.packet(timeout=5.0)
                return pkt_type

    assert _run(scenario()) == PKT_SUBACK, (
        "the retained PUBLISH precedes the SUBACK, so a SUBACK first means none was sent"
    )


def test_mqtt5_retain_handling_1_replays_only_for_a_new_subscription():
    """Retain Handling 1 replays on the first SUBSCRIBE and not on a repeat."""
    topic = _unique("options/rh1")

    async def scenario():
        async with _connect(MQTT_5) as (publisher, _c1):
            await publisher.send(
                _make_publish(topic, b"stored", qos=1, packet_id=3,
                              protocol_level=MQTT_5, retain=True)
            )
            await publisher.await_packet(PKT_PUBACK)
            async with _connect(MQTT_5) as (subscriber, _c2):
                options = SUB_OPT_RETAIN_HANDLING_IF_NEW
                await subscriber.send(
                    _make_subscribe(1, topic, protocol_level=MQTT_5, options=options)
                )
                first = await subscriber.packet(timeout=5.0)
                await subscriber.await_packet(PKT_SUBACK)
                await subscriber.send(
                    _make_subscribe(2, topic, protocol_level=MQTT_5, options=options)
                )
                repeat = await subscriber.packet(timeout=5.0)
                return first, repeat

    (first_type, first_flags, first_body), (repeat_type, _f, _b) = _run(scenario())
    assert first_type == PKT_PUBLISH
    # A retained message keeps the QoS it was published at, so the packet
    # identifier's two bytes are there to be stepped over.
    first_qos = (first_flags >> 1) & 0x03
    assert _parse_publish(first_body, MQTT_5, qos=first_qos)[1] == b"stored"
    assert first_flags & 0x01 == 1, (
        "a message sent because a subscription was established carries RETAIN 1"
    )
    assert repeat_type == PKT_SUBACK, "the subscription already existed"


def test_mqtt5_message_expiry_counts_down_on_delivery():
    """A subscriber gets the expiry interval less the started second; 0 counts as 1, the cap is 604800."""
    topic = _unique("v5/expiry/live")
    intervals = [60, 1, 0, 604801, 0xFFFFFFFF]

    async def scenario():
        async with _connect(MQTT_5) as (sub, _c1):
            await sub.send(_make_subscribe(1, topic, protocol_level=MQTT_5))
            await sub.await_packet(PKT_SUBACK)
            async with _connect(MQTT_5) as (pub, _c2):
                delivered = []
                for interval in intervals:
                    await pub.send(
                        _make_publish(topic, b"m", protocol_level=MQTT_5,
                                      properties=[(PROP_MESSAGE_EXPIRY_INTERVAL, interval)])
                    )
                    _flags, body = await sub.await_packet(PKT_PUBLISH)
                    delivered.append(_prop(_parse_publish(body, MQTT_5)[2], PROP_MESSAGE_EXPIRY_INTERVAL))
                return delivered

    assert _run(scenario()) == [59, 0, 0, 604799, 604799]


def test_mqtt5_expired_retained_message_is_no_longer_served(iot_data_client):
    """Once its expiry interval has passed, a retained message reaches neither a new subscriber nor iot-data."""
    short, kept = _unique("v5/expiry/short"), _unique("v5/expiry/kept")

    async def scenario():
        async with _connect(MQTT_5) as (pub, _c1):
            for packet_id, (topic, interval) in enumerate(((short, 1), (kept, 60)), start=1):
                await pub.send(
                    _make_publish(topic, b"stored", qos=1, packet_id=packet_id,
                                  protocol_level=MQTT_5, retain=True,
                                  properties=[(PROP_MESSAGE_EXPIRY_INTERVAL, interval)])
                )
                await pub.await_packet(PKT_PUBACK)
        before = await asyncio.to_thread(iot_data_client.get_retained_message, topic=short)
        await asyncio.sleep(1.2)
        async with _connect(MQTT_5) as (sub, _c2):
            await sub.send(_make_subscribe(1, short, protocol_level=MQTT_5))
            first_type, _flags, _body = await sub.packet()
            await sub.send(_make_subscribe(2, kept, protocol_level=MQTT_5))
            flags, body = await sub.await_packet(PKT_PUBLISH)
        return before, first_type, _parse_publish(body, MQTT_5, qos=(flags >> 1) & 0x03)[2]

    before, first_type, kept_props = _run(scenario())
    assert before["payload"] == b"stored"
    assert first_type == PKT_SUBACK, "the expired message is not replayed"
    assert 50 <= _prop(kept_props, PROP_MESSAGE_EXPIRY_INTERVAL) <= 58, "lowered by the time it waited"
    with pytest.raises(ClientError) as exc:
        iot_data_client.get_retained_message(topic=short)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    pages = iot_data_client.get_paginator("list_retained_messages").paginate()
    listed = {t["topic"] for page in pages for t in page["retainedTopics"]}
    assert kept in listed and short not in listed


def test_mqtt5_queued_message_expires_while_the_session_is_offline():
    """A resumed session gets only the queued messages still inside their interval, properties lowered."""
    client_id, base = _unique("queue-expiry"), _unique("v5/expiry/queued")
    session = {"clean_start": False, "properties": [(PROP_SESSION_EXPIRY_INTERVAL, 300)]}

    async def scenario():
        async with _connect(MQTT_5, client_id=client_id, **session) as (sub, _c1):
            await sub.send(_make_subscribe(1, f"{base}/#", qos=1, protocol_level=MQTT_5))
            await sub.await_packet(PKT_SUBACK)
            await sub.send(_make_disconnect(MQTT_5))
        await asyncio.sleep(0.5)
        async with _connect(MQTT_5) as (pub, _c2):
            for packet_id, (leaf, interval) in enumerate((("short", 1), ("long", 60), ("none", None)), start=1):
                props = [] if interval is None else [(PROP_MESSAGE_EXPIRY_INTERVAL, interval)]
                await pub.send(
                    _make_publish(f"{base}/{leaf}", b"q", qos=1, packet_id=packet_id,
                                  protocol_level=MQTT_5, properties=props)
                )
                await pub.await_packet(PKT_PUBACK)
        await asyncio.sleep(1.2)
        delivered = {}
        async with _connect(MQTT_5, client_id=client_id, **session) as (back, connack):
            try:
                while True:
                    flags, body = await back.await_packet(PKT_PUBLISH, timeout=1.0)
                    topic, _payload, props = _parse_publish(body, MQTT_5, qos=(flags >> 1) & 0x03)
                    delivered[topic.rsplit("/", 1)[1]] = _prop(props, PROP_MESSAGE_EXPIRY_INTERVAL, "absent")
            except asyncio.TimeoutError:
                pass
            await back.send(_make_disconnect(MQTT_5))
        return connack, delivered

    connack, delivered = _run(scenario())
    assert connack[0] & 0x01 == 1, "the session was resumed"
    assert set(delivered) == {"long", "none"}
    assert 50 <= delivered["long"] <= 58
    assert delivered["none"] == "absent"


def _make_connect_with_will(
    client_id: str,
    will_topic: str,
    will_payload: bytes,
    will_properties: list[tuple[int, object]] | None = None,
    raw_will_properties: bytes | None = None,
    will_qos: int = 0,
    will_retain: bool = False,
) -> bytes:
    """A v5 CONNECT carrying a will.

    Kept separate from ``_make_connect`` so the will-free packets the rest of
    the file builds stay byte-identical. In a v5 CONNECT the will *property
    block precedes the will topic*, so a defect in its parsing corrupts the
    offset of everything after it — which is exactly what these tests pin.
    """
    flags = 0x02 | 0x04 | (will_qos << 3) | (0x20 if will_retain else 0)
    body = (
        _enc_str("MQTT")
        + bytes([MQTT_5])
        + bytes([flags])
        + struct.pack("!H", 60)
        + _enc_props([])  # connect properties
        + _enc_str(client_id)
    )
    body += (
        raw_will_properties
        if raw_will_properties is not None
        else _enc_props(will_properties or [])
    )
    body += _enc_str(will_topic)
    body += struct.pack("!H", len(will_payload)) + will_payload
    return bytes([0x10]) + _enc_remaining(len(body)) + body


def test_mqtt5_will_with_properties_fires_on_transport_drop():
    """An ungraceful drop publishes the will, forwardable properties intact."""
    topic = f"wills/{_unique('t')}"

    async def scenario():
        async with _connect(MQTT_5) as (sub, _connack):
            await sub.send(_make_subscribe(1, topic, protocol_level=MQTT_5))
            await sub.await_packet(PKT_SUBACK)

            with patch_endpoint_dns():
                ws = await websockets.connect(_ws_url(), subprotocols=["mqtt"])
                doomed = _Client(ws, MQTT_5)
                await doomed.send(
                    _make_connect_with_will(
                        _unique("doomed"),
                        topic,
                        b"gone",
                        will_properties=[
                            (PROP_CONTENT_TYPE, "text/plain"),
                            (PROP_USER_PROPERTY, ("cause", "test")),
                        ],
                    )
                )
                _flags, connack = await doomed.await_packet(PKT_CONNACK)
                assert connack[1] == 0x00, "will CONNECT must be accepted"
                # Close the transport without an MQTT DISCONNECT: ungraceful,
                # so the will must go out.
                await ws.close()

            _flags, body = await sub.await_packet(PKT_PUBLISH, timeout=10)
            return _parse_publish(body, MQTT_5)

    got_topic, payload, props = _run(scenario())
    assert got_topic == topic
    assert payload == b"gone"
    assert _prop(props, PROP_CONTENT_TYPE) == "text/plain"
    user_props = [value for ident, value in props if ident == PROP_USER_PROPERTY]
    assert ("cause", "test") in user_props


def test_mqtt5_malformed_will_properties_answer_connack_0x81():
    """A truncated will-property block is a malformed CONNECT.

    The block below claims four bytes but holds a Content Type whose UTF-8
    length points past the end — un-flagged, the parser would read the will
    topic out of the middle of a property. v5 answers CONNACK ``0x81``.
    """

    async def scenario():
        with patch_endpoint_dns():
            async with websockets.connect(_ws_url(), subprotocols=["mqtt"]) as ws:
                client = _Client(ws, MQTT_5)
                await client.send(
                    _make_connect_with_will(
                        _unique("mal"),
                        "wills/never",
                        b"x",
                        raw_will_properties=bytes(
                            [0x04, PROP_CONTENT_TYPE, 0x00, 0x09, 0x41]
                        ),
                    )
                )
                pkt_type, _flags, body = await client.packet(timeout=5.0)
                return pkt_type, body

    pkt_type, body = _run(scenario())
    assert pkt_type == PKT_CONNACK
    assert body[1] == RC5_MALFORMED_PACKET


# ---------------------------------------------------------------------------
# MQTT over TLS (the broker's TCP transport, port 8883)
# ---------------------------------------------------------------------------
# The tests below drive private MiniStack processes on free ports (the shape
# test_tls.py uses) rather than the suite-wide server on 4566: the listener is
# a process-level socket, and half of what these pin is how it behaves at
# startup, on reset and at shutdown. They reuse the MQTT codec above, so only
# the TLS plumbing is new. IOT_MTLS_PORT is pinned per instance because the
# listener is on by default at 8883 and parallel servers would contend for it.

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HYPERCORN_CONF = "file:ministack/core/hypercorn_conf.py"
MTLS_REGION = "us-east-1"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _spawn(env_extra: dict, port: int, log_path=None) -> subprocess.Popen:
    """Start a private MiniStack. With `log_path`, its output is captured there
    (combined stdout/stderr) so a test can assert on what it did or did not log.

    The listener is off here by default: it is on by default at 8883, and a
    fleet of private test servers all reaching for that one port would leave
    every instance but the first degraded. Tests that want it pass
    ``IOT_MTLS_ENABLED=1`` plus their own free port; the default-on test
    removes the key instead.
    """
    env = {
        **os.environ,
        "LOG_LEVEL": "WARNING",
        "PERSIST_STATE": "0",
        "IOT_MTLS_ENABLED": "0",
        # These private servers must not touch (or reap) Docker containers
        # belonging to the host or to the shared test server.
        "DOCKER_HOST": "unix:///nonexistent-skip-reap",
        **env_extra,
    }
    env = {k: v for k, v in env.items() if v is not None}
    sink = subprocess.DEVNULL if log_path is None else open(log_path, "wb")
    try:
        return subprocess.Popen(
            [sys.executable, "-m", "hypercorn", "ministack.app:app",
             "-c", HYPERCORN_CONF,
             "--bind", f"127.0.0.1:{port}",
             "--log-level", "warning"],
            env=env,
            stdout=sink,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
    finally:
        if sink is not subprocess.DEVNULL:
            sink.close()


def _wait_ready(url: str, timeout: float = 30.0) -> None:
    """Poll until 200. /_ministack/ready answers 503 while the mTLS listener is
    still binding, so this covers the MQTT port too."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except Exception as e:
            last = e
            time.sleep(0.3)
    raise AssertionError(f"{url} did not come up within {timeout}s: {last!r}")


def _terminate(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


class _Broker:
    """A private MiniStack instance with the mTLS listener bound."""

    def __init__(self, http_port: int, mqtt_port: int, log_path=None):
        self.http_port = http_port
        self.mqtt_port = mqtt_port
        self.log_path = log_path
        self.url = f"http://127.0.0.1:{http_port}"

    def log(self) -> str:
        """This instance's captured output. Why the listener refused a
        certificate is only visible here, and it is part of what it promises.
        """
        assert self.log_path is not None, "this instance was spawned without capture"
        return self.log_path.read_text(errors="replace")

    def client(self, service: str, access_key: str = "test"):
        return boto3.client(
            service,
            endpoint_url=self.url,
            aws_access_key_id=access_key,
            aws_secret_access_key="test",
            region_name=MTLS_REGION,
            config=Config(region_name=MTLS_REGION, retries={"max_attempts": 0}),
        )

    def ca_pem(self) -> str:
        with urllib.request.urlopen(f"{self.url}/_ministack/iot/ca.pem", timeout=5) as resp:
            return resp.read().decode("utf-8")


@pytest.fixture(scope="module")
def broker(tmp_path_factory):
    # The importorskip sits in the fixture rather than at module scope: the
    # rest of this file needs no X.509 machinery.
    pytest.importorskip("cryptography")
    http_port = _free_port()
    mqtt_port = _free_port()
    log_path = tmp_path_factory.mktemp("mtls-broker") / "server.log"
    proc = _spawn(
        {"IOT_MTLS_ENABLED": "1", "IOT_MTLS_PORT": str(mqtt_port)}, http_port, log_path=log_path
    )
    try:
        _wait_ready(f"http://127.0.0.1:{http_port}/_ministack/ready")
        yield _Broker(http_port, mqtt_port, log_path)
    finally:
        _terminate(proc)


# ---------------------------------------------------------------------------
# Certificates and TLS client
# ---------------------------------------------------------------------------


def _new_cert(broker: _Broker, access_key: str = "test", set_active: bool = True):
    """CreateKeysAndCertificate under `access_key`'s account."""
    resp = broker.client("iot", access_key).create_keys_and_certificate(setAsActive=set_active)
    return resp["certificateId"], resp["certificatePem"], resp["keyPair"]["PrivateKey"]


def _client_context(broker: _Broker, cert_pem: str | None, key_pem: str | None, tmp_path):
    ctx = ssl.create_default_context(cadata=broker.ca_pem())
    if cert_pem is not None:
        chain = tmp_path / f"{uuid.uuid4().hex}.pem"
        chain.write_text(cert_pem + key_pem)
        ctx.load_cert_chain(str(chain))
    return ctx


def _mtls_connect(
    broker: _Broker, cert_pem, key_pem, tmp_path, timeout: float = 10.0,
    server_name: str | None = "localhost",
):
    """Open a TLS socket to the listener, verifying the broker's chain. Another
    ``server_name`` is sent as the SNI without checking it against the
    certificate; None sends no SNI, as when dialling by IP."""
    ctx = _client_context(broker, cert_pem, key_pem, tmp_path)
    raw = socket.create_connection(("127.0.0.1", broker.mqtt_port), timeout=timeout)
    if server_name != "localhost":
        ctx.check_hostname = False
        return ctx.wrap_socket(raw, server_hostname=server_name)
    # `server_hostname` is checked against the certificate's SANs, so this also
    # pins that the broker certificate carries a usable `localhost` name.
    return ctx.wrap_socket(raw, server_hostname="localhost")


def _split_publish(body: bytes) -> tuple[str, bytes]:
    topic_len = struct.unpack_from("!H", body, 0)[0]
    return body[2:2 + topic_len].decode("utf-8"), body[2 + topic_len:]


class _Peer:
    """A connected MQTT client over the TLS socket."""

    def __init__(self, sock):
        self.sock = sock
        self.buf = bytearray()
        # Retained messages are delivered from inside broker_subscribe, i.e.
        # *before* the SUBACK — park any PUBLISH seen while waiting for an ack.
        self.publishes: list[tuple[str, bytes]] = []

    def send(self, data: bytes) -> None:
        self.sock.sendall(data)

    def next_packet(self, timeout: float = 5.0):
        """Return the next (type, flags, body), or None on EOF/timeout."""
        deadline = time.time() + timeout
        while True:
            parsed = _parse_packet(bytes(self.buf))
            if parsed is not None:
                ptype, flags, body, consumed = parsed
                del self.buf[:consumed]
                return ptype, flags, body
            remaining = deadline - time.time()
            if remaining <= 0:
                return None
            self.sock.settimeout(remaining)
            try:
                chunk = self.sock.recv(4096)
            except (socket.timeout, ssl.SSLError):
                return None
            if not chunk:
                return None
            self.buf.extend(chunk)

    def connect(self, client_id: str, timeout: float = 5.0):
        self.send(_make_connect(client_id))
        return self.next_packet(timeout=timeout)

    def subscribe(self, topic: str, packet_id: int = 1, timeout: float = 5.0):
        """Send SUBSCRIBE and return the SUBACK packet."""
        self.send(_make_subscribe(packet_id, topic))
        deadline = time.time() + timeout
        while True:
            pkt = self.next_packet(timeout=max(0.0, deadline - time.time()))
            if pkt is None:
                return None
            ptype, _flags, body = pkt
            if ptype == PKT_PUBLISH:
                self.publishes.append(_split_publish(body))
                continue
            return pkt

    def next_publish(self, timeout: float = 5.0):
        """Return (topic, payload) of the next PUBLISH, or None."""
        if self.publishes:
            return self.publishes.pop(0)
        deadline = time.time() + timeout
        while True:
            pkt = self.next_packet(timeout=max(0.0, deadline - time.time()))
            if pkt is None:
                return None
            ptype, _flags, body = pkt
            if ptype != PKT_PUBLISH:
                continue
            return _split_publish(body)

    def wait_disconnected(self, timeout: float = 5.0) -> bool:
        """True once the broker has hung up. Distinct from `next_packet`
        returning None, which is also what a quiet-but-live session looks like.
        """
        deadline = time.time() + timeout
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return False
            self.sock.settimeout(remaining)
            try:
                if not self.sock.recv(4096):
                    return True
            except (socket.timeout, TimeoutError):
                return False
            except OSError:
                # A TLS alert or a reset instead of a clean close: still a hangup.
                return True

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass


def _refused_below_mqtt(broker, cert_pem, key_pem, tmp_path, client_id: str) -> bool:
    """True when the listener refuses this certificate before MQTT begins.

    TLS 1.3 lets the server finish its half of the handshake before it has
    verified the client's certificate, so a refusal reaches the client only
    after it already believes it is connected — as an alert, as a broken pipe,
    or as a silent EOF, depending on timing. Asserting on any one of those
    shapes pins the platform rather than the behaviour; what the listener
    actually promises is that no CONNACK comes back.
    """
    try:
        peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    except OSError:
        return True
    try:
        return peer.connect(client_id) is None
    except OSError:
        return True
    finally:
        peer.close()


def _assert_connack(pkt, return_code: int = 0) -> None:
    assert pkt is not None, "no CONNACK received"
    ptype, _flags, body = pkt
    assert ptype == PKT_CONNACK, f"expected CONNACK, got packet type {ptype}"
    assert body[1] == return_code, f"CONNACK return code {body[1]}, expected {return_code}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_mtls_on_by_default(tmp_path):
    """Without IOT_MTLS_PORT the listener reaches for 8883 on its own.

    The default port may legitimately be taken (another instance on the same
    machine, this very suite's shared server), and a busy port degrades to a
    log line by design — so the assertion is on the attempt: the log names
    port 8883, either listening or failing to bind, without anyone setting a
    variable. When the bind did succeed, a TCP probe pins that it is real.
    """
    pytest.importorskip("cryptography")
    log = tmp_path / "default-on.log"
    http_port = _free_port()
    proc = _spawn({"LOG_LEVEL": "INFO", "IOT_MTLS_ENABLED": None}, http_port, log_path=log)
    try:
        _wait_ready(f"http://127.0.0.1:{http_port}/_ministack/ready")
        text = log.read_text(errors="replace")
        listening = "MQTT listening on port 8883" in text
        degraded = "failed to bind port 8883" in text
        assert listening or degraded, f"no default-on attempt in the log:\n{text}"
        if listening:
            with socket.create_connection(("127.0.0.1", 8883), timeout=5):
                pass
    finally:
        _terminate(proc)


def test_mtls_disabled_by_env(tmp_path):
    """IOT_MTLS_ENABLED=0 switches the listener off entirely, the way
    SFTP_ENABLED does for the Transfer Family listener — down to the lifespan
    skipping the iot module import, which is the line asserted on."""
    log = tmp_path / "disabled.log"
    http_port = _free_port()
    proc = _spawn({"LOG_LEVEL": "DEBUG"}, http_port, log_path=log)
    try:
        _wait_ready(f"http://127.0.0.1:{http_port}/_ministack/ready")
        text = log.read_text(errors="replace")
        assert "skipping iot module import" in text, f"no opt-out line in the log:\n{text}"
        assert "MQTT listening on port" not in text
    finally:
        _terminate(proc)


def test_mtls_connect_and_connack(broker, tmp_path):
    """A CA-signed, ACTIVE certificate connects and gets CONNACK 0."""
    _cert_id, cert_pem, key_pem = _new_cert(broker)
    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("device")))
    finally:
        peer.close()


def test_mtls_subscribe_receives_http_publish(broker, tmp_path):
    """An HTTP `iot-data publish` reaches a subscriber on the TLS transport."""
    _cert_id, cert_pem, key_pem = _new_cert(broker)
    topic = _unique("mtls/http")
    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("device")))
        ptype, _flags, _body = peer.subscribe(topic)
        assert ptype == PKT_SUBACK

        broker.client("iot-data").publish(topic=topic, payload=b"from-http")
        assert peer.next_publish() == (topic, b"from-http")
    finally:
        peer.close()


def test_mtls_publish_is_brokered(broker, tmp_path):
    """A PUBLISH over TLS goes through the broker: retained, then replayed to a
    second TLS subscriber."""
    _cert_id, cert_pem, key_pem = _new_cert(broker)
    topic = _unique("mtls/retained")

    publisher = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(publisher.connect(_unique("publisher")))
        publisher.send(_make_publish(topic, b"sticky", retain=True))
        # PUBLISH has no ack at QoS 0; give the broker a moment to store it.
        time.sleep(0.5)
    finally:
        publisher.close()

    subscriber = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(subscriber.connect(_unique("subscriber")))
        subscriber.subscribe(topic)
        assert subscriber.next_publish() == (topic, b"sticky")
    finally:
        subscriber.close()


def test_mtls_publish_principal_is_the_certificate_id(broker, tmp_path):
    """principal() of an mTLS publish is the id of the client certificate."""
    cert_id, cert_pem, key_pem = _new_cert(broker)
    topic = _unique("mtls/principal")
    rule = _unique("mtlsprincipal").replace("-", "_")
    role = _rule_role(broker.client("iam"))
    broker.client("iot").create_topic_rule(
        ruleName=rule,
        topicRulePayload={
            "sql": f"SELECT principal() AS p, traceid() AS t FROM '{topic}'",
            "actions": [{"republish": {"topic": f"{topic}/out", "roleArn": role["Arn"]}}],
        },
    )
    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("device")))
        peer.subscribe(f"{topic}/out")
        peer.send(_make_publish(topic, b"{}"))
        received = peer.next_publish()
        assert received is not None, "the rule's republish did not arrive"
        out_topic, payload = received
        event = json.loads(payload)
        assert out_topic == f"{topic}/out"
        assert event["p"] == cert_id
        assert str(uuid.UUID(event["t"])) == event["t"]
    finally:
        peer.close()
        broker.client("iot").delete_topic_rule(ruleName=rule)
        broker.client("iam").delete_role(RoleName=role["RoleName"])


def test_mtls_job_document_urls_name_the_dialled_host(broker, tmp_path):
    """Presigned URLs a device gets over TLS name the host it dialled, on the gateway port."""
    iot = broker.client("iot")
    thing, job_id = _unique("mtls-jobs"), _unique("job")
    iot.create_job(
        jobId=job_id, targets=[iot.create_thing(thingName=thing)["thingArn"]],
        document=json.dumps({"fw": "${aws:iot:s3-presigned-url-v2:s3://bucket/fw.bin}"}),
        presignedUrlConfig={"roleArn": "arn:aws:iam::000000000000:role/presign"},
    )
    port = int(os.environ.get("GATEWAY_PORT", os.environ.get("EDGE_PORT", "4566")))
    topic = f"$aws/things/{thing}/jobs/{job_id}/get"
    for server_name, host in (("localhost", "localhost"), (None, "127.0.0.1")):
        peer = _Peer(_mtls_connect(broker, None, None, tmp_path, server_name=server_name))
        try:
            _assert_connack(peer.connect(_unique("device")))
            peer.subscribe(f"{topic}/accepted")
            peer.send(_make_publish(topic, b"{}"))
            _topic, payload = peer.next_publish()
            url = urlparse(json.loads(payload)["execution"]["jobDocument"]["fw"])
            assert (url.hostname, url.port, url.path) == (host, port, "/bucket/fw.bin")
        finally:
            peer.close()


def test_mtls_no_client_cert_uses_default_account(broker, tmp_path):
    """No client certificate connects fine, exactly like the WebSocket path,
    and the session runs under the default account."""
    topic = _unique("mtls/anonymous")
    peer = _Peer(_mtls_connect(broker, None, None, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("anonymous")))
        peer.subscribe(topic)

        broker.client("iot-data", access_key="111111111111").publish(
            topic=topic, payload=b"someone-else"
        )
        assert peer.next_publish(timeout=_WS_HANDSHAKE_TIMEOUT) is None, "leaked across accounts"

        broker.client("iot-data", access_key="000000000000").publish(
            topic=topic, payload=b"default-account"
        )
        assert peer.next_publish() == (topic, b"default-account")
    finally:
        peer.close()


def test_mtls_unregistered_cert_gets_connack_5(broker, tmp_path):
    """A certificate the registry does not know is refused, not served.

    Presenting a certificate is a claim of identity, so it is read against the
    registry; the no-certificate case above is the one that gets the default
    account.
    """
    cert_id, cert_pem, key_pem = _new_cert(broker)
    iot = broker.client("iot")
    iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
    iot.delete_certificate(certificateId=cert_id)

    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("ghost")), return_code=5)
        assert peer.next_packet(timeout=3.0) is None, "expected the broker to close"
    finally:
        peer.close()
    refusals = [line for line in broker.log().splitlines() if cert_id in line]
    assert refusals and "not registered" in refusals[-1], refusals


def test_mtls_mqtt5_refusal_is_v5_connack(broker, tmp_path):
    """An MQTT 5 client refused over TLS gets a *v5* CONNACK.

    Reason code 0x87 ("Not authorized") plus a property block — not the
    two-byte 3.1.1 form, which a v5 SDK's decoder would die on instead of
    reporting the refusal.
    """
    cert_id, cert_pem, key_pem = _new_cert(broker)
    iot = broker.client("iot")
    iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
    iot.delete_certificate(certificateId=cert_id)

    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        peer.send(_make_connect(_unique("ghost-v5"), protocol_level=MQTT_5))
        pkt = peer.next_packet(timeout=5.0)
        assert pkt is not None, "no CONNACK received"
        ptype, _flags, body = pkt
        assert ptype == PKT_CONNACK, f"expected CONNACK, got packet type {ptype}"
        assert body[1] == 0x87, f"reason code {body[1]:#x}, expected 0x87 (not authorized)"
        assert len(body) >= 3 and body[2] == 0x00, "v5 CONNACK property block missing"
    finally:
        peer.close()


def test_mtls_inactive_cert_refused(broker, tmp_path):
    """Deactivating a certificate cuts the device off.

    This is the lifecycle the control plane already enforces (an ACTIVE
    certificate cannot be deleted, `CertificateStateException` 406), read at
    connect time: without it, `UpdateCertificate` to INACTIVE would mean
    nothing on the wire.
    """
    cert_id, cert_pem, key_pem = _new_cert(broker, access_key="111111111111")
    iot = broker.client("iot", access_key="111111111111")

    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("before-deactivation")))
    finally:
        peer.close()

    iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")

    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("deactivated")), return_code=5)
    finally:
        peer.close()

    iot.update_certificate(certificateId=cert_id, newStatus="ACTIVE")

    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("reactivated")))
    finally:
        peer.close()


def test_mtls_auth_refusal_closes_without_connack(tmp_path):
    """With AUTH=true a refused client certificate's CONNECT is closed without a CONNACK, the log names the reason."""
    pytest.importorskip("cryptography")
    http_port, mqtt_port = _free_port(), _free_port()
    log_path = tmp_path / "auth.log"
    proc = _spawn(
        {"AUTH": "true", "IOT_MTLS_ENABLED": "1", "IOT_MTLS_PORT": str(mqtt_port)}, http_port, log_path=log_path
    )
    try:
        _wait_ready(f"http://127.0.0.1:{http_port}/_ministack/ready")
        private = _Broker(http_port, mqtt_port, log_path)
        iot = private.client("iot")
        policy = _unique("connect")
        iot.create_policy(policyName=policy, policyDocument=json.dumps({
            "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "iot:Connect", "Resource": "*"}],
        }))
        certs = {name: _new_cert(private) for name in ("active", "inactive", "revoked", "deleted")}
        for name in ("active", "inactive", "revoked"):
            arn = iot.describe_certificate(certificateId=certs[name][0])["certificateDescription"]["certificateArn"]
            iot.attach_policy(policyName=policy, target=arn)
        iot.update_certificate(certificateId=certs["inactive"][0], newStatus="INACTIVE")
        iot.update_certificate(certificateId=certs["revoked"][0], newStatus="REVOKED")
        iot.update_certificate(certificateId=certs["deleted"][0], newStatus="INACTIVE")
        iot.delete_certificate(certificateId=certs["deleted"][0])

        def answer(cert, protocol_level):
            """The packet answering a CONNECT, or "closed" when the broker hangs up without one."""
            peer = _Peer(_mtls_connect(private, cert[1], cert[2], tmp_path))
            try:
                peer.send(_make_connect(_unique("device"), protocol_level=protocol_level))
                pkt = peer.next_packet(timeout=5.0)
                if pkt is None:
                    return "closed" if peer.wait_disconnected(timeout=1.0) else "open"
                return pkt[0], pkt[2][1]
            finally:
                peer.close()

        assert answer(certs["active"], MQTT_311) == (PKT_CONNACK, 0)
        for level in (MQTT_311, MQTT_5):
            for name in ("inactive", "revoked", "deleted"):
                assert answer(certs[name], level) == "closed", (name, level)

        _cert_id, active_pem, _key_pem = certs["active"]
        private.client("iot", access_key="222222222222").register_certificate(
            certificatePem=active_pem, setAsActive=True
        )
        for level in (MQTT_311, MQTT_5):
            assert answer(certs["active"], level) == "closed", level

        for name, reason in (
            ("inactive", "status 'INACTIVE'"), ("revoked", "status 'REVOKED'"),
            ("deleted", "not registered"), ("active", "owner is ambiguous"),
        ):
            refusals = [line for line in private.log().splitlines() if certs[name][0] in line]
            assert refusals and reason in refusals[-1], (name, refusals)
    finally:
        _terminate(proc)


def test_mtls_ambiguous_cert_is_refused(broker, tmp_path):
    """A certificate two accounts hold ACTIVE is refused, not awarded.

    `RegisterCertificate` accepts any PEM from any caller, so 222222222222 can
    register a copy of 111111111111's device certificate. Both registrations
    see byte-identical bytes, so a tie-break would decide the tenancy by
    something like account-id order, handing the device's session to whoever
    registered the copy. The connection is refused instead, the log names both
    scopes, and the device is back on its own account once only one
    registration is left ACTIVE.
    """
    cert_id, cert_pem, key_pem = _new_cert(broker, access_key="111111111111")

    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("before-the-copy")))
    finally:
        peer.close()

    hijacker = broker.client("iot", access_key="222222222222")
    hijacker.register_certificate(certificatePem=cert_pem, setAsActive=True)

    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("contested")), return_code=5)
    finally:
        peer.close()

    # A refusal an operator cannot explain is the failure mode this listener
    # exists to avoid, and "your certificate is registered twice" is not a
    # guess anyone makes unaided, so the log names the reason and both scopes.
    refusals = [line for line in broker.log().splitlines() if cert_id in line]
    assert refusals, "the refusal was not logged"
    assert "ambiguous" in refusals[-1], refusals[-1]
    assert "111111111111/us-east-1" in refusals[-1], refusals[-1]
    assert "222222222222/us-east-1" in refusals[-1], refusals[-1]

    hijacker.update_certificate(certificateId=cert_id, newStatus="INACTIVE")

    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("uncontested-again")))
    finally:
        peer.close()


def test_mtls_auth_evaluates_iot_connect(tmp_path):
    """With AUTH=true a device certificate connects only under a policy that
    allows iot:Connect on its client id; otherwise the 3.1.1 CONNECT is closed
    without a CONNACK. A private instance, because AUTH is read at startup."""
    pytest.importorskip("cryptography")
    http_port, mqtt_port = _free_port(), _free_port()
    proc = _spawn({"AUTH": "true", "IOT_MTLS_ENABLED": "1", "IOT_MTLS_PORT": str(mqtt_port)}, http_port)
    try:
        _wait_ready(f"http://127.0.0.1:{http_port}/_ministack/ready")
        private = _Broker(http_port, mqtt_port)
        iot = private.client("iot")
        cert_id, cert_pem, key_pem = _new_cert(private)
        client_id = _unique("device")
        assert _refused_below_mqtt(private, cert_pem, key_pem, tmp_path, client_id)

        policy = _unique("connect")
        iot.create_policy(policyName=policy, policyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "iot:Connect", "Resource": f"arn:aws:iot:*:*:client/{client_id}"}
            ],
        }))
        arn = iot.describe_certificate(certificateId=cert_id)["certificateDescription"]["certificateArn"]
        iot.attach_policy(policyName=policy, target=arn)
        peer = _Peer(_mtls_connect(private, cert_pem, key_pem, tmp_path))
        try:
            _assert_connack(peer.connect(client_id))
        finally:
            peer.close()
        assert _refused_below_mqtt(private, cert_pem, key_pem, tmp_path, _unique("other"))
    finally:
        _terminate(proc)


def test_mtls_registered_cert_connects_whatever_its_ca(broker, tmp_path):
    """An ACTIVE device certificate connects after its CA is deactivated or
    deleted, and when it was registered without a CA, as on AWS; only its own
    status refuses it. The CAs change before the first handshake, so none of
    them was ever loaded while ACTIVE."""
    from ministack.core.x509_utils import generate_ca, sign_leaf_certificate

    iot = broker.client("iot")
    devices = {}
    for case in ("inactive-ca", "deleted-ca", "without-ca"):
        if case == "without-ca":
            ca_pem, ca_key = generate_ca(common_name=_unique(case))
            leaf_pem, leaf_key, _public = sign_leaf_certificate(ca_pem, ca_key, common_name=case)
            cert_id = iot.register_certificate_without_ca(certificatePem=leaf_pem, status="ACTIVE")[
                "certificateId"
            ]
        else:
            ca_pem, ca_key, verification_pem = iot_test_ca(
                iot.get_registration_code()["registrationCode"], _unique(case)
            )
            leaf_pem, leaf_key, _public = sign_leaf_certificate(ca_pem, ca_key, common_name=case)
            ca_id = iot.register_ca_certificate(
                caCertificate=ca_pem, verificationCertificate=verification_pem, setAsActive=True
            )["certificateId"]
            cert_id = iot.register_certificate(
                certificatePem=leaf_pem, caCertificatePem=ca_pem, setAsActive=True
            )["certificateId"]
            iot.update_ca_certificate(certificateId=ca_id, newStatus="INACTIVE")
            if case == "deleted-ca":
                iot.delete_ca_certificate(certificateId=ca_id)
        devices[case] = cert_id, leaf_pem, leaf_key

    for case, (_cert_id, leaf_pem, leaf_key) in devices.items():
        peer = _Peer(_mtls_connect(broker, leaf_pem, leaf_key, tmp_path))
        try:
            _assert_connack(peer.connect(_unique(case)))
        finally:
            peer.close()

    cert_id, leaf_pem, leaf_key = devices["inactive-ca"]
    iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
    peer = _Peer(_mtls_connect(broker, leaf_pem, leaf_key, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("inactive-cert")), return_code=5)
    finally:
        peer.close()


def test_mtls_registered_ca_chain_connects(broker, tmp_path):
    """A leaf signed by a CA registered through the API connects.

    The listener reads its trust anchors out of the IoT registry on every
    handshake, so a CA registered long after the listener bound is trusted
    without a restart.
    """
    from ministack.core.x509_utils import sign_leaf_certificate

    iot = broker.client("iot")
    ca_pem, ca_key, verification_pem = iot_test_ca(
        iot.get_registration_code()["registrationCode"], "Registered Device CA"
    )
    iot.register_ca_certificate(
        caCertificate=ca_pem, verificationCertificate=verification_pem, setAsActive=True
    )
    leaf_pem, leaf_key, _public = sign_leaf_certificate(
        ca_pem, ca_key, common_name="registered-ca-device"
    )
    iot.register_certificate(
        certificatePem=leaf_pem, caCertificatePem=ca_pem, setAsActive=True
    )

    peer = _Peer(_mtls_connect(broker, leaf_pem, leaf_key, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("registered-ca-device")))
    finally:
        peer.close()


def test_mtls_provisioning_claim_connects_and_provisions(broker, tmp_path):
    """A claim certificate connects over mTLS and provisions a thing whose new certificate then connects."""
    iot = broker.client("iot")
    name, serial = _unique("fleet"), _unique("device")
    iot.create_policy(
        policyName=f"{name}-device",
        policyDocument=json.dumps({"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "iot:Connect", "Resource": "*"}]}),
    )
    iot.create_provisioning_template(
        templateName=name,
        templateBody=json.dumps({
            "Parameters": {"SerialNumber": {"Type": "String"}},
            "Resources": {
                "thing": {"Type": "AWS::IoT::Thing",
                          "Properties": {"ThingName": {"Ref": "SerialNumber"}}},
                "certificate": {"Type": "AWS::IoT::Certificate", "Properties": {
                    "CertificateId": {"Ref": "AWS::IoT::Certificate::Id"}, "Status": "ACTIVE"}},
                "policy": {"Type": "AWS::IoT::Policy",
                           "Properties": {"PolicyName": f"{name}-device"}},
            },
        }),
        enabled=True,
        provisioningRoleArn="arn:aws:iam::000000000000:role/provisioning",
    )
    claim = iot.create_provisioning_claim(templateName=name)
    create, provision = "$aws/certificates/create/json", f"$aws/provisioning-templates/{name}/provision/json"

    peer = _Peer(_mtls_connect(broker, claim["certificatePem"], claim["keyPair"]["PrivateKey"], tmp_path))
    try:
        _assert_connack(peer.connect(_unique("claim")))
        peer.subscribe(f"{create}/+", packet_id=1)
        peer.subscribe(f"{provision}/+", packet_id=2)
        peer.send(_make_publish(create, b"", qos=1, packet_id=3))
        topic, payload = peer.next_publish()
        assert topic == f"{create}/accepted"
        created = json.loads(payload)
        peer.send(_make_publish(provision, json.dumps({
            "certificateOwnershipToken": created["certificateOwnershipToken"],
            "parameters": {"SerialNumber": serial},
        }).encode(), qos=1, packet_id=4))
        assert peer.next_publish() == (
            f"{provision}/accepted",
            json.dumps({"deviceConfiguration": {}, "thingName": serial}).encode(),
        )
    finally:
        peer.close()

    device = _Peer(_mtls_connect(broker, created["certificatePem"], created["privateKey"], tmp_path))
    try:
        _assert_connack(device.connect(serial))
    finally:
        device.close()


def test_mtls_auth_provisioning_claim_is_held_to_its_templates_topics(tmp_path):
    """With AUTH=true a claim provisions on its template's topics and is refused anything else, as on AWS."""
    pytest.importorskip("cryptography")
    http_port, mqtt_port = _free_port(), _free_port()
    proc = _spawn({"AUTH": "true", "IOT_MTLS_ENABLED": "1", "IOT_MTLS_PORT": str(mqtt_port)}, http_port)
    try:
        _wait_ready(f"http://127.0.0.1:{http_port}/_ministack/ready")
        private = _Broker(http_port, mqtt_port)
        iot, iam = private.client("iot"), private.client("iam")
        iam.create_role(RoleName="provisioning", AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Principal": {"Service": "iot.amazonaws.com"}, "Action": "sts:AssumeRole"}]}))
        iam.attach_role_policy(
            RoleName="provisioning", PolicyArn="arn:aws:iam::aws:policy/service-role/AWSIoTThingsRegistration")
        name = _unique("fleet")
        iot.create_provisioning_template(
            templateName=name,
            templateBody=json.dumps({"Resources": {"certificate": {"Type": "AWS::IoT::Certificate", "Properties": {
                "CertificateId": {"Ref": "AWS::IoT::Certificate::Id"}, "Status": "ACTIVE"}}}}),
            enabled=True,
            provisioningRoleArn="arn:aws:iam::000000000000:role/provisioning",
        )
        claim = iot.create_provisioning_claim(templateName=name)
        create, provision = "$aws/certificates/create/json", f"$aws/provisioning-templates/{name}/provision/json"
        other, unrelated = "$aws/provisioning-templates/other/provision/json", "devices/telemetry"

        def claim_peer(protocol_level=MQTT_311):
            peer = _Peer(_mtls_connect(private, claim["certificatePem"], claim["keyPair"]["PrivateKey"], tmp_path))
            peer.send(_make_connect(_unique("claim"), protocol_level=protocol_level))
            _assert_connack(peer.next_packet())
            return peer

        peer = claim_peer()
        try:
            for packet_id, topic in enumerate((f"{create}/accepted", f"{provision}/accepted"), start=1):
                assert peer.subscribe(topic, packet_id=packet_id)[2][2:] == b"\x00", topic
            peer.send(_make_publish(create, b"", qos=1, packet_id=3))
            topic, payload = peer.next_publish()
            assert topic == f"{create}/accepted"
            request = json.dumps({"certificateOwnershipToken": json.loads(payload)["certificateOwnershipToken"]})
            peer.send(_make_publish(provision, request.encode(), qos=1, packet_id=4))
            assert peer.next_publish() == (f"{provision}/accepted", b'{"deviceConfiguration": {}}')
        finally:
            peer.close()

        # MQTT 3.1.1: any refused SUBSCRIBE or PUBLISH closes the connection.
        for packet in (
            _make_subscribe(1, unrelated),
            _make_subscribe(1, f"{other}/accepted"),
            _make_publish(unrelated, b"{}", qos=1, packet_id=1),
            _make_publish(other, b"{}", qos=1, packet_id=1),
        ):
            peer = claim_peer()
            try:
                peer.send(packet)
                assert peer.wait_disconnected(), packet
            finally:
                peer.close()

        # MQTT 5: SUBACK 0x87 for the refused filter and PUBACK 0x87, the connection stays.
        peer = claim_peer(MQTT_5)
        try:
            for packet_id, (topic, code) in enumerate(
                ((f"{create}/rejected", 0x00), (unrelated, 0x87), (f"{other}/accepted", 0x87)), start=1
            ):
                peer.send(_make_subscribe(packet_id, topic, protocol_level=MQTT_5))
                ptype, _flags, body = peer.next_packet()
                assert (ptype, body[3:]) == (PKT_SUBACK, bytes([code])), topic
            for packet_id, topic in enumerate((unrelated, other), start=4):
                peer.send(_make_publish(topic, b"{}", qos=1, packet_id=packet_id, protocol_level=MQTT_5))
                ptype, _flags, body = peer.next_packet()
                assert (ptype, body[2]) == (PKT_PUBACK, 0x87), topic
        finally:
            peer.close()
    finally:
        _terminate(proc)


# Names the thing after its certificate and keeps the device's thing type.
_PROVISIONING_HOOK_HANDLER = (
    "def handler(event, context):\n"
    "    return {'allowProvisioning': True, 'parameterOverrides': {\n"
    "        'ThingName': 'dev-' + event['certificateId'][:12],\n"
    "        'ThingTypeName': event['parameters']['ThingTypeName']}}\n"
)


def test_mtls_provisioning_hook_names_the_thing(broker, tmp_path):
    """A pre-provisioning hook Lambda names the thing that the device did not name."""
    iot, lam = broker.client("iot"), broker.client("lambda")
    name = _unique("fleet")
    iot.create_thing_type(thingTypeName=f"{name}-type")
    buf = _io.BytesIO()
    with _zipfile.ZipFile(buf, "w") as z:
        z.writestr("index.py", _PROVISIONING_HOOK_HANDLER)
    role = broker.client("iam").create_role(RoleName=f"{name}-hook", AssumeRolePolicyDocument=json.dumps({
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                                                "Principal": {"Service": "lambda.amazonaws.com"}}]}))
    hook = lam.create_function(
        FunctionName=f"{name}-hook", Runtime="python3.12", Handler="index.handler",
        Role=role["Role"]["Arn"], Code={"ZipFile": buf.getvalue()},
    )["FunctionArn"]
    lam.add_permission(
        FunctionName=f"{name}-hook", StatementId="iot", Action="lambda:InvokeFunction",
        Principal="iot.amazonaws.com",
    )
    iot.create_provisioning_template(
        templateName=name,
        templateBody=json.dumps({
            "Parameters": {"ThingName": {"Type": "String"}, "ThingTypeName": {"Type": "String"}},
            "Resources": {
                "thing": {"Type": "AWS::IoT::Thing", "OverrideSettings": {
                    "AttributePayload": "DO_NOTHING", "ThingGroups": "DO_NOTHING",
                    "ThingTypeName": "DO_NOTHING"}, "Properties": {
                    "ThingName": {"Ref": "ThingName"}, "ThingTypeName": {"Ref": "ThingTypeName"},
                    "AttributePayload": {}, "ThingGroups": []}},
                "certificate": {"Type": "AWS::IoT::Certificate", "Properties": {
                    "CertificateId": {"Ref": "AWS::IoT::Certificate::Id"}, "Status": "Active"}},
            },
        }),
        enabled=True,
        provisioningRoleArn="arn:aws:iam::000000000000:role/provisioning",
        preProvisioningHook={"targetArn": hook},
    )
    claim = iot.create_provisioning_claim(templateName=name)
    create, provision = "$aws/certificates/create/json", f"$aws/provisioning-templates/{name}/provision/json"

    peer = _Peer(_mtls_connect(broker, claim["certificatePem"], claim["keyPair"]["PrivateKey"], tmp_path))
    try:
        _assert_connack(peer.connect(_unique("claim")))
        peer.subscribe(f"{create}/+", packet_id=1)
        peer.subscribe(f"{provision}/+", packet_id=2)
        peer.send(_make_publish(create, b"", qos=1, packet_id=3))
        topic, payload = peer.next_publish()
        assert topic == f"{create}/accepted"
        created = json.loads(payload)
        peer.send(_make_publish(provision, json.dumps({
            "certificateOwnershipToken": created["certificateOwnershipToken"],
            "parameters": {"ThingTypeName": f"{name}-type"},
        }).encode(), qos=1, packet_id=4))
        thing = "dev-" + created["certificateId"][:12]
        assert peer.next_publish(timeout=10) == (
            f"{provision}/accepted",
            json.dumps({"deviceConfiguration": {}, "thingName": thing}).encode(),
        )
    finally:
        peer.close()
    assert iot.describe_thing(thingName=thing)["thingTypeName"] == f"{name}-type"


@pytest.mark.parametrize("hook", [False, True], ids=["plain", "hook"])
def test_mtls_provisioning_applies_the_default_template_version(broker, tmp_path, hook):
    """RegisterThing applies the default version of the template, after its hook if it has one."""
    iot = broker.client("iot")
    name = _unique("fleet")
    extra = {}
    if hook:
        buf = _io.BytesIO()
        with _zipfile.ZipFile(buf, "w") as z:
            z.writestr("index.py", "def handler(event, context):\n    return {'allowProvisioning': True}\n")
        role = broker.client("iam").create_role(RoleName=f"{name}-hook", AssumeRolePolicyDocument=json.dumps({
            "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
                                                    "Principal": {"Service": "lambda.amazonaws.com"}}]}))
        lam = broker.client("lambda")
        extra["preProvisioningHook"] = {"targetArn": lam.create_function(
            FunctionName=f"{name}-hook", Runtime="python3.12", Handler="index.handler",
            Role=role["Role"]["Arn"], Code={"ZipFile": buf.getvalue()},
        )["FunctionArn"]}
        lam.add_permission(
            FunctionName=f"{name}-hook", StatementId="iot", Action="lambda:InvokeFunction",
            Principal="iot.amazonaws.com",
        )

    def body(version):
        return json.dumps({
            "Parameters": {"SerialNumber": {"Type": "String"}},
            "Resources": {
                "thing": {"Type": "AWS::IoT::Thing", "Properties": {
                    "ThingName": {"Fn::Join": ["", [f"{name}-{version}-", {"Ref": "SerialNumber"}]]}}},
                "certificate": {"Type": "AWS::IoT::Certificate", "Properties": {
                    "CertificateId": {"Ref": "AWS::IoT::Certificate::Id"}, "Status": "ACTIVE"}},
            },
            "DeviceConfiguration": {"version": version},
        })

    iot.create_provisioning_template(
        templateName=name, templateBody=body("v1"), enabled=True,
        provisioningRoleArn="arn:aws:iam::000000000000:role/provisioning", **extra,
    )
    claim = iot.create_provisioning_claim(templateName=name)
    create, provision = "$aws/certificates/create/json", f"$aws/provisioning-templates/{name}/provision/json"
    peer = _Peer(_mtls_connect(broker, claim["certificatePem"], claim["keyPair"]["PrivateKey"], tmp_path))
    packet_ids = iter(range(3, 100))

    def register(serial, version):
        peer.send(_make_publish(create, b"", qos=1, packet_id=next(packet_ids)))
        topic, payload = peer.next_publish()
        assert topic == f"{create}/accepted"
        peer.send(_make_publish(provision, json.dumps({
            "certificateOwnershipToken": json.loads(payload)["certificateOwnershipToken"],
            "parameters": {"SerialNumber": serial},
        }).encode(), qos=1, packet_id=next(packet_ids)))
        assert peer.next_publish(timeout=10) == (f"{provision}/accepted", json.dumps({
            "deviceConfiguration": {"version": version}, "thingName": f"{name}-{version}-{serial}",
        }).encode())

    try:
        _assert_connack(peer.connect(_unique("claim")))
        peer.subscribe(f"{create}/+", packet_id=1)
        peer.subscribe(f"{provision}/+", packet_id=2)
        iot.create_provisioning_template_version(templateName=name, templateBody=body("v2"))
        register("a", "v1")
        iot.update_provisioning_template(templateName=name, defaultVersionId=2)
        register("b", "v2")
        iot.create_provisioning_template_version(templateName=name, templateBody=body("v3"), setAsDefault=True)
        register("c", "v3")
    finally:
        peer.close()


def test_mtls_jitr_certificate_is_listed_by_its_ca(broker, tmp_path):
    """As measured on AWS, ListCertificatesByCA lists the certificate that
    just-in-time registration created, while it is still PENDING_ACTIVATION."""
    from ministack.core.x509_utils import get_certificate_id, sign_leaf_certificate

    iot = broker.client("iot")
    ca_pem, ca_key, verification_pem = iot_test_ca(
        iot.get_registration_code()["registrationCode"], _unique("jitr-list-ca")
    )
    ca_id = iot.register_ca_certificate(
        caCertificate=ca_pem, verificationCertificate=verification_pem,
        setAsActive=True, allowAutoRegistration=True,
    )["certificateId"]
    leaf_pem, leaf_key, _public = sign_leaf_certificate(ca_pem, ca_key, common_name="jitr-listed")
    cert_id = get_certificate_id(leaf_pem)
    try:
        assert _refused_below_mqtt(broker, leaf_pem, leaf_key, tmp_path, _unique("jitr-listed"))
        listing = iot.list_certificates_by_ca(caCertificateId=ca_id)["certificates"]
        assert [(c["certificateId"], c["status"]) for c in listing] == [
            (cert_id, "PENDING_ACTIVATION")
        ]
    finally:
        try:
            iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
            iot.delete_certificate(certificateId=cert_id, forceDelete=True)
        except ClientError:
            pass
        iot.update_ca_certificate(certificateId=ca_id, newStatus="INACTIVE")
        iot.delete_ca_certificate(certificateId=ca_id)


def test_mtls_jitr_auto_registers_an_unknown_cert_without_connack(broker, tmp_path):
    """Just-in-time registration, as on AWS: an unknown certificate signed by a
    CA with auto-registration enabled is created PENDING_ACTIVATION, the
    registered event is published and the connection closes without a CONNACK.
    A repeat connect publishes again, now with the creation time. A TLS
    handshake that sends no CONNECT registers nothing (AWS registers on the
    packet, not on the handshake). With auto-registration disabled the refusal
    stays CONNACK 5 and nothing is created."""
    from ministack.core.x509_utils import get_certificate_id, sign_leaf_certificate

    iot = broker.client("iot")
    ca_pem, ca_key, verification_pem = iot_test_ca(
        iot.get_registration_code()["registrationCode"], _unique("jitr-ca")
    )
    ca_id = iot.register_ca_certificate(
        caCertificate=ca_pem, verificationCertificate=verification_pem,
        setAsActive=True, allowAutoRegistration=True,
    )["certificateId"]
    leaf_pem, leaf_key, _public = sign_leaf_certificate(ca_pem, ca_key, common_name="jitr-device")
    cert_id = get_certificate_id(leaf_pem)
    topic = f"$aws/events/certificates/registered/{ca_id}"
    try:

        listener = _Peer(_mtls_connect(broker, None, None, tmp_path))
        events = []
        try:
            _assert_connack(listener.connect(_unique("jitr-listener")))
            listener.subscribe(topic)
            try:
                _Peer(_mtls_connect(broker, leaf_pem, leaf_key, tmp_path)).close()
            except OSError:
                pass
            assert listener.next_publish(timeout=2.0) is None, "handshake alone registered"
            with pytest.raises(ClientError) as exc:
                iot.describe_certificate(certificateId=cert_id)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
            for attempt in ("first", "repeat"):
                assert _refused_below_mqtt(
                    broker, leaf_pem, leaf_key, tmp_path, _unique(f"jitr-{attempt}")
                ), f"{attempt} connect got a CONNACK"
                events.append(listener.next_publish())
        finally:
            listener.close()

        assert None not in events, events
        assert [t for t, _payload in events] == [topic, topic]
        first, repeat = (json.loads(payload) for _t, payload in events)
        assert first["certificateId"] == cert_id
        assert first["caCertificateId"] == ca_id
        assert first["certificateStatus"] == "PENDING_ACTIVATION"
        assert first["certificateRegistrationTimestamp"] is None
        assert first["sourceIp"] == "127.0.0.1"
        assert isinstance(repeat["certificateRegistrationTimestamp"], str)
        desc = iot.describe_certificate(certificateId=cert_id)["certificateDescription"]
        assert desc["status"] == "PENDING_ACTIVATION"
        assert desc["caCertificateId"] == ca_id

        iot.update_ca_certificate(certificateId=ca_id, newAutoRegistrationStatus="DISABLE")
        off_pem, off_key, _public = sign_leaf_certificate(ca_pem, ca_key, common_name="jitr-off")
        peer = _Peer(_mtls_connect(broker, off_pem, off_key, tmp_path))
        try:
            _assert_connack(peer.connect(_unique("jitr-off")), return_code=5)
        finally:
            peer.close()
        with pytest.raises(ClientError) as exc:
            iot.describe_certificate(certificateId=get_certificate_id(off_pem))
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        # The auto-registered certificate and the CA must not outlive the test.
        for cid in (cert_id,):
            try:
                iot.update_certificate(certificateId=cid, newStatus="INACTIVE")
                iot.delete_certificate(certificateId=cid, forceDelete=True)
            except ClientError:
                pass
        iot.update_ca_certificate(certificateId=ca_id, newStatus="INACTIVE")
        iot.delete_ca_certificate(certificateId=ca_id)


def test_mtls_jitr_under_an_sni_only_ca_needs_sni(broker, tmp_path):
    """A CA in SNI_ONLY mode auto-registers a device only when its ClientHello
    carries a server name, and the certificate it creates is in SNI_ONLY mode
    as on AWS. Without one the certificate stays unknown and is
    refused like any other unknown certificate (CONNACK 5)."""
    from ministack.core.x509_utils import generate_ca, get_certificate_id, sign_leaf_certificate

    iot = broker.client("iot")
    ca_pem, ca_key = generate_ca(common_name=_unique("jitr-sni-ca"))
    ca_id = iot.register_ca_certificate(
        caCertificate=ca_pem, certificateMode="SNI_ONLY",
        setAsActive=True, allowAutoRegistration=True,
    )["certificateId"]
    leaf_pem, leaf_key, _public = sign_leaf_certificate(ca_pem, ca_key, common_name="jitr-sni-device")
    cert_id = get_certificate_id(leaf_pem)
    try:
        for server_name in (None,):
            peer = _Peer(_mtls_connect(broker, leaf_pem, leaf_key, tmp_path, server_name=server_name))
            try:
                _assert_connack(peer.connect(_unique("jitr-no-sni")), return_code=5)
            finally:
                peer.close()
            with pytest.raises(ClientError) as exc:
                iot.describe_certificate(certificateId=cert_id)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        assert _refused_below_mqtt(
            broker, leaf_pem, leaf_key, tmp_path, _unique("jitr-sni")
        ), "connect with SNI got a CONNACK"
        desc = iot.describe_certificate(certificateId=cert_id)["certificateDescription"]
        assert desc["status"] == "PENDING_ACTIVATION"
        assert desc["caCertificateId"] == ca_id
        assert desc["certificateMode"] == "SNI_ONLY"
    finally:
        try:
            iot.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
            iot.delete_certificate(certificateId=cert_id, forceDelete=True)
        except ClientError:
            pass
        iot.update_ca_certificate(certificateId=ca_id, newStatus="INACTIVE")
        iot.delete_ca_certificate(certificateId=ca_id)


def test_mtls_account_scoped_delivery(broker, tmp_path):
    """The client certificate decides the tenant: a device whose certificate is
    owned by 111111111111 sees that account's traffic and nobody else's."""
    _cert_id, cert_pem, key_pem = _new_cert(broker, access_key="111111111111")
    topic = _unique("mtls/tenant")

    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("tenant-device")))
        peer.subscribe(topic)

        broker.client("iot-data", access_key="000000000000").publish(
            topic=topic, payload=b"other-tenant"
        )
        assert peer.next_publish(timeout=_WS_HANDSHAKE_TIMEOUT) is None, "leaked across accounts"

        broker.client("iot-data", access_key="111111111111").publish(
            topic=topic, payload=b"own-tenant"
        )
        assert peer.next_publish() == (topic, b"own-tenant")
    finally:
        peer.close()


def test_mtls_garbage_bytes_dropped(broker, tmp_path):
    """Junk after the handshake kills that connection only."""
    _cert_id, cert_pem, key_pem = _new_cert(broker)
    junk = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        junk.send(bytes([0xFF]) * 64)
        junk.send(os.urandom(256))
        junk.next_packet(timeout=_WS_HANDSHAKE_TIMEOUT)
    finally:
        junk.close()

    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("after-junk")))
    finally:
        peer.close()


def test_mtls_duplicate_client_id_evicts_first_connection(broker, tmp_path):
    """A second CONNECT with the same client id closes the first socket — the
    session adapter's `websocket.close` translation."""
    _cert_id, cert_pem, key_pem = _new_cert(broker)
    client_id = _unique("twin")

    first = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    second = None
    try:
        _assert_connack(first.connect(client_id))
        second = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
        _assert_connack(second.connect(client_id))
        assert first.next_packet(timeout=5.0) is None, "first connection should be closed"
    finally:
        first.close()
        if second is not None:
            second.close()


def _await_rebind(broker, tmp_path, client_id: str, timeout: float = 15.0) -> None:
    """Block until a certificate minted from the current CA gets CONNACK 0.

    The rebind after a reset is scheduled on the event loop from reset's worker
    thread, so it lands some time after the HTTP call returns; until it does,
    the listener is either down or still serving the previous CA's certificate.
    Failing this is what "the listener did not come back" looks like.
    """
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            _cert_id, cert_pem, key_pem = _new_cert(broker)
            peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
            try:
                _assert_connack(peer.connect(_unique(client_id)))
                return
            finally:
                peer.close()
        except (ssl.SSLError, OSError, AssertionError) as e:
            last_error = e
            time.sleep(0.5)
    raise AssertionError(
        f"listener did not come back within {timeout}s after reset: {last_error!r}"
    )


def _reset(broker, timeout: float = 10.0) -> None:
    urllib.request.urlopen(
        urllib.request.Request(f"{broker.url}/_ministack/reset", data=b"", method="POST"),
        timeout=timeout,
    )


def test_mtls_listener_survives_reset(broker, tmp_path):
    """`/_ministack/reset` regenerates the CA; the listener rebinds with fresh
    material instead of serving a certificate nobody can verify."""
    _reset(broker)
    _await_rebind(broker, tmp_path, "post-reset")


def test_mtls_reset_rebinds_with_a_device_connected(broker, tmp_path):
    """A live session does not hold the rebind hostage.

    `asyncio.Server.wait_closed()` waits for established connections as well as
    for the acceptor since Python 3.12.1, so a stop that awaited it never
    returned while a device was attached — and since the restart awaited the
    stop while both shared a lock, the listener stayed down for the life of the
    process. The connected device is disconnected instead of outliving the CA
    that admitted it, which is also what makes the rebind bounded.
    """
    _cert_id, cert_pem, key_pem = _new_cert(broker)
    peer = _Peer(_mtls_connect(broker, cert_pem, key_pem, tmp_path))
    try:
        _assert_connack(peer.connect(_unique("attached")))
        _reset(broker)
        # Asserted before the socket is closed, and with it still open: closing
        # it first is what un-wedged the old code, so the whole point is that
        # nothing on the client's side has to happen for the rebind to land.
        assert peer.wait_disconnected(timeout=10.0), (
            "the session outlived the CA and the registry entry that admitted it"
        )
        _await_rebind(broker, tmp_path, "post-reset-with-device")
    finally:
        peer.close()


def test_mtls_shutdown_completes_with_a_device_connected(tmp_path):
    """A connected device does not stop the process from shutting down.

    Same defect as the reset above, on the other caller: `lifespan.shutdown`
    awaits `mtls_stop`, so an unbounded stop hangs the shutdown and the server
    only ever dies by SIGKILL. A private instance, because the assertion is
    about how this one terminates.
    """
    pytest.importorskip("cryptography")
    http_port, mqtt_port = _free_port(), _free_port()
    proc = _spawn({"IOT_MTLS_ENABLED": "1", "IOT_MTLS_PORT": str(mqtt_port)}, http_port)
    peer = None
    try:
        _wait_ready(f"http://127.0.0.1:{http_port}/_ministack/ready")
        private = _Broker(http_port, mqtt_port)
        _cert_id, cert_pem, key_pem = _new_cert(private)
        peer = _Peer(_mtls_connect(private, cert_pem, key_pem, tmp_path))
        _assert_connack(peer.connect(_unique("still-here")))

        proc.terminate()
        try:
            proc.wait(timeout=25)
        except subprocess.TimeoutExpired:
            raise AssertionError(
                "the server did not shut down within 25s with a device connected"
            ) from None
    finally:
        if peer is not None:
            peer.close()
        _terminate(proc)
