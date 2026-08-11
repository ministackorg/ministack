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
import struct
import threading
import time
import uuid
import zipfile as _zipfile
from urllib.parse import quote, urlparse

import pytest
from botocore.exceptions import ClientError

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


def _make_connect(client_id: str) -> bytes:
    body = (
        _enc_str("MQTT")           # protocol name
        + bytes([4])               # protocol level (3.1.1)
        + bytes([0x02])            # connect flags: clean session
        + struct.pack("!H", 60)    # keep-alive
        + _enc_str(client_id)
    )
    return bytes([0x10]) + _enc_remaining(len(body)) + body


def _make_subscribe(packet_id: int, topic: str, qos: int = 0) -> bytes:
    body = struct.pack("!H", packet_id) + _enc_str(topic) + bytes([qos])
    return bytes([0x82]) + _enc_remaining(len(body)) + body


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


def _make_unsubscribe(packet_id: int, topics: list[str]) -> bytes:
    body = struct.pack("!H", packet_id) + b"".join(_enc_str(t) for t in topics)
    return bytes([0xA2]) + _enc_remaining(len(body)) + body


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


async def _ws_subscribe_and_collect(
    ws_url: str, topic: str, ready_event: threading.Event, received: list, stop: threading.Event
):
    async with websockets.connect(ws_url, subprotocols=["mqtt"]) as ws:
        await ws.send(_make_connect("test-client"))
        # Wait for CONNACK
        await asyncio.wait_for(ws.recv(), timeout=2.0)
        # Subscribe
        await ws.send(_make_subscribe(packet_id=1, topic=topic, qos=0))
        # Retained PUBLISH frames may precede SUBACK in the in-process broker.
        while True:
            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
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
        await asyncio.wait_for(ws.recv(), timeout=2.0)
        for packet_id, topic in enumerate(topics, start=1):
            await ws.send(_make_subscribe(packet_id=packet_id, topic=topic, qos=0))
            while True:
                msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
                if _record_publish(msg, received) == 9:  # SUBACK
                    break

        await ws.send(_make_unsubscribe(packet_id=len(topics) + 1, topics=[drop]))
        while True:
            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
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
    assert ready.wait(timeout=5), "WebSocket subscriber did not become ready"

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
    assert ready.wait(timeout=5), "WebSocket subscriber did not become ready"

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
    assert east_ready.wait(timeout=5)
    assert west_ready.wait(timeout=5)

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
    assert live_ready.wait(timeout=5)

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
    assert retained_ready.wait(timeout=5)
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
    assert ready.wait(timeout=5)

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
    assert ready.wait(timeout=5)

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
    deadline = time.time() + timeout
    while time.time() < deadline:
        msgs = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=1, WaitTimeSeconds=1)
        if msgs.get("Messages"):
            return json.loads(msgs["Messages"][0]["Body"])
    return None


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
        assert ready.wait(timeout=5), "WebSocket subscriber did not become ready"

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
        assert ready.wait(timeout=5), "WebSocket subscriber did not become ready"

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
        await asyncio.wait_for(ws.recv(), timeout=2.0)  # CONNACK
        await ws.send(_make_subscribe(packet_id=1, topic=sub_filter, qos=0))
        while True:
            msg = await asyncio.wait_for(ws.recv(), timeout=2.0)
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
