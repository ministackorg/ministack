"""Integration tests for MQTT 5.0 support in the embedded IoT broker.

The broker negotiates its wire format from the CONNECT packet's protocol
level, so these tests drive it with hand-built packets of both versions over
the MQTT-over-WebSocket endpoint: a 5.0 client must get property blocks,
subscription options and reason codes, a 3.1.1 client must keep getting the
exact bytes it got before, and a message must cross between the two.

No MQTT client library is involved — the packets are assembled here, in the
same stdlib style as ``test_iot_data.py``.
"""

from __future__ import annotations

import asyncio
import os
import struct
import uuid
from urllib.parse import urlparse

import pytest
from conftest import patch_endpoint_dns

pytest.importorskip("websockets")

import websockets  # noqa: E402

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

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


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _ws_url(region: str = "us-east-1") -> str:
    parsed = urlparse(ENDPOINT)
    host = parsed.hostname or "localhost"
    port = parsed.port or 4566
    return f"ws://prefix-ats.iot.{region}.{host}:{port}/mqtt"


# ---------------------------------------------------------------------------
# Minimal MQTT 3.1.1 / 5.0 codec for the test client
# ---------------------------------------------------------------------------


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


def _enc_str(s: str) -> bytes:
    raw = s.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw


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
        elif ident in (PROP_SESSION_EXPIRY_INTERVAL,):
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


# ---------------------------------------------------------------------------
# Version negotiation
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Subscribe / publish
# ---------------------------------------------------------------------------


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
    """QoS 1 PUBACK reports Success with a subscriber, No matching subscribers without."""
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
    assert lonely == b"\x00\x07\x10\x00", "packet id 7, No matching subscribers, no properties"
    assert matched == b"\x00\x08\x00\x00", "packet id 8, Success, no properties"


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


# ---------------------------------------------------------------------------
# Malformed packets
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Subscription options
# ---------------------------------------------------------------------------


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
    assert puback == b"\x00\x07\x10\x00", (
        "No matching subscribers: the only subscription was the publisher's own"
    )
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


# ---------------------------------------------------------------------------
# Will (Last Will and Testament) with MQTT 5 properties
# ---------------------------------------------------------------------------


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
