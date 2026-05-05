"""IoT Broker Bridge — embedded MQTT 3.1.1 broker logic over WebSocket.

Phase 1b of the AWS IoT Core feature. The bridge owns a small in-process
pub/sub registry plus an MQTT 3.1.1 framing layer used between the broker
and WebSocket clients (per the AWS WS-MQTT subprotocol).

Architecture (mirrors Transfer Family's shared SFTP listener):

  Client → WebSocket (gateway port) → Bridge → in-memory pub/sub

The broker has no external listeners of its own. All client traffic is
terminated at the ASGI WebSocket layer and bridged here. Multi-tenancy is
enforced by transparent topic prefixing: every PUBLISH/SUBSCRIBE topic seen
on the wire is internally prefixed with the caller's account_id before it
hits the registry, and the prefix is stripped on outbound delivery.

This is the only MQTT transport in Phase 1b. Plain TCP 1883 is intentionally
not exposed (real AWS IoT Core requires TLS or SigV4 on every connection),
and the mTLS-on-8883 path is deferred to Phase 2.
"""

from __future__ import annotations

import asyncio
import logging
import os
import struct
import uuid
from typing import Awaitable, Callable

from ministack.core.responses import (
    _request_account_id,
    get_account_id,
)

logger = logging.getLogger("iot_broker")


# ---------------------------------------------------------------------------
# In-memory pub/sub registry
# ---------------------------------------------------------------------------
#
# All state is keyed on the *fully qualified* (account-prefixed) topic so
# subscribers in different accounts never see each other's traffic. The
# bridge layer adds/strips the prefix transparently.

_subscriptions: dict[str, set["_Subscription"]] = {}
_retained: dict[str, "_RetainedMessage"] = {}
_lock = asyncio.Lock()


class _RetainedMessage:
    __slots__ = ("payload", "qos", "topic", "ts")

    def __init__(self, topic: str, payload: bytes, qos: int):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.ts = asyncio.get_event_loop().time()


class _Subscription:
    """A live subscription attached to either a WS session or a callback.

    Each subscription stores the *prefixed* topic filter it registered for
    plus a delivery callable that takes (unprefixed_topic, payload, qos).
    """

    __slots__ = ("subscription_id", "filter_prefixed", "account_id", "deliver")

    def __init__(
        self,
        filter_prefixed: str,
        account_id: str,
        deliver: Callable[[str, bytes, int], Awaitable[None]],
    ):
        self.subscription_id = uuid.uuid4().hex
        self.filter_prefixed = filter_prefixed
        self.account_id = account_id
        self.deliver = deliver

    def __hash__(self) -> int:
        return hash(self.subscription_id)

    def __eq__(self, other) -> bool:
        return isinstance(other, _Subscription) and other.subscription_id == self.subscription_id


# ---------------------------------------------------------------------------
# Topic prefixing & matching
# ---------------------------------------------------------------------------


def _scoped_topic(account_id: str, topic: str) -> str:
    return f"{account_id}/{topic}"


def _unscope_topic(account_id: str, scoped_topic: str) -> str:
    prefix = f"{account_id}/"
    if scoped_topic.startswith(prefix):
        return scoped_topic[len(prefix):]
    return scoped_topic


def _topic_matches(filter_: str, topic: str) -> bool:
    """MQTT topic-filter matching with ``+`` and ``#`` wildcards."""
    f_parts = filter_.split("/")
    t_parts = topic.split("/")
    fi = ti = 0
    while fi < len(f_parts):
        f = f_parts[fi]
        if f == "#":
            return True
        if ti >= len(t_parts):
            return False
        if f != "+" and f != t_parts[ti]:
            return False
        fi += 1
        ti += 1
    return ti == len(t_parts)


# ---------------------------------------------------------------------------
# Public API consumed by iot_data.py and (future) shadow / rules engine
# ---------------------------------------------------------------------------


def is_available() -> bool:
    """Always True: the bridge has no external library dependency in P1b."""
    return True


async def start_broker() -> None:
    """No-op: the embedded broker is purely in-memory and starts on import.

    Reserved for parity with the design and for future TCP/TLS listeners.
    """
    return None


async def stop_broker() -> None:
    """Drop all subscriptions and retained messages."""
    async with _lock:
        _subscriptions.clear()
        _retained.clear()


async def publish(
    account_id: str,
    topic: str,
    payload: bytes,
    qos: int = 0,
    retain: bool = False,
) -> None:
    """Publish a message scoped to ``account_id``.

    The topic seen by subscribers is the unprefixed value supplied here.
    Internally we store and dispatch on the prefixed form so different
    accounts cannot leak into each other's namespaces.
    """
    scoped = _scoped_topic(account_id, topic)

    if retain:
        if not payload:
            _retained.pop(scoped, None)
        else:
            _retained[scoped] = _RetainedMessage(scoped, payload, qos)

    async with _lock:
        # Snapshot subs to avoid holding the lock during dispatch.
        subs = [s for sset in _subscriptions.values() for s in sset]

    for sub in subs:
        if _topic_matches(sub.filter_prefixed, scoped):
            try:
                await sub.deliver(_unscope_topic(sub.account_id, scoped), payload, qos)
            except Exception:
                logger.exception("IoT broker: subscriber %s delivery failed", sub.subscription_id)


async def subscribe(
    account_id: str,
    topic_filter: str,
    callback: Callable[[str, bytes, int], Awaitable[None]],
) -> str:
    """Register a subscription. Returns a subscription_id."""
    filter_prefixed = _scoped_topic(account_id, topic_filter)
    sub = _Subscription(filter_prefixed, account_id, callback)
    async with _lock:
        _subscriptions.setdefault(filter_prefixed, set()).add(sub)
        # Replay any retained messages matching this filter.
        retained_to_send = [
            r for k, r in _retained.items() if _topic_matches(filter_prefixed, k)
        ]

    for r in retained_to_send:
        try:
            await sub.deliver(_unscope_topic(account_id, r.topic), r.payload, r.qos)
        except Exception:
            logger.exception("IoT broker: retained-message delivery failed")

    return sub.subscription_id


async def unsubscribe(subscription_id: str) -> None:
    async with _lock:
        for filter_, subs in list(_subscriptions.items()):
            for s in list(subs):
                if s.subscription_id == subscription_id:
                    subs.discard(s)
            if not subs:
                _subscriptions.pop(filter_, None)


def reset() -> None:
    """Synchronous reset for ``/_ministack/reset``."""
    _subscriptions.clear()
    _retained.clear()


# ---------------------------------------------------------------------------
# MQTT 3.1.1 frame codec
# ---------------------------------------------------------------------------
#
# We only need to parse and emit a small subset:
#
#   CONNECT(1), CONNACK(2), PUBLISH(3), PUBACK(4), SUBSCRIBE(8),
#   SUBACK(9), UNSUBSCRIBE(10), UNSUBACK(11), PINGREQ(12), PINGRESP(13),
#   DISCONNECT(14)
#
# The fixed header is 1 byte type+flags followed by a variable-length
# remaining-length integer (1..4 bytes).

PKT_CONNECT = 1
PKT_CONNACK = 2
PKT_PUBLISH = 3
PKT_PUBACK = 4
PKT_SUBSCRIBE = 8
PKT_SUBACK = 9
PKT_UNSUBSCRIBE = 10
PKT_UNSUBACK = 11
PKT_PINGREQ = 12
PKT_PINGRESP = 13
PKT_DISCONNECT = 14


def _encode_remaining_length(n: int) -> bytes:
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n > 0:
            byte |= 0x80
        out.append(byte)
        if n == 0:
            return bytes(out)


def _decode_remaining_length(buf: bytes, offset: int) -> tuple[int, int]:
    multiplier = 1
    value = 0
    pos = offset
    while True:
        if pos >= len(buf):
            raise ValueError("Truncated remaining length")
        b = buf[pos]
        pos += 1
        value += (b & 0x7F) * multiplier
        if b & 0x80 == 0:
            break
        multiplier *= 128
        if multiplier > 128 * 128 * 128:
            raise ValueError("Remaining length exceeds 4 bytes")
    return value, pos


def _read_string(buf: bytes, offset: int) -> tuple[str, int]:
    if offset + 2 > len(buf):
        raise ValueError("Truncated string length")
    n = struct.unpack_from("!H", buf, offset)[0]
    offset += 2
    if offset + n > len(buf):
        raise ValueError("Truncated string body")
    return buf[offset:offset + n].decode("utf-8"), offset + n


def _encode_string(s: str) -> bytes:
    raw = s.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw


def _make_connack(return_code: int = 0, session_present: bool = False) -> bytes:
    flags = 1 if session_present else 0
    body = bytes([flags, return_code])
    return bytes([PKT_CONNACK << 4]) + _encode_remaining_length(len(body)) + body


def _make_publish(topic: str, payload: bytes, qos: int = 0, packet_id: int | None = None,
                  retain: bool = False, dup: bool = False) -> bytes:
    fixed = (PKT_PUBLISH << 4) | (qos << 1) | (0x08 if dup else 0) | (0x01 if retain else 0)
    body = _encode_string(topic)
    if qos > 0:
        if packet_id is None:
            packet_id = 1
        body += struct.pack("!H", packet_id)
    body += payload
    return bytes([fixed]) + _encode_remaining_length(len(body)) + body


def _make_puback(packet_id: int) -> bytes:
    return bytes([PKT_PUBACK << 4]) + bytes([2]) + struct.pack("!H", packet_id)


def _make_suback(packet_id: int, granted_qos: list[int]) -> bytes:
    body = struct.pack("!H", packet_id) + bytes(granted_qos)
    return bytes([PKT_SUBACK << 4]) + _encode_remaining_length(len(body)) + body


def _make_unsuback(packet_id: int) -> bytes:
    return bytes([PKT_UNSUBACK << 4]) + bytes([2]) + struct.pack("!H", packet_id)


def _make_pingresp() -> bytes:
    return bytes([PKT_PINGRESP << 4, 0])


# ---------------------------------------------------------------------------
# WebSocket session driver
# ---------------------------------------------------------------------------


def _max_frame_buffer_bytes() -> int:
    # 16 MB cap on per-connection buffer to bound memory if a peer starves.
    return int(os.environ.get("IOT_WS_FRAME_MAX_BYTES", str(16 * 1024 * 1024)))


class _WSSession:
    """Drives one MQTT-over-WebSocket connection."""

    def __init__(self, send_coro, account_id: str):
        self._send = send_coro
        self.account_id = account_id
        self._sub_ids: list[str] = []
        self._buffer = bytearray()
        self._next_pid = 1
        self._send_lock = asyncio.Lock()

    async def send_bytes(self, b: bytes) -> None:
        async with self._send_lock:
            await self._send({"type": "websocket.send", "bytes": b})

    async def deliver_to_client(self, topic: str, payload: bytes, qos: int) -> None:
        # Downgrade subscriber-side QoS to 0 in P1; we don't track in-flight
        # PUBLISH retransmissions on the WS leg.
        await self.send_bytes(_make_publish(topic, payload, qos=0))

    def _take_packet(self) -> tuple[int, int, bytes] | None:
        """Try to extract one MQTT packet from the buffer.

        Returns ``(packet_type, flags, body)`` or ``None`` if not enough bytes.
        """
        if len(self._buffer) < 2:
            return None
        first = self._buffer[0]
        try:
            remaining, header_end = _decode_remaining_length(bytes(self._buffer), 1)
        except ValueError:
            # Need more bytes (or garbage). Try again later.
            if len(self._buffer) > 5:
                # Garbage longer than max remaining-length encoding → drop.
                self._buffer.clear()
            return None
        total = header_end + remaining
        if len(self._buffer) < total:
            return None
        body = bytes(self._buffer[header_end:total])
        # Consume from buffer.
        del self._buffer[:total]
        pkt_type = (first >> 4) & 0x0F
        flags = first & 0x0F
        return pkt_type, flags, body

    async def handle_packet(self, pkt_type: int, flags: int, body: bytes) -> bool:
        """Process one MQTT packet. Returns False to terminate the session."""
        if pkt_type == PKT_CONNECT:
            # Phase 1: anonymous CONNECT always accepted.
            await self.send_bytes(_make_connack(return_code=0))
            return True

        if pkt_type == PKT_PUBLISH:
            qos = (flags >> 1) & 0x03
            retain = bool(flags & 0x01)
            topic, off = _read_string(body, 0)
            packet_id = None
            if qos > 0:
                if off + 2 > len(body):
                    return True
                packet_id = struct.unpack_from("!H", body, off)[0]
                off += 2
            payload = body[off:]
            await publish(self.account_id, topic, payload, qos=qos, retain=retain)
            if qos == 1 and packet_id is not None:
                await self.send_bytes(_make_puback(packet_id))
            return True

        if pkt_type == PKT_SUBSCRIBE:
            packet_id = struct.unpack_from("!H", body, 0)[0]
            off = 2
            granted = []
            while off < len(body):
                topic, off = _read_string(body, off)
                req_qos = body[off]
                off += 1
                # Cap to QoS 1 (we don't implement QoS 2).
                granted_qos = min(req_qos, 1)
                granted.append(granted_qos)
                sid = await subscribe(self.account_id, topic, self.deliver_to_client)
                self._sub_ids.append(sid)
            await self.send_bytes(_make_suback(packet_id, granted))
            return True

        if pkt_type == PKT_UNSUBSCRIBE:
            packet_id = struct.unpack_from("!H", body, 0)[0]
            # In P1 we collapse all unsubs into "unsubscribe everything" since
            # we don't keep a topic→sub_id map per session. Sufficient for the
            # unblocking use case; can be tightened later.
            for sid in list(self._sub_ids):
                await unsubscribe(sid)
            self._sub_ids.clear()
            await self.send_bytes(_make_unsuback(packet_id))
            return True

        if pkt_type == PKT_PINGREQ:
            await self.send_bytes(_make_pingresp())
            return True

        if pkt_type == PKT_DISCONNECT:
            return False

        # Ignore anything we don't recognise (PUBREC/PUBREL/PUBCOMP for QoS 2 etc.)
        return True

    async def cleanup(self) -> None:
        for sid in self._sub_ids:
            await unsubscribe(sid)
        self._sub_ids.clear()


async def handle_websocket(scope: dict, receive, send, account_id: str) -> None:
    """Drive an MQTT-over-WebSocket session.

    Account ID has already been resolved (via SigV4 or default) by the
    ASGI dispatch layer in ``app.py``. We accept the upgrade with the
    ``mqtt`` subprotocol echoed back, then frame-loop until the client
    disconnects.
    """
    msg = await receive()
    if msg.get("type") != "websocket.connect":
        return

    sub_headers = {}
    for name, value in scope.get("headers", []):
        try:
            sub_headers[name.decode("latin-1").lower()] = value.decode("utf-8")
        except UnicodeDecodeError:
            sub_headers[name.decode("latin-1").lower()] = value.decode("latin-1")
    requested = sub_headers.get("sec-websocket-protocol", "")
    chosen = None
    for proto in [p.strip() for p in requested.split(",") if p.strip()]:
        if proto.lower() in ("mqtt", "mqttv3.1", "mqttv5"):
            chosen = proto
            break

    accept: dict = {"type": "websocket.accept"}
    if chosen:
        accept["subprotocol"] = chosen
    await send(accept)

    # Bind the resolved account_id into the request context so ``publish``/
    # ``subscribe`` callers can rely on ``get_account_id()`` even though
    # WebSocket connections don't go through the per-request HTTP setter.
    ctx_token = _request_account_id.set(account_id)
    session = _WSSession(send, account_id)
    max_buffer = _max_frame_buffer_bytes()

    try:
        while True:
            incoming = await receive()
            mtype = incoming.get("type")
            if mtype == "websocket.disconnect":
                break
            if mtype != "websocket.receive":
                continue
            data = incoming.get("bytes")
            if data is None:
                text = incoming.get("text")
                if text is None:
                    continue
                # MQTT-over-WS is binary; ignore stray text frames.
                continue
            session._buffer.extend(data)
            if len(session._buffer) > max_buffer:
                logger.warning("IoT broker: WS buffer overflow, dropping connection")
                break
            while True:
                pkt = session._take_packet()
                if pkt is None:
                    break
                pkt_type, flags, body = pkt
                cont = await session.handle_packet(pkt_type, flags, body)
                if not cont:
                    return
    except Exception:
        logger.exception("IoT broker WebSocket session failed")
    finally:
        await session.cleanup()
        try:
            _request_account_id.reset(ctx_token)
        except Exception:
            pass
        try:
            await send({"type": "websocket.close", "code": 1000})
        except Exception:
            pass
