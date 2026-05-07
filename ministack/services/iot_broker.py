"""IoT Broker Bridge — embedded MQTT 3.1.1 broker logic over WebSocket.

The bridge owns a small in-process
pub/sub registry plus an MQTT 3.1.1 framing layer used between the broker
and WebSocket clients (per the AWS WS-MQTT subprotocol).

Architecture (mirrors Transfer Family's shared SFTP listener):

  Client → WebSocket (gateway port) → Bridge → in-memory pub/sub

The broker has no external listeners of its own. All client traffic is
terminated at the ASGI WebSocket layer and bridged here. Multi-tenancy is
enforced by transparent topic prefixing: every PUBLISH/SUBSCRIBE topic seen
on the wire is internally prefixed with the caller's account_id before it
hits the registry, and the prefix is stripped on outbound delivery.

This is the only MQTT transport currently. Plain TCP 1883 is intentionally
not exposed (real AWS IoT Core requires TLS or SigV4 on every connection),
and the mTLS-on-8883 path is not yet implemented.
"""

from __future__ import annotations

import asyncio
import logging
import os
import struct
import time
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
_connected_clients: dict[tuple[str, str], "_WSSession"] = {}
_persistent_sessions: dict[tuple[str, str], "_PersistentSessionState"] = {}
_lock = asyncio.Lock()

# Session expiry timeout (seconds). Persistent sessions older than this are discarded.
_SESSION_EXPIRY_SECONDS: int = int(os.environ.get("IOT_SESSION_EXPIRY_SECONDS", "3600"))

# Maximum queued messages per persistent session
_MAX_QUEUED_MESSAGES = 1000


class _PersistentSessionState:
    """State preserved for a persistent session (cleanSession=0) after disconnect."""

    __slots__ = ("subscriptions", "queued_messages", "created_at")

    def __init__(self, subscriptions: list[str], created_at: float):
        self.subscriptions: list[str] = subscriptions  # unprefixed topic filters
        self.queued_messages: list[tuple[str, bytes, int]] = []  # (topic, payload, qos)
        self.created_at: float = created_at


def _is_session_expired(session_state: _PersistentSessionState) -> bool:
    """Return True if the persistent session has expired."""
    return (time.time() - session_state.created_at) > _SESSION_EXPIRY_SECONDS


class _RetainedMessage:
    __slots__ = ("payload", "qos", "topic", "ts")

    def __init__(self, topic: str, payload: bytes, qos: int):
        self.topic = topic
        self.payload = payload
        self.qos = qos
        self.ts = asyncio.get_event_loop().time()


class _InFlightMessage:
    """A QoS 1 message awaiting PUBACK from the subscriber."""

    __slots__ = ("packet_id", "topic", "payload", "sent_at", "retransmit_count")

    def __init__(self, packet_id: int, topic: str, payload: bytes):
        self.packet_id = packet_id
        self.topic = topic
        self.payload = payload
        self.sent_at = asyncio.get_event_loop().time()
        self.retransmit_count = 0


_RETRANSMIT_INTERVAL_SECONDS = int(os.environ.get("IOT_RETRANSMIT_SECONDS", "10"))


class _Subscription:
    """A live subscription attached to either a WS session or a callback.

    Each subscription stores the *prefixed* topic filter it registered for
    plus a delivery callable that takes (unprefixed_topic, payload, qos).
    """

    __slots__ = ("subscription_id", "filter_prefixed", "account_id", "deliver", "granted_qos")

    def __init__(
        self,
        filter_prefixed: str,
        account_id: str,
        deliver: Callable[[str, bytes, int], Awaitable[None]],
        granted_qos: int = 0,
    ):
        self.subscription_id = uuid.uuid4().hex
        self.filter_prefixed = filter_prefixed
        self.account_id = account_id
        self.deliver = deliver
        self.granted_qos = granted_qos

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
# Topic validation (mirrors iot_data.py _validate_topic for consistency)
# ---------------------------------------------------------------------------

_MAX_TOPIC_BYTES = 256


def _validate_publish_topic(topic: str) -> bool:
    """Return True if ``topic`` is valid for PUBLISH, False otherwise.

    Validation rules (same as iot_data.py HTTP Publish path):
    - Reject empty topics
    - Reject topics containing ``+`` or ``#`` wildcard characters
    - Reject topics exceeding 256 bytes in UTF-8 encoding
    """
    if not topic:
        return False
    if "+" in topic or "#" in topic:
        return False
    if len(topic.encode("utf-8")) > _MAX_TOPIC_BYTES:
        return False
    return True


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
        _connected_clients.clear()
        _persistent_sessions.clear()


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
                effective_qos = min(qos, sub.granted_qos)
                await sub.deliver(_unscope_topic(sub.account_id, scoped), payload, effective_qos)
            except Exception:
                logger.exception("IoT broker: subscriber %s delivery failed", sub.subscription_id)

    # Queue QoS 1 messages for disconnected persistent sessions
    if qos >= 1:
        for key, ps in list(_persistent_sessions.items()):
            ps_account_id, ps_client_id = key
            if ps_account_id != account_id:
                continue
            # Skip if client is currently connected (already delivered above)
            if key in _connected_clients:
                continue
            # Skip expired sessions
            if _is_session_expired(ps):
                continue
            # Check if any stored subscription filter matches the topic
            for filt in ps.subscriptions:
                scoped_filter = _scoped_topic(ps_account_id, filt)
                if _topic_matches(scoped_filter, scoped):
                    ps.queued_messages.append((topic, payload, qos))
                    # Bound queue size
                    if len(ps.queued_messages) > _MAX_QUEUED_MESSAGES:
                        ps.queued_messages = ps.queued_messages[-_MAX_QUEUED_MESSAGES:]
                    break  # Only queue once per session


async def subscribe(
    account_id: str,
    topic_filter: str,
    callback: Callable[[str, bytes, int], Awaitable[None]],
    granted_qos: int = 0,
) -> str:
    """Register a subscription. Returns a subscription_id."""
    filter_prefixed = _scoped_topic(account_id, topic_filter)
    sub = _Subscription(filter_prefixed, account_id, callback, granted_qos)
    async with _lock:
        _subscriptions.setdefault(filter_prefixed, set()).add(sub)
        # AWS IoT Core: retained messages are NOT replayed for wildcard subscriptions.
        has_wildcard = "+" in topic_filter or "#" in topic_filter
        if not has_wildcard:
            retained_to_send = [
                r for k, r in _retained.items() if _topic_matches(filter_prefixed, k)
            ]
        else:
            retained_to_send = []

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
    _connected_clients.clear()
    _persistent_sessions.clear()


# ---------------------------------------------------------------------------
# Connected-client registry & duplicate detection
# ---------------------------------------------------------------------------


def _register_client(account_id: str, client_id: str, session: "_WSSession") -> None:
    """Register a session in the connected-clients registry."""
    _connected_clients[(account_id, client_id)] = session


def _deregister_client(account_id: str, client_id: str) -> None:
    """Remove a session from the connected-clients registry."""
    _connected_clients.pop((account_id, client_id), None)


async def _force_disconnect_duplicate(account_id: str, client_id: str) -> None:
    """If a client with the same (account_id, client_id) is already connected,
    force-close its WebSocket and clean up its subscriptions."""
    key = (account_id, client_id)
    existing = _connected_clients.get(key)
    if existing is not None:
        logger.info("IoT broker: duplicate client_id=%s, forcing old connection closed", client_id)
        try:
            await existing._send({"type": "websocket.close", "code": 1000})
        except Exception:
            pass
        await existing.cleanup()
        _connected_clients.pop(key, None)


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
        self._sub_filters: dict[str, str] = {}  # subscription_id → unprefixed topic filter
        self._sub_granted_qos: dict[str, int] = {}  # subscription_id → granted_qos
        self._buffer = bytearray()
        self._next_pid = 1
        self._send_lock = asyncio.Lock()
        self._client_id: str = ""
        self._clean_session: bool = True
        self._in_flight: dict[int, _InFlightMessage] = {}
        self._retransmit_task: asyncio.Task | None = None
        # Last Will and Testament fields
        self._will_topic: str | None = None
        self._will_message: bytes | None = None
        self._will_qos: int = 0
        self._will_retain: bool = False
        self._graceful_disconnect: bool = False

    def _alloc_packet_id(self) -> int:
        """Allocate a monotonically increasing packet ID (1–65535, wrapping)."""
        pid = self._next_pid
        self._next_pid = (self._next_pid % 65535) + 1
        return pid

    def _ensure_retransmit_timer(self) -> None:
        """Start the retransmission background task if not already running."""
        if self._retransmit_task is None or self._retransmit_task.done():
            self._retransmit_task = asyncio.ensure_future(self._retransmit_loop())

    async def _retransmit_loop(self) -> None:
        """Periodically retransmit unacknowledged QoS 1 messages."""
        try:
            while self._in_flight:
                await asyncio.sleep(_RETRANSMIT_INTERVAL_SECONDS)
                now = asyncio.get_event_loop().time()
                for pid, msg in list(self._in_flight.items()):
                    if now - msg.sent_at >= _RETRANSMIT_INTERVAL_SECONDS:
                        msg.retransmit_count += 1
                        msg.sent_at = now
                        await self.send_bytes(
                            _make_publish(msg.topic, msg.payload, qos=1, packet_id=pid, dup=True)
                        )
        except asyncio.CancelledError:
            pass

    async def send_bytes(self, b: bytes) -> None:
        async with self._send_lock:
            await self._send({"type": "websocket.send", "bytes": b})

    async def deliver_to_client(self, topic: str, payload: bytes, qos: int) -> None:
        """Deliver a message to the WebSocket client respecting effective QoS."""
        if qos == 0:
            await self.send_bytes(_make_publish(topic, payload, qos=0))
        else:
            # QoS 1: assign packet ID, track in-flight, start retransmit timer
            pid = self._alloc_packet_id()
            self._in_flight[pid] = _InFlightMessage(pid, topic, payload)
            await self.send_bytes(_make_publish(topic, payload, qos=1, packet_id=pid))
            self._ensure_retransmit_timer()

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
            # Parse MQTT 3.1.1 CONNECT variable header:
            # Protocol Name (UTF-8 string), Protocol Level (1 byte),
            # Connect Flags (1 byte), Keep Alive (2 bytes)
            off = 0
            # Skip Protocol Name
            _proto_name, off = _read_string(body, off)
            # Skip Protocol Level
            off += 1
            # Connect Flags byte
            if off >= len(body):
                await self.send_bytes(_make_connack(return_code=0))
                return True
            connect_flags = body[off]
            off += 1
            # Skip Keep Alive (2 bytes)
            off += 2

            # Extract Will fields from Connect Flags
            will_flag = bool(connect_flags & 0x04)  # bit 2
            will_qos = (connect_flags >> 3) & 0x03  # bits 4-3
            will_retain = bool(connect_flags & 0x20)  # bit 5
            clean_session = bool(connect_flags & 0x02)  # bit 1
            username_flag = bool(connect_flags & 0x80)  # bit 7
            password_flag = bool(connect_flags & 0x40)  # bit 6

            self._clean_session = clean_session

            # --- Payload ---
            # 1. Client ID (UTF-8 string)
            if off < len(body):
                client_id, off = _read_string(body, off)
            else:
                client_id = ""
            if not client_id:
                client_id = uuid.uuid4().hex  # Auto-generate for empty Client ID
            self._client_id = client_id
            # 2. Will Topic + Will Message (if Will Flag set)
            if will_flag:
                if off < len(body):
                    will_topic, off = _read_string(body, off)
                else:
                    will_topic = ""
                # Will Message is length-prefixed bytes
                if off + 2 <= len(body):
                    msg_len = struct.unpack_from("!H", body, off)[0]
                    off += 2
                    will_message = body[off:off + msg_len]
                    off += msg_len
                else:
                    will_message = b""
                self._will_topic = will_topic
                self._will_message = will_message
                self._will_qos = will_qos
                self._will_retain = will_retain
            else:
                # No Will — clear any previously stored Will (reconnect case)
                self._will_topic = None
                self._will_message = None
                self._will_qos = 0
                self._will_retain = False

            # Reset graceful disconnect flag on new CONNECT (reconnect)
            self._graceful_disconnect = False

            # Duplicate Client ID detection: force-disconnect any existing session
            await _force_disconnect_duplicate(self.account_id, self._client_id)
            # Register this session in the connected-clients registry
            _register_client(self.account_id, self._client_id, self)

            # Persistent session handling
            session_key = (self.account_id, self._client_id)
            session_present = False

            if clean_session:
                # cleanSession=1: discard any prior session state
                _persistent_sessions.pop(session_key, None)
            else:
                # cleanSession=0: check for existing persistent session
                existing_ps = _persistent_sessions.get(session_key)
                if existing_ps is not None and not _is_session_expired(existing_ps):
                    # Restore subscriptions from the persistent session
                    session_present = True
                    for topic_filter in existing_ps.subscriptions:
                        sid = await subscribe(
                            self.account_id, topic_filter, self.deliver_to_client, 1
                        )
                        self._sub_ids.append(sid)
                        self._sub_filters[sid] = topic_filter
                        self._sub_granted_qos[sid] = 1
                    # Send CONNACK with sessionPresent=1
                    await self.send_bytes(_make_connack(return_code=0, session_present=True))
                    # Deliver queued messages
                    queued = existing_ps.queued_messages[:]
                    existing_ps.queued_messages.clear()
                    for q_topic, q_payload, q_qos in queued:
                        await self.deliver_to_client(q_topic, q_payload, q_qos)
                    return True
                else:
                    # No prior session or expired: create new persistent session entry
                    _persistent_sessions[session_key] = _PersistentSessionState(
                        subscriptions=[], created_at=time.time()
                    )

            await self.send_bytes(_make_connack(return_code=0, session_present=session_present))
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
            # Validate topic before forwarding (same rules as HTTP Publish path)
            if not _validate_publish_topic(topic):
                logger.warning("IoT broker: PUBLISH rejected — invalid topic: %r", topic)
                return False
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
                sid = await subscribe(self.account_id, topic, self.deliver_to_client, granted_qos)
                self._sub_ids.append(sid)
                self._sub_filters[sid] = topic  # Store unprefixed topic filter
                self._sub_granted_qos[sid] = granted_qos
            await self.send_bytes(_make_suback(packet_id, granted))
            return True

        if pkt_type == PKT_PUBACK:
            if len(body) >= 2:
                packet_id = struct.unpack_from("!H", body, 0)[0]
                self._in_flight.pop(packet_id, None)
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
            self._graceful_disconnect = True
            return False

        # Ignore anything we don't recognise (PUBREC/PUBREL/PUBCOMP for QoS 2 etc.)
        return True

    async def cleanup(self) -> None:
        # Cancel retransmit task
        if self._retransmit_task is not None and not self._retransmit_task.done():
            self._retransmit_task.cancel()
            try:
                await self._retransmit_task
            except asyncio.CancelledError:
                pass
            self._retransmit_task = None
        self._in_flight.clear()
        # Publish Will message on ungraceful disconnect
        if not self._graceful_disconnect and self._will_topic is not None:
            await publish(
                self.account_id,
                self._will_topic,
                self._will_message or b"",
                qos=self._will_qos,
                retain=self._will_retain,
            )
        # Preserve session state for persistent sessions before unsubscribing
        if not self._clean_session and self._client_id:
            self._preserve_session()
        for sid in self._sub_ids:
            await unsubscribe(sid)
        self._sub_ids.clear()
        self._sub_filters.clear()
        self._sub_granted_qos.clear()
        # Deregister from connected-clients registry
        if self._client_id:
            _deregister_client(self.account_id, self._client_id)

    def _preserve_session(self) -> None:
        """Store current subscriptions in _persistent_sessions for later restoration."""
        session_key = (self.account_id, self._client_id)
        # Extract unprefixed topic filters from current subscriptions
        unprefixed_filters = list(self._sub_filters.values())
        existing = _persistent_sessions.get(session_key)
        if existing is not None:
            # Update subscriptions and refresh timestamp
            existing.subscriptions = unprefixed_filters
            existing.created_at = time.time()
        else:
            _persistent_sessions[session_key] = _PersistentSessionState(
                subscriptions=unprefixed_filters, created_at=time.time()
            )


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
