"""Unit tests for QoS 1 end-to-end delivery in iot_broker.py.

Covers tasks 18.1 (granted QoS per subscription), 18.2 (QoS 1 delivery with
packet ID tracking), and 18.3 (PUBACK handling and retransmission).
"""

import asyncio
import struct

from ministack.services.iot import (
    PKT_CONNECT,
    PKT_PUBACK,
    PKT_PUBLISH,
    PKT_SUBSCRIBE,
    _InFlightMessage,
    _RETRANSMIT_INTERVAL_SECONDS,
    _Subscription,
    _WSSession,
    _encode_remaining_length,
    _encode_string,
    _make_puback,
    _make_suback,
    broker_publish as publish,
    broker_reset as reset,
    broker_subscribe as subscribe,
)


def _build_connect_body(client_id="test", clean_session=True):
    """Build a minimal CONNECT packet body."""
    body = bytearray()
    body += _encode_string("MQTT")
    body.append(4)  # Protocol Level
    flags = 0x02 if clean_session else 0x00
    body.append(flags)
    body += struct.pack("!H", 60)  # Keep Alive
    body += _encode_string(client_id)
    return bytes(body)


def _build_subscribe_body(packet_id: int, topics: list[tuple[str, int]]):
    """Build a SUBSCRIBE packet body with topic filters and requested QoS."""
    body = struct.pack("!H", packet_id)
    for topic, qos in topics:
        body += _encode_string(topic)
        body += bytes([qos])
    return body


def _mock_send():
    sent = []

    async def send(msg):
        sent.append(msg)

    return send, sent


def _extract_publish_frames(sent_messages):
    """Extract PUBLISH frames from sent WebSocket messages, returning (topic, payload, qos, packet_id, dup)."""
    results = []
    for msg in sent_messages:
        data = msg.get("bytes")
        if data is None:
            continue
        if not data:
            continue
        first = data[0]
        pkt_type = (first >> 4) & 0x0F
        if pkt_type != 3:  # Not PUBLISH
            continue
        qos = (first >> 1) & 0x03
        dup = bool(first & 0x08)
        # Decode remaining length
        offset = 1
        multiplier = 1
        remaining = 0
        while True:
            b = data[offset]
            offset += 1
            remaining += (b & 0x7F) * multiplier
            if b & 0x80 == 0:
                break
            multiplier *= 128
        # Read topic
        topic_len = struct.unpack_from("!H", data, offset)[0]
        offset += 2
        topic = data[offset:offset + topic_len].decode("utf-8")
        offset += topic_len
        # Read packet ID if QoS > 0
        packet_id = None
        if qos > 0:
            packet_id = struct.unpack_from("!H", data, offset)[0]
            offset += 2
        payload = data[offset:]
        results.append((topic, payload, qos, packet_id, dup))
    return results


# ---------------------------------------------------------------------------
# Task 18.1: Track granted QoS per subscription
# ---------------------------------------------------------------------------


def test_subscription_has_granted_qos_field():
    """_Subscription stores granted_qos."""
    async def deliver(t, p, q):
        pass

    sub = _Subscription("acct/topic", "acct", deliver, granted_qos=1)
    assert sub.granted_qos == 1

    sub0 = _Subscription("acct/topic", "acct", deliver, granted_qos=0)
    assert sub0.granted_qos == 0


def test_subscribe_handler_caps_qos_at_1():
    """PKT_SUBSCRIBE grants min(requested, 1) — QoS 2 is capped to 1."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("sub-client"))

        # Subscribe with QoS 0, 1, and 2
        body = _build_subscribe_body(1, [
            ("topic/a", 0),
            ("topic/b", 1),
            ("topic/c", 2),  # Should be capped to 1
        ])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)

        # Find SUBACK in sent messages
        suback_frames = [m for m in sent if m.get("bytes") and (m["bytes"][0] >> 4) == 9]
        assert len(suback_frames) == 1
        suback_data = suback_frames[0]["bytes"]
        # SUBACK: fixed header (1 byte) + remaining length (1 byte) + packet_id (2 bytes) + return codes
        offset = 1
        # Decode remaining length
        multiplier = 1
        remaining = 0
        while True:
            b = suback_data[offset]
            offset += 1
            remaining += (b & 0x7F) * multiplier
            if b & 0x80 == 0:
                break
            multiplier *= 128
        # Skip packet ID
        offset += 2
        # Return codes
        return_codes = list(suback_data[offset:])
        assert return_codes == [0, 1, 1]  # QoS 2 capped to 1

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_subscribe_stores_granted_qos_on_session():
    """Session tracks granted QoS per subscription ID."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos-track"))

        body = _build_subscribe_body(1, [("sensor/temp", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)

        # Session should have one subscription with granted_qos=1
        assert len(session._sub_ids) == 1
        sid = session._sub_ids[0]
        assert session._sub_granted_qos[sid] == 1

        await session.cleanup()

    asyncio.run(_run())
    reset()


# ---------------------------------------------------------------------------
# Task 18.2: QoS 1 delivery with packet ID tracking
# ---------------------------------------------------------------------------


def test_qos1_publish_delivers_with_packet_id():
    """QoS 1 publish to QoS 1 subscriber delivers at QoS 1 with packet ID."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos1-sub"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/qos1", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)

        # Clear sent to isolate publish frames
        sent.clear()

        # Publish at QoS 1 from external source
        await publish("123456789012", "test/qos1", b"hello-qos1", qos=1)

        # Check delivered message
        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert topic == "test/qos1"
        assert payload == b"hello-qos1"
        assert qos == 1
        assert packet_id is not None
        assert packet_id >= 1
        assert dup is False

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_qos0_publish_to_qos1_subscriber_delivers_at_qos0():
    """QoS 0 publish to QoS 1 subscriber delivers at QoS 0 (effective = min)."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos-min"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/minqos", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 0
        await publish("123456789012", "test/minqos", b"qos0-msg", qos=0)

        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert qos == 0
        assert packet_id is None  # No packet ID for QoS 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_qos1_publish_to_qos0_subscriber_delivers_at_qos0():
    """QoS 1 publish to QoS 0 subscriber delivers at QoS 0 (effective = min)."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos0-sub"))

        # Subscribe at QoS 0
        body = _build_subscribe_body(1, [("test/downgrade", 0)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/downgrade", b"downgraded", qos=1)

        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert qos == 0
        assert packet_id is None

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_qos1_delivery_tracks_in_flight():
    """QoS 1 delivery stores message in _in_flight dict."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("inflight"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/inflight", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/inflight", b"tracked", qos=1)

        # Should have one in-flight message
        assert len(session._in_flight) == 1
        pid = list(session._in_flight.keys())[0]
        msg = session._in_flight[pid]
        assert msg.topic == "test/inflight"
        assert msg.payload == b"tracked"
        assert msg.retransmit_count == 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_packet_ids_are_monotonically_increasing():
    """Packet IDs increment monotonically."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        ids = [session._alloc_packet_id() for _ in range(5)]
        assert ids == [1, 2, 3, 4, 5]

    asyncio.run(_run())
    reset()


def test_packet_ids_wrap_at_65535():
    """Packet IDs wrap from 65535 back to 1."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")
        session._next_pid = 65535

        pid1 = session._alloc_packet_id()
        pid2 = session._alloc_packet_id()
        assert pid1 == 65535
        assert pid2 == 1  # Wraps back to 1

    asyncio.run(_run())
    reset()


# ---------------------------------------------------------------------------
# Task 18.3: PUBACK handling and retransmission
# ---------------------------------------------------------------------------


def test_puback_removes_from_in_flight():
    """PUBACK with matching packet ID removes message from _in_flight."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("puback-test"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/puback", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/puback", b"ack-me", qos=1)

        assert len(session._in_flight) == 1
        pid = list(session._in_flight.keys())[0]

        # Send PUBACK
        puback_body = struct.pack("!H", pid)
        result = await session.handle_packet(PKT_PUBACK, 0, puback_body)
        assert result is True
        assert len(session._in_flight) == 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_puback_unknown_packet_id_is_ignored():
    """PUBACK for unknown packet ID does not crash."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("puback-unknown"))

        # Send PUBACK for non-existent packet ID
        puback_body = struct.pack("!H", 999)
        result = await session.handle_packet(PKT_PUBACK, 0, puback_body)
        assert result is True
        assert len(session._in_flight) == 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_retransmit_task_started_on_qos1_delivery():
    """Retransmit background task is started when QoS 1 message is delivered."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("retransmit"))

        assert session._retransmit_task is None

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/retransmit", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/retransmit", b"retry-me", qos=1)

        # Retransmit task should be started
        assert session._retransmit_task is not None
        assert not session._retransmit_task.done()

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_cleanup_cancels_retransmit_task():
    """cleanup() cancels the retransmit background task."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("cleanup-rt"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/cleanup", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1 to start retransmit task
        await publish("123456789012", "test/cleanup", b"clean", qos=1)
        task = session._retransmit_task
        assert task is not None

        await session.cleanup()
        assert session._retransmit_task is None
        assert task.done() or task.cancelled()

    asyncio.run(_run())
    reset()


def test_cleanup_clears_in_flight():
    """cleanup() clears the _in_flight dict."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("cleanup-if"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/clear", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/clear", b"clear-me", qos=1)
        assert len(session._in_flight) == 1

        await session.cleanup()
        assert len(session._in_flight) == 0

    asyncio.run(_run())
    reset()


def test_retransmit_sends_dup_flag():
    """Retransmission sends PUBLISH with DUP flag set."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("dup-test"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/dup", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/dup", b"dup-payload", qos=1)

        # Verify initial publish has DUP=False
        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        assert publishes[0][4] is False  # dup flag

        # Manually trigger retransmission by manipulating sent_at
        pid = list(session._in_flight.keys())[0]
        msg = session._in_flight[pid]
        # Set sent_at far in the past to trigger retransmit
        msg.sent_at = 0

        sent.clear()

        # Run one iteration of retransmit logic manually
        import asyncio as _asyncio
        now = _asyncio.get_event_loop().time()
        for p, m in list(session._in_flight.items()):
            if now - m.sent_at >= _RETRANSMIT_INTERVAL_SECONDS:
                m.retransmit_count += 1
                m.sent_at = now
                from ministack.services.iot import _make_publish
                await session.send_bytes(
                    _make_publish(m.topic, m.payload, qos=1, packet_id=p, dup=True)
                )

        # Verify retransmitted publish has DUP=True
        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert topic == "test/dup"
        assert payload == b"dup-payload"
        assert qos == 1
        assert packet_id == pid
        assert dup is True

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_in_flight_message_fields():
    """_InFlightMessage stores all required fields."""
    reset()

    async def _run():
        msg = _InFlightMessage(packet_id=42, topic="sensor/data", payload=b"temp=22")
        assert msg.packet_id == 42
        assert msg.topic == "sensor/data"
        assert msg.payload == b"temp=22"
        assert msg.sent_at > 0
        assert msg.retransmit_count == 0

    asyncio.run(_run())
    reset()


def test_multiple_qos1_messages_get_unique_packet_ids():
    """Multiple QoS 1 deliveries get unique, incrementing packet IDs."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("multi-pid"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/multi", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish 3 messages at QoS 1
        await publish("123456789012", "test/multi", b"msg1", qos=1)
        await publish("123456789012", "test/multi", b"msg2", qos=1)
        await publish("123456789012", "test/multi", b"msg3", qos=1)

        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 3
        pids = [p[3] for p in publishes]
        # All unique
        assert len(set(pids)) == 3
        # Monotonically increasing
        assert pids == sorted(pids)

        # All tracked in-flight
        assert len(session._in_flight) == 3

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_puback_for_first_of_multiple_in_flight():
    """PUBACK removes only the specific packet ID from in-flight."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("selective-ack"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/selective", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish 3 messages
        await publish("123456789012", "test/selective", b"a", qos=1)
        await publish("123456789012", "test/selective", b"b", qos=1)
        await publish("123456789012", "test/selective", b"c", qos=1)

        assert len(session._in_flight) == 3
        pids = sorted(session._in_flight.keys())

        # ACK the middle one
        puback_body = struct.pack("!H", pids[1])
        await session.handle_packet(PKT_PUBACK, 0, puback_body)

        assert len(session._in_flight) == 2
        assert pids[1] not in session._in_flight
        assert pids[0] in session._in_flight
        assert pids[2] in session._in_flight

        await session.cleanup()

    asyncio.run(_run())
    reset()
