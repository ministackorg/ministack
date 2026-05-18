"""Unit tests for persistent sessions (cleanSession flag) in iot_broker.py."""

import asyncio
import struct

from ministack.services.iot import (
    PKT_CONNECT,
    PKT_CONNACK,
    PKT_DISCONNECT,
    PKT_PUBLISH,
    PKT_SUBSCRIBE,
    _WSSession,
    _encode_string,
    _persistent_sessions,
    broker_publish as publish,
    broker_reset as reset,
    broker_subscribe as subscribe,
)


def _build_connect_body(
    client_id="test",
    clean_session=True,
    will_flag=False,
    will_qos=0,
    will_retain=False,
    will_topic="",
    will_message=b"",
):
    """Build a CONNECT packet body (variable header + payload)."""
    body = bytearray()
    # Protocol Name
    body += _encode_string("MQTT")
    # Protocol Level (4 for MQTT 3.1.1)
    body.append(4)
    # Connect Flags
    flags = 0
    if clean_session:
        flags |= 0x02
    if will_flag:
        flags |= 0x04
        flags |= (will_qos & 0x03) << 3
        if will_retain:
            flags |= 0x20
    body.append(flags)
    # Keep Alive (60 seconds)
    body += struct.pack("!H", 60)
    # Payload: Client ID
    body += _encode_string(client_id)
    # Will Topic + Will Message (if Will Flag set)
    if will_flag:
        body += _encode_string(will_topic)
        msg = will_message if isinstance(will_message, bytes) else will_message.encode()
        body += struct.pack("!H", len(msg)) + msg
    return bytes(body)


def _build_subscribe_body(packet_id, topics_qos):
    """Build a SUBSCRIBE packet body. topics_qos is list of (topic, qos)."""
    body = struct.pack("!H", packet_id)
    for topic, qos in topics_qos:
        body += _encode_string(topic)
        body += bytes([qos])
    return body


def _mock_send():
    sent = []

    async def send(msg):
        sent.append(msg)

    return send, sent


def _parse_connack(sent_messages):
    """Extract sessionPresent flag from CONNACK in sent messages."""
    for msg in sent_messages:
        data = msg.get("bytes")
        if data and len(data) >= 4:
            pkt_type = (data[0] >> 4) & 0x0F
            if pkt_type == PKT_CONNACK:
                # CONNACK: fixed header (1 byte) + remaining length (1 byte) + flags (1 byte) + return code (1 byte)
                session_present = bool(data[2] & 0x01)
                return_code = data[3]
                return session_present, return_code
    return None, None


def test_clean_session_1_sends_session_present_0():
    """cleanSession=1 always sends sessionPresent=0."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="client1", clean_session=True)
        await session.handle_packet(PKT_CONNECT, 0, body)

        session_present, return_code = _parse_connack(sent)
        assert session_present is False
        assert return_code == 0

    asyncio.run(_run())
    reset()


def test_clean_session_0_no_prior_session_sends_session_present_0():
    """cleanSession=0 with no prior session sends sessionPresent=0."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="client1", clean_session=False)
        await session.handle_packet(PKT_CONNECT, 0, body)

        session_present, return_code = _parse_connack(sent)
        assert session_present is False
        assert return_code == 0

    asyncio.run(_run())
    reset()


def test_persistent_session_subscribe_disconnect_reconnect_restores():
    """Connect with cleanSession=0 → subscribe → disconnect → reconnect → sessionPresent=1 and subscriptions restored."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe to a topic
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device1", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        # Subscribe to "sensor/temp"
        sub_body = _build_subscribe_body(1, [("sensor/temp", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect (graceful)
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Second connection: cleanSession=0, same client_id
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, return_code = _parse_connack(sent2)
        assert session_present is True
        assert return_code == 0

        # Verify subscriptions are restored by publishing a message
        received = []

        # The session should already be subscribed, so publish should deliver
        await publish("123456789012", "sensor/temp", b"25C", qos=1)

        # Check that session2 received the message
        # The message should be in sent2 as a PUBLISH packet
        publish_found = False
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_found = True
                break
        assert publish_found, "Restored subscription should receive published messages"

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_clean_session_1_discards_prior_state():
    """cleanSession=1 discards any prior persistent session state."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body_persistent = _build_connect_body(client_id="device2", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body_persistent)

        sub_body = _build_subscribe_body(1, [("alerts/#", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Verify persistent session exists
        assert ("123456789012", "device2") in _persistent_sessions

        # Second connection: cleanSession=1 — should discard prior state
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        connect_body_clean = _build_connect_body(client_id="device2", clean_session=True)
        await session2.handle_packet(PKT_CONNECT, 0, connect_body_clean)

        session_present, return_code = _parse_connack(sent2)
        assert session_present is False
        assert return_code == 0

        # Verify persistent session was discarded
        assert ("123456789012", "device2") not in _persistent_sessions

        # Publish to the old subscription topic — should NOT be delivered
        await publish("123456789012", "alerts/fire", b"alarm", qos=1)

        publish_found = False
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_found = True
                break
        assert not publish_found, "cleanSession=1 should not restore prior subscriptions"

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_offline_qos1_messages_queued_and_delivered_on_reconnect():
    """Persistent session disconnects; QoS 1 messages published; reconnect delivers queued messages."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device3", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("data/stream", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Publish QoS 1 messages while client is offline
        await publish("123456789012", "data/stream", b"msg1", qos=1)
        await publish("123456789012", "data/stream", b"msg2", qos=1)
        await publish("123456789012", "data/stream", b"msg3", qos=1)

        # Verify messages are queued
        ps = _persistent_sessions.get(("123456789012", "device3"))
        assert ps is not None
        assert len(ps.queued_messages) == 3

        # Reconnect with cleanSession=0
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, _ = _parse_connack(sent2)
        assert session_present is True

        # Verify queued messages were delivered
        publish_messages = []
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_messages.append(data)

        assert len(publish_messages) == 3, f"Expected 3 queued messages delivered, got {len(publish_messages)}"

        # Verify queue is now empty
        assert len(ps.queued_messages) == 0

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_qos0_messages_not_queued_for_offline_sessions():
    """QoS 0 messages should NOT be queued for offline persistent sessions."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device4", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("events/log", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Publish QoS 0 messages while client is offline
        await publish("123456789012", "events/log", b"info1", qos=0)
        await publish("123456789012", "events/log", b"info2", qos=0)

        # Verify no messages queued (QoS 0 not queued)
        ps = _persistent_sessions.get(("123456789012", "device4"))
        assert ps is not None
        assert len(ps.queued_messages) == 0

    asyncio.run(_run())
    reset()


def test_queue_bounded_to_1000_messages():
    """Queue should be bounded to 1000 messages, dropping oldest on overflow."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device5", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("bulk/data", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Publish 1050 QoS 1 messages while client is offline
        for i in range(1050):
            await publish("123456789012", "bulk/data", f"msg{i}".encode(), qos=1)

        # Verify queue is bounded to 1000
        ps = _persistent_sessions.get(("123456789012", "device5"))
        assert ps is not None
        assert len(ps.queued_messages) == 1000

        # Verify oldest messages were dropped (first 50 should be gone)
        first_topic, first_payload, first_qos = ps.queued_messages[0]
        assert first_payload == b"msg50"

    asyncio.run(_run())
    reset()


def test_expired_session_not_restored():
    """An expired persistent session should not be restored."""
    reset()

    async def _run():
        import time

        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device6", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("temp/data", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Manually expire the session by setting created_at far in the past
        ps = _persistent_sessions.get(("123456789012", "device6"))
        assert ps is not None
        ps.created_at = time.time() - 7200  # 2 hours ago (default expiry is 1 hour)

        # Reconnect with cleanSession=0
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, return_code = _parse_connack(sent2)
        assert session_present is False  # Expired session not restored
        assert return_code == 0

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_wildcard_subscription_persisted_and_restored():
    """Wildcard subscriptions should be persisted and restored correctly."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe with wildcard
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device7", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("sensors/+/temp", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Reconnect
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, _ = _parse_connack(sent2)
        assert session_present is True

        # Publish to a topic matching the wildcard
        await publish("123456789012", "sensors/room1/temp", b"22C", qos=1)

        publish_found = False
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_found = True
                break
        assert publish_found, "Restored wildcard subscription should receive matching messages"

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_different_accounts_sessions_isolated():
    """Persistent sessions are scoped by (account_id, client_id)."""
    reset()

    async def _run():
        # Account A: connect, subscribe, disconnect
        send_a, sent_a = _mock_send()
        session_a = _WSSession(send_a, "account_A")

        connect_body = _build_connect_body(client_id="shared_id", clean_session=False)
        await session_a.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("topic/a", 1)])
        await session_a.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)
        await session_a.handle_packet(PKT_DISCONNECT, 0, b"")
        await session_a.cleanup()

        # Account B: connect with same client_id — should NOT see account A's session
        send_b, sent_b = _mock_send()
        session_b = _WSSession(send_b, "account_B")

        await session_b.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, _ = _parse_connack(sent_b)
        assert session_present is False  # No prior session for account_B

        await session_b.cleanup()

    asyncio.run(_run())
    reset()
