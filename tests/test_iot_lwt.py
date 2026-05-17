"""Unit tests for Last Will and Testament (LWT) support in iot_broker.py."""

import asyncio
import struct

from ministack.services.iot import (
    PKT_CONNECT,
    PKT_DISCONNECT,
    _WSSession,
    _encode_string,
    broker_reset as reset,
    broker_subscribe as subscribe,
)


def _build_connect_body(
    client_id="test",
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


def _mock_send():
    sent = []

    async def send(msg):
        sent.append(msg)

    return send, sent


def test_will_fields_parsed_from_connect():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device1",
            will_flag=True,
            will_qos=1,
            will_retain=True,
            will_topic="devices/device1/status",
            will_message=b"offline",
        )
        result = await session.handle_packet(PKT_CONNECT, 0, body)
        assert result is True
        assert session._will_topic == "devices/device1/status"
        assert session._will_message == b"offline"
        assert session._will_qos == 1
        assert session._will_retain is True

    asyncio.run(_run())
    reset()


def test_no_will_when_flag_not_set():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="device2", will_flag=False)
        await session.handle_packet(PKT_CONNECT, 0, body)
        assert session._will_topic is None
        assert session._will_message is None
        assert session._will_qos == 0
        assert session._will_retain is False

    asyncio.run(_run())
    reset()


def test_graceful_disconnect_does_not_publish_will():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device3",
            will_flag=True,
            will_qos=0,
            will_retain=False,
            will_topic="devices/device3/status",
            will_message=b"offline",
        )
        await session.handle_packet(PKT_CONNECT, 0, body)

        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        await subscribe("123456789012", "devices/device3/status", on_msg)

        # Graceful disconnect
        result = await session.handle_packet(PKT_DISCONNECT, 0, b"")
        assert result is False
        assert session._graceful_disconnect is True
        await session.cleanup()
        assert len(received) == 0

    asyncio.run(_run())
    reset()


def test_ungraceful_disconnect_publishes_will():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device4",
            will_flag=True,
            will_qos=1,
            will_retain=False,
            will_topic="devices/device4/status",
            will_message=b"gone",
        )
        await session.handle_packet(PKT_CONNECT, 0, body)

        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        # Subscribe at QoS 1 so effective QoS = min(publish_qos=1, granted_qos=1) = 1
        await subscribe("123456789012", "devices/device4/status", on_msg, granted_qos=1)

        # Ungraceful disconnect (no DISCONNECT packet)
        await session.cleanup()
        assert len(received) == 1
        assert received[0] == ("devices/device4/status", b"gone", 1)

    asyncio.run(_run())
    reset()


def test_will_retain_stores_retained_message():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device5",
            will_flag=True,
            will_qos=0,
            will_retain=True,
            will_topic="devices/device5/status",
            will_message=b"dead",
        )
        await session.handle_packet(PKT_CONNECT, 0, body)
        # Ungraceful disconnect publishes Will with retain
        await session.cleanup()

        # New subscriber should get the retained message
        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        await subscribe("123456789012", "devices/device5/status", on_msg)
        assert len(received) == 1
        assert received[0] == ("devices/device5/status", b"dead", 0)

    asyncio.run(_run())
    reset()


def test_reconnect_replaces_will_fields():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body1 = _build_connect_body(
            client_id="device6",
            will_flag=True,
            will_qos=0,
            will_retain=False,
            will_topic="old/topic",
            will_message=b"old",
        )
        await session.handle_packet(PKT_CONNECT, 0, body1)
        assert session._will_topic == "old/topic"

        # Reconnect with new Will
        body2 = _build_connect_body(
            client_id="device6",
            will_flag=True,
            will_qos=1,
            will_retain=True,
            will_topic="new/topic",
            will_message=b"new",
        )
        await session.handle_packet(PKT_CONNECT, 0, body2)
        assert session._will_topic == "new/topic"
        assert session._will_message == b"new"
        assert session._will_qos == 1
        assert session._will_retain is True

    asyncio.run(_run())
    reset()


def test_reconnect_clears_graceful_disconnect_flag():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="device7", will_flag=False)
        await session.handle_packet(PKT_CONNECT, 0, body)

        # Graceful disconnect
        await session.handle_packet(PKT_DISCONNECT, 0, b"")
        assert session._graceful_disconnect is True

        # Reconnect resets the flag
        await session.handle_packet(PKT_CONNECT, 0, body)
        assert session._graceful_disconnect is False

    asyncio.run(_run())
    reset()


def test_reconnect_without_will_clears_previous_will():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        # First connect with Will
        body1 = _build_connect_body(
            client_id="device8",
            will_flag=True,
            will_qos=1,
            will_retain=True,
            will_topic="presence/device8",
            will_message=b"offline",
        )
        await session.handle_packet(PKT_CONNECT, 0, body1)
        assert session._will_topic == "presence/device8"

        # Reconnect without Will
        body2 = _build_connect_body(client_id="device8", will_flag=False)
        await session.handle_packet(PKT_CONNECT, 0, body2)
        assert session._will_topic is None
        assert session._will_message is None

        # Ungraceful disconnect should NOT publish anything
        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        await subscribe("123456789012", "presence/device8", on_msg)
        await session.cleanup()
        assert len(received) == 0

    asyncio.run(_run())
    reset()
