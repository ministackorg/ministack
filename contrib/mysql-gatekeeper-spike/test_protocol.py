"""Offline fail-closed checks for the spike's explicitly limited handshake."""
import socket

import pytest
from relay import client_identity, read
from trusted_bypass import handshake


@pytest.mark.parametrize("plugin", [b"mysql_native_password", b"caching_sha2_password"])
def test_supported_client_identity(plugin):
    assert client_identity(handshake(plugin)) == "iam_user"


@pytest.mark.parametrize("payload", [
    b"", b"\0" * 32, handshake()[:-1], handshake(b"ministack_iam_gate_v1"),
    handshake(b"mysql_clear_password"), handshake(b"unknown"),
])
def test_malformed_or_spoofed_identity(payload):
    with pytest.raises((ValueError, IndexError)):
        client_identity(payload)


@pytest.mark.parametrize("flag", [1 << 5, 1 << 26, 1 << 7])
def test_unsupported_capabilities(flag):
    payload = handshake()
    payload = (int.from_bytes(payload[:4], "little") | flag).to_bytes(4, "little") + payload[4:]
    with pytest.raises(ValueError):
        client_identity(payload)


def test_oversized_frame_rejected_before_reading_body():
    left, right = socket.socketpair()
    with left, right:
        left.sendall((1024 * 1024 + 1).to_bytes(3, "little") + b"\0")
        with pytest.raises(ValueError):
            read(right)
