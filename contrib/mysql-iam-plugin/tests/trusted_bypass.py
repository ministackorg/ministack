"""Negative control: only run inside the isolated MySQL network namespace."""
import socket
import struct

from ministack.core.mysqlproxy import read, send


def handshake(plugin=b"mysql_native_password"):
    flags = (1 << 9) | (1 << 15) | (1 << 19)
    return struct.pack("<IIB23x", flags, 1024 * 1024, 45) + b"iam_user\0\0" + plugin + b"\0"


if __name__ == "__main__":
    with socket.create_connection(("127.0.0.1", 3306), timeout=3) as sock:
        read(sock)
        send(sock, 1, handshake())
        seq, packet = read(sock)
        assert packet.startswith(b"\xfeministack_iam_gate_v1\0")
        send(sock, seq + 1, b"anything\0")
        assert read(sock)[1][:1] == b"\0"
        print("backend accepted unverified login")
