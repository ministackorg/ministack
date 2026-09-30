"""Inactive MySQL IAM adapter: relay password auth and gate IAM auth switches.

Runs in MySQL's network namespace. MySQL listens on loopback only.
No SQL/account rewriting. The caller supplies resource-bound authorization.
Not provisioned by the runtime: backend isolation is mandatory before activation.
"""
import contextlib
import select
import socket
import socketserver
import ssl


def exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data


def read(sock):
    header = exact(sock, 4)
    n = int.from_bytes(header[:3], "little")
    if n > 1024 * 1024:
        raise ValueError("adapter packet limit")
    return header[3], exact(sock, n)


def send(sock, seq, body):
    sock.sendall(len(body).to_bytes(3, "little") + bytes([seq % 256]) + body)


def deny(sock, seq):
    send(sock, seq, b"\xff\x15\x04#28000IAM gate denied")


def limited_greeting(packet):
    # Do not advertise wire modes that this adapter cannot relay safely.
    packet = bytearray(packet)
    lower = packet.index(0, 1) + 14
    upper = lower + 5
    flags = int.from_bytes(packet[lower:lower + 2], "little")
    flags |= int.from_bytes(packet[upper:upper + 2], "little") << 16
    flags &= ~((1 << 5) | (1 << 26) | (1 << 7))
    packet[lower:lower + 2] = (flags & 65535).to_bytes(2, "little")
    packet[upper:upper + 2] = (flags >> 16).to_bytes(2, "little")
    return bytes(packet)


def validate_header(reply):
    """Accept only the explicitly supported protocol and character sets."""
    if len(reply) < 32:
        raise ValueError("short handshake")
    flags = int.from_bytes(reply[:4], "little")
    if not flags & (1 << 19) or not flags & (1 << 9):
        raise ValueError("plugin authentication and protocol 4.1 required")
    if flags & ((1 << 5) | (1 << 26) | (1 << 7)):
        raise ValueError("compression and local infile unsupported")
    if not flags & (1 << 15):
        raise ValueError("secure connection capability required")
    # latin1, utf8mb3, and the tested utf8mb4 collations share ASCII identity.
    if reply[8] not in (8, 33, 45, 46, 255):
        raise ValueError("unsupported handshake charset")


def client_identity(reply):
    """Parse enough HandshakeResponse41 to reject private-plugin spoofing."""
    validate_header(reply)
    flags = int.from_bytes(reply[:4], "little")
    end = reply.index(0, 32)
    name = reply[32:end]
    # MySQL normalizes quoted names and converts the declared charset. Until
    # those forms are supported, restrict identity to unquoted printable ASCII
    # and MySQL's 32-character limit, preventing conversion or truncation drift.
    if not 1 <= len(name) <= 32 or any(c < 33 or c > 126 or c == 39 for c in name):
        raise ValueError("invalid username")
    username = name.decode("ascii")
    pos = end + 1
    if flags & (1 << 21):
        size = reply[pos]
        pos += 1
        if size >= 251:
            width = {252: 2, 253: 3, 254: 8}.get(size)
            if width is None or pos + width > len(reply):
                raise ValueError("invalid auth length")
            size = int.from_bytes(reply[pos:pos + width], "little")
            pos += width
        pos += size
    elif flags & (1 << 15):
        pos += 1 + reply[pos]
    else:
        raise ValueError("secure connection capability required")
    if flags & (1 << 3):
        pos = reply.index(0, pos) + 1
    plugin = reply[pos:reply.index(0, pos)]
    # In particular, never forward the shim's private name from a client:
    # it could suppress the backend auth switch on which the gate depends.
    if plugin not in (b"mysql_native_password", b"caching_sha2_password"):
        raise ValueError("unsupported client auth method")
    return username


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        front = self.request
        back = None
        seq = 0
        phase = "handshake"
        try:
            front.settimeout(5)
            back = socket.create_connection(("127.0.0.1", 3306), timeout=5)
            seq, greeting = read(back)
            send(front, seq, limited_greeting(greeting))
            seq, reply = read(front)
            secure = False
            if len(reply) == 32 and int.from_bytes(reply[:4], "little") & 2048:
                tls_header = reply
                flags = int.from_bytes(reply[:4], "little") & ~(1 << 7)
                reply = flags.to_bytes(4, "little") + reply[4:]
                validate_header(reply)
                send(back, seq, reply)
                # Encryption only on the namespace-local backend leg; server
                # identity verification is not implemented by this adapter.
                back = ssl._create_unverified_context().wrap_socket(back)
                front = self.server.tls.wrap_socket(front, server_side=True)
                secure = True
                seq, reply = read(front)
                # MySQL retains the SSLRequest capabilities/charset. Do not
                # parse a different packet layout or identity on this side.
                if len(reply) < 32 or reply[:4] != tls_header[:4] or reply[8] != tls_header[8]:
                    raise ValueError("TLS handshake header mismatch")
            # libmysqlclient can set LOCAL_FILES even when --local-infile=0.
            # We never advertised it and must not enable it on the backend.
            if len(reply) >= 4:
                flags = int.from_bytes(reply[:4], "little") & ~(1 << 7)
                reply = flags.to_bytes(4, "little") + reply[4:]
            username = client_identity(reply)
            phase = "authentication"
            send(back, seq, reply)
            # Only the trusted backend can select the private IAM method.
            # No client assertion or cached list classifies the SQL account.
            while True:
                seq, packet = read(back)
                if packet[:1] == b"\xff":
                    send(front, seq, packet)
                    return
                if packet[:1] == b"\x00":
                    send(front, seq, packet)
                    break
                is_iam = packet.startswith(b"\xfeministack_iam_gate_v1\0")
                if is_iam and self.server.strict and not secure:
                    deny(front, seq)
                    return
                if is_iam:
                    packet = b"\xfemysql_clear_password\0"
                elif packet[:1] == b"\xfe" and packet[1:].split(b"\0", 1)[0] not in (
                    b"mysql_native_password", b"caching_sha2_password",
                ):
                    raise ValueError("unsupported backend auth method")
                send(front, seq, packet)
                if packet == b"\x01\x03":  # caching_sha2 fast-auth success; OK follows
                    continue
                seq, response = read(front)
                if is_iam:
                    if (len(response) > 65537 or not response.endswith(b"\0") or b"\0" in response[:-1]
                            or not self.server.authorize(username, response[:-1].decode(), self.server.strict)):
                        deny(front, seq + 1)
                        return
                    # Do not expose the frontend token to the accepting shim.
                    response = b"proxy-approved\0"
                send(back, seq, response)
            phase = "commands"
            while True:
                ready = [s for s in (front, back) if isinstance(s, ssl.SSLSocket) and s.pending()]
                if not ready:
                    ready, _, _ = select.select([front, back], [], [], 10)
                if not ready:
                    return
                for source in ready:
                    if source is front:
                        seq, payload = read(front)
                        # Deliberate limited command set; reject CHANGE_USER,
                        # prepared statements, and LOCAL INFILE exchanges.
                        if seq != 0 or not payload or payload[0] not in (1, 2, 3, 14):
                            deny(front, 1)
                            return
                        send(back, seq, payload)
                        if payload[0] == 1:
                            return
                    else:
                        data = back.recv(65536)
                        if not data:
                            return
                        front.sendall(data)
        except Exception as exc:
            # Fail closed without logging usernames, tokens, or exception data.
            known = {"short handshake", "plugin authentication and protocol 4.1 required",
                     "compression and local infile unsupported", "invalid username", "invalid auth length",
                     "secure connection capability required", "unsupported client auth method"}
            reason = str(exc) if type(exc) is ValueError and str(exc) in known else type(exc).__name__
            print("MySQL adapter rejection", phase, reason, flush=True)
            with contextlib.suppress(OSError):
                deny(front, seq + 1)
        finally:
            front.close()
            if back:
                back.close()


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
