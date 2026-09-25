"""Disposable, opt-in protocol spike. Not production code or a CI test lane."""

import contextlib
import secrets
import select
import socketserver
import ssl
import struct
import subprocess
import threading
import time

import pymysql
import pytest
from pymysql.constants import CLIENT


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data


def read_packet(sock):
    header = exact(sock, 4)
    length = int.from_bytes(header[:3], "little")
    if length > 1024 * 1024:
        raise ValueError("spike packet limit")
    return header[3], exact(sock, length)


def send_packet(sock, seq, body):
    sock.sendall(len(body).to_bytes(3, "little") + bytes([seq % 256]) + body)


def error(sock, seq):
    send_packet(sock, seq, b"\xff" + struct.pack("<H", 1045) + b"#28000Spike access denied")


class Gatekeeper(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, backend, context, strict):
        self.backend = backend
        self.context = context
        self.strict = strict
        self.attempts = []
        super().__init__(("127.0.0.1", 0), Handler)


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        front = self.request
        backend = None
        seq = 0
        front.settimeout(5)
        try:
            # Deliberately narrow capabilities: no compression, local infile,
            # multi-statements, or database selection during the handshake.
            flags = CLIENT.PROTOCOL_41 | CLIENT.SECURE_CONNECTION | CLIENT.PLUGIN_AUTH | CLIENT.SSL
            salt = b"0123456789abcdefghij"
            greeting = (b"\x0a8.0.0-spike\0" + struct.pack("<I", 1) + salt[:8] + b"\0"
                        + struct.pack("<H", flags & 65535) + b"\x2d" + struct.pack("<H", 2)
                        + struct.pack("<H", flags >> 16) + b"\x15" + b"\0" * 10
                        + salt[8:] + b"\0mysql_native_password\0")
            send_packet(front, 0, greeting)
            seq, response = read_packet(front)
            secure = False
            if len(response) == 32 and int.from_bytes(response[:4], "little") & CLIENT.SSL:
                front = self.server.context.wrap_socket(front, server_side=True)
                secure = True
                seq, response = read_packet(front)
            if len(response) < 33:
                raise ValueError("short handshake")
            user = response[32:].split(b"\0", 1)[0].decode()
            if self.server.strict and not secure:
                error(front, seq + 1)
                return
            # Request the token only after the transport gate.
            send_packet(front, seq + 1, b"\xfemysql_clear_password\0")
            seq, token = read_packet(front)
            if not token.endswith(b"\0") or b"\0" in token[:-1]:
                raise ValueError("bad token frame")
            db = self.server.backend
            if user not in db["passwords"] or (self.server.strict and token[:-1] != db["token"].encode()):
                error(front, seq + 1)
                return
            # The important experiment: same backend username, NOT root.
            self.server.attempts.append(user)
            backend = pymysql.connect(host="127.0.0.1", port=db["port"], user=user,
                                      password=db["passwords"][user], autocommit=True,
                                      connect_timeout=5, read_timeout=5, write_timeout=5)
            send_packet(front, seq + 1, b"\0\0\0\x02\0\0\0")
            back = backend._sock
            # Relay command packets and opaque backend replies. This is not a
            # full MySQL proxy: only QUERY, QUIT, INIT_DB, PING are supported.
            while True:
                ready, _, _ = select.select([front, back], [], [], 5)
                if not ready:
                    return
                for source in ready:
                    if source is front:
                        sequence, payload = read_packet(front)
                        if sequence != 0 or not payload or payload[0] not in (1, 2, 3, 14):
                            error(front, 1)
                            return
                        send_packet(back, sequence, payload)
                        if payload[0] == 1:
                            return
                    else:
                        data = back.recv(65536)
                        if not data:
                            return
                        front.sendall(data)
        except (OSError, EOFError, ValueError, pymysql.MySQLError):
            with contextlib.suppress(OSError):
                error(front, seq + 1)
        finally:
            if backend is not None:
                backend.close()
            front.close()


@pytest.fixture(scope="module", params=["8.0", "8.4"])
def database(request, tmp_path_factory):
    name = "ministack-gatekeeper-spike-" + secrets.token_hex(4)
    root = secrets.token_hex(16)
    passwords = {u: secrets.token_hex(16) for u in ("app", "iam_user", "locked_user")}
    builder = None
    try:
        command("docker", "run", "-d", "--name", name, "--label", "ministack.task=gatekeeper-spike",
                "-e", "MYSQL_ROOT_PASSWORD=" + root, "-e", "MYSQL_ROOT_HOST=%",
                "-p", "127.0.0.1::3306", "mysql:" + request.param)
        port = int(command("docker", "port", name, "3306/tcp").split(":")[-1])
        deadline = time.monotonic() + 120
        while True:
            try:
                admin = pymysql.connect(host="127.0.0.1", port=port, user="root", password=root, autocommit=True)
                break
            except pymysql.MySQLError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        with admin.cursor() as cur:
            cur.execute("CREATE DATABASE spike")
            cur.execute("CREATE TABLE spike.visible (id INT)")
            cur.execute("INSERT INTO spike.visible VALUES (7)")
            cur.execute("CREATE TABLE spike.hidden (id INT)")
            for user in passwords:
                cur.execute(f"CREATE USER '{user}'@'%%' IDENTIFIED BY %s", (passwords[user],))
                cur.execute(f"GRANT SELECT ON spike.visible TO '{user}'@'%'")
            cur.execute("ALTER USER 'locked_user'@'%' ACCOUNT LOCK")
            cur.execute("SELECT @@plugin_dir")
            plugin_dir = cur.fetchone()[0]
        # Reuse the stage-7 build, with no broker configuration. This proves
        # that moving the external gate does not bypass native plugin auth.
        builder = command("docker", "create", "ministack-iam-stage7-build:local")
        artifact = tmp_path_factory.mktemp("plugin") / "aws_auth_plugin.so"
        command("docker", "cp", f"{builder}:/opt/ministack/mysql-plugins/{request.param}/arm64/aws_auth_plugin.so", str(artifact))
        command("docker", "cp", str(artifact), f"{name}:{plugin_dir}aws_auth_plugin.so")
        with admin.cursor() as cur:
            cur.execute("INSTALL PLUGIN AWSAuthenticationPlugin SONAME 'aws_auth_plugin.so'")
            cur.execute("ALTER USER 'iam_user'@'%' IDENTIFIED WITH AWSAuthenticationPlugin")
        yield dict(port=port, passwords=passwords, token=secrets.token_hex(24), admin=admin)
        admin.close()
    finally:
        if builder:
            command("docker", "rm", "-v", builder)
        command("docker", "rm", "-fv", name)


@pytest.fixture(scope="module")
def tls(tmp_path_factory):
    path = tmp_path_factory.mktemp("tls")
    cert, key = path / "cert.pem", path / "key.pem"
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1",
                    "-keyout", str(key), "-out", str(cert)], check=True, capture_output=True)
    server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server.minimum_version = ssl.TLSVersion.TLSv1_2
    server.load_cert_chain(cert, key)
    return server, ssl.create_default_context(cafile=str(cert))


@contextlib.contextmanager
def gate(database, tls, strict=True):
    with Gatekeeper(database, tls[0], strict) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield server
        finally:
            server.shutdown()
            thread.join()


def connect(server, tls, user="app", token=None, secure=True):
    return pymysql.connect(host="127.0.0.1", port=server.server_address[1], user=user,
                           password=server.backend["token"] if token is None else token,
                           ssl=tls[1] if secure else None, ssl_disabled=not secure, autocommit=True,
                           read_timeout=5, write_timeout=5, connect_timeout=5)


def test_identity_and_grants(database, tls):
    with gate(database, tls) as server, connect(server, tls) as conn, conn.cursor() as cur:
        cur.execute("SELECT CURRENT_USER()")
        assert cur.fetchone()[0] == "app@%"
        cur.execute("SELECT id FROM spike.visible")
        assert cur.fetchone() == (7,)
        for sql in ("SELECT * FROM spike.hidden", "INSERT INTO spike.visible VALUES (8)"):
            with pytest.raises(pymysql.MySQLError) as caught:
                cur.execute(sql)
            assert caught.value.args[0] == 1142
        # Expiring the gate's approval does not break an established session.
        old = database["token"]
        database["token"] = secrets.token_hex(24)
        try:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
            with pytest.raises(pymysql.MySQLError):
                connect(server, tls, token=old)
        finally:
            database["token"] = old


def test_denial_before_backend(database, tls):
    with gate(database, tls) as server:
        for options in ({"token": "invalid"}, {"secure": False}, {"user": "unknown"}):
            with pytest.raises(pymysql.MySQLError):
                connect(server, tls, **options)
        assert server.attempts == []


def test_permissive_plaintext(database, tls):
    with gate(database, tls, strict=False) as server, connect(server, tls, secure=False, token="anything") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT CURRENT_USER()")
            assert cur.fetchone()[0] == "app@%"


def test_bad_backend_password_still_denies(database, tls):
    old = database["passwords"]["app"]
    database["passwords"]["app"] = "wrong-backend-password"
    try:
        with gate(database, tls) as server:
            with pytest.raises(pymysql.MySQLError):
                connect(server, tls)
            assert server.attempts == ["app"]
    finally:
        database["passwords"]["app"] = old


def test_change_user_cannot_bypass_gate(database, tls):
    with gate(database, tls) as server, connect(server, tls) as conn:
        conn._execute_command(17, b"root\0")
        with pytest.raises(pymysql.MySQLError) as caught:
            conn._read_packet()
        assert caught.value.args[0] == 1045


@pytest.mark.parametrize("user", ["locked_user", "iam_user"])
def test_backend_still_controls_login(database, tls, user):
    with gate(database, tls) as server:
        with pytest.raises(pymysql.MySQLError) as caught:
            connect(server, tls, user=user)
        assert caught.value.args[0] == 1045
        assert server.attempts == [user]
