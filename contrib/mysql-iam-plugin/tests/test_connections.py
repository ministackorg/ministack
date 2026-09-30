"""Opt-in live tests: accepting shim + Python gate + real password users."""
import datetime as dt
import os
import secrets
import socket
import ssl
import subprocess
import time
from pathlib import Path
from unittest.mock import patch

import boto3
import pymysql
import pytest
from proof_authorization import HOST, KEY, REGION

SOURCE = Path(__file__).resolve().parent
LABEL = "ministack.task=accepting-gate-spike"
PYTHON_IMAGE = "ghcr.io/ministackorg/ministack:full"


def signed(secret, *, user="iam_user", host=HOST, age=0):
    client = boto3.client("rds", region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=secret)
    now = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=age)
    with patch("botocore.auth.get_current_datetime", return_value=now):
        return client.generate_db_auth_token(DBHostname=host, Port=3306, DBUsername=user)


def run(*args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, check=True, **kwargs).stdout.strip()


@pytest.fixture(scope="module", params=["8.0", "8.4"])
def setup(request, tmp_path_factory):
    suffix = secrets.token_hex(4)
    db = "accepting-spike-" + suffix
    network = db + "-net"
    containers = []
    root, password, secret = [secrets.token_hex(16) for _ in range(3)]
    token = signed(secret)
    temp = tmp_path_factory.mktemp("accepting-shim")
    compiler = "ministack-spike-compiler80:local" if request.param == "8.0" else "ministack-iam-stage7-build:local"
    linked = run("docker", "run", "--rm", "--label", LABEL, "--entrypoint", "sh",
        "-v", f"{SOURCE.parent}:/src:ro", "-v", f"{temp}:/out", compiler, "-c",
        'set -e; header=$(rpm -ql mysql-community-debugsource | grep "/include/mysql/plugin_auth.h$" | head -1); '
        'g++ -Wall -Wextra -Werror -shared -fPIC -DMYSQL_ABI_CHECK -DMYSQL_DYNAMIC_PLUGIN '
        '-DMINISTACK_IAM_PROXY_AUTH=1 '
        '-I"$(dirname "$(dirname "$header")")" /src/aws_auth_plugin.cc -o /out/accepting_shim.so; '
        'g++ -Wall -Wextra -Werror -shared -fPIC -DMYSQL_ABI_CHECK -DMYSQL_DYNAMIC_PLUGIN '
        '-I"$(dirname "$(dirname "$header")")" /src/aws_auth_plugin.cc -o /out/rejecting_shim.so; '
        'ldd /out/accepting_shim.so; ldd /out/rejecting_shim.so')
    assert "libcurl" not in linked
    run("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
        "-subj", "/CN=localhost", "-addext", "subjectAltName=IP:127.0.0.1",
        "-keyout", str(temp / "key.pem"), "-out", str(temp / "cert.pem"))
    context = ssl.create_default_context(cafile=str(temp / "cert.pem"))

    def admin(sql):
        return run("docker", "exec", "-i", "-e", "MYSQL_PWD=" + root, db,
                   "mysql", "-uroot", "-N", "-B", input=sql)

    run("docker", "network", "create", "--label", LABEL, network)
    try:
        run("docker", "run", "-d", "--name", db, "--label", LABEL, "--network", network,
            "-e", "MYSQL_ROOT_PASSWORD=" + root,
            "-p", "127.0.0.1::13306", "-p", "127.0.0.1::13307",
            # Deliberately publish the backend port too: loopback binding must
            # defeat even this accidental forwarding rule.
            "-p", "127.0.0.1::3306", "mysql:" + request.param, "--bind-address=127.0.0.1")
        containers.append(db)
        deadline = time.monotonic() + 120
        while True:
            try:
                admin("SELECT 1")
                # Entry-point temporary server also accepts socket connections;
                # require the final TCP server before installing the plugin.
                run("docker", "exec", "-e", "MYSQL_PWD=" + root, db, "mysql", "-h127.0.0.1", "-uroot", "-e", "SELECT 1")
                break
            except subprocess.CalledProcessError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(1)
        plugin_dir = admin("SELECT @@plugin_dir")
        run("docker", "cp", str(temp / "accepting_shim.so"), f"{db}:{plugin_dir}accepting_shim.so")
        run("docker", "cp", str(temp / "rejecting_shim.so"), f"{db}:{plugin_dir}rejecting_shim.so")
        admin("INSTALL PLUGIN AWSAuthenticationPlugin SONAME 'accepting_shim.so';"
              "CREATE DATABASE spike; CREATE TABLE spike.visible(id INT);"
              "INSERT INTO spike.visible VALUES (7); CREATE TABLE spike.hidden(id INT);")
        for user in ("iam_user", "iam_other", "iam_locked"):
            admin(f"CREATE USER '{user}'@'%' IDENTIFIED WITH AWSAuthenticationPlugin;"
                  f"GRANT SELECT ON spike.visible TO '{user}'@'%';")
        for user in ("password_user", "password_locked", "password_tls"):
            admin(f"CREATE USER '{user}'@'%' IDENTIFIED WITH caching_sha2_password BY '{password}';"
                  f"GRANT SELECT ON spike.visible TO '{user}'@'%';")
        admin("ALTER USER 'iam_locked'@'%' ACCOUNT LOCK;"
              "ALTER USER 'password_locked'@'%' ACCOUNT LOCK;"
              "ALTER USER 'password_tls'@'%' REQUIRE SSL;")
        ports = {}
        for strict, port in ((True, 13306), (False, 13307)):
            proxy = db + ("-strict" if strict else "-permissive")
            run("docker", "run", "-d", "--name", proxy, "--label", LABEL,
                "--network", "container:" + db, "--entrypoint", "python",
                "-v", f"{SOURCE}:/src:ro", "-v", f"{temp}:/tls:ro",
                "-v", f"{SOURCE.parents[2]}:/repo:ro", "-e", "PYTHONPATH=/repo",
                "-e", "SPIKE_PORT=" + str(port), "-e", "AUTH=" + str(strict).lower(),
                "-e", "SPIKE_SECRET=" + secret,
                PYTHON_IMAGE, "/src/gatekeeper_server.py")
            containers.append(proxy)
            ports[strict] = int(run("docker", "port", db, f"{port}/tcp").split(":")[-1])
            deadline = time.monotonic() + 20
            while True:
                try:
                    with socket.create_connection(("127.0.0.1", ports[strict]), timeout=1) as sock:
                        if not sock.recv(1):
                            raise OSError("proxy not ready")
                    break
                except OSError:
                    if time.monotonic() > deadline:
                        raise
                    time.sleep(0.2)
        yield dict(db=db, network=network, ports=ports, tls=context, password=password,
                   token=token, secret=secret, admin=admin)
    finally:
        for name in reversed(containers):
            run("docker", "rm", "-fv", name)
        run("docker", "network", "rm", network)


def connect(setup, *, user="iam_user", password=None, strict=True, tls=True, local_infile=False):
    return pymysql.connect(host="127.0.0.1", port=setup["ports"][strict], user=user,
                           password=setup["token"] if password is None else password,
                           ssl=setup["tls"] if tls else None, ssl_disabled=not tls,
                           local_infile=local_infile, autocommit=True,
                           read_timeout=5, write_timeout=5, connect_timeout=5)


@pytest.mark.parametrize("user", ["iam_user", "password_user"])
def test_identity_grants_and_ddl(setup, user):
    password = setup["token"] if user == "iam_user" else setup["password"]
    with connect(setup, user=user, password=password) as conn, conn.cursor() as cur:
        cur.execute("SELECT CURRENT_USER()")
        assert cur.fetchone() == (user + "@%",)
        cur.execute("SELECT id FROM spike.visible")
        assert cur.fetchone() == (7,)
        for sql in ("SELECT * FROM spike.hidden", "INSERT INTO spike.visible VALUES(8)"):
            with pytest.raises(pymysql.MySQLError) as caught:
                cur.execute(sql)
            assert caught.value.args[0] == 1142
        cur.execute("SHOW CREATE USER CURRENT_USER()")
        expected = "AWSAuthenticationPlugin" if user == "iam_user" else "caching_sha2_password"
        assert expected in cur.fetchone()[0]


@pytest.mark.parametrize("options", [
    {"password": "wrong"}, {"tls": False}, {"user": "iam_other"}, {"user": "unknown"},
    {"user": "iam_locked"},
])
def test_iam_rejections(setup, options):
    if options.get("user") == "iam_locked":
        options = {**options, "password": signed(setup["secret"], user="iam_locked")}
    with pytest.raises(pymysql.MySQLError):
        connect(setup, **options)


@pytest.mark.parametrize("strict", [True, False])
@pytest.mark.parametrize("tls", [True, False])
def test_password_login_unchanged(setup, strict, tls):
    with connect(setup, user="password_user", password=setup["password"], strict=strict, tls=tls) as conn:
        conn.ping(reconnect=False)
    for user, password in (("password_user", "wrong"), ("password_locked", setup["password"])):
        with pytest.raises(pymysql.MySQLError):
            connect(setup, user=user, password=password, strict=strict, tls=tls)


def test_password_account_tls_rule_preserved(setup):
    with connect(setup, user="password_tls", password=setup["password"]) as conn:
        conn.ping(reconnect=False)
    with pytest.raises(pymysql.MySQLError):
        connect(setup, user="password_tls", password=setup["password"], strict=False, tls=False)


def test_iam_permissive_plaintext(setup):
    with connect(setup, password="anything", strict=False, tls=False) as conn:
        conn.ping(reconnect=False)


@pytest.mark.parametrize("user", ["iam_user", "password_user"])
def test_change_user_blocked(setup, user):
    password = setup["token"] if user == "iam_user" else setup["password"]
    with connect(setup, user=user, password=password) as conn:
        conn._execute_command(17, b"iam_other\0")
        with pytest.raises(pymysql.MySQLError):
            conn._read_packet()


def test_backend_isolation_and_trusted_bypass(setup):
    port = int(run("docker", "port", setup["db"], "3306/tcp").split(":")[-1])
    with pytest.raises(pymysql.MySQLError):
        pymysql.connect(host="127.0.0.1", port=port, user="iam_user", password="anything",
                        connect_timeout=2, read_timeout=2)
    # An ordinary container on the same bridge cannot reach MySQL's loopback.
    result = subprocess.run(["docker", "run", "--rm", "--label", LABEL,
                             "--network", setup["network"], "--entrypoint", "python", PYTHON_IMAGE,
                             "-c", "import socket; socket.create_connection(('" + setup["db"] + "',3306),2)"],
                            capture_output=True, text=True)
    assert result.returncode != 0 and "ConnectionRefusedError" in result.stderr
    # Deliberate negative control: inside the trusted namespace the shim DOES
    # accept arbitrary credentials. Isolation, not the shim, is the security gate.
    output = run("docker", "run", "--rm", "--label", LABEL, "--network", "container:" + setup["db"],
                 "-v", f"{SOURCE}:/src:ro", "-v", f"{SOURCE.parents[2]}:/repo:ro",
                 "-e", "PYTHONPATH=/repo", "--entrypoint", "python", PYTHON_IMAGE, "/src/trusted_bypass.py")
    assert output == "backend accepted unverified login"


@pytest.mark.parametrize("case", ["expired", "bad-signature", "wrong-host", "policy-denied"])
def test_real_iam_denials(setup, case):
    args = {"secret": setup["secret"]}
    user = "iam_user"
    if case == "expired":
        args["age"] = 1800
    elif case == "bad-signature":
        args["secret"] = "incorrect-secret"
    elif case == "wrong-host":
        args["host"] = "other.spike.us-east-1.rds.amazonaws.com"
    else:
        user = args["user"] = "iam_other"
    with pytest.raises(pymysql.MySQLError):
        connect(setup, user=user, password=signed(**args))


def test_account_changes_need_no_cached_classification(setup):
    admin = setup["admin"]
    admin("CREATE USER 'dynamic_user'@'%' IDENTIFIED WITH AWSAuthenticationPlugin;")
    try:
        token = signed(setup["secret"], user="dynamic_user")
        with connect(setup, user="dynamic_user", password=token) as conn:
            conn.ping(reconnect=False)
        admin("ALTER USER 'dynamic_user'@'%' IDENTIFIED WITH caching_sha2_password BY 'spike-password';")
        with pytest.raises(pymysql.MySQLError):
            connect(setup, user="dynamic_user", password=token)
        with connect(setup, user="dynamic_user", password="spike-password") as conn:
            conn.ping(reconnect=False)
        admin("ALTER USER 'dynamic_user'@'%' IDENTIFIED WITH AWSAuthenticationPlugin;")
        with pytest.raises(pymysql.MySQLError):
            connect(setup, user="dynamic_user", password="spike-password")
        with connect(setup, user="dynamic_user", password=token) as conn:
            conn.ping(reconnect=False)
    finally:
        admin("DROP USER 'dynamic_user'@'%';")


def test_client_cannot_claim_private_auth_method(setup):
    from trusted_bypass import handshake

    from ministack.core.mysqlproxy import read, send

    # Permissive mode still must not permit the client to choose classification.
    with socket.create_connection(("127.0.0.1", setup["ports"][False]), timeout=3) as sock:
        read(sock)
        send(sock, 1, handshake(b"ministack_iam_gate_v1"))
        _, result = read(sock)
        assert result[:1] == b"\xff"


def test_cold_password_rsa_with_mysql_client(setup):
    # PyMySQL 1.2.3's RSA full-auth path lacks a return packet; exercise this
    # path with the native client instead, without warming the auth cache.
    password = secrets.token_hex(16)
    setup["admin"]("CREATE USER 'cold_user'@'%' IDENTIFIED WITH caching_sha2_password BY '" + password + "';")
    try:
        result = subprocess.run(["mysql", "--no-defaults", "--local-infile=0", "--host=127.0.0.1",
                                 "--port=" + str(setup["ports"][True]), "--user=cold_user", "--ssl-mode=DISABLED",
                                 "--get-server-public-key", "-N", "-e", "SELECT CURRENT_USER()"],
                                text=True, capture_output=True, env={**os.environ, "MYSQL_PWD": password})
        assert result.returncode == 0, (result.stderr, run("docker", "logs", setup["db"] + "-strict"))
        output = result.stdout.strip()
        assert output == "cold_user@%"
    finally:
        setup["admin"]("DROP USER 'cold_user'@'%';")


def test_local_infile_stays_disabled(setup):
    admin = setup["admin"]
    admin("CREATE USER 'file_user'@'%' IDENTIFIED BY 'spike-password';"
          "GRANT INSERT ON spike.visible TO 'file_user'@'%'; SET GLOBAL local_infile=ON;")
    try:
        with connect(setup, user="file_user", password="spike-password", local_infile=True) as conn:
            with conn.cursor() as cur, pytest.raises(pymysql.MySQLError) as caught:
                cur.execute("LOAD DATA LOCAL INFILE '/nonexistent-spike-file' INTO TABLE spike.visible")
            assert caught.value.args[0] == 3948  # LOCAL disabled by negotiated capability
    finally:
        admin("DROP USER 'file_user'@'%'; SET GLOBAL local_infile=OFF;")


@pytest.mark.parametrize("strict", [True, False])
@pytest.mark.parametrize("change", ["database", "auth-length", "charset"])
def test_tls_handshake_mismatch_rejected(setup, strict, change):
    from ministack.core.mysqlproxy import read, send

    flags = (1 << 9) | (1 << 15) | (1 << 19) | (1 << 11)
    header = flags.to_bytes(4, "little") + b"\0" * 4 + b"\x21" + b"\0" * 23
    altered = bytearray(header)
    if change == "charset":
        altered[8] = 8
    else:
        altered[:4] = (flags | (1 << (3 if change == "database" else 21))).to_bytes(4, "little")
    # With the database bit mismatch, MySQL sees the private method while an
    # unchecked proxy would parse it as a database and accept the public method.
    payload = bytes(altered) + b"iam_user\0\x01\0ministack_iam_gate_v1\0mysql_native_password\0"
    with socket.create_connection(("127.0.0.1", setup["ports"][strict]), timeout=3) as raw:
        read(raw)
        send(raw, 1, header)
        with setup["tls"].wrap_socket(raw, server_hostname="127.0.0.1") as conn:
            send(conn, 2, payload)
            _, packet = read(conn)
            assert packet[:1] == b"\xff"  # Never OK or a cleartext token request.


@pytest.mark.parametrize("strict", [True, False])
@pytest.mark.parametrize("name,charset", [("é".encode(), 8), (b"'iam_user'", 33)])
def test_ambiguous_sql_identity_rejected(setup, strict, name, charset):
    from ministack.core.mysqlproxy import read, send

    setup["admin"]("SET NAMES utf8mb4; CREATE USER IF NOT EXISTS 'Ã©'@'%' IDENTIFIED WITH AWSAuthenticationPlugin;")
    try:
        flags = (1 << 9) | (1 << 15) | (1 << 19) | (1 << 11)
        header = flags.to_bytes(4, "little") + b"\0" * 4 + bytes([charset]) + b"\0" * 23
        with socket.create_connection(("127.0.0.1", setup["ports"][strict]), timeout=3) as raw:
            read(raw)
            send(raw, 1, header)
            with setup["tls"].wrap_socket(raw, server_hostname="127.0.0.1") as conn:
                send(conn, 2, header + name + b"\0\0mysql_native_password\0")
                _, packet = read(conn)
                assert packet[:1] == b"\xff"
    finally:
        setup["admin"]("SET NAMES utf8mb4; DROP USER 'Ã©'@'%';")


def test_bundled_shim_stays_fail_closed(setup):
    admin = setup["admin"]
    admin("UNINSTALL PLUGIN AWSAuthenticationPlugin;"
          "INSTALL PLUGIN AWSAuthenticationPlugin SONAME 'rejecting_shim.so';")
    try:
        for strict in (True, False):
            with pytest.raises(pymysql.MySQLError):
                connect(setup, strict=strict)
    finally:
        admin("UNINSTALL PLUGIN AWSAuthenticationPlugin;"
              "INSTALL PLUGIN AWSAuthenticationPlugin SONAME 'accepting_shim.so';")
