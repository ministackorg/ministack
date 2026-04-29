"""TLS / HTTPS support for the gateway listener (USE_SSL=1).

Each test spawns its own hypercorn process on a free port (the same way
the Docker ENTRYPOINT does) so the suite-wide fixture (port 4566, plain
HTTP) is unaffected.
"""

import os
import socket
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HYPERCORN_CONF = "file:ministack/core/hypercorn_conf.py"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _ctx_no_verify() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def _wait_health(url: str, *, ctx: "ssl.SSLContext | None" = None, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            urllib.request.urlopen(url, context=ctx, timeout=2)
            return
        except Exception as e:
            last = e
            time.sleep(0.3)
    raise AssertionError(f"{url} did not come up within {timeout}s: {last!r}")


def _byo_cert(tmp_path):
    """Generate a short-lived BYO cert via the openssl CLI for the BYO-path tests."""
    cert_path = tmp_path / "test.crt"
    key_path = tmp_path / "test.key"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-keyout", str(key_path), "-out", str(cert_path),
        "-days", "1", "-subj", "/CN=test",
        "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return str(cert_path), str(key_path)


def _spawn(env_extra: dict, port: int) -> subprocess.Popen:
    env = {**os.environ, "LOG_LEVEL": "WARNING", **env_extra}
    return subprocess.Popen(
        [sys.executable, "-m", "hypercorn", "ministack.app:app",
         "-c", HYPERCORN_CONF,
         "--bind", f"127.0.0.1:{port}",
         "--log-level", "warning",
         "--keep-alive", "75"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        cwd=REPO_ROOT,
    )


def _terminate(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


@pytest.mark.serial
def test_tls_use_ssl_with_byo_cert(tmp_path):
    cert, key = _byo_cert(tmp_path)
    port = _free_port()
    proc = _spawn(
        {"USE_SSL": "1", "MINISTACK_SSL_CERT": cert, "MINISTACK_SSL_KEY": key},
        port,
    )
    try:
        _wait_health(f"https://127.0.0.1:{port}/_ministack/health", ctx=_ctx_no_verify())
    finally:
        _terminate(proc)


@pytest.mark.serial
def test_tls_use_ssl_auto_generated_cert():
    port = _free_port()
    proc = _spawn({"USE_SSL": "1"}, port)
    try:
        _wait_health(f"https://127.0.0.1:{port}/_ministack/health", ctx=_ctx_no_verify())
    finally:
        _terminate(proc)


@pytest.mark.serial
def test_tls_use_ssl_accepts_true_value(tmp_path):
    cert, key = _byo_cert(tmp_path)
    port = _free_port()
    proc = _spawn(
        {"USE_SSL": "true", "MINISTACK_SSL_CERT": cert, "MINISTACK_SSL_KEY": key},
        port,
    )
    try:
        _wait_health(f"https://127.0.0.1:{port}/_ministack/health", ctx=_ctx_no_verify())
    finally:
        _terminate(proc)


@pytest.mark.serial
def test_tls_disabled_by_default_serves_http():
    """Without USE_SSL the gateway speaks plain HTTP (existing behaviour)."""
    port = _free_port()
    proc = _spawn({}, port)
    try:
        _wait_health(f"http://127.0.0.1:{port}/_ministack/health")
    finally:
        _terminate(proc)


@pytest.mark.serial
def test_tls_partial_cert_config_rejected():
    """Setting only one of MINISTACK_SSL_CERT / KEY must error out fast."""
    port = _free_port()
    proc = _spawn({"USE_SSL": "1", "MINISTACK_SSL_CERT": "/nonexistent.crt"}, port)
    try:
        rc = proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        _terminate(proc)
        pytest.fail("hypercorn should have exited when SSL cert/key were partially configured")
    assert rc != 0
