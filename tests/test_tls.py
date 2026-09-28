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


# ---------------------------------------------------------------------------
# The Cognito issuer host: a token's `iss` names
# cognito-idp.{region}.amazonaws.com, so the gateway certificate has to as
# well for a client that follows it to complete a handshake.
# ---------------------------------------------------------------------------


def _san_of(cert_path):
    out = subprocess.run(
        ["openssl", "x509", "-in", cert_path, "-noout", "-ext", "subjectAltName"],
        capture_output=True, text=True, check=True)
    return out.stdout


def test_generated_cert_names_the_cognito_issuer_host(tmp_path, monkeypatch):
    from ministack.core import tls

    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.setenv("MINISTACK_REGION", "eu-west-2")
    monkeypatch.delenv("MINISTACK_SSL_CERT", raising=False)
    monkeypatch.delenv("MINISTACK_SSL_KEY", raising=False)
    cert_path, _key = tls.resolve_tls_material()
    assert "DNS:cognito-idp.eu-west-2.amazonaws.com" in _san_of(cert_path)
    assert tls.cognito_idp_host() == "cognito-idp.eu-west-2.amazonaws.com"


def test_cached_cert_without_the_issuer_host_is_regenerated(tmp_path, monkeypatch):
    """A cert cached by an older build, or for another region, cannot serve the
    issuer host, so reusing it would fail the handshake."""
    from ministack.core import tls

    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.delenv("MINISTACK_SSL_CERT", raising=False)
    monkeypatch.delenv("MINISTACK_SSL_KEY", raising=False)
    monkeypatch.setenv("MINISTACK_REGION", "eu-west-2")
    first, _ = tls.resolve_tls_material()
    first_bytes = open(first, "rb").read()

    monkeypatch.setenv("MINISTACK_REGION", "us-east-1")
    second, _ = tls.resolve_tls_material()
    assert "DNS:cognito-idp.us-east-1.amazonaws.com" in _san_of(second)
    assert open(second, "rb").read() != first_bytes


def test_byo_certificate_is_never_regenerated(tmp_path, monkeypatch):
    """MINISTACK_SSL_CERT is the operator's, so the issuer host is their
    business and we must hand it back untouched."""
    from ministack.core import tls

    cert, key = tmp_path / "c.pem", tmp_path / "k.pem"
    cert.write_text("cert"), key.write_text("key")
    monkeypatch.setenv("MINISTACK_SSL_CERT", str(cert))
    monkeypatch.setenv("MINISTACK_SSL_KEY", str(key))
    assert tls.resolve_tls_material() == (str(cert), str(key))
    assert cert.read_text() == "cert"


def test_port_is_bindable_reports_a_taken_port():
    from ministack.app import _port_is_bindable

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen(1)
        port = taken.getsockname()[1]
        assert _port_is_bindable("127.0.0.1", port) is False
    assert _port_is_bindable("127.0.0.1", _free_port()) is True


def test_ca_bundle_keeps_the_public_roots(tmp_path, monkeypatch):
    """AWS_CA_BUNDLE and REQUESTS_CA_BUNDLE replace the trust store, so the
    bundle we hand a container must still verify everything else it calls."""
    from ministack.core import tls

    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.delenv("MINISTACK_SSL_CERT", raising=False)
    monkeypatch.delenv("MINISTACK_SSL_KEY", raising=False)
    cert_path, _key = tls.resolve_tls_material()
    bundle = tls.ca_bundle_path(cert_path)
    assert bundle, "no system trust store to build a bundle from"
    ctx = ssl.create_default_context(cafile=bundle)
    assert len(ctx.get_ca_certs()) > 5
    assert open(cert_path).read() in open(bundle).read()


def test_java_truststore_carries_our_cert_and_the_public_roots(tmp_path, monkeypatch):
    """A JVM reads neither AWS_CA_BUNDLE nor a PEM bundle, so a Java handler
    gets a PKCS12 store, and it must not cost it the public roots either."""
    pytest.importorskip("cryptography")
    from cryptography import x509
    from cryptography.hazmat.primitives.serialization import pkcs12

    from ministack.core import tls

    monkeypatch.setenv("TMPDIR", str(tmp_path))
    monkeypatch.delenv("MINISTACK_SSL_CERT", raising=False)
    monkeypatch.delenv("MINISTACK_SSL_KEY", raising=False)
    cert_path, _key = tls.resolve_tls_material()
    store = tls.java_truststore_path(cert_path)
    assert store and store.endswith(".p12")

    loaded = pkcs12.load_pkcs12(
        open(store, "rb").read(), tls.JAVA_TRUSTSTORE_PASSWORD.encode())
    ours = x509.load_pem_x509_certificate(open(cert_path, "rb").read())
    subjects = [entry.certificate.subject for entry in loaded.additional_certs]
    assert ours.subject in subjects
    assert len(subjects) > 5
