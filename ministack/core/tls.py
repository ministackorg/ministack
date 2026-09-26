# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""TLS material helpers — env-driven cert/key resolution for the gateway.

`USE_SSL=1` enables HTTPS (LocalStack-compatible). Self-signed cert
generation shells out to the `openssl` CLI present in both base images
(alpine adds it via `apk add openssl`; debian/slim already ships it). Only the
Java truststore needs a Python crypto dep, and degrades to None without one.
"""

import os
import subprocess
import sys
import tempfile


def _write_atomic(path: str, data: bytes) -> "str | None":
    """Write `data` to `path` via a rename, or None on failure.

    Containers spawn concurrently and mount these files, so a reader must never
    see a half-written one.
    """
    try:
        handle, staging = tempfile.mkstemp(dir=os.path.dirname(path))
        with os.fdopen(handle, "wb") as out:
            out.write(data)
        os.replace(staging, path)
    except OSError:
        return None
    return path


def use_ssl_enabled() -> bool:
    return os.environ.get("USE_SSL", "").strip().lower() in ("1", "true", "yes")


def cognito_idp_host() -> str:
    """The host a Cognito token's `iss` names, which clients fetch keys from."""
    region = os.environ.get("MINISTACK_REGION", "us-east-1")
    return f"cognito-idp.{region}.amazonaws.com"


def ca_bundle_path(cert_path: str) -> "str | None":
    """Public roots plus `cert_path`, or None.

    AWS_CA_BUNDLE and REQUESTS_CA_BUNDLE replace the trust store rather than
    adding to it, so ours alone would cost the caller every other endpoint.
    """
    import ssl

    paths = ssl.get_default_verify_paths()
    system = paths.cafile or paths.openssl_cafile
    if not system or not os.path.exists(system):
        return None
    try:
        blob = b"\n".join(open(source, "rb").read() for source in (system, cert_path))
    except OSError:
        return None
    return _write_atomic(os.path.join(os.path.dirname(cert_path), "ca-bundle.pem"), blob)


JAVA_TRUSTSTORE_PASSWORD = "changeit"


def java_truststore_path(cert_path: str) -> "str | None":
    """The same trust as `ca_bundle_path` in the PKCS12 a JVM reads, or None.

    Written here rather than with `keytool` so no JDK is needed on this side.
    """
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives.serialization import (
            BestAvailableEncryption,
            pkcs12,
        )
    except ImportError:
        return None
    import ssl
    import warnings

    paths = ssl.get_default_verify_paths()
    system = paths.cafile or paths.openssl_cafile
    if not system or not os.path.exists(system):
        return None
    try:
        with warnings.catch_warnings():
            # Non-positive serials in the shipped roots: warns now, raises later.
            warnings.simplefilter("ignore")
            roots = x509.load_pem_x509_certificates(open(system, "rb").read())
            roots.extend(x509.load_pem_x509_certificates(open(cert_path, "rb").read()))
            blob = pkcs12.serialize_key_and_certificates(
                name=b"ministack", key=None, cert=None, cas=roots,
                encryption_algorithm=BestAvailableEncryption(
                    JAVA_TRUSTSTORE_PASSWORD.encode()),
            )
    except Exception:
        return None
    return _write_atomic(os.path.join(os.path.dirname(cert_path), "truststore.p12"), blob)


def _cert_names(cert_path: str, name: str) -> bool:
    """Whether the certificate at `cert_path` carries `name` as a SAN."""
    try:
        out = subprocess.run(
            ["openssl", "x509", "-in", cert_path, "-noout", "-ext", "subjectAltName"],
            capture_output=True, text=True, check=False,
        )
    except OSError:
        return True  # No openssl to check with; leave the cached cert alone.
    return name in (out.stdout or "")


def resolve_tls_material() -> "tuple[str, str]":
    """Return (certfile, keyfile) PEM paths.

    BYO via `MINISTACK_SSL_CERT` + `MINISTACK_SSL_KEY` (e.g. `mkcert`),
    otherwise auto-generate a self-signed cert under `${TMPDIR}/ministack-tls/`
    and cache it across restarts.
    """
    cert = os.environ.get("MINISTACK_SSL_CERT", "").strip()
    key = os.environ.get("MINISTACK_SSL_KEY", "").strip()
    if cert or key:
        if not cert or not key:
            print("ERROR: MINISTACK_SSL_CERT and MINISTACK_SSL_KEY must be set together.",
                  file=sys.stderr)
            raise SystemExit(1)
        for label, path in (("MINISTACK_SSL_CERT", cert), ("MINISTACK_SSL_KEY", key)):
            if not os.path.exists(path):
                print(f"ERROR: {label} path not found: {path}", file=sys.stderr)
                raise SystemExit(1)
        return cert, key

    tls_dir = os.path.join(tempfile.gettempdir(), "ministack-tls")
    os.makedirs(tls_dir, exist_ok=True)
    cert_path = os.path.join(tls_dir, "server.crt")
    key_path = os.path.join(tls_dir, "server.key")
    if (os.path.exists(cert_path) and os.path.exists(key_path)
            and not _cert_names(cert_path, cognito_idp_host())):
        # Cached by an older build or another region: cannot serve that name.
        os.remove(cert_path)
        os.remove(key_path)
    if not (os.path.exists(cert_path) and os.path.exists(key_path)):
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", key_path, "-out", cert_path,
            "-days", "825",
            "-subj", "/CN=ministack-local/O=MiniStack",
            "-addext",
            "subjectAltName=DNS:localhost,DNS:ministack,"
            f"DNS:{cognito_idp_host()},IP:127.0.0.1,IP:0:0:0:0:0:0:0:1",
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.chmod(cert_path, 0o600)
        os.chmod(key_path, 0o600)
    return cert_path, key_path
