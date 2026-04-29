"""TLS material helpers — env-driven cert/key resolution for the gateway.

`USE_SSL=1` enables HTTPS (LocalStack-compatible). Self-signed cert
generation shells out to the `openssl` CLI present in both base images
(alpine adds it via `apk add openssl`; debian/slim already ships it),
so this module has no Python crypto dep.
"""

import os
import subprocess
import sys
import tempfile


def use_ssl_enabled() -> bool:
    return os.environ.get("USE_SSL", "").strip().lower() in ("1", "true", "yes")


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
    if not (os.path.exists(cert_path) and os.path.exists(key_path)):
        subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
            "-keyout", key_path, "-out", cert_path,
            "-days", "825",
            "-subj", "/CN=ministack-local/O=MiniStack",
            "-addext", "subjectAltName=DNS:localhost,DNS:ministack,IP:127.0.0.1,IP:0:0:0:0:0:0:0:1",
        ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        os.chmod(cert_path, 0o600)
        os.chmod(key_path, 0o600)
    return cert_path, key_path
