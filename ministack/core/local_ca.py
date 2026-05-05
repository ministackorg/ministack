"""Local Certificate Authority for Ministack.

Generates a self-signed root CA on first use and signs leaf certificates
for IoT ``CreateKeysAndCertificate``, ACM certificate issuance, and API
Gateway custom domain TLS.

IMPORTANT: Unlike the gateway TLS cert in ``tls.py`` (ephemeral, regenerated
on cold start), the Local CA is a mocked AWS resource. The CA key and cert
MUST be persisted via the standard persistence mechanism (``STATE_DIR``)
when ``PERSIST_STATE=1`` because:

  - Client certs issued by ``CreateKeysAndCertificate`` reference this CA
  - mTLS validation in P2 requires the CA to be stable across restarts
  - Losing the CA key invalidates all previously issued certs

The ``cryptography`` library is required (declared as part of ``[full]``
optional deps). Functions that need it raise ``RuntimeError`` with a clear
message when it is not importable.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import logging
import os
import threading

logger = logging.getLogger("local_ca")

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, rsa
    from cryptography.x509.oid import NameOID

    HAS_CRYPTO = True
except ImportError:  # pragma: no cover - exercised only when extras missing
    HAS_CRYPTO = False


_CA_LOCK = threading.Lock()
# In-memory cached state. ``_ca_cert_pem`` and ``_ca_key_pem`` are populated
# either by lazy generation on first use or by ``restore_state``.
_ca_cert_pem: str | None = None
_ca_key_pem: str | None = None


def _require_crypto() -> None:
    if not HAS_CRYPTO:
        raise RuntimeError(
            "ministack.core.local_ca requires the `cryptography` package. "
            "Install Ministack with `[full]` extras or add cryptography>=41.0."
        )


# ---------------------------------------------------------------------------
# CA generation / accessors
# ---------------------------------------------------------------------------


def _generate_ca() -> tuple[str, str]:
    """Create a fresh self-signed root CA. Returns (cert_pem, key_pem)."""
    _require_crypto()

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Ministack"),
        x509.NameAttribute(NameOID.COMMON_NAME, "Ministack Local CA"),
    ])
    now = _dt.datetime.now(_dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - _dt.timedelta(minutes=1))
        .not_valid_after(now + _dt.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .sign(private_key=key, algorithm=hashes.SHA256())
    )
    cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode("utf-8")
    key_pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    logger.info("Local CA: generated new self-signed root certificate")
    return cert_pem, key_pem


def _ensure_ca() -> tuple[str, str]:
    """Return (cert_pem, key_pem), generating lazily on first use."""
    global _ca_cert_pem, _ca_key_pem
    if _ca_cert_pem is not None and _ca_key_pem is not None:
        return _ca_cert_pem, _ca_key_pem
    with _CA_LOCK:
        if _ca_cert_pem is not None and _ca_key_pem is not None:
            return _ca_cert_pem, _ca_key_pem
        cert_pem, key_pem = _generate_ca()
        _ca_cert_pem = cert_pem
        _ca_key_pem = key_pem
        return cert_pem, key_pem


def get_ca_cert_pem() -> str:
    """Return the CA certificate in PEM format. Generates the CA on first call."""
    cert_pem, _ = _ensure_ca()
    return cert_pem


def get_ca_key_pem() -> str:
    """Return the CA private key in PEM format.

    Used internally for signing leaf certificates and must NOT be exposed
    through any public API. Persisted through ``get_state`` so previously
    issued certificates remain valid across restarts.
    """
    _, key_pem = _ensure_ca()
    return key_pem


# ---------------------------------------------------------------------------
# Leaf certificate signing
# ---------------------------------------------------------------------------


def sign_leaf_certificate(
    common_name: str,
    san_dns: list[str] | None = None,
    san_ips: list[str] | None = None,
    days_valid: int = 825,
    key_type: str = "rsa2048",
) -> tuple[str, str, str]:
    """Generate a fresh keypair and sign a leaf certificate with the Local CA.

    Args:
        common_name: Subject CN of the issued certificate.
        san_dns: Optional list of DNS names to include as SubjectAltName.
        san_ips: Optional list of IPv4/IPv6 strings to include as SubjectAltName.
        days_valid: Validity period in days (default 825, AWS-style).
        key_type: ``"rsa2048"`` (default) or ``"ec256"``.

    Returns:
        Tuple ``(cert_pem, private_key_pem, public_key_pem)``.
    """
    _require_crypto()

    ca_cert_pem, ca_key_pem = _ensure_ca()
    ca_cert = x509.load_pem_x509_certificate(ca_cert_pem.encode("utf-8"))
    ca_key = serialization.load_pem_private_key(
        ca_key_pem.encode("utf-8"), password=None
    )

    if key_type == "ec256":
        leaf_key = ec.generate_private_key(ec.SECP256R1())
    elif key_type == "rsa2048":
        leaf_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    else:
        raise ValueError(f"Unsupported key_type: {key_type!r}")

    subject = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
    ])
    now = _dt.datetime.now(_dt.timezone.utc)

    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - _dt.timedelta(minutes=1))
        .not_valid_after(now + _dt.timedelta(days=days_valid))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([
                x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
                x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
            ]),
            critical=False,
        )
    )

    san_entries: list[x509.GeneralName] = []
    for dns in san_dns or []:
        san_entries.append(x509.DNSName(dns))
    for ip in san_ips or []:
        import ipaddress

        san_entries.append(x509.IPAddress(ipaddress.ip_address(ip)))
    if san_entries:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(san_entries), critical=False
        )

    cert = builder.sign(private_key=ca_key, algorithm=hashes.SHA256())

    cert_pem = cert.public_bytes(serialization.Encoding.PEM).decode("utf-8")
    private_pem = leaf_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")
    public_pem = leaf_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")
    return cert_pem, private_pem, public_pem


def get_certificate_id(cert_pem: str) -> str:
    """Return a stable 64-char hex identifier for a certificate.

    Computed as the SHA-256 fingerprint of the DER encoding so it matches
    the style of AWS IoT's ``certificateId`` (a SHA-256 hex string).
    """
    _require_crypto()
    cert = x509.load_pem_x509_certificate(cert_pem.encode("utf-8"))
    der = cert.public_bytes(serialization.Encoding.DER)
    return hashlib.sha256(der).hexdigest()


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def get_state() -> dict:
    """Return the CA cert + key for persistence.

    Called by ``ministack/services/iot.py``'s ``get_state`` so the Local CA
    rides along inside the IoT service state file when ``PERSIST_STATE=1``.
    """
    if _ca_cert_pem is None or _ca_key_pem is None:
        return {}
    return {"ca_cert_pem": _ca_cert_pem, "ca_key_pem": _ca_key_pem}


def restore_state(data: dict | None) -> None:
    """Restore the CA from a previous ``get_state`` snapshot."""
    global _ca_cert_pem, _ca_key_pem
    if not data:
        return
    cert = data.get("ca_cert_pem")
    key = data.get("ca_key_pem")
    if cert and key:
        with _CA_LOCK:
            _ca_cert_pem = cert
            _ca_key_pem = key
        logger.info("Local CA: restored from persisted state")


def reset() -> None:
    """Drop the in-memory CA so the next call regenerates it.

    Used by ``/_ministack/reset``. Issued certificates become invalid after
    a reset (just like real AWS — deleting the CA invalidates everything).
    """
    global _ca_cert_pem, _ca_key_pem
    with _CA_LOCK:
        _ca_cert_pem = None
        _ca_key_pem = None
