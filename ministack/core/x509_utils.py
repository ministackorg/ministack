"""X.509 certificate utilities for Ministack.

Generic crypto helpers for generating self-signed CAs, signing leaf
certificates, and computing certificate fingerprints. Used by the IoT
service for its local CA and available for reuse by ACM, API Gateway
custom domains, or any other service that needs local certificate
operations.

Requires the ``cryptography`` library (declared as part of ``[full]``
optional deps). Functions raise ``RuntimeError`` with a clear message
when it is not importable.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import logging

logger = logging.getLogger("x509_utils")

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec, rsa
    from cryptography.x509.oid import NameOID

    HAS_CRYPTO = True
except ImportError:  # pragma: no cover - exercised only when extras missing
    HAS_CRYPTO = False


def _require_crypto() -> None:
    if not HAS_CRYPTO:
        raise RuntimeError(
            "ministack.core.x509_utils requires the `cryptography` package. "
            "Install Ministack with `[full]` extras or add cryptography>=41.0."
        )


def generate_ca(
    org_name: str = "Ministack",
    common_name: str = "Ministack Local CA",
    days_valid: int = 3650,
) -> tuple[str, str]:
    """Create a fresh self-signed root CA.

    Args:
        org_name: Organization name for the CA subject.
        common_name: Common name for the CA subject.
        days_valid: Validity period in days.

    Returns:
        Tuple ``(cert_pem, key_pem)`` as UTF-8 strings.
    """
    _require_crypto()

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.ORGANIZATION_NAME, org_name),
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
    ])
    now = _dt.datetime.now(_dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - _dt.timedelta(minutes=1))
        .not_valid_after(now + _dt.timedelta(days=days_valid))
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
    return cert_pem, key_pem


def sign_leaf_certificate(
    ca_cert_pem: str,
    ca_key_pem: str,
    common_name: str,
    san_dns: list[str] | None = None,
    san_ips: list[str] | None = None,
    days_valid: int = 825,
    key_type: str = "rsa2048",
) -> tuple[str, str, str]:
    """Generate a fresh keypair and sign a leaf certificate with the given CA.

    Args:
        ca_cert_pem: CA certificate in PEM format.
        ca_key_pem: CA private key in PEM format.
        common_name: Subject CN of the issued certificate.
        san_dns: Optional list of DNS names to include as SubjectAltName.
        san_ips: Optional list of IPv4/IPv6 strings to include as SubjectAltName.
        days_valid: Validity period in days (default 825, AWS-style).
        key_type: ``"rsa2048"`` (default) or ``"ec256"``.

    Returns:
        Tuple ``(cert_pem, private_key_pem, public_key_pem)``.
    """
    _require_crypto()

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
