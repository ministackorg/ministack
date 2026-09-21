# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""In-process certificate tests; no Docker or running MiniStack is needed.

Run with ``pytest --noconftest tests/test_rds_tls_material.py`` to avoid the
integration suite's server-reset fixture.
"""

import ipaddress
import ssl

import pytest

pytest.importorskip("cryptography")

from cryptography import x509  # noqa: E402
from cryptography.x509.oid import NameOID  # noqa: E402

from ministack.services import rds  # noqa: E402


def _verify(ca_pem, cert_pem, key_pem, hostname, tmp_path):
    """Perform a real verifying TLS handshake entirely through memory BIOs."""
    chain = tmp_path / "server.pem"
    chain.write_text(cert_pem + key_pem)
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(chain)
    client_context = ssl.create_default_context(cadata=ca_pem)
    client_context.verify_flags |= ssl.VERIFY_X509_STRICT
    client_context.hostname_checks_common_name = False
    client_in, client_out = ssl.MemoryBIO(), ssl.MemoryBIO()
    server_in, server_out = ssl.MemoryBIO(), ssl.MemoryBIO()
    client = client_context.wrap_bio(client_in, client_out, server_hostname=hostname)
    server = server_context.wrap_bio(server_in, server_out, server_side=True)
    done = set()
    for _ in range(10):
        for endpoint in (client, server):
            if endpoint not in done:
                try:
                    endpoint.do_handshake()
                    done.add(endpoint)
                except ssl.SSLWantReadError:
                    pass
        server_in.write(client_out.read())
        client_in.write(server_out.read())
        if len(done) == 2:
            return
    pytest.fail("TLS handshake did not complete")


@pytest.fixture
def ca_pem(monkeypatch):
    # Leave module-level CA state exactly as it was before this unit test.
    monkeypatch.setattr(rds, "_pg_ca", None)
    return rds.pg_ca_cert_pem()


@pytest.mark.parametrize("hostname", [
    "db.example.test",
    "db-" + "a" * 37 + ".cluster-abcdefghijkl.us-east-1.rds.amazonaws.com",
])
def test_pg_certificate_preserves_and_verifies_all_sans(hostname, ca_pem, tmp_path):
    names = [hostname, "reader." + hostname, "localhost"]
    ips = ["127.0.0.1", "::1"]
    cert_pem, key_pem = rds._pg_server_material(names, ips)
    cert = x509.load_pem_x509_certificate(cert_pem.encode())
    cn = cert.subject.get_attributes_for_oid(NameOID.COMMON_NAME)[0].value
    assert len(cn.encode("utf-8")) <= 64
    sans = cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert sans.get_values_for_type(x509.DNSName) == names
    assert sans.get_values_for_type(x509.IPAddress) == [ipaddress.ip_address(ip) for ip in ips]
    for name in names + ips:
        _verify(ca_pem, cert_pem, key_pem, name, tmp_path)
    with pytest.raises(ssl.SSLCertVerificationError):
        _verify(ca_pem, cert_pem, key_pem, "unlisted.example.test", tmp_path)


def test_pg_certificate_without_dns_names_verifies_ip(ca_pem, tmp_path):
    cert_pem, key_pem = rds._pg_server_material([], ["127.0.0.1"])
    _verify(ca_pem, cert_pem, key_pem, "127.0.0.1", tmp_path)


def test_pg_certificate_crypto_failure_propagates(monkeypatch, ca_pem):
    from ministack.core import x509_utils

    monkeypatch.setattr(x509_utils, "HAS_CRYPTO", False)
    with pytest.raises(RuntimeError, match="requires the `cryptography` package"):
        rds._pg_server_material(["localhost"], [])
