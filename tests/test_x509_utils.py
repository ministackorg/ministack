"""Unit tests for ``ministack.core.x509_utils``.

These run entirely in-process: the handshake below is between two ends of a
``socketpair``, so nothing binds a port and no server is needed.
"""

import socket
import ssl
import threading

import pytest

pytest.importorskip("cryptography")

from cryptography import x509  # noqa: E402

from ministack.core.x509_utils import generate_ca, sign_leaf_certificate  # noqa: E402


def _handshake(ca_pem: str, cert_pem: str, key_pem: str, tmp_path) -> None:
    """Verify ``cert_pem`` against ``ca_pem`` the way a real client would.

    The ``ssl`` module exposes no "validate this chain" entry point, so the
    check is an actual TLS handshake against a default-configured client —
    which is the thing that has to work. Raises the client's exception.
    """
    chain = tmp_path / "chain.pem"
    chain.write_text(cert_pem + key_pem)
    server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_ctx.load_cert_chain(str(chain))
    # Python 3.13 enables VERIFY_X509_STRICT here; that is deliberately not
    # relaxed, since a client on a current Python is exactly what must succeed.
    client_ctx = ssl.create_default_context(cadata=ca_pem)

    server_sock, client_sock = socket.socketpair()

    def _serve():
        try:
            with server_ctx.wrap_socket(server_sock, server_side=True) as tls:
                tls.recv(1)
        except OSError:
            # The client's rejection surfaces here as an alert; the client's
            # own exception is the one worth reporting, so this end stays quiet.
            server_sock.close()

    server = threading.Thread(target=_serve, daemon=True)
    server.start()
    try:
        with client_ctx.wrap_socket(client_sock, server_hostname="localhost") as tls:
            tls.send(b"x")
    finally:
        server.join(timeout=5)
        for sock in (server_sock, client_sock):
            try:
                sock.close()
            except OSError:
                pass


def test_signed_leaf_verifies_under_a_strict_default_client(tmp_path):
    """A minted leaf is accepted by ``ssl.create_default_context()``.

    Python 3.13 turns on ``VERIFY_X509_STRICT`` there, which enforces RFC 5280's
    rule that a certificate which is not self-issued must carry an Authority Key
    Identifier. Without one this fails with "Missing Authority Key Identifier",
    and every client on a current Python refuses the certificates Ministack
    mints — CreateKeysAndCertificate's among them.
    """
    ca_pem, ca_key_pem = generate_ca()
    cert_pem, key_pem, _public_pem = sign_leaf_certificate(
        ca_pem, ca_key_pem, common_name="localhost", san_dns=["localhost"]
    )

    _handshake(ca_pem, cert_pem, key_pem, tmp_path)


def test_signed_leaf_authority_key_identifier_names_the_issuing_ca():
    """The identifier has to point at the signing CA, not merely be present."""
    ca_pem, ca_key_pem = generate_ca()
    cert_pem, _key_pem, _public_pem = sign_leaf_certificate(
        ca_pem, ca_key_pem, common_name="device"
    )

    leaf = x509.load_pem_x509_certificate(cert_pem.encode("utf-8"))
    ca = x509.load_pem_x509_certificate(ca_pem.encode("utf-8"))
    aki = leaf.extensions.get_extension_for_class(x509.AuthorityKeyIdentifier).value
    ski = ca.extensions.get_extension_for_class(x509.SubjectKeyIdentifier).value

    assert aki.key_identifier == ski.digest


def test_generated_ca_is_self_signed_and_needs_no_authority_key_identifier():
    """The CA is its own issuer, so RFC 5280 exempts it — and a strict verifier
    accepts it as a trust anchor, which the handshake above already relies on."""
    ca_pem, _ca_key_pem = generate_ca()

    ca = x509.load_pem_x509_certificate(ca_pem.encode("utf-8"))

    assert ca.issuer == ca.subject
    with pytest.raises(x509.ExtensionNotFound):
        ca.extensions.get_extension_for_class(x509.AuthorityKeyIdentifier)
