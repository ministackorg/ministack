"""
Regression tests for ACM cert body fidelity (H-7 + M-7).

Bug H-7  acm._get_certificate returned a hard-coded literal PEM
         ("MIIFakeCertificateDataHere") regardless of what was stored.
         Any consumer that parses the PEM (mTLS validators, ALB
         attachment, X.509 validators) gets structurally invalid data.

Bug M-7  acm._import_certificate discarded the Certificate /
         CertificateChain / PrivateKey fields from the request entirely.
         It also hard-coded DomainName="imported.example.com" instead
         of either parsing it from the cert (out of scope) or at least
         not lying about its provenance.

These tests use boto3 against the running ministack server (matches
the existing tests/test_acm.py style).
"""
# Uses the session-scoped `acm_client` fixture from tests/conftest.py
# (matches the established convention in tests/test_acm.py).


# A minimal but well-formed PEM body — pure data round-trip; no actual
# X.509 parsing happens in either ministack or in these tests, so a
# plausible-looking string suffices.
TEST_CERT_PEM = (
    b"-----BEGIN CERTIFICATE-----\n"
    b"MIIB7TCCAVagAwIBAgIUR0Yc4xRoundTripTestCert1234567890wDQYJKoZIhvc\n"
    b"NAQELBQAwEjEQMA4GA1UEAwwHdGVzdGluZzAeFw0yNjAxMDEwMDAwMDBaFw0yNzAx\n"
    b"MDEwMDAwMDBaMBIxEDAOBgNVBAMMB3Rlc3RpbmcwgZ8wDQYJKoZIhvcNAQEBBQAD\n"
    b"-----END CERTIFICATE-----\n"
)
TEST_CHAIN_PEM = (
    b"-----BEGIN CERTIFICATE-----\n"
    b"MIIB7TCCAVagAwIBAgIUR0Yc4xRoundTripTestChain123456789wDQYJKoZIhv\n"
    b"NAQELBQAwEjEQMA4GA1UEAwwHdGVzdGluZzAeFw0yNjAxMDEwMDAwMDBaFw0yNzAx\n"
    b"-----END CERTIFICATE-----\n"
)
TEST_PRIVATE_KEY_PEM = (
    b"-----BEGIN PRIVATE KEY-----\n"
    b"MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC0IamGfakeKey1\n"
    b"-----END PRIVATE KEY-----\n"
)


# ── H-7: GetCertificate returns the stored PEM, not a literal ─────────

def test_import_then_get_returns_supplied_certificate_body(acm_client):
    """ImportCertificate must store the Certificate bytes; GetCertificate
    must return the stored bytes verbatim. Without the fix, GetCertificate
    returned a hard-coded literal containing 'MIIFakeCertificateDataHere'."""
    acm = acm_client
    resp = acm.import_certificate(
        Certificate=TEST_CERT_PEM,
        PrivateKey=TEST_PRIVATE_KEY_PEM,
    )
    arn = resp["CertificateArn"]

    got = acm.get_certificate(CertificateArn=arn)
    assert got["Certificate"] == TEST_CERT_PEM.decode(), (
        "GetCertificate did not return the imported Certificate body — "
        "ACM emulator is silently fabricating PEM data, breaking any "
        "consumer that parses or validates the cert."
    )

    # Defensive: the literal placeholder must not leak.
    assert "MIIFakeCertificateDataHere" not in got["Certificate"]
    assert "MIIFakeChainDataHere" not in got.get("CertificateChain", "")


def test_import_then_get_returns_supplied_chain(acm_client):
    """ImportCertificate's CertificateChain must round-trip through
    GetCertificate."""
    acm = acm_client
    resp = acm.import_certificate(
        Certificate=TEST_CERT_PEM,
        CertificateChain=TEST_CHAIN_PEM,
        PrivateKey=TEST_PRIVATE_KEY_PEM,
    )
    arn = resp["CertificateArn"]

    got = acm.get_certificate(CertificateArn=arn)
    assert got["CertificateChain"] == TEST_CHAIN_PEM.decode(), (
        "GetCertificate did not return the imported CertificateChain."
    )


def test_get_certificate_omits_private_key(acm_client):
    """Real AWS GetCertificate never returns the private key (security).
    The emulator must match this behaviour even though it stores it
    internally for round-trip fidelity."""
    acm = acm_client
    resp = acm.import_certificate(
        Certificate=TEST_CERT_PEM,
        PrivateKey=TEST_PRIVATE_KEY_PEM,
    )
    arn = resp["CertificateArn"]

    got = acm.get_certificate(CertificateArn=arn)
    assert "PrivateKey" not in got, (
        "GetCertificate response leaked the private key — real AWS "
        "ACM never returns the PrivateKey via GetCertificate, only via "
        "ExportCertificate (which requires a passphrase)."
    )


# ── M-7: ImportCertificate must not lie about the domain ──────────────

def test_imported_certificate_does_not_lie_about_domain(acm_client):
    """Real AWS parses DomainName / SubjectAlternativeNames from the
    cert's CN/SAN extensions. The emulator does not implement X.509
    parsing (out of scope), so it MUST NOT advertise a fabricated
    'imported.example.com' that bears no relation to the actual cert.

    Acceptable behaviour for an emulator without ASN.1 parsing:
      - Return an empty / null DomainName, OR
      - Return a placeholder that is clearly synthetic (contains the
        cert ARN, says 'unknown', etc.), OR
      - Echo a DomainName supplied via tags (escape hatch).

    Returning the literal "imported.example.com" misleads CDK /
    Terraform plans into believing the cert covers a domain it does
    not."""
    acm = acm_client
    resp = acm.import_certificate(
        Certificate=TEST_CERT_PEM,
        PrivateKey=TEST_PRIVATE_KEY_PEM,
    )
    arn = resp["CertificateArn"]

    desc = acm.describe_certificate(CertificateArn=arn)["Certificate"]
    assert desc["DomainName"] != "imported.example.com", (
        "ImportCertificate emitted DomainName='imported.example.com' "
        "regardless of input — that's a fabricated domain that misleads "
        "consumers. Either parse from the cert, leave empty, or use a "
        "synthetic placeholder."
    )


def test_re_import_preserves_arn_and_replaces_body(acm_client):
    """When CertificateArn is supplied to ImportCertificate, the cert
    body is replaced in-place (real AWS semantics for cert renewal).
    Without H-7's fix this test would still pass against literal data
    so it's a sanity-check of the new path."""
    acm = acm_client
    first = acm.import_certificate(
        Certificate=TEST_CERT_PEM,
        PrivateKey=TEST_PRIVATE_KEY_PEM,
    )
    arn = first["CertificateArn"]

    new_cert = TEST_CERT_PEM.replace(b"RoundTripTestCert", b"ReimportRoundTrip")
    second = acm.import_certificate(
        CertificateArn=arn,
        Certificate=new_cert,
        PrivateKey=TEST_PRIVATE_KEY_PEM,
    )
    assert second["CertificateArn"] == arn, (
        "Re-import with explicit CertificateArn should preserve the ARN."
    )

    got = acm.get_certificate(CertificateArn=arn)
    assert got["Certificate"] == new_cert.decode()


# ── PrivateKey persistence leak (in-process, not through the live server) ─

def test_get_state_strips_private_key_from_persisted_snapshot():
    """Private keys must not be written to ${STATE_DIR}/acm.json. Real
    AWS only exposes them via passphrase-protected ExportCertificate;
    the GetCertificate wire path already honours that. Persistence must
    not become a side-channel for material the wire refuses to leak.

    Calls the module's `get_state()` directly — the snapshot it returns
    is exactly what `core/persistence.save_state` would JSON-encode to
    disk, so anything in there ends up readable on the filesystem."""
    import importlib
    import json

    from ministack.core.persistence import _json_default
    from ministack.core.responses import _request_account_id
    mod = importlib.import_module("ministack.services.acm")
    mod._certificates._data.clear()  # belt-and-braces

    # Two tenants — the request-scoped iteration would only see one of
    # them. Both must be scrubbed in the snapshot AND in the
    # production-encoder JSON blob.
    arn_a = "arn:aws:acm:us-east-1:000000000000:certificate/leak-check-a"
    arn_b = "arn:aws:acm:us-east-1:111111111111:certificate/leak-check-b"
    secret_a = "-----BEGIN PRIVATE KEY-----\nVERY_SECRET_KEY_TENANT_A\n-----END PRIVATE KEY-----\n"
    secret_b = "-----BEGIN PRIVATE KEY-----\nVERY_SECRET_KEY_TENANT_B\n-----END PRIVATE KEY-----\n"

    token_a = _request_account_id.set("000000000000")
    mod._certificates[arn_a] = {
        "CertificateArn": arn_a,
        "DomainName": "leak-check-a.invalid",
        "Status": "ISSUED",
        "Type": "IMPORTED",
        "_pem_body": "-----BEGIN CERTIFICATE-----\nBODY\n-----END CERTIFICATE-----\n",
        "_pem_chain": "",
        "_private_key": secret_a,
    }
    _request_account_id.reset(token_a)

    token_b = _request_account_id.set("111111111111")
    mod._certificates[arn_b] = {
        "CertificateArn": arn_b,
        "DomainName": "leak-check-b.invalid",
        "Status": "ISSUED",
        "Type": "IMPORTED",
        "_pem_body": "-----BEGIN CERTIFICATE-----\nBODY\n-----END CERTIFICATE-----\n",
        "_pem_chain": "",
        "_private_key": secret_b,
    }
    _request_account_id.reset(token_b)

    snapshot = mod.get_state()

    # Both tenants must have _private_key stripped — using _data so we
    # see all accounts, not just the request-scoped one.
    for cert in snapshot["_certificates"]._data.values():
        assert "_private_key" not in cert, (
            "PrivateKey leaked into the persistence snapshot — get_state() "
            "must scrub it for ALL tenants before save_state writes "
            "plaintext JSON to disk."
        )
        assert cert["_pem_body"].startswith("-----BEGIN CERTIFICATE-----")

    # Defensive: round-trip via the actual production encoder (used by
    # save_state) — `default=str` was request-scoped via __repr__ and
    # missed cross-tenant data.
    blob = json.dumps(snapshot, default=_json_default)
    assert "VERY_SECRET_KEY_TENANT_A" not in blob, (
        "Tenant A private-key material found in JSON-serialised "
        "snapshot — would be written verbatim to ${STATE_DIR}/acm.json."
    )
    assert "VERY_SECRET_KEY_TENANT_B" not in blob, (
        "Tenant B private-key material found in JSON-serialised "
        "snapshot — get_state() must scrub all tenants."
    )

    # Restoring the scrubbed snapshot must not crash and must preserve
    # both tenants' certs (minus the private keys).
    mod._certificates._data.clear()
    mod.restore_state(snapshot)
    restored_arns = {cert["CertificateArn"] for cert in mod._certificates._data.values()}
    assert arn_a in restored_arns
    assert arn_b in restored_arns
    mod._certificates._data.clear()


def test_get_state_preserves_certs_across_all_tenants():
    """get_state() must persist every tenant's certificates, not just
    the current request's account. Iterating `_certificates.items()`
    is request-scoped via AccountScopedDict's contextvar; iterating
    `_certificates._data` captures all (account_id, key) pairs."""
    import importlib

    from ministack.core.responses import _request_account_id
    mod = importlib.import_module("ministack.services.acm")
    mod.reset() if hasattr(mod, "reset") else None
    mod._certificates._data.clear()  # belt-and-braces

    # Pretend we're tenant A and write a cert.
    token_a = _request_account_id.set("111111111111")
    arn_a = "arn:aws:acm:us-east-1:111111111111:certificate/tenant-a"
    mod._certificates[arn_a] = {"CertificateArn": arn_a, "_pem_body": "a"}
    _request_account_id.reset(token_a)

    # Switch to tenant B and write another.
    token_b = _request_account_id.set("222222222222")
    arn_b = "arn:aws:acm:us-east-1:222222222222:certificate/tenant-b"
    mod._certificates[arn_b] = {"CertificateArn": arn_b, "_pem_body": "b"}
    _request_account_id.reset(token_b)

    # Snapshot from tenant B's request scope (worst case).
    token = _request_account_id.set("222222222222")
    snapshot = mod.get_state()
    _request_account_id.reset(token)

    persisted = snapshot["_certificates"]
    raw_keys = list(persisted._data.keys())
    accounts_persisted = {acct for acct, _ in raw_keys}
    assert accounts_persisted == {"111111111111", "222222222222"}, (
        "get_state() dropped a tenant's certs from the snapshot — only "
        f"persisted accounts: {accounts_persisted}. AccountScopedDict.items() "
        "is request-scoped; iterating _data is required to capture all "
        "tenants."
    )
    mod._certificates._data.clear()


def test_restore_state_backfills_pem_body_for_pre_upgrade_snapshots():
    """Pre-fix `acm.json` snapshots have no `_pem_body` / `_pem_chain`
    keys (the old GetCertificate path returned a hard-coded literal
    regardless of stored data). Without backfill in restore_state,
    those certs would return an empty Certificate field after
    warm-boot — strictly worse than the old behaviour. Backfill must
    fill them with the synthetic placeholder so consumers that
    substring-check 'BEGIN CERTIFICATE' (Terraform / CDK) keep
    working."""
    import importlib
    mod = importlib.import_module("ministack.services.acm")
    mod._certificates._data.clear()

    arn = "arn:aws:acm:us-east-1:000000000000:certificate/legacy-cert"
    legacy_snapshot = {
        "_certificates": {
            arn: {
                "CertificateArn": arn,
                "DomainName": "legacy.example.com",
                "Status": "ISSUED",
                "Type": "AMAZON_ISSUED",
                # Note: no _pem_body, no _pem_chain — pre-upgrade shape.
            },
        },
    }
    mod.restore_state(legacy_snapshot)

    # _get_certificate hits the restored record and reads _pem_body.
    cert = mod._certificates.get(arn)
    assert cert is not None, "Restore failed — cert not in dict."
    assert "_pem_body" in cert, (
        "restore_state did not backfill _pem_body — pre-upgrade "
        "GetCertificate would return an empty Certificate field."
    )
    assert "BEGIN CERTIFICATE" in cert["_pem_body"]
    assert cert.get("_pem_chain") == ""
    mod._certificates._data.clear()


def test_synthetic_pem_body_is_valid_base64():
    """The placeholder PEM body issued by RequestCertificate must be
    valid base64 — consumers that pre-decode (PyOpenSSL,
    cryptography) error before they reach ASN.1 parsing if it isn't."""
    import base64
    import importlib
    mod = importlib.import_module("ministack.services.acm")
    pem = mod._synthetic_pem("anything.example.com")
    body_lines = [
        line for line in pem.splitlines()
        if line and not line.startswith("-----")
    ]
    body = "".join(body_lines)
    # Must base64-decode without raising (binascii.Error otherwise).
    decoded = base64.b64decode(body)
    assert isinstance(decoded, bytes)
    assert len(decoded) > 0
