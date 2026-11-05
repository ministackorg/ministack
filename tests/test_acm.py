import itertools
import json
import os

import pytest
from botocore.exceptions import ClientError
from conftest import make_client


def test_acm_request_certificate(acm_client):
    resp = acm_client.request_certificate(
        DomainName="example.com",
        ValidationMethod="DNS",
        SubjectAlternativeNames=["www.example.com"],
    )
    arn = resp["CertificateArn"]
    assert arn.startswith("arn:aws:acm:us-east-1:000000000000:certificate/")

def test_acm_describe_certificate(acm_client):
    arn = acm_client.request_certificate(DomainName="describe.example.com")["CertificateArn"]
    resp = acm_client.describe_certificate(CertificateArn=arn)
    cert = resp["Certificate"]
    assert cert["DomainName"] == "describe.example.com"
    assert cert["Status"] == "ISSUED"
    assert len(cert["DomainValidationOptions"]) >= 1
    assert "ResourceRecord" in cert["DomainValidationOptions"][0]

def test_acm_wildcard_san_validation_record_matches_base_domain(acm_client):
    """A wildcard SAN validates through the same DNS record as its base domain:
    real ACM strips the leading '*.' so '*.example.com' and 'example.com' share
    one _acme-challenge.example.com CNAME with an identical name and value.
    Regression for #1250 — aws_acm_certificate_validation could not find the
    FQDN because the wildcard emitted _acme-challenge.*.example.com."""
    arn = acm_client.request_certificate(
        DomainName="wild.example.com",
        ValidationMethod="DNS",
        SubjectAlternativeNames=["*.wild.example.com"],
    )["CertificateArn"]
    cert = acm_client.describe_certificate(CertificateArn=arn)["Certificate"]
    dvo = {o["DomainName"]: o["ResourceRecord"] for o in cert["DomainValidationOptions"]}

    # The wildcard entry keeps its own DomainName but its record name has the
    # '*.' stripped — never _acme-challenge.*.<domain>.
    assert dvo["*.wild.example.com"]["Name"] == "_acme-challenge.wild.example.com."
    assert "*." not in dvo["*.wild.example.com"]["Name"]
    # Wildcard and apex collapse onto one identical CNAME (name AND value), so
    # Terraform's for_each dedup and validation_record_fqdns line up.
    assert dvo["*.wild.example.com"] == dvo["wild.example.com"]

def test_acm_list_certificates(acm_client):
    arn = acm_client.request_certificate(DomainName="list.example.com")["CertificateArn"]
    resp = acm_client.list_certificates()
    arns = [c["CertificateArn"] for c in resp["CertificateSummaryList"]]
    assert arn in arns

def test_acm_tags(acm_client):
    arn = acm_client.request_certificate(DomainName="tags.example.com")["CertificateArn"]
    acm_client.add_tags_to_certificate(
        CertificateArn=arn,
        Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}],
    )
    tags = acm_client.list_tags_for_certificate(CertificateArn=arn)["Tags"]
    assert any(t["Key"] == "env" and t["Value"] == "test" for t in tags)
    acm_client.remove_tags_from_certificate(
        CertificateArn=arn,
        Tags=[{"Key": "team", "Value": "platform"}],
    )
    tags2 = acm_client.list_tags_for_certificate(CertificateArn=arn)["Tags"]
    assert not any(t["Key"] == "team" for t in tags2)

def test_acm_get_certificate(acm_client):
    arn = acm_client.request_certificate(DomainName="pem.example.com")["CertificateArn"]
    resp = acm_client.get_certificate(CertificateArn=arn)
    assert "BEGIN CERTIFICATE" in resp["Certificate"]

def test_acm_import_certificate(acm_client):
    fake_cert = b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----"
    fake_key = b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----"
    resp = acm_client.import_certificate(Certificate=fake_cert, PrivateKey=fake_key)
    arn = resp["CertificateArn"]
    desc = acm_client.describe_certificate(CertificateArn=arn)
    assert desc["Certificate"]["Type"] == "IMPORTED"

def test_acm_import_certificate_requires_a_private_key(acm_client):
    raw = make_client("acm", {"parameter_validation": False})
    fake_cert = b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----"
    fake_key = b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----"
    arn = acm_client.import_certificate(Certificate=fake_cert, PrivateKey=fake_key)["CertificateArn"]
    for args, constraint in (
        ({}, "Member must not be null"),
        ({"PrivateKey": b""}, "Member must have length greater than or equal to 1"),
        ({"CertificateArn": arn}, "Member must not be null"),
    ):
        with pytest.raises(ClientError) as exc:
            raw.import_certificate(Certificate=fake_cert, **args)
        assert exc.value.response["Error"] == {
            "Code": "ValidationException",
            "Message": f"1 validation error detected: Value at 'privateKey' failed to satisfy constraint: {constraint}",
        }
    acm_client.delete_certificate(CertificateArn=arn)

def test_acm_delete_certificate(acm_client):
    arn = acm_client.request_certificate(DomainName="delete.example.com")["CertificateArn"]
    acm_client.delete_certificate(CertificateArn=arn)
    resp = acm_client.list_certificates()
    arns = [c["CertificateArn"] for c in resp["CertificateSummaryList"]]
    assert arn not in arns

def test_acm_delete_certificate_refused_while_an_active_signing_profile_uses_it(acm_client, signer):
    fake_cert = b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----"
    fake_key = b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----"
    arn = acm_client.import_certificate(Certificate=fake_cert, PrivateKey=fake_key)["CertificateArn"]
    account = arn.split(":")[4]
    assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["InUseBy"] == []
    suffix = os.urandom(4).hex()
    profiles = [
        signer.put_signing_profile(
            profileName=f"acm_in_use_{tag}_{suffix}",
            platformId="AWSIoTDeviceManagement-SHA256-ECDSA",
            signingMaterial={"certificateArn": arn},
        )["arn"]
        for tag in ("first", "second")
    ]
    signer.put_signing_profile(profileName=f"acm_in_use_lambda_{suffix}", platformId="AWSLambda-SHA384-ECDSA")

    for in_use in (profiles, profiles[1:]):
        assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["InUseBy"] == in_use
        with pytest.raises(ClientError) as exc:
            acm_client.delete_certificate(CertificateArn=arn)
        assert exc.value.response["Error"] == {
            "Code": "ResourceInUseException",
            "Message": f"Certificate {arn} in account {account} is in use.",
        }
        signer.cancel_signing_profile(profileName=in_use[0].rsplit("/", 1)[-1])

    assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["InUseBy"] == []
    acm_client.delete_certificate(CertificateArn=arn)

def test_acm_update_certificate_options(acm_client):
    arn = acm_client.request_certificate(DomainName="options.example.com")["CertificateArn"]
    acm_client.update_certificate_options(
        CertificateArn=arn,
        Options={"CertificateTransparencyLoggingPreference": "DISABLED"},
    )
    desc = acm_client.describe_certificate(CertificateArn=arn)
    pref = desc["Certificate"]["Options"]["CertificateTransparencyLoggingPreference"]
    assert pref == "DISABLED"
    acm_client.update_certificate_options(
        CertificateArn=arn,
        Options={"CertificateTransparencyLoggingPreference": "ENABLED"},
    )
    desc2 = acm_client.describe_certificate(CertificateArn=arn)
    pref2 = desc2["Certificate"]["Options"]["CertificateTransparencyLoggingPreference"]
    assert pref2 == "ENABLED"
    acm_client.delete_certificate(CertificateArn=arn)


_ENUM_ERROR = ("Value of the input at 'options.{}' failed to satisfy constraint: "
               "Member must satisfy enum value set: [ENABLED, DISABLED]")


def test_acm_request_certificate_stores_options_with_defaults(acm_client):
    for sent, expected in (
        ({"CertificateTransparencyLoggingPreference": "ENABLED"}, ("ENABLED", "DISABLED")),
        ({"CertificateTransparencyLoggingPreference": "DISABLED"}, ("DISABLED", "DISABLED")),
        ({"Export": "DISABLED"}, ("ENABLED", "DISABLED")),
        ({}, ("ENABLED", "DISABLED")),
        (None, ("ENABLED", "DISABLED")),
    ):
        kwargs = {} if sent is None else {"Options": sent}
        arn = acm_client.request_certificate(DomainName="opts.example.com", ValidationMethod="DNS",
                                             **kwargs)["CertificateArn"]
        options = acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"]
        assert options == {"CertificateTransparencyLoggingPreference": expected[0], "Export": expected[1]}
        acm_client.delete_certificate(CertificateArn=arn)


def test_acm_certificate_options_reject_values_outside_the_enum(acm_client):
    raw = make_client("acm", {"parameter_validation": False})
    arn = acm_client.request_certificate(ValidationMethod="DNS", DomainName="opts-enum.example.com")["CertificateArn"]
    ct, export = _ENUM_ERROR.format("certificateTransparencyLoggingPreference"), _ENUM_ERROR.format("export")
    for options, message in (
        ({"CertificateTransparencyLoggingPreference": "BOGUS"}, f"1 validation error detected: {ct}"),
        ({"CertificateTransparencyLoggingPreference": "disabled"}, f"1 validation error detected: {ct}"),
        ({"CertificateTransparencyLoggingPreference": ""}, f"1 validation error detected: {ct}"),
        ({"Export": "disabled"}, f"1 validation error detected: {export}"),
        ({"Export": "BOGUS"}, f"1 validation error detected: {export}"),
        ({"Export": ""}, f"1 validation error detected: {export}"),
        ({"CertificateTransparencyLoggingPreference": "BOGUS", "Export": "BOGUS"},
         f"2 validation errors detected: {ct}; {export}"),
    ):
        with pytest.raises(ClientError) as exc:
            raw.request_certificate(ValidationMethod="DNS", DomainName="opts-enum.example.com", Options=options)
        assert exc.value.response["Error"] == {"Code": "ValidationException", "Message": message}
        # Checked before the certificate lookup: an unknown ARN gets the same error.
        unknown = (arn.rsplit("/", 1)[0] + "/00000000-0000-0000-0000-000000000000",) if options == {"CertificateTransparencyLoggingPreference": "BOGUS"} else ()
        for target in (arn, *unknown):
            with pytest.raises(ClientError) as exc:
                raw.update_certificate_options(CertificateArn=target, Options=options)
            assert exc.value.response["Error"] == {"Code": "ValidationException", "Message": message}
    with pytest.raises(ClientError) as exc:
        raw.update_certificate_options(CertificateArn=arn)
    assert exc.value.response["Error"] == {
        "Code": "ValidationException",
        "Message": "1 validation error detected: Value null at 'options' failed to satisfy constraint: "
                   "Member must not be null",
    }
    assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"] == {
        "CertificateTransparencyLoggingPreference": "ENABLED", "Export": "DISABLED"}
    acm_client.delete_certificate(CertificateArn=arn)


def test_acm_update_certificate_options_keeps_omitted_options_and_export(acm_client):
    arn = acm_client.request_certificate(
        ValidationMethod="DNS",
        DomainName="opts-merge.example.com",
        Options={"CertificateTransparencyLoggingPreference": "DISABLED"},
    )["CertificateArn"]
    expected = {"CertificateTransparencyLoggingPreference": "DISABLED", "Export": "DISABLED"}
    for options in ({"Export": "DISABLED"}, {}):
        acm_client.update_certificate_options(CertificateArn=arn, Options=options)
        assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"] == expected
    for options in ({"Export": "ENABLED"},
                    {"CertificateTransparencyLoggingPreference": "ENABLED", "Export": "ENABLED"}):
        with pytest.raises(ClientError) as exc:
            acm_client.update_certificate_options(CertificateArn=arn, Options=options)
        assert exc.value.response["Error"] == {
            "Code": "ValidationException", "Message": "Export option for certificates cannot be updated"}
        assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"] == expected
    acm_client.delete_certificate(CertificateArn=arn)


def test_acm_imported_certificate_options(acm_client):
    fake_cert = b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----"
    fake_key = b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----"
    arn = acm_client.import_certificate(Certificate=fake_cert, PrivateKey=fake_key)["CertificateArn"]
    expected = {"CertificateTransparencyLoggingPreference": "DISABLED", "Export": "DISABLED"}
    assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"] == expected
    for options in ({"CertificateTransparencyLoggingPreference": "DISABLED"},
                    {"Export": "ENABLED"}, {"Export": "DISABLED"}):
        with pytest.raises(ClientError) as exc:
            acm_client.update_certificate_options(CertificateArn=arn, Options=options)
        assert exc.value.response["Error"] == {
            "Code": "InvalidStateException",
            "Message": "The UpdateCertificateOptions command is not permitted for imported certificates.",
        }
    raw = make_client("acm", {"parameter_validation": False})
    for options in ({}, {"CertificateTransparencyLoggingPreference": None},
                    {"CertificateTransparencyLoggingPreference": None, "Export": None}):
        raw.update_certificate_options(CertificateArn=arn, Options=options)
    acm_client.import_certificate(CertificateArn=arn, Certificate=fake_cert, PrivateKey=fake_key)
    assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"] == expected
    acm_client.delete_certificate(CertificateArn=arn)


def test_acm_certificate_options_null_members_count_as_not_sent(acm_client):
    raw = make_client("acm", {"parameter_validation": False})
    for sent, expected in (
        ({"CertificateTransparencyLoggingPreference": None}, ("ENABLED", "DISABLED")),
        ({"Export": None}, ("ENABLED", "DISABLED")),
        ({"CertificateTransparencyLoggingPreference": "DISABLED", "Export": None}, ("DISABLED", "DISABLED")),
    ):
        arn = raw.request_certificate(ValidationMethod="DNS", DomainName="opts-null.example.com", Options=sent)["CertificateArn"]
        options = acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"]
        assert options == {"CertificateTransparencyLoggingPreference": expected[0], "Export": expected[1]}
        acm_client.delete_certificate(CertificateArn=arn)
    arn = acm_client.request_certificate(
        ValidationMethod="DNS",
        DomainName="opts-null.example.com", Options={"CertificateTransparencyLoggingPreference": "DISABLED"},
    )["CertificateArn"]
    for options in ({"CertificateTransparencyLoggingPreference": None},
                    {"Export": None},
                    {"CertificateTransparencyLoggingPreference": None, "Export": "DISABLED"}):
        raw.update_certificate_options(CertificateArn=arn, Options=options)
    assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"] == {
        "CertificateTransparencyLoggingPreference": "DISABLED", "Export": "DISABLED"}
    acm_client.delete_certificate(CertificateArn=arn)


def test_acm_request_certificate_domain_name_errors_combine_with_options_errors(acm_client):
    raw = make_client("acm", {"parameter_validation": False})
    null = "Value null at 'domainName' failed to satisfy constraint: Member must not be null"
    empty = ("Value of the input at 'domainName' failed to satisfy constraint: Member must satisfy regular "
             "expression pattern: (\\*\\.)?(((?!-)[A-Za-z0-9-]{0,62}[A-Za-z0-9])\\.)+((?!-)[A-Za-z0-9-]{1,62}"
             "[A-Za-z0-9]); Value of the input at 'domainName' failed to satisfy constraint: Member must have "
             "length greater than or equal to 1")
    ct, export = _ENUM_ERROR.format("certificateTransparencyLoggingPreference"), _ENUM_ERROR.format("export")
    bogus = {"CertificateTransparencyLoggingPreference": "BOGUS"}
    for kwargs, message in (
        ({}, f"1 validation error detected: {null}"),
        ({"DomainName": None}, f"1 validation error detected: {null}"),
        ({"Options": bogus}, f"2 validation errors detected: {null}; {ct}"),
        ({"Options": {**bogus, "Export": "BOGUS"}}, f"3 validation errors detected: {null}; {ct}; {export}"),
        ({"DomainName": ""}, f"2 validation errors detected: {empty}"),
        ({"DomainName": "", "Options": bogus}, f"3 validation errors detected: {empty}; {ct}"),
    ):
        with pytest.raises(ClientError) as exc:
            raw.request_certificate(ValidationMethod="DNS", **kwargs)
        assert exc.value.response["Error"] == {"Code": "ValidationException", "Message": message}


def test_acm_exportable_certificate_options(acm_client):
    both = acm_client.request_certificate(
        ValidationMethod="DNS",
        DomainName="opts-export.example.com",
        Options={"CertificateTransparencyLoggingPreference": "DISABLED", "Export": "ENABLED"},
    )["CertificateArn"]
    assert acm_client.describe_certificate(CertificateArn=both)["Certificate"]["Options"] == {
        "CertificateTransparencyLoggingPreference": "DISABLED", "Export": "ENABLED"}
    acm_client.delete_certificate(CertificateArn=both)
    arn = acm_client.request_certificate(ValidationMethod="DNS", DomainName="opts-export.example.com",
                                         Options={"Export": "ENABLED"})["CertificateArn"]
    expected = {"CertificateTransparencyLoggingPreference": "ENABLED", "Export": "ENABLED"}
    assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"] == expected
    with pytest.raises(ClientError) as exc:
        acm_client.update_certificate_options(CertificateArn=arn, Options={"Export": "DISABLED"})
    assert exc.value.response["Error"] == {
        "Code": "ValidationException", "Message": "Export option for certificates cannot be updated"}
    for options in ({"Export": "ENABLED"}, {}):
        acm_client.update_certificate_options(CertificateArn=arn, Options=options)
    assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"] == expected
    acm_client.delete_certificate(CertificateArn=arn)


def test_acm_renew_certificate(acm_client):
    arn = acm_client.request_certificate(DomainName="renew.example.com")["CertificateArn"]
    # RenewCertificate is a no-op in ministack — just verify it doesn't error
    acm_client.renew_certificate(CertificateArn=arn)
    desc = acm_client.describe_certificate(CertificateArn=arn)
    assert desc["Certificate"]["Status"] in ("ISSUED", "PENDING_VALIDATION")
    acm_client.delete_certificate(CertificateArn=arn)

def test_acm_resend_validation_email(acm_client):
    arn = acm_client.request_certificate(
        DomainName="resend.example.com",
        ValidationMethod="EMAIL",
    )["CertificateArn"]
    acm_client.resend_validation_email(
        CertificateArn=arn,
        Domain="resend.example.com",
        ValidationDomain="example.com",
    )
    desc = acm_client.describe_certificate(CertificateArn=arn)
    assert desc["Certificate"]["DomainName"] == "resend.example.com"
    acm_client.delete_certificate(CertificateArn=arn)


# ========== from test_acm_cert_body.py ==========
# Regression tests for ACM cert body fidelity (H-7 + M-7) and
# private-key persistence leak prevention.
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
    b"-----BEGIN PRIVATE KEY-----\n"  # sadscan:disable np.pem.1 - intentionally fake ACM test fixture.
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


@pytest.fixture
def acm_service_module():
    import importlib

    from ministack.core.responses import _request_account_id, _request_region

    mod = importlib.import_module("ministack.services.acm")
    mod._certificates._data.clear()
    account_token = _request_account_id.set("000000000000")
    region_token = _request_region.set("us-east-1")
    try:
        yield mod
    finally:
        mod._certificates._data.clear()
        _request_region.reset(region_token)
        _request_account_id.reset(account_token)


def _json_body(response):
    return json.loads(response[2].decode())


def _error_code(response):
    return _json_body(response)["__type"]


@pytest.mark.parametrize(
    "bad_arn",
    [
        "arn:nope",
        "arn:aws-cn:acm:us-east-1:000000000000:certificate/wrong-partition",
        "arn:aws:sns:us-east-1:000000000000:certificate/not-acm",
        "arn:aws:acm:us-east-1:111111111111:certificate/wrong-account",
        "arn:aws:acm:us-west-2:000000000000:certificate/wrong-region",
        "arn:aws:acm:us-east-1:000000000000:not-a-certificate/not-acm-resource",
    ],
)
def test_acm_certificate_arn_operations_reject_out_of_scope_arns(acm_service_module, bad_arn):
    mod = acm_service_module
    created = _json_body(mod._request_certificate({"DomainName": "parser-scope.example.com"}))
    good_arn = created["CertificateArn"]

    for handler in (
        mod._describe_certificate,
        mod._get_certificate,
        mod._list_tags,
        mod._renew_certificate,
        mod._resend_validation_email,
    ):
        resp = handler({"CertificateArn": bad_arn})
        assert resp[0] == 400
        assert _error_code(resp) == "ResourceNotFoundException"

    for handler, payload in (
        (mod._add_tags, {"CertificateArn": bad_arn, "Tags": [{"Key": "bad", "Value": "tag"}]}),
        (mod._remove_tags, {"CertificateArn": bad_arn, "Tags": [{"Key": "keep"}]}),
        (
            mod._update_options,
            {
                "CertificateArn": bad_arn,
                "Options": {"CertificateTransparencyLoggingPreference": "DISABLED"},
            },
        ),
        (mod._delete_certificate, {"CertificateArn": bad_arn}),
    ):
        resp = handler(payload)
        assert resp[0] == 400
        assert _error_code(resp) == "ResourceNotFoundException"

    cert = _json_body(mod._describe_certificate({"CertificateArn": good_arn}))["Certificate"]
    assert cert["CertificateArn"] == good_arn
    assert cert["Options"] == {"CertificateTransparencyLoggingPreference": "ENABLED", "Export": "DISABLED"}
    assert cert["Tags"] == []


@pytest.mark.parametrize(
    "bad_arn",
    [
        "arn:nope",
        "arn:aws-cn:acm:us-east-1:000000000000:certificate/wrong-partition",
        "arn:aws:sns:us-east-1:000000000000:certificate/not-acm",
        "arn:aws:acm:us-east-1:111111111111:certificate/wrong-account",
        "arn:aws:acm:us-west-2:000000000000:certificate/wrong-region",
    ],
)
def test_acm_import_certificate_rejects_out_of_scope_certificate_arn(acm_service_module, bad_arn):
    mod = acm_service_module

    resp = mod._import_certificate(
        {
            "CertificateArn": bad_arn,
            "Certificate": TEST_CERT_PEM,
            "PrivateKey": TEST_PRIVATE_KEY_PEM,
            "Tags": [{"Key": "bad", "Value": "import"}],
        }
    )

    assert resp[0] == 400
    assert _error_code(resp) == "ResourceNotFoundException"
    assert mod._certificates._data == {}


def test_acm_valid_reimport_preserves_existing_certificate_arn(acm_service_module):
    mod = acm_service_module
    created = _json_body(
        mod._import_certificate(
            {
                "Certificate": TEST_CERT_PEM,
                "PrivateKey": TEST_PRIVATE_KEY_PEM,
            }
        )
    )
    arn = created["CertificateArn"]

    new_cert = TEST_CERT_PEM.replace(b"RoundTripTestCert", b"ParserAdoptedCert")
    resp = mod._import_certificate(
        {
            "CertificateArn": arn,
            "Certificate": new_cert,
            "PrivateKey": TEST_PRIVATE_KEY_PEM,
        }
    )

    assert resp[0] == 200
    assert _json_body(resp)["CertificateArn"] == arn
    assert _json_body(mod._get_certificate({"CertificateArn": arn}))["Certificate"] == new_cert.decode()


def test_acm_list_certificates_filters_to_request_region(acm_service_module):
    from ministack.core.responses import _request_region

    mod = acm_service_module
    east_arn = _json_body(mod._request_certificate({"DomainName": "east-list.example.com"}))["CertificateArn"]

    west_token = _request_region.set("us-west-2")
    try:
        west_arn = _json_body(mod._request_certificate({"DomainName": "west-list.example.com"}))["CertificateArn"]
        west_list = _json_body(mod._list_certificates({}))["CertificateSummaryList"]
    finally:
        _request_region.reset(west_token)

    east_list = _json_body(mod._list_certificates({}))["CertificateSummaryList"]

    assert [cert["CertificateArn"] for cert in west_list] == [west_arn]
    assert [cert["CertificateArn"] for cert in east_list] == [east_arn]


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
    mod.load_persisted_state(snapshot)
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
    accounts_persisted = {acct for acct, *_ in raw_keys}
    assert accounts_persisted == {"111111111111", "222222222222"}, (
        "get_state() dropped a tenant's certs from the snapshot — only "
        f"persisted accounts: {accounts_persisted}. AccountRegionScopedDict.items() "
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
    mod.load_persisted_state(legacy_snapshot)

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


# ========== from test_misc_medium_low_fixes.py ==========
# ListCertificates wire response must omit NextToken when there's no next
# page. boto3 strips nulls client-side so a boto3-only test can't see this,
# but Java/Go SDKs and pagination loops checking `if "NextToken" in resp`
# loop forever against a literal null. Asserted at the raw-HTTP level.

import urllib.request as _acm_urlreq

_ACM_ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")


def test_acm_list_certificates_omits_nexttoken_when_no_more_pages():
    req = _acm_urlreq.Request(
        _ACM_ENDPOINT.rstrip("/") + "/",
        method="POST",
        headers={
            "x-amz-target": "CertificateManager.ListCertificates",
            "Content-Type": "application/x-amz-json-1.1",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/x/us-east-1/acm/aws4_request",
        },
        data=b"{}",
    )
    body = json.loads(_acm_urlreq.urlopen(req, timeout=5).read())

    assert "NextToken" not in body, (
        f"ListCertificates wire response contains NextToken when there is "
        f"no next page (got {body.get('NextToken')!r}). Real AWS omits the "
        "key. SDK consumers checking `if 'NextToken' in response` "
        "(Java, Go, raw HTTP — boto3 strips nulls) loop forever."
    )


def test_acm_certificates_are_region_isolated(acm_client):
    """A certificate requested in us-east-1 must not be visible from us-west-2."""
    import boto3

    acm_west = boto3.client(
        "acm", endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/"),
        aws_access_key_id="test", aws_secret_access_key="test",
        region_name="us-west-2",
    )
    arn = acm_client.request_certificate(DomainName="iso.example.com")["CertificateArn"]
    assert ":us-east-1:" in arn
    try:
        east = [c["CertificateArn"] for c in acm_client.list_certificates()["CertificateSummaryList"]]
        west = [c["CertificateArn"] for c in acm_west.list_certificates()["CertificateSummaryList"]]
        assert arn in east
        assert arn not in west
    finally:
        acm_client.delete_certificate(CertificateArn=arn)


_CT, _EX, _VM = "CertificateTransparencyLoggingPreference", "Export", "ValidationMethod"
_LIST = "Start of list found where not expected"
_MAP = "Start of structure or map found where not expected."
_NUMBER = "NUMBER_VALUE can not be converted to a String"
_TRUE = "TRUE_VALUE can not be converted to a String"
_FALSE = "FALSE_VALUE can not be converted to a String"
def _same(value, *candidates):
    # Compare as JSON, so True and 1 differ.
    return any(json.dumps(value) == json.dumps(c) for c in candidates)


_MEMBER_TYPES = ((False, _FALSE), (True, _TRUE), (1, _NUMBER), (1.5, _NUMBER), ([], _LIST), (["ENABLED"], _LIST),
                 ({}, _MAP))


@pytest.mark.parametrize("options, message, on_update", [
    ("ENABLED", "Expected null", True),
    (1, "Expected null", True),
    (1.5, "Expected null", True),
    (True, "Expected null", True),
    (False, "Expected null", True),
    ([], _LIST, True),
    *[({_CT: v}, m, True) for v, m in _MEMBER_TYPES],
    *[({_EX: v}, m, True) for v, m in _MEMBER_TYPES],
    *[({_VM: v}, m, False) for v, m in _MEMBER_TYPES if v != ["ENABLED"]],
    # Members are checked in model order, and before enum values.
    ({_EX: True, _CT: 1}, _NUMBER, True),
    ({_CT: 1, _EX: "BOGUS"}, _NUMBER, True),
    ({_CT: "BOGUS", _EX: 1}, _NUMBER, False),
    ({_VM: 1, _CT: True}, _TRUE, False),
    ({_EX: True, _VM: 1}, _TRUE, False),
    ({_VM: [], _EX: True}, _TRUE, False),
    *[(dict(order), _NUMBER, False) for order in itertools.permutations(((_CT, 1), (_EX, True), (_VM, [])))],
])
def test_acm_certificate_options_that_are_not_an_object_fail_to_deserialize(acm_service_module, options, message,
                                                                             on_update):
    mod = acm_service_module
    arn = _json_body(mod._request_certificate({"ValidationMethod": "DNS", "DomainName": "opts-type.example.com"}))["CertificateArn"]
    responses = [mod._request_certificate({"ValidationMethod": "DNS", "DomainName": "opts-type.example.com", "Options": options})]
    if on_update:
        responses.append(mod._update_options({"CertificateArn": arn, "Options": options}))
    unknown = arn.rsplit("/", 1)[0] + "/00000000-0000-0000-0000-000000000000"
    # Checked before DomainName and before the certificate lookup.
    if _same(options, "ENABLED", {_CT: 1}):
        responses.append(mod._request_certificate({"ValidationMethod": "DNS", "Options": options}))
    if _same(options, []):
        responses.append(mod._request_certificate({"ValidationMethod": "DNS", "DomainName": "", "Options": options}))
    if _same(options, "ENABLED", [], {_CT: 1}, {_VM: 1}):
        responses.append(mod._update_options({"CertificateArn": unknown, "Options": options}))
    if _same(options, "ENABLED"):
        responses.append(mod._update_options({"Options": "x"}))
        responses.append(mod._update_options({"CertificateArn": None, "Options": options}))
    for resp in responses:
        assert resp[0] == 400
        assert _json_body(resp) == {"__type": "SerializationException", "Message": message}
    assert list(mod._certificates.keys()) == [arn]


# The Options each DomainName type was sent with: a string Options and a mistyped logging preference.
@pytest.mark.parametrize("domain, message, with_options", [
    (1, _NUMBER, ("ENABLED", {_CT: True})), (1.5, _NUMBER, ("ENABLED", {_CT: 1})), (True, _TRUE, ("ENABLED", {_CT: 1})),
    (False, _FALSE, ("ENABLED", {_CT: 1})), ([], _LIST, ("ENABLED", {_CT: 1})), (["a.example.com"], _LIST, ()),
    ({}, _MAP, ("ENABLED", {_CT: 1})),
])
def test_acm_request_certificate_domain_name_of_another_type_fails_to_deserialize(acm_service_module, domain,
                                                                                    message, with_options):
    mod = acm_service_module
    for data in ({"DomainName": domain}, *({"DomainName": domain, "Options": o} for o in with_options)):
        resp = mod._request_certificate({"ValidationMethod": "DNS", **data})
        assert resp[0] == 400
        assert _json_body(resp) == {"__type": "SerializationException", "Message": message}
    assert not mod._certificates


def test_acm_update_certificate_options_needs_a_certificate_arn(acm_service_module):
    mod = acm_service_module
    null_arn = "Value null at 'certificateArn' failed to satisfy constraint: Member must not be null"
    null_options = "Value null at 'options' failed to satisfy constraint: Member must not be null"
    ct = _ENUM_ERROR.format("certificateTransparencyLoggingPreference")
    for data, message in (
        ({"Options": {_CT: "BOGUS"}}, f"2 validation errors detected: {null_arn}; {ct}"),
        ({}, f"2 validation errors detected: {null_arn}; {null_options}"),
        ({"Options": {_CT: None}}, f"1 validation error detected: {null_arn}"),
    ):
        resp = mod._update_options(data)
        assert resp[0] == 400
        assert _json_body(resp) == {"__type": "ValidationException", "message": message}


def test_acm_update_certificate_options_with_nothing_to_change_skips_the_lookup(acm_service_module):
    mod = acm_service_module
    arn = _json_body(mod._request_certificate({"ValidationMethod": "DNS", "DomainName": "noop.example.com"}))["CertificateArn"]
    zero = "00000000-0000-0000-0000-000000000000"
    for target in (arn.rsplit("/", 1)[0] + "/" + zero,
                   f"arn:aws:acm:us-east-1:123456789012:certificate/{zero}",
                   f"arn:aws:acm:eu-west-1:000000000000:certificate/{zero}",
                   f"arn:aws-cn:acm:cn-north-1:000000000000:certificate/{zero}",
                   f"arn:aws:acm:us-east-1:000000000000:foo/{zero}"):
        resp = mod._update_options({"CertificateArn": target, "Options": {}})
        assert resp[0] == 200, target
    unknown = arn.rsplit("/", 1)[0] + "/" + zero
    for options in ({_CT: None}, {_CT: None, _EX: None}, {_VM: None}, {"Foo": "x"}, {_CT: None, "Foo": "x"}):
        resp = mod._update_options({"CertificateArn": unknown, "Options": options})
        assert resp[0] == 200, options
    for options in ({_VM: "DNS"}, {_EX: "DISABLED"}, {_CT: "ENABLED"}):
        resp = mod._update_options({"CertificateArn": unknown, "Options": options})
        assert resp[0] == 400
        assert _error_code(resp) == "ResourceNotFoundException"
    disabled = _json_body(mod._request_certificate({"ValidationMethod": "DNS", "DomainName": "noop.example.com",
                                                    "Options": {_CT: "DISABLED"}}))["CertificateArn"]
    resp = mod._update_options({"CertificateArn": disabled, "Options": {"Foo": "x", _EX: "DISABLED"}})
    assert resp[0] == 200
    assert _json_body(mod._describe_certificate({"CertificateArn": disabled}))["Certificate"]["Options"] == {
        _CT: "DISABLED", _EX: "DISABLED"}


def test_acm_certificate_options_validation_method(acm_client, acm_service_module):
    raw = make_client("acm", {"parameter_validation": False})
    vm = ("Value of the input at 'options.validationMethod' failed to satisfy constraint: "
          "Member must satisfy enum value set: [HTTP, DNS, EMAIL]")
    ct, export = _ENUM_ERROR.format("certificateTransparencyLoggingPreference"), _ENUM_ERROR.format("export")
    null = "Value null at 'domainName' failed to satisfy constraint: Member must not be null"
    for kwargs, message in (
        ({"Options": {_VM: "BOGUS"}}, f"1 validation error detected: {vm}"),
        ({"Options": {_VM: "dns"}}, f"1 validation error detected: {vm}"),
        ({"Options": {_VM: ""}}, f"1 validation error detected: {vm}"),
        ({"Options": {_CT: "BOGUS", _VM: "BOGUS"}}, f"2 validation errors detected: {vm}; {ct}"),
        ({"Options": {_VM: "BOGUS", _EX: "BOGUS"}}, f"2 validation errors detected: {vm}; {export}"),
        ({"Options": {_CT: "BOGUS", _EX: "BOGUS", _VM: "BOGUS"}}, f"3 validation errors detected: {vm}; {ct}; {export}"),
    ):
        with pytest.raises(ClientError) as exc:
            raw.request_certificate(DomainName="opts-vm.example.com", ValidationMethod="DNS", **kwargs)
        assert exc.value.response["Error"] == {"Code": "ValidationException", "Message": message}
    with pytest.raises(ClientError) as exc:
        raw.request_certificate(ValidationMethod="DNS", Options={_VM: "BOGUS"})
    assert exc.value.response["Error"] == {"Code": "ValidationException",
                                           "Message": f"2 validation errors detected: {null}; {vm}"}
    # Accepted, and not shown by DescribeCertificate.
    for options in ({_VM: "DNS"}, {_VM: None}, {_VM: "EMAIL"}, {_VM: "HTTP"}):
        arn = raw.request_certificate(DomainName="opts-vm.example.com", ValidationMethod="DNS",
                                      Options=options)["CertificateArn"]
        assert acm_client.describe_certificate(CertificateArn=arn)["Certificate"]["Options"] == {
            _CT: "ENABLED", _EX: "DISABLED"}
        acm_client.delete_certificate(CertificateArn=arn)
    # Unknown members are ignored (in-process: boto3 drops members the model doesn't have).
    mod = acm_service_module
    for options, expected in (({"Foo": "x"}, "ENABLED"), ({_CT: "DISABLED", "Foo": 1}, "DISABLED")):
        arn = _json_body(mod._request_certificate({"ValidationMethod": "DNS", "DomainName": "opts-vm.example.com", "Options": options}))[
            "CertificateArn"]
        assert _json_body(mod._describe_certificate({"CertificateArn": arn}))["Certificate"]["Options"] == {
            _CT: expected, _EX: "DISABLED"}

    arn = acm_client.request_certificate(ValidationMethod="DNS", DomainName="opts-vm.example.com")["CertificateArn"]
    for target in (arn, arn.rsplit("/", 1)[0] + "/00000000-0000-0000-0000-000000000000"):
        with pytest.raises(ClientError) as exc:
            raw.update_certificate_options(CertificateArn=target, Options={_VM: "BOGUS"})
        assert exc.value.response["Error"] == {"Code": "ValidationException",
                                               "Message": f"1 validation error detected: {vm}"}
    with pytest.raises(ClientError) as exc:
        raw.update_certificate_options(CertificateArn=arn, Options={_CT: "BOGUS", _VM: "BOGUS"})
    assert exc.value.response["Error"] == {"Code": "ValidationException",
                                           "Message": f"2 validation errors detected: {vm}; {ct}"}
    # On a requested certificate, the Export check comes before anything ValidationMethod does.
    with pytest.raises(ClientError) as exc:
        raw.update_certificate_options(CertificateArn=arn, Options={_VM: "DNS", _EX: "ENABLED"})
    assert exc.value.response["Error"] == {
        "Code": "ValidationException", "Message": "Export option for certificates cannot be updated"}
    acm_client.delete_certificate(CertificateArn=arn)

    fake_cert = b"-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----"
    fake_key = b"-----BEGIN RSA PRIVATE KEY-----\nfake\n-----END RSA PRIVATE KEY-----"
    iarn = acm_client.import_certificate(Certificate=fake_cert, PrivateKey=fake_key)["CertificateArn"]
    with pytest.raises(ClientError) as exc:
        raw.update_certificate_options(CertificateArn=iarn, Options={_VM: "BOGUS"})
    assert exc.value.response["Error"] == {"Code": "ValidationException",
                                           "Message": f"1 validation error detected: {vm}"}
    for options in ({_VM: "DNS"}, {_VM: "EMAIL"}, {_VM: "HTTP"}, {_VM: "DNS", _CT: None}):
        with pytest.raises(ClientError) as exc:
            raw.update_certificate_options(CertificateArn=iarn, Options=options)
        assert exc.value.response["Error"] == {
            "Code": "ValidationException", "Message": "UpdateCertificate can only be used for public certificates."}
    for options in ({_CT: "ENABLED", _VM: "DNS"}, {_EX: "DISABLED", _VM: "DNS"}, {_EX: "ENABLED", _VM: "DNS"},
                    {_VM: None, _CT: "ENABLED"}):
        with pytest.raises(ClientError) as exc:
            raw.update_certificate_options(CertificateArn=iarn, Options=options)
        assert exc.value.response["Error"] == {
            "Code": "InvalidStateException",
            "Message": "The UpdateCertificateOptions command is not permitted for imported certificates.",
        }
    assert acm_client.describe_certificate(CertificateArn=iarn)["Certificate"]["Options"] == {
        _CT: "DISABLED", _EX: "DISABLED"}
    acm_client.delete_certificate(CertificateArn=iarn)
