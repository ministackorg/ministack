import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


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

def test_acm_delete_certificate(acm_client):
    arn = acm_client.request_certificate(DomainName="delete.example.com")["CertificateArn"]
    acm_client.delete_certificate(CertificateArn=arn)
    resp = acm_client.list_certificates()
    arns = [c["CertificateArn"] for c in resp["CertificateSummaryList"]]
    assert arn not in arns

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
