import json
import os

import pytest
import requests

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def test_imds_v1_role_listing():
    r = requests.get(f"{ENDPOINT}/latest/meta-data/iam/security-credentials/")
    assert r.status_code == 200
    assert r.text.strip() == "ministack-instance-role"


def test_imds_v1_credentials_doc_shape():
    r = requests.get(f"{ENDPOINT}/latest/meta-data/iam/security-credentials/ministack-instance-role")
    assert r.status_code == 200
    doc = r.json()
    assert doc["Code"] == "Success"
    assert doc["Type"] == "AWS-HMAC"
    for k in ("AccessKeyId", "SecretAccessKey", "Token", "Expiration", "LastUpdated"):
        assert k in doc and doc[k]


def test_imds_v2_token_then_get():
    t = requests.put(
        f"{ENDPOINT}/latest/api/token",
        headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
    )
    assert t.status_code == 200
    token = t.text.strip()
    assert token

    r = requests.get(
        f"{ENDPOINT}/latest/meta-data/instance-id",
        headers={"X-aws-ec2-metadata-token": token},
    )
    assert r.status_code == 200
    assert r.text.startswith("i-")


def test_imds_identity_document():
    r = requests.get(f"{ENDPOINT}/latest/dynamic/instance-identity/document")
    assert r.status_code == 200
    doc = r.json()
    assert doc["accountId"] == "000000000000"
    assert doc["region"]
    assert doc["instanceId"].startswith("i-")


def test_imds_unknown_leaf_404():
    r = requests.get(f"{ENDPOINT}/latest/meta-data/does-not-exist")
    assert r.status_code == 404


def test_imds_token_endpoint_rejects_get():
    r = requests.get(f"{ENDPOINT}/latest/api/token")
    assert r.status_code == 405


def test_imds_v2_required_blocks_tokenless():
    """When MINISTACK_IMDS_V2_REQUIRED=1 the server rejects token-less GETs."""
    pytest.skip("requires server restart with MINISTACK_IMDS_V2_REQUIRED=1; covered manually")


def test_imds_placement_region():
    r = requests.get(f"{ENDPOINT}/latest/meta-data/placement/region")
    assert r.status_code == 200
    assert r.text.strip()


def test_container_credentials_returns_ecs_strict_shape():
    """ECS task role endpoint: AWS_CONTAINER_CREDENTIALS_RELATIVE_URI=/v2/credentials/<uuid>.
    Real AWS returns the strict 5-field shape (AccessKeyId, SecretAccessKey,
    Token, Expiration, RoleArn). The IMDS-only fields (Code, Type, LastUpdated)
    must NOT be present — distinguishing the ECS endpoint from the EC2 IMDS
    endpoint at /latest/meta-data/iam/security-credentials/<role>."""
    r = requests.get(f"{ENDPOINT}/v2/credentials/68e5868d-1bde-4f9e-9921-6e0442cb567b")
    assert r.status_code == 200
    doc = r.json()
    for k in ("AccessKeyId", "SecretAccessKey", "Token", "Expiration", "RoleArn"):
        assert k in doc and doc[k], f"missing or empty: {k}"
    assert doc["RoleArn"].startswith("arn:aws:iam::") and ":role/" in doc["RoleArn"]
    for k in ("Code", "Type", "LastUpdated"):
        assert k not in doc, f"{k} is IMDS-only and must not leak into the ECS endpoint"


def test_container_credentials_requires_id_segment():
    r = requests.get(f"{ENDPOINT}/v2/credentials/")
    assert r.status_code == 404
    r = requests.get(f"{ENDPOINT}/v2/credentials")
    assert r.status_code == 404


def test_container_credentials_rejects_non_get():
    r = requests.post(f"{ENDPOINT}/v2/credentials/abc")
    assert r.status_code == 405
