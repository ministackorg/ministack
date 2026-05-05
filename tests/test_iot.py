"""Integration tests for the IoT Core control plane (Phase 1a).

Exercises Things, ThingTypes, ThingGroups, Certificates (issued via the
Local CA), Policies, and DescribeEndpoint. The data plane (broker / WS /
iot-data Publish) is covered separately in ``test_iot_data.py``.
"""

import json
import uuid

import pytest
from botocore.exceptions import ClientError


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


def test_iot_describe_endpoint_data_ats(iot_client):
    resp = iot_client.describe_endpoint(endpointType="iot:Data-ATS")
    addr = resp["endpointAddress"]
    assert "-ats.iot." in addr
    assert "us-east-1" in addr


def test_iot_describe_endpoint_default_uses_data_ats(iot_client):
    resp = iot_client.describe_endpoint()
    assert "-ats.iot." in resp["endpointAddress"]


def test_iot_describe_endpoint_data_legacy(iot_client):
    resp = iot_client.describe_endpoint(endpointType="iot:Data")
    addr = resp["endpointAddress"]
    # Legacy endpoint omits the -ats suffix.
    assert ".iot." in addr
    assert "-ats.iot." not in addr


def test_iot_describe_endpoint_unknown_type_rejected(iot_client):
    with pytest.raises(ClientError) as ei:
        iot_client.describe_endpoint(endpointType="iot:Bogus")
    assert ei.value.response["Error"]["Code"] in ("InvalidRequestException",)


# ---------------------------------------------------------------------------
# Thing CRUD
# ---------------------------------------------------------------------------


def test_iot_create_describe_thing(iot_client):
    name = _unique("thing")
    resp = iot_client.create_thing(thingName=name)
    assert resp["thingName"] == name
    assert resp["thingArn"].endswith(f":thing/{name}")
    assert resp["thingId"]

    desc = iot_client.describe_thing(thingName=name)
    assert desc["thingName"] == name
    assert desc["version"] == 1
    iot_client.delete_thing(thingName=name)


def test_iot_create_thing_with_attributes(iot_client):
    name = _unique("thing")
    iot_client.create_thing(
        thingName=name,
        attributePayload={"attributes": {"color": "red", "size": "L"}},
    )
    desc = iot_client.describe_thing(thingName=name)
    assert desc["attributes"] == {"color": "red", "size": "L"}
    iot_client.delete_thing(thingName=name)


def test_iot_create_thing_idempotent_same_config(iot_client):
    name = _unique("thing")
    iot_client.create_thing(
        thingName=name,
        attributePayload={"attributes": {"color": "red"}},
    )
    # Same config must not raise.
    resp2 = iot_client.create_thing(
        thingName=name,
        attributePayload={"attributes": {"color": "red"}},
    )
    assert resp2["thingName"] == name
    iot_client.delete_thing(thingName=name)


def test_iot_create_thing_conflict_different_config(iot_client):
    name = _unique("thing")
    iot_client.create_thing(
        thingName=name,
        attributePayload={"attributes": {"color": "red"}},
    )
    with pytest.raises(ClientError) as ei:
        iot_client.create_thing(
            thingName=name,
            attributePayload={"attributes": {"color": "blue"}},
        )
    assert ei.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
    iot_client.delete_thing(thingName=name)


def test_iot_describe_unknown_thing_404(iot_client):
    with pytest.raises(ClientError) as ei:
        iot_client.describe_thing(thingName=_unique("nope"))
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_iot_update_thing_increments_version(iot_client):
    name = _unique("thing")
    iot_client.create_thing(thingName=name)
    iot_client.update_thing(
        thingName=name,
        attributePayload={"attributes": {"k": "v"}},
    )
    desc = iot_client.describe_thing(thingName=name)
    assert desc["version"] == 2
    assert desc["attributes"] == {"k": "v"}
    iot_client.delete_thing(thingName=name)


def test_iot_list_things_filter_by_attribute(iot_client):
    a = _unique("thing")
    b = _unique("thing")
    iot_client.create_thing(
        thingName=a, attributePayload={"attributes": {"region": "eu"}}
    )
    iot_client.create_thing(
        thingName=b, attributePayload={"attributes": {"region": "us"}}
    )
    resp = iot_client.list_things(attributeName="region", attributeValue="eu")
    names = {t["thingName"] for t in resp["things"]}
    assert a in names and b not in names
    iot_client.delete_thing(thingName=a)
    iot_client.delete_thing(thingName=b)


def test_iot_list_things_filter_by_thing_type(iot_client):
    type_a = _unique("type")
    iot_client.create_thing_type(thingTypeName=type_a)
    name = _unique("thing")
    iot_client.create_thing(thingName=name, thingTypeName=type_a)

    resp = iot_client.list_things(thingTypeName=type_a)
    assert any(t["thingName"] == name for t in resp["things"])

    iot_client.delete_thing(thingName=name)
    iot_client.deprecate_thing_type(thingTypeName=type_a)
    iot_client.delete_thing_type(thingTypeName=type_a)


# ---------------------------------------------------------------------------
# ThingType CRUD
# ---------------------------------------------------------------------------


def test_iot_thing_type_lifecycle(iot_client):
    name = _unique("type")
    iot_client.create_thing_type(thingTypeName=name)
    desc = iot_client.describe_thing_type(thingTypeName=name)
    assert desc["thingTypeName"] == name
    assert desc["thingTypeMetadata"]["deprecated"] is False

    iot_client.deprecate_thing_type(thingTypeName=name)
    desc2 = iot_client.describe_thing_type(thingTypeName=name)
    assert desc2["thingTypeMetadata"]["deprecated"] is True

    iot_client.delete_thing_type(thingTypeName=name)
    with pytest.raises(ClientError) as ei:
        iot_client.describe_thing_type(thingTypeName=name)
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_iot_delete_thing_type_active_rejected(iot_client):
    name = _unique("type")
    iot_client.create_thing_type(thingTypeName=name)
    with pytest.raises(ClientError) as ei:
        iot_client.delete_thing_type(thingTypeName=name)
    assert ei.value.response["Error"]["Code"] == "InvalidRequestException"
    iot_client.deprecate_thing_type(thingTypeName=name)
    iot_client.delete_thing_type(thingTypeName=name)


# ---------------------------------------------------------------------------
# ThingGroup CRUD + membership
# ---------------------------------------------------------------------------


def test_iot_thing_group_membership(iot_client):
    gname = _unique("group")
    tname = _unique("thing")
    iot_client.create_thing_group(thingGroupName=gname)
    iot_client.create_thing(thingName=tname)

    iot_client.add_thing_to_thing_group(thingGroupName=gname, thingName=tname)
    things = iot_client.list_things_in_thing_group(thingGroupName=gname)["things"]
    assert tname in things

    iot_client.remove_thing_from_thing_group(thingGroupName=gname, thingName=tname)
    things2 = iot_client.list_things_in_thing_group(thingGroupName=gname)["things"]
    assert tname not in things2

    iot_client.delete_thing(thingName=tname)
    iot_client.delete_thing_group(thingGroupName=gname)


# ---------------------------------------------------------------------------
# Certificates (issued via the Local CA)
# ---------------------------------------------------------------------------


def test_iot_create_keys_and_certificate_active(iot_client):
    pytest.importorskip("cryptography")
    resp = iot_client.create_keys_and_certificate(setAsActive=True)
    assert resp["certificateId"]
    assert resp["certificateArn"].endswith(":cert/" + resp["certificateId"])
    assert "BEGIN CERTIFICATE" in resp["certificatePem"]
    assert "BEGIN" in resp["keyPair"]["PrivateKey"]
    assert "BEGIN PUBLIC KEY" in resp["keyPair"]["PublicKey"]

    desc = iot_client.describe_certificate(certificateId=resp["certificateId"])
    assert desc["certificateDescription"]["status"] == "ACTIVE"

    # Deactivate and delete
    iot_client.update_certificate(
        certificateId=resp["certificateId"], newStatus="INACTIVE"
    )
    iot_client.delete_certificate(certificateId=resp["certificateId"])


def test_iot_create_keys_and_certificate_inactive(iot_client):
    pytest.importorskip("cryptography")
    resp = iot_client.create_keys_and_certificate(setAsActive=False)
    desc = iot_client.describe_certificate(certificateId=resp["certificateId"])
    assert desc["certificateDescription"]["status"] == "INACTIVE"
    iot_client.delete_certificate(certificateId=resp["certificateId"])


def test_iot_delete_active_certificate_rejected(iot_client):
    pytest.importorskip("cryptography")
    resp = iot_client.create_keys_and_certificate(setAsActive=True)
    cert_id = resp["certificateId"]
    with pytest.raises(ClientError) as ei:
        iot_client.delete_certificate(certificateId=cert_id)
    assert ei.value.response["Error"]["Code"] == "CertificateStateException"
    iot_client.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
    iot_client.delete_certificate(certificateId=cert_id)


def test_iot_register_certificate_preserves_pem_verbatim(iot_client):
    pytest.importorskip("cryptography")
    # Issue a cert, capture its PEM, delete it, then re-register the SAME PEM.
    issued = iot_client.create_keys_and_certificate(setAsActive=False)
    cert_pem = issued["certificatePem"]
    iot_client.delete_certificate(certificateId=issued["certificateId"])

    resp = iot_client.register_certificate(
        certificatePem=cert_pem, status="ACTIVE"
    )
    cert_id = resp["certificateId"]
    desc = iot_client.describe_certificate(certificateId=cert_id)
    assert desc["certificateDescription"]["certificatePem"] == cert_pem
    iot_client.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
    iot_client.delete_certificate(certificateId=cert_id)


def test_iot_attach_detach_thing_principal(iot_client):
    pytest.importorskip("cryptography")
    name = _unique("thing")
    iot_client.create_thing(thingName=name)
    cert = iot_client.create_keys_and_certificate(setAsActive=True)
    arn = cert["certificateArn"]

    iot_client.attach_thing_principal(thingName=name, principal=arn)
    principals = iot_client.list_thing_principals(thingName=name)["principals"]
    assert arn in principals
    things = iot_client.list_principal_things(principal=arn)["things"]
    assert name in things

    iot_client.detach_thing_principal(thingName=name, principal=arn)
    principals2 = iot_client.list_thing_principals(thingName=name)["principals"]
    assert arn not in principals2

    iot_client.update_certificate(certificateId=cert["certificateId"], newStatus="INACTIVE")
    iot_client.delete_certificate(certificateId=cert["certificateId"])
    iot_client.delete_thing(thingName=name)


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------


_POLICY_DOC = json.dumps(
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["iot:Connect", "iot:Publish"],
                "Resource": "*",
            }
        ],
    }
)


def test_iot_policy_lifecycle(iot_client):
    name = _unique("policy")
    resp = iot_client.create_policy(policyName=name, policyDocument=_POLICY_DOC)
    assert resp["policyName"] == name
    assert resp["policyVersionId"] == "1"

    got = iot_client.get_policy(policyName=name)
    assert got["defaultVersionId"] == "1"

    listing = iot_client.list_policies()["policies"]
    assert any(p["policyName"] == name for p in listing)

    iot_client.delete_policy(policyName=name)


def test_iot_create_policy_malformed_400(iot_client):
    name = _unique("policy")
    with pytest.raises(ClientError) as ei:
        iot_client.create_policy(policyName=name, policyDocument="not-json")
    assert ei.value.response["Error"]["Code"] == "MalformedPolicyException"


def test_iot_policy_versions(iot_client):
    name = _unique("policy")
    iot_client.create_policy(policyName=name, policyDocument=_POLICY_DOC)
    new_doc = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": "iot:Subscribe", "Resource": "*"}
            ],
        }
    )
    v2 = iot_client.create_policy_version(
        policyName=name, policyDocument=new_doc, setAsDefault=True
    )
    assert v2["policyVersionId"] == "2"

    versions = iot_client.list_policy_versions(policyName=name)["policyVersions"]
    assert {v["versionId"] for v in versions} == {"1", "2"}
    assert next(v for v in versions if v["versionId"] == "2")["isDefaultVersion"]

    iot_client.delete_policy_version(policyName=name, policyVersionId="1")
    iot_client.delete_policy(policyName=name)


def test_iot_attach_detach_policy(iot_client):
    pytest.importorskip("cryptography")
    name = _unique("policy")
    iot_client.create_policy(policyName=name, policyDocument=_POLICY_DOC)
    cert = iot_client.create_keys_and_certificate(setAsActive=False)
    arn = cert["certificateArn"]

    iot_client.attach_policy(policyName=name, target=arn)
    targets = iot_client.list_targets_for_policy(policyName=name)["targets"]
    assert arn in targets

    iot_client.detach_policy(policyName=name, target=arn)
    targets2 = iot_client.list_targets_for_policy(policyName=name)["targets"]
    assert arn not in targets2

    iot_client.delete_policy(policyName=name)
    iot_client.delete_certificate(certificateId=cert["certificateId"])


# ---------------------------------------------------------------------------
# Local CA admin endpoint
# ---------------------------------------------------------------------------


def test_iot_ca_pem_endpoint_returns_certificate():
    pytest.importorskip("cryptography")
    import os
    import urllib.request

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    with urllib.request.urlopen(f"{endpoint}/_ministack/iot/ca.pem", timeout=5) as resp:
        body = resp.read().decode("utf-8")
    assert "BEGIN CERTIFICATE" in body
    assert "END CERTIFICATE" in body


# ---------------------------------------------------------------------------
# Account isolation
# ---------------------------------------------------------------------------


def test_iot_account_isolation():
    """Two callers using different 12-digit access keys see different Things."""
    import os

    import boto3
    from botocore.config import Config

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

    def _client(account_id):
        return boto3.client(
            "iot",
            endpoint_url=endpoint,
            aws_access_key_id=account_id,
            aws_secret_access_key="test",
            region_name="us-east-1",
            config=Config(retries={"mode": "standard"}),
        )

    a = _client("111111111111")
    b = _client("222222222222")
    name = _unique("thing")
    a.create_thing(thingName=name)
    # Account B must not see Thing in account A.
    b_things = {t["thingName"] for t in b.list_things().get("things", [])}
    assert name not in b_things
    a.delete_thing(thingName=name)
