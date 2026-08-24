"""Integration tests for the IoT Core control plane (Phase 1a).

Exercises Things, ThingTypes, ThingGroups, Certificates (issued via the
Local CA), Policies, and DescribeEndpoint. The data plane (broker / WS /
iot-data Publish) is covered separately in ``test_iot_data.py``.
"""

import base64
import json
import logging
import time
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


def test_iot_create_thing_type_idempotent_recreate(iot_client):
    name = _unique("type")
    first = iot_client.create_thing_type(thingTypeName=name)

    # Identical re-create (no properties) returns 200 with the existing ids.
    again = iot_client.create_thing_type(thingTypeName=name)
    assert again["thingTypeId"] == first["thingTypeId"]
    assert again["thingTypeArn"] == first["thingTypeArn"]

    # Explicit empty properties count as identical to absent ones.
    empty = iot_client.create_thing_type(
        thingTypeName=name,
        thingTypeProperties={"searchableAttributes": []},
    )
    assert empty["thingTypeId"] == first["thingTypeId"]

    iot_client.deprecate_thing_type(thingTypeName=name)
    iot_client.delete_thing_type(thingTypeName=name)


def test_iot_create_thing_type_conflicting_recreate_rejected(iot_client):
    name = _unique("type")
    iot_client.create_thing_type(thingTypeName=name)
    with pytest.raises(ClientError) as ei:
        iot_client.create_thing_type(
            thingTypeName=name,
            thingTypeProperties={"thingTypeDescription": "different"},
        )
    assert ei.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
    iot_client.deprecate_thing_type(thingTypeName=name)
    iot_client.delete_thing_type(thingTypeName=name)


def test_iot_create_thing_type_idempotency_covers_all_properties(iot_client):
    """The comparison spans every modelled property and treats
    searchableAttributes as unordered: a re-create differing only in
    mqtt5Configuration conflicts, while the same attributes in another order
    are idempotent."""
    name = _unique("type")
    mqtt5 = {"propagatingAttributes": [
        {"userPropertyKey": "region", "thingAttribute": "site"},
    ]}
    first = iot_client.create_thing_type(
        thingTypeName=name,
        thingTypeProperties={
            "searchableAttributes": ["site", "region"],
            "mqtt5Configuration": mqtt5,
        },
    )

    same = iot_client.create_thing_type(
        thingTypeName=name,
        thingTypeProperties={
            "searchableAttributes": ["region", "site"],
            "mqtt5Configuration": mqtt5,
        },
    )
    assert same["thingTypeId"] == first["thingTypeId"]

    with pytest.raises(ClientError) as ei:
        iot_client.create_thing_type(
            thingTypeName=name,
            thingTypeProperties={
                "searchableAttributes": ["site", "region"],
                "mqtt5Configuration": {"propagatingAttributes": [
                    {"userPropertyKey": "region", "thingAttribute": "other"},
                ]},
            },
        )
    assert ei.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"

    # The first create's configuration is the one that stuck.
    described = iot_client.describe_thing_type(thingTypeName=name)
    assert described["thingTypeProperties"]["mqtt5Configuration"] == mqtt5

    iot_client.deprecate_thing_type(thingTypeName=name)
    iot_client.delete_thing_type(thingTypeName=name)


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
    # 406 per the service model — the same status the CA-certificate delete
    # answers with for the same exception.
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 406
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


def test_iot_register_certificate_set_as_active_query_param(iot_client):
    pytest.importorskip("cryptography")
    # boto3 sends setAsActive as a querystring parameter (per the botocore
    # model), not in the JSON body — the cert must still come out ACTIVE.
    issued = iot_client.create_keys_and_certificate(setAsActive=False)
    cert_pem = issued["certificatePem"]
    iot_client.delete_certificate(certificateId=issued["certificateId"])

    cert_id = iot_client.register_certificate(
        certificatePem=cert_pem, setAsActive=True
    )["certificateId"]
    desc = iot_client.describe_certificate(certificateId=cert_id)
    assert desc["certificateDescription"]["status"] == "ACTIVE"
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


def test_iot_thing_arn_tail_parser_requires_iot_thing_scope():
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import iot as _iot

    original_account = get_account_id()
    original_region = get_region()
    try:
        set_request_account_id("000000000000")
        set_request_region("us-east-1")
        assert _iot._thing_name_from_arn(
            "arn:aws:iot:us-east-1:000000000000:thing/parser-thing"
        ) == "parser-thing"
        assert _iot._thing_name_from_arn(
            "arn:aws:sqs:us-east-1:000000000000:thing/parser-thing"
        ) == ""
        assert _iot._thing_name_from_arn(
            "arn:aws:iot:us-west-2:000000000000:thing/parser-thing"
        ) == ""
        assert _iot._thing_name_from_arn(
            "arn:aws:iot:us-east-1:000000000000:thing/parser-thing/extra"
        ) == ""
    finally:
        set_request_account_id(original_account)
        set_request_region(original_region)


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
# Certificates: register-no-CA + legacy principal-policy family
# ---------------------------------------------------------------------------


def test_iot_register_certificate_without_ca_roundtrip(iot_client):
    pytest.importorskip("cryptography")
    # Issue a cert, capture its PEM, delete it, then re-register the SAME PEM
    # through the no-CA variant.
    issued = iot_client.create_keys_and_certificate(setAsActive=False)
    cert_pem = issued["certificatePem"]
    iot_client.delete_certificate(certificateId=issued["certificateId"])

    resp = iot_client.register_certificate_without_ca(
        certificatePem=cert_pem, status="ACTIVE"
    )
    cert_id = resp["certificateId"]
    assert resp["certificateArn"].endswith(":cert/" + cert_id)
    # The no-CA output carries ids only — no certificatePem echo.
    assert "certificatePem" not in resp

    desc = iot_client.describe_certificate(certificateId=cert_id)
    assert desc["certificateDescription"]["certificatePem"] == cert_pem
    assert desc["certificateDescription"]["status"] == "ACTIVE"

    iot_client.update_certificate(certificateId=cert_id, newStatus="INACTIVE")
    iot_client.delete_certificate(certificateId=cert_id)


def test_iot_register_certificate_without_ca_duplicate_conflict(iot_client):
    pytest.importorskip("cryptography")
    issued = iot_client.create_keys_and_certificate(setAsActive=False)
    cert_pem = issued["certificatePem"]
    iot_client.delete_certificate(certificateId=issued["certificateId"])

    cert_id = iot_client.register_certificate_without_ca(
        certificatePem=cert_pem, status="INACTIVE"
    )["certificateId"]
    with pytest.raises(ClientError) as ei:
        iot_client.register_certificate_without_ca(
            certificatePem=cert_pem, status="INACTIVE"
        )
    err = ei.value.response
    assert err["Error"]["Code"] == "ResourceAlreadyExistsException"
    assert err["resourceId"] == cert_id
    assert err["resourceArn"].endswith(":cert/" + cert_id)
    iot_client.delete_certificate(certificateId=cert_id)


def test_iot_legacy_principal_policy_family(iot_client):
    pytest.importorskip("cryptography")
    policy = _unique("policy")
    iot_client.create_policy(policyName=policy, policyDocument=_POLICY_DOC)
    cert = iot_client.create_keys_and_certificate(setAsActive=False)
    principal = cert["certificateArn"]

    iot_client.attach_principal_policy(policyName=policy, principal=principal)

    legacy = iot_client.list_principal_policies(principal=principal)["policies"]
    assert any(p["policyName"] == policy for p in legacy)
    # 1:1 with the modern target-scoped view of the same attachment.
    modern = iot_client.list_attached_policies(target=principal)["policies"]
    assert legacy == modern
    principals = iot_client.list_policy_principals(policyName=policy)["principals"]
    assert principals == [principal]

    iot_client.detach_principal_policy(policyName=policy, principal=principal)
    assert iot_client.list_principal_policies(principal=principal)["policies"] == []
    assert iot_client.list_policy_principals(policyName=policy)["principals"] == []
    assert iot_client.list_attached_policies(target=principal)["policies"] == []

    iot_client.delete_policy(policyName=policy)
    iot_client.delete_certificate(certificateId=cert["certificateId"])


def test_iot_legacy_principal_policy_unknown_policy_404(iot_client):
    with pytest.raises(ClientError) as ei:
        iot_client.attach_principal_policy(
            policyName=_unique("nope"), principal="arn:aws:iot:cert/none"
        )
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    
    
# ---------------------------------------------------------------------------
# Fleet indexing (indexing configuration + SearchIndex)
# ---------------------------------------------------------------------------


def _enable_fleet_indexing(
    client, mode: str = "REGISTRY_AND_SHADOW", connectivity: str = "STATUS"
) -> None:
    """Turn the AWS_Things index on, as every fleet stack has to on AWS."""
    thing_cfg = {"thingIndexingMode": mode}
    if mode != "OFF":
        thing_cfg["thingConnectivityIndexingMode"] = connectivity
    client.update_indexing_configuration(thingIndexingConfiguration=thing_cfg)


def _iot_client_for_fresh_account():
    """An IoT client on an account of its own, so its index starts OFF."""
    import os

    import boto3
    from botocore.config import Config

    return boto3.client(
        "iot",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        aws_access_key_id=f"{uuid.uuid4().int % 10**12:012d}",
        aws_secret_access_key="test",
        region_name="us-east-1",
        config=Config(retries={"mode": "standard"}),
    )


def test_iot_indexing_configuration_round_trip():
    client = _iot_client_for_fresh_account()
    # AWS's default for an account that never configured indexing.
    default = client.get_indexing_configuration()
    assert default["thingIndexingConfiguration"]["thingIndexingMode"] == "OFF"
    assert client.list_indices()["indexNames"] == []
    with pytest.raises(ClientError) as ei:
        client.describe_index(indexName="AWS_Things")
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"

    client.update_indexing_configuration(
        thingIndexingConfiguration={"thingIndexingMode": "REGISTRY_AND_SHADOW"},
        thingGroupIndexingConfiguration={"thingGroupIndexingMode": "ON"},
    )
    stored = client.get_indexing_configuration()
    assert stored["thingIndexingConfiguration"]["thingIndexingMode"] == (
        "REGISTRY_AND_SHADOW"
    )
    assert stored["thingGroupIndexingConfiguration"]["thingGroupIndexingMode"] == "ON"
    assert client.list_indices()["indexNames"] == ["AWS_Things"]
    index = client.describe_index(indexName="AWS_Things")
    # The registry *is* the index, so it is never BUILDING.
    assert index["indexStatus"] == "ACTIVE"
    assert index["schema"] == "REGISTRY_AND_SHADOW"

    # Registry-only indexing narrows the schema back.
    _enable_fleet_indexing(client, "REGISTRY", connectivity="OFF")
    assert client.describe_index(indexName="AWS_Things")["schema"] == "REGISTRY"

    # Connectivity status widens it again, the way AWS's schema strings do.
    _enable_fleet_indexing(client, "REGISTRY")
    assert (
        client.describe_index(indexName="AWS_Things")["schema"]
        == "REGISTRY_AND_CONNECTIVITY_STATUS"
    )

    # ...and turning it off retires the index.
    _enable_fleet_indexing(client, "OFF")
    assert client.list_indices()["indexNames"] == []


def test_iot_indexing_configuration_rejects_impossible_modes():
    client = _iot_client_for_fresh_account()
    for kwargs in (
        {"thingIndexingConfiguration": {"thingIndexingMode": "REGISTRY_AND_LOGS"}},
        # AWS: connectivity indexing requires thing indexing to be on.
        {
            "thingIndexingConfiguration": {
                "thingIndexingMode": "OFF",
                "thingConnectivityIndexingMode": "STATUS",
            }
        },
        {"thingGroupIndexingConfiguration": {"thingGroupIndexingMode": "SOMETIMES"}},
    ):
        with pytest.raises(ClientError) as ei:
            client.update_indexing_configuration(**kwargs)
        assert ei.value.response["Error"]["Code"] == "InvalidRequestException", kwargs
    # A rejected update leaves the configuration untouched.
    assert client.get_indexing_configuration()["thingIndexingConfiguration"][
        "thingIndexingMode"
    ] == "OFF"


def test_iot_search_index_requires_indexing_enabled():
    """Searching an account that never enabled indexing is a 404 on AWS."""
    client = _iot_client_for_fresh_account()
    name = _unique("unindexed")
    client.create_thing(thingName=name)
    try:
        with pytest.raises(ClientError) as ei:
            client.search_index(queryString=f"thingName:{name}")
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"

        _enable_fleet_indexing(client)
        assert [
            t["thingName"]
            for t in client.search_index(queryString=f"thingName:{name}")["things"]
        ] == [name]

        # An index this emulator does not have is the same 404.
        with pytest.raises(ClientError) as ei:
            client.search_index(indexName="AWS_Fleet", queryString="thingName:*")
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        client.delete_thing(thingName=name)


def test_iot_search_index_shadow_terms_need_shadow_indexing():
    """REGISTRY-only indexing does not project shadows, so shadow terms 400."""
    client = _iot_client_for_fresh_account()
    _enable_fleet_indexing(client, "REGISTRY")
    with pytest.raises(ClientError) as ei:
        client.search_index(queryString="shadow.reported.firmware:fw-1")
    assert ei.value.response["Error"]["Code"] == "InvalidQueryException"
    # Registry fields keep working in the narrower mode.
    client.search_index(queryString="thingName:whatever")


def test_iot_search_index_connectivity_needs_connectivity_indexing():
    """The connectivity group appears only under THING_CONNECTIVITY_INDEXING.

    That is the switch AWS puts it behind, so a fleet dashboard that forgets
    it has to find out here rather than in the cloud.
    """
    client = _iot_client_for_fresh_account()
    _enable_fleet_indexing(client, connectivity="OFF")
    name = _unique("connoff")
    client.create_thing(thingName=name)
    try:
        hit = client.search_index(queryString=f"thingName:{name}")["things"][0]
        assert "connectivity" not in hit
        with pytest.raises(ClientError) as ei:
            client.search_index(queryString="connectivity.connected:true")
        assert ei.value.response["Error"]["Code"] == "InvalidQueryException"

        _enable_fleet_indexing(client)
        hit = client.search_index(queryString=f"thingName:{name}")["things"][0]
        assert hit["connectivity"] == {"connected": False}
        assert [
            t["thingName"]
            for t in client.search_index(
                queryString=f"thingName:{name} AND connectivity.connected:false"
            )["things"]
        ] == [name]
    finally:
        client.delete_thing(thingName=name)


def test_iot_search_index_registry_and_shadow(iot_client, iot_data_client):
    _enable_fleet_indexing(iot_client)
    suffix = uuid.uuid4().hex[:8]
    type_name = f"searchtype-{suffix}"
    hit = f"searchthing-{suffix}-hit"
    miss = f"searchthing-{suffix}-miss"
    iot_client.create_thing_type(thingTypeName=type_name)
    iot_client.create_thing(
        thingName=hit,
        thingTypeName=type_name,
        attributePayload={"attributes": {"fleet": f"fleet-{suffix}"}},
    )
    iot_client.create_thing(
        thingName=miss, attributePayload={"attributes": {"fleet": "other"}}
    )
    iot_data_client.update_thing_shadow(
        thingName=hit,
        payload=json.dumps(
            {"state": {"reported": {"firmware": f"fw-{suffix}"}}}
        ).encode(),
    )
    try:
        by_name = iot_client.search_index(queryString=f"thingName:{hit}")["things"]
        assert [t["thingName"] for t in by_name] == [hit]
        doc = by_name[0]
        assert doc["thingId"]
        assert doc["thingTypeName"] == type_name
        assert doc["attributes"] == {"fleet": f"fleet-{suffix}"}
        shadow = json.loads(doc["shadow"])  # AWS returns a JSON *string*
        assert shadow["reported"]["firmware"] == f"fw-{suffix}"

        by_type = iot_client.search_index(queryString=f"thingTypeName:{type_name}")[
            "things"
        ]
        assert [t["thingName"] for t in by_type] == [hit]

        by_attr = iot_client.search_index(
            queryString=f"attributes.fleet:fleet-{suffix}"
        )["things"]
        assert [t["thingName"] for t in by_attr] == [hit]

        by_shadow = iot_client.search_index(
            queryString=(
                f"shadow.reported.firmware:fw-{suffix} AND thingTypeName:{type_name}"
            ),
            maxResults=10,
        )["things"]
        assert [t["thingName"] for t in by_shadow] == [hit]

        empty = iot_client.search_index(queryString=f"thingName:absent-{suffix}")
        assert empty["things"] == []
    finally:
        iot_data_client.delete_thing_shadow(thingName=hit)
        iot_client.delete_thing(thingName=hit)
        iot_client.delete_thing(thingName=miss)
        iot_client.deprecate_thing_type(thingTypeName=type_name)
        iot_client.delete_thing_type(thingTypeName=type_name)


def test_iot_search_index_wildcards(iot_client):
    _enable_fleet_indexing(iot_client)
    # The single most common fleet query: every thing under a name prefix.
    suffix = uuid.uuid4().hex[:8]
    fleet = [f"fleet-{suffix}-a", f"fleet-{suffix}-b"]
    outside = f"other-{suffix}"
    for name in (*fleet, outside):
        iot_client.create_thing(
            thingName=name, attributePayload={"attributes": {"site": f"berlin-{suffix}"}}
        )
    try:
        prefix = iot_client.search_index(queryString=f"thingName:fleet-{suffix}-*")
        assert sorted(t["thingName"] for t in prefix["things"]) == fleet

        suffix_hits = iot_client.search_index(queryString=f"thingName:*-{suffix}-a")
        assert [t["thingName"] for t in suffix_hits["things"]] == [fleet[0]]

        contains = iot_client.search_index(queryString=f"thingName:*{suffix}*")
        assert sorted(t["thingName"] for t in contains["things"]) == sorted(
            [*fleet, outside]
        )

        # ? matches exactly one character.
        single = iot_client.search_index(queryString=f"thingName:fleet-{suffix}-?")
        assert sorted(t["thingName"] for t in single["things"]) == fleet
        assert (
            iot_client.search_index(queryString=f"thingName:fleet-{suffix}-??")["things"]
            == []
        )

        # Wildcards work on attributes too, and combine with AND.
        combined = iot_client.search_index(
            queryString=f"thingName:fleet-{suffix}-* AND attributes.site:berlin-*"
        )
        assert sorted(t["thingName"] for t in combined["things"]) == fleet
    finally:
        for name in (*fleet, outside):
            iot_client.delete_thing(thingName=name)


def test_iot_search_index_returns_desired_and_reported_shadow(
    iot_client, iot_data_client
):
    _enable_fleet_indexing(iot_client)
    # shadow.desired.<path> is queryable, so the returned document has to
    # carry the desired half too — not reported only.
    suffix = uuid.uuid4().hex[:8]
    name = f"searchthing-{suffix}-desired"
    iot_client.create_thing(thingName=name)
    iot_data_client.update_thing_shadow(
        thingName=name,
        payload=json.dumps(
            {
                "state": {
                    "desired": {"firmware": f"want-{suffix}"},
                    "reported": {"firmware": f"have-{suffix}"},
                }
            }
        ).encode(),
    )
    try:
        hits = iot_client.search_index(
            queryString=f"shadow.desired.firmware:want-{suffix}"
        )["things"]
        assert [t["thingName"] for t in hits] == [name]
        shadow = json.loads(hits[0]["shadow"])
        assert shadow["desired"]["firmware"] == f"want-{suffix}"
        assert shadow["reported"]["firmware"] == f"have-{suffix}"
    finally:
        iot_data_client.delete_thing_shadow(thingName=name)
        iot_client.delete_thing(thingName=name)


def test_iot_search_index_unsupported_query_rejected(iot_client):
    _enable_fleet_indexing(iot_client)
    for query in (
        "connectivity.online:true",  # the group is closed; this leaf is a typo
        "NOT thingName:foo",
        "thingName:foo AND",  # a dangling AND is an error on AWS, not a no-op
        "AND thingName:foo",
        "thingName:foo AND AND thingTypeName:bar",
        "thingName:foo OR thingTypeName:bar",
    ):
        with pytest.raises(ClientError) as ei:
            iot_client.search_index(queryString=query)
        assert ei.value.response["Error"]["Code"] == "InvalidQueryException", query


def test_iot_search_index_missing_field_never_matches(iot_client):
    _enable_fleet_indexing(iot_client)
    # An untyped thing has no thingTypeName; str(None) == "none" must NOT
    # make it match thingTypeName:none, and a bare * must not resurrect it.
    name = f"searchthing-{uuid.uuid4().hex[:8]}-untyped"
    iot_client.create_thing(thingName=name)
    try:
        for query in ("thingTypeName:none", "thingTypeName:*"):
            hits = iot_client.search_index(queryString=query)["things"]
            assert name not in [t["thingName"] for t in hits], query
    finally:
        iot_client.delete_thing(thingName=name)


def test_iot_search_index_unknown_shadow_subfield_rejected(iot_client):
    """A shadow field outside desired/reported is a typo, not an empty result."""
    _enable_fleet_indexing(iot_client)
    for query in (
        "shadow.metadata.reported.firmware:1",  # AWS field, not projected here
        "shadow.name.telemetry.reported.x:1",  # named shadows are not indexed
        "shadow.reported:anything",  # a whole half is not a leaf
        "shadow.version:2",
    ):
        with pytest.raises(ClientError) as ei:
            iot_client.search_index(queryString=query)
        assert ei.value.response["Error"]["Code"] == "InvalidQueryException", query


def test_iot_search_index_thing_group_terms(iot_client):
    """`thingGroupNames:<group>` is the membership query a fleet console runs."""
    _enable_fleet_indexing(iot_client)
    suffix = uuid.uuid4().hex[:8]
    groups = [f"group-{suffix}-eu", f"group-{suffix}-canary"]
    member = f"grouped-{suffix}"
    outsider = f"ungrouped-{suffix}"
    for group in groups:
        iot_client.create_thing_group(thingGroupName=group)
    iot_client.create_thing(thingName=member)
    iot_client.create_thing(thingName=outsider)
    for group in groups:
        iot_client.add_thing_to_thing_group(thingGroupName=group, thingName=member)
    try:
        for group in groups:
            hits = iot_client.search_index(queryString=f"thingGroupNames:{group}")[
                "things"
            ]
            assert [t["thingName"] for t in hits] == [member], group
            assert sorted(hits[0]["thingGroupNames"]) == sorted(groups)

        # Both memberships are visible at once, and wildcards work on them.
        both = iot_client.search_index(
            queryString=f"thingGroupNames:{groups[0]} AND thingGroupNames:{groups[1]}"
        )["things"]
        assert [t["thingName"] for t in both] == [member]
        assert [
            t["thingName"]
            for t in iot_client.search_index(
                queryString=f"thingGroupNames:group-{suffix}-*"
            )["things"]
        ] == [member]

        # A thing in no group carries the field as an empty list, and a group
        # it does not belong to never matches it.
        hit = iot_client.search_index(queryString=f"thingName:{outsider}")["things"][0]
        assert hit["thingGroupNames"] == []
    finally:
        iot_client.delete_thing(thingName=member)
        iot_client.delete_thing(thingName=outsider)
        for group in groups:
            iot_client.delete_thing_group(thingGroupName=group)


def test_iot_search_index_paginates(iot_client):
    _enable_fleet_indexing(iot_client)
    suffix = uuid.uuid4().hex[:8]
    names = sorted(f"paged-{suffix}-{i}" for i in range(3))
    for name in names:
        iot_client.create_thing(thingName=name)
    try:
        query = f"thingName:paged-{suffix}-*"
        seen = []
        token = None
        for _ in range(len(names) + 1):
            kwargs = {"nextToken": token} if token else {}
            page = iot_client.search_index(queryString=query, maxResults=1, **kwargs)
            seen.extend(t["thingName"] for t in page["things"])
            token = page.get("nextToken")
            if token is None:
                break
        assert token is None, "pagination did not terminate"
        assert sorted(seen) == names

        # The last page of an exact fit carries no token.
        full = iot_client.search_index(queryString=query, maxResults=len(names))
        assert "nextToken" not in full
        assert len(full["things"]) == len(names)
    finally:
        for name in names:
            iot_client.delete_thing(thingName=name)


def test_iot_search_index_rejects_bad_paging_arguments(iot_client):
    _enable_fleet_indexing(iot_client)
    # (maxResults=0 never reaches the server: botocore enforces the model's
    # min=1 client-side. The ceiling is not in the model, so it is ours.)
    for kwargs in (
        {"maxResults": 101},  # AWS's documented ceiling
        {"nextToken": "not-a-real-cursor"},
    ):
        with pytest.raises(ClientError) as ei:
            iot_client.search_index(queryString="thingName:*", **kwargs)
        assert ei.value.response["Error"]["Code"] == "InvalidRequestException", kwargs


def test_iot_search_index_numeric_shadow_values(iot_client, iot_data_client):
    """A JSON number in a shadow compares as a number, not as its repr."""
    _enable_fleet_indexing(iot_client)
    suffix = uuid.uuid4().hex[:8]
    name = f"numeric-{suffix}"
    iot_client.create_thing(thingName=name)
    iot_data_client.update_thing_shadow(
        thingName=name,
        payload=json.dumps(
            {"state": {"reported": {"temp": 10.0, "errors": 0}}}
        ).encode(),
    )
    try:
        for query in (
            f"thingName:{name} AND shadow.reported.temp:10",
            f"thingName:{name} AND shadow.reported.temp:10.0",
            f"thingName:{name} AND shadow.reported.errors:0",
        ):
            hits = iot_client.search_index(queryString=query)["things"]
            assert [t["thingName"] for t in hits] == [name], query
        for query in (
            f"thingName:{name} AND shadow.reported.temp:11",
            f"thingName:{name} AND shadow.reported.temp:1",
        ):
            assert iot_client.search_index(queryString=query)["things"] == [], query

        # A registry string stays a string: 007 is not 7.
        zeros = f"0007{suffix}"
        iot_client.create_thing(thingName=zeros)
        try:
            assert (
                iot_client.search_index(queryString=f"thingName:7{suffix}")["things"]
                == []
            )
        finally:
            iot_client.delete_thing(thingName=zeros)
    finally:
        iot_data_client.delete_thing_shadow(thingName=name)
        iot_client.delete_thing(thingName=name)


def test_iot_search_index_thing_that_never_connected_is_disconnected(iot_client):
    _enable_fleet_indexing(iot_client)
    name = f"searchthing-{uuid.uuid4().hex[:8]}-offline"
    iot_client.create_thing(thingName=name)
    try:
        doc = iot_client.search_index(queryString=f"thingName:{name}")["things"][0]
        assert doc["connectivity"] == {"connected": False}

        found = iot_client.search_index(
            queryString=f"thingName:{name} AND connectivity.connected:false"
        )["things"]
        assert [t["thingName"] for t in found] == [name]

        # It has never disconnected either, so it matches no reason at all.
        assert (
            iot_client.search_index(
                queryString=f"thingName:{name} AND connectivity.disconnectReason:*"
            )["things"]
            == []
        )
    finally:
        iot_client.delete_thing(thingName=name)


# ---------------------------------------------------------------------------
# Topic rules
# ---------------------------------------------------------------------------


def _rule_name(prefix: str = "rule") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def test_iot_topic_rule_lifecycle(iot_client):
    name = _rule_name()
    payload = {
        "sql": "SELECT * FROM 'devices/+/data'",
        "ruleDisabled": False,
        "awsIotSqlVersion": "2016-03-23",
        "actions": [
            {"lambda": {"functionArn": "arn:aws:lambda:us-east-1:000000000000:function:foo"}}
        ],
    }
    iot_client.create_topic_rule(ruleName=name, topicRulePayload=payload)

    got = iot_client.get_topic_rule(ruleName=name)
    assert got["ruleArn"] == f"arn:aws:iot:us-east-1:000000000000:rule/{name}"
    assert got["rule"]["sql"] == payload["sql"]
    assert got["rule"]["actions"] == payload["actions"]
    assert got["rule"]["ruleDisabled"] is False

    listing = iot_client.list_topic_rules()["rules"]
    entry = next(r for r in listing if r["ruleName"] == name)
    assert entry["topicPattern"] == "devices/+/data"

    iot_client.delete_topic_rule(ruleName=name)
    with pytest.raises(ClientError) as ei:
        iot_client.get_topic_rule(ruleName=name)
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_iot_topic_rule_duplicate_rejected(iot_client):
    name = _rule_name()
    payload = {"sql": "SELECT * FROM 'a'", "actions": []}
    iot_client.create_topic_rule(ruleName=name, topicRulePayload=payload)
    with pytest.raises(ClientError) as ei:
        iot_client.create_topic_rule(ruleName=name, topicRulePayload=payload)
    assert ei.value.response["Error"]["Code"] == "ResourceAlreadyExistsException"
    iot_client.delete_topic_rule(ruleName=name)


def test_iot_replace_topic_rule(iot_client):
    name = _rule_name()
    iot_client.create_topic_rule(
        ruleName=name, topicRulePayload={"sql": "SELECT * FROM 'a'", "actions": []}
    )
    iot_client.replace_topic_rule(
        ruleName=name, topicRulePayload={"sql": "SELECT * FROM 'b'", "actions": []}
    )
    assert iot_client.get_topic_rule(ruleName=name)["rule"]["sql"] == "SELECT * FROM 'b'"
    iot_client.delete_topic_rule(ruleName=name)


def test_iot_create_topic_rule_garbage_sql_rejected(iot_client):
    with pytest.raises(ClientError) as ei:
        iot_client.create_topic_rule(
            ruleName=_rule_name(),
            topicRulePayload={"sql": "this is not sql", "actions": []},
        )
    assert ei.value.response["Error"]["Code"] == "SqlParseException"


def test_iot_create_topic_rule_unsupported_where_rejected(iot_client):
    """A WHERE the evaluator cannot parse is rejected at creation instead of
    being stored and silently never firing.

    Malformed SQL only: syntax AWS accepts has to be accepted here too, or the
    rule that deploys on AWS fails to create locally.
    """
    with pytest.raises(ClientError) as ei:
        iot_client.create_topic_rule(
            ruleName=_rule_name(),
            topicRulePayload={
                "sql": "SELECT * FROM 'a' WHERE (state = 'on'",
                "actions": [],
            },
        )
    assert ei.value.response["Error"]["Code"] == "SqlParseException"


def test_iot_topic_rule_role_validated_under_auth(monkeypatch):
    """Under AUTH=true a rule naming an action role IAM cannot resolve is
    refused 400 `InvalidRequestException` ("Unable to assume role: ..."), as
    real IoT does by probing the role at create time — and the rule must NOT
    be stored. In-process because the shared test server runs AUTH=false:
    `validate_role_arn` reads `ministack.app.AUTH` at call time, so the
    monkeypatch reaches it."""
    import json as _json

    import ministack.app as _app
    from ministack.services import iam as _iam
    from ministack.services import iot as _iot

    monkeypatch.setattr(_app, "AUTH", True)
    name = _rule_name()
    bad = {
        "sql": "SELECT * FROM 'a'",
        "actions": [{"sqs": {
            "queueUrl": "http://localhost/000000000000/q",
            "roleArn": "arn:aws:iam::000000000000:role/absent-role",
        }}],
    }
    try:
        # Create door: 400, nothing stored.
        status, _, body = _iot._create_topic_rule(name, bad)
        assert status == 400
        err = _json.loads(body)
        assert err["__type"] == "InvalidRequestException"
        assert "Unable to assume role" in err["message"]
        assert name not in _iot._topic_rules

        # An existing role passes the same check.
        _iam._roles.setdefault("probe-rule-role", {"RoleName": "probe-rule-role"})
        good = {
            "sql": "SELECT * FROM 'a'",
            "actions": [{"sqs": {
                "queueUrl": "http://localhost/000000000000/q",
                "roleArn": "arn:aws:iam::000000000000:role/probe-rule-role",
            }}],
        }
        status, _, _b = _iot._create_topic_rule(name, good)
        assert status == 200
        assert name in _iot._topic_rules

        # Replace door: same 400, the stored rule stays what it was.
        status, _, body = _iot._replace_topic_rule(name, bad)
        assert status == 400
        assert _json.loads(body)["__type"] == "InvalidRequestException"
        assert (_iot._topic_rules[name]["actions"][0]["sqs"]["roleArn"]
                == good["actions"][0]["sqs"]["roleArn"])

        # CFN door: the store-layer helper raises, so the provisioner (which
        # calls put_topic_rule directly) fails the resource instead of
        # green-lighting a stack minus its rule.
        with pytest.raises(_iot.RuleRoleError):
            _iot.put_topic_rule(name + "cfn", bad)
        assert (name + "cfn") not in _iot._topic_rules
    finally:
        _iot._topic_rules.pop(name, None)
        _iam._roles.pop("probe-rule-role", None)


def test_iot_topic_rule_role_not_validated_without_auth(monkeypatch):
    """With AUTH=false (the default) an unresolvable role is stored as before —
    the check is authorization-mode fidelity, not a new default gate."""
    import ministack.app as _app
    from ministack.services import iot as _iot

    monkeypatch.setattr(_app, "AUTH", False)
    name = _rule_name()
    payload = {
        "sql": "SELECT * FROM 'a'",
        "actions": [{"sqs": {
            "queueUrl": "http://localhost/000000000000/q",
            "roleArn": "arn:aws:iam::000000000000:role/absent-role",
        }}],
    }
    try:
        status, _, _b = _iot._create_topic_rule(name, payload)
        assert status == 200
        assert name in _iot._topic_rules
    finally:
        _iot._topic_rules.pop(name, None)


def test_iot_create_topic_rule_accepts_or_and_parentheses(iot_client):
    """`OR` and parenthesised groups are valid AWS rule SQL — rejecting them
    would fail rules that deploy fine on AWS."""
    name = _rule_name()
    sql = "SELECT * FROM 'a' WHERE (state = 'on' OR state = 'standby') AND temp > 20"
    iot_client.create_topic_rule(
        ruleName=name, topicRulePayload={"sql": sql, "actions": []}
    )
    assert iot_client.get_topic_rule(ruleName=name)["rule"]["sql"] == sql
    iot_client.delete_topic_rule(ruleName=name)


def test_iot_create_topic_rule_accepts_full_where_grammar(iot_client):
    """The rest of the WHERE syntax AWS accepts — over-strict validation here
    turns a working setup into a hard 400 (or a failed CloudFormation stack)."""
    name = _rule_name()
    sql = (
        "SELECT * FROM 'a' WHERE state IN ('on', 'idle') AND temp BETWEEN 1 AND 5 "
        "AND serial LIKE 'dev-%' AND nickname IS NOT NULL AND enabled "
        "AND isUndefined(site) AND note <> 'it''s off'"
    )
    iot_client.create_topic_rule(
        ruleName=name, topicRulePayload={"sql": sql, "actions": []}
    )
    assert iot_client.get_topic_rule(ruleName=name)["rule"]["sql"] == sql
    iot_client.delete_topic_rule(ruleName=name)


def test_iot_replace_topic_rule_garbage_sql_rejected(iot_client):
    name = _rule_name()
    iot_client.create_topic_rule(
        ruleName=name, topicRulePayload={"sql": "SELECT * FROM 'a'", "actions": []}
    )
    with pytest.raises(ClientError) as ei:
        iot_client.replace_topic_rule(
            ruleName=name, topicRulePayload={"sql": "garbage", "actions": []}
        )
    assert ei.value.response["Error"]["Code"] == "SqlParseException"
    # The stored rule is untouched.
    assert iot_client.get_topic_rule(ruleName=name)["rule"]["sql"] == "SELECT * FROM 'a'"
    iot_client.delete_topic_rule(ruleName=name)


def test_iot_replace_missing_topic_rule_404(iot_client):
    with pytest.raises(ClientError) as ei:
        iot_client.replace_topic_rule(
            ruleName=_rule_name(), topicRulePayload={"sql": "SELECT * FROM 'a'", "actions": []}
        )
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_iot_topic_rule_cfn_deploy(cfn, iot_client):
    stack = "iot-topicrule-" + uuid.uuid4().hex[:8]
    rule = _rule_name("ingest")
    template = {
        "Resources": {
            "IngestRule": {
                "Type": "AWS::IoT::TopicRule",
                "Properties": {
                    "RuleName": rule,
                    "TopicRulePayload": {
                        "Sql": "SELECT * FROM 'sensors/+/telemetry'",
                        "RuleDisabled": False,
                        "AwsIotSqlVersion": "2016-03-23",
                        "Actions": [
                            {"Lambda": {"FunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:ingest"}}
                        ],
                    },
                },
            }
        },
        "Outputs": {
            "RuleArn": {"Value": {"Fn::GetAtt": ["IngestRule", "Arn"]}},
            "RuleRef": {"Value": {"Ref": "IngestRule"}},
        },
    }
    cfn.create_stack(StackName=stack, TemplateBody=json.dumps(template))
    st = _wait_stack_iot(cfn, stack)
    assert st["StackStatus"] == "CREATE_COMPLETE"
    outputs = {o["OutputKey"]: o["OutputValue"] for o in st["Outputs"]}
    assert outputs["RuleRef"] == rule
    assert outputs["RuleArn"] == f"arn:aws:iot:us-east-1:000000000000:rule/{rule}"

    # PascalCase TopicRulePayload is normalized to the API camelCase shape.
    stored = iot_client.get_topic_rule(ruleName=rule)["rule"]
    assert stored["actions"] == [
        {"lambda": {"functionArn": "arn:aws:lambda:us-east-1:000000000000:function:ingest"}}
    ]

    cfn.delete_stack(StackName=stack)
    _wait_stack_gone_iot(cfn, stack)
    with pytest.raises(ClientError):
        iot_client.get_topic_rule(ruleName=rule)


def test_iot_topic_rule_cfn_invalid_sql_fails_resource(cfn, iot_client):
    """CloudFormation validates rule SQL too: a rule the engine cannot evaluate
    must fail the resource, not reach CREATE_COMPLETE and silently never fire
    (real CloudFormation fails on the underlying CreateTopicRule call).

    No provisioner change is needed for that — `RuleSqlError` is a `ValueError`,
    which is already what the stack runner turns into a CREATE_FAILED carrying
    the reason.
    """
    stack = "iot-badsql-" + uuid.uuid4().hex[:8]
    rule = _rule_name("badsql")
    template = {
        "Resources": {
            "BadRule": {
                "Type": "AWS::IoT::TopicRule",
                "Properties": {
                    "RuleName": rule,
                    "TopicRulePayload": {
                        "Sql": "this is not sql",
                        "Actions": [],
                    },
                },
            }
        },
    }
    try:
        cfn.create_stack(
            StackName=stack, TemplateBody=json.dumps(template), DisableRollback=True
        )
        st = _wait_stack_iot(cfn, stack)
        assert st["StackStatus"] == "CREATE_FAILED"
        reason = st.get("StackStatusReason", "")
        assert "BadRule" in reason and "Rule SQL" in reason

        events = cfn.describe_stack_events(StackName=stack)["StackEvents"]
        assert any(
            event["LogicalResourceId"] == "BadRule"
            and event["ResourceStatus"] == "CREATE_FAILED"
            for event in events
        )
        # Nothing was stored: the invalid rule never entered the rule store.
        with pytest.raises(ClientError) as ei:
            iot_client.get_topic_rule(ruleName=rule)
        assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"
    finally:
        try:
            cfn.delete_stack(StackName=stack)
            _wait_stack_gone_iot(cfn, stack)
        except ClientError:
            pass


def test_iot_topic_rule_cfn_where_with_or_deploys(cfn, iot_client):
    """A WHERE using OR and parentheses deploys through CloudFormation — the
    grammar the validator accepts is the same on both paths."""
    stack = "iot-orsql-" + uuid.uuid4().hex[:8]
    rule = _rule_name("orsql")
    sql = "SELECT * FROM 'sensors/+/telemetry' WHERE (temp > 30 OR humidity > 80) AND state = 'on'"
    template = {
        "Resources": {
            "OrRule": {
                "Type": "AWS::IoT::TopicRule",
                "Properties": {
                    "RuleName": rule,
                    "TopicRulePayload": {"Sql": sql, "Actions": []},
                },
            }
        },
    }
    cfn.create_stack(StackName=stack, TemplateBody=json.dumps(template))
    st = _wait_stack_iot(cfn, stack)
    assert st["StackStatus"] == "CREATE_COMPLETE", st.get("StackStatusReason")
    assert iot_client.get_topic_rule(ruleName=rule)["rule"]["sql"] == sql

    cfn.delete_stack(StackName=stack)
    _wait_stack_gone_iot(cfn, stack)


def _wait_stack_iot(cfn, name, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = cfn.describe_stacks(StackName=name)["Stacks"][0]
        if not st["StackStatus"].endswith("_IN_PROGRESS"):
            return st
        time.sleep(0.5)
    raise TimeoutError(f"Stack {name} did not settle")


def _wait_stack_gone_iot(cfn, name, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            st = cfn.describe_stacks(StackName=name)["Stacks"][0]
        except ClientError:
            return
        if st["StackStatus"] == "DELETE_COMPLETE":
            return
        time.sleep(0.5)


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


# ----------------------------------------------------------------------
# Broker unit-test helpers (white-box tests for the in-process MQTT
# broker that lives in iot.py). Section headers below mark logical
# groupings: LWT (Last Will and Testament), persistent sessions
# (cleanSession=0), and QoS 1 delivery / retransmits.
# ----------------------------------------------------------------------

import asyncio
import struct

from ministack.services.iot import (
    _RETRANSMIT_INTERVAL_SECONDS,
    PKT_CONNACK,
    PKT_CONNECT,
    PKT_DISCONNECT,
    PKT_PUBACK,
    PKT_PUBLISH,
    PKT_SUBACK,
    PKT_SUBSCRIBE,
    PKT_UNSUBACK,
    PKT_UNSUBSCRIBE,
    _decode_properties,
    _decode_remaining_length,
    _encode_remaining_length,
    _encode_string,
    _InFlightMessage,
    _persistent_sessions,
    _property_value,
    _read_string,
    broker_publish,
    broker_subscribe,
)
from ministack.services.iot import (
    _Subscription as _IoTSubscription,
)
from ministack.services.iot import (
    _WSSession as _IoTWSSession,
)
from ministack.services.iot import (
    broker_reset as reset,
)

_TEST_REGION = "us-east-1"


def _WSSession(send, account_id, region=_TEST_REGION):
    return _IoTWSSession(send, account_id, region)


def _Subscription(
    filter_prefixed,
    account_id,
    deliver,
    granted_qos=0,
    region=_TEST_REGION,
):
    return _IoTSubscription(
        filter_prefixed,
        account_id,
        region,
        deliver,
        granted_qos,
    )


async def publish(account_id, topic, payload, **kwargs):
    await broker_publish(
        account_id, _TEST_REGION, topic, payload, **kwargs
    )


async def subscribe(account_id, topic_filter, callback, granted_qos=0):
    return await broker_subscribe(
        account_id,
        _TEST_REGION,
        topic_filter,
        callback,
        granted_qos,
    )


def test_iot_control_plane_identity_stores_are_region_scoped():
    from ministack.core.responses import AccountRegionScopedDict
    from ministack.services import iot as iot_module

    stores = (
        iot_module._things,
        iot_module._thing_types,
        iot_module._thing_groups,
        iot_module._certificates,
        iot_module._policies,
        iot_module._topic_rules,
        iot_module._shadows,
        iot_module._ca_certificates,
        iot_module._registration_codes,
        iot_module._jobs,
        iot_module._job_executions,
    )
    assert all(isinstance(store, AccountRegionScopedDict) for store in stores)


def test_iot_region_scoped_control_plane_accepts_same_resource_names():
    from ministack.core.responses import (
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import iot as iot_module

    account_id = "123456789012"
    resource_name = "same_regional_name"
    policy_document = json.dumps({
        "Version": "2012-10-17",
        "Statement": [],
    })

    async def _request(region, method, path, payload=None):
        set_request_account_id(account_id)
        set_request_region(region)
        body = json.dumps(payload or {}).encode()
        return await iot_module.handle_request(
            method, path, {}, body, {}
        )

    async def _run():
        try:
            east_thing = await _request(
                "us-east-1",
                "POST",
                f"/things/{resource_name}",
                {"attributePayload": {"attributes": {"scope": "east"}}},
            )
            west_thing = await _request(
                "us-west-2",
                "POST",
                f"/things/{resource_name}",
                {"attributePayload": {"attributes": {"scope": "west"}}},
            )
            assert east_thing[0] == 200
            assert west_thing[0] == 200

            for region, expected in (
                ("us-east-1", "east"),
                ("us-west-2", "west"),
            ):
                response = await _request(
                    region, "GET", f"/things/{resource_name}"
                )
                assert response[0] == 200
                assert json.loads(response[2])["attributes"]["scope"] == expected

                policy = await _request(
                    region,
                    "POST",
                    f"/policies/{resource_name}",
                    {"policyDocument": policy_document},
                )
                rule = await _request(
                    region,
                    "POST",
                    f"/rules/{resource_name}",
                    {
                        "sql": "SELECT * FROM 'regional/topic'",
                        "actions": [],
                    },
                )
                assert policy[0] == 200
                assert rule[0] == 200
        finally:
            iot_module.reset()
            iot_module.broker_reset()

    asyncio.run(_run())


def test_iot_broker_publish_and_retained_messages_are_region_isolated():
    reset()

    async def _run():
        account_id = "123456789012"
        east_received = []
        west_received = []

        async def east_callback(topic, payload, qos):
            east_received.append((topic, payload, qos))

        async def west_callback(topic, payload, qos):
            west_received.append((topic, payload, qos))

        await broker_subscribe(
            account_id,
            "us-east-1",
            "sensors/temp",
            east_callback,
        )
        await broker_subscribe(
            account_id,
            "us-west-2",
            "sensors/temp",
            west_callback,
        )
        await broker_publish(
            account_id,
            "us-east-1",
            "sensors/temp",
            b"21C",
            retain=True,
        )

        assert east_received == [("sensors/temp", b"21C", 0)]
        assert west_received == []

        east_retained = []
        west_retained = []

        async def east_retained_callback(topic, payload, qos):
            east_retained.append((topic, payload, qos))

        async def west_retained_callback(topic, payload, qos):
            west_retained.append((topic, payload, qos))

        await broker_subscribe(
            account_id,
            "us-east-1",
            "sensors/temp",
            east_retained_callback,
        )
        await broker_subscribe(
            account_id,
            "us-west-2",
            "sensors/temp",
            west_retained_callback,
        )

        assert east_retained == [("sensors/temp", b"21C", 0)]
        assert west_retained == []

    asyncio.run(_run())
    reset()


@pytest.mark.parametrize("wildcard_region", ["+", "#"])
def test_iot_broker_region_wildcards_cannot_bypass_isolation(
    wildcard_region,
):
    reset()

    async def _run():
        account_id = "123456789012"
        live_received = []
        retained_received = []

        async def live_callback(topic, payload, qos):
            live_received.append((topic, payload, qos))

        async def retained_callback(topic, payload, qos):
            retained_received.append((topic, payload, qos))

        await broker_subscribe(
            account_id,
            wildcard_region,
            "sensors/temp",
            live_callback,
        )
        await broker_publish(
            account_id,
            "us-east-1",
            "sensors/temp",
            b"east",
            retain=True,
        )
        await broker_subscribe(
            account_id,
            wildcard_region,
            "sensors/temp",
            retained_callback,
        )

        assert live_received == []
        assert retained_received == []

    asyncio.run(_run())
    reset()


def test_iot_broker_persistent_sessions_are_region_isolated():
    reset()

    async def _run():
        account_id = "123456789012"
        client_id = "shared-regional-client"
        connect = _build_connect_body(
            client_id=client_id, clean_session=False
        )

        east_send, _ = _mock_send()
        east_session = _WSSession(
            east_send, account_id, region="us-east-1"
        )
        await east_session.handle_packet(PKT_CONNECT, 0, connect)
        await east_session.handle_packet(
            PKT_SUBSCRIBE,
            0x02,
            _build_subscribe_body(1, [("events/#", 1)]),
        )
        await east_session.handle_packet(PKT_DISCONNECT, 0, b"")
        await east_session.cleanup()

        west_send, west_sent = _mock_send()
        west_session = _WSSession(
            west_send, account_id, region="us-west-2"
        )
        await west_session.handle_packet(PKT_CONNECT, 0, connect)

        session_present, return_code = _parse_connack(west_sent)
        assert session_present is False
        assert return_code == 0
        assert (
            account_id,
            "us-east-1",
            client_id,
        ) in _persistent_sessions
        assert (
            account_id,
            "us-west-2",
            client_id,
        ) in _persistent_sessions

        await west_session.cleanup()
        await broker_publish(
            account_id,
            "us-east-1",
            "events/one",
            b"east-only",
            qos=1,
        )
        east_state = _persistent_sessions[
            (account_id, "us-east-1", client_id)
        ]
        west_state = _persistent_sessions[
            (account_id, "us-west-2", client_id)
        ]
        assert east_state.queued_messages == [
            ("events/one", b"east-only", 1)
        ]
        assert west_state.queued_messages == []

    asyncio.run(_run())
    reset()


def test_iot_topic_rules_and_basic_ingest_use_publish_region(monkeypatch):
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    east_rule = {
        "ruleName": "regional_rule",
        "sql": "SELECT * FROM 'sensors/#'",
        "ruleDisabled": False,
        "actions": [],
    }
    west_rule = {
        **east_rule,
        "sql": "SELECT * FROM 'west/#'",
        "description": "west",
    }
    iot_module._topic_rules.set_scoped(
        account_id, "us-east-1", "regional_rule", east_rule
    )
    iot_module._topic_rules.set_scoped(
        account_id, "us-west-2", "regional_rule", west_rule
    )
    dispatched = []

    async def _capture_rule_action(
        dispatched_account_id, dispatched_region, rule, payload, topic="", client_id=None
    ):
        dispatched.append((dispatched_account_id, dispatched_region, rule, payload))

    monkeypatch.setattr(
        iot_module, "_run_rule_actions", _capture_rule_action
    )

    async def _run():
        await broker_publish(
            account_id,
            "us-east-1",
            "sensors/temperature",
            b'{"value": 21}',
        )
        assert dispatched == [
            (account_id, "us-east-1", east_rule, b'{"value": 21}')
        ]

        dispatched.clear()
        await broker_publish(
            account_id,
            "us-west-2",
            "$aws/rules/regional_rule",
            b'{"value": 22}',
        )
        assert dispatched == [
            (account_id, "us-west-2", west_rule, b'{"value": 22}')
        ]

    try:
        asyncio.run(_run())
    finally:
        iot_module._topic_rules.clear()
        reset()


@pytest.mark.parametrize("function_ref_kind", ["name", "full_arn"])
def test_iot_rule_lambda_dispatch_uses_rule_region(function_ref_kind, monkeypatch):
    from ministack.services import iot as iot_module
    from ministack.services import lambda_svc

    class _ImmediateThread:
        def __init__(self, target, args=(), kwargs=None, daemon=None):
            self._target = target
            self._args = args
            self._kwargs = kwargs or {}

        def start(self):
            self._target(*self._args, **self._kwargs)

    account_id = "123456789012"
    function_name = _unique("rulefn")
    east_arn = f"arn:aws:lambda:us-east-1:{account_id}:function:{function_name}"
    west_arn = f"arn:aws:lambda:us-west-2:{account_id}:function:{function_name}"

    def _function_record(function_arn):
        return {
            "config": {
                "FunctionName": function_name,
                "FunctionArn": function_arn,
            },
            "aliases": {},
            "versions": {},
        }

    dispatched = []

    def _capture_execute(func, event):
        dispatched.append((func["config"]["FunctionArn"], event))

    monkeypatch.setattr(iot_module.threading, "Thread", _ImmediateThread)
    monkeypatch.setattr(
        lambda_svc, "_execute_function_with_config_scope", _capture_execute
    )

    lambda_svc._functions.clear()
    iot_module._topic_rules.clear()
    reset()
    try:
        lambda_svc._functions.set_scoped(
            account_id, "us-east-1", function_name, _function_record(east_arn)
        )
        lambda_svc._functions.set_scoped(
            account_id, "us-west-2", function_name, _function_record(west_arn)
        )
        function_ref = function_name if function_ref_kind == "name" else west_arn
        iot_module._topic_rules.set_scoped(
            account_id,
            "us-west-2",
            "regional_rule",
            {
                "ruleName": "regional_rule",
                "sql": "SELECT * FROM 'sensors/#'",
                "ruleDisabled": False,
                "actions": [{"lambda": {"functionArn": function_ref}}],
            },
        )

        async def _run():
            await broker_publish(
                account_id,
                "us-west-2",
                "sensors/temperature",
                b'{"temperature": 22}',
            )

        asyncio.run(_run())
        assert dispatched == [(west_arn, {"temperature": 22})]
    finally:
        lambda_svc._functions.clear()
        iot_module._topic_rules.clear()
        reset()


def _build_connect_body(
    client_id="test",
    clean_session=True,
    will_flag=False,
    will_qos=0,
    will_retain=False,
    will_topic="",
    will_message=b"",
):
    """Build a CONNECT packet body (variable header + payload)."""
    body = bytearray()
    body += _encode_string("MQTT")  # Protocol Name
    body.append(4)                   # Protocol Level (MQTT 3.1.1)
    flags = 0
    if clean_session:
        flags |= 0x02
    if will_flag:
        flags |= 0x04
        flags |= (will_qos & 0x03) << 3
        if will_retain:
            flags |= 0x20
    body.append(flags)
    body += struct.pack("!H", 60)    # Keep Alive
    body += _encode_string(client_id)
    if will_flag:
        body += _encode_string(will_topic)
        msg = will_message if isinstance(will_message, bytes) else will_message.encode()
        body += struct.pack("!H", len(msg)) + msg
    return bytes(body)


def _build_subscribe_body(packet_id, topics_qos):
    """Build a SUBSCRIBE packet body. topics_qos is a list of (topic, qos)."""
    body = struct.pack("!H", packet_id)
    for topic, qos in topics_qos:
        body += _encode_string(topic)
        body += bytes([qos])
    return body


def _build_unsubscribe_body(packet_id, topic_filters):
    """Build an UNSUBSCRIBE packet body. topic_filters is a list of filters."""
    body = struct.pack("!H", packet_id)
    for topic_filter in topic_filters:
        body += _encode_string(topic_filter)
    return body


def _mock_send():
    """Return (async send-callable, captured-message list)."""
    sent = []

    async def send(msg):
        sent.append(msg)

    return send, sent


def _unsuback_packet_ids(sent_messages):
    """Return the packet ID of every UNSUBACK in sent messages."""
    ids = []
    for msg in sent_messages:
        data = msg.get("bytes")
        if data and len(data) >= 4 and (data[0] >> 4) & 0x0F == PKT_UNSUBACK:
            ids.append(struct.unpack_from("!H", data, 2)[0])
    return ids


def _parse_connack(sent_messages):
    """Extract (sessionPresent, return_code) from a CONNACK in sent messages."""
    for msg in sent_messages:
        data = msg.get("bytes")
        if data and len(data) >= 4:
            pkt_type = (data[0] >> 4) & 0x0F
            if pkt_type == PKT_CONNACK:
                session_present = bool(data[2] & 0x01)
                return_code = data[3]
                return session_present, return_code
    return None, None


def _extract_publish_frames(sent_messages):
    """Return list of (topic, payload, qos, packet_id, dup) for every PUBLISH in sent messages."""
    results = []
    for msg in sent_messages:
        data = msg.get("bytes")
        if data is None or not data:
            continue
        first = data[0]
        pkt_type = (first >> 4) & 0x0F
        if pkt_type != 3:  # 3 = PUBLISH
            continue
        qos = (first >> 1) & 0x03
        dup = bool(first & 0x08)
        offset = 1
        multiplier = 1
        remaining = 0
        while True:
            b = data[offset]
            offset += 1
            remaining += (b & 0x7F) * multiplier
            if b & 0x80 == 0:
                break
            multiplier *= 128
        topic_len = struct.unpack_from("!H", data, offset)[0]
        offset += 2
        topic = data[offset:offset + topic_len].decode("utf-8")
        offset += topic_len
        packet_id = None
        if qos > 0:
            packet_id = struct.unpack_from("!H", data, offset)[0]
            offset += 2
        payload = data[offset:]
        results.append((topic, payload, qos, packet_id, dup))
    return results


# ----------------------------------------------------------------------
# Broker — Last Will and Testament
# ----------------------------------------------------------------------



def test_will_fields_parsed_from_connect():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device1",
            will_flag=True,
            will_qos=1,
            will_retain=True,
            will_topic="devices/device1/status",
            will_message=b"offline",
        )
        result = await session.handle_packet(PKT_CONNECT, 0, body)
        assert result is True
        assert session._will_topic == "devices/device1/status"
        assert session._will_message == b"offline"
        assert session._will_qos == 1
        assert session._will_retain is True

    asyncio.run(_run())
    reset()


def test_no_will_when_flag_not_set():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="device2", will_flag=False)
        await session.handle_packet(PKT_CONNECT, 0, body)
        assert session._will_topic is None
        assert session._will_message is None
        assert session._will_qos == 0
        assert session._will_retain is False

    asyncio.run(_run())
    reset()


def test_graceful_disconnect_does_not_publish_will():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device3",
            will_flag=True,
            will_qos=0,
            will_retain=False,
            will_topic="devices/device3/status",
            will_message=b"offline",
        )
        await session.handle_packet(PKT_CONNECT, 0, body)

        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        await subscribe("123456789012", "devices/device3/status", on_msg)

        # Graceful disconnect
        result = await session.handle_packet(PKT_DISCONNECT, 0, b"")
        assert result is False
        assert session._graceful_disconnect is True
        await session.cleanup()
        assert len(received) == 0

    asyncio.run(_run())
    reset()


def test_ungraceful_disconnect_publishes_will():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device4",
            will_flag=True,
            will_qos=1,
            will_retain=False,
            will_topic="devices/device4/status",
            will_message=b"gone",
        )
        await session.handle_packet(PKT_CONNECT, 0, body)

        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        # Subscribe at QoS 1 so effective QoS = min(publish_qos=1, granted_qos=1) = 1
        await subscribe("123456789012", "devices/device4/status", on_msg, granted_qos=1)

        # Ungraceful disconnect (no DISCONNECT packet)
        await session.cleanup()
        assert len(received) == 1
        assert received[0] == ("devices/device4/status", b"gone", 1)

    asyncio.run(_run())
    reset()


def test_will_retain_stores_retained_message():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(
            client_id="device5",
            will_flag=True,
            will_qos=0,
            will_retain=True,
            will_topic="devices/device5/status",
            will_message=b"dead",
        )
        await session.handle_packet(PKT_CONNECT, 0, body)
        # Ungraceful disconnect publishes Will with retain
        await session.cleanup()

        # New subscriber should get the retained message
        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        await subscribe("123456789012", "devices/device5/status", on_msg)
        assert len(received) == 1
        assert received[0] == ("devices/device5/status", b"dead", 0)

    asyncio.run(_run())
    reset()


def test_reconnect_replaces_will_fields():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body1 = _build_connect_body(
            client_id="device6",
            will_flag=True,
            will_qos=0,
            will_retain=False,
            will_topic="old/topic",
            will_message=b"old",
        )
        await session.handle_packet(PKT_CONNECT, 0, body1)
        assert session._will_topic == "old/topic"

        # Reconnect with new Will
        body2 = _build_connect_body(
            client_id="device6",
            will_flag=True,
            will_qos=1,
            will_retain=True,
            will_topic="new/topic",
            will_message=b"new",
        )
        await session.handle_packet(PKT_CONNECT, 0, body2)
        assert session._will_topic == "new/topic"
        assert session._will_message == b"new"
        assert session._will_qos == 1
        assert session._will_retain is True

    asyncio.run(_run())
    reset()


def test_reconnect_clears_graceful_disconnect_flag():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="device7", will_flag=False)
        await session.handle_packet(PKT_CONNECT, 0, body)

        # Graceful disconnect
        await session.handle_packet(PKT_DISCONNECT, 0, b"")
        assert session._graceful_disconnect is True

        # Reconnect resets the flag
        await session.handle_packet(PKT_CONNECT, 0, body)
        assert session._graceful_disconnect is False

    asyncio.run(_run())
    reset()


def test_reconnect_without_will_clears_previous_will():
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        # First connect with Will
        body1 = _build_connect_body(
            client_id="device8",
            will_flag=True,
            will_qos=1,
            will_retain=True,
            will_topic="presence/device8",
            will_message=b"offline",
        )
        await session.handle_packet(PKT_CONNECT, 0, body1)
        assert session._will_topic == "presence/device8"

        # Reconnect without Will
        body2 = _build_connect_body(client_id="device8", will_flag=False)
        await session.handle_packet(PKT_CONNECT, 0, body2)
        assert session._will_topic is None
        assert session._will_message is None

        # Ungraceful disconnect should NOT publish anything
        received = []

        async def on_msg(topic, payload, qos):
            received.append((topic, payload, qos))

        await subscribe("123456789012", "presence/device8", on_msg)
        await session.cleanup()
        assert len(received) == 0

    asyncio.run(_run())
    reset()


# ----------------------------------------------------------------------
# Broker — Persistent sessions (cleanSession flag)
# ----------------------------------------------------------------------



def test_clean_session_1_sends_session_present_0():
    """cleanSession=1 always sends sessionPresent=0."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="client1", clean_session=True)
        await session.handle_packet(PKT_CONNECT, 0, body)

        session_present, return_code = _parse_connack(sent)
        assert session_present is False
        assert return_code == 0

    asyncio.run(_run())
    reset()


def test_clean_session_0_no_prior_session_sends_session_present_0():
    """cleanSession=0 with no prior session sends sessionPresent=0."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")

        body = _build_connect_body(client_id="client1", clean_session=False)
        await session.handle_packet(PKT_CONNECT, 0, body)

        session_present, return_code = _parse_connack(sent)
        assert session_present is False
        assert return_code == 0

    asyncio.run(_run())
    reset()


def test_persistent_session_subscribe_disconnect_reconnect_restores():
    """Connect with cleanSession=0 → subscribe → disconnect → reconnect → sessionPresent=1 and subscriptions restored."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe to a topic
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device1", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        # Subscribe to "sensor/temp"
        sub_body = _build_subscribe_body(1, [("sensor/temp", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect (graceful)
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Second connection: cleanSession=0, same client_id
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, return_code = _parse_connack(sent2)
        assert session_present is True
        assert return_code == 0

        # Verify subscriptions are restored by publishing a message
        received = []

        # The session should already be subscribed, so publish should deliver
        await publish("123456789012", "sensor/temp", b"25C", qos=1)

        # Check that session2 received the message
        # The message should be in sent2 as a PUBLISH packet
        publish_found = False
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_found = True
                break
        assert publish_found, "Restored subscription should receive published messages"

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_clean_session_1_discards_prior_state():
    """cleanSession=1 discards any prior persistent session state."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body_persistent = _build_connect_body(client_id="device2", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body_persistent)

        sub_body = _build_subscribe_body(1, [("alerts/#", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Verify persistent session exists
        assert (
            "123456789012",
            _TEST_REGION,
            "device2",
        ) in _persistent_sessions

        # Second connection: cleanSession=1 — should discard prior state
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        connect_body_clean = _build_connect_body(client_id="device2", clean_session=True)
        await session2.handle_packet(PKT_CONNECT, 0, connect_body_clean)

        session_present, return_code = _parse_connack(sent2)
        assert session_present is False
        assert return_code == 0

        # Verify persistent session was discarded
        assert (
            "123456789012",
            _TEST_REGION,
            "device2",
        ) not in _persistent_sessions

        # Publish to the old subscription topic — should NOT be delivered
        await publish("123456789012", "alerts/fire", b"alarm", qos=1)

        publish_found = False
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_found = True
                break
        assert not publish_found, "cleanSession=1 should not restore prior subscriptions"

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_offline_qos1_messages_queued_and_delivered_on_reconnect():
    """Persistent session disconnects; QoS 1 messages published; reconnect delivers queued messages."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device3", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("data/stream", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Publish QoS 1 messages while client is offline
        await publish("123456789012", "data/stream", b"msg1", qos=1)
        await publish("123456789012", "data/stream", b"msg2", qos=1)
        await publish("123456789012", "data/stream", b"msg3", qos=1)

        # Verify messages are queued
        ps = _persistent_sessions.get(
            ("123456789012", _TEST_REGION, "device3")
        )
        assert ps is not None
        assert len(ps.queued_messages) == 3

        # Reconnect with cleanSession=0
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, _ = _parse_connack(sent2)
        assert session_present is True

        # Verify queued messages were delivered
        publish_messages = []
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_messages.append(data)

        assert len(publish_messages) == 3, f"Expected 3 queued messages delivered, got {len(publish_messages)}"

        # Verify queue is now empty
        assert len(ps.queued_messages) == 0

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_qos0_messages_not_queued_for_offline_sessions():
    """QoS 0 messages should NOT be queued for offline persistent sessions."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device4", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("events/log", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Publish QoS 0 messages while client is offline
        await publish("123456789012", "events/log", b"info1", qos=0)
        await publish("123456789012", "events/log", b"info2", qos=0)

        # Verify no messages queued (QoS 0 not queued)
        ps = _persistent_sessions.get(
            ("123456789012", _TEST_REGION, "device4")
        )
        assert ps is not None
        assert len(ps.queued_messages) == 0

    asyncio.run(_run())
    reset()


def test_queue_bounded_to_1000_messages():
    """Queue should be bounded to 1000 messages, dropping oldest on overflow."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device5", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("bulk/data", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Publish 1050 QoS 1 messages while client is offline
        for i in range(1050):
            await publish("123456789012", "bulk/data", f"msg{i}".encode(), qos=1)

        # Verify queue is bounded to 1000
        ps = _persistent_sessions.get(
            ("123456789012", _TEST_REGION, "device5")
        )
        assert ps is not None
        assert len(ps.queued_messages) == 1000

        # Verify oldest messages were dropped (first 50 should be gone)
        first_topic, first_payload, first_qos = ps.queued_messages[0]
        assert first_payload == b"msg50"

    asyncio.run(_run())
    reset()


def test_expired_session_not_restored():
    """An expired persistent session should not be restored."""
    reset()

    async def _run():
        import time

        # First connection: cleanSession=0, subscribe
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device6", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("temp/data", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Manually expire the session by setting created_at far in the past
        ps = _persistent_sessions.get(
            ("123456789012", _TEST_REGION, "device6")
        )
        assert ps is not None
        ps.created_at = time.time() - 7200  # 2 hours ago (default expiry is 1 hour)

        # Reconnect with cleanSession=0
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, return_code = _parse_connack(sent2)
        assert session_present is False  # Expired session not restored
        assert return_code == 0

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_wildcard_subscription_persisted_and_restored():
    """Wildcard subscriptions should be persisted and restored correctly."""
    reset()

    async def _run():
        # First connection: cleanSession=0, subscribe with wildcard
        send1, sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")

        connect_body = _build_connect_body(client_id="device7", clean_session=False)
        await session1.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("sensors/+/temp", 1)])
        await session1.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)

        # Disconnect
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        # Reconnect
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")

        await session2.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, _ = _parse_connack(sent2)
        assert session_present is True

        # Publish to a topic matching the wildcard
        await publish("123456789012", "sensors/room1/temp", b"22C", qos=1)

        publish_found = False
        for msg in sent2:
            data = msg.get("bytes")
            if data and ((data[0] >> 4) & 0x0F) == PKT_PUBLISH:
                publish_found = True
                break
        assert publish_found, "Restored wildcard subscription should receive matching messages"

        await session2.cleanup()

    asyncio.run(_run())
    reset()


def test_different_accounts_sessions_isolated():
    """Persistent sessions are scoped by account, region, and client ID."""
    reset()

    async def _run():
        # Account A: connect, subscribe, disconnect
        send_a, sent_a = _mock_send()
        session_a = _WSSession(send_a, "account_A")

        connect_body = _build_connect_body(client_id="shared_id", clean_session=False)
        await session_a.handle_packet(PKT_CONNECT, 0, connect_body)

        sub_body = _build_subscribe_body(1, [("topic/a", 1)])
        await session_a.handle_packet(PKT_SUBSCRIBE, 0x02, sub_body)
        await session_a.handle_packet(PKT_DISCONNECT, 0, b"")
        await session_a.cleanup()

        # Account B: connect with same client_id — should NOT see account A's session
        send_b, sent_b = _mock_send()
        session_b = _WSSession(send_b, "account_B")

        await session_b.handle_packet(PKT_CONNECT, 0, connect_body)

        session_present, _ = _parse_connack(sent_b)
        assert session_present is False  # No prior session for account_B

        await session_b.cleanup()

    asyncio.run(_run())
    reset()


# ----------------------------------------------------------------------
# Broker — MQTT 5.0. These drive the session object directly, so they need
# neither a running server nor the websockets package; the wire-level suite
# that needs both lives in test_iot_mqtt5.py.
# ----------------------------------------------------------------------

PROP_SESSION_EXPIRY_INTERVAL = 0x11
PROP_MAXIMUM_QOS = 0x24
PROP_USER_PROPERTY = 0x26


def _mqtt_packet(pkt_type, flags, body):
    """Wrap a packet body in its fixed header."""
    return bytes([(pkt_type << 4) | flags]) + _encode_remaining_length(len(body)) + body


def _build_mqtt5_connect_body(client_id="v5-client", clean_start=True, session_expiry=None):
    """Build an MQTT 5 CONNECT body: like the 3.1.1 one plus a property block."""
    props = b""
    if session_expiry is not None:
        props = bytes([PROP_SESSION_EXPIRY_INTERVAL]) + struct.pack("!I", session_expiry)
    body = bytearray()
    body += _encode_string("MQTT")
    body.append(5)                                    # Protocol Level
    body.append(0x02 if clean_start else 0x00)        # Clean Start
    body += struct.pack("!H", 60)                     # Keep Alive
    body += _encode_remaining_length(len(props)) + props
    body += _encode_string(client_id)
    return bytes(body)


def _build_mqtt5_subscribe_body(packet_id, topic, options):
    """Build an MQTT 5 SUBSCRIBE body: property block, then filter + options."""
    return (
        struct.pack("!H", packet_id)
        + b"\x00"
        + _encode_string(topic)
        + bytes([options])
    )


def _build_mqtt5_publish_body(topic, payload, qos=0, packet_id=None, properties=b"\x00"):
    body = _encode_string(topic)
    if qos:
        body += struct.pack("!H", packet_id or 1)
    return body + properties + payload


def _mqtt5_user_property(name, value):
    """An encoded property block holding one User Property."""
    body = bytes([PROP_USER_PROPERTY]) + _encode_string(name) + _encode_string(value)
    return _encode_remaining_length(len(body)) + body


def _parse_mqtt5_publish(packet):
    """Return (topic, payload, properties) from a PUBLISH the broker sent."""
    qos = (packet[0] >> 1) & 0x03
    _remaining, off = _decode_remaining_length(packet, 1)
    topic, off = _read_string(packet, off)
    if qos:
        off += 2
    props, off = _decode_properties(packet, off)
    return topic, packet[off:], props


def test_mqtt5_session_round_trips_over_a_plain_byte_stream():
    """Version negotiation lives in the session, not in the WebSocket layer.

    The session object is driven here the way a raw TCP listener drives it —
    bytes appended to its buffer, packets taken off by its own framing — with
    every packet split at an offset that lands inside the new property fields,
    which is where a framing mistake would show up first.
    """
    reset()
    topic = "stream/topic"

    async def _run():
        sent = []

        async def _stream_send(message):
            if message.get("type") == "websocket.send":
                sent.append(message["bytes"])

        session = _WSSession(_stream_send, "123456789012")

        async def feed(packet, chunk):
            for start in range(0, len(packet), chunk):
                session._buffer.extend(packet[start:start + chunk])
                while True:
                    parsed = session._take_packet()
                    if parsed is None:
                        break
                    await session.handle_packet(*parsed)

        await feed(
            _mqtt_packet(PKT_CONNECT, 0, _build_mqtt5_connect_body("stream-client")), 3
        )
        await feed(
            _mqtt_packet(PKT_SUBSCRIBE, 0x02, _build_mqtt5_subscribe_body(1, topic, 0x01)),
            5,
        )
        await feed(
            _mqtt_packet(
                PKT_PUBLISH,
                0x02,
                _build_mqtt5_publish_body(
                    topic, b"over-tcp", qos=1, packet_id=9,
                    properties=_mqtt5_user_property("via", "stream"),
                ),
            ),
            4,
        )
        await session.cleanup()
        return sent

    sent = asyncio.run(_run())
    assert len(sent) == 4, "CONNACK, SUBACK, the delivered PUBLISH, then PUBACK"
    connack, suback, publish, puback = sent
    assert connack[:2] == bytes([0x20, len(connack) - 2])
    assert connack[3] == 0x00, "reason code Success"
    connack_props, _end = _decode_properties(connack, 4)
    assert _property_value(connack_props, PROP_MAXIMUM_QOS, None) == 1
    assert suback[0] >> 4 == PKT_SUBACK
    assert publish[0] >> 4 == PKT_PUBLISH
    assert (publish[0] >> 1) & 0x03 == 1, "delivered at the granted QoS"
    delivered_topic, payload, props = _parse_mqtt5_publish(publish)
    assert (delivered_topic, payload) == (topic, b"over-tcp")
    assert _property_value(props, PROP_USER_PROPERTY, None) == ("via", "stream")
    assert puback == bytes([0x40, 0x04, 0x00, 0x09, 0x00, 0x00])
    reset()


def test_mqtt5_session_expiry_is_read_as_an_interval_not_a_flag():
    """Session Expiry Interval says how long the session lives, not whether.

    The session below asks for 60 seconds, so it is restored a second later
    and gone a minute later. Treated as a yes/no flag it would instead have
    fallen back to the module-wide hour and been restored both times.
    """
    reset()
    key = ("123456789012", _TEST_REGION, "expiring")
    connect = _build_mqtt5_connect_body(
        "expiring", clean_start=False, session_expiry=60
    )

    async def _run():
        send1, _sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")
        await session1.handle_packet(PKT_CONNECT, 0, connect)
        await session1.handle_packet(
            PKT_SUBSCRIBE, 0x02, _build_mqtt5_subscribe_body(1, "temp/data", 0x01)
        )
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()

        assert _persistent_sessions[key].expiry_interval == 60

        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")
        await session2.handle_packet(PKT_CONNECT, 0, connect)
        assert _parse_connack(sent2) == (True, 0), "still inside the interval"
        await session2.handle_packet(PKT_DISCONNECT, 0, b"")
        await session2.cleanup()

        # Spend the interval the client asked for, which is still well inside
        # the module-wide default.
        _persistent_sessions[key].created_at = time.time() - 61

        send3, sent3 = _mock_send()
        session3 = _WSSession(send3, "123456789012")
        await session3.handle_packet(PKT_CONNECT, 0, connect)
        assert _parse_connack(sent3) == (False, 0), "the interval is spent"
        await session3.cleanup()

    asyncio.run(_run())
    reset()


def test_mqtt5_resumed_session_without_an_expiry_interval_is_discarded():
    """Clean Start 0 and no Session Expiry Interval: resume one, leave none.

    MQTT 5 splits resumption from retention, so this connection legitimately
    picks a stored session up and legitimately must not leave one behind.
    Nothing used to remove it — the entry outlived every such connection,
    queueing QoS 1 messages nobody would collect and answering the next
    CONNECT with session_present=1.
    """
    reset()
    key = ("123456789012", _TEST_REGION, "transient")

    async def _run():
        # A first connection that does persist, so there is one to resume.
        send1, _sent1 = _mock_send()
        session1 = _WSSession(send1, "123456789012")
        await session1.handle_packet(
            PKT_CONNECT,
            0,
            _build_mqtt5_connect_body(
                "transient", clean_start=False, session_expiry=3600
            ),
        )
        await session1.handle_packet(
            PKT_SUBSCRIBE, 0x02, _build_mqtt5_subscribe_body(1, "temp/data", 0x01)
        )
        await session1.handle_packet(PKT_DISCONNECT, 0, b"")
        await session1.cleanup()
        assert key in _persistent_sessions

        resume_only = _build_mqtt5_connect_body("transient", clean_start=False)
        send2, sent2 = _mock_send()
        session2 = _WSSession(send2, "123456789012")
        await session2.handle_packet(PKT_CONNECT, 0, resume_only)
        assert _parse_connack(sent2) == (True, 0), "Clean Start 0 still resumes"
        await session2.handle_packet(PKT_DISCONNECT, 0, b"")
        await session2.cleanup()
        assert key not in _persistent_sessions

        send3, sent3 = _mock_send()
        session3 = _WSSession(send3, "123456789012")
        await session3.handle_packet(PKT_CONNECT, 0, resume_only)
        assert _parse_connack(sent3) == (False, 0), "and the next client is told so"
        await session3.cleanup()

    asyncio.run(_run())
    reset()


# ----------------------------------------------------------------------
# Broker — QoS 1 + retransmits
# ----------------------------------------------------------------------



def test_subscription_has_granted_qos_field():
    """_Subscription stores granted_qos."""
    async def deliver(t, p, q):
        pass

    sub = _Subscription("acct/topic", "acct", deliver, granted_qos=1)
    assert sub.granted_qos == 1

    sub0 = _Subscription("acct/topic", "acct", deliver, granted_qos=0)
    assert sub0.granted_qos == 0


def test_subscribe_handler_caps_qos_at_1():
    """PKT_SUBSCRIBE grants min(requested, 1) — QoS 2 is capped to 1."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("sub-client"))

        # Subscribe with QoS 0, 1, and 2
        body = _build_subscribe_body(1, [
            ("topic/a", 0),
            ("topic/b", 1),
            ("topic/c", 2),  # Should be capped to 1
        ])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)

        # Find SUBACK in sent messages
        suback_frames = [m for m in sent if m.get("bytes") and (m["bytes"][0] >> 4) == 9]
        assert len(suback_frames) == 1
        suback_data = suback_frames[0]["bytes"]
        # SUBACK: fixed header (1 byte) + remaining length (1 byte) + packet_id (2 bytes) + return codes
        offset = 1
        # Decode remaining length
        multiplier = 1
        remaining = 0
        while True:
            b = suback_data[offset]
            offset += 1
            remaining += (b & 0x7F) * multiplier
            if b & 0x80 == 0:
                break
            multiplier *= 128
        # Skip packet ID
        offset += 2
        # Return codes
        return_codes = list(suback_data[offset:])
        assert return_codes == [0, 1, 1]  # QoS 2 capped to 1

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_subscribe_stores_granted_qos_on_session():
    """Session tracks granted QoS per subscription ID."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos-track"))

        body = _build_subscribe_body(1, [("sensor/temp", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)

        # Session should have one subscription with granted_qos=1
        assert len(session._sub_ids) == 1
        sid = session._sub_ids[0]
        assert session._sub_granted_qos[sid] == 1

        await session.cleanup()

    asyncio.run(_run())
    reset()


# ----------------------------------------------------------------------
# Broker — UNSUBSCRIBE
# ----------------------------------------------------------------------


async def _connect_and_subscribe(client_id, topics, clean_session=True):
    """CONNECT a session and SUBSCRIBE it at QoS 0 to every topic filter."""
    send, sent = _mock_send()
    session = _WSSession(send, "123456789012")
    await session.handle_packet(
        PKT_CONNECT, 0, _build_connect_body(client_id, clean_session=clean_session)
    )
    await session.handle_packet(
        PKT_SUBSCRIBE, 0x02, _build_subscribe_body(1, [(t, 0) for t in topics])
    )
    sent.clear()
    return session, sent


def test_unsubscribe_removes_only_the_named_filters():
    """The filters named in the packet go; every other subscription stays."""
    reset()

    async def _run():
        session, sent = await _connect_and_subscribe(
            "unsub-one", ["sensor/a", "sensor/b", "sensor/c"]
        )

        await session.handle_packet(
            PKT_UNSUBSCRIBE, 0x02, _build_unsubscribe_body(7, ["sensor/b"])
        )
        assert _unsuback_packet_ids(sent) == [7]

        sent.clear()
        for topic in ("sensor/a", "sensor/b", "sensor/c"):
            await publish("123456789012", topic, topic.encode())

        delivered = [topic for topic, _payload, _qos, _pid, _dup in _extract_publish_frames(sent)]
        assert delivered == ["sensor/a", "sensor/c"]

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_unsubscribe_of_filter_never_subscribed_is_a_no_op():
    """An unmatched filter changes nothing and is still acknowledged."""
    reset()

    async def _run():
        session, sent = await _connect_and_subscribe("unsub-unknown", ["sensor/a"])

        await session.handle_packet(
            PKT_UNSUBSCRIBE, 0x02, _build_unsubscribe_body(11, ["sensor/never"])
        )
        # MQTT 3.1.1 UNSUBACK carries no reason codes, so the acknowledgement
        # looks the same whether or not a subscription existed.
        assert _unsuback_packet_ids(sent) == [11]

        sent.clear()
        await publish("123456789012", "sensor/a", b"still-here")

        assert _extract_publish_frames(sent) == [("sensor/a", b"still-here", 0, None, False)]

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_unsubscribe_of_every_filter_stops_delivery():
    """Naming all the session's filters leaves it with no subscriptions."""
    reset()

    async def _run():
        session, sent = await _connect_and_subscribe(
            "unsub-all", ["sensor/a", "sensor/b"]
        )

        await session.handle_packet(
            PKT_UNSUBSCRIBE, 0x02, _build_unsubscribe_body(3, ["sensor/a", "sensor/b"])
        )
        assert _unsuback_packet_ids(sent) == [3]
        assert session._sub_ids == []
        assert session._sub_filters == {}
        assert session._sub_granted_qos == {}

        sent.clear()
        await publish("123456789012", "sensor/a", b"gone")
        await publish("123456789012", "sensor/b", b"gone")

        assert _extract_publish_frames(sent) == []

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_unsubscribe_matches_a_wildcard_filter_by_its_own_text():
    """A wildcard subscription goes by its filter string, not what it matched."""
    reset()

    async def _run():
        session, sent = await _connect_and_subscribe("unsub-wildcard", ["sensor/#"])

        # `sensor/temp` is a topic the wildcard delivers, not a filter the
        # session holds — it must leave the subscription untouched.
        await session.handle_packet(
            PKT_UNSUBSCRIBE, 0x02, _build_unsubscribe_body(1, ["sensor/temp"])
        )
        sent.clear()
        await publish("123456789012", "sensor/temp", b"matched")
        assert len(_extract_publish_frames(sent)) == 1

        await session.handle_packet(
            PKT_UNSUBSCRIBE, 0x02, _build_unsubscribe_body(2, ["sensor/#"])
        )
        sent.clear()
        await publish("123456789012", "sensor/temp", b"unmatched")
        assert _extract_publish_frames(sent) == []

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_unsubscribe_drops_the_filter_from_a_preserved_session():
    """An unsubscribed filter must not come back with a cleanSession=0 resume."""
    reset()

    async def _run():
        client_id = "unsub-persistent"
        session, _sent = await _connect_and_subscribe(
            client_id, ["sensor/a", "sensor/b"], clean_session=False
        )

        await session.handle_packet(
            PKT_UNSUBSCRIBE, 0x02, _build_unsubscribe_body(1, ["sensor/a"])
        )
        await session.handle_packet(PKT_DISCONNECT, 0, b"")
        await session.cleanup()

        preserved = _persistent_sessions[("123456789012", _TEST_REGION, client_id)]
        assert preserved.subscriptions == ["sensor/b"]

    asyncio.run(_run())
    reset()


# ---------------------------------------------------------------------------
# Task 18.2: QoS 1 delivery with packet ID tracking
# ---------------------------------------------------------------------------


def test_qos1_publish_delivers_with_packet_id():
    """QoS 1 publish to QoS 1 subscriber delivers at QoS 1 with packet ID."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos1-sub"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/qos1", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)

        # Clear sent to isolate publish frames
        sent.clear()

        # Publish at QoS 1 from external source
        await publish("123456789012", "test/qos1", b"hello-qos1", qos=1)

        # Check delivered message
        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert topic == "test/qos1"
        assert payload == b"hello-qos1"
        assert qos == 1
        assert packet_id is not None
        assert packet_id >= 1
        assert dup is False

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_qos0_publish_to_qos1_subscriber_delivers_at_qos0():
    """QoS 0 publish to QoS 1 subscriber delivers at QoS 0 (effective = min)."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos-min"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/minqos", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 0
        await publish("123456789012", "test/minqos", b"qos0-msg", qos=0)

        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert qos == 0
        assert packet_id is None  # No packet ID for QoS 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_qos1_publish_to_qos0_subscriber_delivers_at_qos0():
    """QoS 1 publish to QoS 0 subscriber delivers at QoS 0 (effective = min)."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("qos0-sub"))

        # Subscribe at QoS 0
        body = _build_subscribe_body(1, [("test/downgrade", 0)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/downgrade", b"downgraded", qos=1)

        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert qos == 0
        assert packet_id is None

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_qos1_delivery_tracks_in_flight():
    """QoS 1 delivery stores message in _in_flight dict."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("inflight"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/inflight", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/inflight", b"tracked", qos=1)

        # Should have one in-flight message
        assert len(session._in_flight) == 1
        pid = list(session._in_flight.keys())[0]
        msg = session._in_flight[pid]
        assert msg.topic == "test/inflight"
        assert msg.payload == b"tracked"
        assert msg.retransmit_count == 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_packet_ids_are_monotonically_increasing():
    """Packet IDs increment monotonically."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")

        ids = [session._alloc_packet_id() for _ in range(5)]
        assert ids == [1, 2, 3, 4, 5]

    asyncio.run(_run())
    reset()


def test_packet_ids_wrap_at_65535():
    """Packet IDs wrap from 65535 back to 1."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")
        session._next_pid = 65535

        pid1 = session._alloc_packet_id()
        pid2 = session._alloc_packet_id()
        assert pid1 == 65535
        assert pid2 == 1  # Wraps back to 1

    asyncio.run(_run())
    reset()


# ---------------------------------------------------------------------------
# Task 18.3: PUBACK handling and retransmission
# ---------------------------------------------------------------------------


def test_puback_removes_from_in_flight():
    """PUBACK with matching packet ID removes message from _in_flight."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("puback-test"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/puback", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/puback", b"ack-me", qos=1)

        assert len(session._in_flight) == 1
        pid = list(session._in_flight.keys())[0]

        # Send PUBACK
        puback_body = struct.pack("!H", pid)
        result = await session.handle_packet(PKT_PUBACK, 0, puback_body)
        assert result is True
        assert len(session._in_flight) == 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_puback_unknown_packet_id_is_ignored():
    """PUBACK for unknown packet ID does not crash."""
    reset()

    async def _run():
        send, _ = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("puback-unknown"))

        # Send PUBACK for non-existent packet ID
        puback_body = struct.pack("!H", 999)
        result = await session.handle_packet(PKT_PUBACK, 0, puback_body)
        assert result is True
        assert len(session._in_flight) == 0

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_retransmit_task_started_on_qos1_delivery():
    """Retransmit background task is started when QoS 1 message is delivered."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("retransmit"))

        assert session._retransmit_task is None

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/retransmit", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/retransmit", b"retry-me", qos=1)

        # Retransmit task should be started
        assert session._retransmit_task is not None
        assert not session._retransmit_task.done()

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_cleanup_cancels_retransmit_task():
    """cleanup() cancels the retransmit background task."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("cleanup-rt"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/cleanup", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1 to start retransmit task
        await publish("123456789012", "test/cleanup", b"clean", qos=1)
        task = session._retransmit_task
        assert task is not None

        await session.cleanup()
        assert session._retransmit_task is None
        assert task.done() or task.cancelled()

    asyncio.run(_run())
    reset()


def test_cleanup_clears_in_flight():
    """cleanup() clears the _in_flight dict."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("cleanup-if"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/clear", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/clear", b"clear-me", qos=1)
        assert len(session._in_flight) == 1

        await session.cleanup()
        assert len(session._in_flight) == 0

    asyncio.run(_run())
    reset()


def test_retransmit_sends_dup_flag():
    """Retransmission sends PUBLISH with DUP flag set."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("dup-test"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/dup", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish at QoS 1
        await publish("123456789012", "test/dup", b"dup-payload", qos=1)

        # Verify initial publish has DUP=False
        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        assert publishes[0][4] is False  # dup flag

        # Manually trigger retransmission by manipulating sent_at
        pid = list(session._in_flight.keys())[0]
        msg = session._in_flight[pid]
        # Set sent_at far in the past to trigger retransmit
        msg.sent_at = 0

        sent.clear()

        # Run one iteration of retransmit logic manually
        import asyncio as _asyncio
        now = _asyncio.get_event_loop().time()
        for p, m in list(session._in_flight.items()):
            if now - m.sent_at >= _RETRANSMIT_INTERVAL_SECONDS:
                m.retransmit_count += 1
                m.sent_at = now
                from ministack.services.iot import _make_publish
                await session.send_bytes(
                    _make_publish(m.topic, m.payload, qos=1, packet_id=p, dup=True)
                )

        # Verify retransmitted publish has DUP=True
        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 1
        topic, payload, qos, packet_id, dup = publishes[0]
        assert topic == "test/dup"
        assert payload == b"dup-payload"
        assert qos == 1
        assert packet_id == pid
        assert dup is True

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_in_flight_message_fields():
    """_InFlightMessage stores all required fields."""
    reset()

    async def _run():
        msg = _InFlightMessage(packet_id=42, topic="sensor/data", payload=b"temp=22")
        assert msg.packet_id == 42
        assert msg.topic == "sensor/data"
        assert msg.payload == b"temp=22"
        assert msg.sent_at > 0
        assert msg.retransmit_count == 0

    asyncio.run(_run())
    reset()


def test_multiple_qos1_messages_get_unique_packet_ids():
    """Multiple QoS 1 deliveries get unique, incrementing packet IDs."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("multi-pid"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/multi", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish 3 messages at QoS 1
        await publish("123456789012", "test/multi", b"msg1", qos=1)
        await publish("123456789012", "test/multi", b"msg2", qos=1)
        await publish("123456789012", "test/multi", b"msg3", qos=1)

        publishes = _extract_publish_frames(sent)
        assert len(publishes) == 3
        pids = [p[3] for p in publishes]
        # All unique
        assert len(set(pids)) == 3
        # Monotonically increasing
        assert pids == sorted(pids)

        # All tracked in-flight
        assert len(session._in_flight) == 3

        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_puback_for_first_of_multiple_in_flight():
    """PUBACK removes only the specific packet ID from in-flight."""
    reset()

    async def _run():
        send, sent = _mock_send()
        session = _WSSession(send, "123456789012")
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("selective-ack"))

        # Subscribe at QoS 1
        body = _build_subscribe_body(1, [("test/selective", 1)])
        await session.handle_packet(PKT_SUBSCRIBE, 0x02, body)
        sent.clear()

        # Publish 3 messages
        await publish("123456789012", "test/selective", b"a", qos=1)
        await publish("123456789012", "test/selective", b"b", qos=1)
        await publish("123456789012", "test/selective", b"c", qos=1)

        assert len(session._in_flight) == 3
        pids = sorted(session._in_flight.keys())

        # ACK the middle one
        puback_body = struct.pack("!H", pids[1])
        await session.handle_packet(PKT_PUBACK, 0, puback_body)

        assert len(session._in_flight) == 2
        assert pids[1] not in session._in_flight
        assert pids[0] in session._in_flight
        assert pids[2] in session._in_flight

        await session.cleanup()

    asyncio.run(_run())
    reset()


# ----------------------------------------------------------------------
# Rule SQL SELECT projection (white-box tests for _rule_event).
# ----------------------------------------------------------------------

# Every byte value: not valid UTF-8, so it survives the rule path only if the
# payload is never text-decoded.
_BINARY_PAYLOAD = bytes(range(256))


def test_rule_event_select_star_returns_parsed_json():
    from ministack.services.iot import _rule_event

    event = _rule_event("SELECT * FROM 'telemetry'", "telemetry", b'{"temp": 22}')
    assert event == {"temp": 22}


def test_rule_event_encode_base64_round_trips_binary_payload():
    from ministack.services.iot import _rule_event

    event = _rule_event(
        "SELECT encode(*, 'base64') AS data FROM 'telemetry'",
        "telemetry",
        _BINARY_PAYLOAD,
    )
    assert list(event) == ["data"]
    assert base64.b64decode(event["data"]) == _BINARY_PAYLOAD


def test_rule_event_projects_attributes_with_aliases():
    from ministack.services.iot import _rule_event

    event = _rule_event(
        "SELECT deviceId AS id, state.temp AS temp, topic(2) AS device "
        "FROM 'sensors/+/telemetry'",
        "sensors/a1/telemetry",
        json.dumps({"deviceId": "d1", "state": {"temp": 22}}).encode(),
    )
    assert event == {"id": "d1", "temp": 22, "device": "a1"}


def test_rule_event_omits_missing_attributes():
    from ministack.services.iot import _rule_event

    event = _rule_event(
        "SELECT deviceId, absent FROM 'telemetry'", "telemetry", b'{"deviceId": "d1"}'
    )
    assert event == {"deviceId": "d1"}


def test_rule_event_select_star_skips_non_utf8_payload():
    from ministack.services.iot import _MISSING, _rule_event

    assert _rule_event("SELECT * FROM 'telemetry'", "telemetry", _BINARY_PAYLOAD) is _MISSING


def test_rule_event_aliased_star_nests_the_message():
    from ministack.services.iot import _rule_event

    event = _rule_event(
        "SELECT * AS payload FROM 'telemetry'", "telemetry", b'{"temp": 22}'
    )
    assert event == {"payload": {"temp": 22}}


def test_rule_event_from_less_basic_ingest_projects():
    """FROM is optional for Basic Ingest rules, so a FROM-less `SELECT` must
    still project rather than fall through to `*` and deliver the whole message.
    """
    from ministack.services.iot import _rule_event

    event = _rule_event(
        "SELECT deviceId AS id, temp", "$aws/rules/myrule", b'{"deviceId": "d1", "temp": 22, "extra": "x"}'
    )
    assert event == {"id": "d1", "temp": 22}


# ----------------------------------------------------------------------
# Rule SQL functions: newuuid(), replace(), clientid() (and the accepted
# but unresolvable principal() / traceid()).
# ----------------------------------------------------------------------


def test_rule_event_newuuid_projects_a_fresh_uuid():
    from ministack.services.iot import _rule_event

    first = _rule_event("SELECT newuuid() AS id FROM 't'", "t", b"{}")
    second = _rule_event("SELECT newuuid() AS id FROM 't'", "t", b"{}")
    assert uuid.UUID(first["id"])  # well-formed
    assert first["id"] != second["id"]


def test_rule_event_replace_rewrites_strings():
    from ministack.services.iot import _rule_event

    event = _rule_event(
        "SELECT replace(deviceId, 'dev-', 'unit-') AS id FROM 't'",
        "t",
        b'{"deviceId": "dev-42"}',
    )
    assert event == {"id": "unit-42"}


def test_rule_event_replace_of_missing_or_nonstring_is_omitted():
    from ministack.services.iot import _rule_event

    # Missing source attribute → Undefined → omitted.
    event = _rule_event(
        "SELECT replace(absent, 'a', 'b') AS x, deviceId FROM 't'",
        "t",
        b'{"deviceId": "d1"}',
    )
    assert event == {"deviceId": "d1"}
    # Non-string source → Undefined on AWS.
    event = _rule_event(
        "SELECT replace(count, 'a', 'b') AS x FROM 't'", "t", b'{"count": 3}'
    )
    assert event == {}


def test_rule_event_clientid_resolves_for_mqtt_and_is_omitted_for_http():
    from ministack.services.iot import _rule_event

    sql = "SELECT clientid() AS cid, temp FROM 't'"
    # MQTT publish: the broker threads the publishing client's id through.
    event = _rule_event(sql, "t", b'{"temp": 22}', client_id="sensor-7")
    assert event == {"cid": "sensor-7", "temp": 22}
    # HTTP publish: no MQTT client exists — AWS resolves clientid() to
    # Undefined, so the field is omitted from the projection.
    event = _rule_event(sql, "t", b'{"temp": 22}')
    assert event == {"temp": 22}


def test_rule_event_replace_handles_escaped_quotes():
    """`replace()` takes SQL string literals, so a quote in either argument is
    written the SQL way — doubled."""
    from ministack.services.iot import _rule_event

    event = _rule_event(
        "SELECT replace(note, 'it''s', 'it is') AS n FROM 't'",
        "t",
        b'{"note": "it\'s on"}',
    )
    assert event == {"n": "it is on"}


def test_rule_event_principal_and_traceid_accepted_but_unresolved(caplog):
    """Both are real AWS functions, so a rule using them is stored rather than
    failing a stack that deploys on AWS — but this publish path carries no
    certificate identity or trace id, so they warn like any other function the
    evaluator does not implement."""
    from ministack.services import iot as iot_module
    from ministack.services.iot import _rule_event, _validate_rule_sql

    sql = "SELECT principal() AS p, traceid() AS t, temp FROM 't'"
    assert _validate_rule_sql(sql) is None
    # The evaluator warns once per function name for the life of the process.
    iot_module._warned_sql_funcs.clear()
    with caplog.at_level(logging.WARNING, logger="iot"):
        assert _rule_event(sql, "t", b'{"temp": 1}') == {"temp": 1}
    warned = " ".join(r.message for r in caplog.records)
    assert "principal()" in warned and "traceid()" in warned


def test_eval_where_clientid_reaches_every_leaf_form():
    """clientid() is an operand like any other, so it resolves inside the whole
    WHERE grammar, not just a plain comparison."""
    from ministack.services.iot import _eval_where, _rule_message

    raw = b'{"temp": 1}'
    message = _rule_message(raw)
    for pred, expected in (
        ("clientid() = 'sensor-7'", True),
        ("clientid() IN ('sensor-7', 'sensor-8')", True),
        ("clientid() IN ('sensor-8')", False),
        ("clientid() LIKE 'sensor-%'", True),
        ("isUndefined(clientid())", False),
    ):
        assert _eval_where(pred, "t", raw, message, "sensor-7") is expected
    # An HTTP publish carries no client id, so every one of them fails closed.
    for pred in (
        "clientid() = 'sensor-7'",
        "clientid() IN ('sensor-7')",
        "clientid() LIKE 'sensor-%'",
    ):
        assert _eval_where(pred, "t", raw, message) is False
    assert _eval_where("isUndefined(clientid())", "t", raw, message) is True


def test_broker_publish_threads_client_id_into_rule_projection(monkeypatch):
    """`clientid()` sees the WS client's id via broker_publish, end to end."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    iot_module._topic_rules.set_scoped(
        account_id,
        _TEST_REGION,
        "cid_rule",
        {
            "ruleName": "cid_rule",
            "sql": "SELECT clientid() AS cid FROM 'sensors/#'",
            "ruleDisabled": False,
            "actions": [{"lambda": {"functionArn": "arn:aws:lambda:us-east-1:123456789012:function:sink"}}],
        },
    )
    dispatched = []
    monkeypatch.setattr(
        iot_module,
        "_dispatch_rule_to_lambda",
        lambda account, region, arn, event: dispatched.append(event),
    )

    async def _run():
        # A WS PUBLISH passes the connected client's id...
        send, _sent = _mock_send()
        session = _WSSession(send, account_id)
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("sensor-7"))
        await session.handle_packet(
            PKT_PUBLISH, 0, _encode_string("sensors/door") + b'{"n": 1}'
        )
        await session.cleanup()
        # ...while the HTTP publish path passes none.
        await publish(account_id, "sensors/door", b'{"n": 2}')

    try:
        asyncio.run(_run())
        assert dispatched == [{"cid": "sensor-7"}, {}]
    finally:
        iot_module._topic_rules.clear()
        reset()


# ----------------------------------------------------------------------
# Rule SQL WHERE clause (white-box tests for _rule_where_clause /
# _eval_where / _validate_rule_sql).
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("SELECT * FROM 'a/b' WHERE state = 'on'", "state = 'on'"),
        ("SELECT * FROM 'a/b'", ""),
        ("SELECT temp WHERE temp > 20", "temp > 20"),  # FROM-less Basic Ingest form
        ("SELECT temp", ""),
        # A WHERE-ish string inside the topic filter is not a predicate.
        ("SELECT * FROM 'a/WHERE x/b'", ""),
    ],
)
def test_rule_where_clause_extraction(sql, expected):
    from ministack.services.iot import _rule_where_clause

    assert _rule_where_clause(sql) == expected


@pytest.mark.parametrize(
    ("pred", "payload", "expected"),
    [
        # equality (both spellings) on strings and numbers
        ("state = 'on'", {"state": "on"}, True),
        ("state == 'on'", {"state": "on"}, True),
        ("state = 'on'", {"state": "off"}, False),
        ("temp = 22", {"temp": 22}, True),
        ("temp = 22", {"temp": 23}, False),
        # inequality (both spellings)
        ("state <> 'on'", {"state": "off"}, True),
        ("state != 'on'", {"state": "on"}, False),
        # a predicate over a missing attribute is Undefined → never matches,
        # not even for <> (fail closed, as on AWS)
        ("absent = 'x'", {"state": "on"}, False),
        ("absent <> 'x'", {"state": "on"}, False),
        ("absent > 1", {"state": "on"}, False),
        # numeric comparisons
        ("temp > 20", {"temp": 22}, True),
        ("temp > 22", {"temp": 22}, False),
        ("temp >= 22", {"temp": 22}, True),
        ("temp < 30", {"temp": 22}, True),
        ("temp <= 21", {"temp": 22}, False),
        # ordering comparisons are numeric-only — a string operand is Undefined
        ("temp > 20", {"temp": "hot"}, False),
        # nested attribute paths
        ("state.reported.power = 'on'", {"state": {"reported": {"power": "on"}}}, True),
        ("state.reported.power = 'on'", {"state": {"reported": {"power": "off"}}}, False),
        # AND conjunction: all clauses must hold
        ("state = 'on' AND temp > 20", {"state": "on", "temp": 22}, True),
        ("state = 'on' AND temp > 20", {"state": "on", "temp": 19}, False),
        ("state = 'on' AND temp > 20", {"state": "off", "temp": 22}, False),
        # AND inside a string literal is not a conjunction
        ("mode = 'UP AND DOWN'", {"mode": "UP AND DOWN"}, True),
        # OR disjunction: any clause may carry the predicate
        ("state = 'on' OR state = 'off'", {"state": "off"}, True),
        ("state = 'on' OR state = 'off'", {"state": "idle"}, False),
        ("state = 'on' or temp > 20", {"state": "off", "temp": 22}, True),
        # an Undefined leaf is false, but an OR sibling can still carry it
        ("absent = 'x' OR state = 'on'", {"state": "on"}, True),
        ("absent = 'x' OR state = 'on'", {"state": "off"}, False),
        # precedence: OR binds loosest, so this reads (a AND b) OR c
        ("state = 'on' AND temp > 20 OR mode = 'test'",
         {"state": "off", "temp": 1, "mode": "test"}, True),
        ("state = 'on' AND temp > 20 OR mode = 'test'",
         {"state": "on", "temp": 22, "mode": "prod"}, True),
        ("state = 'on' AND temp > 20 OR mode = 'test'",
         {"state": "on", "temp": 1, "mode": "prod"}, False),
        # ...and the same predicate the other way round: c OR (a AND b)
        ("mode = 'test' OR state = 'on' AND temp > 20",
         {"state": "on", "temp": 1, "mode": "prod"}, False),
        ("mode = 'test' OR state = 'on' AND temp > 20",
         {"state": "off", "temp": 1, "mode": "test"}, True),
        # parentheses override that precedence: (a OR b) AND c
        ("(state = 'on' OR mode = 'test') AND temp > 20",
         {"state": "off", "mode": "test", "temp": 22}, True),
        ("(state = 'on' OR mode = 'test') AND temp > 20",
         {"state": "off", "mode": "test", "temp": 1}, False),
        ("(state = 'on' OR mode = 'test') AND temp > 20",
         {"state": "off", "mode": "prod", "temp": 22}, False),
        # a group wrapping the whole predicate is a no-op
        ("(state = 'on')", {"state": "on"}, True),
        ("((state = 'on'))", {"state": "on"}, True),
        # nested groups
        ("((state = 'on' OR state = 'off') AND temp > 20) OR mode = 'test'",
         {"state": "on", "temp": 22, "mode": "prod"}, True),
        ("((state = 'on' OR state = 'off') AND temp > 20) OR mode = 'test'",
         {"state": "idle", "temp": 22, "mode": "prod"}, False),
        ("((state = 'on' OR state = 'off') AND temp > 20) OR mode = 'test'",
         {"state": "idle", "temp": 1, "mode": "test"}, True),
        # AND binds tighter inside an OR branch, groups or not
        ("mode = 'test' OR (state = 'on' AND temp > 20)",
         {"state": "on", "temp": 22, "mode": "prod"}, True),
        # OR inside a string literal is not a disjunction
        ("mode = 'ON OR OFF'", {"mode": "ON OR OFF"}, True),
        ("mode = 'ON OR OFF'", {"mode": "ON"}, False),
        ("mode = 'ON OR OFF' AND temp > 20", {"mode": "ON OR OFF", "temp": 22}, True),
        # an attribute path may contain the keywords without being one
        ("android = 'yes' OR orbit = 1", {"orbit": 1}, True),
        # regexp_matches
        ("regexp_matches(serial, '^dev-[0-9]+$')", {"serial": "dev-42"}, True),
        ("regexp_matches(serial, '^dev-[0-9]+$')", {"serial": "gw-42"}, False),
        ("regexp_matches(absent, '.*')", {"serial": "dev-42"}, False),
        # booleans stay distinct from 0/1
        ("armed = 1", {"armed": True}, False),
        # IN / NOT IN over a value list
        ("state IN ('on', 'idle')", {"state": "idle"}, True),
        ("state IN ('on', 'idle')", {"state": "off"}, False),
        ("state IN('on')", {"state": "on"}, True),  # no space before the list
        ("code IN (1, 2, 3)", {"code": 2}, True),
        ("code IN (1, 2, 3)", {"code": 4}, False),
        ("state NOT IN ('on', 'idle')", {"state": "off"}, True),
        ("state NOT IN ('on', 'idle')", {"state": "on"}, False),
        ("absent IN ('on')", {"state": "on"}, False),
        ("absent NOT IN ('on')", {"state": "on"}, False),  # Undefined fails closed
        # LIKE / NOT LIKE — % matches a run, _ exactly one character
        ("state LIKE 'on%'", {"state": "online"}, True),
        ("state LIKE 'on%'", {"state": "gone"}, False),
        ("state LIKE 'o_'", {"state": "on"}, True),
        ("state LIKE 'o_'", {"state": "one"}, False),
        ("state LIKE 'on'", {"state": "on"}, True),
        # the pattern's own regex metacharacters stay literal
        ("state LIKE 'a.c'", {"state": "abc"}, False),
        ("state LIKE 'a.c'", {"state": "a.c"}, True),
        ("state NOT LIKE 'on%'", {"state": "gone"}, True),
        ("state NOT LIKE 'on%'", {"state": "online"}, False),
        # LIKE wants a String, so AWS converts an Int or a Boolean to one
        # first — the same conversion regexp_matches() gets, pinned together
        # below so the two operators cannot drift apart again.
        ("temp LIKE '2%'", {"temp": 22}, True),
        ("flag LIKE 'tr%'", {"flag": True}, True),
        ("absent LIKE '%'", {"state": "on"}, False),
        ("nickname LIKE '%'", {"nickname": None}, False),  # Null does not convert
        # BETWEEN, inclusive on both bounds — its AND is not a conjunction
        ("temp BETWEEN 20 AND 30", {"temp": 22}, True),
        ("temp BETWEEN 20 AND 30", {"temp": 20}, True),
        ("temp BETWEEN 20 AND 30", {"temp": 30}, True),
        ("temp BETWEEN 20 AND 30", {"temp": 31}, False),
        ("temp BETWEEN 20 AND 30 AND state = 'on'", {"temp": 22, "state": "on"}, True),
        ("temp BETWEEN 20 AND 30 AND state = 'on'", {"temp": 22, "state": "off"}, False),
        ("state = 'on' AND temp BETWEEN 20 AND 30", {"state": "on", "temp": 22}, True),
        ("temp BETWEEN 20 AND 30 OR state = 'on'", {"temp": 99, "state": "on"}, True),
        ("absent BETWEEN 1 AND 5", {"temp": 3}, False),
        ("temp BETWEEN 20 AND 30", {"temp": "hot"}, False),  # numeric-only
        # IS NULL / IS NOT NULL over a JSON null
        ("nickname IS NULL", {"nickname": None}, True),
        ("nickname IS NULL", {"nickname": "gw"}, False),
        ("nickname IS NOT NULL", {"nickname": "gw"}, True),
        ("nickname IS NOT NULL", {"nickname": None}, False),
        # Undefined is neither NULL nor NOT NULL — both fail closed
        ("nickname IS NULL", {"state": "on"}, False),
        ("nickname IS NOT NULL", {"state": "on"}, False),
        # a bare boolean attribute is a predicate on its own
        ("enabled", {"enabled": True}, True),
        ("enabled", {"enabled": False}, False),
        # AWS converts a value to Boolean only from "true"/"false" — a number
        # needs an explicit cast(), so it stays Undefined here.
        ("enabled", {"enabled": "true"}, True),
        ("enabled", {"enabled": "FALSE"}, False),
        ("enabled", {"enabled": 1}, False),
        ("enabled", {"enabled": "yes"}, False),
        ("enabled", {"state": "on"}, False),
        ("enabled AND temp > 20", {"enabled": True, "temp": 22}, True),
        ("enabled AND temp > 20", {"enabled": False, "temp": 22}, False),
        ("state.reported.on", {"state": {"reported": {"on": True}}}, True),
        # isUndefined() — the one predicate a missing attribute satisfies
        ("isUndefined(absent)", {"state": "on"}, True),
        ("isUndefined(state)", {"state": "on"}, False),
        ("isUndefined(state.reported)", {"state": {"reported": {}}}, False),
        ("isUndefined(absent) OR state = 'on'", {"state": "off"}, True),
        # a quote inside a string literal is escaped by doubling it
        ("note = 'it''s on'", {"note": "it's on"}, True),
        ("note = 'it''s on'", {"note": "its on"}, False),
        ("note <> 'it''s on'", {"note": "off"}, True),
        ("note IN ('it''s on', 'off')", {"note": "it's on"}, True),
        # an escaped quote does not end the literal, so this AND is not one
        ("note = 'it''s AND then'", {"note": "it's AND then"}, True),
        # NOT inverts a predicate, and binds tighter than AND
        ("NOT enabled", {"enabled": False}, True),
        ("NOT enabled", {"enabled": True}, False),
        ("NOT state = 'on'", {"state": "off"}, True),
        ("NOT state = 'on'", {"state": "on"}, False),
        ("NOT state IN ('on', 'idle')", {"state": "off"}, True),
        ("NOT temp BETWEEN 20 AND 30", {"temp": 31}, True),
        ("NOT isUndefined(x)", {"x": 1}, True),
        ("NOT isUndefined(x)", {"y": 1}, False),
        ("NOT(a = 1)", {"a": 2}, True),  # abutting its operand's bracket
        ("NOT (a = 1 OR b = 2)", {"a": 9, "b": 9}, True),
        ("NOT (a = 1 OR b = 2)", {"a": 1, "b": 9}, False),
        ("NOT NOT enabled", {"enabled": True}, True),
        ("state = 'on' AND NOT temp > 20", {"state": "on", "temp": 5}, True),
        ("state = 'on' AND NOT temp > 20", {"state": "on", "temp": 25}, False),
        # NOT of Undefined is Undefined, not true: negating a clause over an
        # absent attribute still fails closed, as on AWS.
        ("NOT state = 'on'", {"other": 1}, False),
        ("NOT enabled", {"state": "on"}, False),
        ("state = 'on' AND NOT temp > 20", {"state": "on"}, False),
        # arithmetic operands, multiplicative binding tighter and parentheses
        # regrouping — all four of these deploy on AWS
        ("(a + b) > 10", {"a": 6, "b": 5}, True),
        ("(a + b) > 10", {"a": 6, "b": 3}, False),
        ("temp * 2 > 10", {"temp": 6}, True),
        ("temp * 2 > 10", {"temp": 4}, False),
        ("temp - 2 > 10", {"temp": 13}, True),
        ("temp / 2 > 10", {"temp": 30}, True),
        ("temp % 3 = 1", {"temp": 7}, True),
        ("temp % 3 = 1", {"temp": 6}, False),
        ("a + b * c > 10", {"a": 1, "b": 3, "c": 4}, True),  # 1 + 12, not 4 * 4
        ("(a + b) * c > 10", {"a": 1, "b": 3, "c": 2}, False),  # 8, not 1 + 6
        ("a - b - c = 1", {"a": 5, "b": 3, "c": 1}, True),  # left-associative
        # a sign is not a binary operator
        ("temp > -5", {"temp": 1}, True),
        ("temp * -2 < 0", {"temp": 3}, True),
        # an Undefined or unconvertible operand makes the whole expression
        # Undefined, so the comparison over it fails closed
        ("absent + 1 > 0", {"temp": 1}, False),
        ("temp + 1 > 0", {"temp": "hot"}, False),
        # "+" is overloaded: a String operand makes it concatenation
        ("name + '!' = 'gw!'", {"name": "gw"}, True),
        # ordering converts a numeric-looking string, as AWS does
        ("temp > 10", {"temp": "22"}, True),
        ("temp > 10", {"temp": "hot"}, False),
        # ...but equality does not: a mismatched pair is simply unequal
        ("temp = 22", {"temp": "22"}, False),
        # a keyword may abut a bracket instead of whitespace
        ("(a = 1)AND(b = 2)", {"a": 1, "b": 2}, True),
        ("(a = 1)AND(b = 2)", {"a": 1, "b": 3}, False),
        ("(a = 1)OR(b = 2)", {"a": 9, "b": 2}, True),
        ("(a = 1)OR(b = 2)", {"a": 9, "b": 9}, False),
        ("((a = 1)AND(b = 2))OR(c = 3)", {"a": 9, "b": 9, "c": 3}, True),
    ],
)
def test_eval_where_truth_table(pred, payload, expected):
    from ministack.services.iot import _eval_where, _rule_message

    raw = json.dumps(payload).encode()
    assert _eval_where(pred, "sensors/a1/telemetry", raw, _rule_message(raw)) is expected


def test_eval_where_topic_function():
    from ministack.services.iot import _eval_where, _rule_message

    raw = b'{"x": 1}'
    message = _rule_message(raw)
    assert _eval_where("topic(2) = 'a1'", "sensors/a1/telemetry", raw, message) is True
    assert _eval_where("topic(2) = 'b7'", "sensors/a1/telemetry", raw, message) is False


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM 'a/b'",
        "SELECT temp AS t FROM 'a/+/b' WHERE temp > 20",
        "SELECT * FROM 'a' WHERE state = 'on' AND regexp_matches(serial, '^d')",
        "SELECT deviceId, temp WHERE temp >= 1.5",  # FROM-less Basic Ingest form
        "SELECT encode(*, 'base64') AS data FROM 'bin'",
        # OR and parenthesised groups: real AWS accepts these, so a rule that
        # deploys on AWS must not be rejected here.
        "SELECT * FROM 'a' WHERE state = 'on' OR state = 'off'",
        "SELECT * FROM 'a' WHERE (state = 'on')",
        "SELECT * FROM 'a' WHERE (state = 'on' OR state = 'off') AND temp > 20",
        "SELECT * FROM 'a' WHERE ((a = 1 OR b = 2) AND (c = 3 OR d = 4)) OR e = 5",
        # ...and the rest of the WHERE grammar real AWS accepts. A working local
        # setup must not turn into a hard CreateTopicRule 400 here.
        "SELECT * FROM 'a' WHERE state IN ('on', 'idle')",
        "SELECT * FROM 'a' WHERE state IN('on')",
        "SELECT * FROM 'a' WHERE state NOT IN ('on')",
        "SELECT * FROM 'a' WHERE state LIKE 'on%'",
        "SELECT * FROM 'a' WHERE state NOT LIKE 'on%'",
        "SELECT * FROM 'a' WHERE temp BETWEEN 1 AND 5",
        "SELECT * FROM 'a' WHERE temp BETWEEN 1 AND 5 AND state = 'on'",
        "SELECT * FROM 'a' WHERE nickname IS NULL",
        "SELECT * FROM 'a' WHERE nickname IS NOT NULL",
        "SELECT * FROM 'a' WHERE isUndefined(state)",
        "SELECT * FROM 'a' WHERE enabled",  # a bare boolean attribute
        "SELECT * FROM 'a' WHERE note = 'it''s on'",  # escaped quote
        "SELECT * FROM 'a' WHERE (a = 1)AND(b = 2)",  # no space around the keyword
        # NOT and arithmetic: all of these create a working rule on real AWS, so
        # rejecting them here would fail a stack that deploys.
        "SELECT * FROM 'a' WHERE NOT enabled",
        "SELECT * FROM 'a' WHERE NOT state = 'on'",
        "SELECT * FROM 'a' WHERE NOT(state = 'on')",
        "SELECT * FROM 'a' WHERE state = 'on' AND NOT temp > 20",
        "SELECT * FROM 'a' WHERE NOT state IN ('on', 'idle')",
        "SELECT * FROM 'a' WHERE (a + b) > 10",
        "SELECT * FROM 'a' WHERE temp * 2 > 10",
        "SELECT * FROM 'a' WHERE temp - 2 > 10",
        "SELECT * FROM 'a' WHERE temp / 2 > 10",
        "SELECT * FROM 'a' WHERE id % 2 = 0",
        "SELECT * FROM 'a' WHERE a + b * c > 10",
        "SELECT * FROM 'a' WHERE temp > -5",
        # A call this evaluator does not implement is still accepted: AWS's
        # function library is larger, and rejecting it would fail a stack that
        # deploys on AWS. put_topic_rule warns instead.
        "SELECT * FROM 'a' WHERE bogus_fn(x) = 1",
        "SELECT get_thing_shadow('t') AS s FROM 'a'",
    ],
)
def test_validate_rule_sql_accepts_supported_grammar(sql):
    from ministack.services.iot import _validate_rule_sql

    assert _validate_rule_sql(sql) is None


@pytest.mark.parametrize(
    "sql",
    [
        "this is not sql",
        "",
        "SELECT * FROM topic_without_quotes",
        "SELECT * FROM 'a' WHERE regexp_matches(serial, '[unbalanced')",
        "SELECT * FROM 'a' WHERE 42",  # a literal is not a predicate
        "SELECT * FROM 'a' WHERE state LIKE on",  # LIKE needs a quoted pattern
        "SELECT * FROM 'a' WHERE state IN 'on'",  # ...and IN a bracketed list
        "SELECT * FROM 'a' WHERE state IN ()",
        "SELECT * FROM 'a' WHERE temp BETWEEN 1",  # BETWEEN needs both bounds
        "SELECT * FROM 'a' WHERE nickname IS EMPTY",
        "SELECT * FROM 'a' WHERE note = 'unterminated",
        # unbalanced parentheses stay a parse error rather than being guessed at
        "SELECT * FROM 'a' WHERE (state = 'on'",
        "SELECT * FROM 'a' WHERE state = 'on')",
        "SELECT * FROM 'a' WHERE (state = 'on' AND (temp > 20)",
        "SELECT * FROM 'a' WHERE ()",
        # an empty operand around a keyword is not a predicate
        "SELECT * FROM 'a' WHERE state = 'on' OR",
        "SELECT * FROM 'a' WHERE OR state = 'on'",
        "SELECT * FROM 'a' WHERE state = 'on' AND",
        "SELECT * FROM 'a' WHERE NOT",  # NOT needs a predicate to invert
        "SELECT * FROM 'a' WHERE temp * > 10",  # ...and an operator two operands
        "SELECT * FROM 'a' WHERE temp * 2 +",
    ],
)
def test_validate_rule_sql_rejects_unparseable(sql):
    from ministack.services.iot import _validate_rule_sql

    assert _validate_rule_sql(sql) is not None


def test_put_topic_rule_rejects_unevaluable_sql():
    """Enforcement lives in `put_topic_rule`, so every door into the store — the
    IoT API and the CloudFormation provisioner alike — is covered by one check."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    try:
        with pytest.raises(iot_module.RuleSqlError):
            iot_module.put_topic_rule(
                "bad_rule", {"sql": "SELECT * FROM 'a' WHERE (state = 'on'"}
            )
        assert iot_module._topic_rules.get("bad_rule") is None
    finally:
        iot_module._topic_rules.clear()
        reset()


def test_put_topic_rule_warns_for_unimplemented_sql_function(caplog):
    """AWS's function library is larger than this evaluator's, so a call it does
    not implement is stored rather than failing a stack that deploys on AWS —
    but it warns, because the rule will silently never match on it."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    try:
        with caplog.at_level(logging.WARNING, logger="iot"):
            iot_module.put_topic_rule(
                "fn_rule", {"sql": "SELECT * FROM 'a' WHERE bogus_fn(x) = 1"}
            )
        assert iot_module._topic_rules.get("fn_rule") is not None
        assert any("bogus_fn()" in r.message for r in caplog.records)
        # A function it does implement says nothing.
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger="iot"):
            iot_module.put_topic_rule(
                "ok_rule", {"sql": "SELECT * FROM 'a' WHERE topic(2) = 'x'"}
            )
        assert not caplog.records
    finally:
        iot_module._topic_rules.clear()
        reset()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM 'a' WHERE (state = 'on')",
        "SELECT * FROM 'a' WHERE (a = 1) AND (b = 2)",
        "SELECT * FROM 'a' WHERE state IN ('on', 'idle')",
        "SELECT * FROM 'a' WHERE state NOT IN ('on')",
        "SELECT * FROM 'a' WHERE NOT (a = 1)",
        "SELECT * FROM 'a' WHERE temp BETWEEN 1 AND (2 + 3)",
    ],
)
def test_put_topic_rule_does_not_mistake_a_keyword_for_a_function(caplog, sql):
    """A SQL keyword may sit in front of a bracket without opening a call.

    Scanning for `<name>(` alone read `WHERE (`, `IN (` and `AND (` as calls and
    warned about a missing where()/in()/and() — noise on perfectly ordinary
    rules, and exactly the kind that trains people to ignore the warning that
    matters.
    """
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    try:
        with caplog.at_level(logging.WARNING, logger="iot"):
            iot_module.put_topic_rule("kw_rule", {"sql": sql})
        assert not caplog.records
    finally:
        iot_module._topic_rules.clear()
        reset()


def test_reset_clears_the_sql_function_warning_ledger():
    """`_warned_sql_funcs` makes the unimplemented-function warning fire once
    per name, so it is module state the service reset has to clear — otherwise
    the second test to store the same rule silently gets no warning."""
    from ministack.services import iot as iot_module

    try:
        iot_module._warn_unimplemented_sql_function("get_thing_shadow")
        assert iot_module._warned_sql_funcs
        iot_module.reset()
        assert not iot_module._warned_sql_funcs
    finally:
        iot_module.reset()


def test_like_and_regexp_matches_convert_operands_alike():
    """The two string operators take the same operand policy.

    They used to disagree — `regexp_matches` coerced with str() while LIKE
    demanded a Python string — so the same Int payload matched one and not the
    other. Both now run AWS's documented "to String" conversion, which renders
    an Int and a Boolean (lowercase, as JSON does) and leaves Null Undefined.
    """
    from ministack.services.iot import _eval_where, _rule_message

    def both(attr, payload):
        raw = json.dumps(payload).encode()
        message = _rule_message(raw)
        return (
            _eval_where(f"{attr} LIKE '2%'", "a/b", raw, message),
            _eval_where(f"regexp_matches({attr}, '^2')", "a/b", raw, message),
        )

    assert both("temp", {"temp": 22}) == (True, True)
    assert both("temp", {"temp": "22"}) == (True, True)
    assert both("temp", {"temp": 22.5}) == (True, True)
    assert both("temp", {"temp": 31}) == (False, False)
    assert both("temp", {"temp": None}) == (False, False)
    assert both("temp", {"other": 1}) == (False, False)


def test_eval_where_warns_when_a_stored_predicate_cannot_be_parsed(caplog):
    """Validation keeps these out of the store, but state restored from an older
    MiniStack can carry one — it must be loud, not a silent non-match."""
    from ministack.services.iot import _eval_where, _rule_message

    raw = b'{"state": "on"}'
    with caplog.at_level(logging.WARNING, logger="iot_broker"):
        assert _eval_where("(state = 'on'", "a/b", raw, _rule_message(raw)) is False
    assert any("cannot be parsed" in r.message for r in caplog.records)


def test_run_rule_actions_where_gates_dispatch(monkeypatch):
    """A WHERE-carrying rule dispatches only the matching publishes."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    iot_module._topic_rules.set_scoped(
        account_id,
        _TEST_REGION,
        "gated_rule",
        {
            "ruleName": "gated_rule",
            "sql": "SELECT * FROM 'sensors/#' WHERE state = 'on'",
            "ruleDisabled": False,
            "actions": [{"lambda": {"functionArn": "arn:aws:lambda:us-east-1:123456789012:function:sink"}}],
        },
    )
    dispatched = []
    monkeypatch.setattr(
        iot_module,
        "_dispatch_rule_to_lambda",
        lambda account, region, arn, event: dispatched.append(event),
    )

    async def _run():
        await publish(account_id, "sensors/door", b'{"state": "on", "n": 1}')
        await publish(account_id, "sensors/door", b'{"state": "off", "n": 2}')
        await publish(account_id, "sensors/door", b'{"n": 3}')  # attribute missing

    try:
        asyncio.run(_run())
        assert dispatched == [{"state": "on", "n": 1}]
    finally:
        iot_module._topic_rules.clear()
        reset()


# ----------------------------------------------------------------------
# Rule action dispatch: republish / dynamoDBv2 / sns (white-box).
# ----------------------------------------------------------------------


def _put_rule(account_id, name, sql, actions):
    from ministack.services import iot as iot_module

    iot_module._topic_rules.set_scoped(
        account_id,
        _TEST_REGION,
        name,
        {"ruleName": name, "sql": sql, "ruleDisabled": False, "actions": actions},
    )


def test_rule_republish_action_delivers_projected_event():
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    _put_rule(
        account_id,
        "repub_rule",
        "SELECT temp AS t FROM 'sensors/+/telemetry' WHERE temp > 20",
        [{"republish": {"topic": "filtered/telemetry", "qos": 1}}],
    )
    received = []

    async def _run():
        async def _collect(topic, payload, qos):
            received.append((topic, payload, qos))

        await subscribe(account_id, "filtered/telemetry", _collect, granted_qos=1)
        await publish(account_id, "sensors/a1/telemetry", b'{"temp": 22}')
        await publish(account_id, "sensors/a1/telemetry", b'{"temp": 19}')  # WHERE gates

    try:
        asyncio.run(_run())
        assert len(received) == 1
        topic, payload, _qos = received[0]
        assert topic == "filtered/telemetry"
        assert json.loads(payload) == {"t": 22}
    finally:
        iot_module._topic_rules.clear()
        reset()


def test_rule_republish_onto_own_topic_terminates():
    """A rule republishing onto its own topic filter is user error, but the
    broker must cut the chain instead of recursing forever."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    _put_rule(
        account_id,
        "loop_rule",
        "SELECT * FROM 'loop/topic'",
        [{"republish": {"topic": "loop/topic"}}],
    )
    received = []

    async def _run():
        async def _collect(topic, payload, qos):
            received.append(payload)

        await subscribe(account_id, "loop/topic", _collect)
        await publish(account_id, "loop/topic", b'{"n": 1}')

    try:
        asyncio.run(_run())
        # The original publish plus one republish per allowed depth.
        assert len(received) == 1 + iot_module._MAX_REPUBLISH_DEPTH
    finally:
        iot_module._topic_rules.clear()
        reset()


def test_rule_action_failure_does_not_kill_the_loop(monkeypatch):
    """A failing action is logged and the remaining actions still dispatch."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    _put_rule(
        account_id,
        "multi_rule",
        "SELECT * FROM 'multi/topic'",
        [
            {"sns": {"targetArn": "arn:aws:sns:us-east-1:123456789012:absent-topic"}},
            {"dynamoDBv2": {"putItem": {"tableName": "absent-table"}}},
            {"lambda": {"functionArn": "arn:aws:lambda:us-east-1:123456789012:function:sink"}},
        ],
    )
    dispatched = []
    monkeypatch.setattr(
        iot_module,
        "_dispatch_rule_to_lambda",
        lambda account, region, arn, event: dispatched.append(event),
    )

    def _boom(*args, **kwargs):
        raise RuntimeError("dispatch blew up")

    # First action targets a missing SNS topic (raises on its own), second one
    # is made to raise outright — the lambda action after both must still
    # dispatch.
    monkeypatch.setattr(iot_module, "_dispatch_rule_dynamodb", _boom)

    async def _run():
        await publish(account_id, "multi/topic", b'{"ok": 1}')

    try:
        asyncio.run(_run())
        assert dispatched == [{"ok": 1}]
    finally:
        iot_module._topic_rules.clear()
        reset()


def test_rule_sns_action_writes_a_full_sns_message_record():
    """The dispatcher publishes through SNS's own internal path, so the stored
    record carries every field an HTTP `Publish` writes — a hand-rolled append
    here would quietly drop the ones a subscription filter policy reads."""
    from ministack.services import iot as iot_module
    from ministack.services import sns as sns_module

    reset()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    topic_arn = f"arn:aws:sns:{_TEST_REGION}:{account_id}:rule-sink"
    sns_module._topics.set_scoped(
        account_id,
        _TEST_REGION,
        topic_arn,
        {"arn": topic_arn, "messages": [], "subscriptions": [], "attributes": {}},
    )
    _put_rule(
        account_id,
        "sns_rule",
        "SELECT temp FROM 'sensors/+/telemetry'",
        [{"sns": {"targetArn": topic_arn}}],
    )

    async def _run():
        await publish(account_id, "sensors/a1/telemetry", b'{"temp": 22, "n": 1}')

    try:
        asyncio.run(_run())
        stored = sns_module._topics.get_scoped(account_id, _TEST_REGION, topic_arn)
        assert len(stored["messages"]) == 1
        record = stored["messages"][0]
        assert json.loads(record["message"]) == {"temp": 22}
        assert set(record) == {
            "id",
            "message",
            "subject",
            "message_structure",
            "message_attributes",
            "timestamp",
        }
    finally:
        sns_module._topics.clear()
        iot_module._topic_rules.clear()
        reset()


def test_rule_error_action_runs_when_an_action_fails(monkeypatch):
    """AWS invokes the rule's errorAction when an action fails, with the failure
    document — without it the only trace of a broken pipeline is a local log
    line nobody reads."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    iot_module._topic_rules.set_scoped(
        account_id,
        _TEST_REGION,
        "err_rule",
        {
            "ruleName": "err_rule",
            "sql": "SELECT * FROM 'err/topic'",
            "ruleDisabled": False,
            "actions": [{"dynamoDBv2": {"putItem": {"tableName": "absent"}}}],
            "errorAction": {"republish": {"topic": "err/dlq"}},
        },
    )

    def _boom(*args, **kwargs):
        raise RuntimeError("dispatch blew up")

    monkeypatch.setattr(iot_module, "_dispatch_rule_dynamodb", _boom)
    received = []

    async def _run():
        async def _collect(topic, payload, qos):
            received.append(json.loads(payload))

        await subscribe(account_id, "err/dlq", _collect)
        await publish(account_id, "err/topic", b'{"n": 1}')

    try:
        asyncio.run(_run())
        assert len(received) == 1
        doc = received[0]
        assert doc["ruleName"] == "err_rule"
        assert doc["topic"] == "err/topic"
        assert base64.b64decode(doc["base64OriginalPayload"]) == b'{"n": 1}'
        assert doc["failures"] == [
            {"action": "dynamoDBv2", "errorMessage": "RuntimeError: dispatch blew up"}
        ]
    finally:
        iot_module._topic_rules.clear()
        reset()


@pytest.mark.parametrize(
    ("action", "action_type", "error_fragment"),
    [
        (
            {"dynamoDBv2": {"putItem": {"tableName": "absent-table"}}},
            "dynamoDBv2",
            "absent-table",
        ),
        (
            {"sns": {"targetArn": "arn:aws:sns:us-east-1:123456789012:absent-topic"}},
            "sns",
            "absent-topic",
        ),
    ],
)
def test_rule_error_action_runs_on_an_undeliverable_destination(
    action, action_type, error_fragment
):
    """The failure that actually happens locally is a destination that is not
    there, and it has to reach the errorAction.

    Both dispatchers used to log-and-return for it, so the errorAction only ever
    ran for a monkeypatched raise — the emulator reported a healthy pipeline
    while dropping every message.
    """
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    iot_module._topic_rules.set_scoped(
        account_id,
        _TEST_REGION,
        "undeliverable_rule",
        {
            "ruleName": "undeliverable_rule",
            "sql": "SELECT * FROM 'err/topic'",
            "ruleDisabled": False,
            "actions": [action],
            "errorAction": {"republish": {"topic": "err/dlq"}},
        },
    )
    received = []

    async def _run():
        async def _collect(topic, payload, qos):
            received.append(json.loads(payload))

        await subscribe(account_id, "err/dlq", _collect)
        await publish(account_id, "err/topic", b'{"n": 1}')

    try:
        asyncio.run(_run())
        assert len(received) == 1
        doc = received[0]
        assert doc["ruleName"] == "undeliverable_rule"
        assert base64.b64decode(doc["base64OriginalPayload"]) == b'{"n": 1}'
        assert len(doc["failures"]) == 1
        failure = doc["failures"][0]
        assert failure["action"] == action_type
        assert error_fragment in failure["errorMessage"]
    finally:
        iot_module._topic_rules.clear()
        reset()


def test_rule_error_action_absent_is_a_no_op(monkeypatch):
    """A rule without an errorAction still just logs the failure."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    _put_rule(
        account_id,
        "noerr_rule",
        "SELECT * FROM 'noerr/topic'",
        [{"dynamoDBv2": {"putItem": {"tableName": "absent"}}}],
    )

    def _boom(*args, **kwargs):
        raise RuntimeError("dispatch blew up")

    monkeypatch.setattr(iot_module, "_dispatch_rule_dynamodb", _boom)

    async def _run():
        await publish(account_id, "noerr/topic", b'{"n": 1}')

    try:
        asyncio.run(_run())  # no exception escapes
    finally:
        iot_module._topic_rules.clear()
        reset()


def test_ddb_attribute_value_mapping():
    from ministack.services.iot import _ddb_attribute_value

    assert _ddb_attribute_value(True) == {"BOOL": True}
    assert _ddb_attribute_value(3) == {"N": "3"}
    assert _ddb_attribute_value(2.5) == {"N": "2.5"}
    assert _ddb_attribute_value(None) == {"NULL": True}
    assert _ddb_attribute_value({"a": 1}) == {"S": '{"a": 1}'}
    assert _ddb_attribute_value([1, 2]) == {"S": "[1, 2]"}
    assert _ddb_attribute_value("x") == {"S": "x"}
# Fleet-index connectivity (white-box: transitions and persistence)
# ----------------------------------------------------------------------

from ministack.services.iot import (  # noqa: E402
    _thing_connectivity,
)
from ministack.services.iot import (  # noqa: E402
    get_state as _iot_get_state,
)
from ministack.services.iot import (  # noqa: E402
    restore_state as _iot_restore_state,
)

_ACCT = "123456789012"


def _connectivity_of(client_id):
    return _thing_connectivity(_ACCT, _TEST_REGION, client_id)


def test_connectivity_reports_connected_while_session_is_live():
    reset()

    async def _run():
        send, _sent = _mock_send()
        session = _WSSession(send, _ACCT)
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("dev-live"))
        doc = _connectivity_of("dev-live")
        assert doc["connected"] is True
        assert doc["timestamp"] > 0
        # A live session has no disconnect reason to report.
        assert "disconnectReason" not in doc
        await session.cleanup()

    asyncio.run(_run())
    reset()


def test_connectivity_disconnect_reason_distinguishes_clean_from_dropped():
    reset()

    async def _run():
        send, _sent = _mock_send()
        clean = _WSSession(send, _ACCT)
        await clean.handle_packet(PKT_CONNECT, 0, _build_connect_body("dev-clean"))
        await clean.handle_packet(PKT_DISCONNECT, 0, b"")
        await clean.cleanup()
        doc = _connectivity_of("dev-clean")
        assert doc["connected"] is False
        assert doc["disconnectReason"] == "CLIENT_INITIATED_DISCONNECT"

        send2, _sent2 = _mock_send()
        dropped = _WSSession(send2, _ACCT)
        await dropped.handle_packet(PKT_CONNECT, 0, _build_connect_body("dev-dropped"))
        # No DISCONNECT packet: the transport simply went away.
        await dropped.cleanup()
        assert _connectivity_of("dev-dropped")["disconnectReason"] == "CONNECTION_LOST"

    asyncio.run(_run())
    reset()


def test_connectivity_timestamp_advances_across_transitions():
    reset()

    async def _run():
        send, _sent = _mock_send()
        session = _WSSession(send, _ACCT)
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("dev-clock"))
        connected_at = _connectivity_of("dev-clock")["timestamp"]
        time.sleep(0.01)
        await session.cleanup()
        assert _connectivity_of("dev-clock")["timestamp"] > connected_at

    asyncio.run(_run())
    reset()


def test_connectivity_survives_takeover_by_a_duplicate_client_id():
    """The loser's late second cleanup must not disconnect the live session.

    Taking over a client id cleans the old session up once from the CONNECT
    path; its socket close then drives the same session through cleanup again.
    That second pass has to be a no-op, or the thing reads as offline while a
    session is sitting there connected.
    """
    reset()

    async def _run():
        send1, _s1 = _mock_send()
        first = _WSSession(send1, _ACCT)
        await first.handle_packet(PKT_CONNECT, 0, _build_connect_body("dev-dup"))

        send2, _s2 = _mock_send()
        second = _WSSession(send2, _ACCT)
        await second.handle_packet(PKT_CONNECT, 0, _build_connect_body("dev-dup"))
        assert _connectivity_of("dev-dup")["connected"] is True

        # The evicted session's own handler finally notices and cleans up.
        await first.cleanup()
        doc = _connectivity_of("dev-dup")
        assert doc["connected"] is True
        # The takeover is the transition that produced this live session, so
        # the reason it ended the previous one survives the registration.
        assert doc["disconnectReason"] == "DUPLICATE_CLIENTID"

        await second.cleanup()
        assert _connectivity_of("dev-dup")["connected"] is False

    asyncio.run(_run())
    reset()


def test_connectivity_reconnect_clears_the_previous_disconnect_reason():
    """An ordinary reconnect is a transition of its own, so nothing lingers.

    Only a takeover carries a reason onto the new session, because there the
    eviction *is* the connect. A device that dropped and dialled back in gets
    a clean record, not the ghost of how the last session died.
    """
    reset()

    async def _run():
        send1, _s1 = _mock_send()
        first = _WSSession(send1, _ACCT)
        await first.handle_packet(PKT_CONNECT, 0, _build_connect_body("dev-again"))
        await first.cleanup()  # dropped: CONNECTION_LOST
        assert _connectivity_of("dev-again")["disconnectReason"] == "CONNECTION_LOST"

        send2, _s2 = _mock_send()
        second = _WSSession(send2, _ACCT)
        await second.handle_packet(PKT_CONNECT, 0, _build_connect_body("dev-again"))
        doc = _connectivity_of("dev-again")
        assert doc["connected"] is True
        assert "disconnectReason" not in doc
        await second.cleanup()

    asyncio.run(_run())
    reset()


def test_connectivity_is_not_persisted_and_cannot_restore_as_connected():
    """A snapshot must not be able to claim a thing is online after a restart.

    Nothing reconnects when the process comes back, so a restored ``connected:
    true`` would be a claim with no session behind it.
    """
    reset()

    async def _run():
        send, _sent = _mock_send()
        session = _WSSession(send, _ACCT)
        await session.handle_packet(PKT_CONNECT, 0, _build_connect_body("dev-snap"))
        assert _connectivity_of("dev-snap")["connected"] is True

        snapshot = _iot_get_state()
        assert "connectivity" not in json.dumps(snapshot["mqtt_broker"])

        # Restart: state comes back, sessions do not.
        await session.cleanup()
        reset()
        _iot_restore_state(snapshot)
        doc = _connectivity_of("dev-snap")
        assert doc["connected"] is False
        # And nothing was invented to fill the gap.
        assert "timestamp" not in doc

    asyncio.run(_run())
    reset()
# ---------------------------------------------------------------------------
# CA-certificate registry + JITR (registration code, RegisterCACertificate
# and friends, and the $aws/events/certificates/registered/{caId} event)
# ---------------------------------------------------------------------------


def _generate_ca_and_leaves(count: int = 1) -> tuple[str, list[str]]:
    """A fresh CA PEM and ``count`` leaf certificate PEMs signed by it.

    Leaves have to be signed by the CA they are registered under —
    ``RegisterCertificate`` verifies the ``caCertificatePem`` claim — so a test
    that needs two device certificates under one CA must take them from the
    same CA key, not from two independent ``_generate_ca_and_leaf`` calls.
    """
    pytest.importorskip("cryptography")
    from ministack.core.x509_utils import generate_ca, sign_leaf_certificate

    ca_pem, ca_key_pem = generate_ca(common_name=_unique("jitr-test-ca"))
    leaves = [
        sign_leaf_certificate(
            ca_cert_pem=ca_pem,
            ca_key_pem=ca_key_pem,
            common_name=_unique("jitr-device"),
        )[0]
        for _ in range(count)
    ]
    return ca_pem, leaves


def _generate_ca_and_leaf() -> tuple[str, str]:
    """A fresh CA PEM and a leaf certificate PEM signed by it."""
    ca_pem, leaves = _generate_ca_and_leaves()
    return ca_pem, leaves[0]


def test_iot_registration_code_stable_until_deleted(iot_client):
    code = iot_client.get_registration_code()["registrationCode"]
    assert len(code) == 64
    int(code, 16)  # sha256 hex
    # Stable across calls.
    assert iot_client.get_registration_code()["registrationCode"] == code
    # DELETE discards it; the next GET mints a fresh one.
    iot_client.delete_registration_code()
    fresh = iot_client.get_registration_code()["registrationCode"]
    assert len(fresh) == 64
    assert fresh != code


def test_iot_ca_certificate_lifecycle(iot_client):
    ca_pem, _leaf = _generate_ca_and_leaf()
    resp = iot_client.register_ca_certificate(
        caCertificate=ca_pem, setAsActive=True, allowAutoRegistration=True
    )
    ca_id = resp["certificateId"]
    assert resp["certificateArn"].endswith(":cacert/" + ca_id)

    desc = iot_client.describe_ca_certificate(certificateId=ca_id)[
        "certificateDescription"
    ]
    assert desc["certificatePem"] == ca_pem  # verbatim
    assert desc["certificateId"] == ca_id
    assert desc["status"] == "ACTIVE"
    assert desc["autoRegistrationStatus"] == "ENABLE"
    assert desc["creationDate"]

    listing = iot_client.list_ca_certificates()["certificates"]
    entry = next(c for c in listing if c["certificateId"] == ca_id)
    assert entry["certificateArn"].endswith(":cacert/" + ca_id)
    assert entry["status"] == "ACTIVE"

    # ACTIVE CAs cannot be deleted — 406 per the service model.
    with pytest.raises(ClientError) as ei:
        iot_client.delete_ca_certificate(certificateId=ca_id)
    assert ei.value.response["Error"]["Code"] == "CertificateStateException"
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 406

    iot_client.update_ca_certificate(
        certificateId=ca_id,
        newStatus="INACTIVE",
        newAutoRegistrationStatus="DISABLE",
    )
    desc = iot_client.describe_ca_certificate(certificateId=ca_id)[
        "certificateDescription"
    ]
    assert desc["status"] == "INACTIVE"
    assert desc["autoRegistrationStatus"] == "DISABLE"

    iot_client.delete_ca_certificate(certificateId=ca_id)
    with pytest.raises(ClientError) as ei:
        iot_client.describe_ca_certificate(certificateId=ca_id)
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_iot_register_ca_certificate_duplicate_conflict(iot_client):
    ca_pem, _leaf = _generate_ca_and_leaf()
    ca_id = iot_client.register_ca_certificate(caCertificate=ca_pem)["certificateId"]
    with pytest.raises(ClientError) as ei:
        iot_client.register_ca_certificate(caCertificate=ca_pem)
    err = ei.value.response
    assert err["Error"]["Code"] == "ResourceAlreadyExistsException"
    assert err["resourceId"] == ca_id
    assert err["resourceArn"].endswith(":cacert/" + ca_id)
    iot_client.delete_ca_certificate(certificateId=ca_id)


def test_iot_jitr_registered_event_published_when_auto_registration_enabled():
    """Register a CA with auto-registration ENABLE, subscribe on
    ``$aws/events/certificates/registered/#``, register a device cert under
    that CA — the JITR lifecycle event arrives with certificateId +
    caCertificateId and DescribeCertificate resolves the signing CA.

    Runs in-process against the module broker (the WS route to the live
    server needs ``*.localhost`` DNS this environment lacks), following the
    broker-helper pattern above.
    """
    from ministack.core.responses import (
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import iot as iot_module

    ca_pem, leaf_pem = _generate_ca_and_leaf()
    account_id = "123456789012"
    received: list = []

    async def _run():
        set_request_account_id(account_id)
        set_request_region(_TEST_REGION)

        status, _, body = await iot_module.handle_request(
            "POST",
            "/cacertificate",
            {},
            json.dumps({"caCertificate": ca_pem}).encode(),
            {"setAsActive": "true", "allowAutoRegistration": "true"},
        )
        assert status == 200
        ca_id = json.loads(body)["certificateId"]

        async def _collect(topic, payload, qos):
            received.append((topic, payload, qos))

        await iot_module.broker_subscribe(
            account_id,
            _TEST_REGION,
            "$aws/events/certificates/registered/#",
            _collect,
        )

        status, _, body = await iot_module.handle_request(
            "POST",
            "/certificate/register",
            {},
            json.dumps({
                "certificatePem": leaf_pem,
                "caCertificatePem": ca_pem,
                "status": "PENDING_ACTIVATION",
            }).encode(),
            {},
        )
        assert status == 200
        cert_id = json.loads(body)["certificateId"]

        assert len(received) == 1
        topic, payload, _qos = received[0]
        assert topic == f"$aws/events/certificates/registered/{ca_id}"
        event = json.loads(payload)
        assert event["certificateId"] == cert_id
        assert event["caCertificateId"] == ca_id
        assert event["certificateStatus"] == "PENDING_ACTIVATION"
        assert event["awsAccountId"] == account_id
        assert isinstance(event["timestamp"], int)
        assert event["certificateRegistrationTimestamp"] == str(event["timestamp"])

        # A JITR Lambda resolves the signing CA via DescribeCertificate.
        status, _, body = await iot_module.handle_request(
            "GET", f"/certificates/{cert_id}", {}, b"", {}
        )
        assert status == 200
        assert (
            json.loads(body)["certificateDescription"]["caCertificateId"] == ca_id
        )

    try:
        asyncio.run(_run())
    finally:
        iot_module.reset()
        iot_module.broker_reset()


# ----------------------------------------------------------------------
# Shadow-over-MQTT bridge (white-box).
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("topic", "expected"),
    [
        # Request topics parse.
        ("$aws/things/dev1/shadow/get", ("dev1", "get", None)),
        ("$aws/things/dev1/shadow/update", ("dev1", "update", None)),
        ("$aws/things/dev1/shadow/delete", ("dev1", "delete", None)),
        ("$aws/things/dev1/shadow/name/cfg/get", ("dev1", "get", "cfg")),
        ("$aws/things/dev1/shadow/name/cfg/update", ("dev1", "update", "cfg")),
        ("$aws/things/dev1/shadow/name/cfg/delete", ("dev1", "delete", "cfg")),
        # An empty shadow name parses (so the bridge can reject it on the
        # requester's own reply topic) rather than collapsing onto classic.
        ("$aws/things/dev1/shadow/name//update", ("dev1", "update", "")),
        # Response suffixes never re-trigger the bridge.
        ("$aws/things/dev1/shadow/update/accepted", None),
        ("$aws/things/dev1/shadow/update/rejected", None),
        ("$aws/things/dev1/shadow/update/delta", None),
        ("$aws/things/dev1/shadow/update/documents", None),
        ("$aws/things/dev1/shadow/get/accepted", None),
        ("$aws/things/dev1/shadow/get/rejected", None),
        ("$aws/things/dev1/shadow/delete/accepted", None),
        ("$aws/things/dev1/shadow/name/cfg/update/accepted", None),
        ("$aws/things/dev1/shadow/name/cfg/update/delta", None),
        ("$aws/things/dev1/shadow/name/cfg/get/rejected", None),
        # Malformed / unrelated.
        ("$aws/things/dev1/shadow", None),
        ("$aws/things/dev1/shadow/name", None),
        ("$aws/things/dev1/shadow/name/cfg", None),
        ("$aws/things/dev1/shadow/frobnicate", None),
        ("$aws/things/dev1/notshadow/get", None),
        ("$aws/things", None),
        ("$aws/rules/rule1/some/topic", None),
        ("plain/topic", None),
        ("", None),
    ],
)
def test_parse_shadow_topic_truth_table(topic, expected):
    from ministack.services.iot import _parse_shadow_topic

    assert _parse_shadow_topic(topic) == expected


def test_shadow_mqtt_update_publishes_accepted_delta_documents():
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    frames: dict[str, list] = {}

    async def _run():
        async def _collect(topic, payload, qos):
            frames.setdefault(topic, []).append(json.loads(payload))

        await subscribe(
            account_id, "$aws/things/dev1/shadow/update/+", _collect, granted_qos=1
        )
        await publish(
            account_id,
            "$aws/things/dev1/shadow/update",
            json.dumps(
                {"state": {"desired": {"led": "on"}}, "clientToken": "tok-1"}
            ).encode(),
        )

    try:
        asyncio.run(_run())
        accepted = frames["$aws/things/dev1/shadow/update/accepted"][0]
        assert accepted["state"] == {"desired": {"led": "on"}}
        assert accepted["version"] == 1
        assert accepted["clientToken"] == "tok-1"

        # desired != reported → a delta document, carrying the metadata AWS
        # reports for the delta and the triggering request's clientToken.
        delta = frames["$aws/things/dev1/shadow/update/delta"][0]
        assert delta["state"] == {"led": "on"}
        assert delta["version"] == 1
        assert "timestamp" in delta
        assert delta["clientToken"] == "tok-1"
        assert set(delta["metadata"]) == {"led"}
        assert delta["metadata"]["led"]["timestamp"] == accepted["metadata"]["desired"]["led"]["timestamp"]

        docs = frames["$aws/things/dev1/shadow/update/documents"][0]
        assert docs["previous"] is None
        assert docs["current"]["version"] == 1
        assert docs["current"]["state"]["desired"] == {"led": "on"}
        assert "timestamp" in docs

        # The MQTT update landed in the same store the HTTP data plane reads.
        with iot_module.request_scope(account_id, _TEST_REGION):
            status, doc = iot_module.get_thing_shadow("dev1", "")
        assert status == 200
        assert doc["state"]["desired"] == {"led": "on"}
    finally:
        iot_module._shadows.clear()
        reset()


def test_shadow_mqtt_update_reported_in_sync_emits_no_delta():
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    frames: dict[str, list] = {}

    async def _run():
        async def _collect(topic, payload, qos):
            frames.setdefault(topic, []).append(json.loads(payload))

        await subscribe(
            account_id, "$aws/things/dev2/shadow/update/+", _collect, granted_qos=1
        )
        await publish(
            account_id,
            "$aws/things/dev2/shadow/update",
            json.dumps(
                {"state": {"desired": {"led": "on"}, "reported": {"led": "on"}}}
            ).encode(),
        )

    try:
        asyncio.run(_run())
    finally:
        iot_module.reset()
        iot_module.broker_reset()


def test_iot_jitr_no_event_when_auto_registration_disabled():
    """A CA registered without ``allowAutoRegistration`` links the device cert
    (``caCertificateId``) but publishes no registered event."""
    from ministack.core.responses import (
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import iot as iot_module

    ca_pem, leaf_pem = _generate_ca_and_leaf()
    account_id = "123456789012"
    received: list = []

    async def _run():
        set_request_account_id(account_id)
        set_request_region(_TEST_REGION)

        status, _, body = await iot_module.handle_request(
            "POST",
            "/cacertificate",
            {},
            json.dumps({"caCertificate": ca_pem}).encode(),
            {"setAsActive": "true"},
        )
        assert status == 200
        ca_id = json.loads(body)["certificateId"]

        async def _collect(topic, payload, qos):
            received.append((topic, payload, qos))

        await iot_module.broker_subscribe(
            account_id,
            _TEST_REGION,
            "$aws/events/certificates/registered/#",
            _collect,
        )

        status, _, body = await iot_module.handle_request(
            "POST",
            "/certificate/register",
            {},
            json.dumps({
                "certificatePem": leaf_pem,
                "caCertificatePem": ca_pem,
            }).encode(),
            {},
        )
        assert status == 200
        cert_id = json.loads(body)["certificateId"]
        assert received == []

        status, _, body = await iot_module.handle_request(
            "GET", f"/certificates/{cert_id}", {}, b"", {}
        )
        assert (
            json.loads(body)["certificateDescription"]["caCertificateId"] == ca_id
        )

    try:
        asyncio.run(_run())
    finally:
        iot_module.reset()
        iot_module.broker_reset()


def test_shadow_mqtt_update_rejections():
    """Invalid JSON → 400 (no token); version conflict → 409 with the token."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    rejected: list = []

    async def _run():
        async def _collect(topic, payload, qos):
            rejected.append(json.loads(payload))

        await subscribe(
            account_id, "$aws/things/dev3/shadow/update/rejected", _collect
        )
        await publish(
            account_id, "$aws/things/dev3/shadow/update", b"{not json"
        )
        await publish(
            account_id,
            "$aws/things/dev3/shadow/update",
            json.dumps({"state": {"desired": {"a": 1}}}).encode(),
        )
        await publish(
            account_id,
            "$aws/things/dev3/shadow/update",
            json.dumps(
                {"state": {"desired": {"a": 2}}, "version": 99, "clientToken": "t9"}
            ).encode(),
        )

    try:
        asyncio.run(_run())
        assert len(rejected) == 2
        assert rejected[0]["code"] == 400
        assert "clientToken" not in rejected[0]
        assert rejected[1]["code"] == 409
        assert rejected[1]["clientToken"] == "t9"
    finally:
        iot_module.reset()
        iot_module.broker_reset()


def test_iot_register_certificate_under_ca_links_ca_certificate_id(iot_client):
    """The headline JITR flow over the wire: register a CA, register a device
    certificate under it, and DescribeCertificate resolves the signing CA —
    through the router, botocore serialization and all."""
    ca_pem, leaf_pem = _generate_ca_and_leaf()
    ca_id = iot_client.register_ca_certificate(
        caCertificate=ca_pem, setAsActive=True, allowAutoRegistration=True
    )["certificateId"]
    try:
        cert_id = iot_client.register_certificate(
            certificatePem=leaf_pem,
            caCertificatePem=ca_pem,
            status="PENDING_ACTIVATION",
        )["certificateId"]
        desc = iot_client.describe_certificate(certificateId=cert_id)[
            "certificateDescription"
        ]
        assert desc["caCertificateId"] == ca_id
        assert desc["status"] == "PENDING_ACTIVATION"
        iot_client.delete_certificate(certificateId=cert_id)
    finally:
        iot_client.update_ca_certificate(certificateId=ca_id, newStatus="INACTIVE")
        iot_client.delete_ca_certificate(certificateId=ca_id)


def test_iot_register_certificate_rejects_an_unregistered_ca(iot_client):
    """``caCertificatePem`` naming a CA that was never registered is a
    ``CertificateValidationException``, and nothing is stored — otherwise the
    CA id is whatever the caller sent and DescribeCertificate points at a CA
    that does not exist."""
    from ministack.core.x509_utils import get_certificate_id

    ca_pem, leaf_pem = _generate_ca_and_leaf()
    with pytest.raises(ClientError) as ei:
        iot_client.register_certificate(
            certificatePem=leaf_pem, caCertificatePem=ca_pem
        )
    assert ei.value.response["Error"]["Code"] == "CertificateValidationException"
    assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

    with pytest.raises(ClientError) as ei:
        iot_client.describe_certificate(certificateId=get_certificate_id(leaf_pem))
    assert ei.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_iot_register_certificate_rejects_a_leaf_from_another_ca(iot_client):
    """Both CAs are registered, so the only thing separating them is the
    signature: a leaf signed by CA-X may not register under CA-Y. Without the
    check the JITR event fires on CA-Y's topic and a JITR Lambda that picks
    provisioning templates by ``caCertificateId`` provisions the wrong fleet.
    The same leaf under its real CA still registers."""
    from ministack.core.x509_utils import get_certificate_id

    ca_x_pem, leaf_pem = _generate_ca_and_leaf()
    ca_y_pem, _leaf_y = _generate_ca_and_leaf()
    ca_x_id = iot_client.register_ca_certificate(
        caCertificate=ca_x_pem, setAsActive=True, allowAutoRegistration=True
    )["certificateId"]
    ca_y_id = iot_client.register_ca_certificate(
        caCertificate=ca_y_pem, setAsActive=True, allowAutoRegistration=True
    )["certificateId"]
    try:
        with pytest.raises(ClientError) as ei:
            iot_client.register_certificate(
                certificatePem=leaf_pem, caCertificatePem=ca_y_pem
            )
        assert ei.value.response["Error"]["Code"] == "CertificateValidationException"
        assert ei.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        with pytest.raises(ClientError):
            iot_client.describe_certificate(certificateId=get_certificate_id(leaf_pem))

        # Same leaf, its actual issuer — accepted, and linked to that CA.
        cert_id = iot_client.register_certificate(
            certificatePem=leaf_pem, caCertificatePem=ca_x_pem
        )["certificateId"]
        desc = iot_client.describe_certificate(certificateId=cert_id)[
            "certificateDescription"
        ]
        assert desc["caCertificateId"] == ca_x_id
        iot_client.delete_certificate(certificateId=cert_id)
    finally:
        for ca_id in (ca_x_id, ca_y_id):
            iot_client.update_ca_certificate(certificateId=ca_id, newStatus="INACTIVE")
            iot_client.delete_ca_certificate(certificateId=ca_id)


def test_iot_jitr_no_event_for_a_certificate_from_another_ca():
    """The rejection happens before anything is published, so no JITR consumer
    ever sees an event attributing a foreign certificate to its CA."""
    from ministack.core.responses import (
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import iot as iot_module

    _ca_x_pem, leaf_pem = _generate_ca_and_leaf()
    ca_y_pem, _leaf_y = _generate_ca_and_leaf()
    account_id = "123456789012"
    received: list = []

    async def _run():
        set_request_account_id(account_id)
        set_request_region(_TEST_REGION)

        status, _, _body = await iot_module.handle_request(
            "POST",
            "/cacertificate",
            {},
            json.dumps({"caCertificate": ca_y_pem}).encode(),
            {"setAsActive": "true", "allowAutoRegistration": "true"},
        )
        assert status == 200

        async def _collect(topic, payload, qos):
            received.append((topic, payload, qos))

        await iot_module.broker_subscribe(
            account_id,
            _TEST_REGION,
            "$aws/events/certificates/registered/#",
            _collect,
        )

        status, _, body = await iot_module.handle_request(
            "POST",
            "/certificate/register",
            {},
            json.dumps(
                {"certificatePem": leaf_pem, "caCertificatePem": ca_y_pem}
            ).encode(),
            {},
        )
        assert status == 400
        assert json.loads(body)["__type"] == "CertificateValidationException"
        assert received == []

    try:
        asyncio.run(_run())
    finally:
        iot_module.reset()
        iot_module.broker_reset()


def test_iot_update_ca_certificate_applies_a_body_only_status(iot_client):
    """newStatus / newAutoRegistrationStatus are modeled in the query string,
    but a raw caller that sends them in the JSON body must not get a 200 that
    changed nothing — UpdateCertificate accepts either, and so does this."""
    import os
    import urllib.request

    ca_pem, _leaf = _generate_ca_and_leaf()
    ca_id = iot_client.register_ca_certificate(caCertificate=ca_pem)["certificateId"]
    try:
        assert (
            iot_client.describe_ca_certificate(certificateId=ca_id)[
                "certificateDescription"
            ]["status"]
            == "INACTIVE"
        )
        endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
        req = urllib.request.Request(
            f"{endpoint}/cacertificate/{ca_id}",
            data=json.dumps(
                {"newStatus": "ACTIVE", "newAutoRegistrationStatus": "ENABLE"}
            ).encode(),
            method="PUT",
            headers={
                # The CA registry is account+region scoped, so the credential
                # scope has to name the same region the boto3 client signs with.
                "Authorization": (
                    "AWS4-HMAC-SHA256 Credential=test/0/us-east-1/iot/aws4_request"
                ),
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            assert resp.status == 200
        desc = iot_client.describe_ca_certificate(certificateId=ca_id)[
            "certificateDescription"
        ]
        assert desc["status"] == "ACTIVE"
        assert desc["autoRegistrationStatus"] == "ENABLE"
    finally:
        iot_client.update_ca_certificate(certificateId=ca_id, newStatus="INACTIVE")
        iot_client.delete_ca_certificate(certificateId=ca_id)


def test_iot_jitr_event_requires_an_active_ca():
    """Auto-registration is gated on the CA's own status: an INACTIVE CA with
    allowAutoRegistration publishes nothing (AWS registers nothing under one),
    and activating the same CA starts the events flowing."""
    from ministack.core.responses import (
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import iot as iot_module

    ca_pem, (leaf_pem, leaf_pem2) = _generate_ca_and_leaves(2)
    account_id = "123456789012"
    received: list = []

    async def _run():
        set_request_account_id(account_id)
        set_request_region(_TEST_REGION)

        # INACTIVE (no setAsActive) but auto-registration ENABLE.
        status, _, body = await iot_module.handle_request(
            "POST",
            "/cacertificate",
            {},
            json.dumps({"caCertificate": ca_pem}).encode(),
            {"allowAutoRegistration": "true"},
        )
        assert status == 200
        ca_id = json.loads(body)["certificateId"]

        async def _collect(topic, payload, qos):
            received.append((topic, payload, qos))

        await iot_module.broker_subscribe(
            account_id,
            _TEST_REGION,
            "$aws/events/certificates/registered/#",
            _collect,
        )

        status, _, _body = await iot_module.handle_request(
            "POST",
            "/certificate/register",
            {},
            json.dumps(
                {"certificatePem": leaf_pem, "caCertificatePem": ca_pem}
            ).encode(),
            {},
        )
        assert status == 200
        assert received == []

        # Activate the CA — the very next registration fires.
        status, _, _body = await iot_module.handle_request(
            "PUT", f"/cacertificate/{ca_id}", {}, b"", {"newStatus": "ACTIVE"}
        )
        assert status == 200

        status, _, body = await iot_module.handle_request(
            "POST",
            "/certificate/register",
            {},
            json.dumps(
                {"certificatePem": leaf_pem2, "caCertificatePem": ca_pem}
            ).encode(),
            {},
        )
        assert status == 200
        cert_id2 = json.loads(body)["certificateId"]

        assert len(received) == 1
        topic, payload, _qos = received[0]
        assert topic == f"$aws/events/certificates/registered/{ca_id}"
        assert json.loads(payload)["certificateId"] == cert_id2

    try:
        asyncio.run(_run())
    finally:
        iot_module.reset()
        iot_module.broker_reset()


def test_iot_jitr_registration_survives_a_broker_failure(monkeypatch):
    """The certificate is committed before the event is published, so a broker
    that raises must not turn a successful registration into a 500."""
    from ministack.core.responses import (
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import iot as iot_module

    ca_pem, leaf_pem = _generate_ca_and_leaf()
    account_id = "123456789012"

    async def _boom(*_args, **_kwargs):
        raise RuntimeError("broker down")

    async def _run():
        set_request_account_id(account_id)
        set_request_region(_TEST_REGION)

        status, _, _body = await iot_module.handle_request(
            "POST",
            "/cacertificate",
            {},
            json.dumps({"caCertificate": ca_pem}).encode(),
            {"setAsActive": "true", "allowAutoRegistration": "true"},
        )
        assert status == 200

        monkeypatch.setattr(iot_module, "broker_publish", _boom)
        status, _, body = await iot_module.handle_request(
            "POST",
            "/certificate/register",
            {},
            json.dumps(
                {"certificatePem": leaf_pem, "caCertificatePem": ca_pem}
            ).encode(),
            {},
        )
        assert status == 200
        cert_id = json.loads(body)["certificateId"]

        status, _, body = await iot_module.handle_request(
            "GET", f"/certificates/{cert_id}", {}, b"", {}
        )
        assert status == 200
        assert json.loads(body)["certificateDescription"]["certificateId"] == cert_id

    try:
        asyncio.run(_run())
    finally:
        iot_module.reset()
        iot_module.broker_reset()


def test_iot_update_ca_certificate_rejects_invalid_enum_values(iot_client):
    """The two update fields validate their enums with a 400.

    boto3 already refuses bad values client-side, so the branch is only
    reachable over raw HTTP - which is exactly how a non-SDK caller would
    hit it.
    """
    import os
    import urllib.error
    import urllib.request

    ca_pem, _leaf = _generate_ca_and_leaf()
    ca_id = iot_client.register_ca_certificate(caCertificate=ca_pem)["certificateId"]
    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    try:
        for payload, wanted in (
            ({"newStatus": "BOGUS"}, "newStatus"),
            ({"newAutoRegistrationStatus": "MAYBE"}, "newAutoRegistrationStatus"),
        ):
            req = urllib.request.Request(
                f"{endpoint}/cacertificate/{ca_id}",
                data=json.dumps(payload).encode(),
                method="PUT",
                headers={
                    "Authorization": (
                        "AWS4-HMAC-SHA256 Credential=test/0/us-east-1/iot/aws4_request"
                    ),
                    "Content-Type": "application/json",
                },
            )
            try:
                urllib.request.urlopen(req, timeout=5)
                pytest.fail(f"expected HTTP 400 for {payload}")
            except urllib.error.HTTPError as e:
                assert e.code == 400
                assert wanted in e.read().decode()
        # The invalid updates changed nothing: a freshly registered CA stays
        # INACTIVE with auto-registration DISABLE.
        desc = iot_client.describe_ca_certificate(certificateId=ca_id)[
            "certificateDescription"
        ]
        assert desc["status"] == "INACTIVE"
        assert desc["autoRegistrationStatus"] == "DISABLE"
    finally:
        iot_client.delete_ca_certificate(certificateId=ca_id)


def test_shadow_mqtt_get_delete_roundtrip():
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    frames: dict[str, list] = {}

    async def _collect(topic, payload, qos):
        frames.setdefault(topic, []).append(json.loads(payload))

    async def _run():
        for filt in (
            "$aws/things/dev4/shadow/get/+",
            "$aws/things/dev4/shadow/delete/+",
        ):
            await subscribe(account_id, filt, _collect)
        # get before any update → rejected 404
        await publish(account_id, "$aws/things/dev4/shadow/get", b"")
        await publish(
            account_id,
            "$aws/things/dev4/shadow/update",
            json.dumps({"state": {"reported": {"fw": "1.2"}}}).encode(),
        )
        # get with a clientToken → accepted echoes it
        await publish(
            account_id,
            "$aws/things/dev4/shadow/get",
            json.dumps({"clientToken": "get-1"}).encode(),
        )
        await publish(account_id, "$aws/things/dev4/shadow/delete", b"")
        # delete again → rejected 404
        await publish(account_id, "$aws/things/dev4/shadow/delete", b"")

    try:
        asyncio.run(_run())
        get_rejected = frames["$aws/things/dev4/shadow/get/rejected"]
        assert get_rejected[0]["code"] == 404
        get_accepted = frames["$aws/things/dev4/shadow/get/accepted"][0]
        assert get_accepted["state"]["reported"] == {"fw": "1.2"}
        assert get_accepted["clientToken"] == "get-1"
        del_accepted = frames["$aws/things/dev4/shadow/delete/accepted"][0]
        assert del_accepted["version"] == 1
        del_rejected = frames["$aws/things/dev4/shadow/delete/rejected"][0]
        assert del_rejected["code"] == 404
    finally:
        iot_module._shadows.clear()
        reset()


def test_shadow_mqtt_named_shadow_isolated_from_classic():
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    frames: dict[str, list] = {}

    async def _run():
        async def _collect(topic, payload, qos):
            frames.setdefault(topic, []).append(json.loads(payload))

        await subscribe(
            account_id, "$aws/things/dev5/shadow/name/cfg/update/+", _collect
        )
        await subscribe(account_id, "$aws/things/dev5/shadow/get/+", _collect)
        await publish(
            account_id,
            "$aws/things/dev5/shadow/name/cfg/update",
            json.dumps({"state": {"desired": {"mode": "eco"}}}).encode(),
        )
        # The classic shadow does not exist — only the named one was written.
        await publish(account_id, "$aws/things/dev5/shadow/get", b"")

    try:
        asyncio.run(_run())
        accepted = frames["$aws/things/dev5/shadow/name/cfg/update/accepted"][0]
        assert accepted["state"] == {"desired": {"mode": "eco"}}
        assert frames["$aws/things/dev5/shadow/get/rejected"][0]["code"] == 404
        with iot_module.request_scope(account_id, _TEST_REGION):
            status, doc = iot_module.get_thing_shadow("dev5", "cfg")
        assert status == 200
        assert doc["state"]["desired"] == {"mode": "eco"}
    finally:
        iot_module._shadows.clear()
        reset()


def test_shadow_mqtt_documents_previous_is_the_pre_update_state():
    """`documents.previous` must be the state before the update. The shadow
    store hands back its live dicts and updates merge into them in place, so an
    un-snapshotted `previous` reports the post-update state and the two
    documents come out identical from the second update on."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    frames: dict[str, list] = {}

    async def _run():
        async def _collect(topic, payload, qos):
            frames.setdefault(topic, []).append(json.loads(payload))

        await subscribe(
            account_id, "$aws/things/dev6/shadow/update/documents", _collect
        )
        for temp in (20, 21):
            await publish(
                account_id,
                "$aws/things/dev6/shadow/update",
                json.dumps({"state": {"reported": {"temp": temp}}}).encode(),
            )

    try:
        asyncio.run(_run())
        docs = frames["$aws/things/dev6/shadow/update/documents"]
        assert len(docs) == 2
        assert docs[0]["previous"] is None
        assert docs[0]["current"]["state"]["reported"] == {"temp": 20}
        second = docs[1]
        assert second["previous"]["state"]["reported"] == {"temp": 20}
        assert second["current"]["state"]["reported"] == {"temp": 21}
        assert second["previous"]["state"] != second["current"]["state"]
        assert second["previous"]["version"] == 1
        assert second["current"]["version"] == 2
    finally:
        iot_module._shadows.clear()
        reset()


def test_shadow_mqtt_documents_carry_no_delta_section():
    """A `documents` state reports `desired` and `reported` only.

    Both snapshots come from `get_thing_shadow`, which injects `state.delta`
    (and `metadata.delta`) because that is what a GET answers with. Publishing
    them verbatim put a section on the documents topic that real devices never
    receive — the delta has its own topic, published just above this one.
    """
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    frames: dict[str, list] = {}

    async def _run():
        async def _collect(topic, payload, qos):
            frames.setdefault(topic, []).append(json.loads(payload))

        for suffix in ("update/documents", "update/delta"):
            await subscribe(account_id, f"$aws/things/dev7/shadow/{suffix}", _collect)
        # A desired the reported state does not satisfy, so a delta exists on
        # both the pre-update and the post-update document.
        for reported in (10, 11):
            await publish(
                account_id,
                "$aws/things/dev7/shadow/update",
                json.dumps(
                    {"state": {"desired": {"temp": 30}, "reported": {"temp": reported}}}
                ).encode(),
            )

    try:
        asyncio.run(_run())
        docs = frames["$aws/things/dev7/shadow/update/documents"]
        assert len(docs) == 2
        second = docs[1]
        for snapshot in ("previous", "current"):
            state = second[snapshot]["state"]
            assert set(state) == {"desired", "reported"}
            assert "delta" not in second[snapshot].get("metadata", {})
        assert second["previous"]["state"]["reported"] == {"temp": 10}
        assert second["current"]["state"]["reported"] == {"temp": 11}
        # The delta itself is still reported, on the topic that is meant for it.
        deltas = frames["$aws/things/dev7/shadow/update/delta"]
        assert deltas and deltas[-1]["state"] == {"temp": 30}
    finally:
        iot_module._shadows.clear()
        reset()


def test_shadow_mqtt_update_without_state_node_is_rejected():
    """The most common device-side mistake: valid JSON, no `state` node."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    frames: dict[str, list] = {}

    async def _run():
        async def _collect(topic, payload, qos):
            frames.setdefault(topic, []).append(json.loads(payload))

        await subscribe(account_id, "$aws/things/dev7/shadow/update/+", _collect)
        await publish(
            account_id,
            "$aws/things/dev7/shadow/update",
            json.dumps({"desired": {"led": "on"}, "clientToken": "tok-9"}).encode(),
        )

    try:
        asyncio.run(_run())
        rejected = frames["$aws/things/dev7/shadow/update/rejected"][0]
        assert rejected["code"] == 400
        assert "state" in rejected["message"]
        assert rejected["clientToken"] == "tok-9"
        assert "$aws/things/dev7/shadow/update/accepted" not in frames
    finally:
        iot_module._shadows.clear()
        reset()


def test_shadow_mqtt_empty_named_shadow_is_rejected_on_its_own_topic():
    """`.../shadow/name//update` names no shadow. Treating it as the classic
    shadow would write the wrong record and answer on a topic the requester is
    not subscribed to, so it is rejected where the requester is listening."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    frames: dict[str, list] = {}

    async def _run():
        async def _collect(topic, payload, qos):
            frames.setdefault(topic, []).append(json.loads(payload))

        await subscribe(account_id, "$aws/things/dev8/shadow/name//update/+", _collect)
        await subscribe(account_id, "$aws/things/dev8/shadow/update/+", _collect)
        await publish(
            account_id,
            "$aws/things/dev8/shadow/name//update",
            json.dumps({"state": {"desired": {"led": "on"}}, "clientToken": "t"}).encode(),
        )

    try:
        asyncio.run(_run())
        rejected = frames["$aws/things/dev8/shadow/name//update/rejected"][0]
        assert rejected["code"] == 400
        assert rejected["clientToken"] == "t"
        # Nothing landed on the classic shadow's topics or in its record.
        assert not any(t.startswith("$aws/things/dev8/shadow/update/") for t in frames)
        with iot_module.request_scope(account_id, _TEST_REGION):
            assert iot_module.get_thing_shadow("dev8", "")[0] == 404
    finally:
        iot_module._shadows.clear()
        reset()


def test_shadow_mqtt_named_shadow_full_lifecycle():
    """delta / documents / get / delete all answer on the named shadow's own
    topics — only update/accepted was covered for named shadows."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    frames: dict[str, list] = {}
    base = "$aws/things/dev9/shadow/name/cfg"

    async def _run():
        async def _collect(topic, payload, qos):
            frames.setdefault(topic, []).append(json.loads(payload))

        for filt in (f"{base}/update/+", f"{base}/get/+", f"{base}/delete/+"):
            await subscribe(account_id, filt, _collect, granted_qos=1)
        await publish(
            account_id,
            f"{base}/update",
            json.dumps(
                {"state": {"desired": {"mode": "eco"}}, "clientToken": "tok-n"}
            ).encode(),
        )
        await publish(account_id, f"{base}/get", b"")
        await publish(account_id, f"{base}/delete", b"")
        await publish(account_id, f"{base}/get", b"")

    try:
        asyncio.run(_run())
        delta = frames[f"{base}/update/delta"][0]
        assert delta["state"] == {"mode": "eco"}
        assert delta["clientToken"] == "tok-n"
        assert set(delta["metadata"]) == {"mode"}
        docs = frames[f"{base}/update/documents"][0]
        assert docs["previous"] is None
        assert docs["current"]["state"]["desired"] == {"mode": "eco"}
        assert frames[f"{base}/get/accepted"][0]["state"]["desired"] == {"mode": "eco"}
        assert frames[f"{base}/delete/accepted"][0]["version"] == 1
        assert frames[f"{base}/get/rejected"][0]["code"] == 404
    finally:
        iot_module._shadows.clear()
        reset()


def test_shadow_mqtt_bridge_failure_does_not_kill_the_publish(monkeypatch):
    """The bridge is the last side effect in broker_publish and the only one
    that used to run unguarded, so anything it raised propagated out and tore
    down the caller's MQTT session."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"

    async def _boom(*args, **kwargs):
        raise RecursionError("maximum recursion depth exceeded")

    monkeypatch.setattr(iot_module, "_handle_shadow_publish", _boom)
    delivered = []

    async def _run():
        async def _collect(topic, payload, qos):
            delivered.append(topic)

        await subscribe(account_id, "$aws/things/dev10/shadow/update", _collect)
        await publish(
            account_id,
            "$aws/things/dev10/shadow/update",
            json.dumps({"state": {"desired": {"led": "on"}}}).encode(),
        )

    try:
        asyncio.run(_run())  # no exception escapes
        # ...and the subscriber still got the request publish.
        assert delivered == ["$aws/things/dev10/shadow/update"]
    finally:
        iot_module._shadows.clear()
        reset()


def test_shadow_mqtt_bridge_drives_topic_rules_on_request_and_accepted():
    """Rules fire on the request topic (before the bridge runs) AND on the
    bridge's own `update/accepted` publish (which recursively re-enters
    broker_publish and its now-async rule evaluation)."""
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    iot_module._topic_rules.clear()
    account_id = "123456789012"
    _put_rule(
        account_id,
        "shadow_req_rule",
        "SELECT * FROM '$aws/things/+/shadow/update'",
        [{"republish": {"topic": "seen/request", "qos": 0}}],
    )
    _put_rule(
        account_id,
        "shadow_acc_rule",
        "SELECT * FROM '$aws/things/+/shadow/update/accepted'",
        [{"republish": {"topic": "seen/accepted", "qos": 0}}],
    )
    seen: dict[str, list] = {}

    async def _run():
        async def _collect(topic, payload, qos):
            seen.setdefault(topic, []).append(json.loads(payload))

        await subscribe(account_id, "seen/#", _collect)
        await publish(
            account_id,
            "$aws/things/dev6/shadow/update",
            json.dumps({"state": {"desired": {"n": 1}}}).encode(),
        )

    try:
        asyncio.run(_run())
        assert len(seen.get("seen/request", [])) == 1
        assert seen["seen/request"][0] == {"state": {"desired": {"n": 1}}}
        accepted_events = seen.get("seen/accepted", [])
        assert len(accepted_events) == 1
        assert accepted_events[0]["version"] == 1
        assert accepted_events[0]["state"] == {"desired": {"n": 1}}
    finally:
        iot_module._topic_rules.clear()
        iot_module._shadows.clear()
        reset()


def test_shadow_mqtt_get_and_delete_reject_invalid_json():
    """Malformed JSON rejects on every request verb, not only update.

    A `get`/`delete` payload is optional, but garbage must not be silently
    read as `{}` - as on AWS, the requester hears a 400 on its own
    `rejected` topic.
    """
    from ministack.services import iot as iot_module

    reset()
    iot_module._shadows.clear()
    account_id = "123456789012"
    rejected: list = []
    accepted: list = []

    async def _run():
        async def _collect_rejected(topic, payload, qos):
            rejected.append((topic, json.loads(payload)))

        async def _collect_accepted(topic, payload, qos):
            accepted.append(topic)

        for verb in ("get", "delete"):
            await subscribe(
                account_id, f"$aws/things/dev6/shadow/{verb}/rejected", _collect_rejected
            )
            await subscribe(
                account_id, f"$aws/things/dev6/shadow/{verb}/accepted", _collect_accepted
            )
        await publish(account_id, "$aws/things/dev6/shadow/get", b"{not json")
        await publish(account_id, "$aws/things/dev6/shadow/delete", b"\xff\xfe")

    try:
        asyncio.run(_run())
        assert [t.rsplit("/", 2)[-2] for t, _doc in rejected] == ["get", "delete"]
        assert all(doc["code"] == 400 for _t, doc in rejected)
        assert accepted == [], "malformed JSON must not reach the shadow store"
    finally:
        iot_module._shadows.clear()
        reset()


def test_jobs_restored_timed_out_and_removed_executions_are_terminal():
    """The two service-side statuses nothing here sets must still behave.

    A record restored from persistence (written by a fuller implementation)
    can carry TIMED_OUT or REMOVED. The promise in the jobs header is that
    such a record is terminal: it is counted in ``jobProcessDetails`` and
    does not stop the job from completing. White-box, because only a restore
    can produce these statuses locally.
    """
    from ministack.core.responses import (
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import iot as iot_module

    account_id = "123456789012"

    async def _request(method, path, payload=None):
        set_request_account_id(account_id)
        set_request_region("us-east-1")
        body = json.dumps(payload or {}).encode()
        return await iot_module.handle_request(method, path, {}, body, {})

    async def _scenario():
        set_request_account_id(account_id)
        set_request_region("us-east-1")
        job_id = f"restored-terminal-{uuid.uuid4().hex[:8]}"
        things = [f"jt-{job_id}-a", f"jt-{job_id}-b"]
        try:
            for thing in things:
                status, _headers, _body = await _request("POST", f"/things/{thing}")
                assert status == 200
            status, _headers, _body = await _request(
                "PUT",
                f"/jobs/{job_id}",
                {
                    "targets": [
                        f"arn:aws:iot:us-east-1:{account_id}:thing/{t}"
                        for t in things
                    ],
                    "document": json.dumps({"op": "noop"}),
                },
            )
            assert status == 200

            iot_module._jobs_materialize_executions(job_id)
            iot_module._job_executions[(things[0], job_id)]["status"] = "TIMED_OUT"
            iot_module._job_executions[(things[1], job_id)]["status"] = "REMOVED"
            iot_module._jobs_maybe_complete(job_id)

            status, _headers, body = await _request("GET", f"/jobs/{job_id}")
            assert status == 200
            job = json.loads(body)["job"]
            details = job["jobProcessDetails"]
            assert details["numberOfTimedOutThings"] == 1
            assert details["numberOfRemovedThings"] == 1
            assert job["status"] == "COMPLETED", (
                "a job whose remaining executions are all TIMED_OUT/REMOVED "
                "must complete - the statuses are terminal even though "
                "nothing local sets them"
            )
        finally:
            iot_module._jobs.pop(job_id, None)
            for thing in things:
                iot_module._job_executions.pop((thing, job_id), None)
                await _request("DELETE", f"/things/{thing}")

    asyncio.run(_scenario())
