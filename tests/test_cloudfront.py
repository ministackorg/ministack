import io
import json
import os
import time
import urllib.error
import urllib.request
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

_CF_DIST_CONFIG = {
    "CallerReference": "cf-test-ref-1",
    "Origins": {
        "Quantity": 1,
        "Items": [
            {
                "Id": "myS3Origin",
                "DomainName": "mybucket.s3.amazonaws.com",
                "S3OriginConfig": {"OriginAccessIdentity": ""},
            }
        ],
    },
    "DefaultCacheBehavior": {
        "TargetOriginId": "myS3Origin",
        "ViewerProtocolPolicy": "redirect-to-https",
        "ForwardedValues": {
            "QueryString": False,
            "Cookies": {"Forward": "none"},
        },
        "MinTTL": 0,
    },
    "Comment": "test distribution",
    "Enabled": True,
}

def test_cloudfront_create_distribution(cloudfront):
    resp = cloudfront.create_distribution(DistributionConfig=_CF_DIST_CONFIG)
    dist = resp["Distribution"]
    assert dist["Id"]
    assert dist["DomainName"].endswith(".cloudfront.net")
    assert dist["Status"] == "Deployed"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201

def test_cloudfront_list_distributions(cloudfront):
    cfg_a = {**_CF_DIST_CONFIG, "CallerReference": "cf-list-a", "Comment": "list-a"}
    cfg_b = {**_CF_DIST_CONFIG, "CallerReference": "cf-list-b", "Comment": "list-b"}
    cloudfront.create_distribution(DistributionConfig=cfg_a)
    cloudfront.create_distribution(DistributionConfig=cfg_b)
    resp = cloudfront.list_distributions()
    dist_list = resp["DistributionList"]
    ids = [d["Id"] for d in dist_list.get("Items", [])]
    assert len(ids) >= 2

def test_cloudfront_get_distribution(cloudfront):
    cfg = {**_CF_DIST_CONFIG, "CallerReference": "cf-get-1", "Comment": "get-test"}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]

    resp = cloudfront.get_distribution(Id=dist_id)
    dist = resp["Distribution"]
    assert dist["Id"] == dist_id
    assert dist["DomainName"] == f"{dist_id}.cloudfront.net"
    assert dist["Status"] == "Deployed"

def test_cloudfront_get_distribution_config(cloudfront):
    cfg = {**_CF_DIST_CONFIG, "CallerReference": "cf-getcfg-1", "Comment": "getcfg-test"}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]
    etag = create_resp["ETag"]

    resp = cloudfront.get_distribution_config(Id=dist_id)
    assert resp["ETag"] == etag
    assert resp["DistributionConfig"]["Comment"] == "getcfg-test"

def test_cloudfront_update_distribution(cloudfront):
    cfg = {**_CF_DIST_CONFIG, "CallerReference": "cf-upd-1", "Comment": "before-update"}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]
    etag = create_resp["ETag"]

    updated_cfg = {**cfg, "CallerReference": "cf-upd-1", "Comment": "after-update"}
    upd_resp = cloudfront.update_distribution(DistributionConfig=updated_cfg, Id=dist_id, IfMatch=etag)
    assert upd_resp["Distribution"]["Id"] == dist_id
    assert upd_resp["ETag"] != etag  # new ETag issued

    get_resp = cloudfront.get_distribution_config(Id=dist_id)
    assert get_resp["DistributionConfig"]["Comment"] == "after-update"

def test_cloudfront_update_distribution_etag_mismatch(cloudfront):
    cfg = {**_CF_DIST_CONFIG, "CallerReference": "cf-etag-mismatch", "Comment": "mismatch-test"}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.update_distribution(
            DistributionConfig=cfg, Id=dist_id, IfMatch="wrong-etag-value"
        )
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"

def test_cloudfront_delete_distribution(cloudfront):
    cfg = {**_CF_DIST_CONFIG, "CallerReference": "cf-del-1", "Comment": "delete-test", "Enabled": True}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]
    etag = create_resp["ETag"]

    # Must disable before deleting
    disabled_cfg = {**cfg, "Enabled": False}
    upd_resp = cloudfront.update_distribution(DistributionConfig=disabled_cfg, Id=dist_id, IfMatch=etag)
    new_etag = upd_resp["ETag"]

    cloudfront.delete_distribution(Id=dist_id, IfMatch=new_etag)

    with pytest.raises(ClientError) as exc:
        cloudfront.get_distribution(Id=dist_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchDistribution"

def test_cloudfront_delete_enabled_distribution(cloudfront):
    cfg = {**_CF_DIST_CONFIG, "CallerReference": "cf-del-enabled", "Comment": "del-enabled-test", "Enabled": True}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]
    etag = create_resp["ETag"]

    with pytest.raises(ClientError) as exc:
        cloudfront.delete_distribution(Id=dist_id, IfMatch=etag)
    assert exc.value.response["Error"]["Code"] == "DistributionNotDisabled"

def test_cloudfront_get_nonexistent(cloudfront):
    with pytest.raises(ClientError) as exc:
        cloudfront.get_distribution(Id="ENONEXISTENT1234")
    assert exc.value.response["Error"]["Code"] == "NoSuchDistribution"

def test_cloudfront_create_invalidation(cloudfront):
    cfg = {**_CF_DIST_CONFIG, "CallerReference": "cf-inv-1", "Comment": "inv-test"}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]

    inv_resp = cloudfront.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={
            "Paths": {"Quantity": 2, "Items": ["/index.html", "/static/*"]},
            "CallerReference": "inv-ref-1",
        },
    )
    inv = inv_resp["Invalidation"]
    assert inv["Id"]
    assert inv["Status"] == "Completed"
    assert inv_resp["ResponseMetadata"]["HTTPStatusCode"] == 201

def test_cloudfront_list_invalidations(cloudfront):
    cfg = {**_CF_DIST_CONFIG, "CallerReference": "cf-listinv-1", "Comment": "listinv-test"}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]

    cloudfront.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={"Paths": {"Quantity": 1, "Items": ["/a"]}, "CallerReference": "inv-list-a"},
    )
    cloudfront.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={"Paths": {"Quantity": 1, "Items": ["/b"]}, "CallerReference": "inv-list-b"},
    )

    resp = cloudfront.list_invalidations(DistributionId=dist_id)
    inv_list = resp["InvalidationList"]
    assert inv_list["Quantity"] == 2
    assert len(inv_list["Items"]) == 2

def test_cloudfront_get_invalidation(cloudfront):
    cfg = {**_CF_DIST_CONFIG, "CallerReference": "cf-getinv-1", "Comment": "getinv-test"}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]

    inv_resp = cloudfront.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": ["/getinv-path"]},
            "CallerReference": "inv-get-ref",
        },
    )
    inv_id = inv_resp["Invalidation"]["Id"]

    get_resp = cloudfront.get_invalidation(DistributionId=dist_id, Id=inv_id)
    inv = get_resp["Invalidation"]
    assert inv["Id"] == inv_id
    assert inv["Status"] == "Completed"
    assert "/getinv-path" in inv["InvalidationBatch"]["Paths"]["Items"]

def test_cloudfront_tags(cloudfront):
    """TagResource / ListTagsForResource / UntagResource for CloudFront distributions."""
    resp = cloudfront.create_distribution(
        DistributionConfig={
            "CallerReference": "tag-test-v42",
            "Origins": {"Items": [{"Id": "o1", "DomainName": "example.com",
                                   "S3OriginConfig": {"OriginAccessIdentity": ""}}], "Quantity": 1},
            "DefaultCacheBehavior": {
                "TargetOriginId": "o1", "ViewerProtocolPolicy": "allow-all",
                "ForwardedValues": {"QueryString": False, "Cookies": {"Forward": "none"}},
                "MinTTL": 0,
            },
            "Comment": "tag test", "Enabled": True,
        }
    )
    dist_arn = resp["Distribution"]["ARN"]

    cloudfront.tag_resource(
        Resource=dist_arn,
        Tags={"Items": [
            {"Key": "env", "Value": "test"},
            {"Key": "team", "Value": "platform"},
        ]},
    )

    tags = cloudfront.list_tags_for_resource(Resource=dist_arn)
    tag_map = {t["Key"]: t["Value"] for t in tags["Tags"]["Items"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "platform"

    cloudfront.untag_resource(
        Resource=dist_arn,
        TagKeys={"Items": ["team"]},
    )

    tags = cloudfront.list_tags_for_resource(Resource=dist_arn)
    tag_keys = [t["Key"] for t in tags["Tags"]["Items"]]
    assert "env" in tag_keys
    assert "team" not in tag_keys


# ---------------------------------------------------------------------------
# OAC happy-path integration tests
# ---------------------------------------------------------------------------

def _oac_config(name, description="", origin_type="s3", signing_behavior="always", signing_protocol="sigv4"):
    """Helper to build an OAC config dict for boto3."""
    return {
        "Name": name,
        "Description": description,
        "OriginAccessControlOriginType": origin_type,
        "SigningBehavior": signing_behavior,
        "SigningProtocol": signing_protocol,
    }


def test_oac_create_and_get(cloudfront):
    """Create an OAC and verify all response fields via get."""
    cfg = _oac_config(
        name=f"oac-create-get-{_uuid_mod.uuid4().hex[:8]}",
        description="integration test OAC",
        origin_type="s3",
        signing_behavior="always",
        signing_protocol="sigv4",
    )
    create_resp = cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    assert create_resp["ResponseMetadata"]["HTTPStatusCode"] == 201

    oac = create_resp["OriginAccessControl"]
    oac_id = oac["Id"]
    etag = create_resp["ETag"]

    # Id format: E + 13 alphanumeric
    assert oac_id and len(oac_id) == 14 and oac_id[0] == "E"
    assert etag

    oac_cfg = oac["OriginAccessControlConfig"]
    assert oac_cfg["Name"] == cfg["Name"]
    assert oac_cfg["Description"] == cfg["Description"]
    assert oac_cfg["OriginAccessControlOriginType"] == "s3"
    assert oac_cfg["SigningBehavior"] == "always"
    assert oac_cfg["SigningProtocol"] == "sigv4"

    # Verify via get
    get_resp = cloudfront.get_origin_access_control(Id=oac_id)
    assert get_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert get_resp["ETag"] == etag

    get_oac = get_resp["OriginAccessControl"]
    assert get_oac["Id"] == oac_id
    get_cfg = get_oac["OriginAccessControlConfig"]
    assert get_cfg["Name"] == cfg["Name"]
    assert get_cfg["Description"] == cfg["Description"]
    assert get_cfg["OriginAccessControlOriginType"] == "s3"
    assert get_cfg["SigningBehavior"] == "always"
    assert get_cfg["SigningProtocol"] == "sigv4"


def test_oac_get_config(cloudfront):
    """Create an OAC, get config only, verify config-only response matches input."""
    cfg = _oac_config(
        name=f"oac-get-config-{_uuid_mod.uuid4().hex[:8]}",
        description="config-only test",
        origin_type="mediastore",
        signing_behavior="no-override",
        signing_protocol="sigv4",
    )
    create_resp = cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    oac_id = create_resp["OriginAccessControl"]["Id"]
    etag = create_resp["ETag"]

    config_resp = cloudfront.get_origin_access_control_config(Id=oac_id)
    assert config_resp["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert config_resp["ETag"] == etag

    returned_cfg = config_resp["OriginAccessControlConfig"]
    assert returned_cfg["Name"] == cfg["Name"]
    assert returned_cfg["Description"] == cfg["Description"]
    assert returned_cfg["OriginAccessControlOriginType"] == "mediastore"
    assert returned_cfg["SigningBehavior"] == "no-override"
    assert returned_cfg["SigningProtocol"] == "sigv4"


def test_oac_list(cloudfront):
    """Create multiple OACs, list, verify all present with correct Quantity."""
    names = [f"oac-list-{i}-{_uuid_mod.uuid4().hex[:8]}" for i in range(3)]
    created_ids = []
    for name in names:
        resp = cloudfront.create_origin_access_control(
            OriginAccessControlConfig=_oac_config(name=name, description="list test")
        )
        created_ids.append(resp["OriginAccessControl"]["Id"])

    list_resp = cloudfront.list_origin_access_controls()
    assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    oac_list = list_resp["OriginAccessControlList"]
    quantity = int(oac_list["Quantity"])
    assert quantity >= 3

    listed_ids = [item["Id"] for item in oac_list.get("Items", [])]
    for cid in created_ids:
        assert cid in listed_ids


def test_oac_update(cloudfront):
    """Create an OAC, update config fields, verify updated fields and new ETag."""
    original_name = f"oac-update-orig-{_uuid_mod.uuid4().hex[:8]}"
    cfg = _oac_config(name=original_name, description="before update", origin_type="s3", signing_behavior="always")
    create_resp = cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    oac_id = create_resp["OriginAccessControl"]["Id"]
    old_etag = create_resp["ETag"]

    updated_name = f"oac-update-new-{_uuid_mod.uuid4().hex[:8]}"
    updated_cfg = _oac_config(
        name=updated_name,
        description="after update",
        origin_type="lambda",
        signing_behavior="no-override",
    )
    update_resp = cloudfront.update_origin_access_control(
        Id=oac_id,
        IfMatch=old_etag,
        OriginAccessControlConfig=updated_cfg,
    )
    assert update_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    new_etag = update_resp["ETag"]
    assert new_etag != old_etag

    updated_oac = update_resp["OriginAccessControl"]["OriginAccessControlConfig"]
    assert updated_oac["Name"] == updated_name
    assert updated_oac["Description"] == "after update"
    assert updated_oac["OriginAccessControlOriginType"] == "lambda"
    assert updated_oac["SigningBehavior"] == "no-override"
    assert updated_oac["SigningProtocol"] == "sigv4"


def test_oac_delete(cloudfront):
    """Create an OAC, delete with correct ETag, verify 404 on subsequent get."""
    cfg = _oac_config(name=f"oac-delete-{_uuid_mod.uuid4().hex[:8]}", description="delete test")
    create_resp = cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    oac_id = create_resp["OriginAccessControl"]["Id"]
    etag = create_resp["ETag"]

    del_resp = cloudfront.delete_origin_access_control(Id=oac_id, IfMatch=etag)
    assert del_resp["ResponseMetadata"]["HTTPStatusCode"] == 204

    with pytest.raises(ClientError) as exc:
        cloudfront.get_origin_access_control(Id=oac_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchOriginAccessControl"


def test_oac_list_empty(cloudfront):
    """List OACs and verify Quantity field exists (may include OACs from other tests)."""
    list_resp = cloudfront.list_origin_access_controls()
    assert list_resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    oac_list = list_resp["OriginAccessControlList"]
    assert "Quantity" in oac_list
    # Quantity should be a non-negative integer (string or int depending on parsing)
    quantity = int(oac_list["Quantity"])
    assert quantity >= 0


# ---------------------------------------------------------------------------
# OAC error-path integration tests
# ---------------------------------------------------------------------------


def test_oac_get_nonexistent(cloudfront):
    """Get a non-existent OAC Id, verify 404 NoSuchOriginAccessControl."""
    with pytest.raises(ClientError) as exc:
        cloudfront.get_origin_access_control(Id="ENONEXISTENT1234")
    assert exc.value.response["Error"]["Code"] == "NoSuchOriginAccessControl"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_oac_delete_nonexistent(cloudfront):
    """Delete a non-existent OAC Id, verify 404 NoSuchOriginAccessControl."""
    with pytest.raises(ClientError) as exc:
        cloudfront.delete_origin_access_control(Id="ENONEXISTENT1234", IfMatch="any-etag")
    assert exc.value.response["Error"]["Code"] == "NoSuchOriginAccessControl"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_oac_update_etag_mismatch(cloudfront):
    """Update an OAC with a wrong ETag, verify 412 PreconditionFailed."""
    cfg = _oac_config(name=f"oac-upd-etag-{_uuid_mod.uuid4().hex[:8]}")
    create_resp = cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    oac_id = create_resp["OriginAccessControl"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.update_origin_access_control(
            Id=oac_id,
            IfMatch="wrong-etag-value",
            OriginAccessControlConfig=cfg,
        )
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412


def test_oac_delete_etag_mismatch(cloudfront):
    """Delete an OAC with a wrong ETag, verify 412 PreconditionFailed."""
    cfg = _oac_config(name=f"oac-del-etag-{_uuid_mod.uuid4().hex[:8]}")
    create_resp = cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    oac_id = create_resp["OriginAccessControl"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.delete_origin_access_control(Id=oac_id, IfMatch="wrong-etag-value")
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412


def test_oac_update_no_if_match(cloudfront):
    """Update an OAC without If-Match header, verify error response."""
    cfg = _oac_config(name=f"oac-upd-noifm-{_uuid_mod.uuid4().hex[:8]}")
    create_resp = cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    oac_id = create_resp["OriginAccessControl"]["Id"]

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    url = f"{endpoint}/2020-05-31/origin-access-control/{oac_id}/config"
    xml_body = (
        '<OriginAccessControlConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">'
        f"<Name>{cfg['Name']}</Name>"
        "<Description></Description>"
        "<OriginAccessControlOriginType>s3</OriginAccessControlOriginType>"
        "<SigningBehavior>always</SigningBehavior>"
        "<SigningProtocol>sigv4</SigningProtocol>"
        "</OriginAccessControlConfig>"
    )
    req = urllib.request.Request(
        url,
        data=xml_body.encode("utf-8"),
        method="PUT",
        headers={
            "Content-Type": "text/xml",
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/cloudfront/aws4_request, SignedHeaders=host, Signature=fake",
        },
    )
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(req, timeout=5)
    assert exc.value.code == 400


def test_oac_delete_no_if_match(cloudfront):
    """Delete an OAC without If-Match header, verify error response."""
    cfg = _oac_config(name=f"oac-del-noifm-{_uuid_mod.uuid4().hex[:8]}")
    create_resp = cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    oac_id = create_resp["OriginAccessControl"]["Id"]

    endpoint = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
    url = f"{endpoint}/2020-05-31/origin-access-control/{oac_id}"
    req = urllib.request.Request(
        url,
        method="DELETE",
        headers={
            "Authorization": "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/cloudfront/aws4_request, SignedHeaders=host, Signature=fake",
        },
    )
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(req, timeout=5)
    assert exc.value.code == 400


def test_oac_duplicate_name(cloudfront):
    """Create two OACs with the same name, verify 409 OriginAccessControlAlreadyExists."""
    name = f"oac-dup-{_uuid_mod.uuid4().hex[:8]}"
    cfg = _oac_config(name=name)
    cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)

    with pytest.raises(ClientError) as exc:
        cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    assert exc.value.response["Error"]["Code"] == "OriginAccessControlAlreadyExists"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409


def test_oac_invalid_origin_type(cloudfront):
    """Create an OAC with an invalid origin type, verify 400 InvalidArgument."""
    cfg = _oac_config(
        name=f"oac-bad-origin-{_uuid_mod.uuid4().hex[:8]}",
        origin_type="invalid-origin",
    )
    with pytest.raises(ClientError) as exc:
        cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_oac_invalid_signing_behavior(cloudfront):
    """Create an OAC with an invalid signing behavior, verify 400 InvalidArgument."""
    cfg = _oac_config(
        name=f"oac-bad-sign-{_uuid_mod.uuid4().hex[:8]}",
        signing_behavior="invalid-behavior",
    )
    with pytest.raises(ClientError) as exc:
        cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_oac_invalid_signing_protocol(cloudfront):
    """Create an OAC with an invalid signing protocol, verify 400 InvalidArgument."""
    cfg = _oac_config(
        name=f"oac-bad-proto-{_uuid_mod.uuid4().hex[:8]}",
        signing_protocol="sigv2",
    )
    with pytest.raises(ClientError) as exc:
        cloudfront.create_origin_access_control(OriginAccessControlConfig=cfg)
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
