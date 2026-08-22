import copy
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid as _uuid_mod

import pytest
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

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


def _custom_origin_distribution_config(caller_reference):
    return {
        "CallerReference": caller_reference,
        "Origins": {
            "Quantity": 1,
            "Items": [
                {
                    "Id": "custom-origin",
                    "DomainName": "origin.example.com",
                    "OriginPath": "/app",
                    "CustomHeaders": {
                        "Quantity": 1,
                        "Items": [{"HeaderName": "X-Origin-Test", "HeaderValue": "yes"}],
                    },
                    "CustomOriginConfig": {
                        "HTTPPort": 80,
                        "HTTPSPort": 443,
                        "OriginProtocolPolicy": "https-only",
                        "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                        "OriginReadTimeout": 30,
                        "OriginKeepaliveTimeout": 5,
                    },
                }
            ],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "custom-origin",
            "ViewerProtocolPolicy": "redirect-to-https",
            "ForwardedValues": {
                "QueryString": True,
                "Cookies": {"Forward": "all"},
            },
            "MinTTL": 0,
        },
        "Comment": "custom origin distribution",
        "Enabled": True,
    }


def _first_distribution_origin(config_or_summary):
    origins = config_or_summary["Origins"]
    assert origins["Quantity"] == 1
    return origins["Items"][0]


def test_cloudfront_create_distribution(cloudfront):
    resp = cloudfront.create_distribution(DistributionConfig=_CF_DIST_CONFIG)
    dist = resp["Distribution"]
    assert dist["Id"]
    assert dist["DomainName"].endswith(".cloudfront.net")
    assert dist["Status"] == "Deployed"
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201


def test_cloudfront_create_distribution_with_tags(cloudfront):
    """CreateDistributionWithTags (Terraform aws_cloudfront_distribution tags) unwraps inner config."""
    if not hasattr(cloudfront, "create_distribution_with_tags"):
        pytest.skip("boto3 has no create_distribution_with_tags")
    ref = f"cf-with-tags-{_uuid_mod.uuid4().hex[:12]}"
    cfg = {**_CF_DIST_CONFIG, "CallerReference": ref}
    resp = cloudfront.create_distribution_with_tags(
        DistributionConfigWithTags={
            "DistributionConfig": cfg,
            "Tags": {"Items": [{"Key": "env", "Value": "test"}]},
        }
    )
    dist = resp["Distribution"]
    dist_id = dist["Id"]
    dist_arn = dist["ARN"]
    assert dist["DomainName"].endswith(".cloudfront.net")
    tags = cloudfront.list_tags_for_resource(Resource=dist_arn)["Tags"]["Items"]
    assert any(t["Key"] == "env" and t["Value"] == "test" for t in tags)
    etag = resp["ETag"]
    disabled_cfg = {**cfg, "Enabled": False}
    upd = cloudfront.update_distribution(DistributionConfig=disabled_cfg, Id=dist_id, IfMatch=etag)
    cloudfront.delete_distribution(Id=dist_id, IfMatch=upd["ETag"])


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
    # terraform-provider-aws v6+ dereferences OriginGroups without a nil check
    assert dist["DistributionConfig"]["OriginGroups"]["Quantity"] == 0


def test_cloudfront_get_distribution_config(cloudfront):
    cfg = {**_CF_DIST_CONFIG, "CallerReference": "cf-getcfg-1", "Comment": "getcfg-test"}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]
    etag = create_resp["ETag"]

    resp = cloudfront.get_distribution_config(Id=dist_id)
    assert resp["ETag"] == etag
    assert resp["DistributionConfig"]["Comment"] == "getcfg-test"
    assert resp["DistributionConfig"]["OriginGroups"]["Quantity"] == 0


def test_cloudfront_origin_configuration_round_trips(cloudfront):
    cfg = _custom_origin_distribution_config(f"cf-origin-{_uuid_mod.uuid4().hex[:12]}")
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]

    get_resp = cloudfront.get_distribution(Id=dist_id)
    get_origin = _first_distribution_origin(get_resp["Distribution"]["DistributionConfig"])
    assert get_origin["Id"] == "custom-origin"
    assert get_origin["DomainName"] == "origin.example.com"
    assert get_origin["OriginPath"] == "/app"
    assert get_origin["CustomHeaders"]["Items"][0]["HeaderValue"] == "yes"
    assert get_origin["CustomOriginConfig"]["OriginProtocolPolicy"] == "https-only"
    assert get_origin["CustomOriginConfig"]["OriginSslProtocols"]["Items"] == ["TLSv1.2"]

    config_resp = cloudfront.get_distribution_config(Id=dist_id)
    config_origin = _first_distribution_origin(config_resp["DistributionConfig"])
    assert config_origin["CustomOriginConfig"]["HTTPPort"] == 80
    assert config_origin["CustomOriginConfig"]["HTTPSPort"] == 443

    list_resp = cloudfront.list_distributions()
    summary = next(item for item in list_resp["DistributionList"]["Items"] if item["Id"] == dist_id)
    summary_origin = _first_distribution_origin(summary)
    assert summary_origin["CustomOriginConfig"]["OriginReadTimeout"] == 30
    assert summary["DefaultCacheBehavior"]["TargetOriginId"] == "custom-origin"

    updated_cfg = copy.deepcopy(cfg)
    updated_cfg["Origins"]["Items"][0]["OriginPath"] = "/next"
    update_resp = cloudfront.update_distribution(
        DistributionConfig=updated_cfg,
        Id=dist_id,
        IfMatch=create_resp["ETag"],
    )
    assert update_resp["Distribution"]["DistributionConfig"]["Origins"]["Items"][0]["OriginPath"] == "/next"


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
        cloudfront.update_distribution(DistributionConfig=cfg, Id=dist_id, IfMatch="wrong-etag-value")
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


def test_cloudfront_create_get_list_invalidation_idempotent(cloudfront):
    cfg = {
        **_CF_DIST_CONFIG,
        "CallerReference": f"cf-inv-basic-{_uuid_mod.uuid4().hex[:12]}",
        "Comment": "inv-basic-test",
    }
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]
    caller_ref = f"inv-basic-{_uuid_mod.uuid4().hex[:12]}"

    resp = cloudfront.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={
            "Paths": {
                "Quantity": 2,
                "Items": ["/index.html", "/assets/*"],
            },
            "CallerReference": caller_ref,
        },
    )

    invalidation = resp["Invalidation"]
    invalidation_id = invalidation["Id"]
    assert invalidation_id.startswith("I")
    assert invalidation["Status"] == "Completed"
    assert invalidation["InvalidationBatch"]["CallerReference"] == caller_ref
    assert invalidation["InvalidationBatch"]["Paths"]["Quantity"] == 2
    assert "/index.html" in invalidation["InvalidationBatch"]["Paths"]["Items"]

    duplicate_resp = cloudfront.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={
            "Paths": {
                "Quantity": 2,
                "Items": ["/index.html", "/assets/*"],
            },
            "CallerReference": caller_ref,
        },
    )
    assert duplicate_resp["Invalidation"]["Id"] == invalidation_id

    get_resp = cloudfront.get_invalidation(
        DistributionId=dist_id,
        Id=invalidation_id,
    )
    assert get_resp["Invalidation"]["Id"] == invalidation_id
    assert get_resp["Invalidation"]["Status"] == "Completed"

    list_resp = cloudfront.list_invalidations(DistributionId=dist_id)
    inv_list = list_resp["InvalidationList"]
    assert inv_list["Quantity"] == 1
    assert inv_list["Items"][0]["Id"] == invalidation_id


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


def test_cloudfront_get_missing_invalidation_returns_error(cloudfront):
    cfg = {
        **_CF_DIST_CONFIG,
        "CallerReference": f"cf-inv-missing-{_uuid_mod.uuid4().hex[:12]}",
        "Comment": "inv-missing-test",
    }
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.get_invalidation(
            DistributionId=dist_id,
            Id="IMISSING1234567",
        )
    assert exc.value.response["Error"]["Code"] == "NoSuchInvalidation"


def test_cloudfront_create_invalidation_same_caller_reference_different_paths_errors(cloudfront):
    cfg = {
        **_CF_DIST_CONFIG,
        "CallerReference": f"cf-inv-conflict-{_uuid_mod.uuid4().hex[:12]}",
        "Comment": "inv-conflict-test",
    }
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]
    caller_ref = f"inv-conflict-{_uuid_mod.uuid4().hex[:12]}"

    cloudfront.create_invalidation(
        DistributionId=dist_id,
        InvalidationBatch={"Paths": {"Quantity": 1, "Items": ["/one"]}, "CallerReference": caller_ref},
    )

    with pytest.raises(ClientError) as exc:
        cloudfront.create_invalidation(
            DistributionId=dist_id,
            InvalidationBatch={"Paths": {"Quantity": 1, "Items": ["/two"]}, "CallerReference": caller_ref},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidationBatchAlreadyExists"
    assert cloudfront.list_invalidations(DistributionId=dist_id)["InvalidationList"]["Quantity"] == 1


def test_cloudfront_tags(cloudfront):
    """TagResource / ListTagsForResource / UntagResource for CloudFront distributions."""
    resp = cloudfront.create_distribution(
        DistributionConfig={
            "CallerReference": "tag-test-v42",
            "Origins": {
                "Items": [{"Id": "o1", "DomainName": "example.com", "S3OriginConfig": {"OriginAccessIdentity": ""}}],
                "Quantity": 1,
            },
            "DefaultCacheBehavior": {
                "TargetOriginId": "o1",
                "ViewerProtocolPolicy": "allow-all",
                "ForwardedValues": {"QueryString": False, "Cookies": {"Forward": "none"}},
                "MinTTL": 0,
            },
            "Comment": "tag test",
            "Enabled": True,
        }
    )
    dist_arn = resp["Distribution"]["ARN"]

    cloudfront.tag_resource(
        Resource=dist_arn,
        Tags={
            "Items": [
                {"Key": "env", "Value": "test"},
                {"Key": "team", "Value": "platform"},
            ]
        },
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


@pytest.mark.parametrize(
    ("arn", "code"),
    [
        ("not-an-arn", "InvalidArgument"),
        ("arn:aws:sqs::000000000000:distribution/missing", "InvalidArgument"),
        ("arn:aws:cloudfront:us-east-1:000000000000:distribution/missing", "InvalidArgument"),
        ("arn:aws:cloudfront::000000000000:distribution/missing", "NoSuchDistribution"),
    ],
)
def test_cloudfront_tag_resource_requires_local_cloudfront_arn(cloudfront, arn, code):
    with pytest.raises(ClientError) as exc:
        cloudfront.tag_resource(Resource=arn, Tags={"Items": [{"Key": "env", "Value": "test"}]})

    assert exc.value.response["Error"]["Code"] == code


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
        data=b"",
        method="DELETE",
        headers={
            "Content-Length": "0",
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


def _cf_resp_etag(resp):
    h = resp.get("ResponseMetadata", {}).get("HTTPHeaders") or {}
    return resp.get("ETag") or h.get("etag") or h.get("ETag")


def test_cloudfront_function_create_publish_describe_get_delete(cloudfront):
    """CloudFront Functions API — matches Terraform aws_cloudfront_function (create + publish + read + delete)."""
    name = f"fn-tf-{_uuid_mod.uuid4().hex[:8]}"
    code = b"function handler(event) { return event.request; }"
    cr = cloudfront.create_function(
        Name=name,
        FunctionConfig={"Comment": "strip", "Runtime": "cloudfront-js-1.0"},
        FunctionCode=code,
    )
    assert cr["ResponseMetadata"]["HTTPStatusCode"] == 201
    assert cr["FunctionSummary"]["Name"] == name
    assert cr["FunctionSummary"]["FunctionMetadata"]["Stage"] == "DEVELOPMENT"
    dev_etag = _cf_resp_etag(cr)
    assert dev_etag

    pub = cloudfront.publish_function(Name=name, IfMatch=dev_etag)
    assert pub["FunctionSummary"]["FunctionMetadata"]["Stage"] == "LIVE"
    live_etag = _cf_resp_etag(pub)
    assert live_etag

    d_dev = cloudfront.describe_function(Name=name, Stage="DEVELOPMENT")
    assert _cf_resp_etag(d_dev) == dev_etag
    d_live = cloudfront.describe_function(Name=name, Stage="LIVE")
    assert _cf_resp_etag(d_live) == live_etag

    gf = cloudfront.get_function(Name=name, Stage="DEVELOPMENT")
    body = gf["FunctionCode"]
    got = body.read() if hasattr(body, "read") else body
    assert got == code

    lst = cloudfront.list_functions()
    qty = lst["FunctionList"]["Quantity"]
    assert qty >= 2

    cloudfront.delete_function(Name=name, IfMatch=_cf_resp_etag(d_dev))

    with pytest.raises(ClientError) as exc:
        cloudfront.describe_function(Name=name, Stage="DEVELOPMENT")
    assert exc.value.response["Error"]["Code"] == "NoSuchFunctionExists"


def test_cloudfront_function_duplicate_name(cloudfront):
    name = f"fn-dup-{_uuid_mod.uuid4().hex[:8]}"
    cloudfront.create_function(
        Name=name,
        FunctionConfig={"Comment": "", "Runtime": "cloudfront-js-1.0"},
        FunctionCode=b"x",
    )
    with pytest.raises(ClientError) as exc:
        cloudfront.create_function(
            Name=name,
            FunctionConfig={"Comment": "", "Runtime": "cloudfront-js-1.0"},
            FunctionCode=b"y",
        )
    assert exc.value.response["Error"]["Code"] == "FunctionAlreadyExists"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409


def test_cloudfront_function_describe_requires_stage(cloudfront):
    """DescribeFunction without Stage query param — AWS requires Stage; MiniStack returns InvalidArgument."""
    name = f"fn-nostage-{_uuid_mod.uuid4().hex[:8]}"
    cloudfront.create_function(
        Name=name,
        FunctionConfig={"Comment": "", "Runtime": "cloudfront-js-1.0"},
        FunctionCode=b"//",
    )
    with pytest.raises(ClientError) as exc:
        cloudfront.describe_function(Name=name)
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_cloudfront_sdk_compat_injects_origin_groups():
    """terraform-provider-aws dereferences OriginGroups.Quantity without a nil check."""
    from xml.etree.ElementTree import Element, SubElement

    import ministack.services.cloudfront as cf

    el = Element("DistributionConfig")
    SubElement(el, "CallerReference").text = "unit-ref"
    assert cf._find(el, "OriginGroups") is None
    cf._ensure_distribution_config_sdk_compat(el)
    og = cf._find(el, "OriginGroups")
    assert og is not None
    assert cf._text(og, "Quantity") == "0"


# ---------------------------------------------------------------------------
# KeyValueStore tests
# ---------------------------------------------------------------------------


def test_kvs_create_and_describe(cloudfront):
    resp = cloudfront.create_key_value_store(Name="test-kvs-1", Comment="test comment")
    kvs = resp["KeyValueStore"]
    assert kvs["Name"] == "test-kvs-1"
    assert kvs["Comment"] == "test comment"
    assert kvs["Status"] == "READY"
    assert "Id" in kvs
    assert kvs["ARN"].endswith(":key-value-store/test-kvs-1")
    assert "LastModifiedTime" in kvs
    etag = resp["ETag"]
    assert etag

    desc = cloudfront.describe_key_value_store(Name="test-kvs-1")
    assert desc["KeyValueStore"]["Name"] == "test-kvs-1"
    assert desc["KeyValueStore"]["Id"] == kvs["Id"]
    assert desc["ETag"] == etag


def test_kvs_list(cloudfront):
    name_a = f"kvs-list-a-{_uuid_mod.uuid4().hex[:8]}"
    name_b = f"kvs-list-b-{_uuid_mod.uuid4().hex[:8]}"
    cloudfront.create_key_value_store(Name=name_a, Comment="a")
    cloudfront.create_key_value_store(Name=name_b, Comment="b")

    resp = cloudfront.list_key_value_stores()
    names = [item["Name"] for item in resp["KeyValueStoreList"]["Items"]]
    assert name_a in names
    assert name_b in names
    assert resp["KeyValueStoreList"]["Quantity"] >= 2


def test_kvs_update_comment(cloudfront):
    name = f"kvs-update-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="old")
    etag = create_resp["ETag"]

    update_resp = cloudfront.update_key_value_store(Name=name, Comment="new comment", IfMatch=etag)
    assert update_resp["KeyValueStore"]["Comment"] == "new comment"
    new_etag = update_resp["ETag"]
    assert new_etag != etag

    desc = cloudfront.describe_key_value_store(Name=name)
    assert desc["KeyValueStore"]["Comment"] == "new comment"
    assert desc["ETag"] == new_etag


def test_kvs_delete(cloudfront):
    name = f"kvs-delete-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="to delete")
    etag = create_resp["ETag"]

    cloudfront.delete_key_value_store(Name=name, IfMatch=etag)

    with pytest.raises(ClientError) as exc:
        cloudfront.describe_key_value_store(Name=name)
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_kvs_duplicate_name(cloudfront):
    name = f"kvs-dup-{_uuid_mod.uuid4().hex[:8]}"
    cloudfront.create_key_value_store(Name=name, Comment="first")

    with pytest.raises(ClientError) as exc:
        cloudfront.create_key_value_store(Name=name, Comment="second")
    assert exc.value.response["Error"]["Code"] == "EntityAlreadyExists"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409


def test_kvs_describe_nonexistent(cloudfront):
    with pytest.raises(ClientError) as exc:
        cloudfront.describe_key_value_store(Name="nonexistent-kvs")
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_kvs_delete_etag_mismatch(cloudfront):
    name = f"kvs-del-etag-{_uuid_mod.uuid4().hex[:8]}"
    cloudfront.create_key_value_store(Name=name, Comment="test")

    with pytest.raises(ClientError) as exc:
        cloudfront.delete_key_value_store(Name=name, IfMatch="wrong-etag")
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412


def test_kvs_update_etag_mismatch(cloudfront):
    name = f"kvs-upd-etag-{_uuid_mod.uuid4().hex[:8]}"
    cloudfront.create_key_value_store(Name=name, Comment="test")

    with pytest.raises(ClientError) as exc:
        cloudfront.update_key_value_store(Name=name, Comment="new", IfMatch="wrong-etag")
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 412


def test_kvs_function_association(cloudfront):
    kvs_name = f"kvs-assoc-{_uuid_mod.uuid4().hex[:8]}"
    kvs_resp = cloudfront.create_key_value_store(Name=kvs_name, Comment="for function")
    kvs_arn = kvs_resp["KeyValueStore"]["ARN"]

    func_name = f"fn-kvs-{_uuid_mod.uuid4().hex[:8]}"
    cloudfront.create_function(
        Name=func_name,
        FunctionConfig={
            "Comment": "with kvs",
            "Runtime": "cloudfront-js-2.0",
            "KeyValueStoreAssociations": {
                "Quantity": 1,
                "Items": [{"KeyValueStoreARN": kvs_arn}],
            },
        },
        FunctionCode=b"function handler(event) { return event.response; }",
    )

    desc = cloudfront.describe_function(Name=func_name, Stage="DEVELOPMENT")
    kvs_assocs = desc["FunctionSummary"]["FunctionConfig"]["KeyValueStoreAssociations"]
    assert kvs_assocs["Quantity"] == 1
    assert kvs_assocs["Items"][0]["KeyValueStoreARN"] == kvs_arn


def test_kvs_delete_in_use(cloudfront):
    kvs_name = f"kvs-inuse-{_uuid_mod.uuid4().hex[:8]}"
    kvs_resp = cloudfront.create_key_value_store(Name=kvs_name, Comment="in use")
    kvs_arn = kvs_resp["KeyValueStore"]["ARN"]
    kvs_etag = kvs_resp["ETag"]

    func_name = f"fn-inuse-{_uuid_mod.uuid4().hex[:8]}"
    cloudfront.create_function(
        Name=func_name,
        FunctionConfig={
            "Comment": "uses kvs",
            "Runtime": "cloudfront-js-2.0",
            "KeyValueStoreAssociations": {
                "Quantity": 1,
                "Items": [{"KeyValueStoreARN": kvs_arn}],
            },
        },
        FunctionCode=b"function handler(event) { return event.response; }",
    )

    with pytest.raises(ClientError) as exc:
        cloudfront.delete_key_value_store(Name=kvs_name, IfMatch=kvs_etag)
    assert exc.value.response["Error"]["Code"] == "CannotDeleteEntityWhileInUse"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409


def test_kvs_create_with_import_source(cloudfront):
    """ImportSource (create-only optional input, AWS spec requires SourceType +
    SourceARN) is accepted and round-tripped. Ministack records it but does not
    actually fetch from S3 — same stance as other side-effect creates."""
    name = f"kvs-imp-{_uuid_mod.uuid4().hex[:8]}"
    bucket_arn = "arn:aws:s3:::seed-bucket/initial.json"
    resp = cloudfront.create_key_value_store(
        Name=name,
        Comment="seeded from S3",
        ImportSource={"SourceType": "S3", "SourceARN": bucket_arn},
    )
    assert resp["KeyValueStore"]["Name"] == name
    assert resp["KeyValueStore"]["Status"] == "READY"


def test_kvs_create_with_import_source_missing_field_rejected(cloudfront):
    """ImportSource requires both SourceType and SourceARN per AWS spec; either
    missing is InvalidArgument."""
    name = f"kvs-impbad-{_uuid_mod.uuid4().hex[:8]}"
    with pytest.raises(ClientError) as exc:
        cloudfront.create_key_value_store(
            Name=name,
            Comment="bad import",
            ImportSource={"SourceType": "S3", "SourceARN": ""},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"


# ---------------------------------------------------------------------------
# CloudFront KeyValueStore data-plane tests. Folded from
# test_cloudfront_kvs.py.
# ---------------------------------------------------------------------------


def _describe_store_raw(kvs_arn):
    url = f"{ENDPOINT}/key-value-stores/{urllib.parse.quote(kvs_arn, safe='')}"
    req = urllib.request.Request(url, method="GET")
    return urllib.request.urlopen(req, timeout=10)


def test_kvs_dataplane_describe(cloudfront, cloudfront_kvs):
    name = f"dp-desc-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="describe test")
    arn = create_resp["KeyValueStore"]["ARN"]

    resp = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    assert resp["KvsARN"] == arn
    assert resp["ItemCount"] == 0
    assert resp["TotalSizeInBytes"] == 0
    assert resp["Status"] == "READY"
    assert "etag" in resp["ResponseMetadata"]["HTTPHeaders"]


def test_kvs_dataplane_put_and_get_key(cloudfront, cloudfront_kvs):
    name = f"dp-put-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="put/get test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="route/home", Value="/index.html", IfMatch=etag)
    assert put_resp["ItemCount"] == 1
    assert put_resp["TotalSizeInBytes"] > 0
    new_etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]
    assert new_etag != etag

    get_resp = cloudfront_kvs.get_key(KvsARN=arn, Key="route/home")
    assert get_resp["Key"] == "route/home"
    assert get_resp["Value"] == "/index.html"
    assert get_resp["ItemCount"] == 1


def test_kvs_dataplane_delete_key(cloudfront, cloudfront_kvs):
    name = f"dp-del-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="delete test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="to-delete", Value="val", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]

    del_resp = cloudfront_kvs.delete_key(KvsARN=arn, Key="to-delete", IfMatch=etag)
    assert del_resp["ItemCount"] == 0

    with pytest.raises(ClientError) as exc:
        cloudfront_kvs.get_key(KvsARN=arn, Key="to-delete")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_kvs_dataplane_list_keys(cloudfront, cloudfront_kvs):
    name = f"dp-list-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="list test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="key-a", Value="val-a", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]
    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="key-b", Value="val-b", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]
    cloudfront_kvs.put_key(KvsARN=arn, Key="key-c", Value="val-c", IfMatch=etag)

    resp = cloudfront_kvs.list_keys(KvsARN=arn)
    keys = [item["Key"] for item in resp["Items"]]
    assert "key-a" in keys
    assert "key-b" in keys
    assert "key-c" in keys


def test_kvs_dataplane_update_keys(cloudfront, cloudfront_kvs):
    name = f"dp-upd-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="update keys test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="existing", Value="old", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]

    resp = cloudfront_kvs.update_keys(
        KvsARN=arn,
        IfMatch=etag,
        Puts=[
            {"Key": "new-key", "Value": "new-val"},
            {"Key": "existing", "Value": "updated"},
        ],
        Deletes=[],
    )
    assert resp["ItemCount"] == 2

    get_resp = cloudfront_kvs.get_key(KvsARN=arn, Key="existing")
    assert get_resp["Value"] == "updated"

    get_resp = cloudfront_kvs.get_key(KvsARN=arn, Key="new-key")
    assert get_resp["Value"] == "new-val"


def test_kvs_dataplane_update_keys_with_deletes(cloudfront, cloudfront_kvs):
    name = f"dp-upddel-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="update+delete test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="keep", Value="yes", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]
    put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key="remove", Value="bye", IfMatch=etag)
    etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]

    resp = cloudfront_kvs.update_keys(
        KvsARN=arn,
        IfMatch=etag,
        Puts=[{"Key": "added", "Value": "hello"}],
        Deletes=[{"Key": "remove"}],
    )
    assert resp["ItemCount"] == 2

    with pytest.raises(ClientError) as exc:
        cloudfront_kvs.get_key(KvsARN=arn, Key="remove")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    get_resp = cloudfront_kvs.get_key(KvsARN=arn, Key="added")
    assert get_resp["Value"] == "hello"


def test_kvs_dataplane_etag_conflict(cloudfront, cloudfront_kvs):
    name = f"dp-conflict-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="conflict test")
    arn = create_resp["KeyValueStore"]["ARN"]

    with pytest.raises(ClientError) as exc:
        cloudfront_kvs.put_key(KvsARN=arn, Key="x", Value="y", IfMatch="wrong-etag")
    assert exc.value.response["Error"]["Code"] == "ConflictException"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409


def test_kvs_dataplane_not_found(cloudfront_kvs):
    fake_arn = "arn:aws:cloudfront::000000000000:key-value-store/nonexistent"
    with pytest.raises(ClientError) as exc:
        cloudfront_kvs.describe_key_value_store(KvsARN=fake_arn)
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_kvs_dataplane_rejects_invalid_kvs_arns(cloudfront):
    name = f"dp-invalid-arn-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="invalid arn test")
    arn = create_resp["KeyValueStore"]["ARN"]
    invalid_cases = [
        "arn:aws:cloudfront::000000000000:distribution/example",
        arn.replace(":cloudfront:", ":sqs:"),
        arn.replace(":000000000000:", ":111111111111:"),
        arn.replace("cloudfront::", "cloudfront:us-east-1:"),
        f"{arn}/extra",
    ]

    for bad_arn in invalid_cases:
        with pytest.raises(urllib.error.HTTPError) as exc:
            _describe_store_raw(bad_arn)
        assert exc.value.code == 400
        body = json.loads(exc.value.read().decode("utf-8"))
        assert body["__type"] == "ValidationException"


def test_kvs_dataplane_list_keys_pagination(cloudfront, cloudfront_kvs):
    name = f"dp-page-{_uuid_mod.uuid4().hex[:8]}"
    create_resp = cloudfront.create_key_value_store(Name=name, Comment="pagination test")
    arn = create_resp["KeyValueStore"]["ARN"]

    desc = cloudfront_kvs.describe_key_value_store(KvsARN=arn)
    etag = desc["ResponseMetadata"]["HTTPHeaders"]["etag"]

    for i in range(5):
        put_resp = cloudfront_kvs.put_key(KvsARN=arn, Key=f"k{i:02d}", Value=f"v{i}", IfMatch=etag)
        etag = put_resp["ResponseMetadata"]["HTTPHeaders"]["etag"]

    resp = cloudfront_kvs.list_keys(KvsARN=arn, MaxResults=2)
    assert len(resp["Items"]) == 2
    assert "NextToken" in resp

    resp2 = cloudfront_kvs.list_keys(KvsARN=arn, MaxResults=2, NextToken=resp["NextToken"])
    assert len(resp2["Items"]) == 2

    all_keys = [item["Key"] for item in resp["Items"] + resp2["Items"]]
    assert len(set(all_keys)) == 4


# ---------------------------------------------------------------------------
# Cache policies (aws_cloudfront_cache_policy) — #1249
# ---------------------------------------------------------------------------


def _cache_policy_config(name):
    return {
        "Name": name,
        "Comment": "test cache policy",
        "DefaultTTL": 3600,
        "MaxTTL": 86400,
        "MinTTL": 1,
        "ParametersInCacheKeyAndForwardedToOrigin": {
            "EnableAcceptEncodingGzip": True,
            "EnableAcceptEncodingBrotli": True,
            "HeadersConfig": {
                "HeaderBehavior": "whitelist",
                "Headers": {"Quantity": 1, "Items": ["Authorization"]},
            },
            "CookiesConfig": {"CookieBehavior": "none"},
            "QueryStringsConfig": {
                "QueryStringBehavior": "whitelist",
                "QueryStrings": {"Quantity": 2, "Items": ["a", "b"]},
            },
        },
    }


def test_cloudfront_create_and_get_cache_policy(cloudfront):
    name = f"cp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_cache_policy(CachePolicyConfig=_cache_policy_config(name))
    assert create["ETag"]
    policy = create["CachePolicy"]
    pid = policy["Id"]
    assert policy["Id"]
    assert "LastModifiedTime" in policy

    got = cloudfront.get_cache_policy(Id=pid)
    cfg = got["CachePolicy"]["CachePolicyConfig"]
    assert got["ETag"] == create["ETag"]
    assert cfg["Name"] == name
    assert cfg["MinTTL"] == 1
    assert cfg["DefaultTTL"] == 3600
    assert cfg["MaxTTL"] == 86400
    params = cfg["ParametersInCacheKeyAndForwardedToOrigin"]
    assert params["EnableAcceptEncodingGzip"] is True
    assert params["EnableAcceptEncodingBrotli"] is True
    assert params["HeadersConfig"]["HeaderBehavior"] == "whitelist"
    assert params["HeadersConfig"]["Headers"]["Items"] == ["Authorization"]
    assert params["CookiesConfig"]["CookieBehavior"] == "none"
    assert params["QueryStringsConfig"]["QueryStringBehavior"] == "whitelist"
    assert params["QueryStringsConfig"]["QueryStrings"]["Items"] == ["a", "b"]

    cloudfront.delete_cache_policy(Id=pid, IfMatch=got["ETag"])


def test_cloudfront_get_cache_policy_config(cloudfront):
    name = f"cp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_cache_policy(CachePolicyConfig=_cache_policy_config(name))
    pid = create["CachePolicy"]["Id"]
    resp = cloudfront.get_cache_policy_config(Id=pid)
    assert resp["ETag"] == create["ETag"]
    assert resp["CachePolicyConfig"]["Name"] == name
    assert resp["CachePolicyConfig"]["MinTTL"] == 1
    cloudfront.delete_cache_policy(Id=pid, IfMatch=create["ETag"])


def test_cloudfront_update_cache_policy(cloudfront):
    name = f"cp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_cache_policy(CachePolicyConfig=_cache_policy_config(name))
    pid = create["CachePolicy"]["Id"]

    # No If-Match -> InvalidIfMatchVersion.
    updated_cfg = _cache_policy_config(name)
    updated_cfg["MinTTL"] = 5
    with pytest.raises(ClientError) as exc:
        cloudfront.update_cache_policy(CachePolicyConfig=updated_cfg, Id=pid)
    assert exc.value.response["Error"]["Code"] == "InvalidIfMatchVersion"

    # Stale If-Match -> PreconditionFailed.
    with pytest.raises(ClientError) as exc:
        cloudfront.update_cache_policy(CachePolicyConfig=updated_cfg, Id=pid, IfMatch="stale-etag")
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"

    upd = cloudfront.update_cache_policy(CachePolicyConfig=updated_cfg, Id=pid, IfMatch=create["ETag"])
    assert upd["ETag"] != create["ETag"]
    assert upd["CachePolicy"]["CachePolicyConfig"]["MinTTL"] == 5
    cloudfront.delete_cache_policy(Id=pid, IfMatch=upd["ETag"])


def test_cloudfront_delete_cache_policy(cloudfront):
    name = f"cp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_cache_policy(CachePolicyConfig=_cache_policy_config(name))
    pid = create["CachePolicy"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.delete_cache_policy(Id=pid)
    assert exc.value.response["Error"]["Code"] == "InvalidIfMatchVersion"

    cloudfront.delete_cache_policy(Id=pid, IfMatch=create["ETag"])
    with pytest.raises(ClientError) as exc:
        cloudfront.get_cache_policy(Id=pid)
    assert exc.value.response["Error"]["Code"] == "NoSuchCachePolicy"


def test_cloudfront_cache_policy_duplicate_name_rejected(cloudfront):
    name = f"cp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_cache_policy(CachePolicyConfig=_cache_policy_config(name))
    pid = create["CachePolicy"]["Id"]
    with pytest.raises(ClientError) as exc:
        cloudfront.create_cache_policy(CachePolicyConfig=_cache_policy_config(name))
    assert exc.value.response["Error"]["Code"] == "CachePolicyAlreadyExists"
    cloudfront.delete_cache_policy(Id=pid, IfMatch=create["ETag"])


def test_cloudfront_get_missing_cache_policy(cloudfront):
    with pytest.raises(ClientError) as exc:
        cloudfront.get_cache_policy(Id="no-such-cache-policy")
    assert exc.value.response["Error"]["Code"] == "NoSuchCachePolicy"


def test_cloudfront_list_distributions_by_cache_policy(cloudfront):
    name = f"cp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_cache_policy(CachePolicyConfig=_cache_policy_config(name))
    pid = create["CachePolicy"]["Id"]
    resp = cloudfront.list_distributions_by_cache_policy_id(CachePolicyId=pid)
    dil = resp["DistributionIdList"]
    assert dil["Quantity"] == 0
    assert dil["IsTruncated"] is False
    assert dil.get("Items", []) == []
    cloudfront.delete_cache_policy(Id=pid, IfMatch=create["ETag"])


# ---------------------------------------------------------------------------
# Origin request policies (aws_cloudfront_origin_request_policy) — #1249
# ---------------------------------------------------------------------------


def _orp_config(name):
    return {
        "Name": name,
        "Comment": "test orp",
        "HeadersConfig": {"HeaderBehavior": "whitelist", "Headers": {"Quantity": 1, "Items": ["X-Custom"]}},
        "CookiesConfig": {"CookieBehavior": "all"},
        "QueryStringsConfig": {"QueryStringBehavior": "whitelist", "QueryStrings": {"Quantity": 2, "Items": ["a", "b"]}},
    }


def test_cloudfront_create_and_get_origin_request_policy(cloudfront):
    name = f"orp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_origin_request_policy(OriginRequestPolicyConfig=_orp_config(name))
    assert create["ETag"]
    pid = create["OriginRequestPolicy"]["Id"]
    assert "LastModifiedTime" in create["OriginRequestPolicy"]

    got = cloudfront.get_origin_request_policy(Id=pid)
    cfg = got["OriginRequestPolicy"]["OriginRequestPolicyConfig"]
    assert got["ETag"] == create["ETag"]
    assert cfg["Name"] == name
    assert cfg["HeadersConfig"]["HeaderBehavior"] == "whitelist"
    assert cfg["HeadersConfig"]["Headers"]["Items"] == ["X-Custom"]
    assert cfg["CookiesConfig"]["CookieBehavior"] == "all"
    assert cfg["QueryStringsConfig"]["QueryStringBehavior"] == "whitelist"
    assert cfg["QueryStringsConfig"]["QueryStrings"]["Items"] == ["a", "b"]

    cloudfront.delete_origin_request_policy(Id=pid, IfMatch=got["ETag"])


def test_cloudfront_origin_request_policy_config_and_update(cloudfront):
    name = f"orp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_origin_request_policy(OriginRequestPolicyConfig=_orp_config(name))
    pid = create["OriginRequestPolicy"]["Id"]

    resp = cloudfront.get_origin_request_policy_config(Id=pid)
    assert resp["ETag"] == create["ETag"]
    assert resp["OriginRequestPolicyConfig"]["Name"] == name

    updated = _orp_config(name)
    updated["CookiesConfig"] = {"CookieBehavior": "none"}
    with pytest.raises(ClientError) as exc:
        cloudfront.update_origin_request_policy(OriginRequestPolicyConfig=updated, Id=pid)
    assert exc.value.response["Error"]["Code"] == "InvalidIfMatchVersion"

    upd = cloudfront.update_origin_request_policy(OriginRequestPolicyConfig=updated, Id=pid, IfMatch=create["ETag"])
    assert upd["ETag"] != create["ETag"]
    assert upd["OriginRequestPolicy"]["OriginRequestPolicyConfig"]["CookiesConfig"]["CookieBehavior"] == "none"
    cloudfront.delete_origin_request_policy(Id=pid, IfMatch=upd["ETag"])


def test_cloudfront_origin_request_policy_delete_and_duplicate(cloudfront):
    name = f"orp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_origin_request_policy(OriginRequestPolicyConfig=_orp_config(name))
    pid = create["OriginRequestPolicy"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.create_origin_request_policy(OriginRequestPolicyConfig=_orp_config(name))
    assert exc.value.response["Error"]["Code"] == "OriginRequestPolicyAlreadyExists"

    cloudfront.delete_origin_request_policy(Id=pid, IfMatch=create["ETag"])
    with pytest.raises(ClientError) as exc:
        cloudfront.get_origin_request_policy(Id=pid)
    assert exc.value.response["Error"]["Code"] == "NoSuchOriginRequestPolicy"


def test_cloudfront_list_distributions_by_origin_request_policy(cloudfront):
    name = f"orp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_origin_request_policy(OriginRequestPolicyConfig=_orp_config(name))
    pid = create["OriginRequestPolicy"]["Id"]
    resp = cloudfront.list_distributions_by_origin_request_policy_id(OriginRequestPolicyId=pid)
    assert resp["DistributionIdList"]["Quantity"] == 0
    cloudfront.delete_origin_request_policy(Id=pid, IfMatch=create["ETag"])


# ---------------------------------------------------------------------------
# Response headers policies (aws_cloudfront_response_headers_policy) — #1249
# ---------------------------------------------------------------------------


def _rhp_config(name):
    return {
        "Name": name,
        "Comment": "test rhp",
        "CorsConfig": {
            "AccessControlAllowOrigins": {"Quantity": 1, "Items": ["https://example.com"]},
            "AccessControlAllowHeaders": {"Quantity": 1, "Items": ["X-Custom"]},
            "AccessControlAllowMethods": {"Quantity": 2, "Items": ["GET", "POST"]},
            "AccessControlAllowCredentials": False,
            "AccessControlExposeHeaders": {"Quantity": 1, "Items": ["X-Expose"]},
            "AccessControlMaxAgeSec": 600,
            "OriginOverride": True,
        },
        "SecurityHeadersConfig": {
            "FrameOptions": {"Override": True, "FrameOption": "DENY"},
            "ContentTypeOptions": {"Override": True},
            "ReferrerPolicy": {"Override": True, "ReferrerPolicy": "same-origin"},
            "StrictTransportSecurity": {
                "Override": True, "AccessControlMaxAgeSec": 31536000,
                "IncludeSubdomains": True, "Preload": False,
            },
        },
        "CustomHeadersConfig": {
            "Quantity": 1,
            "Items": [{"Header": "X-Extra", "Value": "yes", "Override": True}],
        },
        "RemoveHeadersConfig": {"Quantity": 1, "Items": [{"Header": "Server"}]},
    }


def test_cloudfront_create_and_get_response_headers_policy(cloudfront):
    name = f"rhp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_response_headers_policy(ResponseHeadersPolicyConfig=_rhp_config(name))
    assert create["ETag"]
    pid = create["ResponseHeadersPolicy"]["Id"]

    got = cloudfront.get_response_headers_policy(Id=pid)
    cfg = got["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]
    assert got["ETag"] == create["ETag"]
    assert cfg["Name"] == name

    cors = cfg["CorsConfig"]
    assert cors["AccessControlAllowOrigins"]["Items"] == ["https://example.com"]
    assert cors["AccessControlAllowMethods"]["Items"] == ["GET", "POST"]
    assert cors["AccessControlAllowCredentials"] is False
    assert cors["AccessControlExposeHeaders"]["Items"] == ["X-Expose"]
    assert cors["AccessControlMaxAgeSec"] == 600
    assert cors["OriginOverride"] is True

    sec = cfg["SecurityHeadersConfig"]
    assert sec["FrameOptions"]["FrameOption"] == "DENY"
    assert sec["ContentTypeOptions"]["Override"] is True
    assert sec["ReferrerPolicy"]["ReferrerPolicy"] == "same-origin"
    assert sec["StrictTransportSecurity"]["AccessControlMaxAgeSec"] == 31536000
    assert sec["StrictTransportSecurity"]["IncludeSubdomains"] is True
    assert sec["StrictTransportSecurity"]["Preload"] is False

    assert cfg["CustomHeadersConfig"]["Items"] == [{"Header": "X-Extra", "Value": "yes", "Override": True}]
    assert cfg["RemoveHeadersConfig"]["Items"] == [{"Header": "Server"}]

    cloudfront.delete_response_headers_policy(Id=pid, IfMatch=got["ETag"])


def test_cloudfront_response_headers_policy_config_update_delete(cloudfront):
    name = f"rhp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_response_headers_policy(ResponseHeadersPolicyConfig=_rhp_config(name))
    pid = create["ResponseHeadersPolicy"]["Id"]

    resp = cloudfront.get_response_headers_policy_config(Id=pid)
    assert resp["ETag"] == create["ETag"]
    assert resp["ResponseHeadersPolicyConfig"]["Name"] == name

    updated = _rhp_config(name)
    updated["CorsConfig"]["AccessControlMaxAgeSec"] = 1200
    with pytest.raises(ClientError) as exc:
        cloudfront.update_response_headers_policy(ResponseHeadersPolicyConfig=updated, Id=pid, IfMatch="stale")
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"

    upd = cloudfront.update_response_headers_policy(
        ResponseHeadersPolicyConfig=updated, Id=pid, IfMatch=create["ETag"]
    )
    assert upd["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]["CorsConfig"]["AccessControlMaxAgeSec"] == 1200

    cloudfront.delete_response_headers_policy(Id=pid, IfMatch=upd["ETag"])
    with pytest.raises(ClientError) as exc:
        cloudfront.get_response_headers_policy(Id=pid)
    assert exc.value.response["Error"]["Code"] == "NoSuchResponseHeadersPolicy"


def test_cloudfront_response_headers_policy_duplicate_and_list(cloudfront):
    name = f"rhp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_response_headers_policy(ResponseHeadersPolicyConfig=_rhp_config(name))
    pid = create["ResponseHeadersPolicy"]["Id"]
    with pytest.raises(ClientError) as exc:
        cloudfront.create_response_headers_policy(ResponseHeadersPolicyConfig=_rhp_config(name))
    assert exc.value.response["Error"]["Code"] == "ResponseHeadersPolicyAlreadyExists"

    resp = cloudfront.list_distributions_by_response_headers_policy_id(ResponseHeadersPolicyId=pid)
    assert resp["DistributionIdList"]["Quantity"] == 0


# ---------------------------------------------------------------------------
# Read-only list surface — ops previously falling through the path dispatch.
# Shapes verified against botocore cloudfront service-2.json (2020-05-31).
# ---------------------------------------------------------------------------


def test_cloudfront_list_key_groups_empty(cloudfront):
    resp = cloudfront.list_key_groups()
    lst = resp["KeyGroupList"]
    assert lst["MaxItems"] == 100
    assert lst["Quantity"] == 0
    assert lst.get("Items", []) == []


def test_cloudfront_list_public_keys_empty(cloudfront):
    resp = cloudfront.list_public_keys()
    lst = resp["PublicKeyList"]
    assert lst["MaxItems"] == 100
    assert lst["Quantity"] == 0
    assert lst.get("Items", []) == []


def test_cloudfront_list_field_level_encryption_configs_empty(cloudfront):
    resp = cloudfront.list_field_level_encryption_configs()
    lst = resp["FieldLevelEncryptionList"]
    assert lst["MaxItems"] == 100
    assert lst["Quantity"] == 0
    assert lst.get("Items", []) == []


def test_cloudfront_list_field_level_encryption_profiles_empty(cloudfront):
    resp = cloudfront.list_field_level_encryption_profiles()
    lst = resp["FieldLevelEncryptionProfileList"]
    assert lst["MaxItems"] == 100
    assert lst["Quantity"] == 0
    assert lst.get("Items", []) == []


def test_cloudfront_list_continuous_deployment_policies_empty(cloudfront):
    resp = cloudfront.list_continuous_deployment_policies()
    lst = resp["ContinuousDeploymentPolicyList"]
    assert lst["MaxItems"] == 100
    assert lst["Quantity"] == 0
    assert lst.get("Items", []) == []


def test_cloudfront_list_origin_access_identities_empty(cloudfront):
    resp = cloudfront.list_cloud_front_origin_access_identities()
    lst = resp["CloudFrontOriginAccessIdentityList"]
    assert lst["MaxItems"] == 100
    assert lst["IsTruncated"] is False
    assert lst["Quantity"] == 0
    assert lst.get("Items", []) == []


def test_cloudfront_list_streaming_distributions_empty(cloudfront):
    resp = cloudfront.list_streaming_distributions()
    lst = resp["StreamingDistributionList"]
    assert lst["MaxItems"] == 100
    assert lst["IsTruncated"] is False
    assert lst["Quantity"] == 0
    assert lst.get("Items", []) == []


def test_cloudfront_list_vpc_origins_empty(cloudfront):
    resp = cloudfront.list_vpc_origins()
    lst = resp["VpcOriginList"]
    assert lst["MaxItems"] == 100
    assert lst["IsTruncated"] is False
    assert lst["Quantity"] == 0
    assert lst.get("Items", []) == []


def test_cloudfront_list_realtime_log_configs_empty(cloudfront):
    resp = cloudfront.list_realtime_log_configs()
    lst = resp["RealtimeLogConfigs"]
    assert lst["MaxItems"] == 100
    assert lst["IsTruncated"] is False
    assert lst.get("Items", []) == []


def test_cloudfront_list_anycast_ip_lists_empty(cloudfront):
    resp = cloudfront.list_anycast_ip_lists()
    coll = resp["AnycastIpLists"]
    assert coll["MaxItems"] == 100
    assert coll["IsTruncated"] is False
    assert coll["Quantity"] == 0
    assert coll.get("Items", []) == []


def test_cloudfront_list_cache_policies_round_trip(cloudfront):
    baseline = cloudfront.list_cache_policies()["CachePolicyList"]["Quantity"]

    name = f"cp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_cache_policy(CachePolicyConfig=_cache_policy_config(name))
    pid = create["CachePolicy"]["Id"]

    listed = cloudfront.list_cache_policies()["CachePolicyList"]
    assert listed["Quantity"] == baseline + 1
    names = [s["CachePolicy"]["CachePolicyConfig"]["Name"] for s in listed["Items"]]
    assert name in names
    types = {s["Type"] for s in listed["Items"]}
    assert types == {"custom"}

    cloudfront.delete_cache_policy(Id=pid, IfMatch=create["ETag"])


def test_cloudfront_list_origin_request_policies_round_trip(cloudfront):
    baseline = cloudfront.list_origin_request_policies()["OriginRequestPolicyList"]["Quantity"]

    name = f"orp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_origin_request_policy(OriginRequestPolicyConfig=_orp_config(name))
    pid = create["OriginRequestPolicy"]["Id"]

    listed = cloudfront.list_origin_request_policies()["OriginRequestPolicyList"]
    assert listed["Quantity"] == baseline + 1
    names = [s["OriginRequestPolicy"]["OriginRequestPolicyConfig"]["Name"] for s in listed["Items"]]
    assert name in names
    assert {s["Type"] for s in listed["Items"]} == {"custom"}

    cloudfront.delete_origin_request_policy(Id=pid, IfMatch=create["ETag"])


def test_cloudfront_list_response_headers_policies_round_trip(cloudfront):
    baseline = cloudfront.list_response_headers_policies()["ResponseHeadersPolicyList"]["Quantity"]

    name = f"rhp-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_response_headers_policy(ResponseHeadersPolicyConfig=_rhp_config(name))
    pid = create["ResponseHeadersPolicy"]["Id"]

    listed = cloudfront.list_response_headers_policies()["ResponseHeadersPolicyList"]
    assert listed["Quantity"] == baseline + 1
    names = [s["ResponseHeadersPolicy"]["ResponseHeadersPolicyConfig"]["Name"] for s in listed["Items"]]
    assert name in names
    assert {s["Type"] for s in listed["Items"]} == {"custom"}

    cloudfront.delete_response_headers_policy(Id=pid, IfMatch=create["ETag"])


def test_cloudfront_get_monitoring_subscription_errors(cloudfront):
    with pytest.raises(ClientError) as exc:
        cloudfront.get_monitoring_subscription(DistributionId="EDOESNOTEXIST0")
    assert exc.value.response["Error"]["Code"] == "NoSuchDistribution"

    cfg = _custom_origin_distribution_config(f"mon-{_uuid_mod.uuid4().hex[:8]}")
    create = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create["Distribution"]["Id"]
    with pytest.raises(ClientError) as exc:
        cloudfront.get_monitoring_subscription(DistributionId=dist_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchMonitoringSubscription"

    etag = create["ETag"]
    disabled = dict(cfg, Enabled=False)
    upd = cloudfront.update_distribution(DistributionConfig=disabled, Id=dist_id, IfMatch=etag)
    cloudfront.delete_distribution(Id=dist_id, IfMatch=upd["ETag"])


# ---------------------------------------------------------------------------
# CloudFront SaaS Manager — connection groups, distribution tenants, managed
# certificates, domain verification. Shapes verified against botocore
# cloudfront service-2.json (2020-05-31).
# ---------------------------------------------------------------------------


def _tenant_only_distribution_config(caller_reference):
    cfg = copy.deepcopy(_CF_DIST_CONFIG)
    cfg["CallerReference"] = caller_reference
    cfg["Comment"] = "multi-tenant distribution"
    cfg["ConnectionMode"] = "tenant-only"
    cfg["TenantConfig"] = {
        "ParameterDefinitions": [
            {"Name": "tenantName", "Definition": {"StringSchema": {"Required": True}}}
        ]
    }
    return cfg


def _create_tenant_only_distribution(cloudfront, sfx):
    cfg = _tenant_only_distribution_config(f"cf-mt-{sfx}")
    return cloudfront.create_distribution(DistributionConfig=cfg)["Distribution"]


def test_cf_saas_create_connection_group(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    resp = cloudfront.create_connection_group(Name=f"cg-{sfx}")
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    assert resp["ETag"]
    cg = resp["ConnectionGroup"]
    assert cg["Id"].startswith("cg_")
    assert cg["Name"] == f"cg-{sfx}"
    assert cg["Arn"] == f"arn:aws:cloudfront::000000000000:connection-group/{cg['Id']}"
    assert cg["RoutingEndpoint"].endswith(".cloudfront.net")
    assert cg["Status"] == "Deployed"
    assert cg["Enabled"] is True
    assert cg["Ipv6Enabled"] is True
    assert cg["IsDefault"] is False
    assert cg["CreatedTime"] and cg["LastModifiedTime"]


def test_cf_saas_create_connection_group_duplicate_name(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    cloudfront.create_connection_group(Name=f"cg-dup-{sfx}")
    with pytest.raises(ClientError) as exc:
        cloudfront.create_connection_group(Name=f"cg-dup-{sfx}")
    assert exc.value.response["Error"]["Code"] == "EntityAlreadyExists"


def test_cf_saas_get_connection_group_by_id_name_arn(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    created = cloudfront.create_connection_group(Name=f"cg-get-{sfx}", Ipv6Enabled=False)
    cg = created["ConnectionGroup"]
    for identifier in (cg["Id"], cg["Name"], cg["Arn"]):
        got = cloudfront.get_connection_group(Identifier=identifier)
        assert got["ConnectionGroup"]["Id"] == cg["Id"]
        assert got["ConnectionGroup"]["Ipv6Enabled"] is False
        assert got["ETag"] == created["ETag"]

    with pytest.raises(ClientError) as exc:
        cloudfront.get_connection_group(Identifier="cg_DOESNOTEXIST")
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cf_saas_get_connection_group_by_routing_endpoint(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    cg = cloudfront.create_connection_group(Name=f"cg-re-{sfx}")["ConnectionGroup"]
    got = cloudfront.get_connection_group_by_routing_endpoint(RoutingEndpoint=cg["RoutingEndpoint"])
    assert got["ConnectionGroup"]["Id"] == cg["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.get_connection_group_by_routing_endpoint(RoutingEndpoint="dnope.cloudfront.net")
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cf_saas_update_connection_group(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    created = cloudfront.create_connection_group(Name=f"cg-upd-{sfx}")
    cg_id = created["ConnectionGroup"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.update_connection_group(Id=cg_id, IfMatch="bogus", Ipv6Enabled=False)
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"

    upd = cloudfront.update_connection_group(
        Id=cg_id, IfMatch=created["ETag"], Ipv6Enabled=False, Enabled=False
    )
    assert upd["ETag"] != created["ETag"]
    assert upd["ConnectionGroup"]["Ipv6Enabled"] is False
    assert upd["ConnectionGroup"]["Enabled"] is False
    assert upd["ConnectionGroup"]["Name"] == f"cg-upd-{sfx}"


def test_cf_saas_delete_connection_group(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    created = cloudfront.create_connection_group(Name=f"cg-del-{sfx}")
    cg_id = created["ConnectionGroup"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.delete_connection_group(Id=cg_id, IfMatch=created["ETag"])
    assert exc.value.response["Error"]["Code"] == "ResourceNotDisabled"

    upd = cloudfront.update_connection_group(Id=cg_id, IfMatch=created["ETag"], Enabled=False)
    cloudfront.delete_connection_group(Id=cg_id, IfMatch=upd["ETag"])
    with pytest.raises(ClientError) as exc:
        cloudfront.get_connection_group(Identifier=cg_id)
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cf_saas_list_connection_groups(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    cg = cloudfront.create_connection_group(Name=f"cg-list-{sfx}")["ConnectionGroup"]
    groups = cloudfront.list_connection_groups()["ConnectionGroups"]
    match = [g for g in groups if g["Id"] == cg["Id"]]
    assert len(match) == 1
    assert match[0]["Name"] == cg["Name"]
    assert match[0]["RoutingEndpoint"] == cg["RoutingEndpoint"]
    assert match[0]["ETag"]


def test_cf_saas_create_distribution_tenant(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    resp = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-{sfx}",
        Domains=[{"Domain": f"app-{sfx}.example.com"}],
        Parameters=[{"Name": "tenantName", "Value": "acme"}],
        Tags={"Items": [{"Key": "env", "Value": "test"}]},
    )
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 201
    assert resp["ETag"]
    tenant = resp["DistributionTenant"]
    assert tenant["Id"].startswith("dt_")
    assert tenant["Arn"] == f"arn:aws:cloudfront::000000000000:distribution-tenant/{tenant['Id']}"
    assert tenant["DistributionId"] == dist["Id"]
    assert tenant["Name"] == f"tenant-{sfx}"
    assert tenant["Domains"] == [{"Domain": f"app-{sfx}.example.com", "Status": "active"}]
    assert tenant["Parameters"] == [{"Name": "tenantName", "Value": "acme"}]
    assert tenant["Enabled"] is True
    assert tenant["Status"] == "Deployed"

    # A default connection group is created lazily when none is specified.
    cg = cloudfront.get_connection_group(Identifier=tenant["ConnectionGroupId"])["ConnectionGroup"]
    assert cg["IsDefault"] is True

    tags = cloudfront.list_tags_for_resource(Resource=tenant["Arn"])["Tags"]["Items"]
    assert {"Key": "env", "Value": "test"} in tags


def test_cf_saas_create_tenant_with_explicit_connection_group(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    cg = cloudfront.create_connection_group(Name=f"cg-exp-{sfx}")["ConnectionGroup"]
    tenant = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-exp-{sfx}",
        Domains=[{"Domain": f"exp-{sfx}.example.com"}],
        ConnectionGroupId=cg["Id"],
    )["DistributionTenant"]
    assert tenant["ConnectionGroupId"] == cg["Id"]

    # An attached connection group cannot be deleted.
    got = cloudfront.get_connection_group(Identifier=cg["Id"])
    upd = cloudfront.update_connection_group(Id=cg["Id"], IfMatch=got["ETag"], Enabled=False)
    with pytest.raises(ClientError) as exc:
        cloudfront.delete_connection_group(Id=cg["Id"], IfMatch=upd["ETag"])
    assert exc.value.response["Error"]["Code"] == "CannotDeleteEntityWhileInUse"


def test_cf_saas_create_tenant_validation_errors(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    with pytest.raises(ClientError) as exc:
        cloudfront.create_distribution_tenant(
            DistributionId="EDOESNOTEXIST0",
            Name=f"tenant-miss-{sfx}",
            Domains=[{"Domain": f"miss-{sfx}.example.com"}],
        )
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"

    # Tenants only attach to tenant-only (multi-tenant) distributions.
    direct_cfg = _custom_origin_distribution_config(f"cf-direct-{sfx}")
    direct = cloudfront.create_distribution(DistributionConfig=direct_cfg)["Distribution"]
    with pytest.raises(ClientError) as exc:
        cloudfront.create_distribution_tenant(
            DistributionId=direct["Id"],
            Name=f"tenant-direct-{sfx}",
            Domains=[{"Domain": f"direct-{sfx}.example.com"}],
        )
    assert exc.value.response["Error"]["Code"] == "InvalidAssociation"

    dist = _create_tenant_only_distribution(cloudfront, sfx)
    cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-dupe-{sfx}",
        Domains=[{"Domain": f"dupe-{sfx}.example.com"}],
    )
    with pytest.raises(ClientError) as exc:
        cloudfront.create_distribution_tenant(
            DistributionId=dist["Id"],
            Name=f"tenant-dupe-{sfx}",
            Domains=[{"Domain": f"dupe2-{sfx}.example.com"}],
        )
    assert exc.value.response["Error"]["Code"] == "EntityAlreadyExists"

    with pytest.raises(ClientError) as exc:
        cloudfront.create_distribution_tenant(
            DistributionId=dist["Id"],
            Name=f"tenant-cname-{sfx}",
            Domains=[{"Domain": f"DUPE-{sfx}.example.com"}],
        )
    assert exc.value.response["Error"]["Code"] == "CNAMEAlreadyExists"


def test_cf_saas_get_distribution_tenant(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    created = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-get-{sfx}",
        Domains=[{"Domain": f"get-{sfx}.example.com"}],
    )
    tenant = created["DistributionTenant"]
    for identifier in (tenant["Id"], tenant["Name"], tenant["Arn"]):
        got = cloudfront.get_distribution_tenant(Identifier=identifier)
        assert got["DistributionTenant"]["Id"] == tenant["Id"]
        assert got["ETag"] == created["ETag"]

    by_domain = cloudfront.get_distribution_tenant_by_domain(Domain=f"get-{sfx}.example.com")
    assert by_domain["DistributionTenant"]["Id"] == tenant["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.get_distribution_tenant(Identifier="dt_DOESNOTEXIST")
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"
    with pytest.raises(ClientError) as exc:
        cloudfront.get_distribution_tenant_by_domain(Domain="nope.example.com")
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cf_saas_update_distribution_tenant(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    created = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-upd-{sfx}",
        Domains=[{"Domain": f"upd-{sfx}.example.com"}],
    )
    tenant = created["DistributionTenant"]

    with pytest.raises(ClientError) as exc:
        cloudfront.update_distribution_tenant(Id=tenant["Id"], IfMatch="bogus", Enabled=False)
    assert exc.value.response["Error"]["Code"] == "PreconditionFailed"

    upd = cloudfront.update_distribution_tenant(
        Id=tenant["Id"],
        IfMatch=created["ETag"],
        Domains=[{"Domain": f"upd-{sfx}.example.com"}, {"Domain": f"upd2-{sfx}.example.com"}],
        Parameters=[{"Name": "tenantName", "Value": "acme2"}],
        Enabled=False,
    )
    assert upd["ETag"] != created["ETag"]
    updated = upd["DistributionTenant"]
    assert [d["Domain"] for d in updated["Domains"]] == [
        f"upd-{sfx}.example.com",
        f"upd2-{sfx}.example.com",
    ]
    assert updated["Parameters"] == [{"Name": "tenantName", "Value": "acme2"}]
    assert updated["Enabled"] is False
    assert updated["Name"] == f"tenant-upd-{sfx}"


def test_cf_saas_delete_distribution_tenant(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    created = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-del-{sfx}",
        Domains=[{"Domain": f"del-{sfx}.example.com"}],
    )
    tenant_id = created["DistributionTenant"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.delete_distribution_tenant(Id=tenant_id, IfMatch=created["ETag"])
    assert exc.value.response["Error"]["Code"] == "ResourceNotDisabled"

    upd = cloudfront.update_distribution_tenant(Id=tenant_id, IfMatch=created["ETag"], Enabled=False)
    cloudfront.delete_distribution_tenant(Id=tenant_id, IfMatch=upd["ETag"])
    with pytest.raises(ClientError) as exc:
        cloudfront.get_distribution_tenant(Identifier=tenant_id)
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cf_saas_list_distribution_tenants(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist_a = _create_tenant_only_distribution(cloudfront, f"{sfx}-a")
    dist_b = _create_tenant_only_distribution(cloudfront, f"{sfx}-b")
    cg = cloudfront.create_connection_group(Name=f"cg-flt-{sfx}")["ConnectionGroup"]
    t_a = cloudfront.create_distribution_tenant(
        DistributionId=dist_a["Id"],
        Name=f"tenant-la-{sfx}",
        Domains=[{"Domain": f"la-{sfx}.example.com"}],
        ConnectionGroupId=cg["Id"],
    )["DistributionTenant"]
    t_b = cloudfront.create_distribution_tenant(
        DistributionId=dist_b["Id"],
        Name=f"tenant-lb-{sfx}",
        Domains=[{"Domain": f"lb-{sfx}.example.com"}],
    )["DistributionTenant"]

    all_ids = [t["Id"] for t in cloudfront.list_distribution_tenants()["DistributionTenantList"]]
    assert t_a["Id"] in all_ids and t_b["Id"] in all_ids

    by_dist = cloudfront.list_distribution_tenants(
        AssociationFilter={"DistributionId": dist_a["Id"]}
    )["DistributionTenantList"]
    assert [t["Id"] for t in by_dist] == [t_a["Id"]]
    assert by_dist[0]["Domains"] == [{"Domain": f"la-{sfx}.example.com", "Status": "active"}]
    assert by_dist[0]["ETag"]

    by_cg = cloudfront.list_distribution_tenants(
        AssociationFilter={"ConnectionGroupId": cg["Id"]}
    )["DistributionTenantList"]
    assert [t["Id"] for t in by_cg] == [t_a["Id"]]


def test_cf_saas_list_distribution_tenants_by_customization(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    cert_arn = f"arn:aws:acm:us-east-1:000000000000:certificate/{sfx}"
    tenant = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-cust-{sfx}",
        Domains=[{"Domain": f"cust-{sfx}.example.com"}],
        Customizations={"Certificate": {"Arn": cert_arn}},
    )["DistributionTenant"]
    assert tenant["Customizations"]["Certificate"]["Arn"] == cert_arn

    by_cert = cloudfront.list_distribution_tenants_by_customization(CertificateArn=cert_arn)[
        "DistributionTenantList"
    ]
    assert [t["Id"] for t in by_cert] == [tenant["Id"]]

    none = cloudfront.list_distribution_tenants_by_customization(
        CertificateArn="arn:aws:acm:us-east-1:000000000000:certificate/none"
    )["DistributionTenantList"]
    assert none == []


def test_cf_saas_tenant_webacl_associate_disassociate(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    created = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-acl-{sfx}",
        Domains=[{"Domain": f"acl-{sfx}.example.com"}],
    )
    tenant_id = created["DistributionTenant"]["Id"]
    acl_arn = f"arn:aws:wafv2:us-east-1:000000000000:global/webacl/test/{sfx}"

    assoc = cloudfront.associate_distribution_tenant_web_acl(
        Id=tenant_id, WebACLArn=acl_arn, IfMatch=created["ETag"]
    )
    assert assoc["Id"] == tenant_id
    assert assoc["WebACLArn"] == acl_arn
    assert assoc["ETag"] != created["ETag"]

    got = cloudfront.get_distribution_tenant(Identifier=tenant_id)["DistributionTenant"]
    assert got["Customizations"]["WebAcl"] == {"Action": "override", "Arn": acl_arn}

    dis = cloudfront.disassociate_distribution_tenant_web_acl(Id=tenant_id, IfMatch=assoc["ETag"])
    assert dis["Id"] == tenant_id
    got = cloudfront.get_distribution_tenant(Identifier=tenant_id)["DistributionTenant"]
    assert "WebAcl" not in got.get("Customizations", {})


def test_cf_saas_tenant_invalidations(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    tenant = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-inv-{sfx}",
        Domains=[{"Domain": f"inv-{sfx}.example.com"}],
    )["DistributionTenant"]

    batch = {
        "Paths": {"Quantity": 2, "Items": ["/index.html", "/assets/*"]},
        "CallerReference": f"inv-{sfx}",
    }
    created = cloudfront.create_invalidation_for_distribution_tenant(
        Id=tenant["Id"], InvalidationBatch=batch
    )
    assert created["ResponseMetadata"]["HTTPStatusCode"] == 201
    inv = created["Invalidation"]
    assert inv["Status"] == "Completed"
    assert sorted(inv["InvalidationBatch"]["Paths"]["Items"]) == ["/assets/*", "/index.html"]

    got = cloudfront.get_invalidation_for_distribution_tenant(
        DistributionTenantId=tenant["Id"], Id=inv["Id"]
    )["Invalidation"]
    assert got["Id"] == inv["Id"]

    listed = cloudfront.list_invalidations_for_distribution_tenant(Id=tenant["Id"])[
        "InvalidationList"
    ]
    assert [i["Id"] for i in listed["Items"]] == [inv["Id"]]

    with pytest.raises(ClientError) as exc:
        cloudfront.get_invalidation_for_distribution_tenant(
            DistributionTenantId=tenant["Id"], Id="IDOESNOTEXIST0"
        )
    assert exc.value.response["Error"]["Code"] == "NoSuchInvalidation"
    with pytest.raises(ClientError) as exc:
        cloudfront.list_invalidations_for_distribution_tenant(Id="dt_DOESNOTEXIST")
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cf_saas_verify_dns_configuration(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    tenant = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-dns-{sfx}",
        Domains=[{"Domain": f"dns1-{sfx}.example.com"}, {"Domain": f"dns2-{sfx}.example.com"}],
    )["DistributionTenant"]

    all_domains = cloudfront.verify_dns_configuration(Identifier=tenant["Id"])[
        "DnsConfigurationList"
    ]
    assert {d["Domain"] for d in all_domains} == {
        f"dns1-{sfx}.example.com",
        f"dns2-{sfx}.example.com",
    }
    assert {d["Status"] for d in all_domains} == {"valid-configuration"}

    one = cloudfront.verify_dns_configuration(
        Identifier=tenant["Id"], Domain=f"dns2-{sfx}.example.com"
    )["DnsConfigurationList"]
    assert [d["Domain"] for d in one] == [f"dns2-{sfx}.example.com"]

    with pytest.raises(ClientError) as exc:
        cloudfront.verify_dns_configuration(Identifier="dt_DOESNOTEXIST")
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cf_saas_managed_certificate_details(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    tenant = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-cert-{sfx}",
        Domains=[{"Domain": f"cert-{sfx}.example.com"}],
        ManagedCertificateRequest={"ValidationTokenHost": "cloudfront"},
    )["DistributionTenant"]

    details = cloudfront.get_managed_certificate_details(Identifier=tenant["Id"])[
        "ManagedCertificateDetails"
    ]
    assert details["CertificateStatus"] == "issued"
    assert details["CertificateArn"].startswith("arn:aws:acm:us-east-1:000000000000:certificate/")
    assert details["ValidationTokenHost"] == "cloudfront"
    assert [d["Domain"] for d in details["ValidationTokenDetails"]] == [f"cert-{sfx}.example.com"]

    # A tenant without a managed certificate returns empty details.
    plain = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-nocert-{sfx}",
        Domains=[{"Domain": f"nocert-{sfx}.example.com"}],
    )["DistributionTenant"]
    empty = cloudfront.get_managed_certificate_details(Identifier=plain["Id"])[
        "ManagedCertificateDetails"
    ]
    assert "CertificateArn" not in empty

    with pytest.raises(ClientError) as exc:
        cloudfront.get_managed_certificate_details(Identifier="dt_DOESNOTEXIST")
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cf_saas_list_domain_conflicts(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    t_a = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-dca-{sfx}",
        Domains=[{"Domain": f"conflict-{sfx}.example.com"}],
    )["DistributionTenant"]
    t_b = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-dcb-{sfx}",
        Domains=[{"Domain": f"other-{sfx}.example.com"}],
    )["DistributionTenant"]

    conflicts = cloudfront.list_domain_conflicts(
        Domain=f"conflict-{sfx}.example.com",
        DomainControlValidationResource={"DistributionTenantId": t_b["Id"]},
    )["DomainConflicts"]
    assert conflicts == [
        {
            "Domain": f"conflict-{sfx}.example.com",
            "ResourceType": "distribution-tenant",
            "ResourceId": t_a["Id"],
            "AccountId": "000000000000",
        }
    ]

    # The querying resource itself is excluded from conflicts.
    own = cloudfront.list_domain_conflicts(
        Domain=f"conflict-{sfx}.example.com",
        DomainControlValidationResource={"DistributionTenantId": t_a["Id"]},
    )["DomainConflicts"]
    assert own == []


def test_cf_saas_update_domain_association(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    t_a = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-mva-{sfx}",
        Domains=[{"Domain": f"move-{sfx}.example.com"}, {"Domain": f"keep-{sfx}.example.com"}],
    )["DistributionTenant"]
    t_b = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-mvb-{sfx}",
        Domains=[{"Domain": f"target-{sfx}.example.com"}],
    )["DistributionTenant"]

    moved = cloudfront.update_domain_association(
        Domain=f"move-{sfx}.example.com",
        TargetResource={"DistributionTenantId": t_b["Id"]},
    )
    assert moved["Domain"] == f"move-{sfx}.example.com"
    assert moved["ResourceId"] == t_b["Id"]
    assert moved["ETag"]

    got_a = cloudfront.get_distribution_tenant(Identifier=t_a["Id"])["DistributionTenant"]
    got_b = cloudfront.get_distribution_tenant(Identifier=t_b["Id"])["DistributionTenant"]
    assert [d["Domain"] for d in got_a["Domains"]] == [f"keep-{sfx}.example.com"]
    assert sorted(d["Domain"] for d in got_b["Domains"]) == [
        f"move-{sfx}.example.com",
        f"target-{sfx}.example.com",
    ]

    with pytest.raises(ClientError) as exc:
        cloudfront.update_domain_association(
            Domain=f"keep-{sfx}.example.com",
            TargetResource={"DistributionTenantId": "dt_DOESNOTEXIST"},
        )
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cf_saas_list_distributions_by_connection_mode(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    mt = _create_tenant_only_distribution(cloudfront, sfx)
    direct_cfg = _custom_origin_distribution_config(f"cf-lbm-{sfx}")
    direct = cloudfront.create_distribution(DistributionConfig=direct_cfg)["Distribution"]

    mt_list = cloudfront.list_distributions_by_connection_mode(ConnectionMode="tenant-only")[
        "DistributionList"
    ]
    mt_ids = [d["Id"] for d in mt_list.get("Items", [])]
    assert mt["Id"] in mt_ids
    assert direct["Id"] not in mt_ids
    assert all(d["ConnectionMode"] == "tenant-only" for d in mt_list.get("Items", []))

    direct_list = cloudfront.list_distributions_by_connection_mode(ConnectionMode="direct")[
        "DistributionList"
    ]
    direct_ids = [d["Id"] for d in direct_list.get("Items", [])]
    assert direct["Id"] in direct_ids
    assert mt["Id"] not in direct_ids


def test_cf_saas_distribution_round_trips_connection_mode(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    got_cfg = cloudfront.get_distribution_config(Id=dist["Id"])["DistributionConfig"]
    assert got_cfg["ConnectionMode"] == "tenant-only"
    defs = got_cfg["TenantConfig"]["ParameterDefinitions"]
    assert defs[0]["Name"] == "tenantName"
    assert defs[0]["Definition"]["StringSchema"]["Required"] is True

    summaries = cloudfront.list_distributions()["DistributionList"]["Items"]
    mine = [d for d in summaries if d["Id"] == dist["Id"]]
    assert mine and mine[0]["ConnectionMode"] == "tenant-only"


def test_cf_saas_rejected_tenant_update_leaves_tenant_unchanged(cloudfront):
    """A validation failure part-way through UpdateDistributionTenant must not
    commit any of the request's mutations (real AWS validates before writing)."""
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    created = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-atomic-{sfx}",
        Domains=[{"Domain": f"atomic-{sfx}.example.com"}],
        Parameters=[{"Name": "tenantName", "Value": "before"}],
    )
    tenant = created["DistributionTenant"]

    with pytest.raises(ClientError) as exc:
        cloudfront.update_distribution_tenant(
            Id=tenant["Id"],
            IfMatch=created["ETag"],
            Domains=[{"Domain": f"atomic-changed-{sfx}.example.com"}],
            Parameters=[{"Name": "tenantName", "Value": "after"}],
            Enabled=False,
            ManagedCertificateRequest={"ValidationTokenHost": "bogus-host"},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"

    got = cloudfront.get_distribution_tenant(Identifier=tenant["Id"])
    assert got["ETag"] == created["ETag"]
    unchanged = got["DistributionTenant"]
    assert [d["Domain"] for d in unchanged["Domains"]] == [f"atomic-{sfx}.example.com"]
    assert unchanged["Parameters"] == [{"Name": "tenantName", "Value": "before"}]
    assert unchanged["Enabled"] is True

    cg = cloudfront.create_connection_group(Name=f"cg-atomic-{sfx}")["ConnectionGroup"]
    with pytest.raises(ClientError) as exc:
        cloudfront.update_distribution_tenant(
            Id=tenant["Id"],
            IfMatch=created["ETag"],
            ConnectionGroupId=cg["Id"],
            Customizations={"WebAcl": {"Action": "bogus-action"}},
        )
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"
    got = cloudfront.get_distribution_tenant(Identifier=tenant["Id"])["DistributionTenant"]
    assert got["ConnectionGroupId"] != cg["Id"]


def test_cf_saas_metadata_only_updates_accept_empty_body(cloudfront):
    """boto3 serializes UpdateConnectionGroup/UpdateDistributionTenant with only
    Id + IfMatch as an empty request body; real AWS accepts it."""
    sfx = _uuid_mod.uuid4().hex[:8]
    created = cloudfront.create_connection_group(Name=f"cg-empty-{sfx}")
    upd = cloudfront.update_connection_group(Id=created["ConnectionGroup"]["Id"], IfMatch=created["ETag"])
    assert upd["ETag"] != created["ETag"]
    assert upd["ConnectionGroup"]["Enabled"] is True
    assert upd["ConnectionGroup"]["Ipv6Enabled"] is True

    dist = _create_tenant_only_distribution(cloudfront, sfx)
    t_created = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-empty-{sfx}",
        Domains=[{"Domain": f"empty-{sfx}.example.com"}],
    )
    t_upd = cloudfront.update_distribution_tenant(
        Id=t_created["DistributionTenant"]["Id"], IfMatch=t_created["ETag"]
    )
    assert t_upd["ETag"] != t_created["ETag"]
    assert [d["Domain"] for d in t_upd["DistributionTenant"]["Domains"]] == [f"empty-{sfx}.example.com"]


def test_cf_saas_create_tenant_rejects_duplicate_domains_in_request(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    with pytest.raises(ClientError) as exc:
        cloudfront.create_distribution_tenant(
            DistributionId=dist["Id"],
            Name=f"tenant-dupdom-{sfx}",
            Domains=[{"Domain": f"dup-{sfx}.example.com"}, {"Domain": f"DUP-{sfx}.example.com"}],
        )
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"
    with pytest.raises(ClientError) as exc:
        cloudfront.get_distribution_tenant(Identifier=f"tenant-dupdom-{sfx}")
    assert exc.value.response["Error"]["Code"] == "EntityNotFound"


def test_cf_saas_delete_distribution_with_tenants_refused(cloudfront):
    """Deleting a multi-tenant distribution that still has tenants must fail
    with ResourceInUse instead of orphaning the tenants."""
    sfx = _uuid_mod.uuid4().hex[:8]
    cfg = _tenant_only_distribution_config(f"cf-deldist-{sfx}")
    created = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = created["Distribution"]["Id"]
    t_created = cloudfront.create_distribution_tenant(
        DistributionId=dist_id,
        Name=f"tenant-deldist-{sfx}",
        Domains=[{"Domain": f"deldist-{sfx}.example.com"}],
    )

    disabled_cfg = dict(cfg, Enabled=False)
    upd = cloudfront.update_distribution(DistributionConfig=disabled_cfg, Id=dist_id, IfMatch=created["ETag"])
    with pytest.raises(ClientError) as exc:
        cloudfront.delete_distribution(Id=dist_id, IfMatch=upd["ETag"])
    assert exc.value.response["Error"]["Code"] == "ResourceInUse"

    t_upd = cloudfront.update_distribution_tenant(
        Id=t_created["DistributionTenant"]["Id"], IfMatch=t_created["ETag"], Enabled=False
    )
    cloudfront.delete_distribution_tenant(Id=t_created["DistributionTenant"]["Id"], IfMatch=t_upd["ETag"])
    cloudfront.delete_distribution(Id=dist_id, IfMatch=upd["ETag"])


def test_cf_saas_by_customization_result_root_element(cloudfront):
    """The by-customization list uses its own result root element; boto3
    tolerates a wrong root, so assert on the raw wire bytes."""
    req = urllib.request.Request(
        f"{ENDPOINT}/2020-05-31/distribution-tenants-by-customization",
        data=b"",
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        body = resp.read()
    assert b"<ListDistributionTenantsByCustomizationResult" in body


def test_cf_saas_tenant_ops_survive_cfn_created_distribution(cfn, cloudfront):
    """CloudFormation provisions distribution records with an empty config_xml;
    tenant-side scans over all distributions must not choke on them."""
    sfx = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cf-saas-cfn-{sfx}"
    template = {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Resources": {
            "Distribution": {
                "Type": "AWS::CloudFront::Distribution",
                "Properties": {"DistributionConfig": {"Enabled": True}},
            },
        },
        "Outputs": {"DistributionId": {"Value": {"Ref": "Distribution"}}},
    }
    cfn.create_stack(StackName=stack_name, TemplateBody=json.dumps(template))
    for _ in range(30):
        stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
        if not stack["StackStatus"].endswith("_IN_PROGRESS"):
            break
        time.sleep(0.5)
    assert stack["StackStatus"] == "CREATE_COMPLETE"
    cfn_dist_id = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}["DistributionId"]

    try:
        dist = _create_tenant_only_distribution(cloudfront, sfx)
        tenant = cloudfront.create_distribution_tenant(
            DistributionId=dist["Id"],
            Name=f"tenant-cfn-{sfx}",
            Domains=[{"Domain": f"cfn-{sfx}.example.com"}],
        )["DistributionTenant"]
        assert tenant["Id"].startswith("dt_")

        direct = cloudfront.list_distributions_by_connection_mode(ConnectionMode="direct")[
            "DistributionList"
        ]
        assert cfn_dist_id in [d["Id"] for d in direct.get("Items", [])]
    finally:
        cfn.delete_stack(StackName=stack_name)


def test_cf_saas_list_domain_conflicts_wildcard_overlap(cloudfront):
    """ListDomainConflicts reports single-level wildcard overlaps, not just
    exact matches."""
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    t_wild = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-wild-{sfx}",
        Domains=[{"Domain": f"*.wc-{sfx}.example.com"}],
    )["DistributionTenant"]
    t_other = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-wildb-{sfx}",
        Domains=[{"Domain": f"wildb-{sfx}.example.com"}],
    )["DistributionTenant"]

    conflicts = cloudfront.list_domain_conflicts(
        Domain=f"app.wc-{sfx}.example.com",
        DomainControlValidationResource={"DistributionTenantId": t_other["Id"]},
    )["DomainConflicts"]
    assert [c["ResourceId"] for c in conflicts] == [t_wild["Id"]]

    # A wildcard covers exactly one label.
    deep = cloudfront.list_domain_conflicts(
        Domain=f"a.b.wc-{sfx}.example.com",
        DomainControlValidationResource={"DistributionTenantId": t_other["Id"]},
    )["DomainConflicts"]
    assert deep == []


def test_cf_saas_resourcegroupstagging_tags_new_families(cloudfront, tagging):
    """The Resource Groups Tagging API can tag a distribution tenant and a
    connection group that have no tags yet."""
    sfx = _uuid_mod.uuid4().hex[:8]
    dist = _create_tenant_only_distribution(cloudfront, sfx)
    tenant = cloudfront.create_distribution_tenant(
        DistributionId=dist["Id"],
        Name=f"tenant-rgt-{sfx}",
        Domains=[{"Domain": f"rgt-{sfx}.example.com"}],
    )["DistributionTenant"]
    cg = cloudfront.create_connection_group(Name=f"cg-rgt-{sfx}")["ConnectionGroup"]

    result = tagging.tag_resources(
        ResourceARNList=[tenant["Arn"], cg["Arn"]], Tags={"team": "edge"}
    )
    assert result["FailedResourcesMap"] == {}
    for arn in (tenant["Arn"], cg["Arn"]):
        tags = cloudfront.list_tags_for_resource(Resource=arn)["Tags"]["Items"]
        assert {"Key": "team", "Value": "edge"} in tags


def test_cf_saas_connection_group_tagging(cloudfront):
    sfx = _uuid_mod.uuid4().hex[:8]
    cg = cloudfront.create_connection_group(
        Name=f"cg-tag-{sfx}", Tags={"Items": [{"Key": "team", "Value": "edge"}]}
    )["ConnectionGroup"]
    tags = cloudfront.list_tags_for_resource(Resource=cg["Arn"])["Tags"]["Items"]
    assert {"Key": "team", "Value": "edge"} in tags

    cloudfront.tag_resource(
        Resource=cg["Arn"], Tags={"Items": [{"Key": "env", "Value": "test"}]}
    )
    tags = cloudfront.list_tags_for_resource(Resource=cg["Arn"])["Tags"]["Items"]
    assert {"Key": "env", "Value": "test"} in tags
