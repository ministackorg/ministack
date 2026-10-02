import asyncio
import copy
import http.client
import http.server
import io
import json
import os
import re
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid as _uuid_mod
import zipfile
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from conftest import GATEWAY_PORT

from ministack.services import cloudfront as cloudfront_svc

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

# AWS's CloudFront edge domain shape: 'd' + 13 lowercase alphanumerics +
# '.cloudfront.net' (e.g. d111111abcdef8.cloudfront.net), unrelated to the
# distribution's own Id.
_CF_DOMAIN_RE = re.compile(r"^d[a-z0-9]{13}\.cloudfront\.net$")

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
    assert _CF_DOMAIN_RE.match(dist["DomainName"])
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
    assert _CF_DOMAIN_RE.match(dist["DomainName"])
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
    assert _CF_DOMAIN_RE.match(dist["DomainName"])
    assert dist_id not in dist["DomainName"]
    assert dist["Status"] == "Deployed"
    # terraform-provider-aws v6+ dereferences OriginGroups without a nil check
    assert dist["DistributionConfig"]["OriginGroups"]["Quantity"] == 0


def test_cloudfront_distribution_domain_name_like_aws(cloudfront):
    """DomainName is 'd' + 13 lowercase alphanumerics, like real AWS, and is
    stable across Get/Update/List — not derived from the distribution's Id."""
    cfg = {**_CF_DIST_CONFIG, "CallerReference": f"cf-domain-{_uuid_mod.uuid4().hex[:12]}"}
    create_resp = cloudfront.create_distribution(DistributionConfig=cfg)
    dist_id = create_resp["Distribution"]["Id"]
    domain = create_resp["Distribution"]["DomainName"]
    assert _CF_DOMAIN_RE.match(domain)
    assert dist_id not in domain

    get_domain = cloudfront.get_distribution(Id=dist_id)["Distribution"]["DomainName"]
    assert get_domain == domain

    disabled_cfg = {**cfg, "Enabled": False}
    upd = cloudfront.update_distribution(
        DistributionConfig=disabled_cfg, Id=dist_id, IfMatch=create_resp["ETag"]
    )
    assert upd["Distribution"]["DomainName"] == domain

    listed = {d["Id"]: d["DomainName"] for d in cloudfront.list_distributions()["DistributionList"]["Items"]}
    assert listed[dist_id] == domain


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


def test_cloudfront_function_update_keeps_the_published_live_stage(cloudfront):
    """UpdateFunction changes the DEVELOPMENT stage only: "To copy the updates
    from the DEVELOPMENT stage to LIVE, you must publish the function". The
    published version keeps serving until PublishFunction runs again."""
    name = f"fn-live-{_uuid_mod.uuid4().hex[:8]}"
    published = b"function handler(event) { return 'published'; }"
    updated = b"function handler(event) { return 'updated'; }"

    cr = cloudfront.create_function(
        Name=name,
        FunctionConfig={"Comment": "v1", "Runtime": "cloudfront-js-1.0"},
        FunctionCode=published,
    )
    cloudfront.publish_function(Name=name, IfMatch=_cf_resp_etag(cr))

    upd = cloudfront.update_function(
        Name=name,
        IfMatch=_cf_resp_etag(cloudfront.describe_function(Name=name, Stage="DEVELOPMENT")),
        FunctionConfig={"Comment": "v2", "Runtime": "cloudfront-js-1.0"},
        FunctionCode=updated,
    )

    live = cloudfront.describe_function(Name=name, Stage="LIVE")["FunctionSummary"]
    assert live["FunctionConfig"]["Comment"] == "v1"
    live_body = cloudfront.get_function(Name=name, Stage="LIVE")["FunctionCode"]
    assert (live_body.read() if hasattr(live_body, "read") else live_body) == published
    dev_body = cloudfront.get_function(Name=name, Stage="DEVELOPMENT")["FunctionCode"]
    assert (dev_body.read() if hasattr(dev_body, "read") else dev_body) == updated

    cloudfront.publish_function(Name=name, IfMatch=_cf_resp_etag(upd))
    live_body = cloudfront.get_function(Name=name, Stage="LIVE")["FunctionCode"]
    assert (live_body.read() if hasattr(live_body, "read") else live_body) == updated
    live = cloudfront.describe_function(Name=name, Stage="LIVE")["FunctionSummary"]
    assert live["FunctionConfig"]["Comment"] == "v2"

    cloudfront.delete_function(
        Name=name,
        IfMatch=_cf_resp_etag(cloudfront.describe_function(Name=name, Stage="DEVELOPMENT")),
    )


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
# AWS-managed cache / origin-request / response-headers policies.
#
# Names, ids and configs verified against the CloudFront Developer Guide's
# "Use managed cache policies", "Use managed origin request policies", and
# "Use managed response headers policies" pages. Real ids used below are the
# ones ops-v2's modules/branding-assets and modules/cloudfront-api already
# reference by literal id/name.
# ---------------------------------------------------------------------------

_MANAGED_CACHING_DISABLED_ID = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
_MANAGED_CACHING_OPTIMIZED_ID = "658327ea-f89d-4fab-a63d-7e88639e58f6"
_MANAGED_ALL_VIEWER_ID = "216adef6-5c7f-47e4-b989-5492eafa07d3"
_MANAGED_ALL_VIEWER_EXCEPT_HOST_ID = "b689b0a8-53d0-40ab-baf2-68738e2966ac"
_MANAGED_SECURITY_HEADERS_ID = "67f7725c-6f97-4210-82d7-5512b31e9d03"


def test_cloudfront_list_cache_policies_includes_aws_managed(cloudfront):
    resp = cloudfront.list_cache_policies()["CachePolicyList"]
    by_id = {i["CachePolicy"]["Id"]: i for i in resp["Items"]}
    assert _MANAGED_CACHING_DISABLED_ID in by_id
    assert _MANAGED_CACHING_OPTIMIZED_ID in by_id
    assert by_id[_MANAGED_CACHING_DISABLED_ID]["Type"] == "managed"
    assert by_id[_MANAGED_CACHING_DISABLED_ID]["CachePolicy"]["CachePolicyConfig"]["Name"] == "Managed-CachingDisabled"

    managed_only = cloudfront.list_cache_policies(Type="managed")["CachePolicyList"]
    assert all(i["Type"] == "managed" for i in managed_only["Items"])
    assert _MANAGED_CACHING_DISABLED_ID in {i["CachePolicy"]["Id"] for i in managed_only["Items"]}

    custom_only = cloudfront.list_cache_policies(Type="custom")["CachePolicyList"]
    assert _MANAGED_CACHING_DISABLED_ID not in {i["CachePolicy"]["Id"] for i in custom_only.get("Items", [])}


def test_cloudfront_get_managed_cache_policy_by_id(cloudfront):
    got = cloudfront.get_cache_policy(Id=_MANAGED_CACHING_DISABLED_ID)
    cfg = got["CachePolicy"]["CachePolicyConfig"]
    assert cfg["Name"] == "Managed-CachingDisabled"
    assert cfg["MinTTL"] == 0 and cfg["MaxTTL"] == 0 and cfg["DefaultTTL"] == 0
    params = cfg["ParametersInCacheKeyAndForwardedToOrigin"]
    assert params["EnableAcceptEncodingGzip"] is False
    assert params["HeadersConfig"]["HeaderBehavior"] == "none"

    # Stable ETag across repeated reads (no mutation is possible).
    assert cloudfront.get_cache_policy(Id=_MANAGED_CACHING_DISABLED_ID)["ETag"] == got["ETag"]

    cfg_resp = cloudfront.get_cache_policy_config(Id=_MANAGED_CACHING_OPTIMIZED_ID)
    optimized = cfg_resp["CachePolicyConfig"]
    assert optimized["Name"] == "Managed-CachingOptimized"
    assert optimized["MinTTL"] == 1 and optimized["DefaultTTL"] == 86400 and optimized["MaxTTL"] == 31536000
    opt_params = optimized["ParametersInCacheKeyAndForwardedToOrigin"]
    assert opt_params["EnableAcceptEncodingGzip"] is True
    assert opt_params["EnableAcceptEncodingBrotli"] is True


def test_cloudfront_managed_cache_policy_is_immutable(cloudfront):
    with pytest.raises(ClientError) as exc:
        cloudfront.update_cache_policy(
            Id=_MANAGED_CACHING_DISABLED_ID,
            IfMatch=cloudfront.get_cache_policy(Id=_MANAGED_CACHING_DISABLED_ID)["ETag"],
            CachePolicyConfig=_cache_policy_config("attempted-rename"),
        )
    assert exc.value.response["Error"]["Code"] == "IllegalUpdate"

    with pytest.raises(ClientError) as exc:
        cloudfront.delete_cache_policy(
            Id=_MANAGED_CACHING_DISABLED_ID,
            IfMatch=cloudfront.get_cache_policy(Id=_MANAGED_CACHING_DISABLED_ID)["ETag"],
        )
    assert exc.value.response["Error"]["Code"] == "IllegalDelete"

    # Still there afterwards.
    assert cloudfront.get_cache_policy(Id=_MANAGED_CACHING_DISABLED_ID)


def test_cloudfront_custom_cache_policy_cannot_reuse_managed_name(cloudfront):
    with pytest.raises(ClientError) as exc:
        cloudfront.create_cache_policy(CachePolicyConfig=_cache_policy_config("Managed-CachingDisabled"))
    assert exc.value.response["Error"]["Code"] == "CachePolicyAlreadyExists"


def test_cloudfront_list_origin_request_policies_includes_aws_managed(cloudfront):
    resp = cloudfront.list_origin_request_policies()["OriginRequestPolicyList"]
    by_id = {i["OriginRequestPolicy"]["Id"]: i for i in resp["Items"]}
    assert _MANAGED_ALL_VIEWER_ID in by_id
    assert _MANAGED_ALL_VIEWER_EXCEPT_HOST_ID in by_id
    assert by_id[_MANAGED_ALL_VIEWER_ID]["Type"] == "managed"
    all_viewer_cfg = by_id[_MANAGED_ALL_VIEWER_ID]["OriginRequestPolicy"]["OriginRequestPolicyConfig"]
    assert all_viewer_cfg["Name"] == "Managed-AllViewer"
    assert all_viewer_cfg["HeadersConfig"]["HeaderBehavior"] == "allViewer"

    except_host_cfg = cloudfront.get_origin_request_policy_config(
        Id=_MANAGED_ALL_VIEWER_EXCEPT_HOST_ID
    )["OriginRequestPolicyConfig"]
    assert except_host_cfg["Name"] == "Managed-AllViewerExceptHostHeader"
    assert except_host_cfg["HeadersConfig"]["HeaderBehavior"] == "allExcept"
    assert except_host_cfg["HeadersConfig"]["Headers"]["Items"] == ["Host"]
    assert except_host_cfg["CookiesConfig"]["CookieBehavior"] == "all"


def test_cloudfront_managed_origin_request_policy_is_immutable(cloudfront):
    with pytest.raises(ClientError) as exc:
        cloudfront.delete_origin_request_policy(
            Id=_MANAGED_ALL_VIEWER_ID,
            IfMatch=cloudfront.get_origin_request_policy(Id=_MANAGED_ALL_VIEWER_ID)["ETag"],
        )
    assert exc.value.response["Error"]["Code"] == "IllegalDelete"

    with pytest.raises(ClientError) as exc:
        cloudfront.update_origin_request_policy(
            Id=_MANAGED_ALL_VIEWER_ID,
            IfMatch=cloudfront.get_origin_request_policy(Id=_MANAGED_ALL_VIEWER_ID)["ETag"],
            OriginRequestPolicyConfig=_orp_config("attempted-rename"),
        )
    assert exc.value.response["Error"]["Code"] == "IllegalUpdate"


def test_cloudfront_list_response_headers_policies_includes_aws_managed(cloudfront):
    resp = cloudfront.list_response_headers_policies()["ResponseHeadersPolicyList"]
    by_id = {i["ResponseHeadersPolicy"]["Id"]: i for i in resp["Items"]}
    assert _MANAGED_SECURITY_HEADERS_ID in by_id
    assert by_id[_MANAGED_SECURITY_HEADERS_ID]["Type"] == "managed"

    cfg = cloudfront.get_response_headers_policy_config(Id=_MANAGED_SECURITY_HEADERS_ID)["ResponseHeadersPolicyConfig"]
    assert cfg["Name"] == "Managed-SecurityHeadersPolicy"
    assert "CorsConfig" not in cfg
    sec = cfg["SecurityHeadersConfig"]
    assert sec["FrameOptions"]["FrameOption"] == "SAMEORIGIN"
    assert sec["ReferrerPolicy"]["ReferrerPolicy"] == "strict-origin-when-cross-origin"
    assert sec["ContentTypeOptions"]["Override"] is True
    assert sec["StrictTransportSecurity"]["AccessControlMaxAgeSec"] == 31536000


def test_cloudfront_managed_response_headers_policy_is_immutable(cloudfront):
    with pytest.raises(ClientError) as exc:
        cloudfront.delete_response_headers_policy(
            Id=_MANAGED_SECURITY_HEADERS_ID,
            IfMatch=cloudfront.get_response_headers_policy(Id=_MANAGED_SECURITY_HEADERS_ID)["ETag"],
        )
    assert exc.value.response["Error"]["Code"] == "IllegalDelete"


# ---------------------------------------------------------------------------
# Monitoring subscriptions (aws_cloudfront_monitoring_subscription).
# Wire shapes verified against botocore cloudfront service-2.json (2020-05-31)
# and the CreateMonitoringSubscription API reference (POST, 200 response;
# DeleteMonitoringSubscription: DELETE, 200 with an empty body).
# ---------------------------------------------------------------------------


def test_cloudfront_monitoring_subscription_lifecycle(cloudfront):
    dist_id = cloudfront.create_distribution(
        DistributionConfig=_custom_origin_distribution_config(f"mon-{_uuid_mod.uuid4().hex[:8]}")
    )["Distribution"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.get_monitoring_subscription(DistributionId=dist_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchMonitoringSubscription"

    created = cloudfront.create_monitoring_subscription(
        DistributionId=dist_id,
        MonitoringSubscription={
            "RealtimeMetricsSubscriptionConfig": {"RealtimeMetricsSubscriptionStatus": "Enabled"}
        },
    )
    assert (
        created["MonitoringSubscription"]["RealtimeMetricsSubscriptionConfig"]["RealtimeMetricsSubscriptionStatus"]
        == "Enabled"
    )

    got = cloudfront.get_monitoring_subscription(DistributionId=dist_id)
    assert (
        got["MonitoringSubscription"]["RealtimeMetricsSubscriptionConfig"]["RealtimeMetricsSubscriptionStatus"]
        == "Enabled"
    )

    # A second Create overwrites rather than erroring: terraform-provider-aws's
    # aws_cloudfront_monitoring_subscription resource calls this same
    # operation for both Create and Update.
    cloudfront.create_monitoring_subscription(
        DistributionId=dist_id,
        MonitoringSubscription={
            "RealtimeMetricsSubscriptionConfig": {"RealtimeMetricsSubscriptionStatus": "Disabled"}
        },
    )
    got = cloudfront.get_monitoring_subscription(DistributionId=dist_id)
    assert (
        got["MonitoringSubscription"]["RealtimeMetricsSubscriptionConfig"]["RealtimeMetricsSubscriptionStatus"]
        == "Disabled"
    )

    cloudfront.delete_monitoring_subscription(DistributionId=dist_id)
    with pytest.raises(ClientError) as exc:
        cloudfront.get_monitoring_subscription(DistributionId=dist_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchMonitoringSubscription"


def test_cloudfront_monitoring_subscription_missing_distribution(cloudfront):
    for call in (
        lambda: cloudfront.get_monitoring_subscription(DistributionId="ENOSUCHDIST0000000"),
        lambda: cloudfront.delete_monitoring_subscription(DistributionId="ENOSUCHDIST0000000"),
        lambda: cloudfront.create_monitoring_subscription(
            DistributionId="ENOSUCHDIST0000000",
            MonitoringSubscription={
                "RealtimeMetricsSubscriptionConfig": {"RealtimeMetricsSubscriptionStatus": "Enabled"}
            },
        ),
    ):
        with pytest.raises(ClientError) as exc:
            call()
        assert exc.value.response["Error"]["Code"] == "NoSuchDistribution"


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
    by_id = {s["CachePolicy"]["Id"]: s for s in listed["Items"]}
    assert by_id[pid]["Type"] == "custom"
    # ListCachePolicies with no Type filter also returns the AWS-managed
    # catalog (Type=managed) alongside custom policies.
    assert "managed" in {s["Type"] for s in listed["Items"]}

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
    by_id = {s["OriginRequestPolicy"]["Id"]: s for s in listed["Items"]}
    assert by_id[pid]["Type"] == "custom"
    # ListOriginRequestPolicies with no Type filter also returns the
    # AWS-managed catalog (Type=managed) alongside custom policies.
    assert "managed" in {s["Type"] for s in listed["Items"]}

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
    by_id = {s["ResponseHeadersPolicy"]["Id"]: s for s in listed["Items"]}
    assert by_id[pid]["Type"] == "custom"
    # ListResponseHeadersPolicies with no Type filter also returns the
    # AWS-managed catalog (Type=managed) alongside custom policies.
    assert "managed" in {s["Type"] for s in listed["Items"]}

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
# Public keys and key groups (signed URLs/cookies, OAC key groups).
# ---------------------------------------------------------------------------

_DUMMY_ENCODED_KEY = (
    "-----BEGIN PUBLIC KEY-----\n"
    "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAMdummykeydummykeydummykeydummy\n"
    "keydummykeydummykeydummykeydummykeydummykeydummykeydummyIDAQAB\n"
    "-----END PUBLIC KEY-----\n"
)


def _public_key_config(name):
    return {
        "CallerReference": f"cr-{_uuid_mod.uuid4().hex[:8]}",
        "Name": name,
        "EncodedKey": _DUMMY_ENCODED_KEY,
        "Comment": "test public key",
    }


def test_cloudfront_create_and_get_public_key(cloudfront):
    name = f"pk-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_public_key(PublicKeyConfig=_public_key_config(name))
    assert create["ETag"]
    pk = create["PublicKey"]
    pk_id = pk["Id"]
    assert pk_id
    assert "CreatedTime" in pk

    got = cloudfront.get_public_key(Id=pk_id)
    cfg = got["PublicKey"]["PublicKeyConfig"]
    assert got["ETag"] == create["ETag"]
    assert cfg["Name"] == name
    assert cfg["EncodedKey"] == _DUMMY_ENCODED_KEY

    cfg_only = cloudfront.get_public_key_config(Id=pk_id)
    assert cfg_only["ETag"] == create["ETag"]
    assert cfg_only["PublicKeyConfig"]["Name"] == name

    cloudfront.delete_public_key(Id=pk_id, IfMatch=got["ETag"])
    with pytest.raises(ClientError) as exc:
        cloudfront.get_public_key(Id=pk_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchPublicKey"


def test_cloudfront_update_public_key(cloudfront):
    name = f"pk-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_public_key(PublicKeyConfig=_public_key_config(name))
    pk_id = create["PublicKey"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.update_public_key(PublicKeyConfig=_public_key_config(name), Id=pk_id)
    assert exc.value.response["Error"]["Code"] == "InvalidIfMatchVersion"

    updated_cfg = dict(create["PublicKey"]["PublicKeyConfig"], Comment="updated comment")
    upd = cloudfront.update_public_key(PublicKeyConfig=updated_cfg, Id=pk_id, IfMatch=create["ETag"])
    assert upd["ETag"] != create["ETag"]
    assert upd["PublicKey"]["PublicKeyConfig"]["Comment"] == "updated comment"

    cloudfront.delete_public_key(Id=pk_id, IfMatch=upd["ETag"])


def test_cloudfront_list_public_keys_round_trip(cloudfront):
    baseline = cloudfront.list_public_keys()["PublicKeyList"]["Quantity"]

    name = f"pk-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_public_key(PublicKeyConfig=_public_key_config(name))
    pk_id = create["PublicKey"]["Id"]

    listed = cloudfront.list_public_keys()["PublicKeyList"]
    assert listed["Quantity"] == baseline + 1
    names = [s["Name"] for s in listed["Items"]]
    assert name in names

    cloudfront.delete_public_key(Id=pk_id, IfMatch=create["ETag"])


def test_cloudfront_create_and_get_key_group(cloudfront):
    pk_name = f"pk-{_uuid_mod.uuid4().hex[:8]}"
    pk = cloudfront.create_public_key(PublicKeyConfig=_public_key_config(pk_name))
    pk_id = pk["PublicKey"]["Id"]

    kg_name = f"kg-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_key_group(
        KeyGroupConfig={"Name": kg_name, "Items": [pk_id], "Comment": "test key group"}
    )
    assert create["ETag"]
    kg = create["KeyGroup"]
    kg_id = kg["Id"]
    assert kg_id
    assert "LastModifiedTime" in kg

    got = cloudfront.get_key_group(Id=kg_id)
    cfg = got["KeyGroup"]["KeyGroupConfig"]
    assert got["ETag"] == create["ETag"]
    assert cfg["Name"] == kg_name
    assert cfg["Items"] == [pk_id]

    cfg_only = cloudfront.get_key_group_config(Id=kg_id)
    assert cfg_only["KeyGroupConfig"]["Items"] == [pk_id]

    cloudfront.delete_key_group(Id=kg_id, IfMatch=got["ETag"])
    with pytest.raises(ClientError) as exc:
        cloudfront.get_key_group(Id=kg_id)
    assert exc.value.response["Error"]["Code"] == "NoSuchResource"

    cloudfront.delete_public_key(Id=pk_id, IfMatch=pk["ETag"])


def test_cloudfront_key_group_requires_existing_public_key(cloudfront):
    with pytest.raises(ClientError) as exc:
        cloudfront.create_key_group(KeyGroupConfig={"Name": f"kg-{_uuid_mod.uuid4().hex[:8]}", "Items": ["KDOESNOTEXIST"]})
    assert exc.value.response["Error"]["Code"] == "InvalidArgument"


def test_cloudfront_key_group_duplicate_name_rejected(cloudfront):
    pk = cloudfront.create_public_key(PublicKeyConfig=_public_key_config(f"pk-{_uuid_mod.uuid4().hex[:8]}"))
    pk_id = pk["PublicKey"]["Id"]
    kg_name = f"kg-{_uuid_mod.uuid4().hex[:8]}"
    create = cloudfront.create_key_group(KeyGroupConfig={"Name": kg_name, "Items": [pk_id]})
    kg_id = create["KeyGroup"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.create_key_group(KeyGroupConfig={"Name": kg_name, "Items": [pk_id]})
    assert exc.value.response["Error"]["Code"] == "KeyGroupAlreadyExists"

    cloudfront.delete_key_group(Id=kg_id, IfMatch=create["ETag"])
    cloudfront.delete_public_key(Id=pk_id, IfMatch=pk["ETag"])


def test_cloudfront_delete_public_key_in_use_rejected(cloudfront):
    pk = cloudfront.create_public_key(PublicKeyConfig=_public_key_config(f"pk-{_uuid_mod.uuid4().hex[:8]}"))
    pk_id = pk["PublicKey"]["Id"]
    kg = cloudfront.create_key_group(
        KeyGroupConfig={"Name": f"kg-{_uuid_mod.uuid4().hex[:8]}", "Items": [pk_id]}
    )
    kg_id = kg["KeyGroup"]["Id"]

    with pytest.raises(ClientError) as exc:
        cloudfront.delete_public_key(Id=pk_id, IfMatch=pk["ETag"])
    assert exc.value.response["Error"]["Code"] == "PublicKeyInUse"

    cloudfront.delete_key_group(Id=kg_id, IfMatch=kg["ETag"])
    cloudfront.delete_public_key(Id=pk_id, IfMatch=pk["ETag"])


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


def test_cloudfront_get_distribution_xml_has_no_namespace_prefixes(cloudfront):
    """Re-serialising the client's own namespaced config used to invent ns0:
    prefixes on every child, which SDK REST-XML parsers read as absent members.
    Raw HTTP because boto3's parser is namespace-tolerant and masked the bug."""
    dist_id = cloudfront.create_distribution(
        DistributionConfig=_CF_DIST_CONFIG)["Distribution"]["Id"]
    req = urllib.request.Request(
        f"{ENDPOINT}/2020-05-31/distribution/{dist_id}",
        headers={"Authorization": "AWS4-HMAC-SHA256 Credential=test/20200101/us-east-1/cloudfront/aws4_request, SignedHeaders=host, Signature=00"},
    )
    with urllib.request.urlopen(req) as resp:
        raw = resp.read()
    assert b"ns0:" not in raw, "namespace prefixes leaked into the response XML"
    assert b"<Origins>" in raw and b"<DefaultCacheBehavior>" in raw


# ---- Data plane: a distribution serving traffic ----
# Managed policy ids (CloudFront Developer Guide, "Managed origin request
# policies" / "Managed cache policies" / "Managed response headers
# policies").
_ALL_VIEWER = "216adef6-5c7f-47e4-b989-5492eafa07d3"
_ALL_VIEWER_EXCEPT_HOST = "b689b0a8-53d0-40ab-baf2-68738e2966ac"
_CACHING_DISABLED = "4135ea2d-6df8-44a3-9df3-4b5a84be39ad"
_MANAGED_CORS_WITH_PREFLIGHT = "5cc3b908-e619-4b99-88e5-2cf7f45965bd"

_ORIGIN_LAMBDA_CODE = (
    b"import json\n"
    b"def handler(event, context):\n"
    b"    path = event.get('rawPath', '')\n"
    b"    headers = event.get('headers', {})\n"
    b"    # MiniStack's apigatewayv2 (payload format 2.0) does not split an\n"
    b"    # incoming Cookie header into the top-level `cookies` array real AWS\n"
    b"    # uses; it stays in `headers['cookie']` like any other header, which\n"
    b"    # is also what cloudfront_dataplane.py actually forwards.\n"
    b"    cookie_header = headers.get('cookie', '')\n"
    b"    cookies = [c.strip() for c in cookie_header.split(';') if c.strip()]\n"
    b"    resp = {\n"
    b"        'statusCode': 200,\n"
    b"        'headers': {'Content-Type': 'application/json', 'X-Origin-Header': 'origin-value'},\n"
    b"        'body': json.dumps({\n"
    b"            'path': path,\n"
    b"            'headers': headers,\n"
    b"            'query': event.get('queryStringParameters') or {},\n"
    b"            'cookies': cookies,\n"
    b"        }),\n"
    b"    }\n"
    b"    if path == '/cookies':\n"
    b"        resp['cookies'] = ['resp1=a; Path=/', 'resp2=b; Path=/']\n"
    b"    if path == '/status/500':\n"
    b"        resp['statusCode'] = 500\n"
    b"    return resp\n"
)


def _zip_lambda(code: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


def _none_cache_policy_config(name: str) -> dict:
    return {
        "Name": name,
        "MinTTL": 0,
        "ParametersInCacheKeyAndForwardedToOrigin": {
            "HeadersConfig": {"HeaderBehavior": "none"},
            "CookiesConfig": {"CookieBehavior": "none"},
            "QueryStringsConfig": {"QueryStringBehavior": "none"},
            "EnableAcceptEncodingGzip": False,
        },
    }


def _response_headers_policy(cloudfront, name: str, custom_headers: list) -> str:
    resp = cloudfront.create_response_headers_policy(
        ResponseHeadersPolicyConfig={
            "Name": name,
            "CustomHeadersConfig": {
                "Quantity": len(custom_headers),
                "Items": [{"Header": h, "Value": v, "Override": o} for (h, v, o) in custom_headers],
            },
        }
    )
    return resp["ResponseHeadersPolicy"]["Id"]


def _create_function(cloudfront, name: str, code: bytes, runtime: str = "cloudfront-js-2.0") -> str:
    created = cloudfront.create_function(
        Name=name, FunctionConfig={"Comment": name, "Runtime": runtime}, FunctionCode=code,
    )
    published = cloudfront.publish_function(Name=name, IfMatch=created["ETag"])
    return published["FunctionSummary"]["FunctionMetadata"]["FunctionARN"]


@pytest.fixture(scope="module")
def cf_backend(apigw, lam):
    """A custom API Gateway HTTP API, backed by a Lambda that echoes the
    inbound request, addressed like a real CloudFront custom origin."""
    suffix = _uuid_mod.uuid4().hex[:10]
    lam.create_function(
        FunctionName=f"cf-dp-origin-{suffix}",
        Runtime="python3.12",
        Role="arn:aws:iam::000000000000:role/test-role",
        Handler="index.handler",
        Code={"ZipFile": _zip_lambda(_ORIGIN_LAMBDA_CODE)},
    )
    api_id = apigw.create_api(Name=f"cf-dp-api-{suffix}", ProtocolType="HTTP")["ApiId"]
    int_id = apigw.create_integration(
        ApiId=api_id, IntegrationType="AWS_PROXY",
        IntegrationUri=f"arn:aws:lambda:us-east-1:000000000000:function:cf-dp-origin-{suffix}",
        PayloadFormatVersion="2.0",
    )["IntegrationId"]
    apigw.create_route(ApiId=api_id, RouteKey="ANY /{proxy+}", Target=f"integrations/{int_id}")
    apigw.create_stage(ApiId=api_id, StageName="$default")
    # "*.execute-api.localhost" resolves to 127.0.0.1 (RFC 6761); the origin
    # is dialed there at the gateway's own listening port, exactly as any
    # custom origin is dialed at its configured DomainName/HTTPPort.
    return {"origin_domain": f"{api_id}.execute-api.localhost"}


@pytest.fixture(scope="module")
def cf_stack(cloudfront, cf_backend):
    """One distribution wired up per the module docstring."""
    suffix = _uuid_mod.uuid4().hex[:10]
    origin_domain = cf_backend["origin_domain"]

    cp_none_id = cloudfront.create_cache_policy(
        CachePolicyConfig=_none_cache_policy_config(f"cf-dp-cp-none-{suffix}")
    )["CachePolicy"]["Id"]

    default_policy_id = _response_headers_policy(
        cloudfront, f"cf-dp-default-policy-{suffix}",
        [("X-Origin-Header", "policy-value", True), ("X-Default-Marker", "yes", False)],
    )
    csv_policy_id = _response_headers_policy(
        cloudfront, f"cf-dp-csv-policy-{suffix}", [("X-Csv-Marker", "yes", False)],
    )

    request_fn_arn = _create_function(
        cloudfront, f"cf-dp-viewer-request-{suffix}",
        b"function handler(event) {\n"
        b"  var request = event.request;\n"
        b"  if (request.uri === '/fn/blocked') {\n"
        b"    return { statusCode: 403, statusDescription: 'Forbidden',\n"
        b"             headers: { 'content-type': { value: 'text/plain' } },\n"
        b"             body: { encoding: 'text', data: 'blocked by viewer-request' } };\n"
        b"  }\n"
        b"  request.uri = request.uri.replace('/fn/', '/');\n"
        b"  return request;\n"
        b"}\n",
    )
    response_fn_arn = _create_function(
        cloudfront, f"cf-dp-viewer-response-{suffix}",
        b"function handler(event) {\n"
        b"  var response = event.response;\n"
        b"  response.headers['x-viewer-response-added'] = { value: 'yes' };\n"
        b"  return response;\n"
        b"}\n",
    )
    async_fn_arn = _create_function(
        cloudfront, f"cf-dp-async-{suffix}",
        b"async function handler(event) {\n"
        b"  var v = await Promise.resolve('yes');\n"
        b"  var request = event.request;\n"
        b"  request.headers['x-async-ran'] = { value: v };\n"
        b"  request.uri = request.uri.replace('/async/', '/');\n"
        b"  return request;\n"
        b"}\n",
    )
    error_fn_arn = _create_function(
        cloudfront, f"cf-dp-error-{suffix}",
        b"function handler(event) { throw new Error('boom'); }\n",
    )
    response_delete_multi_fn_arn = _create_function(
        cloudfront, f"cf-dp-response-delete-multi-{suffix}",
        b"function handler(event) {\n"
        b"  var response = event.response;\n"
        b"  delete response.headers['x-origin-header'];\n"
        b"  response.headers['x-multi-resp'] = { value: 'a', multiValue: [{ value: 'a' }, { value: 'b' }] };\n"
        b"  return response;\n"
        b"}\n",
    )
    viewer_ip_fn_arn = _create_function(
        cloudfront, f"cf-dp-viewer-ip-{suffix}",
        b"function handler(event) {\n"
        b"  var request = event.request;\n"
        b"  request.headers['x-viewer-ip'] = { value: event.viewer.ip };\n"
        b"  return request;\n"
        b"}\n",
    )

    def behavior(path_pattern, target_origin_id="origin-apigw", **overrides):
        b = {
            "PathPattern": path_pattern,
            "TargetOriginId": target_origin_id,
            "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": cp_none_id,
            "OriginRequestPolicyId": _ALL_VIEWER_EXCEPT_HOST,
            "ResponseHeadersPolicyId": default_policy_id,
        }
        b.update(overrides)
        return b

    dist_config = {
        "CallerReference": f"cf-dp-{suffix}",
        "Comment": "cloudfront_dataplane test distribution",
        "Enabled": True,
        "DefaultRootObject": "index.html",
        "Origins": {
            "Quantity": 2,
            "Items": [
                {
                    "Id": "origin-apigw",
                    "DomainName": origin_domain,
                    "CustomOriginConfig": {
                        "HTTPPort": int(GATEWAY_PORT), "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                        "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                        "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                    },
                },
                {
                    "Id": "origin-apigw-prefixed",
                    "DomainName": origin_domain,
                    "OriginPath": "/extra",
                    "CustomHeaders": {
                        "Quantity": 1,
                        "Items": [{"HeaderName": "X-Added-By-Origin", "HeaderValue": "yes"}],
                    },
                    "CustomOriginConfig": {
                        "HTTPPort": int(GATEWAY_PORT), "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                        "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                        "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                    },
                },
            ],
        },
        "DefaultCacheBehavior": {
            k: v for k, v in behavior("*").items() if k != "PathPattern"
        },
        "CacheBehaviors": {
            "Quantity": 8,
            "Items": [
                {**behavior("*.csv"), "ResponseHeadersPolicyId": csv_policy_id},
                {
                    **behavior("/fn/*"),
                    "FunctionAssociations": {
                        "Quantity": 2,
                        "Items": [
                            {"FunctionARN": request_fn_arn, "EventType": "viewer-request"},
                            {"FunctionARN": response_fn_arn, "EventType": "viewer-response"},
                        ],
                    },
                },
                {
                    **behavior("/async/*"),
                    "FunctionAssociations": {
                        "Quantity": 1,
                        "Items": [{"FunctionARN": async_fn_arn, "EventType": "viewer-request"}],
                    },
                },
                {
                    **behavior("/err/*"),
                    "FunctionAssociations": {
                        "Quantity": 1,
                        "Items": [{"FunctionARN": error_fn_arn, "EventType": "viewer-request"}],
                    },
                },
                {**behavior("/cd/*"), "CachePolicyId": _CACHING_DISABLED, "OriginRequestPolicyId": ""},
                {
                    **{k: v for k, v in behavior("/legacy/*").items()
                       if k not in ("CachePolicyId", "OriginRequestPolicyId")},
                    "MinTTL": 0, "DefaultTTL": 0, "MaxTTL": 0,
                    "ForwardedValues": {
                        "QueryString": True,
                        "Cookies": {"Forward": "whitelist", "WhitelistedNames": {"Quantity": 1, "Items": ["keep"]}},
                        "Headers": {"Quantity": 1, "Items": ["X-Legacy"]},
                    },
                },
                {**behavior("/restricted/*"),
                 "AllowedMethods": {"Quantity": 2, "Items": ["GET", "HEAD"]}},
                # No leading "/" — CloudFront treats it the same as "/api/*".
                behavior("api/*"),
                {
                    **behavior("/cors/*"), "ResponseHeadersPolicyId": _MANAGED_CORS_WITH_PREFLIGHT,
                    "AllowedMethods": {"Quantity": 3, "Items": ["GET", "HEAD", "OPTIONS"]},
                },
                {
                    **behavior("/respmulti/*"),
                    "FunctionAssociations": {
                        "Quantity": 1,
                        "Items": [{"FunctionARN": response_delete_multi_fn_arn, "EventType": "viewer-response"}],
                    },
                },
                {
                    **behavior("/viewerip/*"),
                    "FunctionAssociations": {
                        "Quantity": 1,
                        "Items": [{"FunctionARN": viewer_ip_fn_arn, "EventType": "viewer-request"}],
                    },
                },
            ],
        },
    }
    # "/cd/*" carries no OriginRequestPolicyId at all; the empty-string
    # placeholder above exists only so `behavior()`'s dict-merge has a key to
    # drop — CloudFront's own XML omits the element entirely for "no ORP".
    for item in dist_config["CacheBehaviors"]["Items"]:
        if item.get("OriginRequestPolicyId") == "":
            del item["OriginRequestPolicyId"]
    dist_config["CacheBehaviors"]["Items"].append(
        {**behavior("/redirect/*"), "ViewerProtocolPolicy": "redirect-to-https"}
    )
    dist_config["CacheBehaviors"]["Items"].append(
        {**behavior("/prefixed/*", target_origin_id="origin-apigw-prefixed")}
    )
    dist_config["CacheBehaviors"]["Quantity"] = len(dist_config["CacheBehaviors"]["Items"])

    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    return {"dist_host": dist["DomainName"], "origin_domain": origin_domain}


def _get(host: str, path: str, method: str = "GET", extra_headers: dict | None = None):
    """``host`` is a distribution's real AWS-shaped DomainName
    (``<label>.cloudfront.net``), sent verbatim as the Host header — exactly
    what a real viewer sends. The connection itself targets the gateway's own
    address directly, since ``*.cloudfront.net`` doesn't resolve locally (the
    same way ``_raw_get`` below reaches an execute-api/ALB host)."""
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        headers = {"Host": f"{host}:{GATEWAY_PORT}"}
        headers.update(extra_headers or {})
        conn.request(method, path, headers=headers)
        resp = conn.getresponse()
        return resp.status, dict(resp.getheaders()), resp.read()
    finally:
        conn.close()


def _skip_without_node():
    if not cloudfront_svc._cf_functions_available():
        pytest.skip("node is not available")


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def test_routes_by_distribution_domain(cf_stack):
    status, _headers, body = _get(cf_stack["dist_host"], "/hello")
    assert status == 200
    assert json.loads(body)["path"] == "/hello"


def test_path_pattern_without_leading_slash_matches_as_if_it_had_one(cf_stack):
    # PathPattern "api/*" (no leading /) — CloudFront Developer Guide,
    # "Path pattern": "CloudFront treats the path the same with or without
    # the leading /".
    status, _headers, body = _get(cf_stack["dist_host"], "/api/hello")
    assert status == 200
    assert json.loads(body)["path"] == "/api/hello"


def test_unknown_distribution_label_is_not_served_by_cloudfront():
    # Not a 200, and not CloudFront's own data plane (no X-Cache): falls
    # through to whatever the gateway's generic router answers for an
    # otherwise-unrouted host — CloudFront's own control-plane 404, since no
    # other special-case host pattern matches either.
    status, headers, body = _get("d0000000000000.cloudfront.localhost", "/anything")
    assert "X-Cache" not in headers
    assert status == 404
    assert b"<Code>NoSuchResource</Code>" in body


# ---------------------------------------------------------------------------
# CloudFront Functions
# ---------------------------------------------------------------------------


def test_viewer_request_function_rewrites_uri(cf_stack):
    _skip_without_node()
    status, _headers, body = _get(cf_stack["dist_host"], "/fn/hello")
    assert status == 200
    assert json.loads(body)["path"] == "/hello"


def test_function_generated_response_skips_the_origin(cf_stack):
    _skip_without_node()
    status, headers, body = _get(cf_stack["dist_host"], "/fn/blocked")
    assert status == 403
    assert body == b"blocked by viewer-request"
    assert headers.get("X-Cache") == "FunctionGeneratedResponse from cloudfront"
    assert "X-Origin-Header" not in headers  # never reached the origin


def test_viewer_response_function_adds_a_header(cf_stack):
    _skip_without_node()
    status, headers, _body = _get(cf_stack["dist_host"], "/fn/hello")
    assert status == 200
    assert headers.get("X-Viewer-Response-Added") == "yes"


def test_async_2_0_handler_runs_and_awaits(cf_stack):
    _skip_without_node()
    status, _headers, body = _get(cf_stack["dist_host"], "/async/hello")
    assert status == 200
    payload = json.loads(body)
    assert payload["path"] == "/hello"
    assert payload["headers"].get("x-async-ran") == "yes"


def test_function_error_answers_503(cf_stack):
    _skip_without_node()
    status, headers, _body = _get(cf_stack["dist_host"], "/err/hello")
    assert status == 503
    assert headers.get("X-Cache") == "Error from cloudfront"


def test_viewer_response_function_skipped_when_origin_status_is_400_or_above(cf_stack):
    # functions-event-structure.html, "Status code and body" note: CloudFront
    # does not invoke a viewer-response function when the origin answers 400+.
    _skip_without_node()
    status, headers, _body = _get(cf_stack["dist_host"], "/fn/status/500")
    assert status == 500
    assert "X-Viewer-Response-Added" not in headers


def test_viewer_response_function_deletes_header_and_expands_multivalue(cf_stack):
    # Replace, not merge (a deleted header stays gone), and a multiValue
    # response header becomes repeated Set-Cookie-style lines.
    _skip_without_node()
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", "/respmulti/hello", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_stack['dist_host']}:{GATEWAY_PORT}")
        conn.endheaders()
        resp = conn.getresponse()
        resp.read()
        multi_values = resp.msg.get_all("X-Multi-Resp") or []
        has_origin_header = "X-Origin-Header" in resp.msg
    finally:
        conn.close()
    assert sorted(multi_values) == ["a", "b"]
    assert not has_origin_header


def test_function_event_viewer_ip_is_the_tcp_peer_not_the_xff_header(cf_stack):
    # functions-event-structure.html: viewer.ip is the TCP peer, regardless
    # of what X-Forwarded-For the viewer sent.
    _skip_without_node()
    status, _headers, body = _get(
        cf_stack["dist_host"], "/viewerip/hello", extra_headers={"X-Forwarded-For": "203.0.113.9"},
    )
    assert status == 200
    assert json.loads(body)["headers"].get("x-viewer-ip") == "127.0.0.1"


# ---------------------------------------------------------------------------
# Cache behaviors and response-headers policy
# ---------------------------------------------------------------------------


def test_ordered_behavior_matches_csv_glob_with_its_own_policy(cf_stack):
    status, headers, _body = _get(cf_stack["dist_host"], "/report.csv")
    assert status == 200
    assert headers.get("X-Csv-Marker") == "yes"
    assert "X-Default-Marker" not in headers


def test_response_headers_policy_overrides_origin_header_and_adds_marker(cf_stack):
    # The origin always emits X-Origin-Header: origin-value; the default
    # behavior's response-headers policy names the same header with
    # Override=True, and adds X-Default-Marker unconditionally.
    status, headers, _body = _get(cf_stack["dist_host"], "/hello")
    assert status == 200
    assert headers.get("X-Origin-Header") == "policy-value"
    assert headers.get("X-Default-Marker") == "yes"


def test_cors_headers_only_applied_when_origin_header_present(cf_stack):
    status, headers, _body = _get(cf_stack["dist_host"], "/cors/hello")
    assert status == 200
    assert "Access-Control-Allow-Origin" not in headers

    status, headers, _body = _get(
        cf_stack["dist_host"], "/cors/hello", extra_headers={"Origin": "https://example.com"},
    )
    assert status == 200
    assert headers.get("Access-Control-Allow-Origin") == "*"
    assert headers.get("Access-Control-Expose-Headers") == "*"
    # Preflight-only headers (Allow-Methods/-Headers/-Max-Age) never appear
    # on a plain GET.
    assert "Access-Control-Allow-Methods" not in headers


def test_cors_preflight_headers_only_on_options(cf_stack):
    # The response-headers policy applies to whatever status the request
    # produces (here, the backing API Gateway HTTP API's own unconfigured-
    # CORS OPTIONS rejection — orthogonal to the response-headers policy
    # under test), so only the header is asserted, not the status.
    _status, headers, _body = _get(
        cf_stack["dist_host"], "/cors/hello", method="OPTIONS",
        extra_headers={"Origin": "https://example.com"},
    )
    assert headers.get("Access-Control-Allow-Methods") == "DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT"


def test_cloudfront_added_headers_present(cf_stack):
    status, headers, _body = _get(cf_stack["dist_host"], "/hello")
    assert status == 200
    assert headers.get("X-Cache") == "Miss from cloudfront"
    assert headers.get("Via", "").endswith(".cloudfront.net (CloudFront)")
    assert "X-Amz-Cf-Id" in headers
    assert "X-Amz-Cf-Pop" in headers


# ---------------------------------------------------------------------------
# Cache-policy / origin-request-policy forwarding
# ---------------------------------------------------------------------------


def test_all_viewer_except_host_header_forwards_viewer_header_not_host(cf_stack):
    status, _headers, body = _get(
        cf_stack["dist_host"], "/hello", extra_headers={"X-Custom-Test": "present"},
    )
    assert status == 200
    payload = json.loads(body)
    assert payload["headers"].get("x-custom-test") == "present"
    assert payload["headers"].get("host") == cf_stack["origin_domain"]


def test_all_viewer_forwards_the_distributions_own_host(cf_raw_origin_stack):
    # AllViewer forwards every viewer header including Host, so the origin
    # sees the *distribution's* Host, not a default derived from its own
    # DomainName. A real network-distinct origin doesn't care; this uses the
    # raw http.server origin (not the execute-api one) because MiniStack's
    # own execute-api addressing is itself Host-routed, so forwarding the
    # viewer's Host there would recurse into the gateway's own routing
    # instead of reaching the intended origin — an artifact of this
    # emulator's single-port addressing, not of CloudFront's own behavior.
    status, _headers, body = _get(
        cf_raw_origin_stack, "/av/hello", extra_headers={"X-Custom-Test": "present"},
    )
    assert status == 200
    payload = json.loads(body)
    assert payload["headers"].get("x-custom-test") == "present"
    assert payload["headers"].get("host") == f"{cf_raw_origin_stack}:{GATEWAY_PORT}"


def test_caching_disabled_with_no_origin_request_policy_forwards_only_defaults(cf_stack):
    # CachingDisabled's ParametersInCacheKeyAndForwardedToOrigin is
    # none/none/none, and the behavior carries no origin request policy at
    # all: nothing beyond the always-included defaults (Host, User-Agent)
    # reaches the origin.
    status, _headers, body = _get(
        cf_stack["dist_host"], "/cd/hello", extra_headers={"X-Custom-Test": "present"},
    )
    assert status == 200
    payload = json.loads(body)
    assert "x-custom-test" not in payload["headers"]
    assert payload["headers"].get("host") == cf_stack["origin_domain"]


def test_x_forwarded_for_always_appends_regardless_of_policy(cf_stack):
    # CloudFront Developer Guide, "Client IP addresses": X-Forwarded-For is
    # always the viewer's own chain plus the client IP, even under
    # CachingDisabled with no origin request policy forwarding it.
    status, _headers, body = _get(
        cf_stack["dist_host"], "/cd/hello", extra_headers={"X-Forwarded-For": "203.0.113.9"},
    )
    assert status == 200
    payload = json.loads(body)
    assert payload["headers"].get("x-forwarded-for") == "203.0.113.9, 127.0.0.1"


def test_user_agent_defaults_to_amazon_cloudfront_when_not_forwarded(cf_stack):
    # CloudFront Developer Guide, "User-Agent header": replaced unconditionally
    # unless the forwarding policy carries the viewer's own.
    status, _headers, body = _get(
        cf_stack["dist_host"], "/cd/hello", extra_headers={"User-Agent": "custom-client/1.0"},
    )
    assert status == 200
    assert json.loads(body)["headers"].get("user-agent") == "Amazon CloudFront"


def test_legacy_forwarded_values_forwards_only_its_own_selections(cf_stack):
    status, _headers, body = _get(
        cf_stack["dist_host"], "/legacy/hello?a=1&b=2",
        extra_headers={"X-Legacy": "yes", "X-Not-Forwarded": "no", "Cookie": "keep=1; drop=2"},
    )
    assert status == 200
    payload = json.loads(body)
    assert payload["query"] == {"a": "1", "b": "2"}  # ForwardedValues.QueryString=true
    assert payload["headers"].get("x-legacy") == "yes"
    assert "x-not-forwarded" not in payload["headers"]
    assert payload["cookies"] == ["keep=1"]  # only the whitelisted cookie name


# ---------------------------------------------------------------------------
# _combine_forward — the cache-policy / origin-request-policy combinator
# (same function serves headers, cookies and query strings). Exercised
# end-to-end above for "all"/"none"; "whitelist" and "allExcept" are cheaper
# to prove directly against the pure combinator than via more distributions.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cache_kind_set,orp_kind_set,name,expected",
    [
        (("whitelist", {"a"}), None, "a", True),
        (("whitelist", {"a"}), None, "b", False),
        (("whitelist", {"a"}), ("whitelist", {"b"}), "b", True),  # ORP allow-list adds to cache's
        (("allExcept", {"a"}), None, "a", False),  # blocked by the cache policy
        (("allExcept", {"a"}), None, "b", True),
        (("allExcept", {"a"}), ("allExcept", {"b"}), "a", False),  # ORP allExcept doesn't rescue it
        (("allExcept", {"a"}), ("whitelist", {"a"}), "a", True),  # ORP allow-list rescues a blocked name
        (("none", None), ("allExcept", {"a"}), "a", False),
        (("none", None), ("allExcept", {"a"}), "b", True),
        (("none", None), None, "a", False),
    ],
)
def test_combine_forward_whitelist_and_allexcept_branches(cache_kind_set, orp_kind_set, name, expected):
    assert cloudfront_svc._combine_forward(cache_kind_set, orp_kind_set, name) == expected


# ---------------------------------------------------------------------------
# Behavior-level settings
# ---------------------------------------------------------------------------


def test_redirect_to_https_sends_301_with_https_location(cf_stack):
    status, headers, _body = _get(cf_stack["dist_host"], "/redirect/hello")
    assert status == 301
    assert headers.get("Location") == f"https://{cf_stack['dist_host']}:{GATEWAY_PORT}/redirect/hello"


def test_allowed_methods_rejects_a_disallowed_method(cf_stack):
    status, _headers, _body = _get(cf_stack["dist_host"], "/restricted/hello", method="POST")
    assert status == 403


def test_default_root_object_is_requested_for_the_root_url(cf_stack):
    status, _headers, body = _get(cf_stack["dist_host"], "/")
    assert status == 200
    assert json.loads(body)["path"] == "/index.html"


def test_origin_path_and_custom_headers_reach_the_origin(cf_stack):
    status, _headers, body = _get(cf_stack["dist_host"], "/prefixed/hello")
    assert status == 200
    payload = json.loads(body)
    assert payload["path"] == "/extra/prefixed/hello"
    assert payload["headers"].get("x-added-by-origin") == "yes"


def test_multiple_set_cookie_headers_all_carried_through(cf_stack):
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", "/cookies", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_stack['dist_host']}:{GATEWAY_PORT}")
        conn.endheaders()
        resp = conn.getresponse()
        resp.read()
        cookies = resp.msg.get_all("Set-Cookie") or []
    finally:
        conn.close()
    assert sorted(cookies) == ["resp1=a; Path=/", "resp2=b; Path=/"]


# ---------------------------------------------------------------------------
# AWS-owned origin hosts — refused before a socket opens; no server or
# distribution needed, calls the pure function. An emulator must never open
# a socket to a real AWS host.
# ---------------------------------------------------------------------------


def test_aws_owned_origin_host_refused_before_any_socket():
    origin = {
        "domain_name": "abcd1234.execute-api.us-east-1.amazonaws.com.",  # trailing dot
        "origin_path": "", "custom_headers": [], "s3_bucket": None,
        "http_port": 80, "https_port": 443, "protocol_policy": "https-only",
    }
    with mock.patch("http.client.HTTPSConnection") as https_conn, \
            mock.patch("http.client.HTTPConnection") as http_conn:
        with pytest.raises(cloudfront_svc.OriginUnreachable):
            cloudfront_svc._forward_to_origin(
                origin, "GET", "/anything", "", {}, b"", viewer_is_https=True,
            )
    https_conn.assert_not_called()
    http_conn.assert_not_called()


def test_origin_ssl_context_trusts_gateway_cert_under_use_ssl(tmp_path):
    # Precedent: lambda_svc.py's TLS trust for Lambda containers — system
    # roots plus the gateway's own cert, not a replacement of the store.
    from ministack.core.x509_utils import generate_ca

    ca_pem, _ca_key_pem = generate_ca(common_name="cf-dp-origin-ssl-test")
    cert_path = tmp_path / "ca.pem"
    cert_path.write_text(ca_pem)
    with mock.patch("ministack.core.tls.use_ssl_enabled", return_value=True), \
            mock.patch("ministack.core.tls.resolve_tls_material", return_value=(str(cert_path), "key")):
        ctx = cloudfront_svc._origin_ssl_context()
    assert ctx.verify_flags & ssl.VERIFY_X509_PARTIAL_CHAIN
    subjects = [dict(rdn for pair in cert["subject"] for rdn in pair) for cert in ctx.get_ca_certs()]
    assert any(s.get("commonName") == "cf-dp-origin-ssl-test" for s in subjects)


def test_stacked_distribution_via_header_answers_403(cloudfront):
    # CloudFront Developer Guide, http-403-permission-denied.html, "Stacked
    # distributions cause a 403 error": an origin pointed back at this same
    # distribution (via the resolvable `<label>.cloudfront.localhost` form)
    # must answer 403 on the second hop rather than recurse into a hang.
    suffix = _uuid_mod.uuid4().hex[:10]
    dist_config = {
        "CallerReference": f"cf-dp-stacked-{suffix}",
        "Comment": "cloudfront_dataplane stacked-distribution test",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "self-origin",
                "DomainName": "placeholder.cloudfront.localhost",
                "CustomOriginConfig": {
                    "HTTPPort": int(GATEWAY_PORT), "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "self-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
    }
    created = cloudfront.create_distribution(DistributionConfig=dist_config)
    dist = created["Distribution"]
    label = dist["DomainName"].removesuffix(".cloudfront.net")
    dist_config["Origins"]["Items"][0]["DomainName"] = f"{label}.cloudfront.localhost"
    cloudfront.update_distribution(DistributionConfig=dist_config, Id=dist["Id"], IfMatch=created["ETag"])

    status, _headers, _body = _get(dist["DomainName"], "/anything")
    assert status == 403


def test_unreachable_origin_answers_502(cloudfront):
    suffix = _uuid_mod.uuid4().hex[:10]
    dist_config = {
        "CallerReference": f"cf-dp-dead-{suffix}",
        "Comment": "cloudfront_dataplane dead-origin test",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "dead-origin",
                "DomainName": "127.0.0.1",
                "CustomOriginConfig": {
                    "HTTPPort": 1, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",  # nothing listens on port 1
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "dead-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
    }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    status, headers, _body = _get(dist["DomainName"], "/anything")
    assert status == 502
    assert headers.get("X-Cache") == "Error from cloudfront"


def test_forward_to_origin_timeout_raises_origin_timeout():
    # CloudFront Developer Guide, http-504-gateway-timeout.html: "The origin
    # didn't respond before the request expired." OriginReadTimeout default
    # 30s (DownloadDistValuesOrigin.md, "Response timeout").
    origin = {
        "domain_name": "127.0.0.1", "origin_path": "", "custom_headers": [], "s3_bucket": None,
        "http_port": 1, "https_port": 443, "protocol_policy": "http-only", "read_timeout": 30,
    }
    with mock.patch("http.client.HTTPConnection") as mock_conn_cls:
        mock_conn_cls.return_value.request.side_effect = TimeoutError("timed out")
        with pytest.raises(cloudfront_svc.OriginTimeout):
            cloudfront_svc._forward_to_origin(
                origin, "GET", "/anything", "", {}, b"", viewer_is_https=False,
            )


def test_handle_request_maps_origin_timeout_to_504():
    parsed = {
        "default_root_object": "",
        "origins": {"o1": {
            "domain_name": "127.0.0.1", "origin_path": "", "custom_headers": [],
            "s3_bucket": None, "http_port": 1, "https_port": 443,
            "protocol_policy": "http-only", "read_timeout": 30,
        }},
        "default_behavior": {
            "path_pattern": None, "target_origin_id": "o1", "viewer_protocol_policy": "allow-all",
            "allowed_methods": None, "cache_policy_id": _CACHING_DISABLED,
            "origin_request_policy_id": None, "response_headers_policy_id": None,
            "forwarded_values": None, "functions": {},
        },
        "ordered_behaviors": [],
    }
    dist = {"Id": "EDUMMY00000000", "DomainName": "ddummy0000000.cloudfront.net"}
    with mock.patch("ministack.services.cloudfront.parse_distribution_dataplane_config", return_value=parsed), \
            mock.patch("ministack.services.cloudfront._forward_to_origin",
                       side_effect=cloudfront_svc.OriginTimeout("slow")):
        status, headers, _body = asyncio.run(
            cloudfront_svc.handle_viewer_request(dist, "GET", "/anything", "/anything", "", {}, b"", {})
        )
    assert status == 504
    assert headers.get("X-Cache") == "Error from cloudfront"


# ---------------------------------------------------------------------------
# S3 origin — served from MiniStack's own S3 store in-process.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def cf_s3_stack(cloudfront, s3):
    suffix = _uuid_mod.uuid4().hex[:10]
    bucket = f"cf-dp-s3-origin-{suffix}"
    s3.create_bucket(Bucket=bucket)
    body = b"hello from the bucket\n"
    s3.put_object(Bucket=bucket, Key="hello.txt", Body=body, ContentType="text/plain")

    dist_config = {
        "CallerReference": f"cf-dp-s3-{suffix}",
        "Comment": "cloudfront_dataplane S3-origin test distribution",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "s3-origin",
                "DomainName": f"{bucket}.s3.amazonaws.com",
                "S3OriginConfig": {"OriginAccessIdentity": ""},
                "OriginAccessControlId": "test-oac-id",
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "s3-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
    }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    return {"dist_host": dist["DomainName"], "body": body}


def test_s3_origin_serves_bucket_object(cf_s3_stack):
    status, headers, body = _get(cf_s3_stack["dist_host"], "/hello.txt")
    assert status == 200
    assert body == cf_s3_stack["body"]
    assert headers.get("Content-Type") == "text/plain"


def test_s3_origin_range_get_returns_partial_content(cf_s3_stack):
    # Range is forwarded regardless of cache/origin-request-policy header
    # configuration (CloudFront Developer Guide, "HTTP request headers and
    # CloudFront behavior"), even under CachingDisabled with no ORP here.
    status, headers, body = _get(
        cf_s3_stack["dist_host"], "/hello.txt", extra_headers={"Range": "bytes=0-4"},
    )
    assert status == 206
    assert body == cf_s3_stack["body"][:5]
    assert headers.get("Content-Range", "").startswith("bytes 0-4/")


def test_s3_origin_missing_key_returns_access_denied(cf_s3_stack):
    # The OAC bucket policy CloudFront's console generates grants only
    # s3:GetObject, never s3:ListBucket — without ListBucket, S3 answers a
    # missing key with 403 AccessDenied rather than 404 NoSuchKey.
    status, _headers, body = _get(cf_s3_stack["dist_host"], "/does-not-exist.txt")
    assert status == 403
    assert b"<Code>AccessDenied</Code>" in body


# ---------------------------------------------------------------------------
# Percent-encoding fidelity (CloudFront Developer Guide, "Restrictions on all
# edge functions" > "URI, query string, and headers encoding").
# ---------------------------------------------------------------------------


class _PathEchoHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({
            "path": self.path,
            "headers": {k.lower(): v for k, v in self.headers.items()},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        if self.path.endswith("/hopbyhop"):
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Should-Survive", "yes")
        if self.path.endswith("/respdup"):
            self.send_header("X-Resp-Dup", "first")
            self.send_header("X-Resp-Dup", "second")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        posted_body = self.rfile.read(length) if length else b""
        body = json.dumps({
            "path": self.path,
            "body": posted_body.decode("utf-8", errors="replace"),
            "content_length_header": self.headers.get("Content-Length"),
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


@pytest.fixture(scope="module")
def path_echo_origin_port():
    server = http.server.HTTPServer(("127.0.0.1", 0), _PathEchoHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


@pytest.fixture(scope="module")
def cf_raw_origin_stack(cloudfront, path_echo_origin_port):
    suffix = _uuid_mod.uuid4().hex[:10]
    dist_config = {
        "CallerReference": f"cf-dp-raw-{suffix}",
        "Comment": "cloudfront_dataplane raw-path-fidelity test distribution",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "raw-origin",
                "DomainName": "127.0.0.1",
                "CustomOriginConfig": {
                    "HTTPPort": path_echo_origin_port, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "raw-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
        "CacheBehaviors": {
            "Quantity": 1,
            "Items": [{
                "PathPattern": "/av/*",
                "TargetOriginId": "raw-origin", "ViewerProtocolPolicy": "allow-all",
                "CachePolicyId": _CACHING_DISABLED, "OriginRequestPolicyId": _ALL_VIEWER,
            }],
        },
    }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    return dist["DomainName"]


def test_origin_domain_name_port_overrides_http_port(cloudfront, path_echo_origin_port):
    """An origin built from a MiniStack endpoint names the gateway port in its DomainName."""
    suffix = _uuid_mod.uuid4().hex[:10]
    dist = cloudfront.create_distribution(DistributionConfig={
        "CallerReference": f"cf-dp-port-{suffix}",
        "Comment": "origin DomainName with an explicit port",
        "Enabled": True,
        "Origins": {"Quantity": 1, "Items": [{
            "Id": "port-origin",
            "DomainName": f"127.0.0.1:{path_echo_origin_port}",
            "CustomOriginConfig": {
                "HTTPPort": 1, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
            },
        }]},
        "DefaultCacheBehavior": {
            "TargetOriginId": "port-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
    })["Distribution"]
    status, _body = _raw_get(dist["DomainName"], "/port-check")
    assert status == 200

def _raw_get(dist_host: str, raw_path: str):
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", raw_path, skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{dist_host}:{GATEWAY_PORT}")
        conn.endheaders()
        resp = conn.getresponse()
        return resp.status, resp.read()
    finally:
        conn.close()


def test_percent_encoded_path_forwarded_byte_exact(cf_raw_origin_stack):
    for raw_path in (
        "/echo/%21%24%26%27%28%29%2A%2B%2C%3B%3D%40",
        "/ready_pin_guests/123%2Ejson",
    ):
        status, body = _raw_get(cf_raw_origin_stack, raw_path)
        assert status == 200
        assert json.loads(body)["path"] == raw_path


def test_query_string_forwarded_byte_exact_when_unmodified_and_fully_allowed(cf_raw_origin_stack):
    # "/av/*" carries AllViewer (forwards every query-string parameter) and
    # no function: nothing should re-encode the viewer's own percent-encoding.
    raw_path = "/av/hello?a=%68%65%6c%6c%6f&b=1"
    status, body = _raw_get(cf_raw_origin_stack, raw_path)
    assert status == 200
    assert json.loads(body)["path"] == raw_path


def test_post_body_round_trips_with_content_length(cf_raw_origin_stack):
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        payload = b'{"hello": "world"}'
        conn.putrequest("POST", "/av/echo", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_raw_origin_stack}:{GATEWAY_PORT}")
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Content-Length", str(len(payload)))
        conn.endheaders(payload)
        resp = conn.getresponse()
        body = resp.read()
    finally:
        conn.close()
    echoed = json.loads(body)
    assert echoed["body"] == payload.decode()
    assert echoed["content_length_header"] == str(len(payload))


def test_hop_by_hop_response_headers_stripped(cf_raw_origin_stack):
    status, headers, _body = _get(cf_raw_origin_stack, "/av/hopbyhop")
    assert status == 200
    assert "Connection" not in headers
    assert headers.get("X-Should-Survive") == "yes"


# ---------------------------------------------------------------------------
# Raw request-header fidelity toward the origin — a plain http.server or
# http.client doesn't distinguish "one folded line" from "two lines with the
# same name", so this reads the exact bytes off the socket.
# ---------------------------------------------------------------------------


class _RawHeaderCaptureServer:
    """Accepts one connection at a time, records the raw request-header
    block exactly as received, and answers a trivial 200."""

    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(5)
        self.port = self.sock.getsockname()[1]
        self.last_header_lines: list = []
        self._stop = False
        self.thread = threading.Thread(target=self._serve, daemon=True)
        self.thread.start()

    def _serve(self):
        self.sock.settimeout(0.5)
        while not self._stop:
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with conn:
                conn.settimeout(5)
                data = b""
                while b"\r\n\r\n" not in data:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                head, _, _rest = data.partition(b"\r\n\r\n")
                lines = head.decode("latin-1").split("\r\n")
                self.last_header_lines = lines[1:]  # drop the request line
                body = b"ok"
                conn.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: "
                    + str(len(body)).encode() + b"\r\n\r\n" + body
                )

    def stop(self):
        self._stop = True
        self.sock.close()
        self.thread.join(timeout=5)


@pytest.fixture(scope="module")
def raw_header_server():
    server = _RawHeaderCaptureServer()
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture(scope="module")
def cf_raw_header_stack(cloudfront, raw_header_server):
    suffix = _uuid_mod.uuid4().hex[:10]
    dup_edit_fn_arn = None
    if cloudfront_svc._cf_functions_available():
        # Viewer headers are already comma-joined before this module ever
        # sees them (app.py, RFC 9110 §5.2), so a function is the only way to
        # produce a genuine multiValue request header here.
        dup_edit_fn_arn = _create_function(
            cloudfront, f"cf-dp-dup-header-edit-{suffix}",
            b"function handler(event) {\n"
            b"  var request = event.request;\n"
            b"  request.headers['x-dup'] = { value: 'a', multiValue: [{ value: 'a' }, { value: 'b' }] };\n"
            b"  return request;\n"
            b"}\n",
        )
    dist_config = {
        "CallerReference": f"cf-dp-rawheaders-{suffix}",
        "Comment": "cloudfront_dataplane raw request-header test distribution",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "raw-header-origin",
                "DomainName": "127.0.0.1",
                "CustomOriginConfig": {
                    "HTTPPort": raw_header_server.port, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
                # Lowercase, viewer sends Title-Case — proves the overwrite is
                # case-insensitive, not merely a literal-name match.
                "CustomHeaders": {"Quantity": 1, "Items": [{"HeaderName": "x-custom-test", "HeaderValue": "from-origin"}]},
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "raw-header-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED, "OriginRequestPolicyId": _ALL_VIEWER,
        },
    }
    if dup_edit_fn_arn:
        dist_config["CacheBehaviors"] = {
            "Quantity": 1,
            "Items": [{
                "PathPattern": "/dupheader/*",
                "TargetOriginId": "raw-header-origin", "ViewerProtocolPolicy": "allow-all",
                "CachePolicyId": _CACHING_DISABLED, "OriginRequestPolicyId": _ALL_VIEWER,
                "FunctionAssociations": {
                    "Quantity": 1,
                    "Items": [{"FunctionARN": dup_edit_fn_arn, "EventType": "viewer-request"}],
                },
            }],
        }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    return dist["DomainName"]


def test_origin_request_headers_no_duplicates_case_insensitive_overwrite_and_xff(
    cf_raw_header_stack, raw_header_server,
):
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", "/anything", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_raw_header_stack}:{GATEWAY_PORT}")
        conn.putheader("X-Custom-Test", "viewer-value")
        conn.putheader("X-Forwarded-For", "203.0.113.5")
        conn.endheaders()
        resp = conn.getresponse()
        resp.read()
    finally:
        conn.close()

    lines = raw_header_server.last_header_lines
    names_lower = [line.split(":", 1)[0].lower() for line in lines if line]
    # AllViewer forwards Host too: exactly one line, not a lowercase
    # viewer-forwarded one plus a Title-Cased one this module also adds.
    assert names_lower.count("host") == 1

    custom_test_lines = [line for line in lines if line.lower().startswith("x-custom-test:")]
    assert len(custom_test_lines) == 1
    assert custom_test_lines[0].split(":", 1)[1].strip() == "from-origin"

    xff_lines = [line for line in lines if line.lower().startswith("x-forwarded-for:")]
    assert len(xff_lines) == 1
    assert xff_lines[0].split(":", 1)[1].strip() == "203.0.113.5, 127.0.0.1"


def test_function_set_multivalue_header_forwarded_as_one_comma_joined_line(
    cf_raw_header_stack, raw_header_server,
):
    # RFC 9110 §5.3: a repeated field value is one comma-joined line when
    # forwarded to the origin, not several lines of the same name.
    _skip_without_node()
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", "/dupheader/hello", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_raw_header_stack}:{GATEWAY_PORT}")
        conn.endheaders()
        resp = conn.getresponse()
        resp.read()
    finally:
        conn.close()

    dup_lines = [line for line in raw_header_server.last_header_lines if line.lower().startswith("x-dup:")]
    assert len(dup_lines) == 1
    assert dup_lines[0].split(":", 1)[1].strip() == "a, b"


@pytest.fixture(scope="module")
def cf_response_dup_stack(cloudfront, path_echo_origin_port):
    """A distribution whose origin (the raw path-echo server) sends a
    genuinely repeated response header, for the response-side multiValue
    edit-only merge rule below."""
    suffix = _uuid_mod.uuid4().hex[:10]
    fn_arn = None
    if cloudfront_svc._cf_functions_available():
        fn_arn = _create_function(
            cloudfront, f"cf-dp-response-dup-edit-{suffix}",
            b"function handler(event) {\n"
            b"  var response = event.response;\n"
            b"  if (response.headers['x-resp-dup']) {\n"
            b"    response.headers['x-resp-dup'].value = 'edited';\n"
            b"  }\n"
            b"  return response;\n"
            b"}\n",
        )
    behavior = {
        "TargetOriginId": "respdup-origin", "ViewerProtocolPolicy": "allow-all",
        "CachePolicyId": _CACHING_DISABLED,
    }
    if fn_arn:
        behavior["FunctionAssociations"] = {
            "Quantity": 1, "Items": [{"FunctionARN": fn_arn, "EventType": "viewer-response"}],
        }
    dist_config = {
        "CallerReference": f"cf-dp-respdup-{suffix}",
        "Comment": "cloudfront_dataplane response multiValue edit-only test",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "respdup-origin",
                "DomainName": "127.0.0.1",
                "CustomOriginConfig": {
                    "HTTPPort": path_echo_origin_port, "HTTPSPort": 443, "OriginProtocolPolicy": "http-only",
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
            }],
        },
        "DefaultCacheBehavior": behavior,
    }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    return dist["DomainName"]


def test_viewer_response_function_edits_only_value_of_repeated_header_keeps_remaining_values(
    cf_response_dup_stack,
):
    # functions-event-structure.html: a header whose multiValue is unchanged
    # from the event is read only for its primary value; the rest of its
    # original values survive.
    _skip_without_node()
    conn = http.client.HTTPConnection("127.0.0.1", int(GATEWAY_PORT), timeout=10)
    try:
        conn.putrequest("GET", "/respdup", skip_host=True, skip_accept_encoding=True)
        conn.putheader("Host", f"{cf_response_dup_stack}:{GATEWAY_PORT}")
        conn.endheaders()
        resp = conn.getresponse()
        resp.read()
        dup_values = resp.msg.get_all("X-Resp-Dup") or []
    finally:
        conn.close()
    assert dup_values == ["edited", "second"]


# ---------------------------------------------------------------------------
# TLS to a custom https origin.
# ---------------------------------------------------------------------------


@pytest.fixture
def untrusted_https_origin_port(tmp_path):
    pytest.importorskip("cryptography")
    from ministack.core.x509_utils import generate_ca, sign_leaf_certificate

    ca_pem, ca_key_pem = generate_ca(common_name="cf-dp-untrusted-ca")
    leaf_pem, leaf_key_pem, _public = sign_leaf_certificate(
        ca_pem, ca_key_pem, common_name="127.0.0.1", san_ips=["127.0.0.1"],
    )
    cert_path = tmp_path / "leaf.pem"
    cert_path.write_text(leaf_pem + leaf_key_pem)

    server = http.server.HTTPServer(("127.0.0.1", 0), _PathEchoHandler)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(cert_path))
    server.socket = ctx.wrap_socket(server.socket, server_side=True)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield port
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_untrusted_origin_certificate_answers_502(cloudfront, untrusted_https_origin_port):
    # http-502-bad-gateway.html's self-signed/untrusted-chain case: the
    # origin's own CA isn't in the system trust store (and USE_SSL is off in
    # this test harness, so the gateway cert isn't trusted either).
    suffix = _uuid_mod.uuid4().hex[:10]
    dist_config = {
        "CallerReference": f"cf-dp-untrusted-{suffix}",
        "Comment": "cloudfront_dataplane untrusted-origin-cert test",
        "Enabled": True,
        "Origins": {
            "Quantity": 1,
            "Items": [{
                "Id": "untrusted-origin",
                "DomainName": "127.0.0.1",
                "CustomOriginConfig": {
                    "HTTPPort": 80, "HTTPSPort": untrusted_https_origin_port,
                    "OriginProtocolPolicy": "https-only",
                    "OriginSslProtocols": {"Quantity": 1, "Items": ["TLSv1.2"]},
                    "OriginReadTimeout": 30, "OriginKeepaliveTimeout": 5,
                },
            }],
        },
        "DefaultCacheBehavior": {
            "TargetOriginId": "untrusted-origin", "ViewerProtocolPolicy": "allow-all",
            "CachePolicyId": _CACHING_DISABLED,
        },
    }
    dist = cloudfront.create_distribution(DistributionConfig=dist_config)["Distribution"]
    status, headers, _body = _get(dist["DomainName"], "/anything")
    assert status == 502
    assert headers.get("X-Cache") == "Error from cloudfront"
