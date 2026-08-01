import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

from ministack.core.responses import (
    AccountScopedDict,
    get_account_id,
    get_region,
    set_request_account_id,
    set_request_region,
)
from ministack.services import waf as waf_service
from ministack.services.cloudformation import provisioners as cfn_provisioners


def _waf_payload(response):
    status, _headers, body = response
    return status, json.loads(body)


@pytest.fixture
def direct_waf_scope():
    original_account = get_account_id()
    original_region = get_region()
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    waf_service.reset()
    yield
    waf_service.reset()
    set_request_account_id(original_account)
    set_request_region(original_region)


def _direct_web_acl_arn(name="direct-acl"):
    status, payload = _waf_payload(waf_service._create_web_acl({
        "Name": name,
        "Scope": "REGIONAL",
        "DefaultAction": {"Allow": {}},
        "VisibilityConfig": {
            "SampledRequestsEnabled": False,
            "CloudWatchMetricsEnabled": False,
            "MetricName": name,
        },
    }))
    assert status == 200
    return payload["Summary"]["ARN"]


def _direct_cloudfront_web_acl_arn(name="direct-cloudfront-acl", arn_scope="cloudfront"):
    uid = _uuid_mod.uuid4().hex
    arn = f"arn:aws:wafv2:{get_region()}:{get_account_id()}:{arn_scope}/webacl/{name}/{uid}"
    waf_service._web_acls[uid] = {
        "ARN": arn,
        "Id": uid,
        "Name": name,
        "Description": "",
        "DefaultAction": {"Allow": {}},
        "Rules": [],
        "VisibilityConfig": {},
        "Capacity": 0,
        "LockToken": _uuid_mod.uuid4().hex,
        "Scope": "CLOUDFRONT",
    }
    waf_service._waf_tags[arn] = []
    return arn


def _assert_waf_error(response, code):
    status, payload = _waf_payload(response)
    assert status == 400
    assert payload["__type"] == code


def _create_direct_web_acl(name, scope="REGIONAL", tags=None):
    status, payload = _waf_payload(waf_service._create_web_acl({
        "Name": name,
        "Scope": scope,
        "DefaultAction": {"Allow": {}},
        "VisibilityConfig": {
            "SampledRequestsEnabled": False,
            "CloudWatchMetricsEnabled": False,
            "MetricName": name,
        },
        "Tags": tags or [],
    }))
    assert status == 200
    return payload["Summary"]


def test_waf_web_acl_crud(wafv2):
    resp = wafv2.create_web_acl(
        Name="test-acl",
        Scope="REGIONAL",
        DefaultAction={"Allow": {}},
        VisibilityConfig={"SampledRequestsEnabled": True, "CloudWatchMetricsEnabled": False, "MetricName": "test"},
    )
    uid = resp["Summary"]["Id"]
    assert resp["Summary"]["Name"] == "test-acl"

    get_resp = wafv2.get_web_acl(Name="test-acl", Scope="REGIONAL", Id=uid)
    assert get_resp["WebACL"]["Name"] == "test-acl"

    lst = wafv2.list_web_acls(Scope="REGIONAL")
    ids = [a["Id"] for a in lst["WebACLs"]]
    assert uid in ids

    wafv2.delete_web_acl(Name="test-acl", Scope="REGIONAL", Id=uid, LockToken=resp["Summary"]["LockToken"])
    lst2 = wafv2.list_web_acls(Scope="REGIONAL")
    ids2 = [a["Id"] for a in lst2["WebACLs"]]
    assert uid not in ids2


def test_wafv2_regional_web_acls_are_isolated_by_region_direct(direct_waf_scope):
    east_acl = _create_direct_web_acl("same-regional-acl", "REGIONAL")
    assert ":us-east-1:000000000000:regional/webacl/" in east_acl["ARN"]
    _assert_waf_error(
        waf_service._create_web_acl({
            "Name": "same-regional-acl",
            "Scope": "REGIONAL",
        }),
        "WAFDuplicateItemException",
    )

    set_request_region("us-west-2")
    west_acl = _create_direct_web_acl("same-regional-acl", "REGIONAL")
    assert west_acl["Id"] != east_acl["Id"]
    assert ":us-west-2:000000000000:regional/webacl/" in west_acl["ARN"]

    status, payload = _waf_payload(waf_service._list_web_acls({"Scope": "REGIONAL"}))
    assert status == 200
    assert [acl["Id"] for acl in payload["WebACLs"]] == [west_acl["Id"]]

    _assert_waf_error(
        waf_service._get_web_acl({
            "Name": "same-regional-acl",
            "Scope": "REGIONAL",
            "Id": east_acl["Id"],
        }),
        "WAFNonexistentItemException",
    )

    set_request_region("us-east-1")
    status, payload = _waf_payload(waf_service._list_web_acls({"Scope": "REGIONAL"}))
    assert status == 200
    assert [acl["Id"] for acl in payload["WebACLs"]] == [east_acl["Id"]]


def test_wafv2_cloudfront_web_acl_is_homed_globally_direct(direct_waf_scope):
    set_request_region("us-west-2")
    acl = _create_direct_web_acl(
        "global-acl",
        "CLOUDFRONT",
        tags=[{"Key": "scope", "Value": "global"}],
    )

    assert ":us-east-1:000000000000:global/webacl/" in acl["ARN"]
    assert waf_service._web_acls.get_scoped(
        "000000000000",
        "us-east-1",
        acl["Id"],
    )["ARN"] == acl["ARN"]
    assert waf_service._web_acls.get_scoped(
        "000000000000",
        "us-west-2",
        acl["Id"],
    ) is None

    for region in ("us-west-2", "eu-west-1", "us-east-1"):
        set_request_region(region)
        status, payload = _waf_payload(waf_service._list_web_acls({"Scope": "CLOUDFRONT"}))
        assert status == 200
        assert [item["ARN"] for item in payload["WebACLs"]] == [acl["ARN"]]

        status, payload = _waf_payload(waf_service._get_web_acl({
            "Name": "global-acl",
            "Scope": "CLOUDFRONT",
            "Id": acl["Id"],
        }))
        assert status == 200
        assert payload["WebACL"]["ARN"] == acl["ARN"]

        status, payload = _waf_payload(waf_service._list_tags_for_resource({
            "ResourceARN": acl["ARN"],
        }))
        assert status == 200
        assert payload["TagInfoForResource"]["TagList"] == [
            {"Key": "scope", "Value": "global"}
        ]

    status, payload = _waf_payload(waf_service._update_web_acl({
        "Name": "global-acl",
        "Scope": "CLOUDFRONT",
        "Id": acl["Id"],
        "LockToken": acl["LockToken"],
        "DefaultAction": {"Block": {}},
        "VisibilityConfig": {
            "SampledRequestsEnabled": False,
            "CloudWatchMetricsEnabled": False,
            "MetricName": "global-acl",
        },
    }))
    assert status == 200

    status, _payload = _waf_payload(waf_service._delete_web_acl({
        "Name": "global-acl",
        "Scope": "CLOUDFRONT",
        "Id": acl["Id"],
        "LockToken": payload["NextLockToken"],
    }))
    assert status == 200
    assert acl["ARN"] not in waf_service._waf_tags


def test_waf_update_web_acl(wafv2):
    resp = wafv2.create_web_acl(
        Name="update-acl",
        Scope="REGIONAL",
        DefaultAction={"Block": {}},
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m"},
    )
    uid = resp["Summary"]["Id"]
    lock = resp["Summary"]["LockToken"]
    upd = wafv2.update_web_acl(
        Name="update-acl",
        Scope="REGIONAL",
        Id=uid,
        LockToken=lock,
        DefaultAction={"Allow": {}},
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m"},
    )
    assert "NextLockToken" in upd

def test_waf_associate_disassociate(wafv2):
    resp = wafv2.create_web_acl(
        Name="assoc-acl",
        Scope="REGIONAL",
        DefaultAction={"Allow": {}},
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m"},
    )
    acl_arn = resp["Summary"]["ARN"]
    resource_arn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/test/abc"
    wafv2.associate_web_acl(WebACLArn=acl_arn, ResourceArn=resource_arn)
    get_resp = wafv2.get_web_acl_for_resource(ResourceArn=resource_arn)
    assert get_resp["WebACL"]["ARN"] == acl_arn
    wafv2.disassociate_web_acl(ResourceArn=resource_arn)
    try:
        wafv2.get_web_acl_for_resource(ResourceArn=resource_arn)
        assert False, "expected WAFNonexistentItemException"
    except wafv2.exceptions.WAFNonexistentItemException:
        pass


def test_wafv2_associations_are_request_region_scoped_direct(direct_waf_scope):
    acl = _create_direct_web_acl("regional-assoc-acl", "REGIONAL")
    resource_arn = (
        "arn:aws:elasticloadbalancing:us-east-1:000000000000:"
        "loadbalancer/app/regional-assoc/abc"
    )
    status, _payload = _waf_payload(waf_service._associate_web_acl({
        "WebACLArn": acl["ARN"],
        "ResourceArn": resource_arn,
    }))
    assert status == 200

    set_request_region("us-west-2")
    _assert_waf_error(
        waf_service._get_web_acl_for_resource({"ResourceArn": resource_arn}),
        "WAFNonexistentItemException",
    )

    west_acl = _create_direct_web_acl("regional-assoc-acl", "REGIONAL")
    status, payload = _waf_payload(waf_service._list_resources_for_web_acl({
        "WebACLArn": west_acl["ARN"],
    }))
    assert status == 200
    assert payload["ResourceArns"] == []


def test_wafv2_cloudfront_associations_stay_request_region_scoped_direct(direct_waf_scope):
    acl = _create_direct_web_acl("cloudfront-assoc-acl", "CLOUDFRONT")
    resource_arn = "arn:aws:cloudfront::000000000000:distribution/cf-assoc"
    status, _payload = _waf_payload(waf_service._associate_web_acl({
        "WebACLArn": acl["ARN"],
        "ResourceArn": resource_arn,
    }))
    assert status == 200

    status, payload = _waf_payload(waf_service._get_web_acl_for_resource({
        "ResourceArn": resource_arn,
    }))
    assert status == 200
    assert payload["WebACL"]["ARN"] == acl["ARN"]

    set_request_region("us-west-2")
    _assert_waf_error(
        waf_service._get_web_acl_for_resource({"ResourceArn": resource_arn}),
        "WAFNonexistentItemException",
    )
    status, payload = _waf_payload(waf_service._list_resources_for_web_acl({
        "WebACLArn": acl["ARN"],
    }))
    assert status == 200
    assert payload["ResourceArns"] == []


def test_waf_ip_set_crud(wafv2):
    resp = wafv2.create_ip_set(
        Name="test-ipset",
        Scope="REGIONAL",
        IPAddressVersion="IPV4",
        Addresses=["1.2.3.4/32"],
    )
    uid = resp["Summary"]["Id"]
    lock = resp["Summary"]["LockToken"]

    get_resp = wafv2.get_ip_set(Name="test-ipset", Scope="REGIONAL", Id=uid)
    assert "1.2.3.4/32" in get_resp["IPSet"]["Addresses"]

    upd = wafv2.update_ip_set(
        Name="test-ipset",
        Scope="REGIONAL",
        Id=uid,
        LockToken=lock,
        Addresses=["5.6.7.8/32"],
    )
    assert "NextLockToken" in upd

    lst = wafv2.list_ip_sets(Scope="REGIONAL")
    ids = [s["Id"] for s in lst["IPSets"]]
    assert uid in ids

    wafv2.delete_ip_set(Name="test-ipset", Scope="REGIONAL", Id=uid, LockToken=upd["NextLockToken"])
    lst2 = wafv2.list_ip_sets(Scope="REGIONAL")
    ids2 = [s["Id"] for s in lst2["IPSets"]]
    assert uid not in ids2

def test_waf_rule_group_crud(wafv2):
    resp = wafv2.create_rule_group(
        Name="test-rg",
        Scope="REGIONAL",
        Capacity=100,
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m"},
    )
    uid = resp["Summary"]["Id"]
    lock = resp["Summary"]["LockToken"]

    get_resp = wafv2.get_rule_group(Name="test-rg", Scope="REGIONAL", Id=uid)
    assert get_resp["RuleGroup"]["Name"] == "test-rg"
    assert "LockToken" not in get_resp["RuleGroup"]

    upd = wafv2.update_rule_group(
        Name="test-rg",
        Scope="REGIONAL",
        Id=uid,
        LockToken=lock,
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m2"},
    )
    assert "NextLockToken" in upd

    lst = wafv2.list_rule_groups(Scope="REGIONAL")
    ids = [r["Id"] for r in lst["RuleGroups"]]
    assert uid in ids

    wafv2.delete_rule_group(Name="test-rg", Scope="REGIONAL", Id=uid, LockToken=upd["NextLockToken"])
    lst2 = wafv2.list_rule_groups(Scope="REGIONAL")
    ids2 = [r["Id"] for r in lst2["RuleGroups"]]
    assert uid not in ids2


def test_wafv2_ip_sets_and_rule_groups_use_scope_aware_arns_direct(direct_waf_scope):
    set_request_region("us-west-2")
    status, ipset_payload = _waf_payload(waf_service._create_ip_set({
        "Name": "cf-ipset",
        "Scope": "CLOUDFRONT",
        "IPAddressVersion": "IPV4",
        "Addresses": ["192.0.2.0/24"],
    }))
    assert status == 200
    ipset = ipset_payload["Summary"]
    assert ":us-east-1:000000000000:global/ipset/" in ipset["ARN"]

    status, rg_payload = _waf_payload(waf_service._create_rule_group({
        "Name": "cf-rg",
        "Scope": "CLOUDFRONT",
        "Capacity": 10,
        "VisibilityConfig": {
            "SampledRequestsEnabled": False,
            "CloudWatchMetricsEnabled": False,
            "MetricName": "cf-rg",
        },
    }))
    assert status == 200
    rule_group = rg_payload["Summary"]
    assert ":us-east-1:000000000000:global/rulegroup/" in rule_group["ARN"]

    set_request_region("eu-west-1")
    status, ipset_payload = _waf_payload(waf_service._get_ip_set({
        "Name": "cf-ipset",
        "Scope": "CLOUDFRONT",
        "Id": ipset["Id"],
    }))
    assert status == 200
    assert ipset_payload["IPSet"]["ARN"] == ipset["ARN"]

    status, rg_payload = _waf_payload(waf_service._get_rule_group({
        "Name": "cf-rg",
        "Scope": "CLOUDFRONT",
        "Id": rule_group["Id"],
    }))
    assert status == 200
    assert rg_payload["RuleGroup"]["ARN"] == rule_group["ARN"]


def test_waf_tags(wafv2):
    resp = wafv2.create_web_acl(
        Name="tag-acl",
        Scope="REGIONAL",
        DefaultAction={"Allow": {}},
        VisibilityConfig={"SampledRequestsEnabled": False, "CloudWatchMetricsEnabled": False, "MetricName": "m"},
        Tags=[{"Key": "env", "Value": "test"}],
    )
    arn = resp["Summary"]["ARN"]
    tags_resp = wafv2.list_tags_for_resource(ResourceARN=arn)
    assert any(t["Key"] == "env" for t in tags_resp["TagInfoForResource"]["TagList"])
    wafv2.tag_resource(ResourceARN=arn, Tags=[{"Key": "team", "Value": "security"}])
    tags_resp2 = wafv2.list_tags_for_resource(ResourceARN=arn)
    assert any(t["Key"] == "team" for t in tags_resp2["TagInfoForResource"]["TagList"])
    wafv2.untag_resource(ResourceARN=arn, TagKeys=["env"])
    tags_resp3 = wafv2.list_tags_for_resource(ResourceARN=arn)
    assert not any(t["Key"] == "env" for t in tags_resp3["TagInfoForResource"]["TagList"])


def test_waf_tag_apis_accept_local_waf_resource_arns_direct(direct_waf_scope):
    web_acl_arn = _direct_web_acl_arn("direct-tag-acl")
    cloudfront_web_acl_arn = _direct_cloudfront_web_acl_arn("direct-tag-cf-acl")
    global_web_acl_arn = _direct_cloudfront_web_acl_arn(
        "direct-tag-global-acl",
        arn_scope="global",
    )
    ip_set_arn = _waf_payload(waf_service._create_ip_set({
        "Name": "direct-tag-ipset",
        "Scope": "REGIONAL",
        "IPAddressVersion": "IPV4",
        "Addresses": ["192.0.2.0/24"],
    }))[1]["Summary"]["ARN"]
    rule_group_arn = _waf_payload(waf_service._create_rule_group({
        "Name": "direct-tag-rg",
        "Scope": "REGIONAL",
        "Capacity": 10,
        "VisibilityConfig": {
            "SampledRequestsEnabled": False,
            "CloudWatchMetricsEnabled": False,
            "MetricName": "direct-tag-rg",
        },
    }))[1]["Summary"]["ARN"]

    for arn in (
        web_acl_arn,
        cloudfront_web_acl_arn,
        global_web_acl_arn,
        ip_set_arn,
        rule_group_arn,
    ):
        status, _payload = _waf_payload(waf_service._tag_resource({
            "ResourceARN": arn,
            "Tags": [{"Key": "team", "Value": "security"}],
        }))
        assert status == 200
        status, payload = _waf_payload(waf_service._list_tags_for_resource({
            "ResourceARN": arn,
        }))
        assert status == 200
        assert payload["TagInfoForResource"]["TagList"] == [
            {"Key": "team", "Value": "security"}
        ]
        status, _payload = _waf_payload(waf_service._untag_resource({
            "ResourceARN": arn,
            "TagKeys": ["team"],
        }))
        assert status == 200


@pytest.mark.parametrize("resource_arn", [
    "not-an-arn",
    "arn:aws-cn:wafv2:us-east-1:000000000000:regional/webacl/name/id",
    "arn:aws:s3:us-east-1:000000000000:regional/webacl/name/id",
    "arn:aws:wafv2:us-west-2:000000000000:regional/webacl/name/id",
    "arn:aws:wafv2:us-east-1:111111111111:regional/webacl/name/id",
    "arn:aws:wafv2:us-east-1:000000000000:cloudfront/ipset/name/id",
    "arn:aws:wafv2:us-east-1:000000000000:regional/regexset/name/id",
    "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/name/id/extra",
])
def test_waf_tag_apis_reject_invalid_resource_arns_direct(direct_waf_scope, resource_arn):
    _assert_waf_error(
        waf_service._tag_resource({
            "ResourceARN": resource_arn,
            "Tags": [{"Key": "team", "Value": "security"}],
        }),
        "WAFInvalidParameterException",
    )
    _assert_waf_error(
        waf_service._untag_resource({
            "ResourceARN": resource_arn,
            "TagKeys": ["team"],
        }),
        "WAFInvalidParameterException",
    )
    _assert_waf_error(
        waf_service._list_tags_for_resource({"ResourceARN": resource_arn}),
        "WAFInvalidParameterException",
    )
    assert resource_arn not in waf_service._waf_tags


def test_waf_tag_apis_reject_missing_local_resource_arn_direct(direct_waf_scope):
    missing_arn = "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/missing/id"

    _assert_waf_error(
        waf_service._tag_resource({
            "ResourceARN": missing_arn,
            "Tags": [{"Key": "team", "Value": "security"}],
        }),
        "WAFNonexistentItemException",
    )
    assert missing_arn not in waf_service._waf_tags


def test_waf_association_validates_web_acl_arn_but_not_resource_arn_direct(direct_waf_scope):
    web_acl_arn = _direct_web_acl_arn("direct-assoc-acl")
    opaque_resource_arn = "not-a-waf-resource-arn"

    status, _payload = _waf_payload(waf_service._associate_web_acl({
        "WebACLArn": web_acl_arn,
        "ResourceArn": opaque_resource_arn,
    }))
    assert status == 200

    status, payload = _waf_payload(waf_service._list_resources_for_web_acl({
        "WebACLArn": web_acl_arn,
    }))
    assert status == 200
    assert payload["ResourceArns"] == [opaque_resource_arn]

    cloudfront_web_acl_arn = _direct_cloudfront_web_acl_arn("direct-assoc-cf-acl")
    cloudfront_resource_arn = "arn:aws:cloudfront::000000000000:distribution/dist"
    status, _payload = _waf_payload(waf_service._associate_web_acl({
        "WebACLArn": cloudfront_web_acl_arn,
        "ResourceArn": cloudfront_resource_arn,
    }))
    assert status == 200
    status, payload = _waf_payload(waf_service._list_resources_for_web_acl({
        "WebACLArn": cloudfront_web_acl_arn,
    }))
    assert status == 200
    assert payload["ResourceArns"] == [cloudfront_resource_arn]

    global_web_acl_arn = _direct_cloudfront_web_acl_arn(
        "direct-assoc-global-acl",
        arn_scope="global",
    )
    global_resource_arn = "arn:aws:cloudfront::000000000000:distribution/global-dist"
    status, _payload = _waf_payload(waf_service._associate_web_acl({
        "WebACLArn": global_web_acl_arn,
        "ResourceArn": global_resource_arn,
    }))
    assert status == 200
    status, payload = _waf_payload(waf_service._list_resources_for_web_acl({
        "WebACLArn": global_web_acl_arn,
    }))
    assert status == 200
    assert payload["ResourceArns"] == [global_resource_arn]

    wrong_service_arn = web_acl_arn.replace(":wafv2:", ":s3:")
    _assert_waf_error(
        waf_service._associate_web_acl({
            "WebACLArn": wrong_service_arn,
            "ResourceArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/app/id",
        }),
        "WAFInvalidParameterException",
    )
    _assert_waf_error(
        waf_service._list_resources_for_web_acl({"WebACLArn": wrong_service_arn}),
        "WAFInvalidParameterException",
    )


def test_wafv2_restore_legacy_state_canonicalizes_cloudfront_arns_direct(direct_waf_scope):
    account_id = "000000000000"
    legacy_uid = "legacy-cf-acl"
    legacy_arn = (
        "arn:aws:wafv2:us-west-2:000000000000:"
        "cloudfront/webacl/legacy-cf-acl/legacy-cf-acl"
    )
    regional_segment_uid = "legacy-regional-segment-cf-acl"
    regional_segment_arn = (
        "arn:aws:wafv2:ap-southeast-2:000000000000:"
        "regional/webacl/legacy-regional-segment-cf-acl/"
        "legacy-regional-segment-cf-acl"
    )
    legacy_cf_ipset_uid = "legacy-cf-ipset"
    legacy_cf_ipset_arn = (
        "arn:aws:wafv2:us-west-2:000000000000:"
        "regional/ipset/legacy-cf-ipset/legacy-cf-ipset"
    )
    legacy_cf_rule_group_uid = "legacy-cf-rule-group"
    legacy_cf_rule_group_arn = (
        "arn:aws:wafv2:us-west-2:000000000000:"
        "cloudfront/rulegroup/legacy-cf-rule-group/legacy-cf-rule-group"
    )
    regional_legacy_arn = (
        "arn:aws:wafv2:eu-west-1:000000000000:"
        "regional/ipset/legacy-regional-ipset/legacy-regional-ipset"
    )
    resource_arn = (
        "arn:aws:elasticloadbalancing:us-west-2:000000000000:"
        "loadbalancer/app/legacy/abc"
    )
    second_resource_arn = (
        "arn:aws:elasticloadbalancing:ap-southeast-2:000000000000:"
        "loadbalancer/app/legacy-regional-segment/def"
    )

    web_acls = AccountScopedDict()
    web_acls._data[(account_id, legacy_uid)] = {
        "ARN": legacy_arn,
        "Id": legacy_uid,
        "Name": "legacy-cf-acl",
        "Scope": "CLOUDFRONT",
        "LockToken": "lock",
        "Rules": [{
            "Name": "rg-ref",
            "Statement": {
                "RuleGroupReferenceStatement": {
                    "ARN": legacy_cf_rule_group_arn,
                },
            },
        }],
    }
    web_acls._data[(account_id, regional_segment_uid)] = {
        "ARN": regional_segment_arn,
        "Id": regional_segment_uid,
        "Name": "legacy-regional-segment-cf-acl",
        "Scope": "CLOUDFRONT",
        "LockToken": "second-lock",
    }
    ip_sets = AccountScopedDict()
    ip_sets._data[(account_id, "legacy-regional-ipset")] = {
        "ARN": regional_legacy_arn,
        "Id": "legacy-regional-ipset",
        "Name": "legacy-regional-ipset",
        "Scope": "REGIONAL",
        "LockToken": "ip-lock",
    }
    ip_sets._data[(account_id, legacy_cf_ipset_uid)] = {
        "ARN": legacy_cf_ipset_arn,
        "Id": legacy_cf_ipset_uid,
        "Name": "legacy-cf-ipset",
        "Scope": "CLOUDFRONT",
        "LockToken": "cf-ip-lock",
    }
    rule_groups = AccountScopedDict()
    rule_groups._data[(account_id, legacy_cf_rule_group_uid)] = {
        "ARN": legacy_cf_rule_group_arn,
        "Id": legacy_cf_rule_group_uid,
        "Name": "legacy-cf-rule-group",
        "Scope": "CLOUDFRONT",
        "LockToken": "cf-rg-lock",
        "Rules": [{
            "Name": "ipset-ref",
            "Statement": {
                "IPSetReferenceStatement": {
                    "ARN": legacy_cf_ipset_arn,
                },
            },
        }],
    }
    associations = AccountScopedDict()
    associations._data[(account_id, resource_arn)] = legacy_arn
    associations._data[(account_id, second_resource_arn)] = regional_segment_arn
    tags = AccountScopedDict()
    tags._data[(account_id, legacy_arn)] = [{"Key": "legacy", "Value": "yes"}]
    tags._data[(account_id, regional_segment_arn)] = [
        {"Key": "legacy-dialect", "Value": "regional"}
    ]

    waf_service.restore_state({
        "_web_acls": web_acls,
        "_ip_sets": ip_sets,
        "_rule_groups": rule_groups,
        "_associations": associations,
        "_waf_tags": tags,
    })

    canonical_arn = (
        "arn:aws:wafv2:us-east-1:000000000000:"
        "global/webacl/legacy-cf-acl/legacy-cf-acl"
    )
    second_canonical_arn = (
        "arn:aws:wafv2:us-east-1:000000000000:"
        "global/webacl/legacy-regional-segment-cf-acl/"
        "legacy-regional-segment-cf-acl"
    )
    canonical_ipset_arn = (
        "arn:aws:wafv2:us-east-1:000000000000:"
        "global/ipset/legacy-cf-ipset/legacy-cf-ipset"
    )
    canonical_rule_group_arn = (
        "arn:aws:wafv2:us-east-1:000000000000:"
        "global/rulegroup/legacy-cf-rule-group/legacy-cf-rule-group"
    )
    assert waf_service._web_acls.get_scoped(
        account_id,
        "us-east-1",
        legacy_uid,
    )["ARN"] == canonical_arn
    assert waf_service._ip_sets.get_scoped(
        account_id,
        "eu-west-1",
        "legacy-regional-ipset",
    )["ARN"] == regional_legacy_arn
    assert waf_service._web_acls.get_scoped(
        account_id,
        "us-east-1",
        regional_segment_uid,
    )["ARN"] == second_canonical_arn
    assert waf_service._web_acls.get_scoped(
        account_id,
        "us-east-1",
        legacy_uid,
    )["Rules"][0]["Statement"]["RuleGroupReferenceStatement"]["ARN"] == (
        canonical_rule_group_arn
    )
    assert waf_service._rule_groups.get_scoped(
        account_id,
        "us-east-1",
        legacy_cf_rule_group_uid,
    )["Rules"][0]["Statement"]["IPSetReferenceStatement"]["ARN"] == canonical_ipset_arn
    assert waf_service._associations.get_scoped(
        account_id,
        "us-west-2",
        resource_arn,
    ) == canonical_arn
    assert waf_service._associations.get_scoped(
        account_id,
        "ap-southeast-2",
        second_resource_arn,
    ) == second_canonical_arn
    assert waf_service._waf_tags.get_scoped(
        account_id,
        "us-west-2",
        canonical_arn,
    ) == [{"Key": "legacy", "Value": "yes"}]
    assert waf_service._waf_tags.get_scoped(
        account_id,
        "ap-southeast-2",
        second_canonical_arn,
    ) == [{"Key": "legacy-dialect", "Value": "regional"}]
    assert waf_service._waf_tags.get_scoped(account_id, "us-west-2", legacy_arn) is None
    assert waf_service._waf_tags.get_scoped(
        account_id, "ap-southeast-2", regional_segment_arn
    ) is None


def test_wafv2_cloudformation_uses_canonical_web_acl_records_direct(direct_waf_scope):
    set_request_region("us-west-2")
    uid, attrs = cfn_provisioners._waf_web_acl_create(
        "Acl",
        {
            "Name": "cfn-cf-acl",
            "Scope": "CLOUDFRONT",
            "DefaultAction": {"Allow": {}},
            "VisibilityConfig": {
                "SampledRequestsEnabled": False,
                "CloudWatchMetricsEnabled": False,
                "MetricName": "cfn-cf-acl",
            },
            "Tags": [{"Key": "from", "Value": "cfn"}],
        },
        "stack",
    )
    assert attrs["Arn"] == (
        f"arn:aws:wafv2:us-east-1:000000000000:global/webacl/cfn-cf-acl/{uid}"
    )
    assert waf_service._web_acls.get_scoped("000000000000", "us-east-1", uid)[
        "ARN"
    ] == attrs["Arn"]
    assert waf_service._waf_tags[attrs["Arn"]] == [{"Key": "from", "Value": "cfn"}]

    cfn_provisioners._waf_web_acl_delete(uid, {"Scope": "CLOUDFRONT"})
    assert waf_service._web_acls.get_scoped("000000000000", "us-east-1", uid) is None
    assert attrs["Arn"] not in waf_service._waf_tags


def test_waf_check_capacity(wafv2):
    resp = wafv2.check_capacity(
        Scope="REGIONAL",
        Rules=[
            {
                "Name": "rate-rule",
                "Priority": 1,
                "Statement": {"RateBasedStatement": {"Limit": 1000, "AggregateKeyType": "IP"}},
                "Action": {"Block": {}},
                "VisibilityConfig": {
                    "SampledRequestsEnabled": False,
                    "CloudWatchMetricsEnabled": False,
                    "MetricName": "rate",
                },
            }
        ],
    )
    assert "Capacity" in resp
    assert isinstance(resp["Capacity"], int)

def test_waf_describe_managed_rule_group(wafv2):
    resp = wafv2.describe_managed_rule_group(
        VendorName="AWS",
        Name="AWSManagedRulesCommonRuleSet",
        Scope="REGIONAL",
    )
    assert "Capacity" in resp
    assert "Rules" in resp
    assert isinstance(resp["Rules"], list)

def test_waf_list_resources_for_web_acl(wafv2):
    resp = wafv2.create_web_acl(
        Name="res-list-acl",
        Scope="REGIONAL",
        DefaultAction={"Allow": {}},
        VisibilityConfig={
            "SampledRequestsEnabled": False,
            "CloudWatchMetricsEnabled": False,
            "MetricName": "m",
        },
    )
    acl_arn = resp["Summary"]["ARN"]
    resource_arn = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/waf-test/xyz"
    wafv2.associate_web_acl(WebACLArn=acl_arn, ResourceArn=resource_arn)

    list_resp = wafv2.list_resources_for_web_acl(
        WebACLArn=acl_arn, ResourceType="APPLICATION_LOAD_BALANCER"
    )
    assert resource_arn in list_resp.get("ResourceArns", [])
