import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


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
