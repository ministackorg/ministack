"""Tests for WAF Classic + WAF Regional v1 stub.

The v1 stub returns AWS-shape-correct empty-state responses so that legacy
SDKs (Terraform, old CFN, JDK examples) targeting v1 don't get a 405.
For real WebACL state, callers should use wafv2.
"""

import boto3
import pytest


def _client(service):
    return boto3.client(service, endpoint_url="http://localhost:4566",
                        region_name="us-east-1",
                        aws_access_key_id="test", aws_secret_access_key="test")


@pytest.fixture(scope="module")
def waf_regional():
    return _client("waf-regional")


@pytest.fixture(scope="module")
def waf_classic():
    return _client("waf")


def test_waf_regional_list_web_acls_empty(waf_regional):
    r = waf_regional.list_web_acls(Limit=10)
    assert r["WebACLs"] == []


def test_waf_regional_list_ip_sets_empty(waf_regional):
    r = waf_regional.list_ip_sets(Limit=10)
    assert r["IPSets"] == []


def test_waf_regional_list_rules_empty(waf_regional):
    r = waf_regional.list_rules(Limit=10)
    assert r["Rules"] == []


def test_waf_regional_get_change_token_returns_token(waf_regional):
    r = waf_regional.get_change_token()
    token = r["ChangeToken"]
    assert isinstance(token, str)
    assert len(token) >= 8


def test_waf_regional_get_change_token_status_is_insync(waf_regional):
    token = waf_regional.get_change_token()["ChangeToken"]
    s = waf_regional.get_change_token_status(ChangeToken=token)
    assert s["ChangeTokenStatus"] == "INSYNC"


def test_waf_regional_get_unknown_webacl_raises_not_found(waf_regional):
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        waf_regional.get_web_acl(WebACLId="never-existed")
    assert exc.value.response["Error"]["Code"] == "WAFNonexistentItemException"


def test_waf_classic_list_web_acls_empty(waf_classic):
    """waf-classic shares the same v1 module — verify the second target prefix routes correctly."""
    r = waf_classic.list_web_acls(Limit=10)
    assert r["WebACLs"] == []


def test_waf_classic_get_change_token(waf_classic):
    r = waf_classic.get_change_token()
    assert r["ChangeToken"]


def test_waf_v1_does_not_collide_with_wafv2():
    """Sanity: wafv2 uses the real handler, v1 the stub. Listing on each
    targets distinct backends and must not cross-contaminate."""
    v1 = _client("waf-regional")
    v2 = _client("wafv2")
    # An empty wafv2 list still works alongside the v1 stub
    assert isinstance(v1.list_web_acls(Limit=5)["WebACLs"], list)
    assert isinstance(v2.list_web_acls(Scope="REGIONAL", Limit=5)["WebACLs"], list)


def test_waf_v1_module_exposes_no_op_reset():
    from ministack.services import waf_v1
    assert callable(getattr(waf_v1, "reset", None))
    waf_v1.reset()
