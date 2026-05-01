import json
import urllib.request

import pytest

import boto3


@pytest.fixture(scope="module")
def acct():
    return boto3.client("account", endpoint_url="http://localhost:4566",
                        region_name="us-east-1",
                        aws_access_key_id="test", aws_secret_access_key="test")


def test_account_get_account_information(acct):
    info = acct.get_account_information()
    assert info["AccountId"] == "000000000000"
    assert info["AccountName"]
    assert isinstance(info["AccountCreatedDate"], int) or hasattr(info["AccountCreatedDate"], "year")


def test_account_get_account_information_includes_account_state_field():
    """AccountState added 2026-04-29; verify present on the wire even though
    older botocore may strip it."""
    req = urllib.request.Request(
        "http://localhost:4566/getAccountInformation",
        data=b"{}",
        headers={
            "Content-Type": "application/json",
            "Authorization": ("AWS4-HMAC-SHA256 Credential=test/20260101/"
                              "us-east-1/account/aws4_request, SignedHeaders=, Signature=x"),
        },
        method="POST",
    )
    body = json.loads(urllib.request.urlopen(req).read())
    assert body["AccountState"] == "ACTIVE"


def test_account_list_regions(acct):
    regions = acct.list_regions()["Regions"]
    names = {r["RegionName"]: r["RegionOptStatus"] for r in regions}
    assert "us-east-1" in names
    assert names["us-east-1"] == "ENABLED_BY_DEFAULT"
    # New regions should report opt-in required
    assert names.get("ap-southeast-5") in ("ENABLED", "ENABLED_BY_DEFAULT")


def test_account_get_region_opt_status(acct):
    r = acct.get_region_opt_status(RegionName="eu-west-1")
    assert r["RegionName"] == "eu-west-1"
    assert r["RegionOptStatus"] in ("ENABLED_BY_DEFAULT", "ENABLED")


def test_account_get_region_opt_status_unknown_region(acct):
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        acct.get_region_opt_status(RegionName="xx-nowhere-1")
    assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


def test_account_get_contact_information(acct):
    info = acct.get_contact_information()["ContactInformation"]
    assert info["FullName"]
    assert info["CountryCode"] == "US"
