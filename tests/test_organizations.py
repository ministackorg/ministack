import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


def _client(service, account="test"):
    return boto3.client(service, endpoint_url=ENDPOINT, region_name=REGION,
                        aws_access_key_id=account, aws_secret_access_key="test")


def _client_for(account):
    return _client("organizations", account=account)


@pytest.fixture(scope="module")
def orgs():
    return _client("organizations")


def test_organizations_describe_organization(orgs):
    org = orgs.describe_organization()["Organization"]
    assert org["Id"].startswith("o-")
    assert org["FeatureSet"] == "ALL"
    assert org["Arn"].startswith("arn:aws:organizations::")


def test_organizations_list_roots_singleton(orgs):
    roots = orgs.list_roots()["Roots"]
    assert len(roots) == 1
    assert roots[0]["Id"].startswith("r-")
    assert roots[0]["Name"] == "Root"


def test_organizations_list_accounts_includes_master(orgs):
    accounts = orgs.list_accounts()["Accounts"]
    assert any(a["Id"] == "000000000000" for a in accounts)


def test_organizations_describe_missing_account(orgs):
    with pytest.raises(ClientError) as exc:
        orgs.describe_account(AccountId="999999999999")
    assert exc.value.response["Error"]["Code"] == "AccountNotFoundException"


def test_organizations_create_nested_ou_path():
    """Path field reflects parent chain — additive 2026-03 AWS field."""
    o = _client_for("333333333333")
    root_id = o.list_roots()["Roots"][0]["Id"]
    eng = o.create_organizational_unit(ParentId=root_id, Name="Engineering")["OrganizationalUnit"]
    backend = o.create_organizational_unit(ParentId=eng["Id"], Name="Backend")["OrganizationalUnit"]

    # boto3 1.38 deserialiser may not surface Path; verify via raw HTTP call.
    import json as _json
    import urllib.request as _r
    req = _r.Request(
        "http://localhost:4566/",
        data=_json.dumps({"OrganizationalUnitId": backend["Id"]}).encode(),
        headers={
            "X-Amz-Target": "AWSOrganizationsV20161128.DescribeOrganizationalUnit",
            "Content-Type": "application/x-amz-json-1.1",
            "Authorization": ("AWS4-HMAC-SHA256 Credential=333333333333/20260101/"
                              "us-east-1/organizations/aws4_request, SignedHeaders=, Signature=x"),
        },
    )
    body = _json.loads(_r.urlopen(req).read())
    assert body["OrganizationalUnit"]["Path"] == "/Engineering/Backend/"

    o.delete_organizational_unit(OrganizationalUnitId=backend["Id"])
    o.delete_organizational_unit(OrganizationalUnitId=eng["Id"])


def test_organizations_list_ous_for_parent():
    o = _client_for("444444444444")
    root_id = o.list_roots()["Roots"][0]["Id"]
    ou = o.create_organizational_unit(ParentId=root_id, Name="Sales")["OrganizationalUnit"]
    try:
        listed = o.list_organizational_units_for_parent(ParentId=root_id)["OrganizationalUnits"]
        assert any(x["Id"] == ou["Id"] for x in listed)
    finally:
        o.delete_organizational_unit(OrganizationalUnitId=ou["Id"])
