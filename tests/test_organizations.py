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


def test_organizations_list_parents_ou_under_root():
    """Terraform Read path: ListParents on a top-level OU returns the ROOT parent."""
    o = _client_for("555555555555")
    root_id = o.list_roots()["Roots"][0]["Id"]
    ou = o.create_organizational_unit(ParentId=root_id, Name="Platform")["OrganizationalUnit"]
    try:
        parents = o.list_parents(ChildId=ou["Id"])["Parents"]
        assert len(parents) == 1
        assert parents[0]["Id"] == root_id
        assert parents[0]["Type"] == "ROOT"
    finally:
        o.delete_organizational_unit(OrganizationalUnitId=ou["Id"])


def test_organizations_list_parents_nested_ou():
    """A nested OU reports its parent OU with Type ORGANIZATIONAL_UNIT."""
    o = _client_for("666666666666")
    root_id = o.list_roots()["Roots"][0]["Id"]
    parent = o.create_organizational_unit(ParentId=root_id, Name="Workloads")["OrganizationalUnit"]
    child = o.create_organizational_unit(ParentId=parent["Id"], Name="Prod")["OrganizationalUnit"]
    try:
        parents = o.list_parents(ChildId=child["Id"])["Parents"]
        assert len(parents) == 1
        assert parents[0]["Id"] == parent["Id"]
        assert parents[0]["Type"] == "ORGANIZATIONAL_UNIT"
    finally:
        o.delete_organizational_unit(OrganizationalUnitId=child["Id"])
        o.delete_organizational_unit(OrganizationalUnitId=parent["Id"])


def test_organizations_list_parents_account():
    """ListParents resolves an account's parent (master account sits under root)."""
    o = _client_for("777777777777")
    root_id = o.list_roots()["Roots"][0]["Id"]
    parents = o.list_parents(ChildId="777777777777")["Parents"]
    assert len(parents) == 1
    assert parents[0]["Id"] == root_id
    assert parents[0]["Type"] == "ROOT"


def test_organizations_list_parents_unknown_child():
    o = _client_for("888888888888")
    with pytest.raises(ClientError) as exc:
        o.list_parents(ChildId="ou-xxxx-doesnotexist")
    assert exc.value.response["Error"]["Code"] == "ChildNotFoundException"


def test_organizations_tag_untag_list_resource():
    """TagResource / UntagResource / ListTagsForResource round-trip on an OU — the tag
    read-back the Terraform aws_organizations_organizational_unit Read requires."""
    o = _client_for("999999999999")
    root_id = o.list_roots()["Roots"][0]["Id"]
    ou = o.create_organizational_unit(ParentId=root_id, Name="Tagged")["OrganizationalUnit"]
    try:
        o.tag_resource(
            ResourceId=ou["Id"],
            Tags=[{"Key": "team", "Value": "platform"}, {"Key": "env", "Value": "prod"}],
        )
        tags = {t["Key"]: t["Value"] for t in o.list_tags_for_resource(ResourceId=ou["Id"])["Tags"]}
        assert tags == {"team": "platform", "env": "prod"}
        o.untag_resource(ResourceId=ou["Id"], TagKeys=["env"])
        tags = {t["Key"]: t["Value"] for t in o.list_tags_for_resource(ResourceId=ou["Id"])["Tags"]}
        assert tags == {"team": "platform"}
    finally:
        o.delete_organizational_unit(OrganizationalUnitId=ou["Id"])


def test_organizations_create_ou_with_inline_tags():
    """CreateOrganizationalUnit captures inline Tags so the Terraform Read sees them."""
    o = _client_for("112233445566")
    root_id = o.list_roots()["Roots"][0]["Id"]
    ou = o.create_organizational_unit(
        ParentId=root_id, Name="InlineTagged",
        Tags=[{"Key": "owner", "Value": "secops"}],
    )["OrganizationalUnit"]
    try:
        tags = {t["Key"]: t["Value"] for t in o.list_tags_for_resource(ResourceId=ou["Id"])["Tags"]}
        assert tags == {"owner": "secops"}
    finally:
        o.delete_organizational_unit(OrganizationalUnitId=ou["Id"])


def test_organizations_list_tags_untagged_ou_empty():
    """An untagged OU returns an empty tag set (not an error) — the provider Read calls
    ListTagsForResource on every OU whether or not tags are set, so it must not fail."""
    o = _client_for("223344556677")
    root_id = o.list_roots()["Roots"][0]["Id"]
    ou = o.create_organizational_unit(ParentId=root_id, Name="Untagged")["OrganizationalUnit"]
    try:
        assert o.list_tags_for_resource(ResourceId=ou["Id"])["Tags"] == []
    finally:
        o.delete_organizational_unit(OrganizationalUnitId=ou["Id"])


def test_organizations_list_tags_unknown_resource():
    """ListTagsForResource on a resource that doesn't exist → TargetNotFoundException,
    matching real AWS (not an empty set)."""
    o = _client_for("334455667788")
    o.list_roots()  # ensure org
    with pytest.raises(ClientError) as exc:
        o.list_tags_for_resource(ResourceId="ou-9999-doesnotexist")
    assert exc.value.response["Error"]["Code"] == "TargetNotFoundException"


def test_organizations_tags_are_account_scoped():
    """Tags are account-scoped: one tenant's OU + tags are invisible to another
    (a different account can't even resolve the OU → TargetNotFoundException)."""
    a = _client_for("445566778899")
    b = _client_for("556677889900")
    root_a = a.list_roots()["Roots"][0]["Id"]
    ou_a = a.create_organizational_unit(ParentId=root_a, Name="ScopedA")["OrganizationalUnit"]
    try:
        a.tag_resource(ResourceId=ou_a["Id"], Tags=[{"Key": "owner", "Value": "a"}])
        with pytest.raises(ClientError) as exc:
            b.list_tags_for_resource(ResourceId=ou_a["Id"])
        assert exc.value.response["Error"]["Code"] == "TargetNotFoundException"
        tags = {t["Key"]: t["Value"] for t in a.list_tags_for_resource(ResourceId=ou_a["Id"])["Tags"]}
        assert tags == {"owner": "a"}
    finally:
        a.delete_organizational_unit(OrganizationalUnitId=ou_a["Id"])
