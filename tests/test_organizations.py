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


def test_organizations_create_account_is_async():
    """CreateAccount returns IN_PROGRESS; the account materialises on first describe."""
    o = _client_for("555555555551")
    car = o.create_account(Email="dev@example.com", AccountName="dev")["CreateAccountStatus"]
    assert car["State"] == "IN_PROGRESS"
    assert car["Id"].startswith("car-")
    assert "AccountId" not in car  # not assigned until the create completes

    status = o.describe_create_account_status(
        CreateAccountRequestId=car["Id"])["CreateAccountStatus"]
    assert status["State"] == "SUCCEEDED"
    acct_id = status["AccountId"]
    assert len(acct_id) == 12 and acct_id.isdigit()

    ids = [a["Id"] for a in o.list_accounts()["Accounts"]]
    assert acct_id in ids


def test_organizations_describe_create_account_status_missing():
    o = _client_for("555555555552")
    with pytest.raises(ClientError) as exc:
        o.describe_create_account_status(CreateAccountRequestId="car-doesnotexist")
    assert exc.value.response["Error"]["Code"] == "CreateAccountStatusNotFoundException"


def test_organizations_move_account_and_placement():
    o = _client_for("555555555553")
    root_id = o.list_roots()["Roots"][0]["Id"]
    ou_id = o.create_organizational_unit(
        ParentId=root_id, Name="Security")["OrganizationalUnit"]["Id"]
    car = o.create_account(Email="sec@example.com", AccountName="sec")["CreateAccountStatus"]
    acct_id = o.describe_create_account_status(
        CreateAccountRequestId=car["Id"])["CreateAccountStatus"]["AccountId"]

    # Newly created account sits at the root.
    parents = o.list_parents(ChildId=acct_id)["Parents"]
    assert parents == [{"Id": root_id, "Type": "ROOT"}]

    o.move_account(AccountId=acct_id, SourceParentId=root_id, DestinationParentId=ou_id)

    parents = o.list_parents(ChildId=acct_id)["Parents"]
    assert parents[0]["Id"] == ou_id
    assert parents[0]["Type"] == "ORGANIZATIONAL_UNIT"

    accts = [a["Id"] for a in o.list_accounts_for_parent(ParentId=ou_id)["Accounts"]]
    assert acct_id in accts

    children = o.list_children(ParentId=ou_id, ChildType="ACCOUNT")["Children"]
    assert any(c["Id"] == acct_id and c["Type"] == "ACCOUNT" for c in children)

    ou_children = o.list_children(ParentId=root_id, ChildType="ORGANIZATIONAL_UNIT")["Children"]
    assert any(c["Id"] == ou_id for c in ou_children)


def test_organizations_move_account_errors():
    o = _client_for("555555555554")
    root_id = o.list_roots()["Roots"][0]["Id"]
    ou_id = o.create_organizational_unit(
        ParentId=root_id, Name="Ops")["OrganizationalUnit"]["Id"]
    car = o.create_account(Email="ops@example.com", AccountName="ops")["CreateAccountStatus"]
    acct_id = o.describe_create_account_status(
        CreateAccountRequestId=car["Id"])["CreateAccountStatus"]["AccountId"]

    with pytest.raises(ClientError) as exc:
        o.move_account(AccountId="999999999999", SourceParentId=root_id,
                       DestinationParentId=ou_id)
    assert exc.value.response["Error"]["Code"] == "AccountNotFoundException"

    # Wrong source parent (account is actually under the root).
    with pytest.raises(ClientError) as exc:
        o.move_account(AccountId=acct_id, SourceParentId=ou_id,
                       DestinationParentId=root_id)
    assert exc.value.response["Error"]["Code"] == "SourceParentNotFoundException"

    with pytest.raises(ClientError) as exc:
        o.move_account(AccountId=acct_id, SourceParentId=root_id,
                       DestinationParentId="ou-0000-doesnotexist")
    assert exc.value.response["Error"]["Code"] == "DestinationParentNotFoundException"


def test_organizations_invite_and_accept_pins_account():
    """The invite path lets a caller-chosen 12-digit id join the org."""
    master = _client_for("666666666661")
    hs = master.invite_account_to_organization(
        Target={"Id": "720000000001", "Type": "ACCOUNT"})["Handshake"]
    assert hs["State"] == "OPEN"
    assert hs["Id"].startswith("h-")

    member = _client_for("720000000001")
    accepted = member.accept_handshake(HandshakeId=hs["Id"])["Handshake"]
    assert accepted["State"] == "ACCEPTED"

    # The master's org now contains the pinned account.
    ids = [a["Id"] for a in master.list_accounts()["Accounts"]]
    assert "720000000001" in ids


def test_organizations_accept_handshake_wrong_caller():
    master = _client_for("666666666662")
    hs = master.invite_account_to_organization(
        Target={"Id": "720000000002", "Type": "ACCOUNT"})["Handshake"]
    intruder = _client_for("999999999998")
    with pytest.raises(ClientError) as exc:
        intruder.accept_handshake(HandshakeId=hs["Id"])
    assert exc.value.response["Error"]["Code"] == "AccountOwnerNotVerifiedException"


def test_organizations_list_handshakes_for_account():
    master = _client_for("666666666663")
    hs = master.invite_account_to_organization(
        Target={"Id": "720000000003", "Type": "ACCOUNT"})["Handshake"]
    member = _client_for("720000000003")
    listed = member.list_handshakes_for_account()["Handshakes"]
    assert any(h["Id"] == hs["Id"] for h in listed)
