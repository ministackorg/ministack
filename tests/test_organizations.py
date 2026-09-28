import os

import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566").rstrip("/")
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
        f"{ENDPOINT}/",
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


def test_organizations_list_responses_omit_empty_next_token(orgs):
    """Empty NextToken is omitted (null breaks non-boto pagination clients)."""
    orgs.describe_organization()
    assert "NextToken" not in orgs.list_roots()
    assert "NextToken" not in orgs.list_accounts()
    root_id = orgs.list_roots()["Roots"][0]["Id"]
    assert "NextToken" not in orgs.list_organizational_units_for_parent(ParentId=root_id)
    assert "NextToken" not in orgs.list_accounts_for_parent(ParentId=root_id)


# ---------------------------------------------------------------------------
# Accounts, SCPs and attachments: the surface Terraform's
# aws_organizations_account / _policy / _policy_attachment drive.
# ---------------------------------------------------------------------------

_SCP = "SERVICE_CONTROL_POLICY"
_DOC = '{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}'


def _policy(client, name, content=_DOC, **kw):
    return client.create_policy(Name=name, Description="d", Type=_SCP,
                                Content=content, **kw)["Policy"]


def test_organizations_root_has_scps_enabled_and_full_access_attached():
    """An ALL-features org has SCPs on its root and the AWS-managed
    FullAWSAccess policy, so a plan that reads them never sees an empty list."""
    o = _client_for("110000000001")
    root = o.list_roots()["Roots"][0]
    assert {"Type": _SCP, "Status": "ENABLED"} in root["PolicyTypes"]
    managed = [p for p in o.list_policies(Filter=_SCP)["Policies"]
               if p["Id"] == "p-FullAWSAccess"]
    assert managed and managed[0]["AwsManaged"] is True
    assert root["Id"] in [t["TargetId"]
                          for t in o.list_targets_for_policy(PolicyId="p-FullAWSAccess")["Targets"]]


def test_organizations_create_account_is_readable_through_its_request_id():
    """CreateAccount answers only with a status, so the id arrives through
    DescribeCreateAccountStatus, which is how the provider learns it."""
    o = _client_for("110000000002")
    status = o.create_account(Email="dev@example.com", AccountName="dev")["CreateAccountStatus"]
    assert status["Id"].startswith("car-")
    assert status["State"] == "SUCCEEDED"
    account_id = status["AccountId"]
    assert len(account_id) == 12 and account_id.isdigit()

    again = o.describe_create_account_status(
        CreateAccountRequestId=status["Id"])["CreateAccountStatus"]
    assert again["AccountId"] == account_id
    account = o.describe_account(AccountId=account_id)["Account"]
    assert account["Name"] == "dev"
    assert account["Email"] == "dev@example.com"
    assert account["JoinedMethod"] == "CREATED"
    with pytest.raises(ClientError) as exc:
        o.describe_create_account_status(CreateAccountRequestId="car-deadbeef")
    assert exc.value.response["Error"]["Code"] == "CreateAccountStatusNotFoundException"


def test_organizations_account_moves_between_parents_and_closes():
    o = _client_for("110000000003")
    root = o.list_roots()["Roots"][0]["Id"]
    ou = o.create_organizational_unit(ParentId=root, Name="Workloads")["OrganizationalUnit"]
    account_id = o.create_account(
        Email="m@example.com", AccountName="m")["CreateAccountStatus"]["AccountId"]

    o.move_account(AccountId=account_id, SourceParentId=root, DestinationParentId=ou["Id"])
    assert o.list_parents(ChildId=account_id)["Parents"][0]["Id"] == ou["Id"]
    assert account_id in [a["Id"] for a in o.list_accounts_for_parent(ParentId=ou["Id"])["Accounts"]]

    o.close_account(AccountId=account_id)
    assert o.describe_account(AccountId=account_id)["Account"]["Status"] == "SUSPENDED"
    with pytest.raises(ClientError) as exc:
        o.close_account(AccountId=account_id)
    assert exc.value.response["Error"]["Code"] == "AccountAlreadyClosedException"


def test_organizations_policy_crud_round_trips_its_document():
    o = _client_for("110000000004")
    created = _policy(o, "deny-all")
    policy_id = created["PolicySummary"]["Id"]
    assert policy_id.startswith("p-")
    assert created["PolicySummary"]["AwsManaged"] is False
    assert created["Content"] == _DOC

    o.update_policy(PolicyId=policy_id, Description="updated")
    described = o.describe_policy(PolicyId=policy_id)["Policy"]
    assert described["PolicySummary"]["Description"] == "updated"
    assert described["Content"] == _DOC

    o.delete_policy(PolicyId=policy_id)
    with pytest.raises(ClientError) as exc:
        o.describe_policy(PolicyId=policy_id)
    assert exc.value.response["Error"]["Code"] == "PolicyNotFoundException"


def test_organizations_policy_attaches_and_detaches_from_a_target():
    o = _client_for("110000000005")
    root = o.list_roots()["Roots"][0]["Id"]
    ou = o.create_organizational_unit(ParentId=root, Name="Restricted")["OrganizationalUnit"]
    policy_id = _policy(o, "deny-root")["PolicySummary"]["Id"]

    o.attach_policy(PolicyId=policy_id, TargetId=ou["Id"])
    assert policy_id in [p["Id"] for p in o.list_policies_for_target(
        TargetId=ou["Id"], Filter=_SCP)["Policies"]]
    target = [t for t in o.list_targets_for_policy(PolicyId=policy_id)["Targets"]
              if t["TargetId"] == ou["Id"]][0]
    assert target["Type"] == "ORGANIZATIONAL_UNIT"
    assert target["Name"] == "Restricted"

    with pytest.raises(ClientError) as exc:
        o.attach_policy(PolicyId=policy_id, TargetId=ou["Id"])
    assert exc.value.response["Error"]["Code"] == "DuplicatePolicyAttachmentException"
    with pytest.raises(ClientError) as exc:
        o.delete_policy(PolicyId=policy_id)
    assert exc.value.response["Error"]["Code"] == "PolicyInUseException"

    o.detach_policy(PolicyId=policy_id, TargetId=ou["Id"])
    assert o.list_policies_for_target(TargetId=ou["Id"], Filter=_SCP)["Policies"] == []
    with pytest.raises(ClientError) as exc:
        o.detach_policy(PolicyId=policy_id, TargetId=ou["Id"])
    assert exc.value.response["Error"]["Code"] == "PolicyNotAttachedException"
    o.delete_policy(PolicyId=policy_id)


def test_organizations_create_policy_validates_its_input():
    o = _client_for("110000000006")
    with pytest.raises(ClientError) as exc:
        o.create_policy(Name="bad", Description="d", Type=_SCP, Content="not json")
    assert exc.value.response["Error"]["Code"] == "MalformedPolicyDocumentException"
    _policy(o, "dupe")
    with pytest.raises(ClientError) as exc:
        _policy(o, "dupe")
    assert exc.value.response["Error"]["Code"] == "DuplicatePolicyException"
    with pytest.raises(ClientError) as exc:
        o.update_policy(PolicyId="p-FullAWSAccess", Description="mine")
    assert exc.value.response["Error"]["Code"] == "AccessDeniedException"


def test_organizations_attach_requires_the_policy_type_enabled_on_the_root():
    o = _client_for("110000000007")
    root = o.list_roots()["Roots"][0]["Id"]
    policy_id = _policy(o, "scp")["PolicySummary"]["Id"]
    o.disable_policy_type(RootId=root, PolicyType=_SCP)
    with pytest.raises(ClientError) as exc:
        o.attach_policy(PolicyId=policy_id, TargetId=root)
    assert exc.value.response["Error"]["Code"] == "PolicyTypeNotEnabledException"

    updated = o.enable_policy_type(RootId=root, PolicyType=_SCP)["Root"]
    assert {"Type": _SCP, "Status": "ENABLED"} in updated["PolicyTypes"]
    o.attach_policy(PolicyId=policy_id, TargetId=root)
    with pytest.raises(ClientError) as exc:
        o.enable_policy_type(RootId=root, PolicyType=_SCP)
    assert exc.value.response["Error"]["Code"] == "PolicyTypeAlreadyEnabledException"


def test_organizations_policies_are_account_scoped():
    a, b = _client_for("110000000008"), _client_for("110000000009")
    policy_id = _policy(a, "tenant-a")["PolicySummary"]["Id"]
    assert policy_id not in [p["Id"] for p in b.list_policies(Filter=_SCP)["Policies"]]
    with pytest.raises(ClientError) as exc:
        b.describe_policy(PolicyId=policy_id)
    assert exc.value.response["Error"]["Code"] == "PolicyNotFoundException"
