"""AWS Resource Groups (resource-groups, 2017-11-27) tests.

Driven via boto3 to lock the wire format against the real
botocore service-2.json. Tag-sync operations are intentionally not
exercised — they aren't reachable through the AWS CLI or Terraform.
"""

import json
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


def _client(account="test"):
    return boto3.client(
        "resource-groups",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=account,
        aws_secret_access_key="test",
        config=Config(retries={"mode": "standard"}),
    )


@pytest.fixture(scope="module")
def rg():
    return _client()


def _uid():
    return uuid.uuid4().hex[:6]


def _tag_query(key="env", value="test"):
    return {
        "Type": "TAG_FILTERS_1_0",
        "Query": json.dumps({
            "ResourceTypeFilters": ["AWS::AllSupported"],
            "TagFilters": [{"Key": key, "Values": [value]}],
        }),
    }


# ---------------------------------------------------------------------------
# Group CRUD
# ---------------------------------------------------------------------------

def test_create_group_with_resource_query(rg):
    name = f"g-{_uid()}"
    resp = rg.create_group(Name=name, Description="x", ResourceQuery=_tag_query())
    assert resp["Group"]["Name"] == name
    assert resp["Group"]["GroupArn"].endswith(f":group/{name}")
    assert resp["ResourceQuery"]["Type"] == "TAG_FILTERS_1_0"
    rg.delete_group(Group=name)


def test_create_group_duplicate_rejected(rg):
    name = f"g-{_uid()}"
    rg.create_group(Name=name, ResourceQuery=_tag_query())
    try:
        with pytest.raises(ClientError) as exc:
            rg.create_group(Name=name, ResourceQuery=_tag_query())
        assert exc.value.response["Error"]["Code"] == "BadRequestException"
    finally:
        rg.delete_group(Group=name)


def test_create_group_invalid_query_type_rejected(rg):
    with pytest.raises(ClientError) as exc:
        rg.create_group(Name=f"g-{_uid()}", ResourceQuery={"Type": "INVALID", "Query": "{}"})
    assert exc.value.response["Error"]["Code"] == "BadRequestException"


def test_get_group_by_name_and_by_arn(rg):
    name = f"g-{_uid()}"
    arn = rg.create_group(Name=name, ResourceQuery=_tag_query())["Group"]["GroupArn"]
    try:
        by_name = rg.get_group(GroupName=name)
        by_arn = rg.get_group(Group=arn)
        assert by_name["Group"]["Name"] == name
        assert by_arn["Group"]["Name"] == name
    finally:
        rg.delete_group(Group=name)


def test_get_group_not_found(rg):
    with pytest.raises(ClientError) as exc:
        rg.get_group(GroupName=f"missing-{_uid()}")
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_update_group_fields_persist(rg):
    name = f"g-{_uid()}"
    rg.create_group(Name=name, ResourceQuery=_tag_query())
    try:
        rg.update_group(
            GroupName=name,
            Description="updated",
            Criticality=3,
            Owner="ops",
            DisplayName="Group X",
        )
        out = rg.get_group(GroupName=name)["Group"]
        assert out["Description"] == "updated"
        assert out["Criticality"] == 3
        assert out["Owner"] == "ops"
        assert out["DisplayName"] == "Group X"
    finally:
        rg.delete_group(Group=name)


def test_delete_group_returns_group_record(rg):
    name = f"g-{_uid()}"
    rg.create_group(Name=name, ResourceQuery=_tag_query())
    out = rg.delete_group(Group=name)
    assert out["Group"]["Name"] == name
    with pytest.raises(ClientError):
        rg.get_group(GroupName=name)


def test_list_groups(rg):
    name = f"g-{_uid()}"
    rg.create_group(Name=name, ResourceQuery=_tag_query())
    try:
        out = rg.list_groups()
        names = [g["GroupName"] for g in out["GroupIdentifiers"]]
        assert name in names
    finally:
        rg.delete_group(Group=name)


# ---------------------------------------------------------------------------
# Group query + configuration
# ---------------------------------------------------------------------------

def test_get_and_update_group_query(rg):
    name = f"g-{_uid()}"
    rg.create_group(Name=name, ResourceQuery=_tag_query("env", "dev"))
    try:
        gq = rg.get_group_query(GroupName=name)["GroupQuery"]
        assert gq["GroupName"] == name
        assert gq["ResourceQuery"]["Type"] == "TAG_FILTERS_1_0"

        new_query = _tag_query("env", "prod")
        rg.update_group_query(GroupName=name, ResourceQuery=new_query)
        gq2 = rg.get_group_query(GroupName=name)["GroupQuery"]
        body = json.loads(gq2["ResourceQuery"]["Query"])
        assert body["TagFilters"][0]["Values"] == ["prod"]
    finally:
        rg.delete_group(Group=name)


def test_put_and_get_group_configuration(rg):
    name = f"g-{_uid()}"
    rg.create_group(Name=name, ResourceQuery=_tag_query())
    try:
        cfg = [{"Type": "AWS::ResourceGroups::Generic", "Parameters": [
            {"Name": "allowed-resource-types", "Values": ["AWS::EC2::Instance"]}
        ]}]
        rg.put_group_configuration(Group=name, Configuration=cfg)
        out = rg.get_group_configuration(Group=name)["GroupConfiguration"]
        assert out["Status"] == "UPDATE_COMPLETE"
        assert out["Configuration"][0]["Type"] == "AWS::ResourceGroups::Generic"
    finally:
        rg.delete_group(Group=name)


# ---------------------------------------------------------------------------
# Group membership
# ---------------------------------------------------------------------------

def test_group_and_ungroup_and_list_resources(rg):
    name = f"g-{_uid()}"
    rg.create_group(Name=name, ResourceQuery=_tag_query())
    try:
        arns = [
            "arn:aws:ec2:us-east-1:000000000000:instance/i-aaa",
            "arn:aws:ec2:us-east-1:000000000000:instance/i-bbb",
            "arn:aws:s3:::some-bucket",
        ]
        out = rg.group_resources(Group=name, ResourceArns=arns)
        assert sorted(out["Succeeded"]) == sorted(arns)
        assert out["Failed"] == []

        listed = rg.list_group_resources(GroupName=name)
        listed_arns = [r["ResourceArn"] for r in listed["ResourceIdentifiers"]]
        assert sorted(listed_arns) == sorted(arns)
        for r in listed["Resources"]:
            assert r["Status"]["Name"] == "ACTIVE"

        # resource-type filter
        ec2_only = rg.list_group_resources(
            GroupName=name,
            Filters=[{"Name": "resource-type", "Values": ["AWS::EC2::Instance"]}],
        )
        assert all(
            r["ResourceType"] == "AWS::EC2::Instance"
            for r in ec2_only["ResourceIdentifiers"]
        )
        assert len(ec2_only["ResourceIdentifiers"]) == 2

        rg.ungroup_resources(Group=name, ResourceArns=[arns[0]])
        after = rg.list_group_resources(GroupName=name)
        after_arns = {r["ResourceArn"] for r in after["ResourceIdentifiers"]}
        assert arns[0] not in after_arns
        assert arns[1] in after_arns
    finally:
        rg.delete_group(Group=name)


def test_list_grouping_statuses(rg):
    name = f"g-{_uid()}"
    rg.create_group(Name=name, ResourceQuery=_tag_query())
    try:
        rg.group_resources(Group=name, ResourceArns=[
            "arn:aws:ec2:us-east-1:000000000000:instance/i-zzz"
        ])
        out = rg.list_grouping_statuses(Group=name)
        assert out["Group"].endswith(f":group/{name}")
        assert out["GroupingStatuses"][0]["Status"] == "SUCCESS"
        assert out["GroupingStatuses"][0]["Action"] == "GROUP"
    finally:
        rg.delete_group(Group=name)


def test_search_resources_returns_empty_with_round_tripped_pagination(rg):
    out = rg.search_resources(ResourceQuery=_tag_query(), MaxResults=10)
    assert out["ResourceIdentifiers"] == []
    assert out["QueryErrors"] == []


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def test_tag_untag_get_tags_lifecycle(rg):
    name = f"g-{_uid()}"
    arn = rg.create_group(Name=name, ResourceQuery=_tag_query())["Group"]["GroupArn"]
    try:
        rg.tag(Arn=arn, Tags={"env": "test", "owner": "ministack"})
        got = rg.get_tags(Arn=arn)
        assert got["Arn"] == arn
        assert got["Tags"] == {"env": "test", "owner": "ministack"}

        rg.untag(Arn=arn, Keys=["env"])
        got2 = rg.get_tags(Arn=arn)
        assert "env" not in got2["Tags"]
        assert got2["Tags"]["owner"] == "ministack"
    finally:
        rg.delete_group(Group=name)


def test_tag_unknown_arn_returns_404(rg):
    bogus = "arn:aws:resource-groups:us-east-1:000000000000:group/does-not-exist"
    with pytest.raises(ClientError) as exc:
        rg.tag(Arn=bogus, Tags={"k": "v"})
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


# ---------------------------------------------------------------------------
# Account settings
# ---------------------------------------------------------------------------

def test_get_and_update_account_settings(rg):
    out = rg.get_account_settings()
    assert "GroupLifecycleEventsDesiredStatus" in out["AccountSettings"]

    rg.update_account_settings(GroupLifecycleEventsDesiredStatus="ACTIVE")
    out2 = rg.get_account_settings()
    assert out2["AccountSettings"]["GroupLifecycleEventsDesiredStatus"] == "ACTIVE"

    # reset to INACTIVE so other tests/modules see a clean default
    rg.update_account_settings(GroupLifecycleEventsDesiredStatus="INACTIVE")


def test_update_account_settings_invalid_value_rejected(rg):
    with pytest.raises(ClientError) as exc:
        rg.update_account_settings(GroupLifecycleEventsDesiredStatus="MAYBE")
    assert exc.value.response["Error"]["Code"] == "BadRequestException"


# ---------------------------------------------------------------------------
# Multi-tenancy
# ---------------------------------------------------------------------------

def test_account_isolation_for_groups():
    a = _client("111111111111")
    b = _client("222222222222")
    name = f"acct-{_uid()}"
    a.create_group(Name=name, ResourceQuery=_tag_query())
    try:
        a_names = {g["GroupName"] for g in a.list_groups()["GroupIdentifiers"]}
        b_names = {g["GroupName"] for g in b.list_groups()["GroupIdentifiers"]}
        assert name in a_names
        assert name not in b_names
        with pytest.raises(ClientError) as exc:
            b.get_group(GroupName=name)
        assert exc.value.response["Error"]["Code"] == "NotFoundException"
    finally:
        a.delete_group(Group=name)
