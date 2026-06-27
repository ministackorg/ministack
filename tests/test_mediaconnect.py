"""
Integration tests for AWS Elemental MediaConnect emulator (control-plane stub).
"""
import uuid

import boto3
import pytest
from botocore.exceptions import ClientError

ENDPOINT = "http://localhost:4566"
REGION = "us-east-1"


@pytest.fixture(scope="module")
def mc():
    return boto3.client("mediaconnect", endpoint_url=ENDPOINT,
                        aws_access_key_id="test", aws_secret_access_key="test",
                        region_name=REGION)


def _uid():
    return uuid.uuid4().hex[:8]


def _basic_source(name="src1"):
    return {
        "Name": name,
        "Protocol": "rist",
        "WhitelistCidr": "0.0.0.0/0",
        "IngestPort": 5000,
    }


# ---------------------------------------------------------------------------
# CreateFlow / DescribeFlow / ListFlows
# ---------------------------------------------------------------------------

def test_mediaconnect_create_describe_flow(mc):
    name = f"flow-{_uid()}"
    resp = mc.create_flow(
        Name=name,
        Source=_basic_source(),
        AvailabilityZone=f"{REGION}a",
    )
    flow = resp["Flow"]
    assert flow["Name"] == name
    assert flow["FlowArn"].startswith(
        f"arn:aws:mediaconnect:{REGION}:")
    assert ":flow:" in flow["FlowArn"]
    assert flow["FlowArn"].endswith(f":{name}")
    assert flow["Status"] == "STANDBY"
    assert flow["Source"]["Name"] == "src1"

    desc = mc.describe_flow(FlowArn=flow["FlowArn"])["Flow"]
    assert desc["FlowArn"] == flow["FlowArn"]
    assert desc["Name"] == name


def test_mediaconnect_describe_unknown_flow_404(mc):
    bogus = f"arn:aws:mediaconnect:{REGION}:000000000000:flow:{uuid.uuid4()}:nope"
    with pytest.raises(ClientError) as e:
        mc.describe_flow(FlowArn=bogus)
    assert e.value.response["Error"]["Code"] == "NotFoundException"


def test_mediaconnect_list_flows_uses_listed_projection(mc):
    name = f"flow-list-{_uid()}"
    created = mc.create_flow(
        Name=name, Source=_basic_source(),
    )["Flow"]
    flows = mc.list_flows()["Flows"]
    ours = next((f for f in flows if f["FlowArn"] == created["FlowArn"]),
                None)
    assert ours is not None
    # ListedFlow projection — slimmer than Flow. These keys must be present;
    # the heavy ones (Outputs / Sources / Entitlements) must NOT.
    assert ours["Name"] == name
    assert ours["Status"] == "STANDBY"
    assert ours["SourceType"] == "OWNED"
    assert "Outputs" not in ours
    assert "Sources" not in ours
    assert "Entitlements" not in ours


def test_mediaconnect_list_flows_source_type_entitled(mc):
    name = f"flow-ent-{_uid()}"
    created = mc.create_flow(
        Name=name,
        Source={
            "Name": "ent-src",
            "EntitlementArn": (
                f"arn:aws:mediaconnect:{REGION}:000000000000:"
                f"entitlement:{uuid.uuid4()}:ent"
            ),
        },
    )["Flow"]
    flows = mc.list_flows()["Flows"]
    ours = next((f for f in flows if f["FlowArn"] == created["FlowArn"]), None)
    assert ours is not None
    assert ours["SourceType"] == "ENTITLED"


# ---------------------------------------------------------------------------
# UpdateFlow
# ---------------------------------------------------------------------------

def test_mediaconnect_update_flow_patches_allowed_fields(mc):
    name = f"flow-upd-{_uid()}"
    flow = mc.create_flow(Name=name, Source=_basic_source())["Flow"]

    failover = {"State": "ENABLED", "RecoveryWindow": 3000}
    maintenance = {"MaintenanceDay": "Tuesday", "MaintenanceStartHour": "02:00"}
    resp = mc.update_flow(
        FlowArn=flow["FlowArn"],
        SourceFailoverConfig=failover,
        Maintenance=maintenance,
    )
    updated = resp["Flow"]
    assert updated["SourceFailoverConfig"]["State"] == "ENABLED"
    assert updated["Maintenance"]["MaintenanceDay"] == "Tuesday"

    # Re-describe to verify persistence.
    desc = mc.describe_flow(FlowArn=flow["FlowArn"])["Flow"]
    assert desc["SourceFailoverConfig"]["State"] == "ENABLED"
    assert desc["Maintenance"]["MaintenanceStartHour"] == "02:00"


def test_mediaconnect_update_unknown_flow_404(mc):
    bogus = f"arn:aws:mediaconnect:{REGION}:000000000000:flow:{uuid.uuid4()}:nope"
    with pytest.raises(ClientError) as e:
        mc.update_flow(
            FlowArn=bogus,
            Maintenance={"MaintenanceDay": "Sunday",
                         "MaintenanceStartHour": "01:00"},
        )
    assert e.value.response["Error"]["Code"] == "NotFoundException"


# ---------------------------------------------------------------------------
# ListTagsForResource
# ---------------------------------------------------------------------------

def test_mediaconnect_list_tags_for_created_flow_returns_empty_map(mc):
    name = f"flow-tags-{_uid()}"
    flow = mc.create_flow(Name=name, Source=_basic_source())["Flow"]

    resp = mc.list_tags_for_resource(ResourceArn=flow["FlowArn"])
    # AWS returns no Tags key when the map is empty; boto3 may include {} or omit.
    assert resp.get("Tags", {}) == {}


def test_mediaconnect_list_tags_unknown_resource_404(mc):
    bogus = f"arn:aws:mediaconnect:{REGION}:000000000000:flow:{uuid.uuid4()}:nope"
    with pytest.raises(ClientError) as e:
        mc.list_tags_for_resource(ResourceArn=bogus)
    assert e.value.response["Error"]["Code"] == "NotFoundException"


@pytest.mark.parametrize(
    ("arn", "code"),
    [
        ("not-an-arn", "BadRequestException"),
        ("arn:aws:sqs:us-east-1:000000000000:flow:abc:nope", "BadRequestException"),
        ("arn:aws:mediaconnect:us-west-2:000000000000:flow:abc:nope", "BadRequestException"),
    ],
)
def test_mediaconnect_list_tags_requires_local_flow_arn(mc, arn, code):
    with pytest.raises(ClientError) as e:
        mc.list_tags_for_resource(ResourceArn=arn)
    assert e.value.response["Error"]["Code"] == code
