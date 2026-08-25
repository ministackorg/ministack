import ipaddress
import json
import os
import time
import urllib.error
import urllib.request
import uuid as _uuid_mod
from urllib.parse import urlencode

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _ec2_client(region, account_id="test"):
    return boto3.client(
        "ec2",
        endpoint_url=ENDPOINT,
        region_name=region,
        aws_access_key_id=account_id,
        aws_secret_access_key="test",
        config=Config(region_name=region),
    )


def _create_ec2_iam_instance_profile(iam, suffix):
    assume = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    role_name = f"ec2-ip-role-{suffix}"
    profile_name = f"ec2-ip-{suffix}"
    iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=assume)
    profile = iam.create_instance_profile(
        InstanceProfileName=profile_name
    )["InstanceProfile"]
    iam.add_role_to_instance_profile(
        InstanceProfileName=profile_name,
        RoleName=role_name,
    )
    return role_name, profile_name, profile


def _delete_ec2_iam_instance_profile(iam, role_name, profile_name):
    try:
        iam.remove_role_from_instance_profile(
            InstanceProfileName=profile_name,
            RoleName=role_name,
        )
    except ClientError:
        pass
    try:
        iam.delete_instance_profile(InstanceProfileName=profile_name)
    except ClientError:
        pass
    try:
        iam.delete_role(RoleName=role_name)
    except ClientError:
        pass


def test_ec2_describe_vpcs_default(ec2):
    resp = ec2.describe_vpcs()
    vpcs = resp["Vpcs"]
    assert any(v["IsDefault"] for v in vpcs)

def test_ec2_describe_subnets_default(ec2):
    resp = ec2.describe_subnets()
    assert len(resp["Subnets"]) >= 1

def test_ec2_describe_availability_zones(ec2):
    resp = ec2.describe_availability_zones()
    azs = [az["ZoneName"] for az in resp["AvailabilityZones"]]
    assert any("us-east-1" in az for az in azs)


def test_ec2_availability_zone_ids_are_region_coded(ec2):
    """ZoneId is the account-stable id ("use1-az1"), never the account-local ZoneName: code
    handed an id has to resolve it here, and an id that equals the name hides that requirement
    until it runs against real AWS."""
    zones = ec2.describe_availability_zones()["AvailabilityZones"]
    assert [z["ZoneId"] for z in zones] == ["use1-az1", "use1-az2", "use1-az3"]
    assert all(z["ZoneId"] != z["ZoneName"] for z in zones)
    assert [z["ZoneName"] for z in zones] == ["us-east-1a", "us-east-1b", "us-east-1c"]
    assert all(z["RegionName"] == "us-east-1" and z["State"] == "available" for z in zones)

    # The prefix follows AWS's own coding, including the compound directions.
    from ministack.services.ec2 import _az_id_prefix
    assert _az_id_prefix("eu-central-1") == "euc1"
    assert _az_id_prefix("ap-southeast-2") == "apse2"
    assert _az_id_prefix("ap-northeast-1") == "apne1"
    assert _az_id_prefix("us-west-2") == "usw2"
    assert _az_id_prefix("ca-central-1") == "cac1"
    assert _az_id_prefix("sa-east-1") == "sae1"
    # A partitioned region keeps every part between the geography and the index.
    assert _az_id_prefix("us-gov-west-1") == "usgw1"
    assert _az_id_prefix("cn-northwest-1") == "cnnw1"


def test_ec2_availability_zones_carry_group_and_opt_in(ec2):
    """GroupName / NetworkBorderGroup / OptInStatus are optional members, so leaving
    them out reads to an SDK as "no value" rather than as an error: a caller filtering
    on opt_in_status or grouping by group_names gets a wrong answer, not a failure."""
    zones = ec2.describe_availability_zones()["AvailabilityZones"]
    assert all(z["GroupName"] == "us-east-1-zg-1" for z in zones)
    assert all(z["NetworkBorderGroup"] == "us-east-1" for z in zones)
    assert all(z["OptInStatus"] == "opt-in-not-required" for z in zones)


def test_ec2_describe_regions_returns_commercial_regions(ec2):
    """DescribeRegions must list at least the four legacy us-* regions
    with opt-in-not-required, and emit the shape AWS returns."""
    resp = ec2.describe_regions()
    regions = {r["RegionName"]: r for r in resp["Regions"]}
    for name in ("us-east-1", "us-east-2", "us-west-1", "us-west-2"):
        assert name in regions
        assert regions[name]["OptInStatus"] == "opt-in-not-required"
        assert regions[name]["Endpoint"] == f"ec2.{name}.amazonaws.com"


def test_ec2_describe_regions_with_filter(ec2):
    resp = ec2.describe_regions(RegionNames=["us-east-1", "eu-west-1"])
    names = {r["RegionName"] for r in resp["Regions"]}
    assert names == {"us-east-1", "eu-west-1"}


def test_ec2_describe_regions_all_regions_includes_opt_in(ec2):
    base = len(ec2.describe_regions()["Regions"])
    full = len(ec2.describe_regions(AllRegions=True)["Regions"])
    assert full == base
    assert base >= 30


def test_ec2_default_network_resources_are_region_scoped():
    east = _ec2_client("us-east-1")
    west = _ec2_client("us-west-2")

    east_vpcs = east.describe_vpcs(
        Filters=[{"Name": "is-default", "Values": ["true"]}]
    )["Vpcs"]
    west_vpcs = west.describe_vpcs(
        Filters=[{"Name": "is-default", "Values": ["true"]}]
    )["Vpcs"]

    assert len(east_vpcs) == 1
    assert len(west_vpcs) == 1

    east_subnets = east.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [east_vpcs[0]["VpcId"]]}]
    )["Subnets"]
    west_subnets = west.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [west_vpcs[0]["VpcId"]]}]
    )["Subnets"]
    west_default_sgs = west.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [west_vpcs[0]["VpcId"]]},
            {"Name": "group-name", "Values": ["default"]},
        ]
    )["SecurityGroups"]

    assert {s["AvailabilityZone"] for s in east_subnets} == {
        "us-east-1a",
        "us-east-1b",
        "us-east-1c",
    }
    assert {s["AvailabilityZone"] for s in west_subnets} == {
        "us-west-2a",
        "us-west-2b",
        "us-west-2c",
    }
    assert len(west_default_sgs) == 1
    assert west_default_sgs[0]["VpcId"] == west_vpcs[0]["VpcId"]


def test_ec2_named_resources_and_tags_are_region_scoped():
    east = _ec2_client("us-east-1")
    west = _ec2_client("us-west-2")
    suffix = _uuid_mod.uuid4().hex[:8]
    key_name = f"qa-ec2-key-region-{suffix}"
    pg_name = f"qa-ec2-pg-region-{suffix}"
    sg_name = f"qa-ec2-sg-region-{suffix}"

    east.create_key_pair(KeyName=key_name)
    west.create_key_pair(KeyName=key_name)
    east.create_placement_group(GroupName=pg_name, Strategy="cluster")
    west.create_placement_group(GroupName=pg_name, Strategy="cluster")
    east_sg = east.create_security_group(GroupName=sg_name, Description="east")
    west_sg = west.create_security_group(GroupName=sg_name, Description="west")
    east.create_tags(Resources=[east_sg["GroupId"]], Tags=[{"Key": "Region", "Value": "east"}])
    west.create_tags(Resources=[west_sg["GroupId"]], Tags=[{"Key": "Region", "Value": "west"}])
    east_instance = east.run_instances(
        ImageId="ami-00000000",
        MinCount=1,
        MaxCount=1,
        InstanceType="t2.micro",
    )["Instances"][0]

    try:
        east_key = east.describe_key_pairs(KeyNames=[key_name])["KeyPairs"][0]
        west_key = west.describe_key_pairs(KeyNames=[key_name])["KeyPairs"][0]
        east_pg = east.describe_placement_groups(GroupNames=[pg_name])["PlacementGroups"][0]
        west_pg = west.describe_placement_groups(GroupNames=[pg_name])["PlacementGroups"][0]
        east_group = east.describe_security_groups(GroupIds=[east_sg["GroupId"]])[
            "SecurityGroups"
        ][0]
        west_group = west.describe_security_groups(GroupIds=[west_sg["GroupId"]])[
            "SecurityGroups"
        ][0]
        east_instances = east.describe_instances(InstanceIds=[east_instance["InstanceId"]])[
            "Reservations"
        ][0]["Instances"]

        assert east_key["KeyPairId"] != west_key["KeyPairId"]
        assert ":us-east-1:" in east_pg["GroupArn"]
        assert ":us-west-2:" in west_pg["GroupArn"]
        assert east_group["Description"] == "east"
        assert west_group["Description"] == "west"
        assert east_instances[0]["InstanceId"] == east_instance["InstanceId"]
        assert east.describe_tags(
            Filters=[{"Name": "resource-id", "Values": [east_sg["GroupId"]]}]
        )["Tags"][0]["Value"] == "east"
        assert west.describe_tags(
            Filters=[{"Name": "resource-id", "Values": [west_sg["GroupId"]]}]
        )["Tags"][0]["Value"] == "west"
        with pytest.raises(ClientError):
            east.describe_security_groups(GroupIds=[west_sg["GroupId"]])
        with pytest.raises(ClientError):
            west.describe_instances(InstanceIds=[east_instance["InstanceId"]])
    finally:
        try:
            east.terminate_instances(InstanceIds=[east_instance["InstanceId"]])
        except ClientError:
            pass
        for client, sg_id in ((east, east_sg["GroupId"]), (west, west_sg["GroupId"])):
            try:
                client.delete_security_group(GroupId=sg_id)
            except ClientError:
                pass
        for client in (east, west):
            try:
                client.delete_key_pair(KeyName=key_name)
            except ClientError:
                pass
            try:
                client.delete_placement_group(GroupName=pg_name)
            except ClientError:
                pass


def test_ec2_run_describe_terminate_instances(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1, InstanceType="t2.micro")
    assert len(resp["Instances"]) == 1
    instance_id = resp["Instances"][0]["InstanceId"]
    assert instance_id.startswith("i-")
    assert resp["Instances"][0]["State"]["Name"] == "running"

    desc = ec2.describe_instances(InstanceIds=[instance_id])
    assert len(desc["Reservations"]) == 1
    assert desc["Reservations"][0]["Instances"][0]["InstanceId"] == instance_id

    term = ec2.terminate_instances(InstanceIds=[instance_id])
    assert term["TerminatingInstances"][0]["CurrentState"]["Name"] == "terminated"


def test_ec2_run_instances_honors_private_ip_and_iam_profile(ec2):
    """RunInstances must reflect --private-ip-address and --iam-instance-profile
    on the created instance and in DescribeInstances. Regression for issue #594."""
    profile_arn = "arn:aws:iam::000000000000:instance-profile/ec2-test-profile"
    resp = ec2.run_instances(
        ImageId="ami-12345678",
        MinCount=1,
        MaxCount=1,
        InstanceType="t3.micro",
        PrivateIpAddress="172.31.0.42",
        IamInstanceProfile={"Arn": profile_arn},
    )
    inst = resp["Instances"][0]
    iid = inst["InstanceId"]
    try:
        # PrivateIpAddress is the value the caller passed, not a random one,
        # and is well-formed (4 octets ≤ 255).
        assert inst["PrivateIpAddress"] == "172.31.0.42"
        assert all(0 <= int(o) <= 255 for o in inst["PrivateIpAddress"].split("."))
        # PrivateDnsName is derived from the assigned IP, not malformed.
        assert inst["PrivateDnsName"] == "ip-172-31-0-42.ec2.internal"
        # IamInstanceProfile is attached and surfaced.
        assert inst["IamInstanceProfile"]["Arn"] == profile_arn
        assert inst["IamInstanceProfile"]["Id"]

        # DescribeInstances surfaces the same.
        desc = ec2.describe_instances(InstanceIds=[iid])
        d_inst = desc["Reservations"][0]["Instances"][0]
        assert d_inst["PrivateIpAddress"] == "172.31.0.42"
        assert d_inst["IamInstanceProfile"]["Arn"] == profile_arn
    finally:
        ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_run_instances_with_real_iam_profile_creates_association(iam, ec2):
    suffix = f"launch-{_uuid_mod.uuid4().hex[:8]}"
    role_name, profile_name, profile = _create_ec2_iam_instance_profile(iam, suffix)
    resp = ec2.run_instances(
        ImageId="ami-12345678",
        MinCount=1,
        MaxCount=1,
        InstanceType="t3.micro",
        IamInstanceProfile={"Name": profile_name},
    )
    inst = resp["Instances"][0]
    iid = inst["InstanceId"]

    try:
        assert inst["IamInstanceProfile"]["Arn"] == profile["Arn"]
        assert inst["IamInstanceProfile"]["Id"] == profile["InstanceProfileId"]

        described = ec2.describe_iam_instance_profile_associations(
            Filters=[{"Name": "instance-id", "Values": [iid]}]
        )["IamInstanceProfileAssociations"]
        assert len(described) == 1
        assoc = described[0]
        assert assoc["AssociationId"].startswith("iip-assoc-")
        assert assoc["InstanceId"] == iid
        assert assoc["State"] == "associated"
        assert assoc["IamInstanceProfile"]["Arn"] == profile["Arn"]
        assert assoc["IamInstanceProfile"]["Id"] == profile["InstanceProfileId"]

        ec2.terminate_instances(InstanceIds=[iid])

        active = ec2.describe_iam_instance_profile_associations(
            Filters=[
                {"Name": "instance-id", "Values": [iid]},
                {"Name": "state", "Values": ["associated"]},
            ]
        )["IamInstanceProfileAssociations"]
        assert active == []

        all_states = ec2.describe_iam_instance_profile_associations(
            AssociationIds=[assoc["AssociationId"]]
        )["IamInstanceProfileAssociations"]
        assert len(all_states) == 1
        assert all_states[0]["State"] == "disassociated"
    finally:
        try:
            ec2.terminate_instances(InstanceIds=[iid])
        except ClientError:
            pass
        _delete_ec2_iam_instance_profile(iam, role_name, profile_name)


def test_ec2_iam_instance_profile_association_lifecycle(iam, ec2):
    suffix = _uuid_mod.uuid4().hex[:8]
    role_a, profile_a_name, profile_a = _create_ec2_iam_instance_profile(
        iam, f"a-{suffix}"
    )
    role_b, profile_b_name, profile_b = _create_ec2_iam_instance_profile(
        iam, f"b-{suffix}"
    )
    resp = ec2.run_instances(ImageId="ami-12345678", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    try:
        assoc = ec2.associate_iam_instance_profile(
            InstanceId=iid,
            IamInstanceProfile={"Name": profile_a_name},
        )["IamInstanceProfileAssociation"]
        assert assoc["State"] == "associated"
        assert assoc["IamInstanceProfile"]["Arn"] == profile_a["Arn"]
        assert assoc["IamInstanceProfile"]["Id"] == profile_a["InstanceProfileId"]

        all_assocs = ec2.describe_iam_instance_profile_associations()[
            "IamInstanceProfileAssociations"
        ]
        assert any(
            item["AssociationId"] == assoc["AssociationId"] for item in all_assocs
        )

        by_instance = ec2.describe_iam_instance_profile_associations(
            Filters=[{"Name": "instance-id", "Values": [iid]}]
        )["IamInstanceProfileAssociations"]
        assert len(by_instance) == 1
        assert by_instance[0]["AssociationId"] == assoc["AssociationId"]

        replaced = ec2.replace_iam_instance_profile_association(
            AssociationId=assoc["AssociationId"],
            IamInstanceProfile={"Arn": profile_b["Arn"]},
        )["IamInstanceProfileAssociation"]
        assert replaced["AssociationId"] == assoc["AssociationId"]
        assert replaced["State"] == "associated"
        assert replaced["IamInstanceProfile"]["Arn"] == profile_b["Arn"]
        assert replaced["IamInstanceProfile"]["Id"] == profile_b["InstanceProfileId"]

        inst = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0]
        assert inst["IamInstanceProfile"]["Arn"] == profile_b["Arn"]
        assert inst["IamInstanceProfile"]["Id"] == profile_b["InstanceProfileId"]

        disassoc = ec2.disassociate_iam_instance_profile(
            AssociationId=assoc["AssociationId"]
        )["IamInstanceProfileAssociation"]
        assert disassoc["AssociationId"] == assoc["AssociationId"]
        assert disassoc["State"] == "disassociated"

        active = ec2.describe_iam_instance_profile_associations(
            Filters=[
                {"Name": "association-id", "Values": [assoc["AssociationId"]]},
                {"Name": "state", "Values": ["associated"]},
            ]
        )["IamInstanceProfileAssociations"]
        assert active == []

        described = ec2.describe_iam_instance_profile_associations(
            AssociationIds=[assoc["AssociationId"]]
        )["IamInstanceProfileAssociations"]
        assert len(described) == 1
        assert described[0]["State"] == "disassociated"

        inst = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0]
        assert "IamInstanceProfile" not in inst or not inst["IamInstanceProfile"]
    finally:
        try:
            ec2.terminate_instances(InstanceIds=[iid])
        except ClientError:
            pass
        _delete_ec2_iam_instance_profile(iam, role_a, profile_a_name)
        _delete_ec2_iam_instance_profile(iam, role_b, profile_b_name)


def test_ec2_run_instances_default_private_ip_is_well_formed(ec2):
    """When the caller does not pass --private-ip-address, the auto-generated
    address must still be a valid IPv4. Regression for the 10.0193.216 bug
    in #594 (missing dot separator in _random_ip prefix)."""
    resp = ec2.run_instances(ImageId="ami-1", MinCount=1, MaxCount=1)
    inst = resp["Instances"][0]
    try:
        octets = inst["PrivateIpAddress"].split(".")
        assert len(octets) == 4, f"malformed IP: {inst['PrivateIpAddress']}"
        assert all(0 <= int(o) <= 255 for o in octets)
    finally:
        ec2.terminate_instances(InstanceIds=[inst["InstanceId"]])


def test_ec2_run_instances_source_dest_check_defaults_true(ec2):
    resp = ec2.run_instances(ImageId="ami-12345678", MinCount=1, MaxCount=1)
    inst = resp["Instances"][0]
    iid = inst["InstanceId"]
    try:
        assert inst["SourceDestCheck"] is True
        described = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0]
        assert described["SourceDestCheck"] is True
    finally:
        ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_run_instances_emits_default_root_block_device_mapping(ec2):
    """RunInstances without an explicit BDM must auto-attach a root EBS volume,
    matching real AWS. Cloud Custodian / AWS Config use BlockDeviceMappings to
    classify EBS-backed vs ephemeral instances; a missing BDM breaks them."""
    resp = ec2.run_instances(
        ImageId="ami-12345678", MinCount=1, MaxCount=1, InstanceType="t2.micro"
    )
    iid = resp["Instances"][0]["InstanceId"]

    bdms = resp["Instances"][0].get("BlockDeviceMappings")
    assert bdms, "RunInstances response must include BlockDeviceMappings"
    assert len(bdms) == 1
    root = bdms[0]
    assert root["DeviceName"] == "/dev/xvda"
    ebs = root["Ebs"]
    assert ebs["VolumeId"].startswith("vol-")
    assert ebs["Status"] == "attached"
    assert ebs["DeleteOnTermination"] is True
    assert "AttachTime" in ebs

    desc = ec2.describe_instances(InstanceIds=[iid])
    inst = desc["Reservations"][0]["Instances"][0]
    desc_bdms = inst.get("BlockDeviceMappings", [])
    assert len(desc_bdms) == 1
    assert desc_bdms[0]["DeviceName"] == "/dev/xvda"
    assert desc_bdms[0]["Ebs"]["VolumeId"] == ebs["VolumeId"]
    assert desc_bdms[0]["Ebs"]["Status"] == "attached"

    vols = ec2.describe_volumes(VolumeIds=[ebs["VolumeId"]])["Volumes"]
    assert len(vols) == 1
    atts = vols[0]["Attachments"]
    assert atts and atts[0]["InstanceId"] == iid
    assert atts[0]["Device"] == "/dev/xvda"

def test_ec2_describe_instance_status(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1, InstanceType="t2.micro")
    iid = resp["Instances"][0]["InstanceId"]

    # Running instance should appear by default
    status = ec2.describe_instance_status(InstanceIds=[iid])
    assert len(status["InstanceStatuses"]) == 1
    s = status["InstanceStatuses"][0]
    assert s["InstanceId"] == iid
    assert s["InstanceState"]["Name"] == "running"
    assert s["SystemStatus"]["Status"] == "ok"
    assert s["InstanceStatus"]["Status"] == "ok"

    # Stopped instance should NOT appear without IncludeAllInstances
    ec2.stop_instances(InstanceIds=[iid])
    status2 = ec2.describe_instance_status(InstanceIds=[iid])
    assert len(status2["InstanceStatuses"]) == 0

    # With IncludeAllInstances=True it should appear
    status3 = ec2.describe_instance_status(InstanceIds=[iid], IncludeAllInstances=True)
    assert len(status3["InstanceStatuses"]) == 1
    assert status3["InstanceStatuses"][0]["InstanceState"]["Name"] == "stopped"

    ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_stop_start_instances(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    stop = ec2.stop_instances(InstanceIds=[iid])
    assert stop["StoppingInstances"][0]["CurrentState"]["Name"] == "stopped"

    start = ec2.start_instances(InstanceIds=[iid])
    assert start["StartingInstances"][0]["CurrentState"]["Name"] == "running"

    ec2.terminate_instances(InstanceIds=[iid])

def test_ec2_run_multiple_instances(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=3, MaxCount=3)
    assert len(resp["Instances"]) == 3
    ids = [i["InstanceId"] for i in resp["Instances"]]
    assert len(set(ids)) == 3
    ec2.terminate_instances(InstanceIds=ids)

def test_ec2_describe_images(ec2):
    resp = ec2.describe_images(Owners=["self"])
    assert len(resp["Images"]) >= 1
    assert all("ImageId" in img for img in resp["Images"])


def test_ec2_describe_images_has_root_device_and_block_mappings(ec2):
    # Terraform's AWS provider resolves these before aws_instance creation;
    # absence produced "finding Root Device Name for AMI" and blocked apply.
    resp = ec2.describe_images(ImageIds=["ami-0abcdef1234567890"])
    img = resp["Images"][0]
    assert img["RootDeviceType"] == "ebs"
    assert img["RootDeviceName"] == "/dev/xvda"
    bdms = img["BlockDeviceMappings"]
    assert bdms and bdms[0]["DeviceName"] == "/dev/xvda"
    assert bdms[0]["Ebs"]["VolumeSize"] == 8
    assert bdms[0]["Ebs"]["VolumeType"] == "gp2"

    # Windows AMI uses /dev/sda1 and exposes Platform=windows.
    resp = ec2.describe_images(ImageIds=["ami-0fedcba9876543210"])
    win = resp["Images"][0]
    assert win["RootDeviceName"] == "/dev/sda1"
    assert win.get("Platform") == "windows"
    assert win["BlockDeviceMappings"][0]["DeviceName"] == "/dev/sda1"

def test_ec2_security_group_crud(ec2):
    created = ec2.create_security_group(GroupName="qa-ec2-sg", Description="test sg")
    sg_id = created["GroupId"]
    assert sg_id.startswith("sg-")
    assert created["SecurityGroupArn"] == f"arn:aws:ec2:us-east-1:000000000000:security-group/{sg_id}"

    desc = ec2.describe_security_groups(GroupIds=[sg_id])
    assert desc["SecurityGroups"][0]["GroupName"] == "qa-ec2-sg"
    assert desc["SecurityGroups"][0]["Description"] == "test sg"

    deleted = ec2.delete_security_group(GroupId=sg_id)
    assert deleted["Return"] is True
    assert deleted["GroupId"] == sg_id
    desc2 = ec2.describe_security_groups()
    assert not any(sg["GroupId"] == sg_id for sg in desc2["SecurityGroups"])

def test_ec2_security_group_duplicate(ec2):
    ec2.create_security_group(GroupName="qa-ec2-sg-dup", Description="d")
    with pytest.raises(ClientError) as exc:
        ec2.create_security_group(GroupName="qa-ec2-sg-dup", Description="d")
    assert exc.value.response["Error"]["Code"] == "InvalidGroup.Duplicate"


def test_ec2_describe_security_groups_malformed_id(ec2):
    with pytest.raises(ClientError) as exc:
        ec2.describe_security_groups(GroupIds=["sg-0123456789abcdef0"])

    error = exc.value.response["Error"]
    assert error["Code"] == "InvalidGroupId.Malformed"
    assert error["Message"] == 'Invalid id: "sg-0123456789abcdef0"'


def test_ec2_sg_authorize_revoke_ingress(ec2):
    sg_id = ec2.create_security_group(GroupName="qa-ec2-sg-rules", Description="rules test")["GroupId"]

    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 80,
                "ToPort": 80,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )
    desc = ec2.describe_security_groups(GroupIds=[sg_id])
    perms = desc["SecurityGroups"][0]["IpPermissions"]
    assert any(p["FromPort"] == 80 for p in perms)

    ec2.revoke_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 80,
                "ToPort": 80,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )
    desc2 = ec2.describe_security_groups(GroupIds=[sg_id])
    assert not any(p.get("FromPort") == 80 for p in desc2["SecurityGroups"][0]["IpPermissions"])

    ec2.delete_security_group(GroupId=sg_id)


def test_ec2_revoke_security_group_egress_returns_revoked_rules(ec2):
    sg_id = ec2.create_security_group(GroupName="qa-ec2-sg-revoke-egress", Description="egress")["GroupId"]

    resp = ec2.revoke_security_group_egress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "-1",
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )

    assert resp["Return"] is True
    revoked = resp["RevokedSecurityGroupRules"]
    assert len(revoked) == 1
    assert revoked[0]["SecurityGroupRuleId"].startswith("sgr-")
    assert revoked[0]["GroupId"] == sg_id
    assert revoked[0]["IsEgress"] is True
    assert revoked[0]["IpProtocol"] == "-1"
    assert revoked[0]["CidrIpv4"] == "0.0.0.0/0"

    ec2.delete_security_group(GroupId=sg_id)


def test_ec2_revoke_sg_ingress_by_rule_id(ec2):
    """RevokeSecurityGroupIngress(SecurityGroupRuleIds=[...]) must remove exactly
    the addressed rules — previously the ids were ignored and the call returned
    Return=true while revoking nothing."""
    suffix = _uuid_mod.uuid4().hex[:8]
    sg_id = ec2.create_security_group(
        GroupName=f"qa-ec2-sg-revoke-by-id-{suffix}", Description="revoke by id")["GroupId"]

    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80,
             "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
            {"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
             "IpRanges": [{"CidrIp": "0.0.0.0/0"}]},
        ],
    )
    sgr = ec2.describe_security_group_rules(
        Filters=[{"Name": "group-id", "Values": [sg_id]}])["SecurityGroupRules"]
    rule_id = next(r["SecurityGroupRuleId"] for r in sgr
                   if not r["IsEgress"] and r["FromPort"] == 80)

    resp = ec2.revoke_security_group_ingress(GroupId=sg_id, SecurityGroupRuleIds=[rule_id])
    assert resp["Return"] is True
    revoked = resp["RevokedSecurityGroupRules"]
    assert len(revoked) == 1
    assert revoked[0]["SecurityGroupRuleId"] == rule_id
    assert revoked[0]["GroupId"] == sg_id
    assert revoked[0]["IsEgress"] is False
    assert revoked[0]["CidrIpv4"] == "0.0.0.0/0"

    perms = ec2.describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]["IpPermissions"]
    assert not any(p.get("FromPort") == 80 for p in perms)
    assert any(p.get("FromPort") == 443 for p in perms)

    ec2.delete_security_group(GroupId=sg_id)


def test_ec2_revoke_sg_ingress_unknown_rule_id_rejects_whole_call(ec2):
    """One unknown SecurityGroupRuleId rejects the whole revoke with
    InvalidSecurityGroupRuleId.NotFound — the valid ids in the same call must
    not be revoked (AWS validates before mutating)."""
    suffix = _uuid_mod.uuid4().hex[:8]
    sg_id = ec2.create_security_group(
        GroupName=f"qa-ec2-sg-revoke-bad-id-{suffix}", Description="bad rule id")["GroupId"]

    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
             "IpRanges": [{"CidrIp": "10.0.0.0/8"}]},
        ],
    )
    sgr = ec2.describe_security_group_rules(
        Filters=[{"Name": "group-id", "Values": [sg_id]}])["SecurityGroupRules"]
    rule_id = next(r["SecurityGroupRuleId"] for r in sgr if not r["IsEgress"])

    with pytest.raises(ClientError) as exc:
        ec2.revoke_security_group_ingress(
            GroupId=sg_id,
            SecurityGroupRuleIds=[rule_id, "sgr-00000000000000000"])
    assert exc.value.response["Error"]["Code"] == "InvalidSecurityGroupRuleId.NotFound"

    perms = ec2.describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]["IpPermissions"]
    assert any(p.get("FromPort") == 22 for p in perms)

    ec2.delete_security_group(GroupId=sg_id)


def test_ec2_revoke_sg_rules_by_id_derives_missing_stored_ids():
    """A rule dict carrying no stored SecurityGroupRuleId is matched by the
    content-derived id instead, and direction is part of that identity: an id
    derived for egress never matches an ingress rule. Exercised over the
    handlers' in-memory shape so both outcomes are asserted on the group state;
    the seeded default egress rule takes the same derive path over the wire
    (test_ec2_revoke_sg_default_egress_rule_by_id)."""
    from ministack.services.ec2 import _revoke_sg_rules_by_id, _sg_rule_id

    sg_id = "sg-0unit000000000000"
    rule_80 = {"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80,
               "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}
    rule_443 = {"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}
    # Pre-fix persisted state: no stored SecurityGroupRuleId on either rule.
    assert "SecurityGroupRuleId" not in rule_80
    sg = {"IpPermissions": [rule_80, rule_443], "IpPermissionsEgress": []}

    derived = _sg_rule_id(sg_id, False, rule_80)
    assert derived.startswith("sgr-")

    revoked, err = _revoke_sg_rules_by_id(sg, sg_id, [derived], is_egress=False)
    assert err is None
    assert revoked == [rule_80]
    assert sg["IpPermissions"] == [rule_443]

    # An id derived for the egress direction must NOT match the ingress rule.
    revoked, err = _revoke_sg_rules_by_id(
        sg, sg_id, [_sg_rule_id(sg_id, True, rule_443)], is_egress=False)
    assert revoked is None
    assert err is not None
    assert sg["IpPermissions"] == [rule_443]


def test_ec2_revoke_sg_default_egress_rule_by_id(ec2):
    """The allow-all egress rule CreateSecurityGroup seeds is stored without a
    SecurityGroupRuleId, so revoking it by the id DescribeSecurityGroupRules
    reports drives the content-derived fallback over the wire. It is also the
    rule an aws_vpc_security_group_egress_rule plan destroys first, and the
    revoke used to be a no-op that left it in place forever."""
    suffix = _uuid_mod.uuid4().hex[:8]
    sg_id = ec2.create_security_group(
        GroupName=f"qa-ec2-sg-default-egress-{suffix}", Description="default egress")["GroupId"]

    def _rule_ids():
        return [r["SecurityGroupRuleId"] for r in ec2.describe_security_group_rules(
            Filters=[{"Name": "group-id", "Values": [sg_id]}])["SecurityGroupRules"]]

    rules = ec2.describe_security_group_rules(
        Filters=[{"Name": "group-id", "Values": [sg_id]}])["SecurityGroupRules"]
    default_egress = next(r for r in rules if r["IsEgress"] and r["IpProtocol"] == "-1")
    rule_id = default_egress["SecurityGroupRuleId"]
    assert rule_id.startswith("sgr-")

    resp = ec2.revoke_security_group_egress(GroupId=sg_id, SecurityGroupRuleIds=[rule_id])
    assert resp["Return"] is True
    assert [r["SecurityGroupRuleId"] for r in resp["RevokedSecurityGroupRules"]] == [rule_id]

    assert rule_id not in _rule_ids()
    assert ec2.describe_security_groups(
        GroupIds=[sg_id])["SecurityGroups"][0]["IpPermissionsEgress"] == []

    # Re-authorizing the rule brings it back under the same content-derived id,
    # so a destroy-then-recreate cycle round-trips.
    reauth = ec2.authorize_security_group_egress(
        GroupId=sg_id,
        IpPermissions=[{"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}]}],
    )
    assert [r["SecurityGroupRuleId"] for r in reauth["SecurityGroupRules"]] == [rule_id]
    assert _rule_ids() == [rule_id]

    ec2.delete_security_group(GroupId=sg_id)


def test_ec2_revoke_sg_egress_by_rule_id(ec2):
    """RevokeSecurityGroupEgress(SecurityGroupRuleIds=[...]) removes the addressed
    rule and echoes it in RevokedSecurityGroupRules; an unknown id is rejected
    with InvalidSecurityGroupRuleId.NotFound."""
    suffix = _uuid_mod.uuid4().hex[:8]
    sg_id = ec2.create_security_group(
        GroupName=f"qa-ec2-sg-revoke-egress-id-{suffix}", Description="egress by id")["GroupId"]

    ec2.authorize_security_group_egress(
        GroupId=sg_id,
        IpPermissions=[
            {"IpProtocol": "tcp", "FromPort": 5432, "ToPort": 5432,
             "IpRanges": [{"CidrIp": "10.2.0.0/16"}]},
        ],
    )
    sgr = ec2.describe_security_group_rules(
        Filters=[{"Name": "group-id", "Values": [sg_id]}])["SecurityGroupRules"]
    rule_id = next(r["SecurityGroupRuleId"] for r in sgr
                   if r["IsEgress"] and r.get("FromPort") == 5432)

    resp = ec2.revoke_security_group_egress(GroupId=sg_id, SecurityGroupRuleIds=[rule_id])
    assert resp["Return"] is True
    revoked = resp["RevokedSecurityGroupRules"]
    assert len(revoked) == 1
    assert revoked[0]["SecurityGroupRuleId"] == rule_id
    assert revoked[0]["GroupId"] == sg_id
    assert revoked[0]["IsEgress"] is True
    assert revoked[0]["CidrIpv4"] == "10.2.0.0/16"

    egress = ec2.describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]["IpPermissionsEgress"]
    assert not any(p.get("FromPort") == 5432 for p in egress)

    with pytest.raises(ClientError) as exc:
        ec2.revoke_security_group_egress(
            GroupId=sg_id, SecurityGroupRuleIds=["sgr-00000000000000000"])
    assert exc.value.response["Error"]["Code"] == "InvalidSecurityGroupRuleId.NotFound"

    ec2.delete_security_group(GroupId=sg_id)


def test_ec2_sg_authorize_ingress_idempotent_duplicate(ec2):
    """Re-authorizing the same ingress rule must succeed (Terraform may re-apply unchanged rules)."""
    sg_id = ec2.create_security_group(GroupName="qa-ec2-sg-dup-auth", Description="dup auth")["GroupId"]
    perm = {
        "IpProtocol": "tcp",
        "FromPort": 5432,
        "ToPort": 5432,
        "IpRanges": [{"CidrIp": "10.1.0.0/16"}],
    }
    ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=[perm])
    ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=[perm])
    desc = ec2.describe_security_groups(GroupIds=[sg_id])
    ingress = desc["SecurityGroups"][0]["IpPermissions"]
    matching = [p for p in ingress if p.get("FromPort") == 5432 and p.get("ToPort") == 5432]
    assert len(matching) == 1
    ec2.delete_security_group(GroupId=sg_id)


def test_ec2_sg_authorize_ingress_legacy_source_group(ec2):
    """Legacy top-level --source-group params (CLI/Terraform) must create a real
    referenced-group rule visible in DescribeSecurityGroups AND
    DescribeSecurityGroupRules, with a SecurityGroupRuleId matching the Authorize
    response. Regression for issue #916."""
    vpc = ec2.create_vpc(CidrBlock="10.111.0.0/16")["Vpc"]
    sg_a = ec2.create_security_group(
        GroupName="sg-a-916", Description="a", VpcId=vpc["VpcId"])["GroupId"]
    sg_b = ec2.create_security_group(
        GroupName="sg-b-916", Description="b", VpcId=vpc["VpcId"])["GroupId"]

    # The AWS CLI `authorize-security-group-ingress --protocol tcp --port 8080
    # --source-group <id>` (and older SDKs) serialize to legacy top-level
    # parameters rather than the nested IpPermissions.N.Groups.M form. Inject
    # exactly that wire shape so the test exercises the same code path.
    def _inject_legacy(request, **kwargs):
        request.data = (
            f"Action=AuthorizeSecurityGroupIngress&Version=2016-11-15"
            f"&GroupId={sg_b}&IpProtocol=tcp&FromPort=8080&ToPort=8080"
            f"&SourceSecurityGroupId={sg_a}"
        )

    ec2.meta.events.register(
        "before-sign.ec2.AuthorizeSecurityGroupIngress", _inject_legacy)
    try:
        resp = ec2.authorize_security_group_ingress(GroupId=sg_b)
    finally:
        ec2.meta.events.unregister(
            "before-sign.ec2.AuthorizeSecurityGroupIngress", _inject_legacy)

    assert resp.get("Return") is True
    auth_rules = resp.get("SecurityGroupRules", [])
    assert len(auth_rules) == 1, f"expected 1 rule from Authorize, got {auth_rules}"
    auth_rule_id = auth_rules[0]["SecurityGroupRuleId"]
    assert auth_rules[0].get("ReferencedGroupInfo", {}).get("GroupId") == sg_a

    # DescribeSecurityGroups must show the UserIdGroupPairs reference.
    perms = ec2.describe_security_groups(
        GroupIds=[sg_b])["SecurityGroups"][0]["IpPermissions"]
    pairs = [pair for p in perms for pair in p.get("UserIdGroupPairs", [])]
    assert any(pair.get("GroupId") == sg_a for pair in pairs), \
        f"UserIdGroupPairs missing reference to {sg_a}: {perms}"

    # DescribeSecurityGroupRules must return the rule with ReferencedGroupInfo
    # and a SecurityGroupRuleId matching the Authorize response.
    sgr = ec2.describe_security_group_rules(
        Filters=[{"Name": "group-id", "Values": [sg_b]}])["SecurityGroupRules"]
    ref_rules = [r for r in sgr if r.get("ReferencedGroupInfo", {}).get("GroupId") == sg_a]
    assert len(ref_rules) == 1, \
        f"DescribeSecurityGroupRules missing referenced-group rule: {sgr}"
    assert ref_rules[0]["SecurityGroupRuleId"] == auth_rule_id
    assert ref_rules[0]["IsEgress"] is False
    assert ref_rules[0]["IpProtocol"] == "tcp"
    assert ref_rules[0]["FromPort"] == 8080
    assert ref_rules[0]["ToPort"] == 8080

    ec2.delete_security_group(GroupId=sg_b)
    ec2.delete_security_group(GroupId=sg_a)


def test_ec2_sg_authorize_ingress_referenced_group_nested(ec2):
    """The nested IpPermissions form (what boto3 and the Terraform AWS provider
    send for a source-security-group rule) must return ReferencedGroupInfo in the
    AuthorizeSecurityGroupIngress response, with a SecurityGroupRuleId consistent
    with DescribeSecurityGroupRules. Regression for issue #916: previously the
    Authorize response omitted ReferencedGroupInfo for group-pair rules."""
    vpc = ec2.create_vpc(CidrBlock="10.112.0.0/16")["Vpc"]
    sg_a = ec2.create_security_group(
        GroupName="sg-a-916n", Description="a", VpcId=vpc["VpcId"])["GroupId"]
    sg_b = ec2.create_security_group(
        GroupName="sg-b-916n", Description="b", VpcId=vpc["VpcId"])["GroupId"]

    resp = ec2.authorize_security_group_ingress(
        GroupId=sg_b,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 8080, "ToPort": 8080,
            "UserIdGroupPairs": [{"GroupId": sg_a}],
        }],
    )
    auth_rules = resp.get("SecurityGroupRules", [])
    assert len(auth_rules) == 1, f"expected 1 rule from Authorize, got {auth_rules}"
    assert auth_rules[0].get("ReferencedGroupInfo", {}).get("GroupId") == sg_a, \
        f"Authorize response missing ReferencedGroupInfo: {auth_rules}"
    auth_rule_id = auth_rules[0]["SecurityGroupRuleId"]

    # The same referenced-group rule must be discoverable (and ID-consistent) via
    # DescribeSecurityGroupRules — this is what Terraform's create waiter polls.
    sgr = ec2.describe_security_group_rules(
        Filters=[{"Name": "group-id", "Values": [sg_b]}])["SecurityGroupRules"]
    ref_rules = [r for r in sgr if r.get("ReferencedGroupInfo", {}).get("GroupId") == sg_a]
    assert len(ref_rules) == 1, \
        f"DescribeSecurityGroupRules missing referenced-group rule: {sgr}"
    assert ref_rules[0]["SecurityGroupRuleId"] == auth_rule_id
    assert ref_rules[0]["IsEgress"] is False

    ec2.delete_security_group(GroupId=sg_b)
    ec2.delete_security_group(GroupId=sg_a)


def test_ec2_key_pair_crud(ec2):
    resp = ec2.create_key_pair(KeyName="qa-ec2-key")
    assert resp["KeyName"] == "qa-ec2-key"
    assert "KeyMaterial" in resp

    desc = ec2.describe_key_pairs(KeyNames=["qa-ec2-key"])
    assert len(desc["KeyPairs"]) == 1

    ec2.delete_key_pair(KeyName="qa-ec2-key")
    desc2 = ec2.describe_key_pairs()
    assert not any(kp["KeyName"] == "qa-ec2-key" for kp in desc2["KeyPairs"])

def test_ec2_key_pair_duplicate(ec2):
    ec2.create_key_pair(KeyName="qa-ec2-key-dup")
    with pytest.raises(ClientError) as exc:
        ec2.create_key_pair(KeyName="qa-ec2-key-dup")
    assert exc.value.response["Error"]["Code"] == "InvalidKeyPair.Duplicate"


def test_ec2_placement_group_crud(ec2):
    created = ec2.create_placement_group(
        GroupName="qa-ec2-pg", Strategy="cluster"
    )["PlacementGroup"]
    assert created["GroupName"] == "qa-ec2-pg"
    assert created["State"] == "available"
    assert created["Strategy"] == "cluster"
    assert created["GroupId"].startswith("pg-")
    assert created["GroupArn"] == (
        "arn:aws:ec2:us-east-1:000000000000:placement-group/qa-ec2-pg"
    )

    desc = ec2.describe_placement_groups(GroupNames=["qa-ec2-pg"])
    assert len(desc["PlacementGroups"]) == 1
    assert desc["PlacementGroups"][0]["GroupId"] == created["GroupId"]

    ec2.delete_placement_group(GroupName="qa-ec2-pg")
    # After delete a named lookup reports the group as unknown, which lets
    # terraform detect the resource is gone on the next read.
    with pytest.raises(ClientError) as exc:
        ec2.describe_placement_groups(GroupNames=["qa-ec2-pg"])
    assert exc.value.response["Error"]["Code"] == "InvalidPlacementGroup.Unknown"


def test_ec2_placement_group_partition_reports_count(ec2):
    created = ec2.create_placement_group(
        GroupName="qa-ec2-pg-part", Strategy="partition", PartitionCount=3
    )["PlacementGroup"]
    assert created["Strategy"] == "partition"
    assert created["PartitionCount"] == 3

    desc = ec2.describe_placement_groups(GroupNames=["qa-ec2-pg-part"])
    assert desc["PlacementGroups"][0]["PartitionCount"] == 3
    ec2.delete_placement_group(GroupName="qa-ec2-pg-part")


def test_ec2_placement_group_duplicate(ec2):
    ec2.create_placement_group(GroupName="qa-ec2-pg-dup", Strategy="spread")
    with pytest.raises(ClientError) as exc:
        ec2.create_placement_group(GroupName="qa-ec2-pg-dup", Strategy="spread")
    assert exc.value.response["Error"]["Code"] == "InvalidPlacementGroup.Duplicate"
    ec2.delete_placement_group(GroupName="qa-ec2-pg-dup")


def test_ec2_placement_group_delete_unknown(ec2):
    with pytest.raises(ClientError) as exc:
        ec2.delete_placement_group(GroupName="qa-ec2-pg-nope")
    assert exc.value.response["Error"]["Code"] == "InvalidPlacementGroup.Unknown"


def test_ec2_placement_group_tag_round_trip(ec2):
    suffix = _uuid_mod.uuid4().hex[:8]
    name = f"qa-ec2-pg-tagged-{suffix}"
    ec2.create_placement_group(
        GroupName=name,
        Strategy="cluster",
        TagSpecifications=[{
            "ResourceType": "placement-group",
            "Tags": [{"Key": "Env", "Value": f"prod-{suffix}"}],
        }],
    )
    try:
        desc = ec2.describe_placement_groups(GroupNames=[name])
        tags = desc["PlacementGroups"][0].get("Tags", [])
        assert any(t["Key"] == "Env" and t["Value"] == f"prod-{suffix}" for t in tags)

        # tag: filter selects the tagged group.
        filtered = ec2.describe_placement_groups(
            Filters=[{"Name": "tag:Env", "Values": [f"prod-{suffix}"]}]
        )
        assert any(pg["GroupName"] == name for pg in filtered["PlacementGroups"])
    finally:
        ec2.delete_placement_group(GroupName=name)


def test_ec2_vpc_create_delete(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.1.0.0/16")["Vpc"]["VpcId"]
    assert vpc_id.startswith("vpc-")

    desc = ec2.describe_vpcs(VpcIds=[vpc_id])
    assert desc["Vpcs"][0]["CidrBlock"] == "10.1.0.0/16"
    assert not desc["Vpcs"][0]["IsDefault"]

    ec2.delete_vpc(VpcId=vpc_id)
    desc2 = ec2.describe_vpcs()
    assert not any(v["VpcId"] == vpc_id for v in desc2["Vpcs"])

def test_ec2_subnet_create_delete(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.2.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.2.1.0/24")["Subnet"]["SubnetId"]
    assert subnet_id.startswith("subnet-")

    desc = ec2.describe_subnets(SubnetIds=[subnet_id])
    assert desc["Subnets"][0]["CidrBlock"] == "10.2.1.0/24"

    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_internet_gateway_crud(ec2):
    igw_id = ec2.create_internet_gateway()["InternetGateway"]["InternetGatewayId"]
    assert igw_id.startswith("igw-")

    vpc_id = ec2.create_vpc(CidrBlock="10.3.0.0/16")["Vpc"]["VpcId"]
    ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

    desc = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
    assert len(desc["InternetGateways"][0]["Attachments"]) == 1

    ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    ec2.delete_internet_gateway(InternetGatewayId=igw_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_describe_internet_gateways_honors_filters(ec2):
    """Filters must narrow the result. Test filtering on different parameters"""
    import uuid as _uuid

    tag = _uuid.uuid4().hex[:8]
    vpc_id = ec2.create_vpc(CidrBlock="10.39.0.0/16")["Vpc"]["VpcId"]
    attached = ec2.create_internet_gateway(
        TagSpecifications=[{"ResourceType": "internet-gateway",
                            "Tags": [{"Key": "check", "Value": tag}]}],
    )["InternetGateway"]["InternetGatewayId"]
    # A second, unattached gateway: Should get ignored by the filters
    spare = ec2.create_internet_gateway()["InternetGateway"]["InternetGatewayId"]
    ec2.attach_internet_gateway(InternetGatewayId=attached, VpcId=vpc_id)

    def ids(**kwargs):
        return sorted(g["InternetGatewayId"]
                      for g in ec2.describe_internet_gateways(**kwargs)["InternetGateways"])

    try:
        assert ids(Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]) == [attached]
        # Any other attached gateway in the account matches this one too; what it must exclude is
        # the gateway attached to nothing.
        state_matched = ids(Filters=[{"Name": "attachment.state", "Values": ["available"]}])
        assert attached in state_matched and spare not in state_matched
        assert ids(Filters=[{"Name": "internet-gateway-id", "Values": [spare]}]) == [spare]
        assert ids(Filters=[{"Name": "tag:check", "Values": [tag]}]) == [attached]
        # Read the owner off the gateway rather than hardcoding it, so this holds against AWS too.
        owner = ec2.describe_internet_gateways(
            InternetGatewayIds=[attached])["InternetGateways"][0]["OwnerId"]
        assert attached in ids(Filters=[{"Name": "owner-id", "Values": [owner]}])
        assert ids(Filters=[{"Name": "owner-id", "Values": ["999999999999"]}]) == []
        # Two values for one name is an OR; no match is an empty list, not "everything".
        assert ids(Filters=[{"Name": "internet-gateway-id",
                             "Values": [attached, spare]}]) == sorted([attached, spare])
        assert ids(Filters=[{"Name": "attachment.vpc-id", "Values": ["vpc-00000000"]}]) == []
    finally:
        ec2.detach_internet_gateway(InternetGatewayId=attached, VpcId=vpc_id)
        ec2.delete_internet_gateway(InternetGatewayId=attached)
        ec2.delete_internet_gateway(InternetGatewayId=spare)
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_elastic_ip_crud(ec2):
    alloc = ec2.allocate_address(Domain="vpc")
    alloc_id = alloc["AllocationId"]
    assert alloc_id.startswith("eipalloc-")
    assert "PublicIp" in alloc

    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    assoc = ec2.associate_address(AllocationId=alloc_id, InstanceId=iid)
    assert "AssociationId" in assoc

    desc = ec2.describe_addresses(AllocationIds=[alloc_id])
    assert desc["Addresses"][0]["InstanceId"] == iid

    ec2.disassociate_address(AssociationId=assoc["AssociationId"])
    ec2.release_address(AllocationId=alloc_id)
    ec2.terminate_instances(InstanceIds=[iid])

def test_ec2_tags_crud(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    ec2.create_tags(Resources=[iid], Tags=[{"Key": "Name", "Value": "qa-box"}])

    desc = ec2.describe_instances(InstanceIds=[iid])
    tags = desc["Reservations"][0]["Instances"][0]["Tags"]
    assert any(t["Key"] == "Name" and t["Value"] == "qa-box" for t in tags)

    ec2.delete_tags(Resources=[iid], Tags=[{"Key": "Name"}])
    desc2 = ec2.describe_instances(InstanceIds=[iid])
    tags2 = desc2["Reservations"][0]["Instances"][0].get("Tags", [])
    assert not any(t["Key"] == "Name" for t in tags2)

    ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_describe_instances_tag_filter_excludes_untagged(ec2):
    owner = f"devuser-{_uuid_mod.uuid4().hex}"
    untagged_id = ec2.run_instances(ImageId="ami-untagged", MinCount=1, MaxCount=1)["Instances"][0]["InstanceId"]
    tagged_id = ec2.run_instances(
        ImageId="ami-tagged",
        MinCount=1,
        MaxCount=1,
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "PopOpsOwner", "Value": owner}],
        }],
    )["Instances"][0]["InstanceId"]

    resp = ec2.describe_instances(Filters=[{"Name": "tag:PopOpsOwner", "Values": [owner]}])
    ids = [
        inst["InstanceId"]
        for reservation in resp["Reservations"]
        for inst in reservation["Instances"]
    ]

    assert tagged_id in ids
    assert untagged_id not in ids

    ec2.terminate_instances(InstanceIds=[untagged_id, tagged_id])


def test_ec2_describe_instances_tag_filter_wildcard(ec2):
    suffix = _uuid_mod.uuid4().hex[:8]
    tagged = ec2.run_instances(
        ImageId="ami-wild",
        MinCount=1,
        MaxCount=1,
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "Env", "Value": f"prod-{suffix}"}],
        }],
    )["Instances"][0]["InstanceId"]
    other = ec2.run_instances(
        ImageId="ami-wild",
        MinCount=1,
        MaxCount=1,
        TagSpecifications=[{
            "ResourceType": "instance",
            "Tags": [{"Key": "Env", "Value": f"dev-{suffix}"}],
        }],
    )["Instances"][0]["InstanceId"]

    resp = ec2.describe_instances(Filters=[{"Name": "tag:Env", "Values": [f"prod-{suffix[:4]}*"]}])
    ids = [i["InstanceId"] for r in resp["Reservations"] for i in r["Instances"]]
    assert tagged in ids
    assert other not in ids
    ec2.terminate_instances(InstanceIds=[tagged, other])


def test_ec2_describe_instances_tag_value_and_tag_key_filters(ec2):
    suffix = _uuid_mod.uuid4().hex[:8]
    match_v = ec2.run_instances(
        ImageId="ami-tv",
        MinCount=1, MaxCount=1,
        TagSpecifications=[{"ResourceType": "instance",
                            "Tags": [{"Key": f"anykey-{suffix}", "Value": f"payload-{suffix}"}]}],
    )["Instances"][0]["InstanceId"]
    other_v = ec2.run_instances(
        ImageId="ami-tv",
        MinCount=1, MaxCount=1,
        TagSpecifications=[{"ResourceType": "instance",
                            "Tags": [{"Key": f"anykey-{suffix}", "Value": f"nope-{suffix}"}]}],
    )["Instances"][0]["InstanceId"]

    resp = ec2.describe_instances(Filters=[{"Name": "tag-value", "Values": [f"payload-{suffix}"]}])
    ids = [i["InstanceId"] for r in resp["Reservations"] for i in r["Instances"]]
    assert match_v in ids
    assert other_v not in ids

    resp = ec2.describe_instances(Filters=[{"Name": "tag-key", "Values": [f"anykey-{suffix}"]}])
    ids = [i["InstanceId"] for r in resp["Reservations"] for i in r["Instances"]]
    assert match_v in ids and other_v in ids

    ec2.terminate_instances(InstanceIds=[match_v, other_v])


def test_ec2_describe_vpcs_tag_filter(ec2):
    suffix = _uuid_mod.uuid4().hex[:8]
    tagged_vpc = ec2.create_vpc(
        CidrBlock="10.99.0.0/16",
        TagSpecifications=[{"ResourceType": "vpc",
                            "Tags": [{"Key": "Team", "Value": f"core-{suffix}"}]}],
    )["Vpc"]["VpcId"]
    untagged_vpc = ec2.create_vpc(CidrBlock="10.98.0.0/16")["Vpc"]["VpcId"]
    try:
        resp = ec2.describe_vpcs(Filters=[{"Name": "tag:Team", "Values": [f"core-{suffix}"]}])
        ids = [v["VpcId"] for v in resp["Vpcs"]]
        assert tagged_vpc in ids
        assert untagged_vpc not in ids
    finally:
        ec2.delete_vpc(VpcId=tagged_vpc)
        ec2.delete_vpc(VpcId=untagged_vpc)


def test_ec2_describe_security_groups_tag_filter(ec2):
    suffix = _uuid_mod.uuid4().hex[:8]
    vpc = ec2.create_vpc(CidrBlock="10.97.0.0/16")["Vpc"]["VpcId"]
    try:
        tagged_sg = ec2.create_security_group(
            GroupName=f"tagged-{suffix}", Description="x", VpcId=vpc,
            TagSpecifications=[{"ResourceType": "security-group",
                                "Tags": [{"Key": "Scope", "Value": f"svc-{suffix}"}]}],
        )["GroupId"]
        untagged_sg = ec2.create_security_group(
            GroupName=f"untagged-{suffix}", Description="y", VpcId=vpc,
        )["GroupId"]
        resp = ec2.describe_security_groups(Filters=[{"Name": "tag:Scope", "Values": [f"svc-{suffix}"]}])
        ids = [s["GroupId"] for s in resp["SecurityGroups"]]
        assert tagged_sg in ids
        assert untagged_sg not in ids
    finally:
        ec2.delete_security_group(GroupId=tagged_sg)
        ec2.delete_security_group(GroupId=untagged_sg)
        ec2.delete_vpc(VpcId=vpc)


def test_ec2_modify_vpc_attribute(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.10.0.0/16")["Vpc"]["VpcId"]
    ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
    ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_modify_subnet_attribute(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.11.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.11.1.0/24")["Subnet"]["SubnetId"]
    ec2.modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
    desc = ec2.describe_subnets(SubnetIds=[subnet_id])
    assert desc["Subnets"][0]["MapPublicIpOnLaunch"] is True
    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_route_table_crud(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.20.0.0/16")["Vpc"]["VpcId"]
    rtb_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]
    assert rtb_id.startswith("rtb-")

    desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assert desc["RouteTables"][0]["RouteTableId"] == rtb_id

    ec2.delete_route_table(RouteTableId=rtb_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_route_table_associate_disassociate(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.21.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.21.1.0/24")["Subnet"]["SubnetId"]
    rtb_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]

    assoc_id = ec2.associate_route_table(RouteTableId=rtb_id, SubnetId=subnet_id)["AssociationId"]
    assert assoc_id.startswith("rtbassoc-")

    desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assocs = desc["RouteTables"][0]["Associations"]
    assert any(a["RouteTableAssociationId"] == assoc_id for a in assocs)

    ec2.disassociate_route_table(AssociationId=assoc_id)
    desc2 = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assert not any(a["RouteTableAssociationId"] == assoc_id for a in desc2["RouteTables"][0]["Associations"])

    ec2.delete_route_table(RouteTableId=rtb_id)
    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_route_create_replace_delete(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.22.0.0/16")["Vpc"]["VpcId"]
    rtb_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]
    igw_id = ec2.create_internet_gateway()["InternetGateway"]["InternetGatewayId"]

    ec2.create_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw_id)
    desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    routes = desc["RouteTables"][0]["Routes"]
    assert any(r.get("DestinationCidrBlock") == "0.0.0.0/0" for r in routes)

    ec2.replace_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0", GatewayId="local")

    ec2.delete_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0")
    desc2 = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assert not any(r.get("DestinationCidrBlock") == "0.0.0.0/0" for r in desc2["RouteTables"][0]["Routes"])

    ec2.delete_internet_gateway(InternetGatewayId=igw_id)
    ec2.delete_route_table(RouteTableId=rtb_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_network_interface_crud(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.30.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.30.1.0/24")["Subnet"]["SubnetId"]

    eni_id = ec2.create_network_interface(SubnetId=subnet_id, Description="qa-eni")["NetworkInterface"][
        "NetworkInterfaceId"
    ]
    assert eni_id.startswith("eni-")

    desc = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    assert desc["NetworkInterfaces"][0]["Description"] == "qa-eni"
    assert desc["NetworkInterfaces"][0]["Status"] == "available"

    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
    desc2 = ec2.describe_network_interfaces()
    assert not any(e["NetworkInterfaceId"] == eni_id for e in desc2["NetworkInterfaces"])

    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_network_interface_attach_detach(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.31.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.31.1.0/24")["Subnet"]["SubnetId"]
    eni_id = ec2.create_network_interface(SubnetId=subnet_id)["NetworkInterface"]["NetworkInterfaceId"]
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    attach_resp = ec2.attach_network_interface(NetworkInterfaceId=eni_id, InstanceId=iid, DeviceIndex=1)
    attachment_id = attach_resp["AttachmentId"]
    assert attachment_id.startswith("eni-attach-")

    desc = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    eni = desc["NetworkInterfaces"][0]
    assert eni["Status"] == "in-use"
    # Real EC2 surfaces Attachment.AttachTime on every attached ENI. Issue #1178.
    assert "AttachTime" in eni["Attachment"]
    assert eni["Attachment"]["AttachTime"] is not None

    ec2.detach_network_interface(AttachmentId=attachment_id)
    desc2 = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    assert desc2["NetworkInterfaces"][0]["Status"] == "available"

    ec2.terminate_instances(InstanceIds=[iid])
    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_vpc_endpoint_crud(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.40.0.0/16")["Vpc"]["VpcId"]

    vpce_id = ec2.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName="com.amazonaws.us-east-1.s3",
        VpcEndpointType="Gateway",
    )["VpcEndpoint"]["VpcEndpointId"]
    assert vpce_id.startswith("vpce-")

    desc = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
    assert desc["VpcEndpoints"][0]["ServiceName"] == "com.amazonaws.us-east-1.s3"
    assert desc["VpcEndpoints"][0]["State"] == "available"

    ec2.delete_vpc_endpoints(VpcEndpointIds=[vpce_id])
    desc2 = ec2.describe_vpc_endpoints()
    assert not any(e["VpcEndpointId"] == vpce_id for e in desc2["VpcEndpoints"])

    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_vpc_endpoint_tags(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.41.0.0/16")["Vpc"]["VpcId"]

    vpce_id = ec2.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName="com.amazonaws.us-east-1.s3",
        VpcEndpointType="Gateway",
        TagSpecifications=[{
            "ResourceType": "vpc-endpoint",
            "Tags": [{"Key": "Env", "Value": "test"}, {"Key": "Team", "Value": "infra"}],
        }],
    )["VpcEndpoint"]["VpcEndpointId"]

    desc = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
    tags = {t["Key"]: t["Value"] for t in desc["VpcEndpoints"][0].get("Tags", [])}
    assert tags == {"Env": "test", "Team": "infra"}

    ec2.delete_vpc_endpoints(VpcEndpointIds=[vpce_id])
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_describe_vpc_endpoint_services(ec2):
    resp = ec2.describe_vpc_endpoint_services()
    names = resp["ServiceNames"]
    assert "com.amazonaws.us-east-1.s3" in names
    assert "com.amazonaws.us-east-1.dynamodb" in names
    assert "com.amazonaws.us-east-1.sts" in names

    by_name = {d["ServiceName"]: d for d in resp["ServiceDetails"]}
    s3 = by_name["com.amazonaws.us-east-1.s3"]
    assert s3["ServiceType"][0]["ServiceType"] == "Gateway"
    assert s3["Owner"] == "amazon"
    assert "us-east-1a" in s3["AvailabilityZones"]
    assert s3["BaseEndpointDnsNames"] == ["s3.us-east-1.amazonaws.com"]
    assert "PrivateDnsName" not in s3

    sts = by_name["com.amazonaws.us-east-1.sts"]
    assert sts["ServiceType"][0]["ServiceType"] == "Interface"
    assert sts["PrivateDnsName"] == "sts.us-east-1.amazonaws.com"

    filtered = ec2.describe_vpc_endpoint_services(
        ServiceNames=["com.amazonaws.us-east-1.s3"],
    )
    assert filtered["ServiceNames"] == ["com.amazonaws.us-east-1.s3"]

    gw_only = ec2.describe_vpc_endpoint_services(
        Filters=[{"Name": "service-type", "Values": ["Gateway"]}],
    )
    assert all(
        d["ServiceType"][0]["ServiceType"] == "Gateway"
        for d in gw_only["ServiceDetails"]
    )

def test_ec2_describe_route_tables_default(ec2):
    desc = ec2.describe_route_tables()
    assert any(rt["VpcId"] == "vpc-00000001" for rt in desc["RouteTables"])

def test_ec2_nat_gateway_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.100.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.100.1.0/24")
    subnet_id = subnet["Subnet"]["SubnetId"]

    resp = ec2.create_nat_gateway(SubnetId=subnet_id, ConnectivityType="private")
    nat_id = resp["NatGateway"]["NatGatewayId"]
    assert nat_id.startswith("nat-")
    assert resp["NatGateway"]["State"] == "available"

    desc = ec2.describe_nat_gateways(NatGatewayIds=[nat_id])
    assert len(desc["NatGateways"]) == 1
    assert desc["NatGateways"][0]["NatGatewayId"] == nat_id
    assert desc["NatGateways"][0]["SubnetId"] == subnet_id

    ec2.delete_nat_gateway(NatGatewayId=nat_id)
    desc2 = ec2.describe_nat_gateways(NatGatewayIds=[nat_id])
    assert desc2["NatGateways"][0]["State"] == "deleted"

def test_ec2_nat_gateway_filter_by_vpc(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.101.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.101.1.0/24")
    subnet_id = subnet["Subnet"]["SubnetId"]
    ec2.create_nat_gateway(SubnetId=subnet_id, ConnectivityType="private")

    desc = ec2.describe_nat_gateways(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    assert all(n["VpcId"] == vpc_id for n in desc["NatGateways"])

def test_ec2_network_acl_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.102.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    resp = ec2.create_network_acl(VpcId=vpc_id)
    acl_id = resp["NetworkAcl"]["NetworkAclId"]
    assert acl_id.startswith("acl-")
    assert resp["NetworkAcl"]["VpcId"] == vpc_id
    assert resp["NetworkAcl"]["IsDefault"] is False

    desc = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc["NetworkAcls"]) == 1
    assert desc["NetworkAcls"][0]["NetworkAclId"] == acl_id

    ec2.create_network_acl_entry(
        NetworkAclId=acl_id,
        RuleNumber=100,
        Protocol="-1",
        RuleAction="allow",
        Egress=False,
        CidrBlock="0.0.0.0/0",
    )
    desc2 = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc2["NetworkAcls"][0]["Entries"]) == 1

    ec2.delete_network_acl_entry(NetworkAclId=acl_id, RuleNumber=100, Egress=False)
    desc3 = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc3["NetworkAcls"][0]["Entries"]) == 0

    ec2.delete_network_acl(NetworkAclId=acl_id)
    desc4 = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc4["NetworkAcls"]) == 0

def test_ec2_network_acl_replace_entry(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.103.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    resp = ec2.create_network_acl(VpcId=vpc_id)
    acl_id = resp["NetworkAcl"]["NetworkAclId"]

    ec2.create_network_acl_entry(
        NetworkAclId=acl_id, RuleNumber=200, Protocol="-1", RuleAction="deny", Egress=False, CidrBlock="10.0.0.0/8"
    )
    ec2.replace_network_acl_entry(
        NetworkAclId=acl_id, RuleNumber=200, Protocol="-1", RuleAction="allow", Egress=False, CidrBlock="10.0.0.0/8"
    )
    desc = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    entries = desc["NetworkAcls"][0]["Entries"]
    assert len(entries) == 1
    assert entries[0]["RuleAction"] == "allow"

def test_ec2_flow_logs_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.104.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    resp = ec2.create_flow_logs(
        ResourceIds=[vpc_id],
        ResourceType="VPC",
        TrafficType="ALL",
        LogDestinationType="cloud-watch-logs",
        LogGroupName="/aws/vpc/flowlogs",
    )
    assert resp["Unsuccessful"] == []
    fl_ids = resp["FlowLogIds"]
    assert len(fl_ids) == 1
    assert fl_ids[0].startswith("fl-")

    desc = ec2.describe_flow_logs(FlowLogIds=fl_ids)
    assert len(desc["FlowLogs"]) == 1
    assert desc["FlowLogs"][0]["FlowLogId"] == fl_ids[0]
    assert desc["FlowLogs"][0]["FlowLogStatus"] == "ACTIVE"

    ec2.delete_flow_logs(FlowLogIds=fl_ids)
    desc2 = ec2.describe_flow_logs(FlowLogIds=fl_ids)
    assert len(desc2["FlowLogs"]) == 0


def test_ec2_flow_log_tags(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.105.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    fl_ids = ec2.create_flow_logs(
        ResourceIds=[vpc_id],
        ResourceType="VPC",
        TrafficType="ALL",
        LogDestinationType="cloud-watch-logs",
        LogGroupName="/aws/vpc/flowlogs-tags",
        TagSpecifications=[{
            "ResourceType": "flow-log",
            "Tags": [{"Key": "Project", "Value": "ministack"}],
        }],
    )["FlowLogIds"]

    desc = ec2.describe_flow_logs(FlowLogIds=fl_ids)
    tags = {t["Key"]: t["Value"] for t in desc["FlowLogs"][0].get("Tags", [])}
    assert tags == {"Project": "ministack"}

    ec2.delete_flow_logs(FlowLogIds=fl_ids)
    tag_resp = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": fl_ids}])
    assert tag_resp["Tags"] == []

    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_vpc_peering_crud(ec2):
    vpc1 = ec2.create_vpc(CidrBlock="10.105.0.0/16")
    vpc2 = ec2.create_vpc(CidrBlock="10.106.0.0/16")
    vpc_id1 = vpc1["Vpc"]["VpcId"]
    vpc_id2 = vpc2["Vpc"]["VpcId"]

    resp = ec2.create_vpc_peering_connection(VpcId=vpc_id1, PeerVpcId=vpc_id2)
    pcx = resp["VpcPeeringConnection"]
    pcx_id = pcx["VpcPeeringConnectionId"]
    assert pcx_id.startswith("pcx-")
    assert pcx["Status"]["Code"] == "pending-acceptance"

    accepted = ec2.accept_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
    assert accepted["VpcPeeringConnection"]["Status"]["Code"] == "active"

    desc = ec2.describe_vpc_peering_connections(VpcPeeringConnectionIds=[pcx_id])
    assert len(desc["VpcPeeringConnections"]) == 1
    assert desc["VpcPeeringConnections"][0]["Status"]["Code"] == "active"

    ec2.delete_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
    desc2 = ec2.describe_vpc_peering_connections(VpcPeeringConnectionIds=[pcx_id])
    assert desc2["VpcPeeringConnections"][0]["Status"]["Code"] == "deleted"


def test_ec2_vpc_peering_accepts_from_peer_region():
    east = _ec2_client("us-east-1")
    west = _ec2_client("us-west-2")
    east_vpc_id = east.create_vpc(CidrBlock="10.107.0.0/16")["Vpc"]["VpcId"]
    west_vpc_id = west.create_vpc(CidrBlock="10.108.0.0/16")["Vpc"]["VpcId"]

    resp = east.create_vpc_peering_connection(
        VpcId=east_vpc_id,
        PeerVpcId=west_vpc_id,
        PeerRegion="us-west-2",
        TagSpecifications=[
            {
                "ResourceType": "vpc-peering-connection",
                "Tags": [{"Key": "Scope", "Value": "inter-region"}],
            }
        ],
    )
    pcx_id = resp["VpcPeeringConnection"]["VpcPeeringConnectionId"]

    assert west.describe_vpc_peering_connections(
        VpcPeeringConnectionIds=[pcx_id]
    )["VpcPeeringConnections"][0]["Status"]["Code"] == "pending-acceptance"

    accepted = west.accept_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
    assert accepted["VpcPeeringConnection"]["Status"]["Code"] == "active"
    assert east.describe_vpc_peering_connections(
        VpcPeeringConnectionIds=[pcx_id]
    )["VpcPeeringConnections"][0]["Status"]["Code"] == "active"
    assert east.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"][0]["Value"] == "inter-region"
    assert west.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"] == []

    west.delete_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
    assert east.describe_vpc_peering_connections(
        VpcPeeringConnectionIds=[pcx_id]
    )["VpcPeeringConnections"][0]["Status"]["Code"] == "deleted"


def test_ec2_vpc_peering_tags_are_region_local():
    east = _ec2_client("us-east-1")
    west = _ec2_client("us-west-2")
    east_vpc_id = east.create_vpc(CidrBlock="10.109.0.0/16")["Vpc"]["VpcId"]
    west_vpc_id = west.create_vpc(CidrBlock="10.110.0.0/16")["Vpc"]["VpcId"]

    resp = east.create_vpc_peering_connection(
        VpcId=east_vpc_id,
        PeerVpcId=west_vpc_id,
        PeerRegion="us-west-2",
        TagSpecifications=[
            {
                "ResourceType": "vpc-peering-connection",
                "Tags": [{"Key": "Scope", "Value": "initial"}],
            }
        ],
    )
    pcx_id = resp["VpcPeeringConnection"]["VpcPeeringConnectionId"]

    assert east.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"][0]["Value"] == "initial"
    assert west.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"] == []
    assert len(east.describe_vpc_peering_connections(
        Filters=[{"Name": "tag:Scope", "Values": ["initial"]}]
    )["VpcPeeringConnections"]) == 1
    assert west.describe_vpc_peering_connections(
        Filters=[{"Name": "tag:Scope", "Values": ["initial"]}]
    )["VpcPeeringConnections"] == []

    west.create_tags(Resources=[pcx_id], Tags=[{"Key": "Scope", "Value": "peer"}])
    assert east.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"][0]["Value"] == "initial"
    assert west.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"][0]["Value"] == "peer"
    assert east.describe_vpc_peering_connections(
        Filters=[{"Name": "tag:Scope", "Values": ["peer"]}]
    )["VpcPeeringConnections"] == []
    assert len(west.describe_vpc_peering_connections(
        Filters=[{"Name": "tag:Scope", "Values": ["peer"]}]
    )["VpcPeeringConnections"]) == 1

    east.delete_tags(Resources=[pcx_id], Tags=[{"Key": "Scope"}])
    assert east.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"] == []
    assert west.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"][0]["Value"] == "peer"


def test_ec2_vpc_peering_cross_account_tags_are_account_isolated():
    requester_account = "111111111111"
    accepter_account = "222222222222"
    east = _ec2_client("us-east-1", requester_account)
    west = _ec2_client("us-west-2", accepter_account)
    east_vpc_id = east.create_vpc(CidrBlock="10.111.0.0/16")["Vpc"]["VpcId"]
    west_vpc_id = west.create_vpc(CidrBlock="10.112.0.0/16")["Vpc"]["VpcId"]

    resp = east.create_vpc_peering_connection(
        VpcId=east_vpc_id,
        PeerVpcId=west_vpc_id,
        PeerOwnerId=accepter_account,
        PeerRegion="us-west-2",
        TagSpecifications=[
            {
                "ResourceType": "vpc-peering-connection",
                "Tags": [{"Key": "Side", "Value": "requester"}],
            }
        ],
    )
    pcx_id = resp["VpcPeeringConnection"]["VpcPeeringConnectionId"]

    assert east.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"][0]["Value"] == "requester"
    assert west.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"] == []

    west.create_tags(Resources=[pcx_id], Tags=[{"Key": "Side", "Value": "accepter"}])
    assert west.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"][0]["Value"] == "accepter"
    assert east.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"][0]["Value"] == "requester"

    east.create_tags(
        Resources=[pcx_id],
        Tags=[{"Key": "Side", "Value": "requester-updated"}],
    )
    assert east.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"][0]["Value"] == "requester-updated"
    assert west.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [pcx_id]}]
    )["Tags"][0]["Value"] == "accepter"


def test_ec2_vpc_peering_not_found(ec2):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        ec2.accept_vpc_peering_connection(VpcPeeringConnectionId="pcx-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]

def test_ec2_dhcp_options_crud(ec2):
    resp = ec2.create_dhcp_options(
        DhcpConfigurations=[
            {"Key": "domain-name", "Values": ["example.internal"]},
            {"Key": "domain-name-servers", "Values": ["10.0.0.1", "10.0.0.2"]},
        ]
    )
    dopt = resp["DhcpOptions"]
    dopt_id = dopt["DhcpOptionsId"]
    assert dopt_id.startswith("dopt-")

    desc = ec2.describe_dhcp_options(DhcpOptionsIds=[dopt_id])
    assert len(desc["DhcpOptions"]) == 1
    configs = {c["Key"]: [v["Value"] for v in c["Values"]] for c in desc["DhcpOptions"][0]["DhcpConfigurations"]}
    assert configs["domain-name"] == ["example.internal"]
    assert "10.0.0.1" in configs["domain-name-servers"]

    vpc = ec2.create_vpc(CidrBlock="10.107.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    ec2.associate_dhcp_options(DhcpOptionsId=dopt_id, VpcId=vpc_id)

    ec2.delete_dhcp_options(DhcpOptionsId=dopt_id)
    desc2 = ec2.describe_dhcp_options(DhcpOptionsIds=[dopt_id])
    assert len(desc2["DhcpOptions"]) == 0

def test_ec2_dhcp_options_not_found(ec2):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        ec2.delete_dhcp_options(DhcpOptionsId="dopt-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]

def test_ec2_egress_only_igw_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.108.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    resp = ec2.create_egress_only_internet_gateway(VpcId=vpc_id)
    eigw = resp["EgressOnlyInternetGateway"]
    eigw_id = eigw["EgressOnlyInternetGatewayId"]
    assert eigw_id.startswith("eigw-")
    assert eigw["Attachments"][0]["State"] == "attached"
    assert eigw["Attachments"][0]["VpcId"] == vpc_id

    desc = ec2.describe_egress_only_internet_gateways(EgressOnlyInternetGatewayIds=[eigw_id])
    assert len(desc["EgressOnlyInternetGateways"]) == 1
    assert desc["EgressOnlyInternetGateways"][0]["EgressOnlyInternetGatewayId"] == eigw_id

    ec2.delete_egress_only_internet_gateway(EgressOnlyInternetGatewayId=eigw_id)
    desc2 = ec2.describe_egress_only_internet_gateways(EgressOnlyInternetGatewayIds=[eigw_id])
    assert len(desc2["EgressOnlyInternetGateways"]) == 0

def test_ec2_egress_only_igw_not_found(ec2):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        ec2.delete_egress_only_internet_gateway(EgressOnlyInternetGatewayId="eigw-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]

def test_ec2_describe_instance_attribute_instance_type(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1, InstanceType="t3.micro")
    iid = resp["Instances"][0]["InstanceId"]

    attr = ec2.describe_instance_attribute(InstanceId=iid, Attribute="instanceType")
    assert attr["InstanceId"] == iid
    assert attr["InstanceType"]["Value"] == "t3.micro"

    ec2.terminate_instances(InstanceIds=[iid])

def test_ec2_describe_instance_attribute_shutdown_behavior(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    attr = ec2.describe_instance_attribute(InstanceId=iid, Attribute="instanceInitiatedShutdownBehavior")
    assert attr["InstanceId"] == iid
    assert attr["InstanceInitiatedShutdownBehavior"]["Value"] == "stop"

    ec2.terminate_instances(InstanceIds=[iid])

def test_ec2_describe_instance_attribute_not_found(ec2):
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        ec2.describe_instance_attribute(InstanceId="i-000000000000nonex", Attribute="instanceType")
    assert exc.value.response["Error"]["Code"] == "InvalidInstanceID.NotFound"

def test_ec2_describe_instance_credit_specifications(ec2):
    iid = ec2.run_instances(ImageId="ami-test", MinCount=1, MaxCount=1)["Instances"][0]["InstanceId"]
    resp = ec2.describe_instance_credit_specifications(InstanceIds=[iid])
    specs = resp["InstanceCreditSpecifications"]
    assert len(specs) == 1
    assert specs[0]["InstanceId"] == iid
    assert specs[0]["CpuCredits"] == "standard"

def test_ec2_describe_spot_instance_requests(ec2):
    resp = ec2.describe_spot_instance_requests()
    assert "SpotInstanceRequests" in resp

def test_ec2_describe_capacity_reservations(ec2):
    resp = ec2.describe_capacity_reservations()
    assert "CapacityReservations" in resp

def test_ec2_describe_instance_types_defaults(ec2):
    resp = ec2.describe_instance_types()
    types = [t["InstanceType"] for t in resp["InstanceTypes"]]
    assert "t2.micro" in types
    assert "t3.micro" in types
    assert len(resp["InstanceTypes"]) >= 4
    # Spot-check shape
    sample = resp["InstanceTypes"][0]
    assert "VCpuInfo" in sample
    assert "MemoryInfo" in sample
    assert sample["VCpuInfo"]["DefaultVCpus"] >= 1
    assert sample["MemoryInfo"]["SizeInMiB"] >= 512

def test_ec2_describe_instance_types_filter(ec2):
    resp = ec2.describe_instance_types(InstanceTypes=["t2.micro", "m5.large"])
    types = {t["InstanceType"] for t in resp["InstanceTypes"]}
    assert types == {"t2.micro", "m5.large"}

def test_ec2_describe_vpc_attribute(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.99.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    resp = ec2.describe_vpc_attribute(VpcId=vpc_id, Attribute="enableDnsSupport")
    assert resp["EnableDnsSupport"]["Value"] in (True, False)
    resp2 = ec2.describe_vpc_attribute(VpcId=vpc_id, Attribute="enableDnsHostnames")
    assert resp2["EnableDnsHostnames"]["Value"] in (True, False)

def test_ec2_create_vpc_default_resources(ec2):
    """CreateVpc must create per-VPC default ACL, SG, and main route table."""
    vpc = ec2.create_vpc(CidrBlock="10.99.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        # DescribeNetworkAcls with vpc-id + default=true
        acls = ec2.describe_network_acls(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "default", "Values": ["true"]},
        ])
        assert len(acls["NetworkAcls"]) == 1
        acl = acls["NetworkAcls"][0]
        assert acl["IsDefault"] is True
        assert acl["VpcId"] == vpc_id

        # DescribeSecurityGroups with vpc-id + group-name=default
        sgs = ec2.describe_security_groups(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": ["default"]},
        ])
        assert len(sgs["SecurityGroups"]) == 1
        assert sgs["SecurityGroups"][0]["VpcId"] == vpc_id

        # DescribeRouteTables with vpc-id + association.main=true
        rtbs = ec2.describe_route_tables(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "association.main", "Values": ["true"]},
        ])
        assert len(rtbs["RouteTables"]) == 1
        assert rtbs["RouteTables"][0]["VpcId"] == vpc_id
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_route_table_association_filter(ec2):
    """AssociateRouteTable + DescribeRouteTables filter by association ID."""
    vpc = ec2.create_vpc(CidrBlock="10.98.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.98.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        rtb = ec2.create_route_table(VpcId=vpc_id)
        rtb_id = rtb["RouteTable"]["RouteTableId"]
        assoc = ec2.associate_route_table(RouteTableId=rtb_id, SubnetId=subnet_id)
        assoc_id = assoc["AssociationId"]

        # Filter by association ID
        result = ec2.describe_route_tables(Filters=[
            {"Name": "association.route-table-association-id", "Values": [assoc_id]},
        ])
        assert len(result["RouteTables"]) == 1
        assert result["RouteTables"][0]["RouteTableId"] == rtb_id

        # Filter by subnet ID
        result2 = ec2.describe_route_tables(Filters=[
            {"Name": "association.subnet-id", "Values": [subnet_id]},
        ])
        assert len(result2["RouteTables"]) == 1

        ec2.disassociate_route_table(AssociationId=assoc_id)
        ec2.delete_route_table(RouteTableId=rtb_id)
        ec2.delete_subnet(SubnetId=subnet_id)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_replace_route_table_association(ec2):
    """ReplaceRouteTableAssociation moves subnet to a different route table."""
    vpc = ec2.create_vpc(CidrBlock="10.97.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.97.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        rtb1 = ec2.create_route_table(VpcId=vpc_id)
        rtb1_id = rtb1["RouteTable"]["RouteTableId"]
        rtb2 = ec2.create_route_table(VpcId=vpc_id)
        rtb2_id = rtb2["RouteTable"]["RouteTableId"]

        assoc = ec2.associate_route_table(RouteTableId=rtb1_id, SubnetId=subnet_id)
        old_assoc_id = assoc["AssociationId"]

        # Replace association to rtb2
        new = ec2.replace_route_table_association(AssociationId=old_assoc_id, RouteTableId=rtb2_id)
        new_assoc_id = new["NewAssociationId"]
        assert new_assoc_id != old_assoc_id

        # Verify subnet is now on rtb2
        result = ec2.describe_route_tables(Filters=[
            {"Name": "association.subnet-id", "Values": [subnet_id]},
        ])
        assert result["RouteTables"][0]["RouteTableId"] == rtb2_id

        ec2.disassociate_route_table(AssociationId=new_assoc_id)
        ec2.delete_route_table(RouteTableId=rtb1_id)
        ec2.delete_route_table(RouteTableId=rtb2_id)
        ec2.delete_subnet(SubnetId=subnet_id)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_modify_vpc_endpoint(ec2):
    """ModifyVpcEndpoint adds/removes route tables."""
    vpc = ec2.create_vpc(CidrBlock="10.96.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        rtb = ec2.create_route_table(VpcId=vpc_id)
        rtb_id = rtb["RouteTable"]["RouteTableId"]
        ep = ec2.create_vpc_endpoint(
            VpcId=vpc_id, ServiceName="com.amazonaws.us-east-1.s3",
            VpcEndpointType="Gateway",
        )
        vpce_id = ep["VpcEndpoint"]["VpcEndpointId"]

        # Add route table
        ec2.modify_vpc_endpoint(VpcEndpointId=vpce_id, AddRouteTableIds=[rtb_id])
        desc = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
        assert rtb_id in desc["VpcEndpoints"][0]["RouteTableIds"]

        # Remove route table
        ec2.modify_vpc_endpoint(VpcEndpointId=vpce_id, RemoveRouteTableIds=[rtb_id])
        desc = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
        assert rtb_id not in desc["VpcEndpoints"][0]["RouteTableIds"]

        ec2.delete_vpc_endpoints(VpcEndpointIds=[vpce_id])
        ec2.delete_route_table(RouteTableId=rtb_id)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_describe_prefix_lists(ec2):
    """DescribePrefixLists returns built-in AWS service prefix lists."""
    result = ec2.describe_prefix_lists()
    pl_names = [pl["PrefixListName"] for pl in result["PrefixLists"]]
    assert any("s3" in n for n in pl_names)
    assert any("dynamodb" in n for n in pl_names)

def test_ec2_managed_prefix_list_crud(ec2):
    """Full lifecycle: create, describe, get entries, modify, delete."""
    pl = ec2.create_managed_prefix_list(
        PrefixListName="test-pl", MaxEntries=5, AddressFamily="IPv4",
        Entries=[{"Cidr": "10.0.0.0/8", "Description": "RFC1918-10"}],
    )
    pl_id = pl["PrefixList"]["PrefixListId"]
    assert pl["PrefixList"]["PrefixListName"] == "test-pl"

    # Describe
    desc = ec2.describe_managed_prefix_lists(PrefixListIds=[pl_id])
    assert len(desc["PrefixLists"]) == 1
    assert desc["PrefixLists"][0]["PrefixListName"] == "test-pl"

    # Get entries
    entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
    assert len(entries["Entries"]) == 1
    assert entries["Entries"][0]["Cidr"] == "10.0.0.0/8"

    # Modify — add entry
    ec2.modify_managed_prefix_list(
        PrefixListId=pl_id, CurrentVersion=1,
        AddEntries=[{"Cidr": "172.16.0.0/12", "Description": "RFC1918-172"}],
    )
    entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
    cidrs = [e["Cidr"] for e in entries["Entries"]]
    assert "10.0.0.0/8" in cidrs
    assert "172.16.0.0/12" in cidrs

    # Modify — remove entry
    ec2.modify_managed_prefix_list(
        PrefixListId=pl_id, CurrentVersion=2,
        RemoveEntries=[{"Cidr": "10.0.0.0/8"}],
    )
    entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
    cidrs = [e["Cidr"] for e in entries["Entries"]]
    assert "10.0.0.0/8" not in cidrs
    assert "172.16.0.0/12" in cidrs

    # Delete
    ec2.delete_managed_prefix_list(PrefixListId=pl_id)
    desc = ec2.describe_managed_prefix_lists(PrefixListIds=[pl_id])
    assert len(desc["PrefixLists"]) == 0


def test_ec2_aws_managed_prefix_lists_in_describe_managed(ec2):
    """DescribeManagedPrefixLists includes AWS-managed prefix lists with OwnerId=AWS."""
    result = ec2.describe_managed_prefix_lists()
    aws_pls = [pl for pl in result["PrefixLists"] if pl.get("OwnerId") == "AWS"]
    assert len(aws_pls) > 0
    names = [pl["PrefixListName"] for pl in aws_pls]
    assert any("s3" in n for n in names)
    assert any("dynamodb" in n for n in names)


def test_ec2_aws_managed_prefix_list_filter_by_owner(ec2):
    """Filtering DescribeManagedPrefixLists by owner-id=AWS returns only AWS-managed lists."""
    result = ec2.describe_managed_prefix_lists(
        Filters=[{"Name": "owner-id", "Values": ["AWS"]}]
    )
    assert all(pl.get("OwnerId") == "AWS" for pl in result["PrefixLists"])
    assert len(result["PrefixLists"]) > 0


def test_ec2_aws_managed_prefix_list_get_entries(ec2):
    """GetManagedPrefixListEntries works for AWS-managed prefix lists."""
    # Get an AWS-managed prefix list ID
    result = ec2.describe_managed_prefix_lists(
        Filters=[{"Name": "owner-id", "Values": ["AWS"]}]
    )
    aws_pl = next(pl for pl in result["PrefixLists"] if "s3" in pl["PrefixListName"])
    entries = ec2.get_managed_prefix_list_entries(PrefixListId=aws_pl["PrefixListId"])
    assert len(entries["Entries"]) >= 1
    # CIDR should be in CGNAT space (100.64.0.0/10)
    cidr = entries["Entries"][0]["Cidr"]
    assert cidr.startswith("100.")


def test_ec2_aws_managed_prefix_list_deterministic_cidr(ec2):
    """AWS-managed prefix list CIDRs are deterministic across calls."""
    result1 = ec2.describe_prefix_lists()
    result2 = ec2.describe_prefix_lists()
    for pl1, pl2 in zip(result1["PrefixLists"], result2["PrefixLists"]):
        assert pl1["Cidrs"] == pl2["Cidrs"]


def test_ec2_aws_managed_prefix_list_cannot_modify(ec2):
    """Modifying an AWS-managed prefix list returns UnsupportedOperation."""
    result = ec2.describe_managed_prefix_lists(
        Filters=[{"Name": "owner-id", "Values": ["AWS"]}]
    )
    aws_pl_id = result["PrefixLists"][0]["PrefixListId"]
    with pytest.raises(ClientError) as exc_info:
        ec2.modify_managed_prefix_list(
            PrefixListId=aws_pl_id, CurrentVersion=1,
            AddEntries=[{"Cidr": "10.0.0.0/8", "Description": "should fail"}],
        )
    assert exc_info.value.response["Error"]["Code"] == "UnsupportedOperation"


def test_ec2_aws_managed_prefix_list_cannot_delete(ec2):
    """Deleting an AWS-managed prefix list returns UnsupportedOperation."""
    result = ec2.describe_managed_prefix_lists(
        Filters=[{"Name": "owner-id", "Values": ["AWS"]}]
    )
    aws_pl_id = result["PrefixLists"][0]["PrefixListId"]
    with pytest.raises(ClientError) as exc_info:
        ec2.delete_managed_prefix_list(PrefixListId=aws_pl_id)
    assert exc_info.value.response["Error"]["Code"] == "UnsupportedOperation"


def test_ec2_describe_prefix_lists_cidr_not_placeholder(ec2):
    """DescribePrefixLists returns real CIDRs, not 0.0.0.0/0 placeholders."""
    result = ec2.describe_prefix_lists()
    for pl in result["PrefixLists"]:
        for cidr in pl["Cidrs"]:
            assert cidr != "0.0.0.0/0"


def test_ec2_aws_managed_prefix_list_cannot_create(ec2):
    """Creating a prefix list with an AWS-managed name returns UnsupportedOperation."""
    with pytest.raises(ClientError) as exc_info:
        ec2.create_managed_prefix_list(
            PrefixListName="com.amazonaws.us-east-1.s3",
            MaxEntries=5, AddressFamily="IPv4",
        )
    assert exc_info.value.response["Error"]["Code"] == "UnsupportedOperation"

def test_ec2_vpn_gateway_crud(ec2):
    """Full lifecycle: create, attach, describe, detach, delete."""
    vpc = ec2.create_vpc(CidrBlock="10.95.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        vgw = ec2.create_vpn_gateway(Type="ipsec.1")
        vgw_id = vgw["VpnGateway"]["VpnGatewayId"]
        assert vgw["VpnGateway"]["State"] == "available"

        # Attach
        ec2.attach_vpn_gateway(VpnGatewayId=vgw_id, VpcId=vpc_id)
        desc = ec2.describe_vpn_gateways(VpnGatewayIds=[vgw_id])
        attachments = desc["VpnGateways"][0]["VpcAttachments"]
        assert len(attachments) == 1
        assert attachments[0]["VpcId"] == vpc_id
        assert attachments[0]["State"] == "attached"

        # Filter by attachment.vpc-id
        filtered = ec2.describe_vpn_gateways(Filters=[
            {"Name": "attachment.vpc-id", "Values": [vpc_id]},
        ])
        assert len(filtered["VpnGateways"]) == 1

        # Detach
        ec2.detach_vpn_gateway(VpnGatewayId=vgw_id, VpcId=vpc_id)
        desc = ec2.describe_vpn_gateways(VpnGatewayIds=[vgw_id])
        assert desc["VpnGateways"][0]["VpcAttachments"] == []

        # Delete
        ec2.delete_vpn_gateway(VpnGatewayId=vgw_id)
        desc = ec2.describe_vpn_gateways(VpnGatewayIds=[vgw_id])
        assert len(desc["VpnGateways"]) == 0
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_vgw_route_propagation(ec2):
    """EnableVgwRoutePropagation / DisableVgwRoutePropagation with DescribeRouteTables verification."""
    vpc = ec2.create_vpc(CidrBlock="10.94.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        rtb = ec2.create_route_table(VpcId=vpc_id)
        rtb_id = rtb["RouteTable"]["RouteTableId"]
        vgw = ec2.create_vpn_gateway(Type="ipsec.1")
        vgw_id = vgw["VpnGateway"]["VpnGatewayId"]

        # Enable and verify it appears in DescribeRouteTables
        ec2.enable_vgw_route_propagation(RouteTableId=rtb_id, GatewayId=vgw_id)
        desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
        propagating = desc["RouteTables"][0].get("PropagatingVgws", [])
        assert {"GatewayId": vgw_id} in propagating

        # Idempotent — enabling again doesn't duplicate
        ec2.enable_vgw_route_propagation(RouteTableId=rtb_id, GatewayId=vgw_id)
        desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
        propagating = desc["RouteTables"][0].get("PropagatingVgws", [])
        assert len([v for v in propagating if v["GatewayId"] == vgw_id]) == 1

        # Disable and verify it's removed
        ec2.disable_vgw_route_propagation(RouteTableId=rtb_id, GatewayId=vgw_id)
        desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
        propagating = desc["RouteTables"][0].get("PropagatingVgws", [])
        assert {"GatewayId": vgw_id} not in propagating

        ec2.delete_vpn_gateway(VpnGatewayId=vgw_id)
        ec2.delete_route_table(RouteTableId=rtb_id)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_customer_gateway_crud(ec2):
    """Full lifecycle: create, describe, delete."""
    cgw = ec2.create_customer_gateway(BgpAsn=65000, IpAddress="203.0.113.1", Type="ipsec.1")
    cgw_id = cgw["CustomerGateway"]["CustomerGatewayId"]
    assert cgw["CustomerGateway"]["State"] == "available"
    assert cgw["CustomerGateway"]["IpAddress"] == "203.0.113.1"

    # Describe
    desc = ec2.describe_customer_gateways(CustomerGatewayIds=[cgw_id])
    assert len(desc["CustomerGateways"]) == 1
    assert desc["CustomerGateways"][0]["BgpAsn"] == "65000"

    # Delete
    ec2.delete_customer_gateway(CustomerGatewayId=cgw_id)
    desc = ec2.describe_customer_gateways(CustomerGatewayIds=[cgw_id])
    assert len(desc["CustomerGateways"]) == 0


def test_ec2_vpn_connection_crud(ec2):
    """CreateVpnConnection, DescribeVpnConnections, DeleteVpnConnection."""
    cgw = ec2.create_customer_gateway(BgpAsn=65000, IpAddress="203.0.113.1", Type="ipsec.1")
    cgw_id = cgw["CustomerGateway"]["CustomerGatewayId"]
    vgw = ec2.create_vpn_gateway(Type="ipsec.1")
    vgw_id = vgw["VpnGateway"]["VpnGatewayId"]

    vpn = ec2.create_vpn_connection(
        Type="ipsec.1",
        CustomerGatewayId=cgw_id,
        VpnGatewayId=vgw_id,
        Options={"StaticRoutesOnly": True},
    )
    conn = vpn["VpnConnection"]
    vpn_id = conn["VpnConnectionId"]
    assert vpn_id.startswith("vpn-")
    assert conn["State"] == "available"
    assert conn["Type"] == "ipsec.1"
    assert conn["CustomerGatewayId"] == cgw_id
    assert conn["VpnGatewayId"] == vgw_id
    assert conn["Options"]["StaticRoutesOnly"] is True

    # Describe
    desc = ec2.describe_vpn_connections(VpnConnectionIds=[vpn_id])
    assert len(desc["VpnConnections"]) == 1
    assert desc["VpnConnections"][0]["VpnConnectionId"] == vpn_id

    # Delete
    ec2.delete_vpn_connection(VpnConnectionId=vpn_id)
    desc = ec2.describe_vpn_connections(VpnConnectionIds=[vpn_id])
    assert len(desc["VpnConnections"]) == 0

    ec2.delete_vpn_gateway(VpnGatewayId=vgw_id)
    ec2.delete_customer_gateway(CustomerGatewayId=cgw_id)



def test_ec2_vpn_connection_route(ec2):
    """CreateVpnConnectionRoute / DeleteVpnConnectionRoute."""
    cgw = ec2.create_customer_gateway(BgpAsn=65000, IpAddress="203.0.113.2", Type="ipsec.1")
    cgw_id = cgw["CustomerGateway"]["CustomerGatewayId"]
    vgw = ec2.create_vpn_gateway(Type="ipsec.1")
    vgw_id = vgw["VpnGateway"]["VpnGatewayId"]
    vpn = ec2.create_vpn_connection(Type="ipsec.1", CustomerGatewayId=cgw_id, VpnGatewayId=vgw_id, Options={"StaticRoutesOnly": True})
    vpn_id = vpn["VpnConnection"]["VpnConnectionId"]

    # Create route
    ec2.create_vpn_connection_route(VpnConnectionId=vpn_id, DestinationCidrBlock="10.0.0.0/16")
    desc = ec2.describe_vpn_connections(VpnConnectionIds=[vpn_id])
    routes = desc["VpnConnections"][0]["Routes"]
    assert any(r["DestinationCidrBlock"] == "10.0.0.0/16" for r in routes)

    # Delete route
    ec2.delete_vpn_connection_route(VpnConnectionId=vpn_id, DestinationCidrBlock="10.0.0.0/16")
    desc = ec2.describe_vpn_connections(VpnConnectionIds=[vpn_id])
    routes = desc["VpnConnections"][0]["Routes"]
    assert not any(r["DestinationCidrBlock"] == "10.0.0.0/16" for r in routes)

    ec2.delete_vpn_connection(VpnConnectionId=vpn_id)
    ec2.delete_vpn_gateway(VpnGatewayId=vgw_id)
    ec2.delete_customer_gateway(CustomerGatewayId=cgw_id)
def test_ec2_create_route_nat_gateway(ec2):
    """CreateRoute with NatGatewayId stores it separately from GatewayId."""
    vpc = ec2.create_vpc(CidrBlock="10.93.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.93.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        eip = ec2.allocate_address(Domain="vpc")
        nat = ec2.create_nat_gateway(SubnetId=subnet_id, AllocationId=eip["AllocationId"])
        nat_id = nat["NatGateway"]["NatGatewayId"]
        rtb = ec2.create_route_table(VpcId=vpc_id)
        rtb_id = rtb["RouteTable"]["RouteTableId"]

        ec2.create_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0", NatGatewayId=nat_id)

        desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
        routes = desc["RouteTables"][0]["Routes"]
        nat_route = [r for r in routes if r.get("DestinationCidrBlock") == "0.0.0.0/0"][0]
        assert nat_route.get("NatGatewayId") == nat_id
        assert nat_route.get("GatewayId", "") == ""

        ec2.delete_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0")
        ec2.delete_route_table(RouteTableId=rtb_id)
        ec2.delete_nat_gateway(NatGatewayId=nat_id)
        ec2.release_address(AllocationId=eip["AllocationId"])
        ec2.delete_subnet(SubnetId=subnet_id)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_full_terraform_vpc_flow(ec2):
    """End-to-end Terraform VPC module flow: VPC → subnets → IGW → NAT → routes → associations."""
    # 1. Create VPC
    vpc = ec2.create_vpc(CidrBlock="10.50.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        # 2. Verify default resources
        acls = ec2.describe_network_acls(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "default", "Values": ["true"]},
        ])
        assert len(acls["NetworkAcls"]) == 1

        sgs = ec2.describe_security_groups(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": ["default"]},
        ])
        assert len(sgs["SecurityGroups"]) == 1

        main_rtbs = ec2.describe_route_tables(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "association.main", "Values": ["true"]},
        ])
        assert len(main_rtbs["RouteTables"]) == 1

        # 3. Create 6 subnets
        subnets = []
        for cidr, az in [
            ("10.50.0.0/20", "us-east-1a"), ("10.50.16.0/20", "us-east-1b"), ("10.50.32.0/20", "us-east-1c"),
            ("10.50.64.0/20", "us-east-1a"), ("10.50.80.0/20", "us-east-1b"), ("10.50.96.0/20", "us-east-1c"),
        ]:
            s = ec2.create_subnet(VpcId=vpc_id, CidrBlock=cidr, AvailabilityZone=az)
            subnets.append(s["Subnet"]["SubnetId"])

        # 4. IGW
        igw = ec2.create_internet_gateway()
        igw_id = igw["InternetGateway"]["InternetGatewayId"]
        ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

        # 5. EIP + NAT
        eip = ec2.allocate_address(Domain="vpc")
        nat = ec2.create_nat_gateway(SubnetId=subnets[3], AllocationId=eip["AllocationId"])
        nat_id = nat["NatGateway"]["NatGatewayId"]

        # 6. Public + private route tables
        pub_rtb = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]
        priv_rtb = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]

        # 7. Associate subnets (3 public, 3 private)
        assoc_ids = []
        for i in range(3):
            a = ec2.associate_route_table(RouteTableId=pub_rtb, SubnetId=subnets[i + 3])
            assoc_ids.append(a["AssociationId"])
            # Verify filter works
            found = ec2.describe_route_tables(Filters=[
                {"Name": "association.route-table-association-id", "Values": [a["AssociationId"]]},
            ])
            assert len(found["RouteTables"]) == 1
        for i in range(3):
            a = ec2.associate_route_table(RouteTableId=priv_rtb, SubnetId=subnets[i])
            assoc_ids.append(a["AssociationId"])

        # 8. Routes
        ec2.create_route(RouteTableId=pub_rtb, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw_id)
        ec2.create_route(RouteTableId=priv_rtb, DestinationCidrBlock="0.0.0.0/0", NatGatewayId=nat_id)

        # Verify NAT route
        desc = ec2.describe_route_tables(RouteTableIds=[priv_rtb])
        nat_route = [r for r in desc["RouteTables"][0]["Routes"] if r.get("DestinationCidrBlock") == "0.0.0.0/0"][0]
        assert nat_route.get("NatGatewayId") == nat_id

        # 9. Cleanup
        ec2.delete_route(RouteTableId=pub_rtb, DestinationCidrBlock="0.0.0.0/0")
        ec2.delete_route(RouteTableId=priv_rtb, DestinationCidrBlock="0.0.0.0/0")
        for aid in assoc_ids:
            ec2.disassociate_route_table(AssociationId=aid)
        ec2.delete_route_table(RouteTableId=pub_rtb)
        ec2.delete_route_table(RouteTableId=priv_rtb)
        ec2.delete_nat_gateway(NatGatewayId=nat_id)
        ec2.release_address(AllocationId=eip["AllocationId"])
        ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        ec2.delete_internet_gateway(InternetGatewayId=igw_id)
        for sid in subnets:
            ec2.delete_subnet(SubnetId=sid)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

# ---------------------------------------------------------------------------
# EC2 Launch Templates
# ---------------------------------------------------------------------------

def test_ec2_launch_template_crud(ec2):
    """Create, describe, and delete a launch template."""
    resp = ec2.create_launch_template(
        LaunchTemplateName="qa-lt-basic",
        LaunchTemplateData={
            "InstanceType": "t3.micro",
            "ImageId": "ami-12345678",
            "KeyName": "my-key",
        },
    )
    lt = resp["LaunchTemplate"]
    lt_id = lt["LaunchTemplateId"]
    assert lt_id.startswith("lt-")
    assert lt["LaunchTemplateName"] == "qa-lt-basic"
    assert lt["DefaultVersionNumber"] == 1
    assert lt["LatestVersionNumber"] == 1

    # Describe
    desc = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])
    assert len(desc["LaunchTemplates"]) == 1
    assert desc["LaunchTemplates"][0]["LaunchTemplateName"] == "qa-lt-basic"

    # Describe by name
    desc2 = ec2.describe_launch_templates(LaunchTemplateNames=["qa-lt-basic"])
    assert len(desc2["LaunchTemplates"]) == 1

    # Describe versions
    versions = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
    assert len(versions["LaunchTemplateVersions"]) == 1
    ver = versions["LaunchTemplateVersions"][0]
    assert ver["VersionNumber"] == 1
    assert ver["LaunchTemplateData"]["InstanceType"] == "t3.micro"
    assert ver["LaunchTemplateData"]["ImageId"] == "ami-12345678"

    # Delete
    ec2.delete_launch_template(LaunchTemplateId=lt_id)
    desc3 = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])
    assert len(desc3["LaunchTemplates"]) == 0


def test_ec2_launch_template_duplicate_name(ec2):
    """Creating a template with a duplicate name should fail."""
    ec2.create_launch_template(
        LaunchTemplateName="qa-lt-dup",
        LaunchTemplateData={"InstanceType": "t3.micro"},
    )
    with pytest.raises(ClientError) as exc:
        ec2.create_launch_template(
            LaunchTemplateName="qa-lt-dup",
            LaunchTemplateData={"InstanceType": "t3.small"},
        )
    assert "AlreadyExists" in exc.value.response["Error"]["Code"]
    # Cleanup
    ec2.delete_launch_template(LaunchTemplateName="qa-lt-dup")


def test_ec2_launch_template_versions(ec2):
    """Create multiple versions and query $Latest / $Default."""
    resp = ec2.create_launch_template(
        LaunchTemplateName="qa-lt-ver",
        LaunchTemplateData={"InstanceType": "t3.micro", "ImageId": "ami-v1"},
    )
    lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]

    # Create version 2
    ec2.create_launch_template_version(
        LaunchTemplateId=lt_id,
        LaunchTemplateData={"InstanceType": "t3.small", "ImageId": "ami-v2"},
        VersionDescription="version two",
    )
    # Create version 3
    ec2.create_launch_template_version(
        LaunchTemplateId=lt_id,
        LaunchTemplateData={"InstanceType": "t3.large", "ImageId": "ami-v3"},
    )

    # Latest should be version 3
    latest = ec2.describe_launch_template_versions(
        LaunchTemplateId=lt_id, Versions=["$Latest"],
    )
    assert len(latest["LaunchTemplateVersions"]) == 1
    assert latest["LaunchTemplateVersions"][0]["VersionNumber"] == 3
    assert latest["LaunchTemplateVersions"][0]["LaunchTemplateData"]["InstanceType"] == "t3.large"

    # Default should still be version 1
    default = ec2.describe_launch_template_versions(
        LaunchTemplateId=lt_id, Versions=["$Default"],
    )
    assert default["LaunchTemplateVersions"][0]["VersionNumber"] == 1

    # All versions
    all_ver = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
    assert len(all_ver["LaunchTemplateVersions"]) == 3

    # Modify default to version 2
    ec2.modify_launch_template(LaunchTemplateId=lt_id, DefaultVersion="2")
    desc = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])
    assert desc["LaunchTemplates"][0]["DefaultVersionNumber"] == 2

    default2 = ec2.describe_launch_template_versions(
        LaunchTemplateId=lt_id, Versions=["$Default"],
    )
    assert default2["LaunchTemplateVersions"][0]["VersionNumber"] == 2

    # Cleanup
    ec2.delete_launch_template(LaunchTemplateId=lt_id)


def test_ec2_create_launch_template_version_returns_version_number(ec2):
    """CreateLaunchTemplateVersion must return VersionNumber at the
    `launchTemplateVersion` root, not wrapped in `<item>` — otherwise the
    Go SDK reads it as null and Terraform sends ``SetDefaultVersion=0`` to
    the follow-up ModifyLaunchTemplate, which AWS rejects with
    ``InvalidLaunchTemplateId.VersionNotFound``. Repro for issue #753."""
    resp = ec2.create_launch_template(
        LaunchTemplateName="qa-lt-vnum",
        LaunchTemplateData={"InstanceType": "t3.micro"},
    )
    lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]
    try:
        v2 = ec2.create_launch_template_version(
            LaunchTemplateId=lt_id,
            LaunchTemplateData={"InstanceType": "t3.small"},
        )
        # botocore parses the response shape; if the inner XML is wrong, this
        # field reads as None / missing rather than the version number.
        assert "LaunchTemplateVersion" in v2
        v = v2["LaunchTemplateVersion"]
        assert v.get("VersionNumber") == 2, v
        assert v.get("LaunchTemplateId") == lt_id

        # End-to-end: Terraform-style follow-up that previously failed.
        modified = ec2.modify_launch_template(
            LaunchTemplateId=lt_id,
            DefaultVersion=str(v["VersionNumber"]),
        )
        assert modified["LaunchTemplate"]["DefaultVersionNumber"] == 2
    finally:
        ec2.delete_launch_template(LaunchTemplateId=lt_id)


def test_ec2_launch_template_with_block_devices(ec2):
    """Create a template with block device mappings."""
    resp = ec2.create_launch_template(
        LaunchTemplateName="qa-lt-bdm",
        LaunchTemplateData={
            "InstanceType": "t3.micro",
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/xvda",
                    "Ebs": {
                        "VolumeSize": 50,
                        "VolumeType": "gp3",
                        "Encrypted": True,
                        "DeleteOnTermination": True,
                    },
                }
            ],
        },
    )
    lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]

    versions = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
    data = versions["LaunchTemplateVersions"][0]["LaunchTemplateData"]
    assert len(data["BlockDeviceMappings"]) == 1
    bdm = data["BlockDeviceMappings"][0]
    assert bdm["DeviceName"] == "/dev/xvda"
    assert bdm["Ebs"]["VolumeSize"] == 50
    assert bdm["Ebs"]["VolumeType"] == "gp3"

    ec2.delete_launch_template(LaunchTemplateId=lt_id)


def test_ec2_launch_template_not_found(ec2):
    """Describe/delete a non-existent template should fail."""
    with pytest.raises(ClientError) as exc:
        ec2.describe_launch_template_versions(LaunchTemplateId="lt-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]


def test_ec2_default_subnets_three_azs(ec2):
    """Default VPC should have 3 subnets, one per AZ (a/b/c) with correct CIDRs."""
    resp = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": ["vpc-00000001"]}])
    subnets = resp["Subnets"]
    assert len(subnets) >= 3

    by_az = {s["AvailabilityZone"]: s for s in subnets}
    assert "us-east-1a" in by_az
    assert "us-east-1b" in by_az
    assert "us-east-1c" in by_az

    assert by_az["us-east-1a"]["CidrBlock"] == "172.31.0.0/20"
    assert by_az["us-east-1b"]["CidrBlock"] == "172.31.16.0/20"
    assert by_az["us-east-1c"]["CidrBlock"] == "172.31.32.0/20"

    for s in subnets:
        assert s["DefaultForAz"] is True
        assert s["MapPublicIpOnLaunch"] is True


def test_ec2_describe_subnets_tags_filters(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.77.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.77.1.0/24")["Subnet"]["SubnetId"]
    ec2.create_tags(Resources=[subnet_id], Tags=[{"Key": "Tier", "Value": "private"}, {"Key": "Env", "Value": "dev"}])

    resp = ec2.describe_subnets(Filters=[
        {"Name": "vpc-id", "Values": [vpc_id]},
        {"Name": "tag:Tier", "Values": ["private"]},
    ])
    assert any(s["SubnetId"] == subnet_id for s in resp["Subnets"])

    resp = ec2.describe_subnets(Filters=[{"Name": "tag-key", "Values": ["Tier"]}])
    assert any(s["SubnetId"] == subnet_id for s in resp["Subnets"])

    resp = ec2.describe_subnets(Filters=[
        {"Name": "vpc-id", "Values": [vpc_id]},
        {"Name": "tag:Tier", "Values": ["public"]},
    ])
    assert all(s["SubnetId"] != subnet_id for s in resp["Subnets"])

    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_describe_subnets_cidr_block_filter(ec2):
    """cidr-block, and the cidr/cidrBlock spellings of it, match the CIDR exactly."""
    vpc_id = ec2.create_vpc(CidrBlock="10.63.0.0/16")["Vpc"]["VpcId"]
    first = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.63.0.0/20",
                              AvailabilityZone="us-east-1a")["Subnet"]["SubnetId"]
    second = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.63.16.0/20",
                               AvailabilityZone="us-east-1b")["Subnet"]["SubnetId"]
    in_vpc = {"Name": "vpc-id", "Values": [vpc_id]}

    def subnets(*filters):
        return sorted(s["SubnetId"] for s in ec2.describe_subnets(Filters=list(filters))["Subnets"])

    # The per-AZ idempotency precheck: ignored, the filter returns the other AZ's subnet too, and a
    # provisioner adopts it as this AZ's.
    assert subnets(in_vpc, {"Name": "cidr-block", "Values": ["10.63.16.0/20"]}) == [second]
    for alias in ("cidr", "cidrBlock"):
        assert subnets(in_vpc, {"Name": alias, "Values": ["10.63.0.0/20"]}) == [first]
    # Exact, not enclosing: the VPC's own /16 contains both subnets but is neither subnet's CIDR.
    assert subnets(in_vpc, {"Name": "cidr-block", "Values": ["10.63.0.0/16"]}) == []
    assert subnets(in_vpc, {"Name": "cidr-block", "Values": ["10.63.99.0/28"]}) == []
    # Two values for one name is an OR, and the CIDRs are distinctive enough to filter account-wide.
    assert subnets({"Name": "cidr-block",
                    "Values": ["10.63.0.0/20", "10.63.16.0/20"]}) == sorted([first, second])
    # Control: both subnets really are in the VPC, so the assertions above narrowed the result
    # rather than emptying it for some unrelated reason.
    assert subnets(in_vpc) == sorted([first, second])

    ec2.delete_subnet(SubnetId=first)
    ec2.delete_subnet(SubnetId=second)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_describe_tags_filters(ec2):
    """DescribeTags respects resource-id and key filters."""
    # Create two instances and tag them differently
    r1 = ec2.run_instances(ImageId="ami-test1", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    r2 = ec2.run_instances(ImageId="ami-test2", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    id1 = r1["Instances"][0]["InstanceId"]
    id2 = r2["Instances"][0]["InstanceId"]

    ec2.create_tags(Resources=[id1], Tags=[{"Key": "Name", "Value": "first"}, {"Key": "Env", "Value": "prod"}])
    ec2.create_tags(Resources=[id2], Tags=[{"Key": "Name", "Value": "second"}])

    # Filter by resource-id — should only return tags for id1
    resp = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": [id1]}])
    tags = resp["Tags"]
    assert all(t["ResourceId"] == id1 for t in tags)
    assert len(tags) == 2

    # Filter by key — should return "Env" tag only for id1
    resp = ec2.describe_tags(Filters=[{"Name": "key", "Values": ["Env"]}])
    tags = resp["Tags"]
    assert all(t["Key"] == "Env" for t in tags)
    assert any(t["ResourceId"] == id1 for t in tags)

    # Filter by resource-id + key — should return exactly one tag
    resp = ec2.describe_tags(Filters=[
        {"Name": "resource-id", "Values": [id1]},
        {"Name": "key", "Values": ["Name"]},
    ])
    tags = resp["Tags"]
    assert len(tags) == 1
    assert tags[0]["ResourceId"] == id1
    assert tags[0]["Key"] == "Name"
    assert tags[0]["Value"] == "first"

    # Filter by resource-id that has no tags — should return empty
    resp = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": ["i-doesnotexist"]}])
    assert len(resp["Tags"]) == 0

    # All tags have correct resource type
    resp = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": [id1, id2]}])
    assert all(t["ResourceType"] == "instance" for t in resp["Tags"])


def test_ec2_default_vpc_network_acl(ec2):
    """Default VPC's network ACL should exist and be queryable."""
    resp = ec2.describe_network_acls(
        Filters=[{"Name": "default", "Values": ["true"]}]
    )
    acls = resp["NetworkAcls"]
    assert len(acls) >= 1
    default_acl = acls[0]
    assert default_acl["IsDefault"] is True
    # Should have both allow and deny entries
    assert len(default_acl["Entries"]) >= 4


def test_ec2_create_default_vpc_already_exists(ec2):
    """CreateDefaultVpc should fail when a default VPC already exists."""
    with pytest.raises(ClientError) as exc:
        ec2.create_default_vpc()
    assert exc.value.response["Error"]["Code"] == "DefaultVpcAlreadyExists"


def test_ec2_create_default_vpc(ec2):
    """CreateDefaultVpc should create a VPC with subnets, IGW, SG, route table, ACL."""
    # First, find and delete the existing default VPC and its dependencies
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"]
    if vpcs:
        default_vpc_id = vpcs[0]["VpcId"]
        # Delete subnets
        for s in ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [default_vpc_id]}])["Subnets"]:
            ec2.delete_subnet(SubnetId=s["SubnetId"])
        # Delete non-default security groups (other tests may have created them)
        for sg in ec2.describe_security_groups(
            Filters=[{"Name": "vpc-id", "Values": [default_vpc_id]}]
        )["SecurityGroups"]:
            if sg["GroupName"] != "default":
                ec2.delete_security_group(GroupId=sg["GroupId"])
        # Detach and delete IGWs
        for igw in ec2.describe_internet_gateways(
            Filters=[{"Name": "attachment.vpc-id", "Values": [default_vpc_id]}]
        )["InternetGateways"]:
            ec2.detach_internet_gateway(InternetGatewayId=igw["InternetGatewayId"], VpcId=default_vpc_id)
            ec2.delete_internet_gateway(InternetGatewayId=igw["InternetGatewayId"])
        ec2.delete_vpc(VpcId=default_vpc_id)

    # Now create a new default VPC
    resp = ec2.create_default_vpc()
    vpc = resp["Vpc"]
    assert vpc["IsDefault"] is True
    assert vpc["CidrBlock"] == "172.31.0.0/16"
    assert vpc["State"] == "available"

    vpc_id = vpc["VpcId"]

    # Verify 3 default subnets were created
    subnets = ec2.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["Subnets"]
    assert len(subnets) == 3
    for s in subnets:
        assert s["DefaultForAz"] is True
        assert s["MapPublicIpOnLaunch"] is True

    # Verify IGW attached
    igws = ec2.describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )["InternetGateways"]
    assert len(igws) == 1

    # Verify calling again fails
    with pytest.raises(ClientError) as exc:
        ec2.create_default_vpc()
    assert exc.value.response["Error"]["Code"] == "DefaultVpcAlreadyExists"


def test_ec2_authorize_sg_ingress_returns_rules(ec2):
    """AuthorizeSecurityGroupIngress returns SecurityGroupRules in response (provider v6)."""
    vpc = ec2.create_vpc(CidrBlock="10.99.0.0/16")["Vpc"]
    sg = ec2.create_security_group(
        GroupName="sgr-test", Description="test", VpcId=vpc["VpcId"])
    resp = ec2.authorize_security_group_ingress(
        GroupId=sg["GroupId"],
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
            "IpRanges": [{"CidrIp": "10.0.0.0/16"}],
        }],
    )
    assert resp.get("Return") is True
    rules = resp.get("SecurityGroupRules", [])
    assert len(rules) >= 1
    rule = rules[0]
    assert rule["SecurityGroupRuleId"].startswith("sgr-")
    assert rule["GroupId"] == sg["GroupId"]
    assert rule["IsEgress"] is False
    assert rule["IpProtocol"] == "tcp"
    assert rule["FromPort"] == 443
    assert rule["ToPort"] == 443
    assert rule["CidrIpv4"] == "10.0.0.0/16"


def test_ec2_authorize_sg_egress_returns_rules(ec2):
    """AuthorizeSecurityGroupEgress returns SecurityGroupRules in response (provider v6)."""
    vpc = ec2.create_vpc(CidrBlock="10.98.0.0/16")["Vpc"]
    sg = ec2.create_security_group(
        GroupName="sgr-egress-test", Description="test", VpcId=vpc["VpcId"])
    resp = ec2.authorize_security_group_egress(
        GroupId=sg["GroupId"],
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 80, "ToPort": 80,
            "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
        }],
    )
    assert resp.get("Return") is True
    rules = resp.get("SecurityGroupRules", [])
    assert len(rules) >= 1
    assert rules[0]["IsEgress"] is True
    assert rules[0]["CidrIpv4"] == "0.0.0.0/0"


def test_ec2_authorize_sg_egress_duplicate_of_default_returns_rule(ec2):
    """Re-authorizing the default allow-all egress rule must still return it.

    CreateSecurityGroup seeds the AWS default egress rule (-1, 0.0.0.0/0), and
    Terraform's aws_vpc_security_group_egress_rule declares exactly that rule. The
    authorize is idempotent, but the response must still carry the rule: the AWS
    provider reads SecurityGroupRules[0].SecurityGroupRuleId with no length check and
    panics on an empty securityGroupRuleSet.
    """
    vpc = ec2.create_vpc(CidrBlock="10.96.0.0/16")["Vpc"]
    sg_id = ec2.create_security_group(
        GroupName="sgr-egress-dup", Description="test", VpcId=vpc["VpcId"])["GroupId"]

    seeded = ec2.describe_security_group_rules(
        Filters=[{"Name": "group-id", "Values": [sg_id]}])["SecurityGroupRules"]
    default_egress = [r for r in seeded if r["IsEgress"]]
    assert len(default_egress) == 1

    resp = ec2.authorize_security_group_egress(
        GroupId=sg_id,
        IpPermissions=[{
            "IpProtocol": "-1",
            "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "managed by terraform"}],
        }],
        TagSpecifications=[{
            "ResourceType": "security-group-rule",
            "Tags": [{"Key": "crossplane-name", "Value": "pgdb"}],
        }],
    )

    assert resp.get("Return") is True
    rules = resp.get("SecurityGroupRules", [])
    assert len(rules) == 1
    assert rules[0]["SecurityGroupRuleId"] == default_egress[0]["SecurityGroupRuleId"]
    assert rules[0]["IsEgress"] is True

    # Idempotent: the duplicate must not add a second rule.
    after = ec2.describe_security_group_rules(
        Filters=[{"Name": "group-id", "Values": [sg_id]}])["SecurityGroupRules"]
    assert len([r for r in after if r["IsEgress"]]) == 1


def test_ec2_authorize_sg_ingress_duplicate_returns_rule(ec2):
    """A duplicate ingress authorize returns the stored rule id, not an empty set."""
    vpc = ec2.create_vpc(CidrBlock="10.95.0.0/16")["Vpc"]
    sg_id = ec2.create_security_group(
        GroupName="sgr-ingress-dup", Description="test", VpcId=vpc["VpcId"])["GroupId"]
    perm = {
        "IpProtocol": "tcp", "FromPort": 5432, "ToPort": 5432,
        "IpRanges": [{"CidrIp": "10.0.0.0/8"}],
    }

    first = ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=[perm])
    second = ec2.authorize_security_group_ingress(GroupId=sg_id, IpPermissions=[perm])

    assert len(second["SecurityGroupRules"]) == 1
    assert (second["SecurityGroupRules"][0]["SecurityGroupRuleId"]
            == first["SecurityGroupRules"][0]["SecurityGroupRuleId"])

    after = ec2.describe_security_group_rules(
        Filters=[{"Name": "group-id", "Values": [sg_id]}])["SecurityGroupRules"]
    assert len([r for r in after if not r["IsEgress"]]) == 1


def test_ec2_authorize_sg_ingress_ipv6(ec2):
    """AuthorizeSecurityGroupIngress returns rules with CidrIpv6."""
    vpc = ec2.create_vpc(CidrBlock="10.97.0.0/16")["Vpc"]
    sg = ec2.create_security_group(
        GroupName="sgr-ipv6-test", Description="test", VpcId=vpc["VpcId"])
    resp = ec2.authorize_security_group_ingress(
        GroupId=sg["GroupId"],
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
            "Ipv6Ranges": [{"CidrIpv6": "::/0"}],
        }],
    )
    assert resp.get("Return") is True
    rules = resp.get("SecurityGroupRules", [])
    assert len(rules) >= 1
    assert rules[0]["CidrIpv6"] == "::/0"


def test_ec2_terminate_unknown_instance(ec2):
    """TerminateInstances with a non-existent ID should return InvalidInstanceID.NotFound."""
    with pytest.raises(ClientError) as exc:
        ec2.terminate_instances(InstanceIds=["i-nonexistent0000000"])
    assert exc.value.response["Error"]["Code"] == "InvalidInstanceID.NotFound"


def test_ec2_stop_unknown_instance(ec2):
    """StopInstances with a non-existent ID should return InvalidInstanceID.NotFound."""
    with pytest.raises(ClientError) as exc:
        ec2.stop_instances(InstanceIds=["i-nonexistent0000000"])
    assert exc.value.response["Error"]["Code"] == "InvalidInstanceID.NotFound"


def test_ec2_vpc_cidr_block_association_set(ec2):
    """CreateVpc and DescribeVpcs should include cidrBlockAssociationSet."""
    vpc = ec2.create_vpc(CidrBlock="10.99.0.0/16")["Vpc"]
    assocs = vpc.get("CidrBlockAssociationSet", [])
    assert len(assocs) >= 1
    assert assocs[0]["CidrBlock"] == "10.99.0.0/16"
    assert assocs[0]["CidrBlockState"]["State"] == "associated"

    # DescribeVpcs should also include it
    desc = ec2.describe_vpcs(VpcIds=[vpc["VpcId"]])["Vpcs"][0]
    assert len(desc.get("CidrBlockAssociationSet", [])) >= 1
    ec2.delete_vpc(VpcId=vpc["VpcId"])


# ========== from test_ebs.py ==========
# EBS volumes / snapshots are EC2-service operations on AWS — same
# botocore client ("ec2"), same SigV4 scope. The conftest `ebs`
# fixture was a thin alias for `ec2`.
def test_ebs_create_and_describe_volume(ec2):
    resp = ec2.create_volume(
        AvailabilityZone="us-east-1a",
        Size=20,
        VolumeType="gp3",
    )
    vol_id = resp["VolumeId"]
    assert vol_id.startswith("vol-")
    assert resp["State"] == "available"
    assert resp["Size"] == 20
    assert resp["VolumeType"] == "gp3"

    desc = ec2.describe_volumes(VolumeIds=[vol_id])
    assert len(desc["Volumes"]) == 1
    assert desc["Volumes"][0]["VolumeId"] == vol_id

def test_ebs_attach_detach_volume(ec2):
    inst = ec2.run_instances(ImageId="ami-00000001", MinCount=1, MaxCount=1)
    instance_id = inst["Instances"][0]["InstanceId"]

    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]

    ec2.attach_volume(VolumeId=vol_id, InstanceId=instance_id, Device="/dev/xvdf")
    desc = ec2.describe_volumes(VolumeIds=[vol_id])
    assert desc["Volumes"][0]["State"] == "in-use"
    assert desc["Volumes"][0]["Attachments"][0]["InstanceId"] == instance_id

    ec2.detach_volume(VolumeId=vol_id)
    desc2 = ec2.describe_volumes(VolumeIds=[vol_id])
    assert desc2["Volumes"][0]["State"] == "available"
    assert desc2["Volumes"][0]["Attachments"] == []

def test_ebs_describe_volumes_honors_filters(ec2):
    """Filters must narrow the result. Ignoring them returns every volume in the account, so
    "the volume I tagged for this run" resolves to somebody else's."""
    tag = _uuid_mod.uuid4().hex[:8]
    key = f"check-{tag}"          # unique, so other tests' volumes cannot match the tag filters
    zone = ec2.describe_availability_zones()["AvailabilityZones"][0]["ZoneName"]
    tagged = ec2.create_volume(
        AvailabilityZone=zone, Size=77, VolumeType="gp3",
        TagSpecifications=[{"ResourceType": "volume", "Tags": [{"Key": key, "Value": tag}]}],
    )["VolumeId"]
    # A second volume differing in size and type: with the filters ignored it comes back too.
    spare = ec2.create_volume(AvailabilityZone=zone, Size=79, VolumeType="gp2")["VolumeId"]
    instance_id = ec2.run_instances(ImageId="ami-00000001", MinCount=1, MaxCount=1,
                                    )["Instances"][0]["InstanceId"]

    def ids(**kwargs):
        return sorted(v["VolumeId"] for v in ec2.describe_volumes(**kwargs)["Volumes"])

    try:
        assert ids(Filters=[{"Name": f"tag:{key}", "Values": [tag]}]) == [tagged]
        assert ids(Filters=[{"Name": "tag-key", "Values": [key]}]) == [tagged]
        assert ids(Filters=[{"Name": "size", "Values": ["77"]}]) == [tagged]
        assert ids(Filters=[{"Name": "volume-id", "Values": [spare]}]) == [spare]
        assert tagged in ids(Filters=[{"Name": "encrypted", "Values": ["false"]}])
        # Other tests leave gp2 volumes behind, so this one asserts on membership.
        by_type = ids(Filters=[{"Name": "volume-type", "Values": ["gp2"]}])
        assert spare in by_type and tagged not in by_type
        # Two filter names is an AND, two values for one name an OR.
        assert ids(Filters=[{"Name": "size", "Values": ["77"]},
                            {"Name": "volume-type", "Values": ["gp2"]}]) == []
        assert ids(Filters=[{"Name": "volume-id", "Values": [tagged, spare]}]) == sorted(
            [tagged, spare])
        # A filter that matches nothing is an empty list, not "everything".
        assert ids(Filters=[{"Name": "size", "Values": ["4096"]}]) == []

        # attachment.* matches on any one attachment. The instance's own root volume is attached
        # too, so these assert on membership rather than an exact list -- except the device, which
        # only this attachment uses.
        ec2.attach_volume(VolumeId=tagged, InstanceId=instance_id, Device="/dev/xvdz")
        attached_here = ids(Filters=[{"Name": "attachment.instance-id", "Values": [instance_id]}])
        assert tagged in attached_here and spare not in attached_here
        assert ids(Filters=[{"Name": "attachment.device", "Values": ["/dev/xvdz"]}]) == [tagged]
        in_use = ids(Filters=[{"Name": "status", "Values": ["in-use"]}])
        assert tagged in in_use and spare not in in_use
        assert spare in ids(Filters=[{"Name": "status", "Values": ["available"]}])
    finally:
        # Cleanup must not mask a failed assertion above: a volume still attached because an
        # assertion raised first would fail to delete, and that error would be the one reported.
        try:
            ec2.detach_volume(VolumeId=tagged)
        except ClientError:
            pass
        try:
            ec2.terminate_instances(InstanceIds=[instance_id])
        except ClientError:
            pass
        for volume_id in (tagged, spare):
            try:
                ec2.delete_volume(VolumeId=volume_id)
            except ClientError:
                pass

def test_ebs_delete_volume(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=5, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    ec2.delete_volume(VolumeId=vol_id)
    with pytest.raises(ClientError) as exc:
        ec2.describe_volumes(VolumeIds=[vol_id])
    assert exc.value.response["Error"]["Code"] == "InvalidVolume.NotFound"

def test_ebs_modify_volume(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    resp = ec2.modify_volume(VolumeId=vol_id, Size=50, VolumeType="gp3")
    assert resp["VolumeModification"]["TargetSize"] == 50
    assert resp["VolumeModification"]["TargetVolumeType"] == "gp3"

def test_ebs_volume_status(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=8, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    resp = ec2.describe_volume_status(VolumeIds=[vol_id])
    assert len(resp["VolumeStatuses"]) == 1
    assert resp["VolumeStatuses"][0]["VolumeStatus"]["Status"] == "ok"

def test_ebs_create_and_describe_snapshot(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    snap = ec2.create_snapshot(VolumeId=vol_id, Description="test snapshot")
    snap_id = snap["SnapshotId"]
    assert snap_id.startswith("snap-")
    assert snap["State"] == "completed"

    desc = ec2.describe_snapshots(SnapshotIds=[snap_id])
    assert len(desc["Snapshots"]) == 1
    assert desc["Snapshots"][0]["VolumeId"] == vol_id
    assert desc["Snapshots"][0]["Description"] == "test snapshot"

def test_ebs_describe_snapshots_honors_filters(ec2):
    """Filters must narrow the result. Ignoring them returns every snapshot in the account, so a
    tag scan looking for "the snapshot of this backup" adopts one taken by something else."""
    tag = _uuid_mod.uuid4().hex[:8]
    key = f"check-{tag}"          # unique, so other tests' snapshots cannot match the tag filters
    first_vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=11, VolumeType="gp2")["VolumeId"]
    second_vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=12, VolumeType="gp2")["VolumeId"]
    tagged = ec2.create_snapshot(
        VolumeId=first_vol, Description=f"tagged {tag}",
        TagSpecifications=[{"ResourceType": "snapshot", "Tags": [{"Key": key, "Value": tag}]}],
    )["SnapshotId"]
    # A second snapshot of the same volume, untagged: with the filters ignored it comes back too.
    spare = ec2.create_snapshot(VolumeId=first_vol, Description=f"spare {tag}")["SnapshotId"]
    other_volume = ec2.create_snapshot(VolumeId=second_vol, Description=f"other {tag}")["SnapshotId"]

    def ids(**kwargs):
        return sorted(s["SnapshotId"] for s in ec2.describe_snapshots(**kwargs)["Snapshots"])

    try:
        assert ids(Filters=[{"Name": f"tag:{key}", "Values": [tag]}]) == [tagged]
        assert ids(Filters=[{"Name": "tag-key", "Values": [key]}]) == [tagged]
        assert ids(Filters=[{"Name": "volume-id", "Values": [second_vol]}]) == [other_volume]
        assert ids(Filters=[{"Name": "description", "Values": [f"spare {tag}"]}]) == [spare]
        assert ids(Filters=[{"Name": "volume-size", "Values": ["12"]}]) == [other_volume]
        assert ids(Filters=[{"Name": "snapshot-id", "Values": [spare]}]) == [spare]
        # Other tests leave completed snapshots behind, so this one asserts on membership.
        completed = ids(Filters=[{"Name": "status", "Values": ["completed"]}])
        assert tagged in completed and spare in completed
        # Two filter names is an AND, two values for one name an OR.
        assert ids(Filters=[{"Name": "volume-id", "Values": [first_vol]},
                            {"Name": "volume-size", "Values": ["12"]}]) == []
        assert ids(Filters=[{"Name": "volume-id", "Values": [first_vol, second_vol]}]) == sorted(
            [tagged, spare, other_volume])
        # A filter that matches nothing is an empty list, not "everything".
        assert ids(Filters=[{"Name": f"tag:{key}", "Values": ["matches-nothing"]}]) == []
        assert ids(Filters=[{"Name": "volume-size", "Values": ["4096"]}]) == []
        # The SnapshotIds argument and the filters narrow together rather than either winning.
        assert ids(SnapshotIds=[tagged, spare],
                   Filters=[{"Name": f"tag:{key}", "Values": [tag]}]) == [tagged]
    finally:
        # Cleanup must not mask a failed assertion above.
        for snapshot_id in (tagged, spare, other_volume):
            try:
                ec2.delete_snapshot(SnapshotId=snapshot_id)
            except ClientError:
                pass
        for volume_id in (first_vol, second_vol):
            try:
                ec2.delete_volume(VolumeId=volume_id)
            except ClientError:
                pass

def test_ebs_delete_snapshot(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    snap = ec2.create_snapshot(VolumeId=vol["VolumeId"])
    snap_id = snap["SnapshotId"]
    ec2.delete_snapshot(SnapshotId=snap_id)
    with pytest.raises(ClientError) as exc:
        ec2.describe_snapshots(SnapshotIds=[snap_id])
    assert exc.value.response["Error"]["Code"] == "InvalidSnapshot.NotFound"

def test_ebs_copy_snapshot(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    snap = ec2.create_snapshot(VolumeId=vol["VolumeId"], Description="original")
    snap_id = snap["SnapshotId"]
    copy = ec2.copy_snapshot(SourceRegion="us-east-1", SourceSnapshotId=snap_id, Description="copy")
    new_snap_id = copy["SnapshotId"]
    assert new_snap_id != snap_id
    assert new_snap_id.startswith("snap-")


def test_ebs_copy_snapshot_respects_source_region():
    east = _ec2_client("us-east-1")
    west = _ec2_client("us-west-2")

    vol = east.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    snap = east.create_snapshot(VolumeId=vol["VolumeId"], Description="east source")
    snap_id = snap["SnapshotId"]

    req = urllib.request.Request(
        f"{ENDPOINT}/",
        data=urlencode({
            "Action": "CopySnapshot",
            "Version": "2016-11-15",
            "SourceSnapshotId": snap_id,
            "Description": "missing source region",
        }).encode("utf-8"),
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": (
                "AWS4-HMAC-SHA256 "
                "Credential=test/20260729/us-west-2/ec2/aws4_request, "
                "SignedHeaders=host, Signature=test"
            ),
        },
        method="POST",
    )
    with pytest.raises(urllib.error.HTTPError) as exc:
        urllib.request.urlopen(req, timeout=15)
    assert exc.value.code == 400
    assert b"InvalidSnapshot.NotFound" in exc.value.read()

    copied = west.copy_snapshot(
        SourceRegion="us-east-1",
        SourceSnapshotId=snap_id,
        Description="west copy",
    )
    copied_id = copied["SnapshotId"]

    assert copied_id != snap_id
    assert east.describe_snapshots(SnapshotIds=[snap_id])["Snapshots"][0]["SnapshotId"] == snap_id
    assert west.describe_snapshots(SnapshotIds=[copied_id])["Snapshots"][0]["Description"] == "west copy"
    with pytest.raises(ClientError):
        west.describe_snapshots(SnapshotIds=[snap_id])


def test_ebs_snapshot_attribute(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    snap = ec2.create_snapshot(VolumeId=vol["VolumeId"], Description="attr test")
    snap_id = snap["SnapshotId"]

    ec2.modify_snapshot_attribute(
        SnapshotId=snap_id,
        Attribute="createVolumePermission",
        OperationType="add",
        UserIds=["123456789012"],
    )
    resp = ec2.describe_snapshot_attribute(
        SnapshotId=snap_id, Attribute="createVolumePermission"
    )
    assert resp["SnapshotId"] == snap_id
    assert any(
        p.get("UserId") == "123456789012"
        for p in resp.get("CreateVolumePermissions", [])
    )

def test_ebs_volume_attribute(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    resp = ec2.describe_volume_attribute(VolumeId=vol_id, Attribute="autoEnableIO")
    assert resp["VolumeId"] == vol_id
    assert "AutoEnableIO" in resp

def test_ebs_describe_volumes_modifications(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    ec2.modify_volume(VolumeId=vol_id, Size=50, VolumeType="gp3")
    resp = ec2.describe_volumes_modifications(VolumeIds=[vol_id])
    mods = resp["VolumesModifications"]
    assert len(mods) >= 1
    assert mods[0]["VolumeId"] == vol_id
    assert mods[0]["TargetSize"] == 50
    assert mods[0]["TargetVolumeType"] == "gp3"


def test_ebs_enable_volume_io(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    ec2.enable_volume_io(VolumeId=vol_id)
    # Stub — just verify it doesn't error
    resp = ec2.describe_volume_attribute(VolumeId=vol_id, Attribute="autoEnableIO")
    assert resp["VolumeId"] == vol_id


def test_ebs_modify_volume_attribute(ec2):
    vol = ec2.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    ec2.modify_volume_attribute(VolumeId=vol_id, AutoEnableIO={"Value": True})
    # Stub — just verify it doesn't error
    resp = ec2.describe_volume_attribute(VolumeId=vol_id, Attribute="autoEnableIO")
    assert resp["VolumeId"] == vol_id


def test_ec2_create_describe_fleet(ec2):
    lt = ec2.create_launch_template(
        LaunchTemplateName="test-fleet-lt",
        LaunchTemplateData={"ImageId": "ami-12345678", "InstanceType": "t2.small"}
    )
    lt_id = lt["LaunchTemplate"]["LaunchTemplateId"]

    # Create fleet with overrides and tag specifications
    fleet = ec2.create_fleet(
        LaunchTemplateConfigs=[
            {
                "LaunchTemplateSpecification": {
                    "LaunchTemplateId": lt_id,
                    "Version": "1"
                },
                "Overrides": [
                    {
                        "InstanceType": "t2.medium"
                    }
                ]
            }
        ],
        TargetCapacitySpecification={
            "TotalTargetCapacity": 2,
            "OnDemandTargetCapacity": 2,
        },
        Type="instant",
        TagSpecifications=[
            {
                "ResourceType": "fleet",
                "Tags": [
                    {"Key": "Environment", "Value": "Production"}
                ]
            }
        ]
    )

    fleet_id = fleet["FleetId"]
    assert fleet_id.startswith("fleet-")
    assert len(fleet["Instances"]) == 1
    assert fleet["Instances"][0]["InstanceType"] == "t2.medium"
    assert fleet["Instances"][0]["Lifecycle"] == "on-demand"
    assert len(fleet["Instances"][0]["InstanceIds"]) == 2

    # Verify instances are running
    inst_ids = fleet["Instances"][0]["InstanceIds"]
    desc_inst = ec2.describe_instances(InstanceIds=inst_ids)
    reservations = desc_inst["Reservations"]
    assert len(reservations) >= 1
    launched_instances = [inst for r in reservations for inst in r["Instances"]]
    assert len(launched_instances) == 2
    for inst in launched_instances:
        assert inst["InstanceType"] == "t2.medium"
        assert inst["ImageId"] == "ami-12345678"

    # Describe fleet and verify details and tags
    resp = ec2.describe_fleets(FleetIds=[fleet_id])
    assert len(resp["Fleets"]) == 1
    f = resp["Fleets"][0]
    assert f["FleetId"] == fleet_id
    assert f["FulfilledCapacity"] == 2.0
    assert f["FulfilledOnDemandCapacity"] == 2.0
    assert f["TargetCapacitySpecification"]["TotalTargetCapacity"] == 2
    assert any(t["Key"] == "Environment" and t["Value"] == "Production" for t in f.get("Tags", []))


def test_create_fleet_default_capacity_type_spot(ec2):
    """DefaultTargetCapacityType=spot drives lifecycle + capacity-slot accounting,
    not the top-level Type (which is {request, maintain, instant})."""
    lt = ec2.create_launch_template(
        LaunchTemplateName=f"spot-lt-{_uuid_mod.uuid4().hex[:8]}",
        LaunchTemplateData={"ImageId": "ami-deadbeef", "InstanceType": "t3.small"},
    )
    lt_id = lt["LaunchTemplate"]["LaunchTemplateId"]
    resp = ec2.create_fleet(
        LaunchTemplateConfigs=[{
            "LaunchTemplateSpecification": {"LaunchTemplateId": lt_id, "Version": "1"},
        }],
        TargetCapacitySpecification={
            "TotalTargetCapacity": 2,
            "SpotTargetCapacity": 2,
            "DefaultTargetCapacityType": "spot",
        },
        Type="instant",
    )
    desc = ec2.describe_fleets(FleetIds=[resp["FleetId"]])["Fleets"][0]
    tcs = desc["TargetCapacitySpecification"]
    assert tcs["DefaultTargetCapacityType"] == "spot"
    assert tcs["SpotTargetCapacity"] == 2
    assert tcs["OnDemandTargetCapacity"] == 0
    assert desc["Instances"][0]["Lifecycle"] == "spot"


def test_create_fleet_distributes_across_configs_and_overrides(ec2):
    """Multi-config × multi-override should produce one Instances[*] item
    per (config, override) bucket, with capacity round-robin'd across them."""
    lt1 = ec2.create_launch_template(
        LaunchTemplateName=f"multi-lt1-{_uuid_mod.uuid4().hex[:8]}",
        LaunchTemplateData={"ImageId": "ami-aaaaaaaa", "InstanceType": "t3.micro"},
    )["LaunchTemplate"]["LaunchTemplateId"]
    lt2 = ec2.create_launch_template(
        LaunchTemplateName=f"multi-lt2-{_uuid_mod.uuid4().hex[:8]}",
        LaunchTemplateData={"ImageId": "ami-bbbbbbbb", "InstanceType": "t3.micro"},
    )["LaunchTemplate"]["LaunchTemplateId"]
    resp = ec2.create_fleet(
        LaunchTemplateConfigs=[
            {
                "LaunchTemplateSpecification": {"LaunchTemplateId": lt1, "Version": "1"},
                "Overrides": [
                    {"InstanceType": "t3.small"},
                    {"InstanceType": "t3.medium"},
                ],
            },
            {
                "LaunchTemplateSpecification": {"LaunchTemplateId": lt2, "Version": "1"},
                "Overrides": [
                    {"InstanceType": "t3.large"},
                    {"InstanceType": "t3.xlarge"},
                ],
            },
        ],
        TargetCapacitySpecification={"TotalTargetCapacity": 4, "OnDemandTargetCapacity": 4},
        Type="instant",
    )
    instance_types = sorted(i["InstanceType"] for i in resp["Instances"])
    assert instance_types == ["t3.large", "t3.medium", "t3.small", "t3.xlarge"]
    total_ids = [iid for item in resp["Instances"] for iid in item["InstanceIds"]]
    assert len(total_ids) == 4


def test_create_fleet_maintain_returns_fleetid_only(ec2):
    """For Type=maintain (and request), AWS does not launch synchronously —
    response carries FleetId only; no Instances / no Errors."""
    lt = ec2.create_launch_template(
        LaunchTemplateName=f"maintain-lt-{_uuid_mod.uuid4().hex[:8]}",
        LaunchTemplateData={"ImageId": "ami-11111111", "InstanceType": "t2.micro"},
    )["LaunchTemplate"]["LaunchTemplateId"]
    resp = ec2.create_fleet(
        LaunchTemplateConfigs=[{
            "LaunchTemplateSpecification": {"LaunchTemplateId": lt, "Version": "1"},
        }],
        TargetCapacitySpecification={"TotalTargetCapacity": 3, "OnDemandTargetCapacity": 3},
        Type="maintain",
    )
    assert resp["FleetId"].startswith("fleet-")
    assert not resp.get("Instances")
    assert not resp.get("Errors")
    desc = ec2.describe_fleets(FleetIds=[resp["FleetId"]])["Fleets"][0]
    assert desc["Type"] == "maintain"
    assert desc["FulfilledCapacity"] == 0.0
    assert desc["ActivityStatus"] == "pending_fulfillment"
    assert desc.get("Instances", []) == []


def test_describe_fleets_unknown_id_returns_invalid_fleet_id(ec2):
    bogus = f"fleet-{_uuid_mod.uuid4().hex}"
    with pytest.raises(ClientError) as exc:
        ec2.describe_fleets(FleetIds=[bogus])
    assert exc.value.response["Error"]["Code"] == "InvalidFleetId.NotFound"


def test_describe_security_group_rules_by_id_without_group_filter(ec2):
    """Regression for #1121: Terraform's aws_vpc_security_group_ingress_rule
    refreshes by calling DescribeSecurityGroupRules(SecurityGroupRuleIds=[...])
    with no group filter, and the rule id must stay stable when other rules on
    the same group are revoked."""
    vpc = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]
    sg = ec2.create_security_group(
        GroupName=f"sgr-1121-{_uuid_mod.uuid4().hex[:8]}",
        Description="issue-1121",
        VpcId=vpc["VpcId"],
    )["GroupId"]

    rule_a = ec2.authorize_security_group_ingress(
        GroupId=sg,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
            "IpRanges": [{"CidrIp": "10.0.0.0/24"}],
        }],
    )["SecurityGroupRules"][0]["SecurityGroupRuleId"]
    rule_b = ec2.authorize_security_group_ingress(
        GroupId=sg,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 80, "ToPort": 80,
            "IpRanges": [{"CidrIp": "10.0.1.0/24"}],
        }],
    )["SecurityGroupRules"][0]["SecurityGroupRuleId"]

    assert rule_a.startswith("sgr-") and rule_b.startswith("sgr-")
    assert rule_a != rule_b

    # Refresh by id with NO group filter -- the exact call Terraform makes.
    found = ec2.describe_security_group_rules(
        SecurityGroupRuleIds=[rule_b],
    )["SecurityGroupRules"]
    assert [r["SecurityGroupRuleId"] for r in found] == [rule_b]

    # Revoking rule_a must not shift rule_b's id (index-based ids did).
    ec2.revoke_security_group_ingress(
        GroupId=sg,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
            "IpRanges": [{"CidrIp": "10.0.0.0/24"}],
        }],
    )
    still_found = ec2.describe_security_group_rules(
        SecurityGroupRuleIds=[rule_b],
    )["SecurityGroupRules"]
    assert [r["SecurityGroupRuleId"] for r in still_found] == [rule_b]

    # The revoked rule's id no longer resolves.
    assert ec2.describe_security_group_rules(
        SecurityGroupRuleIds=[rule_a],
    )["SecurityGroupRules"] == []


def test_security_group_rule_tags_and_arn_round_trip(ec2):
    """Regression for #1121 follow-up: DescribeSecurityGroupRules must return a
    rule's Tags and SecurityGroupRuleArn, so Terraform's
    aws_vpc_security_group_ingress_rule stops perpetually diffing tags_all."""
    vpc = ec2.create_vpc(CidrBlock="10.100.100.0/24")["Vpc"]["VpcId"]
    sg = ec2.create_security_group(
        GroupName=f"sgr-tags-{_uuid_mod.uuid4().hex[:8]}",
        Description="issue-1121-tags",
        VpcId=vpc,
    )["GroupId"]

    # The AWS provider tags the rule at authorize time via a security-group-rule
    # TagSpecification; the response echoes Tags and the ARN.
    created = ec2.authorize_security_group_ingress(
        GroupId=sg,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 5678, "ToPort": 5678,
            "IpRanges": [{"CidrIp": "10.100.100.0/24"}],
        }],
        TagSpecifications=[{
            "ResourceType": "security-group-rule",
            "Tags": [{"Key": "Name", "Value": "mytest-ingress-5678"}],
        }],
    )["SecurityGroupRules"][0]
    rule_id = created["SecurityGroupRuleId"]
    assert created["Tags"] == [{"Key": "Name", "Value": "mytest-ingress-5678"}]
    assert created["SecurityGroupRuleArn"] == (
        f"arn:aws:ec2:us-east-1:000000000000:security-group-rule/{rule_id}"
    )

    # The Terraform refresh path (describe by id) returns the same Tags + ARN.
    refreshed = ec2.describe_security_group_rules(
        SecurityGroupRuleIds=[rule_id],
    )["SecurityGroupRules"][0]
    assert refreshed["Tags"] == [{"Key": "Name", "Value": "mytest-ingress-5678"}]
    assert refreshed["SecurityGroupRuleArn"] == created["SecurityGroupRuleArn"]

    # tag: filter selects the rule, and CreateTags on the sgr- id also applies.
    assert len(ec2.describe_security_group_rules(
        Filters=[{"Name": "tag:Name", "Values": ["mytest-ingress-5678"]}],
    )["SecurityGroupRules"]) == 1
    ec2.create_tags(Resources=[rule_id], Tags=[{"Key": "Env", "Value": "test"}])
    tags_now = {t["Key"]: t["Value"] for t in ec2.describe_security_group_rules(
        SecurityGroupRuleIds=[rule_id],
    )["SecurityGroupRules"][0]["Tags"]}
    assert tags_now == {"Name": "mytest-ingress-5678", "Env": "test"}

    # Revoking the rule drops its tags.
    ec2.revoke_security_group_ingress(
        GroupId=sg,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 5678, "ToPort": 5678,
            "IpRanges": [{"CidrIp": "10.100.100.0/24"}],
        }],
    )
    assert ec2.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [rule_id]}],
    )["Tags"] == []


def test_security_group_rule_description_round_trip(ec2):
    """DescribeSecurityGroupRules must return the top-level rule Description."""
    vpc = ec2.create_vpc(CidrBlock="10.101.0.0/24")['Vpc']['VpcId']
    sg = ec2.create_security_group(
        GroupName=f"sgr-desc-{_uuid_mod.uuid4().hex[:8]}",
        Description="issue-1121-desc",
        VpcId=vpc,
    )["GroupId"]

    created = ec2.authorize_security_group_ingress(
        GroupId=sg,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 5678, "ToPort": 5678,
            "IpRanges": [{"CidrIp": "10.101.0.0/24", "Description": "allow app traffic"}],
        }],
    )["SecurityGroupRules"][0]

    described = ec2.describe_security_group_rules(
        SecurityGroupRuleIds=[created["SecurityGroupRuleId"]],
    )["SecurityGroupRules"]
    assert len(described) == 1
    assert described[0]["SecurityGroupRuleId"] == created["SecurityGroupRuleId"]
    assert described[0]["Description"] == "allow app traffic"

    ec2.delete_security_group(GroupId=sg)
    ec2.delete_vpc(VpcId=vpc)


def test_modify_security_group_rules_updates_description(ec2):
    """ModifySecurityGroupRules must update rule fields by rule id."""
    vpc = ec2.create_vpc(CidrBlock="10.102.0.0/24")["Vpc"]["VpcId"]
    sg = ec2.create_security_group(
        GroupName=f"sgr-mod-{_uuid_mod.uuid4().hex[:8]}",
        Description="issue-modify-sgr",
        VpcId=vpc,
    )["GroupId"]

    created = ec2.authorize_security_group_ingress(
        GroupId=sg,
        IpPermissions=[{
            "IpProtocol": "tcp", "FromPort": 5678, "ToPort": 5678,
            "IpRanges": [{"CidrIp": "10.102.0.0/24", "Description": "old desc"}],
        }],
    )["SecurityGroupRules"][0]
    rule_id = created["SecurityGroupRuleId"]

    ec2.modify_security_group_rules(
        GroupId=sg,
        SecurityGroupRules=[{
            "SecurityGroupRuleId": rule_id,
            "SecurityGroupRule": {
                "Description": "new desc",
                "FromPort": 5679,
                "ToPort": 5679,
            },
        }],
    )

    described = ec2.describe_security_group_rules(
        SecurityGroupRuleIds=[rule_id],
    )["SecurityGroupRules"]
    assert len(described) == 1
    assert described[0]["SecurityGroupRuleId"] == rule_id
    assert described[0]["Description"] == "new desc"
    assert described[0]["FromPort"] == 5679
    assert described[0]["ToPort"] == 5679

    ec2.delete_security_group(GroupId=sg)
    ec2.delete_vpc(VpcId=vpc)



def test_ec2_public_ip_and_dns_surface_via_sdk(ec2):
    """The public address must ride the real wire tags (ipAddress/dnsName) —
    the previous publicIpAddress/publicDnsName tags are not in the EC2 schema,
    so boto3 silently dropped both fields and PublicIpAddress was always None."""
    iid = ec2.run_instances(ImageId="ami-0abcdef1234567890", MinCount=1,
                            MaxCount=1)["Instances"][0]["InstanceId"]
    try:
        inst = ec2.describe_instances(InstanceIds=[iid])["Reservations"][0]["Instances"][0]
        assert inst.get("PublicIpAddress"), "PublicIpAddress missing from the wire"
        assert inst.get("PublicDnsName"), "PublicDnsName missing from the wire"
        assert inst["PrivateIpAddress"]
        # The value has to be an address, not just present. Fixing the tags is
        # what made this reachable: while the field never left the server, a
        # malformed one could not be noticed.
        ipaddress.ip_address(inst["PublicIpAddress"])
        ipaddress.ip_address(inst["PrivateIpAddress"])
    finally:
        ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_allocated_address_is_a_valid_ipv4(ec2):
    """AllocateAddress hands back `PublicIp` verbatim, so a generator that
    completes a one-octet prefix with only two octets produces `52.55.218`,
    which no address parser accepts."""
    alloc = ec2.allocate_address(Domain="vpc")
    try:
        ipaddress.ip_address(alloc["PublicIp"])
        described = ec2.describe_addresses(
            AllocationIds=[alloc["AllocationId"]])["Addresses"][0]
        ipaddress.ip_address(described["PublicIp"])
    finally:
        ec2.release_address(AllocationId=alloc["AllocationId"])


# ---------------------------------------------------------------------------
# Registered AMIs and container-backed instances
# ---------------------------------------------------------------------------
# Unit tier: in-process against a faked Docker client, so it runs with no
# daemon. Live tier: gated on a reachable daemon, probed rather than read from
# an env var CI never sets.

import ministack.services.ec2 as ec2mod


def _docker_reachable():
    try:
        import docker as _probe

        _probe.from_env(timeout=5).ping()
        return True
    except Exception:
        return False


requires_docker = pytest.mark.skipif(
    not _docker_reachable(), reason="no reachable Docker daemon")


class _FakeContainer:
    def __init__(self, name, kwargs, registry, ip="172.30.0.9", network="testnet",
                 shell=True):
        self.id = f"cid-{name}"
        self.name = name
        self.kwargs = kwargs
        self.status = "running"
        self.attrs = {"NetworkSettings": {"Networks": {network: {"IPAddress": ip}}}}
        self.execs = []
        self.stop_calls = self.start_calls = self.restart_calls = self.remove_calls = 0
        self._registry = registry
        self._shell = shell

    def reload(self):
        pass

    def exec_run(self, cmd, demux=False, **kw):
        self.execs.append(cmd)
        if not self._shell:
            raise RuntimeError("no shell in image")
        out = (b"out", b"err") if demux else b"out"

        class _R:
            exit_code = 0
            output = out
        return _R()

    def start(self):
        self.start_calls += 1
        self.status = "running"

    def stop(self, timeout=10):
        self.stop_calls += 1
        self.status = "exited"

    def restart(self, timeout=10):
        self.restart_calls += 1

    def remove(self, force=False, v=False):
        self.remove_calls += 1
        self._registry.pop(self.id, None)
        self._registry.pop(self.name, None)


def _fake_docker(run_error=None, shell=True, entrypoint=None):
    registry, runs = {}, []

    class _FakeImage:
        attrs = {"Config": {"Entrypoint": entrypoint}}

    class _Images:
        def get(self, ref):
            return _FakeImage()

        def pull(self, ref):
            return _FakeImage()

    class _Containers:
        def run(self, image, **kwargs):
            if run_error is not None:
                raise run_error
            runs.append({"image": image, **kwargs})
            c = _FakeContainer(kwargs["name"], kwargs, registry, shell=shell)
            registry[c.id] = registry[c.name] = c
            return c

        def get(self, ref):
            if ref not in registry:
                from docker import errors
                raise errors.NotFound("absent")
            return registry[ref]

        def list(self, all=False, filters=None):
            want = (filters or {}).get("label") or []
            want = [want] if isinstance(want, str) else list(want)
            out = []
            for c in set(registry.values()):
                have = {f"{k}={v}" for k, v in c.kwargs.get("labels", {}).items()}
                if not [w for w in want if w not in have]:
                    out.append(c)
            return out

    class _Docker:
        def __init__(self):
            self.containers = _Containers()
            self.images = _Images()

        def ping(self):
            return True

    d = _Docker()
    d._registry, d._runs = registry, runs
    return d


@pytest.fixture
def vm(monkeypatch):
    """A faked Docker daemon wired into the ec2 module, with state torn down."""
    fake = _fake_docker()
    monkeypatch.setattr(ec2mod, "_get_docker", lambda: fake)
    monkeypatch.setattr(ec2mod, "_get_ministack_network", lambda _c: "testnet")
    created = []
    yield fake, created
    for iid in created:
        ec2mod._instances.pop(iid, None)
    for key in list(ec2mod._images.keys()):
        ec2mod._images.pop(key, None)


def _register(location="example/box:1", name="box"):
    resp = ec2mod._register_image({"Name": [name], "ImageLocation": [location]})
    import re as _re
    return _re.search(r"<imageId>(ami-[0-9a-f]+)</imageId>", resp[2].decode()).group(1)


def _launch(ami, count=1):
    resp = ec2mod._run_instances({"ImageId": [ami], "MinCount": [str(count)],
                                  "MaxCount": [str(count)]})
    import re as _re
    body = resp[2].decode()
    return _re.findall(r"<instanceId>(i-[0-9a-f]+)</instanceId>", body), resp


def test_ec2_register_image_round_trips(ec2):
    reg = ec2.register_image(Name=f"box-{_uuid_mod.uuid4().hex[:8]}",
                             ImageLocation="alpine:3", Description="desc",
                             Architecture="x86_64")
    ami = reg["ImageId"]
    try:
        assert ami.startswith("ami-") and len(ami) == len("ami-") + 17
        img = ec2.describe_images(ImageIds=[ami])["Images"][0]
        assert img["State"] == "available"
        # A container has no persistent root disk: instance-store semantics,
        # so no EBS root mapping either.
        assert img["RootDeviceType"] == "instance-store"
        assert img["RootDeviceName"] == "/dev/sda1"
        assert img.get("BlockDeviceMappings", []) == []
        assert img["Description"] == "desc"
        assert img["Public"] is False
    finally:
        ec2.deregister_image(ImageId=ami)
    with pytest.raises(ClientError) as exc:
        ec2.deregister_image(ImageId=ami)
    assert exc.value.response["Error"]["Code"] == "InvalidAMIID.NotFound"


def test_ec2_register_image_accepts_a_bare_name(ec2):
    # Only Name is required on AWS — ImageLocation and BlockDeviceMapping.N are both optional and
    # registration never checks the result can boot. Refusing here would be stricter than AWS,
    # which is the failure mode that bites: code that works against AWS breaking locally.
    ami = ec2.register_image(Name=f"bare-{_uuid_mod.uuid4().hex[:8]}")["ImageId"]
    try:
        img = ec2.describe_images(ImageIds=[ami])["Images"][0]
        assert img["RootDeviceType"] == "ebs"
        # Documented RegisterImage defaults.
        assert img["Architecture"] == "i386"
        assert img["VirtualizationType"] == "paravirtual"
        # Nothing to boot, so the failure lands on the call that cannot do it.
        with pytest.raises(ClientError) as exc:
            ec2.run_instances(ImageId=ami, MinCount=1, MaxCount=1)
        assert exc.value.response["Error"]["Code"] == "InvalidAMIID.Unavailable"
    finally:
        ec2.deregister_image(ImageId=ami)


def test_ec2_register_image_rejects_a_malformed_name(ec2):
    # "AMI names must be between 3 and 128 characters long, and may only contain letters,
    # numbers, and the following special characters..."
    for bad in ("a" * 129, "no,commas", "semi;colon"):
        with pytest.raises(ClientError) as exc:
            ec2.register_image(Name=bad, ImageLocation="alpine:3")
        assert exc.value.response["Error"]["Code"] == "InvalidAMIName.Malformed", bad


def test_ec2_describe_images_filters_registered_and_stubs(ec2):
    unique = _uuid_mod.uuid4().hex[:8]
    ami = ec2.register_image(Name=f"filtered-{unique}", ImageLocation="alpine:3")["ImageId"]
    try:
        by_name = ec2.describe_images(
            Filters=[{"Name": "name", "Values": [f"filtered-{unique}"]}])["Images"]
        assert [i["ImageId"] for i in by_name] == [ami]
        wildcard = ec2.describe_images(
            Filters=[{"Name": "name", "Values": ["filtered-*"]}])["Images"]
        assert ami in [i["ImageId"] for i in wildcard]
        assert ec2.describe_images(
            Filters=[{"Name": "name", "Values": ["nothing-matches"]}])["Images"] == []
        # The stub AMIs are still listed, so a workload naming one still works.
        assert len(ec2.describe_images()["Images"]) >= 4
    finally:
        ec2.deregister_image(ImageId=ami)


def test_ec2_unregistered_ami_never_touches_docker(monkeypatch):
    def _boom():
        raise AssertionError("Docker must not be reached with no image registered")

    monkeypatch.setattr(ec2mod, "_get_docker", _boom)
    iids, _ = _launch("ami-00000000")
    try:
        inst = ec2mod._instances[iids[0]]
        assert "VmManager" not in inst
        assert "_container_id" not in inst
    finally:
        for iid in iids:
            ec2mod._instances.pop(iid, None)


def test_ec2_registered_ami_boots_a_container(vm):
    fake, created = vm
    ami = _register("example/box:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    assert len(fake._runs) == 1
    run = fake._runs[0]
    assert run["image"] == "example/box:1"
    assert run["init"] is True
    # A real instance does not exit the moment it boots.
    assert run["command"] == ["sleep", "infinity"]
    assert run["network"] == "testnet"
    assert run["labels"]["ministack"] == "ec2"
    assert run["labels"]["instance_id"] == iids[0]
    inst = ec2mod._instances[iids[0]]
    assert inst["VmManager"] == "docker"
    assert inst["PrivateIpAddress"] == "172.30.0.9"
    assert inst["PublicIpAddress"] == "172.30.0.9"
    assert inst["PrivateDnsName"] == "ip-172-30-0-9.ec2.internal"


def test_ec2_parse_docker_flags():
    kwargs = ec2mod._parse_ec2_docker_flags(
        "--privileged -e A=1 --env B=two -v /h:/c:ro --cap-add SYS_ADMIN "
        "--tmpfs /run:rw,size=64m --add-host me:10.0.0.1 -m 512m --shm-size 128m "
        "--init=false --bogus-flag xyz")
    assert kwargs["privileged"] is True
    assert kwargs["environment"] == {"A": "1", "B": "two"}
    assert kwargs["volumes"] == ["/h:/c:ro"]
    assert kwargs["cap_add"] == ["SYS_ADMIN"]
    assert kwargs["tmpfs"] == {"/run": "rw,size=64m"}
    assert kwargs["extra_hosts"] == {"me": "10.0.0.1"}
    assert kwargs["mem_limit"] == "512m"
    assert kwargs["shm_size"] == "128m"
    # --init is refused, never forwarded: instance containers always run with init.
    assert "init" not in kwargs
    assert ec2mod._parse_ec2_docker_flags("") == {}
    # Malformed input is ignored rather than raising or exiting inside the server:
    # a flag missing its value trips argparse's error path, an unbalanced quote
    # trips shlex.
    for bad in ("-m", "--cap-add", "'unbalanced"):
        assert ec2mod._parse_ec2_docker_flags(bad) == {}


def test_ec2_docker_flags_reach_the_container(vm, monkeypatch):
    fake, created = vm
    monkeypatch.setattr(ec2mod, "EC2_DOCKER_FLAGS", "--privileged --cap-add SYS_ADMIN")
    ami = _register("example/box:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    run = fake._runs[0]
    assert run["privileged"] is True
    assert run["cap_add"] == ["SYS_ADMIN"]
    assert run["init"] is True


def test_ec2_failed_boot_backs_every_record_out(monkeypatch):
    fake = _fake_docker(run_error=RuntimeError("no such image"))
    monkeypatch.setattr(ec2mod, "_get_docker", lambda: fake)
    monkeypatch.setattr(ec2mod, "_get_ministack_network", lambda _c: "testnet")
    ami = _register("example/missing:1")
    try:
        volumes_before = len(list(ec2mod._volumes.keys()))
        instances_before = len(list(ec2mod._instances.keys()))
        resp = ec2mod._run_instances({"ImageId": [ami], "MinCount": ["2"], "MaxCount": ["2"]})
        assert resp[0] == 500
        assert b"InternalError" in resp[2]
        # No phantom instance, and no orphaned synthetic root volume.
        assert len(list(ec2mod._instances.keys())) == instances_before
        assert len(list(ec2mod._volumes.keys())) == volumes_before
    finally:
        ec2mod._images.pop(ami, None)


def test_ec2_dead_container_reports_terminated(vm):
    fake, created = vm
    ami = _register("example/box:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    inst = ec2mod._instances[iids[0]]
    fake._registry[inst["_container_id"]].status = "exited"
    ec2mod._vm_reconcile_running([inst])
    # Instance-store backed: a lost root disk is a lost instance, so AWS
    # terminates rather than stops.
    assert inst["State"]["Name"] == "terminated"
    assert "_container_id" not in inst


def test_ec2_container_backed_instance_cannot_stop_or_start(vm):
    _fake, created = vm
    ami = _register("example/box:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    for handler in (ec2mod._stop_instances, ec2mod._start_instances):
        resp = handler({"InstanceId.1": [iids[0]]})
        assert resp[0] == 400
        assert b"UnsupportedOperation" in resp[2]
        assert b"instance store-backed" in resp[2]


def test_ec2_container_backed_instance_is_instance_store(vm):
    _fake, created = vm
    ami = _register("example/box:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    inst = ec2mod._instances[iids[0]]
    # No EBS root volume exists for an instance-store instance.
    assert inst["RootDeviceType"] == "instance-store"
    assert inst["BlockDeviceMappings"] == []


def test_ec2_snapshot_backed_registration_is_accepted(ec2):
    # Registration is metadata-only on AWS: it does not check the image can
    # boot, so neither do we. import-snapshot then register-image is the
    # ordinary Packer/Terraform flow.
    snap = ec2.create_snapshot(VolumeId=ec2.create_volume(
        AvailabilityZone="us-east-1a", Size=8)["VolumeId"])["SnapshotId"]
    ami = ec2.register_image(
        Name=f"snap-{_uuid_mod.uuid4().hex[:8]}",
        RootDeviceName="/dev/xvda",
        BlockDeviceMappings=[{"DeviceName": "/dev/xvda",
                              "Ebs": {"SnapshotId": snap, "VolumeSize": 8}}])["ImageId"]
    try:
        img = ec2.describe_images(ImageIds=[ami])["Images"][0]
        assert img["RootDeviceType"] == "ebs"
        assert img["BlockDeviceMappings"][0]["Ebs"]["SnapshotId"] == snap
        # The divergence lands on the call that genuinely cannot do the thing.
        with pytest.raises(ClientError) as exc:
            ec2.run_instances(ImageId=ami, MinCount=1, MaxCount=1)
        assert exc.value.response["Error"]["Code"] == "InvalidAMIID.Unavailable"
        assert not [r for r in ec2.describe_instances(
            Filters=[{"Name": "image-id", "Values": [ami]}])["Reservations"]]
    finally:
        ec2.deregister_image(ImageId=ami)


def test_ec2_terminate_removes_the_container(vm):
    fake, created = vm
    ami = _register("example/box:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    cid = ec2mod._instances[iids[0]]["_container_id"]
    ec2mod._terminate_instances({"InstanceId.1": [iids[0]]})
    assert cid not in fake._registry


def test_ec2_state_round_trip_reports_restored_boxes_stopped(vm):
    fake, created = vm
    ami = _register("example/box:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    state = ec2mod.get_state()
    saved = state["instances"].get(iids[0])
    # Container handles would dangle after a restart; the image id must survive
    # so StartInstances can relaunch.
    assert "_container_id" not in saved
    assert saved["VmManager"] == "docker"
    assert saved["ImageId"] == ami
    ec2mod._reconcile_backed_instances_after_restore()
    assert ec2mod._instances[iids[0]]["State"]["Name"] == "terminated"


def test_ec2_instance_without_a_shell_is_unmanaged(monkeypatch):
    fake = _fake_docker(shell=False)
    monkeypatch.setattr(ec2mod, "_get_docker", lambda: fake)
    monkeypatch.setattr(ec2mod, "_get_ministack_network", lambda _c: "testnet")
    ami = _register("example/distroless:1")
    iids, _ = _launch(ami)
    try:
        # No shell is this emulator's analogue of no SSM agent: AWS refuses
        # commands for an unmanaged instance and so do we.
        assert ec2mod.instance_is_managed(iids[0]) is False
    finally:
        for iid in iids:
            ec2mod._instances.pop(iid, None)
        ec2mod._images.pop(ami, None)


def test_ec2_managed_instance_execs(vm):
    fake, created = vm
    ami = _register("example/box:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    assert ec2mod.instance_is_managed(iids[0]) is True
    code, out, err = ec2mod.exec_in_instance_blocking(iids[0], ["/bin/sh", "-c", "true"])
    assert (code, out, err) == (0, "out", "err")


def test_ec2_exec_on_instance_without_a_box_fails(vm):
    _fake, created = vm
    iids, _ = _launch("ami-00000000")
    created.extend(iids)
    code, _out, err = ec2mod.exec_in_instance_blocking(iids[0], ["/bin/sh", "-c", "true"])
    assert code == 255
    assert "no backing container" in err


@requires_docker
def test_ec2_docker_end_to_end_live(ec2):
    ami = ec2.register_image(Name=f"live-{_uuid_mod.uuid4().hex[:8]}",
                             ImageLocation="alpine:3")["ImageId"]
    iid = None
    try:
        iid = ec2.run_instances(ImageId=ami, MinCount=1,
                                MaxCount=1)["Instances"][0]["InstanceId"]
        inst = ec2.describe_instances(
            InstanceIds=[iid])["Reservations"][0]["Instances"][0]
        assert inst["State"]["Name"] == "running"
        # The reported address is the container's, not a synthetic 10.x.
        assert not inst["PrivateIpAddress"].startswith("10.0.")
        import docker
        client = docker.from_env(timeout=10)
        assert [c for c in client.containers.list() if iid in c.name]
        ec2.terminate_instances(InstanceIds=[iid])
        iid = None
        time.sleep(1)
        assert not [c for c in client.containers.list(all=True) if ami in c.name]
    finally:
        if iid:
            ec2.terminate_instances(InstanceIds=[iid])
        ec2.deregister_image(ImageId=ami)


def test_ec2_ami_names_are_unique_per_account_and_region(ec2):
    """AMI names are unique per account per region on AWS, not globally."""
    name = f"shared-{_uuid_mod.uuid4().hex[:8]}"
    ami = ec2.register_image(Name=name, ImageLocation="alpine:3")["ImageId"]
    other_account = _ec2_client("us-east-1", account_id="123456789012")
    other_region = _ec2_client("eu-west-1")
    ami_other_account = ami_other_region = None
    try:
        with pytest.raises(ClientError) as exc:
            ec2.register_image(Name=name, ImageLocation="alpine:3")
        assert exc.value.response["Error"]["Code"] == "InvalidAMIName.Duplicate"
        # The same name is free in another account and in another region.
        ami_other_account = other_account.register_image(
            Name=name, ImageLocation="alpine:3")["ImageId"]
        ami_other_region = other_region.register_image(
            Name=name, ImageLocation="alpine:3")["ImageId"]
        assert len({ami, ami_other_account, ami_other_region}) == 3
        # And they do not leak into each other's DescribeImages.
        assert ami not in [i["ImageId"] for i in other_account.describe_images(
            Filters=[{"Name": "name", "Values": [name]}])["Images"]]
    finally:
        ec2.deregister_image(ImageId=ami)
        if ami_other_account:
            other_account.deregister_image(ImageId=ami_other_account)
        if ami_other_region:
            other_region.deregister_image(ImageId=ami_other_region)


def test_ec2_name_is_free_again_after_deregister(ec2):
    name = f"recycled-{_uuid_mod.uuid4().hex[:8]}"
    first = ec2.register_image(Name=name, ImageLocation="alpine:3")["ImageId"]
    ec2.deregister_image(ImageId=first)
    second = ec2.register_image(Name=name, ImageLocation="alpine:3")["ImageId"]
    try:
        assert second != first
        assert [i["ImageId"] for i in ec2.describe_images(
            Filters=[{"Name": "name", "Values": [name]}])["Images"]] == [second]
    finally:
        ec2.deregister_image(ImageId=second)


def test_ec2_describe_images_filters_apply_to_the_stubs(ec2):
    """The stubs now flow through the same filter path as registered images."""
    ami = ec2.register_image(Name=f"lin-{_uuid_mod.uuid4().hex[:8]}",
                             ImageLocation="alpine:3")["ImageId"]
    try:
        windows = ec2.describe_images(
            Filters=[{"Name": "platform", "Values": ["windows"]}])["Images"]
        # Only the Windows stub carries a platform; a registered image has none.
        assert [i["ImageId"] for i in windows] == ["ami-0fedcba9876543210"]
        assert ami not in [i["ImageId"] for i in windows]

        public = ec2.describe_images(
            Filters=[{"Name": "is-public", "Values": ["true"]}])["Images"]
        assert {i["ImageId"] for i in public} == {
            "ami-0abcdef1234567890", "ami-0123456789abcdef0", "ami-0fedcba9876543210"}

        private = ec2.describe_images(
            Filters=[{"Name": "is-public", "Values": ["false"]}])["Images"]
        assert ami in [i["ImageId"] for i in private]

        # root-device-type separates the two kinds.
        ebs = [i["ImageId"] for i in ec2.describe_images(
            Filters=[{"Name": "root-device-type", "Values": ["ebs"]}])["Images"]]
        assert ami not in ebs and "ami-0abcdef1234567890" in ebs
    finally:
        ec2.deregister_image(ImageId=ami)


def test_ec2_container_instance_creates_no_root_volume(vm):
    """Instance-store means no synthetic root volume, and nothing may quietly
    add one back — it would contradict the stop/start refusal."""
    _fake, created = vm
    before = {v["VolumeId"] for v in ec2mod._volumes.values()}
    ami = _register("example/box:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    after = {v["VolumeId"] for v in ec2mod._volumes.values()}
    assert after == before
    inst = ec2mod._instances[iids[0]]
    assert inst["BlockDeviceMappings"] == []
    assert inst["RootDeviceType"] == "instance-store"
    assert not [v for v in ec2mod._volumes.values()
                for a in v.get("Attachments", []) if a.get("InstanceId") == iids[0]]


def test_ec2_unpullable_image_is_a_client_error(ec2):
    """A reference that cannot be pulled is the caller's problem, not the
    server's — retrying an InternalError here would fail identically forever."""
    ami = ec2.register_image(Name=f"nopull-{_uuid_mod.uuid4().hex[:8]}",
                             ImageLocation="ministack-nonexistent/nope:404")["ImageId"]
    try:
        with pytest.raises(ClientError) as exc:
            ec2.run_instances(ImageId=ami, MinCount=1, MaxCount=1)
        err = exc.value.response["Error"]
        assert err["Code"] == "InvalidAMIID.Unavailable"
        assert "could not be pulled" in err["Message"]
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    finally:
        ec2.deregister_image(ImageId=ami)


def test_ec2_deregister_does_not_orphan_a_running_box(vm):
    """A registration can be withdrawn while its instances are still running, so the
    describe-time reconcile must not be gated on the registry still holding the image."""
    fake, created = vm
    ami = _register("example/box:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    ec2mod._deregister_image({"ImageId": [ami]})
    assert not ec2mod._any_image_registered()
    # Still true, because a container of ours is out there.
    assert ec2mod._docker_touched()
    inst = ec2mod._instances[iids[0]]
    fake._registry[inst["_container_id"]].status = "exited"
    ec2mod._vm_reconcile_running([inst])
    assert inst["State"]["Name"] == "terminated"


def test_ec2_base_image_gets_a_keepalive(vm):
    """A base image's command is a shell that exits at once, so the box needs keeping alive."""
    fake, created = vm
    ami = _register("example/base:1")
    iids, _ = _launch(ami)
    created.extend(iids)
    assert fake._runs[0]["command"] == ["sleep", "infinity"]


def test_ec2_image_with_an_entrypoint_is_left_alone(monkeypatch):
    """Overriding the command of an image built to run a service would stop it ever starting."""
    fake = _fake_docker(entrypoint=["/docker-entrypoint.sh"])
    monkeypatch.setattr(ec2mod, "_get_docker", lambda: fake)
    monkeypatch.setattr(ec2mod, "_get_ministack_network", lambda _c: "testnet")
    ami = _register("example/service:1")
    iids, _ = _launch(ami)
    try:
        assert "command" not in fake._runs[0]
        assert fake._runs[0]["init"] is True
    finally:
        for iid in iids:
            ec2mod._instances.pop(iid, None)
        ec2mod._images.pop(ami, None)


def test_ec2_describe_images_scopes_by_owner(ec2):
    """Owner.N: account ids and `self` select ours; an alias or another account selects nothing."""
    ami = ec2.register_image(Name=f"owned-{_uuid_mod.uuid4().hex[:8]}",
                             ImageLocation="alpine:3")["ImageId"]
    try:
        assert ami in [i["ImageId"] for i in ec2.describe_images(Owners=["self"])["Images"]]
        assert ami in [i["ImageId"] for i in
                       ec2.describe_images(Owners=["000000000000"])["Images"]]
        for scope in (["amazon"], ["aws-marketplace"], ["123456789012"]):
            assert ec2.describe_images(Owners=scope)["Images"] == [], scope
    finally:
        ec2.deregister_image(ImageId=ami)


def test_ec2_describe_images_scopes_by_executable_by(ec2):
    """ExecutableBy.N: `all` is the public images; nothing is shared with anyone here."""
    ami = ec2.register_image(Name=f"exec-{_uuid_mod.uuid4().hex[:8]}",
                             ImageLocation="alpine:3")["ImageId"]
    try:
        public = [i["ImageId"] for i in ec2.describe_images(ExecutableUsers=["all"])["Images"]]
        assert "ami-0abcdef1234567890" in public
        assert ami not in public
        assert ec2.describe_images(ExecutableUsers=["self"])["Images"] == []
    finally:
        ec2.deregister_image(ImageId=ami)
