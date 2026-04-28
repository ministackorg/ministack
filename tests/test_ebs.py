import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


def test_ebs_create_and_describe_volume(ebs):
    resp = ebs.create_volume(
        AvailabilityZone="us-east-1a",
        Size=20,
        VolumeType="gp3",
    )
    vol_id = resp["VolumeId"]
    assert vol_id.startswith("vol-")
    assert resp["State"] == "available"
    assert resp["Size"] == 20
    assert resp["VolumeType"] == "gp3"

    desc = ebs.describe_volumes(VolumeIds=[vol_id])
    assert len(desc["Volumes"]) == 1
    assert desc["Volumes"][0]["VolumeId"] == vol_id

def test_ebs_attach_detach_volume(ebs):
    inst = ebs.run_instances(ImageId="ami-00000001", MinCount=1, MaxCount=1)
    instance_id = inst["Instances"][0]["InstanceId"]

    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]

    ebs.attach_volume(VolumeId=vol_id, InstanceId=instance_id, Device="/dev/xvdf")
    desc = ebs.describe_volumes(VolumeIds=[vol_id])
    assert desc["Volumes"][0]["State"] == "in-use"
    assert desc["Volumes"][0]["Attachments"][0]["InstanceId"] == instance_id

    ebs.detach_volume(VolumeId=vol_id)
    desc2 = ebs.describe_volumes(VolumeIds=[vol_id])
    assert desc2["Volumes"][0]["State"] == "available"
    assert desc2["Volumes"][0]["Attachments"] == []

def test_ebs_delete_volume(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=5, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    ebs.delete_volume(VolumeId=vol_id)
    with pytest.raises(ClientError) as exc:
        ebs.describe_volumes(VolumeIds=[vol_id])
    assert exc.value.response["Error"]["Code"] == "InvalidVolume.NotFound"

def test_ebs_modify_volume(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    resp = ebs.modify_volume(VolumeId=vol_id, Size=50, VolumeType="gp3")
    assert resp["VolumeModification"]["TargetSize"] == 50
    assert resp["VolumeModification"]["TargetVolumeType"] == "gp3"

def test_ebs_volume_status(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=8, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    resp = ebs.describe_volume_status(VolumeIds=[vol_id])
    assert len(resp["VolumeStatuses"]) == 1
    assert resp["VolumeStatuses"][0]["VolumeStatus"]["Status"] == "ok"

def test_ebs_create_and_describe_snapshot(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    snap = ebs.create_snapshot(VolumeId=vol_id, Description="test snapshot")
    snap_id = snap["SnapshotId"]
    assert snap_id.startswith("snap-")
    assert snap["State"] == "completed"

    desc = ebs.describe_snapshots(SnapshotIds=[snap_id])
    assert len(desc["Snapshots"]) == 1
    assert desc["Snapshots"][0]["VolumeId"] == vol_id
    assert desc["Snapshots"][0]["Description"] == "test snapshot"

def test_ebs_delete_snapshot(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    snap = ebs.create_snapshot(VolumeId=vol["VolumeId"])
    snap_id = snap["SnapshotId"]
    ebs.delete_snapshot(SnapshotId=snap_id)
    with pytest.raises(ClientError) as exc:
        ebs.describe_snapshots(SnapshotIds=[snap_id])
    assert exc.value.response["Error"]["Code"] == "InvalidSnapshot.NotFound"

def test_ebs_copy_snapshot(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    snap = ebs.create_snapshot(VolumeId=vol["VolumeId"], Description="original")
    snap_id = snap["SnapshotId"]
    copy = ebs.copy_snapshot(SourceRegion="us-east-1", SourceSnapshotId=snap_id, Description="copy")
    new_snap_id = copy["SnapshotId"]
    assert new_snap_id != snap_id
    assert new_snap_id.startswith("snap-")

def test_ebs_snapshot_attribute(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    snap = ebs.create_snapshot(VolumeId=vol["VolumeId"], Description="attr test")
    snap_id = snap["SnapshotId"]

    ebs.modify_snapshot_attribute(
        SnapshotId=snap_id,
        Attribute="createVolumePermission",
        OperationType="add",
        UserIds=["123456789012"],
    )
    resp = ebs.describe_snapshot_attribute(
        SnapshotId=snap_id, Attribute="createVolumePermission"
    )
    assert resp["SnapshotId"] == snap_id
    assert any(
        p.get("UserId") == "123456789012"
        for p in resp.get("CreateVolumePermissions", [])
    )

def test_ebs_volume_attribute(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    resp = ebs.describe_volume_attribute(VolumeId=vol_id, Attribute="autoEnableIO")
    assert resp["VolumeId"] == vol_id
    assert "AutoEnableIO" in resp

def test_ebs_describe_volumes_modifications(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    ebs.modify_volume(VolumeId=vol_id, Size=50, VolumeType="gp3")
    resp = ebs.describe_volumes_modifications(VolumeIds=[vol_id])
    mods = resp["VolumesModifications"]
    assert len(mods) >= 1
    assert mods[0]["VolumeId"] == vol_id
    assert mods[0]["TargetSize"] == 50
    assert mods[0]["TargetVolumeType"] == "gp3"


def test_ebs_enable_volume_io(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    ebs.enable_volume_io(VolumeId=vol_id)
    # Stub — just verify it doesn't error
    resp = ebs.describe_volume_attribute(VolumeId=vol_id, Attribute="autoEnableIO")
    assert resp["VolumeId"] == vol_id


def test_ebs_modify_volume_attribute(ebs):
    vol = ebs.create_volume(AvailabilityZone="us-east-1a", Size=10, VolumeType="gp2")
    vol_id = vol["VolumeId"]
    ebs.modify_volume_attribute(VolumeId=vol_id, AutoEnableIO={"Value": True})
    # Stub — just verify it doesn't error
    resp = ebs.describe_volume_attribute(VolumeId=vol_id, Attribute="autoEnableIO")
    assert resp["VolumeId"] == vol_id
