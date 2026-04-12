import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
from botocore.exceptions import ClientError
import uuid as _uuid_mod

def test_efs_create_and_describe_filesystem(efs):
    resp = efs.create_file_system(
        PerformanceMode="generalPurpose",
        ThroughputMode="bursting",
        Encrypted=False,
        Tags=[{"Key": "Name", "Value": "test-fs"}],
    )
    fs_id = resp["FileSystemId"]
    assert fs_id.startswith("fs-")
    assert resp["LifeCycleState"] == "available"
    assert resp["ThroughputMode"] == "bursting"

    desc = efs.describe_file_systems(FileSystemId=fs_id)
    assert len(desc["FileSystems"]) == 1
    assert desc["FileSystems"][0]["FileSystemId"] == fs_id
    assert desc["FileSystems"][0]["Name"] == "test-fs"

def test_efs_creation_token_idempotency(efs):
    token = "unique-token-abc123"
    r1 = efs.create_file_system(CreationToken=token)
    r2 = efs.create_file_system(CreationToken=token)
    assert r1["FileSystemId"] == r2["FileSystemId"]

def test_efs_delete_filesystem(efs):
    resp = efs.create_file_system()
    fs_id = resp["FileSystemId"]
    efs.delete_file_system(FileSystemId=fs_id)
    with pytest.raises(ClientError) as exc:
        efs.describe_file_systems(FileSystemId=fs_id)
    assert exc.value.response["Error"]["Code"] == "FileSystemNotFound"

def test_efs_mount_target(efs):
    fs = efs.create_file_system()
    fs_id = fs["FileSystemId"]
    mt = efs.create_mount_target(FileSystemId=fs_id, SubnetId="subnet-00000001")
    mt_id = mt["MountTargetId"]
    assert mt_id.startswith("fsmt-")
    assert mt["LifeCycleState"] == "available"

    desc = efs.describe_mount_targets(FileSystemId=fs_id)
    assert len(desc["MountTargets"]) == 1
    assert desc["MountTargets"][0]["MountTargetId"] == mt_id

    import botocore.exceptions

    try:
        efs.delete_file_system(FileSystemId=fs_id)
        assert False, "should raise"
    except botocore.exceptions.ClientError as e:
        assert e.response["Error"]["Code"] in ("FileSystemInUse", "400") or "mount targets" in str(e).lower()

    efs.delete_mount_target(MountTargetId=mt_id)
    desc2 = efs.describe_mount_targets(FileSystemId=fs_id)
    assert len(desc2["MountTargets"]) == 0

def test_efs_access_point(efs):
    fs = efs.create_file_system()
    fs_id = fs["FileSystemId"]
    ap = efs.create_access_point(
        FileSystemId=fs_id,
        Tags=[{"Key": "Name", "Value": "my-ap"}],
        RootDirectory={"Path": "/data"},
    )
    ap_id = ap["AccessPointId"]
    assert ap_id.startswith("fsap-")
    assert ap["LifeCycleState"] == "available"

    desc = efs.describe_access_points(FileSystemId=fs_id)
    assert any(a["AccessPointId"] == ap_id for a in desc["AccessPoints"])

    efs.delete_access_point(AccessPointId=ap_id)
    desc2 = efs.describe_access_points(FileSystemId=fs_id)
    assert not any(a["AccessPointId"] == ap_id for a in desc2["AccessPoints"])

def test_efs_tags(efs):
    fs = efs.create_file_system(Tags=[{"Key": "env", "Value": "test"}])
    fs_arn = fs["FileSystemArn"]
    efs.tag_resource(ResourceId=fs_arn, Tags=[{"Key": "team", "Value": "data"}])
    tags_resp = efs.list_tags_for_resource(ResourceId=fs_arn)
    tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "data"

    efs.untag_resource(ResourceId=fs_arn, TagKeys=["env"])
    tags_resp2 = efs.list_tags_for_resource(ResourceId=fs_arn)
    keys = [t["Key"] for t in tags_resp2["Tags"]]
    assert "env" not in keys
    assert "team" in keys

def test_efs_lifecycle_configuration(efs):
    fs = efs.create_file_system()
    fs_id = fs["FileSystemId"]
    efs.put_lifecycle_configuration(
        FileSystemId=fs_id,
        LifecyclePolicies=[{"TransitionToIA": "AFTER_30_DAYS"}],
    )
    resp = efs.describe_lifecycle_configuration(FileSystemId=fs_id)
    assert len(resp["LifecyclePolicies"]) == 1
    assert resp["LifecyclePolicies"][0]["TransitionToIA"] == "AFTER_30_DAYS"

def test_efs_backup_policy(efs):
    fs = efs.create_file_system()
    fs_id = fs["FileSystemId"]
    efs.put_backup_policy(
        FileSystemId=fs_id,
        BackupPolicy={"Status": "ENABLED"},
    )
    resp = efs.describe_backup_policy(FileSystemId=fs_id)
    assert resp["BackupPolicy"]["Status"] == "ENABLED"
