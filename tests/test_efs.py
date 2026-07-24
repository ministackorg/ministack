import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


def _efs_client(region):
    return boto3.client(
        "efs",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


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


def test_efs_resources_are_region_scoped():
    east = _efs_client("us-east-1")
    west = _efs_client("us-west-2")
    token = f"regional-{_uuid_mod.uuid4().hex}"
    east_fs = east.create_file_system(CreationToken=token)
    west_fs = west.create_file_system(CreationToken=token)
    east_mt = east.create_mount_target(
        FileSystemId=east_fs["FileSystemId"],
        SubnetId="subnet-00000001",
    )
    west_mt = west.create_mount_target(
        FileSystemId=west_fs["FileSystemId"],
        SubnetId="subnet-00000002",
    )
    east_ap = east.create_access_point(FileSystemId=east_fs["FileSystemId"])
    west_ap = west.create_access_point(FileSystemId=west_fs["FileSystemId"])
    east.put_lifecycle_configuration(
        FileSystemId=east_fs["FileSystemId"],
        LifecyclePolicies=[{"TransitionToIA": "AFTER_7_DAYS"}],
    )
    west.put_lifecycle_configuration(
        FileSystemId=west_fs["FileSystemId"],
        LifecyclePolicies=[{"TransitionToIA": "AFTER_30_DAYS"}],
    )
    east.put_backup_policy(
        FileSystemId=east_fs["FileSystemId"],
        BackupPolicy={"Status": "ENABLED"},
    )
    west.put_backup_policy(
        FileSystemId=west_fs["FileSystemId"],
        BackupPolicy={"Status": "DISABLED"},
    )

    try:
        east_ids = {fs["FileSystemId"] for fs in east.describe_file_systems()["FileSystems"]}
        west_ids = {fs["FileSystemId"] for fs in west.describe_file_systems()["FileSystems"]}
        assert east_fs["FileSystemId"] in east_ids
        assert east_fs["FileSystemId"] not in west_ids
        assert west_fs["FileSystemId"] in west_ids
        assert west_fs["FileSystemId"] not in east_ids
        assert ":us-east-1:" in east_fs["FileSystemArn"]
        assert ":us-west-2:" in west_fs["FileSystemArn"]

        assert east.describe_mount_targets(FileSystemId=east_fs["FileSystemId"])["MountTargets"][0]["MountTargetId"] == east_mt["MountTargetId"]
        assert west.describe_mount_targets(FileSystemId=west_fs["FileSystemId"])["MountTargets"][0]["MountTargetId"] == west_mt["MountTargetId"]
        assert east.describe_access_points(FileSystemId=east_fs["FileSystemId"])["AccessPoints"][0]["AccessPointId"] == east_ap["AccessPointId"]
        assert west.describe_access_points(FileSystemId=west_fs["FileSystemId"])["AccessPoints"][0]["AccessPointId"] == west_ap["AccessPointId"]
        assert east.describe_lifecycle_configuration(FileSystemId=east_fs["FileSystemId"])["LifecyclePolicies"] == [{"TransitionToIA": "AFTER_7_DAYS"}]
        assert west.describe_lifecycle_configuration(FileSystemId=west_fs["FileSystemId"])["LifecyclePolicies"] == [{"TransitionToIA": "AFTER_30_DAYS"}]
        assert east.describe_backup_policy(FileSystemId=east_fs["FileSystemId"])["BackupPolicy"]["Status"] == "ENABLED"
        assert west.describe_backup_policy(FileSystemId=west_fs["FileSystemId"])["BackupPolicy"]["Status"] == "DISABLED"
    finally:
        east.delete_access_point(AccessPointId=east_ap["AccessPointId"])
        west.delete_access_point(AccessPointId=west_ap["AccessPointId"])
        east.delete_mount_target(MountTargetId=east_mt["MountTargetId"])
        west.delete_mount_target(MountTargetId=west_mt["MountTargetId"])
        east.delete_file_system(FileSystemId=east_fs["FileSystemId"])
        west.delete_file_system(FileSystemId=west_fs["FileSystemId"])


def test_efs_restore_legacy_state_places_children_beside_parent():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import efs as service

    original_account = get_account_id()
    original_region = get_region()
    account_id = "111111111111"
    boot_region = "us-east-1"
    resource_region = "us-west-2"
    fs_id = "fs-11111111111111111"
    mt_id = "fsmt-11111111111111111"
    ap_id = "fsap-11111111111111111"
    payload = {}

    set_request_account_id(account_id)
    set_request_region(boot_region)
    for state_key, resource_key, value in (
        (
            "file_systems",
            fs_id,
            {
                "FileSystemId": fs_id,
                "FileSystemArn": f"arn:aws:elasticfilesystem:{resource_region}:{account_id}:file-system/{fs_id}",
            },
        ),
        (
            "mount_targets",
            mt_id,
            {
                "MountTargetId": mt_id,
                "FileSystemId": fs_id,
                "MountTargetArn": f"arn:aws:elasticfilesystem:{resource_region}:{account_id}:file-system/{fs_id}/mount-target/{mt_id}",
            },
        ),
        (
            "access_points",
            ap_id,
            {
                "AccessPointId": ap_id,
                "FileSystemId": fs_id,
                "AccessPointArn": f"arn:aws:elasticfilesystem:{resource_region}:{account_id}:access-point/{ap_id}",
            },
        ),
        ("lifecycle_configs", fs_id, [{"TransitionToIA": "AFTER_30_DAYS"}]),
        ("backup_policies", fs_id, {"Status": "ENABLED"}),
    ):
        store = AccountScopedDict()
        store[resource_key] = value
        payload[state_key] = store

    service.reset()
    try:
        service.restore_state(payload)
        assert service._file_systems.get_scoped(account_id, resource_region, fs_id)["FileSystemId"] == fs_id
        assert service._mount_targets.get_scoped(account_id, resource_region, mt_id)["FileSystemId"] == fs_id
        assert service._access_points.get_scoped(account_id, resource_region, ap_id)["FileSystemId"] == fs_id
        assert service._lifecycle_configs.get_scoped(account_id, resource_region, fs_id) == [{"TransitionToIA": "AFTER_30_DAYS"}]
        assert service._backup_policies.get_scoped(account_id, resource_region, fs_id) == {"Status": "ENABLED"}
        for store, key in (
            (service._file_systems, fs_id),
            (service._mount_targets, mt_id),
            (service._access_points, ap_id),
            (service._lifecycle_configs, fs_id),
            (service._backup_policies, fs_id),
        ):
            assert store.get_scoped(account_id, boot_region, key) is None
    finally:
        service.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_efs_reset_clears_state_across_regions():
    from ministack.core.responses import get_region, set_request_region
    from ministack.services import efs as service

    original_region = get_region()
    stores = (
        service._file_systems,
        service._mount_targets,
        service._access_points,
        service._lifecycle_configs,
        service._backup_policies,
    )
    service.reset()
    try:
        for region in ("us-east-1", "us-west-2"):
            set_request_region(region)
            for store in stores:
                store[f"resource-{region}"] = {"region": region}
        service.reset()
        assert all(not store.has_any() for store in stores)
    finally:
        service.reset()
        set_request_region(original_region)


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


def test_efs_access_point_tags_accept_arn(efs):
    fs = efs.create_file_system(CreationToken=f"ap-tags-{_uuid_mod.uuid4().hex[:8]}")
    ap = efs.create_access_point(FileSystemId=fs["FileSystemId"])
    ap_arn = ap["AccessPointArn"]

    efs.tag_resource(ResourceId=ap_arn, Tags=[{"Key": "team", "Value": "data"}])

    tags_resp = efs.list_tags_for_resource(ResourceId=ap_arn)
    tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
    assert tag_map["team"] == "data"


def test_efs_tag_resource_rejects_invalid_arns(efs):
    fs = efs.create_file_system(CreationToken=f"invalid-tags-{_uuid_mod.uuid4().hex[:8]}")
    fs_arn = fs["FileSystemArn"]
    ap = efs.create_access_point(FileSystemId=fs["FileSystemId"])
    invalid_cases = [
        ("not-an-arn", "BadRequest"),
        (fs_arn.replace(":elasticfilesystem:", ":s3:"), "BadRequest"),
        (fs_arn.replace(":000000000000:", ":111111111111:"), "FileSystemNotFound"),
        (fs_arn.replace(":us-east-1:", ":us-west-2:"), "FileSystemNotFound"),
        (f"arn:aws:elasticfilesystem:us-east-1:000000000000:mount-target/{fs['FileSystemId']}", "BadRequest"),
        (f"arn:aws:elasticfilesystem:us-east-1:000000000000:file-system/{ap['AccessPointId']}", "BadRequest"),
        (f"arn:aws:elasticfilesystem:us-east-1:000000000000:access-point/{fs['FileSystemId']}", "BadRequest"),
    ]

    for bad_resource_id, expected_code in invalid_cases:
        with pytest.raises(ClientError) as exc:
            efs.tag_resource(ResourceId=bad_resource_id, Tags=[{"Key": "bad", "Value": "value"}])
        assert exc.value.response["Error"]["Code"] == expected_code

    tags_resp = efs.list_tags_for_resource(ResourceId=fs_arn)
    tag_map = {t["Key"]: t["Value"] for t in tags_resp["Tags"]}
    assert "bad" not in tag_map


def test_efs_list_and_untag_reject_invalid_arns(efs):
    for operation, kwargs in [
        (efs.list_tags_for_resource, {}),
        (efs.untag_resource, {"TagKeys": ["missing"]}),
    ]:
        with pytest.raises(ClientError) as exc:
            operation(ResourceId="arn:aws:s3:us-east-1:000000000000:file-system/fs-1234567890abcdef0", **kwargs)
        assert exc.value.response["Error"]["Code"] == "BadRequest"


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

def _uid():
    return _uuid_mod.uuid4().hex[:8]

def test_efs_update_file_system(efs):
    fs = efs.create_file_system(
        CreationToken=f"update-fs-{_uid()}",
        ThroughputMode="bursting",
    )
    fs_id = fs["FileSystemId"]
    assert fs["ThroughputMode"] == "bursting"

    resp = efs.update_file_system(
        FileSystemId=fs_id,
        ThroughputMode="provisioned",
        ProvisionedThroughputInMibps=256.0,
    )
    assert resp["ThroughputMode"] == "provisioned"
    assert resp["ProvisionedThroughputInMibps"] == 256.0

    desc = efs.describe_file_systems(FileSystemId=fs_id)
    updated = desc["FileSystems"][0]
    assert updated["ThroughputMode"] == "provisioned"
    assert updated["ProvisionedThroughputInMibps"] == 256.0

    efs.delete_file_system(FileSystemId=fs_id)

def test_efs_describe_mount_target_security_groups(efs):
    fs = efs.create_file_system(CreationToken=f"sg-desc-{_uid()}")
    fs_id = fs["FileSystemId"]
    mt = efs.create_mount_target(
        FileSystemId=fs_id,
        SubnetId="subnet-00000001",
        SecurityGroups=["sg-aaa111aaa", "sg-bbb222bbb"],
    )
    mt_id = mt["MountTargetId"]

    resp = efs.describe_mount_target_security_groups(MountTargetId=mt_id)
    assert set(resp["SecurityGroups"]) == {"sg-aaa111aaa", "sg-bbb222bbb"}

    efs.delete_mount_target(MountTargetId=mt_id)
    efs.delete_file_system(FileSystemId=fs_id)

def test_efs_modify_mount_target_security_groups(efs):
    fs = efs.create_file_system(CreationToken=f"sg-mod-{_uid()}")
    fs_id = fs["FileSystemId"]
    mt = efs.create_mount_target(
        FileSystemId=fs_id,
        SubnetId="subnet-00000001",
        SecurityGroups=["sg-old111old"],
    )
    mt_id = mt["MountTargetId"]

    efs.modify_mount_target_security_groups(
        MountTargetId=mt_id,
        SecurityGroups=["sg-new111new", "sg-new222new"],
    )

    resp = efs.describe_mount_target_security_groups(MountTargetId=mt_id)
    assert set(resp["SecurityGroups"]) == {"sg-new111new", "sg-new222new"}

    efs.delete_mount_target(MountTargetId=mt_id)
    efs.delete_file_system(FileSystemId=fs_id)

def test_efs_describe_account_preferences(efs):
    resp = efs.describe_account_preferences()
    pref = resp["ResourceIdPreference"]
    assert "ResourceIdType" in pref
    assert "Resources" in pref
    assert isinstance(pref["Resources"], list)

def test_efs_put_account_preferences(efs):
    resp = efs.put_account_preferences(ResourceIdType="LONG_ID")
    pref = resp["ResourceIdPreference"]
    assert pref["ResourceIdType"] == "LONG_ID"
    assert "FILE_SYSTEM" in pref["Resources"]
    assert "MOUNT_TARGET" in pref["Resources"]
