"""
EFS (Elastic File System) Service Emulator.
REST/JSON protocol — /2015-02-01/* paths.
In-memory only — no real filesystem.

Supports:
  File Systems:   CreateFileSystem, DescribeFileSystems, DeleteFileSystem, UpdateFileSystem
  Mount Targets:  CreateMountTarget, DescribeMountTargets, DeleteMountTarget
                  DescribeMountTargetSecurityGroups, ModifyMountTargetSecurityGroups
  Access Points:  CreateAccessPoint, DescribeAccessPoints, DeleteAccessPoint
  Tags:           TagResource, UntagResource, ListTagsForResource,
                  CreateTags (legacy), DeleteTags (legacy), DescribeTags (legacy)
  Lifecycle:      PutLifecycleConfiguration, DescribeLifecycleConfiguration
  Backup Policy:  PutBackupPolicy, DescribeBackupPolicy
  Account:        DescribeAccountPreferences, PutAccountPreferences
"""

import os
import json
import random
import string
import logging
import time
import re

logger = logging.getLogger("efs")

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_file_systems: dict = {}    # fs_id -> fs record
_mount_targets: dict = {}   # mt_id -> mount target record
_access_points: dict = {}   # ap_id -> access point record

# ---------------------------------------------------------------------------
# ID generators
# ---------------------------------------------------------------------------

def _fs_id():
    return "fs-" + "".join(random.choices(string.hexdigits[:16], k=17))

def _mt_id():
    return "fsmt-" + "".join(random.choices(string.hexdigits[:16], k=17))

def _ap_id():
    return "fsap-" + "".join(random.choices(string.hexdigits[:16], k=17))

def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())

# ---------------------------------------------------------------------------
# File Systems
# ---------------------------------------------------------------------------

def _create_file_system(body):
    perf_mode = body.get("PerformanceMode", "generalPurpose")
    throughput_mode = body.get("ThroughputMode", "bursting")
    encrypted = body.get("Encrypted", False)
    kms_key_id = body.get("KmsKeyId", "")
    tags = body.get("Tags", [])
    provisioned_throughput = body.get("ProvisionedThroughputInMibps")
    creation_token = body.get("CreationToken", _fs_id())

    # Idempotency — same CreationToken returns existing FS
    for fs in _file_systems.values():
        if fs.get("CreationToken") == creation_token:
            return _json(200, _fs_response(fs))

    fs_id = _fs_id()
    arn = f"arn:aws:elasticfilesystem:{REGION}:{ACCOUNT_ID}:file-system/{fs_id}"
    now = _now_iso()

    record = {
        "FileSystemId": fs_id,
        "FileSystemArn": arn,
        "CreationToken": creation_token,
        "CreationTime": now,
        "LifeCycleState": "available",
        "NumberOfMountTargets": 0,
        "SizeInBytes": {"Value": 0, "Timestamp": now, "ValueInIA": 0, "ValueInStandard": 0},
        "PerformanceMode": perf_mode,
        "ThroughputMode": throughput_mode,
        "Encrypted": encrypted,
        "KmsKeyId": kms_key_id,
        "Tags": tags,
        "OwnerId": ACCOUNT_ID,
        "Name": next((t["Value"] for t in tags if t["Key"] == "Name"), ""),
    }
    if provisioned_throughput:
        record["ProvisionedThroughputInMibps"] = provisioned_throughput

    _file_systems[fs_id] = record
    return _json(201, _fs_response(record))


def _describe_file_systems(query):
    fs_id = query.get("FileSystemId")
    creation_token = query.get("CreationToken")
    max_items = int(query.get("MaxItems", 100))

    results = []
    for fs in _file_systems.values():
        if fs_id and fs["FileSystemId"] != fs_id:
            continue
        if creation_token and fs.get("CreationToken") != creation_token:
            continue
        results.append(_fs_response(fs))

    return _json(200, {"FileSystems": results[:max_items]})


def _delete_file_system(fs_id):
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    if fs["NumberOfMountTargets"] > 0:
        return _error(400, "FileSystemInUse",
                      f"File system '{fs_id}' has mount targets and cannot be deleted.")
    del _file_systems[fs_id]
    return _json(204, {})


def _update_file_system(fs_id, body):
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    if "ThroughputMode" in body:
        fs["ThroughputMode"] = body["ThroughputMode"]
    if "ProvisionedThroughputInMibps" in body:
        fs["ProvisionedThroughputInMibps"] = body["ProvisionedThroughputInMibps"]
    return _json(202, _fs_response(fs))


def _fs_response(fs):
    r = {k: v for k, v in fs.items()}
    return r


# ---------------------------------------------------------------------------
# Mount Targets
# ---------------------------------------------------------------------------

def _create_mount_target(body):
    fs_id = body.get("FileSystemId")
    subnet_id = body.get("SubnetId", "")
    ip_address = body.get("IpAddress", f"10.0.{random.randint(0,255)}.{random.randint(1,254)}")
    security_groups = body.get("SecurityGroups", [])

    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")

    mt_id = _mt_id()
    arn = f"arn:aws:elasticfilesystem:{REGION}:{ACCOUNT_ID}:file-system/{fs_id}/mount-target/{mt_id}"
    now = _now_iso()

    record = {
        "MountTargetId": mt_id,
        "FileSystemId": fs_id,
        "SubnetId": subnet_id,
        "AvailabilityZoneId": f"use1-az{random.randint(1,3)}",
        "AvailabilityZoneName": f"{REGION}a",
        "VpcId": "vpc-00000001",
        "LifeCycleState": "available",
        "IpAddress": ip_address,
        "NetworkInterfaceId": f"eni-{random.choices(string.hexdigits[:16], k=17)}",
        "OwnerId": ACCOUNT_ID,
        "MountTargetArn": arn,
        "SecurityGroups": security_groups,
    }
    _mount_targets[mt_id] = record
    fs["NumberOfMountTargets"] = fs.get("NumberOfMountTargets", 0) + 1

    return _json(200, _mt_response(record))


def _describe_mount_targets(query):
    fs_id = query.get("FileSystemId")
    mt_id = query.get("MountTargetId")
    max_items = int(query.get("MaxItems", 100))

    results = []
    for mt in _mount_targets.values():
        if fs_id and mt["FileSystemId"] != fs_id:
            continue
        if mt_id and mt["MountTargetId"] != mt_id:
            continue
        results.append(_mt_response(mt))

    return _json(200, {"MountTargets": results[:max_items]})


def _delete_mount_target(mt_id):
    mt = _mount_targets.get(mt_id)
    if not mt:
        return _error(404, "MountTargetNotFound", f"Mount target '{mt_id}' does not exist.")
    fs = _file_systems.get(mt["FileSystemId"])
    if fs:
        fs["NumberOfMountTargets"] = max(0, fs.get("NumberOfMountTargets", 1) - 1)
    del _mount_targets[mt_id]
    return _json(204, {})


def _describe_mount_target_security_groups(mt_id):
    mt = _mount_targets.get(mt_id)
    if not mt:
        return _error(404, "MountTargetNotFound", f"Mount target '{mt_id}' does not exist.")
    return _json(200, {"SecurityGroups": mt.get("SecurityGroups", [])})


def _modify_mount_target_security_groups(mt_id, body):
    mt = _mount_targets.get(mt_id)
    if not mt:
        return _error(404, "MountTargetNotFound", f"Mount target '{mt_id}' does not exist.")
    mt["SecurityGroups"] = body.get("SecurityGroups", [])
    return _json(204, {})


def _mt_response(mt):
    return {k: v for k, v in mt.items() if k != "SecurityGroups"}


# ---------------------------------------------------------------------------
# Access Points
# ---------------------------------------------------------------------------

def _create_access_point(body):
    fs_id = body.get("FileSystemId")
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")

    ap_id = _ap_id()
    arn = f"arn:aws:elasticfilesystem:{REGION}:{ACCOUNT_ID}:access-point/{ap_id}"
    now = _now_iso()
    tags = body.get("Tags", [])

    record = {
        "AccessPointId": ap_id,
        "AccessPointArn": arn,
        "FileSystemId": fs_id,
        "LifeCycleState": "available",
        "ClientToken": body.get("ClientToken", ap_id),
        "PosixUser": body.get("PosixUser", {}),
        "RootDirectory": body.get("RootDirectory", {"Path": "/"}),
        "Tags": tags,
        "OwnerId": ACCOUNT_ID,
        "Name": next((t["Value"] for t in tags if t["Key"] == "Name"), ""),
    }
    _access_points[ap_id] = record
    return _json(200, record)


def _describe_access_points(query):
    fs_id = query.get("FileSystemId")
    ap_id = query.get("AccessPointId")
    max_results = int(query.get("MaxResults", 100))

    results = []
    for ap in _access_points.values():
        if fs_id and ap["FileSystemId"] != fs_id:
            continue
        if ap_id and ap["AccessPointId"] != ap_id:
            continue
        results.append(ap)

    return _json(200, {"AccessPoints": results[:max_results]})


def _delete_access_point(ap_id):
    if ap_id not in _access_points:
        return _error(404, "AccessPointNotFound", f"Access point '{ap_id}' does not exist.")
    del _access_points[ap_id]
    return _json(204, {})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(resource_id, body):
    resource = _find_resource(resource_id)
    if resource is None:
        return _error(404, "ResourceNotFound", f"Resource '{resource_id}' does not exist.")
    tags = body.get("Tags", [])
    existing = {t["Key"]: i for i, t in enumerate(resource.get("Tags", []))}
    tag_list = resource.setdefault("Tags", [])
    for tag in tags:
        idx = existing.get(tag["Key"])
        if idx is not None:
            tag_list[idx] = tag
        else:
            tag_list.append(tag)
    return _json(200, {})


def _untag_resource(resource_id, keys):
    resource = _find_resource(resource_id)
    if resource is None:
        return _error(404, "ResourceNotFound", f"Resource '{resource_id}' does not exist.")
    resource["Tags"] = [t for t in resource.get("Tags", []) if t["Key"] not in keys]
    return _json(200, {})


def _list_tags_for_resource(resource_id):
    resource = _find_resource(resource_id)
    if resource is None:
        return _error(404, "ResourceNotFound", f"Resource '{resource_id}' does not exist.")
    return _json(200, {"Tags": resource.get("Tags", [])})


def _find_resource(resource_id):
    if resource_id in _file_systems:
        return _file_systems[resource_id]
    if resource_id in _access_points:
        return _access_points[resource_id]
    for fs in _file_systems.values():
        if fs["FileSystemArn"] == resource_id:
            return fs
    for ap in _access_points.values():
        if ap["AccessPointArn"] == resource_id:
            return ap
    return None


# ---------------------------------------------------------------------------
# Lifecycle / Backup / Account (stubs)
# ---------------------------------------------------------------------------

_lifecycle_configs: dict = {}
_backup_policies: dict = {}

def _put_lifecycle_configuration(fs_id, body):
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    _lifecycle_configs[fs_id] = body.get("LifecyclePolicies", [])
    return _json(200, {"LifecyclePolicies": _lifecycle_configs[fs_id]})


def _describe_lifecycle_configuration(fs_id):
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    return _json(200, {"LifecyclePolicies": _lifecycle_configs.get(fs_id, [])})


def _put_backup_policy(fs_id, body):
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    _backup_policies[fs_id] = body.get("BackupPolicy", {"Status": "DISABLED"})
    return _json(200, {"BackupPolicy": _backup_policies[fs_id]})


def _describe_backup_policy(fs_id):
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    return _json(200, {"BackupPolicy": _backup_policies.get(fs_id, {"Status": "DISABLED"})})


def _describe_account_preferences():
    return _json(200, {"ResourceIdPreference": {"ResourceIdType": "LONG_ID", "Resources": ["FILE_SYSTEM", "MOUNT_TARGET"]}})


def _put_account_preferences(body):
    return _json(200, {"ResourceIdPreference": {"ResourceIdType": body.get("ResourceIdType", "LONG_ID"), "Resources": ["FILE_SYSTEM", "MOUNT_TARGET"]}})


# ---------------------------------------------------------------------------
# Request router
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body_bytes, query_params):
    try:
        body = json.loads(body_bytes) if body_bytes else {}
    except json.JSONDecodeError:
        body = {}

    # Flatten single-value query params
    query = {k: (v[0] if isinstance(v, list) else v) for k, v in query_params.items()}

    # Strip base path prefix
    p = re.sub(r"^/2015-02-01", "", path)

    # File Systems
    if p == "/file-systems":
        if method == "POST":
            return await _a(_create_file_system(body))
        if method == "GET":
            return await _a(_describe_file_systems(query))

    m = re.fullmatch(r"/file-systems/([^/]+)", p)
    if m:
        fs_id = m.group(1)
        if method == "DELETE":
            return await _a(_delete_file_system(fs_id))
        if method == "PUT":
            return await _a(_update_file_system(fs_id, body))

    # Mount Targets
    if p == "/mount-targets":
        if method == "POST":
            return await _a(_create_mount_target(body))
        if method == "GET":
            return await _a(_describe_mount_targets(query))

    m = re.fullmatch(r"/mount-targets/([^/]+)", p)
    if m:
        mt_id = m.group(1)
        if method == "DELETE":
            return await _a(_delete_mount_target(mt_id))

    m = re.fullmatch(r"/mount-targets/([^/]+)/security-groups", p)
    if m:
        mt_id = m.group(1)
        if method == "GET":
            return await _a(_describe_mount_target_security_groups(mt_id))
        if method == "PUT":
            return await _a(_modify_mount_target_security_groups(mt_id, body))

    # Access Points
    if p == "/access-points":
        if method == "POST":
            return await _a(_create_access_point(body))
        if method == "GET":
            return await _a(_describe_access_points(query))

    m = re.fullmatch(r"/access-points/([^/]+)", p)
    if m:
        ap_id = m.group(1)
        if method == "DELETE":
            return await _a(_delete_access_point(ap_id))

    # Tags
    m = re.fullmatch(r"/resource-tags/(.+)", p)
    if m:
        resource_id = m.group(1)
        if method == "GET":
            return await _a(_list_tags_for_resource(resource_id))
        if method == "POST":
            return await _a(_tag_resource(resource_id, body))
        if method == "DELETE":
            keys = query.get("tagKeys", "").split(",") if query.get("tagKeys") else body.get("TagKeys", [])
            return await _a(_untag_resource(resource_id, keys))

    # Lifecycle
    m = re.fullmatch(r"/file-systems/([^/]+)/lifecycle-configuration", p)
    if m:
        fs_id = m.group(1)
        if method == "PUT":
            return await _a(_put_lifecycle_configuration(fs_id, body))
        if method == "GET":
            return await _a(_describe_lifecycle_configuration(fs_id))

    # Backup Policy
    m = re.fullmatch(r"/file-systems/([^/]+)/backup-policy", p)
    if m:
        fs_id = m.group(1)
        if method == "PUT":
            return await _a(_put_backup_policy(fs_id, body))
        if method == "GET":
            return await _a(_describe_backup_policy(fs_id))

    # Account Preferences
    if p == "/account-preferences":
        if method == "GET":
            return await _a(_describe_account_preferences())
        if method == "PUT":
            return await _a(_put_account_preferences(body))

    return _error(400, "InvalidAction", f"Unknown EFS path: {method} {path}")


async def _a(result):
    return result


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------

def _json(status, data):
    if status == 204:
        return status, {}, b""
    body = json.dumps(data).encode("utf-8")
    return status, {"Content-Type": "application/json"}, body


def _error(status, code, message):
    body = json.dumps({"ErrorCode": code, "Message": message, "error": {"code": code}}).encode("utf-8")
    return status, {"Content-Type": "application/json", "x-amzn-errortype": code}, body


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

def reset():
    _file_systems.clear()
    _mount_targets.clear()
    _access_points.clear()
    _lifecycle_configs.clear()
    _backup_policies.clear()
