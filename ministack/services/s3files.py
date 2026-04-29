"""
Amazon S3 Files Service Emulator.
REST/JSON API — file systems, mount targets, access points.

Supports:
  File Systems:    CreateFileSystem, GetFileSystem, ListFileSystems, DeleteFileSystem
  Mount Targets:   CreateMountTarget, GetMountTarget, ListMountTargets,
                   DeleteMountTarget, UpdateMountTarget
  Access Points:   CreateAccessPoint, GetAccessPoint, ListAccessPoints, DeleteAccessPoint
  Policies:        GetFileSystemPolicy, PutFileSystemPolicy, DeleteFileSystemPolicy
  Sync:            GetSynchronizationConfiguration, PutSynchronizationConfiguration
  Tags:            TagResource, UntagResource, ListTagsForResource
"""

import copy
import json
import logging
import os
import time

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("s3files")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_file_systems = AccountScopedDict()
_mount_targets = AccountScopedDict()
_access_points = AccountScopedDict()
_policies = AccountScopedDict()
_sync_configs = AccountScopedDict()
_tags = AccountScopedDict()


def get_state():
    return copy.deepcopy({
        "file_systems": _file_systems,
        "mount_targets": _mount_targets,
        "access_points": _access_points,
        "policies": _policies,
        "sync_configs": _sync_configs,
        "tags": _tags,
    })


def restore_state(data):
    if not data:
        return
    _file_systems.update(data.get("file_systems", {}))
    _mount_targets.update(data.get("mount_targets", {}))
    _access_points.update(data.get("access_points", {}))
    _policies.update(data.get("policies", {}))
    _sync_configs.update(data.get("sync_configs", {}))
    _tags.update(data.get("tags", {}))


try:
    _restored = load_state("s3files")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


def reset():
    _file_systems.clear()
    _mount_targets.clear()
    _access_points.clear()
    _policies.clear()
    _sync_configs.clear()
    _tags.clear()


def _arn(resource_type, resource_id):
    return f"arn:aws:s3files:{get_region()}:{get_account_id()}:{resource_type}/{resource_id}"


def _now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, TypeError):
        data = {}

    parts = [p for p in path.strip("/").split("/") if p]

    # POST /file-systems
    if method == "POST" and parts == ["file-systems"]:
        return _create_file_system(data)

    # GET /file-systems
    if method == "GET" and parts == ["file-systems"]:
        return _list_file_systems(data, query_params)

    # GET /file-systems/{id}
    if method == "GET" and len(parts) == 2 and parts[0] == "file-systems":
        return _get_file_system(parts[1])

    # DELETE /file-systems/{id}
    if method == "DELETE" and len(parts) == 2 and parts[0] == "file-systems":
        return _delete_file_system(parts[1])

    # GET /file-systems/{id}/policy
    if method == "GET" and len(parts) == 3 and parts[0] == "file-systems" and parts[2] == "policy":
        return _get_file_system_policy(parts[1])

    # PUT /file-systems/{id}/policy
    if method == "PUT" and len(parts) == 3 and parts[0] == "file-systems" and parts[2] == "policy":
        return _put_file_system_policy(parts[1], data)

    # DELETE /file-systems/{id}/policy
    if method == "DELETE" and len(parts) == 3 and parts[0] == "file-systems" and parts[2] == "policy":
        return _delete_file_system_policy(parts[1])

    # POST /mount-targets
    if method == "POST" and parts == ["mount-targets"]:
        return _create_mount_target(data)

    # GET /mount-targets
    if method == "GET" and parts == ["mount-targets"]:
        return _list_mount_targets(data, query_params)

    # GET /mount-targets/{id}
    if method == "GET" and len(parts) == 2 and parts[0] == "mount-targets":
        return _get_mount_target(parts[1])

    # PUT /mount-targets/{id}
    if method == "PUT" and len(parts) == 2 and parts[0] == "mount-targets":
        return _update_mount_target(parts[1], data)

    # DELETE /mount-targets/{id}
    if method == "DELETE" and len(parts) == 2 and parts[0] == "mount-targets":
        return _delete_mount_target(parts[1])

    # POST /access-points
    if method == "POST" and parts == ["access-points"]:
        return _create_access_point(data)

    # GET /access-points
    if method == "GET" and parts == ["access-points"]:
        return _list_access_points(data, query_params)

    # GET /access-points/{id}
    if method == "GET" and len(parts) == 2 and parts[0] == "access-points":
        return _get_access_point(parts[1])

    # DELETE /access-points/{id}
    if method == "DELETE" and len(parts) == 2 and parts[0] == "access-points":
        return _delete_access_point(parts[1])

    # GET /file-systems/{id}/synchronization-configuration
    if method == "GET" and len(parts) == 3 and parts[2] == "synchronization-configuration":
        return _get_sync_config(parts[1])

    # PUT /file-systems/{id}/synchronization-configuration
    if method == "PUT" and len(parts) == 3 and parts[2] == "synchronization-configuration":
        return _put_sync_config(parts[1], data)

    # POST /tags/{arn}
    if method == "POST" and len(parts) >= 2 and parts[0] == "tags":
        resource_arn = "/".join(parts[1:])
        return _tag_resource(resource_arn, data)

    # DELETE /tags/{arn}
    if method == "DELETE" and len(parts) >= 2 and parts[0] == "tags":
        resource_arn = "/".join(parts[1:])
        tag_keys = query_params.get("tagKeys", [])
        if isinstance(tag_keys, str):
            tag_keys = [tag_keys]
        return _untag_resource(resource_arn, tag_keys)

    # GET /tags/{arn}
    if method == "GET" and len(parts) >= 2 and parts[0] == "tags":
        resource_arn = "/".join(parts[1:])
        return _list_tags(resource_arn)

    return error_response_json("InvalidRequest", f"Unknown S3 Files route: {method} {path}", 400)


# ---------------------------------------------------------------------------
# File Systems
# ---------------------------------------------------------------------------

def _create_file_system(data):
    fs_id = "fs-" + new_uuid().replace("-", "")[:17]
    bucket_name = data.get("BucketName", "")
    arn = _arn("file-system", fs_id)
    fs = {
        "FileSystemId": fs_id,
        "FileSystemArn": arn,
        "BucketName": bucket_name,
        "LifeCycleState": "available",
        "CreationTime": _now_iso(),
        "OwnerId": get_account_id(),
    }
    _file_systems[fs_id] = fs
    logger.info("Created S3 file system %s for bucket %s", fs_id, bucket_name)
    return json_response(fs, 201)


def _get_file_system(fs_id):
    fs = _file_systems.get(fs_id)
    if not fs:
        return error_response_json("FileSystemNotFound", f"File system {fs_id} not found", 404)
    return json_response(fs)


def _list_file_systems(data, query_params):
    items = list(_file_systems.values())
    return json_response({"FileSystems": items})


def _delete_file_system(fs_id):
    if fs_id not in _file_systems:
        return error_response_json("FileSystemNotFound", f"File system {fs_id} not found", 404)
    del _file_systems[fs_id]
    _policies.pop(fs_id, None)
    _sync_configs.pop(fs_id, None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Mount Targets
# ---------------------------------------------------------------------------

def _create_mount_target(data):
    mt_id = "fsmt-" + new_uuid().replace("-", "")[:17]
    fs_id = data.get("FileSystemId", "")
    subnet_id = data.get("SubnetId", "")
    mt = {
        "MountTargetId": mt_id,
        "FileSystemId": fs_id,
        "SubnetId": subnet_id,
        "LifeCycleState": "available",
        "IpAddress": data.get("IpAddress", "10.0.0.1"),
        "VpcId": data.get("VpcId", "vpc-00000001"),
        "AvailabilityZone": data.get("AvailabilityZone", f"{get_region()}a"),
    }
    _mount_targets[mt_id] = mt
    logger.info("Created mount target %s for fs %s", mt_id, fs_id)
    return json_response(mt, 201)


def _get_mount_target(mt_id):
    mt = _mount_targets.get(mt_id)
    if not mt:
        return error_response_json("MountTargetNotFound", f"Mount target {mt_id} not found", 404)
    return json_response(mt)


def _list_mount_targets(data, query_params):
    fs_id = query_params.get("FileSystemId", [""])[0] if isinstance(query_params.get("FileSystemId"), list) else query_params.get("FileSystemId", "")
    items = list(_mount_targets.values())
    if fs_id:
        items = [mt for mt in items if mt.get("FileSystemId") == fs_id]
    return json_response({"MountTargets": items})


def _update_mount_target(mt_id, data):
    mt = _mount_targets.get(mt_id)
    if not mt:
        return error_response_json("MountTargetNotFound", f"Mount target {mt_id} not found", 404)
    for key in ("SubnetId", "IpAddress", "SecurityGroups"):
        if key in data:
            mt[key] = data[key]
    return json_response(mt)


def _delete_mount_target(mt_id):
    if mt_id not in _mount_targets:
        return error_response_json("MountTargetNotFound", f"Mount target {mt_id} not found", 404)
    del _mount_targets[mt_id]
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Access Points
# ---------------------------------------------------------------------------

def _create_access_point(data):
    ap_id = "fsap-" + new_uuid().replace("-", "")[:17]
    fs_id = data.get("FileSystemId", "")
    arn = _arn("access-point", ap_id)
    ap = {
        "AccessPointId": ap_id,
        "AccessPointArn": arn,
        "FileSystemId": fs_id,
        "Name": data.get("Name", ""),
        "LifeCycleState": "available",
    }
    _access_points[ap_id] = ap
    return json_response(ap, 201)


def _get_access_point(ap_id):
    ap = _access_points.get(ap_id)
    if not ap:
        return error_response_json("AccessPointNotFound", f"Access point {ap_id} not found", 404)
    return json_response(ap)


def _list_access_points(data, query_params):
    items = list(_access_points.values())
    return json_response({"AccessPoints": items})


def _delete_access_point(ap_id):
    if ap_id not in _access_points:
        return error_response_json("AccessPointNotFound", f"Access point {ap_id} not found", 404)
    del _access_points[ap_id]
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Policies
# ---------------------------------------------------------------------------

def _get_file_system_policy(fs_id):
    policy = _policies.get(fs_id)
    if not policy:
        return error_response_json("PolicyNotFound", f"No policy for file system {fs_id}", 404)
    return json_response({"FileSystemId": fs_id, "Policy": policy})


def _put_file_system_policy(fs_id, data):
    if fs_id not in _file_systems:
        return error_response_json("FileSystemNotFound", f"File system {fs_id} not found", 404)
    _policies[fs_id] = data.get("Policy", "")
    return json_response({"FileSystemId": fs_id, "Policy": _policies[fs_id]})


def _delete_file_system_policy(fs_id):
    _policies.pop(fs_id, None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Synchronization Configuration
# ---------------------------------------------------------------------------

def _get_sync_config(fs_id):
    config = _sync_configs.get(fs_id, {})
    return json_response({"FileSystemId": fs_id, "SynchronizationConfiguration": config})


def _put_sync_config(fs_id, data):
    if fs_id not in _file_systems:
        return error_response_json("FileSystemNotFound", f"File system {fs_id} not found", 404)
    _sync_configs[fs_id] = data.get("SynchronizationConfiguration", data)
    return json_response({"FileSystemId": fs_id, "SynchronizationConfiguration": _sync_configs[fs_id]})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _tag_resource(resource_arn, data):
    tags = _tags.setdefault(resource_arn, [])
    for tag in data.get("Tags", []):
        existing = next((t for t in tags if t["Key"] == tag["Key"]), None)
        if existing:
            existing["Value"] = tag["Value"]
        else:
            tags.append(tag)
    return json_response({})


def _untag_resource(resource_arn, tag_keys):
    _tags[resource_arn] = [t for t in _tags.get(resource_arn, []) if t["Key"] not in tag_keys]
    return json_response({})


def _list_tags(resource_arn):
    return json_response({"Tags": _tags.get(resource_arn, [])})
