"""
Amazon S3 Files Service Emulator (s3files-2025-05-05).
REST/JSON API. Routes, payloads, and response shapes match the AWS spec.

Supports:
  File Systems:    CreateFileSystem, GetFileSystem, ListFileSystems, DeleteFileSystem
  Mount Targets:   CreateMountTarget, GetMountTarget, ListMountTargets,
                   DeleteMountTarget, UpdateMountTarget
  Access Points:   CreateAccessPoint, GetAccessPoint, ListAccessPoints, DeleteAccessPoint
  Policies:        GetFileSystemPolicy, PutFileSystemPolicy, DeleteFileSystemPolicy
  Sync:            GetSynchronizationConfiguration, PutSynchronizationConfiguration
  Tags:            TagResource, UntagResource, ListTagsForResource
"""

import base64
import copy
import json
import logging
import re
import time
from urllib.parse import unquote

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    new_uuid,
)

_JSON_CT = "application/json"


def _json(body, status=200):
    """restJson1 response: spec mandates Content-Type: application/json."""
    return status, {"Content-Type": _JSON_CT}, json.dumps(body, ensure_ascii=False).encode("utf-8")


def _empty_200():
    return 200, {"Content-Type": _JSON_CT}, b""

logger = logging.getLogger("s3files")

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
    logger.exception("Failed to restore persisted state; continuing with fresh store")


def reset():
    _file_systems.clear()
    _mount_targets.clear()
    _access_points.clear()
    _policies.clear()
    _sync_configs.clear()
    _tags.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FS_ARN_RE = re.compile(r"^arn:aws[-a-z]*:s3files:[^:]*:[^:]*:file-system/(fs-[0-9a-f]{17,40})(?:/access-point/(fsap-[0-9a-f]{17,40}))?$")


def _hex_id(prefix):
    return prefix + new_uuid().replace("-", "")[:20]


def _resolve_id(value, prefer="fs"):
    """Normalize a bare ID or full ARN to the bare ID.
    `prefer="fs"` returns the file-system ID; `prefer="ap"` returns the
    access-point ID (falling back to file-system ID); `prefer="any"` returns
    whichever is most specific (access-point if present, else file-system).
    """
    if not value:
        return ""
    m = _FS_ARN_RE.match(value)
    if not m:
        return value
    if prefer == "fs":
        return m.group(1)
    if prefer == "ap":
        return m.group(2) or m.group(1)
    return m.group(2) or m.group(1)


def _fs_arn(fs_id):
    return f"arn:aws:s3files:{get_region()}:{get_account_id()}:file-system/{fs_id}"


def _ap_arn(fs_id, ap_id):
    return f"arn:aws:s3files:{get_region()}:{get_account_id()}:file-system/{fs_id}/access-point/{ap_id}"


def _name_from_tags(tags):
    for t in tags or ():
        if t.get("key") == "Name":
            return t.get("value", "")
    return ""


def _now_epoch():
    return int(time.time())


def _qp_str(query_params, key, default=""):
    v = query_params.get(key)
    if isinstance(v, list):
        return v[0] if v else default
    return v if v is not None else default


def _qp_int(query_params, key, default=None):
    raw = _qp_str(query_params, key, "")
    if raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _qp_list(query_params, key):
    v = query_params.get(key)
    if v is None:
        return []
    return v if isinstance(v, list) else [v]


def _opaque_encode(offset):
    return base64.urlsafe_b64encode(str(offset).encode()).decode().rstrip("=")


def _opaque_decode(token):
    if not token:
        return 0
    try:
        pad = "=" * (-len(token) % 4)
        return int(base64.urlsafe_b64decode(token + pad).decode())
    except Exception:
        return 0


def _paginate(items, max_results, next_token):
    start = _opaque_decode(next_token)
    end = start + max_results
    page = items[start:end]
    nxt = _opaque_encode(end) if end < len(items) else None
    return page, nxt


def _error(code, message, status):
    """restJson1 error: shared helper uses application/x-amz-json-1.0 — override
    Content-Type to application/json while keeping the __type body and
    x-amzn-errortype header (which boto3/Java/Go SDK v2 all read)."""
    s, headers, body = error_response_json(code, message, status)
    return s, {**headers, "Content-Type": _JSON_CT}, body


def _VALIDATION(msg):
    return _error("ValidationException", msg, 400)


def _NOT_FOUND(msg):
    return _error("ResourceNotFoundException", msg, 404)


def _CONFLICT(msg):
    return _error("ConflictException", msg, 409)


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    try:
        data = json.loads(body) if body else {}
    except (json.JSONDecodeError, TypeError):
        data = {}

    parts = [unquote(p) for p in path.strip("/").split("/") if p]

    # /file-systems
    if parts and parts[0] == "file-systems":
        if len(parts) == 1:
            if method == "PUT":
                return _create_file_system(data)
            if method == "GET":
                return _list_file_systems(query_params)
        elif len(parts) == 2:
            fs_id = _resolve_id(parts[1])
            if method == "GET":
                return _get_file_system(fs_id)
            if method == "DELETE":
                return _delete_file_system(fs_id)
        elif len(parts) == 3 and parts[2] == "policy":
            fs_id = _resolve_id(parts[1])
            if method == "GET":
                return _get_file_system_policy(fs_id)
            if method == "PUT":
                return _put_file_system_policy(fs_id, data)
            if method == "DELETE":
                return _delete_file_system_policy(fs_id)
        elif len(parts) == 3 and parts[2] == "synchronization-configuration":
            fs_id = _resolve_id(parts[1])
            if method == "GET":
                return _get_sync_config(fs_id)
            if method == "PUT":
                return _put_sync_config(fs_id, data)

    # /mount-targets
    if parts and parts[0] == "mount-targets":
        if len(parts) == 1:
            if method == "PUT":
                return _create_mount_target(data)
            if method == "GET":
                return _list_mount_targets(query_params)
        elif len(parts) == 2:
            mt_id = parts[1]
            if method == "GET":
                return _get_mount_target(mt_id)
            if method == "PUT":
                return _update_mount_target(mt_id, data)
            if method == "DELETE":
                return _delete_mount_target(mt_id)

    # /access-points
    if parts and parts[0] == "access-points":
        if len(parts) == 1:
            if method == "PUT":
                return _create_access_point(data)
            if method == "GET":
                return _list_access_points(query_params)
        elif len(parts) == 2:
            ap_id = _resolve_id(parts[1], prefer="ap")
            if method == "GET":
                return _get_access_point(ap_id)
            if method == "DELETE":
                return _delete_access_point(ap_id)

    # /resource-tags/{resourceId}
    if parts and parts[0] == "resource-tags" and len(parts) >= 2:
        resource_id = _resolve_id("/".join(parts[1:]), prefer="any")
        if method == "POST":
            return _tag_resource(resource_id, data)
        if method == "DELETE":
            keys = _qp_list(query_params, "tagKeys")
            return _untag_resource(resource_id, keys)
        if method == "GET":
            return _list_tags_for_resource(resource_id, query_params)

    return _VALIDATION(f"Unknown S3 Files route: {method} {path}")


# ---------------------------------------------------------------------------
# File Systems
# ---------------------------------------------------------------------------

def _file_system_record(fs_id):
    return _file_systems.get(fs_id)


def _create_file_system(data):
    bucket = data.get("bucket")
    role_arn = data.get("roleArn")
    if not bucket:
        return _VALIDATION("bucket is required")
    if not role_arn:
        return _VALIDATION("roleArn is required")

    fs_id = _hex_id("fs-")
    arn = _fs_arn(fs_id)
    tags = list(data.get("tags") or [])
    name = _name_from_tags(tags)

    fs = {
        "fileSystemId": fs_id,
        "fileSystemArn": arn,
        "bucket": bucket,
        "roleArn": role_arn,
        "prefix": data.get("prefix", ""),
        "kmsKeyId": data.get("kmsKeyId", ""),
        "clientToken": data.get("clientToken", ""),
        "name": name,
        "ownerId": get_account_id(),
        "creationTime": _now_epoch(),
        "status": "available",
        "statusMessage": "",
        "tags": tags,
    }
    _file_systems[fs_id] = fs
    if tags:
        _tags[fs_id] = list(tags)
    logger.info("Created S3 Files file system %s for bucket %s", fs_id, bucket)
    return _json(fs, 201)


def _get_file_system(fs_id):
    fs = _file_system_record(fs_id)
    if not fs:
        return _NOT_FOUND(f"File system {fs_id} not found")
    out = dict(fs)
    out["tags"] = list(_tags.get(fs_id, fs.get("tags", [])))
    return _json(out)


def _list_file_systems(query_params):
    bucket = _qp_str(query_params, "bucket")
    max_results = _qp_int(query_params, "maxResults", 100) or 100
    next_token = _qp_str(query_params, "nextToken")

    items = list(_file_systems.values())
    if bucket:
        items = [fs for fs in items if fs.get("bucket") == bucket]

    summaries = [
        {
            "fileSystemId": fs["fileSystemId"],
            "fileSystemArn": fs["fileSystemArn"],
            "bucket": fs["bucket"],
            "roleArn": fs["roleArn"],
            "name": fs.get("name", ""),
            "ownerId": fs["ownerId"],
            "creationTime": fs["creationTime"],
            "status": fs["status"],
            "statusMessage": fs.get("statusMessage", ""),
        }
        for fs in items
    ]
    page, nxt = _paginate(summaries, max_results, next_token)
    out = {"fileSystems": page}
    if nxt:
        out["nextToken"] = nxt
    return _json(out)


def _delete_file_system(fs_id):
    if fs_id not in _file_systems:
        return _NOT_FOUND(f"File system {fs_id} not found")
    del _file_systems[fs_id]
    _policies.pop(fs_id, None)
    _sync_configs.pop(fs_id, None)
    _tags.pop(fs_id, None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Mount Targets
# ---------------------------------------------------------------------------

def _mt_response(mt):
    return {
        "mountTargetId": mt["mountTargetId"],
        "fileSystemId": mt["fileSystemId"],
        "subnetId": mt["subnetId"],
        "vpcId": mt.get("vpcId", "vpc-00000000"),
        "availabilityZoneId": mt.get("availabilityZoneId", f"{get_region()}-az1"),
        "ipv4Address": mt.get("ipv4Address", "10.0.0.10"),
        "ipv6Address": mt.get("ipv6Address", ""),
        "networkInterfaceId": mt.get("networkInterfaceId", ""),
        "ownerId": mt.get("ownerId", get_account_id()),
        "securityGroups": list(mt.get("securityGroups", [])),
        "status": mt.get("status", "available"),
        "statusMessage": mt.get("statusMessage", ""),
    }


def _create_mount_target(data):
    fs_ref = data.get("fileSystemId")
    subnet_id = data.get("subnetId")
    if not fs_ref:
        return _VALIDATION("fileSystemId is required")
    if not subnet_id:
        return _VALIDATION("subnetId is required")
    fs_id = _resolve_id(fs_ref)
    if fs_id not in _file_systems:
        return _NOT_FOUND(f"File system {fs_id} not found")

    mt_id = _hex_id("fsmt-")
    mt = {
        "mountTargetId": mt_id,
        "fileSystemId": fs_id,
        "subnetId": subnet_id,
        "vpcId": "vpc-00000000",
        "availabilityZoneId": f"{get_region()}-az1",
        "ipv4Address": data.get("ipv4Address") or "10.0.0.10",
        "ipv6Address": data.get("ipv6Address", ""),
        "networkInterfaceId": "eni-" + new_uuid().replace("-", "")[:17],
        "ownerId": get_account_id(),
        "securityGroups": list(data.get("securityGroups") or []),
        "status": "available",
        "statusMessage": "",
    }
    _mount_targets[mt_id] = mt
    logger.info("Created mount target %s for fs %s", mt_id, fs_id)
    return _json(_mt_response(mt))


def _get_mount_target(mt_id):
    mt = _mount_targets.get(mt_id)
    if not mt:
        return _NOT_FOUND(f"Mount target {mt_id} not found")
    return _json(_mt_response(mt))


def _list_mount_targets(query_params):
    fs_ref = _qp_str(query_params, "fileSystemId")
    ap_ref = _qp_str(query_params, "accessPointId")
    max_results = _qp_int(query_params, "maxResults", 100) or 100
    next_token = _qp_str(query_params, "nextToken")

    items = list(_mount_targets.values())
    if fs_ref:
        fs_id = _resolve_id(fs_ref)
        items = [mt for mt in items if mt.get("fileSystemId") == fs_id]
    if ap_ref:
        ap_id = _resolve_id(ap_ref, prefer="ap")
        ap = _access_points.get(ap_id)
        if ap:
            items = [mt for mt in items if mt.get("fileSystemId") == ap.get("fileSystemId")]
        else:
            items = []

    summaries = [_mt_response(mt) for mt in items]
    page, nxt = _paginate(summaries, max_results, next_token)
    out = {"mountTargets": page}
    if nxt:
        out["nextToken"] = nxt
    return _json(out)


def _update_mount_target(mt_id, data):
    mt = _mount_targets.get(mt_id)
    if not mt:
        return _NOT_FOUND(f"Mount target {mt_id} not found")
    if "securityGroups" not in data:
        return _VALIDATION("securityGroups is required")
    mt["securityGroups"] = list(data["securityGroups"])
    return _json(_mt_response(mt))


def _delete_mount_target(mt_id):
    if mt_id not in _mount_targets:
        return _NOT_FOUND(f"Mount target {mt_id} not found")
    del _mount_targets[mt_id]
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Access Points
# ---------------------------------------------------------------------------

def _ap_response(ap):
    return {
        "accessPointId": ap["accessPointId"],
        "accessPointArn": ap["accessPointArn"],
        "fileSystemId": ap["fileSystemId"],
        "name": ap.get("name", ""),
        "ownerId": ap.get("ownerId", get_account_id()),
        "clientToken": ap.get("clientToken", ""),
        "posixUser": ap.get("posixUser") or {},
        "rootDirectory": ap.get("rootDirectory") or {},
        "status": ap.get("status", "available"),
        "tags": list(_tags.get(ap["accessPointId"], ap.get("tags", []))),
    }


def _create_access_point(data):
    fs_ref = data.get("fileSystemId")
    if not fs_ref:
        return _VALIDATION("fileSystemId is required")
    fs_id = _resolve_id(fs_ref)
    if fs_id not in _file_systems:
        return _NOT_FOUND(f"File system {fs_id} not found")

    ap_id = _hex_id("fsap-")
    arn = _ap_arn(fs_id, ap_id)
    tags = list(data.get("tags") or [])
    name = _name_from_tags(tags)

    ap = {
        "accessPointId": ap_id,
        "accessPointArn": arn,
        "fileSystemId": fs_id,
        "name": name,
        "ownerId": get_account_id(),
        "clientToken": data.get("clientToken", ""),
        "posixUser": data.get("posixUser") or {},
        "rootDirectory": data.get("rootDirectory") or {},
        "status": "available",
        "tags": tags,
    }
    _access_points[ap_id] = ap
    if tags:
        _tags[ap_id] = list(tags)
    return _json(_ap_response(ap))


def _get_access_point(ap_id):
    ap = _access_points.get(ap_id)
    if not ap:
        return _NOT_FOUND(f"Access point {ap_id} not found")
    return _json(_ap_response(ap))


def _list_access_points(query_params):
    fs_ref = _qp_str(query_params, "fileSystemId")
    if not fs_ref:
        return _VALIDATION("fileSystemId is required")
    fs_id = _resolve_id(fs_ref)
    max_results = _qp_int(query_params, "maxResults", 1000) or 1000
    next_token = _qp_str(query_params, "nextToken")

    items = [ap for ap in _access_points.values() if ap.get("fileSystemId") == fs_id]
    summaries = [
        {
            "accessPointId": ap["accessPointId"],
            "accessPointArn": ap["accessPointArn"],
            "fileSystemId": ap["fileSystemId"],
            "name": ap.get("name", ""),
            "ownerId": ap.get("ownerId", get_account_id()),
            "posixUser": ap.get("posixUser") or {},
            "rootDirectory": ap.get("rootDirectory") or {},
            "status": ap.get("status", "available"),
        }
        for ap in items
    ]
    page, nxt = _paginate(summaries, max_results, next_token)
    out = {"accessPoints": page}
    if nxt:
        out["nextToken"] = nxt
    return _json(out)


def _delete_access_point(ap_id):
    if ap_id not in _access_points:
        return _NOT_FOUND(f"Access point {ap_id} not found")
    del _access_points[ap_id]
    _tags.pop(ap_id, None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# File-system policy
# ---------------------------------------------------------------------------

def _get_file_system_policy(fs_id):
    if fs_id not in _file_systems:
        return _NOT_FOUND(f"File system {fs_id} not found")
    policy = _policies.get(fs_id)
    if not policy:
        return _NOT_FOUND(f"No policy for file system {fs_id}")
    return _json({"fileSystemId": fs_id, "policy": policy})


def _put_file_system_policy(fs_id, data):
    if fs_id not in _file_systems:
        return _NOT_FOUND(f"File system {fs_id} not found")
    policy = data.get("policy")
    if not policy:
        return _VALIDATION("policy is required")
    _policies[fs_id] = policy
    return _empty_200()


def _delete_file_system_policy(fs_id):
    if fs_id not in _file_systems:
        return _NOT_FOUND(f"File system {fs_id} not found")
    _policies.pop(fs_id, None)
    return 204, {}, b""


# ---------------------------------------------------------------------------
# Synchronization configuration
# ---------------------------------------------------------------------------

def _get_sync_config(fs_id):
    if fs_id not in _file_systems:
        return _NOT_FOUND(f"File system {fs_id} not found")
    cfg = _sync_configs.get(fs_id)
    if not cfg:
        return _NOT_FOUND(f"No synchronization configuration for file system {fs_id}")
    return _json({
        "expirationDataRules": cfg.get("expirationDataRules", []),
        "importDataRules": cfg.get("importDataRules", []),
        "latestVersionNumber": cfg.get("latestVersionNumber", 1),
    })


def _put_sync_config(fs_id, data):
    if fs_id not in _file_systems:
        return _NOT_FOUND(f"File system {fs_id} not found")
    expiration = data.get("expirationDataRules")
    imports = data.get("importDataRules")
    if not expiration:
        return _VALIDATION("expirationDataRules is required")
    if not imports:
        return _VALIDATION("importDataRules is required")

    current = _sync_configs.get(fs_id)
    expected_version = data.get("latestVersionNumber")
    if current and expected_version is not None and expected_version != current.get("latestVersionNumber"):
        return _CONFLICT("latestVersionNumber does not match the current configuration")

    new_version = (current.get("latestVersionNumber", 0) + 1) if current else 1
    _sync_configs[fs_id] = {
        "expirationDataRules": list(expiration),
        "importDataRules": list(imports),
        "latestVersionNumber": new_version,
    }
    return _empty_200()


# ---------------------------------------------------------------------------
# Tagging
# ---------------------------------------------------------------------------

def _resource_exists(resource_id):
    return resource_id in _file_systems or resource_id in _access_points


def _tag_resource(resource_id, data):
    if not _resource_exists(resource_id):
        return _NOT_FOUND(f"Resource {resource_id} not found")
    incoming = data.get("tags")
    if not incoming:
        return _VALIDATION("tags is required")
    existing = _tags.get(resource_id, [])
    by_key = {t["key"]: t for t in existing if "key" in t}
    for t in incoming:
        k = t.get("key")
        if not k:
            continue
        by_key[k] = {"key": k, "value": t.get("value", "")}
    _tags[resource_id] = list(by_key.values())
    return _empty_200()


def _untag_resource(resource_id, tag_keys):
    if not _resource_exists(resource_id):
        return _NOT_FOUND(f"Resource {resource_id} not found")
    if not tag_keys:
        return _VALIDATION("tagKeys is required")
    existing = _tags.get(resource_id, [])
    _tags[resource_id] = [t for t in existing if t.get("key") not in set(tag_keys)]
    return _empty_200()


def _list_tags_for_resource(resource_id, query_params):
    if not _resource_exists(resource_id):
        return _NOT_FOUND(f"Resource {resource_id} not found")
    # AWS URI shows both `MaxResults` and `maxResults`; accept either.
    max_results = (
        _qp_int(query_params, "maxResults", None)
        or _qp_int(query_params, "MaxResults", 50)
        or 50
    )
    next_token = _qp_str(query_params, "nextToken") or _qp_str(query_params, "NextToken")
    tags = list(_tags.get(resource_id, []))
    page, nxt = _paginate(tags, max_results, next_token)
    out = {"tags": page}
    if nxt:
        out["nextToken"] = nxt
    return _json(out)
