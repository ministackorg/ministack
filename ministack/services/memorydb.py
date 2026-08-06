"""
MemoryDB Emulator.
JSON-based API via X-Amz-Target (prefix: AmazonMemoryDB).

Control-plane emulation only. MemoryDB is Redis-compatible, and MiniStack
already backs ElastiCache with real Redis; the data plane is ElastiCache's
concern. Here we emulate only the control plane (clusters, subnet groups,
ACLs, parameter groups, users), with state stored in in-memory dicts.

Shapes verified against botocore memorydb/2021-01-01/service-2.json
(targetPrefix AmazonMemoryDB, endpointPrefix memory-db, signingName memorydb).
Timestamps are not part of these shapes; created resources report
Status: available immediately (mirrors how rds/elasticache report status).
"""

import json
import logging

from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
)

logger = logging.getLogger("memorydb")

# resource_name -> record dict (JSON-serializable, botocore shapes)
_clusters = AccountRegionScopedDict()
_subnet_groups = AccountRegionScopedDict()
_acls = AccountRegionScopedDict()
_parameter_groups = AccountRegionScopedDict()
_users = AccountRegionScopedDict()

# Default resources exist in a real MemoryDB account. We seed the "open-access"
# ACL and the "default" user so ACL/user round-trips mirror AWS defaults.
_DEFAULT_PARAMETER_GROUP = "default.memorydb-redis7"


def _arn(resource_type, name):
    return (
        f"arn:aws:memorydb:{get_region()}:{get_account_id()}:{resource_type}/{name}"
    )


# ── Resource builders (botocore output shapes) ─────────────────


def _cluster_record(data):
    name = data.get("ClusterName", "")
    num_shards = data.get("NumShards", 1)
    node_type = data.get("NodeType", "db.r6g.large")
    engine = data.get("Engine", "redis")
    engine_version = data.get("EngineVersion", "7.1")
    param_group = data.get("ParameterGroupName", _DEFAULT_PARAMETER_GROUP)
    rec = {
        "Name": name,
        "Description": data.get("Description", ""),
        "Status": "available",
        "NumberOfShards": num_shards,
        "AvailabilityMode": "MultiAZ"
        if data.get("NumReplicasPerShard", 1) >= 1
        else "SingleAZ",
        "ClusterEndpoint": {
            "Address": f"clustercfg.{name}.xxxxxx.memorydb.{get_region()}.amazonaws.com",
            "Port": data.get("Port", 6379),
        },
        "NodeType": node_type,
        "Engine": engine,
        "EngineVersion": engine_version,
        "EnginePatchVersion": engine_version,
        "ParameterGroupName": param_group,
        "ParameterGroupStatus": "in-sync",
        "SecurityGroups": [
            {"SecurityGroupId": sg, "Status": "active"}
            for sg in data.get("SecurityGroupIds", [])
        ],
        "SubnetGroupName": data.get("SubnetGroupName", "default"),
        "TLSEnabled": data.get("TLSEnabled", True),
        "ARN": _arn("cluster", name),
        "SnsTopicArn": data.get("SnsTopicArn", ""),
        "SnsTopicStatus": "active" if data.get("SnsTopicArn") else "",
        "SnapshotRetentionLimit": data.get("SnapshotRetentionLimit", 0),
        "MaintenanceWindow": data.get("MaintenanceWindow", "wed:09:00-wed:10:00"),
        "SnapshotWindow": data.get("SnapshotWindow", "07:00-08:00"),
        "ACLName": data.get("ACLName", "open-access"),
        "AutoMinorVersionUpgrade": data.get("AutoMinorVersionUpgrade", True),
        "DataTiering": "true" if data.get("DataTiering") else "false",
    }
    # Echo optional inputs only when supplied (AWS omits unset members).
    for key in ("KmsKeyId", "MultiRegionClusterName", "NetworkType", "IpDiscovery"):
        if data.get(key):
            rec[key] = data[key]
    return rec


def _subnet_group_record(data):
    name = data.get("SubnetGroupName", "")
    subnet_ids = data.get("SubnetIds", [])
    return {
        "Name": name,
        "Description": data.get("Description", ""),
        "VpcId": "vpc-ministack",
        "Subnets": [
            {
                "Identifier": sid,
                "AvailabilityZone": {"Name": f"{get_region()}a"},
                "SupportedNetworkTypes": ["ipv4"],
            }
            for sid in subnet_ids
        ],
        "ARN": _arn("subnetgroup", name),
        "SupportedNetworkTypes": ["ipv4"],
    }


def _acl_record(data):
    name = data.get("ACLName", "")
    user_names = data.get("UserNames", [])
    return {
        "Name": name,
        "Status": "active",
        "UserNames": user_names,
        "MinimumEngineVersion": "7.1",
        "Clusters": [],
        "ARN": _arn("acl", name),
    }


def _parameter_group_record(data):
    name = data.get("ParameterGroupName", "")
    return {
        "Name": name,
        "Family": data.get("Family", "memorydb_redis7"),
        "Description": data.get("Description", ""),
        "ARN": _arn("parametergroup", name),
    }


def _user_record(data):
    name = data.get("UserName", "")
    auth_mode = data.get("AuthenticationMode", {}) or {}
    auth_type = auth_mode.get("Type", "password")
    return {
        "Name": name,
        "Status": "active",
        "AccessString": data.get("AccessString", ""),
        "ACLNames": [],
        "MinimumEngineVersion": "7.1",
        "Authentication": {
            "Type": auth_type,
            "PasswordCount": len(auth_mode.get("Passwords", []))
            if auth_type == "password"
            else 0,
        },
        "ARN": _arn("user", name),
    }


# ── Cluster ops ────────────────────────────────────────────────


def _create_cluster(data):
    name = data.get("ClusterName", "")
    if name in _clusters:
        return error_response_json(
            "ClusterAlreadyExistsFault",
            f"Cluster {name} already exists.",
            400,
        )
    rec = _cluster_record(data)
    _clusters[name] = rec
    logger.info("Created cluster %s", name)
    return json_response({"Cluster": rec})


def _describe_clusters(data):
    name = data.get("ClusterName")
    if name:
        rec = _clusters.get(name)
        if not rec:
            return error_response_json(
                "ClusterNotFoundFault",
                f"Cluster {name} not found.",
                400,
            )
        clusters = [rec]
    else:
        clusters = list(_clusters.values())
    return json_response({"Clusters": clusters})


def _delete_cluster(data):
    name = data.get("ClusterName", "")
    rec = _clusters.get(name)
    if not rec:
        return error_response_json(
            "ClusterNotFoundFault",
            f"Cluster {name} not found.",
            400,
        )
    rec["Status"] = "deleting"
    del _clusters[name]
    return json_response({"Cluster": rec})


def _update_cluster(data):
    name = data.get("ClusterName", "")
    rec = _clusters.get(name)
    if not rec:
        return error_response_json(
            "ClusterNotFoundFault",
            f"Cluster {name} not found.",
            400,
        )
    for field, key in (
        ("Description", "Description"),
        ("MaintenanceWindow", "MaintenanceWindow"),
        ("SnsTopicArn", "SnsTopicArn"),
        ("ParameterGroupName", "ParameterGroupName"),
        ("SnapshotWindow", "SnapshotWindow"),
        ("SnapshotRetentionLimit", "SnapshotRetentionLimit"),
        ("NodeType", "NodeType"),
        ("EngineVersion", "EngineVersion"),
        ("ACLName", "ACLName"),
    ):
        if key in data:
            rec[field] = data[key]
    if data.get("SecurityGroupIds") is not None:
        rec["SecurityGroups"] = [
            {"SecurityGroupId": sg, "Status": "active"}
            for sg in data["SecurityGroupIds"]
        ]
    return json_response({"Cluster": rec})


# ── Subnet group ops ───────────────────────────────────────────


def _create_subnet_group(data):
    name = data.get("SubnetGroupName", "")
    if name in _subnet_groups:
        return error_response_json(
            "SubnetGroupAlreadyExistsFault",
            f"SubnetGroup {name} already exists.",
            400,
        )
    rec = _subnet_group_record(data)
    _subnet_groups[name] = rec
    return json_response({"SubnetGroup": rec})


def _describe_subnet_groups(data):
    name = data.get("SubnetGroupName")
    if name:
        rec = _subnet_groups.get(name)
        if not rec:
            return error_response_json(
                "SubnetGroupNotFoundFault",
                f"SubnetGroup {name} not found.",
                400,
            )
        groups = [rec]
    else:
        groups = list(_subnet_groups.values())
    return json_response({"SubnetGroups": groups})


def _delete_subnet_group(data):
    name = data.get("SubnetGroupName", "")
    rec = _subnet_groups.get(name)
    if not rec:
        return error_response_json(
            "SubnetGroupNotFoundFault",
            f"SubnetGroup {name} not found.",
            400,
        )
    del _subnet_groups[name]
    return json_response({"SubnetGroup": rec})


# ── ACL ops ────────────────────────────────────────────────────


def _create_acl(data):
    name = data.get("ACLName", "")
    if name in _acls:
        return error_response_json(
            "ACLAlreadyExistsFault",
            f"ACL {name} already exists.",
            400,
        )
    rec = _acl_record(data)
    _acls[name] = rec
    return json_response({"ACL": rec})


def _describe_acls(data):
    name = data.get("ACLName")
    if name:
        rec = _acls.get(name)
        if not rec:
            return error_response_json(
                "ACLNotFoundFault",
                f"ACL {name} not found.",
                400,
            )
        acls = [rec]
    else:
        acls = list(_acls.values())
    return json_response({"ACLs": acls})


def _delete_acl(data):
    name = data.get("ACLName", "")
    rec = _acls.get(name)
    if not rec:
        return error_response_json(
            "ACLNotFoundFault",
            f"ACL {name} not found.",
            400,
        )
    del _acls[name]
    return json_response({"ACL": rec})


# ── Parameter group ops ────────────────────────────────────────


def _create_parameter_group(data):
    name = data.get("ParameterGroupName", "")
    if name in _parameter_groups:
        return error_response_json(
            "ParameterGroupAlreadyExistsFault",
            f"ParameterGroup {name} already exists.",
            400,
        )
    rec = _parameter_group_record(data)
    _parameter_groups[name] = rec
    return json_response({"ParameterGroup": rec})


def _describe_parameter_groups(data):
    name = data.get("ParameterGroupName")
    if name:
        rec = _parameter_groups.get(name)
        if not rec:
            return error_response_json(
                "ParameterGroupNotFoundFault",
                f"ParameterGroup {name} not found.",
                400,
            )
        groups = [rec]
    else:
        groups = list(_parameter_groups.values())
    return json_response({"ParameterGroups": groups})


def _describe_parameters(data):
    name = data.get("ParameterGroupName", "")
    # DescribeParameters targets a named parameter group; unknown -> fault.
    if name and name not in _parameter_groups and name != _DEFAULT_PARAMETER_GROUP:
        return error_response_json(
            "ParameterGroupNotFoundFault",
            f"ParameterGroup {name} not found.",
            400,
        )
    return json_response({"Parameters": []})


# ── User ops ───────────────────────────────────────────────────


def _create_user(data):
    name = data.get("UserName", "")
    if name in _users:
        return error_response_json(
            "UserAlreadyExistsFault",
            f"User {name} already exists.",
            400,
        )
    rec = _user_record(data)
    _users[name] = rec
    return json_response({"User": rec})


def _describe_users(data):
    name = data.get("UserName")
    if name:
        rec = _users.get(name)
        if not rec:
            return error_response_json(
                "UserNotFoundFault",
                f"User {name} not found.",
                400,
            )
        users = [rec]
    else:
        users = list(_users.values())
    return json_response({"Users": users})


def _delete_user(data):
    name = data.get("UserName", "")
    rec = _users.get(name)
    if not rec:
        return error_response_json(
            "UserNotFoundFault",
            f"User {name} not found.",
            400,
        )
    del _users[name]
    return json_response({"User": rec})


# ── Tags ───────────────────────────────────────────────────────
# MemoryDB Tag shape is {Key, Value}. Tags are keyed off the resource ARN.

_tags = AccountRegionScopedDict()  # ResourceArn -> list[{Key, Value}]


def _resource_exists(arn):
    """MemoryDB tag ops validate the ARN against a real resource."""
    for store, rtype in (
        (_clusters, "cluster"),
        (_subnet_groups, "subnetgroup"),
        (_acls, "acl"),
        (_parameter_groups, "parametergroup"),
        (_users, "user"),
    ):
        for rec in store.values():
            if rec.get("ARN") == arn:
                return True
    return False


def _list_tags(data):
    arn = data.get("ResourceArn", "")
    if not _resource_exists(arn):
        return error_response_json(
            "InvalidARNFault",
            f"{arn} is not a valid MemoryDB resource.",
            400,
        )
    return json_response({"TagList": _tags.get(arn, [])})


def _tag_resource(data):
    arn = data.get("ResourceArn", "")
    if not _resource_exists(arn):
        return error_response_json(
            "InvalidARNFault",
            f"{arn} is not a valid MemoryDB resource.",
            400,
        )
    current = _tags.get(arn, [])
    by_key = {t["Key"]: t for t in current}
    for tag in data.get("Tags", []):
        by_key[tag["Key"]] = {"Key": tag["Key"], "Value": tag.get("Value", "")}
    tag_list = list(by_key.values())
    _tags[arn] = tag_list
    return json_response({"TagList": tag_list})


def _untag_resource(data):
    arn = data.get("ResourceArn", "")
    if not _resource_exists(arn):
        return error_response_json(
            "InvalidARNFault",
            f"{arn} is not a valid MemoryDB resource.",
            400,
        )
    remove = set(data.get("TagKeys", []))
    tag_list = [t for t in _tags.get(arn, []) if t["Key"] not in remove]
    _tags[arn] = tag_list
    return json_response({"TagList": tag_list})


# ── Request handler ────────────────────────────────────────────

_HANDLERS = {
    "CreateCluster": _create_cluster,
    "DescribeClusters": _describe_clusters,
    "DeleteCluster": _delete_cluster,
    "UpdateCluster": _update_cluster,
    "CreateSubnetGroup": _create_subnet_group,
    "DescribeSubnetGroups": _describe_subnet_groups,
    "DeleteSubnetGroup": _delete_subnet_group,
    "CreateACL": _create_acl,
    "DescribeACLs": _describe_acls,
    "DeleteACL": _delete_acl,
    "CreateParameterGroup": _create_parameter_group,
    "DescribeParameterGroups": _describe_parameter_groups,
    "DescribeParameters": _describe_parameters,
    "CreateUser": _create_user,
    "DescribeUsers": _describe_users,
    "DeleteUser": _delete_user,
    "ListTags": _list_tags,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handler = _HANDLERS.get(action)
    if not handler:
        logger.warning("Unknown MemoryDB action: %s", action)
        return error_response_json(
            "InvalidAction", f"Unknown action: {action}", 400
        )
    return handler(data)


def reset():
    _clusters.clear()
    _subnet_groups.clear()
    _acls.clear()
    _parameter_groups.clear()
    _users.clear()
    _tags.clear()
