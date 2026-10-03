# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
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
  FS Policy:      PutFileSystemPolicy, DescribeFileSystemPolicy, DeleteFileSystemPolicy
  Protection:     UpdateFileSystemProtection
  Replication:    CreateReplicationConfiguration, DescribeReplicationConfigurations,
                  DeleteReplicationConfiguration
  Account:        DescribeAccountPreferences, PutAccountPreferences
"""

import copy
import fnmatch
import ipaddress
import json
import logging
import os
import random
import re
import string
import time
import uuid

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    get_account_id,
    get_region,
    request_scope,
)

logger = logging.getLogger("efs")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_file_systems = AccountRegionScopedDict()    # fs_id -> fs record
_mount_targets = AccountRegionScopedDict()   # mt_id -> mount target record
_access_points = AccountRegionScopedDict()   # ap_id -> access point record
_replication_configs = AccountRegionScopedDict()  # source fs_id -> replication configuration

_PROTECTION_DEFAULT = "ENABLED"
_IP_ADDRESS_TYPES = ("IPV4_ONLY", "IPV6_ONLY", "DUAL_STACK")
_POLICY_MAX_LENGTH = 20000

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
    return int(time.time())

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
    arn = f"arn:aws:elasticfilesystem:{get_region()}:{get_account_id()}:file-system/{fs_id}"
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
        "OwnerId": get_account_id(),
        "Name": next((t["Value"] for t in tags if t["Key"] == "Name"), ""),
        "FileSystemProtection": {"ReplicationOverwriteProtection": _PROTECTION_DEFAULT},
    }
    if provisioned_throughput:
        record["ProvisionedThroughputInMibps"] = provisioned_throughput
    if body.get("AvailabilityZoneName"):
        from ministack.services import ec2

        record["AvailabilityZoneName"] = body["AvailabilityZoneName"]
        record["AvailabilityZoneId"] = ec2._az_id_for_zone_name(body["AvailabilityZoneName"])

    _file_systems[fs_id] = record
    return _json(201, _fs_response(record))


def _describe_file_systems(query):
    fs_id = query.get("FileSystemId")
    creation_token = query.get("CreationToken")
    max_items = int(query.get("MaxItems", 100))

    if fs_id and fs_id not in _file_systems:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")

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
        return _error(409, "FileSystemInUse",
                      f"File system '{fs_id}' has mount targets and cannot be deleted.")
    if _replication_of(fs_id):
        # "You cannot delete a file system that is part of an EFS replication
        # configuration." The documentation names no error code for it.
        return _error(409, "FileSystemInUse",
                      f"File system '{fs_id}' is part of a replication configuration; "
                      "delete the replication configuration first.")
    del _file_systems[fs_id]
    _lifecycle_configs.pop(fs_id, None)
    _backup_policies.pop(fs_id, None)
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
    response = {k: v for k, v in fs.items() if k != "FileSystemPolicy"}
    response.setdefault("FileSystemProtection", {"ReplicationOverwriteProtection": _PROTECTION_DEFAULT})
    return response


def _fs_id_from(value):
    """A file system id from an id or a ``...:file-system/fs-...`` ARN."""
    return str(value or "").rsplit("/", 1)[-1]


def _update_file_system_protection(fs_id, body):
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    value = body.get("ReplicationOverwriteProtection")
    if value is None:
        return _json(200, {"ReplicationOverwriteProtection": _fs_response(fs)["FileSystemProtection"][
            "ReplicationOverwriteProtection"]})
    if _replication_of(fs_id):
        return _error(409, "ReplicationAlreadyExists",
                      f"File system '{fs_id}' is already included in a replication configuration.")
    # REPLICATING marks a destination and is set by replication only; the
    # documentation does not say what a caller setting it gets back.
    if value not in ("ENABLED", "DISABLED"):
        return _error(400, "BadRequest",
                      "ReplicationOverwriteProtection must be ENABLED or DISABLED.")
    fs["FileSystemProtection"] = {"ReplicationOverwriteProtection": value}
    return _json(200, {"ReplicationOverwriteProtection": value})


# ---------------------------------------------------------------------------
# Replication
# ---------------------------------------------------------------------------

def _replication_of(fs_id):
    """The replication configuration that has the file system as its source or
    destination, from any region of the account, else None."""
    account = get_account_id()
    for (acct, _region, _source), config in _replication_configs.all_items():
        if acct != account:
            continue
        if config["SourceFileSystemId"] == fs_id or any(
                d["FileSystemId"] == fs_id for d in config["Destinations"]):
            return config
    return None


def _create_replication_configuration(source_id, body):
    source_id = _fs_id_from(source_id)
    source = _file_systems.get(source_id)
    if not source:
        return _error(404, "FileSystemNotFound", f"File system '{source_id}' does not exist.")
    destinations = body.get("Destinations")
    if not isinstance(destinations, list) or len(destinations) != 1:
        return _error(400, "BadRequest", "Exactly one destination is supported.")
    # "This file system cannot already be a source or destination file system in
    # another replication configuration." The documentation names no error code.
    if _replication_of(source_id):
        return _error(400, "BadRequest",
                      f"File system '{source_id}' is already part of a replication configuration.")
    destination = destinations[0]
    account = get_account_id()
    source_region = get_region()
    destination_region = destination.get("Region") or source_region
    destination_id = _fs_id_from(destination["FileSystemId"]) if destination.get("FileSystemId") else None
    with request_scope(account, destination_region):
        if destination_id:
            target = _file_systems.get(destination_id)
            if not target:
                return _error(404, "FileSystemNotFound",
                              f"File system '{destination_id}' does not exist.")
            if destination_id == source_id or _replication_of(destination_id):
                return _error(400, "BadRequest",
                              f"File system '{destination_id}' is already part of a replication configuration.")
            if _fs_response(target)["FileSystemProtection"]["ReplicationOverwriteProtection"] != "DISABLED":
                return _error(400, "BadRequest",
                              "The destination file system's replication overwrite protection must be DISABLED.")
            if source.get("Encrypted") and not target.get("Encrypted"):
                return _error(409, "ConflictException",
                              "The source file system is encrypted but the destination file system is not.")
        else:
            create = {
                "PerformanceMode": source["PerformanceMode"],
                "Encrypted": bool(source.get("Encrypted")),
            }
            if destination.get("KmsKeyId"):
                create["KmsKeyId"] = destination["KmsKeyId"]
                create["Encrypted"] = True
            if destination.get("AvailabilityZoneName"):
                create["AvailabilityZoneName"] = destination["AvailabilityZoneName"]
            created = json.loads(_create_file_system(create)[2])
            destination_id = created["FileSystemId"]
            target = _file_systems[destination_id]
        target["FileSystemProtection"] = {"ReplicationOverwriteProtection": "REPLICATING"}
    now = _now_iso()
    entry = {
        "Status": "ENABLED",
        "FileSystemId": destination_id,
        "Region": destination_region,
        "LastReplicatedTimestamp": now,
        "OwnerId": account,
    }
    if destination.get("RoleArn"):
        entry["RoleArn"] = destination["RoleArn"]
    config = {
        "SourceFileSystemId": source_id,
        "SourceFileSystemRegion": source_region,
        "SourceFileSystemArn": source["FileSystemArn"],
        "OriginalSourceFileSystemArn": source["FileSystemArn"],
        "CreationTime": now,
        "Destinations": [entry],
        "SourceFileSystemOwnerId": account,
    }
    _replication_configs[source_id] = config
    return _json(200, config)


def _describe_replication_configurations(query):
    fs_id = _fs_id_from(query.get("FileSystemId")) or None
    if fs_id and fs_id not in _file_systems:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    account = get_account_id()
    region = get_region()
    results = []
    for (acct, _region, _source), config in _replication_configs.all_items():
        if acct != account:
            continue
        if fs_id:
            if config["SourceFileSystemId"] != fs_id and not any(
                    d["FileSystemId"] == fs_id for d in config["Destinations"]):
                continue
        elif config["SourceFileSystemRegion"] != region and not any(
                d["Region"] == region for d in config["Destinations"]):
            continue
        results.append(config)
    if fs_id and not results:
        return _error(404, "ReplicationNotFound",
                      f"File system '{fs_id}' does not have a replication configuration.")
    results.sort(key=lambda c: c["SourceFileSystemId"])
    try:
        offset = int(query.get("NextToken") or 0)
        limit = int(query.get("MaxResults") or 100)
    except ValueError:
        return _error(400, "BadRequest", "NextToken or MaxResults is not valid.")
    page = results[offset:offset + limit]
    response = {"Replications": page}
    if offset + limit < len(results):
        response["NextToken"] = str(offset + limit)
    return _json(200, response)


def _delete_replication_configuration(source_id, query):
    source_id = _fs_id_from(source_id)
    if source_id not in _file_systems:
        return _error(404, "FileSystemNotFound", f"File system '{source_id}' does not exist.")
    config = _replication_configs.get(source_id)
    if not config:
        return _error(404, "ReplicationNotFound",
                      f"File system '{source_id}' does not have a replication configuration.")
    mode = query.get("deletionMode") or query.get("DeletionMode") or "ALL_CONFIGURATIONS"
    if mode not in ("ALL_CONFIGURATIONS", "LOCAL_CONFIGURATION_ONLY"):
        return _error(400, "BadRequest", "DeletionMode must be ALL_CONFIGURATIONS or LOCAL_CONFIGURATION_ONLY.")
    if mode == "LOCAL_CONFIGURATION_ONLY" and all(
            d["Region"] == get_region() for d in config["Destinations"]):
        return _error(400, "BadRequest",
                      "LOCAL_CONFIGURATION_ONLY is not valid for same-account, same-region replication.")
    # "After a replication configuration is deleted, the destination file system
    # becomes writeable and its replication overwrite protection is re-enabled."
    for destination in config["Destinations"]:
        with request_scope(get_account_id(), destination["Region"]):
            target = _file_systems.get(destination["FileSystemId"])
            if target:
                target["FileSystemProtection"] = {"ReplicationOverwriteProtection": _PROTECTION_DEFAULT}
    del _replication_configs[source_id]
    return _json(204, {})


# ---------------------------------------------------------------------------
# Mount Targets
# ---------------------------------------------------------------------------

def _pick_address(cidr, used, requested, kind):
    """The address a mount target takes in the subnet range: the requested one
    when it lies in the range and is free, else the first free host after the
    four addresses a subnet reserves at its start. Returns (address, error)."""
    network = ipaddress.ip_network(cidr, strict=False)
    if requested:
        try:
            address = ipaddress.ip_address(requested)
        except ValueError:
            return None, _error(400, "BadRequest", f"'{requested}' is not a valid {kind} address.")
        if address.version != network.version or address not in network:
            return None, _error(400, "BadRequest",
                                f"{kind} address '{requested}' is not in the subnet range {cidr}.")
        if str(address) in used:
            return None, _error(409, "IpAddressInUse", f"{kind} address '{requested}' is already in use.")
        return str(address), None
    for host in network.hosts():
        if int(host) - int(network.network_address) < 4:
            continue
        if str(host) not in used:
            return str(host), None
    return None, _error(409, "NoFreeAddressesInSubnet", f"The subnet has no free {kind} addresses.")


def _security_groups_in_vpc(security_groups, vpc_id):
    """The security groups for a mount target: the ones given, which must exist
    in the subnet's VPC, else the VPC's default group. Returns (ids, error)."""
    from ministack.services import ec2

    if not security_groups:
        default = next(
            (g["GroupId"] for g in ec2._security_groups.values()
             if g.get("VpcId") == vpc_id and g.get("GroupName") == "default"), None)
        return ([default] if default else []), None
    for group_id in security_groups:
        group = ec2._security_groups.get(group_id)
        if not group or group.get("VpcId") != vpc_id:
            return None, _error(400, "SecurityGroupNotFound",
                                f"Security group '{group_id}' does not exist in VPC '{vpc_id}'.")
    return list(security_groups), None


def _create_mount_target(body):
    from ministack.services import ec2

    fs_id = _fs_id_from(body.get("FileSystemId"))
    subnet_id = body.get("SubnetId", "")
    address_type = body.get("IpAddressType") or "IPV4_ONLY"

    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    if fs.get("LifeCycleState") != "available":
        return _error(409, "IncorrectFileSystemLifeCycleState",
                      f"File system '{fs_id}' is not in the available state.")
    if address_type not in _IP_ADDRESS_TYPES:
        return _error(400, "BadRequest", f"IpAddressType must be one of {', '.join(_IP_ADDRESS_TYPES)}.")
    wants_v4 = address_type in ("IPV4_ONLY", "DUAL_STACK")
    wants_v6 = address_type in ("IPV6_ONLY", "DUAL_STACK")
    if body.get("IpAddress") and not wants_v4:
        return _error(400, "BadRequest", "IpAddress requires an IpAddressType of IPV4_ONLY or DUAL_STACK.")
    if body.get("Ipv6Address") and not wants_v6:
        return _error(400, "BadRequest", "Ipv6Address requires an IpAddressType of IPV6_ONLY or DUAL_STACK.")

    ec2._ensure_defaults_initialized()
    subnet = ec2._subnets.get(subnet_id)
    if not subnet:
        return _error(400, "SubnetNotFound", f"The subnet '{subnet_id}' does not exist.")
    ipv6_cidr = subnet.get("Ipv6CidrBlock")
    if wants_v6 and not ipv6_cidr:
        return _error(400, "BadRequest",
                      f"Subnet '{subnet_id}' has no IPv6 CIDR block; the subnet type must match the IpAddressType.")
    zone = subnet["AvailabilityZone"]
    if fs.get("AvailabilityZoneName") and fs["AvailabilityZoneName"] != zone:
        return _error(400, "AvailabilityZonesMismatch",
                      f"The subnet is in {zone}, but the One Zone file system is in {fs['AvailabilityZoneName']}.")
    for other in _mount_targets.values():
        if other["FileSystemId"] != fs_id:
            continue
        if other["VpcId"] != subnet["VpcId"]:
            return _error(409, "MountTargetConflict",
                          "A file system can have mount targets in only one VPC.")
        if other["AvailabilityZoneName"] == zone:
            return _error(409, "MountTargetConflict",
                          f"The file system already has a mount target in {zone}.")
    groups, err = _security_groups_in_vpc(body.get("SecurityGroups") or [], subnet["VpcId"])
    if err:
        return err

    in_subnet = [mt for mt in _mount_targets.values() if mt["SubnetId"] == subnet_id]
    ipv4 = ipv6 = None
    if wants_v4:
        ipv4, err = _pick_address(subnet["CidrBlock"], {mt.get("IpAddress") for mt in in_subnet},
                                  body.get("IpAddress"), "IPv4")
        if err:
            return err
    if wants_v6:
        ipv6, err = _pick_address(ipv6_cidr, {mt.get("Ipv6Address") for mt in in_subnet},
                                  body.get("Ipv6Address"), "IPv6")
        if err:
            return err

    mt_id = _mt_id()
    arn = f"arn:aws:elasticfilesystem:{get_region()}:{get_account_id()}:file-system/{fs_id}/mount-target/{mt_id}"
    record = {
        "MountTargetId": mt_id,
        "FileSystemId": fs_id,
        "SubnetId": subnet_id,
        "AvailabilityZoneId": subnet["AvailabilityZoneId"],
        "AvailabilityZoneName": zone,
        "VpcId": subnet["VpcId"],
        "LifeCycleState": "available",
        "NetworkInterfaceId": "eni-" + "".join(random.choices(string.hexdigits[:16], k=17)),
        "OwnerId": get_account_id(),
        "MountTargetArn": arn,
        "SecurityGroups": groups,
        "IpAddressType": address_type,
    }
    if ipv4:
        record["IpAddress"] = ipv4
    if ipv6:
        record["Ipv6Address"] = ipv6
    _mount_targets[mt_id] = record
    fs["NumberOfMountTargets"] = fs.get("NumberOfMountTargets", 0) + 1

    return _json(200, _mt_response(record))


def _describe_mount_targets(query):
    fs_id = query.get("FileSystemId")
    mt_id = query.get("MountTargetId")
    max_items = int(query.get("MaxItems", 100))

    if mt_id and mt_id not in _mount_targets:
        return _error(404, "MountTargetNotFound", f"Mount target '{mt_id}' does not exist.")

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
    groups, err = _security_groups_in_vpc(body.get("SecurityGroups") or [], mt["VpcId"])
    if err:
        return err
    mt["SecurityGroups"] = groups
    return _json(204, {})


def _mt_response(mt):
    return {k: v for k, v in mt.items() if k not in ("SecurityGroups", "IpAddressType")}


# ---------------------------------------------------------------------------
# Access Points
# ---------------------------------------------------------------------------

def _create_access_point(body):
    fs_id = body.get("FileSystemId")
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")

    ap_id = _ap_id()
    arn = f"arn:aws:elasticfilesystem:{get_region()}:{get_account_id()}:access-point/{ap_id}"
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
        "OwnerId": get_account_id(),
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

def _tag_resource_not_found(resource_id):
    # Per AWS EFS docs the resource-not-found error is keyed by the resource
    # type — FileSystemNotFound for fs-* / file-system ARNs,
    # AccessPointNotFound for fsap-* / access-point ARNs. Falling back to
    # BadRequest for anything else matches the EFS regex on ResourceId.
    if resource_id.startswith("fs-") or "file-system/fs-" in resource_id:
        return _error(404, "FileSystemNotFound", f"File system '{resource_id}' does not exist.")
    if resource_id.startswith("fsap-") or "access-point/fsap-" in resource_id:
        return _error(404, "AccessPointNotFound", f"Access point '{resource_id}' does not exist.")
    return _error(400, "BadRequest", f"Resource id '{resource_id}' is not a valid EFS resource.")


def _resource_id_from_arn(resource_id):
    try:
        spec = parse_arn(resource_id)
    except ArnParseError:
        return None, _error(400, "BadRequest", f"Resource id '{resource_id}' is not a valid EFS resource.")

    if spec.partition != "aws" or spec.service != "elasticfilesystem":
        return None, _error(400, "BadRequest", f"Resource id '{resource_id}' is not a valid EFS resource.")

    parts = spec.resource.split("/")
    if len(parts) != 2 or parts[0] not in {"file-system", "access-point"} or not parts[1]:
        return None, _error(400, "BadRequest", f"Resource id '{resource_id}' is not a valid EFS resource.")
    resource_type, lookup_id = parts
    if resource_type == "file-system" and not lookup_id.startswith("fs-"):
        return None, _error(400, "BadRequest", f"Resource id '{resource_id}' is not a valid EFS resource.")
    if resource_type == "access-point" and not lookup_id.startswith("fsap-"):
        return None, _error(400, "BadRequest", f"Resource id '{resource_id}' is not a valid EFS resource.")

    if spec.region != get_region() or spec.account_id != get_account_id():
        return None, _tag_resource_not_found(resource_id)
    return (resource_type, lookup_id), None


def _resolve_tag_resource(resource_id):
    lookup_id = resource_id
    resource_type = None
    if resource_id.startswith("arn:"):
        resolved, err = _resource_id_from_arn(resource_id)
        if err:
            return None, err
        resource_type, lookup_id = resolved
    resource = _find_resource(lookup_id, resource_type)
    if resource is None:
        return None, _tag_resource_not_found(resource_id)
    return resource, None


def _tag_resource(resource_id, body):
    resource, err = _resolve_tag_resource(resource_id)
    if err:
        return err
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
    resource, err = _resolve_tag_resource(resource_id)
    if err:
        return err
    resource["Tags"] = [t for t in resource.get("Tags", []) if t["Key"] not in keys]
    return _json(200, {})


def _list_tags_for_resource(resource_id):
    resource, err = _resolve_tag_resource(resource_id)
    if err:
        return err
    return _json(200, {"Tags": resource.get("Tags", [])})


def _find_resource(resource_id, resource_type=None):
    if resource_type in (None, "file-system") and resource_id in _file_systems:
        return _file_systems[resource_id]
    if resource_type in (None, "access-point") and resource_id in _access_points:
        return _access_points[resource_id]
    return None


# ---------------------------------------------------------------------------
# Lifecycle / Backup / Account (stubs)
# ---------------------------------------------------------------------------

_lifecycle_configs = AccountRegionScopedDict()
_backup_policies = AccountRegionScopedDict()


def _clear_state():
    _file_systems.clear()
    _mount_targets.clear()
    _access_points.clear()
    _replication_configs.clear()
    _lifecycle_configs.clear()
    _backup_policies.clear()


def get_state():
    return copy.deepcopy({
        "file_systems": _file_systems,
        "mount_targets": _mount_targets,
        "access_points": _access_points,
        "replication_configs": _replication_configs,
        "lifecycle_configs": _lifecycle_configs,
        "backup_policies": _backup_policies,
    })


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not data:
        return
    _clear_state()
    _file_systems.update(data.get("file_systems", {}))
    _mount_targets.update(data.get("mount_targets", {}))
    _access_points.update(data.get("access_points", {}))
    _replication_configs.update(data.get("replication_configs", {}))
    fs_regions = {
        (account_id, fs_id): region
        for (account_id, region, fs_id), _fs in _file_systems.all_items()
    }
    for store, state_key in (
        (_lifecycle_configs, "lifecycle_configs"),
        (_backup_policies, "backup_policies"),
    ):
        _restore_file_system_child_store(
            store,
            data.get(state_key, {}),
            fs_regions,
        )


def _restore_file_system_child_store(store, restored, fs_regions):
    """Adopt legacy ARN-less state into its parent file system's region."""
    if isinstance(restored, AccountRegionScopedDict):
        store.update(restored)
        return

    if isinstance(restored, AccountScopedDict):
        items = restored._data.items()
    else:
        account_id = get_account_id()
        items = (((account_id, key), value) for key, value in restored.items())

    for (account_id, fs_id), value in items:
        region = fs_regions.get(
            (account_id, fs_id),
            store._region_for_legacy_value(fs_id, value),
        )
        store.set_scoped(account_id, region, fs_id, value)




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


def _policy_locks_out_caller(statements):
    """Whether a statement denies PutFileSystemPolicy to every principal with no
    condition: the lockout the safety check refuses. Allow statements never
    lock out a same-account caller, and a conditional Deny cannot be evaluated
    here, so only the unconditional Deny counts (inference: the documentation
    describes the check by its purpose, not its rules)."""
    for statement in statements:
        if statement.get("Effect") != "Deny" or statement.get("Condition"):
            continue
        principal = statement.get("Principal")
        if isinstance(principal, dict):
            values = [v for value in principal.values() for v in (value if isinstance(value, list) else [value])]
        else:
            values = [principal]
        if "*" not in values:
            continue
        actions = statement.get("Action") or []
        actions = [actions] if isinstance(actions, str) else actions
        if any(fnmatch.fnmatchcase("elasticfilesystem:putfilesystempolicy", str(a).lower()) for a in actions):
            return True
    return False


def _put_file_system_policy(fs_id, body):
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    raw = body.get("Policy")
    if not isinstance(raw, str) or not 1 <= len(raw) <= _POLICY_MAX_LENGTH:
        return _error(400, "BadRequest", f"Policy must be between 1 and {_POLICY_MAX_LENGTH} characters.")
    try:
        policy = json.loads(raw)
    except ValueError:
        return _error(400, "InvalidPolicyException", "The file system policy is not valid JSON.")
    statements = policy.get("Statement") if isinstance(policy, dict) else None
    if isinstance(statements, dict):
        statements = [statements]
    if not statements or not all(
            isinstance(s, dict) and s.get("Effect") in ("Allow", "Deny") and ("Action" in s or "NotAction" in s)
            for s in statements):
        return _error(400, "InvalidPolicyException",
                      "The file system policy needs a Statement list whose entries have an Effect and an Action.")
    if not body.get("BypassPolicyLockoutSafetyCheck") and _policy_locks_out_caller(statements):
        return _error(400, "InvalidPolicyException",
                      "The policy would lock out the caller from PutFileSystemPolicy; "
                      "set BypassPolicyLockoutSafetyCheck to override.")
    # The documentation's example responses carry an Id, a Sid per statement and
    # the file system ARN as Resource where the request left them out.
    policy["Statement"] = statements
    policy.setdefault("Id", "1")
    for statement in statements:
        statement.setdefault("Sid", f"efs-statement-{uuid.uuid4()}")
        if "Resource" not in statement and "NotResource" not in statement:
            statement["Resource"] = fs["FileSystemArn"]
    stored = json.dumps(policy)
    fs["FileSystemPolicy"] = stored
    return _json(200, {"FileSystemId": fs_id, "Policy": stored})


def _describe_file_system_policy(fs_id):
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    policy = fs.get("FileSystemPolicy")
    if not policy:
        return _error(404, "PolicyNotFound", f"File system '{fs_id}' has no file system policy.")
    return _json(200, {"FileSystemId": fs_id, "Policy": policy})


def _delete_file_system_policy(fs_id):
    fs = _file_systems.get(fs_id)
    if not fs:
        return _error(404, "FileSystemNotFound", f"File system '{fs_id}' does not exist.")
    fs.pop("FileSystemPolicy", None)
    return _json(200, {})


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

    # Replication and protection
    if p == "/file-systems/replication-configurations" and method == "GET":
        return await _a(_describe_replication_configurations(query))

    m = re.fullmatch(r"/file-systems/([^/]+)/replication-configuration", p)
    if m:
        if method == "POST":
            return await _a(_create_replication_configuration(m.group(1), body))
        if method == "DELETE":
            return await _a(_delete_replication_configuration(m.group(1), query))

    m = re.fullmatch(r"/file-systems/([^/]+)/protection", p)
    if m and method == "PUT":
        return await _a(_update_file_system_protection(_fs_id_from(m.group(1)), body))

    # File System Policy
    m = re.fullmatch(r"/file-systems/([^/]+)/policy", p)
    if m:
        fs_id = m.group(1)
        if method == "PUT":
            return await _a(_put_file_system_policy(fs_id, body))
        if method == "GET":
            return await _a(_describe_file_system_policy(fs_id))
        if method == "DELETE":
            return await _a(_delete_file_system_policy(fs_id))

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
    _clear_state()
