"""
EC2 Service Emulator.
Query API (Action=...) — instances exist in memory only, no real VMs launched.

Supports:
  Instances:       RunInstances, TerminateInstances, DescribeInstances,
                   DescribeInstanceStatus, StartInstances, StopInstances, RebootInstances
  Images:          DescribeImages (stub — returns common AMI IDs)
  Security Groups: CreateSecurityGroup, DeleteSecurityGroup, DescribeSecurityGroups,
                   AuthorizeSecurityGroupIngress, RevokeSecurityGroupIngress,
                   AuthorizeSecurityGroupEgress, RevokeSecurityGroupEgress
  Key Pairs:       CreateKeyPair, DeleteKeyPair, DescribeKeyPairs, ImportKeyPair
  VPC / Subnets:   DescribeVpcs, DescribeSubnets, DescribeAvailabilityZones
                   CreateVpc, CreateDefaultVpc, DeleteVpc, CreateSubnet, DeleteSubnet
                   CreateInternetGateway, DeleteInternetGateway, DescribeInternetGateways,
                   AttachInternetGateway, DetachInternetGateway
  Elastic IPs:     AllocateAddress, ReleaseAddress, AssociateAddress, DisassociateAddress,
                   DescribeAddresses
  Tags:            CreateTags, DeleteTags, DescribeTags
  VPC attributes:  ModifyVpcAttribute, ModifySubnetAttribute
  Route Tables:    CreateRouteTable, DeleteRouteTable, DescribeRouteTables,
                   AssociateRouteTable, DisassociateRouteTable, ReplaceRouteTableAssociation,
                   CreateRoute, ReplaceRoute, DeleteRoute
  ENI:             CreateNetworkInterface, DeleteNetworkInterface, DescribeNetworkInterfaces,
                   AttachNetworkInterface, DetachNetworkInterface
  VPC Endpoints:   CreateVpcEndpoint, DeleteVpcEndpoints, DescribeVpcEndpoints,
                   ModifyVpcEndpoint, DescribePrefixLists
  EBS Volumes:     CreateVolume, DeleteVolume, DescribeVolumes, DescribeVolumeStatus,
                   AttachVolume, DetachVolume, ModifyVolume, DescribeVolumesModifications,
                   EnableVolumeIO, ModifyVolumeAttribute, DescribeVolumeAttribute
  EBS Snapshots:   CreateSnapshot, DeleteSnapshot, DescribeSnapshots,
                   ModifySnapshotAttribute, DescribeSnapshotAttribute, CopySnapshot
  NAT Gateways:    CreateNatGateway, DescribeNatGateways, DeleteNatGateway
  Network ACLs:    CreateNetworkAcl, DescribeNetworkAcls, DeleteNetworkAcl,
                   CreateNetworkAclEntry, DeleteNetworkAclEntry, ReplaceNetworkAclEntry,
                   ReplaceNetworkAclAssociation
  Flow Logs:       CreateFlowLogs, DescribeFlowLogs, DeleteFlowLogs
  VPC Peering:     CreateVpcPeeringConnection, AcceptVpcPeeringConnection,
                   DescribeVpcPeeringConnections, DeleteVpcPeeringConnection
  DHCP Options:    CreateDhcpOptions, AssociateDhcpOptions, DescribeDhcpOptions,
                   DeleteDhcpOptions
  Egress IGW:      CreateEgressOnlyInternetGateway, DescribeEgressOnlyInternetGateways,
                   DeleteEgressOnlyInternetGateway
  Prefix Lists:    CreateManagedPrefixList, DescribeManagedPrefixLists,
                   GetManagedPrefixListEntries, ModifyManagedPrefixList,
                   DeleteManagedPrefixList
  VPN Gateways:    CreateVpnGateway, DescribeVpnGateways, AttachVpnGateway,
                   DetachVpnGateway, DeleteVpnGateway,
                   EnableVgwRoutePropagation, DisableVgwRoutePropagation
  Customer GW:     CreateCustomerGateway, DescribeCustomerGateways,
                   DeleteCustomerGateway
  Launch Tmpl:     CreateLaunchTemplate, CreateLaunchTemplateVersion,
                   DescribeLaunchTemplates, DescribeLaunchTemplateVersions,
                   ModifyLaunchTemplate, DeleteLaunchTemplate
"""

import copy
import logging
import os
import random
import string
import time
from urllib.parse import parse_qs
from xml.sax.saxutils import escape as _esc

from ministack.core.persistence import load_state, PERSIST_STATE
from ministack.core.responses import AccountScopedDict, get_account_id, new_uuid

logger = logging.getLogger("ec2")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_instances = AccountScopedDict()
_security_groups = AccountScopedDict()
_key_pairs = AccountScopedDict()
_vpcs = AccountScopedDict()
_subnets = AccountScopedDict()
_internet_gateways = AccountScopedDict()
_addresses = AccountScopedDict()       # allocation_id -> address record
_tags = AccountScopedDict()            # resource_id -> [{"Key": ..., "Value": ...}]
_route_tables = AccountScopedDict()    # rtb_id -> route table record
_network_interfaces = AccountScopedDict()  # eni_id -> ENI record
_vpc_endpoints = AccountScopedDict()   # vpce_id -> endpoint record
_volumes = AccountScopedDict()         # vol_id -> volume record
_snapshots = AccountScopedDict()       # snap_id -> snapshot record
_nat_gateways = AccountScopedDict()    # nat_id -> NAT gateway record
_network_acls = AccountScopedDict()    # acl_id -> network ACL record
_flow_logs = AccountScopedDict()       # flow_log_id -> flow log record
_vpc_peering = AccountScopedDict()     # pcx_id -> peering connection record
_dhcp_options = AccountScopedDict()    # dopt_id -> DHCP options record
_egress_igws = AccountScopedDict()     # eigw_id -> egress-only internet gateway record
_prefix_lists = AccountScopedDict()    # pl_id -> managed prefix list record
_vpn_gateways = AccountScopedDict()    # vgw_id -> VPN gateway record
_customer_gateways = AccountScopedDict()  # cgw_id -> customer gateway record
_launch_templates = AccountScopedDict()   # lt_id -> launch template record (includes versions list)


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "instances": copy.deepcopy(_instances),
        "security_groups": copy.deepcopy(_security_groups),
        "key_pairs": copy.deepcopy(_key_pairs),
        "vpcs": copy.deepcopy(_vpcs),
        "subnets": copy.deepcopy(_subnets),
        "internet_gateways": copy.deepcopy(_internet_gateways),
        "addresses": copy.deepcopy(_addresses),
        "tags": copy.deepcopy(_tags),
        "route_tables": copy.deepcopy(_route_tables),
        "network_interfaces": copy.deepcopy(_network_interfaces),
        "vpc_endpoints": copy.deepcopy(_vpc_endpoints),
        "volumes": copy.deepcopy(_volumes),
        "snapshots": copy.deepcopy(_snapshots),
        "nat_gateways": copy.deepcopy(_nat_gateways),
        "network_acls": copy.deepcopy(_network_acls),
        "flow_logs": copy.deepcopy(_flow_logs),
        "vpc_peering": copy.deepcopy(_vpc_peering),
        "dhcp_options": copy.deepcopy(_dhcp_options),
        "egress_igws": copy.deepcopy(_egress_igws),
        "prefix_lists": copy.deepcopy(_prefix_lists),
        "vpn_gateways": copy.deepcopy(_vpn_gateways),
        "customer_gateways": copy.deepcopy(_customer_gateways),
        "launch_templates": copy.deepcopy(_launch_templates),
    }


def restore_state(data):
    if data:
        _instances.update(data.get("instances", {}))
        _security_groups.update(data.get("security_groups", {}))
        _key_pairs.update(data.get("key_pairs", {}))
        _vpcs.update(data.get("vpcs", {}))
        _subnets.update(data.get("subnets", {}))
        _internet_gateways.update(data.get("internet_gateways", {}))
        _addresses.update(data.get("addresses", {}))
        _tags.update(data.get("tags", {}))
        _route_tables.update(data.get("route_tables", {}))
        _network_interfaces.update(data.get("network_interfaces", {}))
        _vpc_endpoints.update(data.get("vpc_endpoints", {}))
        _volumes.update(data.get("volumes", {}))
        _snapshots.update(data.get("snapshots", {}))
        _nat_gateways.update(data.get("nat_gateways", {}))
        _network_acls.update(data.get("network_acls", {}))
        _flow_logs.update(data.get("flow_logs", {}))
        _vpc_peering.update(data.get("vpc_peering", {}))
        _dhcp_options.update(data.get("dhcp_options", {}))
        _egress_igws.update(data.get("egress_igws", {}))
        _prefix_lists.update(data.get("prefix_lists", {}))
        _vpn_gateways.update(data.get("vpn_gateways", {}))
        _customer_gateways.update(data.get("customer_gateways", {}))
        _launch_templates.update(data.get("launch_templates", {}))


_restored = load_state("ec2")
if _restored:
    restore_state(_restored)


# Default VPC / subnet created at import time so DescribeVpcs always returns something
_DEFAULT_VPC_ID = "vpc-00000001"
_DEFAULT_SUBNET_ID = "subnet-00000001"
_DEFAULT_SUBNET_ID_B = "subnet-00000002"
_DEFAULT_SUBNET_ID_C = "subnet-00000003"
_DEFAULT_SG_ID = "sg-00000001"
_DEFAULT_RTB_ID = "rtb-00000001"
_DEFAULT_ACL_ID = "acl-00000001"
_DEFAULT_IGW_ID = "igw-00000001"


def _init_defaults():
    if _DEFAULT_VPC_ID not in _vpcs:
        _vpcs[_DEFAULT_VPC_ID] = {
            "VpcId": _DEFAULT_VPC_ID,
            "CidrBlock": "172.31.0.0/16",
            "State": "available",
            "IsDefault": True,
            "DhcpOptionsId": "dopt-00000001",
            "InstanceTenancy": "default",
            "OwnerId": get_account_id(),
            "DefaultNetworkAclId": _DEFAULT_ACL_ID,
            "DefaultSecurityGroupId": _DEFAULT_SG_ID,
            "MainRouteTableId": _DEFAULT_RTB_ID,
        }
    _default_subnets = [
        (_DEFAULT_SUBNET_ID, "172.31.0.0/20", f"{REGION}a"),
        (_DEFAULT_SUBNET_ID_B, "172.31.16.0/20", f"{REGION}b"),
        (_DEFAULT_SUBNET_ID_C, "172.31.32.0/20", f"{REGION}c"),
    ]
    for subnet_id, cidr, az in _default_subnets:
        if subnet_id not in _subnets:
            _subnets[subnet_id] = {
                "SubnetId": subnet_id,
                "VpcId": _DEFAULT_VPC_ID,
                "CidrBlock": cidr,
                "AvailabilityZone": az,
                "AvailableIpAddressCount": 4091,
                "State": "available",
                "DefaultForAz": True,
                "MapPublicIpOnLaunch": True,
                "OwnerId": get_account_id(),
            }
    if _DEFAULT_SG_ID not in _security_groups:
        _security_groups[_DEFAULT_SG_ID] = {
            "GroupId": _DEFAULT_SG_ID,
            "GroupName": "default",
            "Description": "default VPC security group",
            "VpcId": _DEFAULT_VPC_ID,
            "OwnerId": get_account_id(),
            "IpPermissions": [],
            "IpPermissionsEgress": [
                {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                 "Ipv6Ranges": [], "PrefixListIds": [], "UserIdGroupPairs": []},
            ],
        }
    if _DEFAULT_ACL_ID not in _network_acls:
        _network_acls[_DEFAULT_ACL_ID] = {
            "NetworkAclId": _DEFAULT_ACL_ID, "VpcId": _DEFAULT_VPC_ID, "IsDefault": True,
            "Entries": [
                {"RuleNumber": 100, "Protocol": "-1", "RuleAction": "allow", "Egress": False, "CidrBlock": "0.0.0.0/0"},
                {"RuleNumber": 32767, "Protocol": "-1", "RuleAction": "deny", "Egress": False, "CidrBlock": "0.0.0.0/0"},
                {"RuleNumber": 100, "Protocol": "-1", "RuleAction": "allow", "Egress": True, "CidrBlock": "0.0.0.0/0"},
                {"RuleNumber": 32767, "Protocol": "-1", "RuleAction": "deny", "Egress": True, "CidrBlock": "0.0.0.0/0"},
            ],
            "Associations": [], "Tags": [], "OwnerId": get_account_id(),
        }
    if _DEFAULT_IGW_ID not in _internet_gateways:
        _internet_gateways[_DEFAULT_IGW_ID] = {
            "InternetGatewayId": _DEFAULT_IGW_ID,
            "OwnerId": get_account_id(),
            "Attachments": [{"VpcId": _DEFAULT_VPC_ID, "State": "available"}],
        }
    default_rtb = "rtb-00000001"
    if default_rtb not in _route_tables:
        _route_tables[default_rtb] = {
            "RouteTableId": default_rtb,
            "VpcId": _DEFAULT_VPC_ID,
            "OwnerId": get_account_id(),
            "Routes": [
                {"DestinationCidrBlock": "172.31.0.0/16", "GatewayId": "local",
                 "State": "active", "Origin": "CreateRouteTable"},
            ],
            "Associations": [
                {"RouteTableAssociationId": "rtbassoc-00000001",
                 "RouteTableId": default_rtb,
                 "Main": True, "AssociationState": {"State": "associated"}},
            ],
        }


_init_defaults()


# ---------------------------------------------------------------------------
# Request routing
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body, query_params):
    params = dict(query_params)
    if method in ("POST", "PUT") and body:
        raw = body if isinstance(body, str) else body.decode("utf-8", errors="replace")
        for k, v in parse_qs(raw).items():
            params[k] = v

    action = _p(params, "Action")
    handler = _ACTION_MAP.get(action)
    if not handler:
        return _error("InvalidAction", f"Unknown EC2 action: {action}", 400)
    return handler(params)


# ---------------------------------------------------------------------------
# Instances
# ---------------------------------------------------------------------------

def _run_instances(p):
    image_id = _p(p, "ImageId") or "ami-00000000"
    instance_type = _p(p, "InstanceType") or "t2.micro"
    min_count = int(_p(p, "MinCount") or "1")
    max_count = int(_p(p, "MaxCount") or "1")
    if min_count > max_count:
        return _error("InvalidParameterCombination",
                      f"Value ({min_count}) for parameter MinCount is not valid. "
                      f"MinCount must not exceed MaxCount.", 400)
    key_name = _p(p, "KeyName") or ""
    subnet_id = _p(p, "SubnetId") or _DEFAULT_SUBNET_ID
    user_data = _p(p, "UserData") or ""

    sg_ids = _parse_member_list(p, "SecurityGroupId")
    if not sg_ids:
        sg_ids = [_DEFAULT_SG_ID]

    now = _now_ts()
    created = []
    for _ in range(max(1, min(min_count, max_count))):
        instance_id = _new_instance_id()
        private_ip = _random_ip("10.0")
        _instances[instance_id] = {
            "InstanceId": instance_id,
            "ImageId": image_id,
            "InstanceType": instance_type,
            "KeyName": key_name,
            "State": {"Code": 16, "Name": "running"},
            "SubnetId": subnet_id,
            "VpcId": _vpcs.get(
                _subnets.get(subnet_id, {}).get("VpcId", _DEFAULT_VPC_ID),
                {},
            ).get("VpcId", _DEFAULT_VPC_ID),
            "PrivateIpAddress": private_ip,
            "PublicIpAddress": _random_ip("54."),
            "PrivateDnsName": f"ip-{private_ip.replace('.', '-')}.ec2.internal",
            "PublicDnsName": f"ec2-{private_ip.replace('.', '-')}.compute-1.amazonaws.com",
            "SecurityGroups": [
                {"GroupId": sg, "GroupName": _security_groups.get(sg, {}).get("GroupName", sg)}
                for sg in sg_ids
            ],
            "Architecture": "x86_64",
            "RootDeviceType": "ebs",
            "RootDeviceName": "/dev/xvda",
            "Hypervisor": "xen",
            "Virtualization": "hvm",
            "Placement": {"AvailabilityZone": f"{REGION}a", "Tenancy": "default"},
            "Monitoring": {"State": "disabled"},
            "AmiLaunchIndex": 0,
            "UserData": user_data,
            "LaunchTime": now,
        }
        created.append(_instances[instance_id])

    # Process TagSpecifications
    i = 1
    while _p(p, f"TagSpecification.{i}.ResourceType"):
        rtype = _p(p, f"TagSpecification.{i}.ResourceType")
        spec_tags = []
        j = 1
        while _p(p, f"TagSpecification.{i}.Tag.{j}.Key"):
            spec_tags.append({
                "Key": _p(p, f"TagSpecification.{i}.Tag.{j}.Key"),
                "Value": _p(p, f"TagSpecification.{i}.Tag.{j}.Value", ""),
            })
            j += 1
        if rtype == "instance" and spec_tags:
            for inst in created:
                _tags[inst["InstanceId"]] = spec_tags[:]
        i += 1

    items = "".join(_instance_xml(i) for i in created)
    inner = f"""<instancesSet>{items}</instancesSet>
    <reservationId>r-{new_uuid().replace('-','')[:17]}</reservationId>
    <ownerId>{get_account_id()}</ownerId>
    <groupSet/>"""
    return _xml(200, "RunInstancesResponse", inner)


def _describe_instances(p):
    filter_ids = _parse_member_list(p, "InstanceId")
    filters = _parse_filters(p)

    # Clean up instances terminated more than 60s ago (AWS removes after ~1 hour)
    stale = [k for k, v in _instances.items()
             if v["State"]["Name"] == "terminated"
             and time.time() - v.get("_terminated_at", 0) > 60]
    for k in stale:
        _instances.pop(k, None)

    if filter_ids:
        for iid in filter_ids:
            if iid not in _instances:
                return _error("InvalidInstanceID.NotFound", f"The instance ID '{iid}' does not exist", 400)

    results = []
    for inst in _instances.values():
        if filter_ids and inst["InstanceId"] not in filter_ids:
            continue
        if not _matches_filters(inst, filters):
            continue
        results.append(inst)

    items = "".join(
        f"""<item>
            <reservationId>r-{inst['InstanceId'][2:]}</reservationId>
            <ownerId>{get_account_id()}</ownerId>
            <groupSet/>
            <instancesSet>{_instance_xml(inst)}</instancesSet>
        </item>"""
        for inst in results
    )
    return _xml(200, "DescribeInstancesResponse", f"<reservationSet>{items}</reservationSet>")


def _describe_instance_status(p):
    filter_ids = _parse_member_list(p, "InstanceId")
    raw = p.get("IncludeAllInstances", "false")
    if isinstance(raw, list):
        raw = raw[0] if raw else "false"
    include_all = raw.lower() == "true"

    results = []
    for iid, inst in _instances.items():
        if filter_ids and iid not in filter_ids:
            continue
        state = inst["State"]["Name"]
        if not include_all and state != "running":
            continue
        az = inst.get("Placement", {}).get("AvailabilityZone", "us-east-1a")
        results.append(f"""<item>
            <instanceId>{iid}</instanceId>
            <availabilityZone>{az}</availabilityZone>
            <instanceState>
                <code>{inst['State']['Code']}</code>
                <name>{state}</name>
            </instanceState>
            <systemStatus>
                <status>ok</status>
                <details><item><name>reachability</name><status>passed</status></item></details>
            </systemStatus>
            <instanceStatus>
                <status>ok</status>
                <details><item><name>reachability</name><status>passed</status></item></details>
            </instanceStatus>
        </item>""")

    items = "".join(results)
    return _xml(200, "DescribeInstanceStatusResponse",
                f"<instanceStatusSet>{items}</instanceStatusSet>")


def _terminate_instances(p):
    ids = _parse_member_list(p, "InstanceId")
    items = ""
    for iid in ids:
        inst = _instances.get(iid)
        if inst:
            prev = inst["State"].copy()
            inst["State"] = {"Code": 48, "Name": "terminated"}
            inst["_terminated_at"] = time.time()
            items += f"""<item>
                <instanceId>{iid}</instanceId>
                <previousState><code>{prev['Code']}</code><name>{prev['Name']}</name></previousState>
                <currentState><code>48</code><name>terminated</name></currentState>
            </item>"""
    return _xml(200, "TerminateInstancesResponse", f"<instancesSet>{items}</instancesSet>")


def _stop_instances(p):
    ids = _parse_member_list(p, "InstanceId")
    items = ""
    for iid in ids:
        inst = _instances.get(iid)
        if inst:
            prev = inst["State"].copy()
            inst["State"] = {"Code": 80, "Name": "stopped"}
            items += f"""<item>
                <instanceId>{iid}</instanceId>
                <previousState><code>{prev['Code']}</code><name>{prev['Name']}</name></previousState>
                <currentState><code>80</code><name>stopped</name></currentState>
            </item>"""
    return _xml(200, "StopInstancesResponse", f"<instancesSet>{items}</instancesSet>")


def _start_instances(p):
    ids = _parse_member_list(p, "InstanceId")
    items = ""
    for iid in ids:
        inst = _instances.get(iid)
        if inst:
            prev = inst["State"].copy()
            inst["State"] = {"Code": 16, "Name": "running"}
            items += f"""<item>
                <instanceId>{iid}</instanceId>
                <previousState><code>{prev['Code']}</code><name>{prev['Name']}</name></previousState>
                <currentState><code>16</code><name>running</name></currentState>
            </item>"""
    return _xml(200, "StartInstancesResponse", f"<instancesSet>{items}</instancesSet>")


def _reboot_instances(p):
    return _xml(200, "RebootInstancesResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# Images (AMIs) — stub
# ---------------------------------------------------------------------------

_STUB_AMIS = [
    ("ami-0abcdef1234567890", "amzn2-ami-hvm-2.0.20231116.0-x86_64-gp2", "Amazon Linux 2"),
    ("ami-0123456789abcdef0", "ubuntu/images/hvm-ssd/ubuntu-22.04-amd64-server", "Ubuntu 22.04"),
    ("ami-0fedcba9876543210", "Windows_Server-2022-English-Full-Base", "Windows Server 2022"),
]


def _describe_images(p):
    filter_ids = _parse_member_list(p, "ImageId")
    items = ""
    for ami_id, name, desc in _STUB_AMIS:
        if filter_ids and ami_id not in filter_ids:
            continue
        items += f"""<item>
            <imageId>{ami_id}</imageId>
            <imageLocation>{name}</imageLocation>
            <imageState>available</imageState>
            <imageOwnerId>{get_account_id()}</imageOwnerId>
            <isPublic>true</isPublic>
            <architecture>x86_64</architecture>
            <imageType>machine</imageType>
            <name>{name}</name>
            <description>{desc}</description>
            <rootDeviceType>ebs</rootDeviceType>
            <virtualizationType>hvm</virtualizationType>
            <hypervisor>xen</hypervisor>
        </item>"""
    return _xml(200, "DescribeImagesResponse", f"<imagesSet>{items}</imagesSet>")


# ---------------------------------------------------------------------------
# Security Groups
# ---------------------------------------------------------------------------

def _create_security_group(p):
    name = _p(p, "GroupName")
    desc = _p(p, "Description") or name
    vpc_id = _p(p, "VpcId") or _DEFAULT_VPC_ID
    if not name:
        return _error("MissingParameter", "GroupName is required", 400)

    for sg in _security_groups.values():
        if sg["GroupName"] == name and sg["VpcId"] == vpc_id:
            return _error("InvalidGroup.Duplicate",
                          f"The security group '{name}' already exists", 400)

    sg_id = _new_sg_id()
    _security_groups[sg_id] = {
        "GroupId": sg_id,
        "GroupName": name,
        "Description": desc,
        "VpcId": vpc_id,
        "OwnerId": get_account_id(),
        "IpPermissions": [],
        "IpPermissionsEgress": [
            {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
             "Ipv6Ranges": [], "PrefixListIds": [], "UserIdGroupPairs": []},
        ],
    }
    _parse_tag_specs(p, "security-group", sg_id)
    return _xml(200, "CreateSecurityGroupResponse",
                f"<return>true</return><groupId>{sg_id}</groupId>")


def _delete_security_group(p):
    sg_id = _p(p, "GroupId")
    if sg_id and sg_id in _security_groups:
        # Block deletion of default security group
        if _security_groups[sg_id]["GroupName"] == "default":
            return _error("CannotDelete",
                          f"the specified group: \"{sg_id}\" name: \"default\" cannot be deleted by a user", 400)
        del _security_groups[sg_id]
    elif sg_id:
        return _error("InvalidGroup.NotFound",
                      f"The security group '{sg_id}' does not exist", 400)
    return _xml(200, "DeleteSecurityGroupResponse", "<return>true</return>")


def _describe_security_groups(p):
    filter_ids = _parse_member_list(p, "GroupId")
    filters = _parse_filters(p)
    if filter_ids:
        for gid in filter_ids:
            if gid not in _security_groups:
                return _error("InvalidGroup.NotFound", f"The security group '{gid}' does not exist", 400)
    items = ""
    for sg in _security_groups.values():
        if filter_ids and sg["GroupId"] not in filter_ids:
            continue
        vpc_filter = filters.get("vpc-id", [])
        if vpc_filter and sg.get("VpcId", "") not in vpc_filter:
            continue
        name_filter = filters.get("group-name", [])
        if name_filter and sg.get("GroupName", "") not in name_filter:
            continue
        items += _sg_xml(sg)
    return _xml(200, "DescribeSecurityGroupsResponse",
                f"<securityGroupInfo>{items}</securityGroupInfo>")


def _authorize_sg_ingress(p):
    sg_id = _p(p, "GroupId")
    sg = _security_groups.get(sg_id)
    if not sg:
        return _error("InvalidGroup.NotFound", f"Security group {sg_id} not found", 400)
    rules = _parse_ip_permissions(p, "IpPermissions")
    for r in rules:
        if r in sg["IpPermissions"]:
            return _error("InvalidPermission.Duplicate", "The specified rule already exists", 400)
        sg["IpPermissions"].append(r)
    return _xml(200, "AuthorizeSecurityGroupIngressResponse", "<return>true</return>")


def _revoke_sg_ingress(p):
    sg_id = _p(p, "GroupId")
    sg = _security_groups.get(sg_id)
    if not sg:
        return _error("InvalidGroup.NotFound", f"Security group {sg_id} not found", 400)
    rules = _parse_ip_permissions(p, "IpPermissions")
    for r in rules:
        try:
            sg["IpPermissions"].remove(r)
        except ValueError:
            pass
    return _xml(200, "RevokeSecurityGroupIngressResponse", "<return>true</return>")


def _authorize_sg_egress(p):
    sg_id = _p(p, "GroupId")
    sg = _security_groups.get(sg_id)
    if not sg:
        return _error("InvalidGroup.NotFound", f"Security group {sg_id} not found", 400)
    rules = _parse_ip_permissions(p, "IpPermissions")
    for r in rules:
        if r not in sg["IpPermissionsEgress"]:
            sg["IpPermissionsEgress"].append(r)
    return _xml(200, "AuthorizeSecurityGroupEgressResponse", "<return>true</return>")


def _revoke_sg_egress(p):
    sg_id = _p(p, "GroupId")
    sg = _security_groups.get(sg_id)
    if not sg:
        return _error("InvalidGroup.NotFound", f"Security group {sg_id} not found", 400)
    rules = _parse_ip_permissions(p, "IpPermissions")
    for r in rules:
        try:
            sg["IpPermissionsEgress"].remove(r)
        except ValueError:
            pass
    return _xml(200, "RevokeSecurityGroupEgressResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# Key Pairs
# ---------------------------------------------------------------------------

def _create_key_pair(p):
    name = _p(p, "KeyName")
    if not name:
        return _error("MissingParameter", "KeyName is required", 400)
    if name in _key_pairs:
        return _error("InvalidKeyPair.Duplicate",
                      f"The key pair '{name}' already exists", 400)
    fingerprint = ":".join(f"{random.randint(0,255):02x}" for _ in range(20))
    material = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA(stub)\n-----END RSA PRIVATE KEY-----"
    _key_pairs[name] = {
        "KeyName": name,
        "KeyFingerprint": fingerprint,
        "KeyPairId": f"key-{new_uuid().replace('-','')[:17]}",
    }
    _parse_tag_specs(p, "key-pair", _key_pairs[name]['KeyPairId'])
    return _xml(200, "CreateKeyPairResponse", f"""
        <keyName>{name}</keyName>
        <keyFingerprint>{fingerprint}</keyFingerprint>
        <keyMaterial>{material}</keyMaterial>
        <keyPairId>{_key_pairs[name]['KeyPairId']}</keyPairId>""")


def _delete_key_pair(p):
    name = _p(p, "KeyName")
    _key_pairs.pop(name, None)
    return _xml(200, "DeleteKeyPairResponse", "<return>true</return>")


def _describe_key_pairs(p):
    filter_names = _parse_member_list(p, "KeyName")
    if filter_names:
        for kn in filter_names:
            if kn not in _key_pairs:
                return _error("InvalidKeyPair.NotFound", f"The key pair '{kn}' does not exist", 400)
    items = ""
    for kp in _key_pairs.values():
        if filter_names and kp["KeyName"] not in filter_names:
            continue
        items += f"""<item>
            <keyName>{kp['KeyName']}</keyName>
            <keyFingerprint>{kp['KeyFingerprint']}</keyFingerprint>
            <keyPairId>{kp['KeyPairId']}</keyPairId>
        </item>"""
    return _xml(200, "DescribeKeyPairsResponse", f"<keySet>{items}</keySet>")


def _import_key_pair(p):
    name = _p(p, "KeyName")
    if not name:
        return _error("MissingParameter", "KeyName is required", 400)
    fingerprint = ":".join(f"{random.randint(0,255):02x}" for _ in range(20))
    _key_pairs[name] = {
        "KeyName": name,
        "KeyFingerprint": fingerprint,
        "KeyPairId": f"key-{new_uuid().replace('-','')[:17]}",
    }
    return _xml(200, "ImportKeyPairResponse", f"""
        <keyName>{name}</keyName>
        <keyFingerprint>{fingerprint}</keyFingerprint>
        <keyPairId>{_key_pairs[name]['KeyPairId']}</keyPairId>""")


# ---------------------------------------------------------------------------
# VPCs
# ---------------------------------------------------------------------------

def _describe_vpcs(p):
    filter_ids = _parse_member_list(p, "VpcId")
    if filter_ids:
        for vid in filter_ids:
            if vid not in _vpcs:
                return _error("InvalidVpcID.NotFound", f"The vpc ID '{vid}' does not exist", 400)
    filters = _parse_filters(p)
    items = ""
    for vpc in _vpcs.values():
        if filter_ids and vpc["VpcId"] not in filter_ids:
            continue
        if not _matches_vpc_filters(vpc, filters):
            continue
        items += _vpc_xml(vpc)
    return _xml(200, "DescribeVpcsResponse", f"<vpcSet>{items}</vpcSet>")


def _matches_vpc_filters(vpc, filters):
    for name, vals in filters.items():
        if name == "vpc-id":
            if vpc["VpcId"] not in vals:
                return False
        elif name == "cidr" or name == "cidr-block-association.cidr-block":
            if vpc["CidrBlock"] not in vals:
                return False
        elif name == "state":
            if vpc["State"] not in vals:
                return False
        elif name == "owner-id":
            if vpc["OwnerId"] not in vals:
                return False
        elif name == "is-default":
            is_def = "true" if vpc["IsDefault"] else "false"
            if is_def not in vals:
                return False
        elif name.startswith("tag:"):
            tag_key = name[4:]
            tag_list = _tags.get(vpc["VpcId"], [])
            tag_val = next((t["Value"] for t in tag_list if t["Key"] == tag_key), None)
            if tag_val not in vals:
                return False
        elif name == "tag-key":
            tag_list = _tags.get(vpc["VpcId"], [])
            if not any(t["Key"] in vals for t in tag_list):
                return False
    return True


def _create_vpc(p):
    cidr = _p(p, "CidrBlock") or "10.0.0.0/16"
    try:
        import ipaddress
        ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        return _error("InvalidParameterValue", f"Value ({cidr}) for parameter cidrBlock is invalid.", 400)
    vpc_id = _new_vpc_id()
    # Per-VPC default network ACL
    acl_id = "acl-" + "".join(random.choices(string.hexdigits[:16], k=17))
    _network_acls[acl_id] = {
        "NetworkAclId": acl_id, "VpcId": vpc_id, "IsDefault": True,
        "Entries": [
            {"RuleNumber": 100, "Protocol": "-1", "RuleAction": "allow", "Egress": False, "CidrBlock": "0.0.0.0/0"},
            {"RuleNumber": 32767, "Protocol": "-1", "RuleAction": "deny", "Egress": False, "CidrBlock": "0.0.0.0/0"},
            {"RuleNumber": 100, "Protocol": "-1", "RuleAction": "allow", "Egress": True, "CidrBlock": "0.0.0.0/0"},
            {"RuleNumber": 32767, "Protocol": "-1", "RuleAction": "deny", "Egress": True, "CidrBlock": "0.0.0.0/0"},
        ],
        "Associations": [], "Tags": [], "OwnerId": get_account_id(),
    }
    # Per-VPC main route table
    rtb_id = "rtb-" + "".join(random.choices(string.hexdigits[:16], k=17))
    rtb_assoc_id = "rtbassoc-" + "".join(random.choices(string.hexdigits[:16], k=17))
    _route_tables[rtb_id] = {
        "RouteTableId": rtb_id, "VpcId": vpc_id, "OwnerId": get_account_id(),
        "Routes": [{"DestinationCidrBlock": cidr, "GatewayId": "local", "State": "active", "Origin": "CreateRouteTable"}],
        "Associations": [{"RouteTableAssociationId": rtb_assoc_id, "RouteTableId": rtb_id, "Main": True,
                          "AssociationState": {"State": "associated"}}],
    }
    # Per-VPC default security group
    sg_id = _new_sg_id()
    _security_groups[sg_id] = {
        "GroupId": sg_id, "GroupName": "default", "Description": "default VPC security group",
        "VpcId": vpc_id, "OwnerId": get_account_id(), "IpPermissions": [],
        "IpPermissionsEgress": [
            {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
             "Ipv6Ranges": [], "PrefixListIds": [], "UserIdGroupPairs": []},
        ],
    }
    _vpcs[vpc_id] = {
        "VpcId": vpc_id, "CidrBlock": cidr, "State": "available", "IsDefault": False,
        "DhcpOptionsId": "dopt-00000001", "InstanceTenancy": _p(p, "InstanceTenancy") or "default",
        "OwnerId": get_account_id(), "DefaultNetworkAclId": acl_id,
        "DefaultSecurityGroupId": sg_id, "MainRouteTableId": rtb_id,
    }
    _parse_tag_specs(p, "vpc", vpc_id)
    return _xml(200, "CreateVpcResponse", _vpc_fields_xml(_vpcs[vpc_id], tag="vpc"))


def _delete_vpc(p):
    vpc_id = _p(p, "VpcId")
    if vpc_id not in _vpcs:
        return _error("InvalidVpcID.NotFound", f"The vpc ID '{vpc_id}' does not exist", 400)
    # Check for attached subnets
    for s in _subnets.values():
        if s["VpcId"] == vpc_id:
            return _error("DependencyViolation",
                          f"The vpc '{vpc_id}' has dependencies and cannot be deleted.", 400)
    # Check for non-default security groups
    for sg in _security_groups.values():
        if sg["VpcId"] == vpc_id and sg["GroupName"] != "default":
            return _error("DependencyViolation",
                          f"The vpc '{vpc_id}' has dependencies and cannot be deleted.", 400)
    # Check for attached internet gateways
    for igw in _internet_gateways.values():
        for att in igw.get("Attachments", []):
            if att.get("VpcId") == vpc_id:
                return _error("DependencyViolation",
                              f"The vpc '{vpc_id}' has dependencies and cannot be deleted.", 400)
    # Clean up VPC-associated default resources
    to_del_sgs = [sid for sid, sg in _security_groups.items() if sg["VpcId"] == vpc_id]
    for sid in to_del_sgs:
        del _security_groups[sid]
    to_del_rtb = [rid for rid, r in _route_tables.items() if r["VpcId"] == vpc_id]
    for rid in to_del_rtb:
        del _route_tables[rid]
    to_del_acl = [aid for aid, a in _network_acls.items() if a["VpcId"] == vpc_id]
    for aid in to_del_acl:
        del _network_acls[aid]
    del _vpcs[vpc_id]
    return _xml(200, "DeleteVpcResponse", "<return>true</return>")


def _create_default_vpc(p):
    # AWS returns DefaultVpcAlreadyExists if one already exists
    for vpc in _vpcs.values():
        if vpc.get("IsDefault"):
            return _error("DefaultVpcAlreadyExists",
                          "A Default VPC already exists for this account in this region.", 400)
    cidr = "172.31.0.0/16"
    vpc_id = _new_vpc_id()
    acl_id = "acl-" + "".join(random.choices(string.hexdigits[:16], k=17))
    _network_acls[acl_id] = {
        "NetworkAclId": acl_id, "VpcId": vpc_id, "IsDefault": True,
        "Entries": [
            {"RuleNumber": 100, "Protocol": "-1", "RuleAction": "allow", "Egress": False, "CidrBlock": "0.0.0.0/0"},
            {"RuleNumber": 32767, "Protocol": "-1", "RuleAction": "deny", "Egress": False, "CidrBlock": "0.0.0.0/0"},
            {"RuleNumber": 100, "Protocol": "-1", "RuleAction": "allow", "Egress": True, "CidrBlock": "0.0.0.0/0"},
            {"RuleNumber": 32767, "Protocol": "-1", "RuleAction": "deny", "Egress": True, "CidrBlock": "0.0.0.0/0"},
        ],
        "Associations": [], "Tags": [], "OwnerId": get_account_id(),
    }
    rtb_id = "rtb-" + "".join(random.choices(string.hexdigits[:16], k=17))
    rtb_assoc_id = "rtbassoc-" + "".join(random.choices(string.hexdigits[:16], k=17))
    _route_tables[rtb_id] = {
        "RouteTableId": rtb_id, "VpcId": vpc_id, "OwnerId": get_account_id(),
        "Routes": [
            {"DestinationCidrBlock": cidr, "GatewayId": "local", "State": "active", "Origin": "CreateRouteTable"},
        ],
        "Associations": [
            {"RouteTableAssociationId": rtb_assoc_id, "RouteTableId": rtb_id, "Main": True,
             "AssociationState": {"State": "associated"}},
        ],
    }
    sg_id = _new_sg_id()
    _security_groups[sg_id] = {
        "GroupId": sg_id, "GroupName": "default", "Description": "default VPC security group",
        "VpcId": vpc_id, "OwnerId": get_account_id(), "IpPermissions": [],
        "IpPermissionsEgress": [
            {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
             "Ipv6Ranges": [], "PrefixListIds": [], "UserIdGroupPairs": []},
        ],
    }
    igw_id = _new_igw_id()
    _internet_gateways[igw_id] = {
        "InternetGatewayId": igw_id, "OwnerId": get_account_id(),
        "Attachments": [{"VpcId": vpc_id, "State": "available"}],
    }
    _vpcs[vpc_id] = {
        "VpcId": vpc_id, "CidrBlock": cidr, "State": "available", "IsDefault": True,
        "DhcpOptionsId": "dopt-00000001", "InstanceTenancy": "default",
        "OwnerId": get_account_id(), "DefaultNetworkAclId": acl_id,
        "DefaultSecurityGroupId": sg_id, "MainRouteTableId": rtb_id,
    }
    # Create default subnets (one per AZ, matching AWS behavior)
    for i, (sub_cidr, az_suffix) in enumerate([
        ("172.31.0.0/20", "a"), ("172.31.16.0/20", "b"), ("172.31.32.0/20", "c"),
    ]):
        subnet_id = _new_subnet_id()
        _subnets[subnet_id] = {
            "SubnetId": subnet_id, "VpcId": vpc_id, "CidrBlock": sub_cidr,
            "AvailabilityZone": f"{REGION}{az_suffix}",
            "AvailableIpAddressCount": 4091, "State": "available",
            "DefaultForAz": True, "MapPublicIpOnLaunch": True,
            "OwnerId": get_account_id(),
        }
    return _xml(200, "CreateDefaultVpcResponse", _vpc_fields_xml(_vpcs[vpc_id], tag="vpc"))


# ---------------------------------------------------------------------------
# Subnets
# ---------------------------------------------------------------------------

def _describe_subnets(p):
    filter_ids = _parse_member_list(p, "SubnetId")
    filters = _parse_filters(p)
    if filter_ids:
        for sid in filter_ids:
            if sid not in _subnets:
                return _error("InvalidSubnetID.NotFound", f"The subnet ID '{sid}' does not exist", 400)
    items = ""
    for subnet in _subnets.values():
        if filter_ids and subnet["SubnetId"] not in filter_ids:
            continue
        if not _matches_subnet_filters(subnet, filters):
            continue
        items += _subnet_xml(subnet)
    return _xml(200, "DescribeSubnetsResponse", f"<subnetSet>{items}</subnetSet>")


def _matches_subnet_filters(subnet, filters):
    for name, vals in filters.items():
        if name == "vpc-id":
            if subnet["VpcId"] not in vals:
                return False
        elif name == "availability-zone":
            if subnet["AvailabilityZone"] not in vals:
                return False
        elif name == "subnet-id":
            if subnet["SubnetId"] not in vals:
                return False
        elif name == "default-for-az":
            val = "true" if subnet.get("DefaultForAz") else "false"
            if val not in vals:
                return False
        elif name.startswith("tag:"):
            tag_key = name[4:]
            tag_list = _tags.get(subnet["SubnetId"], [])
            tag_val = next((t["Value"] for t in tag_list if t["Key"] == tag_key), None)
            if tag_val not in vals:
                return False
        elif name == "tag-key":
            tag_list = _tags.get(subnet["SubnetId"], [])
            if not any(t["Key"] in vals for t in tag_list):
                return False
    return True


def _create_subnet(p):
    vpc_id = _p(p, "VpcId") or _DEFAULT_VPC_ID
    cidr = _p(p, "CidrBlock") or "10.0.1.0/24"
    az = _p(p, "AvailabilityZone") or f"{REGION}a"
    subnet_id = _new_subnet_id()
    _subnets[subnet_id] = {
        "SubnetId": subnet_id,
        "VpcId": vpc_id,
        "CidrBlock": cidr,
        "AvailabilityZone": az,
        "AvailableIpAddressCount": 251,
        "State": "available",
        "DefaultForAz": False,
        "MapPublicIpOnLaunch": False,
        "OwnerId": get_account_id(),
    }
    _parse_tag_specs(p, "subnet", subnet_id)
    return _xml(200, "CreateSubnetResponse", _subnet_fields_xml(_subnets[subnet_id], tag="subnet"))


def _delete_subnet(p):
    subnet_id = _p(p, "SubnetId")
    if subnet_id not in _subnets:
        return _error("InvalidSubnetID.NotFound",
                      f"The subnet ID '{subnet_id}' does not exist", 400)
    del _subnets[subnet_id]
    return _xml(200, "DeleteSubnetResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# Internet Gateways
# ---------------------------------------------------------------------------

def _create_internet_gateway(p):
    igw_id = _new_igw_id()
    _internet_gateways[igw_id] = {
        "InternetGatewayId": igw_id,
        "OwnerId": get_account_id(),
        "Attachments": [],
    }
    _parse_tag_specs(p, "internet-gateway", igw_id)
    return _xml(200, "CreateInternetGatewayResponse",
                _igw_fields_xml(_internet_gateways[igw_id], tag="internetGateway"))


def _delete_internet_gateway(p):
    igw_id = _p(p, "InternetGatewayId")
    if igw_id not in _internet_gateways:
        return _error("InvalidInternetGatewayID.NotFound",
                      f"The internet gateway ID '{igw_id}' does not exist", 400)
    del _internet_gateways[igw_id]
    return _xml(200, "DeleteInternetGatewayResponse", "<return>true</return>")


def _describe_internet_gateways(p):
    filter_ids = _parse_member_list(p, "InternetGatewayId")
    if filter_ids:
        for gid in filter_ids:
            if gid not in _internet_gateways:
                return _error("InvalidInternetGatewayID.NotFound", f"The internet gateway ID '{gid}' does not exist", 400)
    items = ""
    for igw in _internet_gateways.values():
        if filter_ids and igw["InternetGatewayId"] not in filter_ids:
            continue
        items += _igw_xml(igw)
    return _xml(200, "DescribeInternetGatewaysResponse",
                f"<internetGatewaySet>{items}</internetGatewaySet>")


def _attach_internet_gateway(p):
    igw_id = _p(p, "InternetGatewayId")
    vpc_id = _p(p, "VpcId")
    igw = _internet_gateways.get(igw_id)
    if not igw:
        return _error("InvalidInternetGatewayID.NotFound",
                      f"The internet gateway ID '{igw_id}' does not exist", 400)
    igw["Attachments"] = [{"VpcId": vpc_id, "State": "available"}]
    return _xml(200, "AttachInternetGatewayResponse", "<return>true</return>")


def _detach_internet_gateway(p):
    igw_id = _p(p, "InternetGatewayId")
    igw = _internet_gateways.get(igw_id)
    if igw:
        igw["Attachments"] = []
    return _xml(200, "DetachInternetGatewayResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# VPC / Subnet attribute modifications
# ---------------------------------------------------------------------------

def _modify_vpc_attribute(p):
    vpc_id = _p(p, "VpcId")
    if vpc_id not in _vpcs:
        return _error("InvalidVpcID.NotFound", f"The vpc ID '{vpc_id}' does not exist", 400)
    # EnableDnsSupport / EnableDnsHostnames — store but don't enforce
    for attr in ("EnableDnsSupport.Value", "EnableDnsHostnames.Value"):
        val = _p(p, attr)
        if val:
            _vpcs[vpc_id][attr.split(".")[0]] = val.lower() == "true"
    return _xml(200, "ModifyVpcAttributeResponse", "<return>true</return>")


def _describe_vpc_attribute(p):
    vpc_id = _p(p, "VpcId")
    attribute = _p(p, "Attribute")
    if vpc_id not in _vpcs:
        return _error("InvalidVpcID.NotFound", f"The vpc ID '{vpc_id}' does not exist", 400)
    vpc = _vpcs[vpc_id]
    if attribute == "enableDnsSupport":
        val = vpc.get("EnableDnsSupport", True)
        return _xml(200, "DescribeVpcAttributeResponse",
                    f"<vpcId>{vpc_id}</vpcId><enableDnsSupport><value>{'true' if val else 'false'}</value></enableDnsSupport>")
    elif attribute == "enableDnsHostnames":
        val = vpc.get("EnableDnsHostnames", False)
        return _xml(200, "DescribeVpcAttributeResponse",
                    f"<vpcId>{vpc_id}</vpcId><enableDnsHostnames><value>{'true' if val else 'false'}</value></enableDnsHostnames>")
    elif attribute == "enableNetworkAddressUsageMetrics":
        return _xml(200, "DescribeVpcAttributeResponse",
                    f"<vpcId>{vpc_id}</vpcId><enableNetworkAddressUsageMetrics><value>false</value></enableNetworkAddressUsageMetrics>")
    return _xml(200, "DescribeVpcAttributeResponse", f"<vpcId>{vpc_id}</vpcId>")


def _describe_vpc_classic_link(p):
    """Stub — ClassicLink is deprecated, return empty set."""
    return _xml(200, "DescribeVpcClassicLinkResponse", "<vpcSet/>")


def _describe_vpc_classic_link_dns_support(p):
    """Stub — ClassicLink DNS support, return empty set."""
    return _xml(200, "DescribeVpcClassicLinkDnsSupportResponse", "<vpcs/>")


def _modify_subnet_attribute(p):
    subnet_id = _p(p, "SubnetId")
    if subnet_id not in _subnets:
        return _error("InvalidSubnetID.NotFound",
                      f"The subnet ID '{subnet_id}' does not exist", 400)
    val = _p(p, "MapPublicIpOnLaunch.Value")
    if val:
        _subnets[subnet_id]["MapPublicIpOnLaunch"] = val.lower() == "true"
    return _xml(200, "ModifySubnetAttributeResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# Route Tables
# ---------------------------------------------------------------------------

def _create_route_table(p):
    vpc_id = _p(p, "VpcId") or _DEFAULT_VPC_ID
    rtb_id = "rtb-" + "".join(random.choices(string.hexdigits[:16], k=17))
    _route_tables[rtb_id] = {
        "RouteTableId": rtb_id,
        "VpcId": vpc_id,
        "OwnerId": get_account_id(),
        "Routes": [
            {"DestinationCidrBlock": _vpcs.get(vpc_id, {}).get("CidrBlock", "10.0.0.0/16"),
             "GatewayId": "local", "State": "active", "Origin": "CreateRouteTable"},
        ],
        "Associations": [],
    }
    _parse_tag_specs(p, "route-table", rtb_id)
    return _xml(200, "CreateRouteTableResponse",
                _rtb_fields_xml(_route_tables[rtb_id], tag="routeTable"))


def _delete_route_table(p):
    rtb_id = _p(p, "RouteTableId")
    if rtb_id not in _route_tables:
        return _error("InvalidRouteTableID.NotFound",
                      f"The route table '{rtb_id}' does not exist", 400)
    del _route_tables[rtb_id]
    return _xml(200, "DeleteRouteTableResponse", "<return>true</return>")


def _describe_route_tables(p):
    filter_ids = _parse_member_list(p, "RouteTableId")
    filters = _parse_filters(p)
    results = []
    for rtb in _route_tables.values():
        if filter_ids and rtb["RouteTableId"] not in filter_ids:
            continue
        # Filter by association.route-table-association-id
        assoc_filter = filters.get("association.route-table-association-id", [])
        if assoc_filter:
            assoc_ids = [a["RouteTableAssociationId"] for a in rtb.get("Associations", [])]
            if not any(af in assoc_ids for af in assoc_filter):
                continue
        # Filter by association.subnet-id
        subnet_filter = filters.get("association.subnet-id", [])
        if subnet_filter:
            subnet_ids = [a.get("SubnetId", "") for a in rtb.get("Associations", [])]
            if not any(sf in subnet_ids for sf in subnet_filter):
                continue
        # Filter by association.main
        main_filter = filters.get("association.main", [])
        if main_filter:
            want_main = main_filter[0].lower() == "true"
            has_main = any(a.get("Main") for a in rtb.get("Associations", []))
            if has_main != want_main:
                continue
        # Filter by vpc-id
        vpc_filter = filters.get("vpc-id", [])
        if vpc_filter and rtb.get("VpcId", "") not in vpc_filter:
            continue
        results.append(rtb)
    items = "".join(_rtb_fields_xml(rtb) for rtb in results)
    return _xml(200, "DescribeRouteTablesResponse",
                f"<routeTableSet>{items}</routeTableSet>")


def _associate_route_table(p):
    rtb_id = _p(p, "RouteTableId")
    subnet_id = _p(p, "SubnetId")
    rtb = _route_tables.get(rtb_id)
    if not rtb:
        return _error("InvalidRouteTableID.NotFound",
                      f"The route table '{rtb_id}' does not exist", 400)
    assoc_id = "rtbassoc-" + "".join(random.choices(string.hexdigits[:16], k=17))
    rtb["Associations"].append({
        "RouteTableAssociationId": assoc_id,
        "RouteTableId": rtb_id,
        "SubnetId": subnet_id,
        "Main": False,
        "AssociationState": {"State": "associated"},
    })
    return _xml(200, "AssociateRouteTableResponse",
                f"<associationId>{assoc_id}</associationId>")


def _disassociate_route_table(p):
    assoc_id = _p(p, "AssociationId")
    for rtb in _route_tables.values():
        rtb["Associations"] = [
            a for a in rtb["Associations"]
            if a["RouteTableAssociationId"] != assoc_id
        ]
    return _xml(200, "DisassociateRouteTableResponse", "<return>true</return>")


def _create_route(p):
    rtb_id = _p(p, "RouteTableId")
    rtb = _route_tables.get(rtb_id)
    if not rtb:
        return _error("InvalidRouteTableID.NotFound",
                      f"The route table '{rtb_id}' does not exist", 400)
    dest = _p(p, "DestinationCidrBlock")
    route = {"DestinationCidrBlock": dest, "State": "active", "Origin": "CreateRoute"}
    if _p(p, "GatewayId"):
        route["GatewayId"] = _p(p, "GatewayId")
    elif _p(p, "NatGatewayId"):
        route["NatGatewayId"] = _p(p, "NatGatewayId")
    elif _p(p, "InstanceId"):
        route["InstanceId"] = _p(p, "InstanceId")
    elif _p(p, "VpcPeeringConnectionId"):
        route["VpcPeeringConnectionId"] = _p(p, "VpcPeeringConnectionId")
    elif _p(p, "TransitGatewayId"):
        route["TransitGatewayId"] = _p(p, "TransitGatewayId")
    else:
        route["GatewayId"] = "local"
    rtb["Routes"].append(route)
    return _xml(200, "CreateRouteResponse", "<return>true</return>")


def _replace_route(p):
    rtb_id = _p(p, "RouteTableId")
    rtb = _route_tables.get(rtb_id)
    if not rtb:
        return _error("InvalidRouteTableID.NotFound",
                      f"The route table '{rtb_id}' does not exist", 400)
    dest = _p(p, "DestinationCidrBlock")
    for route in rtb["Routes"]:
        if route.get("DestinationCidrBlock") == dest:
            route.pop("GatewayId", None)
            route.pop("NatGatewayId", None)
            route.pop("InstanceId", None)
            if _p(p, "GatewayId"):
                route["GatewayId"] = _p(p, "GatewayId")
            elif _p(p, "NatGatewayId"):
                route["NatGatewayId"] = _p(p, "NatGatewayId")
            elif _p(p, "InstanceId"):
                route["InstanceId"] = _p(p, "InstanceId")
            else:
                route["GatewayId"] = "local"
            break
    return _xml(200, "ReplaceRouteResponse", "<return>true</return>")


def _delete_route(p):
    rtb_id = _p(p, "RouteTableId")
    rtb = _route_tables.get(rtb_id)
    if not rtb:
        return _error("InvalidRouteTableID.NotFound",
                      f"The route table '{rtb_id}' does not exist", 400)
    dest = _p(p, "DestinationCidrBlock")
    rtb["Routes"] = [r for r in rtb["Routes"] if r.get("DestinationCidrBlock") != dest]
    return _xml(200, "DeleteRouteResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# Network Interfaces (ENI)
# ---------------------------------------------------------------------------

def _create_network_interface(p):
    subnet_id = _p(p, "SubnetId") or _DEFAULT_SUBNET_ID
    description = _p(p, "Description") or ""
    sg_ids = _parse_member_list(p, "SecurityGroupId")
    if not sg_ids:
        sg_ids = [_DEFAULT_SG_ID]
    eni_id = "eni-" + "".join(random.choices(string.hexdigits[:16], k=17))
    private_ip = _random_ip("10.0")
    az = _subnets.get(subnet_id, {}).get("AvailabilityZone", f"{REGION}a")
    _network_interfaces[eni_id] = {
        "NetworkInterfaceId": eni_id,
        "SubnetId": subnet_id,
        "VpcId": _subnets.get(subnet_id, {}).get("VpcId", _DEFAULT_VPC_ID),
        "AvailabilityZone": az,
        "Description": description,
        "OwnerId": get_account_id(),
        "Status": "available",
        "PrivateIpAddress": private_ip,
        "InterfaceType": "interface",
        "SourceDestCheck": True,
        "MacAddress": ":".join(f"{random.randint(0,255):02x}" for _ in range(6)),
        "Groups": [
            {"GroupId": sg, "GroupName": _security_groups.get(sg, {}).get("GroupName", sg)}
            for sg in sg_ids
        ],
        "Attachment": None,
    }
    return _xml(200, "CreateNetworkInterfaceResponse",
                _eni_fields_xml(_network_interfaces[eni_id], tag="networkInterface"))


def _delete_network_interface(p):
    eni_id = _p(p, "NetworkInterfaceId")
    if eni_id not in _network_interfaces:
        return _error("InvalidNetworkInterfaceID.NotFound",
                      f"The network interface '{eni_id}' does not exist", 400)
    del _network_interfaces[eni_id]
    return _xml(200, "DeleteNetworkInterfaceResponse", "<return>true</return>")


def _describe_network_interfaces(p):
    filter_ids = _parse_member_list(p, "NetworkInterfaceId")
    items = "".join(
        _eni_fields_xml(eni)
        for eni in _network_interfaces.values()
        if not filter_ids or eni["NetworkInterfaceId"] in filter_ids
    )
    return _xml(200, "DescribeNetworkInterfacesResponse",
                f"<networkInterfaceSet>{items}</networkInterfaceSet>")


def _attach_network_interface(p):
    eni_id = _p(p, "NetworkInterfaceId")
    instance_id = _p(p, "InstanceId")
    device_index = _p(p, "DeviceIndex") or "1"
    eni = _network_interfaces.get(eni_id)
    if not eni:
        return _error("InvalidNetworkInterfaceID.NotFound",
                      f"The network interface '{eni_id}' does not exist", 400)
    attachment_id = "eni-attach-" + "".join(random.choices(string.hexdigits[:16], k=17))
    eni["Status"] = "in-use"
    eni["Attachment"] = {
        "AttachmentId": attachment_id,
        "InstanceId": instance_id,
        "DeviceIndex": int(device_index),
        "Status": "attached",
    }
    return _xml(200, "AttachNetworkInterfaceResponse",
                f"<attachmentId>{attachment_id}</attachmentId>")


def _detach_network_interface(p):
    attachment_id = _p(p, "AttachmentId")
    for eni in _network_interfaces.values():
        if eni.get("Attachment", {}) and eni["Attachment"].get("AttachmentId") == attachment_id:
            eni["Status"] = "available"
            eni["Attachment"] = None
            break
    return _xml(200, "DetachNetworkInterfaceResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# VPC Endpoints
# ---------------------------------------------------------------------------

def _create_vpc_endpoint(p):
    vpc_id = _p(p, "VpcId") or _DEFAULT_VPC_ID
    service_name = _p(p, "ServiceName") or ""
    endpoint_type = _p(p, "VpcEndpointType") or "Gateway"
    vpce_id = "vpce-" + "".join(random.choices(string.hexdigits[:16], k=17))
    _vpc_endpoints[vpce_id] = {
        "VpcEndpointId": vpce_id,
        "VpcEndpointType": endpoint_type,
        "VpcId": vpc_id,
        "ServiceName": service_name,
        "State": "available",
        "RouteTableIds": _parse_member_list(p, "RouteTableId"),
        "SubnetIds": _parse_member_list(p, "SubnetId"),
        "OwnerId": get_account_id(),
    }
    return _xml(200, "CreateVpcEndpointResponse",
                _vpce_fields_xml(_vpc_endpoints[vpce_id], tag="vpcEndpoint"))


def _delete_vpc_endpoints(p):
    ids = _parse_member_list(p, "VpcEndpointId")
    for vpce_id in ids:
        _vpc_endpoints.pop(vpce_id, None)
    return _xml(200, "DeleteVpcEndpointsResponse", "<unsuccessful/>")


def _describe_vpc_endpoints(p):
    filter_ids = _parse_member_list(p, "VpcEndpointId")
    items = "".join(
        _vpce_fields_xml(ep)
        for ep in _vpc_endpoints.values()
        if not filter_ids or ep["VpcEndpointId"] in filter_ids
    )
    return _xml(200, "DescribeVpcEndpointsResponse",
                f"<vpcEndpointSet>{items}</vpcEndpointSet>")


# ---------------------------------------------------------------------------
# Availability Zones
# ---------------------------------------------------------------------------

def _describe_availability_zones(p):
    azs = [f"{REGION}a", f"{REGION}b", f"{REGION}c"]
    items = "".join(f"""<item>
        <zoneName>{az}</zoneName>
        <zoneState>available</zoneState>
        <regionName>{REGION}</regionName>
        <zoneId>{az}</zoneId>
    </item>""" for az in azs)
    return _xml(200, "DescribeAvailabilityZonesResponse",
                f"<availabilityZoneInfo>{items}</availabilityZoneInfo>")


# ---------------------------------------------------------------------------
# Elastic IPs
# ---------------------------------------------------------------------------

def _allocate_address(p):
    domain = _p(p, "Domain") or "vpc"
    allocation_id = f"eipalloc-{new_uuid().replace('-','')[:17]}"
    public_ip = _random_ip("52.")
    _addresses[allocation_id] = {
        "AllocationId": allocation_id,
        "PublicIp": public_ip,
        "Domain": domain,
        "AssociationId": None,
        "InstanceId": None,
        "NetworkInterfaceId": None,
        "PrivateIpAddress": None,
    }
    return _xml(200, "AllocateAddressResponse", f"""
        <publicIp>{public_ip}</publicIp>
        <domain>{domain}</domain>
        <allocationId>{allocation_id}</allocationId>""")


def _release_address(p):
    allocation_id = _p(p, "AllocationId")
    if allocation_id and allocation_id in _addresses:
        del _addresses[allocation_id]
    elif allocation_id:
        return _error("InvalidAllocationID.NotFound",
                      f"The allocation ID '{allocation_id}' does not exist", 400)
    return _xml(200, "ReleaseAddressResponse", "<return>true</return>")


def _associate_address(p):
    allocation_id = _p(p, "AllocationId")
    instance_id = _p(p, "InstanceId")
    addr = _addresses.get(allocation_id)
    if not addr:
        return _error("InvalidAllocationID.NotFound",
                      f"The allocation ID '{allocation_id}' does not exist", 400)
    association_id = f"eipassoc-{new_uuid().replace('-','')[:17]}"
    addr["AssociationId"] = association_id
    addr["InstanceId"] = instance_id
    return _xml(200, "AssociateAddressResponse",
                f"<return>true</return><associationId>{association_id}</associationId>")


def _disassociate_address(p):
    association_id = _p(p, "AssociationId")
    for addr in _addresses.values():
        if addr.get("AssociationId") == association_id:
            addr["AssociationId"] = None
            addr["InstanceId"] = None
            break
    return _xml(200, "DisassociateAddressResponse", "<return>true</return>")


def _describe_addresses(p):
    filter_ids = _parse_member_list(p, "AllocationId")
    items = ""
    for addr in _addresses.values():
        if filter_ids and addr["AllocationId"] not in filter_ids:
            continue
        assoc = f"<associationId>{addr['AssociationId']}</associationId>" if addr["AssociationId"] else ""
        inst = f"<instanceId>{addr['InstanceId']}</instanceId>" if addr["InstanceId"] else ""
        items += f"""<item>
            <allocationId>{addr['AllocationId']}</allocationId>
            <publicIp>{addr['PublicIp']}</publicIp>
            <domain>{addr['Domain']}</domain>
            {assoc}{inst}
        </item>"""
    return _xml(200, "DescribeAddressesResponse", f"<addressesSet>{items}</addressesSet>")


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def _tag_set_xml(resource_id):
    """Build <tagSet> XML from _tags for a resource. Returns <tagSet/> if no tags."""
    tag_list = _tags.get(resource_id, [])
    if not tag_list:
        return "<tagSet/>"
    items = "".join(
        f"<item><key>{_esc(t['Key'])}</key><value>{_esc(t.get('Value', ''))}</value></item>"
        for t in tag_list
    )
    return f"<tagSet>{items}</tagSet>"


def _create_tags(p):
    resource_ids = _parse_member_list(p, "ResourceId")
    tags = _parse_tags(p)
    for rid in resource_ids:
        existing = _tags.setdefault(rid, [])
        existing_map = {t["Key"]: i for i, t in enumerate(existing)}
        for tag in tags:
            idx = existing_map.get(tag["Key"])
            if idx is not None:
                existing[idx] = tag
            else:
                existing.append(tag)
                existing_map[tag["Key"]] = len(existing) - 1
    return _xml(200, "CreateTagsResponse", "<return>true</return>")


def _delete_tags(p):
    resource_ids = _parse_member_list(p, "ResourceId")
    tags_to_remove = _parse_tags(p)
    keys_to_remove = {t["Key"] for t in tags_to_remove}
    for rid in resource_ids:
        if rid in _tags:
            _tags[rid] = [t for t in _tags[rid] if t["Key"] not in keys_to_remove]
    return _xml(200, "DeleteTagsResponse", "<return>true</return>")


def _describe_tags(p):
    filters = _parse_filters(p)
    filter_resource_ids = set(filters.get("resource-id", []))
    filter_resource_types = set(filters.get("resource-type", []))
    filter_keys = set(filters.get("key", []))
    filter_values = set(filters.get("value", []))

    items = ""
    for rid, tag_list in _tags.items():
        if filter_resource_ids and rid not in filter_resource_ids:
            continue
        resource_type = _guess_resource_type(rid)
        if filter_resource_types and resource_type not in filter_resource_types:
            continue
        for tag in tag_list:
            if filter_keys and tag["Key"] not in filter_keys:
                continue
            if filter_values and tag.get("Value", "") not in filter_values:
                continue
            items += f"""<item>
                <resourceId>{rid}</resourceId>
                <resourceType>{resource_type}</resourceType>
                <key>{_esc(tag['Key'])}</key>
                <value>{_esc(tag['Value'])}</value>
            </item>"""
    return _xml(200, "DescribeTagsResponse", f"<tagSet>{items}</tagSet>")


# ---------------------------------------------------------------------------
# EBS Volumes
# ---------------------------------------------------------------------------

def _new_volume_id():
    return "vol-" + "".join(random.choices(string.hexdigits[:16], k=17))

def _new_snapshot_id():
    return "snap-" + "".join(random.choices(string.hexdigits[:16], k=17))


def _create_volume(p):
    vol_id = _new_volume_id()
    az = _p(p, "AvailabilityZone") or f"{REGION}a"
    size = int(_p(p, "Size") or "8")
    vol_type = _p(p, "VolumeType") or "gp2"
    snapshot_id = _p(p, "SnapshotId") or ""
    iops = _p(p, "Iops") or ""
    encrypted = _p(p, "Encrypted") or "false"
    now = _now_ts()
    _volumes[vol_id] = {
        "VolumeId": vol_id,
        "Size": size,
        "AvailabilityZone": az,
        "State": "available",
        "VolumeType": vol_type,
        "SnapshotId": snapshot_id,
        "Iops": int(iops) if iops else (3000 if vol_type in ("gp3", "io1", "io2") else 0),
        "Encrypted": encrypted.lower() == "true",
        "CreateTime": now,
        "Attachments": [],
        "MultiAttachEnabled": False,
        "Throughput": 125 if vol_type == "gp3" else 0,
    }
    # Process TagSpecifications
    i = 1
    while _p(p, f"TagSpecification.{i}.ResourceType"):
        if _p(p, f"TagSpecification.{i}.ResourceType") == "volume":
            vol_tags = []
            j = 1
            while _p(p, f"TagSpecification.{i}.Tag.{j}.Key"):
                vol_tags.append({"Key": _p(p, f"TagSpecification.{i}.Tag.{j}.Key"),
                                 "Value": _p(p, f"TagSpecification.{i}.Tag.{j}.Value", "")})
                j += 1
            if vol_tags:
                _tags[vol_id] = vol_tags
        i += 1
    return _xml(200, "CreateVolumeResponse", _volume_inner_xml(_volumes[vol_id]))


def _delete_volume(p):
    vol_id = _p(p, "VolumeId")
    if vol_id not in _volumes:
        return _error("InvalidVolume.NotFound", f"The volume '{vol_id}' does not exist.", 400)
    vol = _volumes[vol_id]
    if vol["Attachments"]:
        return _error("VolumeInUse", f"Volume {vol_id} is currently attached.", 400)
    del _volumes[vol_id]
    return _xml(200, "DeleteVolumeResponse", "<return>true</return>")


def _describe_volumes(p):
    filter_ids = _parse_member_list(p, "VolumeId")
    if filter_ids:
        for vid in filter_ids:
            if vid not in _volumes:
                return _error("InvalidVolume.NotFound", f"The volume '{vid}' does not exist", 400)
    items = ""
    for vol in _volumes.values():
        if filter_ids and vol["VolumeId"] not in filter_ids:
            continue
        items += f"<item>{_volume_inner_xml(vol)}</item>"
    return _xml(200, "DescribeVolumesResponse", f"<volumeSet>{items}</volumeSet>")


def _describe_volume_status(p):
    filter_ids = _parse_member_list(p, "VolumeId")
    items = ""
    for vol in _volumes.values():
        if filter_ids and vol["VolumeId"] not in filter_ids:
            continue
        items += f"""<item>
            <volumeId>{vol['VolumeId']}</volumeId>
            <availabilityZone>{vol['AvailabilityZone']}</availabilityZone>
            <volumeStatus>
                <status>ok</status>
                <details><item><name>io-enabled</name><status>passed</status></item></details>
            </volumeStatus>
            <actionsSet/>
            <eventsSet/>
        </item>"""
    return _xml(200, "DescribeVolumeStatusResponse", f"<volumeStatusSet>{items}</volumeStatusSet>")


def _attach_volume(p):
    vol_id = _p(p, "VolumeId")
    instance_id = _p(p, "InstanceId")
    device = _p(p, "Device") or "/dev/xvdf"
    vol = _volumes.get(vol_id)
    if not vol:
        return _error("InvalidVolume.NotFound", f"The volume '{vol_id}' does not exist.", 400)
    if not _instances.get(instance_id):
        return _error("InvalidInstanceID.NotFound", f"The instance ID '{instance_id}' does not exist.", 400)
    now = _now_ts()
    attachment = {
        "VolumeId": vol_id,
        "InstanceId": instance_id,
        "Device": device,
        "State": "attached",
        "AttachTime": now,
        "DeleteOnTermination": False,
    }
    vol["Attachments"] = [attachment]
    vol["State"] = "in-use"
    return _xml(200, "AttachVolumeResponse", f"""
        <volumeId>{vol_id}</volumeId>
        <instanceId>{instance_id}</instanceId>
        <device>{device}</device>
        <status>attached</status>
        <attachTime>{now}</attachTime>
        <deleteOnTermination>false</deleteOnTermination>""")


def _detach_volume(p):
    vol_id = _p(p, "VolumeId")
    vol = _volumes.get(vol_id)
    if not vol:
        return _error("InvalidVolume.NotFound", f"The volume '{vol_id}' does not exist.", 400)
    vol["Attachments"] = []
    vol["State"] = "available"
    return _xml(200, "DetachVolumeResponse", f"""
        <volumeId>{vol_id}</volumeId>
        <status>detached</status>""")


def _modify_volume(p):
    vol_id = _p(p, "VolumeId")
    vol = _volumes.get(vol_id)
    if not vol:
        return _error("InvalidVolume.NotFound", f"The volume '{vol_id}' does not exist.", 400)
    if _p(p, "Size"):
        vol["Size"] = int(_p(p, "Size"))
    if _p(p, "VolumeType"):
        vol["VolumeType"] = _p(p, "VolumeType")
    if _p(p, "Iops"):
        vol["Iops"] = int(_p(p, "Iops"))
    now = _now_ts()
    return _xml(200, "ModifyVolumeResponse", f"""
        <volumeModification>
            <volumeId>{vol_id}</volumeId>
            <modificationState>completed</modificationState>
            <targetSize>{vol['Size']}</targetSize>
            <targetVolumeType>{vol['VolumeType']}</targetVolumeType>
            <targetIops>{vol['Iops']}</targetIops>
            <startTime>{now}</startTime>
            <endTime>{now}</endTime>
            <progress>100</progress>
        </volumeModification>""")


def _describe_volumes_modifications(p):
    filter_ids = _parse_member_list(p, "VolumeId")
    items = ""
    for vol in _volumes.values():
        if filter_ids and vol["VolumeId"] not in filter_ids:
            continue
        now = _now_ts()
        items += f"""<item>
            <volumeId>{vol['VolumeId']}</volumeId>
            <modificationState>completed</modificationState>
            <targetSize>{vol['Size']}</targetSize>
            <targetVolumeType>{vol['VolumeType']}</targetVolumeType>
            <targetIops>{vol['Iops']}</targetIops>
            <startTime>{now}</startTime>
            <endTime>{now}</endTime>
            <progress>100</progress>
        </item>"""
    return _xml(200, "DescribeVolumesModificationsResponse", f"<volumeModificationSet>{items}</volumeModificationSet>")


def _enable_volume_io(p):
    return _xml(200, "EnableVolumeIOResponse", "<return>true</return>")


def _modify_volume_attribute(p):
    return _xml(200, "ModifyVolumeAttributeResponse", "<return>true</return>")


def _describe_volume_attribute(p):
    vol_id = _p(p, "VolumeId")
    attribute = _p(p, "Attribute") or "autoEnableIO"
    return _xml(200, "DescribeVolumeAttributeResponse", f"""
        <volumeId>{vol_id}</volumeId>
        <autoEnableIO><value>false</value></autoEnableIO>""")


def _volume_inner_xml(vol):
    attachments = "".join(f"""<item>
        <volumeId>{a['VolumeId']}</volumeId>
        <instanceId>{a['InstanceId']}</instanceId>
        <device>{a['Device']}</device>
        <status>{a['State']}</status>
        <attachTime>{a['AttachTime']}</attachTime>
        <deleteOnTermination>{'true' if a['DeleteOnTermination'] else 'false'}</deleteOnTermination>
    </item>""" for a in vol.get("Attachments", []))
    snap = f"<snapshotId>{vol['SnapshotId']}</snapshotId>" if vol.get("SnapshotId") else "<snapshotId/>"
    iops = f"<iops>{vol['Iops']}</iops>" if vol.get("Iops") else ""
    return f"""
        <volumeId>{vol['VolumeId']}</volumeId>
        <size>{vol['Size']}</size>
        <availabilityZone>{vol['AvailabilityZone']}</availabilityZone>
        <status>{vol['State']}</status>
        <createTime>{vol['CreateTime']}</createTime>
        <volumeType>{vol['VolumeType']}</volumeType>
        {snap}
        {iops}
        <encrypted>{'true' if vol['Encrypted'] else 'false'}</encrypted>
        <multiAttachEnabled>{'true' if vol['MultiAttachEnabled'] else 'false'}</multiAttachEnabled>
        <attachmentSet>{attachments}</attachmentSet>
        {_tag_set_xml(vol['VolumeId'])}"""


# ---------------------------------------------------------------------------
# EBS Snapshots
# ---------------------------------------------------------------------------

def _create_snapshot(p):
    vol_id = _p(p, "VolumeId")
    description = _p(p, "Description") or ""
    vol = _volumes.get(vol_id)
    if not vol:
        return _error("InvalidVolume.NotFound", f"The volume '{vol_id}' does not exist.", 400)
    snap_id = _new_snapshot_id()
    now = _now_ts()
    _snapshots[snap_id] = {
        "SnapshotId": snap_id,
        "VolumeId": vol_id,
        "VolumeSize": vol["Size"],
        "Description": description,
        "State": "completed",
        "StartTime": now,
        "Progress": "100%",
        "OwnerId": get_account_id(),
        "Encrypted": vol["Encrypted"],
        "StorageTier": "standard",
    }
    # Process TagSpecifications
    i = 1
    while _p(p, f"TagSpecification.{i}.ResourceType"):
        if _p(p, f"TagSpecification.{i}.ResourceType") == "snapshot":
            snap_tags = []
            j = 1
            while _p(p, f"TagSpecification.{i}.Tag.{j}.Key"):
                snap_tags.append({"Key": _p(p, f"TagSpecification.{i}.Tag.{j}.Key"),
                                  "Value": _p(p, f"TagSpecification.{i}.Tag.{j}.Value", "")})
                j += 1
            if snap_tags:
                _tags[snap_id] = snap_tags
        i += 1
    return _xml(200, "CreateSnapshotResponse", _snapshot_inner_xml(_snapshots[snap_id]))


def _delete_snapshot(p):
    snap_id = _p(p, "SnapshotId")
    if snap_id not in _snapshots:
        return _error("InvalidSnapshot.NotFound", f"The snapshot '{snap_id}' does not exist.", 400)
    del _snapshots[snap_id]
    return _xml(200, "DeleteSnapshotResponse", "<return>true</return>")


def _describe_snapshots(p):
    filter_ids = _parse_member_list(p, "SnapshotId")
    owner_ids = _parse_member_list(p, "Owner")
    if filter_ids:
        for sid in filter_ids:
            if sid not in _snapshots:
                return _error("InvalidSnapshot.NotFound", f"The snapshot '{sid}' does not exist", 400)
    items = ""
    for snap in _snapshots.values():
        if filter_ids and snap["SnapshotId"] not in filter_ids:
            continue
        if owner_ids and snap["OwnerId"] not in owner_ids and "self" not in owner_ids:
            continue
        items += f"<item>{_snapshot_inner_xml(snap)}</item>"
    return _xml(200, "DescribeSnapshotsResponse", f"<snapshotSet>{items}</snapshotSet>")


def _copy_snapshot(p):
    source_snap_id = _p(p, "SourceSnapshotId")
    description = _p(p, "Description") or ""
    source = _snapshots.get(source_snap_id)
    if not source:
        return _error("InvalidSnapshot.NotFound", f"The snapshot '{source_snap_id}' does not exist.", 400)
    new_snap_id = _new_snapshot_id()
    now = _now_ts()
    _snapshots[new_snap_id] = {
        **source,
        "SnapshotId": new_snap_id,
        "Description": description or source["Description"],
        "StartTime": now,
    }
    return _xml(200, "CopySnapshotResponse", f"<snapshotId>{new_snap_id}</snapshotId>")


def _modify_snapshot_attribute(p):
    snap_id = _p(p, "SnapshotId")
    snap = _snapshots.get(snap_id)
    if not snap:
        return _error("InvalidSnapshot.NotFound", f"Snapshot '{snap_id}' not found", 400)
    op = _p(p, "OperationType")
    user_ids = _parse_member_list(p, "UserId")
    perms = snap.setdefault("CreateVolumePermissions", [])
    if op == "add":
        for uid in user_ids:
            if not any(pp.get("UserId") == uid for pp in perms):
                perms.append({"UserId": uid})
    elif op == "remove":
        perms[:] = [pp for pp in perms if pp.get("UserId") not in user_ids]
    return _xml(200, "ModifySnapshotAttributeResponse", "<return>true</return>")


def _describe_snapshot_attribute(p):
    snap_id = _p(p, "SnapshotId")
    snap = _snapshots.get(snap_id)
    perms_xml = ""
    if snap:
        for pp in snap.get("CreateVolumePermissions", []):
            perms_xml += f"<item><userId>{pp['UserId']}</userId></item>"
    return _xml(200, "DescribeSnapshotAttributeResponse", f"""
        <snapshotId>{snap_id}</snapshotId>
        <createVolumePermission>{perms_xml}</createVolumePermission>""")


def _snapshot_inner_xml(snap):
    return f"""
        <snapshotId>{snap['SnapshotId']}</snapshotId>
        <volumeId>{snap['VolumeId']}</volumeId>
        <status>{snap['State']}</status>
        <startTime>{snap['StartTime']}</startTime>
        <progress>{snap['Progress']}</progress>
        <ownerId>{snap['OwnerId']}</ownerId>
        <volumeSize>{snap['VolumeSize']}</volumeSize>
        <description>{_esc(snap['Description'])}</description>
        <encrypted>{'true' if snap['Encrypted'] else 'false'}</encrypted>
        <storageTier>{snap['StorageTier']}</storageTier>
        {_tag_set_xml(snap['SnapshotId'])}"""


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def _instance_xml(inst):
    sgs = "".join(
        f"""<item><groupId>{sg['GroupId']}</groupId><groupName>{sg['GroupName']}</groupName></item>"""
        for sg in inst.get("SecurityGroups", [])
    )
    tags = "".join(
        f"<item><key>{_esc(t['Key'])}</key><value>{_esc(t['Value'])}</value></item>"
        for t in _tags.get(inst["InstanceId"], [])
    )
    return f"""<item>
        <instanceId>{inst['InstanceId']}</instanceId>
        <imageId>{inst['ImageId']}</imageId>
        <instanceState>
            <code>{inst['State']['Code']}</code>
            <name>{inst['State']['Name']}</name>
        </instanceState>
        <instanceType>{inst['InstanceType']}</instanceType>
        <keyName>{inst.get('KeyName','')}</keyName>
        <launchTime>{inst['LaunchTime']}</launchTime>
        <placement>
            <availabilityZone>{inst['Placement']['AvailabilityZone']}</availabilityZone>
            <tenancy>{inst['Placement']['Tenancy']}</tenancy>
        </placement>
        <privateDnsName>{inst['PrivateDnsName']}</privateDnsName>
        <privateIpAddress>{inst['PrivateIpAddress']}</privateIpAddress>
        <publicDnsName>{inst['PublicDnsName']}</publicDnsName>
        <publicIpAddress>{inst['PublicIpAddress']}</publicIpAddress>
        <subnetId>{inst['SubnetId']}</subnetId>
        <vpcId>{inst['VpcId']}</vpcId>
        <architecture>{inst['Architecture']}</architecture>
        <rootDeviceType>{inst['RootDeviceType']}</rootDeviceType>
        <rootDeviceName>{inst['RootDeviceName']}</rootDeviceName>
        <virtualizationType>{inst['Virtualization']}</virtualizationType>
        <hypervisor>{inst['Hypervisor']}</hypervisor>
        <monitoring><state>{inst['Monitoring']['State']}</state></monitoring>
        <groupSet>{sgs}</groupSet>
        <tagSet>{tags}</tagSet>
        <amiLaunchIndex>{inst['AmiLaunchIndex']}</amiLaunchIndex>
    </item>"""


def _sg_xml(sg):
    ingress = "".join(_perm_xml(r) for r in sg.get("IpPermissions", []))
    egress = "".join(_perm_xml(r) for r in sg.get("IpPermissionsEgress", []))
    return f"""<item>
        <ownerId>{sg['OwnerId']}</ownerId>
        <groupId>{sg['GroupId']}</groupId>
        <groupName>{sg['GroupName']}</groupName>
        <groupDescription>{sg['Description']}</groupDescription>
        <vpcId>{sg['VpcId']}</vpcId>
        <ipPermissions>{ingress}</ipPermissions>
        <ipPermissionsEgress>{egress}</ipPermissionsEgress>
        {_tag_set_xml(sg['GroupId'])}
    </item>"""


def _perm_xml(r):
    ranges = "".join(
        f"<item><cidrIp>{ip['CidrIp']}</cidrIp></item>"
        for ip in r.get("IpRanges", [])
    )
    from_port = f"<fromPort>{r['FromPort']}</fromPort>" if "FromPort" in r else ""
    to_port = f"<toPort>{r['ToPort']}</toPort>" if "ToPort" in r else ""
    return f"""<item>
        <ipProtocol>{r.get('IpProtocol','-1')}</ipProtocol>
        {from_port}{to_port}
        <ipRanges>{ranges}</ipRanges>
        <ipv6Ranges/><prefixListIds/><groups/>
    </item>"""


def _vpc_fields_xml(vpc, tag="item"):
    return f"""<{tag}>
        <vpcId>{vpc['VpcId']}</vpcId>
        <state>{vpc['State']}</state>
        <cidrBlock>{vpc['CidrBlock']}</cidrBlock>
        <dhcpOptionsId>{vpc['DhcpOptionsId']}</dhcpOptionsId>
        <instanceTenancy>{vpc['InstanceTenancy']}</instanceTenancy>
        <isDefault>{'true' if vpc['IsDefault'] else 'false'}</isDefault>
        <ownerId>{vpc['OwnerId']}</ownerId>
        {'<defaultNetworkAclId>' + vpc.get('DefaultNetworkAclId', '') + '</defaultNetworkAclId>' if vpc.get('DefaultNetworkAclId') else ''}
        {'<defaultSecurityGroupId>' + vpc.get('DefaultSecurityGroupId', '') + '</defaultSecurityGroupId>' if vpc.get('DefaultSecurityGroupId') else ''}
        {'<mainRouteTableId>' + vpc.get('MainRouteTableId', '') + '</mainRouteTableId>' if vpc.get('MainRouteTableId') else ''}
        {_tag_set_xml(vpc['VpcId'])}
    </{tag}>"""


def _vpc_xml(vpc):
    return _vpc_fields_xml(vpc, tag="item")


def _subnet_fields_xml(subnet, tag="item"):
    return f"""<{tag}>
        <subnetId>{subnet['SubnetId']}</subnetId>
        <subnetArn>arn:aws:ec2:{REGION}:{get_account_id()}:subnet/{subnet['SubnetId']}</subnetArn>
        <state>{subnet['State']}</state>
        <vpcId>{subnet['VpcId']}</vpcId>
        <cidrBlock>{subnet['CidrBlock']}</cidrBlock>
        <availableIpAddressCount>{subnet['AvailableIpAddressCount']}</availableIpAddressCount>
        <availabilityZone>{subnet['AvailabilityZone']}</availabilityZone>
        <defaultForAz>{'true' if subnet['DefaultForAz'] else 'false'}</defaultForAz>
        <mapPublicIpOnLaunch>{'true' if subnet['MapPublicIpOnLaunch'] else 'false'}</mapPublicIpOnLaunch>
        <ownerId>{subnet['OwnerId']}</ownerId>
        {_tag_set_xml(subnet['SubnetId'])}
    </{tag}>"""


def _subnet_xml(subnet):
    return _subnet_fields_xml(subnet, tag="item")


def _igw_fields_xml(igw, tag="item"):
    attachments = "".join(
        f"<item><vpcId>{a['VpcId']}</vpcId><state>{a['State']}</state></item>"
        for a in igw.get("Attachments", [])
    )
    return f"""<{tag}>
        <internetGatewayId>{igw['InternetGatewayId']}</internetGatewayId>
        <ownerId>{igw['OwnerId']}</ownerId>
        <attachmentSet>{attachments}</attachmentSet>
        {_tag_set_xml(igw['InternetGatewayId'])}
    </{tag}>"""


def _igw_xml(igw):
    return _igw_fields_xml(igw, tag="item")


def _rtb_fields_xml(rtb, tag="item"):
    def _route_xml(r):
        target = ""
        if r.get("GatewayId"):
            target = f"<gatewayId>{r['GatewayId']}</gatewayId>"
        if r.get("NatGatewayId"):
            target += f"<natGatewayId>{r['NatGatewayId']}</natGatewayId>"
        if r.get("InstanceId"):
            target += f"<instanceId>{r['InstanceId']}</instanceId>"
        if r.get("VpcPeeringConnectionId"):
            target += f"<vpcPeeringConnectionId>{r['VpcPeeringConnectionId']}</vpcPeeringConnectionId>"
        if r.get("TransitGatewayId"):
            target += f"<transitGatewayId>{r['TransitGatewayId']}</transitGatewayId>"
        return f"""<item>
        <destinationCidrBlock>{r.get('DestinationCidrBlock','')}</destinationCidrBlock>
        {target}
        <state>{r.get('State','active')}</state>
        <origin>{r.get('Origin','')}</origin>
    </item>"""
    routes = "".join(_route_xml(r) for r in rtb.get("Routes", []))
    assocs = "".join(f"""<item>
        <routeTableAssociationId>{a['RouteTableAssociationId']}</routeTableAssociationId>
        <routeTableId>{a['RouteTableId']}</routeTableId>
        <main>{'true' if a.get('Main') else 'false'}</main>
        {'<subnetId>' + a['SubnetId'] + '</subnetId>' if a.get('SubnetId') else ''}
        <associationState><state>associated</state></associationState>
    </item>""" for a in rtb.get("Associations", []))
    return f"""<{tag}>
        <routeTableId>{rtb['RouteTableId']}</routeTableId>
        <vpcId>{rtb['VpcId']}</vpcId>
        <ownerId>{rtb['OwnerId']}</ownerId>
        <routeSet>{routes}</routeSet>
        <associationSet>{assocs}</associationSet>
        <propagatingVgwSet/>
        {_tag_set_xml(rtb['RouteTableId'])}
    </{tag}>"""


def _eni_fields_xml(eni, tag="item"):
    groups = "".join(
        f"<item><groupId>{g['GroupId']}</groupId><groupName>{g['GroupName']}</groupName></item>"
        for g in eni.get("Groups", [])
    )
    attachment = ""
    if eni.get("Attachment"):
        a = eni["Attachment"]
        attachment = f"""<attachment>
            <attachmentId>{a['AttachmentId']}</attachmentId>
            <instanceId>{a.get('InstanceId','')}</instanceId>
            <deviceIndex>{a.get('DeviceIndex',0)}</deviceIndex>
            <status>{a.get('Status','attached')}</status>
        </attachment>"""
    private_ip = eni['PrivateIpAddress']
    return f"""<{tag}>
        <networkInterfaceId>{eni['NetworkInterfaceId']}</networkInterfaceId>
        <subnetId>{eni['SubnetId']}</subnetId>
        <vpcId>{eni['VpcId']}</vpcId>
        <availabilityZone>{eni.get('AvailabilityZone', REGION + 'a')}</availabilityZone>
        <description>{eni['Description']}</description>
        <ownerId>{eni['OwnerId']}</ownerId>
        <status>{eni['Status']}</status>
        <privateIpAddress>{private_ip}</privateIpAddress>
        <sourceDestCheck>{'true' if eni.get('SourceDestCheck', True) else 'false'}</sourceDestCheck>
        <interfaceType>{eni.get('InterfaceType', 'interface')}</interfaceType>
        <macAddress>{eni['MacAddress']}</macAddress>
        <groupSet>{groups}</groupSet>
        <privateIpAddressesSet>
            <item>
                <privateIpAddress>{private_ip}</privateIpAddress>
                <primary>true</primary>
            </item>
        </privateIpAddressesSet>
        {attachment}
        <tagSet/>
    </{tag}>"""


def _vpce_fields_xml(ep, tag="item"):
    rtb_ids = "".join(f"<item>{r}</item>" for r in ep.get("RouteTableIds", []))
    subnet_ids = "".join(f"<item>{s}</item>" for s in ep.get("SubnetIds", []))
    return f"""<{tag}>
        <vpcEndpointId>{ep['VpcEndpointId']}</vpcEndpointId>
        <vpcEndpointType>{ep['VpcEndpointType']}</vpcEndpointType>
        <vpcId>{ep['VpcId']}</vpcId>
        <serviceName>{ep['ServiceName']}</serviceName>
        <state>{ep['State']}</state>
        <ownerId>{ep['OwnerId']}</ownerId>
        <routeTableIdSet>{rtb_ids}</routeTableIdSet>
        <subnetIdSet>{subnet_ids}</subnetIdSet>
        <tagSet/>
    </{tag}>"""


# ---------------------------------------------------------------------------
# Parse helpers
# ---------------------------------------------------------------------------

def _p(params, key, default=""):
    val = params.get(key, [default])
    if isinstance(val, list):
        return val[0] if val else default
    return val


def _parse_tag_specs(p, resource_type, resource_id):
    """Parse TagSpecification.N from params and store tags for the given resource."""
    i = 1
    while _p(p, f"TagSpecification.{i}.ResourceType"):
        if _p(p, f"TagSpecification.{i}.ResourceType") == resource_type:
            tags = []
            j = 1
            while _p(p, f"TagSpecification.{i}.Tag.{j}.Key"):
                tags.append({
                    "Key": _p(p, f"TagSpecification.{i}.Tag.{j}.Key"),
                    "Value": _p(p, f"TagSpecification.{i}.Tag.{j}.Value", ""),
                })
                j += 1
            if tags:
                _tags[resource_id] = tags
        i += 1


def _parse_member_list(params, prefix):
    items = []
    i = 1
    while True:
        val = _p(params, f"{prefix}.{i}")
        if not val:
            break
        items.append(val)
        i += 1
    return items


def _parse_tags(params):
    tags = []
    i = 1
    while True:
        key = _p(params, f"Tag.{i}.Key")
        if not key:
            break
        tags.append({"Key": key, "Value": _p(params, f"Tag.{i}.Value", "")})
        i += 1
    return tags


def _parse_filters(params):
    filters = {}
    i = 1
    while True:
        name = _p(params, f"Filter.{i}.Name")
        if not name:
            break
        vals = []
        j = 1
        while True:
            v = _p(params, f"Filter.{i}.Value.{j}")
            if not v:
                break
            vals.append(v)
            j += 1
        filters[name] = vals
        i += 1
    return filters


def _matches_filters(inst, filters):
    for name, vals in filters.items():
        if name == "instance-state-name":
            if inst["State"]["Name"] not in vals:
                return False
        elif name == "instance-type":
            if inst["InstanceType"] not in vals:
                return False
        elif name == "image-id":
            if inst["ImageId"] not in vals:
                return False
    return True


def _parse_ip_permissions(params, prefix):
    rules = []
    i = 1
    while True:
        proto = _p(params, f"{prefix}.{i}.IpProtocol")
        if not proto:
            break
        rule = {"IpProtocol": proto, "IpRanges": [], "Ipv6Ranges": [],
                "PrefixListIds": [], "UserIdGroupPairs": []}
        from_port = _p(params, f"{prefix}.{i}.FromPort")
        to_port = _p(params, f"{prefix}.{i}.ToPort")
        if from_port:
            rule["FromPort"] = int(from_port)
        if to_port:
            rule["ToPort"] = int(to_port)
        j = 1
        while True:
            cidr = _p(params, f"{prefix}.{i}.IpRanges.{j}.CidrIp")
            if not cidr:
                break
            rule["IpRanges"].append({"CidrIp": cidr})
            j += 1
        rules.append(rule)
        i += 1
    return rules


# ---------------------------------------------------------------------------
# ID generators
# ---------------------------------------------------------------------------

def _new_instance_id():
    return "i-" + "".join(random.choices(string.hexdigits[:16], k=17))


def _new_sg_id():
    return "sg-" + "".join(random.choices(string.hexdigits[:16], k=17))


def _new_vpc_id():
    return "vpc-" + "".join(random.choices(string.hexdigits[:16], k=17))


def _new_subnet_id():
    return "subnet-" + "".join(random.choices(string.hexdigits[:16], k=17))


def _new_igw_id():
    return "igw-" + "".join(random.choices(string.hexdigits[:16], k=17))


def _random_ip(prefix):
    return f"{prefix}{random.randint(1,254)}.{random.randint(1,254)}"


def _now_ts():
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def _guess_resource_type(resource_id):
    _PREFIX_MAP = {
        "i-": "instance",
        "sg-": "security-group",
        "vpc-": "vpc",
        "subnet-": "subnet",
        "igw-": "internet-gateway",
        "eipalloc-": "elastic-ip",
        "rtb-": "route-table",
        "eni-": "network-interface",
        "vpce-": "vpc-endpoint",
        "vol-": "volume",
        "snap-": "snapshot",
        "acl-": "network-acl",
        "nat-": "natgateway",
        "dopt-": "dhcp-options",
        "eigw-": "egress-only-internet-gateway",
        "lt-": "launch-template",
        "pl-": "managed-prefix-list",
        "vgw-": "vpn-gateway",
        "cgw-": "customer-gateway",
        "ami-": "image",
        "tgw-": "transit-gateway",
    }
    for prefix, rtype in _PREFIX_MAP.items():
        if resource_id.startswith(prefix):
            return rtype
    return "resource"


# ---------------------------------------------------------------------------
# XML response builders
# ---------------------------------------------------------------------------

def _xml(status, root_tag, inner):
    from ministack.core.responses import new_uuid as _uuid
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<{root_tag} xmlns="http://ec2.amazonaws.com/doc/2016-11-15/">
    {inner}
    <requestId>{_uuid()}</requestId>
</{root_tag}>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


def _error(code, message, status):
    from ministack.core.responses import new_uuid as _uuid
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Errors><Error>
        <Code>{code}</Code>
        <Message>{message}</Message>
    </Error></Errors>
    <RequestID>{_uuid()}</RequestID>
</Response>""".encode("utf-8")
    return status, {"Content-Type": "application/xml"}, body


# ---------------------------------------------------------------------------
# NAT Gateways
# ---------------------------------------------------------------------------

def _create_nat_gateway(params):
    subnet_id = _p(params, "SubnetId")
    alloc_id = _p(params, "AllocationId")
    connectivity = _p(params, "ConnectivityType") or "public"
    if not subnet_id:
        return _error("MissingParameter", "SubnetId is required", 400)
    nat_id = "nat-" + "".join(random.choices(string.hexdigits[:16], k=17))
    subnet = _subnets.get(subnet_id)
    vpc_id = subnet["VpcId"] if subnet else _DEFAULT_VPC_ID
    tags = _parse_tags(params)
    record = {
        "NatGatewayId": nat_id,
        "SubnetId": subnet_id,
        "VpcId": vpc_id,
        "AllocationId": alloc_id,
        "ConnectivityType": connectivity,
        "State": "available",
        "CreateTime": _now_ts(),
        "Tags": tags,
    }
    _nat_gateways[nat_id] = record
    if tags:
        _tags[nat_id] = tags
    _parse_tag_specs(params, "natgateway", nat_id)
    inner = f"""<natGateway>
        <natGatewayId>{nat_id}</natGatewayId>
        <subnetId>{subnet_id}</subnetId>
        <vpcId>{vpc_id}</vpcId>
        <state>available</state>
        <connectivityType>{connectivity}</connectivityType>
        <createTime>{_now_ts()}</createTime>
        <natGatewayAddressSet/>
        <tagSet/>
    </natGateway>"""
    return _xml(200, "CreateNatGatewayResponse", inner)


def _describe_nat_gateways(params):
    filters = _parse_filters(params)
    ids = _parse_member_list(params, "NatGatewayId")
    items = ""
    for nat in _nat_gateways.values():
        if ids and nat["NatGatewayId"] not in ids:
            continue
        if filters.get("state") and nat["State"] not in filters["state"]:
            continue
        if filters.get("vpc-id") and nat["VpcId"] not in filters["vpc-id"]:
            continue
        if filters.get("subnet-id") and nat["SubnetId"] not in filters["subnet-id"]:
            continue
        items += f"""<item>
            <natGatewayId>{nat['NatGatewayId']}</natGatewayId>
            <subnetId>{nat['SubnetId']}</subnetId>
            <vpcId>{nat['VpcId']}</vpcId>
            <state>{nat['State']}</state>
            <connectivityType>{nat['ConnectivityType']}</connectivityType>
            <createTime>{nat['CreateTime']}</createTime>
            <natGatewayAddressSet/>
            {_tag_set_xml(nat['NatGatewayId'])}
        </item>"""
    return _xml(200, "DescribeNatGatewaysResponse",
                f"<natGatewaySet>{items}</natGatewaySet>")


def _delete_nat_gateway(params):
    nat_id = _p(params, "NatGatewayId")
    if nat_id not in _nat_gateways:
        return _error("NatGatewayNotFound", f"NatGateway {nat_id} not found", 400)
    _nat_gateways[nat_id]["State"] = "deleted"
    return _xml(200, "DeleteNatGatewayResponse",
                f"<natGatewayId>{nat_id}</natGatewayId>")


# ---------------------------------------------------------------------------
# Network ACLs
# ---------------------------------------------------------------------------

def _create_network_acl(params):
    vpc_id = _p(params, "VpcId")
    if not vpc_id:
        return _error("MissingParameter", "VpcId is required", 400)
    acl_id = "acl-" + "".join(random.choices(string.hexdigits[:16], k=17))
    tags = _parse_tags(params)
    record = {
        "NetworkAclId": acl_id,
        "VpcId": vpc_id,
        "IsDefault": False,
        "Entries": [],
        "Associations": [],
        "Tags": tags,
        "OwnerId": get_account_id(),
    }
    _network_acls[acl_id] = record
    if tags:
        _tags[acl_id] = tags
    _parse_tag_specs(params, "network-acl", acl_id)
    inner = f"""<networkAcl>
        <networkAclId>{acl_id}</networkAclId>
        <vpcId>{vpc_id}</vpcId>
        <default>false</default>
        <entrySet/>
        <associationSet/>
        <tagSet/>
        <ownerId>{get_account_id()}</ownerId>
    </networkAcl>"""
    return _xml(200, "CreateNetworkAclResponse", inner)


def _describe_network_acls(params):
    filters = _parse_filters(params)
    ids = _parse_member_list(params, "NetworkAclId")
    items = ""
    for acl in _network_acls.values():
        if ids and acl["NetworkAclId"] not in ids:
            continue
        if filters.get("vpc-id") and acl["VpcId"] not in filters["vpc-id"]:
            continue
        if filters.get("default"):
            want_default = filters["default"][0].lower() == "true"
            if acl.get("IsDefault", False) != want_default:
                continue
        entries = "".join(f"""<item>
            <ruleNumber>{e['RuleNumber']}</ruleNumber>
            <protocol>{e['Protocol']}</protocol>
            <ruleAction>{e['RuleAction']}</ruleAction>
            <egress>{'true' if e['Egress'] else 'false'}</egress>
            <cidrBlock>{e.get('CidrBlock','0.0.0.0/0')}</cidrBlock>
        </item>""" for e in acl["Entries"])
        assocs = "".join(f"""<item>
            <networkAclAssociationId>{a['NetworkAclAssociationId']}</networkAclAssociationId>
            <networkAclId>{acl['NetworkAclId']}</networkAclId>
            <subnetId>{a['SubnetId']}</subnetId>
        </item>""" for a in acl["Associations"])
        items += f"""<item>
            <networkAclId>{acl['NetworkAclId']}</networkAclId>
            <vpcId>{acl['VpcId']}</vpcId>
            <default>{'true' if acl['IsDefault'] else 'false'}</default>
            <entrySet>{entries}</entrySet>
            <associationSet>{assocs}</associationSet>
            <tagSet/>
            <ownerId>{acl['OwnerId']}</ownerId>
        </item>"""
    return _xml(200, "DescribeNetworkAclsResponse",
                f"<networkAclSet>{items}</networkAclSet>")


def _delete_network_acl(params):
    acl_id = _p(params, "NetworkAclId")
    if acl_id not in _network_acls:
        return _error("InvalidNetworkAclID.NotFound", f"The network ACL '{acl_id}' does not exist", 400)
    del _network_acls[acl_id]
    return _xml(200, "DeleteNetworkAclResponse", "<return>true</return>")


def _create_network_acl_entry(params):
    acl_id = _p(params, "NetworkAclId")
    if acl_id not in _network_acls:
        return _error("InvalidNetworkAclID.NotFound", f"The network ACL '{acl_id}' does not exist", 400)
    entry = {
        "RuleNumber": int(_p(params, "RuleNumber") or 100),
        "Protocol": _p(params, "Protocol") or "-1",
        "RuleAction": _p(params, "RuleAction") or "allow",
        "Egress": _p(params, "Egress") == "true",
        "CidrBlock": _p(params, "CidrBlock") or "0.0.0.0/0",
    }
    _network_acls[acl_id]["Entries"].append(entry)
    return _xml(200, "CreateNetworkAclEntryResponse", "<return>true</return>")


def _delete_network_acl_entry(params):
    acl_id = _p(params, "NetworkAclId")
    rule_num = int(_p(params, "RuleNumber") or 0)
    egress = _p(params, "Egress") == "true"
    if acl_id not in _network_acls:
        return _error("InvalidNetworkAclID.NotFound", f"The network ACL '{acl_id}' does not exist", 400)
    acl = _network_acls[acl_id]
    acl["Entries"] = [e for e in acl["Entries"]
                      if not (e["RuleNumber"] == rule_num and e["Egress"] == egress)]
    return _xml(200, "DeleteNetworkAclEntryResponse", "<return>true</return>")


def _replace_network_acl_entry(params):
    acl_id = _p(params, "NetworkAclId")
    rule_num = int(_p(params, "RuleNumber") or 0)
    egress = _p(params, "Egress") == "true"
    if acl_id not in _network_acls:
        return _error("InvalidNetworkAclID.NotFound", f"The network ACL '{acl_id}' does not exist", 400)
    acl = _network_acls[acl_id]
    acl["Entries"] = [e for e in acl["Entries"]
                      if not (e["RuleNumber"] == rule_num and e["Egress"] == egress)]
    acl["Entries"].append({
        "RuleNumber": rule_num,
        "Protocol": _p(params, "Protocol") or "-1",
        "RuleAction": _p(params, "RuleAction") or "allow",
        "Egress": egress,
        "CidrBlock": _p(params, "CidrBlock") or "0.0.0.0/0",
    })
    return _xml(200, "ReplaceNetworkAclEntryResponse", "<return>true</return>")


def _replace_network_acl_association(params):
    assoc_id = _p(params, "AssociationId")
    new_acl_id = _p(params, "NetworkAclId")
    if new_acl_id not in _network_acls:
        return _error("InvalidNetworkAclID.NotFound", f"The network ACL '{new_acl_id}' does not exist", 400)
    new_assoc_id = "aclassoc-" + "".join(random.choices(string.hexdigits[:16], k=17))
    # Remove old association from whichever ACL owns it
    for acl in _network_acls.values():
        acl["Associations"] = [a for a in acl["Associations"]
                                if a["NetworkAclAssociationId"] != assoc_id]
    subnet_id = ""
    _network_acls[new_acl_id]["Associations"].append({
        "NetworkAclAssociationId": new_assoc_id,
        "SubnetId": subnet_id,
    })
    return _xml(200, "ReplaceNetworkAclAssociationResponse",
                f"<newAssociationId>{new_assoc_id}</newAssociationId>")


# ---------------------------------------------------------------------------
# Flow Logs
# ---------------------------------------------------------------------------

def _create_flow_logs(params):
    resource_ids = _parse_member_list(params, "ResourceId")
    resource_type = _p(params, "ResourceType") or "VPC"
    traffic_type = _p(params, "TrafficType") or "ALL"
    log_dest_type = _p(params, "LogDestinationType") or "cloud-watch-logs"
    log_dest = _p(params, "LogDestination") or _p(params, "LogGroupName")
    created = []
    for rid in resource_ids:
        fl_id = "fl-" + "".join(random.choices(string.hexdigits[:16], k=17))
        _flow_logs[fl_id] = {
            "FlowLogId": fl_id,
            "ResourceId": rid,
            "ResourceType": resource_type,
            "TrafficType": traffic_type,
            "LogDestinationType": log_dest_type,
            "LogDestination": log_dest,
            "FlowLogStatus": "ACTIVE",
            "CreationTime": _now_ts(),
        }
        created.append(fl_id)
    ids_xml = "".join(f"<item>{fid}</item>" for fid in created)
    return _xml(200, "CreateFlowLogsResponse",
                f"<flowLogIdSet>{ids_xml}</flowLogIdSet><unsuccessful/>")


def _describe_flow_logs(params):
    ids = _parse_member_list(params, "FlowLogId")
    filters = _parse_filters(params)
    items = ""
    for fl in _flow_logs.values():
        if ids and fl["FlowLogId"] not in ids:
            continue
        if filters.get("resource-id") and fl["ResourceId"] not in filters["resource-id"]:
            continue
        items += f"""<item>
            <flowLogId>{fl['FlowLogId']}</flowLogId>
            <resourceId>{fl['ResourceId']}</resourceId>
            <trafficType>{fl['TrafficType']}</trafficType>
            <logDestinationType>{fl['LogDestinationType']}</logDestinationType>
            <logDestination>{fl.get('LogDestination','')}</logDestination>
            <flowLogStatus>{fl['FlowLogStatus']}</flowLogStatus>
            <creationTime>{fl['CreationTime']}</creationTime>
        </item>"""
    return _xml(200, "DescribeFlowLogsResponse", f"<flowLogSet>{items}</flowLogSet>")


def _delete_flow_logs(params):
    ids = _parse_member_list(params, "FlowLogId")
    for fid in ids:
        _flow_logs.pop(fid, None)
    return _xml(200, "DeleteFlowLogsResponse", "<unsuccessful/>")


# ---------------------------------------------------------------------------
# VPC Peering Connections
# ---------------------------------------------------------------------------

def _create_vpc_peering_connection(params):
    vpc_id = _p(params, "VpcId")
    peer_vpc_id = _p(params, "PeerVpcId")
    peer_owner_id = _p(params, "PeerOwnerId") or get_account_id()
    peer_region = _p(params, "PeerRegion") or REGION
    if not vpc_id or not peer_vpc_id:
        return _error("MissingParameter", "VpcId and PeerVpcId are required", 400)
    pcx_id = "pcx-" + "".join(random.choices(string.hexdigits[:16], k=17))
    record = {
        "VpcPeeringConnectionId": pcx_id,
        "RequesterVpcInfo": {"VpcId": vpc_id, "OwnerId": get_account_id(), "Region": REGION},
        "AccepterVpcInfo": {"VpcId": peer_vpc_id, "OwnerId": peer_owner_id, "Region": peer_region},
        "Status": {"Code": "pending-acceptance", "Message": "Pending Acceptance by " + peer_owner_id},
        "ExpirationTime": _now_ts(),
        "Tags": [],
    }
    _vpc_peering[pcx_id] = record
    inner = f"""<vpcPeeringConnection>
        <vpcPeeringConnectionId>{pcx_id}</vpcPeeringConnectionId>
        <requesterVpcInfo><vpcId>{vpc_id}</vpcId><ownerId>{get_account_id()}</ownerId><region>{REGION}</region></requesterVpcInfo>
        <accepterVpcInfo><vpcId>{peer_vpc_id}</vpcId><ownerId>{peer_owner_id}</ownerId><region>{peer_region}</region></accepterVpcInfo>
        <status><code>pending-acceptance</code></status>
        <tagSet/>
    </vpcPeeringConnection>"""
    return _xml(200, "CreateVpcPeeringConnectionResponse", inner)


def _accept_vpc_peering_connection(params):
    pcx_id = _p(params, "VpcPeeringConnectionId")
    if pcx_id not in _vpc_peering:
        return _error("InvalidVpcPeeringConnectionID.NotFound",
                      f"The VPC peering connection '{pcx_id}' does not exist", 400)
    _vpc_peering[pcx_id]["Status"] = {"Code": "active", "Message": "Active"}
    pcx = _vpc_peering[pcx_id]
    inner = f"""<vpcPeeringConnection>
        <vpcPeeringConnectionId>{pcx_id}</vpcPeeringConnectionId>
        <requesterVpcInfo><vpcId>{pcx['RequesterVpcInfo']['VpcId']}</vpcId><ownerId>{pcx['RequesterVpcInfo']['OwnerId']}</ownerId><region>{pcx['RequesterVpcInfo']['Region']}</region></requesterVpcInfo>
        <accepterVpcInfo><vpcId>{pcx['AccepterVpcInfo']['VpcId']}</vpcId><ownerId>{pcx['AccepterVpcInfo']['OwnerId']}</ownerId><region>{pcx['AccepterVpcInfo']['Region']}</region></accepterVpcInfo>
        <status><code>active</code></status>
        <tagSet/>
    </vpcPeeringConnection>"""
    return _xml(200, "AcceptVpcPeeringConnectionResponse", inner)


def _describe_vpc_peering_connections(params):
    ids = _parse_member_list(params, "VpcPeeringConnectionId")
    filters = _parse_filters(params)
    items = ""
    for pcx in _vpc_peering.values():
        if ids and pcx["VpcPeeringConnectionId"] not in ids:
            continue
        if filters.get("status-code") and pcx["Status"]["Code"] not in filters["status-code"]:
            continue
        items += f"""<item>
            <vpcPeeringConnectionId>{pcx['VpcPeeringConnectionId']}</vpcPeeringConnectionId>
            <requesterVpcInfo><vpcId>{pcx['RequesterVpcInfo']['VpcId']}</vpcId><ownerId>{pcx['RequesterVpcInfo']['OwnerId']}</ownerId><region>{pcx['RequesterVpcInfo']['Region']}</region></requesterVpcInfo>
            <accepterVpcInfo><vpcId>{pcx['AccepterVpcInfo']['VpcId']}</vpcId><ownerId>{pcx['AccepterVpcInfo']['OwnerId']}</ownerId><region>{pcx['AccepterVpcInfo']['Region']}</region></accepterVpcInfo>
            <status><code>{pcx['Status']['Code']}</code><message>{pcx['Status']['Message']}</message></status>
            <tagSet/>
        </item>"""
    return _xml(200, "DescribeVpcPeeringConnectionsResponse",
                f"<vpcPeeringConnectionSet>{items}</vpcPeeringConnectionSet>")


def _delete_vpc_peering_connection(params):
    pcx_id = _p(params, "VpcPeeringConnectionId")
    if pcx_id not in _vpc_peering:
        return _error("InvalidVpcPeeringConnectionID.NotFound",
                      f"The VPC peering connection '{pcx_id}' does not exist", 400)
    _vpc_peering[pcx_id]["Status"] = {"Code": "deleted", "Message": "Deleted"}
    return _xml(200, "DeleteVpcPeeringConnectionResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# DHCP Options
# ---------------------------------------------------------------------------

def _create_dhcp_options(params):
    # Parse DhcpConfigurations: DhcpConfiguration.N.Key, DhcpConfiguration.N.Value.N
    configs = []
    i = 1
    while True:
        key = _p(params, f"DhcpConfiguration.{i}.Key")
        if not key:
            break
        vals = []
        j = 1
        while True:
            v = _p(params, f"DhcpConfiguration.{i}.Value.{j}")
            if not v:
                break
            vals.append(v)
            j += 1
        configs.append({"Key": key, "Values": vals})
        i += 1
    dopt_id = "dopt-" + "".join(random.choices(string.hexdigits[:16], k=17))
    tags = _parse_tags(params)
    record = {
        "DhcpOptionsId": dopt_id,
        "DhcpConfigurations": configs,
        "OwnerId": get_account_id(),
        "Tags": tags,
    }
    _dhcp_options[dopt_id] = record
    if tags:
        _tags[dopt_id] = tags
    configs_xml = "".join(f"""<item>
        <key>{c['Key']}</key>
        <valueSet>{"".join(f'<item><value>{v}</value></item>' for v in c['Values'])}</valueSet>
    </item>""" for c in configs)
    inner = f"""<dhcpOptions>
        <dhcpOptionsId>{dopt_id}</dhcpOptionsId>
        <dhcpConfigurationSet>{configs_xml}</dhcpConfigurationSet>
        <ownerId>{get_account_id()}</ownerId>
        <tagSet/>
    </dhcpOptions>"""
    return _xml(200, "CreateDhcpOptionsResponse", inner)


def _associate_dhcp_options(params):
    dopt_id = _p(params, "DhcpOptionsId")
    vpc_id = _p(params, "VpcId")
    if vpc_id not in _vpcs:
        return _error("InvalidVpcID.NotFound", f"The VPC '{vpc_id}' does not exist", 400)
    # "default" is valid — resets to AWS-provided DHCP options
    if dopt_id != "default" and dopt_id not in _dhcp_options:
        return _error("InvalidDhcpOptionsID.NotFound",
                      f"The dhcp options '{dopt_id}' does not exist", 400)
    _vpcs[vpc_id]["DhcpOptionsId"] = dopt_id
    return _xml(200, "AssociateDhcpOptionsResponse", "<return>true</return>")


def _describe_dhcp_options(params):
    ids = _parse_member_list(params, "DhcpOptionsId")
    items = ""
    for dopt in _dhcp_options.values():
        if ids and dopt["DhcpOptionsId"] not in ids:
            continue
        configs_xml = "".join(f"""<item>
            <key>{c['Key']}</key>
            <valueSet>{"".join(f'<item><value>{v}</value></item>' for v in c['Values'])}</valueSet>
        </item>""" for c in dopt["DhcpConfigurations"])
        items += f"""<item>
            <dhcpOptionsId>{dopt['DhcpOptionsId']}</dhcpOptionsId>
            <dhcpConfigurationSet>{configs_xml}</dhcpConfigurationSet>
            <ownerId>{dopt['OwnerId']}</ownerId>
            <tagSet/>
        </item>"""
    return _xml(200, "DescribeDhcpOptionsResponse", f"<dhcpOptionsSet>{items}</dhcpOptionsSet>")


def _delete_dhcp_options(params):
    dopt_id = _p(params, "DhcpOptionsId")
    if dopt_id not in _dhcp_options:
        return _error("InvalidDhcpOptionsID.NotFound",
                      f"The dhcp options '{dopt_id}' does not exist", 400)
    del _dhcp_options[dopt_id]
    return _xml(200, "DeleteDhcpOptionsResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# Egress-Only Internet Gateways
# ---------------------------------------------------------------------------

def _create_egress_only_igw(params):
    vpc_id = _p(params, "VpcId")
    if not vpc_id:
        return _error("MissingParameter", "VpcId is required", 400)
    eigw_id = "eigw-" + "".join(random.choices(string.hexdigits[:16], k=17))
    tags = _parse_tags(params)
    record = {
        "EgressOnlyInternetGatewayId": eigw_id,
        "VpcId": vpc_id,
        "State": "attached",
        "Tags": tags,
    }
    _egress_igws[eigw_id] = record
    if tags:
        _tags[eigw_id] = tags
    inner = f"""<egressOnlyInternetGateway>
        <egressOnlyInternetGatewayId>{eigw_id}</egressOnlyInternetGatewayId>
        <attachmentSet>
            <item>
                <vpcId>{vpc_id}</vpcId>
                <state>attached</state>
            </item>
        </attachmentSet>
        <tagSet/>
    </egressOnlyInternetGateway>"""
    return _xml(200, "CreateEgressOnlyInternetGatewayResponse", inner)


def _describe_egress_only_igws(params):
    ids = _parse_member_list(params, "EgressOnlyInternetGatewayId")
    items = ""
    for eigw in _egress_igws.values():
        if ids and eigw["EgressOnlyInternetGatewayId"] not in ids:
            continue
        items += f"""<item>
            <egressOnlyInternetGatewayId>{eigw['EgressOnlyInternetGatewayId']}</egressOnlyInternetGatewayId>
            <attachmentSet>
                <item>
                    <vpcId>{eigw['VpcId']}</vpcId>
                    <state>{eigw['State']}</state>
                </item>
            </attachmentSet>
            <tagSet/>
        </item>"""
    return _xml(200, "DescribeEgressOnlyInternetGatewaysResponse",
                f"<egressOnlyInternetGatewaySet>{items}</egressOnlyInternetGatewaySet>")


def _delete_egress_only_igw(params):
    eigw_id = _p(params, "EgressOnlyInternetGatewayId")
    if eigw_id not in _egress_igws:
        return _error("InvalidGatewayID.NotFound",
                      f"The egress only internet gateway '{eigw_id}' does not exist", 400)
    del _egress_igws[eigw_id]
    return _xml(200, "DeleteEgressOnlyInternetGatewayResponse", "<returnCode>true</returnCode>")


# ---------------------------------------------------------------------------
# ReplaceRouteTableAssociation
# ---------------------------------------------------------------------------

def _replace_route_table_association(p):
    assoc_id = _p(p, "AssociationId")
    new_rtb_id = _p(p, "RouteTableId")
    if new_rtb_id not in _route_tables:
        return _error("InvalidRouteTableID.NotFound", f"The route table '{new_rtb_id}' does not exist", 400)
    new_assoc_id = "rtbassoc-" + "".join(random.choices(string.hexdigits[:16], k=17))
    for rtb in _route_tables.values():
        for i, a in enumerate(rtb["Associations"]):
            if a["RouteTableAssociationId"] == assoc_id:
                subnet_id = a.get("SubnetId")
                is_main = a.get("Main", False)
                rtb["Associations"].pop(i)
                _route_tables[new_rtb_id]["Associations"].append({
                    "RouteTableAssociationId": new_assoc_id,
                    "RouteTableId": new_rtb_id,
                    "SubnetId": subnet_id,
                    "Main": is_main,
                    "AssociationState": {"State": "associated"},
                })
                return _xml(200, "ReplaceRouteTableAssociationResponse",
                            f"<newAssociationId>{new_assoc_id}</newAssociationId>")
    return _error("InvalidAssociationID.NotFound", f"Association '{assoc_id}' not found", 400)


# ---------------------------------------------------------------------------
# ModifyVpcEndpoint
# ---------------------------------------------------------------------------

def _modify_vpc_endpoint(p):
    vpce_id = _p(p, "VpcEndpointId")
    ep = _vpc_endpoints.get(vpce_id)
    if not ep:
        return _error("InvalidVpcEndpointId.NotFound", f"The VPC endpoint '{vpce_id}' does not exist", 400)
    add_rtbs = _parse_member_list(p, "AddRouteTableId")
    rm_rtbs = _parse_member_list(p, "RemoveRouteTableId")
    add_subnets = _parse_member_list(p, "AddSubnetId")
    rm_subnets = _parse_member_list(p, "RemoveSubnetId")
    if add_rtbs:
        ep["RouteTableIds"] = list(set(ep.get("RouteTableIds", []) + add_rtbs))
    if rm_rtbs:
        ep["RouteTableIds"] = [r for r in ep.get("RouteTableIds", []) if r not in rm_rtbs]
    if add_subnets:
        ep["SubnetIds"] = list(set(ep.get("SubnetIds", []) + add_subnets))
    if rm_subnets:
        ep["SubnetIds"] = [s for s in ep.get("SubnetIds", []) if s not in rm_subnets]
    policy = _p(p, "PolicyDocument")
    if policy:
        ep["PolicyDocument"] = policy
    return _xml(200, "ModifyVpcEndpointResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# DescribePrefixLists
# ---------------------------------------------------------------------------

_AWS_PREFIX_LISTS = {
    "com.amazonaws.{region}.s3": ("pl-63a5400a", "com.amazonaws.{region}.s3"),
    "com.amazonaws.{region}.dynamodb": ("pl-02cd2c6b", "com.amazonaws.{region}.dynamodb"),
}

def _describe_prefix_lists(p):
    filter_ids = _parse_member_list(p, "PrefixListId")
    filters = _parse_filters(p)
    items = ""
    # Built-in AWS service prefix lists
    for tpl_svc, (pl_id, tpl_name) in _AWS_PREFIX_LISTS.items():
        svc = tpl_svc.replace("{region}", REGION)
        name = tpl_name.replace("{region}", REGION)
        if filter_ids and pl_id not in filter_ids:
            continue
        if filters.get("prefix-list-name") and name not in filters["prefix-list-name"]:
            continue
        items += f"""<item>
            <prefixListId>{pl_id}</prefixListId>
            <prefixListName>{name}</prefixListName>
            <cidrSet><item><cidr>0.0.0.0/0</cidr></item></cidrSet>
        </item>"""
    # User-created managed prefix lists
    for pl in _prefix_lists.values():
        if filter_ids and pl["PrefixListId"] not in filter_ids:
            continue
        if filters.get("prefix-list-name") and pl.get("PrefixListName", "") not in filters["prefix-list-name"]:
            continue
        entries = "".join(f"<item><cidr>{e['Cidr']}</cidr></item>" for e in pl.get("Entries", []))
        items += f"""<item>
            <prefixListId>{pl['PrefixListId']}</prefixListId>
            <prefixListName>{pl.get('PrefixListName','')}</prefixListName>
            <cidrSet>{entries}</cidrSet>
        </item>"""
    return _xml(200, "DescribePrefixListsResponse", f"<prefixListSet>{items}</prefixListSet>")


# ---------------------------------------------------------------------------
# Managed Prefix Lists
# ---------------------------------------------------------------------------

def _create_managed_prefix_list(p):
    name = _p(p, "PrefixListName") or ""
    max_entries = int(_p(p, "MaxEntries") or "10")
    af = _p(p, "AddressFamily") or "IPv4"
    pl_id = "pl-" + "".join(random.choices(string.hexdigits[:16], k=17))
    entries = []
    i = 1
    while _p(p, f"Entry.{i}.Cidr"):
        entries.append({"Cidr": _p(p, f"Entry.{i}.Cidr"), "Description": _p(p, f"Entry.{i}.Description")})
        i += 1
    tags = _parse_tags(p)
    _prefix_lists[pl_id] = {
        "PrefixListId": pl_id, "PrefixListName": name, "State": "create-complete",
        "AddressFamily": af, "MaxEntries": max_entries, "Version": 1,
        "Entries": entries, "Tags": tags, "OwnerId": get_account_id(),
        "PrefixListArn": f"arn:aws:ec2:{REGION}:{get_account_id()}:prefix-list/{pl_id}",
    }
    if tags:
        _tags[pl_id] = tags
    return _xml(200, "CreateManagedPrefixListResponse", _prefix_list_xml(_prefix_lists[pl_id], tag="prefixList"))


def _describe_managed_prefix_lists(p):
    filter_ids = _parse_member_list(p, "PrefixListId")
    filters = _parse_filters(p)
    items = ""
    for pl in _prefix_lists.values():
        if filter_ids and pl["PrefixListId"] not in filter_ids:
            continue
        if filters.get("prefix-list-name") and pl.get("PrefixListName", "") not in filters["prefix-list-name"]:
            continue
        items += _prefix_list_xml(pl)
    return _xml(200, "DescribeManagedPrefixListsResponse", f"<prefixListSet>{items}</prefixListSet>")


def _get_managed_prefix_list_entries(p):
    pl_id = _p(p, "PrefixListId")
    pl = _prefix_lists.get(pl_id)
    if not pl:
        return _error("InvalidPrefixListID.NotFound", f"Prefix list '{pl_id}' not found", 400)
    entries = "".join(f"""<item>
        <cidr>{e['Cidr']}</cidr>
        <description>{e.get('Description','')}</description>
    </item>""" for e in pl.get("Entries", []))
    return _xml(200, "GetManagedPrefixListEntriesResponse", f"<entrySet>{entries}</entrySet>")


def _modify_managed_prefix_list(p):
    pl_id = _p(p, "PrefixListId")
    pl = _prefix_lists.get(pl_id)
    if not pl:
        return _error("InvalidPrefixListID.NotFound", f"Prefix list '{pl_id}' not found", 400)
    name = _p(p, "PrefixListName")
    if name:
        pl["PrefixListName"] = name
    max_e = _p(p, "MaxEntries")
    if max_e:
        pl["MaxEntries"] = int(max_e)
    # Add entries
    i = 1
    while _p(p, f"AddEntry.{i}.Cidr"):
        pl["Entries"].append({"Cidr": _p(p, f"AddEntry.{i}.Cidr"), "Description": _p(p, f"AddEntry.{i}.Description")})
        i += 1
    # Remove entries
    i = 1
    rm_cidrs = set()
    while _p(p, f"RemoveEntry.{i}.Cidr"):
        rm_cidrs.add(_p(p, f"RemoveEntry.{i}.Cidr"))
        i += 1
    if rm_cidrs:
        pl["Entries"] = [e for e in pl["Entries"] if e["Cidr"] not in rm_cidrs]
    pl["Version"] = pl.get("Version", 1) + 1
    return _xml(200, "ModifyManagedPrefixListResponse", _prefix_list_xml(pl, tag="prefixList"))


def _delete_managed_prefix_list(p):
    pl_id = _p(p, "PrefixListId")
    if pl_id not in _prefix_lists:
        return _error("InvalidPrefixListID.NotFound", f"Prefix list '{pl_id}' not found", 400)
    del _prefix_lists[pl_id]
    return _xml(200, "DeleteManagedPrefixListResponse", "<return>true</return>")


def _prefix_list_xml(pl, tag="item"):
    return f"""<{tag}>
        <prefixListId>{pl['PrefixListId']}</prefixListId>
        <prefixListName>{pl.get('PrefixListName','')}</prefixListName>
        <state>{pl.get('State','create-complete')}</state>
        <addressFamily>{pl.get('AddressFamily','IPv4')}</addressFamily>
        <maxEntries>{pl.get('MaxEntries',10)}</maxEntries>
        <version>{pl.get('Version',1)}</version>
        <prefixListArn>{pl.get('PrefixListArn','')}</prefixListArn>
        <ownerId>{pl.get('OwnerId', get_account_id())}</ownerId>
        <tagSet/>
    </{tag}>"""


# ---------------------------------------------------------------------------
# VPN Gateways
# ---------------------------------------------------------------------------

def _create_vpn_gateway(p):
    gw_type = _p(p, "Type") or "ipsec.1"
    az = _p(p, "AvailabilityZone") or ""
    asn = _p(p, "AmazonSideAsn") or "64512"
    vgw_id = "vgw-" + "".join(random.choices(string.hexdigits[:16], k=17))
    tags = _parse_tags(p)
    _vpn_gateways[vgw_id] = {
        "VpnGatewayId": vgw_id, "Type": gw_type, "State": "available",
        "AvailabilityZone": az, "AmazonSideAsn": asn,
        "Attachments": [], "Tags": tags, "OwnerId": get_account_id(),
    }
    if tags:
        _tags[vgw_id] = tags
    return _xml(200, "CreateVpnGatewayResponse", _vgw_xml(_vpn_gateways[vgw_id], tag="vpnGateway"))


def _describe_vpn_gateways(p):
    filter_ids = _parse_member_list(p, "VpnGatewayId")
    filters = _parse_filters(p)
    items = ""
    for vgw in _vpn_gateways.values():
        if filter_ids and vgw["VpnGatewayId"] not in filter_ids:
            continue
        if filters.get("attachment.vpc-id"):
            vpc_ids = [a["VpcId"] for a in vgw.get("Attachments", [])]
            if not any(v in vpc_ids for v in filters["attachment.vpc-id"]):
                continue
        items += _vgw_xml(vgw)
    return _xml(200, "DescribeVpnGatewaysResponse", f"<vpnGatewaySet>{items}</vpnGatewaySet>")


def _attach_vpn_gateway(p):
    vgw_id = _p(p, "VpnGatewayId")
    vpc_id = _p(p, "VpcId")
    vgw = _vpn_gateways.get(vgw_id)
    if not vgw:
        return _error("InvalidVpnGatewayID.NotFound", f"VPN gateway '{vgw_id}' not found", 400)
    vgw["Attachments"] = [{"VpcId": vpc_id, "State": "attached"}]
    return _xml(200, "AttachVpnGatewayResponse",
                f"<attachment><vpcId>{vpc_id}</vpcId><state>attached</state></attachment>")


def _detach_vpn_gateway(p):
    vgw_id = _p(p, "VpnGatewayId")
    vgw = _vpn_gateways.get(vgw_id)
    if not vgw:
        return _error("InvalidVpnGatewayID.NotFound", f"VPN gateway '{vgw_id}' not found", 400)
    vgw["Attachments"] = []
    vgw["State"] = "detached"
    return _xml(200, "DetachVpnGatewayResponse", "<return>true</return>")


def _delete_vpn_gateway(p):
    vgw_id = _p(p, "VpnGatewayId")
    if vgw_id not in _vpn_gateways:
        return _error("InvalidVpnGatewayID.NotFound", f"VPN gateway '{vgw_id}' not found", 400)
    del _vpn_gateways[vgw_id]
    return _xml(200, "DeleteVpnGatewayResponse", "<return>true</return>")


def _vgw_xml(vgw, tag="item"):
    attachments = "".join(
        f"<item><vpcId>{a['VpcId']}</vpcId><state>{a['State']}</state></item>"
        for a in vgw.get("Attachments", [])
    )
    return f"""<{tag}>
        <vpnGatewayId>{vgw['VpnGatewayId']}</vpnGatewayId>
        <state>{vgw['State']}</state>
        <type>{vgw['Type']}</type>
        <availabilityZone>{vgw.get('AvailabilityZone','')}</availabilityZone>
        <amazonSideAsn>{vgw.get('AmazonSideAsn','64512')}</amazonSideAsn>
        <attachments>{attachments}</attachments>
        <tagSet/>
    </{tag}>"""


# ---------------------------------------------------------------------------
# VPN Gateway Route Propagation
# ---------------------------------------------------------------------------

def _enable_vgw_route_propagation(p):
    rtb_id = _p(p, "RouteTableId")
    vgw_id = _p(p, "GatewayId")
    rtb = _route_tables.get(rtb_id)
    if not rtb:
        return _error("InvalidRouteTableID.NotFound", f"Route table '{rtb_id}' not found", 400)
    propagating = rtb.setdefault("PropagatingVgws", [])
    if vgw_id not in propagating:
        propagating.append(vgw_id)
    return _xml(200, "EnableVgwRoutePropagationResponse", "<return>true</return>")


def _disable_vgw_route_propagation(p):
    rtb_id = _p(p, "RouteTableId")
    vgw_id = _p(p, "GatewayId")
    rtb = _route_tables.get(rtb_id)
    if not rtb:
        return _error("InvalidRouteTableID.NotFound", f"Route table '{rtb_id}' not found", 400)
    propagating = rtb.get("PropagatingVgws", [])
    if vgw_id in propagating:
        propagating.remove(vgw_id)
    return _xml(200, "DisableVgwRoutePropagationResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# Customer Gateways
# ---------------------------------------------------------------------------

def _create_customer_gateway(p):
    bgp_asn = _p(p, "BgpAsn") or "65000"
    ip_address = _p(p, "IpAddress") or _p(p, "PublicIp") or ""
    gw_type = _p(p, "Type") or "ipsec.1"
    cgw_id = "cgw-" + "".join(random.choices(string.hexdigits[:16], k=17))
    tags = _parse_tags(p)
    _customer_gateways[cgw_id] = {
        "CustomerGatewayId": cgw_id, "BgpAsn": bgp_asn, "IpAddress": ip_address,
        "Type": gw_type, "State": "available", "Tags": tags, "OwnerId": get_account_id(),
    }
    if tags:
        _tags[cgw_id] = tags
    return _xml(200, "CreateCustomerGatewayResponse", _cgw_xml(_customer_gateways[cgw_id], tag="customerGateway"))


def _describe_customer_gateways(p):
    filter_ids = _parse_member_list(p, "CustomerGatewayId")
    items = ""
    for cgw in _customer_gateways.values():
        if filter_ids and cgw["CustomerGatewayId"] not in filter_ids:
            continue
        items += _cgw_xml(cgw)
    return _xml(200, "DescribeCustomerGatewaysResponse", f"<customerGatewaySet>{items}</customerGatewaySet>")


def _delete_customer_gateway(p):
    cgw_id = _p(p, "CustomerGatewayId")
    if cgw_id not in _customer_gateways:
        return _error("InvalidCustomerGatewayID.NotFound", f"Customer gateway '{cgw_id}' not found", 400)
    del _customer_gateways[cgw_id]
    return _xml(200, "DeleteCustomerGatewayResponse", "<return>true</return>")


def _cgw_xml(cgw, tag="item"):
    return f"""<{tag}>
        <customerGatewayId>{cgw['CustomerGatewayId']}</customerGatewayId>
        <bgpAsn>{cgw['BgpAsn']}</bgpAsn>
        <ipAddress>{cgw['IpAddress']}</ipAddress>
        <type>{cgw['Type']}</type>
        <state>{cgw['State']}</state>
        <tagSet/>
    </{tag}>"""


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

def reset():
    _instances.clear()
    _security_groups.clear()
    _key_pairs.clear()
    _vpcs.clear()
    _subnets.clear()
    _internet_gateways.clear()
    _addresses.clear()
    _tags.clear()
    _route_tables.clear()
    _network_interfaces.clear()
    _vpc_endpoints.clear()
    _volumes.clear()
    _snapshots.clear()
    _nat_gateways.clear()
    _network_acls.clear()
    _flow_logs.clear()
    _vpc_peering.clear()
    _dhcp_options.clear()
    _egress_igws.clear()
    _prefix_lists.clear()
    _vpn_gateways.clear()
    _customer_gateways.clear()
    _launch_templates.clear()
    _init_defaults()


# ---------------------------------------------------------------------------
# Action map
# ---------------------------------------------------------------------------

def _describe_instance_attribute(p):
    instance_id = _p(p, "InstanceId")
    attribute = _p(p, "Attribute")
    inst = _instances.get(instance_id)
    if not inst:
        return _error("InvalidInstanceID.NotFound",
                      f"The instance ID '{instance_id}' does not exist", 400)

    if attribute == "instanceInitiatedShutdownBehavior":
        value_xml = "<instanceInitiatedShutdownBehavior><value>stop</value></instanceInitiatedShutdownBehavior>"
    elif attribute == "disableApiTermination":
        value_xml = "<disableApiTermination><value>false</value></disableApiTermination>"
    elif attribute == "instanceType":
        value_xml = f"<instanceType><value>{inst.get('InstanceType', 't2.micro')}</value></instanceType>"
    elif attribute == "userData":
        value_xml = "<userData/>"
    elif attribute == "rootDeviceName":
        value_xml = f"<rootDeviceName><value>{inst.get('RootDeviceName', '/dev/xvda')}</value></rootDeviceName>"
    elif attribute == "blockDeviceMapping":
        value_xml = "<blockDeviceMapping/>"
    elif attribute == "sourceDestCheck":
        value_xml = "<sourceDestCheck><value>true</value></sourceDestCheck>"
    elif attribute == "groupSet":
        sgs = "".join(
            f"<item><groupId>{sg['GroupId']}</groupId><groupName>{sg['GroupName']}</groupName></item>"
            for sg in inst.get("SecurityGroups", [])
        )
        value_xml = f"<groupSet>{sgs}</groupSet>"
    elif attribute == "ebsOptimized":
        value_xml = "<ebsOptimized><value>false</value></ebsOptimized>"
    elif attribute == "enaSupport":
        value_xml = "<enaSupport><value>true</value></enaSupport>"
    elif attribute == "sriovNetSupport":
        value_xml = "<sriovNetSupport><value>simple</value></sriovNetSupport>"
    else:
        value_xml = f"<{attribute}/>"

    return _xml(200, "DescribeInstanceAttributeResponse",
                f"<instanceId>{instance_id}</instanceId>{value_xml}")


def _describe_instance_types(p):
    # Collect requested types
    requested = _parse_member_list(p, "InstanceType")
    # Common types Terraform provider v6+ queries
    all_types = requested or [
        "t2.micro", "t2.small", "t2.medium", "t2.large",
        "t3.micro", "t3.small", "t3.medium", "t3.large",
        "m5.large", "m5.xlarge", "c5.large", "c5.xlarge",
    ]
    items = ""
    for itype in all_types:
        family = itype.split(".")[0]
        vcpus = 2 if "micro" in itype else 4 if "small" in itype else 8
        mem_mib = 1024 if "micro" in itype else 2048 if "small" in itype else 4096
        items += f"""<item>
            <instanceType>{itype}</instanceType>
            <currentGeneration>true</currentGeneration>
            <freeTierEligible>{'true' if itype == 't2.micro' else 'false'}</freeTierEligible>
            <supportedUsageClasses><item>on-demand</item><item>spot</item></supportedUsageClasses>
            <supportedRootDeviceTypes><item>ebs</item></supportedRootDeviceTypes>
            <supportedVirtualizationTypes><item>hvm</item></supportedVirtualizationTypes>
            <bareMetal>false</bareMetal>
            <hypervisor>xen</hypervisor>
            <processorInfo>
                <supportedArchitectures><item>x86_64</item></supportedArchitectures>
                <sustainedClockSpeedInGhz>2.5</sustainedClockSpeedInGhz>
            </processorInfo>
            <vCpuInfo>
                <defaultVCpus>{vcpus}</defaultVCpus>
                <defaultCores>{vcpus}</defaultCores>
                <defaultThreadsPerCore>1</defaultThreadsPerCore>
            </vCpuInfo>
            <memoryInfo><sizeInMiB>{mem_mib}</sizeInMiB></memoryInfo>
            <instanceStorageSupported>false</instanceStorageSupported>
            <ebsInfo>
                <ebsOptimizedSupport>unsupported</ebsOptimizedSupport>
                <encryptionSupport>supported</encryptionSupport>
                <ebsOptimizedInfo>
                    <baselineBandwidthInMbps>256</baselineBandwidthInMbps>
                    <baselineThroughputInMBps>32.0</baselineThroughputInMBps>
                    <baselineIops>2000</baselineIops>
                    <maximumBandwidthInMbps>256</maximumBandwidthInMbps>
                    <maximumThroughputInMBps>32.0</maximumThroughputInMBps>
                    <maximumIops>2000</maximumIops>
                </ebsOptimizedInfo>
                <nvmeSupport>unsupported</nvmeSupport>
            </ebsInfo>
            <networkInfo>
                <networkPerformance>Low to Moderate</networkPerformance>
                <maximumNetworkInterfaces>2</maximumNetworkInterfaces>
                <maximumNetworkCards>1</maximumNetworkCards>
                <defaultNetworkCardIndex>0</defaultNetworkCardIndex>
                <networkCards><item>
                    <networkCardIndex>0</networkCardIndex>
                    <networkPerformance>Low to Moderate</networkPerformance>
                    <maximumNetworkInterfaces>2</maximumNetworkInterfaces>
                    <baselineBandwidthInGbps>0.1</baselineBandwidthInGbps>
                    <peakBandwidthInGbps>0.5</peakBandwidthInGbps>
                </item></networkCards>
                <ipv4AddressesPerInterface>2</ipv4AddressesPerInterface>
                <ipv6AddressesPerInterface>2</ipv6AddressesPerInterface>
                <ipv6Supported>true</ipv6Supported>
                <enaSupport>required</enaSupport>
                <efaSupported>false</efaSupported>
            </networkInfo>
            <placementGroupInfo>
                <supportedStrategies><item>partition</item><item>spread</item></supportedStrategies>
            </placementGroupInfo>
            <hibernationSupported>false</hibernationSupported>
            <burstablePerformanceSupported>{'true' if family in ('t2','t3','t4g') else 'false'}</burstablePerformanceSupported>
            <dedicatedHostsSupported>false</dedicatedHostsSupported>
            <autoRecoverySupported>true</autoRecoverySupported>
        </item>"""

    return _xml(200, "DescribeInstanceTypesResponse",
                f"<instanceTypeSet>{items}</instanceTypeSet>")


def _describe_instance_credit_specifications(p):
    instance_ids = _parse_member_list(p, "InstanceId")
    items = "".join(
        f"<item><instanceId>{iid}</instanceId><cpuCredits>standard</cpuCredits></item>"
        for iid in (instance_ids or list(_instances.keys()))
    )
    return _xml(200, "DescribeInstanceCreditSpecificationsResponse",
                f"<instanceCreditSpecificationSet>{items}</instanceCreditSpecificationSet>")


def _describe_instance_maintenance_options(p):
    instance_ids = _parse_member_list(p, "InstanceId")
    items = "".join(
        f"<item><instanceId>{iid}</instanceId><autoRecovery>default</autoRecovery></item>"
        for iid in (instance_ids or list(_instances.keys()))
    )
    return _xml(200, "DescribeInstanceMaintenanceOptionsResponse",
                f"<instanceMaintenanceOptionSet>{items}</instanceMaintenanceOptionSet>")


def _describe_instance_auto_recovery_attribute(p):
    instance_ids = _parse_member_list(p, "InstanceId")
    items = "".join(
        f"<item><instanceId>{iid}</instanceId><autoRecovery><value>default</value></autoRecovery></item>"
        for iid in (instance_ids or list(_instances.keys()))
    )
    return _xml(200, "DescribeInstanceAutoRecoveryAttributeResponse",
                f"<instanceAutoRecoveryAttributeSet>{items}</instanceAutoRecoveryAttributeSet>")


def _modify_instance_maintenance_options(p):
    instance_id = _p(p, "InstanceId")
    return _xml(200, "ModifyInstanceMaintenanceOptionsResponse",
                f"<instanceId>{instance_id}</instanceId><autoRecovery>default</autoRecovery>")


def _describe_instance_topology(p):
    return _xml(200, "DescribeInstanceTopologyResponse", "<instanceSet/>")


def _describe_spot_instance_requests(p):
    return _xml(200, "DescribeSpotInstanceRequestsResponse", "<spotInstanceRequestSet/>")


def _describe_capacity_reservations(p):
    return _xml(200, "DescribeCapacityReservationsResponse", "<capacityReservationSet/>")


def _describe_addresses_attribute(p):
    alloc_id = _p(p, "AllocationId") or _parse_member_list(p, "AllocationId")
    items = ""
    if isinstance(alloc_id, list):
        for aid in alloc_id:
            items += f"<item><allocationId>{aid}</allocationId><ptrRecord></ptrRecord></item>"
    elif alloc_id:
        items = f"<item><allocationId>{alloc_id}</allocationId><ptrRecord></ptrRecord></item>"
    return _xml(200, "DescribeAddressesAttributeResponse", f"<addressSet>{items}</addressSet>")


def _describe_security_group_rules(p):
    sg_ids = _parse_member_list(p, "SecurityGroupId") or []
    filters = _parse_filters(p)
    sg_id_filter = filters.get("group-id", [])
    if sg_id_filter:
        sg_ids = sg_id_filter

    items = ""
    for sg_id in sg_ids:
        sg = _security_groups.get(sg_id)
        if not sg:
            continue
        for i, rule in enumerate(sg.get("IpPermissions", [])):
            rule_id = f"sgr-{sg_id[3:]}-ingress-{i}"
            for cidr in rule.get("IpRanges", []):
                items += f"""<item>
                    <securityGroupRuleId>{rule_id}</securityGroupRuleId>
                    <groupId>{sg_id}</groupId>
                    <groupOwnerId>{get_account_id()}</groupOwnerId>
                    <isEgress>false</isEgress>
                    <ipProtocol>{rule.get('IpProtocol', '-1')}</ipProtocol>
                    <fromPort>{rule.get('FromPort', -1)}</fromPort>
                    <toPort>{rule.get('ToPort', -1)}</toPort>
                    <cidrIpv4>{cidr.get('CidrIp', '')}</cidrIpv4>
                </item>"""
        for i, rule in enumerate(sg.get("IpPermissionsEgress", [])):
            rule_id = f"sgr-{sg_id[3:]}-egress-{i}"
            for cidr in rule.get("IpRanges", []):
                items += f"""<item>
                    <securityGroupRuleId>{rule_id}</securityGroupRuleId>
                    <groupId>{sg_id}</groupId>
                    <groupOwnerId>{get_account_id()}</groupOwnerId>
                    <isEgress>true</isEgress>
                    <ipProtocol>{rule.get('IpProtocol', '-1')}</ipProtocol>
                    <fromPort>{rule.get('FromPort', -1)}</fromPort>
                    <toPort>{rule.get('ToPort', -1)}</toPort>
                    <cidrIpv4>{cidr.get('CidrIp', '')}</cidrIpv4>
                </item>"""
    return _xml(200, "DescribeSecurityGroupRulesResponse", f"<securityGroupRuleSet>{items}</securityGroupRuleSet>")


# ---------------------------------------------------------------------------
# Launch Templates
# ---------------------------------------------------------------------------

def _new_lt_id():
    return "lt-" + "".join(random.choices(string.hexdigits[:16], k=17))


def _parse_lt_data(params, prefix="LaunchTemplateData"):
    """Extract LaunchTemplateData from EC2 Query API params."""
    data = {}
    img = _p(params, f"{prefix}.ImageId")
    if img:
        data["ImageId"] = img
    itype = _p(params, f"{prefix}.InstanceType")
    if itype:
        data["InstanceType"] = itype
    key = _p(params, f"{prefix}.KeyName")
    if key:
        data["KeyName"] = key
    ud = _p(params, f"{prefix}.UserData")
    if ud:
        data["UserData"] = ud
    # Security group IDs
    sg_ids = []
    i = 1
    while True:
        sg = _p(params, f"{prefix}.SecurityGroupId.{i}")
        if not sg:
            break
        sg_ids.append(sg)
        i += 1
    if sg_ids:
        data["SecurityGroupIds"] = sg_ids
    # Security groups by name
    sg_names = []
    i = 1
    while True:
        sg = _p(params, f"{prefix}.SecurityGroup.{i}")
        if not sg:
            break
        sg_names.append(sg)
        i += 1
    if sg_names:
        data["SecurityGroups"] = sg_names
    # Block device mappings
    bdms = []
    i = 1
    while True:
        dev = _p(params, f"{prefix}.BlockDeviceMapping.{i}.DeviceName")
        if not dev:
            break
        bdm = {"DeviceName": dev}
        ebs = {}
        vol_size = _p(params, f"{prefix}.BlockDeviceMapping.{i}.Ebs.VolumeSize")
        if vol_size:
            ebs["VolumeSize"] = int(vol_size)
        vol_type = _p(params, f"{prefix}.BlockDeviceMapping.{i}.Ebs.VolumeType")
        if vol_type:
            ebs["VolumeType"] = vol_type
        encrypted = _p(params, f"{prefix}.BlockDeviceMapping.{i}.Ebs.Encrypted")
        if encrypted:
            ebs["Encrypted"] = encrypted.lower() == "true"
        delete_on = _p(params, f"{prefix}.BlockDeviceMapping.{i}.Ebs.DeleteOnTermination")
        if delete_on:
            ebs["DeleteOnTermination"] = delete_on.lower() == "true"
        snap = _p(params, f"{prefix}.BlockDeviceMapping.{i}.Ebs.SnapshotId")
        if snap:
            ebs["SnapshotId"] = snap
        iops = _p(params, f"{prefix}.BlockDeviceMapping.{i}.Ebs.Iops")
        if iops:
            ebs["Iops"] = int(iops)
        throughput = _p(params, f"{prefix}.BlockDeviceMapping.{i}.Ebs.Throughput")
        if throughput:
            ebs["Throughput"] = int(throughput)
        if ebs:
            bdm["Ebs"] = ebs
        bdms.append(bdm)
        i += 1
    if bdms:
        data["BlockDeviceMappings"] = bdms
    # Network interfaces
    nis = []
    i = 1
    while True:
        dev_idx = _p(params, f"{prefix}.NetworkInterface.{i}.DeviceIndex")
        if not dev_idx and not _p(params, f"{prefix}.NetworkInterface.{i}.SubnetId"):
            break
        ni = {}
        if dev_idx:
            ni["DeviceIndex"] = int(dev_idx)
        sub = _p(params, f"{prefix}.NetworkInterface.{i}.SubnetId")
        if sub:
            ni["SubnetId"] = sub
        assoc_pub = _p(params, f"{prefix}.NetworkInterface.{i}.AssociatePublicIpAddress")
        if assoc_pub:
            ni["AssociatePublicIpAddress"] = assoc_pub.lower() == "true"
        desc = _p(params, f"{prefix}.NetworkInterface.{i}.Description")
        if desc:
            ni["Description"] = desc
        groups = []
        j = 1
        while True:
            g = _p(params, f"{prefix}.NetworkInterface.{i}.Groups.SecurityGroupId.{j}")
            if not g:
                g = _p(params, f"{prefix}.NetworkInterface.{i}.SecurityGroupId.{j}")
            if not g:
                break
            groups.append(g)
            j += 1
        if groups:
            ni["Groups"] = groups
        nis.append(ni)
        i += 1
    if nis:
        data["NetworkInterfaces"] = nis
    # IamInstanceProfile
    iam_arn = _p(params, f"{prefix}.IamInstanceProfile.Arn")
    iam_name = _p(params, f"{prefix}.IamInstanceProfile.Name")
    if iam_arn or iam_name:
        iip = {}
        if iam_arn:
            iip["Arn"] = iam_arn
        if iam_name:
            iip["Name"] = iam_name
        data["IamInstanceProfile"] = iip
    # TagSpecifications
    tag_specs = []
    i = 1
    while True:
        rtype = _p(params, f"{prefix}.TagSpecification.{i}.ResourceType")
        if not rtype:
            break
        ts = {"ResourceType": rtype, "Tags": []}
        j = 1
        while True:
            tk = _p(params, f"{prefix}.TagSpecification.{i}.Tag.{j}.Key")
            if not tk:
                break
            ts["Tags"].append({"Key": tk, "Value": _p(params, f"{prefix}.TagSpecification.{i}.Tag.{j}.Value", "")})
            j += 1
        tag_specs.append(ts)
        i += 1
    if tag_specs:
        data["TagSpecifications"] = tag_specs
    # Monitoring
    monitoring = _p(params, f"{prefix}.Monitoring.Enabled")
    if monitoring:
        data["Monitoring"] = {"Enabled": monitoring.lower() == "true"}
    # DisableApiTermination
    disable_api = _p(params, f"{prefix}.DisableApiTermination")
    if disable_api:
        data["DisableApiTermination"] = disable_api.lower() == "true"
    # EbsOptimized
    ebs_opt = _p(params, f"{prefix}.EbsOptimized")
    if ebs_opt:
        data["EbsOptimized"] = ebs_opt.lower() == "true"
    return data


def _lt_data_xml(data):
    """Render LaunchTemplateData dict as XML response fragment."""
    xml = ""
    if data.get("ImageId"):
        xml += f"<imageId>{_esc(data['ImageId'])}</imageId>"
    if data.get("InstanceType"):
        xml += f"<instanceType>{_esc(data['InstanceType'])}</instanceType>"
    if data.get("KeyName"):
        xml += f"<keyName>{_esc(data['KeyName'])}</keyName>"
    if data.get("UserData"):
        xml += f"<userData>{_esc(data['UserData'])}</userData>"
    if data.get("EbsOptimized") is not None:
        xml += f"<ebsOptimized>{str(data['EbsOptimized']).lower()}</ebsOptimized>"
    if data.get("DisableApiTermination") is not None:
        xml += f"<disableApiTermination>{str(data['DisableApiTermination']).lower()}</disableApiTermination>"
    if data.get("SecurityGroupIds"):
        inner = "".join(f"<item>{_esc(s)}</item>" for s in data["SecurityGroupIds"])
        xml += f"<securityGroupIdSet>{inner}</securityGroupIdSet>"
    if data.get("SecurityGroups"):
        inner = "".join(f"<item>{_esc(s)}</item>" for s in data["SecurityGroups"])
        xml += f"<securityGroupSet>{inner}</securityGroupSet>"
    if data.get("BlockDeviceMappings"):
        inner = ""
        for bdm in data["BlockDeviceMappings"]:
            inner += f"<item><deviceName>{_esc(bdm['DeviceName'])}</deviceName>"
            if "Ebs" in bdm:
                ebs = bdm["Ebs"]
                inner += "<ebs>"
                if "VolumeSize" in ebs:
                    inner += f"<volumeSize>{ebs['VolumeSize']}</volumeSize>"
                if "VolumeType" in ebs:
                    inner += f"<volumeType>{_esc(ebs['VolumeType'])}</volumeType>"
                if "Encrypted" in ebs:
                    inner += f"<encrypted>{str(ebs['Encrypted']).lower()}</encrypted>"
                if "DeleteOnTermination" in ebs:
                    inner += f"<deleteOnTermination>{str(ebs['DeleteOnTermination']).lower()}</deleteOnTermination>"
                if "SnapshotId" in ebs:
                    inner += f"<snapshotId>{_esc(ebs['SnapshotId'])}</snapshotId>"
                if "Iops" in ebs:
                    inner += f"<iops>{ebs['Iops']}</iops>"
                if "Throughput" in ebs:
                    inner += f"<throughput>{ebs['Throughput']}</throughput>"
                inner += "</ebs>"
            inner += "</item>"
        xml += f"<blockDeviceMappingSet>{inner}</blockDeviceMappingSet>"
    if data.get("NetworkInterfaces"):
        inner = ""
        for ni in data["NetworkInterfaces"]:
            inner += "<item>"
            if "DeviceIndex" in ni:
                inner += f"<deviceIndex>{ni['DeviceIndex']}</deviceIndex>"
            if "SubnetId" in ni:
                inner += f"<subnetId>{_esc(ni['SubnetId'])}</subnetId>"
            if "AssociatePublicIpAddress" in ni:
                inner += f"<associatePublicIpAddress>{str(ni['AssociatePublicIpAddress']).lower()}</associatePublicIpAddress>"
            if "Description" in ni:
                inner += f"<description>{_esc(ni['Description'])}</description>"
            if "Groups" in ni:
                gi = "".join(f"<item>{_esc(g)}</item>" for g in ni["Groups"])
                inner += f"<groupSet>{gi}</groupSet>"
            inner += "</item>"
        xml += f"<networkInterfaceSet>{inner}</networkInterfaceSet>"
    if data.get("IamInstanceProfile"):
        iip = data["IamInstanceProfile"]
        xml += "<iamInstanceProfile>"
        if "Arn" in iip:
            xml += f"<arn>{_esc(iip['Arn'])}</arn>"
        if "Name" in iip:
            xml += f"<name>{_esc(iip['Name'])}</name>"
        xml += "</iamInstanceProfile>"
    if data.get("TagSpecifications"):
        inner = ""
        for ts in data["TagSpecifications"]:
            inner += f"<item><resourceType>{_esc(ts['ResourceType'])}</resourceType><tagSet>"
            for t in ts.get("Tags", []):
                inner += f"<item><key>{_esc(t['Key'])}</key><value>{_esc(t.get('Value', ''))}</value></item>"
            inner += "</tagSet></item>"
        xml += f"<tagSpecificationSet>{inner}</tagSpecificationSet>"
    if data.get("Monitoring"):
        xml += f"<monitoring><enabled>{str(data['Monitoring'].get('Enabled', False)).lower()}</enabled></monitoring>"
    return xml


def _lt_version_xml(ver):
    """Render a single launch template version as XML."""
    xml = f"""<item>
        <launchTemplateId>{_esc(ver['LaunchTemplateId'])}</launchTemplateId>
        <launchTemplateName>{_esc(ver['LaunchTemplateName'])}</launchTemplateName>
        <versionNumber>{ver['VersionNumber']}</versionNumber>
        <versionDescription>{_esc(ver.get('VersionDescription', ''))}</versionDescription>
        <defaultVersion>{str(ver.get('DefaultVersion', False)).lower()}</defaultVersion>
        <createTime>{ver['CreateTime']}</createTime>
        <createdBy>arn:aws:iam::{get_account_id()}:root</createdBy>
        <launchTemplateData>{_lt_data_xml(ver.get('LaunchTemplateData', {}))}</launchTemplateData>
    </item>"""
    return xml


def _create_launch_template(p):
    name = _p(p, "LaunchTemplateName")
    if not name:
        return _error("MissingParameter", "LaunchTemplateName is required", 400)
    # Check uniqueness
    for lt in _launch_templates.values():
        if lt["LaunchTemplateName"] == name:
            return _error("InvalidLaunchTemplateName.AlreadyExistsException",
                          f"Launch template name already in use: {name}", 400)
    lt_id = _new_lt_id()
    lt_data = _parse_lt_data(p)
    ver_desc = _p(p, "VersionDescription")
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    version = {
        "LaunchTemplateId": lt_id,
        "LaunchTemplateName": name,
        "VersionNumber": 1,
        "VersionDescription": ver_desc,
        "DefaultVersion": True,
        "CreateTime": now,
        "LaunchTemplateData": lt_data,
    }
    lt = {
        "LaunchTemplateId": lt_id,
        "LaunchTemplateName": name,
        "CreateTime": now,
        "DefaultVersionNumber": 1,
        "LatestVersionNumber": 1,
        "Versions": [version],
    }
    # Parse tag specifications for the template itself
    tags = []
    i = 1
    while True:
        rtype = _p(p, f"TagSpecification.{i}.ResourceType")
        if not rtype:
            break
        if rtype == "launch-template":
            j = 1
            while True:
                tk = _p(p, f"TagSpecification.{i}.Tag.{j}.Key")
                if not tk:
                    break
                tags.append({"Key": tk, "Value": _p(p, f"TagSpecification.{i}.Tag.{j}.Value", "")})
                j += 1
        i += 1
    if tags:
        lt["Tags"] = tags
        _tags[lt_id] = tags
    _launch_templates[lt_id] = lt
    tags_xml = ""
    for t in tags:
        tags_xml += f"<item><key>{_esc(t['Key'])}</key><value>{_esc(t.get('Value', ''))}</value></item>"
    return _xml(200, "CreateLaunchTemplateResponse", f"""<launchTemplate>
        <launchTemplateId>{lt_id}</launchTemplateId>
        <launchTemplateName>{_esc(name)}</launchTemplateName>
        <createTime>{now}</createTime>
        <createdBy>arn:aws:iam::{get_account_id()}:root</createdBy>
        <defaultVersionNumber>1</defaultVersionNumber>
        <latestVersionNumber>1</latestVersionNumber>
        <tags>{tags_xml}</tags>
    </launchTemplate>""")


def _create_launch_template_version(p):
    lt_id = _p(p, "LaunchTemplateId")
    lt_name = _p(p, "LaunchTemplateName")
    lt = None
    if lt_id:
        lt = _launch_templates.get(lt_id)
    elif lt_name:
        for t in _launch_templates.values():
            if t["LaunchTemplateName"] == lt_name:
                lt = t
                break
    if not lt:
        return _error("InvalidLaunchTemplateId.NotFoundException",
                      "The specified launch template does not exist", 400)
    lt_data = _parse_lt_data(p)
    # Merge with source version if SourceVersion specified
    source_ver = _p(p, "SourceVersion")
    if source_ver:
        src = None
        for v in lt["Versions"]:
            if str(v["VersionNumber"]) == source_ver:
                src = v
                break
        if src:
            merged = copy.deepcopy(src.get("LaunchTemplateData", {}))
            merged.update(lt_data)
            lt_data = merged
    ver_num = lt["LatestVersionNumber"] + 1
    ver_desc = _p(p, "VersionDescription")
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    version = {
        "LaunchTemplateId": lt["LaunchTemplateId"],
        "LaunchTemplateName": lt["LaunchTemplateName"],
        "VersionNumber": ver_num,
        "VersionDescription": ver_desc,
        "DefaultVersion": ver_num == lt["DefaultVersionNumber"],
        "CreateTime": now,
        "LaunchTemplateData": lt_data,
    }
    lt["Versions"].append(version)
    lt["LatestVersionNumber"] = ver_num
    return _xml(200, "CreateLaunchTemplateVersionResponse",
                f"<launchTemplateVersion>{_lt_version_xml(version)}</launchTemplateVersion>")


def _describe_launch_templates(p):
    lt_ids = _parse_member_list(p, "LaunchTemplateId")
    lt_names = _parse_member_list(p, "LaunchTemplateName")
    filters = _parse_filters(p)
    items = ""
    for lt in _launch_templates.values():
        if lt_ids and lt["LaunchTemplateId"] not in lt_ids:
            continue
        if lt_names and lt["LaunchTemplateName"] not in lt_names:
            continue
        if filters:
            if "launch-template-name" in filters:
                if lt["LaunchTemplateName"] not in filters["launch-template-name"]:
                    continue
        tags_xml = ""
        for t in lt.get("Tags", _tags.get(lt["LaunchTemplateId"], [])):
            tags_xml += f"<item><key>{_esc(t['Key'])}</key><value>{_esc(t.get('Value', ''))}</value></item>"
        items += f"""<item>
            <launchTemplateId>{lt['LaunchTemplateId']}</launchTemplateId>
            <launchTemplateName>{_esc(lt['LaunchTemplateName'])}</launchTemplateName>
            <createTime>{lt['CreateTime']}</createTime>
            <createdBy>arn:aws:iam::{get_account_id()}:root</createdBy>
            <defaultVersionNumber>{lt['DefaultVersionNumber']}</defaultVersionNumber>
            <latestVersionNumber>{lt['LatestVersionNumber']}</latestVersionNumber>
            <tags>{tags_xml}</tags>
        </item>"""
    return _xml(200, "DescribeLaunchTemplatesResponse",
                f"<launchTemplates>{items}</launchTemplates>")


def _describe_launch_template_versions(p):
    lt_id = _p(p, "LaunchTemplateId")
    lt_name = _p(p, "LaunchTemplateName")
    lt = None
    if lt_id:
        lt = _launch_templates.get(lt_id)
    elif lt_name:
        for t in _launch_templates.values():
            if t["LaunchTemplateName"] == lt_name:
                lt = t
                break
    if not lt:
        return _error("InvalidLaunchTemplateId.NotFoundException",
                      "The specified launch template does not exist", 400)
    # Filter by version numbers
    req_versions = _parse_member_list(p, "LaunchTemplateVersion")
    versions = lt["Versions"]
    if req_versions:
        filtered = []
        for rv in req_versions:
            if rv == "$Latest":
                for v in versions:
                    if v["VersionNumber"] == lt["LatestVersionNumber"]:
                        filtered.append(v)
            elif rv == "$Default":
                for v in versions:
                    if v["VersionNumber"] == lt["DefaultVersionNumber"]:
                        filtered.append(v)
            else:
                for v in versions:
                    if str(v["VersionNumber"]) == rv:
                        filtered.append(v)
        versions = filtered
    items = "".join(_lt_version_xml(v) for v in versions)
    return _xml(200, "DescribeLaunchTemplateVersionsResponse",
                f"<launchTemplateVersionSet>{items}</launchTemplateVersionSet>")


def _modify_launch_template(p):
    lt_id = _p(p, "LaunchTemplateId")
    lt_name = _p(p, "LaunchTemplateName")
    lt = None
    if lt_id:
        lt = _launch_templates.get(lt_id)
    elif lt_name:
        for t in _launch_templates.values():
            if t["LaunchTemplateName"] == lt_name:
                lt = t
                break
    if not lt:
        return _error("InvalidLaunchTemplateId.NotFoundException",
                      "The specified launch template does not exist", 400)
    default_ver = _p(p, "SetDefaultVersion")
    if default_ver:
        ver_num = int(default_ver)
        found = any(v["VersionNumber"] == ver_num for v in lt["Versions"])
        if not found:
            return _error("InvalidLaunchTemplateId.VersionNotFound",
                          f"Version {ver_num} does not exist", 400)
        lt["DefaultVersionNumber"] = ver_num
        for v in lt["Versions"]:
            v["DefaultVersion"] = v["VersionNumber"] == ver_num
    return _xml(200, "ModifyLaunchTemplateResponse", f"""<launchTemplate>
        <launchTemplateId>{lt['LaunchTemplateId']}</launchTemplateId>
        <launchTemplateName>{_esc(lt['LaunchTemplateName'])}</launchTemplateName>
        <createTime>{lt['CreateTime']}</createTime>
        <createdBy>arn:aws:iam::{get_account_id()}:root</createdBy>
        <defaultVersionNumber>{lt['DefaultVersionNumber']}</defaultVersionNumber>
        <latestVersionNumber>{lt['LatestVersionNumber']}</latestVersionNumber>
    </launchTemplate>""")


def _delete_launch_template(p):
    lt_id = _p(p, "LaunchTemplateId")
    lt_name = _p(p, "LaunchTemplateName")
    lt = None
    if lt_id:
        lt = _launch_templates.get(lt_id)
    elif lt_name:
        for t in _launch_templates.values():
            if t["LaunchTemplateName"] == lt_name:
                lt = t
                lt_id = lt["LaunchTemplateId"]
                break
    if not lt:
        return _error("InvalidLaunchTemplateId.NotFoundException",
                      "The specified launch template does not exist", 400)
    _launch_templates.pop(lt_id, None)
    _tags.pop(lt_id, None)
    return _xml(200, "DeleteLaunchTemplateResponse", f"""<launchTemplate>
        <launchTemplateId>{lt['LaunchTemplateId']}</launchTemplateId>
        <launchTemplateName>{_esc(lt['LaunchTemplateName'])}</launchTemplateName>
        <createTime>{lt['CreateTime']}</createTime>
        <defaultVersionNumber>{lt['DefaultVersionNumber']}</defaultVersionNumber>
        <latestVersionNumber>{lt['LatestVersionNumber']}</latestVersionNumber>
    </launchTemplate>""")


_ACTION_MAP = {
    "RunInstances": _run_instances,
    "DescribeInstances": _describe_instances,
    "DescribeInstanceStatus": _describe_instance_status,
    "DescribeInstanceAttribute": _describe_instance_attribute,
    "DescribeInstanceCreditSpecifications": _describe_instance_credit_specifications,
    "DescribeInstanceMaintenanceOptions": _describe_instance_maintenance_options,
    "DescribeInstanceAutoRecoveryAttribute": _describe_instance_auto_recovery_attribute,
    "ModifyInstanceMaintenanceOptions": _modify_instance_maintenance_options,
    "DescribeInstanceTopology": _describe_instance_topology,
    "DescribeSpotInstanceRequests": _describe_spot_instance_requests,
    "DescribeCapacityReservations": _describe_capacity_reservations,
    "DescribeInstanceTypes": _describe_instance_types,
    "TerminateInstances": _terminate_instances,
    "StopInstances": _stop_instances,
    "StartInstances": _start_instances,
    "RebootInstances": _reboot_instances,
    "DescribeImages": _describe_images,
    "CreateSecurityGroup": _create_security_group,
    "DeleteSecurityGroup": _delete_security_group,
    "DescribeSecurityGroups": _describe_security_groups,
    "AuthorizeSecurityGroupIngress": _authorize_sg_ingress,
    "RevokeSecurityGroupIngress": _revoke_sg_ingress,
    "AuthorizeSecurityGroupEgress": _authorize_sg_egress,
    "RevokeSecurityGroupEgress": _revoke_sg_egress,
    "CreateKeyPair": _create_key_pair,
    "DeleteKeyPair": _delete_key_pair,
    "DescribeKeyPairs": _describe_key_pairs,
    "ImportKeyPair": _import_key_pair,
    "DescribeVpcs": _describe_vpcs,
    "CreateVpc": _create_vpc,
    "CreateDefaultVpc": _create_default_vpc,
    "DeleteVpc": _delete_vpc,
    "DescribeSubnets": _describe_subnets,
    "CreateSubnet": _create_subnet,
    "DeleteSubnet": _delete_subnet,
    "CreateInternetGateway": _create_internet_gateway,
    "DeleteInternetGateway": _delete_internet_gateway,
    "DescribeInternetGateways": _describe_internet_gateways,
    "AttachInternetGateway": _attach_internet_gateway,
    "DetachInternetGateway": _detach_internet_gateway,
    "DescribeAvailabilityZones": _describe_availability_zones,
    "AllocateAddress": _allocate_address,
    "ReleaseAddress": _release_address,
    "AssociateAddress": _associate_address,
    "DisassociateAddress": _disassociate_address,
    "DescribeAddresses": _describe_addresses,
    "CreateTags": _create_tags,
    "DeleteTags": _delete_tags,
    "DescribeTags": _describe_tags,
    "ModifyVpcAttribute": _modify_vpc_attribute,
    "DescribeVpcAttribute": _describe_vpc_attribute,
    "DescribeVpcClassicLink": _describe_vpc_classic_link,
    "DescribeVpcClassicLinkDnsSupport": _describe_vpc_classic_link_dns_support,
    "DescribeAddressesAttribute": _describe_addresses_attribute,
    "DescribeSecurityGroupRules": _describe_security_group_rules,
    "ModifySubnetAttribute": _modify_subnet_attribute,
    "CreateRouteTable": _create_route_table,
    "DeleteRouteTable": _delete_route_table,
    "DescribeRouteTables": _describe_route_tables,
    "AssociateRouteTable": _associate_route_table,
    "DisassociateRouteTable": _disassociate_route_table,
    "CreateRoute": _create_route,
    "ReplaceRoute": _replace_route,
    "DeleteRoute": _delete_route,
    "CreateNetworkInterface": _create_network_interface,
    "DeleteNetworkInterface": _delete_network_interface,
    "DescribeNetworkInterfaces": _describe_network_interfaces,
    "AttachNetworkInterface": _attach_network_interface,
    "DetachNetworkInterface": _detach_network_interface,
    "CreateVpcEndpoint": _create_vpc_endpoint,
    "DeleteVpcEndpoints": _delete_vpc_endpoints,
    "DescribeVpcEndpoints": _describe_vpc_endpoints,
    "ReplaceRouteTableAssociation": _replace_route_table_association,
    "ModifyVpcEndpoint": _modify_vpc_endpoint,
    "DescribePrefixLists": _describe_prefix_lists,
    "CreateManagedPrefixList": _create_managed_prefix_list,
    "DescribeManagedPrefixLists": _describe_managed_prefix_lists,
    "GetManagedPrefixListEntries": _get_managed_prefix_list_entries,
    "ModifyManagedPrefixList": _modify_managed_prefix_list,
    "DeleteManagedPrefixList": _delete_managed_prefix_list,
    "CreateVpnGateway": _create_vpn_gateway,
    "DescribeVpnGateways": _describe_vpn_gateways,
    "AttachVpnGateway": _attach_vpn_gateway,
    "DetachVpnGateway": _detach_vpn_gateway,
    "DeleteVpnGateway": _delete_vpn_gateway,
    "EnableVgwRoutePropagation": _enable_vgw_route_propagation,
    "DisableVgwRoutePropagation": _disable_vgw_route_propagation,
    "CreateCustomerGateway": _create_customer_gateway,
    "DescribeCustomerGateways": _describe_customer_gateways,
    "DeleteCustomerGateway": _delete_customer_gateway,
    # EBS Volumes
    "CreateVolume": _create_volume,
    "DeleteVolume": _delete_volume,
    "DescribeVolumes": _describe_volumes,
    "DescribeVolumeStatus": _describe_volume_status,
    "AttachVolume": _attach_volume,
    "DetachVolume": _detach_volume,
    "ModifyVolume": _modify_volume,
    "DescribeVolumesModifications": _describe_volumes_modifications,
    "EnableVolumeIO": _enable_volume_io,
    "ModifyVolumeAttribute": _modify_volume_attribute,
    "DescribeVolumeAttribute": _describe_volume_attribute,
    # EBS Snapshots
    "CreateSnapshot": _create_snapshot,
    "DeleteSnapshot": _delete_snapshot,
    "DescribeSnapshots": _describe_snapshots,
    "CopySnapshot": _copy_snapshot,
    "ModifySnapshotAttribute": _modify_snapshot_attribute,
    "DescribeSnapshotAttribute": _describe_snapshot_attribute,
    # NAT Gateways
    "CreateNatGateway": _create_nat_gateway,
    "DescribeNatGateways": _describe_nat_gateways,
    "DeleteNatGateway": _delete_nat_gateway,
    # Network ACLs
    "CreateNetworkAcl": _create_network_acl,
    "DescribeNetworkAcls": _describe_network_acls,
    "DeleteNetworkAcl": _delete_network_acl,
    "CreateNetworkAclEntry": _create_network_acl_entry,
    "DeleteNetworkAclEntry": _delete_network_acl_entry,
    "ReplaceNetworkAclEntry": _replace_network_acl_entry,
    "ReplaceNetworkAclAssociation": _replace_network_acl_association,
    # Flow Logs
    "CreateFlowLogs": _create_flow_logs,
    "DescribeFlowLogs": _describe_flow_logs,
    "DeleteFlowLogs": _delete_flow_logs,
    # VPC Peering
    "CreateVpcPeeringConnection": _create_vpc_peering_connection,
    "AcceptVpcPeeringConnection": _accept_vpc_peering_connection,
    "DescribeVpcPeeringConnections": _describe_vpc_peering_connections,
    "DeleteVpcPeeringConnection": _delete_vpc_peering_connection,
    # DHCP Options
    "CreateDhcpOptions": _create_dhcp_options,
    "AssociateDhcpOptions": _associate_dhcp_options,
    "DescribeDhcpOptions": _describe_dhcp_options,
    "DeleteDhcpOptions": _delete_dhcp_options,
    # Egress-Only Internet Gateways
    "CreateEgressOnlyInternetGateway": _create_egress_only_igw,
    "DescribeEgressOnlyInternetGateways": _describe_egress_only_igws,
    "DeleteEgressOnlyInternetGateway": _delete_egress_only_igw,
    # Launch Templates
    "CreateLaunchTemplate": _create_launch_template,
    "CreateLaunchTemplateVersion": _create_launch_template_version,
    "DescribeLaunchTemplates": _describe_launch_templates,
    "DescribeLaunchTemplateVersions": _describe_launch_template_versions,
    "ModifyLaunchTemplate": _modify_launch_template,
    "DeleteLaunchTemplate": _delete_launch_template,
}
