"""
EC2 Service Emulator.
Query API (Action=...) — instances exist in memory only, no real VMs launched.

Supports:
  Instances:       RunInstances, TerminateInstances, DescribeInstances,
                   StartInstances, StopInstances, RebootInstances
  Images:          DescribeImages (stub — returns common AMI IDs)
  Security Groups: CreateSecurityGroup, DeleteSecurityGroup, DescribeSecurityGroups,
                   AuthorizeSecurityGroupIngress, RevokeSecurityGroupIngress,
                   AuthorizeSecurityGroupEgress, RevokeSecurityGroupEgress
  Key Pairs:       CreateKeyPair, DeleteKeyPair, DescribeKeyPairs, ImportKeyPair
  VPC / Subnets:   DescribeVpcs, DescribeSubnets, DescribeAvailabilityZones
                   CreateVpc, DeleteVpc, CreateSubnet, DeleteSubnet
                   CreateInternetGateway, DeleteInternetGateway, DescribeInternetGateways,
                   AttachInternetGateway, DetachInternetGateway
  Elastic IPs:     AllocateAddress, ReleaseAddress, AssociateAddress, DisassociateAddress,
                   DescribeAddresses
  Tags:            CreateTags, DeleteTags, DescribeTags
  VPC attributes:  ModifyVpcAttribute, ModifySubnetAttribute
  Route Tables:    CreateRouteTable, DeleteRouteTable, DescribeRouteTables,
                   AssociateRouteTable, DisassociateRouteTable,
                   CreateRoute, ReplaceRoute, DeleteRoute
  ENI:             CreateNetworkInterface, DeleteNetworkInterface, DescribeNetworkInterfaces,
                   AttachNetworkInterface, DetachNetworkInterface
  VPC Endpoints:   CreateVpcEndpoint, DeleteVpcEndpoints, DescribeVpcEndpoints
  EBS Volumes:     CreateVolume, DeleteVolume, DescribeVolumes, DescribeVolumeStatus,
                   AttachVolume, DetachVolume, ModifyVolume, DescribeVolumesModifications,
                   EnableVolumeIO, ModifyVolumeAttribute, DescribeVolumeAttribute
  EBS Snapshots:   CreateSnapshot, DeleteSnapshot, DescribeSnapshots,
                   ModifySnapshotAttribute, DescribeSnapshotAttribute, CopySnapshot
"""

import os
import time
import random
import string
import logging
from urllib.parse import parse_qs

from ministack.core.responses import new_uuid

logger = logging.getLogger("ec2")

ACCOUNT_ID = os.environ.get("MINISTACK_ACCOUNT_ID", "000000000000")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_instances: dict = {}
_security_groups: dict = {}
_key_pairs: dict = {}
_vpcs: dict = {}
_subnets: dict = {}
_internet_gateways: dict = {}
_addresses: dict = {}       # allocation_id -> address record
_tags: dict = {}            # resource_id -> [{"Key": ..., "Value": ...}]
_route_tables: dict = {}    # rtb_id -> route table record
_network_interfaces: dict = {}  # eni_id -> ENI record
_vpc_endpoints: dict = {}   # vpce_id -> endpoint record
_volumes: dict = {}         # vol_id -> volume record
_snapshots: dict = {}       # snap_id -> snapshot record

# Default VPC / subnet created at import time so DescribeVpcs always returns something
_DEFAULT_VPC_ID = "vpc-00000001"
_DEFAULT_SUBNET_ID = "subnet-00000001"
_DEFAULT_SG_ID = "sg-00000001"
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
            "OwnerId": ACCOUNT_ID,
        }
    if _DEFAULT_SUBNET_ID not in _subnets:
        _subnets[_DEFAULT_SUBNET_ID] = {
            "SubnetId": _DEFAULT_SUBNET_ID,
            "VpcId": _DEFAULT_VPC_ID,
            "CidrBlock": "172.31.0.0/20",
            "AvailabilityZone": f"{REGION}a",
            "AvailableIpAddressCount": 4091,
            "State": "available",
            "DefaultForAz": True,
            "MapPublicIpOnLaunch": True,
            "OwnerId": ACCOUNT_ID,
        }
    if _DEFAULT_SG_ID not in _security_groups:
        _security_groups[_DEFAULT_SG_ID] = {
            "GroupId": _DEFAULT_SG_ID,
            "GroupName": "default",
            "Description": "default VPC security group",
            "VpcId": _DEFAULT_VPC_ID,
            "OwnerId": ACCOUNT_ID,
            "IpPermissions": [],
            "IpPermissionsEgress": [
                {"IpProtocol": "-1", "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                 "Ipv6Ranges": [], "PrefixListIds": [], "UserIdGroupPairs": []},
            ],
        }
    if _DEFAULT_IGW_ID not in _internet_gateways:
        _internet_gateways[_DEFAULT_IGW_ID] = {
            "InternetGatewayId": _DEFAULT_IGW_ID,
            "OwnerId": ACCOUNT_ID,
            "Attachments": [{"VpcId": _DEFAULT_VPC_ID, "State": "available"}],
        }
    default_rtb = "rtb-00000001"
    if default_rtb not in _route_tables:
        _route_tables[default_rtb] = {
            "RouteTableId": default_rtb,
            "VpcId": _DEFAULT_VPC_ID,
            "OwnerId": ACCOUNT_ID,
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

    items = "".join(_instance_xml(i) for i in created)
    inner = f"""<instancesSet>{items}</instancesSet>
    <reservationId>r-{new_uuid().replace('-','')[:17]}</reservationId>
    <ownerId>{ACCOUNT_ID}</ownerId>
    <groupSet/>"""
    return _xml(200, "RunInstancesResponse", inner)


def _describe_instances(p):
    filter_ids = _parse_member_list(p, "InstanceId")
    filters = _parse_filters(p)

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
            <ownerId>{ACCOUNT_ID}</ownerId>
            <groupSet/>
            <instancesSet>{_instance_xml(inst)}</instancesSet>
        </item>"""
        for inst in results
    )
    return _xml(200, "DescribeInstancesResponse", f"<reservationSet>{items}</reservationSet>")


def _terminate_instances(p):
    ids = _parse_member_list(p, "InstanceId")
    items = ""
    for iid in ids:
        inst = _instances.get(iid)
        if inst:
            prev = inst["State"].copy()
            inst["State"] = {"Code": 48, "Name": "terminated"}
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
            <imageOwnerId>{ACCOUNT_ID}</imageOwnerId>
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
        "OwnerId": ACCOUNT_ID,
        "IpPermissions": [],
        "IpPermissionsEgress": [],
    }
    return _xml(200, "CreateSecurityGroupResponse",
                f"<return>true</return><groupId>{sg_id}</groupId>")


def _delete_security_group(p):
    sg_id = _p(p, "GroupId")
    if sg_id and sg_id in _security_groups:
        del _security_groups[sg_id]
    elif sg_id:
        return _error("InvalidGroup.NotFound",
                      f"The security group '{sg_id}' does not exist", 400)
    return _xml(200, "DeleteSecurityGroupResponse", "<return>true</return>")


def _describe_security_groups(p):
    filter_ids = _parse_member_list(p, "GroupId")
    items = ""
    for sg in _security_groups.values():
        if filter_ids and sg["GroupId"] not in filter_ids:
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
    sg["IpPermissions"].extend(rules)
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
    sg["IpPermissionsEgress"].extend(rules)
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
    items = ""
    for vpc in _vpcs.values():
        if filter_ids and vpc["VpcId"] not in filter_ids:
            continue
        items += _vpc_xml(vpc)
    return _xml(200, "DescribeVpcsResponse", f"<vpcSet>{items}</vpcSet>")


def _create_vpc(p):
    cidr = _p(p, "CidrBlock") or "10.0.0.0/16"
    vpc_id = _new_vpc_id()
    _vpcs[vpc_id] = {
        "VpcId": vpc_id,
        "CidrBlock": cidr,
        "State": "available",
        "IsDefault": False,
        "DhcpOptionsId": "dopt-00000001",
        "InstanceTenancy": _p(p, "InstanceTenancy") or "default",
        "OwnerId": ACCOUNT_ID,
    }
    return _xml(200, "CreateVpcResponse", _vpc_fields_xml(_vpcs[vpc_id], tag="vpc"))


def _delete_vpc(p):
    vpc_id = _p(p, "VpcId")
    if vpc_id not in _vpcs:
        return _error("InvalidVpcID.NotFound", f"The vpc ID '{vpc_id}' does not exist", 400)
    del _vpcs[vpc_id]
    return _xml(200, "DeleteVpcResponse", "<return>true</return>")


# ---------------------------------------------------------------------------
# Subnets
# ---------------------------------------------------------------------------

def _describe_subnets(p):
    filter_ids = _parse_member_list(p, "SubnetId")
    items = ""
    for subnet in _subnets.values():
        if filter_ids and subnet["SubnetId"] not in filter_ids:
            continue
        items += _subnet_xml(subnet)
    return _xml(200, "DescribeSubnetsResponse", f"<subnetSet>{items}</subnetSet>")


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
        "OwnerId": ACCOUNT_ID,
    }
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
        "OwnerId": ACCOUNT_ID,
        "Attachments": [],
    }
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
        "OwnerId": ACCOUNT_ID,
        "Routes": [
            {"DestinationCidrBlock": _vpcs.get(vpc_id, {}).get("CidrBlock", "10.0.0.0/16"),
             "GatewayId": "local", "State": "active", "Origin": "CreateRouteTable"},
        ],
        "Associations": [],
    }
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
    items = "".join(
        _rtb_fields_xml(rtb)
        for rtb in _route_tables.values()
        if not filter_ids or rtb["RouteTableId"] in filter_ids
    )
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
    gw = _p(p, "GatewayId") or _p(p, "NatGatewayId") or _p(p, "InstanceId") or "local"
    rtb["Routes"].append({
        "DestinationCidrBlock": dest,
        "GatewayId": gw,
        "State": "active",
        "Origin": "CreateRoute",
    })
    return _xml(200, "CreateRouteResponse", "<return>true</return>")


def _replace_route(p):
    rtb_id = _p(p, "RouteTableId")
    rtb = _route_tables.get(rtb_id)
    if not rtb:
        return _error("InvalidRouteTableID.NotFound",
                      f"The route table '{rtb_id}' does not exist", 400)
    dest = _p(p, "DestinationCidrBlock")
    gw = _p(p, "GatewayId") or _p(p, "NatGatewayId") or _p(p, "InstanceId") or "local"
    for route in rtb["Routes"]:
        if route.get("DestinationCidrBlock") == dest:
            route["GatewayId"] = gw
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
        "OwnerId": ACCOUNT_ID,
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
        "OwnerId": ACCOUNT_ID,
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
    items = ""
    for rid, tag_list in _tags.items():
        resource_type = _guess_resource_type(rid)
        for tag in tag_list:
            items += f"""<item>
                <resourceId>{rid}</resourceId>
                <resourceType>{resource_type}</resourceType>
                <key>{tag['Key']}</key>
                <value>{tag['Value']}</value>
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
        <tagSet/>"""


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
        "OwnerId": ACCOUNT_ID,
        "Encrypted": vol["Encrypted"],
        "StorageTier": "standard",
    }
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
    return _xml(200, "ModifySnapshotAttributeResponse", "<return>true</return>")


def _describe_snapshot_attribute(p):
    snap_id = _p(p, "SnapshotId")
    return _xml(200, "DescribeSnapshotAttributeResponse", f"""
        <snapshotId>{snap_id}</snapshotId>
        <createVolumePermission/>""")


def _snapshot_inner_xml(snap):
    return f"""
        <snapshotId>{snap['SnapshotId']}</snapshotId>
        <volumeId>{snap['VolumeId']}</volumeId>
        <status>{snap['State']}</status>
        <startTime>{snap['StartTime']}</startTime>
        <progress>{snap['Progress']}</progress>
        <ownerId>{snap['OwnerId']}</ownerId>
        <volumeSize>{snap['VolumeSize']}</volumeSize>
        <description>{snap['Description']}</description>
        <encrypted>{'true' if snap['Encrypted'] else 'false'}</encrypted>
        <storageTier>{snap['StorageTier']}</storageTier>
        <tagSet/>"""


# ---------------------------------------------------------------------------
# XML helpers
# ---------------------------------------------------------------------------

def _instance_xml(inst):
    sgs = "".join(
        f"""<item><groupId>{sg['GroupId']}</groupId><groupName>{sg['GroupName']}</groupName></item>"""
        for sg in inst.get("SecurityGroups", [])
    )
    tags = "".join(
        f"<item><key>{t['Key']}</key><value>{t['Value']}</value></item>"
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
        <tagSet/>
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
        <tagSet/>
    </{tag}>"""


def _vpc_xml(vpc):
    return _vpc_fields_xml(vpc, tag="item")


def _subnet_fields_xml(subnet, tag="item"):
    return f"""<{tag}>
        <subnetId>{subnet['SubnetId']}</subnetId>
        <subnetArn>arn:aws:ec2:{REGION}:{ACCOUNT_ID}:subnet/{subnet['SubnetId']}</subnetArn>
        <state>{subnet['State']}</state>
        <vpcId>{subnet['VpcId']}</vpcId>
        <cidrBlock>{subnet['CidrBlock']}</cidrBlock>
        <availableIpAddressCount>{subnet['AvailableIpAddressCount']}</availableIpAddressCount>
        <availabilityZone>{subnet['AvailabilityZone']}</availabilityZone>
        <defaultForAz>{'true' if subnet['DefaultForAz'] else 'false'}</defaultForAz>
        <mapPublicIpOnLaunch>{'true' if subnet['MapPublicIpOnLaunch'] else 'false'}</mapPublicIpOnLaunch>
        <ownerId>{subnet['OwnerId']}</ownerId>
        <tagSet/>
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
        <tagSet/>
    </{tag}>"""


def _igw_xml(igw):
    return _igw_fields_xml(igw, tag="item")


def _rtb_fields_xml(rtb, tag="item"):
    routes = "".join(f"""<item>
        <destinationCidrBlock>{r.get('DestinationCidrBlock','')}</destinationCidrBlock>
        <gatewayId>{r.get('GatewayId','')}</gatewayId>
        <state>{r.get('State','active')}</state>
        <origin>{r.get('Origin','')}</origin>
    </item>""" for r in rtb.get("Routes", []))
    assocs = "".join(f"""<item>
        <routeTableAssociationId>{a['RouteTableAssociationId']}</routeTableAssociationId>
        <routeTableId>{a['RouteTableId']}</routeTableId>
        <main>{'true' if a.get('Main') else 'false'}</main>
        {'<subnetId>' + a['SubnetId'] + '</subnetId>' if a.get('SubnetId') else ''}
    </item>""" for a in rtb.get("Associations", []))
    return f"""<{tag}>
        <routeTableId>{rtb['RouteTableId']}</routeTableId>
        <vpcId>{rtb['VpcId']}</vpcId>
        <ownerId>{rtb['OwnerId']}</ownerId>
        <routeSet>{routes}</routeSet>
        <associationSet>{assocs}</associationSet>
        <propagatingVgwSet/>
        <tagSet/>
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
    if resource_id.startswith("i-"):
        return "instance"
    if resource_id.startswith("sg-"):
        return "security-group"
    if resource_id.startswith("vpc-"):
        return "vpc"
    if resource_id.startswith("subnet-"):
        return "subnet"
    if resource_id.startswith("igw-"):
        return "internet-gateway"
    if resource_id.startswith("eipalloc-"):
        return "elastic-ip"
    if resource_id.startswith("rtb-"):
        return "route-table"
    if resource_id.startswith("eni-"):
        return "network-interface"
    if resource_id.startswith("vpce-"):
        return "vpc-endpoint"
    if resource_id.startswith("vol-"):
        return "volume"
    if resource_id.startswith("snap-"):
        return "snapshot"
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
    _init_defaults()


# ---------------------------------------------------------------------------
# Action map
# ---------------------------------------------------------------------------

_ACTION_MAP = {
    "RunInstances": _run_instances,
    "DescribeInstances": _describe_instances,
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
}
