import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

def test_ec2_describe_vpcs_default(ec2):
    resp = ec2.describe_vpcs()
    vpcs = resp["Vpcs"]
    assert any(v["IsDefault"] for v in vpcs)

def test_ec2_describe_subnets_default(ec2):
    resp = ec2.describe_subnets()
    assert len(resp["Subnets"]) >= 1

def test_ec2_describe_availability_zones(ec2):
    resp = ec2.describe_availability_zones()
    azs = [az["ZoneName"] for az in resp["AvailabilityZones"]]
    assert any("us-east-1" in az for az in azs)

def test_ec2_run_describe_terminate_instances(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1, InstanceType="t2.micro")
    assert len(resp["Instances"]) == 1
    instance_id = resp["Instances"][0]["InstanceId"]
    assert instance_id.startswith("i-")
    assert resp["Instances"][0]["State"]["Name"] == "running"

    desc = ec2.describe_instances(InstanceIds=[instance_id])
    assert len(desc["Reservations"]) == 1
    assert desc["Reservations"][0]["Instances"][0]["InstanceId"] == instance_id

    term = ec2.terminate_instances(InstanceIds=[instance_id])
    assert term["TerminatingInstances"][0]["CurrentState"]["Name"] == "terminated"

def test_ec2_describe_instance_status(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1, InstanceType="t2.micro")
    iid = resp["Instances"][0]["InstanceId"]

    # Running instance should appear by default
    status = ec2.describe_instance_status(InstanceIds=[iid])
    assert len(status["InstanceStatuses"]) == 1
    s = status["InstanceStatuses"][0]
    assert s["InstanceId"] == iid
    assert s["InstanceState"]["Name"] == "running"
    assert s["SystemStatus"]["Status"] == "ok"
    assert s["InstanceStatus"]["Status"] == "ok"

    # Stopped instance should NOT appear without IncludeAllInstances
    ec2.stop_instances(InstanceIds=[iid])
    status2 = ec2.describe_instance_status(InstanceIds=[iid])
    assert len(status2["InstanceStatuses"]) == 0

    # With IncludeAllInstances=True it should appear
    status3 = ec2.describe_instance_status(InstanceIds=[iid], IncludeAllInstances=True)
    assert len(status3["InstanceStatuses"]) == 1
    assert status3["InstanceStatuses"][0]["InstanceState"]["Name"] == "stopped"

    ec2.terminate_instances(InstanceIds=[iid])


def test_ec2_stop_start_instances(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    stop = ec2.stop_instances(InstanceIds=[iid])
    assert stop["StoppingInstances"][0]["CurrentState"]["Name"] == "stopped"

    start = ec2.start_instances(InstanceIds=[iid])
    assert start["StartingInstances"][0]["CurrentState"]["Name"] == "running"

    ec2.terminate_instances(InstanceIds=[iid])

def test_ec2_run_multiple_instances(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=3, MaxCount=3)
    assert len(resp["Instances"]) == 3
    ids = [i["InstanceId"] for i in resp["Instances"]]
    assert len(set(ids)) == 3
    ec2.terminate_instances(InstanceIds=ids)

def test_ec2_describe_images(ec2):
    resp = ec2.describe_images(Owners=["self"])
    assert len(resp["Images"]) >= 1
    assert all("ImageId" in img for img in resp["Images"])

def test_ec2_security_group_crud(ec2):
    sg_id = ec2.create_security_group(GroupName="qa-ec2-sg", Description="test sg")["GroupId"]
    assert sg_id.startswith("sg-")

    desc = ec2.describe_security_groups(GroupIds=[sg_id])
    assert desc["SecurityGroups"][0]["GroupName"] == "qa-ec2-sg"

    ec2.delete_security_group(GroupId=sg_id)
    desc2 = ec2.describe_security_groups()
    assert not any(sg["GroupId"] == sg_id for sg in desc2["SecurityGroups"])

def test_ec2_security_group_duplicate(ec2):
    ec2.create_security_group(GroupName="qa-ec2-sg-dup", Description="d")
    with pytest.raises(ClientError) as exc:
        ec2.create_security_group(GroupName="qa-ec2-sg-dup", Description="d")
    assert exc.value.response["Error"]["Code"] == "InvalidGroup.Duplicate"

def test_ec2_sg_authorize_revoke_ingress(ec2):
    sg_id = ec2.create_security_group(GroupName="qa-ec2-sg-rules", Description="rules test")["GroupId"]

    ec2.authorize_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 80,
                "ToPort": 80,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )
    desc = ec2.describe_security_groups(GroupIds=[sg_id])
    perms = desc["SecurityGroups"][0]["IpPermissions"]
    assert any(p["FromPort"] == 80 for p in perms)

    ec2.revoke_security_group_ingress(
        GroupId=sg_id,
        IpPermissions=[
            {
                "IpProtocol": "tcp",
                "FromPort": 80,
                "ToPort": 80,
                "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
            }
        ],
    )
    desc2 = ec2.describe_security_groups(GroupIds=[sg_id])
    assert not any(p.get("FromPort") == 80 for p in desc2["SecurityGroups"][0]["IpPermissions"])

    ec2.delete_security_group(GroupId=sg_id)

def test_ec2_key_pair_crud(ec2):
    resp = ec2.create_key_pair(KeyName="qa-ec2-key")
    assert resp["KeyName"] == "qa-ec2-key"
    assert "KeyMaterial" in resp

    desc = ec2.describe_key_pairs(KeyNames=["qa-ec2-key"])
    assert len(desc["KeyPairs"]) == 1

    ec2.delete_key_pair(KeyName="qa-ec2-key")
    desc2 = ec2.describe_key_pairs()
    assert not any(kp["KeyName"] == "qa-ec2-key" for kp in desc2["KeyPairs"])

def test_ec2_key_pair_duplicate(ec2):
    ec2.create_key_pair(KeyName="qa-ec2-key-dup")
    with pytest.raises(ClientError) as exc:
        ec2.create_key_pair(KeyName="qa-ec2-key-dup")
    assert exc.value.response["Error"]["Code"] == "InvalidKeyPair.Duplicate"

def test_ec2_vpc_create_delete(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.1.0.0/16")["Vpc"]["VpcId"]
    assert vpc_id.startswith("vpc-")

    desc = ec2.describe_vpcs(VpcIds=[vpc_id])
    assert desc["Vpcs"][0]["CidrBlock"] == "10.1.0.0/16"
    assert not desc["Vpcs"][0]["IsDefault"]

    ec2.delete_vpc(VpcId=vpc_id)
    desc2 = ec2.describe_vpcs()
    assert not any(v["VpcId"] == vpc_id for v in desc2["Vpcs"])

def test_ec2_subnet_create_delete(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.2.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.2.1.0/24")["Subnet"]["SubnetId"]
    assert subnet_id.startswith("subnet-")

    desc = ec2.describe_subnets(SubnetIds=[subnet_id])
    assert desc["Subnets"][0]["CidrBlock"] == "10.2.1.0/24"

    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_internet_gateway_crud(ec2):
    igw_id = ec2.create_internet_gateway()["InternetGateway"]["InternetGatewayId"]
    assert igw_id.startswith("igw-")

    vpc_id = ec2.create_vpc(CidrBlock="10.3.0.0/16")["Vpc"]["VpcId"]
    ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

    desc = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])
    assert len(desc["InternetGateways"][0]["Attachments"]) == 1

    ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
    ec2.delete_internet_gateway(InternetGatewayId=igw_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_elastic_ip_crud(ec2):
    alloc = ec2.allocate_address(Domain="vpc")
    alloc_id = alloc["AllocationId"]
    assert alloc_id.startswith("eipalloc-")
    assert "PublicIp" in alloc

    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    assoc = ec2.associate_address(AllocationId=alloc_id, InstanceId=iid)
    assert "AssociationId" in assoc

    desc = ec2.describe_addresses(AllocationIds=[alloc_id])
    assert desc["Addresses"][0]["InstanceId"] == iid

    ec2.disassociate_address(AssociationId=assoc["AssociationId"])
    ec2.release_address(AllocationId=alloc_id)
    ec2.terminate_instances(InstanceIds=[iid])

def test_ec2_tags_crud(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    ec2.create_tags(Resources=[iid], Tags=[{"Key": "Name", "Value": "qa-box"}])

    desc = ec2.describe_instances(InstanceIds=[iid])
    tags = desc["Reservations"][0]["Instances"][0]["Tags"]
    assert any(t["Key"] == "Name" and t["Value"] == "qa-box" for t in tags)

    ec2.delete_tags(Resources=[iid], Tags=[{"Key": "Name"}])
    desc2 = ec2.describe_instances(InstanceIds=[iid])
    tags2 = desc2["Reservations"][0]["Instances"][0].get("Tags", [])
    assert not any(t["Key"] == "Name" for t in tags2)

    ec2.terminate_instances(InstanceIds=[iid])

def test_ec2_modify_vpc_attribute(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.10.0.0/16")["Vpc"]["VpcId"]
    ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
    ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_modify_subnet_attribute(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.11.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.11.1.0/24")["Subnet"]["SubnetId"]
    ec2.modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
    desc = ec2.describe_subnets(SubnetIds=[subnet_id])
    assert desc["Subnets"][0]["MapPublicIpOnLaunch"] is True
    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_route_table_crud(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.20.0.0/16")["Vpc"]["VpcId"]
    rtb_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]
    assert rtb_id.startswith("rtb-")

    desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assert desc["RouteTables"][0]["RouteTableId"] == rtb_id

    ec2.delete_route_table(RouteTableId=rtb_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_route_table_associate_disassociate(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.21.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.21.1.0/24")["Subnet"]["SubnetId"]
    rtb_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]

    assoc_id = ec2.associate_route_table(RouteTableId=rtb_id, SubnetId=subnet_id)["AssociationId"]
    assert assoc_id.startswith("rtbassoc-")

    desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assocs = desc["RouteTables"][0]["Associations"]
    assert any(a["RouteTableAssociationId"] == assoc_id for a in assocs)

    ec2.disassociate_route_table(AssociationId=assoc_id)
    desc2 = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assert not any(a["RouteTableAssociationId"] == assoc_id for a in desc2["RouteTables"][0]["Associations"])

    ec2.delete_route_table(RouteTableId=rtb_id)
    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_route_create_replace_delete(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.22.0.0/16")["Vpc"]["VpcId"]
    rtb_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]
    igw_id = ec2.create_internet_gateway()["InternetGateway"]["InternetGatewayId"]

    ec2.create_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw_id)
    desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    routes = desc["RouteTables"][0]["Routes"]
    assert any(r.get("DestinationCidrBlock") == "0.0.0.0/0" for r in routes)

    ec2.replace_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0", GatewayId="local")

    ec2.delete_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0")
    desc2 = ec2.describe_route_tables(RouteTableIds=[rtb_id])
    assert not any(r.get("DestinationCidrBlock") == "0.0.0.0/0" for r in desc2["RouteTables"][0]["Routes"])

    ec2.delete_internet_gateway(InternetGatewayId=igw_id)
    ec2.delete_route_table(RouteTableId=rtb_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_network_interface_crud(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.30.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.30.1.0/24")["Subnet"]["SubnetId"]

    eni_id = ec2.create_network_interface(SubnetId=subnet_id, Description="qa-eni")["NetworkInterface"][
        "NetworkInterfaceId"
    ]
    assert eni_id.startswith("eni-")

    desc = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    assert desc["NetworkInterfaces"][0]["Description"] == "qa-eni"
    assert desc["NetworkInterfaces"][0]["Status"] == "available"

    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
    desc2 = ec2.describe_network_interfaces()
    assert not any(e["NetworkInterfaceId"] == eni_id for e in desc2["NetworkInterfaces"])

    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_network_interface_attach_detach(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.31.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.31.1.0/24")["Subnet"]["SubnetId"]
    eni_id = ec2.create_network_interface(SubnetId=subnet_id)["NetworkInterface"]["NetworkInterfaceId"]
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    attach_resp = ec2.attach_network_interface(NetworkInterfaceId=eni_id, InstanceId=iid, DeviceIndex=1)
    attachment_id = attach_resp["AttachmentId"]
    assert attachment_id.startswith("eni-attach-")

    desc = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    assert desc["NetworkInterfaces"][0]["Status"] == "in-use"

    ec2.detach_network_interface(AttachmentId=attachment_id)
    desc2 = ec2.describe_network_interfaces(NetworkInterfaceIds=[eni_id])
    assert desc2["NetworkInterfaces"][0]["Status"] == "available"

    ec2.terminate_instances(InstanceIds=[iid])
    ec2.delete_network_interface(NetworkInterfaceId=eni_id)
    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_vpc_endpoint_crud(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.40.0.0/16")["Vpc"]["VpcId"]

    vpce_id = ec2.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName="com.amazonaws.us-east-1.s3",
        VpcEndpointType="Gateway",
    )["VpcEndpoint"]["VpcEndpointId"]
    assert vpce_id.startswith("vpce-")

    desc = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
    assert desc["VpcEndpoints"][0]["ServiceName"] == "com.amazonaws.us-east-1.s3"
    assert desc["VpcEndpoints"][0]["State"] == "available"

    ec2.delete_vpc_endpoints(VpcEndpointIds=[vpce_id])
    desc2 = ec2.describe_vpc_endpoints()
    assert not any(e["VpcEndpointId"] == vpce_id for e in desc2["VpcEndpoints"])

    ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_describe_route_tables_default(ec2):
    desc = ec2.describe_route_tables()
    assert any(rt["VpcId"] == "vpc-00000001" for rt in desc["RouteTables"])

def test_ec2_nat_gateway_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.100.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.100.1.0/24")
    subnet_id = subnet["Subnet"]["SubnetId"]

    resp = ec2.create_nat_gateway(SubnetId=subnet_id, ConnectivityType="private")
    nat_id = resp["NatGateway"]["NatGatewayId"]
    assert nat_id.startswith("nat-")
    assert resp["NatGateway"]["State"] == "available"

    desc = ec2.describe_nat_gateways(NatGatewayIds=[nat_id])
    assert len(desc["NatGateways"]) == 1
    assert desc["NatGateways"][0]["NatGatewayId"] == nat_id
    assert desc["NatGateways"][0]["SubnetId"] == subnet_id

    ec2.delete_nat_gateway(NatGatewayId=nat_id)
    desc2 = ec2.describe_nat_gateways(NatGatewayIds=[nat_id])
    assert desc2["NatGateways"][0]["State"] == "deleted"

def test_ec2_nat_gateway_filter_by_vpc(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.101.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.101.1.0/24")
    subnet_id = subnet["Subnet"]["SubnetId"]
    ec2.create_nat_gateway(SubnetId=subnet_id, ConnectivityType="private")

    desc = ec2.describe_nat_gateways(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])
    assert all(n["VpcId"] == vpc_id for n in desc["NatGateways"])

def test_ec2_network_acl_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.102.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    resp = ec2.create_network_acl(VpcId=vpc_id)
    acl_id = resp["NetworkAcl"]["NetworkAclId"]
    assert acl_id.startswith("acl-")
    assert resp["NetworkAcl"]["VpcId"] == vpc_id
    assert resp["NetworkAcl"]["IsDefault"] is False

    desc = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc["NetworkAcls"]) == 1
    assert desc["NetworkAcls"][0]["NetworkAclId"] == acl_id

    ec2.create_network_acl_entry(
        NetworkAclId=acl_id,
        RuleNumber=100,
        Protocol="-1",
        RuleAction="allow",
        Egress=False,
        CidrBlock="0.0.0.0/0",
    )
    desc2 = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc2["NetworkAcls"][0]["Entries"]) == 1

    ec2.delete_network_acl_entry(NetworkAclId=acl_id, RuleNumber=100, Egress=False)
    desc3 = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc3["NetworkAcls"][0]["Entries"]) == 0

    ec2.delete_network_acl(NetworkAclId=acl_id)
    desc4 = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    assert len(desc4["NetworkAcls"]) == 0

def test_ec2_network_acl_replace_entry(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.103.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    resp = ec2.create_network_acl(VpcId=vpc_id)
    acl_id = resp["NetworkAcl"]["NetworkAclId"]

    ec2.create_network_acl_entry(
        NetworkAclId=acl_id, RuleNumber=200, Protocol="-1", RuleAction="deny", Egress=False, CidrBlock="10.0.0.0/8"
    )
    ec2.replace_network_acl_entry(
        NetworkAclId=acl_id, RuleNumber=200, Protocol="-1", RuleAction="allow", Egress=False, CidrBlock="10.0.0.0/8"
    )
    desc = ec2.describe_network_acls(NetworkAclIds=[acl_id])
    entries = desc["NetworkAcls"][0]["Entries"]
    assert len(entries) == 1
    assert entries[0]["RuleAction"] == "allow"

def test_ec2_flow_logs_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.104.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    resp = ec2.create_flow_logs(
        ResourceIds=[vpc_id],
        ResourceType="VPC",
        TrafficType="ALL",
        LogDestinationType="cloud-watch-logs",
        LogGroupName="/aws/vpc/flowlogs",
    )
    assert resp["Unsuccessful"] == []
    fl_ids = resp["FlowLogIds"]
    assert len(fl_ids) == 1
    assert fl_ids[0].startswith("fl-")

    desc = ec2.describe_flow_logs(FlowLogIds=fl_ids)
    assert len(desc["FlowLogs"]) == 1
    assert desc["FlowLogs"][0]["FlowLogId"] == fl_ids[0]
    assert desc["FlowLogs"][0]["FlowLogStatus"] == "ACTIVE"

    ec2.delete_flow_logs(FlowLogIds=fl_ids)
    desc2 = ec2.describe_flow_logs(FlowLogIds=fl_ids)
    assert len(desc2["FlowLogs"]) == 0

def test_ec2_vpc_peering_crud(ec2):
    vpc1 = ec2.create_vpc(CidrBlock="10.105.0.0/16")
    vpc2 = ec2.create_vpc(CidrBlock="10.106.0.0/16")
    vpc_id1 = vpc1["Vpc"]["VpcId"]
    vpc_id2 = vpc2["Vpc"]["VpcId"]

    resp = ec2.create_vpc_peering_connection(VpcId=vpc_id1, PeerVpcId=vpc_id2)
    pcx = resp["VpcPeeringConnection"]
    pcx_id = pcx["VpcPeeringConnectionId"]
    assert pcx_id.startswith("pcx-")
    assert pcx["Status"]["Code"] == "pending-acceptance"

    accepted = ec2.accept_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
    assert accepted["VpcPeeringConnection"]["Status"]["Code"] == "active"

    desc = ec2.describe_vpc_peering_connections(VpcPeeringConnectionIds=[pcx_id])
    assert len(desc["VpcPeeringConnections"]) == 1
    assert desc["VpcPeeringConnections"][0]["Status"]["Code"] == "active"

    ec2.delete_vpc_peering_connection(VpcPeeringConnectionId=pcx_id)
    desc2 = ec2.describe_vpc_peering_connections(VpcPeeringConnectionIds=[pcx_id])
    assert desc2["VpcPeeringConnections"][0]["Status"]["Code"] == "deleted"

def test_ec2_vpc_peering_not_found(ec2):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        ec2.accept_vpc_peering_connection(VpcPeeringConnectionId="pcx-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]

def test_ec2_dhcp_options_crud(ec2):
    resp = ec2.create_dhcp_options(
        DhcpConfigurations=[
            {"Key": "domain-name", "Values": ["example.internal"]},
            {"Key": "domain-name-servers", "Values": ["10.0.0.1", "10.0.0.2"]},
        ]
    )
    dopt = resp["DhcpOptions"]
    dopt_id = dopt["DhcpOptionsId"]
    assert dopt_id.startswith("dopt-")

    desc = ec2.describe_dhcp_options(DhcpOptionsIds=[dopt_id])
    assert len(desc["DhcpOptions"]) == 1
    configs = {c["Key"]: [v["Value"] for v in c["Values"]] for c in desc["DhcpOptions"][0]["DhcpConfigurations"]}
    assert configs["domain-name"] == ["example.internal"]
    assert "10.0.0.1" in configs["domain-name-servers"]

    vpc = ec2.create_vpc(CidrBlock="10.107.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    ec2.associate_dhcp_options(DhcpOptionsId=dopt_id, VpcId=vpc_id)

    ec2.delete_dhcp_options(DhcpOptionsId=dopt_id)
    desc2 = ec2.describe_dhcp_options(DhcpOptionsIds=[dopt_id])
    assert len(desc2["DhcpOptions"]) == 0

def test_ec2_dhcp_options_not_found(ec2):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        ec2.delete_dhcp_options(DhcpOptionsId="dopt-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]

def test_ec2_egress_only_igw_crud(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.108.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]

    resp = ec2.create_egress_only_internet_gateway(VpcId=vpc_id)
    eigw = resp["EgressOnlyInternetGateway"]
    eigw_id = eigw["EgressOnlyInternetGatewayId"]
    assert eigw_id.startswith("eigw-")
    assert eigw["Attachments"][0]["State"] == "attached"
    assert eigw["Attachments"][0]["VpcId"] == vpc_id

    desc = ec2.describe_egress_only_internet_gateways(EgressOnlyInternetGatewayIds=[eigw_id])
    assert len(desc["EgressOnlyInternetGateways"]) == 1
    assert desc["EgressOnlyInternetGateways"][0]["EgressOnlyInternetGatewayId"] == eigw_id

    ec2.delete_egress_only_internet_gateway(EgressOnlyInternetGatewayId=eigw_id)
    desc2 = ec2.describe_egress_only_internet_gateways(EgressOnlyInternetGatewayIds=[eigw_id])
    assert len(desc2["EgressOnlyInternetGateways"]) == 0

def test_ec2_egress_only_igw_not_found(ec2):
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as exc:
        ec2.delete_egress_only_internet_gateway(EgressOnlyInternetGatewayId="eigw-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]

def test_ec2_describe_instance_attribute_instance_type(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1, InstanceType="t3.micro")
    iid = resp["Instances"][0]["InstanceId"]

    attr = ec2.describe_instance_attribute(InstanceId=iid, Attribute="instanceType")
    assert attr["InstanceId"] == iid
    assert attr["InstanceType"]["Value"] == "t3.micro"

    ec2.terminate_instances(InstanceIds=[iid])

def test_ec2_describe_instance_attribute_shutdown_behavior(ec2):
    resp = ec2.run_instances(ImageId="ami-00000000", MinCount=1, MaxCount=1)
    iid = resp["Instances"][0]["InstanceId"]

    attr = ec2.describe_instance_attribute(InstanceId=iid, Attribute="instanceInitiatedShutdownBehavior")
    assert attr["InstanceId"] == iid
    assert attr["InstanceInitiatedShutdownBehavior"]["Value"] == "stop"

    ec2.terminate_instances(InstanceIds=[iid])

def test_ec2_describe_instance_attribute_not_found(ec2):
    from botocore.exceptions import ClientError
    with pytest.raises(ClientError) as exc:
        ec2.describe_instance_attribute(InstanceId="i-000000000000nonex", Attribute="instanceType")
    assert exc.value.response["Error"]["Code"] == "InvalidInstanceID.NotFound"

def test_ec2_describe_instance_credit_specifications(ec2):
    iid = ec2.run_instances(ImageId="ami-test", MinCount=1, MaxCount=1)["Instances"][0]["InstanceId"]
    resp = ec2.describe_instance_credit_specifications(InstanceIds=[iid])
    specs = resp["InstanceCreditSpecifications"]
    assert len(specs) == 1
    assert specs[0]["InstanceId"] == iid
    assert specs[0]["CpuCredits"] == "standard"

def test_ec2_describe_spot_instance_requests(ec2):
    resp = ec2.describe_spot_instance_requests()
    assert "SpotInstanceRequests" in resp

def test_ec2_describe_capacity_reservations(ec2):
    resp = ec2.describe_capacity_reservations()
    assert "CapacityReservations" in resp

def test_ec2_describe_instance_types_defaults(ec2):
    resp = ec2.describe_instance_types()
    types = [t["InstanceType"] for t in resp["InstanceTypes"]]
    assert "t2.micro" in types
    assert "t3.micro" in types
    assert len(resp["InstanceTypes"]) >= 4
    # Spot-check shape
    sample = resp["InstanceTypes"][0]
    assert "VCpuInfo" in sample
    assert "MemoryInfo" in sample
    assert sample["VCpuInfo"]["DefaultVCpus"] >= 1
    assert sample["MemoryInfo"]["SizeInMiB"] >= 512

def test_ec2_describe_instance_types_filter(ec2):
    resp = ec2.describe_instance_types(InstanceTypes=["t2.micro", "m5.large"])
    types = {t["InstanceType"] for t in resp["InstanceTypes"]}
    assert types == {"t2.micro", "m5.large"}

def test_ec2_describe_vpc_attribute(ec2):
    vpc = ec2.create_vpc(CidrBlock="10.99.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    resp = ec2.describe_vpc_attribute(VpcId=vpc_id, Attribute="enableDnsSupport")
    assert resp["EnableDnsSupport"]["Value"] in (True, False)
    resp2 = ec2.describe_vpc_attribute(VpcId=vpc_id, Attribute="enableDnsHostnames")
    assert resp2["EnableDnsHostnames"]["Value"] in (True, False)

def test_ec2_create_vpc_default_resources(ec2):
    """CreateVpc must create per-VPC default ACL, SG, and main route table."""
    vpc = ec2.create_vpc(CidrBlock="10.99.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        # DescribeNetworkAcls with vpc-id + default=true
        acls = ec2.describe_network_acls(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "default", "Values": ["true"]},
        ])
        assert len(acls["NetworkAcls"]) == 1
        acl = acls["NetworkAcls"][0]
        assert acl["IsDefault"] is True
        assert acl["VpcId"] == vpc_id

        # DescribeSecurityGroups with vpc-id + group-name=default
        sgs = ec2.describe_security_groups(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": ["default"]},
        ])
        assert len(sgs["SecurityGroups"]) == 1
        assert sgs["SecurityGroups"][0]["VpcId"] == vpc_id

        # DescribeRouteTables with vpc-id + association.main=true
        rtbs = ec2.describe_route_tables(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "association.main", "Values": ["true"]},
        ])
        assert len(rtbs["RouteTables"]) == 1
        assert rtbs["RouteTables"][0]["VpcId"] == vpc_id
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_route_table_association_filter(ec2):
    """AssociateRouteTable + DescribeRouteTables filter by association ID."""
    vpc = ec2.create_vpc(CidrBlock="10.98.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.98.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        rtb = ec2.create_route_table(VpcId=vpc_id)
        rtb_id = rtb["RouteTable"]["RouteTableId"]
        assoc = ec2.associate_route_table(RouteTableId=rtb_id, SubnetId=subnet_id)
        assoc_id = assoc["AssociationId"]

        # Filter by association ID
        result = ec2.describe_route_tables(Filters=[
            {"Name": "association.route-table-association-id", "Values": [assoc_id]},
        ])
        assert len(result["RouteTables"]) == 1
        assert result["RouteTables"][0]["RouteTableId"] == rtb_id

        # Filter by subnet ID
        result2 = ec2.describe_route_tables(Filters=[
            {"Name": "association.subnet-id", "Values": [subnet_id]},
        ])
        assert len(result2["RouteTables"]) == 1

        ec2.disassociate_route_table(AssociationId=assoc_id)
        ec2.delete_route_table(RouteTableId=rtb_id)
        ec2.delete_subnet(SubnetId=subnet_id)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_replace_route_table_association(ec2):
    """ReplaceRouteTableAssociation moves subnet to a different route table."""
    vpc = ec2.create_vpc(CidrBlock="10.97.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.97.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        rtb1 = ec2.create_route_table(VpcId=vpc_id)
        rtb1_id = rtb1["RouteTable"]["RouteTableId"]
        rtb2 = ec2.create_route_table(VpcId=vpc_id)
        rtb2_id = rtb2["RouteTable"]["RouteTableId"]

        assoc = ec2.associate_route_table(RouteTableId=rtb1_id, SubnetId=subnet_id)
        old_assoc_id = assoc["AssociationId"]

        # Replace association to rtb2
        new = ec2.replace_route_table_association(AssociationId=old_assoc_id, RouteTableId=rtb2_id)
        new_assoc_id = new["NewAssociationId"]
        assert new_assoc_id != old_assoc_id

        # Verify subnet is now on rtb2
        result = ec2.describe_route_tables(Filters=[
            {"Name": "association.subnet-id", "Values": [subnet_id]},
        ])
        assert result["RouteTables"][0]["RouteTableId"] == rtb2_id

        ec2.disassociate_route_table(AssociationId=new_assoc_id)
        ec2.delete_route_table(RouteTableId=rtb1_id)
        ec2.delete_route_table(RouteTableId=rtb2_id)
        ec2.delete_subnet(SubnetId=subnet_id)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_modify_vpc_endpoint(ec2):
    """ModifyVpcEndpoint adds/removes route tables."""
    vpc = ec2.create_vpc(CidrBlock="10.96.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        rtb = ec2.create_route_table(VpcId=vpc_id)
        rtb_id = rtb["RouteTable"]["RouteTableId"]
        ep = ec2.create_vpc_endpoint(
            VpcId=vpc_id, ServiceName="com.amazonaws.us-east-1.s3",
            VpcEndpointType="Gateway",
        )
        vpce_id = ep["VpcEndpoint"]["VpcEndpointId"]

        # Add route table
        ec2.modify_vpc_endpoint(VpcEndpointId=vpce_id, AddRouteTableIds=[rtb_id])
        desc = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
        assert rtb_id in desc["VpcEndpoints"][0]["RouteTableIds"]

        # Remove route table
        ec2.modify_vpc_endpoint(VpcEndpointId=vpce_id, RemoveRouteTableIds=[rtb_id])
        desc = ec2.describe_vpc_endpoints(VpcEndpointIds=[vpce_id])
        assert rtb_id not in desc["VpcEndpoints"][0]["RouteTableIds"]

        ec2.delete_vpc_endpoints(VpcEndpointIds=[vpce_id])
        ec2.delete_route_table(RouteTableId=rtb_id)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_describe_prefix_lists(ec2):
    """DescribePrefixLists returns built-in AWS service prefix lists."""
    result = ec2.describe_prefix_lists()
    pl_names = [pl["PrefixListName"] for pl in result["PrefixLists"]]
    assert any("s3" in n for n in pl_names)
    assert any("dynamodb" in n for n in pl_names)

def test_ec2_managed_prefix_list_crud(ec2):
    """Full lifecycle: create, describe, get entries, modify, delete."""
    pl = ec2.create_managed_prefix_list(
        PrefixListName="test-pl", MaxEntries=5, AddressFamily="IPv4",
        Entries=[{"Cidr": "10.0.0.0/8", "Description": "RFC1918-10"}],
    )
    pl_id = pl["PrefixList"]["PrefixListId"]
    assert pl["PrefixList"]["PrefixListName"] == "test-pl"

    # Describe
    desc = ec2.describe_managed_prefix_lists(PrefixListIds=[pl_id])
    assert len(desc["PrefixLists"]) == 1
    assert desc["PrefixLists"][0]["PrefixListName"] == "test-pl"

    # Get entries
    entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
    assert len(entries["Entries"]) == 1
    assert entries["Entries"][0]["Cidr"] == "10.0.0.0/8"

    # Modify — add entry
    ec2.modify_managed_prefix_list(
        PrefixListId=pl_id, CurrentVersion=1,
        AddEntries=[{"Cidr": "172.16.0.0/12", "Description": "RFC1918-172"}],
    )
    entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
    cidrs = [e["Cidr"] for e in entries["Entries"]]
    assert "10.0.0.0/8" in cidrs
    assert "172.16.0.0/12" in cidrs

    # Modify — remove entry
    ec2.modify_managed_prefix_list(
        PrefixListId=pl_id, CurrentVersion=2,
        RemoveEntries=[{"Cidr": "10.0.0.0/8"}],
    )
    entries = ec2.get_managed_prefix_list_entries(PrefixListId=pl_id)
    cidrs = [e["Cidr"] for e in entries["Entries"]]
    assert "10.0.0.0/8" not in cidrs
    assert "172.16.0.0/12" in cidrs

    # Delete
    ec2.delete_managed_prefix_list(PrefixListId=pl_id)
    desc = ec2.describe_managed_prefix_lists(PrefixListIds=[pl_id])
    assert len(desc["PrefixLists"]) == 0

def test_ec2_vpn_gateway_crud(ec2):
    """Full lifecycle: create, attach, describe, detach, delete."""
    vpc = ec2.create_vpc(CidrBlock="10.95.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        vgw = ec2.create_vpn_gateway(Type="ipsec.1")
        vgw_id = vgw["VpnGateway"]["VpnGatewayId"]
        assert vgw["VpnGateway"]["State"] == "available"

        # Attach
        ec2.attach_vpn_gateway(VpnGatewayId=vgw_id, VpcId=vpc_id)
        desc = ec2.describe_vpn_gateways(VpnGatewayIds=[vgw_id])
        attachments = desc["VpnGateways"][0]["VpcAttachments"]
        assert len(attachments) == 1
        assert attachments[0]["VpcId"] == vpc_id
        assert attachments[0]["State"] == "attached"

        # Filter by attachment.vpc-id
        filtered = ec2.describe_vpn_gateways(Filters=[
            {"Name": "attachment.vpc-id", "Values": [vpc_id]},
        ])
        assert len(filtered["VpnGateways"]) == 1

        # Detach
        ec2.detach_vpn_gateway(VpnGatewayId=vgw_id, VpcId=vpc_id)
        desc = ec2.describe_vpn_gateways(VpnGatewayIds=[vgw_id])
        assert desc["VpnGateways"][0]["VpcAttachments"] == []

        # Delete
        ec2.delete_vpn_gateway(VpnGatewayId=vgw_id)
        desc = ec2.describe_vpn_gateways(VpnGatewayIds=[vgw_id])
        assert len(desc["VpnGateways"]) == 0
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_vgw_route_propagation(ec2):
    """EnableVgwRoutePropagation / DisableVgwRoutePropagation."""
    vpc = ec2.create_vpc(CidrBlock="10.94.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        rtb = ec2.create_route_table(VpcId=vpc_id)
        rtb_id = rtb["RouteTable"]["RouteTableId"]
        vgw = ec2.create_vpn_gateway(Type="ipsec.1")
        vgw_id = vgw["VpnGateway"]["VpnGatewayId"]

        ec2.enable_vgw_route_propagation(RouteTableId=rtb_id, GatewayId=vgw_id)
        # No error = success (propagation stored server-side)

        ec2.disable_vgw_route_propagation(RouteTableId=rtb_id, GatewayId=vgw_id)
        # No error = success

        ec2.delete_vpn_gateway(VpnGatewayId=vgw_id)
        ec2.delete_route_table(RouteTableId=rtb_id)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_customer_gateway_crud(ec2):
    """Full lifecycle: create, describe, delete."""
    cgw = ec2.create_customer_gateway(BgpAsn=65000, IpAddress="203.0.113.1", Type="ipsec.1")
    cgw_id = cgw["CustomerGateway"]["CustomerGatewayId"]
    assert cgw["CustomerGateway"]["State"] == "available"
    assert cgw["CustomerGateway"]["IpAddress"] == "203.0.113.1"

    # Describe
    desc = ec2.describe_customer_gateways(CustomerGatewayIds=[cgw_id])
    assert len(desc["CustomerGateways"]) == 1
    assert desc["CustomerGateways"][0]["BgpAsn"] == "65000"

    # Delete
    ec2.delete_customer_gateway(CustomerGatewayId=cgw_id)
    desc = ec2.describe_customer_gateways(CustomerGatewayIds=[cgw_id])
    assert len(desc["CustomerGateways"]) == 0

def test_ec2_create_route_nat_gateway(ec2):
    """CreateRoute with NatGatewayId stores it separately from GatewayId."""
    vpc = ec2.create_vpc(CidrBlock="10.93.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        subnet = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.93.1.0/24")
        subnet_id = subnet["Subnet"]["SubnetId"]
        eip = ec2.allocate_address(Domain="vpc")
        nat = ec2.create_nat_gateway(SubnetId=subnet_id, AllocationId=eip["AllocationId"])
        nat_id = nat["NatGateway"]["NatGatewayId"]
        rtb = ec2.create_route_table(VpcId=vpc_id)
        rtb_id = rtb["RouteTable"]["RouteTableId"]

        ec2.create_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0", NatGatewayId=nat_id)

        desc = ec2.describe_route_tables(RouteTableIds=[rtb_id])
        routes = desc["RouteTables"][0]["Routes"]
        nat_route = [r for r in routes if r.get("DestinationCidrBlock") == "0.0.0.0/0"][0]
        assert nat_route.get("NatGatewayId") == nat_id
        assert nat_route.get("GatewayId", "") == ""

        ec2.delete_route(RouteTableId=rtb_id, DestinationCidrBlock="0.0.0.0/0")
        ec2.delete_route_table(RouteTableId=rtb_id)
        ec2.delete_nat_gateway(NatGatewayId=nat_id)
        ec2.release_address(AllocationId=eip["AllocationId"])
        ec2.delete_subnet(SubnetId=subnet_id)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

def test_ec2_full_terraform_vpc_flow(ec2):
    """End-to-end Terraform VPC module flow: VPC → subnets → IGW → NAT → routes → associations."""
    # 1. Create VPC
    vpc = ec2.create_vpc(CidrBlock="10.50.0.0/16")
    vpc_id = vpc["Vpc"]["VpcId"]
    try:
        # 2. Verify default resources
        acls = ec2.describe_network_acls(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "default", "Values": ["true"]},
        ])
        assert len(acls["NetworkAcls"]) == 1

        sgs = ec2.describe_security_groups(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": ["default"]},
        ])
        assert len(sgs["SecurityGroups"]) == 1

        main_rtbs = ec2.describe_route_tables(Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "association.main", "Values": ["true"]},
        ])
        assert len(main_rtbs["RouteTables"]) == 1

        # 3. Create 6 subnets
        subnets = []
        for cidr, az in [
            ("10.50.0.0/20", "us-east-1a"), ("10.50.16.0/20", "us-east-1b"), ("10.50.32.0/20", "us-east-1c"),
            ("10.50.64.0/20", "us-east-1a"), ("10.50.80.0/20", "us-east-1b"), ("10.50.96.0/20", "us-east-1c"),
        ]:
            s = ec2.create_subnet(VpcId=vpc_id, CidrBlock=cidr, AvailabilityZone=az)
            subnets.append(s["Subnet"]["SubnetId"])

        # 4. IGW
        igw = ec2.create_internet_gateway()
        igw_id = igw["InternetGateway"]["InternetGatewayId"]
        ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)

        # 5. EIP + NAT
        eip = ec2.allocate_address(Domain="vpc")
        nat = ec2.create_nat_gateway(SubnetId=subnets[3], AllocationId=eip["AllocationId"])
        nat_id = nat["NatGateway"]["NatGatewayId"]

        # 6. Public + private route tables
        pub_rtb = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]
        priv_rtb = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]

        # 7. Associate subnets (3 public, 3 private)
        assoc_ids = []
        for i in range(3):
            a = ec2.associate_route_table(RouteTableId=pub_rtb, SubnetId=subnets[i + 3])
            assoc_ids.append(a["AssociationId"])
            # Verify filter works
            found = ec2.describe_route_tables(Filters=[
                {"Name": "association.route-table-association-id", "Values": [a["AssociationId"]]},
            ])
            assert len(found["RouteTables"]) == 1
        for i in range(3):
            a = ec2.associate_route_table(RouteTableId=priv_rtb, SubnetId=subnets[i])
            assoc_ids.append(a["AssociationId"])

        # 8. Routes
        ec2.create_route(RouteTableId=pub_rtb, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw_id)
        ec2.create_route(RouteTableId=priv_rtb, DestinationCidrBlock="0.0.0.0/0", NatGatewayId=nat_id)

        # Verify NAT route
        desc = ec2.describe_route_tables(RouteTableIds=[priv_rtb])
        nat_route = [r for r in desc["RouteTables"][0]["Routes"] if r.get("DestinationCidrBlock") == "0.0.0.0/0"][0]
        assert nat_route.get("NatGatewayId") == nat_id

        # 9. Cleanup
        ec2.delete_route(RouteTableId=pub_rtb, DestinationCidrBlock="0.0.0.0/0")
        ec2.delete_route(RouteTableId=priv_rtb, DestinationCidrBlock="0.0.0.0/0")
        for aid in assoc_ids:
            ec2.disassociate_route_table(AssociationId=aid)
        ec2.delete_route_table(RouteTableId=pub_rtb)
        ec2.delete_route_table(RouteTableId=priv_rtb)
        ec2.delete_nat_gateway(NatGatewayId=nat_id)
        ec2.release_address(AllocationId=eip["AllocationId"])
        ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
        ec2.delete_internet_gateway(InternetGatewayId=igw_id)
        for sid in subnets:
            ec2.delete_subnet(SubnetId=sid)
    finally:
        ec2.delete_vpc(VpcId=vpc_id)

# ---------------------------------------------------------------------------
# EC2 Launch Templates
# ---------------------------------------------------------------------------

def test_ec2_launch_template_crud(ec2):
    """Create, describe, and delete a launch template."""
    resp = ec2.create_launch_template(
        LaunchTemplateName="qa-lt-basic",
        LaunchTemplateData={
            "InstanceType": "t3.micro",
            "ImageId": "ami-12345678",
            "KeyName": "my-key",
        },
    )
    lt = resp["LaunchTemplate"]
    lt_id = lt["LaunchTemplateId"]
    assert lt_id.startswith("lt-")
    assert lt["LaunchTemplateName"] == "qa-lt-basic"
    assert lt["DefaultVersionNumber"] == 1
    assert lt["LatestVersionNumber"] == 1

    # Describe
    desc = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])
    assert len(desc["LaunchTemplates"]) == 1
    assert desc["LaunchTemplates"][0]["LaunchTemplateName"] == "qa-lt-basic"

    # Describe by name
    desc2 = ec2.describe_launch_templates(LaunchTemplateNames=["qa-lt-basic"])
    assert len(desc2["LaunchTemplates"]) == 1

    # Describe versions
    versions = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
    assert len(versions["LaunchTemplateVersions"]) == 1
    ver = versions["LaunchTemplateVersions"][0]
    assert ver["VersionNumber"] == 1
    assert ver["LaunchTemplateData"]["InstanceType"] == "t3.micro"
    assert ver["LaunchTemplateData"]["ImageId"] == "ami-12345678"

    # Delete
    ec2.delete_launch_template(LaunchTemplateId=lt_id)
    desc3 = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])
    assert len(desc3["LaunchTemplates"]) == 0


def test_ec2_launch_template_duplicate_name(ec2):
    """Creating a template with a duplicate name should fail."""
    ec2.create_launch_template(
        LaunchTemplateName="qa-lt-dup",
        LaunchTemplateData={"InstanceType": "t3.micro"},
    )
    with pytest.raises(ClientError) as exc:
        ec2.create_launch_template(
            LaunchTemplateName="qa-lt-dup",
            LaunchTemplateData={"InstanceType": "t3.small"},
        )
    assert "AlreadyExists" in exc.value.response["Error"]["Code"]
    # Cleanup
    ec2.delete_launch_template(LaunchTemplateName="qa-lt-dup")


def test_ec2_launch_template_versions(ec2):
    """Create multiple versions and query $Latest / $Default."""
    resp = ec2.create_launch_template(
        LaunchTemplateName="qa-lt-ver",
        LaunchTemplateData={"InstanceType": "t3.micro", "ImageId": "ami-v1"},
    )
    lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]

    # Create version 2
    ec2.create_launch_template_version(
        LaunchTemplateId=lt_id,
        LaunchTemplateData={"InstanceType": "t3.small", "ImageId": "ami-v2"},
        VersionDescription="version two",
    )
    # Create version 3
    ec2.create_launch_template_version(
        LaunchTemplateId=lt_id,
        LaunchTemplateData={"InstanceType": "t3.large", "ImageId": "ami-v3"},
    )

    # Latest should be version 3
    latest = ec2.describe_launch_template_versions(
        LaunchTemplateId=lt_id, Versions=["$Latest"],
    )
    assert len(latest["LaunchTemplateVersions"]) == 1
    assert latest["LaunchTemplateVersions"][0]["VersionNumber"] == 3
    assert latest["LaunchTemplateVersions"][0]["LaunchTemplateData"]["InstanceType"] == "t3.large"

    # Default should still be version 1
    default = ec2.describe_launch_template_versions(
        LaunchTemplateId=lt_id, Versions=["$Default"],
    )
    assert default["LaunchTemplateVersions"][0]["VersionNumber"] == 1

    # All versions
    all_ver = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
    assert len(all_ver["LaunchTemplateVersions"]) == 3

    # Modify default to version 2
    ec2.modify_launch_template(LaunchTemplateId=lt_id, DefaultVersion="2")
    desc = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])
    assert desc["LaunchTemplates"][0]["DefaultVersionNumber"] == 2

    default2 = ec2.describe_launch_template_versions(
        LaunchTemplateId=lt_id, Versions=["$Default"],
    )
    assert default2["LaunchTemplateVersions"][0]["VersionNumber"] == 2

    # Cleanup
    ec2.delete_launch_template(LaunchTemplateId=lt_id)


def test_ec2_launch_template_with_block_devices(ec2):
    """Create a template with block device mappings."""
    resp = ec2.create_launch_template(
        LaunchTemplateName="qa-lt-bdm",
        LaunchTemplateData={
            "InstanceType": "t3.micro",
            "BlockDeviceMappings": [
                {
                    "DeviceName": "/dev/xvda",
                    "Ebs": {
                        "VolumeSize": 50,
                        "VolumeType": "gp3",
                        "Encrypted": True,
                        "DeleteOnTermination": True,
                    },
                }
            ],
        },
    )
    lt_id = resp["LaunchTemplate"]["LaunchTemplateId"]

    versions = ec2.describe_launch_template_versions(LaunchTemplateId=lt_id)
    data = versions["LaunchTemplateVersions"][0]["LaunchTemplateData"]
    assert len(data["BlockDeviceMappings"]) == 1
    bdm = data["BlockDeviceMappings"][0]
    assert bdm["DeviceName"] == "/dev/xvda"
    assert bdm["Ebs"]["VolumeSize"] == 50
    assert bdm["Ebs"]["VolumeType"] == "gp3"

    ec2.delete_launch_template(LaunchTemplateId=lt_id)


def test_ec2_launch_template_not_found(ec2):
    """Describe/delete a non-existent template should fail."""
    with pytest.raises(ClientError) as exc:
        ec2.describe_launch_template_versions(LaunchTemplateId="lt-nonexistent")
    assert "NotFound" in exc.value.response["Error"]["Code"]


def test_ec2_default_subnets_three_azs(ec2):
    """Default VPC should have 3 subnets, one per AZ (a/b/c) with correct CIDRs."""
    resp = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": ["vpc-00000001"]}])
    subnets = resp["Subnets"]
    assert len(subnets) >= 3

    by_az = {s["AvailabilityZone"]: s for s in subnets}
    assert "us-east-1a" in by_az
    assert "us-east-1b" in by_az
    assert "us-east-1c" in by_az

    assert by_az["us-east-1a"]["CidrBlock"] == "172.31.0.0/20"
    assert by_az["us-east-1b"]["CidrBlock"] == "172.31.16.0/20"
    assert by_az["us-east-1c"]["CidrBlock"] == "172.31.32.0/20"

    for s in subnets:
        assert s["DefaultForAz"] is True
        assert s["MapPublicIpOnLaunch"] is True


def test_ec2_describe_subnets_tags_filters(ec2):
    vpc_id = ec2.create_vpc(CidrBlock="10.77.0.0/16")["Vpc"]["VpcId"]
    subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.77.1.0/24")["Subnet"]["SubnetId"]
    ec2.create_tags(Resources=[subnet_id], Tags=[{"Key": "Tier", "Value": "private"}, {"Key": "Env", "Value": "dev"}])

    resp = ec2.describe_subnets(Filters=[
        {"Name": "vpc-id", "Values": [vpc_id]},
        {"Name": "tag:Tier", "Values": ["private"]},
    ])
    assert any(s["SubnetId"] == subnet_id for s in resp["Subnets"])

    resp = ec2.describe_subnets(Filters=[{"Name": "tag-key", "Values": ["Tier"]}])
    assert any(s["SubnetId"] == subnet_id for s in resp["Subnets"])

    resp = ec2.describe_subnets(Filters=[
        {"Name": "vpc-id", "Values": [vpc_id]},
        {"Name": "tag:Tier", "Values": ["public"]},
    ])
    assert all(s["SubnetId"] != subnet_id for s in resp["Subnets"])

    ec2.delete_subnet(SubnetId=subnet_id)
    ec2.delete_vpc(VpcId=vpc_id)


def test_ec2_describe_tags_filters(ec2):
    """DescribeTags respects resource-id and key filters."""
    # Create two instances and tag them differently
    r1 = ec2.run_instances(ImageId="ami-test1", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    r2 = ec2.run_instances(ImageId="ami-test2", InstanceType="t2.micro", MinCount=1, MaxCount=1)
    id1 = r1["Instances"][0]["InstanceId"]
    id2 = r2["Instances"][0]["InstanceId"]

    ec2.create_tags(Resources=[id1], Tags=[{"Key": "Name", "Value": "first"}, {"Key": "Env", "Value": "prod"}])
    ec2.create_tags(Resources=[id2], Tags=[{"Key": "Name", "Value": "second"}])

    # Filter by resource-id — should only return tags for id1
    resp = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": [id1]}])
    tags = resp["Tags"]
    assert all(t["ResourceId"] == id1 for t in tags)
    assert len(tags) == 2

    # Filter by key — should return "Env" tag only for id1
    resp = ec2.describe_tags(Filters=[{"Name": "key", "Values": ["Env"]}])
    tags = resp["Tags"]
    assert all(t["Key"] == "Env" for t in tags)
    assert any(t["ResourceId"] == id1 for t in tags)

    # Filter by resource-id + key — should return exactly one tag
    resp = ec2.describe_tags(Filters=[
        {"Name": "resource-id", "Values": [id1]},
        {"Name": "key", "Values": ["Name"]},
    ])
    tags = resp["Tags"]
    assert len(tags) == 1
    assert tags[0]["ResourceId"] == id1
    assert tags[0]["Key"] == "Name"
    assert tags[0]["Value"] == "first"

    # Filter by resource-id that has no tags — should return empty
    resp = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": ["i-doesnotexist"]}])
    assert len(resp["Tags"]) == 0

    # All tags have correct resource type
    resp = ec2.describe_tags(Filters=[{"Name": "resource-id", "Values": [id1, id2]}])
    assert all(t["ResourceType"] == "instance" for t in resp["Tags"])


def test_ec2_default_vpc_network_acl(ec2):
    """Default VPC's network ACL should exist and be queryable."""
    resp = ec2.describe_network_acls(
        Filters=[{"Name": "default", "Values": ["true"]}]
    )
    acls = resp["NetworkAcls"]
    assert len(acls) >= 1
    default_acl = acls[0]
    assert default_acl["IsDefault"] is True
    # Should have both allow and deny entries
    assert len(default_acl["Entries"]) >= 4


def test_ec2_create_default_vpc_already_exists(ec2):
    """CreateDefaultVpc should fail when a default VPC already exists."""
    with pytest.raises(ClientError) as exc:
        ec2.create_default_vpc()
    assert exc.value.response["Error"]["Code"] == "DefaultVpcAlreadyExists"


def test_ec2_create_default_vpc(ec2):
    """CreateDefaultVpc should create a VPC with subnets, IGW, SG, route table, ACL."""
    # First, find and delete the existing default VPC and its dependencies
    vpcs = ec2.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"]
    if vpcs:
        default_vpc_id = vpcs[0]["VpcId"]
        # Delete subnets
        for s in ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [default_vpc_id]}])["Subnets"]:
            ec2.delete_subnet(SubnetId=s["SubnetId"])
        # Delete non-default security groups (other tests may have created them)
        for sg in ec2.describe_security_groups(
            Filters=[{"Name": "vpc-id", "Values": [default_vpc_id]}]
        )["SecurityGroups"]:
            if sg["GroupName"] != "default":
                ec2.delete_security_group(GroupId=sg["GroupId"])
        # Detach and delete IGWs
        for igw in ec2.describe_internet_gateways(
            Filters=[{"Name": "attachment.vpc-id", "Values": [default_vpc_id]}]
        )["InternetGateways"]:
            ec2.detach_internet_gateway(InternetGatewayId=igw["InternetGatewayId"], VpcId=default_vpc_id)
            ec2.delete_internet_gateway(InternetGatewayId=igw["InternetGatewayId"])
        ec2.delete_vpc(VpcId=default_vpc_id)

    # Now create a new default VPC
    resp = ec2.create_default_vpc()
    vpc = resp["Vpc"]
    assert vpc["IsDefault"] is True
    assert vpc["CidrBlock"] == "172.31.0.0/16"
    assert vpc["State"] == "available"

    vpc_id = vpc["VpcId"]

    # Verify 3 default subnets were created
    subnets = ec2.describe_subnets(
        Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
    )["Subnets"]
    assert len(subnets) == 3
    for s in subnets:
        assert s["DefaultForAz"] is True
        assert s["MapPublicIpOnLaunch"] is True

    # Verify IGW attached
    igws = ec2.describe_internet_gateways(
        Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}]
    )["InternetGateways"]
    assert len(igws) == 1

    # Verify calling again fails
    with pytest.raises(ClientError) as exc:
        ec2.create_default_vpc()
    assert exc.value.response["Error"]["Code"] == "DefaultVpcAlreadyExists"
