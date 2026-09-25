"""CloudFormation updates of EC2 networking: VPCs, subnets, security groups, launch templates, gateways, routes."""

import json
import uuid as _uuid_mod

from botocore.exceptions import ClientError
from test_cfn import (
    _FAILING_RESOURCE,
    _cfn_output,
    _cfn_with_failing_resource,
    _delete_cfn_test_stack,
    _output,
    _template_tags,
    _wait_stack,
)


def test_cfn_ec2_vpc_update_keeps_id_and_leaves_no_default_children(cfn, ec2):
    """EnableDnsHostnames is No interruption on AWS::EC2::VPC, so the VPC keeps
    its id. The create mints a new one and also writes a default security
    group, main route table and network ACL, none of which the CFN delete
    removes, so every update leaked all three."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-vpc-upd-{suffix}"

    def template(dns):
        return json.dumps({
            "Resources": {
                "Vpc": {
                    "Type": "AWS::EC2::VPC",
                    "Properties": {
                        "CidrBlock": "10.42.0.0/16",
                        "EnableDnsHostnames": dns,
                        "Tags": [{"Key": "probe", "Value": str(dns)}],
                    },
                },
            },
            "Outputs": {"VpcId": {"Value": {"Ref": "Vpc"}}},
        })

    def vpc_id():
        return next(o["OutputValue"] for o in
                    cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
                    if o["OutputKey"] == "VpcId")

    def children_of(vid):
        sgs = [g for g in ec2.describe_security_groups()["SecurityGroups"]
               if g.get("VpcId") == vid]
        rtbs = [r for r in ec2.describe_route_tables()["RouteTables"]
                if r.get("VpcId") == vid]
        acls = [a for a in ec2.describe_network_acls()["NetworkAcls"]
                if a.get("VpcId") == vid]
        return sgs, rtbs, acls

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template(False))
        _wait_stack(cfn, stack_name)
        before = vpc_id()

        cfn.update_stack(StackName=stack_name, TemplateBody=template(True))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        assert vpc_id() == before, "the VPC came back under a new id"
        sgs, rtbs, acls = children_of(before)
        assert len(sgs) == 1 and len(rtbs) == 1 and len(acls) == 1, (
            "the update left a second set of default children behind: "
            f"{len(sgs)} security groups, {len(rtbs)} route tables, {len(acls)} ACLs")
        assert ec2.describe_vpc_attribute(
            VpcId=before, Attribute="enableDnsHostnames"
        )["EnableDnsHostnames"]["Value"] is True
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def _cfn_vpc_children(ec2, vpc_id):
    return (
        [g["GroupId"] for g in ec2.describe_security_groups()["SecurityGroups"]
         if g.get("VpcId") == vpc_id],
        [r["RouteTableId"] for r in ec2.describe_route_tables()["RouteTables"]
         if r.get("VpcId") == vpc_id],
        [a["NetworkAclId"] for a in ec2.describe_network_acls()["NetworkAcls"]
         if a.get("VpcId") == vpc_id],
    )


def test_cfn_ec2_vpc_stack_delete_removes_its_default_children(cfn, ec2):
    """DeleteVpc removes the default security group, main route table and
    default network ACL with the VPC, so a stack delete leaves none of the
    three behind. The CFN delete popped only the VPC, which left all three
    pointing at a VPC id that no longer existed."""
    stack_name = f"cfn-vpc-del-{_uuid_mod.uuid4().hex[:8]}"
    template = json.dumps({
        "Resources": {"Vpc": {"Type": "AWS::EC2::VPC",
                              "Properties": {"CidrBlock": "10.47.0.0/16"}}},
        "Outputs": {"VpcId": {"Value": {"Ref": "Vpc"}}},
    })
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        vpc_id = _output(_wait_stack(cfn, stack_name), "VpcId")
        sgs, rtbs, acls = _cfn_vpc_children(ec2, vpc_id)
        assert len(sgs) == 1 and len(rtbs) == 1 and len(acls) == 1

        cfn.delete_stack(StackName=stack_name)
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"

        assert _cfn_vpc_children(ec2, vpc_id) == ([], [], []), (
            "the stack delete left the VPC's default children behind")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_vpc_stack_delete_keeps_a_security_group_made_outside_it(cfn, ec2):
    """AWS refuses DeleteVpc with DependencyViolation while a non-default
    security group lives in the VPC, so a stack whose VPC holds a group made
    outside the stack ends DELETE_FAILED and the group survives. The CFN
    delete popped every security group with the VPC's id, the foreign one
    included."""
    stack_name = f"cfn-vpc-dep-{_uuid_mod.uuid4().hex[:8]}"
    template = json.dumps({
        "Resources": {"Vpc": {"Type": "AWS::EC2::VPC",
                              "Properties": {"CidrBlock": "10.48.0.0/16"}}},
        "Outputs": {"VpcId": {"Value": {"Ref": "Vpc"}}},
    })
    group_id = None
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template)
        vpc_id = _output(_wait_stack(cfn, stack_name), "VpcId")
        group_id = ec2.create_security_group(
            GroupName=f"foreign-{stack_name}", Description="outside the stack",
            VpcId=vpc_id,
        )["GroupId"]

        cfn.delete_stack(StackName=stack_name)
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_FAILED"

        groups = ec2.describe_security_groups(GroupIds=[group_id])["SecurityGroups"]
        assert [g["GroupId"] for g in groups] == [group_id]
        assert ec2.describe_vpcs(VpcIds=[vpc_id])["Vpcs"]
    finally:
        if group_id:
            try:
                ec2.delete_security_group(GroupId=group_id)
            except ClientError:
                pass
        _delete_cfn_test_stack(cfn, stack_name)


def _cfn_vpc_attribute(ec2, vpc_id, attribute):
    key = attribute[0].upper() + attribute[1:]
    return ec2.describe_vpc_attribute(VpcId=vpc_id, Attribute=attribute)[key]["Value"]


def test_cfn_ec2_vpc_dns_property_removed_returns_to_its_default(cfn, ec2):
    """A DNS property the template stops setting goes back to the VPC's
    default, which is what AWS does (measured 2026-09-21): a removed
    EnableDnsHostnames reads false, a removed EnableDnsSupport true. The
    update only wrote a property that was present, so a removed one kept the
    value the template had set."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-vpc-dns-{suffix}"

    def template(both, off):
        return json.dumps({
            "Resources": {
                "Both": {"Type": "AWS::EC2::VPC", "Properties": {
                    "CidrBlock": "10.45.0.0/16", **both}},
                "Off": {"Type": "AWS::EC2::VPC", "Properties": {
                    "CidrBlock": "10.46.0.0/16", **off}},
            },
            "Outputs": {"Both": {"Value": {"Ref": "Both"}}, "Off": {"Value": {"Ref": "Off"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(
        {"EnableDnsSupport": True, "EnableDnsHostnames": True}, {"EnableDnsSupport": False}))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        both, off = _cfn_output(cfn, stack_name, "Both"), _cfn_output(cfn, stack_name, "Off")
        assert _cfn_vpc_attribute(ec2, both, "enableDnsHostnames") is True
        assert _cfn_vpc_attribute(ec2, off, "enableDnsSupport") is False

        cfn.update_stack(StackName=stack_name, TemplateBody=template(
            {"EnableDnsSupport": True}, {}))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_output(cfn, stack_name, "Both") == both
        assert _cfn_output(cfn, stack_name, "Off") == off
        assert _cfn_vpc_attribute(ec2, both, "enableDnsHostnames") is False
        assert _cfn_vpc_attribute(ec2, both, "enableDnsSupport") is True
        assert _cfn_vpc_attribute(ec2, off, "enableDnsSupport") is True
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_vpc_tag_change_keeps_tags_added_through_the_api(cfn, ec2):
    """Tags is No interruption on AWS::EC2::VPC, and on AWS a template tag
    change leaves a tag added through CreateTags alone (measured 2026-09-21:
    a=1 -> a=2 plus b=3, api=kept stays). The update overwrote the whole tag
    list."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-vpc-tags-{suffix}"

    def template(tags):
        return json.dumps({
            "Resources": {"Vpc": {"Type": "AWS::EC2::VPC", "Properties": {
                "CidrBlock": "10.47.0.0/16", "Tags": tags}}},
            "Outputs": {"VpcId": {"Value": {"Ref": "Vpc"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template([{"Key": "a", "Value": "1"}]))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        vpc_id = _cfn_output(cfn, stack_name, "VpcId")
        ec2.create_tags(Resources=[vpc_id], Tags=[{"Key": "api", "Value": "kept"}])

        cfn.update_stack(StackName=stack_name, TemplateBody=template(
            [{"Key": "a", "Value": "2"}, {"Key": "b", "Value": "3"}]))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        tags = ec2.describe_vpcs(VpcIds=[vpc_id])["Vpcs"][0].get("Tags", [])
        assert sorted((t["Key"], t["Value"]) for t in tags
                      if not t["Key"].startswith("aws:")) == [
            ("a", "2"), ("api", "kept"), ("b", "3")]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_vpc_dns_hostnames_change_is_rolled_back(cfn, ec2):
    """EnableDnsHostnames is applied in place, so a later failure in the same
    update has to set it back. Measured on AWS 2026-09-21: the VPC reads
    enableDnsHostnames false again after UPDATE_ROLLBACK_COMPLETE."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-vpc-rb-{suffix}"

    def template(hostnames, with_bad):
        resources = {"Vpc": {"Type": "AWS::EC2::VPC", "Properties": {
            "CidrBlock": "10.48.0.0/16", "EnableDnsSupport": True,
            "EnableDnsHostnames": hostnames}}}
        if with_bad:
            resources["Bad"] = {**_FAILING_RESOURCE, "DependsOn": "Vpc"}
        return json.dumps({"Resources": resources,
                           "Outputs": {"VpcId": {"Value": {"Ref": "Vpc"}}}})

    cfn.create_stack(StackName=stack_name, TemplateBody=template(False, False))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        vpc_id = _cfn_output(cfn, stack_name, "VpcId")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(True, True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_output(cfn, stack_name, "VpcId") == vpc_id
        assert _cfn_vpc_attribute(ec2, vpc_id, "enableDnsHostnames") is False
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_vpc_create_only_ipam_members_replace_the_vpc(cfn, ec2):
    """Ipv4IpamPoolId, Ipv4NetmaskLength and VpcEncryptionControl are
    create-only on AWS::EC2::VPC (the registry's createOnlyProperties), so a
    change replaces the VPC. The handler keyed its replacement on CidrBlock
    and tenancy alone and applied such a change as an in-place no-op."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-vpc-ipam-{suffix}"

    def template(netmask):
        return json.dumps({
            "Resources": {"Vpc": {"Type": "AWS::EC2::VPC", "Properties": {
                "Ipv4IpamPoolId": "ipam-pool-0123456789abcdef0",
                "Ipv4NetmaskLength": netmask}}},
            "Outputs": {"VpcId": {"Value": {"Ref": "Vpc"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template(24))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        before = _cfn_output(cfn, stack_name, "VpcId")
        cfn.update_stack(StackName=stack_name, TemplateBody=template(28))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        after = _cfn_output(cfn, stack_name, "VpcId")
        assert after != before
        assert not ec2.describe_vpcs(Filters=[{"Name": "vpc-id", "Values": [before]}])["Vpcs"]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_subnet_update_keeps_id(cfn, ec2):
    """MapPublicIpOnLaunch is No interruption on AWS::EC2::Subnet, so the
    subnet keeps its id; anything holding it (instances, ENIs, load balancer
    subnet lists) would otherwise dangle."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-subnet-upd-{suffix}"

    def template(public):
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC",
                        "Properties": {"CidrBlock": "10.43.0.0/16"}},
                "Subnet": {
                    "Type": "AWS::EC2::Subnet",
                    "Properties": {
                        "VpcId": {"Ref": "Vpc"},
                        "CidrBlock": "10.43.1.0/24",
                        "MapPublicIpOnLaunch": public,
                    },
                },
            },
            "Outputs": {"SubnetId": {"Value": {"Ref": "Subnet"}}},
        })

    def subnet_id():
        return next(o["OutputValue"] for o in
                    cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
                    if o["OutputKey"] == "SubnetId")

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template(False))
        _wait_stack(cfn, stack_name)
        before = subnet_id()

        cfn.update_stack(StackName=stack_name, TemplateBody=template(True))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        assert subnet_id() == before, "the subnet came back under a new id"
        described = ec2.describe_subnets(SubnetIds=[before])["Subnets"][0]
        assert described["MapPublicIpOnLaunch"] is True
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_subnet_keeps_map_public_ip_when_the_property_is_removed(cfn, ec2):
    """A template that stops setting MapPublicIpOnLaunch leaves the subnet's
    attribute as it is: measured on AWS 2026-09-21, true stays true after the
    property is removed. The update wrote the default false instead."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-subnet-map-{suffix}"

    def template(extra):
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.49.0.0/16"}},
                "Subnet": {"Type": "AWS::EC2::Subnet", "Properties": {
                    "VpcId": {"Ref": "Vpc"}, "CidrBlock": "10.49.1.0/24", **extra}},
            },
            "Outputs": {"SubnetId": {"Value": {"Ref": "Subnet"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template({"MapPublicIpOnLaunch": True}))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        subnet_id = _cfn_output(cfn, stack_name, "SubnetId")

        cfn.update_stack(StackName=stack_name, TemplateBody=template({}))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_output(cfn, stack_name, "SubnetId") == subnet_id
        described = ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"][0]
        assert described["MapPublicIpOnLaunch"] is True
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_subnet_create_only_members_replace_the_subnet(cfn, ec2):
    """AvailabilityZoneId, the IPAM members, Ipv6Native and OutpostArn are
    create-only on AWS::EC2::Subnet (the registry's createOnlyProperties).
    The handler keyed its replacement on VpcId, CidrBlock and
    AvailabilityZone alone, so a zone given by id was ignored at create and
    its change applied as an in-place no-op."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-subnet-azid-{suffix}"

    def template(zone_id):
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.53.0.0/16"}},
                "Subnet": {"Type": "AWS::EC2::Subnet", "Properties": {
                    "VpcId": {"Ref": "Vpc"}, "CidrBlock": "10.53.1.0/24",
                    "AvailabilityZoneId": zone_id}},
            },
            "Outputs": {"SubnetId": {"Value": {"Ref": "Subnet"}}},
        })

    cfn.create_stack(StackName=stack_name, TemplateBody=template("use1-az2"))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        before = _cfn_output(cfn, stack_name, "SubnetId")
        subnet = ec2.describe_subnets(SubnetIds=[before])["Subnets"][0]
        assert (subnet["AvailabilityZoneId"], subnet["AvailabilityZone"]) == ("use1-az2", "us-east-1b")
        cfn.update_stack(StackName=stack_name, TemplateBody=template("use1-az3"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        after = _cfn_output(cfn, stack_name, "SubnetId")
        assert after != before
        assert ec2.describe_subnets(SubnetIds=[after])["Subnets"][0]["AvailabilityZoneId"] == "use1-az3"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_subnet_dns_name_options_update_in_place(cfn, ec2):
    """PrivateDnsNameOptionsOnLaunch is No interruption on AWS::EC2::Subnet.
    AWS answers it (and EnableDns64, Ipv6Native, AssignIpv6AddressOnCreation,
    false by default) in DescribeSubnets, applies a change in place, keeps
    the value when the template removes the property, and fails EnableDns64
    on a subnet without an IPv6 block (measured 2026-09-21). None of the
    four was stored or answered."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-subnet-dns-{suffix}"

    def template(extra):
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.55.0.0/16"}},
                "Subnet": {"Type": "AWS::EC2::Subnet", "Properties": {
                    "VpcId": {"Ref": "Vpc"}, "CidrBlock": "10.55.1.0/24", **extra}},
            },
            "Outputs": {"SubnetId": {"Value": {"Ref": "Subnet"}}},
        })

    def attributes(subnet_id):
        subnet = ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"][0]
        return {key: subnet.get(key) for key in (
            "PrivateDnsNameOptionsOnLaunch", "EnableDns64", "Ipv6Native",
            "AssignIpv6AddressOnCreation")}

    def options(hostname_type, a_record):
        return {"HostnameType": hostname_type, "EnableResourceNameDnsARecord": a_record,
                "EnableResourceNameDnsAAAARecord": False}

    resource_name = {"HostnameType": "resource-name", "EnableResourceNameDnsARecord": True}
    cfn.create_stack(StackName=stack_name, TemplateBody=template(
        {"PrivateDnsNameOptionsOnLaunch": resource_name}))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        subnet_id = _cfn_output(cfn, stack_name, "SubnetId")
        defaults = {"EnableDns64": False, "Ipv6Native": False, "AssignIpv6AddressOnCreation": False}
        assert attributes(subnet_id) == {
            "PrivateDnsNameOptionsOnLaunch": options("resource-name", True), **defaults}

        cfn.update_stack(StackName=stack_name, TemplateBody=template({
            "PrivateDnsNameOptionsOnLaunch": {
                "HostnameType": "ip-name", "EnableResourceNameDnsARecord": False}}))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert _cfn_output(cfn, stack_name, "SubnetId") == subnet_id
        assert attributes(subnet_id)["PrivateDnsNameOptionsOnLaunch"] == options("ip-name", False)

        cfn.update_stack(StackName=stack_name, TemplateBody=template(
            {"PrivateDnsNameOptionsOnLaunch": resource_name}))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        cfn.update_stack(StackName=stack_name, TemplateBody=template({}))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert attributes(subnet_id)["PrivateDnsNameOptionsOnLaunch"] == options("resource-name", True)

        cfn.update_stack(StackName=stack_name, TemplateBody=template({"EnableDns64": True}))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE"
        reasons = [e.get("ResourceStatusReason", "")
                   for e in cfn.describe_stack_events(StackName=stack_name)["StackEvents"]
                   if e["LogicalResourceId"] == "Subnet" and e["ResourceStatus"] == "UPDATE_FAILED"]
        assert reasons and "Property Ipv6CidrBlock or Ipv6IpamPoolId cannot be empty." in reasons[0]
        assert attributes(subnet_id)["EnableDns64"] is False
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_security_group_update_keeps_id_and_rules(cfn, ec2):
    """SecurityGroupIngress is "Some interruptions" on
    AWS::EC2::SecurityGroup, which is an in-place update, and Tags is No
    interruption. The create mints a new group id, so every rule added through
    AuthorizeSecurityGroupIngress went with the old group."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sg-upd-{suffix}"

    def template(port):
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC",
                        "Properties": {"CidrBlock": "10.44.0.0/16"}},
                "Sg": {
                    "Type": "AWS::EC2::SecurityGroup",
                    "Properties": {
                        "GroupDescription": "probe",
                        "VpcId": {"Ref": "Vpc"},
                        "SecurityGroupIngress": [
                            {"IpProtocol": "tcp", "FromPort": port,
                             "ToPort": port, "CidrIp": "10.0.0.0/8"},
                        ],
                    },
                },
            },
            "Outputs": {"SgId": {"Value": {"Fn::GetAtt": ["Sg", "GroupId"]}}},
        })

    def sg_id():
        return next(o["OutputValue"] for o in
                    cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
                    if o["OutputKey"] == "SgId")

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template(80))
        _wait_stack(cfn, stack_name)
        before = sg_id()
        # A rule added out of band is what the replacement destroys.
        ec2.authorize_security_group_ingress(
            GroupId=before,
            IpPermissions=[{"IpProtocol": "tcp", "FromPort": 8443, "ToPort": 8443,
                            "IpRanges": [{"CidrIp": "192.0.2.0/24"}]}])

        cfn.update_stack(StackName=stack_name, TemplateBody=template(443))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        assert sg_id() == before, "the security group came back under a new id"
        perms = ec2.describe_security_groups(
            GroupIds=[before])["SecurityGroups"][0]["IpPermissions"]
        ports = sorted(p["FromPort"] for p in perms if "FromPort" in p)
        assert 443 in ports, "the template's new ingress rule was not applied"
        assert 8443 in ports, "the out-of-band rule was destroyed"
        assert 80 not in ports, "the template's old ingress rule survived"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_security_group_rule_members_update_in_place(cfn, ec2):
    """A rule's Description, a SourcePrefixListId source and a source group's
    name and owner are members of SecurityGroupIngress and
    SecurityGroupEgress. AWS applies a rule with a prefix-list source and
    changes a description in place (measured 2026-09-21); the provisioner
    dropped all of them, so a prefix-list rule was lost outright and two
    rules differing only in their prefix list compared equal."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sg-members-{suffix}"
    prefix_list = ec2.create_managed_prefix_list(
        PrefixListName=f"cfn-sg-members-{suffix}", MaxEntries=1, AddressFamily="IPv4",
        Entries=[{"Cidr": "198.51.100.0/24"}])["PrefixList"]["PrefixListId"]

    def template(ingress_description, egress_description, with_list):
        ingress = [{"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22,
                    "CidrIp": "10.0.0.0/8", "Description": ingress_description},
                   {"IpProtocol": "tcp", "FromPort": 8080, "ToPort": 8080,
                    "SourceSecurityGroupId": {"Fn::GetAtt": ["Vpc", "DefaultSecurityGroup"]},
                    "Description": "from the default group"}]
        if with_list:
            ingress.append({"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
                            "SourcePrefixListId": prefix_list, "Description": "from the list"})
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.57.0.0/16"}},
                "Sg": {"Type": "AWS::EC2::SecurityGroup", "Properties": {
                    "GroupDescription": "members", "VpcId": {"Ref": "Vpc"},
                    "SecurityGroupIngress": ingress,
                    "SecurityGroupEgress": [{"IpProtocol": "-1", "CidrIp": "0.0.0.0/0",
                                             "Description": egress_description}]}},
            },
            "Outputs": {"Sg": {"Value": {"Fn::GetAtt": ["Sg", "GroupId"]}}},
        })

    def rules(group_id, member):
        group = ec2.describe_security_groups(GroupIds=[group_id])["SecurityGroups"][0]
        flat = []
        for perm in group[member]:
            for entry in perm.get("IpRanges", []):
                flat.append((perm.get("FromPort"), entry["CidrIp"], entry.get("Description")))
            for entry in perm.get("PrefixListIds", []):
                flat.append((perm.get("FromPort"), entry["PrefixListId"], entry.get("Description")))
            for entry in perm.get("UserIdGroupPairs", []):
                flat.append((perm.get("FromPort"), "group", entry.get("Description")))
        return sorted(flat, key=str)

    cfn.create_stack(StackName=stack_name, TemplateBody=template("a", "e1", True))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        group_id = _cfn_output(cfn, stack_name, "Sg")
        listed = (443, prefix_list, "from the list")
        by_group = (8080, "group", "from the default group")
        assert rules(group_id, "IpPermissions") == sorted(
            [(22, "10.0.0.0/8", "a"), listed, by_group], key=str)
        assert rules(group_id, "IpPermissionsEgress") == [(None, "0.0.0.0/0", "e1")]

        cfn.update_stack(StackName=stack_name, TemplateBody=template("b", "e1", True))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert _cfn_output(cfn, stack_name, "Sg") == group_id
        assert rules(group_id, "IpPermissions") == sorted(
            [(22, "10.0.0.0/8", "b"), listed, by_group], key=str)

        cfn.update_stack(StackName=stack_name, TemplateBody=template("b", "e2", True))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert rules(group_id, "IpPermissionsEgress") == [(None, "0.0.0.0/0", "e2")]

        cfn.update_stack(StackName=stack_name, TemplateBody=template("b", "e2", False))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert rules(group_id, "IpPermissions") == sorted(
            [(22, "10.0.0.0/8", "b"), by_group], key=str)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        ec2.delete_managed_prefix_list(PrefixListId=prefix_list)


def test_cfn_ec2_security_group_egress_update_matches_the_create(cfn, ec2):
    """A group whose template declares no egress gets the allow-all egress
    rule, and one that declares egress gets only those rules. Measured on AWS
    2026-09-21: declaring egress in an update takes the allow-all rule away,
    and dropping the declaration again leaves the group with no egress rule
    from the template; allow-all does not come back. A rule authorized
    outside the template stays either way."""
    stack_name = f"cfn-sg-egress-{_uuid_mod.uuid4().hex[:8]}"

    def template(egress):
        props = {"GroupDescription": "egress", "VpcId": {"Ref": "Vpc"}}
        if egress:
            props["SecurityGroupEgress"] = [
                {"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443,
                 "CidrIp": "10.0.0.0/8"}]
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.58.0.0/16"}},
                "Sg": {"Type": "AWS::EC2::SecurityGroup", "Properties": props},
            },
            "Outputs": {"Sg": {"Value": {"Fn::GetAtt": ["Sg", "GroupId"]}}},
        })

    def egress(group_id):
        group = ec2.describe_security_groups(GroupIds=[group_id])["SecurityGroups"][0]
        return sorted(
            (perm["IpProtocol"], perm.get("FromPort"), entry["CidrIp"])
            for perm in group["IpPermissionsEgress"]
            for entry in perm.get("IpRanges", [])
        )

    allow_all = ("-1", None, "0.0.0.0/0")
    declared = ("tcp", 443, "10.0.0.0/8")
    foreign = ("tcp", 25, "192.0.2.0/24")
    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template(False))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        group_id = _cfn_output(cfn, stack_name, "Sg")
        ec2.authorize_security_group_egress(
            GroupId=group_id,
            IpPermissions=[{"IpProtocol": "tcp", "FromPort": 25, "ToPort": 25,
                            "IpRanges": [{"CidrIp": "192.0.2.0/24"}]}])
        assert egress(group_id) == sorted([allow_all, foreign])

        cfn.update_stack(StackName=stack_name, TemplateBody=template(True))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert egress(group_id) == sorted([declared, foreign]), (
            "declaring egress kept the allow-all rule the create would not add")

        cfn.update_stack(StackName=stack_name, TemplateBody=template(False))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert egress(group_id) == sorted([foreign]), (
            "dropping the egress declaration brought the allow-all rule back")
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_security_group_ingress_change_is_rolled_back(cfn, ec2):
    """An ingress rule change is applied in place, so a later failure in the
    same update has to take the new rule away again. Measured on AWS
    2026-09-21: the group's ingress ports read [22] again after
    UPDATE_ROLLBACK_COMPLETE, under the same group id."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-sg-rb-{suffix}"

    def template(ports, with_bad):
        resources = {
            "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.50.0.0/16"}},
            "Sg": {"Type": "AWS::EC2::SecurityGroup", "Properties": {
                "GroupDescription": "probe", "VpcId": {"Ref": "Vpc"},
                "SecurityGroupIngress": [
                    {"IpProtocol": "tcp", "FromPort": port, "ToPort": port,
                     "CidrIp": "10.0.0.0/8"} for port in ports]}},
        }
        if with_bad:
            resources["Bad"] = {**_FAILING_RESOURCE, "DependsOn": "Sg"}
        return json.dumps({"Resources": resources,
                           "Outputs": {"SgId": {"Value": {"Fn::GetAtt": ["Sg", "GroupId"]}}}})

    cfn.create_stack(StackName=stack_name, TemplateBody=template([22], False))
    try:
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        sg_id = _cfn_output(cfn, stack_name, "SgId")

        cfn.update_stack(StackName=stack_name, TemplateBody=template([22, 443], True))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_output(cfn, stack_name, "SgId") == sg_id
        perms = ec2.describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]["IpPermissions"]
        assert sorted(p["FromPort"] for p in perms if "FromPort" in p) == [22]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_launch_template_update_adds_a_version(cfn, ec2):
    """LaunchTemplateData is No interruption on AWS::EC2::LaunchTemplate: AWS
    adds version N+1 under the same template. The create minted a new lt- id
    and reset Versions and LatestVersionNumber to 1, which is exactly what an
    Auto Scaling group reads through Fn::GetAtt LatestVersionNumber."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-lt-upd-{suffix}"
    name = f"cfn-lt-upd-{suffix}"

    def template(instance_type):
        return json.dumps({
            "Resources": {
                "LT": {
                    "Type": "AWS::EC2::LaunchTemplate",
                    "Properties": {
                        "LaunchTemplateName": name,
                        "LaunchTemplateData": {
                            "ImageId": "ami-cfn7777",
                            "InstanceType": instance_type,
                        },
                    },
                },
            },
            "Outputs": {
                "LtId": {"Value": {"Ref": "LT"}},
                "Latest": {"Value": {"Fn::GetAtt": ["LT", "LatestVersionNumber"]}},
                "Default": {"Value": {"Fn::GetAtt": ["LT", "DefaultVersionNumber"]}},
            },
        })

    def outputs():
        return {o["OutputKey"]: o["OutputValue"] for o in
                cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]}

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("t3.micro"))
        _wait_stack(cfn, stack_name)
        before = outputs()

        cfn.update_stack(StackName=stack_name, TemplateBody=template("t3.large"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        after = outputs()
        assert after["LtId"] == before["LtId"], \
            "the launch template came back under a new id"
        assert after["Latest"] == "2", \
            f"the data change did not add a version (latest {after['Latest']})"
        versions = ec2.describe_launch_template_versions(
            LaunchTemplateId=before["LtId"])["LaunchTemplateVersions"]
        assert sorted(v["VersionNumber"] for v in versions) == [1, 2]
        latest = next(v for v in versions if v["VersionNumber"] == 2)
        assert latest["LaunchTemplateData"]["InstanceType"] == "t3.large"
        # The default version stays where it was: measured on AWS 2026-09-21,
        # a data change answers latest 2 and default 1.
        assert after["Default"] == "1"
        assert latest["DefaultVersion"] is False
        described = ec2.describe_launch_templates(
            LaunchTemplateIds=[before["LtId"]])["LaunchTemplates"][0]
        assert (described["LatestVersionNumber"], described["DefaultVersionNumber"]) == (2, 1)
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_launch_template_tags_come_from_tag_specifications(cfn, ec2):
    """AWS::EC2::LaunchTemplate has no Tags property: the template's own tags
    are the TagSpecifications entry for "launch-template", which the create
    ignored. Measured on AWS 2026-09-21: they are applied at creation only, so
    a TagSpecifications change adds a version and leaves the template's tags,
    and a tag added through CreateTags, as they were."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-lt-tags-{suffix}"

    def template(value):
        return json.dumps({
            "Resources": {"LT": {"Type": "AWS::EC2::LaunchTemplate", "Properties": {
                "LaunchTemplateData": {"ImageId": "ami-cfn7777", "InstanceType": "t3.micro"},
                "TagSpecifications": [
                    {"ResourceType": "launch-template",
                     "Tags": [{"Key": "v", "Value": value}]},
                    {"ResourceType": "instance",
                     "Tags": [{"Key": "launched", "Value": "yes"}]},
                ]}}},
            "Outputs": {"LtId": {"Value": {"Ref": "LT"}}},
        })

    def tags(lt_id):
        described = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])["LaunchTemplates"][0]
        return sorted((t["Key"], t["Value"]) for t in described.get("Tags", []))

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("one"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        lt_id = _cfn_output(cfn, stack_name, "LtId")
        assert tags(lt_id) == [("v", "one")]
        ec2.create_tags(Resources=[lt_id], Tags=[{"Key": "api", "Value": "kept"}])
        assert tags(lt_id) == [("api", "kept"), ("v", "one")]

        cfn.update_stack(StackName=stack_name, TemplateBody=template("two"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_output(cfn, stack_name, "LtId") == lt_id
        assert tags(lt_id) == [("api", "kept"), ("v", "one")]
        described = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])["LaunchTemplates"][0]
        assert (described["LatestVersionNumber"], described["DefaultVersionNumber"]) == (2, 1)

        # The stack delete takes the template's tags with it.
        cfn.delete_stack(StackName=stack_name)
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "DELETE_COMPLETE"
        assert ec2.describe_tags(
            Filters=[{"Name": "resource-id", "Values": [lt_id]}])["Tags"] == []
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_launch_template_data_change_is_rolled_back(cfn, ec2):
    """A LaunchTemplateData change adds a version, so a later failure in the
    same update rolls it back the way AWS does (measured 2026-09-21): by
    adding one more version with the old data. The template keeps its id,
    the latest version is 3 (t3.micro, t3.large, t3.micro), the default stays
    1, and the stack's GetAtt outputs read what they read before the update."""
    stack_name = f"cfn-lt-rb-{_uuid_mod.uuid4().hex[:8]}"

    def template(instance_type):
        return json.dumps({
            "Resources": {"Lt": {"Type": "AWS::EC2::LaunchTemplate", "Properties": {
                "LaunchTemplateData": {"ImageId": "ami-cfn7777",
                                       "InstanceType": instance_type}}}},
            "Outputs": {
                "LtId": {"Value": {"Ref": "Lt"}},
                "Latest": {"Value": {"Fn::GetAtt": ["Lt", "LatestVersionNumber"]}},
                "Default": {"Value": {"Fn::GetAtt": ["Lt", "DefaultVersionNumber"]}},
            },
        })

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("t3.micro"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        lt_id = _cfn_output(cfn, stack_name, "LtId")

        cfn.update_stack(StackName=stack_name,
                         TemplateBody=_cfn_with_failing_resource(template("t3.large"), "Lt"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_output(cfn, stack_name, "LtId") == lt_id
        assert (_cfn_output(cfn, stack_name, "Latest"),
                _cfn_output(cfn, stack_name, "Default")) == ("1", "1")
        described = ec2.describe_launch_templates(LaunchTemplateIds=[lt_id])["LaunchTemplates"][0]
        assert (described["LatestVersionNumber"], described["DefaultVersionNumber"]) == (3, 1)
        versions = ec2.describe_launch_template_versions(
            LaunchTemplateId=lt_id)["LaunchTemplateVersions"]
        assert sorted((v["VersionNumber"], v["LaunchTemplateData"]["InstanceType"],
                       v["DefaultVersion"]) for v in versions) == [
            (1, "t3.micro", True), (2, "t3.large", False), (3, "t3.micro", False)]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_internet_gateway_tag_update_keeps_id_and_attachment(cfn, ec2):
    """Tags is the only property AWS::EC2::InternetGateway has, and it is No
    interruption. The create ignored props entirely, so a tag change minted a
    new igw- id, reset Attachments to empty and still applied no tags."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-igw-upd-{suffix}"

    def template(stage):
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC",
                        "Properties": {"CidrBlock": "10.45.0.0/16"}},
                "Igw": {
                    "Type": "AWS::EC2::InternetGateway",
                    "Properties": {"Tags": [{"Key": "stage", "Value": stage}]},
                },
                "Attach": {
                    "Type": "AWS::EC2::VPCGatewayAttachment",
                    "Properties": {"VpcId": {"Ref": "Vpc"},
                                   "InternetGatewayId": {"Ref": "Igw"}},
                },
            },
            "Outputs": {"IgwId": {"Value": {"Ref": "Igw"}}},
        })

    def igw_id():
        return next(o["OutputValue"] for o in
                    cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
                    if o["OutputKey"] == "IgwId")

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("before"))
        _wait_stack(cfn, stack_name)
        before = igw_id()

        cfn.update_stack(StackName=stack_name, TemplateBody=template("after"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        assert igw_id() == before, "the gateway came back under a new id"
        described = ec2.describe_internet_gateways(
            InternetGatewayIds=[before])["InternetGateways"][0]
        assert described["Attachments"], "the VPC attachment was dropped"
        assert _template_tags(described.get("Tags", [])) == \
            [{"Key": "stage", "Value": "after"}]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_internet_gateway_tag_change_is_rolled_back(cfn, ec2):
    """A tag change on an internet gateway is applied in place, so a later
    failure in the same update has to set it back. Measured on AWS
    2026-09-21: the gateway reads stage=before again after
    UPDATE_ROLLBACK_COMPLETE, under the same id and still attached."""
    stack_name = f"cfn-igw-rb-{_uuid_mod.uuid4().hex[:8]}"

    def template(stage):
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.52.0.0/16"}},
                "Igw": {"Type": "AWS::EC2::InternetGateway",
                        "Properties": {"Tags": [{"Key": "stage", "Value": stage}]}},
                "Attach": {"Type": "AWS::EC2::VPCGatewayAttachment", "Properties": {
                    "VpcId": {"Ref": "Vpc"}, "InternetGatewayId": {"Ref": "Igw"}}},
            },
            "Outputs": {"IgwId": {"Value": {"Ref": "Igw"}}},
        })

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("before"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        igw_id = _cfn_output(cfn, stack_name, "IgwId")

        cfn.update_stack(StackName=stack_name,
                         TemplateBody=_cfn_with_failing_resource(template("after"), "Igw"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_output(cfn, stack_name, "IgwId") == igw_id
        described = ec2.describe_internet_gateways(
            InternetGatewayIds=[igw_id])["InternetGateways"][0]
        assert described["Attachments"], "the VPC attachment was dropped"
        assert _template_tags(described.get("Tags", [])) == [{"Key": "stage", "Value": "before"}]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_route_table_tag_update_keeps_id_and_routes(cfn, ec2):
    """Tags is the only property AWS::EC2::RouteTable has. The create minted a
    new rtb- id and reset Routes to the local route and Associations to empty,
    so anything added through CreateRoute or AssociateRouteTable was lost."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-rtb-upd-{suffix}"

    def template(stage):
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC",
                        "Properties": {"CidrBlock": "10.46.0.0/16"}},
                "Rtb": {
                    "Type": "AWS::EC2::RouteTable",
                    "Properties": {"VpcId": {"Ref": "Vpc"},
                                   "Tags": [{"Key": "stage", "Value": stage}]},
                },
            },
            "Outputs": {"RtbId": {"Value": {"Ref": "Rtb"}}},
        })

    def rtb_id():
        return next(o["OutputValue"] for o in
                    cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
                    if o["OutputKey"] == "RtbId")

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("before"))
        _wait_stack(cfn, stack_name)
        before = rtb_id()
        # A route added out of band is what the replacement destroys.
        ec2.create_route(RouteTableId=before, DestinationCidrBlock="198.51.100.0/24",
                         GatewayId="local")

        cfn.update_stack(StackName=stack_name, TemplateBody=template("after"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        assert rtb_id() == before, "the route table came back under a new id"
        table = ec2.describe_route_tables(RouteTableIds=[before])["RouteTables"][0]
        dests = [r.get("DestinationCidrBlock") for r in table["Routes"]]
        assert "198.51.100.0/24" in dests, "the out-of-band route was destroyed"
        assert _template_tags(table.get("Tags", [])) == \
            [{"Key": "stage", "Value": "after"}]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_route_table_tag_change_is_rolled_back(cfn, ec2):
    """A tag change on a route table is applied in place, so a later failure
    in the same update has to set it back. Measured on AWS 2026-09-21: the
    table reads stage=before again after UPDATE_ROLLBACK_COMPLETE, under the
    same id."""
    stack_name = f"cfn-rtb-rb-{_uuid_mod.uuid4().hex[:8]}"

    def template(stage):
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.53.0.0/16"}},
                "Rtb": {"Type": "AWS::EC2::RouteTable", "Properties": {
                    "VpcId": {"Ref": "Vpc"}, "Tags": [{"Key": "stage", "Value": stage}]}},
            },
            "Outputs": {"RtbId": {"Value": {"Ref": "Rtb"}}},
        })

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("before"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "CREATE_COMPLETE"
        rtb_id = _cfn_output(cfn, stack_name, "RtbId")

        cfn.update_stack(StackName=stack_name,
                         TemplateBody=_cfn_with_failing_resource(template("after"), "Rtb"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_ROLLBACK_COMPLETE", stack.get("StackStatusReason")
        assert _cfn_output(cfn, stack_name, "RtbId") == rtb_id
        table = ec2.describe_route_tables(RouteTableIds=[rtb_id])["RouteTables"][0]
        assert _template_tags(table.get("Tags", [])) == [{"Key": "stage", "Value": "before"}]
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_network_tag_change_keeps_tags_added_through_the_api(cfn, ec2):
    """The subnet, security group, internet gateway and route table apply a
    template tag change the way the VPC does: the template's tags take their
    new values and a tag added through CreateTags survives, as on AWS
    (measured 2026-09-21 on a VPC). The updates overwrote the whole tag
    list."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-net-tags-{suffix}"

    def template(value):
        tags = [{"Key": "a", "Value": value}]
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.51.0.0/16"}},
                "Subnet": {"Type": "AWS::EC2::Subnet", "Properties": {
                    "VpcId": {"Ref": "Vpc"}, "CidrBlock": "10.51.1.0/24", "Tags": tags}},
                "Sg": {"Type": "AWS::EC2::SecurityGroup", "Properties": {
                    "GroupDescription": "probe", "VpcId": {"Ref": "Vpc"}, "Tags": tags}},
                "Igw": {"Type": "AWS::EC2::InternetGateway", "Properties": {"Tags": tags}},
                "Rtb": {"Type": "AWS::EC2::RouteTable", "Properties": {
                    "VpcId": {"Ref": "Vpc"}, "Tags": tags}},
            },
            "Outputs": {
                "Subnet": {"Value": {"Ref": "Subnet"}},
                "Sg": {"Value": {"Fn::GetAtt": ["Sg", "GroupId"]}},
                "Igw": {"Value": {"Ref": "Igw"}},
                "Rtb": {"Value": {"Ref": "Rtb"}},
            },
        })

    def tags_of(resource_id):
        found = ec2.describe_tags(
            Filters=[{"Name": "resource-id", "Values": [resource_id]}])["Tags"]
        return sorted((t["Key"], t["Value"]) for t in found if not t["Key"].startswith("aws:"))

    cfn.create_stack(StackName=stack_name, TemplateBody=template("1"))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        ids = {key: _cfn_output(cfn, stack_name, key) for key in ("Subnet", "Sg", "Igw", "Rtb")}
        ec2.create_tags(Resources=list(ids.values()), Tags=[{"Key": "api", "Value": "kept"}])

        cfn.update_stack(StackName=stack_name, TemplateBody=template("2"))
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "UPDATE_COMPLETE", stack.get("StackStatusReason")
        for key, resource_id in ids.items():
            assert _cfn_output(cfn, stack_name, key) == resource_id, key
            assert tags_of(resource_id) == [("a", "2"), ("api", "kept")], key
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_networking_resources_carry_the_stack_tags(cfn, ec2):
    """A VPC, subnet, security group, internet gateway and route table carry
    the three aws:cloudformation:* tags and the stack-level tags beside their
    own, and a stack-tag change reaches them (measured on AWS 2026-09-21).
    The five types were missing from the stack-tag table, so they carried the
    template's tags only."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-net-stack-tags-{suffix}"
    own = [{"Key": "own", "Value": "a"}]
    template = json.dumps({
        "Resources": {
            "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.63.0.0/16", "Tags": own}},
            "Subnet": {"Type": "AWS::EC2::Subnet", "Properties": {
                "VpcId": {"Ref": "Vpc"}, "CidrBlock": "10.63.1.0/24", "Tags": own}},
            "Sg": {"Type": "AWS::EC2::SecurityGroup", "Properties": {
                "GroupDescription": "stack tags", "VpcId": {"Ref": "Vpc"}, "Tags": own}},
            "Igw": {"Type": "AWS::EC2::InternetGateway", "Properties": {"Tags": own}},
            "Rtb": {"Type": "AWS::EC2::RouteTable", "Properties": {"VpcId": {"Ref": "Vpc"}, "Tags": own}},
        },
        "Outputs": {
            "Vpc": {"Value": {"Ref": "Vpc"}}, "Subnet": {"Value": {"Ref": "Subnet"}},
            "Sg": {"Value": {"Fn::GetAtt": ["Sg", "GroupId"]}},
            "Igw": {"Value": {"Ref": "Igw"}}, "Rtb": {"Value": {"Ref": "Rtb"}},
        },
    })

    def tags_of(resource_id):
        return {t["Key"]: t["Value"] for t in ec2.describe_tags(
            Filters=[{"Name": "resource-id", "Values": [resource_id]}])["Tags"]}

    cfn.create_stack(StackName=stack_name, TemplateBody=template,
                     Tags=[{"Key": "stage", "Value": "one"}])
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        ids = {key: _cfn_output(cfn, stack_name, key) for key in ("Vpc", "Subnet", "Sg", "Igw", "Rtb")}
        for logical_id, resource_id in ids.items():
            assert tags_of(resource_id) == {
                "own": "a", "stage": "one",
                "aws:cloudformation:stack-name": stack_name,
                "aws:cloudformation:stack-id": stack["StackId"],
                "aws:cloudformation:logical-id": logical_id,
            }, logical_id
        cfn.update_stack(StackName=stack_name, UsePreviousTemplate=True,
                         Tags=[{"Key": "stage", "Value": "two"}])
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        for logical_id, resource_id in ids.items():
            assert tags_of(resource_id)["stage"] == "two", logical_id
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_route_target_change_does_not_duplicate(cfn, ec2):
    """The physical id of AWS::EC2::Route is "{table}|{destination}", so a
    changed target leaves it unmoved and the engine declares no replacement.
    The create then appended a second entry, leaving the table holding two
    routes for one destination."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-route-upd-{suffix}"

    def template(target):
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC",
                        "Properties": {"CidrBlock": "10.47.0.0/16"}},
                "Rtb": {"Type": "AWS::EC2::RouteTable",
                        "Properties": {"VpcId": {"Ref": "Vpc"}}},
                "Igw": {"Type": "AWS::EC2::InternetGateway"},
                "Attach": {
                    "Type": "AWS::EC2::VPCGatewayAttachment",
                    "Properties": {"VpcId": {"Ref": "Vpc"},
                                   "InternetGatewayId": {"Ref": "Igw"}},
                },
                "Route": {
                    "Type": "AWS::EC2::Route",
                    "DependsOn": "Attach",
                    "Properties": {
                        "RouteTableId": {"Ref": "Rtb"},
                        "DestinationCidrBlock": "0.0.0.0/0",
                        "GatewayId": target,
                    },
                },
            },
            "Outputs": {"RtbId": {"Value": {"Ref": "Rtb"}}},
        })

    try:
        cfn.create_stack(StackName=stack_name, TemplateBody=template("igw-aaaaaaaa"))
        _wait_stack(cfn, stack_name)
        rtb = next(o["OutputValue"] for o in
                   cfn.describe_stacks(StackName=stack_name)["Stacks"][0]["Outputs"]
                   if o["OutputKey"] == "RtbId")

        cfn.update_stack(StackName=stack_name, TemplateBody=template("igw-bbbbbbbb"))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"

        table = ec2.describe_route_tables(RouteTableIds=[rtb])["RouteTables"][0]
        default = [r for r in table["Routes"]
                   if r.get("DestinationCidrBlock") == "0.0.0.0/0"]
        assert len(default) == 1, \
            f"the target change left {len(default)} routes for one destination"
        assert default[0].get("GatewayId") == "igw-bbbbbbbb"
    finally:
        _delete_cfn_test_stack(cfn, stack_name)


def test_cfn_ec2_route_destinations_are_separate_routes(cfn, ec2):
    """DestinationCidrBlock, DestinationIpv6CidrBlock and
    DestinationPrefixListId are three create-only properties of
    AWS::EC2::Route, each naming its own route: on AWS a prefix-list route
    and an IPv6 default route sit beside the IPv4 default route, and removing
    one leaves the others (measured 2026-09-21). The provisioner read only
    DestinationCidrBlock and defaulted it to 0.0.0.0/0, so a prefix-list or
    IPv6 route took the IPv4 default route's place."""
    suffix = _uuid_mod.uuid4().hex[:8]
    stack_name = f"cfn-route-dest-{suffix}"
    prefix_list = ec2.create_managed_prefix_list(
        PrefixListName=f"cfn-route-dest-{suffix}", MaxEntries=1, AddressFamily="IPv4",
        Entries=[{"Cidr": "198.51.100.0/24"}])["PrefixList"]["PrefixListId"]

    def template(with_v4):
        routes = {
            "RoutePl": {"DestinationPrefixListId": prefix_list},
            "Route6": {"DestinationIpv6CidrBlock": "::/0"},
        }
        if with_v4:
            routes["Route4"] = {"DestinationCidrBlock": "0.0.0.0/0"}
        return json.dumps({
            "Resources": {
                "Vpc": {"Type": "AWS::EC2::VPC", "Properties": {"CidrBlock": "10.59.0.0/16"}},
                "Rtb": {"Type": "AWS::EC2::RouteTable", "Properties": {"VpcId": {"Ref": "Vpc"}}},
                "Igw": {"Type": "AWS::EC2::InternetGateway"},
                "Attach": {"Type": "AWS::EC2::VPCGatewayAttachment", "Properties": {
                    "VpcId": {"Ref": "Vpc"}, "InternetGatewayId": {"Ref": "Igw"}}},
                **{logical_id: {"Type": "AWS::EC2::Route", "DependsOn": "Attach", "Properties": {
                    "RouteTableId": {"Ref": "Rtb"}, "GatewayId": {"Ref": "Igw"}, **destination}}
                   for logical_id, destination in routes.items()},
            },
            "Outputs": {"Rtb": {"Value": {"Ref": "Rtb"}}, "RoutePl": {"Value": {"Ref": "RoutePl"}}},
        })

    def destinations(rtb):
        table = ec2.describe_route_tables(RouteTableIds=[rtb])["RouteTables"][0]
        return sorted(
            r.get("DestinationCidrBlock") or r.get("DestinationIpv6CidrBlock")
            or r.get("DestinationPrefixListId") for r in table["Routes"])

    cfn.create_stack(StackName=stack_name, TemplateBody=template(True))
    try:
        stack = _wait_stack(cfn, stack_name)
        assert stack["StackStatus"] == "CREATE_COMPLETE", stack.get("StackStatusReason")
        rtb = _cfn_output(cfn, stack_name, "Rtb")
        assert _cfn_output(cfn, stack_name, "RoutePl") == f"{rtb}|{prefix_list}"
        assert destinations(rtb) == sorted(["0.0.0.0/0", "10.59.0.0/16", "::/0", prefix_list])

        cfn.update_stack(StackName=stack_name, TemplateBody=template(False))
        assert _wait_stack(cfn, stack_name)["StackStatus"] == "UPDATE_COMPLETE"
        assert destinations(rtb) == sorted(["10.59.0.0/16", "::/0", prefix_list])
    finally:
        _delete_cfn_test_stack(cfn, stack_name)
        ec2.delete_managed_prefix_list(PrefixListId=prefix_list)
