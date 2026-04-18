import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

def test_servicediscovery_flow(sd):
    # 1. Create Private DNS Namespace
    ns_name = "example.terraform.local"
    resp = sd.create_private_dns_namespace(
        Name=ns_name,
        Description="example",
        Vpc="vpc-12345"
    )
    op_id = resp["OperationId"]
    assert op_id

    # Verify Operation
    op = sd.get_operation(OperationId=op_id)["Operation"]
    assert op["Status"] == "SUCCESS"
    ns_id = op["Targets"]["NAMESPACE"]

    # Verify Namespace
    ns = sd.get_namespace(Id=ns_id)["Namespace"]
    assert ns["Name"] == ns_name
    
    # Verify Hosted Zone integration
    props = ns.get("Properties", {})
    dns_props = props.get("DnsProperties", {})
    hz_id = dns_props.get("HostedZoneId")
    assert hz_id, f"Expected HostedZoneId in namespace properties: {ns}"
    
    from conftest import make_client
    r53 = make_client("route53")
    hz = r53.get_hosted_zone(Id=hz_id)["HostedZone"]
    assert hz["Name"] == ns_name + "."
    assert hz["Config"]["PrivateZone"] is True

    # 2. Create Service
    svc_name = "example-service"
    resp = sd.create_service(
        Name=svc_name,
        NamespaceId=ns_id,
        DnsConfig={
            "DnsRecords": [{"Type": "A", "TTL": 10}],
            "RoutingPolicy": "MULTIVALUE"
        }
    )
    svc_id = resp["Service"]["Id"]
    assert svc_id

    # 3. Register Instance
    inst_id = "example-instance-id"
    resp = sd.register_instance(
        ServiceId=svc_id,
        InstanceId=inst_id,
        Attributes={
            "AWS_INSTANCE_IPV4": "172.18.0.1",
            "custom_attribute": "custom"
        }
    )
    assert resp["OperationId"]

    # 4. Discover Instances
    resp = sd.discover_instances(
        NamespaceName=ns_name,
        ServiceName=svc_name
    )
    instances = resp["Instances"]
    assert len(instances) == 1
    assert instances[0]["InstanceId"] == inst_id
    assert instances[0]["Attributes"]["AWS_INSTANCE_IPV4"] == "172.18.0.1"

    # 5. List Operations
    namespaces = sd.list_namespaces()["Namespaces"]
    assert any(n["Id"] == ns_id for n in namespaces)

    services = sd.list_services()["Services"]
    assert any(s["Id"] == svc_id for s in services)

    insts = sd.list_instances(ServiceId=svc_id)["Instances"]
    assert any(i["Id"] == inst_id for i in insts)

    # 6. Deregister & Delete
    sd.deregister_instance(ServiceId=svc_id, InstanceId=inst_id)
    insts = sd.list_instances(ServiceId=svc_id)["Instances"]
    assert len(insts) == 0

    sd.delete_service(Id=svc_id)
    sd.delete_namespace(Id=ns_id)

def test_servicediscovery_tagging(sd):
    # 1. Create Namespace with tags
    ns_name = "tag-test-ns"
    resp = sd.create_http_namespace(
        Name=ns_name,
        Tags=[{"Key": "Owner", "Value": "TeamA"}]
    )
    op_id = resp["OperationId"]
    op = sd.get_operation(OperationId=op_id)["Operation"]
    ns_id = op["Targets"]["NAMESPACE"]
    ns = sd.get_namespace(Id=ns_id)["Namespace"]
    ns_arn = ns["Arn"]

    # 2. List tags
    resp = sd.list_tags_for_resource(ResourceARN=ns_arn)
    assert any(t["Key"] == "Owner" and t["Value"] == "TeamA" for t in resp["Tags"])

    # 3. Add more tags
    sd.tag_resource(
        ResourceARN=ns_arn,
        Tags=[{"Key": "Env", "Value": "Dev"}]
    )
    resp = sd.list_tags_for_resource(ResourceARN=ns_arn)
    assert len(resp["Tags"]) == 2

    # 4. Untag
    sd.untag_resource(ResourceARN=ns_arn, TagKeys=["Owner"])
    resp = sd.list_tags_for_resource(ResourceARN=ns_arn)
    assert len(resp["Tags"]) == 1
    assert resp["Tags"][0]["Key"] == "Env"

    # Cleanup
    sd.delete_namespace(Id=ns_id)

def test_servicediscovery_additional_operations(sd):
    ns_name = "ops-test.local"
    ns_op = sd.create_private_dns_namespace(
        Name=ns_name,
        Description="ops test",
        Vpc="vpc-12345",
    )
    ns_id = sd.get_operation(OperationId=ns_op["OperationId"])["Operation"]["Targets"]["NAMESPACE"]

    svc = sd.create_service(
        Name="ops-service",
        NamespaceId=ns_id,
        DnsConfig={"DnsRecords": [{"Type": "A", "TTL": 10}], "RoutingPolicy": "MULTIVALUE"},
    )["Service"]
    svc_id = svc["Id"]

    # service attributes CRUD
    sd.update_service_attributes(ServiceId=svc_id, Attributes={"team": "core", "env": "test"})
    attrs = sd.get_service_attributes(ServiceId=svc_id)["ServiceAttributes"]["Attributes"]
    assert attrs["team"] == "core"
    assert attrs["env"] == "test"

    sd.delete_service_attributes(ServiceId=svc_id, Attributes=["env"])
    attrs = sd.get_service_attributes(ServiceId=svc_id)["ServiceAttributes"]["Attributes"]
    assert "env" not in attrs
    assert attrs["team"] == "core"

    # namespace/service update operations
    ns_update_op = sd.update_private_dns_namespace(
        Id=ns_id,
        UpdaterRequestId="upd-ns-1",
        Namespace={"Description": "updated namespace"},
    )["OperationId"]
    assert sd.get_operation(OperationId=ns_update_op)["Operation"]["Targets"]["NAMESPACE"] == ns_id

    svc_update_op = sd.update_service(
        Id=svc_id,
        Service={"Description": "updated service"},
    )["OperationId"]
    assert sd.get_operation(OperationId=svc_update_op)["Operation"]["Targets"]["SERVICE"] == svc_id

    # operations listing
    ops = sd.list_operations(MaxResults=50)["Operations"]
    assert any(o["Id"] == ns_update_op for o in ops)
    assert any(o["Id"] == svc_update_op for o in ops)

    # instance health + revision
    sd.register_instance(
        ServiceId=svc_id,
        InstanceId="inst-1",
        Attributes={"AWS_INSTANCE_IPV4": "10.0.0.1"},
    )
    rev_before = sd.discover_instances_revision(NamespaceName=ns_name, ServiceName="ops-service")["InstancesRevision"]

    sd.update_instance_custom_health_status(ServiceId=svc_id, InstanceId="inst-1", Status="UNHEALTHY")
    health = sd.get_instances_health_status(ServiceId=svc_id)["Status"]
    assert health["inst-1"] == "UNHEALTHY"

    discovered = sd.discover_instances(NamespaceName=ns_name, ServiceName="ops-service", HealthStatus="ALL")["Instances"]
    assert discovered[0]["HealthStatus"] == "UNHEALTHY"

    rev_after = sd.discover_instances_revision(NamespaceName=ns_name, ServiceName="ops-service")["InstancesRevision"]
    assert rev_after > rev_before

    # cleanup
    sd.deregister_instance(ServiceId=svc_id, InstanceId="inst-1")
    sd.delete_service(Id=svc_id)
    sd.delete_namespace(Id=ns_id)

def test_servicediscovery_create_public_dns_namespace(sd):
    ns_name = "public-test.example.com"
    resp = sd.create_public_dns_namespace(
        Name=ns_name,
        Description="public dns namespace test",
    )
    op_id = resp["OperationId"]
    assert op_id

    op = sd.get_operation(OperationId=op_id)["Operation"]
    assert op["Status"] == "SUCCESS"
    ns_id = op["Targets"]["NAMESPACE"]

    ns = sd.get_namespace(Id=ns_id)["Namespace"]
    assert ns["Name"] == ns_name
    assert ns["Type"] == "DNS_PUBLIC"

    # verify hosted zone was created (public, not private)
    props = ns.get("Properties", {})
    dns_props = props.get("DnsProperties", {})
    hz_id = dns_props.get("HostedZoneId")
    assert hz_id, f"Expected HostedZoneId in namespace properties: {ns}"

    from conftest import make_client
    r53 = make_client("route53")
    hz = r53.get_hosted_zone(Id=hz_id)["HostedZone"]
    assert hz["Name"] == ns_name + "."
    assert hz["Config"]["PrivateZone"] is False

    # cleanup
    sd.delete_namespace(Id=ns_id)

def test_servicediscovery_get_instance(sd):
    ns_name = "get-inst.local"
    ns_op = sd.create_private_dns_namespace(
        Name=ns_name,
        Description="get instance test",
        Vpc="vpc-12345",
    )
    ns_id = sd.get_operation(OperationId=ns_op["OperationId"])["Operation"]["Targets"]["NAMESPACE"]

    svc = sd.create_service(
        Name="get-inst-svc",
        NamespaceId=ns_id,
        DnsConfig={"DnsRecords": [{"Type": "A", "TTL": 10}], "RoutingPolicy": "MULTIVALUE"},
    )["Service"]
    svc_id = svc["Id"]

    inst_id = "my-instance-1"
    sd.register_instance(
        ServiceId=svc_id,
        InstanceId=inst_id,
        Attributes={"AWS_INSTANCE_IPV4": "10.0.0.42", "role": "web"},
    )

    # get_instance returns the single instance
    resp = sd.get_instance(ServiceId=svc_id, InstanceId=inst_id)
    inst = resp["Instance"]
    assert inst["Id"] == inst_id
    assert inst["Attributes"]["AWS_INSTANCE_IPV4"] == "10.0.0.42"
    assert inst["Attributes"]["role"] == "web"

    # cleanup
    sd.deregister_instance(ServiceId=svc_id, InstanceId=inst_id)
    sd.delete_service(Id=svc_id)
    sd.delete_namespace(Id=ns_id)

def test_servicediscovery_get_service(sd):
    ns_op = sd.create_http_namespace(Name="get-svc-ns")
    ns_id = sd.get_operation(OperationId=ns_op["OperationId"])["Operation"]["Targets"]["NAMESPACE"]

    svc_name = "my-http-service"
    svc = sd.create_service(
        Name=svc_name,
        NamespaceId=ns_id,
        Description="a service to fetch",
    )["Service"]
    svc_id = svc["Id"]

    # get_service returns the full service object
    resp = sd.get_service(Id=svc_id)
    fetched = resp["Service"]
    assert fetched["Id"] == svc_id
    assert fetched["Name"] == svc_name
    assert fetched["Description"] == "a service to fetch"
    assert fetched["NamespaceId"] == ns_id

    # cleanup
    sd.delete_service(Id=svc_id)
    sd.delete_namespace(Id=ns_id)

def test_servicediscovery_update_http_namespace(sd):
    ns_op = sd.create_http_namespace(
        Name="upd-http-ns",
        Description="original description",
    )
    ns_id = sd.get_operation(OperationId=ns_op["OperationId"])["Operation"]["Targets"]["NAMESPACE"]

    # update the namespace description
    upd_op = sd.update_http_namespace(
        Id=ns_id,
        UpdaterRequestId="upd-http-1",
        Namespace={"Description": "updated http description"},
    )
    upd_op_id = upd_op["OperationId"]
    assert upd_op_id

    op = sd.get_operation(OperationId=upd_op_id)["Operation"]
    assert op["Status"] == "SUCCESS"
    assert op["Targets"]["NAMESPACE"] == ns_id

    # verify update took effect
    ns = sd.get_namespace(Id=ns_id)["Namespace"]
    assert ns["Description"] == "updated http description"

    # cleanup
    sd.delete_namespace(Id=ns_id)

def test_servicediscovery_update_public_dns_namespace(sd):
    ns_op = sd.create_public_dns_namespace(
        Name="upd-public.example.com",
        Description="original public desc",
    )
    ns_id = sd.get_operation(OperationId=ns_op["OperationId"])["Operation"]["Targets"]["NAMESPACE"]

    # update the namespace description
    upd_op = sd.update_public_dns_namespace(
        Id=ns_id,
        UpdaterRequestId="upd-pub-1",
        Namespace={"Description": "updated public description"},
    )
    upd_op_id = upd_op["OperationId"]
    assert upd_op_id

    op = sd.get_operation(OperationId=upd_op_id)["Operation"]
    assert op["Status"] == "SUCCESS"
    assert op["Targets"]["NAMESPACE"] == ns_id

    # verify update took effect
    ns = sd.get_namespace(Id=ns_id)["Namespace"]
    assert ns["Description"] == "updated public description"

    # cleanup
    sd.delete_namespace(Id=ns_id)
