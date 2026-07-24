import asyncio
import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

import ministack.services.servicediscovery as sd_svc


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


# ---------------------------------------------------------------------------
# ARN adoption / tag-API in-process unit tests. Folded from
# test_servicediscovery_arn_adoption.py (drive the module directly).
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_servicediscovery_state():
    sd_svc.reset()
    yield
    sd_svc.reset()


def _json_result(response):
    status, headers, body = response
    return status, headers, json.loads(body.decode("utf-8"))


def _seed_namespace(ns_id="ns-direct"):
    arn = sd_svc._namespace_arn(ns_id)
    sd_svc._namespaces[ns_id] = {
        "Id": ns_id,
        "Arn": arn,
        "Name": "direct.local",
        "Type": "HTTP",
    }
    return arn


def _seed_service(svc_id="srv-direct"):
    ns_id = "ns-for-service"
    sd_svc._namespaces[ns_id] = {
        "Id": ns_id,
        "Arn": sd_svc._namespace_arn(ns_id),
        "Name": "service.local",
        "Type": "HTTP",
    }
    arn = sd_svc._service_arn(svc_id)
    sd_svc._services[svc_id] = {
        "Id": svc_id,
        "Arn": arn,
        "Name": "direct-service",
        "NamespaceId": ns_id,
    }
    return arn


@pytest.mark.parametrize("seed_resource", [_seed_namespace, _seed_service])
def test_servicediscovery_tag_apis_accept_local_namespace_and_service_arns(seed_resource):
    arn = seed_resource()

    status, _, body = _json_result(
        sd_svc._tag_resource(
            {
                "ResourceARN": arn,
                "Tags": [
                    {"Key": "env", "Value": "test"},
                    {"Key": "owner", "Value": "platform"},
                ],
            }
        )
    )
    assert status == 200
    assert body == {}

    status, _, body = _json_result(sd_svc._list_tags_for_resource({"ResourceARN": arn}))
    assert status == 200
    assert body["Tags"] == [
        {"Key": "env", "Value": "test"},
        {"Key": "owner", "Value": "platform"},
    ]

    status, _, body = _json_result(sd_svc._untag_resource({"ResourceARN": arn, "TagKeys": ["env"]}))
    assert status == 200
    assert body == {}

    status, _, body = _json_result(sd_svc._list_tags_for_resource({"ResourceARN": arn}))
    assert status == 200
    assert body["Tags"] == [{"Key": "owner", "Value": "platform"}]


@pytest.mark.parametrize(
    "bad_arn",
    [
        "not-an-arn",
        "arn:aws-cn:servicediscovery:us-east-1:000000000000:namespace/ns-direct",
        "arn:aws:s3:us-east-1:000000000000:namespace/ns-direct",
        "arn:aws:servicediscovery:us-east-1:111122223333:namespace/ns-direct",
        "arn:aws:servicediscovery:us-west-2:000000000000:namespace/ns-direct",
        "arn:aws:servicediscovery:us-east-1:000000000000:instance/ns-direct/inst-1",
        "arn:aws:servicediscovery:us-east-1:000000000000:namespace/ns-direct/child",
        "arn:aws:servicediscovery:us-east-1:000000000000:service",
    ],
)
def test_servicediscovery_tag_apis_reject_invalid_resource_arns_before_touching_tags(bad_arn):
    valid_arn = _seed_namespace()
    sd_svc._resource_tags[valid_arn] = [{"Key": "keep", "Value": "yes"}]

    status, _, body = _json_result(
        sd_svc._tag_resource({"ResourceARN": bad_arn, "Tags": [{"Key": "new", "Value": "tag"}]})
    )
    assert status == 400
    assert body["__type"] == "InvalidInput"
    assert sd_svc._resource_tags.get(bad_arn) is None

    status, _, body = _json_result(sd_svc._untag_resource({"ResourceARN": bad_arn, "TagKeys": ["keep"]}))
    assert status == 400
    assert body["__type"] == "InvalidInput"

    status, _, body = _json_result(sd_svc._list_tags_for_resource({"ResourceARN": bad_arn}))
    assert status == 400
    assert body["__type"] == "InvalidInput"

    assert sd_svc._resource_tags[valid_arn] == [{"Key": "keep", "Value": "yes"}]


@pytest.mark.parametrize(
    ("resource_arn", "expected_error"),
    [
        ("arn:aws:servicediscovery:us-east-1:000000000000:namespace/ns-missing", "NamespaceNotFound"),
        ("arn:aws:servicediscovery:us-east-1:000000000000:service/srv-missing", "ServiceNotFound"),
    ],
)
def test_servicediscovery_tag_apis_reject_missing_local_resources(resource_arn, expected_error):
    for call in (
        lambda: sd_svc._tag_resource({"ResourceARN": resource_arn, "Tags": [{"Key": "new", "Value": "tag"}]}),
        lambda: sd_svc._untag_resource({"ResourceARN": resource_arn, "TagKeys": ["old"]}),
        lambda: sd_svc._list_tags_for_resource({"ResourceARN": resource_arn}),
    ):
        status, _, body = _json_result(call())
        assert status == 404
        assert body["__type"] == expected_error

    assert sd_svc._resource_tags.get(resource_arn) is None


def _create_http_namespace_direct(name, tags=None):
    status, _, body = _json_result(
        asyncio.run(
            sd_svc._create_namespace(
                {
                    "Name": name,
                    "Tags": tags or [],
                    "_action": "CreateHttpNamespace",
                }
            )
        )
    )
    assert status == 200
    operation = _json_result(
        sd_svc._get_operation({"OperationId": body["OperationId"]})
    )[2]["Operation"]
    return operation["Targets"]["NAMESPACE"]


def _create_service_direct(namespace_id, name):
    status, _, body = _json_result(
        sd_svc._create_service({"NamespaceId": namespace_id, "Name": name})
    )
    assert status == 200
    return body["Service"]["Id"]


def test_servicediscovery_state_is_isolated_by_region():
    from ministack.core.responses import set_request_region

    namespace_name = "shared-http-namespace"
    service_name = "shared-service"

    east_namespace_id = _create_http_namespace_direct(
        namespace_name, [{"Key": "region", "Value": "east"}]
    )
    east_service_id = _create_service_direct(east_namespace_id, service_name)
    assert _json_result(
        sd_svc._register_instance(
            {
                "ServiceId": east_service_id,
                "InstanceId": "shared-instance",
                "Attributes": {"AWS_INSTANCE_IPV4": "10.0.0.1"},
            }
        )
    )[0] == 200
    east_operation_ids = set(sd_svc._operations.keys())

    set_request_region("us-west-2")
    west_namespace_id = _create_http_namespace_direct(
        namespace_name, [{"Key": "region", "Value": "west"}]
    )
    west_service_id = _create_service_direct(west_namespace_id, service_name)
    assert _json_result(
        sd_svc._register_instance(
            {
                "ServiceId": west_service_id,
                "InstanceId": "shared-instance",
                "Attributes": {"AWS_INSTANCE_IPV4": "10.0.0.2"},
            }
        )
    )[0] == 200
    west_operation_ids = set(sd_svc._operations.keys())

    assert _json_result(sd_svc._list_namespaces({}))[2]["Namespaces"] == [
        sd_svc._namespaces[west_namespace_id]
    ]
    assert _json_result(sd_svc._list_services({}))[2]["Services"] == [
        sd_svc._services[west_service_id]
    ]
    listed_west_operation_ids = {
        operation["Id"]
        for operation in _json_result(sd_svc._list_operations({}))[2]["Operations"]
    }
    assert listed_west_operation_ids == west_operation_ids
    assert listed_west_operation_ids.isdisjoint(east_operation_ids)
    assert _json_result(sd_svc._get_namespace({"Id": east_namespace_id}))[0] == 404
    assert _json_result(sd_svc._get_service({"Id": east_service_id}))[0] == 404
    assert _json_result(
        sd_svc._get_instance(
            {"ServiceId": east_service_id, "InstanceId": "shared-instance"}
        )
    )[0] == 404

    set_request_region("us-east-1")
    assert _json_result(sd_svc._list_namespaces({}))[2]["Namespaces"] == [
        sd_svc._namespaces[east_namespace_id]
    ]
    assert _json_result(
        asyncio.run(sd_svc._create_namespace({"Name": namespace_name}))
    )[0] == 409


def test_servicediscovery_legacy_children_follow_parent_service_region():
    from ministack.core.responses import AccountScopedDict

    account_id = "000000000000"
    region = "us-west-2"
    namespace_id = "ns-legacy"
    service_id = "srv-legacy"
    operation_id = "op-legacy"
    namespace_arn = (
        f"arn:aws:servicediscovery:{region}:{account_id}:namespace/{namespace_id}"
    )
    service_arn = (
        f"arn:aws:servicediscovery:{region}:{account_id}:service/{service_id}"
    )

    def legacy_store(key, value):
        store = AccountScopedDict()
        store.set_scoped(account_id, "ignored", key, value)
        return store

    sd_svc.load_persisted_state(
        {
            "namespaces": legacy_store(
                namespace_id,
                {"Id": namespace_id, "Arn": namespace_arn, "Name": "legacy.local"},
            ),
            "services": legacy_store(
                service_id,
                {
                    "Id": service_id,
                    "Arn": service_arn,
                    "Name": "legacy-service",
                    "NamespaceId": namespace_id,
                },
            ),
            "instances": legacy_store(
                service_id, {"instance-1": {"Id": "instance-1"}}
            ),
            "operations": legacy_store(
                operation_id,
                {
                    "Id": operation_id,
                    "Targets": {"SERVICE": service_id},
                    "Status": "SUCCESS",
                },
            ),
            "resource_tags": legacy_store(
                service_arn, [{"Key": "legacy", "Value": "true"}]
            ),
            "service_attributes": legacy_store(service_id, {"team": "legacy"}),
            "instance_health_status": legacy_store(
                service_id, {"instance-1": "HEALTHY"}
            ),
            "instances_revision": legacy_store(service_id, 7),
        }
    )

    for store in (
        sd_svc._services,
        sd_svc._instances,
        sd_svc._service_attributes,
        sd_svc._instance_health_status,
        sd_svc._instances_revision,
    ):
        assert store.get_scoped(account_id, region, service_id) is not None
        assert store.get_scoped(account_id, "us-east-1", service_id) is None
    assert sd_svc._namespaces.get_scoped(account_id, region, namespace_id) is not None
    assert sd_svc._operations.get_scoped(account_id, region, operation_id) is not None
    assert sd_svc._resource_tags.get_scoped(account_id, "ignored", service_arn) == [
        {"Key": "legacy", "Value": "true"}
    ]


def test_servicediscovery_reset_clears_all_regions():
    from ministack.core.responses import set_request_region

    regional_stores = (
        sd_svc._namespaces,
        sd_svc._services,
        sd_svc._instances,
        sd_svc._operations,
        sd_svc._service_attributes,
        sd_svc._instance_health_status,
        sd_svc._instances_revision,
    )
    for region in ("us-east-1", "us-west-2"):
        set_request_region(region)
        for store in regional_stores:
            store[f"key-{region}"] = {}
        sd_svc._resource_tags[
            f"arn:aws:servicediscovery:{region}:000000000000:service/key-{region}"
        ] = []

    sd_svc.reset()

    assert all(not store.has_any() for store in regional_stores)
    assert sd_svc._resource_tags._data == {}
