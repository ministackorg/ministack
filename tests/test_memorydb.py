"""
MemoryDB control-plane emulator tests.

Calls the JSON-RPC handler (handle_request) directly. Shapes asserted against
botocore memorydb/2021-01-01/service-2.json (targetPrefix AmazonMemoryDB):
CreateCluster/DescribeClusters/DeleteCluster round-trip, subnet-group + ACL
round-trips, unknown-cluster fault.
"""

import asyncio
import json

import pytest

import ministack.services.memorydb as memorydb
from ministack.core.responses import (
    set_request_account_id,
    set_request_region,
)


@pytest.fixture
def env():
    memorydb.reset()
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    yield
    memorydb.reset()


def _req(action, body=None):
    headers = {"x-amz-target": f"AmazonMemoryDB.{action}"}
    payload = json.dumps(body or {}).encode()
    return asyncio.run(
        memorydb.handle_request("POST", "/", headers, payload, {})
    )


def _body(resp):
    return json.loads(resp[2].decode())


# ── Cluster round-trip ─────────────────────────────────────────


def test_cluster_round_trip(env):
    status, _h, _b = _req(
        "CreateCluster",
        {"ClusterName": "mydb", "NodeType": "db.r6g.large", "ACLName": "open-access"},
    )
    assert status == 200
    cluster = _body((status, _h, _b))["Cluster"]
    assert cluster["Name"] == "mydb"
    assert cluster["Status"] == "available"
    assert cluster["NodeType"] == "db.r6g.large"
    assert cluster["ARN"] == "arn:aws:memorydb:us-east-1:000000000000:cluster/mydb"
    # Endpoint shape.
    assert "Address" in cluster["ClusterEndpoint"]
    assert cluster["ClusterEndpoint"]["Port"] == 6379

    status, _h, _b = _req("DescribeClusters", {})
    body = _body((status, _h, _b))
    assert status == 200
    assert "Clusters" in body
    assert [c["Name"] for c in body["Clusters"]] == ["mydb"]

    status, _h, _b = _req("DescribeClusters", {"ClusterName": "mydb"})
    assert _body((status, _h, _b))["Clusters"][0]["Name"] == "mydb"

    status, _h, _b = _req("DeleteCluster", {"ClusterName": "mydb"})
    assert status == 200
    assert _body((status, _h, _b))["Cluster"]["Name"] == "mydb"

    status, _h, _b = _req("DescribeClusters", {})
    assert _body((status, _h, _b))["Clusters"] == []


def test_update_cluster(env):
    _req("CreateCluster", {"ClusterName": "c1"})
    status, _h, _b = _req(
        "UpdateCluster", {"ClusterName": "c1", "Description": "updated"}
    )
    assert status == 200
    assert _body((status, _h, _b))["Cluster"]["Description"] == "updated"


def test_describe_unknown_cluster_fault(env):
    status, headers, body = _req("DescribeClusters", {"ClusterName": "nope"})
    assert status == 400
    payload = json.loads(body.decode())
    assert payload["__type"] == "ClusterNotFoundFault"
    assert headers["x-amzn-errortype"] == "ClusterNotFoundFault"


def test_create_duplicate_cluster_fault(env):
    _req("CreateCluster", {"ClusterName": "dup"})
    status, _h, body = _req("CreateCluster", {"ClusterName": "dup"})
    assert status == 400
    assert json.loads(body.decode())["__type"] == "ClusterAlreadyExistsFault"


# ── Subnet group round-trip ────────────────────────────────────


def test_subnet_group_round_trip(env):
    status, _h, _b = _req(
        "CreateSubnetGroup",
        {"SubnetGroupName": "sg1", "SubnetIds": ["subnet-a", "subnet-b"]},
    )
    assert status == 200
    sg = _body((status, _h, _b))["SubnetGroup"]
    assert sg["Name"] == "sg1"
    assert sg["ARN"] == "arn:aws:memorydb:us-east-1:000000000000:subnetgroup/sg1"
    assert {s["Identifier"] for s in sg["Subnets"]} == {"subnet-a", "subnet-b"}

    status, _h, _b = _req("DescribeSubnetGroups", {})
    assert _body((status, _h, _b))["SubnetGroups"][0]["Name"] == "sg1"

    status, _h, _b = _req("DeleteSubnetGroup", {"SubnetGroupName": "sg1"})
    assert status == 200
    assert _body((status, _h, _b))["SubnetGroup"]["Name"] == "sg1"

    status, _h, body = _req(
        "DescribeSubnetGroups", {"SubnetGroupName": "sg1"}
    )
    assert status == 400
    assert json.loads(body.decode())["__type"] == "SubnetGroupNotFoundFault"


# ── ACL round-trip ─────────────────────────────────────────────


def test_acl_round_trip(env):
    status, _h, _b = _req(
        "CreateACL", {"ACLName": "acl1", "UserNames": ["default"]}
    )
    assert status == 200
    acl = _body((status, _h, _b))["ACL"]
    assert acl["Name"] == "acl1"
    assert acl["Status"] == "active"
    assert acl["UserNames"] == ["default"]
    assert acl["ARN"] == "arn:aws:memorydb:us-east-1:000000000000:acl/acl1"

    status, _h, _b = _req("DescribeACLs", {})
    assert _body((status, _h, _b))["ACLs"][0]["Name"] == "acl1"

    status, _h, _b = _req("DeleteACL", {"ACLName": "acl1"})
    assert status == 200
    assert _body((status, _h, _b))["ACL"]["Name"] == "acl1"

    status, _h, body = _req("DescribeACLs", {"ACLName": "acl1"})
    assert status == 400
    assert json.loads(body.decode())["__type"] == "ACLNotFoundFault"


# ── Parameter group + parameters ───────────────────────────────


def test_parameter_group_and_parameters(env):
    status, _h, _b = _req(
        "CreateParameterGroup",
        {"ParameterGroupName": "pg1", "Family": "memorydb_redis7"},
    )
    assert status == 200
    pg = _body((status, _h, _b))["ParameterGroup"]
    assert pg["Name"] == "pg1"
    assert pg["Family"] == "memorydb_redis7"

    status, _h, _b = _req("DescribeParameterGroups", {})
    assert _body((status, _h, _b))["ParameterGroups"][0]["Name"] == "pg1"

    status, _h, _b = _req("DescribeParameters", {"ParameterGroupName": "pg1"})
    assert status == 200
    assert _body((status, _h, _b))["Parameters"] == []

    status, _h, body = _req(
        "DescribeParameters", {"ParameterGroupName": "missing"}
    )
    assert status == 400
    assert json.loads(body.decode())["__type"] == "ParameterGroupNotFoundFault"


# ── User round-trip ────────────────────────────────────────────


def test_user_round_trip(env):
    status, _h, _b = _req(
        "CreateUser",
        {
            "UserName": "u1",
            "AccessString": "on ~* +@all",
            "AuthenticationMode": {"Type": "password", "Passwords": ["pw123456789012"]},
        },
    )
    assert status == 200
    user = _body((status, _h, _b))["User"]
    assert user["Name"] == "u1"
    assert user["Status"] == "active"
    assert user["Authentication"]["Type"] == "password"
    assert user["Authentication"]["PasswordCount"] == 1
    assert user["ARN"] == "arn:aws:memorydb:us-east-1:000000000000:user/u1"

    status, _h, _b = _req("DescribeUsers", {})
    assert _body((status, _h, _b))["Users"][0]["Name"] == "u1"

    status, _h, _b = _req("DeleteUser", {"UserName": "u1"})
    assert status == 200

    status, _h, body = _req("DescribeUsers", {"UserName": "u1"})
    assert status == 400
    assert json.loads(body.decode())["__type"] == "UserNotFoundFault"


# ── Tags (Key/Value shape, keyed off ARN) ──────────────────────


def test_tag_round_trip(env):
    _req("CreateCluster", {"ClusterName": "tagged"})
    arn = "arn:aws:memorydb:us-east-1:000000000000:cluster/tagged"

    status, _h, _b = _req(
        "TagResource",
        {"ResourceArn": arn, "Tags": [{"Key": "env", "Value": "prod"}]},
    )
    assert status == 200
    assert _body((status, _h, _b))["TagList"] == [{"Key": "env", "Value": "prod"}]

    status, _h, _b = _req("ListTags", {"ResourceArn": arn})
    assert _body((status, _h, _b))["TagList"] == [{"Key": "env", "Value": "prod"}]

    status, _h, _b = _req(
        "UntagResource", {"ResourceArn": arn, "TagKeys": ["env"]}
    )
    assert status == 200
    assert _body((status, _h, _b))["TagList"] == []


def test_tag_invalid_arn_fault(env):
    status, _h, body = _req(
        "ListTags",
        {"ResourceArn": "arn:aws:memorydb:us-east-1:000000000000:cluster/ghost"},
    )
    assert status == 400
    assert json.loads(body.decode())["__type"] == "InvalidARNFault"


# ── Region scope ───────────────────────────────────────────────


def test_region_scope(env):
    _req("CreateCluster", {"ClusterName": "east"})
    set_request_region("eu-west-1")
    # Cluster created in us-east-1 is not visible in eu-west-1.
    status, _h, _b = _req("DescribeClusters", {})
    assert _body((status, _h, _b))["Clusters"] == []
    status, _h, _b = _req("CreateCluster", {"ClusterName": "west"})
    assert (
        _body((status, _h, _b))["Cluster"]["ARN"]
        == "arn:aws:memorydb:eu-west-1:000000000000:cluster/west"
    )


def test_unknown_action(env):
    status, _h, body = _req("Frobnicate", {})
    assert status == 400
    assert json.loads(body.decode())["__type"] == "InvalidAction"
