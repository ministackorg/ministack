"""CloudFormation AWS::ElastiCache::* resource types (#1874)."""

import json
import os
import uuid

import pytest
from botocore.exceptions import ClientError, WaiterError

from ministack.services.cloudformation.stacks import _diff_resources

_WAIT = {"Delay": 1, "MaxAttempts": 120}
_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"

requires_docker = pytest.mark.skipif(
    not os.environ.get("DOCKER_NETWORK"),
    reason="DOCKER_NETWORK not set - skipping network connectivity test",
)


def _suffix():
    return uuid.uuid4().hex[:8]


def _create(cfn, name, template):
    cfn.create_stack(StackName=name, TemplateBody=json.dumps(template))
    cfn.get_waiter("stack_create_complete").wait(StackName=name, WaiterConfig=_WAIT)


def _update(cfn, name, template):
    cfn.update_stack(StackName=name, TemplateBody=json.dumps(template))
    cfn.get_waiter("stack_update_complete").wait(StackName=name, WaiterConfig=_WAIT)


def _delete(cfn, name):
    cfn.delete_stack(StackName=name)
    cfn.get_waiter("stack_delete_complete").wait(StackName=name, WaiterConfig=_WAIT)


def _outputs(cfn, name):
    stack = cfn.describe_stacks(StackName=name)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def _reasons(cfn, name, logical_id):
    events = cfn.describe_stack_events(StackName=name)["StackEvents"]
    return [e.get("ResourceStatusReason", "") for e in events
            if e["LogicalResourceId"] == logical_id]


def _error_code(call):
    with pytest.raises(ClientError) as exc:
        call()
    return exc.value.response["Error"]["Code"]


def _params(ec, group):
    return {p["ParameterName"]: p["ParameterValue"]
            for p in ec.describe_cache_parameters(CacheParameterGroupName=group)["Parameters"]}


def _cache_stack(s, *, rg_description="sessions", num_cache_clusters=2,
                 params=None, subnet_description="cache subnets",
                 access_string="on ~app:* +@all", extra_user=False,
                 cluster_nodes=1, rg_tags=None):
    resources = {
        "Subnets": {"Type": "AWS::ElastiCache::SubnetGroup", "Properties": {
            "CacheSubnetGroupName": f"subnets-{s}",
            "Description": subnet_description,
            "SubnetIds": ["subnet-aaa", "subnet-bbb"],
        }},
        "Params": {"Type": "AWS::ElastiCache::ParameterGroup", "Properties": {
            "CacheParameterGroupFamily": "redis7",
            "Description": "app params",
            "Properties": params if params is not None else {
                "maxmemory-policy": "allkeys-lru", "timeout": "300"},
        }},
        "User": {"Type": "AWS::ElastiCache::User", "Properties": {
            "UserId": f"app-{s}", "UserName": f"app-{s}", "Engine": "redis",
            "AccessString": access_string, "Passwords": ["correct-horse-battery"],
        }},
        "Users": {"Type": "AWS::ElastiCache::UserGroup", "Properties": {
            "UserGroupId": f"users-{s}", "Engine": "redis",
            "UserIds": [{"Ref": "User"}] + ([{"Ref": "Other"}] if extra_user else []),
        }},
        "Cache": {"Type": "AWS::ElastiCache::ReplicationGroup", "Properties": {
            "ReplicationGroupId": f"cache-{s}",
            "ReplicationGroupDescription": rg_description,
            "Engine": "redis",
            "CacheNodeType": "cache.t3.micro",
            "NumCacheClusters": num_cache_clusters,
            "CacheSubnetGroupName": {"Ref": "Subnets"},
            "CacheParameterGroupName": {"Ref": "Params"},
            "UserGroupIds": [{"Ref": "Users"}],
            "TransitEncryptionEnabled": True,
            "Tags": rg_tags if rg_tags is not None else [{"Key": "team", "Value": "data"}],
        }},
        "Single": {"Type": "AWS::ElastiCache::CacheCluster", "Properties": {
            "ClusterName": f"single-{s}",
            "Engine": "redis",
            "CacheNodeType": "cache.t3.micro",
            "NumCacheNodes": cluster_nodes,
            "CacheSubnetGroupName": {"Ref": "Subnets"},
        }},
    }
    if extra_user:
        resources["Other"] = {"Type": "AWS::ElastiCache::User", "Properties": {
            "UserId": f"other-{s}", "UserName": f"other-{s}", "Engine": "redis",
            "AccessString": "on ~* +@read", "NoPasswordRequired": True,
        }}
    outputs = {k: {"Value": {"Ref": k}} for k in resources}

    def att(logical_id, attr):
        return {"Value": {"Fn::GetAtt": [logical_id, attr]}}

    outputs.update({
        "ParamsName": att("Params", "CacheParameterGroupName"),
        "UserArn": att("User", "Arn"),
        "UsersArn": att("Users", "Arn"),
        "PrimaryAddress": att("Cache", "PrimaryEndPoint.Address"),
        "PrimaryPort": att("Cache", "PrimaryEndPoint.Port"),
        "ReaderAddress": att("Cache", "ReaderEndPoint.Address"),
        "ReadAddresses": att("Cache", "ReadEndPoint.Addresses"),
        "SingleAddress": att("Single", "RedisEndpoint.Address"),
        "SinglePort": att("Single", "RedisEndpoint.Port"),
    })
    return {"Resources": resources, "Outputs": outputs}


def test_cfn_elasticache_stack_create_and_delete(cfn, ec):
    s = _suffix()
    stack = f"ec-full-{s}"
    _create(cfn, stack, _cache_stack(s))
    out = _outputs(cfn, stack)
    assert out["Subnets"] == f"subnets-{s}"
    assert out["Cache"] == f"cache-{s}"
    assert out["Single"] == f"single-{s}"
    assert out["User"] == f"app-{s}" and out["Users"] == f"users-{s}"
    # The parameter group's name is read-only in the schema: always generated.
    assert out["Params"] == out["ParamsName"] == out["Params"].lower()
    try:
        groups = ec.describe_cache_subnet_groups(CacheSubnetGroupName=f"subnets-{s}")
        subnets = groups["CacheSubnetGroups"][0]["Subnets"]
        assert [n["SubnetIdentifier"] for n in subnets] == ["subnet-aaa", "subnet-bbb"]
        assert _params(ec, out["Params"])["maxmemory-policy"] == "allkeys-lru"

        user = ec.describe_users(UserId=f"app-{s}")["Users"][0]
        assert user["ARN"] == out["UserArn"]
        assert user["AccessString"] == "on ~app:* +@all"
        group = ec.describe_user_groups(UserGroupId=f"users-{s}")["UserGroups"][0]
        assert group["ARN"] == out["UsersArn"]
        assert group["UserIds"] == [f"app-{s}"]
        assert group["ReplicationGroups"] == [f"cache-{s}"]

        # The stack's endpoints are the ones the API reports for the same group.
        rg = ec.describe_replication_groups(ReplicationGroupId=f"cache-{s}")["ReplicationGroups"][0]
        primary = rg["NodeGroups"][0]["PrimaryEndpoint"]
        assert (out["PrimaryAddress"], out["PrimaryPort"]) == (primary["Address"], str(primary["Port"]))
        assert out["ReaderAddress"] == rg["NodeGroups"][0]["ReaderEndpoint"]["Address"]
        replicas = [m for m in rg["NodeGroups"][0]["NodeGroupMembers"] if m["CurrentRole"] == "replica"]
        assert out["ReadAddresses"] == ",".join(m["ReadEndpoint"]["Address"] for m in replicas)
        assert rg["CacheNodeType"] == "cache.t3.micro"
        assert rg["TransitEncryptionEnabled"] is True
        tags = ec.list_tags_for_resource(ResourceName=rg["ARN"])["TagList"]
        assert {"Key": "team", "Value": "data"} in tags

        cluster = ec.describe_cache_clusters(
            CacheClusterId=f"single-{s}", ShowCacheNodeInfo=True)["CacheClusters"][0]
        node = cluster["CacheNodes"][0]["Endpoint"]
        assert (out["SingleAddress"], out["SinglePort"]) == (node["Address"], str(node["Port"]))
        assert cluster["CacheSubnetGroupName"] == f"subnets-{s}"
    finally:
        _delete(cfn, stack)

    assert _error_code(lambda: ec.describe_replication_groups(
        ReplicationGroupId=f"cache-{s}")) == "ReplicationGroupNotFoundFault"
    assert _error_code(lambda: ec.describe_cache_clusters(
        CacheClusterId=f"single-{s}")) == "CacheClusterNotFound"
    assert _error_code(lambda: ec.describe_cache_subnet_groups(
        CacheSubnetGroupName=f"subnets-{s}")) == "CacheSubnetGroupNotFoundFault"
    assert _error_code(lambda: ec.describe_cache_parameter_groups(
        CacheParameterGroupName=out["Params"])) == "CacheParameterGroupNotFound"
    assert _error_code(lambda: ec.describe_users(UserId=f"app-{s}")) == "UserNotFound"
    assert _error_code(lambda: ec.describe_user_groups(
        UserGroupId=f"users-{s}")) == "UserGroupNotFound"


def test_cfn_elasticache_update_in_place(cfn, ec):
    s = _suffix()
    stack = f"ec-upd-{s}"
    _create(cfn, stack, _cache_stack(s))
    before = _outputs(cfn, stack)
    try:
        _update(cfn, stack, _cache_stack(
            s, rg_description="sessions v2", num_cache_clusters=3,
            params={"maxmemory-policy": "volatile-ttl"}, subnet_description="new subnets",
            access_string="on ~* +@all", extra_user=True, cluster_nodes=2,
            rg_tags=[{"Key": "team", "Value": "platform"}]))
        after = _outputs(cfn, stack)
        for key in ("Subnets", "Params", "User", "Users", "Cache", "Single"):
            assert after[key] == before[key], key

        rg = ec.describe_replication_groups(ReplicationGroupId=f"cache-{s}")["ReplicationGroups"][0]
        assert rg["Description"] == "sessions v2"
        assert len(rg["NodeGroups"][0]["NodeGroupMembers"]) == 3
        tags = ec.list_tags_for_resource(ResourceName=rg["ARN"])["TagList"]
        assert {"Key": "team", "Value": "platform"} in tags

        params = _params(ec, before["Params"])
        assert params["maxmemory-policy"] == "volatile-ttl"
        # A parameter the template dropped goes back to its default.
        assert params["timeout"] == "0"
        group = ec.describe_cache_subnet_groups(CacheSubnetGroupName=f"subnets-{s}")
        assert group["CacheSubnetGroups"][0]["CacheSubnetGroupDescription"] == "new subnets"
        assert ec.describe_users(UserId=f"app-{s}")["Users"][0]["AccessString"] == "on ~* +@all"
        users = ec.describe_user_groups(UserGroupId=f"users-{s}")["UserGroups"][0]["UserIds"]
        assert sorted(users) == sorted([f"app-{s}", f"other-{s}"])
        cluster = ec.describe_cache_clusters(CacheClusterId=f"single-{s}")["CacheClusters"][0]
        assert cluster["NumCacheNodes"] == 2
    finally:
        _delete(cfn, stack)


def test_cfn_elasticache_replication_group_rename_replaces(cfn, ec):
    s = _suffix()
    stack = f"ec-ren-{s}"

    def template(rg_id):
        return {"Resources": {"Rg": {"Type": "AWS::ElastiCache::ReplicationGroup", "Properties": {
            "ReplicationGroupId": rg_id, "ReplicationGroupDescription": "rename",
            "Engine": "redis", "CacheNodeType": "cache.t3.micro"}}}}

    _create(cfn, stack, template(f"first-{s}"))
    try:
        _update(cfn, stack, template(f"second-{s}"))
        ec.describe_replication_groups(ReplicationGroupId=f"second-{s}")
        assert _error_code(lambda: ec.describe_replication_groups(
            ReplicationGroupId=f"first-{s}")) == "ReplicationGroupNotFoundFault"
    finally:
        _delete(cfn, stack)


def test_cfn_elasticache_generated_cluster_replaced_on_create_only_change(cfn, ec):
    s = _suffix()
    stack = f"Ec-Gen-{s}"

    def template(subnet_ref):
        return {
            "Resources": {
                "A": {"Type": "AWS::ElastiCache::SubnetGroup", "Properties": {
                    "Description": "a", "SubnetIds": ["subnet-a"]}},
                "B": {"Type": "AWS::ElastiCache::SubnetGroup", "Properties": {
                    "Description": "b", "SubnetIds": ["subnet-b"]}},
                "C": {"Type": "AWS::ElastiCache::CacheCluster", "Properties": {
                    "Engine": "redis", "CacheNodeType": "cache.t3.micro", "NumCacheNodes": 1,
                    "CacheSubnetGroupName": {"Ref": subnet_ref}}},
            },
            "Outputs": {k: {"Value": {"Ref": k}} for k in ("A", "B", "C")},
        }

    _create(cfn, stack, template("A"))
    try:
        out = _outputs(cfn, stack)
        # Generated names are lowercase, as ElastiCache stores them, and a
        # cluster name fits the 50-character limit.
        assert out["A"] == out["A"].lower() and out["A"].startswith(f"ec-gen-{s}-a-")
        assert out["C"] == out["C"].lower() and len(out["C"]) <= 50
        first = ec.describe_cache_clusters(CacheClusterId=out["C"])["CacheClusters"][0]
        # CacheSubnetGroupName is create-only: the cluster is replaced and
        # takes its generated name back.
        _update(cfn, stack, template("B"))
        assert _outputs(cfn, stack)["C"] == out["C"]
        second = ec.describe_cache_clusters(CacheClusterId=out["C"])["CacheClusters"][0]
        assert second["CacheSubnetGroupName"] == out["B"]
        assert second["CacheClusterCreateTime"] >= first["CacheClusterCreateTime"]
    finally:
        _delete(cfn, stack)


def test_cfn_elasticache_named_cluster_engine_change_is_refused(cfn, ec):
    s = _suffix()
    stack = f"ec-ref-{s}"

    def template(engine):
        return {"Resources": {"C": {"Type": "AWS::ElastiCache::CacheCluster", "Properties": {
            "ClusterName": f"named-{s}", "Engine": engine,
            "CacheNodeType": "cache.t3.micro", "NumCacheNodes": 1}}}}

    _create(cfn, stack, template("redis"))
    try:
        cfn.update_stack(StackName=stack, TemplateBody=json.dumps(template("memcached")))
        with pytest.raises(WaiterError):
            cfn.get_waiter("stack_update_complete").wait(StackName=stack, WaiterConfig=_WAIT)
        assert any("custom-named resource requires replacing" in r
                   for r in _reasons(cfn, stack, "C"))
        cluster = ec.describe_cache_clusters(CacheClusterId=f"named-{s}")["CacheClusters"][0]
        assert cluster["Engine"] == "redis"
    finally:
        _delete(cfn, stack)


def test_cfn_elasticache_configuration_endpoint_needs_cluster_mode(cfn):
    # "Fn::GetAtt returns a value for this attribute only if the replication
    # group is clustered. Otherwise, Fn::GetAtt fails."
    s = _suffix()
    stack = f"ec-cfg-{s}"
    cfn.create_stack(StackName=stack, TemplateBody=json.dumps({
        "Resources": {"Rg": {"Type": "AWS::ElastiCache::ReplicationGroup", "Properties": {
            "ReplicationGroupDescription": "no cluster mode", "Engine": "redis",
            "CacheNodeType": "cache.t3.micro"}}},
        "Outputs": {"Cfg": {"Value": {"Fn::GetAtt": ["Rg", "ConfigurationEndPoint.Address"]}}},
    }))
    try:
        with pytest.raises(WaiterError):
            cfn.get_waiter("stack_create_complete").wait(StackName=stack, WaiterConfig=_WAIT)
        status = cfn.describe_stacks(StackName=stack)["Stacks"][0]["StackStatus"]
        assert status == "ROLLBACK_COMPLETE"
    finally:
        _delete(cfn, stack)


def test_cfn_elasticache_memcached_configuration_endpoint(cfn, ec):
    s = _suffix()
    stack = f"ec-mc-{s}"
    _create(cfn, stack, {
        "Resources": {"Mc": {"Type": "AWS::ElastiCache::CacheCluster", "Properties": {
            "ClusterName": f"mc-{s}", "Engine": "memcached", "EngineVersion": "1.6.17",
            "CacheNodeType": "cache.t3.micro", "NumCacheNodes": 1}}},
        "Outputs": {"Address": {"Value": {"Fn::GetAtt": ["Mc", "ConfigurationEndpoint.Address"]}},
                    "Port": {"Value": {"Fn::GetAtt": ["Mc", "ConfigurationEndpoint.Port"]}}},
    })
    try:
        out = _outputs(cfn, stack)
        node = ec.describe_cache_clusters(
            CacheClusterId=f"mc-{s}", ShowCacheNodeInfo=True)["CacheClusters"][0]["CacheNodes"][0]
        assert (out["Address"], out["Port"]) == (node["Endpoint"]["Address"], str(node["Endpoint"]["Port"]))
    finally:
        _delete(cfn, stack)


# Expected values follow the createOnlyProperties and
# conditionalCreateOnlyProperties of the published registry schemas; they were
# not observed on an AWS change set.
@pytest.mark.parametrize("rtype,old,new,expected", [
    ("AWS::ElastiCache::CacheCluster", {"Engine": "redis"}, {"Engine": "memcached"}, "Always"),
    ("AWS::ElastiCache::CacheCluster", {"NumCacheNodes": 1}, {"NumCacheNodes": 2}, "Never"),
    ("AWS::ElastiCache::CacheCluster", {"IpDiscovery": "ipv4"}, {"IpDiscovery": "ipv6"}, "Conditionally"),
    ("AWS::ElastiCache::ReplicationGroup", {"Port": 6379}, {"Port": 6380}, "Always"),
    ("AWS::ElastiCache::ReplicationGroup", {"AuthToken": "a" * 16}, {"AuthToken": "b" * 16}, "Conditionally"),
    ("AWS::ElastiCache::ReplicationGroup", {"CacheNodeType": "cache.t3.micro"},
     {"CacheNodeType": "cache.t3.small"}, "Never"),
    ("AWS::ElastiCache::ParameterGroup", {"CacheParameterGroupFamily": "redis6.x"},
     {"CacheParameterGroupFamily": "redis7"}, "Always"),
    ("AWS::ElastiCache::User", {"UserName": "a"}, {"UserName": "b"}, "Always"),
    ("AWS::ElastiCache::UserGroup", {"UserIds": ["a"]}, {"UserIds": ["b"]}, "Never"),
])
def test_cfn_elasticache_change_set_recreation(rtype, old, new, expected):
    def tmpl(props):
        return {"Resources": {"R": {"Type": rtype, "Properties": props}}}

    change = _diff_resources(tmpl(old), tmpl(new))[0]["ResourceChange"]
    targets = {d["Target"]["Name"]: d["Target"]["RequiresRecreation"]
               for d in change.get("Details", []) if d["Target"].get("Name")}
    assert set(targets.values()) == {expected}


@requires_docker
@pytest.mark.data_plane
def test_cfn_elasticache_lambda_reaches_replication_group(cfn, lam):
    """The issue's use case: a stack passes PrimaryEndPoint into a function's
    environment, and the function talks to the container behind it."""
    s = _suffix()
    stack = f"ec-lam-{s}"
    code = (
        "import os, socket\n"
        "def handler(event, context):\n"
        "    s = socket.create_connection((os.environ['REDIS_HOST'], int(os.environ['REDIS_PORT'])), timeout=5)\n"
        "    s.sendall(b'PING\\r\\n')\n"
        "    reply = s.recv(64).decode()\n"
        "    s.close()\n"
        "    return {'reply': reply.strip()}\n"
    )
    _create(cfn, stack, {
        "Resources": {
            "Cache": {"Type": "AWS::ElastiCache::ReplicationGroup", "Properties": {
                "ReplicationGroupDescription": "lambda", "Engine": "redis",
                "CacheNodeType": "cache.t3.micro"}},
            "Fn": {"Type": "AWS::Lambda::Function", "Properties": {
                "FunctionName": f"ec-ping-{s}", "Runtime": "python3.12",
                "Handler": "index.handler", "Role": _LAMBDA_ROLE, "Timeout": 15,
                "Code": {"ZipFile": code},
                "Environment": {"Variables": {
                    "REDIS_HOST": {"Fn::GetAtt": ["Cache", "PrimaryEndPoint.Address"]},
                    "REDIS_PORT": {"Fn::GetAtt": ["Cache", "PrimaryEndPoint.Port"]},
                }}}},
        },
    })
    try:
        import time
        deadline = time.time() + 60
        while True:
            resp = lam.invoke(FunctionName=f"ec-ping-{s}", Payload=b"{}")
            result = json.loads(resp["Payload"].read())
            if result.get("reply") == "+PONG" or time.time() > deadline:
                break
            time.sleep(1)
        assert result.get("reply") == "+PONG", result
    finally:
        _delete(cfn, stack)
