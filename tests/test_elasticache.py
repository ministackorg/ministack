import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from ministack.services import elasticache
from ministack.services.elasticache import _engine_image_and_port

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")

# Most ElastiCache tests need a live Docker network because CreateCacheCluster
# spawns a real Redis container. Mark them with @requires_docker so they skip
# cleanly in CI without docker, while unit-style tests (e.g. the #853 respawn
# tests at the bottom of this file) still run.
requires_docker = pytest.mark.skipif(
    not os.environ.get("DOCKER_NETWORK"),
    reason="DOCKER_NETWORK not set - skipping network connectivity test",
)


def _ec_client(region):
    return boto3.client(
        "elasticache",
        endpoint_url=ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )

@requires_docker
def test_elasticache_create(ec):
    ec.create_cache_cluster(
        CacheClusterId="test-redis",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.describe_cache_clusters(CacheClusterId="test-redis")
    clusters = resp["CacheClusters"]
    assert len(clusters) == 1
    assert clusters[0]["CacheClusterId"] == "test-redis"
    assert clusters[0]["Engine"] == "redis"

@requires_docker
def test_elasticache_cache_node_full_fields(ec):
    """terraform-provider-aws v6 derefs CacheNodeCreateTime / ParameterGroupStatus /
    CustomerAvailabilityZone without nil checks. Issue #675."""
    ec.create_cache_cluster(
        CacheClusterId="cn-fields",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.describe_cache_clusters(CacheClusterId="cn-fields", ShowCacheNodeInfo=True)
    node = resp["CacheClusters"][0]["CacheNodes"][0]
    assert "CacheNodeCreateTime" in node
    assert node["ParameterGroupStatus"] == "in-sync"
    assert node["CustomerAvailabilityZone"]  # non-empty
    assert node["CacheNodeId"] == "0001"
    assert node["CacheNodeStatus"] == "available"


@requires_docker
def test_elasticache_replication_group(ec):
    ec.create_replication_group(
        ReplicationGroupId="test-rg",
        ReplicationGroupDescription="Test replication group",
        CacheNodeType="cache.t3.micro",
    )
    resp = ec.describe_replication_groups(ReplicationGroupId="test-rg")
    assert resp["ReplicationGroups"][0]["ReplicationGroupId"] == "test-rg"

@requires_docker
def test_elasticache_engines(ec):
    resp = ec.describe_cache_engine_versions(Engine="redis")
    assert len(resp["CacheEngineVersions"]) > 0

@requires_docker
def test_elasticache_valkey_engine_versions(ec):
    resp = ec.describe_cache_engine_versions(Engine="valkey")
    versions = resp["CacheEngineVersions"]
    assert len(versions) > 0
    assert all(v["Engine"] == "valkey" for v in versions)
    assert all(v["CacheParameterGroupFamily"].startswith("valkey") for v in versions)

@requires_docker
def test_elasticache_valkey_create_cluster(ec):
    ec.create_cache_cluster(
        CacheClusterId="test-valkey",
        Engine="valkey",
        EngineVersion="8.0",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.describe_cache_clusters(CacheClusterId="test-valkey", ShowCacheNodeInfo=True)
    cluster = resp["CacheClusters"][0]
    assert cluster["Engine"] == "valkey"
    assert cluster["EngineVersion"] == "8.0"
    assert cluster["CacheParameterGroup"]["CacheParameterGroupName"] == "default.valkey8"
    # A real valkey container (or the redis-protocol fallback) — never memcached's port.
    assert cluster["CacheNodes"][0]["Endpoint"]["Port"] != 11211

@requires_docker
def test_elasticache_valkey_replication_group(ec):
    ec.create_replication_group(
        ReplicationGroupId="test-valkey-rg",
        ReplicationGroupDescription="Valkey replication group",
        Engine="valkey",
        CacheNodeType="cache.t3.micro",
    )
    resp = ec.describe_replication_groups(ReplicationGroupId="test-valkey-rg")
    rg = resp["ReplicationGroups"][0]
    assert rg["ReplicationGroupId"] == "test-valkey-rg"
    assert rg["NodeGroups"][0]["PrimaryEndpoint"]["Port"] != 11211

@requires_docker
def test_elasticache_modify_subnet_group(ec):
    ec.create_cache_subnet_group(
        CacheSubnetGroupName="test-mod-ecsg",
        CacheSubnetGroupDescription="Test EC SG",
        SubnetIds=["subnet-aaa"],
    )
    ec.modify_cache_subnet_group(
        CacheSubnetGroupName="test-mod-ecsg",
        CacheSubnetGroupDescription="Updated EC SG",
        SubnetIds=["subnet-bbb"],
    )
    resp = ec.describe_cache_subnet_groups(CacheSubnetGroupName="test-mod-ecsg")
    assert resp["CacheSubnetGroups"][0]["CacheSubnetGroupDescription"] == "Updated EC SG"

@requires_docker
def test_elasticache_user_crud(ec):
    ec.create_user(
        UserId="test-user-1",
        UserName="test-user-1",
        Engine="redis",
        AccessString="on ~* +@all",
        NoPasswordRequired=True,
    )
    resp = ec.describe_users(UserId="test-user-1")
    assert len(resp["Users"]) >= 1
    assert resp["Users"][0]["UserId"] == "test-user-1"
    ec.modify_user(UserId="test-user-1", AccessString="on ~keys:* +get")
    ec.delete_user(UserId="test-user-1")

@requires_docker
def test_elasticache_user_group_crud(ec):
    ec.create_user(
        UserId="ug-usr-1",
        UserName="ug-usr-1",
        Engine="redis",
        AccessString="on ~* +@all",
        NoPasswordRequired=True,
    )
    ec.create_user_group(UserGroupId="test-ug-1", Engine="redis", UserIds=["ug-usr-1"])
    resp = ec.describe_user_groups(UserGroupId="test-ug-1")
    assert len(resp["UserGroups"]) >= 1
    assert resp["UserGroups"][0]["UserGroupId"] == "test-ug-1"
    ec.delete_user_group(UserGroupId="test-ug-1")
    ec.delete_user(UserId="ug-usr-1")

@requires_docker
def test_elasticache_reset_clears_param_groups():
    """ElastiCache reset restores built-in parameter groups and resets port counter."""
    from ministack.services import elasticache as _ec
    _ec._param_group_params["test-group"] = {"param1": "val1"}
    _ec._port_counter[0] = 99999
    _ec.reset()
    assert "test-group" not in _ec._param_group_params
    assert "default.redis7" in _ec._param_group_params
    assert _ec._port_counter[0] == _ec.BASE_PORT

@requires_docker
def test_elasticache_parameter_group_crud(ec):
    """CreateCacheParameterGroup / DescribeCacheParameterGroups / DeleteCacheParameterGroup."""
    ec.create_cache_parameter_group(
        CacheParameterGroupName="test-pg-v39",
        CacheParameterGroupFamily="redis7",
        Description="Test param group",
    )
    desc = ec.describe_cache_parameter_groups(CacheParameterGroupName="test-pg-v39")
    groups = desc["CacheParameterGroups"]
    assert len(groups) == 1
    assert groups[0]["CacheParameterGroupName"] == "test-pg-v39"
    assert groups[0]["CacheParameterGroupFamily"] == "redis7"
    ec.delete_cache_parameter_group(CacheParameterGroupName="test-pg-v39")

@requires_docker
def test_elasticache_snapshot_crud(ec):
    """CreateSnapshot / DescribeSnapshots / DeleteSnapshot."""
    ec.create_cache_cluster(
        CacheClusterId="snap-cluster-v39",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    ec.create_snapshot(SnapshotName="test-snap-v39", CacheClusterId="snap-cluster-v39")
    desc = ec.describe_snapshots(SnapshotName="test-snap-v39")
    assert len(desc["Snapshots"]) == 1
    assert desc["Snapshots"][0]["SnapshotName"] == "test-snap-v39"
    ec.delete_snapshot(SnapshotName="test-snap-v39")

@requires_docker
def test_elasticache_tags(ec):
    """AddTagsToResource / ListTagsForResource / RemoveTagsFromResource."""
    ec.create_cache_cluster(
        CacheClusterId="tag-cluster-v39",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    arn = "arn:aws:elasticache:us-east-1:000000000000:cluster:tag-cluster-v39"
    ec.add_tags_to_resource(
        ResourceName=arn,
        Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}],
    )
    tags = ec.list_tags_for_resource(ResourceName=arn)
    tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "platform"
    ec.remove_tags_from_resource(ResourceName=arn, TagKeys=["team"])
    tags = ec.list_tags_for_resource(ResourceName=arn)
    tag_keys = [t["Key"] for t in tags["TagList"]]
    assert "env" in tag_keys
    assert "team" not in tag_keys


def test_elasticache_tag_arns_must_parse_to_local_resources(ec):
    name = f"tag-pg-{_uuid_mod.uuid4().hex[:8]}"
    ec.create_cache_parameter_group(
        CacheParameterGroupName=name,
        CacheParameterGroupFamily="redis7.0",
        Description="tag parser test",
    )
    arn = f"arn:aws:elasticache:us-east-1:000000000000:parametergroup:{name}"

    ec.add_tags_to_resource(ResourceName=arn, Tags=[{"Key": "env", "Value": "test"}])
    tags = ec.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert {t["Key"]: t["Value"] for t in tags} == {"env": "test"}

    with pytest.raises(ClientError) as exc:
        ec.add_tags_to_resource(
            ResourceName=f"arn:aws:elasticache:us-west-2:000000000000:parametergroup:{name}",
            Tags=[{"Key": "env", "Value": "test"}],
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

    with pytest.raises(ClientError) as exc:
        ec.list_tags_for_resource(
            ResourceName="arn:aws:elasticache:us-east-1:000000000000:parametergroup:missing"
        )
    assert exc.value.response["Error"]["Code"] == "CacheParameterGroupNotFound"

    ec.delete_cache_parameter_group(CacheParameterGroupName=name)


def test_elasticache_cluster_tag_arn_uses_cache_cluster_arn_field(ec):
    cluster_id = f"tag-cluster-{_uuid_mod.uuid4().hex[:8]}"
    ec.create_cache_cluster(
        CacheClusterId=cluster_id,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    arn = ec.describe_cache_clusters(CacheClusterId=cluster_id)["CacheClusters"][0]["ARN"]

    ec.add_tags_to_resource(ResourceName=arn, Tags=[{"Key": "env", "Value": "test"}])
    tags = ec.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert {t["Key"]: t["Value"] for t in tags} == {"env": "test"}

    ec.delete_cache_cluster(CacheClusterId=cluster_id)


# Migrated from test_ec.py
@requires_docker
def test_elasticache_create_cluster_v2(ec):
    resp = ec.create_cache_cluster(
        CacheClusterId="ec-cc-v2",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    c = resp["CacheCluster"]
    assert c["CacheClusterId"] == "ec-cc-v2"
    assert c["Engine"] == "redis"
    assert c["CacheClusterStatus"] == "available"
    assert len(c["CacheNodes"]) == 1

@requires_docker
def test_elasticache_describe_clusters_v2(ec):
    ec.create_cache_cluster(
        CacheClusterId="ec-dc-v2a",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    ec.create_cache_cluster(
        CacheClusterId="ec-dc-v2b",
        Engine="memcached",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.describe_cache_clusters()
    ids = [c["CacheClusterId"] for c in resp["CacheClusters"]]
    assert "ec-dc-v2a" in ids
    assert "ec-dc-v2b" in ids

    resp2 = ec.describe_cache_clusters(CacheClusterId="ec-dc-v2b")
    assert resp2["CacheClusters"][0]["Engine"] == "memcached"

@requires_docker
def test_elasticache_replication_group_v2(ec):
    resp = ec.create_replication_group(
        ReplicationGroupId="ec-rg-v2",
        ReplicationGroupDescription="Test RG v2",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumNodeGroups=1,
        ReplicasPerNodeGroup=1,
    )
    rg = resp["ReplicationGroup"]
    assert rg["ReplicationGroupId"] == "ec-rg-v2"
    assert rg["Status"] == "available"
    assert len(rg["NodeGroups"]) == 1

    desc = ec.describe_replication_groups(ReplicationGroupId="ec-rg-v2")
    assert desc["ReplicationGroups"][0]["ReplicationGroupId"] == "ec-rg-v2"

@requires_docker
def test_elasticache_engine_versions_v2(ec):
    redis = ec.describe_cache_engine_versions(Engine="redis")
    assert len(redis["CacheEngineVersions"]) > 0
    assert all(v["Engine"] == "redis" for v in redis["CacheEngineVersions"])

    mc = ec.describe_cache_engine_versions(Engine="memcached")
    assert len(mc["CacheEngineVersions"]) > 0

@requires_docker
def test_elasticache_tags_v2(ec):
    ec.create_cache_cluster(
        CacheClusterId="ec-tag-v2",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    arn = ec.describe_cache_clusters(CacheClusterId="ec-tag-v2")["CacheClusters"][0]["ARN"]

    ec.add_tags_to_resource(
        ResourceName=arn,
        Tags=[
            {"Key": "env", "Value": "prod"},
            {"Key": "tier", "Value": "cache"},
        ],
    )
    tags = ec.list_tags_for_resource(ResourceName=arn)["TagList"]
    tag_map = {t["Key"]: t["Value"] for t in tags}
    assert tag_map["env"] == "prod"
    assert tag_map["tier"] == "cache"

    ec.remove_tags_from_resource(ResourceName=arn, TagKeys=["env"])
    tags2 = ec.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert not any(t["Key"] == "env" for t in tags2)
    assert any(t["Key"] == "tier" for t in tags2)

@requires_docker
def test_elasticache_snapshot_v2(ec):
    ec.create_cache_cluster(
        CacheClusterId="ec-snap-v2",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.create_snapshot(SnapshotName="ec-snap-v2-s1", CacheClusterId="ec-snap-v2")
    assert resp["Snapshot"]["SnapshotName"] == "ec-snap-v2-s1"
    assert resp["Snapshot"]["SnapshotStatus"] == "available"

    desc = ec.describe_snapshots(SnapshotName="ec-snap-v2-s1")
    assert len(desc["Snapshots"]) == 1
    assert desc["Snapshots"][0]["SnapshotName"] == "ec-snap-v2-s1"

@requires_docker
def test_elasticache_describe_cache_parameters(ec):
    """DescribeCacheParameters returns parameters for a parameter group."""
    ec.create_cache_parameter_group(
        CacheParameterGroupName="qa-ec-params",
        CacheParameterGroupFamily="redis7.0",
        Description="test",
    )
    resp = ec.describe_cache_parameters(CacheParameterGroupName="qa-ec-params")
    assert "Parameters" in resp
    assert len(resp["Parameters"]) > 0

@requires_docker
def test_elasticache_modify_cache_parameter_group(ec):
    """ModifyCacheParameterGroup updates parameter values."""
    ec.create_cache_parameter_group(
        CacheParameterGroupName="qa-ec-modify-params",
        CacheParameterGroupFamily="redis7.0",
        Description="test",
    )
    ec.modify_cache_parameter_group(
        CacheParameterGroupName="qa-ec-modify-params",
        ParameterNameValues=[{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"}],
    )
    params = ec.describe_cache_parameters(CacheParameterGroupName="qa-ec-modify-params")["Parameters"]
    maxmem = next((p for p in params if p["ParameterName"] == "maxmemory-policy"), None)
    assert maxmem is not None
    assert maxmem["ParameterValue"] == "allkeys-lru"


def test_elasticache_default_parameter_groups_are_immutable(ec):
    """Built-in default parameter groups cannot be overwritten, deleted, or modified."""
    resp = ec.describe_cache_parameter_groups(CacheParameterGroupName="default.redis7")
    assert resp["CacheParameterGroups"][0]["CacheParameterGroupFamily"] == "redis7"

    with pytest.raises(ClientError) as exc:
        ec.create_cache_parameter_group(
            CacheParameterGroupName="default.redis7",
            CacheParameterGroupFamily="redis7",
            Description="duplicate default",
        )
    assert exc.value.response["Error"]["Code"] == "CacheParameterGroupAlreadyExists"

    with pytest.raises(ClientError) as exc:
        ec.delete_cache_parameter_group(CacheParameterGroupName="default.redis7")
    assert exc.value.response["Error"]["Code"] == "InvalidCacheParameterGroupState"

    with pytest.raises(ClientError) as exc:
        ec.modify_cache_parameter_group(
            CacheParameterGroupName="default.redis7",
            ParameterNameValues=[{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"}],
        )
    assert exc.value.response["Error"]["Code"] == "InvalidCacheParameterGroupState"

    with pytest.raises(ClientError) as exc:
        ec.reset_cache_parameter_group(
            CacheParameterGroupName="default.redis7",
            ResetAllParameters=True,
        )
    assert exc.value.response["Error"]["Code"] == "InvalidCacheParameterGroupState"


def test_elasticache_default_parameter_group_family_mapping(ec):
    """Default parameter group names use AWS-style engine family names."""
    cid = f"pg-map-mc-{_uid()}"
    resp = ec.create_cache_cluster(
        CacheClusterId=cid,
        Engine="memcached",
        EngineVersion="1.6.17",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    group = resp["CacheCluster"]["CacheParameterGroup"]
    assert group["CacheParameterGroupName"] == "default.memcached1.6"
    ec.delete_cache_cluster(CacheClusterId=cid)

    redis_versions = ec.describe_cache_engine_versions(Engine="redis")["CacheEngineVersions"]
    families = {v["EngineVersion"]: v["CacheParameterGroupFamily"] for v in redis_versions}
    assert families["7.0.12"] == "redis7"
    assert families["6.2.14"] == "redis6.x"
    assert families["5.0.6"] == "redis5.0"


def _uid():
    return _uuid_mod.uuid4().hex[:8]


def test_elasticache_clusters_are_region_scoped_by_name():
    east = _ec_client("us-east-1")
    west = _ec_client("us-west-2")
    cid = f"scope-cluster-{_uid()}"

    east.create_cache_cluster(
        CacheClusterId=cid,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    west.create_cache_cluster(
        CacheClusterId=cid,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )

    try:
        east_cluster = east.describe_cache_clusters(CacheClusterId=cid)["CacheClusters"][0]
        west_cluster = west.describe_cache_clusters(CacheClusterId=cid)["CacheClusters"][0]

        assert east_cluster["ARN"] == f"arn:aws:elasticache:us-east-1:000000000000:cluster:{cid}"
        assert west_cluster["ARN"] == f"arn:aws:elasticache:us-west-2:000000000000:cluster:{cid}"
        assert east_cluster["PreferredAvailabilityZone"].startswith("us-east-1")
        assert west_cluster["PreferredAvailabilityZone"].startswith("us-west-2")

        west.delete_cache_cluster(CacheClusterId=cid)
        with pytest.raises(ClientError) as exc:
            west.describe_cache_clusters(CacheClusterId=cid)
        assert exc.value.response["Error"]["Code"] == "CacheClusterNotFound"
        assert east.describe_cache_clusters(CacheClusterId=cid)["CacheClusters"][0]["ARN"] == east_cluster["ARN"]
    finally:
        for client in (east, west):
            try:
                client.delete_cache_cluster(CacheClusterId=cid)
            except ClientError:
                pass


def test_elasticache_replication_groups_are_region_scoped():
    east = _ec_client("us-east-1")
    west = _ec_client("us-west-2")
    rg_id = f"scope-rg-{_uid()}"

    east.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="east rg",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
    )
    west.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="west rg",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
    )

    try:
        east_rg = east.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]
        west_rg = west.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]

        assert east_rg["ARN"] == f"arn:aws:elasticache:us-east-1:000000000000:replicationgroup:{rg_id}"
        assert west_rg["ARN"] == f"arn:aws:elasticache:us-west-2:000000000000:replicationgroup:{rg_id}"
        assert east_rg["Description"] == "east rg"
        assert west_rg["Description"] == "west rg"

        east.delete_replication_group(ReplicationGroupId=rg_id)
        with pytest.raises(ClientError) as exc:
            east.describe_replication_groups(ReplicationGroupId=rg_id)
        assert exc.value.response["Error"]["Code"] == "ReplicationGroupNotFoundFault"
        assert west.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]["ARN"] == west_rg["ARN"]
    finally:
        for client in (east, west):
            try:
                client.delete_replication_group(ReplicationGroupId=rg_id)
            except ClientError:
                pass


def test_elasticache_users_and_user_groups_are_region_scoped():
    east = _ec_client("us-east-1")
    west = _ec_client("us-west-2")
    user_id = f"scope-user-{_uid()}"
    group_id = f"scope-group-{_uid()}"

    for client in (east, west):
        client.create_user(
            UserId=user_id,
            UserName=user_id,
            Engine="redis",
            AccessString="on ~* +@all",
            NoPasswordRequired=True,
        )
        client.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[user_id])

    try:
        east_user = east.describe_users(UserId=user_id)["Users"][0]
        west_user = west.describe_users(UserId=user_id)["Users"][0]
        east_group = east.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]
        west_group = west.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]

        assert east_user["ARN"] == f"arn:aws:elasticache:us-east-1:000000000000:user:{user_id}"
        assert west_user["ARN"] == f"arn:aws:elasticache:us-west-2:000000000000:user:{user_id}"
        assert east_group["ARN"] == f"arn:aws:elasticache:us-east-1:000000000000:usergroup:{group_id}"
        assert west_group["ARN"] == f"arn:aws:elasticache:us-west-2:000000000000:usergroup:{group_id}"

        east.delete_user_group(UserGroupId=group_id)
        east.delete_user(UserId=user_id)
        with pytest.raises(ClientError) as exc:
            east.describe_users(UserId=user_id)
        assert exc.value.response["Error"]["Code"] == "UserNotFound"
        assert west.describe_users(UserId=user_id)["Users"][0]["ARN"] == west_user["ARN"]
    finally:
        for client in (east, west):
            try:
                client.delete_user_group(UserGroupId=group_id)
            except ClientError:
                pass
            try:
                client.delete_user(UserId=user_id)
            except ClientError:
                pass


def test_elasticache_subnet_and_param_groups_are_region_scoped():
    east = _ec_client("us-east-1")
    west = _ec_client("us-west-2")
    subnet_name = f"scope-subnet-{_uid()}"
    param_name = f"scope-param-{_uid()}"

    east.create_cache_subnet_group(
        CacheSubnetGroupName=subnet_name,
        CacheSubnetGroupDescription="east subnet",
        SubnetIds=["subnet-east"],
    )
    west.create_cache_subnet_group(
        CacheSubnetGroupName=subnet_name,
        CacheSubnetGroupDescription="west subnet",
        SubnetIds=["subnet-west"],
    )
    east.create_cache_parameter_group(
        CacheParameterGroupName=param_name,
        CacheParameterGroupFamily="redis7",
        Description="east param",
    )
    west.create_cache_parameter_group(
        CacheParameterGroupName=param_name,
        CacheParameterGroupFamily="redis7",
        Description="west param",
    )

    try:
        east_subnet = east.describe_cache_subnet_groups(CacheSubnetGroupName=subnet_name)["CacheSubnetGroups"][0]
        west_subnet = west.describe_cache_subnet_groups(CacheSubnetGroupName=subnet_name)["CacheSubnetGroups"][0]
        east_param = east.describe_cache_parameter_groups(CacheParameterGroupName=param_name)["CacheParameterGroups"][0]
        west_param = west.describe_cache_parameter_groups(CacheParameterGroupName=param_name)["CacheParameterGroups"][0]

        assert east_subnet["ARN"] == f"arn:aws:elasticache:us-east-1:000000000000:subnetgroup:{subnet_name}"
        assert west_subnet["ARN"] == f"arn:aws:elasticache:us-west-2:000000000000:subnetgroup:{subnet_name}"
        assert east_param["ARN"] == f"arn:aws:elasticache:us-east-1:000000000000:parametergroup:{param_name}"
        assert west_param["ARN"] == f"arn:aws:elasticache:us-west-2:000000000000:parametergroup:{param_name}"

        west.delete_cache_subnet_group(CacheSubnetGroupName=subnet_name)
        west.delete_cache_parameter_group(CacheParameterGroupName=param_name)
        with pytest.raises(ClientError) as exc:
            west.describe_cache_subnet_groups(CacheSubnetGroupName=subnet_name)
        assert exc.value.response["Error"]["Code"] == "CacheSubnetGroupNotFoundFault"
        with pytest.raises(ClientError) as exc:
            west.describe_cache_parameter_groups(CacheParameterGroupName=param_name)
        assert exc.value.response["Error"]["Code"] == "CacheParameterGroupNotFound"
        assert east.describe_cache_subnet_groups(CacheSubnetGroupName=subnet_name)["CacheSubnetGroups"][0]["ARN"] == east_subnet["ARN"]
        assert east.describe_cache_parameter_groups(CacheParameterGroupName=param_name)["CacheParameterGroups"][0]["ARN"] == east_param["ARN"]
    finally:
        for client in (east, west):
            try:
                client.delete_cache_subnet_group(CacheSubnetGroupName=subnet_name)
            except ClientError:
                pass
            try:
                client.delete_cache_parameter_group(CacheParameterGroupName=param_name)
            except ClientError:
                pass


def test_elasticache_default_param_groups_seed_per_region():
    east = _ec_client("us-east-1")
    west = _ec_client("us-west-2")

    east_group = east.describe_cache_parameter_groups(
        CacheParameterGroupName="default.redis7",
    )["CacheParameterGroups"][0]
    west_group = west.describe_cache_parameter_groups(
        CacheParameterGroupName="default.redis7",
    )["CacheParameterGroups"][0]

    assert east_group["ARN"] == "arn:aws:elasticache:us-east-1:000000000000:parametergroup:default.redis7"
    assert west_group["ARN"] == "arn:aws:elasticache:us-west-2:000000000000:parametergroup:default.redis7"


def test_elasticache_tags_stay_arn_keyed_and_request_region_validated():
    east = _ec_client("us-east-1")
    west = _ec_client("us-west-2")
    name = f"tag-scope-{_uid()}"

    east.create_cache_parameter_group(
        CacheParameterGroupName=name,
        CacheParameterGroupFamily="redis7",
        Description="tag scope",
    )
    arn = f"arn:aws:elasticache:us-east-1:000000000000:parametergroup:{name}"
    try:
        east.add_tags_to_resource(ResourceName=arn, Tags=[{"Key": "env", "Value": "east"}])
        assert east.list_tags_for_resource(ResourceName=arn)["TagList"] == [
            {"Key": "env", "Value": "east"},
        ]

        with pytest.raises(ClientError) as exc:
            west.list_tags_for_resource(ResourceName=arn)
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"
    finally:
        try:
            east.delete_cache_parameter_group(CacheParameterGroupName=name)
        except ClientError:
            pass


def test_elasticache_events_are_region_scoped():
    east = _ec_client("us-east-1")
    west = _ec_client("us-west-2")
    cid = f"event-scope-{_uid()}"

    east.create_cache_cluster(
        CacheClusterId=cid,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    west.create_cache_cluster(
        CacheClusterId=cid,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )

    try:
        east_events = east.describe_events(SourceIdentifier=cid)["Events"]
        west_events = west.describe_events(SourceIdentifier=cid)["Events"]

        assert len(east_events) == 1
        assert len(west_events) == 1
        assert east_events[0]["Message"] == "Cache cluster created"
        assert west_events[0]["Message"] == "Cache cluster created"

        east.delete_cache_cluster(CacheClusterId=cid)
        east_events = east.describe_events(SourceIdentifier=cid)["Events"]
        west_events = west.describe_events(SourceIdentifier=cid)["Events"]
        assert [e["Message"] for e in east_events] == [
            "Cache cluster created",
            "Cache cluster deleted",
        ]
        assert [e["Message"] for e in west_events] == ["Cache cluster created"]
    finally:
        for client in (east, west):
            try:
                client.delete_cache_cluster(CacheClusterId=cid)
            except ClientError:
                pass


def test_elasticache_restore_legacy_account_scoped_state_adopts_record_arn_region():
    from ministack.core.responses import (
        AccountScopedDict,
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import elasticache as _ec

    original_account = get_account_id()
    original_region = get_region()
    account_id = "000000000000"
    region = "us-west-2"
    name = f"legacy-{_uid()}"

    legacy_clusters = AccountScopedDict()
    legacy_clusters._data[(account_id, name)] = {
        "CacheClusterId": name,
        "CacheClusterArn": f"arn:aws:elasticache:{region}:{account_id}:cluster:{name}",
        "CacheClusterStatus": "available",
        "Engine": "redis",
        "EngineVersion": "7.1",
        "CacheNodes": [],
    }
    legacy_rgs = AccountScopedDict()
    legacy_rgs._data[(account_id, name)] = {
        "ReplicationGroupId": name,
        "ARN": f"arn:aws:elasticache:{region}:{account_id}:replicationgroup:{name}",
        "Status": "available",
        "Engine": "redis",
        "NodeGroups": [],
    }
    legacy_subnets = AccountScopedDict()
    legacy_subnets._data[(account_id, name)] = {
        "CacheSubnetGroupName": name,
        "ARN": f"arn:aws:elasticache:{region}:{account_id}:subnetgroup:{name}",
    }
    legacy_params = AccountScopedDict()
    legacy_params._data[(account_id, name)] = {
        "CacheParameterGroupName": name,
        "CacheParameterGroupFamily": "redis7",
        "ARN": f"arn:aws:elasticache:{region}:{account_id}:parametergroup:{name}",
    }
    legacy_param_values = AccountScopedDict()
    legacy_param_values._data[(account_id, name)] = {
        "maxmemory-policy": {"Value": "allkeys-lru"},
    }
    legacy_snapshots = AccountScopedDict()
    legacy_snapshots._data[(account_id, name)] = {
        "SnapshotName": name,
        "SnapshotStatus": "available",
        "ARN": f"arn:aws:elasticache:{region}:{account_id}:snapshot:{name}",
    }
    legacy_users = AccountScopedDict()
    legacy_users._data[(account_id, name)] = {
        "UserId": name,
        "ARN": f"arn:aws:elasticache:{region}:{account_id}:user:{name}",
    }
    legacy_groups = AccountScopedDict()
    legacy_groups._data[(account_id, name)] = {
        "UserGroupId": name,
        "ARN": f"arn:aws:elasticache:{region}:{account_id}:usergroup:{name}",
    }

    _ec.reset()
    try:
        set_request_account_id(account_id)
        set_request_region("us-east-1")
        _ec.restore_state({
            "clusters": legacy_clusters,
            "replication_groups": legacy_rgs,
            "subnet_groups": legacy_subnets,
            "param_groups": legacy_params,
            "param_group_params": legacy_param_values,
            "snapshots": legacy_snapshots,
            "users": legacy_users,
            "user_groups": legacy_groups,
        })

        assert _ec._clusters.get_scoped(account_id, region, name)["CacheClusterArn"].endswith(f":cluster:{name}")
        assert _ec._clusters.get_scoped(account_id, "us-east-1", name) is None
        assert _ec._replication_groups.get_scoped(account_id, region, name)["ARN"].endswith(f":replicationgroup:{name}")
        assert _ec._subnet_groups.get_scoped(account_id, region, name)["ARN"].endswith(f":subnetgroup:{name}")
        assert _ec._param_groups.get_scoped(account_id, region, name)["ARN"].endswith(f":parametergroup:{name}")
        assert _ec._param_group_params.get_scoped(account_id, region, name)["maxmemory-policy"]["Value"] == "allkeys-lru"
        assert _ec._param_group_params.get_scoped(account_id, "us-east-1", name) is None
        assert _ec._snapshots.get_scoped(account_id, region, name)["ARN"].endswith(f":snapshot:{name}")
        assert _ec._users.get_scoped(account_id, region, name)["ARN"].endswith(f":user:{name}")
        assert _ec._user_groups.get_scoped(account_id, region, name)["ARN"].endswith(f":usergroup:{name}")
    finally:
        _ec.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


# ---------------------------------------------------------------------------
# 1. ModifyCacheCluster
# ---------------------------------------------------------------------------

@requires_docker
def test_modify_cache_cluster_num_nodes(ec):
    """ModifyCacheCluster: scale NumCacheNodes up and down."""
    cid = f"mod-cc-{_uid()}"
    ec.create_cache_cluster(
        CacheClusterId=cid,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    # scale up
    resp = ec.modify_cache_cluster(CacheClusterId=cid, NumCacheNodes=3)
    cluster = resp["CacheCluster"]
    assert cluster["NumCacheNodes"] == 3
    assert len(cluster["CacheNodes"]) == 3

    # scale down
    resp = ec.modify_cache_cluster(CacheClusterId=cid, NumCacheNodes=2)
    cluster = resp["CacheCluster"]
    assert cluster["NumCacheNodes"] == 2
    assert len(cluster["CacheNodes"]) == 2

    ec.delete_cache_cluster(CacheClusterId=cid)


@requires_docker
def test_modify_cache_cluster_node_type_and_engine(ec):
    """ModifyCacheCluster: update CacheNodeType and EngineVersion."""
    cid = f"mod-nt-{_uid()}"
    ec.create_cache_cluster(
        CacheClusterId=cid,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.modify_cache_cluster(
        CacheClusterId=cid,
        CacheNodeType="cache.m5.large",
        EngineVersion="7.1.0",
    )
    cluster = resp["CacheCluster"]
    assert cluster["CacheNodeType"] == "cache.m5.large"
    assert cluster["EngineVersion"] == "7.1.0"

    ec.delete_cache_cluster(CacheClusterId=cid)


# ---------------------------------------------------------------------------
# 2. RebootCacheCluster
# ---------------------------------------------------------------------------

@requires_docker
def test_reboot_cache_cluster(ec):
    """RebootCacheCluster: reboot and verify cluster stays available."""
    cid = f"reboot-{_uid()}"
    ec.create_cache_cluster(
        CacheClusterId=cid,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.reboot_cache_cluster(
        CacheClusterId=cid,
        CacheNodeIdsToReboot=["0001"],
    )
    cluster = resp["CacheCluster"]
    assert cluster["CacheClusterId"] == cid
    assert cluster["CacheClusterStatus"] == "available"

    ec.delete_cache_cluster(CacheClusterId=cid)


# ---------------------------------------------------------------------------
# 3. DeleteReplicationGroup
# ---------------------------------------------------------------------------

@requires_docker
def test_delete_replication_group(ec):
    """DeleteReplicationGroup: create then delete, verify gone."""
    rg_id = f"del-rg-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="To be deleted",
        CacheNodeType="cache.t3.micro",
    )
    # verify exists
    resp = ec.describe_replication_groups(ReplicationGroupId=rg_id)
    assert len(resp["ReplicationGroups"]) == 1

    # delete
    ec.delete_replication_group(ReplicationGroupId=rg_id)

    # verify gone
    with pytest.raises(ClientError) as exc:
        ec.describe_replication_groups(ReplicationGroupId=rg_id)
    assert "ReplicationGroupNotFoundFault" in str(exc.value)


# ---------------------------------------------------------------------------
# 4. ModifyReplicationGroup
# ---------------------------------------------------------------------------

@requires_docker
def test_modify_replication_group(ec):
    """ModifyReplicationGroup: update description and CacheNodeType."""
    rg_id = f"mod-rg-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Original desc",
        CacheNodeType="cache.t3.micro",
    )
    resp = ec.modify_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Updated desc",
        CacheNodeType="cache.m5.large",
    )
    rg = resp["ReplicationGroup"]
    assert rg["Description"] == "Updated desc"
    assert rg["CacheNodeType"] == "cache.m5.large"

    ec.delete_replication_group(ReplicationGroupId=rg_id)


def test_create_replication_group_missing_user_group_fails(ec):
    """CreateReplicationGroup returns UserGroupNotFound for unknown UserGroupIds."""
    rg_id = f"rg-missing-ug-{_uid()}"
    with pytest.raises(ClientError) as exc:
        ec.create_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="Missing user group",
            CacheNodeType="cache.t3.micro",
            UserGroupIds=["missing-user-group"],
        )
    err = exc.value.response["Error"]
    assert err["Code"] == "UserGroupNotFound"
    assert err["Message"] == "The user group was not found or does not exist"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    with pytest.raises(ClientError) as exc:
        ec.describe_replication_groups(ReplicationGroupId=rg_id)
    assert exc.value.response["Error"]["Code"] == "ReplicationGroupNotFoundFault"


def test_modify_replication_group_missing_user_group_fails_before_mutation(ec):
    """ModifyReplicationGroup validates user groups before applying other changes."""
    rg_id = f"mod-rg-missing-ug-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Original desc",
        CacheNodeType="cache.t3.micro",
    )

    with pytest.raises(ClientError) as exc:
        ec.modify_replication_group(
            ReplicationGroupId=rg_id,
            ReplicationGroupDescription="Updated desc",
            UserGroupIdsToAdd=["missing-user-group"],
        )
    err = exc.value.response["Error"]
    assert err["Code"] == "UserGroupNotFound"
    assert err["Message"] == "The user group was not found or does not exist"
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    rg = ec.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]
    assert rg["Description"] == "Original desc"
    ec.delete_replication_group(ReplicationGroupId=rg_id)


# ---------------------------------------------------------------------------
# 5. IncreaseReplicaCount
# ---------------------------------------------------------------------------

@requires_docker
def test_increase_replica_count(ec):
    """IncreaseReplicaCount: scale replicas up from 1 to 3."""
    rg_id = f"inc-rep-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Scale up test",
        CacheNodeType="cache.t3.micro",
        NumNodeGroups=1,
        ReplicasPerNodeGroup=1,
    )
    # verify initial: 1 primary + 1 replica = 2 members
    desc = ec.describe_replication_groups(ReplicationGroupId=rg_id)
    initial_members = len(desc["ReplicationGroups"][0]["NodeGroups"][0]["NodeGroupMembers"])
    assert initial_members == 2

    resp = ec.increase_replica_count(
        ReplicationGroupId=rg_id,
        NewReplicaCount=3,
        ApplyImmediately=True,
    )
    rg = resp["ReplicationGroup"]
    # 1 primary + 3 replicas = 4 members
    assert len(rg["NodeGroups"][0]["NodeGroupMembers"]) == 4

    ec.delete_replication_group(ReplicationGroupId=rg_id)


# ---------------------------------------------------------------------------
# 6. DecreaseReplicaCount
# ---------------------------------------------------------------------------

@requires_docker
def test_decrease_replica_count(ec):
    """DecreaseReplicaCount: scale replicas down from 3 to 1."""
    rg_id = f"dec-rep-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Scale down test",
        CacheNodeType="cache.t3.micro",
        NumNodeGroups=1,
        ReplicasPerNodeGroup=3,
    )
    # verify initial: 1 primary + 3 replicas = 4 members
    desc = ec.describe_replication_groups(ReplicationGroupId=rg_id)
    assert len(desc["ReplicationGroups"][0]["NodeGroups"][0]["NodeGroupMembers"]) == 4

    resp = ec.decrease_replica_count(
        ReplicationGroupId=rg_id,
        NewReplicaCount=1,
        ApplyImmediately=True,
    )
    rg = resp["ReplicationGroup"]
    # 1 primary + 1 replica = 2 members
    assert len(rg["NodeGroups"][0]["NodeGroupMembers"]) == 2

    ec.delete_replication_group(ReplicationGroupId=rg_id)


# ---------------------------------------------------------------------------
# 7. DeleteCacheSubnetGroup
# ---------------------------------------------------------------------------

@requires_docker
def test_delete_cache_subnet_group(ec):
    """DeleteCacheSubnetGroup: create then delete, verify gone."""
    name = f"del-sg-{_uid()}"
    ec.create_cache_subnet_group(
        CacheSubnetGroupName=name,
        CacheSubnetGroupDescription="To be deleted",
        SubnetIds=["subnet-aaa"],
    )
    # verify exists
    resp = ec.describe_cache_subnet_groups(CacheSubnetGroupName=name)
    assert len(resp["CacheSubnetGroups"]) == 1

    # delete
    ec.delete_cache_subnet_group(CacheSubnetGroupName=name)

    # verify gone
    with pytest.raises(ClientError) as exc:
        ec.describe_cache_subnet_groups(CacheSubnetGroupName=name)
    assert "CacheSubnetGroupNotFoundFault" in str(exc.value)


# ---------------------------------------------------------------------------
# 8. ResetCacheParameterGroup
# ---------------------------------------------------------------------------

@requires_docker
def test_reset_cache_parameter_group_full(ec):
    """ResetCacheParameterGroup: full reset restores defaults."""
    pg = f"reset-full-{_uid()}"
    ec.create_cache_parameter_group(
        CacheParameterGroupName=pg,
        CacheParameterGroupFamily="redis7.0",
        Description="Full reset test",
    )
    # modify a parameter away from default
    ec.modify_cache_parameter_group(
        CacheParameterGroupName=pg,
        ParameterNameValues=[{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"}],
    )
    params = ec.describe_cache_parameters(CacheParameterGroupName=pg)["Parameters"]
    maxmem = next(p for p in params if p["ParameterName"] == "maxmemory-policy")
    assert maxmem["ParameterValue"] == "allkeys-lru"

    # full reset
    ec.reset_cache_parameter_group(
        CacheParameterGroupName=pg,
        ResetAllParameters=True,
    )
    params = ec.describe_cache_parameters(CacheParameterGroupName=pg)["Parameters"]
    maxmem = next(p for p in params if p["ParameterName"] == "maxmemory-policy")
    assert maxmem["ParameterValue"] == "volatile-lru"

    ec.delete_cache_parameter_group(CacheParameterGroupName=pg)


@requires_docker
def test_reset_cache_parameter_group_selective(ec):
    """ResetCacheParameterGroup: selective reset of specific parameter."""
    pg = f"reset-sel-{_uid()}"
    ec.create_cache_parameter_group(
        CacheParameterGroupName=pg,
        CacheParameterGroupFamily="redis7.0",
        Description="Selective reset test",
    )
    # modify two parameters
    ec.modify_cache_parameter_group(
        CacheParameterGroupName=pg,
        ParameterNameValues=[
            {"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"},
            {"ParameterName": "timeout", "ParameterValue": "300"},
        ],
    )
    # selective reset only maxmemory-policy
    ec.reset_cache_parameter_group(
        CacheParameterGroupName=pg,
        ResetAllParameters=False,
        ParameterNameValues=[{"ParameterName": "maxmemory-policy", "ParameterValue": ""}],
    )
    params = ec.describe_cache_parameters(CacheParameterGroupName=pg)["Parameters"]
    maxmem = next(p for p in params if p["ParameterName"] == "maxmemory-policy")
    timeout_p = next(p for p in params if p["ParameterName"] == "timeout")
    # maxmemory-policy should be back to default
    assert maxmem["ParameterValue"] == "volatile-lru"
    # timeout should still have the modified value
    assert timeout_p["ParameterValue"] == "300"

    ec.delete_cache_parameter_group(CacheParameterGroupName=pg)


# ---------------------------------------------------------------------------
# 9. DeleteSnapshot (explicit)
# ---------------------------------------------------------------------------

@requires_docker
def test_delete_snapshot_explicit(ec):
    """DeleteSnapshot: create snapshot, delete it, verify gone."""
    cid = f"snap-del-{_uid()}"
    snap_name = f"snap-{_uid()}"
    ec.create_cache_cluster(
        CacheClusterId=cid,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    ec.create_snapshot(SnapshotName=snap_name, CacheClusterId=cid)

    # verify exists
    resp = ec.describe_snapshots(SnapshotName=snap_name)
    assert len(resp["Snapshots"]) == 1

    # delete
    del_resp = ec.delete_snapshot(SnapshotName=snap_name)
    assert del_resp["Snapshot"]["SnapshotStatus"] == "deleting"

    # verify gone
    resp = ec.describe_snapshots(SnapshotName=snap_name)
    assert len(resp["Snapshots"]) == 0

    ec.delete_cache_cluster(CacheClusterId=cid)


# ---------------------------------------------------------------------------
# 10. DescribeEvents
# ---------------------------------------------------------------------------

@requires_docker
def test_describe_events_all(ec):
    """DescribeEvents: listing all events returns results."""
    # create a cluster to generate at least one event
    cid = f"evt-all-{_uid()}"
    ec.create_cache_cluster(
        CacheClusterId=cid,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.describe_events()
    assert "Events" in resp
    assert len(resp["Events"]) > 0

    ec.delete_cache_cluster(CacheClusterId=cid)


@requires_docker
def test_describe_events_filter_source_type(ec):
    """DescribeEvents: filter by SourceType."""
    rg_id = f"evt-rg-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Event filter test",
        CacheNodeType="cache.t3.micro",
    )
    resp = ec.describe_events(SourceType="replication-group")
    assert "Events" in resp
    # all returned events should be replication-group type
    for evt in resp["Events"]:
        assert evt["SourceType"] == "replication-group"

    ec.delete_replication_group(ReplicationGroupId=rg_id)


@requires_docker
def test_describe_events_filter_source_id(ec):
    """DescribeEvents: filter by SourceIdentifier."""
    cid = f"evt-src-{_uid()}"
    ec.create_cache_cluster(
        CacheClusterId=cid,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    resp = ec.describe_events(SourceIdentifier=cid)
    assert "Events" in resp
    for evt in resp["Events"]:
        assert evt["SourceIdentifier"] == cid

    ec.delete_cache_cluster(CacheClusterId=cid)


# ---------------------------------------------------------------------------
# 11. Replication group member cluster lifecycle
# ---------------------------------------------------------------------------

def test_replication_group_creates_member_clusters(ec):
    """CreateReplicationGroup should register member clusters visible via DescribeCacheClusters."""
    rg_id = f"rg-members-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Member cluster test",
        CacheNodeType="cache.t3.micro",
        NumCacheClusters=2,
    )
    rg = ec.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]
    member_ids = rg["MemberClusters"]
    assert len(member_ids) == 2

    for cid in member_ids:
        resp = ec.describe_cache_clusters(CacheClusterId=cid)
        cluster = resp["CacheClusters"][0]
        assert cluster["CacheClusterId"] == cid
        assert cluster["ReplicationGroupId"] == rg_id

    ec.delete_replication_group(ReplicationGroupId=rg_id)


def test_delete_replication_group_removes_member_clusters(ec):
    """DeleteReplicationGroup should also remove its member clusters."""
    rg_id = f"rg-del-mem-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Delete members test",
        CacheNodeType="cache.t3.micro",
        NumCacheClusters=2,
    )
    member_ids = ec.describe_replication_groups(
        ReplicationGroupId=rg_id
    )["ReplicationGroups"][0]["MemberClusters"]

    ec.delete_replication_group(ReplicationGroupId=rg_id)

    for cid in member_ids:
        with pytest.raises(ClientError) as exc:
            ec.describe_cache_clusters(CacheClusterId=cid)
        assert "CacheClusterNotFound" in str(exc.value)


def test_replication_group_tags_on_create(ec):
    """Tags passed at CreateReplicationGroup should be retrievable via ListTagsForResource."""
    rg_id = f"rg-tags-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Tag on create",
        CacheNodeType="cache.t3.micro",
        Tags=[{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "infra"}],
    )
    arn = ec.describe_replication_groups(
        ReplicationGroupId=rg_id
    )["ReplicationGroups"][0]["ARN"]

    tags = ec.list_tags_for_resource(ResourceName=arn)
    tag_map = {t["Key"]: t["Value"] for t in tags["TagList"]}
    assert tag_map["env"] == "test"
    assert tag_map["team"] == "infra"

    ec.delete_replication_group(ReplicationGroupId=rg_id)


def test_replication_group_cluster_mode_uses_cluster_on_default_group(ec):
    """Cluster-mode-enabled replication groups use the .cluster.on default group."""
    rg_id = f"rg-cluster-pg-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Cluster default parameter group",
        CacheNodeType="cache.t3.micro",
        NumNodeGroups=3,
        ReplicasPerNodeGroup=0,
    )
    rg = ec.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]
    member_id = rg["MemberClusters"][0]
    cluster = ec.describe_cache_clusters(CacheClusterId=member_id)["CacheClusters"][0]
    assert cluster["CacheParameterGroup"]["CacheParameterGroupName"] == "default.redis7.cluster.on"
    ec.delete_replication_group(ReplicationGroupId=rg_id)


def test_replication_group_tag_updates_propagate_to_member_clusters(ec):
    """AddTagsToResource/RemoveTagsFromResource on a replication group fan out to members."""
    rg_id = f"rg-tag-fanout-{_uid()}"
    ec.create_replication_group(
        ReplicationGroupId=rg_id,
        ReplicationGroupDescription="Tag fanout",
        CacheNodeType="cache.t3.micro",
        NumCacheClusters=2,
    )
    rg = ec.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]
    rg_arn = rg["ARN"]
    member_id = rg["MemberClusters"][0]
    member = ec.describe_cache_clusters(CacheClusterId=member_id)["CacheClusters"][0]
    member_arn = member["ARN"]

    ec.add_tags_to_resource(ResourceName=rg_arn, Tags=[{"Key": "env", "Value": "test"}])
    member_tags = ec.list_tags_for_resource(ResourceName=member_arn)["TagList"]
    assert {t["Key"]: t["Value"] for t in member_tags}["env"] == "test"

    ec.remove_tags_from_resource(ResourceName=rg_arn, TagKeys=["env"])
    member_tags = ec.list_tags_for_resource(ResourceName=member_arn)["TagList"]
    assert not any(t["Key"] == "env" for t in member_tags)
    ec.delete_replication_group(ReplicationGroupId=rg_id)


# ---------------------------------------------------------------------------
# 12. Serverless cache operations — not implemented in MiniStack
# ---------------------------------------------------------------------------

@requires_docker
def test_serverless_cache_not_implemented(ec):
    """Serverless cache operations are not yet implemented; verify graceful error."""
    with pytest.raises(ClientError):
        ec.create_serverless_cache(
            ServerlessCacheName="test-serverless",
            Engine="redis",
        )



# ========== from test_elasticache_lambda_network.py ==========
# ElastiCache+Lambda network reachability via DOCKER_NETWORK auto-detect.

_LAMBDA_ROLE = "arn:aws:iam::000000000000:role/lambda-role"


def _make_zip(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.py", code)
    return buf.getvalue()


def _make_zip_js(code: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("index.js", code)
    return buf.getvalue()


@requires_docker
def test_elasticache_lambda_network_connectivity(ec, lam):
    """Prove that Lambda containers can TCP-connect to an ElastiCache container."""
    cluster_id = "net-test-redis"
    fn_py = "ec-net-test-py"
    fn_js = "ec-net-test-js"

    # 1. Create ElastiCache Redis cluster
    ec.create_cache_cluster(
        CacheClusterId=cluster_id,
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )

    try:
        resp = ec.describe_cache_clusters(CacheClusterId=cluster_id)
        cluster = resp["CacheClusters"][0]
        node = cluster["CacheNodes"][0]
        host = node["Endpoint"]["Address"]
        port = int(node["Endpoint"]["Port"])

        # 2. Endpoint.Address must NOT be localhost when DOCKER_NETWORK is set
        assert host not in ("localhost", "redis"), (
            f"Expected container IP, got '{host}' — DOCKER_NETWORK not working"
        )

        # 3. Wait for Redis container to accept connections
        import socket
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                with socket.create_connection((host, port), timeout=2):
                    break
            except OSError:
                time.sleep(1)
        else:
            pytest.fail(f"ElastiCache container at {host}:{port} not reachable after 60s")

        # 4. Python Lambda — TCP connect to ElastiCache endpoint
        py_code = f"""\
import socket, json
def handler(event, context):
    try:
        s = socket.create_connection(("{host}", {port}), timeout=5)
        s.close()
        return {{"connected": True}}
    except Exception as e:
        return {{"connected": False, "error": str(e)}}
"""
        lam.create_function(
            FunctionName=fn_py,
            Runtime="python3.12",
            Role=_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _make_zip(py_code)},
            Timeout=15,
        )

        resp = lam.invoke(FunctionName=fn_py, Payload=json.dumps({}))
        result = json.loads(resp["Payload"].read())
        assert result.get("connected") is True, f"Python Lambda failed: {result}"

        # 5. JS Lambda — TCP connect to ElastiCache endpoint
        js_code = f"""\
const net = require("net");
exports.handler = async (event) => {{
    return new Promise((resolve) => {{
        const sock = new net.Socket();
        sock.setTimeout(5000);
        sock.connect({port}, "{host}", () => {{
            sock.destroy();
            resolve({{ connected: true }});
        }});
        sock.on("error", (err) => {{
            sock.destroy();
            resolve({{ connected: false, error: err.message }});
        }});
        sock.on("timeout", () => {{
            sock.destroy();
            resolve({{ connected: false, error: "timeout" }});
        }});
    }});
}};
"""
        lam.create_function(
            FunctionName=fn_js,
            Runtime="nodejs20.x",
            Role=_LAMBDA_ROLE,
            Handler="index.handler",
            Code={"ZipFile": _make_zip_js(js_code)},
            Timeout=15,
        )

        resp = lam.invoke(FunctionName=fn_js, Payload=json.dumps({}))
        result = json.loads(resp["Payload"].read())
        assert result.get("connected") is True, f"JS Lambda failed: {result}"

    finally:
        # 6. Cleanup
        for fn in (fn_py, fn_js):
            try:
                lam.delete_function(FunctionName=fn)
            except Exception:
                pass
        try:
            ec.delete_cache_cluster(CacheClusterId=cluster_id)
        except Exception:
            pass




# ── Container respawn on restore (#853) — unit-style, no Docker ──────


def test_elasticache_restore_state_marks_clusters_for_respawn(monkeypatch):
    """Issue #853: restoring persisted cluster state must re-spawn the
    Redis Docker container — the persisted container id is dead after
    restart. Verifies the lazy-respawn path is triggered."""
    from ministack.services import elasticache as _ec
    _ec.reset()

    spawned = []

    def fake_spawn(name, engine, engine_version, labels):
        spawned.append({"name": name, "engine": engine,
                         "engine_version": engine_version, "labels": labels})
        return ("ministack-elasticache", 6379, f"cid-{name}")

    monkeypatch.setattr(_ec, "_spawn_redis_container", fake_spawn)

    _ec.restore_state({
        "clusters": {
            "clustertest": {
                "CacheClusterId": "clustertest",
                "Engine": "redis", "EngineVersion": "7.1",
                "CacheNodes": [{"CacheNodeId": "0001",
                                "Endpoint": {"Address": "stale", "Port": 1}}],
                "_docker_container_id": "old-dead-cid",
                "CacheClusterStatus": "available",
            },
        },
    })

    assert spawned == []  # respawn deferred — _spawn_redis_container not yet bound at import time
    assert ("000000000000", "us-east-1", "clustertest") in _ec._pending_cluster_respawn

    _ec._ensure_live_containers()

    assert any(s["name"] == "ministack-elasticache-000000000000-us-east-1-clustertest" for s in spawned)
    cl = _ec._clusters["clustertest"]
    assert cl["_docker_container_id"] == "cid-ministack-elasticache-000000000000-us-east-1-clustertest"
    assert cl["CacheNodes"][0]["Endpoint"]["Address"] == "ministack-elasticache"
    assert cl["CacheNodes"][0]["Endpoint"]["Port"] == 6379
    assert ("000000000000", "us-east-1", "clustertest") not in _ec._pending_cluster_respawn

    spawned.clear()
    _ec._ensure_live_containers()
    assert spawned == []  # idempotent — no second respawn


def test_elasticache_restore_state_wipes_stale_replication_group_container_ids(monkeypatch):
    from ministack.core.responses import set_request_account_id
    from ministack.services import elasticache as _ec
    set_request_account_id("000000000000")
    _ec.reset()
    monkeypatch.setattr(_ec, "_spawn_redis_container",
                          lambda name, engine, engine_version, labels: ("rg-host", 6379, f"cid-{name}"))

    _ec.restore_state({
        "replication_groups": {
            "rg-1": {
                "ReplicationGroupId": "rg-1",
                "Engine": "redis", "EngineVersion": "7.1",
                "NodeGroups": [{"NodeGroupId": "0001"}],
                "_docker_container_ids": ["dead-cid-1", "dead-cid-2"],
            },
        },
    })
    assert _ec._replication_groups["rg-1"]["_docker_container_ids"] == []
    assert ("000000000000", "us-east-1", "rg-1") in _ec._pending_rg_respawn

    _ec._ensure_live_containers()
    cids = _ec._replication_groups["rg-1"]["_docker_container_ids"]
    assert cids == ["cid-ministack-elasticache-rg-000000000000-us-east-1-rg-1-0001"]
    assert ("000000000000", "us-east-1", "rg-1") not in _ec._pending_rg_respawn


def test_elasticache_restore_state_respawns_same_names_per_region(monkeypatch):
    from ministack.core.responses import AccountRegionScopedDict
    from ministack.services import elasticache as _ec

    _ec.reset()
    spawned = []

    def fake_spawn(name, engine, engine_version, labels):
        spawned.append({"name": name, "labels": labels})
        return (f"{labels['region']}-host", 6379, f"cid-{name}")

    monkeypatch.setattr(_ec, "_spawn_redis_container", fake_spawn)

    clusters = AccountRegionScopedDict()
    replication_groups = AccountRegionScopedDict()
    account_id = "000000000000"
    for region in ("us-east-1", "us-west-2"):
        clusters.set_scoped(account_id, region, "same-cache", {
            "CacheClusterId": "same-cache",
            "CacheClusterStatus": "available",
            "Engine": "redis",
            "EngineVersion": "7.1",
            "CacheNodes": [{"CacheNodeId": "0001", "Endpoint": {"Address": "stale", "Port": 1}}],
            "_docker_container_id": f"dead-{region}",
        })
        replication_groups.set_scoped(account_id, region, "same-rg", {
            "ReplicationGroupId": "same-rg",
            "Engine": "redis",
            "EngineVersion": "7.1",
            "NodeGroups": [{"NodeGroupId": "0001"}],
            "_docker_container_ids": [f"dead-rg-{region}"],
        })

    _ec.restore_state({
        "clusters": clusters,
        "replication_groups": replication_groups,
    })

    assert _ec._pending_cluster_respawn == {
        (account_id, "us-east-1", "same-cache"),
        (account_id, "us-west-2", "same-cache"),
    }
    assert _ec._pending_rg_respawn == {
        (account_id, "us-east-1", "same-rg"),
        (account_id, "us-west-2", "same-rg"),
    }

    _ec._ensure_live_containers()

    names = {entry["name"] for entry in spawned}
    assert names == {
        "ministack-elasticache-000000000000-us-east-1-same-cache",
        "ministack-elasticache-000000000000-us-west-2-same-cache",
        "ministack-elasticache-rg-000000000000-us-east-1-same-rg-0001",
        "ministack-elasticache-rg-000000000000-us-west-2-same-rg-0001",
    }
    assert {entry["labels"]["region"] for entry in spawned} == {"us-east-1", "us-west-2"}
    assert _ec._clusters.get_scoped(account_id, "us-east-1", "same-cache")["_docker_container_id"] == (
        "cid-ministack-elasticache-000000000000-us-east-1-same-cache"
    )
    assert _ec._clusters.get_scoped(account_id, "us-west-2", "same-cache")["_docker_container_id"] == (
        "cid-ministack-elasticache-000000000000-us-west-2-same-cache"
    )
    assert _ec._replication_groups.get_scoped(account_id, "us-east-1", "same-rg")["_docker_container_ids"] == [
        "cid-ministack-elasticache-rg-000000000000-us-east-1-same-rg-0001",
    ]
    assert _ec._replication_groups.get_scoped(account_id, "us-west-2", "same-rg")["_docker_container_ids"] == [
        "cid-ministack-elasticache-rg-000000000000-us-west-2-same-rg-0001",
    ]
    assert _ec._pending_cluster_respawn == set()
    assert _ec._pending_rg_respawn == set()


def test_elasticache_respawn_failure_is_logged_and_does_not_block_requests(monkeypatch, caplog):
    import logging

    from ministack.services import elasticache as _ec

    _ec.reset()

    def boom(*a, **kw):
        raise RuntimeError("docker daemon unreachable")

    monkeypatch.setattr(_ec, "_spawn_redis_container", boom)

    _ec.restore_state({
        "clusters": {
            "c1": {
                "CacheClusterId": "c1", "Engine": "redis", "EngineVersion": "7.1",
                "CacheNodes": [], "_docker_container_id": "old",
                "CacheClusterStatus": "available",
            },
        },
    })
    with caplog.at_level(logging.WARNING):
        _ec._ensure_live_containers()
    assert ("000000000000", "us-east-1", "c1") not in _ec._pending_cluster_respawn  # cleared even on failure (no retry storm)
    assert any("failed to respawn" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Valkey engine unit tests (engine -> image/port mapping, no-Docker fallback).
# Folded from test_elasticache_valkey.py; no running server or Docker needed.
# ---------------------------------------------------------------------------


def _img(name):
    return elasticache.apply_image_prefix(name)


def test_valkey_image_and_port():
    assert _engine_image_and_port("valkey", "8.0") == (_img("valkey/valkey:8.0-alpine"), 6379)
    assert _engine_image_and_port("valkey", "7.2") == (_img("valkey/valkey:7.2-alpine"), 6379)
    assert _engine_image_and_port("valkey", "8.1") == (_img("valkey/valkey:8.1-alpine"), 6379)


def test_valkey_image_tag_truncates_patch_version():
    assert _engine_image_and_port("valkey", "7.2.6") == (_img("valkey/valkey:7.2-alpine"), 6379)


def test_valkey_image_tag_defaults():
    assert _engine_image_and_port("valkey", "8") == (_img("valkey/valkey:8-alpine"), 6379)
    assert _engine_image_and_port("valkey", "") == (_img("valkey/valkey:8.0-alpine"), 6379)


def test_redis_and_memcached_images_unchanged():
    assert _engine_image_and_port("redis", "7.1.0") == (_img("redis:7-alpine"), 6379)
    assert _engine_image_and_port("memcached", "1.6.17") == (_img("memcached:1.6.17-alpine"), 11211)


def test_valkey_no_docker_fallback_uses_redis_port(monkeypatch):
    """Valkey previously fell into the memcached branch: nonexistent
    memcached:<ver>-alpine image, then a fallback advertising port 11211."""
    monkeypatch.setattr(elasticache, "_get_docker", lambda: None)
    host, port, cid = elasticache._spawn_redis_container(
        "ms-valkey-test", "valkey", "8.0", {"ministack": "elasticache"}
    )
    assert (host, port) == (elasticache.REDIS_DEFAULT_HOST, elasticache.REDIS_DEFAULT_PORT)
    assert cid is None
