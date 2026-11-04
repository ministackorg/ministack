import io
import json
import os
import re
import time
import types
import uuid as _uuid_mod
import zipfile

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import make_client

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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
def test_elasticache_replication_group(ec):
    ec.create_replication_group(
        ReplicationGroupId="test-rg",
        ReplicationGroupDescription="Test replication group",
        CacheNodeType="cache.t3.micro",
    )
    resp = ec.describe_replication_groups(ReplicationGroupId="test-rg")
    assert resp["ReplicationGroups"][0]["ReplicationGroupId"] == "test-rg"

def test_elasticache_engines(ec):
    resp = ec.describe_cache_engine_versions(Engine="redis")
    assert len(resp["CacheEngineVersions"]) > 0

def test_elasticache_valkey_engine_versions(ec):
    resp = ec.describe_cache_engine_versions(Engine="valkey")
    versions = resp["CacheEngineVersions"]
    assert len(versions) > 0
    assert all(v["Engine"] == "valkey" for v in versions)
    assert all(v["CacheParameterGroupFamily"].startswith("valkey") for v in versions)

@requires_docker
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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

def test_elasticache_modify_subnet_group(ec):
    ec.create_cache_subnet_group(
        CacheSubnetGroupName="test-mod-ecsg",
        CacheSubnetGroupDescription="Test EC SG",
        SubnetIds=["subnet-00000001"],
    )
    ec.modify_cache_subnet_group(
        CacheSubnetGroupName="test-mod-ecsg",
        CacheSubnetGroupDescription="Updated EC SG",
        SubnetIds=["subnet-00000002"],
    )
    resp = ec.describe_cache_subnet_groups(CacheSubnetGroupName="test-mod-ecsg")
    assert resp["CacheSubnetGroups"][0]["CacheSubnetGroupDescription"] == "Updated EC SG"

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

def test_elasticache_user_group_crud(ec):
    ec.create_user(
        UserId="ug-usr-1",
        UserName="default",
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

def test_elasticache_reset_clears_param_groups():
    """ElastiCache reset restores built-in parameter groups and resets port counter."""
    from ministack.services import elasticache as _ec
    _ec._param_group_params["test-group"] = {"param1": "val1"}
    _ec._port_counter[0] = 99999
    _ec.reset()
    assert "test-group" not in _ec._param_group_params
    assert "default.redis7" in _ec._param_group_params
    assert _ec._port_counter[0] == _ec.BASE_PORT

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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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

def test_elasticache_engine_versions_v2(ec):
    redis = ec.describe_cache_engine_versions(Engine="redis")
    assert len(redis["CacheEngineVersions"]) > 0
    assert all(v["Engine"] == "redis" for v in redis["CacheEngineVersions"])

    mc = ec.describe_cache_engine_versions(Engine="memcached")
    assert len(mc["CacheEngineVersions"]) > 0

@requires_docker
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
            UserName="default",
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


_DEFAULT_USER_REQUIRED = {
    "Type": "Sender", "Code": "DefaultUserRequired",
    "Message": "Redis user group needs to contain a user with the user name default.",
}


def _assert_default_user_required(exc):
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"] == _DEFAULT_USER_REQUIRED


@pytest.fixture
def ec_users(ec):
    """Users like the capture rounds': Redis IAM (three, and one created with Engine `Redis`), Redis `default` (two),
    Redis `Default` (two), Valkey IAM and Valkey `default`. Yields their ids by role and deletes them, with any user group they joined."""
    s = _uid()
    ids = {k: f"dug-{k}-{s}" for k in ("r-iam", "r-iam2", "r-iam3", "r-def", "r-def2", "r-case", "r-case2", "up-iam", "v-iam", "v-def")}
    names = {"r-def": "default", "r-def2": "default", "r-case": "Default", "r-case2": "Default", "v-def": "default"}
    groups, created = [], []
    try:
        for k, uid in ids.items():
            auth = ({"Type": "password", "Passwords": ["msc-capture-default-password-1"]} if k in names
                    else {"Type": "iam"})
            engine = {"r": "redis", "u": "Redis", "v": "valkey"}[k[0]]
            ec.create_user(UserId=uid, UserName=names.get(k, uid), Engine=engine,
                           AccessString="on ~* +@all", AuthenticationMode=auth)
            created.append(uid)
        yield ids, groups, s
    finally:
        for g in groups:
            try:
                ec.delete_user_group(UserGroupId=g)
            except ClientError:
                pass
        for uid in created:
            try:
                ec.delete_user(UserId=uid)
            except ClientError:
                pass


@pytest.mark.parametrize("key,engine,members", [
    ("cug-r-no-default", "redis", ["r-iam"]),
    ("cug-r-no-users", "redis", None),
    ("cug-r-empty-list", "redis", []),
    ("cug-r-case-default", "redis", ["r-case", "r-iam"]),
    ("cug-engine-Redis", "Redis", ["r-iam"]),
    ("cug-engine-REDIS", "REDIS", ["r-iam"]),
    ("cug-r-upper-engine-iam-no-default", "redis", ["up-iam"]),
])
def test_create_redis_user_group_requires_default_user(ec, ec_users, key, engine, members):
    """A Redis OSS user group needs a user named exactly `default`, whatever the case of `Engine`, and the
    refused id stays free (real AWS, us-east-2: cug-r-no-default, cug-r-no-users, cug-r-empty-list,
    cug-r-case-default, cug-engine-Redis, cug-engine-REDIS; a user created with Engine `Redis` counts
    as a Redis user, cug-r-upper-engine-iam-no-default; the id is reused in cug-r-retry1-no-users then
    cug-r-retry1-ok, and cug-r-retry2-iam-only then cug-r-retry2-ok)."""
    ids, groups, s = ec_users
    group_id = f"dug-{_uid()}-{s}"
    groups.append(group_id)
    params = {"UserGroupId": group_id, "Engine": engine}
    if members is not None:
        params["UserIds"] = [ids[m] for m in members]
    raw = make_client("elasticache", {"parameter_validation": False})  # the capture's empty UserIds
    with pytest.raises(ClientError) as exc:
        raw.create_user_group(**params)
    _assert_default_user_required(exc)
    ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-def"], ids["r-iam"]])


def test_create_user_group_already_exists_before_default_user(ec, ec_users):
    """An existing group id is refused before the default-user check
    (real AWS, us-east-2: cug-r-ok, cug-r-duplicate-no-default)."""
    ids, groups, s = ec_users
    group_id = f"dug-ug-r-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(group_id)
    with pytest.raises(ClientError) as exc:
        ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-iam"]])
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    assert exc.value.response["Error"] == {
        "Type": "Sender", "Code": "UserGroupAlreadyExists",
        "Message": f"User group {group_id} already exists. Please retry with another name.",
    }


@pytest.mark.parametrize("key,members", [
    ("cug-v-no-default", ["v-iam"]),
    ("cug-v-no-users", None),
    ("cug-v-redis-default", ["r-def", "v-iam"]),
])
def test_create_valkey_user_group_needs_no_default_user(ec, ec_users, key, members):
    """Valkey user groups are created without a Valkey user named `default`
    (real AWS, us-east-2: cug-v-no-default, cug-v-no-users, cug-v-redis-default)."""
    ids, groups, s = ec_users
    group_id = f"dug-{_uid()}-{s}"
    params = {"UserGroupId": group_id, "Engine": "valkey"}
    if members is not None:
        params["UserIds"] = [ids[m] for m in members]
    resp = ec.create_user_group(**params)
    groups.append(group_id)
    assert resp["UserGroupId"] == group_id
    assert resp["UserIds"] == params.get("UserIds", [])


@pytest.mark.parametrize("key,members,remove,add", [
    ("mug-r-remove-default", ["r-def", "r-iam"], ["r-def"], []),
    ("mug-r-remove-default-add-case", ["r-def", "r-iam"], ["r-def"], ["r-case"]),
    ("mug-r-remove-default-add-iam2", ["r-def", "r-iam"], ["r-def"], ["r-iam2"]),
    ("mug-r-remove-only-default", ["r-def"], ["r-def"], []),
    ("mug-r-remove-default-and-member", ["r-def", "r-iam", "r-iam2"], ["r-def", "r-iam"], []),
    ("mug-r-remove-all", ["r-def", "r-iam", "r-iam2"], ["r-def", "r-iam", "r-iam2"], []),
    ("mug-r-remove-default-add-two", ["r-def", "r-iam"], ["r-def"], ["r-iam2", "r-iam3"]),
])
def test_modify_redis_user_group_keeps_default_user(ec, ec_users, key, members, remove, add):
    """Removing a Redis group's `default` user is refused, also when it is the only user or another user
    is added, also with other users or all of them removed, and the group is left as it was (real AWS,
    us-east-2: mug-r-remove-default, mug-r-remove-default-add-case, then describe-ug-r-1 after mug-r-remove-iam
    took out only the IAM user; round 2's mug-r-remove-default-add-iam2, mug-r-remove-only-default and their
    describe-ug-r-1, describe-ug-r-only-1; round 3's mug-r-remove-default-and-member, mug-r-remove-all,
    describe-ug-r3; round 4's mug-r-remove-default-add-two, describe-ug-r4)."""
    ids, groups, s = ec_users
    group_id = f"dug-ug-r-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids[m] for m in members])
    groups.append(group_id)
    params = {"UserGroupId": group_id, "UserIdsToRemove": [ids[m] for m in remove]}
    if add:
        params["UserIdsToAdd"] = [ids[m] for m in add]
    with pytest.raises(ClientError) as exc:
        ec.modify_user_group(**params)
    _assert_default_user_required(exc)
    group = ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]
    assert group["UserIds"] == [ids[m] for m in members]


def test_modify_redis_user_group_with_default_user_left(ec, ec_users):
    """Removing another user, or swapping one `default` user for another, is accepted
    (real AWS, us-east-2: mug-r-remove-iam, describe-ug-r-1, mug-r-swap-default, describe-ug-r-2)."""
    ids, groups, s = ec_users
    group_id = f"dug-ug-r-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(group_id)
    ec.modify_user_group(UserGroupId=group_id, UserIdsToRemove=[ids["r-iam"]])
    assert ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]["UserIds"] == [ids["r-def"]]
    ec.modify_user_group(UserGroupId=group_id, UserIdsToAdd=[ids["r-def2"]], UserIdsToRemove=[ids["r-def"]])
    assert ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]["UserIds"] == [ids["r-def2"]]


def test_modify_upper_case_engine_user_group_keeps_default_user(ec, ec_users):
    """A group created with Engine `REDIS` is a Redis group for the rule too (real AWS, us-east-2:
    cug-REDIS-ok, mug-REDIS-remove-default, describe-ug-REDIS-after)."""
    ids, groups, s = ec_users
    group_id = f"dug-ug-upper-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine="REDIS", UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(group_id)
    with pytest.raises(ClientError) as exc:
        ec.modify_user_group(UserGroupId=group_id, UserIdsToRemove=[ids["r-def"]])
    _assert_default_user_required(exc)
    assert ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]["UserIds"] == [ids["r-def"], ids["r-iam"]]


def test_modify_attached_user_group_keeps_default_user(ec, ec_users):
    """A group attached to a replication group is refused the same way and stays attached and unchanged
    (real AWS, us-east-2, round6-replication-group.py: mug-attached-remove-default,
    describe-ug-attached-after)."""
    ids, groups, s = ec_users
    group_id, rg_id = f"dug-ug-rg-{s}", f"dug-rg-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(group_id)
    ec.create_replication_group(ReplicationGroupId=rg_id, ReplicationGroupDescription="ministack test",
                                Engine="redis", CacheNodeType="cache.t4g.micro", NumCacheClusters=1,
                                TransitEncryptionEnabled=True, UserGroupIds=[group_id])
    try:
        with pytest.raises(ClientError) as exc:
            ec.modify_user_group(UserGroupId=group_id, UserIdsToRemove=[ids["r-def"]])
        _assert_default_user_required(exc)
        group = ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]
        assert group["UserIds"] == [ids["r-def"], ids["r-iam"]]
        assert group["ReplicationGroups"] == [rg_id]
    finally:
        ec.delete_replication_group(ReplicationGroupId=rg_id)


def test_modify_serverless_attached_user_group_keeps_default_user(ec, ec_users):
    """A group attached to a serverless cache is refused the same way and stays unchanged (real AWS,
    us-east-2, round12-attached-cfn.py: mug-sl-remove-default, describe-after-mug-sl-remove-default)."""
    ids, groups, s = ec_users
    group_id, name = f"dug-{_uid()}-{s}", f"dug-sl-{_uid()}"
    ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(group_id)
    ec.create_serverless_cache(ServerlessCacheName=name, Engine="redis", MajorEngineVersion="7", UserGroupId=group_id)
    try:
        with pytest.raises(ClientError) as exc:
            ec.modify_user_group(UserGroupId=group_id, UserIdsToRemove=[ids["r-def"]])
        _assert_default_user_required(exc)
        assert ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]["UserIds"] == [ids["r-def"], ids["r-iam"]]
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)


def test_modify_valkey_user_group_removes_default_user(ec, ec_users):
    """A Valkey group's `default` user can be removed (real AWS, us-east-2: cug-v-ok, mug-v-remove-default,
    describe-ug-v)."""
    ids, groups, s = ec_users
    group_id = f"dug-ug-v-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine="valkey", UserIds=[ids["v-def"], ids["v-iam"]])
    groups.append(group_id)
    ec.modify_user_group(UserGroupId=group_id, UserIdsToRemove=[ids["v-def"]])
    assert ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]["UserIds"] == [ids["v-iam"]]


@pytest.mark.parametrize("key,engine,members,modify,code", [
    ("cug-r-missing-user", "redis", ["missing"], None, "UserNotFound"),
    ("cug-r-missing-user-with-default", "redis", ["r-def", "missing"], None, "UserNotFound"),
    ("cug-r-same-id-twice", "redis", ["r-iam", "r-iam"], None, "InvalidParameterValue"),
    ("cug-r-two-defaults", "redis", ["r-def", "r-def2"], None, None),
    ("cug-r-two-case-defaults", "redis", ["r-case", "r-case2"], None, None),
    ("cug-r-valkey-default", "redis", ["v-def", "r-iam"], None, None),
    ("cug-no-engine", None, ["r-iam"], None, None),
    ("mug-r-remove-default-and-missing", "redis", ["r-def", "r-iam"], {"UserIdsToRemove": ["r-def", "missing"]},
     "InvalidParameterValue"),
    ("mug-r-remove-default-add-missing", "redis", ["r-def", "r-iam"],
     {"UserIdsToAdd": ["missing"], "UserIdsToRemove": ["r-def"]}, "UserNotFound"),
    ("mug-r-remove-default-add-member", "redis", ["r-def", "r-iam"],
     {"UserIdsToAdd": ["r-iam"], "UserIdsToRemove": ["r-def"]}, "InvalidParameterCombination"),
    ("mug-r-remove-member-twice", "redis", ["r-def", "r-iam"], {"UserIdsToRemove": ["r-iam", "r-iam"]},
     "InvalidParameterValue"),
    ("mug-r-remove-default-twice", "redis", ["r-def", "r-iam"], {"UserIdsToRemove": ["r-def", "r-def"]},
     "InvalidParameterValue"),
    ("mug-r-remove-default-add-twice", "redis", ["r-def", "r-iam"],
     {"UserIdsToAdd": ["r-iam2", "r-iam2"], "UserIdsToRemove": ["r-def"]}, "InvalidParameterValue"),
    ("mug-r-remove-default-and-existing-non-member", "redis", ["r-def", "r-iam"],
     {"UserIdsToRemove": ["r-def", "r-iam2"]}, "InvalidParameterValue"),
    ("mug-r-remove-default-add-valkey-default", "redis", ["r-def", "r-iam"],
     {"UserIdsToAdd": ["v-def"], "UserIdsToRemove": ["r-def"]}, None),
    ("mug-r-remove-default-add-two-case", "redis", ["r-def", "r-iam"],
     {"UserIdsToAdd": ["r-case", "r-case2"], "UserIdsToRemove": ["r-def"]}, None),
])
def test_redis_user_group_earlier_errors_come_before_default_user(ec, ec_users, key, engine, members, modify, code):
    """AWS refuses these with another error before the default-user check (real AWS, us-east-2: the key
    named). Where MiniStack emulates that error the call gets its code; MiniStack contract: where it
    doesn't yet (code None), the call keeps MiniStack's earlier answer, 200, rather than DefaultUserRequired."""
    ids, groups, s = ec_users
    ids = {**ids, "missing": f"dug-missing-{s}"}
    raw = make_client("elasticache", {"parameter_validation": False})
    group_id = f"dug-{_uid()}-{s}"
    groups.append(group_id)
    params = {"UserGroupId": group_id, "UserIds": [ids[m] for m in members]}
    if engine:
        params["Engine"] = engine
    call, kwargs = raw.create_user_group, params
    if modify:
        ec.create_user_group(**params)
        call, kwargs = raw.modify_user_group, {"UserGroupId": group_id,
                                               **{k: [ids[m] for m in v] for k, v in modify.items()}}
    if code is None:
        assert call(**kwargs)["ResponseMetadata"]["HTTPStatusCode"] == 200
    else:
        with pytest.raises(ClientError) as exc:
            call(**kwargs)
        assert exc.value.response["Error"]["Code"] == code


@pytest.mark.parametrize("key,group_engine,engine", [
    ("mug-engine-redis-remove-default", "redis", "redis"),
    ("mug-engine-Redis-remove-default", "redis", "Redis"),
    ("mug-engine-memcached-remove-default", "redis", "memcached"),
    ("mug-r-engine-junk-remove-default", "redis", "not-an-engine"),
    ("mug-v-engine-redis-remove-default", "valkey", "redis"),
    ("mug-v-engine-memcached-remove-default", "valkey", "memcached"),
    ("mug-v-engine-junk-remove-default", "valkey", "not-an-engine"),
])
def test_modify_user_group_engine_redis_keeps_default_user(ec, ec_users, key, group_engine, engine):
    """Sending any Engine but `valkey` with the change keeps the rule, also on a Valkey group of Redis users,
    and the group is left as it was, engine included (real AWS, us-east-2, rounds 7, 9, 10 and 11: the key
    named and describe-after-<key>)."""
    ids, groups, s = ec_users
    group_id = f"dug-{_uid()}-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine=group_engine, UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(group_id)
    raw = make_client("elasticache", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        raw.modify_user_group(UserGroupId=group_id, Engine=engine, UserIdsToRemove=[ids["r-def"]])
    _assert_default_user_required(exc)
    group = ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]
    assert (group["Engine"], group["UserIds"]) == (group_engine, [ids["r-def"], ids["r-iam"]])


@pytest.mark.parametrize("key,group_engine,engine,add,remove,after", [
    ("mug-r-engine-valkey-add-user", "redis", "valkey", ["r-iam2"], [], ["r-def", "r-iam", "r-iam2"]),
    ("mug-r-engine-valkey-remove-iam", "redis", "valkey", [], ["r-iam"], ["r-def"]),
    ("mug-v-engine-redis-add-user", "valkey", "redis", ["r-iam2"], [], ["r-def", "r-iam", "r-iam2"]),
    ("mug-v-engine-redis-remove-iam", "valkey", "redis", [], ["r-iam"], ["r-def"]),
    ("mug-r-engine-valkey-add-Default", "redis", "valkey", ["r-case"], [], ["r-def", "r-iam", "r-case"]),
    ("mug-r-engine-valkey-remove-all", "redis", "valkey", [], ["r-def", "r-iam"], []),
])
def test_modify_user_group_engine_change_with_other_users(ec, ec_users, key, group_engine, engine, add, remove,
                                                          after):
    """An engine change made together with adding or removing other users is applied with them, `Default`
    beside `default` included (real AWS, us-east-2, round10-last.py and round12-attached-cfn.py: the key named
    and describe-after-<key>; round13-tags-engine.py for removing every user)."""
    ids, groups, s = ec_users
    group_id = f"dug-{_uid()}-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine=group_engine, UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(group_id)
    params = {"UserGroupId": group_id, "Engine": engine}
    if add:
        params["UserIdsToAdd"] = [ids[m] for m in add]
    if remove:
        params["UserIdsToRemove"] = [ids[m] for m in remove]
    ec.modify_user_group(**params)
    group = ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]
    assert (group["Engine"], group["UserIds"]) == (engine, [ids[m] for m in after])


@pytest.mark.parametrize("engine", ["valkey", "Valkey", "VALKEY"])
def test_modify_user_group_engine_valkey_removes_default_user(ec, ec_users, engine):
    """Changing the group to Valkey (any case) in the same call lets its `default` user go, and the group is
    Valkey afterwards (real AWS, us-east-2: round 7's mug-engine-valkey-remove-default, round 9's
    mug-engine-Valkey-remove-default, mug-engine-VALKEY-remove-default, and their describe-after-<key>)."""
    ids, groups, s = ec_users
    group_id = f"dug-engine-valkey-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(group_id)
    ec.modify_user_group(UserGroupId=group_id, Engine=engine, UserIdsToRemove=[ids["r-def"]])
    group = ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]
    assert (group["Engine"], group["UserIds"]) == ("valkey", [ids["r-iam"]])


def test_modify_valkey_user_group_engine_redis(ec, ec_users):
    """A Valkey group of Redis users with a `default` one changed to Redis on its own becomes Redis (real AWS,
    us-east-2, round9-remaining.py: mug-v-engine-redis-only, describe-after-mug-v-engine-redis-only)."""
    ids, groups, s = ec_users
    changed = f"dug-{_uid()}-{s}"
    ec.create_user_group(UserGroupId=changed, Engine="valkey", UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(changed)
    ec.modify_user_group(UserGroupId=changed, Engine="redis")
    group = ec.describe_user_groups(UserGroupId=changed)["UserGroups"][0]
    assert (group["Engine"], group["UserIds"]) == ("redis", [ids["r-def"], ids["r-iam"]])


def test_modify_valkey_user_group_engine_redis_adding_default_user(ec, ec_users):
    """A Valkey group can be made Redis in the call that adds its `default` user (real AWS, us-east-2,
    round13-tags-engine.py: mug-v-engine-redis-add-default, describe-after-mug-v-engine-redis-add-default)."""
    ids, groups, s = ec_users
    group_id = f"dug-{_uid()}-{s}"
    groups.append(group_id)
    ec.create_user_group(UserGroupId=group_id, Engine="valkey", UserIds=[ids["r-iam"]])
    ec.modify_user_group(UserGroupId=group_id, Engine="redis", UserIdsToAdd=[ids["r-def"]])
    group = ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]
    assert (group["Engine"], group["UserIds"]) == ("redis", [ids["r-iam"], ids["r-def"]])


@pytest.mark.parametrize("key,members", [
    ("mug-v-no-default-engine-redis", ["r-iam"]),
    ("mug-v-empty-engine-redis", []),
])
def test_modify_valkey_user_group_engine_redis_requires_default_user(ec, ec_users, key, members):
    """Making a Valkey group without a `default` user Redis is refused, and it stays Valkey (real AWS,
    us-east-2, round13-tags-engine.py: the key named and describe-after-<key>)."""
    ids, groups, s = ec_users
    group_id = f"dug-{_uid()}-{s}"
    groups.append(group_id)
    raw = make_client("elasticache", {"parameter_validation": False})  # the capture's empty UserIds
    raw.create_user_group(UserGroupId=group_id, Engine="valkey", UserIds=[ids[m] for m in members])
    with pytest.raises(ClientError) as exc:
        ec.modify_user_group(UserGroupId=group_id, Engine="redis")
    _assert_default_user_required(exc)
    group = ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]
    assert (group["Engine"], group["UserIds"]) == ("valkey", [ids[m] for m in members])


@pytest.mark.parametrize("case", ["valkey-user", "duplicate-name", "attached", "serverless-attached"])
def test_modify_user_group_uncaptured_engine_changes_are_ignored(ec, ec_users, case):
    """MiniStack contract: Engine `valkey` leaves the engine as it was, as before this change, with a second
    `default` user added (AWS refuses two users with one name: DuplicateUserName, cug-r-two-defaults,
    mug-r-remove-default-add-two-case), or on a group attached to a replication group or a serverless
    cache (AWS: InvalidParameterValue, mug-rg-engine-valkey-only, mug-sl-engine-valkey-only), errors
    MiniStack doesn't emulate; and, not captured, with a Valkey user added."""
    ids, groups, s = ec_users
    group_id, rg_id = f"dug-{_uid()}-{s}", f"dug-rg-{_uid()}-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(group_id)
    params = {"UserGroupId": group_id, "Engine": "valkey"}
    if case == "valkey-user":
        params["UserIdsToAdd"] = [ids["v-iam"]]
    elif case == "duplicate-name":
        params["UserIdsToAdd"] = [ids["r-def2"]]
    elif case == "serverless-attached":
        ec.create_serverless_cache(ServerlessCacheName=rg_id, Engine="redis", MajorEngineVersion="7",
                                   UserGroupId=group_id)
    else:
        ec.create_replication_group(ReplicationGroupId=rg_id, ReplicationGroupDescription="ministack test",
                                    Engine="redis", CacheNodeType="cache.t4g.micro", NumCacheClusters=1,
                                    TransitEncryptionEnabled=True, UserGroupIds=[group_id])
    try:
        ec.modify_user_group(**params)
        assert ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]["Engine"] == "redis"
        if case in ("attached", "valkey-user"):  # and the rest of the call goes through, as before
            ec.modify_user_group(UserGroupId=group_id, Engine="valkey", UserIdsToRemove=[ids["r-def"]])
            assert ids["r-def"] not in ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]["UserIds"]
    finally:
        if case == "attached":
            ec.delete_replication_group(ReplicationGroupId=rg_id)
        elif case == "serverless-attached":
            ec.delete_serverless_cache(ServerlessCacheName=rg_id)


@pytest.mark.parametrize("key,template,length", [
    ("cug-upper-id-no-default", "dug-UPPER-{s}", None),
    ("cug-id-39-no-default", "dug-{s}-" + "a" * 26, 39),
    ("cug-id-40-no-default", "dug-{s}-" + "a" * 27, 40),
    ("cug-id-trailing-hyphen-no-default", "dug-trail-{s}-", None),
    ("cug-id-1-char-no-default", "{c}", 1),
])
def test_create_user_group_accepted_ids_require_default_user(ec, ec_users, key, template, length):
    """Ids with upper-case letters, of 1, 39 or 40 characters, or ending with a hyphen pass AWS's id check and
    get the rule (real AWS, us-east-2, round9-remaining.py, round10-last.py and round14-dup-tags.py: the key
    named)."""
    ids, groups, s = ec_users
    group_id = template.format(s=s, c="abcdefghijklmnop"[int(s[0], 16)])  # a 1-character id still varies
    groups.append(group_id)
    assert length is None or len(group_id) == length
    with pytest.raises(ClientError) as exc:
        ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-iam"]])
    _assert_default_user_required(exc)


@pytest.mark.parametrize("key,tags", [
    ("cug-tags-valid-no-default", [{"Key": "k", "Value": "v"}]),
    ("cug-tags-key-128-no-default", [{"Key": "k" * 128, "Value": "v"}]),
    ("cug-tags-no-value-no-default", [{"Key": "k"}]),
    ("cug-tags-51-tags-no-default", [{"Key": f"k{i}", "Value": "v"} for i in range(51)]),
    ("cug-tags-duplicate-keys-no-default", [{"Key": "k", "Value": "a"}, {"Key": "k", "Value": "b"}]),
    ("cug-tags-multibyte-128-no-default", [{"Key": "é" * 128, "Value": "v"}]),
])
def test_create_user_group_accepted_tags_require_default_user(ec, ec_users, key, tags):
    """Tags with keys up to 128 characters (multibyte ones too), without a value, with a key repeated, or
    more than 50 of them pass AWS's checks and the group gets the rule (real AWS, us-east-2,
    round13-tags-engine.py and round14-dup-tags.py: the key named)."""
    ids, groups, s = ec_users
    group_id = f"dug-{_uid()}-{s}"
    groups.append(group_id)
    raw = make_client("elasticache", {"parameter_validation": False})
    with pytest.raises(ClientError) as exc:
        raw.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-iam"]], Tags=tags)
    _assert_default_user_required(exc)


@pytest.mark.parametrize("key,group_id,tags", [
    ("cug-bad-chars-no-default", "dug-Bad_ID!!-{s}", None),
    ("cug-long-id-no-default", "dug-{s}-" + "x" * 300, None),
    ("cug-id-41-no-default", "dug-{s}-" + "a" * 28, None),
    ("cug-id-leading-digit-no-default", "9dug-digit-{s}", None),
    ("cug-id-double-hyphen-no-default", "dug--double-{s}", None),
    ("cug-empty-tag-key-no-default", "dug-tagkey-{s}", [{"Key": "", "Value": "v"}]),
    ("cug-tags-key-129-no-default", "dug-tags-{s}", [{"Key": "k" * 129, "Value": "v"}]),
    ("cug-tags-aws-prefix-no-default", "dug-tags-{s}", [{"Key": "aws:x", "Value": "v"}]),
    ("cug-tags-value-257-no-default", "dug-tags-{s}", [{"Key": "k", "Value": "v" * 257}]),
    ("cug-tags-odd-chars-no-default", "dug-tags-{s}", [{"Key": "a#b*{}", "Value": "v"}]),
])
def test_create_user_group_input_errors_are_not_default_user_required(ec, ec_users, key, group_id, tags):
    """MiniStack contract: AWS refuses these with InvalidParameterValue before the default-user check (real
    AWS, us-east-2, round9-remaining.py, round10-last.py, round11-id-length.py, round13-tags-engine.py and
    round14-dup-tags.py: the key named), which
    MiniStack doesn't emulate yet. Until it does, they keep MiniStack's earlier answer, 200, rather than
    DefaultUserRequired."""
    ids, groups, s = ec_users
    group_id = group_id.format(s=s)
    raw = make_client("elasticache", {"parameter_validation": False})
    params = {"UserGroupId": group_id, "Engine": "redis", "UserIds": [ids["r-iam"]]}
    if tags:
        params["Tags"] = tags
    resp = raw.create_user_group(**params)
    groups.append(group_id)
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_modify_user_group_engine_valkey_then_removes_default_user(ec, ec_users):
    """A group changed to Valkey on its own is a Valkey group, whose `default` user can then be removed
    (real AWS, us-east-2, round8-engine-sequence.py: mug-seq-engine-valkey, describe-seq-after-engine,
    mug-seq-remove-default, describe-seq-after-remove)."""
    ids, groups, s = ec_users
    group_id = f"dug-engine-valkey-only-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["r-def"], ids["r-iam"]])
    groups.append(group_id)
    ec.modify_user_group(UserGroupId=group_id, Engine="valkey")
    group = ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]
    assert (group["Engine"], group["UserIds"]) == ("valkey", [ids["r-def"], ids["r-iam"]])
    ec.modify_user_group(UserGroupId=group_id, UserIdsToRemove=[ids["r-def"]])
    assert ec.describe_user_groups(UserGroupId=group_id)["UserGroups"][0]["UserIds"] == [ids["r-iam"]]


def test_redis_user_group_valkey_default_user_removal_is_not_default_user_required(ec, ec_users):
    """MiniStack contract, not captured: removing a Valkey `default` user from a Redis group (a group AWS
    refuses to create, cug-r-valkey-default) keeps MiniStack's earlier answer, 200."""
    ids, groups, s = ec_users
    group_id = f"dug-vdef-in-redis-{s}"
    ec.create_user_group(UserGroupId=group_id, Engine="redis", UserIds=[ids["v-def"], ids["r-iam"]])
    groups.append(group_id)
    resp = ec.modify_user_group(UserGroupId=group_id, UserIdsToRemove=[ids["v-def"]])
    assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_elasticache_subnet_and_param_groups_are_region_scoped():
    east = _ec_client("us-east-1")
    west = _ec_client("us-west-2")
    subnet_name = f"scope-subnet-{_uid()}"
    param_name = f"scope-param-{_uid()}"

    east.create_cache_subnet_group(
        CacheSubnetGroupName=subnet_name,
        CacheSubnetGroupDescription="east subnet",
        SubnetIds=["subnet-00000001"],
    )
    west.create_cache_subnet_group(
        CacheSubnetGroupName=subnet_name,
        CacheSubnetGroupDescription="west subnet",
        SubnetIds=["subnet-00000001"],
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
        _ec.load_persisted_state({
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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

def test_delete_cache_subnet_group(ec):
    """DeleteCacheSubnetGroup: create then delete, verify gone."""
    name = f"del-sg-{_uid()}"
    ec.create_cache_subnet_group(
        CacheSubnetGroupName=name,
        CacheSubnetGroupDescription="To be deleted",
        SubnetIds=["subnet-00000001"],
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


def test_cache_subnet_group_keeps_subnets_and_tags(ec):
    name = f"sg-keep-{_uid()}"
    group = ec.create_cache_subnet_group(
        CacheSubnetGroupName=name,
        CacheSubnetGroupDescription="two subnets",
        SubnetIds=["subnet-00000001", "subnet-00000002"],
        Tags=[{"Key": "env", "Value": "dev"}],
    )["CacheSubnetGroup"]
    try:
        described = ec.describe_cache_subnet_groups(CacheSubnetGroupName=name)["CacheSubnetGroups"][0]
        for g in (group, described):
            assert g["VpcId"] == "vpc-00000001"
            assert [(s["SubnetIdentifier"], s["SubnetAvailabilityZone"]["Name"]) for s in g["Subnets"]] == [
                ("subnet-00000001", "us-east-1a"), ("subnet-00000002", "us-east-1b")]
        assert ec.list_tags_for_resource(ResourceName=group["ARN"])["TagList"] == [{"Key": "env", "Value": "dev"}]

        with pytest.raises(ClientError) as exc:
            ec.create_cache_subnet_group(CacheSubnetGroupName=name, CacheSubnetGroupDescription="again",
                                         SubnetIds=["subnet-00000001"])
        assert exc.value.response["Error"]["Code"] == "CacheSubnetGroupAlreadyExists"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

        modified = ec.modify_cache_subnet_group(
            CacheSubnetGroupName=name, SubnetIds=["subnet-00000003"])["CacheSubnetGroup"]
        assert modified["VpcId"] == "vpc-00000001"
        assert [s["SubnetIdentifier"] for s in modified["Subnets"]] == ["subnet-00000003"]

        with pytest.raises(ClientError) as exc:
            ec.modify_cache_subnet_group(CacheSubnetGroupName=name, SubnetIds=["subnet-doesnotexist"])
        assert exc.value.response["Error"]["Code"] == "InvalidSubnet"
    finally:
        ec.delete_cache_subnet_group(CacheSubnetGroupName=name)

    with pytest.raises(ClientError) as exc:
        ec.describe_cache_subnet_groups(CacheSubnetGroupName=name)
    assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


# ---------------------------------------------------------------------------
# 8. ResetCacheParameterGroup
# ---------------------------------------------------------------------------

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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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
@pytest.mark.data_plane
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

@pytest.mark.data_plane
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


@pytest.mark.data_plane
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


@pytest.mark.data_plane
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


@pytest.mark.data_plane
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


@pytest.mark.data_plane
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
# 12. Serverless caches
# ---------------------------------------------------------------------------

def _wait_serverless(ec, name, timeout=180):
    deadline = time.time() + timeout
    while True:
        cache = ec.describe_serverless_caches(ServerlessCacheName=name)["ServerlessCaches"][0]
        if cache["Status"] not in ("creating", "modifying") or time.time() > deadline:
            return cache
        time.sleep(0.5)


def _serverless_ca_file(tmp_path):
    import urllib.request
    with urllib.request.urlopen(f"{ENDPOINT}/_ministack/elasticache/ca.pem", timeout=10) as resp:
        pem = resp.read()
    assert pem.startswith(b"-----BEGIN CERTIFICATE-----")
    path = tmp_path / "elasticache-ca.pem"
    path.write_bytes(pem)
    return str(path)


def _tls_command(endpoint, ca_file, command):
    import socket
    import ssl
    ctx = ssl.create_default_context(cafile=ca_file)
    with socket.create_connection((endpoint["Address"], endpoint["Port"]), timeout=10) as raw:
        with ctx.wrap_socket(raw, server_hostname=endpoint["Address"]) as conn:
            parts = command.split()
            conn.sendall(f"*{len(parts)}\r\n".encode()
                         + b"".join(f"${len(p)}\r\n{p}\r\n".encode() for p in parts))
            return conn.recv(65536)


def _tls_session(endpoint, ca_file, *commands):
    """Replies to ``commands`` sent in order on one TLS connection."""
    import socket
    import ssl
    ctx = ssl.create_default_context(cafile=ca_file)
    replies = []
    with socket.create_connection((endpoint["Address"], endpoint["Port"]), timeout=10) as raw:
        with ctx.wrap_socket(raw, server_hostname=endpoint["Address"]) as conn:
            for parts in commands:
                conn.sendall(f"*{len(parts)}\r\n".encode()
                             + b"".join(f"${len(p)}\r\n{p}\r\n".encode() for p in parts))
                time.sleep(0.2)
                replies.append(conn.recv(65536))
    return replies


def _docker_reachable():
    try:
        import docker
        return docker.from_env().ping()
    except Exception:
        return False


@pytest.mark.skipif(not _docker_reachable(), reason="needs Docker for the serverless cache container")
def test_serverless_cache_user_group_enforces_acl(ec, tmp_path):
    """#2457: with a Valkey user group the engine's default user is off (AWS RBAC docs), the group's users
    exist with their access strings and passwords, and an iam user logs in with its token."""
    uid, pw = _uid(), "a-long-enough-password-1"
    pw_user, iam_user, ro_user, group, name = (f"u-pw-{uid}", f"u-iam-{uid}", f"u-ro-{uid}", f"g-{uid}", f"sls-acl-{uid}")
    ec.create_user(UserId=pw_user, UserName=pw_user, Engine="valkey", AccessString="on ~* +@all",
                   AuthenticationMode={"Type": "password", "Passwords": [pw]})
    ec.create_user(UserId=iam_user, UserName=iam_user, Engine="valkey", AccessString="on ~* +@all",
                   AuthenticationMode={"Type": "iam"})
    ec.create_user(UserId=ro_user, UserName=ro_user, Engine="valkey", AccessString="on ~app:* -@all +get",
                   AuthenticationMode={"Type": "password", "Passwords": [pw]})
    ec.create_user_group(UserGroupId=group, Engine="valkey", UserIds=[pw_user, iam_user, ro_user])
    ec.create_serverless_cache(ServerlessCacheName=name, Engine="valkey", MajorEngineVersion="8", UserGroupId=group)
    try:
        cache = _wait_serverless(ec, name)
        assert cache["Status"] == "available"
        ep, ca = cache["Endpoint"], _serverless_ca_file(tmp_path)
        noauth, wrongpass = b"-NOAUTH Authentication required.\r\n", \
            b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"
        assert _tls_session(ep, ca, ("PING",), ("SET", "k", "v")) == [noauth, noauth]
        assert _tls_session(ep, ca, ("AUTH", pw_user, "wrong-password-123456"), ("PING",)) == [wrongpass, noauth]
        assert _tls_session(ep, ca, ("AUTH", "default", pw), ("AUTH", pw)) == [wrongpass, wrongpass]
        assert _tls_session(ep, ca, ("AUTH", pw_user, pw), ("ACL", "WHOAMI"), ("SET", "k", "v")) == [
            b"+OK\r\n", f"${len(pw_user)}\r\n{pw_user}\r\n".encode(), b"+OK\r\n"]
        assert _tls_session(ep, ca, ("AUTH", iam_user, f"{name}/?Action=connect&User={iam_user}"), ("PING",)) == [
            b"+OK\r\n", b"+PONG\r\n"]
        ro = _tls_session(ep, ca, ("AUTH", ro_user, pw), ("GET", "app:1"), ("SET", "app:1", "x"))
        assert ro[:2] == [b"+OK\r\n", b"$-1\r\n"] and ro[2].startswith(b"-NOPERM")
        assert _tls_session(ep, ca, ("HELLO", "3", "AUTH", pw_user, "nope-nope-nope-nope")) == [wrongpass]
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)
        ec.delete_user_group(UserGroupId=group)
        for user in (pw_user, iam_user, ro_user):
            ec.delete_user(UserId=user)


@pytest.mark.skipif(not _docker_reachable(), reason="needs Docker for the serverless cache container")
def test_serverless_cache_user_changes_reach_the_engine_with_restricted_commands(ec, tmp_path):
    """User and group changes reach a running serverless cache (#2554), its users cannot run ACL SETUSER or
    CONFIG (AWS restricted commands), and detaching the group reopens `default`."""
    uid, pw = _uid(), "a-long-enough-password-1"
    keep, rem, group, name = f"u-keep-{uid}", f"u-rem-{uid}", f"g-{uid}", f"sls-chg-{uid}"
    for user in (keep, rem):
        ec.create_user(UserId=user, UserName=user, Engine="valkey", AccessString="on ~* +@all",
                       AuthenticationMode={"Type": "password", "Passwords": [pw]})
    ec.create_user_group(UserGroupId=group, Engine="valkey", UserIds=[keep, rem])
    ec.create_serverless_cache(ServerlessCacheName=name, Engine="valkey", MajorEngineVersion="8", UserGroupId=group)
    try:
        cache = _wait_serverless(ec, name)
        ep, ca = cache["Endpoint"], _serverless_ca_file(tmp_path)
        ok, wrongpass = b"+OK\r\n", b"-WRONGPASS invalid username-password pair or user is disabled.\r\n"
        replies = _tls_session(ep, ca, ("AUTH", keep, pw), ("SET", "k", "v"), ("ACL", "SETUSER", "x", "on"),
                               ("CONFIG", "GET", "maxmemory"))
        assert replies[:2] == [ok, ok] and all(r.startswith(b"-NOPERM") for r in replies[2:])
        assert _tls_session(ep, ca, ("AUTH", rem, pw)) == [ok]

        ec.modify_user_group(UserGroupId=group, UserIdsToRemove=[rem])
        assert _tls_session(ep, ca, ("AUTH", rem, pw)) == [wrongpass]
        assert _tls_session(ep, ca, ("AUTH", keep, pw), ("GET", "k")) == [ok, b"$1\r\nv\r\n"]  # data kept

        ec.modify_user(UserId=keep, AccessString="on ~app:* -@all +get")
        assert _tls_session(ep, ca, ("AUTH", keep, pw), ("SET", "k", "v"))[1].startswith(b"-NOPERM")

        ec.modify_serverless_cache(ServerlessCacheName=name, RemoveUserGroup=True)
        assert _tls_session(ep, ca, ("PING",)) == [b"+PONG\r\n"]
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)
        ec.delete_user_group(UserGroupId=group)
        for user in (keep, rem):
            ec.delete_user(UserId=user)


# Each access string's channels on the engine, as `ACL GETUSER` reported them on AWS in us-east-2: the
# same on serverless Valkey 7, 8 and 9 and Redis OSS 7 and on node-based Valkey 7.2 (capture rounds
# aws-serverless-cases, aws-selectors-serverless, aws-engines (v8, v9, r7), aws-channel-cases and
# aws-selectors-node). Round aws-one-group-password captured every string here again in one group per
# engine, as the test sets them up, and three of them also as password users, with the same channels.
_AWS_ACL_CHANNELS = {
    "on ~* +@all": "&*",
    "on ~* &allowed* +@all": "&allowed*",
    "on ~* resetchannels +@all": "",
    "on ~* allchannels +@all": "&*",
    "on ~* &* +@all": "&*",
    "on ~* &a resetchannels &b +@all": "&b",
    "on ~* +@all (&chan +get ~k)": "",
    "on ~* +@all (+get ~k)": "",
    # without channel rules, serverless only (aws-no-channel-shapes, aws-no-channel-shapes-v8-v9)
    "on ~k +get": "&*",
    "off ~* +@all": "&*",
    "on ~* -@all +publish +subscribe": "&*",
    "on ~app:* +@all": "&*",
    "on +@all": "&*",
    # the #2518 unit test's string; serverless only (aws-one-group-password)
    "on ~foo) (~x +get) +set": "",
}
# More selector shapes, captured in groups that also held a password user named default, on every
# serverless engine (aws-selectors-default); checked in the same setup.
_AWS_ACL_CHANNELS_WITH_DEFAULT = {
    "on ~* +@all": "&*",
    "on ~k +get (&chan +publish)": "",
    "off (+get ~k)": "",
    "on ~* &x +@all (+get ~k)": "&x",
    "on ~* +@all (+get ~k) (&c +publish)": "",
    "on (~k +get)": "",
}


def test_user_access_string_with_channel_pattern_round_trips(ec):
    """AWS returns an access string with `&` XML-escaped in CreateUser, DescribeUsers, ModifyUser and
    DeleteUser (captures create-user-ctl, describe-user-ctl, modify-user-amp, delete-user-amp); AWS also
    rewrites it (`on ~* resetchannels &allowed* +@all`), which MiniStack does not, so only the pattern
    is compared."""
    uid = f"u-amp-{_uid()}"
    created = ec.create_user(UserId=uid, UserName=uid, Engine="valkey", AccessString="on ~* &allowed* +@all",
                             AuthenticationMode={"Type": "iam"})
    try:
        assert "&allowed*" in created["AccessString"]
        assert "&allowed*" in ec.describe_users(UserId=uid)["Users"][0]["AccessString"]
        assert "&other*" in ec.modify_user(UserId=uid, AccessString="on ~* &other* +@all")["AccessString"]
    finally:
        assert "&other*" in ec.delete_user(UserId=uid)["AccessString"]


def test_user_acl_channels_default_to_all():
    """The ACL SETUSER commands MiniStack sends: `&*` before an access string without channel rules or
    selectors, and nothing before the rest."""
    def user(access):
        return {"UserId": "u", "UserName": "u", "AccessString": access, "Authentication": {"Type": "iam"}}

    with_all_channels = {"on ~* +@all", "on ~k +get", "off ~* +@all", "on ~* -@all +publish +subscribe",
                         "on ~app:* +@all", "on +@all"}  # the rest set channels or have a selector
    for access in {**_AWS_ACL_CHANNELS, **_AWS_ACL_CHANNELS_WITH_DEFAULT}:
        prefix = ["reset", "&*"] if access in with_all_channels else ["reset"]
        assert elasticache._acl_setuser(user(access)) == ["ACL", "SETUSER", "u", *prefix, *access.split(), "nopass"]


def test_user_acl_keeps_unicode_space_inside_a_pattern():
    """AWS stores `~a\\xa0b` appended to `on ~x +@all` as one key pattern (us-east-2, issue #2551,
    round23 modify-a-key-nbsp). MiniStack sends the engine the rules as stored; no capture reads a
    real cache's ACL for it."""
    user = {"UserId": "u", "UserName": "u", "AccessString": "on ~x ~a\xa0b +@all", "Authentication": {"Type": "iam"}}
    assert elasticache._acl_setuser(user) == ["ACL", "SETUSER", "u", "reset", "&*", "on", "~x", "~a\xa0b", "+@all",
                                              "nopass"]


@pytest.mark.skipif(not _docker_reachable(), reason="needs Docker for the serverless cache container")
@pytest.mark.parametrize("engine,major", [("valkey", "7"), ("valkey", "8"), ("valkey", "9"), ("redis", "7")])
def test_serverless_cache_user_pubsub_channels(ec, tmp_path, engine, major):
    """Capture rounds aws-serverless-cases, aws-selectors-serverless, aws-engines (v8, v9, r7),
    aws-no-channel-shapes and aws-no-channel-shapes-v8-v9 (AWS, us-east-2): each user's channels match
    `_AWS_ACL_CHANNELS`, an iam user with
    `on ~* +@all` can SUBSCRIBE and PUBLISH on any channel, and one with `on ~* &allowed* +@all` gets
    NOPERM outside `allowed*` (Valkey's exact text; for Redis only a channel NOPERM, see below).
    A Valkey group without a default user has it as `off &* -@all`; AWS refuses a Redis group without
    one (DefaultUserRequired), so the Redis group has one with `on ~* +@all`, which gets `&*`."""
    import socket
    import ssl
    uid, pw = _uid(), "msc-capture-default-password-1"
    expected = _AWS_ACL_CHANNELS
    users = {access: f"u-{i}-{uid}" for i, access in enumerate(expected)}
    all_user, ctl_user = users["on ~* +@all"], users["on ~* &allowed* +@all"]
    group, name, default_id = f"g-{uid}", f"sls-chan-{uid}", f"u-def-{uid}"
    for access, user_id in users.items():
        ec.create_user(UserId=user_id, UserName=user_id, Engine=engine, AccessString=access,
                       AuthenticationMode={"Type": "iam"})
    group_ids = list(users.values())
    if engine == "redis":
        ec.create_user(UserId=default_id, UserName="default", Engine="redis", AccessString="on ~* +@all",
                       AuthenticationMode={"Type": "password", "Passwords": [pw]})
        group_ids.append(default_id)
    ec.create_user_group(UserGroupId=group, Engine=engine, UserIds=group_ids)
    ec.create_serverless_cache(ServerlessCacheName=name, Engine=engine, MajorEngineVersion=major, UserGroupId=group)
    try:
        cache = _wait_serverless(ec, name)
        assert cache["Status"] == "available"
        ep, ca = cache["Endpoint"], _serverless_ca_file(tmp_path)
        # AWS's Redis OSS 7.1 words the error "this user has no permissions to access one of the channels
        # used as arguments"; MiniStack's redis:7 image words it like Valkey, so for Redis only a channel
        # NOPERM is required.
        valkey_nochan = b"-NOPERM No permissions to access a channel\r\n"

        def nochan(reply):
            if engine == "redis":
                return reply.startswith(b"-NOPERM ") and b"channel" in reply
            return reply == valkey_nochan

        def auth(user):
            return ("AUTH", user, f"{name}/?Action=connect&User={user}")

        def subscribe_and_publish(user, channel):
            ctx = ssl.create_default_context(cafile=ca)
            with socket.create_connection((ep["Address"], ep["Port"]), timeout=10) as raw:
                with ctx.wrap_socket(raw, server_hostname=ep["Address"]) as sub:
                    for parts in (auth(user), ("SUBSCRIBE", channel)):
                        sub.sendall(f"*{len(parts)}\r\n".encode()
                                    + b"".join(f"${len(p)}\r\n{p}\r\n".encode() for p in parts))
                        time.sleep(0.2)
                        subscribed = sub.recv(65536)
                    published = _tls_session(ep, ca, auth(user), ("PUBLISH", channel, "hello"))[1]
                    time.sleep(0.2)
                    return subscribed, published, sub.recv(65536) if published == b":1\r\n" else None

        for access, user_id in users.items():
            channels = expected[access]
            # read through the `on ~* +@all` user, as in the capture: an `off` user cannot AUTH
            reply = _tls_session(ep, ca, auth(all_user), ("ACL", "GETUSER", user_id))[1]
            # the root channels come right before `selectors`; each selector has its own channels field
            assert f"$8\r\nchannels\r\n${len(channels)}\r\n{channels}\r\n$9\r\nselectors\r\n".encode() in reply, access
        selector_user = users["on ~* +@all (&chan +get ~k)"]
        reply = _tls_session(ep, ca, auth(selector_user), ("ACL", "GETUSER", selector_user), ("PUBLISH", "chan", "x"))
        # a selector's channels are its last field, and the selectors list ends the reply
        assert reply[1].endswith(b"$8\r\nchannels\r\n$5\r\n&chan\r\n") and nochan(reply[2])
        selector_user = users["on ~* +@all (+get ~k)"]
        reply = _tls_session(ep, ca, auth(all_user), ("ACL", "GETUSER", selector_user))[1]
        assert reply.endswith(b"$8\r\nchannels\r\n$0\r\n\r\n")
        if engine == "redis":
            default = _tls_session(ep, ca, ("AUTH", "default", pw), ("ACL", "GETUSER", "default"))[1]
            assert b"$8\r\nchannels\r\n$2\r\n&*\r\n$9\r\nselectors\r\n" in default
        else:
            assert b"$25\r\nuser default off &* -@all\r\n" in _tls_session(ep, ca, auth(all_user), ("ACL", "LIST"))[1]
        chan = b"$8\r\nany-chan\r\n"
        assert subscribe_and_publish(all_user, "any-chan") == (
            b"*3\r\n$9\r\nsubscribe\r\n" + chan + b":1\r\n", b":1\r\n",
            b"*3\r\n$7\r\nmessage\r\n" + chan + b"$5\r\nhello\r\n")
        subscribed, published, message = subscribe_and_publish(ctl_user, "any-chan")
        assert nochan(subscribed) and nochan(published) and message is None
        assert subscribe_and_publish(ctl_user, "allowed-chan")[1] == b":1\r\n"
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)
        ec.delete_user_group(UserGroupId=group)
        for user in group_ids:
            ec.delete_user(UserId=user)


@pytest.mark.skipif(not _docker_reachable(), reason="needs Docker for the serverless cache container")
@pytest.mark.parametrize("engine,major", [("valkey", "7"), ("valkey", "8"), ("valkey", "9"), ("redis", "7")])
def test_serverless_cache_default_user_pubsub_channels(ec, tmp_path, engine, major):
    """Capture rounds aws-engines (v7def) and aws-selectors-default (AWS, serverless, us-east-2): a
    password user named default with `on ~* +@all`, in the group when the cache is created, has channels
    `&*` and can PUBLISH, and the group's other users have the root channels in
    `_AWS_ACL_CHANNELS_WITH_DEFAULT`."""
    uid, pw = _uid(), "msc-capture-default-password-1"
    user_id, group, name = f"u-def-{uid}", f"g-{uid}", f"sls-def-{uid}"
    ec.create_user(UserId=user_id, UserName="default", Engine=engine, AccessString="on ~* +@all",
                   AuthenticationMode={"Type": "password", "Passwords": [pw]})
    users = {access: f"u-{i}-{uid}" for i, access in enumerate(_AWS_ACL_CHANNELS_WITH_DEFAULT)}
    for access, other in users.items():
        ec.create_user(UserId=other, UserName=other, Engine=engine, AccessString=access,
                       AuthenticationMode={"Type": "iam"})
    ec.create_user_group(UserGroupId=group, Engine=engine, UserIds=[user_id, *users.values()])
    ec.create_serverless_cache(ServerlessCacheName=name, Engine=engine, MajorEngineVersion=major, UserGroupId=group)
    try:
        cache = _wait_serverless(ec, name)
        assert cache["Status"] == "available"
        ep, ca = cache["Endpoint"], _serverless_ca_file(tmp_path)
        replies = _tls_session(ep, ca, ("AUTH", "default", pw), ("ACL", "GETUSER", "default"),
                               ("PUBLISH", "any-chan", "x"))
        assert replies[0] == b"+OK\r\n" and b"$8\r\nchannels\r\n$2\r\n&*\r\n$9\r\nselectors\r\n" in replies[1]
        assert replies[2] == b":0\r\n"
        reader = users["on ~* +@all"]
        for access, other in users.items():
            channels = _AWS_ACL_CHANNELS_WITH_DEFAULT[access]
            reply = _tls_session(ep, ca, ("AUTH", reader, f"{name}/?Action=connect&User={reader}"),
                                 ("ACL", "GETUSER", other))[1]
            assert f"$8\r\nchannels\r\n${len(channels)}\r\n{channels}\r\n$9\r\nselectors\r\n".encode() in reply, access
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)
        ec.delete_user_group(UserGroupId=group)
        for user in (user_id, *users.values()):
            ec.delete_user(UserId=user)


def test_serverless_cache_create_describe_delete(ec):
    name = f"sls-{_uid()}"
    created = ec.create_serverless_cache(
        ServerlessCacheName=name,
        Engine="valkey",
        MajorEngineVersion="8",
        Description="cache & <counters>",
        SecurityGroupIds=["sg-0123"],
        SubnetIds=["subnet-a", "subnet-b"],
        CacheUsageLimits={"DataStorage": {"Maximum": 1, "Unit": "GB"},
                          "ECPUPerSecond": {"Maximum": 1000}},
        Tags=[{"Key": "env", "Value": "dev"}],
    )["ServerlessCache"]
    try:
        assert created["Status"] in ("creating", "available")
        assert created["Engine"] == "valkey"
        assert created["MajorEngineVersion"] == "8"
        assert created["FullEngineVersion"] == "8.1"
        assert created["Description"] == "cache & <counters>"
        assert created["SecurityGroupIds"] == ["sg-0123"]
        assert created["SubnetIds"] == ["subnet-a", "subnet-b"]
        assert created["CacheUsageLimits"] == {"DataStorage": {"Maximum": 1, "Unit": "GB"},
                                               "ECPUPerSecond": {"Maximum": 1000}}
        assert created["SnapshotRetentionLimit"] == 0
        assert "KmsKeyId" not in created and "UserGroupId" not in created
        assert created["StorageEncryptionType"] == "sse-elasticache"
        arn = created["ARN"]
        assert arn.startswith("arn:aws:elasticache:us-east-1:") and arn.endswith(f":serverlesscache:{name}")

        described = ec.describe_serverless_caches(ServerlessCacheName=name)["ServerlessCaches"]
        assert [c["ARN"] for c in described] == [arn]
        assert {t["Key"]: t["Value"] for t in ec.list_tags_for_resource(ResourceName=arn)["TagList"]} == {"env": "dev"}
        ec.add_tags_to_resource(ResourceName=arn, Tags=[{"Key": "team", "Value": "core"}])
        ec.remove_tags_from_resource(ResourceName=arn, TagKeys=["env"])
        assert ec.list_tags_for_resource(ResourceName=arn)["TagList"] == [{"Key": "team", "Value": "core"}]

        with pytest.raises(ClientError) as exc:
            ec.create_serverless_cache(ServerlessCacheName=name, Engine="valkey")
        assert exc.value.response["Error"]["Code"] == "ServerlessCacheAlreadyExistsFault"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
    finally:
        deleted = ec.delete_serverless_cache(ServerlessCacheName=name)["ServerlessCache"]
    assert deleted["Status"] == "deleting"
    for call in (lambda: ec.describe_serverless_caches(ServerlessCacheName=name),
                 lambda: ec.delete_serverless_cache(ServerlessCacheName=name),
                 lambda: ec.list_tags_for_resource(ResourceName=arn)):
        with pytest.raises(ClientError) as exc:
            call()
        assert exc.value.response["Error"]["Code"] == "ServerlessCacheNotFoundFault"
        assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


@pytest.mark.parametrize("engine,major", [("memcached", None), ("valkey", "6"), ("redis", "8")])
def test_serverless_cache_rejects_unsupported_engine_version(ec, engine, major):
    kwargs = {"MajorEngineVersion": major} if major else {}
    with pytest.raises(ClientError) as exc:
        ec.create_serverless_cache(ServerlessCacheName=f"sls-bad-{_uid()}", Engine=engine, **kwargs)
    assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"


def test_serverless_cache_storage_encryption_type_sse_kms(ec):
    name = f"sls-kms-{_uid()}"
    created = ec.create_serverless_cache(
        ServerlessCacheName=name, Engine="valkey",
        KmsKeyId="arn:aws:kms:us-east-1:000000000000:key/test-key",
    )["ServerlessCache"]
    try:
        assert created["KmsKeyId"] == "arn:aws:kms:us-east-1:000000000000:key/test-key"
        assert created["StorageEncryptionType"] == "sse-kms"
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)


def test_serverless_cache_valkey_major_9(ec):
    name = f"sls-v9-{_uid()}"
    created = ec.create_serverless_cache(
        ServerlessCacheName=name, Engine="valkey", MajorEngineVersion="9",
    )["ServerlessCache"]
    try:
        assert created["MajorEngineVersion"] == "9"
        assert created["FullEngineVersion"] == "9.0"
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)


def test_serverless_cache_name_is_lowercased(ec):
    mixed = f"MyCache-{_uid()}"
    lower = mixed.lower()
    created = ec.create_serverless_cache(ServerlessCacheName=mixed, Engine="valkey")["ServerlessCache"]
    try:
        assert created["ServerlessCacheName"] == lower
        assert created["ARN"].endswith(f":serverlesscache:{lower}")
        described = ec.describe_serverless_caches(ServerlessCacheName=mixed.upper())["ServerlessCaches"]
        assert [c["ServerlessCacheName"] for c in described] == [lower]
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=mixed)
    with pytest.raises(ClientError) as exc:
        ec.describe_serverless_caches(ServerlessCacheName=lower)
    assert exc.value.response["Error"]["Code"] == "ServerlessCacheNotFoundFault"


def test_serverless_cache_modify_rejects_invalid_engine_change(ec):
    name = f"sls-engchg-{_uid()}"
    ec.create_serverless_cache(ServerlessCacheName=name, Engine="valkey", MajorEngineVersion="8")
    try:
        cache = _wait_serverless(ec, name)
        assert (cache["Engine"], cache["MajorEngineVersion"]) == ("valkey", "8")

        with pytest.raises(ClientError) as exc:
            ec.modify_serverless_cache(ServerlessCacheName=name, MajorEngineVersion="7")
        assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
        cache = ec.describe_serverless_caches(ServerlessCacheName=name)["ServerlessCaches"][0]
        assert (cache["Engine"], cache["MajorEngineVersion"]) == ("valkey", "8")

        with pytest.raises(ClientError) as exc:
            ec.modify_serverless_cache(ServerlessCacheName=name, Engine="redis")
        assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
        cache = ec.describe_serverless_caches(ServerlessCacheName=name)["ServerlessCaches"][0]
        assert (cache["Engine"], cache["MajorEngineVersion"]) == ("valkey", "8")
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)


def test_serverless_cache_modify(ec):
    name = f"sls-mod-{_uid()}"
    ec.create_serverless_cache(
        ServerlessCacheName=name, Engine="redis",
        CacheUsageLimits={"DataStorage": {"Maximum": 5, "Unit": "GB"}},
    )
    try:
        cache = _wait_serverless(ec, name)
        assert cache["Status"] == "available"
        assert (cache["MajorEngineVersion"], cache["FullEngineVersion"]) == ("7", "7.1")

        modified = ec.modify_serverless_cache(ServerlessCacheName=name, Description="rate limits")
        assert modified["ServerlessCache"]["Description"] == "rate limits"
        ec.modify_serverless_cache(ServerlessCacheName=name, SnapshotRetentionLimit=3,
                                   DailySnapshotTime="09:00")
        # All-zero limits remove them, as the Terraform provider sends to clear the block.
        ec.modify_serverless_cache(ServerlessCacheName=name, CacheUsageLimits={
            "DataStorage": {"Maximum": 0, "Minimum": 0, "Unit": "GB"},
            "ECPUPerSecond": {"Maximum": 0, "Minimum": 0}})
        cache = ec.describe_serverless_caches(ServerlessCacheName=name)["ServerlessCaches"][0]
        assert cache["SnapshotRetentionLimit"] == 3
        assert cache["DailySnapshotTime"] == "09:00"
        assert "CacheUsageLimits" not in cache

        for kwargs, code in (
            ({}, "InvalidParameterCombination"),
            ({"UserGroupId": "missing-group", "RemoveUserGroup": True}, "InvalidParameterCombination"),
            ({"UserGroupId": "missing-group"}, "UserGroupNotFound"),
        ):
            with pytest.raises(ClientError) as exc:
                ec.modify_serverless_cache(ServerlessCacheName=name, **kwargs)
            assert exc.value.response["Error"]["Code"] == code
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)
    with pytest.raises(ClientError) as exc:
        ec.modify_serverless_cache(ServerlessCacheName=name, Description="gone")
    assert exc.value.response["Error"]["Code"] == "ServerlessCacheNotFoundFault"


def test_serverless_cache_describe_pages(ec):
    names = [f"sls-page-{_uid()}" for _ in range(3)]
    for name in names:
        ec.create_serverless_cache(ServerlessCacheName=name, Engine="valkey")
    try:
        seen, token = [], None
        while True:
            kwargs = {"MaxResults": 2, **({"NextToken": token} if token else {})}
            page = ec.describe_serverless_caches(**kwargs)
            assert len(page["ServerlessCaches"]) <= 2
            seen += [c["ServerlessCacheName"] for c in page["ServerlessCaches"]]
            token = page.get("NextToken")
            if not token:
                break
        assert set(names) <= set(seen)
        assert len(seen) == len(set(seen))
    finally:
        for name in names:
            ec.delete_serverless_cache(ServerlessCacheName=name)


@pytest.mark.data_plane
def test_serverless_cache_serves_tls_only(ec, tmp_path):
    """Serverless caches have no plaintext listener; a client that trusts
    MiniStack's ElastiCache CA connects with TLS and verifies the hostname."""
    import socket
    ca_file = _serverless_ca_file(tmp_path)
    name = f"sls-tls-{_uid()}"
    ec.create_serverless_cache(ServerlessCacheName=name, Engine="valkey")
    try:
        cache = _wait_serverless(ec, name)
        assert cache["Status"] == "available"
        endpoint = cache["Endpoint"]
        assert cache["ReaderEndpoint"] == endpoint
        assert _tls_command(endpoint, ca_file, "PING") == b"+PONG\r\n"
        assert b"valkey_version:8.1" in _tls_command(endpoint, ca_file, "INFO server")

        with socket.create_connection((endpoint["Address"], endpoint["Port"]), timeout=10) as raw:
            raw.sendall(b"*1\r\n$4\r\nPING\r\n")
            raw.settimeout(5)
            try:
                reply = raw.recv(64)
            except (ConnectionResetError, socket.timeout):
                reply = b""
        assert b"PONG" not in reply
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)
    with pytest.raises(OSError):
        _tls_command(endpoint, ca_file, "PING")


@pytest.mark.data_plane
def test_serverless_cache_is_one_cluster_mode_shard(ec, tmp_path):
    """AWS serves a serverless cache in cluster mode: keys in different slots
    fail together with CROSSSLOT, and CLUSTER SLOTS names the cache endpoint."""
    ca_file = _serverless_ca_file(tmp_path)
    name = f"sls-slots-{_uid()}"
    ec.create_serverless_cache(ServerlessCacheName=name, Engine="valkey")
    try:
        endpoint = _wait_serverless(ec, name)["Endpoint"]
        assert _tls_command(endpoint, ca_file, "MGET {a}x {b}y").startswith(b"-CROSSSLOT")
        assert _tls_command(endpoint, ca_file, "MGET {a}x {a}y") == b"*2\r\n$-1\r\n$-1\r\n"
        address = endpoint["Address"]
        assert _tls_command(endpoint, ca_file, "CLUSTER SLOTS").startswith(
            f"*1\r\n*3\r\n:0\r\n:16383\r\n*4\r\n${len(address)}\r\n{address}\r\n:{endpoint['Port']}\r\n".encode())
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)


@pytest.mark.data_plane
def test_serverless_cache_engine_change_replaces_container_on_same_endpoint(ec, tmp_path):
    ca_file = _serverless_ca_file(tmp_path)
    name = f"sls-eng-{_uid()}"
    ec.create_serverless_cache(ServerlessCacheName=name, Engine="redis")
    try:
        before = _wait_serverless(ec, name)
        assert b"redis_version:" in _tls_command(before["Endpoint"], ca_file, "INFO server")
        assert b"valkey_version" not in _tls_command(before["Endpoint"], ca_file, "INFO server")

        modified = ec.modify_serverless_cache(ServerlessCacheName=name, Engine="valkey",
                                              MajorEngineVersion="8")["ServerlessCache"]
        assert modified["Status"] == "modifying"
        with pytest.raises(ClientError) as exc:
            ec.modify_serverless_cache(ServerlessCacheName=name, Description="busy")
        assert exc.value.response["Error"]["Code"] == "InvalidServerlessCacheStateFault"

        after = _wait_serverless(ec, name)
        assert (after["Status"], after["Engine"], after["FullEngineVersion"]) == ("available", "valkey", "8.1")
        if not os.environ.get("DOCKER_NETWORK"):
            assert after["Endpoint"] == before["Endpoint"]
        assert b"valkey_version:8.1" in _tls_command(after["Endpoint"], ca_file, "INFO server")
    finally:
        ec.delete_serverless_cache(ServerlessCacheName=name)



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
@pytest.mark.data_plane
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

    def fake_spawn(name, engine, engine_version, labels, **_):
        spawned.append({"name": name, "engine": engine,
                         "engine_version": engine_version, "labels": labels})
        return ("ministack-elasticache", 6379, f"cid-{name}")

    monkeypatch.setattr(_ec, "_spawn_redis_container", fake_spawn)

    _ec.load_persisted_state({
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
                          lambda name, engine, engine_version, labels, **_: ("rg-host", 6379, f"cid-{name}"))

    _ec.load_persisted_state({
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


def test_elasticache_restore_state_reports_the_respawned_replication_group_address(monkeypatch):
    """A respawned node group container has a new address; the group's endpoints report it (#2521)."""
    from ministack.core.responses import set_request_account_id
    from ministack.services import elasticache as _ec
    set_request_account_id("000000000000")
    _ec.reset()
    monkeypatch.setattr(_ec, "_spawn_redis_container",
                        lambda name, engine, engine_version, labels, **_: ("172.18.0.9", 6379, f"cid-{name}"))
    old = {"Address": "172.18.0.4", "Port": 6379}
    _ec.load_persisted_state({
        "replication_groups": {
            "rg-ep": {
                "ReplicationGroupId": "rg-ep", "Engine": "valkey", "EngineVersion": "8.1",
                "ConfigurationEndpoint": dict(old),
                "NodeGroups": [{"NodeGroupId": "0001", "PrimaryEndpoint": dict(old), "ReaderEndpoint": dict(old),
                                "NodeGroupMembers": [{"CacheClusterId": "rg-ep-0001-001", "ReadEndpoint": dict(old)}]}],
                "_docker_container_ids": ["dead-cid"],
            },
        },
    })
    _ec._ensure_live_containers()
    rg = _ec._replication_groups["rg-ep"]
    new = {"Address": "172.18.0.9", "Port": 6379}
    ng = rg["NodeGroups"][0]
    assert ng["PrimaryEndpoint"] == ng["ReaderEndpoint"] == ng["NodeGroupMembers"][0]["ReadEndpoint"] == new
    assert rg["ConfigurationEndpoint"] == new


def test_elasticache_restore_state_respawns_same_names_per_region(monkeypatch):
    from ministack.core.responses import AccountRegionScopedDict
    from ministack.services import elasticache as _ec

    _ec.reset()
    spawned = []

    def fake_spawn(name, engine, engine_version, labels, **_):
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

    _ec.load_persisted_state({
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

    _ec.load_persisted_state({
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


# ---------------------------------------------------------------------------
# Deferred (cold-image) provisioning — state, not liveness
# ---------------------------------------------------------------------------

@pytest.mark.serial
def test_elasticache_cold_image_publishes_the_real_endpoint(fake_docker, monkeypatch):
    """Describe must report the port the container actually listens on.

    When the engine image is not cached, CreateCacheCluster returns at once and
    a background finisher starts the container. The finisher wrote
    `ConfigurationEndpoint` while every Describe path reads `_endpoint` and
    `CacheNodes[].Endpoint` — which create had filled with the *engine default*
    port, no container existing yet. So a cold-image create reported
    `localhost:6379` for the rest of its life while the container was on the
    allocated host port, and never self-corrected. Reported by @Areson.
    """
    import threading

    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import elasticache as ec

    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    monkeypatch.setattr(ec, "_get_docker", lambda: fake_docker)
    monkeypatch.setattr(ec, "_image_is_local", lambda *_a: False)
    monkeypatch.setattr(ec, "_spawn_redis_container",
                        lambda **kw: ("localhost", 16379, "cid-real"))

    done = threading.Event()
    orig = ec.spawn_background

    def tracked(fn, *a, **kw):
        t = orig(fn, *a, **kw)
        threading.Thread(target=lambda: (t.join(), done.set()), daemon=True).start()
        return t
    monkeypatch.setattr(ec, "spawn_background", tracked)

    ec._create_cache_cluster({"CacheClusterId": ["cold-1"], "Engine": ["redis"],
                              "CacheNodeType": ["cache.t3.micro"], "NumCacheNodes": ["1"]})
    assert done.wait(timeout=10), "deferred finisher never completed"

    rec = ec._clusters["cold-1"]
    assert rec["CacheClusterStatus"] == "available"
    assert rec["_endpoint"] == {"Address": "localhost", "Port": 16379}, (
        f"Describe would report {rec['_endpoint']} — nothing is listening there"
    )
    assert rec["CacheNodes"][0]["Endpoint"]["Port"] == 16379


@pytest.mark.serial
def test_elasticache_delete_during_deferred_start_leaves_no_container(fake_docker, monkeypatch):
    """A delete while the container is being started must not strand it.

    The finisher checked the record existed and then published the container id
    into it. A delete in that gap removed the record, found no id to tear down,
    and the container ran on referenced by nothing — unreachable by any API
    call. Reported by @Areson.
    """
    import threading

    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import elasticache as ec

    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    monkeypatch.setattr(ec, "_get_docker", lambda: fake_docker)
    monkeypatch.setattr(ec, "_image_is_local", lambda *_a: False)

    spawned, release, finished = threading.Event(), threading.Event(), threading.Event()

    def fake_spawn(**kw):
        name = kw.get("name")
        fake_docker.containers.run(name=name, labels=kw.get("labels"))
        spawned.set()
        release.wait(timeout=10)          # hold inside the provisioning window
        return "localhost", 16380, f"cid-{name}"
    monkeypatch.setattr(ec, "_spawn_redis_container", fake_spawn)

    orig = ec.spawn_background

    def tracked(fn, *a, **kw):
        t = orig(fn, *a, **kw)
        threading.Thread(target=lambda: (t.join(), finished.set()), daemon=True).start()
        return t
    monkeypatch.setattr(ec, "spawn_background", tracked)

    ec._create_cache_cluster({"CacheClusterId": ["race-1"], "Engine": ["redis"],
                              "CacheNodeType": ["cache.t3.micro"], "NumCacheNodes": ["1"]})
    assert spawned.wait(timeout=10), "container was never started"

    ec._delete_cache_cluster({"CacheClusterId": ["race-1"]})
    release.set()
    assert finished.wait(timeout=10), "finisher never completed"

    assert "race-1" not in ec._clusters
    assert fake_docker.live() == [], (
        f"leaked container(s) {fake_docker.live()} — running, and no API call can reach them"
    )


@pytest.mark.data_plane
def test_elasticache_cluster_create_time_present_and_parsed(ec):
    """DescribeCacheClusters emits CacheClusterCreateTime as an ISO8601 TStamp
    (it was stored as a float epoch and previously absent from the XML)."""
    import datetime

    ec.create_cache_cluster(
        CacheClusterId="cluster-createtime",
        Engine="redis",
        CacheNodeType="cache.t3.micro",
        NumCacheNodes=1,
    )
    try:
        cluster = ec.describe_cache_clusters(
            CacheClusterId="cluster-createtime")["CacheClusters"][0]
        assert isinstance(cluster.get("CacheClusterCreateTime"), datetime.datetime)
    finally:
        ec.delete_cache_cluster(CacheClusterId="cluster-createtime")


# ---------------------------------------------------------------------------
# Serverless caches — provisioning state, not liveness
# ---------------------------------------------------------------------------

def test_serverless_announce_flags_skip_a_hostname_redis_refuses():
    assert elasticache._serverless_announce_flags("localhost", 26500) == (
        "--cluster-announce-tls-port 26500 --cluster-announce-hostname localhost "
        "--cluster-preferred-endpoint-type hostname")
    assert elasticache._serverless_announce_flags("ministack_app_1", 26500) == "--cluster-announce-tls-port 26500"


def _serverless_create_params(name, engine="valkey"):
    return {"ServerlessCacheName": [name], "Engine": [engine]}


@pytest.mark.serial
def test_serverless_cache_without_docker_points_at_sidecar(monkeypatch):
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import elasticache as ec

    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    ec.reset()
    monkeypatch.setattr(ec, "_get_docker", lambda: None)
    status, _headers, _body = ec._create_serverless_cache(_serverless_create_params("nodocker"))
    assert status == 200
    rec = ec._serverless_caches["nodocker"]
    assert rec["Status"] == "available"
    assert rec["Endpoint"] == {"Address": ec.REDIS_DEFAULT_HOST, "Port": ec.REDIS_DEFAULT_PORT}
    ec.reset()


@pytest.mark.serial
def test_serverless_cache_deleted_while_provisioning_leaves_no_container(fake_docker, monkeypatch):
    """A container that comes up after its cache was deleted is removed by the
    finisher itself: the delete had nothing to find, by id or by name."""
    import threading

    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import elasticache as ec

    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    ec.reset()
    monkeypatch.setattr(ec, "_get_docker", lambda: fake_docker)

    started, release, finished = threading.Event(), threading.Event(), threading.Event()

    def fake_spawn(name, engine, full_version, host_port, labels, wait=True):
        started.set()
        release.wait(timeout=10)
        container = fake_docker.containers.run(name=name, labels=labels)
        return "localhost", host_port, container.id
    monkeypatch.setattr(ec, "_spawn_serverless_container", fake_spawn)

    orig = ec.spawn_background

    def tracked(fn, *a, **kw):
        t = orig(fn, *a, **kw)
        threading.Thread(target=lambda: (t.join(), finished.set()), daemon=True).start()
        return t
    monkeypatch.setattr(ec, "spawn_background", tracked)

    ec._create_serverless_cache(_serverless_create_params("race"))
    assert started.wait(timeout=10)
    ec._delete_serverless_cache({"ServerlessCacheName": ["race"]})
    release.set()
    assert finished.wait(timeout=10)

    assert "race" not in ec._serverless_caches
    assert fake_docker.live() == [], f"leaked container(s) {fake_docker.live()}"


@pytest.mark.serial
def test_serverless_cache_failed_start_reports_create_failed(fake_docker, monkeypatch):
    import threading

    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import elasticache as ec

    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    ec.reset()
    monkeypatch.setattr(ec, "_get_docker", lambda: fake_docker)

    def boom(**_kw):
        raise RuntimeError("valkey never answered a TLS PING")
    monkeypatch.setattr(ec, "_spawn_serverless_container", boom)
    finished = threading.Event()
    orig = ec.spawn_background

    def tracked(fn, *a, **kw):
        t = orig(fn, *a, **kw)
        threading.Thread(target=lambda: (t.join(), finished.set()), daemon=True).start()
        return t
    monkeypatch.setattr(ec, "spawn_background", tracked)

    ec._create_serverless_cache(_serverless_create_params("broken"))
    assert finished.wait(timeout=10)
    assert ec._serverless_caches["broken"]["Status"] == "create-failed"
    assert "Endpoint" not in ec._serverless_caches["broken"]
    ec.reset()


def test_serverless_cache_persists_and_respawns_on_the_same_port(monkeypatch):
    from ministack.services import elasticache as ec

    ec.reset()
    record = {
        "ServerlessCacheName": "kept", "Status": "available", "Engine": "valkey",
        "MajorEngineVersion": "8", "FullEngineVersion": "8.1", "CreateTime": time.time(),
        "ARN": "arn:aws:elasticache:us-west-2:000000000000:serverlesscache:kept",
        "Endpoint": {"Address": "localhost", "Port": 16400},
        "_host_port": 16400, "_docker_container_id": "dead",
    }
    ec._serverless_caches.set_scoped("000000000000", "us-west-2", "kept", record)
    state = ec.get_state()
    persisted = state["serverless_caches"].get_scoped("000000000000", "us-west-2", "kept")
    assert "_docker_container_id" not in persisted
    ec.reset()

    spawned = []

    def fake_spawn(name, engine, full_version, host_port, labels, wait=True):
        spawned.append((name, engine, full_version, host_port, labels["region"], wait))
        return "localhost", host_port, "cid-new"
    monkeypatch.setattr(ec, "_spawn_serverless_container", fake_spawn)
    monkeypatch.setattr(ec, "_get_docker", lambda: object())

    ec.load_persisted_state(state)
    ec._ensure_live_containers()
    assert spawned == [("ministack-elasticache-serverless-000000000000-us-west-2-kept",
                        "valkey", "8.1", 16400, "us-west-2", False)]
    restored = ec._serverless_caches.get_scoped("000000000000", "us-west-2", "kept")
    assert restored["_docker_container_id"] == "cid-new"
    assert restored["Endpoint"] == {"Address": "localhost", "Port": 16400}
    assert "cid-new" in ec._live_container_ids()
    ec.reset()


def test_serverless_ca_persists_across_restart(monkeypatch):
    from ministack.services import elasticache as ec

    pem = ec.serverless_ca_cert_pem()
    state = ec.get_state()
    assert state["serverless_ca"]["ca_cert_pem"] == pem

    monkeypatch.setattr(ec, "_serverless_ca", None)
    ec.load_persisted_state(state)
    assert ec.serverless_ca_cert_pem() == pem


def test_replication_group_auth_token_needs_transit_encryption(ec):
    rg_id = f"auth-{_uid()}"
    for kwargs, code, message in (
        ({"TransitEncryptionEnabled": False}, "InvalidParameterValue",
         "The AUTH token is only supported when encryption-in-transit is enabled"),
        ({"TransitEncryptionEnabled": True, "TransitEncryptionMode": "preferred"}, "InvalidParameterCombination",
         "Transit encryption preferred is not supported with access control enabled clusters."),
    ):
        with pytest.raises(ClientError) as exc:
            ec.create_replication_group(ReplicationGroupId=rg_id, ReplicationGroupDescription="t", Engine="redis",
                                        NumCacheClusters=1, AuthToken="a-very-long-auth-token-123", **kwargs)
        assert exc.value.response["Error"]["Code"] == code
        assert exc.value.response["Error"]["Message"] == message

    plain = ec.create_replication_group(ReplicationGroupId=rg_id, ReplicationGroupDescription="t",
                                        Engine="redis", NumCacheClusters=1)["ReplicationGroup"]
    try:
        assert plain["TransitEncryptionEnabled"] is False and "TransitEncryptionMode" not in plain
        member = ec.describe_cache_clusters(CacheClusterId=plain["MemberClusters"][0])["CacheClusters"][0]
        assert (member["AuthTokenEnabled"], member["TransitEncryptionEnabled"], member["AtRestEncryptionEnabled"]) == (
            False, False, False)
        with pytest.raises(ClientError) as exc:
            ec.modify_replication_group(ReplicationGroupId=rg_id, AuthToken="a-very-long-auth-token-123",
                                        AuthTokenUpdateStrategy="ROTATE")
        assert exc.value.response["Error"]["Message"] == (
            "The AUTH token modification is only supported when encryption-in-transit is enabled")
    finally:
        ec.delete_replication_group(ReplicationGroupId=rg_id)


def _redis_session(endpoint, ca_file, *commands):
    import socket
    import ssl
    ctx = ssl.create_default_context(cafile=ca_file)
    deadline = time.time() + 30
    while True:
        try:
            with socket.create_connection((endpoint["Address"], endpoint["Port"]), timeout=5) as raw:
                with ctx.wrap_socket(raw, server_hostname=endpoint["Address"]) as conn:
                    replies = []
                    for parts in commands:
                        conn.sendall(f"*{len(parts)}\r\n".encode()
                                     + b"".join(f"${len(p)}\r\n{p}\r\n".encode() for p in parts))
                        replies.append(conn.recv(65536))
                    return replies
        except (OSError, ssl.SSLError):
            if time.time() > deadline:
                raise
            time.sleep(0.3)


@pytest.mark.data_plane
def test_replication_group_serves_tls_and_requires_auth_token(ec, tmp_path):
    rg_id = f"tls-{_uid()}"
    first, second = "first-auth-token-0123", "second-auth-token-4567"
    rg = ec.create_replication_group(ReplicationGroupId=rg_id, ReplicationGroupDescription="t", Engine="redis",
                                     NumCacheClusters=1, TransitEncryptionEnabled=True,
                                     AtRestEncryptionEnabled=True, AuthToken=first)["ReplicationGroup"]
    try:
        assert (rg["TransitEncryptionEnabled"], rg["TransitEncryptionMode"], rg["AuthTokenEnabled"]) == (
            True, "required", True)
        member = ec.describe_cache_clusters(CacheClusterId=rg["MemberClusters"][0])["CacheClusters"][0]
        assert (member["TransitEncryptionEnabled"], member["TransitEncryptionMode"],
                member["AuthTokenEnabled"], member["AtRestEncryptionEnabled"]) == (True, "required", True, True)

        endpoint = rg["NodeGroups"][0]["PrimaryEndpoint"]
        ca_file = _serverless_ca_file(tmp_path)
        assert _redis_session(endpoint, ca_file, ["PING"])[0].startswith(b"-NOAUTH")
        assert _redis_session(endpoint, ca_file, ["AUTH", first], ["PING"]) == [b"+OK\r\n", b"+PONG\r\n"]

        ec.modify_replication_group(ReplicationGroupId=rg_id, AuthToken=second,
                                    AuthTokenUpdateStrategy="ROTATE", ApplyImmediately=True)
        for token in (first, second):
            assert _redis_session(endpoint, ca_file, ["AUTH", token])[0] == b"+OK\r\n"
        ec.modify_replication_group(ReplicationGroupId=rg_id, AuthToken=second,
                                    AuthTokenUpdateStrategy="SET", ApplyImmediately=True)
        assert _redis_session(endpoint, ca_file, ["AUTH", first])[0].startswith(b"-WRONGPASS")
        assert _redis_session(endpoint, ca_file, ["AUTH", second])[0] == b"+OK\r\n"
    finally:
        ec.delete_replication_group(ReplicationGroupId=rg_id)


def test_elasticache_host_port_skips_a_port_already_taken():
    """#2456: a host port another process holds is skipped, so the container's docker run does not
    fail with "port is already allocated"."""
    import socket

    from ministack.services import elasticache as ec_mod

    holder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    holder.bind(("0.0.0.0", 0))
    holder.listen(1)
    taken = holder.getsockname()[1]
    saved = ec_mod._port_counter[0]
    try:
        ec_mod._port_counter[0] = taken
        port = ec_mod._next_host_port()
        assert port != taken
        assert port > taken
        assert ec_mod._port_counter[0] == port + 1
    finally:
        ec_mod._port_counter[0] = saved
        holder.close()


@pytest.mark.parametrize("engine,minimum", [("valkey", "7.2"), ("redis", "7.0")])
def test_elasticache_iam_user_follows_aws(ec, engine, minimum):
    """#2458, captured on AWS: an iam user keeps Type iam with no PasswordCount, its MinimumEngineVersion,
    and AWS's InvalidParameterCombination errors."""
    uid = f"iam-{engine}-{_uuid_mod.uuid4().hex[:6]}"
    created = ec.create_user(UserId=uid, UserName=uid, Engine=engine, AccessString="on ~* +@all",
                             AuthenticationMode={"Type": "iam"})
    try:
        assert created["Authentication"] == {"Type": "iam"}
        assert created["MinimumEngineVersion"] == minimum
        described = ec.describe_users(UserId=uid)["Users"][0]
        assert (described["Authentication"], described["MinimumEngineVersion"]) == ({"Type": "iam"}, minimum)
    finally:
        ec.delete_user(UserId=uid)
    for kwargs, message in (
            ({"UserName": f"{uid}-other", "AuthenticationMode": {"Type": "iam"}},
             "User Id and User name must be same for authentication type: iam"),
            ({"UserName": uid, "AuthenticationMode": {"Type": "iam", "Passwords": ["a-long-enough-password-1"]}},
             "Password field is not allowed with authentication type: iam")):
        with pytest.raises(ClientError) as exc:
            ec.create_user(UserId=uid, Engine=engine, AccessString="on ~* +@all", **kwargs)
        err = exc.value.response["Error"]
        assert (err["Code"], err["Message"]) == ("InvalidParameterCombination", message)


def test_elasticache_user_modes_and_modify_follow_aws(ec):
    """#2458: no-password-required is refused for Valkey, a password user reports its PasswordCount,
    and ModifyUser to iam answers with the user modifying."""
    with pytest.raises(ClientError) as exc:
        ec.create_user(UserId=f"nopw-{_uuid_mod.uuid4().hex[:6]}", UserName="n", Engine="valkey",
                       AccessString="on ~* +@all", AuthenticationMode={"Type": "no-password-required"})
    assert exc.value.response["Error"]["Message"] == "No-password-required is not allowed for a user with engine Valkey"
    uid = f"pw-{_uuid_mod.uuid4().hex[:6]}"
    created = ec.create_user(UserId=uid, UserName=uid, Engine="valkey", AccessString="on ~* +@all",
                             AuthenticationMode={"Type": "password", "Passwords": ["a-long-enough-password-1"]})
    try:
        assert created["Authentication"] == {"Type": "password", "PasswordCount": 1}
        modified = ec.modify_user(UserId=uid, AuthenticationMode={"Type": "iam"})
        assert (modified["Status"], modified["Authentication"]) == ("modifying", {"Type": "iam"})
        assert ec.describe_users(UserId=uid)["Users"][0]["Authentication"] == {"Type": "iam"}
    finally:
        ec.delete_user(UserId=uid)


# Real AWS (us-east-2, issue #2551): each captured user's CreateUser, ModifyUser and DescribeUsers calls in
# order, with AWS's answers. Built from the raw captures; a user's calls stop before one MiniStack doesn't model.
def _append_captures():
    """Expand the compact fixture: a step is ["c", create], [append, expect], ["m", params, expect] or
    ["d", expect], and an integer expect is an index into "errors"."""
    with open(os.path.join(os.path.dirname(__file__), "fixtures", "elasticache", "append_access_string.json")) as f:
        data = json.load(f)
    def step(row):
        if row[0] == "c":
            return "c", data["creates"][row[1]], None
        op, params, expect = (("d", None, row[1]) if row[0] == "d" else ("m", row[1], row[2]) if row[0] == "m"
                              else ("m", {"AppendAccessString": row[0]}, row[1]))
        return op, params, data["errors"][expect] if isinstance(expect, int) else expect
    return [{"id": case_id, "steps": [step(row) for row in rows]} for case_id, rows in data["cases"]]


_APPEND_CAPTURES = _append_captures()


@pytest.mark.parametrize("case", _APPEND_CAPTURES, ids=[c["id"] for c in _APPEND_CAPTURES])
def test_elasticache_modify_user_append_replays_aws(ec, case):
    """Each step is sent as captured and answered as AWS answered: the error's status, code and message, or
    the user's Status, AccessString, MinimumEngineVersion and Authentication."""
    uid = f"app-{_uuid_mod.uuid4().hex[:8]}"
    calls = {"c": ec.create_user, "m": ec.modify_user, "d": ec.describe_users}
    created = False
    try:
        for op, params, expect in case["steps"]:
            kwargs = {"UserId": uid, **({"UserName": uid} if op == "c" else {}), **(params or {})}
            if expect and isinstance(expect[0], int):
                with pytest.raises(ClientError) as exc:
                    calls[op](**kwargs)
                response = exc.value.response
                assert [response["ResponseMetadata"]["HTTPStatusCode"], response["Error"]["Code"],
                        response["Error"]["Message"]] == expect
                continue
            answer = calls[op](**kwargs)
            created = created or op == "c"
            if op == "c":
                continue
            user = answer["Users"][0] if op == "d" else answer
            want = {"Status": "modifying" if op == "m" else "active", "AccessString": expect[0],
                    "MinimumEngineVersion": expect[1], "Authentication": expect[2] if len(expect) > 2 else {"Type": "iam"}}
            if want["Authentication"] is None:  # no-password: AWS omits PasswordCount, MiniStack writes 0
                del want["Authentication"]
                assert user["Authentication"]["Type"] == "no-password"
            assert {k: user.get(k) for k in want} == want
    finally:
        if created:
            ec.delete_user(UserId=uid)


def test_elasticache_modify_user_append_skips_stored_rules_aws_refuses(ec):
    """MiniStack contract, not AWS behavior: a stored AccessString can hold rules AWS would have refused
    (`reset` here), since CreateUser doesn't check them all. An append skips them instead of failing."""
    uid = f"app-{_uuid_mod.uuid4().hex[:6]}"
    ec.create_user(UserId=uid, UserName=uid, Engine="valkey", AccessString="reset on ~* +@all",
                   AuthenticationMode={"Type": "iam"})
    try:
        assert ec.modify_user(UserId=uid, AppendAccessString="-flushall")["AccessString"] == "on ~* +@all -flushall"
    finally:
        ec.delete_user(UserId=uid)


def test_elasticache_no_password_redis_user_reports_engine_floor(ec):
    """AWS reports 6.0 for a no-password Redis user (us-east-2, issue #2551, round22 create-nopass-get)."""
    uid = f"nopass-{_uuid_mod.uuid4().hex[:8]}"
    user = ec.create_user(UserId=uid, UserName=uid, Engine="redis", AccessString="on ~* -@all",
                          AuthenticationMode={"Type": "no-password-required"})
    try:
        assert user["MinimumEngineVersion"] == "6.0"
        assert user["Authentication"]["Type"] == "no-password"
    finally:
        ec.delete_user(UserId=uid)


def test_elasticache_modify_user_append_refuses_control_characters(ec):
    """MiniStack contract: XML can't carry a control character, so an append holding one is refused and
    the user keeps its access string. (AWS stores `~a\\x1cb` and then answers InternalFailure, round 23.)"""
    uid = f"ctrl-{_uuid_mod.uuid4().hex[:8]}"
    ec.create_user(UserId=uid, UserName=uid, Engine="valkey", AccessString="on ~x +@all",
                   AuthenticationMode={"Type": "iam"})
    try:
        for append in ("~a\x1cb", "&a\x01b", "(+get|\x01)", "~a\ufffeb"):
            with pytest.raises(ClientError) as exc:
                ec.modify_user(UserId=uid, AppendAccessString=append)
            assert exc.value.response["Error"]["Message"] == "The access-string is invalid."
        assert ec.describe_users(UserId=uid)["Users"][0]["AccessString"] == "on ~x +@all"
    finally:
        ec.delete_user(UserId=uid)


def test_elasticache_modify_user_access_string_and_append_together(ec):
    """AccessString with AppendAccessString is refused and the user keeps its rules
    (AWS captures round1 modify-both, describe-both); an unknown user is reported first
    (round2 modify-both-missing)."""
    uid = f"app-{_uuid_mod.uuid4().hex[:6]}"
    ec.create_user(UserId=uid, UserName=uid, Engine="valkey", AccessString="on ~* +@all",
                   AuthenticationMode={"Type": "iam"})
    try:
        with pytest.raises(ClientError) as exc:
            ec.modify_user(UserId=uid, AccessString="on ~* +get", AppendAccessString="-get")
        err = exc.value.response["Error"]
        assert (exc.value.response["ResponseMetadata"]["HTTPStatusCode"], err["Code"], err["Message"]) == (
            400, "InvalidParameterCombination",
            "Both AccessString and AppendAccessString provided together. Pass either one of them.")
        assert ec.describe_users(UserId=uid)["Users"][0]["AccessString"] == "on ~* +@all"
        with pytest.raises(ClientError) as exc:  # round8 modify-both-spaces: an append of only spaces counts
            ec.modify_user(UserId=uid, AccessString="on ~* +get", AppendAccessString="   ")
        assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
        with pytest.raises(ClientError) as exc:
            ec.modify_user(UserId=f"{uid}-nosuch", AccessString="on ~* +get", AppendAccessString="-get")
        assert (exc.value.response["ResponseMetadata"]["HTTPStatusCode"],
                exc.value.response["Error"]["Code"]) == (404, "UserNotFound")
    finally:
        ec.delete_user(UserId=uid)


def test_elasticache_user_group_changes_answer_as_aws(ec):
    """Real AWS (us-west-2): access strings are reported with `-@all`, user-group changes answer pending,
    a group's default user cannot be deleted, and UserGroupIds is absent once the last group is detached."""
    uid, pw = _uid(), "a-long-enough-password-1"
    default, app, group, rg_id = f"d-{uid}", f"a-{uid}", f"g-{uid}", f"rg-{uid}"
    ec.create_user(UserId=default, UserName="default", Engine="valkey", AccessString="off -@all", Passwords=[pw])
    ec.create_user(UserId=app, UserName=app, Engine="valkey", AccessString="on ~* +@all", Passwords=[pw])
    ec.create_user_group(UserGroupId=group, Engine="valkey", UserIds=[default, app])
    ec.create_replication_group(ReplicationGroupId=rg_id, ReplicationGroupDescription="ug", Engine="valkey",
                                CacheNodeType="cache.t4g.micro", TransitEncryptionEnabled=True, UserGroupIds=[group])
    try:
        assert ec.modify_user(UserId=app, AccessString="on ~app:* +@read")["AccessString"] == "on ~app:* -@all +@read"
        removed = ec.modify_user_group(UserGroupId=group, UserIdsToRemove=[app])
        assert (removed["Status"], removed["UserIds"], removed["PendingChanges"]) == (
            "modifying", [default, app], {"UserIdsToRemove": [app]})
        with pytest.raises(ClientError) as exc:
            ec.delete_user(UserId=default)
        assert exc.value.response["Error"]["Code"] == "DefaultUserAssociatedToUserGroup"
        detached = ec.modify_replication_group(ReplicationGroupId=rg_id, UserGroupIdsToRemove=[group],
                                               ApplyImmediately=True)["ReplicationGroup"]
        assert (detached["Status"], detached["UserGroupIds"], detached["PendingModifiedValues"]) == (
            "modifying", [group], {"UserGroups": {"UserGroupIdsToAdd": [], "UserGroupIdsToRemove": [group]}})
        assert "UserGroupIds" not in ec.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]
        # A group still attached cannot be deleted; a user in it can (real AWS, us-west-2).
        ec.modify_replication_group(ReplicationGroupId=rg_id, UserGroupIdsToAdd=[group], ApplyImmediately=True)
        with pytest.raises(ClientError) as exc:
            ec.delete_user_group(UserGroupId=group)
        assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
            "InvalidUserGroupState", f"User group {group} can't be deleted because it is still associated "
            "with at least 1 replication group or serverless cache.")
        assert ec.delete_user(UserId=app)["Status"] == "deleting"
        ec.modify_replication_group(ReplicationGroupId=rg_id, UserGroupIdsToRemove=[group], ApplyImmediately=True)
    finally:
        ec.delete_replication_group(ReplicationGroupId=rg_id)
        ec.delete_user_group(UserGroupId=group)
        ec.delete_user(UserId=default)


def test_elasticache_user_access_string_validated_as_aws(ec):
    """Real AWS (us-west-2, Valkey 8.1): unknown commands, categories and syntax are refused; subcommands are not checked."""
    uid = _uid()
    for access, message in (
            ("on ~* +doesnotexist", "Access string contains invalid command(s). "
                                    "Please verify the list of available commands in user-api."),
            ("on ~* +@notacategory", "Access string contains invalid category(s). "
                                     "Please verify the list of available category in user-api."),
            ("garbage", "The access-string is invalid.")):
        with pytest.raises(ClientError) as exc:
            ec.create_user(UserId=f"bad-{uid}", UserName=f"bad-{uid}", Engine="valkey", AccessString=access,
                           Passwords=["a-long-enough-password-1"])
        assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
            "InvalidParameterValue", message)
    ec.create_user(UserId=f"ok-{uid}", UserName=f"ok-{uid}", Engine="valkey", AccessString="on ~* +get|nosub",
                   Passwords=["a-long-enough-password-1"])
    try:
        assert ec.describe_users(UserId=f"ok-{uid}")["Users"][0]["AccessString"] == "on ~* -@all +get|nosub"
        with pytest.raises(ClientError) as exc:
            ec.modify_user(UserId=f"ok-{uid}", AccessString="on ~* (+doesnotexist)")
        assert "invalid command(s)" in exc.value.response["Error"]["Message"]
        assert ec.modify_user(UserId=f"ok-{uid}", AccessString="on ~* +@json +bf.add +hgetex")["AccessString"] == (
            "on ~* -@all +@json +bf.add +hgetex")
    finally:
        ec.delete_user(UserId=f"ok-{uid}")


@requires_docker
@pytest.mark.data_plane
def test_elasticache_replication_group_user_groups_reach_the_engine(ec, tmp_path):
    """A replication group's user group is configured on its engine and follows every change: default is
    off without a default user, application users cannot change users, and detaching restores default."""
    uid, pw = _uid(), "a-long-enough-password-1"
    app, group, rg_id = f"a-{uid}", f"g-{uid}", f"rg-{uid}"
    ec.create_user(UserId=app, UserName=app, Engine="valkey", AccessString="on ~* +@all (~* +@all)", Passwords=[pw])
    ec.create_user_group(UserGroupId=group, Engine="valkey", UserIds=[app])
    ec.create_replication_group(ReplicationGroupId=rg_id, ReplicationGroupDescription="ug", Engine="valkey",
                                CacheNodeType="cache.t4g.micro", TransitEncryptionEnabled=True, UserGroupIds=[group])
    try:
        ep = ec.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]["NodeGroups"][0]["PrimaryEndpoint"]
        ca = _serverless_ca_file(tmp_path)
        ok, deadline = b"+OK\r\n", time.time() + 30
        while True:
            try:
                unauthenticated = _tls_session(ep, ca, ("SET", "k", "v"))[0]
                break
            except OSError:  # still starting
                assert time.time() < deadline
                time.sleep(0.5)
        assert unauthenticated.startswith(b"-NOAUTH")
        assert _tls_session(ep, ca, ("AUTH", app, "wrong-password-123456"))[0].startswith(b"-WRONGPASS")
        assert _tls_session(ep, ca, ("AUTH", app, pw), ("SET", "k", "v")) == [ok, ok]
        # No channels outside its selectors, as on AWS, for an access string with a selector.
        assert _tls_session(ep, ca, ("AUTH", app, pw), ("PUBLISH", "ch", "x"))[1].startswith(b"-NOPERM")
        took = _tls_session(ep, ca, ("AUTH", app, pw), ("ACL", "SETUSER", "default", "on", "nopass", "+@all"))
        assert took[1].startswith(b"-NOPERM")
        # Only the group's users and `default off`, as on AWS (real AWS, us-west-2, Valkey 8.1).
        users, acl = _tls_session(ep, ca, ("AUTH", app, pw), ("ACL", "USERS"), ("ACL", "LIST"))[1:]
        assert users == f"*2\r\n${len(app)}\r\n{app}\r\n$7\r\ndefault\r\n".encode()
        acl_lines = [line for line in acl.split(b"\r\n") if line.startswith(b"user ")]
        assert len(acl_lines) == 2 and acl_lines[0].startswith(f"user {app} on #".encode())
        assert acl_lines[1] == b"user default off &* -@all"

        ec.modify_user(UserId=app, AccessString="on ~app:* +@read")  # re-applied with default off
        assert _tls_session(ep, ca, ("AUTH", app, pw), ("SET", "k", "v"))[1].startswith(b"-NOPERM")
        ec.modify_user_group(UserGroupId=group, UserIdsToRemove=[app])
        assert _tls_session(ep, ca, ("AUTH", app, pw))[0].startswith(b"-WRONGPASS")
        ec.modify_replication_group(ReplicationGroupId=rg_id, UserGroupIdsToRemove=[group], ApplyImmediately=True)
        assert _tls_session(ep, ca, ("SET", "k", "v")) == [ok]
    finally:
        ec.delete_replication_group(ReplicationGroupId=rg_id)
        ec.delete_user_group(UserGroupId=group)
        ec.delete_user(UserId=app)


def test_elasticache_acl_setuser_denies_inside_selectors_only():
    """Restricted commands are denied inside each (selector), and a key pattern ending in `)` is no selector."""
    from ministack.services import elasticache as _ec
    user = {"UserName": "u", "AccessString": "on ~foo) (~x +get) +set", "Authentication": {"Type": "iam"}}
    assert _ec._acl_setuser(user, ("-a", "-b")) == [
        "ACL", "SETUSER", "u", "reset", "on", "~foo)", "(~x", "+get", "-a", "-b)", "+set", "-a", "-b", "nopass"]
    assert _ec._reported_access_string("on ~* (+get +set ~*)") == "on ~* -@all (+get +set ~*)"
    assert _ec._engine_acl({"Engine": "redis", "EngineVersion": "5.0.6"}, None, None) is None  # no ACL before 6
    assert _ec._engine_rules(["on", "+json.get", "(+@bloom", "~x)", "(~y", "+bf.add)", "(-@json)"],
                             _ec._ACL_MODULE_NAMES) == ["on", "(~x)", "(~y)", "()"]
    from ministack.core.responses import get_account_id, get_region
    uid = _uid()
    _ec._users[f"u-{uid}"] = {"UserId": f"u-{uid}", "UserName": "u", "AccessString": "on ~* +get +hgetex +json.get",
                              "Authentication": {"Type": "iam"}}
    _ec._user_groups[f"g-{uid}"] = {"UserGroupId": f"g-{uid}", "UserIds": [f"u-{uid}"]}
    try:
        _ec._users[f"u-{uid}"]["AccessString"] += " -eval_ro"
        for engine, version, rules in (("valkey", "8.0", "~* +get -eval_ro"), ("valkey", "9.0", "~* +get +hgetex -eval_ro"),
                                       ("redis", "7", "~* +get -eval_ro"), ("redis", "6.2", "~* +get")):
            rg = {"Engine": engine, "EngineVersion": version, "UserGroupIds": [f"g-{uid}"]}
            assert f"user u reset &* on {rules} -acl" in _ec._engine_acl(rg, get_account_id(), get_region())
    finally:
        del _ec._users[f"u-{uid}"], _ec._user_groups[f"g-{uid}"]


def test_elasticache_engine_acl_takes_no_credentials_or_lines_from_the_api():
    """A user name cannot add ACL file lines, and an access string cannot set passwords or nopass."""
    from ministack.core.responses import get_account_id, get_region
    from ministack.services import elasticache as _ec
    uid = _uid()
    _ec._users[f"u-{uid}"] = {"UserId": f"u-{uid}", "UserName": "app\nuser evil on nopass ~* &* +@all",
                              "AccessString": "on ~* +@all", "Authentication": {"Type": "iam"}}
    _ec._users[f"p-{uid}"] = {"UserId": f"p-{uid}", "UserName": "pw", "Authentication": {"Type": "password"},
                              "AccessString": "on ~* >secret #ab !cd <x nopass resetpass reset +@all",
                              "_PasswordHashes": ["ef"]}
    _ec._user_groups[f"g-{uid}"] = {"UserGroupId": f"g-{uid}", "UserIds": [f"u-{uid}", f"p-{uid}"]}
    try:
        acl = _ec._engine_acl({"Engine": "valkey", "EngineVersion": "8.0", "UserGroupIds": [f"g-{uid}"]},
                              get_account_id(), get_region())
        assert "evil" not in acl and acl.count("\n") == 2
        assert "user pw reset &* on ~* +@all -acl|setuser -acl|deluser -acl|load -acl|save -config #ef\n" in acl
    finally:
        del _ec._users[f"u-{uid}"], _ec._users[f"p-{uid}"], _ec._user_groups[f"g-{uid}"]


@requires_docker
@pytest.mark.data_plane
def test_elasticache_replication_group_keeps_its_data_when_users_change(ec, tmp_path):
    """A user change restarts the engines to read the new ACL file; the data survives the restart."""
    uid, pw = _uid(), "a-long-enough-password-1"
    app, group, rg_id = f"a-{uid}", f"g-{uid}", f"rg-{uid}"
    ec.create_user(UserId=app, UserName=app, Engine="valkey", AccessString="on ~* +@all", Passwords=[pw])
    ec.create_user_group(UserGroupId=group, Engine="valkey", UserIds=[app])
    ec.create_replication_group(ReplicationGroupId=rg_id, ReplicationGroupDescription="ug", Engine="valkey",
                                CacheNodeType="cache.t4g.micro", TransitEncryptionEnabled=True, UserGroupIds=[group])
    try:
        ep = ec.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]["NodeGroups"][0]["PrimaryEndpoint"]
        ca = _serverless_ca_file(tmp_path)
        assert _tls_session(ep, ca, ("AUTH", app, pw), ("SET", "app:k", "v"))[1] == b"+OK\r\n"
        ec.modify_user(UserId=app, AccessString="on ~app:* +@read")
        got = _tls_session(ep, ca, ("AUTH", app, pw), ("GET", "app:k"), ("SET", "app:k", "w"))
        assert got[1] == b"$1\r\nv\r\n" and got[2].startswith(b"-NOPERM")  # the new access string, the old key
        # Module commands the engine image does not load are left out of its ACL; the rest applies.
        ec.modify_user(UserId=app, AccessString="on ~* +@write +json.get (+@bloom ~x)")
        assert _tls_session(ep, ca, ("AUTH", app, pw), ("SET", "app:k", "w")) == [b"+OK\r\n", b"+OK\r\n"]
        assert ec.describe_replication_groups(ReplicationGroupId=rg_id)["ReplicationGroups"][0]["NodeGroups"][0][
            "PrimaryEndpoint"] == ep
    finally:
        ec.delete_replication_group(ReplicationGroupId=rg_id)
        ec.delete_user_group(UserGroupId=group)
        ec.delete_user(UserId=app)


def test_elasticache_user_group_apply_converges_on_changes_made_meanwhile(monkeypatch):
    """A change that lands while the ACL file is being written is written too, without a lock."""
    from ministack.core.responses import get_account_id, get_region
    from ministack.services import elasticache as _ec
    uid = _uid()
    user = {"UserId": f"a-{uid}", "UserName": f"a-{uid}", "AccessString": "on ~* +@all",
            "Authentication": {"Type": "iam"}}
    _ec._users[user["UserId"]] = user
    _ec._user_groups[f"g-{uid}"] = {"UserGroupId": f"g-{uid}", "UserIds": [user["UserId"]]}
    written = []

    class Container:
        id = "c1"

        def put_archive(self, path, data):
            import tarfile
            with tarfile.open(fileobj=io.BytesIO(data)) as bundle:
                written.append(bundle.extractfile(bundle.getmembers()[0]).read().decode())
            user["AccessString"] = "on ~app:* -@all +@read"  # a ModifyUser landing meanwhile
            return True

        def restart(self, timeout):
            written.append("restart")

        def exec_run(self, cmd):
            return types.SimpleNamespace(exit_code=0, output=b"NOAUTH Authentication required.")

    client = types.SimpleNamespace(containers=types.SimpleNamespace(get=lambda cid: Container()))
    monkeypatch.setattr(_ec, "_get_docker", lambda: client)
    rg = {"ReplicationGroupId": f"rg-{uid}", "Engine": "valkey", "EngineVersion": "8.0",
          "UserGroupIds": [f"g-{uid}"], "_docker_container_ids": ["c1"]}
    try:
        _ec._apply_user_groups(rg, get_account_id(), get_region())
        assert written[-1] == "restart" and "~app:*" in written[-2] and "+@all" not in written[-2]
        assert written[-2].endswith("user default reset &*\n")
    finally:
        del _ec._users[user["UserId"]], _ec._user_groups[f"g-{uid}"]



def test_elasticache_modify_user_group_refuses_member_adds_and_non_member_removes(ec):
    """Adding a current member or removing a non-member is refused before anything changes, adds checked
    first, the first bad entry named (real AWS, us-east-1); a group is never recorded twice on a user."""
    uid = _uid()
    dflt, member, other, group = f"d-{uid}", f"m-{uid}", f"o-{uid}", f"g-{uid}"
    ec.create_user(UserId=dflt, UserName="default", Engine="redis", AccessString="on ~* +@all", NoPasswordRequired=True)
    for user in (member, other):
        ec.create_user(UserId=user, UserName=user, Engine="redis", AccessString="on ~* +@all", NoPasswordRequired=True)
    ec.create_user_group(UserGroupId=group, Engine="redis", UserIds=[dflt, member])

    def refused(**kwargs):
        with pytest.raises(ClientError) as exc:
            ec.modify_user_group(UserGroupId=group, **kwargs)
        err = exc.value.response["Error"]
        return exc.value.response["ResponseMetadata"]["HTTPStatusCode"], err["Code"], err["Message"]

    try:
        already = (400, "InvalidParameterCombination", f"{member} is already a member of {group}.")
        assert refused(UserIdsToAdd=[member], UserIdsToRemove=[member]) == already
        assert refused(UserIdsToAdd=[member]) == already
        assert refused(UserIdsToAdd=[other, member]) == already
        not_member = (400, "InvalidParameterValue", f"User {other} is not a member of user group {group}.")
        assert refused(UserIdsToRemove=[other]) == not_member
        assert refused(UserIdsToAdd=[other], UserIdsToRemove=[other]) == not_member
        assert refused(UserIdsToRemove=[member, other]) == not_member
        assert refused(UserIdsToAdd=[member], UserIdsToRemove=[other]) == already  # adds first
        group_after = ec.describe_user_groups(UserGroupId=group)["UserGroups"][0]
        assert sorted(group_after["UserIds"]) == sorted([dflt, member])
        assert ec.describe_users(UserId=other)["Users"][0]["UserGroupIds"] == []
        assert ec.describe_users(UserId=member)["Users"][0]["UserGroupIds"] == [group]
    finally:
        ec.delete_user_group(UserGroupId=group)
        for user in (dflt, member, other):
            ec.delete_user(UserId=user)


def test_elasticache_builtin_default_user(ec):
    """Every account has the built-in `default` user, which can't be modified or deleted (real AWS, us-east-1)."""
    user = ec.describe_users(UserId="default")["Users"][0]
    assert {k: v for k, v in user.items() if k != "ARN"} == {
        "UserId": "default", "UserName": "default", "Status": "active", "Engine": "redis",
        "MinimumEngineVersion": "6.0", "AccessString": "on ~* +@all", "UserGroupIds": [],
        "Authentication": {"Type": "no-password"}}
    assert re.fullmatch(r"arn:aws:elasticache:[a-z0-9-]+:\d{12}:user:default", user["ARN"])
    assert [u for u in ec.describe_users()["Users"] if u["UserId"] == "default"] == [user]
    for call, word in ((lambda: ec.modify_user(UserId="default", AccessString="on ~* +@read"), "modified"),
                       (lambda: ec.delete_user(UserId="default"), "deleted")):
        with pytest.raises(ClientError) as exc:
            call()
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"
        assert exc.value.response["Error"]["Message"] == f"default user can't be {word}."
    uid = _uid()
    group = f"g-{uid}"
    ec.create_user_group(UserGroupId=group, Engine="redis", UserIds=["default"])
    try:
        assert ec.describe_users(UserId="default")["Users"][0]["UserGroupIds"] == [group]
    finally:
        ec.delete_user_group(UserGroupId=group)
    assert ec.describe_users(UserId="default")["Users"][0]["UserGroupIds"] == []


def test_elasticache_user_group_duplicate_and_unknown_users(ec):
    """ModifyUserGroup checks run member-to-add, duplicate-to-add, unknown-to-add, non-member-to-remove,
    duplicate-to-remove; CreateUserGroup checks duplicates before unknown users; DeleteUser drops the user
    from its groups (real AWS, us-east-1)."""
    uid = _uid()
    dflt, member, other, group, ghost = f"d-{uid}", f"m-{uid}", f"o-{uid}", f"g-{uid}", f"x-{uid}"
    ec.create_user(UserId=dflt, UserName="default", Engine="redis", AccessString="on ~* +@all", NoPasswordRequired=True)
    for user in (member, other):
        ec.create_user(UserId=user, UserName=user, Engine="redis", AccessString="on ~* +@all", NoPasswordRequired=True)

    def refused(fn, **kwargs):
        with pytest.raises(ClientError) as exc:
            fn(**kwargs)
        err = exc.value.response["Error"]
        return exc.value.response["ResponseMetadata"]["HTTPStatusCode"], err["Code"], err["Message"]

    dup = (400, "InvalidParameterValue", "Duplicate user id specified.")
    unknown = (404, "UserNotFound", f"User {ghost} not found.")
    already = (400, "InvalidParameterCombination", f"{member} is already a member of {group}.")
    not_member = (400, "InvalidParameterValue", f"User {other} is not a member of user group {group}.")
    create = lambda ids: refused(ec.create_user_group, UserGroupId=group, Engine="redis", UserIds=ids)
    assert create([dflt, member, member]) == dup
    assert create([dflt, ghost, member, member]) == dup
    assert create([dflt, ghost, f"y-{uid}"]) == unknown
    assert ec.describe_users(UserId=member)["Users"][0]["UserGroupIds"] == []
    ec.create_user_group(UserGroupId=group, Engine="redis", UserIds=[dflt, member])
    modify = lambda **kw: refused(ec.modify_user_group, UserGroupId=group, **kw)
    try:
        assert modify(UserIdsToAdd=[other, other, member]) == already
        assert modify(UserIdsToAdd=[ghost, other, other]) == dup
        assert modify(UserIdsToAdd=[ghost], UserIdsToRemove=[member, member]) == unknown
        assert modify(UserIdsToAdd=[ghost, f"y-{uid}"]) == unknown
        assert modify(UserIdsToAdd=[ghost], UserIdsToRemove=[other]) == unknown
        assert modify(UserIdsToRemove=[member, member, other]) == not_member
        assert modify(UserIdsToRemove=[ghost]) == (400, "InvalidParameterValue",
                                                   f"User {ghost} is not a member of user group {group}.")
        assert modify(UserIdsToAdd=[other], UserIdsToRemove=[member, member]) == dup
        assert sorted(ec.describe_user_groups(UserGroupId=group)["UserGroups"][0]["UserIds"]) == sorted([dflt, member])
        assert ec.describe_users(UserId=other)["Users"][0]["UserGroupIds"] == []
        ec.delete_user(UserId=member)
        assert ec.describe_user_groups(UserGroupId=group)["UserGroups"][0]["UserIds"] == [dflt]
        assert modify(UserIdsToRemove=[member]) == (400, "InvalidParameterValue",
                                                    f"User {member} is not a member of user group {group}.")
    finally:
        ec.delete_user_group(UserGroupId=group)
        for user in (dflt, other):
            ec.delete_user(UserId=user)
