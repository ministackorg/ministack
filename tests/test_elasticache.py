import io
import json
import os
import time
import zipfile
from urllib.parse import urlparse
import pytest
from botocore.exceptions import ClientError
import uuid as _uuid_mod

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

def test_elasticache_reset_clears_param_groups():
    """ElastiCache reset clears _param_group_params and resets port counter."""
    from ministack.services import elasticache as _ec
    _ec._param_group_params["test-group"] = {"param1": "val1"}
    _ec._port_counter[0] = 99999
    _ec.reset()
    assert not _ec._param_group_params
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

# Migrated from test_ec.py
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


def _uid():
    return _uuid_mod.uuid4().hex[:8]


# ---------------------------------------------------------------------------
# 1. ModifyCacheCluster
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# 5. IncreaseReplicaCount
# ---------------------------------------------------------------------------

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
# 11. Serverless cache operations — not implemented in MiniStack
# ---------------------------------------------------------------------------

def test_serverless_cache_not_implemented(ec):
    """Serverless cache operations are not yet implemented; verify graceful error."""
    with pytest.raises(ClientError):
        ec.create_serverless_cache(
            ServerlessCacheName="test-serverless",
            Engine="redis",
        )

