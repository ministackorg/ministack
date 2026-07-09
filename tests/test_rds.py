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

DEFAULT_AURORA_MYSQL_ENGINE_VERSION = "8.0.mysql_aurora.3.10.3"
UNSUPPORTED_AURORA_MYSQL_ENGINE_VERSION = "9.0.mysql_aurora.9.0.1"
EXPECTED_AURORA_MYSQL_ENGINE_VERSIONS = {
    "5.7.mysql_aurora.2.11.1": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.11.2": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.11.3": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.11.4": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.11.5": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.11.6": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.12.0": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.12.1": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.12.2": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.12.3": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.12.4": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.12.5": "aurora-mysql5.7",
    "5.7.mysql_aurora.2.12.6": "aurora-mysql5.7",
    "8.0.mysql_aurora.3.04.0": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.04.1": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.04.2": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.04.3": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.04.4": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.04.6": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.08.0": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.08.1": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.08.2": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.09.0": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.10.0": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.10.1": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.10.2": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.10.3": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.10.4": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.11.1": "aurora-mysql8.0",
    "8.0.mysql_aurora.3.12.0": "aurora-mysql8.0",
    "8.4.mysql_aurora.8.4.7": "aurora-mysql8.4",
}


def test_rds_create(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="test-db",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password123",
        DBName="testdb",
        AllocatedStorage=20,
    )
    resp = rds.describe_db_instances(DBInstanceIdentifier="test-db")
    instances = resp["DBInstances"]
    assert len(instances) == 1
    assert instances[0]["DBInstanceIdentifier"] == "test-db"
    assert instances[0]["Engine"] == "postgres"
    assert "Address" in instances[0]["Endpoint"]

def test_rds_create_rejects_unknown_parameter_group(rds):
    """CreateDBInstance / CreateDBCluster reject a reference to a parameter group
    that doesn't exist; a created group (or a default.* group) is accepted (#1278)."""
    import botocore
    with pytest.raises(botocore.exceptions.ClientError) as ei:
        rds.create_db_instance(
            DBInstanceIdentifier="pg-missing-db", DBInstanceClass="db.t3.micro",
            Engine="postgres", DBParameterGroupName="nope-not-here",
            MasterUsername="u", MasterUserPassword="Passw0rd!23", AllocatedStorage=20)
    assert ei.value.response["Error"]["Code"] == "DBParameterGroupNotFound"

    rds.create_db_parameter_group(
        DBParameterGroupName="pg-real", DBParameterGroupFamily="postgres15", Description="x")
    rds.create_db_instance(
        DBInstanceIdentifier="pg-real-db", DBInstanceClass="db.t3.micro",
        Engine="postgres", DBParameterGroupName="pg-real",
        MasterUsername="u", MasterUserPassword="Passw0rd!23", AllocatedStorage=20)

    with pytest.raises(botocore.exceptions.ClientError) as ec:
        rds.create_db_cluster(
            DBClusterIdentifier="pg-missing-cl", Engine="aurora-postgresql",
            DBClusterParameterGroupName="nope-cluster",
            MasterUsername="u", MasterUserPassword="Passw0rd!23")
    assert ec.value.response["Error"]["Code"] == "DBClusterParameterGroupNotFound"

def test_rds_engines(rds):
    resp = rds.describe_db_engine_versions(Engine="postgres")
    assert len(resp["DBEngineVersions"]) > 0

def test_rds_cluster(rds):
    rds.create_db_cluster(
        DBClusterIdentifier="test-cluster",
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    resp = rds.describe_db_clusters(DBClusterIdentifier="test-cluster")
    assert resp["DBClusters"][0]["DBClusterIdentifier"] == "test-cluster"

def test_rds_cluster_default_field_serialization(rds):
    """Regression: DescribeDBClusters defaults must match real AWS for
    DatabaseName (absent/None), NetworkType ("IPV4"), and EngineLifecycleSupport
    ("open-source-rds-extended-support") when not supplied at create time."""
    rds.create_db_cluster(
        DBClusterIdentifier="cluster-defaults",
        Engine="aurora-mysql",
        MasterUsername="root",
        MasterUserPassword="password123",
    )
    cluster = rds.describe_db_clusters(DBClusterIdentifier="cluster-defaults")["DBClusters"][0]
    # AWS returns null/absent when no initial database was specified, not "".
    assert cluster.get("DatabaseName") is None
    assert cluster.get("NetworkType") == "IPV4"
    assert cluster.get("EngineLifecycleSupport") == "open-source-rds-extended-support"
    assert cluster["EngineVersion"] == DEFAULT_AURORA_MYSQL_ENGINE_VERSION

def test_rds_cluster_explicit_field_round_trip(rds):
    """Explicit DatabaseName / NetworkType / EngineLifecycleSupport round-trip
    through DescribeDBClusters."""
    rds.create_db_cluster(
        DBClusterIdentifier="cluster-explicit",
        Engine="aurora-mysql",
        MasterUsername="root",
        MasterUserPassword="password123",
        DatabaseName="mydb",
        NetworkType="DUAL",
        EngineLifecycleSupport="open-source-rds-extended-support-disabled",
    )
    cluster = rds.describe_db_clusters(DBClusterIdentifier="cluster-explicit")["DBClusters"][0]
    assert cluster.get("DatabaseName") == "mydb"
    assert cluster.get("NetworkType") == "DUAL"
    assert cluster.get("EngineLifecycleSupport") == "open-source-rds-extended-support-disabled"

def test_rds_create_instance_v2(rds):
    resp = rds.create_db_instance(
        DBInstanceIdentifier="rds-ci-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass123",
        AllocatedStorage=20,
        DBName="mydb",
    )
    inst = resp["DBInstance"]
    assert inst["DBInstanceIdentifier"] == "rds-ci-v2"
    assert inst["DBInstanceStatus"] in ("creating", "available")
    inst = _wait_for_rds(rds, "rds-ci-v2")
    assert inst["DBInstanceStatus"] == "available"
    # Real AWS CreateDBInstance returns "creating" when a backing container
    # is being spawned, "available" when the call is control-plane-only.
    # Both are valid post-create states; ministack mirrors that.
    assert inst["DBInstanceStatus"] in ("available", "creating")
    assert inst["Engine"] == "postgres"
    assert "Address" in inst["Endpoint"]
    assert "Port" in inst["Endpoint"]

def test_rds_describe_instances_v2(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-di-v2a",
        DBInstanceClass="db.t3.micro",
        Engine="mysql",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    rds.create_db_instance(
        DBInstanceIdentifier="rds-di-v2b",
        DBInstanceClass="db.t3.small",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=20,
    )
    resp = rds.describe_db_instances()
    ids = [i["DBInstanceIdentifier"] for i in resp["DBInstances"]]
    assert "rds-di-v2a" in ids
    assert "rds-di-v2b" in ids

    resp2 = rds.describe_db_instances(DBInstanceIdentifier="rds-di-v2a")
    assert len(resp2["DBInstances"]) == 1
    assert resp2["DBInstances"][0]["Engine"] == "mysql"

def test_rds_delete_instance_v2(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-del-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    rds.delete_db_instance(DBInstanceIdentifier="rds-del-v2", SkipFinalSnapshot=True)
    with pytest.raises(ClientError) as exc:
        rds.describe_db_instances(DBInstanceIdentifier="rds-del-v2")
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"

def test_rds_modify_instance_v2(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-mod-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=20,
    )
    rds.modify_db_instance(
        DBInstanceIdentifier="rds-mod-v2",
        DBInstanceClass="db.t3.small",
        AllocatedStorage=50,
        ApplyImmediately=True,
    )
    resp = rds.describe_db_instances(DBInstanceIdentifier="rds-mod-v2")
    inst = resp["DBInstances"][0]
    assert inst["DBInstanceClass"] == "db.t3.small"
    assert inst["AllocatedStorage"] == 50

def test_rds_create_instance_honors_preferred_maintenance_window(rds):
    # Regression: CreateDBInstance previously hardcoded
    # PreferredMaintenanceWindow to "sun:05:00-sun:06:00", silently
    # discarding any user-supplied value.
    rds.create_db_instance(
        DBInstanceIdentifier="rds-pmw-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=20,
        PreferredMaintenanceWindow="tue:03:00-tue:04:00",
    )
    resp = rds.describe_db_instances(DBInstanceIdentifier="rds-pmw-v2")
    inst = resp["DBInstances"][0]
    assert inst["PreferredMaintenanceWindow"] == "tue:03:00-tue:04:00"

def test_rds_create_instance_default_preferred_maintenance_window(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-pmw-default-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=20,
    )
    resp = rds.describe_db_instances(DBInstanceIdentifier="rds-pmw-default-v2")
    inst = resp["DBInstances"][0]
    assert inst["PreferredMaintenanceWindow"] == "sun:05:00-sun:06:00"

def test_rds_describe_pending_maintenance_actions_noop(rds):
    cid = f"pending-maint-{_uuid_mod.uuid4().hex[:10]}"
    rds.create_db_cluster(
        DBClusterIdentifier=cid,
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    cluster_arn = rds.describe_db_clusters(DBClusterIdentifier=cid)["DBClusters"][0]["DBClusterArn"]

    resp = rds.describe_pending_maintenance_actions(ResourceIdentifier=cluster_arn)
    assert resp["PendingMaintenanceActions"] == []

    resp = rds.describe_pending_maintenance_actions()
    assert resp["PendingMaintenanceActions"] == []

    resp = rds.describe_pending_maintenance_actions(
        ResourceIdentifier=cluster_arn,
        Filters=[{"Name": "db-cluster-id", "Values": [cid]}],
        Marker="ignored-marker",
        MaxRecords=20,
    )
    assert resp["PendingMaintenanceActions"] == []

def test_rds_create_cluster_v2(rds):
    resp = rds.create_db_cluster(
        DBClusterIdentifier="rds-cc-v2",
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="pass123",
    )
    cluster = resp["DBCluster"]
    assert cluster["DBClusterIdentifier"] == "rds-cc-v2"
    assert cluster["Status"] == "available"
    assert cluster["Engine"] == "aurora-postgresql"
    assert "DBClusterArn" in cluster

    desc = rds.describe_db_clusters(DBClusterIdentifier="rds-cc-v2")
    assert desc["DBClusters"][0]["DBClusterIdentifier"] == "rds-cc-v2"

def test_rds_engine_versions_v2(rds):
    pg = rds.describe_db_engine_versions(Engine="postgres")
    assert len(pg["DBEngineVersions"]) > 0
    assert all(v["Engine"] == "postgres" for v in pg["DBEngineVersions"])

    mysql = rds.describe_db_engine_versions(Engine="mysql")
    assert len(mysql["DBEngineVersions"]) > 0
    assert all(v["Engine"] == "mysql" for v in mysql["DBEngineVersions"])

def test_rds_snapshot_v2(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-snap-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    resp = rds.create_db_snapshot(
        DBSnapshotIdentifier="rds-snap-v2-s1",
        DBInstanceIdentifier="rds-snap-v2",
    )
    snap = resp["DBSnapshot"]
    assert snap["DBSnapshotIdentifier"] == "rds-snap-v2-s1"
    assert snap["Status"] == "available"

    desc = rds.describe_db_snapshots(DBSnapshotIdentifier="rds-snap-v2-s1")
    assert len(desc["DBSnapshots"]) == 1

    rds.delete_db_snapshot(DBSnapshotIdentifier="rds-snap-v2-s1")
    with pytest.raises(ClientError) as exc:
        rds.describe_db_snapshots(DBSnapshotIdentifier="rds-snap-v2-s1")
    assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"

def test_rds_tags_v2(rds):
    rds.create_db_instance(
        DBInstanceIdentifier="rds-tag-v2",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
        Tags=[{"Key": "env", "Value": "dev"}],
    )
    arn = rds.describe_db_instances(DBInstanceIdentifier="rds-tag-v2")["DBInstances"][0]["DBInstanceArn"]

    tags = rds.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert any(t["Key"] == "env" and t["Value"] == "dev" for t in tags)

    rds.add_tags_to_resource(ResourceName=arn, Tags=[{"Key": "team", "Value": "dba"}])
    tags2 = rds.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert any(t["Key"] == "team" and t["Value"] == "dba" for t in tags2)

    rds.remove_tags_from_resource(ResourceName=arn, TagKeys=["env"])
    tags3 = rds.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert not any(t["Key"] == "env" for t in tags3)
    assert any(t["Key"] == "team" for t in tags3)

def test_rds_cluster_parameter_group(rds):
    rds.create_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cpg",
        DBParameterGroupFamily="aurora-mysql8.0",
        Description="Test cluster param group",
    )
    resp = rds.describe_db_cluster_parameter_groups(DBClusterParameterGroupName="test-cpg")
    groups = resp["DBClusterParameterGroups"]
    assert len(groups) >= 1
    assert groups[0]["DBClusterParameterGroupName"] == "test-cpg"
    rds.delete_db_cluster_parameter_group(DBClusterParameterGroupName="test-cpg")

def test_rds_modify_db_parameter_group(rds):
    rds.create_db_parameter_group(
        DBParameterGroupName="test-mpg",
        DBParameterGroupFamily="mysql8.0",
        Description="Test param group for modify",
    )
    resp = rds.modify_db_parameter_group(
        DBParameterGroupName="test-mpg",
        Parameters=[
            {
                "ParameterName": "max_connections",
                "ParameterValue": "100",
                "ApplyMethod": "immediate",
            }
        ],
    )
    assert resp["DBParameterGroupName"] == "test-mpg"

def test_rds_cluster_snapshot(rds):
    rds.create_db_cluster(
        DBClusterIdentifier="snap-cl",
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    rds.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="snap-cl-snap",
        DBClusterIdentifier="snap-cl",
    )
    resp = rds.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier="snap-cl-snap")
    snaps = resp["DBClusterSnapshots"]
    assert len(snaps) >= 1
    assert snaps[0]["DBClusterSnapshotIdentifier"] == "snap-cl-snap"
    rds.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier="snap-cl-snap")

def test_rds_option_group(rds):
    rds.create_option_group(
        OptionGroupName="test-og",
        EngineName="mysql",
        MajorEngineVersion="8.0",
        OptionGroupDescription="Test option group",
    )
    resp = rds.describe_option_groups(OptionGroupName="test-og")
    groups = resp["OptionGroupsList"]
    assert len(groups) >= 1
    assert groups[0]["OptionGroupName"] == "test-og"
    rds.delete_option_group(OptionGroupName="test-og")

def test_rds_start_stop_cluster(rds):
    rds.create_db_cluster(
        DBClusterIdentifier="ss-cl",
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    rds.stop_db_cluster(DBClusterIdentifier="ss-cl")
    resp = rds.describe_db_clusters(DBClusterIdentifier="ss-cl")
    assert resp["DBClusters"][0]["Status"] == "stopped"
    rds.start_db_cluster(DBClusterIdentifier="ss-cl")
    resp2 = rds.describe_db_clusters(DBClusterIdentifier="ss-cl")
    assert resp2["DBClusters"][0]["Status"] == "available"

def test_rds_modify_subnet_group(rds):
    rds.create_db_subnet_group(
        DBSubnetGroupName="test-mod-sg",
        DBSubnetGroupDescription="Test SG",
        SubnetIds=["subnet-111"],
    )
    rds.modify_db_subnet_group(
        DBSubnetGroupName="test-mod-sg",
        DBSubnetGroupDescription="Updated SG",
        SubnetIds=["subnet-222", "subnet-333"],
    )
    resp = rds.describe_db_subnet_groups(DBSubnetGroupName="test-mod-sg")
    assert resp["DBSubnetGroups"][0]["DBSubnetGroupDescription"] == "Updated SG"

def test_rds_snapshot_crud(rds):
    """CreateDBSnapshot / DescribeDBSnapshots / DeleteDBSnapshot."""
    rds.create_db_instance(
        DBInstanceIdentifier="qa-rds-snap-db",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password",
        AllocatedStorage=20,
    )
    try:
        rds.create_db_snapshot(DBSnapshotIdentifier="qa-rds-snap-1", DBInstanceIdentifier="qa-rds-snap-db")
        snaps = rds.describe_db_snapshots(DBSnapshotIdentifier="qa-rds-snap-1")["DBSnapshots"]
        assert len(snaps) == 1
        assert snaps[0]["DBSnapshotIdentifier"] == "qa-rds-snap-1"
        assert snaps[0]["Status"] == "available"
        rds.delete_db_snapshot(DBSnapshotIdentifier="qa-rds-snap-1")
        snaps2 = rds.describe_db_snapshots()["DBSnapshots"]
        assert not any(s["DBSnapshotIdentifier"] == "qa-rds-snap-1" for s in snaps2)
    finally:
        rds.delete_db_instance(DBInstanceIdentifier="qa-rds-snap-db", SkipFinalSnapshot=True)

def test_rds_deletion_protection(rds):
    """DeleteDBInstance fails when DeletionProtection=True."""
    rds.create_db_instance(
        DBInstanceIdentifier="qa-rds-protected",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password",
        AllocatedStorage=20,
        DeletionProtection=True,
    )
    try:
        with pytest.raises(ClientError) as exc:
            rds.delete_db_instance(DBInstanceIdentifier="qa-rds-protected")
        assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
    finally:
        rds.modify_db_instance(
            DBInstanceIdentifier="qa-rds-protected",
            DeletionProtection=False,
            ApplyImmediately=True,
        )
        rds.delete_db_instance(DBInstanceIdentifier="qa-rds-protected", SkipFinalSnapshot=True)

def test_rds_global_cluster_lifecycle(rds):
    """CreateGlobalCluster / DescribeGlobalClusters / DeleteGlobalCluster lifecycle."""
    rds.create_global_cluster(
        GlobalClusterIdentifier="test-global-1",
        Engine="aurora-postgresql",
        EngineVersion="15.3",
    )
    try:
        resp = rds.describe_global_clusters(GlobalClusterIdentifier="test-global-1")
        gcs = resp["GlobalClusters"]
        assert len(gcs) == 1
        gc = gcs[0]
        assert gc["GlobalClusterIdentifier"] == "test-global-1"
        assert gc["Engine"] == "aurora-postgresql"
        assert gc["Status"] == "available"
        assert "GlobalClusterArn" in gc
        assert "GlobalClusterResourceId" in gc
    finally:
        rds.delete_global_cluster(GlobalClusterIdentifier="test-global-1")

    with pytest.raises(ClientError) as exc:
        rds.describe_global_clusters(GlobalClusterIdentifier="test-global-1")
    assert exc.value.response["Error"]["Code"] == "GlobalClusterNotFoundFault"

def test_rds_global_cluster_with_source(rds):
    """CreateGlobalCluster with SourceDBClusterIdentifier picks up engine from source."""
    rds.create_db_cluster(
        DBClusterIdentifier="gc-source-cluster",
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    try:
        rds.create_global_cluster(
            GlobalClusterIdentifier="test-global-src",
            SourceDBClusterIdentifier="gc-source-cluster",
        )
        resp = rds.describe_global_clusters(GlobalClusterIdentifier="test-global-src")
        gc = resp["GlobalClusters"][0]
        assert gc["Engine"] == "aurora-postgresql"
        members = gc["GlobalClusterMembers"]
        assert len(members) == 1
        assert members[0]["IsWriter"] is True

        # Remove the member, then delete
        rds.remove_from_global_cluster(
            GlobalClusterIdentifier="test-global-src",
            DbClusterIdentifier="gc-source-cluster",
        )
        resp2 = rds.describe_global_clusters(GlobalClusterIdentifier="test-global-src")
        assert len(resp2["GlobalClusters"][0]["GlobalClusterMembers"]) == 0

        rds.delete_global_cluster(GlobalClusterIdentifier="test-global-src")
    finally:
        rds.delete_db_cluster(DBClusterIdentifier="gc-source-cluster", SkipFinalSnapshot=True)

def test_rds_global_cluster_delete_with_members_fails(rds):
    """DeleteGlobalCluster fails when writer members still attached."""
    rds.create_db_cluster(
        DBClusterIdentifier="gc-member-cluster",
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    rds.create_global_cluster(
        GlobalClusterIdentifier="test-global-members",
        SourceDBClusterIdentifier="gc-member-cluster",
    )
    try:
        with pytest.raises(ClientError) as exc:
            rds.delete_global_cluster(GlobalClusterIdentifier="test-global-members")
        assert exc.value.response["Error"]["Code"] == "InvalidGlobalClusterStateFault"
    finally:
        rds.remove_from_global_cluster(
            GlobalClusterIdentifier="test-global-members",
            DbClusterIdentifier="gc-member-cluster",
        )
        rds.delete_global_cluster(GlobalClusterIdentifier="test-global-members")
        rds.delete_db_cluster(DBClusterIdentifier="gc-member-cluster", SkipFinalSnapshot=True)

def test_rds_global_cluster_modify(rds):
    """ModifyGlobalCluster can rename and toggle DeletionProtection."""
    rds.create_global_cluster(
        GlobalClusterIdentifier="test-global-mod",
        Engine="aurora-postgresql",
    )
    try:
        rds.modify_global_cluster(
            GlobalClusterIdentifier="test-global-mod",
            DeletionProtection=True,
        )
        gc = rds.describe_global_clusters(
            GlobalClusterIdentifier="test-global-mod"
        )["GlobalClusters"][0]
        assert gc["DeletionProtection"] is True

        # Cannot delete while protected
        with pytest.raises(ClientError) as exc:
            rds.delete_global_cluster(GlobalClusterIdentifier="test-global-mod")
        assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"

        # Rename
        rds.modify_global_cluster(
            GlobalClusterIdentifier="test-global-mod",
            NewGlobalClusterIdentifier="test-global-renamed",
            DeletionProtection=False,
        )
        resp = rds.describe_global_clusters(GlobalClusterIdentifier="test-global-renamed")
        assert resp["GlobalClusters"][0]["GlobalClusterIdentifier"] == "test-global-renamed"

        with pytest.raises(ClientError):
            rds.describe_global_clusters(GlobalClusterIdentifier="test-global-mod")
    finally:
        try:
            rds.modify_global_cluster(
                GlobalClusterIdentifier="test-global-renamed",
                DeletionProtection=False,
            )
            rds.delete_global_cluster(GlobalClusterIdentifier="test-global-renamed")
        except Exception:
            pass



def test_rds_modify_and_describe_db_parameters(rds):
    """ModifyDBParameterGroup stores ApplyMethod; DescribeDBParameters returns it with Source filter."""
    rds.create_db_parameter_group(
        DBParameterGroupName="test-param-persist",
        DBParameterGroupFamily="mysql8.0",
        Description="param persistence test",
    )
    rds.modify_db_parameter_group(
        DBParameterGroupName="test-param-persist",
        Parameters=[
            {
                "ParameterName": "max_connections",
                "ParameterValue": "200",
                "ApplyMethod": "immediate",
            },
            {
                "ParameterName": "custom_param_xyz",
                "ParameterValue": "hello",
                "ApplyMethod": "pending-reboot",
            },
        ],
    )
    # Describe with Source=user - should only return modified params
    resp = rds.describe_db_parameters(
        DBParameterGroupName="test-param-persist", Source="user"
    )
    params = resp["Parameters"]
    names = [p["ParameterName"] for p in params]
    assert "max_connections" in names
    assert "custom_param_xyz" in names
    mc = next(p for p in params if p["ParameterName"] == "max_connections")
    assert mc["ParameterValue"] == "200"
    assert mc["ApplyMethod"] == "immediate"
    cp = next(p for p in params if p["ParameterName"] == "custom_param_xyz")
    assert cp["ParameterValue"] == "hello"
    assert cp["ApplyMethod"] == "pending-reboot"


def test_rds_reset_db_parameters(rds):
    """ResetDBParameterGroup supports targeted and full reset of user overrides."""
    rds.create_db_parameter_group(
        DBParameterGroupName="test-param-reset",
        DBParameterGroupFamily="mysql8.0",
        Description="param reset test",
    )
    rds.modify_db_parameter_group(
        DBParameterGroupName="test-param-reset",
        Parameters=[
            {
                "ParameterName": "max_connections",
                "ParameterValue": "200",
                "ApplyMethod": "immediate",
            },
            {
                "ParameterName": "custom_param_xyz",
                "ParameterValue": "hello",
                "ApplyMethod": "pending-reboot",
            },
        ],
    )

    rds.reset_db_parameter_group(
        DBParameterGroupName="test-param-reset",
        Parameters=[
            {
                "ParameterName": "custom_param_xyz",
                "ApplyMethod": "pending-reboot",
            },
        ],
    )
    resp = rds.describe_db_parameters(
        DBParameterGroupName="test-param-reset", Source="user"
    )
    names = [p["ParameterName"] for p in resp["Parameters"]]
    assert "max_connections" in names
    assert "custom_param_xyz" not in names

    rds.reset_db_parameter_group(
        DBParameterGroupName="test-param-reset",
        ResetAllParameters=True,
    )
    resp2 = rds.describe_db_parameters(
        DBParameterGroupName="test-param-reset", Source="user"
    )
    assert len(resp2["Parameters"]) == 0

    defaults = rds.describe_db_parameters(
        DBParameterGroupName="test-param-reset", Source="engine-default"
    )["Parameters"]
    max_connections = next(
        p for p in defaults if p["ParameterName"] == "max_connections"
    )
    assert max_connections["ParameterValue"] == "151"


def test_rds_modify_and_describe_cluster_parameters(rds):
    """ModifyDBClusterParameterGroup stores ApplyMethod; DescribeDBClusterParameters returns it."""
    rds.create_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cparam-persist",
        DBParameterGroupFamily="aurora-mysql8.0",
        Description="cluster param persistence test",
    )
    rds.modify_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cparam-persist",
        Parameters=[
            {
                "ParameterName": "innodb_lock_wait_timeout",
                "ParameterValue": "60",
                "ApplyMethod": "immediate",
            },
        ],
    )
    resp = rds.describe_db_cluster_parameters(
        DBClusterParameterGroupName="test-cparam-persist", Source="user"
    )
    params = resp["Parameters"]
    assert len(params) >= 1
    p = next(p for p in params if p["ParameterName"] == "innodb_lock_wait_timeout")
    assert p["ParameterValue"] == "60"
    assert p["ApplyMethod"] == "immediate"
    resp2 = rds.describe_db_cluster_parameters(
        DBClusterParameterGroupName="test-cparam-persist", Source="engine-default"
    )
    default_names = [p["ParameterName"] for p in resp2["Parameters"]]
    assert "max_connections" in default_names
    assert "innodb_lock_wait_timeout" not in default_names


def test_rds_describe_cluster_parameters_emits_source(rds):
    """DescribeDBClusterParameters must emit Source=user for modified params.

    Regression test for omission of <Source> in the cluster parameter
    response XML, which caused botocore to materialize Source as None.
    """
    rds.create_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cparam-source",
        DBParameterGroupFamily="aurora-mysql8.0",
        Description="cluster param source test",
    )
    rds.modify_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cparam-source",
        Parameters=[
            {
                "ParameterName": "binlog_format",
                "ParameterValue": "ROW",
                "ApplyMethod": "pending-reboot",
            },
        ],
    )
    resp = rds.describe_db_cluster_parameters(
        DBClusterParameterGroupName="test-cparam-source"
    )
    p = next(
        p for p in resp["Parameters"] if p["ParameterName"] == "binlog_format"
    )
    assert p.get("Source") == "user"


def test_rds_reset_cluster_parameters(rds):
    """ResetDBClusterParameterGroup clears targeted overrides and full group state."""
    rds.create_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cparam-reset",
        DBParameterGroupFamily="aurora-mysql8.0",
        Description="cluster param reset test",
    )
    rds.modify_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cparam-reset",
        Parameters=[
            {
                "ParameterName": "innodb_lock_wait_timeout",
                "ParameterValue": "60",
                "ApplyMethod": "immediate",
            },
            {
                "ParameterName": "time_zone",
                "ParameterValue": "UTC",
                "ApplyMethod": "pending-reboot",
            },
        ],
    )

    rds.reset_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cparam-reset",
        Parameters=[
            {
                "ParameterName": "time_zone",
                "ApplyMethod": "pending-reboot",
            },
        ],
    )
    resp = rds.describe_db_cluster_parameters(
        DBClusterParameterGroupName="test-cparam-reset", Source="user"
    )
    names = [p["ParameterName"] for p in resp["Parameters"]]
    assert "innodb_lock_wait_timeout" in names
    assert "time_zone" not in names

    rds.reset_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cparam-reset",
        ResetAllParameters=True,
    )
    resp2 = rds.describe_db_cluster_parameters(
        DBClusterParameterGroupName="test-cparam-reset", Source="user"
    )
    assert len(resp2["Parameters"]) == 0


def test_rds_describe_engine_versions_family(rds):
    """DBParameterGroupFamily should not double-prefix the engine name."""
    resp = rds.describe_db_engine_versions(Engine="aurora-mysql")
    versions = resp["DBEngineVersions"]
    assert len(versions) >= 1
    for v in versions:
        family = v["DBParameterGroupFamily"]
        # Should be e.g. "aurora-mysql8.0", not "aurora-mysqlaurora-mysql8.0"
        assert not family.startswith("aurora-mysqlaurora-"), f"Double-prefixed family: {family}"


def test_rds_describe_aurora_mysql_engine_versions_by_family(rds):
    resp = rds.describe_db_engine_versions(Engine="aurora-mysql")
    families = {
        v["EngineVersion"]: v["DBParameterGroupFamily"]
        for v in resp["DBEngineVersions"]
    }
    assert families == EXPECTED_AURORA_MYSQL_ENGINE_VERSIONS
    assert DEFAULT_AURORA_MYSQL_ENGINE_VERSION in families

    filtered = rds.describe_db_engine_versions(
        Engine="aurora-mysql",
        EngineVersion=UNSUPPORTED_AURORA_MYSQL_ENGINE_VERSION,
    )["DBEngineVersions"]
    assert filtered == []


def test_rds_aurora_mysql_create_rejects_unsupported_explicit_engine_version(rds):
    with pytest.raises(ClientError) as cluster_exc:
        rds.create_db_cluster(
            DBClusterIdentifier="unsupported-aurora-version-cluster",
            Engine="aurora-mysql",
            EngineVersion=UNSUPPORTED_AURORA_MYSQL_ENGINE_VERSION,
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
    assert cluster_exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
    assert (
        cluster_exc.value.response["Error"]["Message"]
        == f"Cannot find version {UNSUPPORTED_AURORA_MYSQL_ENGINE_VERSION} for aurora-mysql"
    )

    with pytest.raises(ClientError) as instance_exc:
        rds.create_db_instance(
            DBInstanceIdentifier="unsupported-aurora-version-instance",
            DBInstanceClass="db.t3.micro",
            Engine="aurora-mysql",
            EngineVersion=UNSUPPORTED_AURORA_MYSQL_ENGINE_VERSION,
            MasterUsername="admin",
            MasterUserPassword="password123",
            AllocatedStorage=20,
        )
    assert instance_exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
    assert (
        instance_exc.value.response["Error"]["Message"]
        == f"Cannot find version {UNSUPPORTED_AURORA_MYSQL_ENGINE_VERSION} for aurora-mysql"
    )


def test_rds_aurora_mysql_create_accepts_cataloged_84_engine_version(rds):
    rds.create_db_cluster(
        DBClusterIdentifier="supported-aurora-84-cluster",
        Engine="aurora-mysql",
        EngineVersion="8.4.mysql_aurora.8.4.7",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    try:
        cluster = rds.describe_db_clusters(
            DBClusterIdentifier="supported-aurora-84-cluster"
        )["DBClusters"][0]
        assert cluster["EngineVersion"] == "8.4.mysql_aurora.8.4.7"
    finally:
        rds.delete_db_cluster(
            DBClusterIdentifier="supported-aurora-84-cluster",
            SkipFinalSnapshot=True,
        )


def test_rds_aurora_mysql_parameter_defaults_are_family_aware(rds):
    rds.create_db_parameter_group(
        DBParameterGroupName="test-mysql80-defaults",
        DBParameterGroupFamily="aurora-mysql8.0",
        Description="aurora mysql 8.0 defaults",
    )
    rds.create_db_parameter_group(
        DBParameterGroupName="test-mysql84-defaults",
        DBParameterGroupFamily="aurora-mysql8.4",
        Description="aurora mysql 8.4 defaults",
    )
    mysql80 = rds.describe_db_parameters(
        DBParameterGroupName="test-mysql80-defaults",
        Source="engine-default",
    )["Parameters"]
    mysql84 = rds.describe_db_parameters(
        DBParameterGroupName="test-mysql84-defaults",
        Source="engine-default",
    )["Parameters"]

    mysql80_names = {p["ParameterName"] for p in mysql80}
    mysql84_names = {p["ParameterName"] for p in mysql84}
    assert "max_connections" in mysql80_names
    assert "max_connections" in mysql84_names
    assert "skip-character-set-client-handshake" in mysql80_names
    assert "skip-character-set-client-handshake" not in mysql84_names


def test_rds_cluster_parameter_defaults_are_family_aware(rds):
    rds.create_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cmysql80-defaults",
        DBParameterGroupFamily="aurora-mysql8.0",
        Description="aurora mysql 8.0 cluster defaults",
    )
    rds.create_db_cluster_parameter_group(
        DBClusterParameterGroupName="test-cmysql84-defaults",
        DBParameterGroupFamily="aurora-mysql8.4",
        Description="aurora mysql 8.4 cluster defaults",
    )
    mysql80 = rds.describe_db_cluster_parameters(
        DBClusterParameterGroupName="test-cmysql80-defaults",
        Source="engine-default",
    )["Parameters"]
    mysql84 = rds.describe_db_cluster_parameters(
        DBClusterParameterGroupName="test-cmysql84-defaults",
        Source="engine-default",
    )["Parameters"]

    mysql80_names = {p["ParameterName"] for p in mysql80}
    mysql84_names = {p["ParameterName"] for p in mysql84}
    assert "max_connections" in mysql80_names
    assert "max_connections" in mysql84_names
    assert "skip-character-set-client-handshake" in mysql80_names
    assert "skip-character-set-client-handshake" not in mysql84_names


def test_rds_parse_member_list_both_formats():
    """_parse_member_list handles both Prefix.member.N and Prefix.MemberName.N formats."""
    from ministack.services.rds import _parse_member_list

    # Standard member.N format (direct API calls)
    params_standard = {
        "SubnetIds.member.1": "subnet-aaa",
        "SubnetIds.member.2": "subnet-bbb",
    }
    result = _parse_member_list(params_standard, "SubnetIds")
    assert result == ["subnet-aaa", "subnet-bbb"]

    # Botocore serializer format: Prefix.MemberName.N (via SFN aws-sdk)
    params_botocore = {
        "SubnetIds.SubnetIdentifier.1": "subnet-xxx",
        "SubnetIds.SubnetIdentifier.2": "subnet-yyy",
        "SubnetIds.SubnetIdentifier.3": "subnet-zzz",
    }
    result2 = _parse_member_list(params_botocore, "SubnetIds")
    assert result2 == ["subnet-xxx", "subnet-yyy", "subnet-zzz"]

    # Empty case
    assert _parse_member_list({}, "SubnetIds") == []


def test_rds_describe_by_dbi_resource_id(rds):
    """DescribeDBInstances should accept DbiResourceId as the identifier (AWS parity)."""
    resp = rds.create_db_instance(
        DBInstanceIdentifier="resid-lookup-test",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password123",
        AllocatedStorage=20,
    )
    resource_id = resp["DBInstance"]["DbiResourceId"]
    assert resource_id.startswith("db-")

    desc = rds.describe_db_instances(DBInstanceIdentifier=resource_id)
    assert len(desc["DBInstances"]) == 1
    assert desc["DBInstances"][0]["DBInstanceIdentifier"] == "resid-lookup-test"
    assert desc["DBInstances"][0]["DbiResourceId"] == resource_id


def test_rds_instance_inherits_cluster_username(rds):
    """CreateDBInstance inherits MasterUsername from parent cluster."""
    rds.create_db_cluster(
        DBClusterIdentifier="inherit-cluster",
        Engine="aurora-mysql",
        MasterUsername="myadmin",
        MasterUserPassword="s3cret!",
    )
    rds.create_db_instance(
        DBInstanceIdentifier="inherit-cluster-1",
        DBClusterIdentifier="inherit-cluster",
        DBInstanceClass="db.r6g.large",
        Engine="aurora-mysql",
    )
    resp = rds.describe_db_instances(DBInstanceIdentifier="inherit-cluster-1")
    inst = resp["DBInstances"][0]
    assert inst["MasterUsername"] == "myadmin"
    assert inst["DBClusterIdentifier"] == "inherit-cluster"


def test_rds_handle_request_describe_with_json_body():
    """DescribeDBInstances works when the request body is JSON (not form-encoded)."""
    from ministack.core.responses import set_request_account_id
    from ministack.services import rds as m

    set_request_account_id("111111111111")
    iid = f"inproc-json-{_uuid_mod.uuid4().hex[:12]}"
    m._create_db_instance({
        "DBInstanceIdentifier": [iid],
        "DBInstanceClass": ["db.t3.micro"],
        "Engine": ["postgres"],
        "MasterUsername": ["admin"],
        "MasterUserPassword": ["pw"],
        "AllocatedStorage": ["20"],
    })

    async def desc():
        body = json.dumps({"DBInstanceIdentifier": iid}).encode()
        hdrs = {
            "x-amz-target": "AmazonRDSv19.DescribeDBInstances",
            "content-type": "application/x-amz-json-1.1",
        }
        return await m.handle_request("POST", "/", hdrs, body, {})

    status, _, xml = asyncio.run(desc())
    assert status == 200
    assert iid.encode() in xml


def test_rds_flatten_json_request_params():
    """JSON protocol bodies are merged into query-style params for existing handlers."""
    from ministack.services import rds as m

    params = {}
    m._flatten_json_request_params(
        params,
        {
            "DBInstanceIdentifier": "my-writer",
            "ApplyImmediately": True,
            "BackupRetentionPeriod": 7,
            "Filters": [
                {"Name": "db-instance-id", "Values": ["a", "b"]},
            ],
        },
    )
    assert params["DBInstanceIdentifier"] == ["my-writer"]
    assert params["ApplyImmediately"] == ["true"]
    assert params["BackupRetentionPeriod"] == ["7"]
    assert params["Filters.member.1.Name"] == ["db-instance-id"]
    assert params["Filters.member.1.Values.member.1"] == ["a"]
    assert params["Filters.member.1.Values.member.2"] == ["b"]

    params2 = {}
    m._flatten_json_request_params(
        params2,
        {"dbInstanceIdentifier": "smithy-style-id", "filters": []},
    )
    assert params2["DBInstanceIdentifier"] == ["smithy-style-id"]


def test_rds_aurora_cluster_lists_instance_member(rds):
    """CreateDBInstance for a cluster updates DescribeDBClusters DBClusterMembers."""
    cid = f"memclus-{_uuid_mod.uuid4().hex[:10]}"
    iid = f"{cid}-writer"
    rds.create_db_cluster(
        DBClusterIdentifier=cid,
        Engine="aurora-postgresql",
        MasterUsername="admin",
        MasterUserPassword="pw",
    )
    rds.create_db_instance(
        DBInstanceIdentifier=iid,
        DBClusterIdentifier=cid,
        DBInstanceClass="db.r6g.large",
        Engine="aurora-postgresql",
    )
    out = rds.describe_db_clusters(DBClusterIdentifier=cid)
    members = out["DBClusters"][0].get("DBClusterMembers") or []
    assert any(m["DBInstanceIdentifier"] == iid for m in members)


def test_rds_aurora_cluster_endpoints_follow_backing_instance(rds):
    """Aurora cluster endpoints should be reachable through the local instance."""
    cid = f"epclus-{_uuid_mod.uuid4().hex[:10]}"
    iid = f"{cid}-writer"
    rds.create_db_cluster(
        DBClusterIdentifier=cid,
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="pw",
    )
    rds.create_db_instance(
        DBInstanceIdentifier=iid,
        DBClusterIdentifier=cid,
        DBInstanceClass="db.r6g.large",
        Engine="aurora-mysql",
    )

    inst = rds.describe_db_instances(DBInstanceIdentifier=iid)["DBInstances"][0]
    cluster = rds.describe_db_clusters(DBClusterIdentifier=cid)["DBClusters"][0]

    assert cluster["Endpoint"] == inst["Endpoint"]["Address"]
    assert cluster["ReaderEndpoint"] == inst["Endpoint"]["Address"]
    assert cluster["Port"] == inst["Endpoint"]["Port"]


def test_rds_mysql_master_user_privilege_grants(monkeypatch):
    """MySQL master users get admin grants, with dynamic grants best-effort."""
    import sys
    import types

    from ministack.services import rds as m

    calls = []

    class FakeCursor:
        def execute(self, sql, params=None):
            calls.append((sql, params))
            if "APPLICATION_PASSWORD_ADMIN" in sql:
                raise Exception("unsupported privilege")

        def close(self):
            calls.append(("cursor.close", None))

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            calls.append(("connection.close", None))

    def fake_connect(**kwargs):
        calls.append(("connect", kwargs))
        return FakeConnection()

    monkeypatch.setitem(
        sys.modules,
        "pymysql",
        types.SimpleNamespace(connect=fake_connect),
    )

    m._grant_mysql_master_user_privileges(
        "10.0.0.12", 3306, "admin", "password123", "mysql-test")

    assert calls[0] == (
        "connect",
        {
            "host": "10.0.0.12",
            "port": 3306,
            "user": "root",
            "password": "password123",
            "autocommit": True,
        },
    )
    assert (
        "CREATE USER IF NOT EXISTS %s@'%%' IDENTIFIED BY %s",
        ("admin", "password123"),
    ) in calls
    assert (
        "GRANT ALL PRIVILEGES ON *.* TO %s@'%%' WITH GRANT OPTION",
        ("admin",),
    ) in calls
    assert ("FLUSH PRIVILEGES", None) in calls


def test_rds_create_db_instance_returns_before_container_is_ready(rds):
    """CreateDBInstance must return immediately matching real AWS:
    status=creating now, status=available after the background readiness
    finalisation flips it.

    Real AWS docs: CreateDBInstance "creates a new DB instance" — the
    response is the freshly-created record with `DBInstanceStatus=creating`,
    not a blocking wait until provisioning completes.
    """
    iid = f"intg-rds-nonblock-{_uuid_mod.uuid4().hex[:8]}"
    t0 = time.time()
    resp = rds.create_db_instance(
        DBInstanceIdentifier=iid,
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="secret",
        AllocatedStorage=20,
    )
    elapsed = time.time() - t0
    # Real AWS `CreateDBInstance` returns within milliseconds — readiness is
    # observed asynchronously via `DescribeDBInstances`. We allow a couple of
    # seconds for docker-client setup but must not block on the boot itself.
    assert elapsed < 10, (
        f"CreateDBInstance blocked {elapsed:.1f}s — should return immediately "
        "with status=creating and let a background thread finalise readiness"
    )

    status = resp["DBInstance"]["DBInstanceStatus"]
    # Either "creating" (real container still booting) or "available"
    # (the unit-test path with no Docker — endpoint_host is stub, no real
    # readiness check fires). Both are valid AWS shapes for the early return.
    assert status in ("creating", "available"), f"unexpected status: {status}"

    try:
        rds.delete_db_instance(DBInstanceIdentifier=iid, SkipFinalSnapshot=True)
    except Exception:
        pass


def test_rds_modify_cluster_password(rds):
    """ModifyDBCluster with MasterUserPassword succeeds."""
    rds.create_db_cluster(
        DBClusterIdentifier="pw-mod-cluster",
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="old_pass",
    )
    rds.modify_db_cluster(
        DBClusterIdentifier="pw-mod-cluster",
        MasterUserPassword="new_pass",
    )
    resp = rds.describe_db_clusters(DBClusterIdentifier="pw-mod-cluster")
    cluster = resp["DBClusters"][0]
    assert cluster["DBClusterIdentifier"] == "pw-mod-cluster"


def test_rds_modify_instance_password(rds):
    """ModifyDBInstance with MasterUserPassword updates the stored password."""
    rds.create_db_instance(
        DBInstanceIdentifier="pw-mod-inst",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="old_pass",
        AllocatedStorage=20,
    )
    _wait_for_rds(rds, "pw-mod-inst")
    # Password change should succeed without error
    rds.modify_db_instance(
        DBInstanceIdentifier="pw-mod-inst",
        MasterUserPassword="new_pass",
        ApplyImmediately=True,
    )
    resp = rds.describe_db_instances(DBInstanceIdentifier="pw-mod-inst")
    inst = resp["DBInstances"][0]
    assert inst["DBInstanceIdentifier"] == "pw-mod-inst"
    # Other fields should remain unchanged
    assert inst["MasterUsername"] == "admin"
    assert inst["Engine"] == "postgres"
    assert inst["DBInstanceStatus"] == "available"


# ---------------------------------------------------------------------------
# Tests for the 8 previously-untested operations
# ---------------------------------------------------------------------------


def test_rds_create_read_replica(rds):
    """CreateDBInstanceReadReplica creates a replica linked to the source."""
    rds.create_db_instance(
        DBInstanceIdentifier="rr-source",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass123",
        AllocatedStorage=20,
    )
    try:
        resp = rds.create_db_instance_read_replica(
            DBInstanceIdentifier="rr-replica",
            SourceDBInstanceIdentifier="rr-source",
        )
        replica = resp["DBInstance"]
        assert replica["DBInstanceIdentifier"] == "rr-replica"
        assert replica["ReadReplicaSourceDBInstanceIdentifier"] == "rr-source"
        assert replica["DBInstanceStatus"] == "available"
        assert replica["Engine"] == "postgres"
        assert "Address" in replica["Endpoint"]

        # Source should list the replica
        source = rds.describe_db_instances(DBInstanceIdentifier="rr-source")["DBInstances"][0]
        assert "rr-replica" in source["ReadReplicaDBInstanceIdentifiers"]

        # Duplicate replica id should fail
        with pytest.raises(ClientError) as exc:
            rds.create_db_instance_read_replica(
                DBInstanceIdentifier="rr-replica",
                SourceDBInstanceIdentifier="rr-source",
            )
        assert exc.value.response["Error"]["Code"] == "DBInstanceAlreadyExistsFault"
    finally:
        rds.delete_db_instance(DBInstanceIdentifier="rr-replica", SkipFinalSnapshot=True)
        rds.delete_db_instance(DBInstanceIdentifier="rr-source", SkipFinalSnapshot=True)


def test_rds_create_read_replica_source_not_found(rds):
    """CreateDBInstanceReadReplica fails when the source instance does not exist."""
    with pytest.raises(ClientError) as exc:
        rds.create_db_instance_read_replica(
            DBInstanceIdentifier="rr-orphan",
            SourceDBInstanceIdentifier="rr-nonexistent",
        )
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


def test_rds_reboot_db_instance(rds):
    """RebootDBInstance sets the instance status back to available."""
    rds.create_db_instance(
        DBInstanceIdentifier="reboot-test",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    try:
        resp = rds.reboot_db_instance(DBInstanceIdentifier="reboot-test")
        assert resp["DBInstance"]["DBInstanceStatus"] == "available"

        desc = rds.describe_db_instances(DBInstanceIdentifier="reboot-test")
        assert desc["DBInstances"][0]["DBInstanceStatus"] == "available"
    finally:
        rds.delete_db_instance(DBInstanceIdentifier="reboot-test", SkipFinalSnapshot=True)


def test_rds_reboot_db_instance_not_found(rds):
    """RebootDBInstance fails for a non-existent instance."""
    with pytest.raises(ClientError) as exc:
        rds.reboot_db_instance(DBInstanceIdentifier="no-such-instance")
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


def test_rds_restore_from_snapshot(rds):
    """RestoreDBInstanceFromDBSnapshot creates a new instance from a snapshot."""
    rds.create_db_instance(
        DBInstanceIdentifier="restore-src",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=20,
        DBName="srcdb",
    )
    rds.create_db_snapshot(
        DBSnapshotIdentifier="restore-snap",
        DBInstanceIdentifier="restore-src",
    )
    try:
        resp = rds.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier="restored-db",
            DBSnapshotIdentifier="restore-snap",
            DBInstanceClass="db.t3.small",
        )
        inst = resp["DBInstance"]
        assert inst["DBInstanceIdentifier"] == "restored-db"
        assert inst["DBInstanceStatus"] == "available"
        assert inst["Engine"] == "postgres"
        assert inst["DBInstanceClass"] == "db.t3.small"

        desc = rds.describe_db_instances(DBInstanceIdentifier="restored-db")
        assert len(desc["DBInstances"]) == 1

        # Duplicate target id should fail
        with pytest.raises(ClientError) as exc:
            rds.restore_db_instance_from_db_snapshot(
                DBInstanceIdentifier="restored-db",
                DBSnapshotIdentifier="restore-snap",
            )
        assert exc.value.response["Error"]["Code"] == "DBInstanceAlreadyExistsFault"
    finally:
        rds.delete_db_instance(DBInstanceIdentifier="restored-db", SkipFinalSnapshot=True)
        rds.delete_db_snapshot(DBSnapshotIdentifier="restore-snap")
        rds.delete_db_instance(DBInstanceIdentifier="restore-src", SkipFinalSnapshot=True)


def test_rds_restore_from_snapshot_not_found(rds):
    """RestoreDBInstanceFromDBSnapshot fails when the snapshot does not exist."""
    with pytest.raises(ClientError) as exc:
        rds.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier="will-not-exist",
            DBSnapshotIdentifier="no-such-snap",
        )
    assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"


def _wait_for_status(rds, db_id, expected, timeout=10):
    """Poll DescribeDBInstances until status == expected. Needed because
    CreateDBInstance spawns a background readiness thread that flips status
    to "available" on its own clock, which can race with a subsequent
    StopDBInstance and overwrite the "stopped" state we just set."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = rds.describe_db_instances(
            DBInstanceIdentifier=db_id)["DBInstances"][0]["DBInstanceStatus"]
        if last == expected:
            return last
        time.sleep(0.2)
    return last


def test_rds_start_db_instance(rds):
    """StartDBInstance transitions a stopped instance to available."""
    rds.create_db_instance(
        DBInstanceIdentifier="start-test",
        DBInstanceClass="db.t3.micro",
        Engine="mysql",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    try:
        # Let the bg readiness thread (if any) settle before stopping, so it
        # can't race-overwrite "stopped" back to "available".
        _wait_for_status(rds, "start-test", "available")
        rds.stop_db_instance(DBInstanceIdentifier="start-test")
        assert _wait_for_status(rds, "start-test", "stopped") == "stopped"

        resp = rds.start_db_instance(DBInstanceIdentifier="start-test")
        assert resp["DBInstance"]["DBInstanceStatus"] == "available"

        started = rds.describe_db_instances(DBInstanceIdentifier="start-test")["DBInstances"][0]
        assert started["DBInstanceStatus"] == "available"
    finally:
        rds.delete_db_instance(DBInstanceIdentifier="start-test", SkipFinalSnapshot=True)


def test_rds_start_db_instance_not_found(rds):
    """StartDBInstance fails for a non-existent instance."""
    with pytest.raises(ClientError) as exc:
        rds.start_db_instance(DBInstanceIdentifier="ghost-instance")
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


def test_rds_stop_db_instance(rds):
    """StopDBInstance transitions an available instance to stopped."""
    rds.create_db_instance(
        DBInstanceIdentifier="stop-test",
        DBInstanceClass="db.t3.micro",
        Engine="mysql",
        MasterUsername="admin",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    try:
        # Wait for bg readiness thread to settle so it can't race-overwrite
        # the "stopped" state.
        _wait_for_status(rds, "stop-test", "available")
        resp = rds.stop_db_instance(DBInstanceIdentifier="stop-test")
        assert resp["DBInstance"]["DBInstanceStatus"] == "stopped"

        assert _wait_for_status(rds, "stop-test", "stopped") == "stopped"
    finally:
        rds.delete_db_instance(DBInstanceIdentifier="stop-test", SkipFinalSnapshot=True)


def test_rds_stop_db_instance_not_found(rds):
    """StopDBInstance fails for a non-existent instance."""
    with pytest.raises(ClientError) as exc:
        rds.stop_db_instance(DBInstanceIdentifier="ghost-instance-2")
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


def test_rds_describe_option_group_options(rds):
    """DescribeOptionGroupOptions returns an empty list (stub)."""
    resp = rds.describe_option_group_options(EngineName="mysql")
    assert "OptionGroupOptions" in resp
    assert resp["OptionGroupOptions"] == []


def test_rds_describe_orderable_db_instance_options(rds):
    """DescribeOrderableDBInstanceOptions returns instance classes for an engine."""
    resp = rds.describe_orderable_db_instance_options(Engine="postgres")
    options = resp["OrderableDBInstanceOptions"]
    assert len(options) > 0
    engines = {o["Engine"] for o in options}
    assert engines == {"postgres"}
    classes = {o["DBInstanceClass"] for o in options}
    assert "db.t3.micro" in classes
    assert "db.r5.large" in classes

    # Filter by DBInstanceClass
    resp2 = rds.describe_orderable_db_instance_options(
        Engine="mysql", DBInstanceClass="db.t3.micro",
    )
    options2 = resp2["OrderableDBInstanceOptions"]
    assert len(options2) == 1
    assert options2[0]["DBInstanceClass"] == "db.t3.micro"
    assert options2[0]["Engine"] == "mysql"

    aurora_default = rds.describe_orderable_db_instance_options(
        Engine="aurora-mysql",
        DBInstanceClass="db.t3.micro",
    )["OrderableDBInstanceOptions"]
    assert len(aurora_default) == 1
    assert aurora_default[0]["DBInstanceClass"] == "db.t3.micro"
    assert aurora_default[0]["Engine"] == "aurora-mysql"
    assert aurora_default[0]["EngineVersion"] == DEFAULT_AURORA_MYSQL_ENGINE_VERSION

    aurora_supported = rds.describe_orderable_db_instance_options(
        Engine="aurora-mysql",
        EngineVersion="8.4.mysql_aurora.8.4.7",
        DBInstanceClass="db.t3.micro",
    )["OrderableDBInstanceOptions"]
    assert len(aurora_supported) == 1
    assert aurora_supported[0]["DBInstanceClass"] == "db.t3.micro"
    assert aurora_supported[0]["Engine"] == "aurora-mysql"
    assert aurora_supported[0]["EngineVersion"] == "8.4.mysql_aurora.8.4.7"

    with pytest.raises(ClientError) as exc:
        rds.describe_orderable_db_instance_options(
            Engine="aurora-mysql",
            EngineVersion=UNSUPPORTED_AURORA_MYSQL_ENGINE_VERSION,
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
    assert (
        exc.value.response["Error"]["Message"]
        == f"Cannot find version {UNSUPPORTED_AURORA_MYSQL_ENGINE_VERSION} for aurora-mysql"
    )


def test_rds_enable_http_endpoint(rds):
    """EnableHttpEndpoint enables Data API on an Aurora cluster."""
    rds.create_db_cluster(
        DBClusterIdentifier="http-ep-cluster",
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    try:
        cluster_arn = rds.describe_db_clusters(
            DBClusterIdentifier="http-ep-cluster"
        )["DBClusters"][0]["DBClusterArn"]

        resp = rds.enable_http_endpoint(ResourceArn=cluster_arn)
        assert resp["ResourceArn"] == cluster_arn
        assert resp["HttpEndpointEnabled"] is True

        desc = rds.describe_db_clusters(DBClusterIdentifier="http-ep-cluster")
        assert desc["DBClusters"][0]["HttpEndpointEnabled"] is True
    finally:
        rds.delete_db_cluster(DBClusterIdentifier="http-ep-cluster", SkipFinalSnapshot=True)


def test_rds_enable_http_endpoint_not_found(rds):
    """EnableHttpEndpoint fails when the cluster ARN does not exist."""
    with pytest.raises(ClientError) as exc:
        rds.enable_http_endpoint(
            ResourceArn="arn:aws:rds:us-east-1:123456789012:cluster:no-such-cluster"
        )
    assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"


# ── Postgres 18+ mount-path compatibility ──────────────────


def test_docker_image_for_engine_postgres_pre_18_uses_data_subdir():
    """Postgres < 18 keeps the pre-existing mount path /var/lib/postgresql/data."""
    from ministack.services.rds import _docker_image_for_engine

    for version in ("12.15", "13.11", "14.8", "15.3", "16.4", "17.5"):
        image, env, port, data_path = _docker_image_for_engine(
            "postgres", version, "admin", "pw", "mydb"
        )
        major = version.split(".")[0]
        assert image == f"postgres:{major}-alpine"
        assert port == 5432
        assert data_path == "/var/lib/postgresql/data", (
            f"postgres {version} should mount at /var/lib/postgresql/data"
        )
        assert env["POSTGRES_USER"] == "admin"
        assert env["POSTGRES_PASSWORD"] == "pw"
        assert env["POSTGRES_DB"] == "mydb"


def test_docker_image_for_engine_postgres_18_uses_new_layout():
    """Postgres 18+ must mount at /var/lib/postgresql (not /data).

    The official postgres:18+ image moved to a major-version-specific on-disk
    layout and refuses to start with the old pre-18 mount path. Regression
    test for fix/rds-postgres-18-mount-layout.
    """
    from ministack.services.rds import _docker_image_for_engine

    for version in ("18.0", "18.3", "19.1"):
        image, env, port, data_path = _docker_image_for_engine(
            "postgres", version, "admin", "pw", "mydb"
        )
        major = version.split(".")[0]
        assert image == f"postgres:{major}-alpine"
        assert port == 5432
        assert data_path == "/var/lib/postgresql", (
            f"postgres {version} should mount at /var/lib/postgresql (new layout)"
        )


def test_docker_image_for_engine_aurora_postgres_18_uses_new_layout():
    """aurora-postgresql 18+ follows the same layout switch as vanilla postgres."""
    from ministack.services.rds import _docker_image_for_engine

    _, _, _, data_path_17 = _docker_image_for_engine(
        "aurora-postgresql", "17.5", "admin", "pw", "mydb"
    )
    _, _, _, data_path_18 = _docker_image_for_engine(
        "aurora-postgresql", "18.3", "admin", "pw", "mydb"
    )
    assert data_path_17 == "/var/lib/postgresql/data"
    assert data_path_18 == "/var/lib/postgresql"


def test_mysql_image_for_version_maps_aurora_tracks():
    from ministack.services.rds import _mysql_image_for_version

    assert _mysql_image_for_version("8.4.mysql_aurora.8.4.7") == "mysql:8.4"
    assert _mysql_image_for_version("8.0.mysql_aurora.3.12.0") == "mysql:8.0"
    assert _mysql_image_for_version("5.7.mysql_aurora.2.12.6") == "mysql:5.7"
    assert _mysql_image_for_version("5.6.mysql_aurora.1.23.4") == "mysql:5.6"
    assert _mysql_image_for_version("8.4.7") == "mysql:8.4"
    assert _mysql_image_for_version("9.0.mysql_aurora.9.0.1") == "mysql:8.4"
    assert _mysql_image_for_version("not-a-version") == "mysql:8.4"


def test_docker_image_for_engine_mysql_uses_versioned_images():
    """MySQL / MariaDB / Aurora MySQL keep /var/lib/mysql, but MySQL-compatible
    engines use explicit versioned MySQL images instead of the floating mysql:8 tag."""
    from ministack.services.rds import _docker_image_for_engine

    for engine, version, expected_image in [
        ("mysql", "8.0.33", "mysql:8.0"),
        ("mysql", "5.7.43", "mysql:5.7"),
        ("aurora-mysql", "5.7.mysql_aurora.2.12.6", "mysql:5.7"),
        ("aurora-mysql", "8.0.mysql_aurora.3.12.0", "mysql:8.0"),
        ("aurora-mysql", "8.4.mysql_aurora.8.4.7", "mysql:8.4"),
        ("mariadb", "10.6.14", "mariadb:latest"),
    ]:
        image, _, port, data_path = _docker_image_for_engine(
            engine, version, "admin", "pw", "mydb"
        )
        assert image == expected_image
        assert port == 3306
        assert data_path == "/var/lib/mysql"


def test_docker_image_for_engine_malformed_version_defaults_to_pre_18():
    """An unparseable major version falls back to the pre-18 layout rather
    than crashing. Real AWS RDS only accepts numeric majors, but defensive
    fallback keeps the emulator forgiving."""
    from ministack.services.rds import _docker_image_for_engine

    _, _, _, data_path = _docker_image_for_engine(
        "postgres", "garbage.3", "admin", "pw", "mydb"
    )
    assert data_path == "/var/lib/postgresql/data"


def test_docker_image_for_engine_unknown_engine_returns_nones():
    """Unknown engine returns (None, None, None, None) — the 4-arity tuple
    must be preserved so call sites can safely destructure."""
    from ministack.services.rds import _docker_image_for_engine

    result = _docker_image_for_engine("oracle", "19.0", "admin", "pw", "mydb")
    assert result == (None, None, None, None)


def test_rds_describe_postgres_18_engine_version(rds):
    """DescribeDBEngineVersions exposes the Postgres 18 entry so Terraform's
    validation (and callers that list supported versions) sees it."""
    resp = rds.describe_db_engine_versions(Engine="postgres", EngineVersion="18.3")
    versions = resp["DBEngineVersions"]
    assert len(versions) == 1
    assert versions[0]["EngineVersion"] == "18.3"
    assert versions[0]["DBParameterGroupFamily"] == "18"


def test_rds_create_db_instance_postgres_18(rds):
    """CreateDBInstance accepts EngineVersion=18.3 and round-trips it through
    DescribeDBInstances. Covers the API layer regardless of whether Docker
    is available to actually start the underlying Postgres 18 container."""
    rds.create_db_instance(
        DBInstanceIdentifier="pg18-test",
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        EngineVersion="18.3",
        MasterUsername="admin",
        MasterUserPassword="password123",
        DBName="testdb",
        AllocatedStorage=20,
    )
    try:
        resp = rds.describe_db_instances(DBInstanceIdentifier="pg18-test")
        inst = resp["DBInstances"][0]
        assert inst["Engine"] == "postgres"
        assert inst["EngineVersion"] == "18.3"
        assert "Address" in inst["Endpoint"]
    finally:
        rds.delete_db_instance(DBInstanceIdentifier="pg18-test", SkipFinalSnapshot=True)


def test_rds_restore_state_respawns_docker_container(monkeypatch):
    """restore_state must spawn a Docker container for every persisted
    instance. Without this, instances come back marked "available" with no
    running container, and the metadata-only StartDBInstance /
    RebootDBInstance ops can't recover them. Regression test for #692.
    """
    from ministack.core.responses import get_account_id, get_region
    from ministack.services import rds as m

    runs = []

    class FakeContainer:
        def __init__(self, name, container_id="cid-fake"):
            self.id = container_id
            self.name = name
            self.attrs = {"NetworkSettings": {"Networks": {}}}

        def reload(self): pass
        def stop(self, timeout=2): pass
        def remove(self, v=False): pass

    class FakeContainers:
        def get(self, name):
            raise Exception("not found")

        def run(self, **kwargs):
            runs.append(kwargs)
            return FakeContainer(kwargs["name"])

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda c: None)

    db_id = "respawn-test-db"
    persisted_state = {
        "instances": {db_id: {
            "DBInstanceIdentifier": db_id,
            "Engine": "postgres",
            "EngineVersion": "16.3",
            "MasterUsername": "admin",
            "_MasterUserPassword": "password123",
            "DBName": "mydb",
            "DBInstanceStatus": "available",
            "Endpoint": {"Address": "localhost", "Port": 15500, "HostedZoneId": "Z"},
        }},
        "clusters": {}, "subnet_groups": {}, "param_groups": {},
        "snapshots": {}, "db_cluster_param_groups": {},
        "db_cluster_snapshots": {}, "option_groups": {},
        "global_clusters": {}, "tags": {}, "port_counter": 15500,
    }

    m._instances.clear()
    m.restore_state(persisted_state)

    deadline = time.time() + 5
    while time.time() < deadline and not runs:
        time.sleep(0.05)

    assert runs, "restore_state did not respawn the Docker container"
    assert runs[0]["name"] == m._rds_docker_name(db_id)
    assert runs[0]["image"].endswith("postgres:16-alpine")
    assert runs[0]["environment"]["POSTGRES_USER"] == "admin"
    assert runs[0]["environment"]["POSTGRES_PASSWORD"] == "password123"
    assert runs[0]["environment"]["POSTGRES_DB"] == "mydb"
    assert runs[0]["labels"] == {
        "ministack": "rds",
        "db_id": db_id,
        "account_id": get_account_id(),
        "region": get_region(),
    }

    restored = m._instances.get(db_id)
    assert restored is not None
    assert restored["_docker_container_id"] == "cid-fake"
    assert restored["DBInstanceStatus"] == "available"

    m._instances.clear()


def test_rds_restore_state_removes_stale_container_before_respawn(monkeypatch):
    """If a container with the deterministic name already exists, restore
    must remove the stale one before re-creating, otherwise containers.run
    would fail with a name conflict.
    """
    from ministack.services import rds as m

    runs = []
    removed = []

    class FakeContainer:
        def __init__(self, name, container_id="cid-new"):
            self.id = container_id
            self.name = name
            self.attrs = {"NetworkSettings": {"Networks": {}}}

        def reload(self): pass
        def stop(self, timeout=2): pass
        def remove(self, **kwargs):
            removed.append(self.name)

    stale_name = m._rds_docker_name("stale-db")
    stale = FakeContainer(name=stale_name, container_id="cid-stale")

    class FakeContainers:
        def get(self, name):
            # First call returns the stale container; after .remove() is
            # called and `removed` is populated, subsequent .get()s for
            # the same name raise "not found" — mirrors real docker after
            # a successful force-remove.
            if name == stale_name and stale_name not in removed:
                return stale
            raise Exception("not found")

        def run(self, **kwargs):
            runs.append(kwargs)
            return FakeContainer(kwargs["name"])

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda c: None)

    persisted = {
        "instances": {"stale-db": {
            "DBInstanceIdentifier": "stale-db",
            "Engine": "postgres",
            "EngineVersion": "16.3",
            "MasterUsername": "admin",
            "_MasterUserPassword": "pw",
            "DBName": "db",
            "DBInstanceStatus": "available",
            "Endpoint": {"Address": "localhost", "Port": 15501, "HostedZoneId": "Z"},
        }},
        "clusters": {}, "subnet_groups": {}, "param_groups": {},
        "snapshots": {}, "db_cluster_param_groups": {},
        "db_cluster_snapshots": {}, "option_groups": {},
        "global_clusters": {}, "tags": {}, "port_counter": 15501,
    }

    m._instances.clear()
    m.restore_state(persisted)

    deadline = time.time() + 5
    while time.time() < deadline and not runs:
        time.sleep(0.05)

    assert stale_name in removed, "stale container not removed"
    assert runs, "fresh container not spawned after removing stale one"
    assert runs[0]["name"] == stale_name

    m._instances.clear()


def test_rds_restore_state_preserves_legacy_persistent_volume_name(monkeypatch):
    from ministack.services import rds as m

    runs = []
    removed = []

    class FakeContainer:
        def __init__(self, name):
            self.id = "cid-fake"
            self.name = name
            self.attrs = {"NetworkSettings": {"Networks": {}}}

        def reload(self): pass
        def stop(self, timeout=2): pass
        def remove(self, **kwargs):
            removed.append(self.name)

    class FakeContainers:
        def __init__(self, legacy_name):
            self.legacy_name = legacy_name

        def get(self, name):
            if name == self.legacy_name and name not in removed:
                return FakeContainer(name)
            raise Exception("not found")

        def run(self, **kwargs):
            runs.append(kwargs)
            return FakeContainer(kwargs["name"])

    class FakeDocker:
        def __init__(self, legacy_name):
            self.containers = FakeContainers(legacy_name)

    monkeypatch.setattr(m, "RDS_PERSIST", True)
    db_id = "legacy-volume-db"
    legacy_container_name = m._legacy_rds_docker_name(db_id)
    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker(legacy_container_name))
    monkeypatch.setattr(m, "_get_ministack_network", lambda c: None)

    persisted = {
        "instances": {db_id: {
            "DBInstanceIdentifier": db_id,
            "Engine": "postgres",
            "EngineVersion": "16.3",
            "MasterUsername": "admin",
            "_MasterUserPassword": "pw",
            "DBName": "db",
            "DBInstanceStatus": "available",
            "Endpoint": {"Address": "localhost", "Port": 15501, "HostedZoneId": "Z"},
        }},
        "clusters": {}, "subnet_groups": {}, "param_groups": {},
        "snapshots": {}, "db_cluster_param_groups": {},
        "db_cluster_snapshots": {}, "option_groups": {},
        "global_clusters": {}, "tags": {}, "port_counter": 15501,
    }

    m._instances.clear()
    m.restore_state(persisted)

    deadline = time.time() + 5
    while time.time() < deadline and not runs:
        time.sleep(0.05)

    assert runs, "restore_state did not respawn the Docker container"
    assert legacy_container_name in removed
    assert runs[0]["volumes"] == {
        m._legacy_rds_docker_volume_name(db_id): {
            "bind": "/var/lib/postgresql/data",
            "mode": "rw",
        },
    }
    assert m._instances[db_id]["_docker_volume_name"] == m._legacy_rds_docker_volume_name(db_id)

    m._instances.clear()


def test_rds_respawn_does_not_bind_engine_port_on_host(monkeypatch):
    """Real AWS reports `Endpoint.Port` as the engine's standard port
    (5432 for postgres) regardless of the docker host port mapping.
    Respawn must NOT read `Endpoint.Port` as the host bind port — doing
    so makes every restart try to bind 0.0.0.0:5432 and collide.
    Regression for the bug doodaz reported on #692 after 1.3.48: the
    1.3.47 + 1.3.48 fixes covered restore-then-respawn but left this
    port-reuse bug live."""
    from ministack.services import rds as m

    runs = []

    class FakeContainer:
        def __init__(self, name):
            self.id = "cid-fake"
            self.name = name
            self.attrs = {"NetworkSettings": {"Networks": {}}}
        def reload(self): pass
        def stop(self, timeout=2): pass
        def remove(self, v=False): pass

    class FakeContainers:
        def get(self, name): raise Exception("not found")
        def run(self, **kwargs):
            runs.append(kwargs)
            return FakeContainer(kwargs["name"])

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda c: None)

    db_id = "port-collision-db"
    # Simulate state as persisted by a real run: Endpoint.Port has been
    # overwritten to the container port (5432 for postgres) to match AWS.
    persisted_state = {
        "instances": {db_id: {
            "DBInstanceIdentifier": db_id,
            "Engine": "postgres",
            "EngineVersion": "16.3",
            "MasterUsername": "admin",
            "_MasterUserPassword": "password123",
            "DBName": "mydb",
            "DBInstanceStatus": "available",
            "Endpoint": {"Address": "localhost", "Port": 5432, "HostedZoneId": "Z"},
        }},
        "clusters": {}, "subnet_groups": {}, "param_groups": {},
        "snapshots": {}, "db_cluster_param_groups": {},
        "db_cluster_snapshots": {}, "option_groups": {},
        "global_clusters": {}, "tags": {}, "port_counter": 15500,
    }

    m._instances.clear()
    m.restore_state(persisted_state)

    deadline = time.time() + 5
    while time.time() < deadline and not runs:
        time.sleep(0.05)

    assert runs, "restore_state did not respawn the Docker container"
    port_mapping = runs[0]["ports"]
    # ports == {"5432/tcp": host_port} — host_port must NOT be 5432.
    assert port_mapping == {"5432/tcp": runs[0]["ports"]["5432/tcp"]}, port_mapping
    host_port = port_mapping["5432/tcp"]
    assert host_port != 5432, (
        f"respawn tried to bind container port 5432 to host port 5432 — "
        f"will collide with anything else listening on 5432. "
        f"ports={port_mapping}"
    )
    assert host_port >= 15432, (
        f"respawn host port {host_port} not in MiniStack's allocated range "
        f"(>=15432) — looks like Endpoint.Port leaked through again."
    )

    # The instance must now carry _HostPort so subsequent respawns reuse
    # the same host port instead of allocating a new one each time.
    restored = m._instances.get(db_id)
    assert restored.get("_HostPort") == host_port, (
        "respawn did not persist _HostPort on the instance — next restart "
        "will pick a different port and break clients with cached connection strings."
    )

    m._instances.clear()


def test_rds_next_port_skips_busy_ports(monkeypatch):
    """`_next_port` must probe each candidate and skip ports already
    bound on the host. Without this, a counter-only allocator hands out
    a port that `docker run` will immediately fail to bind."""
    from ministack.services import rds as m

    busy_ports = {15432, 15433, 15434}
    monkeypatch.setattr(m, "_is_host_port_free", lambda p: p not in busy_ports)
    monkeypatch.setattr(m, "_port_counter", [15432])

    port = m._next_port()
    assert port == 15435, (
        f"_next_port returned {port} but ports 15432-15434 were busy — "
        f"it must skip taken ports, not blindly hand them out."
    )


def test_rds_respawn_falls_back_when_persisted_host_port_taken(monkeypatch):
    """If `_HostPort` from persisted state is taken by another process
    on the host, respawn must fall back to a fresh free port instead
    of trying to bind a port we know is unavailable."""
    from ministack.services import rds as m

    runs = []

    class FakeContainer:
        def __init__(self, name):
            self.id = "cid-fake"
            self.name = name
            self.attrs = {"NetworkSettings": {"Networks": {}}}
        def reload(self): pass
        def stop(self, timeout=2): pass
        def remove(self, **kwargs): pass

    class FakeContainers:
        def get(self, name): raise Exception("not found")
        def run(self, **kwargs):
            runs.append(kwargs)
            return FakeContainer(kwargs["name"])

    class FakeDocker:
        def __init__(self): self.containers = FakeContainers()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda c: None)
    # Persisted port 15500 is "taken"; anything else is free.
    monkeypatch.setattr(m, "_is_host_port_free", lambda p: p != 15500)
    monkeypatch.setattr(m, "_port_counter", [15600])

    db_id = "fallback-db"
    persisted_state = {
        "instances": {db_id: {
            "DBInstanceIdentifier": db_id,
            "Engine": "postgres",
            "EngineVersion": "16.3",
            "MasterUsername": "admin",
            "_MasterUserPassword": "p",
            "DBName": "mydb",
            "DBInstanceStatus": "available",
            "Endpoint": {"Address": "localhost", "Port": 5432, "HostedZoneId": "Z"},
            "_HostPort": 15500,
        }},
        "clusters": {}, "subnet_groups": {}, "param_groups": {},
        "snapshots": {}, "db_cluster_param_groups": {},
        "db_cluster_snapshots": {}, "option_groups": {},
        "global_clusters": {}, "tags": {}, "port_counter": 15600,
    }

    m._instances.clear()
    m.restore_state(persisted_state)

    deadline = time.time() + 5
    while time.time() < deadline and not runs:
        time.sleep(0.05)

    assert runs, "respawn never happened"
    host_port = runs[0]["ports"]["5432/tcp"]
    assert host_port != 15500, (
        f"respawn used stored _HostPort=15500 despite it being busy — "
        f"will fail at docker bind. Got host_port={host_port}."
    )
    assert m._instances[db_id]["_HostPort"] == host_port, (
        "fallback port not persisted on instance — next restart will "
        "try the same busy port again."
    )

    m._instances.clear()


def test_rds_respawn_force_removes_stale_created_container(monkeypatch):
    """Doodaz observed a half-spawned `Created`-status container with
    the deterministic name blocking respawn. Plain `.remove()` doesn't
    handle running/created containers; respawn must use `force=True`."""
    from ministack.services import rds as m

    remove_calls = []

    class FakeStaleContainer:
        def remove(self, **kwargs):
            remove_calls.append(kwargs)
            if not kwargs.get("force"):
                raise Exception("cannot remove non-stopped container without force")

    class FakeContainer:
        def __init__(self, name):
            self.id = "cid-fake"
            self.name = name
            self.attrs = {"NetworkSettings": {"Networks": {}}}
        def reload(self): pass
        def stop(self, timeout=2): pass
        def remove(self, **kwargs): pass

    name_present = {"yes": True}

    class FakeContainers:
        def get(self, name):
            if name_present["yes"]:
                name_present["yes"] = False  # second .get() (verification) returns "not found"
                return FakeStaleContainer()
            raise Exception("not found")
        def run(self, **kwargs):
            return FakeContainer(kwargs["name"])

    class FakeDocker:
        def __init__(self): self.containers = FakeContainers()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda c: None)
    monkeypatch.setattr(m, "_is_host_port_free", lambda p: True)
    monkeypatch.setattr(m, "_port_counter", [15700])

    db_id = "stale-created-db"
    persisted_state = {
        "instances": {db_id: {
            "DBInstanceIdentifier": db_id,
            "Engine": "postgres",
            "EngineVersion": "16.3",
            "MasterUsername": "admin",
            "_MasterUserPassword": "p",
            "DBName": "mydb",
            "DBInstanceStatus": "available",
            "Endpoint": {"Address": "localhost", "Port": 5432, "HostedZoneId": "Z"},
        }},
        "clusters": {}, "subnet_groups": {}, "param_groups": {},
        "snapshots": {}, "db_cluster_param_groups": {},
        "db_cluster_snapshots": {}, "option_groups": {},
        "global_clusters": {}, "tags": {}, "port_counter": 15700,
    }

    m._instances.clear()
    m.restore_state(persisted_state)

    deadline = time.time() + 5
    while time.time() < deadline and m._instances.get(db_id, {}).get("DBInstanceStatus") not in ("available", "failed"):
        time.sleep(0.05)

    assert remove_calls, "respawn did not attempt to remove stale container"
    assert any(c.get("force") for c in remove_calls), (
        f"respawn called .remove() without force=True (calls={remove_calls}) — "
        f"will fail on Created/Running containers like the one doodaz hit."
    )

    m._instances.clear()


# ========== from test_rds_lambda_network.py ==========
# RDS+Lambda network reachability via DOCKER_NETWORK auto-detect.
import io
import json
import os
import time
import zipfile

import pytest

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


def _wait_for_rds(rds_client, db_id, timeout=120):
    """Poll DescribeDBInstances until the instance is available."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = rds_client.describe_db_instances(DBInstanceIdentifier=db_id)
        inst = resp["DBInstances"][0]
        if inst["DBInstanceStatus"] == "available":
            return inst
        time.sleep(2)
    raise TimeoutError(f"RDS instance {db_id} not available after {timeout}s")


@pytest.mark.skipif(
    not os.environ.get("DOCKER_NETWORK"),
    reason="DOCKER_NETWORK not set -- skipping network connectivity test",
)
def test_rds_lambda_network_connectivity(rds, lam):
    """Prove that Lambda containers can TCP-connect to an RDS container."""
    db_id = "net-test-pg"
    fn_py = "rds-net-test-py"
    fn_js = "rds-net-test-js"

    # 1. Create RDS Postgres instance
    rds.create_db_instance(
        DBInstanceIdentifier=db_id,
        DBInstanceClass="db.t3.micro",
        Engine="postgres",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )

    try:
        inst = _wait_for_rds(rds, db_id)
        endpoint = inst["Endpoint"]
        host = endpoint["Address"]
        port = int(endpoint["Port"])

        # 2. Endpoint.Address must NOT be localhost when DOCKER_NETWORK is set
        assert host != "localhost", (
            "Expected container IP, got 'localhost' — DOCKER_NETWORK not working"
        )

        # 3. Wait for the Postgres container to accept connections
        import socket
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                with socket.create_connection((host, port), timeout=2):
                    break
            except OSError:
                time.sleep(1)
        else:
            pytest.fail(f"RDS container at {host}:{port} not reachable after 60s")

        # 4. Python Lambda — TCP connect to RDS endpoint
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

        # 5. JS Lambda — TCP connect to RDS endpoint
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
            rds.delete_db_instance(
                DBInstanceIdentifier=db_id, SkipFinalSnapshot=True
            )
        except Exception:
            pass
