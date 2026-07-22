import asyncio
import contextlib
import io
import json
import os
import sys
import time
import types
import uuid
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import ENDPOINT

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


def test_rds_aurora_cluster_members_share_one_container(monkeypatch):
    """Cluster members attach to one cluster-owned container and member delete keeps it alive."""
    import threading

    from ministack.services import rds as m

    runs = []
    containers = {}
    next_ports = []
    removed_volumes = []
    wait_calls = []
    stale_wait_started = threading.Event()
    release_stale_wait = threading.Event()

    class FakeContainer:
        def __init__(self, name):
            self.id = "cluster-container-id"
            self.name = name
            self.status = "running"
            self.attrs = {"NetworkSettings": {"Networks": {}}}
            self.stop_calls = 0
            self.start_calls = 0
            self.remove_calls = 0

        def reload(self):
            pass

        def stop(self, timeout=5):
            self.stop_calls += 1
            self.status = "exited"

        def start(self):
            self.start_calls += 1
            self.status = "running"

        def remove(self, v=False, force=False):
            self.remove_calls += 1

    class FakeContainers:
        def run(self, **kwargs):
            runs.append(kwargs)
            container = FakeContainer(kwargs["name"])
            containers[container.id] = container
            containers[container.name] = container
            return container

        def get(self, identifier):
            if identifier not in containers:
                raise Exception("not found")
            return containers[identifier]

    class FakeVolume:
        def __init__(self, name):
            self.name = name

        def remove(self):
            removed_volumes.append(self.name)

    class FakeVolumes:
        def get(self, name):
            return FakeVolume(name)

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()
            self.volumes = FakeVolumes()

    fake_docker = FakeDocker()

    def _next_port():
        port = 16000 + len(next_ports)
        next_ports.append(port)
        return port

    def _wait_for_database_ready(*_args):
        wait_calls.append(len(wait_calls) + 1)
        if len(wait_calls) == 2:
            stale_wait_started.set()
            release_stale_wait.wait(timeout=2)
            return False
        return True

    monkeypatch.setattr(m, "_get_docker", lambda: fake_docker)
    monkeypatch.setattr(m, "_get_ministack_network", lambda _client: None)
    monkeypatch.setattr(m, "_next_port", _next_port)
    monkeypatch.setattr(m, "_is_host_port_free", lambda _port: True)
    monkeypatch.setattr(m, "_wait_for_database_ready", _wait_for_database_ready)
    monkeypatch.setattr(m, "_grant_mysql_master_user_privileges", lambda *_args: None)

    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "shared-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "password123",
        })
        for db_id in ("shared-writer", "shared-reader"):
            m._create_db_instance({
                "DBInstanceIdentifier": db_id,
                "DBClusterIdentifier": "shared-cluster",
                "DBInstanceClass": "db.r6g.large",
                "Engine": "aurora-mysql",
            })

        deadline = time.time() + 2
        while time.time() < deadline:
            if all(
                m._instances.get(db_id, {}).get("DBInstanceStatus") == "available"
                for db_id in ("shared-writer", "shared-reader")
            ):
                break
            time.sleep(0.01)

        cluster = m._clusters.get("shared-cluster")
        writer = m._instances.get("shared-writer")
        reader = m._instances.get("shared-reader")
        assert len(runs) == 1
        assert next_ports == [16000]
        assert runs[0]["name"] == m._rds_cluster_docker_name("shared-cluster")
        assert runs[0]["labels"]["cluster_id"] == "shared-cluster"
        volume_name = m._rds_cluster_docker_volume_name("shared-cluster")
        assert runs[0]["volumes"] == {
            volume_name: {"bind": "/var/lib/mysql", "mode": "rw"},
        }
        assert writer["_docker_container_id"] == reader["_docker_container_id"]
        assert writer["Endpoint"] == reader["Endpoint"] == cluster["_shared_endpoint"]
        assert cluster["Endpoint"] == cluster["ReaderEndpoint"] == writer["Endpoint"]["Address"]

        persisted = m.get_state()
        assert "_shared_container_id" not in persisted["clusters"].get("shared-cluster")
        assert "_docker_container_id" not in persisted["instances"].get("shared-writer")

        container = containers[writer["_docker_container_id"]]
        m._delete_db_instance({
            "DBInstanceIdentifier": "shared-reader",
            "SkipFinalSnapshot": "true",
        })
        assert container.stop_calls == 0
        assert container.remove_calls == 0

        m._delete_db_instance({
            "DBInstanceIdentifier": "shared-writer",
            "SkipFinalSnapshot": "true",
        })
        assert cluster["Status"] == "available"
        assert cluster["DBClusterMembers"] == []
        assert cluster["_shared_container_ready"] is False
        assert container.stop_calls == 1
        assert container.remove_calls == 0

        m._create_db_instance({
            "DBInstanceIdentifier": "shared-stale-restart",
            "DBClusterIdentifier": "shared-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })
        assert stale_wait_started.wait(timeout=1)
        stale_epoch = cluster["_shared_container_epoch"]
        m._delete_db_instance({
            "DBInstanceIdentifier": "shared-stale-restart",
            "SkipFinalSnapshot": "true",
        })

        m._create_db_instance({
            "DBInstanceIdentifier": "shared-replacement",
            "DBClusterIdentifier": "shared-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })
        deadline = time.time() + 2
        while time.time() < deadline:
            replacement = m._instances.get("shared-replacement")
            if replacement and replacement.get("DBInstanceStatus") == "available":
                break
            time.sleep(0.01)
        replacement = m._instances.get("shared-replacement")
        assert replacement["DBInstanceStatus"] == "available"
        assert replacement["_docker_container_id"] == container.id
        assert cluster["_shared_container_ready"] is True
        assert cluster["_shared_container_epoch"] > stale_epoch
        release_stale_wait.set()
        time.sleep(0.05)
        assert replacement["DBInstanceStatus"] == "available"
        assert cluster["_shared_container_ready"] is True
        assert container.start_calls == 2
        assert len(runs) == 1

        m._delete_db_instance({
            "DBInstanceIdentifier": "shared-replacement",
            "SkipFinalSnapshot": "true",
        })
        assert container.stop_calls == 3
        assert container.remove_calls == 0

        m._delete_db_cluster({
            "DBClusterIdentifier": "shared-cluster",
            "SkipFinalSnapshot": "true",
        })
        assert container.stop_calls == 4
        assert container.remove_calls == 1
        assert removed_volumes == [volume_name]
    finally:
        m._instances.clear()
        m._clusters.clear()


def test_rds_new_member_retries_failed_shared_container(monkeypatch):
    """A new member restarts shared compute after its first boot fails."""
    from ministack.services import rds as m

    runs = []
    readiness_calls = []

    class FakeContainer:
        def __init__(self, name):
            self.id = "failed-shared-container"
            self.name = name
            self.status = "running"
            self.attrs = {"NetworkSettings": {"Networks": {}}}
            self.start_calls = 0

        def reload(self):
            pass

        def start(self):
            self.start_calls += 1
            self.status = "running"

    container = FakeContainer(
        m._rds_cluster_docker_name("failed-retry-cluster"),
    )

    class FakeContainers:
        def run(self, **kwargs):
            runs.append(kwargs)
            return container

        def get(self, identifier):
            if identifier in (container.id, container.name):
                return container
            raise Exception("not found")

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()

    def _wait_for_database_ready(*_args):
        readiness_calls.append(len(readiness_calls) + 1)
        if len(readiness_calls) == 1:
            container.status = "exited"
            return False
        return True

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda _client: None)
    monkeypatch.setattr(m, "_next_port", lambda: 16041)
    monkeypatch.setattr(m, "_is_host_port_free", lambda _port: True)
    monkeypatch.setattr(m, "_wait_for_database_ready", _wait_for_database_ready)
    monkeypatch.setattr(m, "_grant_mysql_master_user_privileges", lambda *_args: None)

    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "failed-retry-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "password123",
        })
        m._create_db_instance({
            "DBInstanceIdentifier": "failed-original",
            "DBClusterIdentifier": "failed-retry-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })

        deadline = time.time() + 2
        while time.time() < deadline:
            if m._instances["failed-original"]["DBInstanceStatus"] == "failed":
                break
            time.sleep(0.01)

        cluster = m._clusters["failed-retry-cluster"]
        assert m._instances["failed-original"]["DBInstanceStatus"] == "failed"
        assert cluster["_shared_container_ready"] is False
        assert cluster["_shared_container_id"] == container.id
        assert container.status == "exited"

        m._create_db_instance({
            "DBInstanceIdentifier": "failed-replacement",
            "DBClusterIdentifier": "failed-retry-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })

        deadline = time.time() + 2
        while time.time() < deadline:
            if all(
                m._instances[db_id]["DBInstanceStatus"] == "available"
                for db_id in ("failed-original", "failed-replacement")
            ):
                break
            time.sleep(0.01)

        assert readiness_calls == [1, 2]
        assert container.start_calls == 1
        assert len(runs) == 1
        assert cluster["_shared_container_ready"] is True
        assert all(
            m._instances[db_id]["DBInstanceStatus"] == "available"
            for db_id in ("failed-original", "failed-replacement")
        )
        assert all(
            m._instances[db_id]["_docker_container_id"] == container.id
            for db_id in ("failed-original", "failed-replacement")
        )
    finally:
        m._instances.clear()
        m._clusters.clear()


def test_rds_password_change_after_failed_shared_start_uses_new_password(
    monkeypatch,
):
    """A fresh retry must not rotate credentials on compute that never started."""
    from ministack.services import rds as m

    runs = []
    readiness_passwords = []
    rotation_calls = []

    class FakeContainer:
        id = "password-retry-container"
        status = "running"
        attrs = {"NetworkSettings": {"Networks": {}}}

        def reload(self):
            pass

    container = FakeContainer()

    class FakeContainers:
        def run(self, **kwargs):
            runs.append(kwargs)
            if len(runs) == 1:
                raise RuntimeError("initial start failed")
            return container

        def get(self, identifier):
            if identifier == container.id:
                return container
            raise Exception("not found")

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()

    def _wait_for_database_ready(_host, _port, _engine, _user, password, *_args):
        readiness_passwords.append(password)
        return True

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda _client: None)
    monkeypatch.setattr(m, "_next_port", lambda: 16042)
    monkeypatch.setattr(m, "_is_host_port_free", lambda _port: True)
    monkeypatch.setattr(m, "_wait_for_database_ready", _wait_for_database_ready)
    monkeypatch.setattr(m, "_grant_mysql_master_user_privileges", lambda *_args: None)
    monkeypatch.setattr(
        m,
        "_rotate_real_password",
        lambda *_args: rotation_calls.append(True) or False,
    )

    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "password-retry-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "old-password",
        })
        m._create_db_instance({
            "DBInstanceIdentifier": "password-retry-original",
            "DBClusterIdentifier": "password-retry-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })

        cluster = m._clusters["password-retry-cluster"]
        original = m._instances["password-retry-original"]
        assert original["DBInstanceStatus"] == "failed"
        assert cluster["_shared_container_id"] is None
        assert cluster["_shared_container_ready"] is False

        m._modify_db_cluster({
            "DBClusterIdentifier": "password-retry-cluster",
            "MasterUserPassword": "new-password",
        })
        assert rotation_calls == []
        assert "_pending_master_password_rotation" not in cluster

        m._create_db_instance({
            "DBInstanceIdentifier": "password-retry-replacement",
            "DBClusterIdentifier": "password-retry-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })

        deadline = time.time() + 2
        while time.time() < deadline:
            if all(
                m._instances[db_id]["DBInstanceStatus"] == "available"
                for db_id in (
                    "password-retry-original",
                    "password-retry-replacement",
                )
            ):
                break
            time.sleep(0.01)

        assert len(runs) == 2
        assert runs[1]["environment"]["MYSQL_ROOT_PASSWORD"] == "new-password"
        assert readiness_passwords == ["new-password"]
        assert all(
            m._instances[db_id]["DBInstanceStatus"] == "available"
            for db_id in (
                "password-retry-original",
                "password-retry-replacement",
            )
        )
    finally:
        m._instances.clear()
        m._clusters.clear()


def test_rds_restored_initialized_storage_defers_password_rotation(monkeypatch):
    """Persisted initialized storage still contains the previous password."""
    from ministack.services import rds as m

    rotation_calls = []
    monkeypatch.setattr(
        m,
        "_rotate_real_password",
        lambda *_args: rotation_calls.append(True) or False,
    )

    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "restored-password-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "old-password",
        })
        cluster = m._clusters["restored-password-cluster"]
        cluster.update({
            "_shared_container_id": "stopped-shared-container",
            "_shared_storage_initialized": True,
            "_shared_volume_name": "restored-password-volume",
            "_shared_container_ready": False,
        })

        persisted = m.get_state()
        persisted_cluster = persisted["clusters"]["restored-password-cluster"]
        assert "_shared_container_id" not in persisted_cluster
        assert persisted_cluster["_shared_storage_initialized"] is True

        m._clusters.clear()
        m.restore_state(persisted)
        restored = m._clusters["restored-password-cluster"]
        assert restored["_shared_container_id"] is None

        m._modify_db_cluster({
            "DBClusterIdentifier": "restored-password-cluster",
            "MasterUserPassword": "new-password",
        })

        assert rotation_calls == []
        assert restored["_pending_master_password_rotation"] == {
            "old_password": "old-password",
            "new_password": "new-password",
        }
    finally:
        m._instances.clear()
        m._clusters.clear()


def test_rds_last_member_delete_wins_after_readiness_epoch_check(monkeypatch):
    """A worker already finalizing readiness cannot revive stopped compute."""
    import threading

    from ministack.services import rds as m

    grant_entered = threading.Event()
    release_grant = threading.Event()
    stop_entered = threading.Event()
    delete_done = threading.Event()
    grant_calls = []

    class FakeContainer:
        id = "readiness-race-container"
        attrs = {"NetworkSettings": {"Networks": {}}}

        def __init__(self):
            self.status = "running"

        def reload(self):
            pass

        def start(self):
            self.status = "running"

        def stop(self, timeout=5):
            self.status = "exited"

    container = FakeContainer()

    class FakeContainers:
        def run(self, **_kwargs):
            return container

        def get(self, identifier):
            if identifier in (
                container.id,
                m._rds_cluster_docker_name("readiness-race-cluster"),
            ):
                return container
            raise Exception("not found")

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda _client: None)
    monkeypatch.setattr(m, "_next_port", lambda: 16020)
    monkeypatch.setattr(m, "_is_host_port_free", lambda _port: True)
    monkeypatch.setattr(m, "_wait_for_database_ready", lambda *_args: True)

    def _grant(*_args):
        grant_calls.append(True)
        if len(grant_calls) == 2:
            grant_entered.set()
            release_grant.wait(timeout=2)

    monkeypatch.setattr(m, "_grant_mysql_master_user_privileges", _grant)

    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "readiness-race-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "password123",
        })
        m._create_db_instance({
            "DBInstanceIdentifier": "readiness-race-original",
            "DBClusterIdentifier": "readiness-race-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })
        deadline = time.time() + 2
        while time.time() < deadline:
            if m._instances.get("readiness-race-original", {}).get(
                "DBInstanceStatus",
            ) == "available":
                break
            time.sleep(0.01)
        m._delete_db_instance({
            "DBInstanceIdentifier": "readiness-race-original",
            "SkipFinalSnapshot": "true",
        })

        original_stop = m._stop_empty_cluster_shared_container

        def _observed_stop(cluster_id, cluster):
            stop_entered.set()
            return original_stop(cluster_id, cluster)

        monkeypatch.setattr(
            m, "_stop_empty_cluster_shared_container", _observed_stop,
        )
        m._create_db_instance({
            "DBInstanceIdentifier": "readiness-race-replacement",
            "DBClusterIdentifier": "readiness-race-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })
        assert grant_entered.wait(timeout=1)

        def _delete_replacement():
            m._delete_db_instance({
                "DBInstanceIdentifier": "readiness-race-replacement",
                "SkipFinalSnapshot": "true",
            })
            delete_done.set()

        delete_thread = threading.Thread(target=_delete_replacement)
        delete_thread.start()
        assert stop_entered.wait(timeout=1)
        assert not delete_done.is_set()
        release_grant.set()
        delete_thread.join(timeout=2)

        cluster = m._clusters.get("readiness-race-cluster")
        assert delete_done.is_set()
        assert cluster["DBClusterMembers"] == []
        assert cluster["_shared_container_ready"] is False
        assert container.status == "exited"
    finally:
        release_grant.set()
        m._instances.clear()
        m._clusters.clear()


def test_rds_stale_readiness_ignores_same_id_cluster_recreation(monkeypatch):
    """Epoch reuse cannot let an old container worker mutate a new cluster."""
    from ministack.services import rds as m

    workers = []
    containers = {}
    ports = iter((16050, 16051))
    run_count = [0]

    class DeferredThread:
        def __init__(self, target, args=(), **_kwargs):
            self.target = target
            self.args = args

        def start(self):
            workers.append(self)

    class FakeContainer:
        attrs = {"NetworkSettings": {"Networks": {}}}

        def __init__(self, name):
            run_count[0] += 1
            self.id = f"recreated-container-{run_count[0]}"
            self.name = name
            self.status = "running"

        def reload(self):
            pass

        def stop(self, timeout=5):
            self.status = "exited"

        def remove(self, **_kwargs):
            containers.pop(self.id, None)
            containers.pop(self.name, None)

    class FakeContainers:
        def run(self, **kwargs):
            container = FakeContainer(kwargs["name"])
            containers[container.id] = container
            containers[container.name] = container
            return container

        def get(self, identifier):
            if identifier not in containers:
                raise Exception("not found")
            return containers[identifier]

    class FakeVolume:
        def remove(self):
            pass

    class FakeVolumes:
        def get(self, _name):
            return FakeVolume()

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()
            self.volumes = FakeVolumes()

    monkeypatch.setattr(m.threading, "Thread", DeferredThread)
    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda _client: None)
    monkeypatch.setattr(m, "_next_port", lambda: next(ports))
    monkeypatch.setattr(m, "_is_host_port_free", lambda _port: True)
    monkeypatch.setattr(
        m,
        "_wait_for_database_ready",
        lambda _host, port, *_args: port == 16051,
    )
    monkeypatch.setattr(m, "_grant_mysql_master_user_privileges", lambda *_args: None)

    cluster_id = "recreated-readiness-cluster"
    m._instances.clear()
    m._clusters.clear()
    try:
        for member_id in ("old-member", "new-member"):
            m._create_db_cluster({
                "DBClusterIdentifier": cluster_id,
                "Engine": "aurora-mysql",
                "MasterUsername": "admin",
                "MasterUserPassword": "password123",
            })
            m._create_db_instance({
                "DBInstanceIdentifier": member_id,
                "DBClusterIdentifier": cluster_id,
                "DBInstanceClass": "db.r6g.large",
                "Engine": "aurora-mysql",
            })
            if member_id == "old-member":
                old_container_id = m._clusters[cluster_id][
                    "_shared_container_id"
                ]
                m._delete_db_instance({
                    "DBInstanceIdentifier": member_id,
                    "SkipFinalSnapshot": "true",
                })
                m._delete_db_cluster({
                    "DBClusterIdentifier": cluster_id,
                    "SkipFinalSnapshot": "true",
                })

        cluster = m._clusters[cluster_id]
        new_container_id = cluster["_shared_container_id"]
        assert old_container_id != new_container_id
        assert len(workers) == 2

        # Finalize the replacement first. Both cluster incarnations use epoch
        # 1, so epoch-only validation cannot distinguish the old worker.
        workers[1].target(*workers[1].args)
        assert cluster["_shared_container_epoch"] == 1
        assert cluster["_shared_container_ready"] is True
        assert m._instances["new-member"]["DBInstanceStatus"] == "available"

        workers[0].target(*workers[0].args)
        assert cluster["_shared_container_ready"] is True
        assert m._instances["new-member"]["DBInstanceStatus"] == "available"
    finally:
        m._instances.clear()
        m._clusters.clear()


def test_rds_empty_control_plane_cluster_accepts_new_member(monkeypatch):
    """A no-Docker cluster becomes connectable again when compute returns."""
    from ministack.services import rds as m

    monkeypatch.setattr(m, "_get_docker", lambda: None)
    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "control-plane-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "password123",
        })
        m._create_db_instance({
            "DBInstanceIdentifier": "control-plane-original",
            "DBClusterIdentifier": "control-plane-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })
        m._delete_db_instance({
            "DBInstanceIdentifier": "control-plane-original",
            "SkipFinalSnapshot": "true",
        })

        cluster = m._clusters.get("control-plane-cluster")
        assert cluster["DBClusterMembers"] == []
        assert cluster["_shared_container_ready"] is False

        m._create_db_instance({
            "DBInstanceIdentifier": "control-plane-replacement",
            "DBClusterIdentifier": "control-plane-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })
        replacement = m._instances.get("control-plane-replacement")
        assert replacement["DBInstanceStatus"] == "available"
        assert cluster["_shared_container_ready"] is True
        assert [
            member["DBInstanceIdentifier"]
            for member in cluster["DBClusterMembers"]
        ] == ["control-plane-replacement"]
    finally:
        m._instances.clear()
        m._clusters.clear()


def test_rds_invalid_member_does_not_restart_empty_cluster(monkeypatch):
    """Request validation happens before a stopped cluster becomes reachable."""
    from ministack.services import rds as m

    start_calls = []

    class FakeContainer:
        status = "exited"
        attrs = {"NetworkSettings": {"Networks": {}}}

        def start(self):
            start_calls.append(True)

        def reload(self):
            pass

    class FakeContainers:
        def get(self, identifier):
            assert identifier == "stopped-shared-container"
            return FakeContainer()

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    m._instances.clear()
    m._clusters.clear()
    m._param_groups.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "invalid-member-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "password123",
        })
        cluster = m._clusters["invalid-member-cluster"]
        cluster.update({
            "_shared_container_id": "stopped-shared-container",
            "_shared_host_port": 16042,
            "_shared_endpoint": {
                "Address": "localhost",
                "Port": 16042,
                "HostedZoneId": "Z2R2ITUGPM61AM",
            },
            "_shared_container_ready": False,
        })

        m._create_db_instance({
            "DBInstanceIdentifier": "invalid-member",
            "DBClusterIdentifier": "invalid-member-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
            "DBParameterGroupName": "missing-custom-group",
        })

        assert start_calls == []
        assert "invalid-member" not in m._instances
        assert cluster["DBClusterMembers"] == []
        assert cluster["_shared_container_ready"] is False
    finally:
        m._instances.clear()
        m._clusters.clear()
        m._param_groups.clear()


def test_rds_cluster_member_connection_fields_are_normalized(monkeypatch):
    from ministack.services import rds as m

    monkeypatch.setattr(m, "_get_docker", lambda: None)
    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "credential-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "cluster_admin",
            "MasterUserPassword": "cluster-password",
            "DatabaseName": "cluster_db",
        })
        m._create_db_instance({
            "DBInstanceIdentifier": "credential-member",
            "DBClusterIdentifier": "credential-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-postgresql",
            "MasterUsername": "wrong_admin",
            "MasterUserPassword": "wrong-password",
            "DBName": "wrong_db",
        })

        member = m._instances.get("credential-member")
        assert member["Engine"] == "aurora-mysql"
        assert member["MasterUsername"] == "cluster_admin"
        assert member["_MasterUserPassword"] == "cluster-password"
        assert member["DBName"] == "cluster_db"
    finally:
        m._instances.clear()
        m._clusters.clear()


@pytest.mark.parametrize("rotation_succeeds", [True, False])
def test_rds_empty_cluster_applies_pending_password_on_restart(
    monkeypatch, rotation_succeeds,
):
    from ministack.services import rds as m

    readiness_passwords = []
    rotations = []
    grants = []

    class FakeContainer:
        id = "pending-password-container"
        attrs = {"NetworkSettings": {"Networks": {}}}

        def __init__(self):
            self.status = "exited"

        def start(self):
            self.status = "running"

        def reload(self):
            pass

    container = FakeContainer()

    class FakeContainers:
        def get(self, identifier):
            assert identifier == container.id
            return container

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()

    def _wait_for_database_ready(_host, _port, _engine, _user, password, *_args):
        readiness_passwords.append(password)
        return True

    def _rotate(_cluster, old_password, new_password):
        rotations.append((old_password, new_password))
        return rotation_succeeds

    def _grant(_host, _port, user, password, db_id):
        grants.append((user, password, db_id))

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda _client: None)
    monkeypatch.setattr(m, "_wait_for_database_ready", _wait_for_database_ready)
    monkeypatch.setattr(m, "_rotate_real_password", _rotate)
    monkeypatch.setattr(m, "_grant_mysql_master_user_privileges", _grant)

    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "pending-password-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "old-password",
            "DatabaseName": "appdb",
        })
        cluster = m._clusters.get("pending-password-cluster")
        cluster.update({
            "_shared_container_id": container.id,
            "_shared_host_port": 16021,
            "_shared_endpoint": {
                "Address": "localhost",
                "Port": 16021,
                "HostedZoneId": "Z2R2ITUGPM61AM",
            },
            "_shared_container_ready": False,
            "_shared_container_epoch": 1,
        })

        m._modify_db_cluster({
            "DBClusterIdentifier": "pending-password-cluster",
            "MasterUserPassword": "new-password",
        })
        assert cluster["_pending_master_password_rotation"] == {
            "old_password": "old-password",
            "new_password": "new-password",
        }

        m._create_db_instance({
            "DBInstanceIdentifier": "pending-password-writer",
            "DBClusterIdentifier": "pending-password-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
            "MasterUserPassword": "member-password-must-be-ignored",
        })
        deadline = time.time() + 2
        while time.time() < deadline:
            if m._instances.get("pending-password-writer", {}).get(
                "DBInstanceStatus",
            ) in {"available", "failed"}:
                break
            time.sleep(0.01)

        member = m._instances.get("pending-password-writer")
        assert readiness_passwords == ["old-password"]
        assert rotations == [("old-password", "new-password")]
        assert member["_MasterUserPassword"] == "new-password"
        if rotation_succeeds:
            assert member["DBInstanceStatus"] == "available"
            assert cluster["_shared_container_ready"] is True
            assert "_pending_master_password_rotation" not in cluster
            assert grants == [
                ("admin", "new-password", "pending-password-cluster"),
            ]
        else:
            assert member["DBInstanceStatus"] == "failed"
            assert cluster["_shared_container_ready"] is False
            assert cluster["_pending_master_password_rotation"] == {
                "old_password": "old-password",
                "new_password": "new-password",
            }
            assert grants == []
    finally:
        m._instances.clear()
        m._clusters.clear()


def test_rds_rotate_real_mysql_master_password(monkeypatch):
    from ministack.services import rds as m

    connects = []
    executions = []

    class FakeCursor:
        def execute(self, query, params):
            executions.append((query, params))

        def close(self):
            pass

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    def _connect(**kwargs):
        connects.append(kwargs)
        return FakeConnection()

    fake_pymysql = types.ModuleType("pymysql")
    fake_pymysql.connect = _connect
    monkeypatch.setitem(sys.modules, "pymysql", fake_pymysql)
    m._instances.clear()
    try:
        m._instances["rotation-member"] = {
            "DBInstanceIdentifier": "rotation-member",
            "DBClusterIdentifier": "rotation-cluster",
            "Endpoint": {"Address": "localhost", "Port": 16031},
        }
        assert m._rotate_real_password({
            "DBClusterIdentifier": "rotation-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "app_admin",
        }, "old-password", "new-password") is True

        assert connects == [{
            "host": "localhost",
            "port": 16031,
            "user": "root",
            "password": "old-password",
            "autocommit": True,
        }]
        assert executions == [
            (
                "ALTER USER %s@'%%' IDENTIFIED BY %s",
                ("app_admin", "new-password"),
            ),
            ("ALTER USER 'root'@'%%' IDENTIFIED BY %s", ("new-password",)),
        ]
    finally:
        m._instances.clear()


def test_rds_password_change_serializes_with_readiness_finalization(monkeypatch):
    """A rotation racing final readiness is applied exactly once."""
    import threading

    from ministack.services import rds as m

    grant_entered = threading.Event()
    release_grant = threading.Event()
    modify_done = threading.Event()
    rotations = []

    class FakeContainer:
        id = "password-race-container"
        attrs = {"NetworkSettings": {"Networks": {}}}
        status = "running"

        def reload(self):
            pass

    class FakeContainers:
        def run(self, **_kwargs):
            return FakeContainer()

        def get(self, identifier):
            if identifier == FakeContainer.id:
                return FakeContainer()
            raise Exception("not found")

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()

    def _grant(*_args):
        grant_entered.set()
        release_grant.wait(timeout=2)

    def _rotate(_cluster, old_password, new_password):
        rotations.append((old_password, new_password))
        return True

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda _client: None)
    monkeypatch.setattr(m, "_next_port", lambda: 16033)
    monkeypatch.setattr(m, "_is_host_port_free", lambda _port: True)
    monkeypatch.setattr(m, "_wait_for_database_ready", lambda *_args: True)
    monkeypatch.setattr(m, "_grant_mysql_master_user_privileges", _grant)
    monkeypatch.setattr(m, "_rotate_real_password", _rotate)

    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "password-race-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "old-password",
        })
        m._create_db_instance({
            "DBInstanceIdentifier": "password-race-writer",
            "DBClusterIdentifier": "password-race-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })
        assert grant_entered.wait(timeout=1)

        def _modify_password():
            m._modify_db_cluster({
                "DBClusterIdentifier": "password-race-cluster",
                "MasterUserPassword": "new-password",
            })
            modify_done.set()

        modify_thread = threading.Thread(target=_modify_password)
        modify_thread.start()
        time.sleep(0.05)
        assert not modify_done.is_set()

        release_grant.set()
        modify_thread.join(timeout=2)

        cluster = m._clusters.get("password-race-cluster")
        member = m._instances.get("password-race-writer")
        assert modify_done.is_set()
        assert rotations == [("old-password", "new-password")]
        assert cluster["_MasterUserPassword"] == "new-password"
        assert "_pending_master_password_rotation" not in cluster
        assert cluster["_shared_container_ready"] is True
        assert member["DBInstanceStatus"] == "available"
    finally:
        release_grant.set()
        m._instances.clear()
        m._clusters.clear()


def test_rds_rotate_real_postgres_master_password(monkeypatch):
    from ministack.services import rds as m

    connects = []
    executions = []

    class FakeCursor:
        def execute(self, query, params):
            executions.append((query, params))

        def close(self):
            pass

    class FakeConnection:
        autocommit = False

        def cursor(self):
            return FakeCursor()

        def close(self):
            pass

    def _connect(**kwargs):
        connects.append(kwargs)
        return FakeConnection()

    class FakeIdentifier:
        def __init__(self, value):
            self.value = value

        def __repr__(self):
            return f"Identifier({self.value!r})"

    class FakeSQL:
        def __init__(self, value):
            self.value = value

        def format(self, **kwargs):
            return self.value, kwargs

    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_psycopg2.connect = _connect
    fake_psycopg2.sql = types.SimpleNamespace(
        Identifier=FakeIdentifier,
        SQL=FakeSQL,
    )
    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)
    m._instances.clear()
    try:
        m._instances["rotation-member"] = {
            "DBInstanceIdentifier": "rotation-member",
            "DBClusterIdentifier": "rotation-cluster",
            "Endpoint": {"Address": "localhost", "Port": 16032},
        }
        assert m._rotate_real_password({
            "DBClusterIdentifier": "rotation-cluster",
            "Engine": "aurora-postgresql",
            "MasterUsername": "app_admin",
            "DatabaseName": "appdb",
        }, "old-password", "new-password") is True

        assert connects == [{
            "host": "localhost",
            "port": 16032,
            "user": "app_admin",
            "password": "old-password",
            "dbname": "appdb",
        }]
        assert len(executions) == 1
        assert "Identifier('app_admin')" in repr(executions[0][0])
        assert executions[0][1] == ("new-password",)
    finally:
        m._instances.clear()


def test_rds_delete_cluster_rejects_attached_members(monkeypatch):
    from ministack.services import rds as m

    monkeypatch.setattr(m, "_get_docker", lambda: None)
    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "member-owned-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "password123",
        })
        m._create_db_instance({
            "DBInstanceIdentifier": "member-owned-writer",
            "DBClusterIdentifier": "member-owned-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })

        status, _, body = m._delete_db_cluster({
            "DBClusterIdentifier": "member-owned-cluster",
            "SkipFinalSnapshot": "true",
        })

        assert status == 400
        assert b"InvalidDBClusterStateFault" in body
        assert m._clusters.get("member-owned-cluster") is not None
        assert m._instances.get("member-owned-writer")["DBInstanceStatus"] == "available"
    finally:
        m._instances.clear()
        m._clusters.clear()


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


def test_rds_modify_cluster_member_password_is_rejected(monkeypatch):
    from ministack.services import rds as m

    rotations = []
    monkeypatch.setattr(
        m,
        "_rotate_instance_password",
        lambda *_args: rotations.append(_args),
    )
    monkeypatch.setattr(m, "_get_docker", lambda: None)
    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": "member-password-cluster",
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "cluster-password",
        })
        m._create_db_instance({
            "DBInstanceIdentifier": "member-password-writer",
            "DBClusterIdentifier": "member-password-cluster",
            "DBInstanceClass": "db.r6g.large",
            "Engine": "aurora-mysql",
        })
        member = m._instances.get("member-password-writer")

        status, _, body = m._modify_db_instance({
            "DBInstanceIdentifier": "member-password-writer",
            "MasterUserPassword": "member-password",
            "ApplyImmediately": "true",
        })

        assert status == 400
        assert b"InvalidParameterCombination" in body
        assert b"Use ModifyDBCluster instead" in body
        assert member["_MasterUserPassword"] == "cluster-password"
        assert m._clusters.get("member-password-cluster")[
            "_MasterUserPassword"
        ] == "cluster-password"
        assert rotations == []
    finally:
        m._instances.clear()
        m._clusters.clear()


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


def test_rds_disable_http_endpoint(rds):
    """DisableHttpEndpoint disables Data API on an Aurora cluster."""
    rds.create_db_cluster(
        DBClusterIdentifier="http-ep-disable-cluster",
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="password123",
    )
    try:
        cluster_arn = rds.describe_db_clusters(
            DBClusterIdentifier="http-ep-disable-cluster"
        )["DBClusters"][0]["DBClusterArn"]

        rds.enable_http_endpoint(ResourceArn=cluster_arn)

        resp = rds.disable_http_endpoint(ResourceArn=cluster_arn)
        assert resp["ResourceArn"] == cluster_arn
        assert resp["HttpEndpointEnabled"] is False

        desc = rds.describe_db_clusters(DBClusterIdentifier="http-ep-disable-cluster")
        assert desc["DBClusters"][0]["HttpEndpointEnabled"] is False
    finally:
        rds.delete_db_cluster(DBClusterIdentifier="http-ep-disable-cluster", SkipFinalSnapshot=True)


def test_rds_disable_http_endpoint_not_found(rds):
    """DisableHttpEndpoint fails when the cluster ARN does not exist."""
    with pytest.raises(ClientError) as exc:
        rds.disable_http_endpoint(
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


@pytest.mark.parametrize(
    "scenario",
    [
        "ready",
        "not-ready",
        "writer-removal-fails",
        "reader-removal-fails",
        "ownership-mismatch",
        "member-added-during-readiness",
        "member-added-during-migration",
        "pending-password-rotation",
        "last-member-deleted-during-readiness",
        "last-members-deleted-before-start",
    ],
)
def test_rds_restore_state_respawns_one_container_per_cluster(
    monkeypatch, scenario,
):
    """Legacy volumes are reaped only after the adopted writer is ready."""
    import threading

    from ministack.core.responses import AccountRegionScopedDict, get_account_id, get_region
    from ministack.services import rds as m

    runs = []
    removed_containers = []
    removed_volumes = []
    readiness_credentials = []
    rotations = []
    grants = []
    remaining_legacy_names = set()
    legacy_owner_by_name = {}
    writer_legacy_container_name = [None]
    reader_legacy_container_name = [None]
    callback_action_done = [False]
    migration_pause_done = [False]
    migration_remove_started = threading.Event()
    release_migration_remove = threading.Event()
    database_ready = scenario != "not-ready"
    writer_removal_succeeds = scenario != "writer-removal-fails"
    reader_removal_succeeds = scenario != "reader-removal-fails"

    class FakeContainer:
        id = "restored-shared-container"
        attrs = {"NetworkSettings": {"Networks": {}}}
        status = "running"

        def reload(self):
            pass

    class FakeLegacyContainer:
        def __init__(self, name):
            self.name = name
            self.labels = {
                "ministack": "rds",
                "db_id": legacy_owner_by_name[name],
                "account_id": get_account_id(),
                "region": get_region(),
            }
            if (
                scenario == "ownership-mismatch"
                and name == reader_legacy_container_name[0]
            ):
                self.labels.pop("db_id")
                self.labels["cluster_id"] = "different-current-cluster"
            self.attrs = {"Config": {"Labels": self.labels}}

        def remove(self, force=False, v=False):
            assert force is True
            assert v is False
            if (
                scenario in (
                    "last-members-deleted-before-start",
                    "member-added-during-migration",
                )
                and not migration_pause_done[0]
            ):
                migration_pause_done[0] = True
                migration_remove_started.set()
                release_migration_remove.wait(timeout=2)
            if (
                self.name == writer_legacy_container_name[0]
                and not writer_removal_succeeds
            ):
                raise Exception("writer container remains running")
            if (
                self.name == reader_legacy_container_name[0]
                and not reader_removal_succeeds
            ):
                raise Exception("reader container remains running")
            if scenario == "ownership-mismatch" and self.name == (
                reader_legacy_container_name[0]
            ):
                raise AssertionError("unowned current container must not be removed")
            removed_containers.append(self.name)
            remaining_legacy_names.discard(self.name)

    class FakeContainers:
        def get(self, identifier):
            if identifier in remaining_legacy_names:
                return FakeLegacyContainer(identifier)
            raise Exception("not found")

        def run(self, **kwargs):
            runs.append(kwargs)
            return FakeContainer()

    class FakeVolume:
        def __init__(self, name):
            self.name = name

        def remove(self):
            removed_volumes.append(self.name)

    class FakeVolumes:
        def get(self, name):
            return FakeVolume(name)

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()
            self.volumes = FakeVolumes()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda _client: None)
    monkeypatch.setattr(m, "_is_host_port_free", lambda _port: True)
    def _wait_for_database_ready(
        _host, _port, _engine, user, password, db_name, *_args,
    ):
        readiness_credentials.append((user, password, db_name))
        if (
            scenario == "member-added-during-readiness"
            and not callback_action_done[0]
        ):
            callback_action_done[0] = True
            m._create_db_instance({
                "DBInstanceIdentifier": "restored-late-reader",
                "DBClusterIdentifier": "restored-shared-cluster",
                "DBInstanceClass": "db.r6g.large",
                "Engine": "aurora-mysql",
            })
        elif (
            scenario == "last-member-deleted-during-readiness"
            and not callback_action_done[0]
        ):
            callback_action_done[0] = True
            for db_id in ("restored-reader", "restored-writer"):
                del m._instances[db_id]
                m._unregister_instance_from_clusters(db_id)
            restored_cluster = m._clusters["restored-shared-cluster"]
            m._stop_empty_cluster_shared_container(
                "restored-shared-cluster", restored_cluster,
            )
        return database_ready

    def _rotate(_cluster, old_password, new_password):
        rotations.append((old_password, new_password))
        return True

    def _grant(_host, _port, user, password, db_id):
        grants.append((user, password, db_id))

    monkeypatch.setattr(m, "_wait_for_database_ready", _wait_for_database_ready)
    monkeypatch.setattr(m, "_rotate_real_password", _rotate)
    monkeypatch.setattr(m, "_grant_mysql_master_user_privileges", _grant)

    account_id = get_account_id()
    region = get_region()
    cluster_id = "restored-shared-cluster"
    legacy_container_names = {
        m._legacy_scoped_rds_docker_name(
            db_id, account_id, region,
        )
        for db_id in ("restored-writer", "restored-reader")
    }
    remaining_legacy_names.update(legacy_container_names)
    legacy_owner_by_name.update({
        m._legacy_scoped_rds_docker_name(db_id, account_id, region): db_id
        for db_id in ("restored-writer", "restored-reader")
    })
    writer_legacy_container_name[0] = m._legacy_scoped_rds_docker_name(
        "restored-writer", account_id, region,
    )
    reader_legacy_container_name[0] = m._legacy_scoped_rds_docker_name(
        "restored-reader", account_id, region,
    )
    clusters = AccountRegionScopedDict()
    cluster_record = {
        "DBClusterIdentifier": cluster_id,
        "Engine": "aurora-mysql",
        "EngineVersion": DEFAULT_AURORA_MYSQL_ENGINE_VERSION,
        "MasterUsername": "cluster-admin",
        "_MasterUserPassword": "cluster-password",
        "DatabaseName": "cluster-db",
        "Port": 3306,
        "HostedZoneId": "Z2R2ITUGPM61AM",
        "DBClusterMembers": [
            {"DBInstanceIdentifier": "restored-writer", "IsClusterWriter": True},
            {"DBInstanceIdentifier": "restored-reader", "IsClusterWriter": False},
        ],
        "_shared_container_id": "stale-container-id",
        "_shared_host_port": 16010,
        "_shared_endpoint": {
            "Address": "localhost",
            "Port": 16010,
            "HostedZoneId": "Z2R2ITUGPM61AM",
        },
    }
    if scenario == "pending-password-rotation":
        cluster_record["_MasterUserPassword"] = "rotated-password"
        cluster_record["_pending_master_password_rotation"] = {
            "old_password": "writer-password",
            "new_password": "rotated-password",
        }
    clusters.set_scoped(account_id, region, cluster_id, cluster_record)
    instances = AccountRegionScopedDict()
    for db_id in ("restored-writer", "restored-reader"):
        instances.set_scoped(account_id, region, db_id, {
            "DBInstanceIdentifier": db_id,
            "DBClusterIdentifier": cluster_id,
            "Engine": "aurora-mysql",
            "EngineVersion": DEFAULT_AURORA_MYSQL_ENGINE_VERSION,
            "MasterUsername": (
                "writer-admin" if db_id == "restored-writer" else "reader-admin"
            ),
            "_MasterUserPassword": (
                "writer-password" if db_id == "restored-writer" else "reader-password"
            ),
            "DBName": "writer-db" if db_id == "restored-writer" else "reader-db",
            "DBInstanceStatus": "available",
            "Endpoint": {"Address": "localhost", "Port": 16010},
            "_docker_container_id": "stale-container-id",
            "_docker_volume_name": f"legacy-{db_id}-volume",
        })

    m._instances.clear()
    m._clusters.clear()
    try:
        m.restore_state({
            "instances": instances,
            "clusters": clusters,
            "subnet_groups": {},
            "param_groups": {},
            "snapshots": {},
            "db_cluster_param_groups": {},
            "db_cluster_snapshots": {},
            "option_groups": {},
            "global_clusters": {},
            "tags": {},
            "port_counter": 16010,
        })

        if scenario == "last-members-deleted-before-start":
            assert migration_remove_started.wait(timeout=1)
            for db_id in ("restored-reader", "restored-writer"):
                del m._instances[db_id]
                m._unregister_instance_from_clusters(db_id)
            restored_cluster = m._clusters[cluster_id]
            m._stop_empty_cluster_shared_container(
                cluster_id,
                restored_cluster,
            )
            release_migration_remove.set()
        elif scenario == "member-added-during-migration":
            assert migration_remove_started.wait(timeout=1)
            restored_cluster = m._clusters[cluster_id]
            assert restored_cluster["_shared_legacy_migration_in_progress"] is True
            response = m._create_db_instance({
                "DBInstanceIdentifier": "racing-migration-member",
                "DBClusterIdentifier": cluster_id,
                "DBInstanceClass": "db.r6g.large",
                "Engine": "aurora-mysql",
            })
            assert response[0] == 400
            assert "racing-migration-member" not in m._instances
            assert runs == []
            release_migration_remove.set()

        deadline = time.time() + 2
        while time.time() < deadline:
            if scenario in (
                "last-member-deleted-during-readiness",
                "last-members-deleted-before-start",
            ):
                if m._clusters.get(cluster_id, {}).get("DBClusterMembers") == []:
                    break
                time.sleep(0.01)
                continue
            statuses = {
                m._instances.get(db_id, {}).get("DBInstanceStatus")
                for db_id in ("restored-writer", "restored-reader")
            }
            if statuses <= {"available", "failed"}:
                break
            time.sleep(0.01)

        if not writer_removal_succeeds or not reader_removal_succeeds or (
            scenario == "ownership-mismatch"
        ):
            assert runs == []
            assert readiness_credentials == []
            expected_remaining_name = (
                writer_legacy_container_name[0]
                if not writer_removal_succeeds
                else reader_legacy_container_name[0]
            )
            assert expected_remaining_name in remaining_legacy_names
            assert removed_volumes == []
            restored_cluster = m._clusters.get(cluster_id)
            assert restored_cluster[
                "_shared_legacy_migration_blocked"
            ] is True
            m._create_db_instance({
                "DBInstanceIdentifier": "blocked-migration-member",
                "DBClusterIdentifier": cluster_id,
                "DBInstanceClass": "db.r6g.large",
                "Engine": "aurora-mysql",
            })
            assert "blocked-migration-member" not in m._instances
            assert runs == []
            assert all(
                m._instances.get(db_id)["DBInstanceStatus"] == "failed"
                for db_id in ("restored-writer", "restored-reader")
            )
            return

        if scenario == "last-members-deleted-before-start":
            restored_cluster = m._clusters.get(cluster_id)
            assert runs == []
            assert readiness_credentials == []
            assert restored_cluster["DBClusterMembers"] == []
            assert restored_cluster["_shared_container_ready"] is False
            assert restored_cluster["_shared_container_epoch"] > 0
            assert rotations == []
            assert grants == []
            assert removed_volumes == []
            return

        if scenario == "last-member-deleted-during-readiness":
            restored_cluster = m._clusters.get(cluster_id)
            assert restored_cluster["DBClusterMembers"] == []
            assert restored_cluster["_shared_container_ready"] is False
            assert restored_cluster["_shared_container_epoch"] > 1
            assert rotations == []
            assert grants == []
            assert removed_volumes == []
            return

        assert len(runs) == 1
        assert runs[0]["name"] == m._rds_cluster_docker_name(cluster_id)
        assert runs[0]["environment"]["MYSQL_USER"] == "writer-admin"
        expected_password = (
            "rotated-password"
            if scenario == "pending-password-rotation"
            else "writer-password"
        )
        assert runs[0]["environment"]["MYSQL_ROOT_PASSWORD"] == expected_password
        assert runs[0]["environment"]["MYSQL_DATABASE"] == "writer-db"
        assert readiness_credentials == [
            ("writer-admin", "writer-password", "writer-db"),
        ]
        assert runs[0]["volumes"] == {
            "legacy-restored-writer-volume": {
                "bind": "/var/lib/mysql",
                "mode": "rw",
            },
        }
        assert set(removed_containers) == legacy_container_names
        assert removed_volumes == (
            ["legacy-restored-reader-volume"] if database_ready else []
        )
        restored_cluster = m._clusters.get(cluster_id)
        assert restored_cluster["_shared_container_id"] == "restored-shared-container"
        assert restored_cluster["_shared_volume_name"] == "legacy-restored-writer-volume"
        assert restored_cluster["MasterUsername"] == "writer-admin"
        assert restored_cluster["_MasterUserPassword"] == expected_password
        assert restored_cluster["DatabaseName"] == "writer-db"
        assert restored_cluster["_shared_container_ready"] is database_ready
        assert restored_cluster["Endpoint"] == restored_cluster["ReaderEndpoint"]
        assert rotations == (
            [("writer-password", "rotated-password")]
            if scenario == "pending-password-rotation"
            else []
        )
        assert grants == (
            [("writer-admin", expected_password, cluster_id)]
            if database_ready
            else []
        )
        for db_id in ("restored-writer", "restored-reader"):
            instance = m._instances.get(db_id)
            assert instance["MasterUsername"] == "writer-admin"
            assert instance["_MasterUserPassword"] == expected_password
            assert instance["DBName"] == "writer-db"
            assert instance["_docker_container_id"] == "restored-shared-container"
            assert instance["_shared_cluster_id"] == cluster_id
            assert instance["Endpoint"] == restored_cluster["_shared_endpoint"]
            assert instance["DBInstanceStatus"] == (
                "available" if database_ready else "failed"
            )
        if scenario == "member-added-during-readiness":
            late_member = m._instances.get("restored-late-reader")
            assert late_member["DBInstanceStatus"] == "available"
            assert late_member["_docker_container_id"] == (
                "restored-shared-container"
            )
            assert restored_cluster["Status"] == "available"
    finally:
        m._instances.clear()
        m._clusters.clear()


@pytest.mark.parametrize("cleanup_action", ["delete", "delete-arn", "reset"])
def test_rds_restored_empty_cluster_cleanup_recovers_container_by_name(
    monkeypatch, cleanup_action,
):
    from ministack.services import rds as m

    stopped = []
    removed_containers = []
    removed_volumes = []
    cluster_id = f"restored-empty-{cleanup_action}"
    container_name = m._rds_cluster_docker_name(cluster_id)
    volume_name = m._rds_cluster_docker_volume_name(cluster_id)

    class FakeContainer:
        def stop(self, timeout=5):
            stopped.append(timeout)

        def remove(self, v=False):
            removed_containers.append(v)

    class FakeContainers:
        def get(self, identifier):
            if identifier == container_name:
                return FakeContainer()
            raise Exception("not found")

    class FakeVolume:
        def remove(self):
            removed_volumes.append(volume_name)

    class FakeVolumes:
        def get(self, name):
            assert name == volume_name
            return FakeVolume()

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()
            self.volumes = FakeVolumes()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    m._instances.clear()
    m._clusters.clear()
    try:
        m._create_db_cluster({
            "DBClusterIdentifier": cluster_id,
            "Engine": "aurora-mysql",
            "MasterUsername": "admin",
            "MasterUserPassword": "password123",
        })
        cluster = m._clusters.get(cluster_id)
        cluster.update({
            "_shared_container_id": "unrestorable-container-id",
            "_shared_endpoint": {
                "Address": "localhost",
                "Port": 16022,
                "HostedZoneId": "Z2R2ITUGPM61AM",
            },
            "_shared_volume_name": volume_name,
            "_shared_container_ready": False,
        })
        persisted = m.get_state()
        m._clusters.clear()
        m.restore_state(persisted)

        restored = m._clusters.get(cluster_id)
        assert restored["DBClusterMembers"] == []
        assert restored["_shared_container_id"] is None
        if cleanup_action in ("delete", "delete-arn"):
            cluster_identifier = (
                restored["DBClusterArn"]
                if cleanup_action == "delete-arn"
                else cluster_id
            )
            status, _, _ = m._delete_db_cluster({
                "DBClusterIdentifier": cluster_identifier,
                "SkipFinalSnapshot": "true",
            })
            assert status == 200
        else:
            m.reset()

        assert stopped == [5 if cleanup_action in ("delete", "delete-arn") else 2]
        assert removed_containers == [True]
        assert removed_volumes == [volume_name]
    finally:
        m._instances.clear()
        m._clusters.clear()


def test_rds_reset_removes_shared_container_once(monkeypatch):
    """Reset reaps cluster-owned containers and volumes once."""
    from ministack.services import rds as m

    stop_calls = []
    remove_calls = []
    removed_volumes = []

    class FakeContainer:
        def stop(self, timeout=2):
            stop_calls.append(timeout)

        def remove(self, v=False):
            remove_calls.append(v)

    class FakeContainers:
        def get(self, identifier):
            assert identifier == "shared-reset-container"
            return FakeContainer()

    class FakeVolume:
        def remove(self):
            removed_volumes.append("shared-reset-volume")

    class FakeVolumes:
        def get(self, name):
            assert name == "shared-reset-volume"
            return FakeVolume()

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()
            self.volumes = FakeVolumes()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    m._instances.clear()
    m._clusters.clear()
    try:
        m._clusters["reset-cluster"] = {
            "DBClusterIdentifier": "reset-cluster",
            "_shared_container_id": "shared-reset-container",
            "_shared_volume_name": "shared-reset-volume",
        }
        for db_id in ("reset-writer", "reset-reader"):
            m._instances[db_id] = {
                "DBInstanceIdentifier": db_id,
                "_docker_container_id": "shared-reset-container",
                "_shared_cluster_id": "reset-cluster",
            }

        m.reset()

        assert stop_calls == [2]
        assert remove_calls == [True]
        assert removed_volumes == ["shared-reset-volume"]
        assert not m._instances
        assert not m._clusters
    finally:
        m._instances.clear()
        m._clusters.clear()


def test_rds_reset_uses_each_clusters_account_and_region(monkeypatch):
    from ministack.services import rds as m

    foreign_account = "111122223333"
    foreign_region = "us-west-2"
    cluster_id = "foreign-empty-cluster"
    container_name = m._rds_cluster_docker_name(
        cluster_id, foreign_account, foreign_region,
    )
    volume_name = m._rds_cluster_docker_volume_name(
        cluster_id, foreign_account, foreign_region,
    )
    container_lookups = []
    volume_lookups = []
    stop_calls = []
    remove_calls = []

    class FakeContainer:
        def stop(self, timeout=2):
            stop_calls.append(timeout)

        def remove(self, v=False):
            remove_calls.append(v)

    class FakeContainers:
        def get(self, identifier):
            container_lookups.append(identifier)
            if identifier == container_name:
                return FakeContainer()
            raise Exception("not found")

    class FakeVolume:
        def remove(self):
            pass

    class FakeVolumes:
        def get(self, name):
            volume_lookups.append(name)
            if name == volume_name:
                return FakeVolume()
            raise Exception("not found")

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()
            self.volumes = FakeVolumes()

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    m._instances.clear()
    m._clusters.clear()
    try:
        m._clusters.set_scoped(
            foreign_account,
            foreign_region,
            cluster_id,
            {
                "DBClusterIdentifier": cluster_id,
                "DBClusterMembers": [],
                "_shared_container_id": None,
                "_shared_endpoint": {
                    "Address": "localhost",
                    "Port": 16040,
                },
                "_shared_volume_name": None,
            },
        )

        m.reset()

        assert container_lookups == [container_name]
        assert volume_lookups == [volume_name]
        assert stop_calls == [2]
        assert remove_calls == [True]
        assert not m._clusters.has_any()
    finally:
        m._instances.clear()
        m._clusters.clear()


def test_rds_host_port_probe_rejects_loopback_listener():
    """A loopback listener must not be mistaken for a reusable Docker port."""
    import socket

    from ministack.services import rds as m

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    try:
        assert not m._is_host_port_free(listener.getsockname()[1])
    finally:
        listener.close()


def test_rds_container_names_separate_instances_from_clusters():
    from ministack.services import rds as m

    assert m._rds_docker_name("cluster-orders") != m._rds_cluster_docker_name(
        "orders",
    )
    assert "-instance-cluster-orders" in m._rds_docker_name("cluster-orders")
    assert "-cluster-orders" in m._rds_cluster_docker_name("orders")


def test_rds_cluster_volume_name_cannot_match_legacy_instance():
    from ministack.services import rds as m

    assert m._rds_cluster_docker_volume_name(
        "orders",
    ) != m._legacy_scoped_rds_docker_volume_name("cluster-orders")
    assert "ministack-rds-cluster-" in m._rds_cluster_docker_volume_name(
        "orders",
    )


def test_rds_restore_migrates_legacy_instance_name_before_cluster_claims_it(
    monkeypatch,
):
    from ministack.core.responses import AccountRegionScopedDict, get_account_id, get_region
    from ministack.services import rds as m

    account_id = get_account_id()
    region = get_region()
    cluster_id = "orders"
    standalone_id = "cluster-orders"
    collision_name = m._legacy_scoped_rds_docker_name(
        standalone_id, account_id, region,
    )
    assert collision_name == m._rds_cluster_docker_name(
        cluster_id, account_id, region,
    )
    containers = {}
    removed = []
    runs = []

    class FakeContainer:
        def __init__(self, name, container_id, labels=None):
            self.name = name
            self.id = container_id
            self.labels = labels or {}
            self.attrs = {
                "Config": {"Labels": self.labels},
                "NetworkSettings": {"Networks": {}},
            }

        def reload(self):
            pass

        def remove(self, force=False, v=False):
            removed.append((self.name, self.id, force, v))
            containers.pop(self.name, None)
            containers.pop(self.id, None)

    legacy = FakeContainer(
        collision_name,
        "legacy-standalone-container",
        labels={
            "ministack": "rds",
            "db_id": standalone_id,
            "account_id": account_id,
            "region": region,
        },
    )
    containers[legacy.name] = legacy
    containers[legacy.id] = legacy

    class FakeContainers:
        def get(self, identifier):
            if identifier not in containers:
                raise Exception("not found")
            return containers[identifier]

        def run(self, **kwargs):
            container = FakeContainer(
                kwargs["name"], f"new-container-{len(runs)}",
            )
            runs.append(kwargs)
            containers[container.name] = container
            containers[container.id] = container
            return container

    class FakeDocker:
        def __init__(self):
            self.containers = FakeContainers()

    class ImmediateThread:
        def __init__(self, target, args=(), **_kwargs):
            self.target = target
            self.args = args

        def start(self):
            self.target(*self.args)

    monkeypatch.setattr(m, "_get_docker", lambda: FakeDocker())
    monkeypatch.setattr(m, "_get_ministack_network", lambda _client: None)
    monkeypatch.setattr(m, "_is_host_port_free", lambda _port: True)
    monkeypatch.setattr(m.threading, "Thread", ImmediateThread)

    clusters = AccountRegionScopedDict()
    clusters.set_scoped(account_id, region, cluster_id, {
        "DBClusterIdentifier": cluster_id,
        "DBClusterArn": f"arn:aws:rds:{region}:{account_id}:cluster:{cluster_id}",
        "Engine": "aurora-mysql",
        "EngineVersion": DEFAULT_AURORA_MYSQL_ENGINE_VERSION,
        "MasterUsername": "admin",
        "_MasterUserPassword": "password123",
        "DatabaseName": "mydb",
        "Port": 3306,
        "DBClusterMembers": [
            {"DBInstanceIdentifier": "orders-writer", "IsClusterWriter": True},
        ],
    })
    instances = AccountRegionScopedDict()
    for db_id, parent_id in (
        ("orders-writer", cluster_id),
        (standalone_id, ""),
    ):
        instances.set_scoped(account_id, region, db_id, {
            "DBInstanceIdentifier": db_id,
            "DBClusterIdentifier": parent_id,
            "DBInstanceArn": f"arn:aws:rds:{region}:{account_id}:db:{db_id}",
            "Engine": "aurora-mysql" if parent_id else "mysql",
            "EngineVersion": "8.0",
            "MasterUsername": "admin",
            "_MasterUserPassword": "password123",
            "DBName": "mydb",
            "DBInstanceStatus": "available",
            "Endpoint": {"Address": "localhost", "Port": 16030},
        })

    m._instances.clear()
    m._clusters.clear()
    try:
        m.restore_state({
            "instances": instances,
            "clusters": clusters,
            "subnet_groups": {},
            "param_groups": {},
            "snapshots": {},
            "db_cluster_param_groups": {},
            "db_cluster_snapshots": {},
            "option_groups": {},
            "global_clusters": {},
            "tags": {},
            "port_counter": 16030,
        })

        assert removed == [
            (collision_name, "legacy-standalone-container", True, False),
        ]
        assert {run["name"] for run in runs} == {
            m._rds_cluster_docker_name(cluster_id),
            m._rds_docker_name(standalone_id),
        }
        assert containers[collision_name].id != "legacy-standalone-container"
        assert containers[collision_name].name == m._rds_cluster_docker_name(
            cluster_id,
        )
    finally:
        m._instances.clear()
        m._clusters.clear()


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

    stale_name = m._legacy_scoped_rds_docker_name("stale-db")
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
    assert runs[0]["name"] == m._rds_docker_name("stale-db")

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


# ===========================================================================
# Region scoping, ARN adoption, password rotation, and live Aurora shared-
# storage. Folded from test_rds_{regions,arn_adoption,password_rotation,
# aurora_cluster_integration}.py. The 3 tests that were duplicated across
# regions+arn_adoption keep the arn_adoption (superset) versions; aurora
# tests carry a per-test DOCKER_NETWORK skip (was a module-level pytestmark).
# ===========================================================================


def _regional_rds(region, access_key_id="test"):
    return boto3.client(
        "rds",
        endpoint_url=ENDPOINT,
        aws_access_key_id=access_key_id,
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


def _delete_cluster(client, cluster_id):
    try:
        client.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
    except ClientError:
        pass


def _delete_instance(client, instance_id):
    try:
        client.delete_db_instance(DBInstanceIdentifier=instance_id, SkipFinalSnapshot=True)
    except ClientError:
        pass


def _remove_global_member(client, global_id, cluster_id):
    try:
        client.remove_from_global_cluster(
            GlobalClusterIdentifier=global_id,
            DbClusterIdentifier=cluster_id,
        )
    except ClientError:
        pass


def _delete_global_cluster(client, global_id):
    try:
        client.modify_global_cluster(
            GlobalClusterIdentifier=global_id,
            DeletionProtection=False,
        )
    except ClientError:
        pass
    try:
        client.delete_global_cluster(GlobalClusterIdentifier=global_id)
    except ClientError:
        pass


def _cleanup_two_member_global(east, global_id, primary_arn=None, secondary_arn=None):
    if primary_arn:
        try:
            east.switchover_global_cluster(
                GlobalClusterIdentifier=global_id,
                TargetDbClusterIdentifier=primary_arn,
            )
        except ClientError:
            pass
    for cluster_arn in (secondary_arn, primary_arn):
        if cluster_arn:
            _remove_global_member(east, global_id, cluster_arn)
    _delete_global_cluster(east, global_id)


def test_rds_clusters_are_region_scoped():
    east = _regional_rds("us-east-1")
    west = _regional_rds("us-west-2")
    east_only = f"rds-east-only-{uuid.uuid4().hex[:8]}"
    shared = f"rds-shared-{uuid.uuid4().hex[:8]}"

    try:
        east.create_db_cluster(
            DBClusterIdentifier=east_only,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        with pytest.raises(ClientError) as exc:
            west.describe_db_clusters(DBClusterIdentifier=east_only)
        assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"

        east.create_db_cluster(
            DBClusterIdentifier=shared,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
            DatabaseName="eastdb",
        )
        west.create_db_cluster(
            DBClusterIdentifier=shared,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
            DatabaseName="westdb",
        )

        east_cluster = east.describe_db_clusters(DBClusterIdentifier=shared)["DBClusters"][0]
        west_cluster = west.describe_db_clusters(DBClusterIdentifier=shared)["DBClusters"][0]
        assert east_cluster["DBClusterArn"] != west_cluster["DBClusterArn"]
        assert ":us-east-1:" in east_cluster["DBClusterArn"]
        assert ":us-west-2:" in west_cluster["DBClusterArn"]
        assert east_cluster["DatabaseName"] == "eastdb"
        assert west_cluster["DatabaseName"] == "westdb"
    finally:
        for client, cluster_id in (
            (east, east_only),
            (east, shared),
            (west, shared),
        ):
            _delete_cluster(client, cluster_id)


def test_rds_cluster_arn_lookup_rejects_foreign_account():
    account_a = _regional_rds("us-west-2", access_key_id="111111111111")
    account_b = _regional_rds("us-west-2", access_key_id="222222222222")
    cluster_id = f"rds-cross-account-{uuid.uuid4().hex[:8]}"

    try:
        cluster = account_a.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]

        same_account = account_a.describe_db_clusters(
            DBClusterIdentifier=cluster["DBClusterArn"],
        )["DBClusters"][0]
        assert same_account["DBClusterIdentifier"] == cluster_id

        with pytest.raises(ClientError) as exc:
            account_b.describe_db_clusters(DBClusterIdentifier=cluster["DBClusterArn"])
        assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"
    finally:
        _delete_cluster(account_a, cluster_id)


def test_rds_regional_cluster_apis_reject_foreign_region_arns():
    east = _regional_rds("us-east-1")
    west = _regional_rds("us-west-2")
    cluster_id = f"rds-foreign-region-{uuid.uuid4().hex[:8]}"
    instance_id = f"rds-foreign-region-{uuid.uuid4().hex[:8]}"
    global_id = f"global-foreign-region-{uuid.uuid4().hex[:8]}"

    try:
        cluster = west.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        cluster_arn = cluster["DBClusterArn"]

        same_region = west.describe_db_clusters(
            DBClusterIdentifier=cluster_arn,
        )["DBClusters"][0]
        assert same_region["DBClusterIdentifier"] == cluster_id

        with pytest.raises(ClientError) as exc:
            east.describe_db_clusters(DBClusterIdentifier=cluster_arn)
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            east.modify_db_cluster(
                DBClusterIdentifier=cluster_arn,
                BackupRetentionPeriod=1,
                ApplyImmediately=True,
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            east.delete_db_cluster(DBClusterIdentifier=cluster_arn, SkipFinalSnapshot=True)
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            east.enable_http_endpoint(ResourceArn=cluster_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundFault"

        with pytest.raises(ClientError) as exc:
            east.create_db_instance(
                DBInstanceIdentifier=instance_id,
                DBClusterIdentifier=cluster_arn,
                DBInstanceClass="db.t3.micro",
                Engine="aurora-mysql",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            east.create_global_cluster(
                GlobalClusterIdentifier=global_id,
                SourceDBClusterIdentifier=cluster_arn,
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            west.describe_db_instances(DBInstanceIdentifier=instance_id)
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"
    finally:
        _delete_global_cluster(east, global_id)
        _delete_instance(west, instance_id)
        _delete_cluster(west, cluster_id)


def test_rds_instances_are_region_scoped():
    east = _regional_rds("us-east-1")
    west = _regional_rds("us-west-2")
    shared = f"rds-inst-shared-{uuid.uuid4().hex[:8]}"

    try:
        east.create_db_instance(
            DBInstanceIdentifier=shared,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername="admin",
            MasterUserPassword="pass",
            AllocatedStorage=10,
        )
        west.create_db_instance(
            DBInstanceIdentifier=shared,
            DBInstanceClass="db.t3.small",
            Engine="postgres",
            MasterUsername="admin",
            MasterUserPassword="pass",
            AllocatedStorage=20,
        )

        east_instance = east.describe_db_instances(DBInstanceIdentifier=shared)["DBInstances"][0]
        west_instance = west.describe_db_instances(DBInstanceIdentifier=shared)["DBInstances"][0]
        assert east_instance["DBInstanceArn"] != west_instance["DBInstanceArn"]
        assert ":us-east-1:" in east_instance["DBInstanceArn"]
        assert ":us-west-2:" in west_instance["DBInstanceArn"]
        assert east_instance["DBInstanceClass"] == "db.t3.micro"
        assert west_instance["DBInstanceClass"] == "db.t3.small"
    finally:
        _delete_instance(east, shared)
        _delete_instance(west, shared)


def test_rds_regional_instance_apis_reject_foreign_region_arns():
    east = _regional_rds("us-east-1")
    west = _regional_rds("us-west-2")
    instance_id = f"rds-inst-arn-{uuid.uuid4().hex[:8]}"
    snapshot_id = f"rds-inst-arn-snap-{uuid.uuid4().hex[:8]}"

    try:
        instance = west.create_db_instance(
            DBInstanceIdentifier=instance_id,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername="admin",
            MasterUserPassword="pass",
            AllocatedStorage=10,
        )["DBInstance"]
        instance_arn = instance["DBInstanceArn"]

        same_region = west.describe_db_instances(DBInstanceIdentifier=instance_arn)["DBInstances"][0]
        assert same_region["DBInstanceIdentifier"] == instance_id

        with pytest.raises(ClientError) as exc:
            east.describe_db_instances(DBInstanceIdentifier=instance_arn)
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            east.modify_db_instance(
                DBInstanceIdentifier=instance_arn,
                DBInstanceClass="db.t3.small",
                ApplyImmediately=True,
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            east.create_db_snapshot(
                DBSnapshotIdentifier=snapshot_id,
                DBInstanceIdentifier=instance_arn,
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            east.delete_db_instance(DBInstanceIdentifier=instance_arn, SkipFinalSnapshot=True)
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"
    finally:
        try:
            west.delete_db_instance(DBInstanceIdentifier=instance_id, SkipFinalSnapshot=True)
        except ClientError:
            pass


def test_rds_legacy_instance_restore_preserves_arn_region(monkeypatch):
    from ministack.core.responses import AccountScopedDict, get_region, set_request_region
    from ministack.services import rds

    class ImmediateThread:
        def __init__(self, target, args=(), daemon=None):
            self.target = target
            self.args = args

        def start(self):
            self.target(*self.args)

    original_region = get_region()
    instance_id = f"rds-restore-{uuid.uuid4().hex[:8]}"
    instance = {
        "DBInstanceIdentifier": instance_id,
        "DBInstanceArn": f"arn:aws:rds:us-west-2:000000000000:db:{instance_id}",
    }
    legacy = AccountScopedDict()
    legacy.set_scoped("000000000000", "us-east-1", instance_id, instance)

    monkeypatch.setattr(rds, "_get_docker", lambda: None)
    monkeypatch.setattr(rds.threading, "Thread", ImmediateThread)

    try:
        rds.reset()
        rds.restore_state({"instances": legacy})

        assert rds._instances.get_scoped("000000000000", "us-east-1", instance_id) is None
        restored = rds._instances.get_scoped("000000000000", "us-west-2", instance_id)
        assert restored["DBInstanceArn"] == instance["DBInstanceArn"]
        assert restored["DBInstanceStatus"] == "available"
    finally:
        rds.reset()
        set_request_region(original_region)


def test_rds_docker_artifact_names_are_region_scoped():
    from ministack.core.responses import get_region, set_request_region
    from ministack.services import rds

    original_region = get_region()
    try:
        set_request_region("us-east-1")
        east_name = rds._rds_docker_name("shared-db")
        east_volume = rds._rds_docker_volume_name("shared-db")

        set_request_region("us-west-2")
        west_name = rds._rds_docker_name("shared-db")
        west_volume = rds._rds_docker_volume_name("shared-db")

        assert east_name != west_name
        assert east_volume != west_volume
        assert east_name.endswith("-shared-db")
        assert west_name.endswith("-shared-db")
    finally:
        set_request_region(original_region)


def test_create_db_cluster_first_global_member_is_writer():
    east = _regional_rds("us-east-1")
    suffix = uuid.uuid4().hex[:8]
    global_id = f"global-empty-{suffix}"
    cluster_id = f"global-first-{suffix}"

    try:
        east.create_global_cluster(
            GlobalClusterIdentifier=global_id,
            Engine="aurora-mysql",
        )
        cluster = east.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            GlobalClusterIdentifier=global_id,
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]

        global_cluster = east.describe_global_clusters(
            GlobalClusterIdentifier=global_id,
        )["GlobalClusters"][0]
        members = {m["DBClusterArn"]: m for m in global_cluster["GlobalClusterMembers"]}
        assert members[cluster["DBClusterArn"]]["IsWriter"] is True
    finally:
        _remove_global_member(east, global_id, cluster_id)
        _delete_cluster(east, cluster_id)
        _delete_global_cluster(east, global_id)


def test_create_db_cluster_validates_global_cluster_identifier_and_engine():
    east = _regional_rds("us-east-1")
    suffix = uuid.uuid4().hex[:8]
    global_id = f"global-validate-{suffix}"
    global_arn_id = f"arn:aws:rds::000000000000:global-cluster:{global_id}"
    cluster_id = f"global-validate-member-{suffix}"

    try:
        global_cluster = east.create_global_cluster(
            GlobalClusterIdentifier=global_id,
            Engine="aurora-postgresql",
            EngineVersion="15.3",
        )["GlobalCluster"]

        with pytest.raises(ClientError) as exc:
            east.create_db_cluster(
                DBClusterIdentifier=f"{cluster_id}-arn",
                Engine="aurora-postgresql",
                GlobalClusterIdentifier=global_arn_id,
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            east.create_db_cluster(
                DBClusterIdentifier=f"{cluster_id}-engine",
                Engine="aurora-mysql",
                GlobalClusterIdentifier=global_id,
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            east.create_db_cluster(
                DBClusterIdentifier=f"{cluster_id}-version",
                Engine="aurora-postgresql",
                EngineVersion="14.8",
                GlobalClusterIdentifier=global_id,
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        member = east.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-postgresql",
            GlobalClusterIdentifier=global_id,
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        assert member["Engine"] == global_cluster["Engine"]
        assert member["EngineVersion"] == global_cluster["EngineVersion"]

        with pytest.raises(ClientError) as exc:
            east.create_db_cluster(
                DBClusterIdentifier=f"{cluster_id}-same-region",
                Engine="aurora-postgresql",
                GlobalClusterIdentifier=global_id,
                MasterUsername="admin",
                MasterUserPassword="password123",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"
    finally:
        _remove_global_member(east, global_id, cluster_id)
        _delete_cluster(east, cluster_id)
        _delete_global_cluster(east, global_id)


def test_aurora_global_metadata_spans_regions():
    east = _regional_rds("us-east-1")
    west = _regional_rds("us-west-2")
    suffix = uuid.uuid4().hex[:8]
    primary_id = f"global-primary-{suffix}"
    secondary_id = f"global-secondary-{suffix}"
    global_id = f"global-metadata-{suffix}"

    try:
        primary = east.create_db_cluster(
            DBClusterIdentifier=primary_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        east.create_global_cluster(
            GlobalClusterIdentifier=global_id,
            SourceDBClusterIdentifier=primary["DBClusterArn"],
            DeletionProtection=True,
        )

        primary_after_attach = east.describe_db_clusters(
            DBClusterIdentifier=primary_id,
        )["DBClusters"][0]
        assert primary_after_attach["GlobalClusterIdentifier"] == global_id

        west.create_db_cluster(
            DBClusterIdentifier=secondary_id,
            Engine="aurora-mysql",
            GlobalClusterIdentifier=global_id,
            KmsKeyId="alias/aws/rds",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )
        secondary = west.describe_db_clusters(
            DBClusterIdentifier=secondary_id,
        )["DBClusters"][0]
        assert secondary["GlobalClusterIdentifier"] == global_id
        assert secondary["KmsKeyId"] == "alias/aws/rds"

        east_global = east.describe_global_clusters(
            GlobalClusterIdentifier=global_id,
        )["GlobalClusters"][0]
        west_global = west.describe_global_clusters(
            GlobalClusterIdentifier=global_id,
        )["GlobalClusters"][0]
        assert east_global == west_global
        assert east_global["DeletionProtection"] is True

        by_arn = {m["DBClusterArn"]: m for m in east_global["GlobalClusterMembers"]}
        assert set(by_arn) == {primary["DBClusterArn"], secondary["DBClusterArn"]}
        assert by_arn[primary["DBClusterArn"]]["IsWriter"] is True
        assert by_arn[secondary["DBClusterArn"]]["IsWriter"] is False
        assert by_arn[secondary["DBClusterArn"]]["SynchronizationStatus"] == "connected"
        assert by_arn[secondary["DBClusterArn"]]["GlobalWriteForwardingStatus"] == "disabled"

        with pytest.raises(ClientError) as exc:
            west.delete_db_cluster(DBClusterIdentifier=secondary_id, SkipFinalSnapshot=True)
        assert exc.value.response["Error"]["Code"] == "InvalidDBClusterStateFault"

        east.modify_global_cluster(GlobalClusterIdentifier=global_id, DeletionProtection=False)
        east.modify_db_cluster(DBClusterIdentifier=primary_id, DeletionProtection=True)
        primary_modified = east.describe_db_clusters(
            DBClusterIdentifier=primary_id,
        )["DBClusters"][0]
        assert primary_modified["DeletionProtection"] is True
        east.modify_db_cluster(DBClusterIdentifier=primary_id, DeletionProtection=False)

        with pytest.raises(ClientError) as exc:
            east.delete_global_cluster(GlobalClusterIdentifier=global_id)
        assert exc.value.response["Error"]["Code"] == "InvalidGlobalClusterStateFault"

        with pytest.raises(ClientError) as exc:
            west.remove_from_global_cluster(
                GlobalClusterIdentifier=global_id,
                DbClusterIdentifier=primary["DBClusterArn"],
            )
        assert exc.value.response["Error"]["Code"] == "InvalidGlobalClusterStateFault"

        east.remove_from_global_cluster(
            GlobalClusterIdentifier=global_id,
            DbClusterIdentifier=secondary["DBClusterArn"],
        )
        secondary_after_detach = west.describe_db_clusters(
            DBClusterIdentifier=secondary_id,
        )["DBClusters"][0]
        assert "GlobalClusterIdentifier" not in secondary_after_detach
        remaining = east.describe_global_clusters(
            GlobalClusterIdentifier=global_id,
        )["GlobalClusters"][0]["GlobalClusterMembers"]
        assert len(remaining) == 1
        west.delete_db_cluster(DBClusterIdentifier=secondary_id, SkipFinalSnapshot=True)

        east.remove_from_global_cluster(
            GlobalClusterIdentifier=global_id,
            DbClusterIdentifier=primary["DBClusterArn"],
        )
        empty_global = east.describe_global_clusters(
            GlobalClusterIdentifier=global_id,
        )["GlobalClusters"][0]
        assert empty_global["GlobalClusterMembers"] == []
        east.delete_global_cluster(GlobalClusterIdentifier=global_id)
        east.delete_db_cluster(DBClusterIdentifier=primary_id, SkipFinalSnapshot=True)
    finally:
        _remove_global_member(west, global_id, secondary_id)
        _remove_global_member(east, global_id, primary_id)
        _delete_global_cluster(east, global_id)
        _delete_cluster(west, secondary_id)
        _delete_cluster(east, primary_id)


def test_switchover_global_cluster_promotes_foreign_region_member_arn():
    east = _regional_rds("us-east-1")
    west = _regional_rds("us-west-2")
    suffix = uuid.uuid4().hex[:8]
    primary_id = f"global-switch-primary-{suffix}"
    secondary_id = f"global-switch-secondary-{suffix}"
    global_id = f"global-switch-{suffix}"
    primary_arn = None
    secondary_arn = None

    try:
        primary = east.create_db_cluster(
            DBClusterIdentifier=primary_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        primary_arn = primary["DBClusterArn"]
        east.create_global_cluster(
            GlobalClusterIdentifier=global_id,
            SourceDBClusterIdentifier=primary_arn,
        )
        secondary = west.create_db_cluster(
            DBClusterIdentifier=secondary_id,
            Engine="aurora-mysql",
            GlobalClusterIdentifier=global_id,
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        secondary_arn = secondary["DBClusterArn"]

        response = east.switchover_global_cluster(
            GlobalClusterIdentifier=global_id,
            TargetDbClusterIdentifier=secondary_arn,
        )["GlobalCluster"]
        assert response["Status"] == "switching-over"
        assert response["FailoverState"]["Status"] == "pending"
        assert response["FailoverState"]["FromDbClusterArn"] == primary_arn
        assert response["FailoverState"]["ToDbClusterArn"] == secondary_arn
        assert response["FailoverState"]["IsDataLossAllowed"] is False

        members = {m["DBClusterArn"]: m for m in response["GlobalClusterMembers"]}
        assert members[primary_arn]["IsWriter"] is False
        assert members[secondary_arn]["IsWriter"] is True
        assert members[secondary_arn]["Readers"] == [primary_arn]

        final = west.describe_global_clusters(
            GlobalClusterIdentifier=global_id,
        )["GlobalClusters"][0]
        final_members = {m["DBClusterArn"]: m for m in final["GlobalClusterMembers"]}
        assert final["Status"] == "available"
        assert "FailoverState" not in final
        assert final_members[primary_arn]["IsWriter"] is False
        assert final_members[secondary_arn]["IsWriter"] is True

        switchback = west.switchover_global_cluster(
            GlobalClusterIdentifier=global_id,
            TargetDbClusterIdentifier=primary_arn,
        )["GlobalCluster"]
        switchback_members = {
            m["DBClusterArn"]: m for m in switchback["GlobalClusterMembers"]
        }
        assert switchback_members[primary_arn]["IsWriter"] is True
        assert switchback_members[secondary_arn]["IsWriter"] is False
    finally:
        _cleanup_two_member_global(east, global_id, primary_arn, secondary_arn)
        _delete_cluster(west, secondary_id)
        _delete_cluster(east, primary_id)


def test_failover_global_cluster_allows_data_loss_promotes_target():
    east = _regional_rds("us-east-1")
    west = _regional_rds("us-west-2")
    suffix = uuid.uuid4().hex[:8]
    primary_id = f"global-fail-primary-{suffix}"
    secondary_id = f"global-fail-secondary-{suffix}"
    global_id = f"global-fail-{suffix}"
    primary_arn = None
    secondary_arn = None

    try:
        primary = east.create_db_cluster(
            DBClusterIdentifier=primary_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        primary_arn = primary["DBClusterArn"]
        east.create_global_cluster(
            GlobalClusterIdentifier=global_id,
            SourceDBClusterIdentifier=primary_arn,
        )
        secondary = west.create_db_cluster(
            DBClusterIdentifier=secondary_id,
            Engine="aurora-mysql",
            GlobalClusterIdentifier=global_id,
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        secondary_arn = secondary["DBClusterArn"]

        response = east.failover_global_cluster(
            GlobalClusterIdentifier=global_id,
            TargetDbClusterIdentifier=secondary_arn,
            AllowDataLoss=True,
        )["GlobalCluster"]
        assert response["Status"] == "failing-over"
        assert response["FailoverState"]["Status"] == "pending"
        assert response["FailoverState"]["IsDataLossAllowed"] is True
        members = {m["DBClusterArn"]: m for m in response["GlobalClusterMembers"]}
        assert members[primary_arn]["IsWriter"] is False
        assert members[secondary_arn]["IsWriter"] is True

        with pytest.raises(ClientError) as exc:
            east.failover_global_cluster(
                GlobalClusterIdentifier=global_id,
                TargetDbClusterIdentifier=primary_arn,
                AllowDataLoss=True,
                Switchover=True,
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"

        with pytest.raises(ClientError) as exc:
            east.failover_global_cluster(
                GlobalClusterIdentifier=global_id,
                TargetDbClusterIdentifier=primary_arn,
                AllowDataLoss=False,
                Switchover=True,
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
    finally:
        _cleanup_two_member_global(east, global_id, primary_arn, secondary_arn)
        _delete_cluster(west, secondary_id)
        _delete_cluster(east, primary_id)


def test_failover_global_cluster_missing_global_validated_before_parameter_combo():
    east = _regional_rds("us-east-1")
    with pytest.raises(ClientError) as exc:
        east.failover_global_cluster(
            GlobalClusterIdentifier=f"missing-global-{uuid.uuid4().hex[:8]}",
            TargetDbClusterIdentifier="arn:aws:rds:us-east-1:000000000000:cluster:missing-secondary",
            AllowDataLoss=True,
            Switchover=True,
        )
    assert exc.value.response["Error"]["Code"] == "GlobalClusterNotFoundFault"


def test_create_global_cluster_rejects_already_attached_source_cluster():
    east = _regional_rds("us-east-1")
    suffix = uuid.uuid4().hex[:8]
    cluster_id = f"global-reuse-source-{suffix}"
    first_global_id = f"global-reuse-first-{suffix}"
    second_global_id = f"global-reuse-second-{suffix}"

    try:
        cluster = east.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        east.create_global_cluster(
            GlobalClusterIdentifier=first_global_id,
            SourceDBClusterIdentifier=cluster["DBClusterArn"],
        )

        with pytest.raises(ClientError) as exc:
            east.create_global_cluster(
                GlobalClusterIdentifier=second_global_id,
                SourceDBClusterIdentifier=cluster["DBClusterArn"],
            )
        assert exc.value.response["Error"]["Code"] == "InvalidDBClusterStateFault"

        first_global = east.describe_global_clusters(
            GlobalClusterIdentifier=first_global_id,
        )["GlobalClusters"][0]
        assert [m["DBClusterArn"] for m in first_global["GlobalClusterMembers"]] == [
            cluster["DBClusterArn"],
        ]
        with pytest.raises(ClientError):
            east.describe_global_clusters(GlobalClusterIdentifier=second_global_id)
    finally:
        _remove_global_member(east, first_global_id, cluster_id)
        _delete_global_cluster(east, first_global_id)
        _delete_global_cluster(east, second_global_id)
        _delete_cluster(east, cluster_id)


def test_aurora_engine_versions_advertise_global_database_support():
    rds = _regional_rds("us-east-1")

    resp = rds.describe_db_engine_versions(Engine="aurora-mysql")
    assert resp["DBEngineVersions"]
    assert all(v["SupportsGlobalDatabases"] is True for v in resp["DBEngineVersions"])


def test_rds_same_region_arn_lookup_requires_stored_resource_arn_match():
    east = _regional_rds("us-east-1")
    west = _regional_rds("us-west-2")
    suffix = uuid.uuid4().hex[:8]
    cluster_id = f"rds-fabricated-arn-{suffix}"
    instance_id = f"rds-fabricated-arn-{suffix}"

    try:
        cluster = west.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        instance = west.create_db_instance(
            DBInstanceIdentifier=instance_id,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername="admin",
            MasterUserPassword="pass",
            AllocatedStorage=10,
        )["DBInstance"]

        fabricated_cluster_arn = cluster["DBClusterArn"].replace(":us-west-2:", ":us-east-1:")
        fabricated_instance_arn = instance["DBInstanceArn"].replace(":us-west-2:", ":us-east-1:")

        with pytest.raises(ClientError) as exc:
            east.describe_db_clusters(DBClusterIdentifier=fabricated_cluster_arn)
        assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"

        with pytest.raises(ClientError) as exc:
            east.describe_db_instances(DBInstanceIdentifier=fabricated_instance_arn)
        assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"
    finally:
        _delete_instance(west, instance_id)
        _delete_cluster(west, cluster_id)


def test_rds_db_snapshot_filter_by_instance_arn_survives_source_deletion():
    east = _regional_rds("us-east-1")
    suffix = uuid.uuid4().hex[:8]
    instance_id = f"rds-snap-src-arn-{suffix}"
    snapshot_id = f"rds-snap-src-arn-{suffix}"

    try:
        instance = east.create_db_instance(
            DBInstanceIdentifier=instance_id,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername="admin",
            MasterUserPassword="pass",
            AllocatedStorage=10,
        )["DBInstance"]
        east.create_db_snapshot(
            DBSnapshotIdentifier=snapshot_id,
            DBInstanceIdentifier=instance["DBInstanceArn"],
        )
        east.delete_db_instance(DBInstanceIdentifier=instance_id, SkipFinalSnapshot=True)

        by_source_arn = east.describe_db_snapshots(
            DBInstanceIdentifier=instance["DBInstanceArn"],
        )["DBSnapshots"]
        assert any(s["DBSnapshotIdentifier"] == snapshot_id for s in by_source_arn)
    finally:
        try:
            east.delete_db_snapshot(DBSnapshotIdentifier=snapshot_id)
        except ClientError:
            pass
        _delete_instance(east, instance_id)


def test_rds_create_instance_with_cluster_arn_stores_canonical_cluster_id():
    east = _regional_rds("us-east-1")
    suffix = uuid.uuid4().hex[:8]
    cluster_id = f"rds-inst-cluster-arn-{suffix}"
    instance_id = f"rds-inst-cluster-arn-{suffix}"

    try:
        cluster = east.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        east.create_db_instance(
            DBInstanceIdentifier=instance_id,
            DBClusterIdentifier=cluster["DBClusterArn"],
            DBInstanceClass="db.t3.micro",
            Engine="aurora-mysql",
        )

        instance = east.describe_db_instances(DBInstanceIdentifier=instance_id)["DBInstances"][0]
        assert instance["DBClusterIdentifier"] == cluster_id

        cluster = east.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
        members = cluster["DBClusterMembers"]
        assert any(member["DBInstanceIdentifier"] == instance_id for member in members)
    finally:
        _delete_instance(east, instance_id)
        _delete_cluster(east, cluster_id)


def test_rds_protected_cluster_member_delete_preserves_membership():
    east = _regional_rds("us-east-1")
    suffix = uuid.uuid4().hex[:8]
    cluster_id = f"rds-protected-member-{suffix}"
    instance_id = f"rds-protected-member-{suffix}"

    try:
        cluster = east.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        instance = east.create_db_instance(
            DBInstanceIdentifier=instance_id,
            DBClusterIdentifier=cluster["DBClusterArn"],
            DBInstanceClass="db.t3.micro",
            Engine="aurora-mysql",
            DeletionProtection=True,
        )["DBInstance"]

        for identifier in (instance["DBInstanceArn"], instance["DbiResourceId"]):
            with pytest.raises(ClientError) as exc:
                east.delete_db_instance(
                    DBInstanceIdentifier=identifier,
                    SkipFinalSnapshot=True,
                )
            assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"

            cluster_after = east.describe_db_clusters(
                DBClusterIdentifier=cluster_id,
            )["DBClusters"][0]
            members = cluster_after["DBClusterMembers"]
            assert any(
                member["DBInstanceIdentifier"] == instance_id
                for member in members
            )
    finally:
        try:
            east.modify_db_instance(
                DBInstanceIdentifier=instance_id,
                DeletionProtection=False,
                ApplyImmediately=True,
            )
        except ClientError:
            pass
        _delete_instance(east, instance_id)
        _delete_cluster(east, cluster_id)


def test_rds_read_replica_from_instance_arn_stores_canonical_source_id():
    east = _regional_rds("us-east-1")
    suffix = uuid.uuid4().hex[:8]
    source_id = f"rds-replica-arn-src-{suffix}"
    replica_id = f"rds-replica-arn-{suffix}"

    try:
        source = east.create_db_instance(
            DBInstanceIdentifier=source_id,
            DBInstanceClass="db.t3.micro",
            Engine="postgres",
            MasterUsername="admin",
            MasterUserPassword="pass",
            AllocatedStorage=10,
        )["DBInstance"]
        replica = east.create_db_instance_read_replica(
            DBInstanceIdentifier=replica_id,
            SourceDBInstanceIdentifier=source["DBInstanceArn"],
        )["DBInstance"]

        assert replica["ReadReplicaSourceDBInstanceIdentifier"] == source_id

        source = east.describe_db_instances(DBInstanceIdentifier=source_id)["DBInstances"][0]
        assert replica_id in source["ReadReplicaDBInstanceIdentifiers"]
    finally:
        _delete_instance(east, replica_id)
        _delete_instance(east, source_id)


def test_rds_tag_resource_arns_are_request_region_scoped():
    east = _regional_rds("us-east-1")
    west = _regional_rds("us-west-2")
    cluster_id = f"rds-tag-scope-{uuid.uuid4().hex[:8]}"

    try:
        cluster = west.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        cluster_arn = cluster["DBClusterArn"]
        bogus_account_arn = cluster_arn.replace(":000000000000:", ":111111111111:")

        west.add_tags_to_resource(
            ResourceName=cluster_arn,
            Tags=[{"Key": "scope", "Value": "west"}],
        )
        assert west.list_tags_for_resource(ResourceName=cluster_arn)["TagList"] == [
            {"Key": "scope", "Value": "west"},
        ]

        with pytest.raises(ClientError) as exc:
            east.add_tags_to_resource(
                ResourceName=cluster_arn,
                Tags=[{"Key": "scope", "Value": "east"}],
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            east.list_tags_for_resource(ResourceName=cluster_arn)
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            west.add_tags_to_resource(
                ResourceName=bogus_account_arn,
                Tags=[{"Key": "scope", "Value": "bogus"}],
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        cluster = west.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
        assert cluster["TagList"] == [{"Key": "scope", "Value": "west"}]
    finally:
        _delete_cluster(west, cluster_id)


def test_rds_cluster_snapshot_from_arn_stores_canonical_cluster_id():
    east = _regional_rds("us-east-1")
    suffix = uuid.uuid4().hex[:8]
    cluster_id = f"rds-snap-arn-{suffix}"
    snapshot_id = f"rds-snap-arn-{suffix}"

    try:
        cluster = east.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]
        east.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier=snapshot_id,
            DBClusterIdentifier=cluster["DBClusterArn"],
        )

        by_snapshot = east.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=snapshot_id,
        )["DBClusterSnapshots"][0]
        assert by_snapshot["DBClusterIdentifier"] == cluster_id

        by_cluster = east.describe_db_cluster_snapshots(
            DBClusterIdentifier=cluster["DBClusterArn"],
        )["DBClusterSnapshots"]
        assert any(s["DBClusterSnapshotIdentifier"] == snapshot_id for s in by_cluster)

        east.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)

        by_deleted_source_arn = east.describe_db_cluster_snapshots(
            DBClusterIdentifier=cluster["DBClusterArn"],
        )["DBClusterSnapshots"]
        assert any(
            s["DBClusterSnapshotIdentifier"] == snapshot_id
            for s in by_deleted_source_arn
        )
    finally:
        try:
            east.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier=snapshot_id)
        except ClientError:
            pass
        _delete_cluster(east, cluster_id)


def test_describe_global_clusters_rejects_global_cluster_arns():
    account_a = _regional_rds("us-east-1", access_key_id="111111111111")
    account_b = _regional_rds("us-east-1", access_key_id="222222222222")
    global_id = f"global-cross-account-{uuid.uuid4().hex[:8]}"
    global_arn_id = f"arn:aws:rds::{111111111111}:global-cluster:{global_id}-arn"

    try:
        with pytest.raises(ClientError) as exc:
            account_a.create_global_cluster(
                GlobalClusterIdentifier=global_arn_id,
                Engine="aurora-mysql",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        global_cluster = account_a.create_global_cluster(
            GlobalClusterIdentifier=global_id,
            Engine="aurora-mysql",
        )["GlobalCluster"]

        same_account = account_a.describe_global_clusters(
            GlobalClusterIdentifier=global_id,
        )["GlobalClusters"][0]
        assert same_account["GlobalClusterIdentifier"] == global_id

        with pytest.raises(ClientError) as exc:
            account_a.describe_global_clusters(
                GlobalClusterIdentifier=global_cluster["GlobalClusterArn"],
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            account_b.describe_global_clusters(
                GlobalClusterIdentifier=global_cluster["GlobalClusterArn"],
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            account_a.modify_global_cluster(
                GlobalClusterIdentifier=global_cluster["GlobalClusterArn"],
                DeletionProtection=False,
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            account_a.modify_global_cluster(
                GlobalClusterIdentifier=global_id,
                NewGlobalClusterIdentifier=global_arn_id,
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            account_a.remove_from_global_cluster(
                GlobalClusterIdentifier=global_cluster["GlobalClusterArn"],
                DbClusterIdentifier="does-not-matter",
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"

        with pytest.raises(ClientError) as exc:
            account_a.delete_global_cluster(
                GlobalClusterIdentifier=global_cluster["GlobalClusterArn"],
            )
        assert exc.value.response["Error"]["Code"] == "InvalidParameterValue"
    finally:
        _delete_global_cluster(account_a, global_id)


def test_rds_mysql_readiness_probe_requires_successful_query(monkeypatch):
    """MySQL readiness requires an executable query, not only connection auth."""
    from ministack.services import rds

    attempts = []

    class FakeCursor:
        def execute(self, sql, params=None):
            attempts.append(sql)
            raise RuntimeError("(2013, 'Lost connection to MySQL server during query')")

        def close(self):
            attempts.append("cursor.close")

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            attempts.append("connection.close")

    def connect(**kwargs):
        return FakeConnection()

    monkeypatch.setitem(sys.modules, "pymysql", types.SimpleNamespace(connect=connect))

    assert not rds._try_database_connect(
        "127.0.0.1",
        3306,
        "aurora-mysql",
        "admin",
        "old_pass",
        None,
    )
    assert attempts == ["SELECT 1", "cursor.close", "connection.close"]


def test_rds_cluster_password_rotation_alters_mysql_password(monkeypatch):
    """Password rotation assumes the instance already passed readiness."""
    from ministack.services import rds

    attempts = []

    class FakeCursor:
        def execute(self, sql, params=None):
            attempts.append((sql, params))

        def close(self):
            attempts.append(("cursor.close", None))

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            attempts.append(("connection.close", None))

    def connect(**kwargs):
        attempts.append(("connect", kwargs))
        return FakeConnection()

    monkeypatch.setitem(sys.modules, "pymysql", types.SimpleNamespace(connect=connect))
    instances = rds.AccountRegionScopedDict()
    instances["pw-retry-instance"] = {
        "DBClusterIdentifier": "pw-retry-cluster",
        "Engine": "aurora-mysql",
        "_internal_address": "127.0.0.1",
        "_internal_port": 3306,
    }
    monkeypatch.setattr(rds, "_instances", instances)

    assert rds._rotate_real_password(
        {"DBClusterIdentifier": "pw-retry-cluster"},
        "old_pass",
        "new_pass",
    )
    assert (
        "ALTER USER 'root'@'%%' IDENTIFIED BY %s",
        ("new_pass",),
    ) in attempts


PASSWORD = "SharedStorage123!"


DATABASE = "paritydb"


def _wait_for_instance(rds, db_id, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        instance = rds.describe_db_instances(
            DBInstanceIdentifier=db_id,
        )["DBInstances"][0]
        if instance["DBInstanceStatus"] == "available":
            return instance
        if instance["DBInstanceStatus"] == "failed":
            pytest.fail(f"RDS instance {db_id} failed while starting")
        time.sleep(2)
    raise TimeoutError(f"RDS instance {db_id} not available after {timeout}s")


def _aurora_connect(endpoint, user="admin", password=PASSWORD, database=DATABASE):
    import pymysql

    return pymysql.connect(
        host=endpoint["Address"],
        port=int(endpoint["Port"]),
        user=user,
        password=password,
        database=database,
        autocommit=True,
        connect_timeout=5,
    )


@contextlib.contextmanager
def _live_cluster(rds):
    suffix = uuid.uuid4().hex[:10]
    cluster_id = f"shared-{suffix}"
    writer_id = f"{cluster_id}-writer"
    reader_id = f"{cluster_id}-reader"
    try:
        rds.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword=PASSWORD,
            DatabaseName=DATABASE,
        )
        for db_id in (writer_id, reader_id):
            rds.create_db_instance(
                DBInstanceIdentifier=db_id,
                DBClusterIdentifier=cluster_id,
                DBInstanceClass="db.r6g.large",
                Engine="aurora-mysql",
            )
        writer = _wait_for_instance(rds, writer_id)
        reader = _wait_for_instance(rds, reader_id)
        cluster = rds.describe_db_clusters(
            DBClusterIdentifier=cluster_id,
        )["DBClusters"][0]
        yield cluster_id, writer_id, reader_id, writer, reader, cluster
    finally:
        for db_id in (reader_id, writer_id):
            try:
                rds.delete_db_instance(
                    DBInstanceIdentifier=db_id,
                    SkipFinalSnapshot=True,
                )
            except ClientError as e:
                if e.response["Error"]["Code"] != "DBInstanceNotFound":
                    raise
        try:
            rds.delete_db_cluster(
                DBClusterIdentifier=cluster_id,
                SkipFinalSnapshot=True,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "DBClusterNotFoundFault":
                raise


@pytest.mark.skipif(not os.environ.get("DOCKER_NETWORK"), reason="DOCKER_NETWORK not set -- live Aurora")
def test_aurora_writer_data_is_visible_through_reader(rds):
    with _live_cluster(rds) as (_cid, _wid, _rid, writer, reader, _cluster):
        with _aurora_connect(writer["Endpoint"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE TABLE shared_rows (id INT PRIMARY KEY, value VARCHAR(32))")
                cursor.execute("INSERT INTO shared_rows VALUES (1, 'writer-data')")
        with _aurora_connect(reader["Endpoint"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, value FROM shared_rows")
                assert cursor.fetchall() == ((1, "writer-data"),)


@pytest.mark.skipif(not os.environ.get("DOCKER_NETWORK"), reason="DOCKER_NETWORK not set -- live Aurora")
def test_aurora_user_and_grant_are_visible_through_reader(rds):
    with _live_cluster(rds) as (_cid, _wid, _rid, writer, reader, _cluster):
        app_user = f"app_{uuid.uuid4().hex[:8]}"
        app_password = "AppPassword123!"
        with _aurora_connect(writer["Endpoint"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE TABLE granted_rows (id INT PRIMARY KEY)")
                cursor.execute(f"CREATE USER `{app_user}`@'%%' IDENTIFIED BY %s", (app_password,))
                cursor.execute(f"GRANT SELECT ON `{DATABASE}`.* TO `{app_user}`@'%'")
        with _aurora_connect(reader["Endpoint"], app_user, app_password) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM granted_rows")
                assert cursor.fetchone() == (0,)


@pytest.mark.skipif(not os.environ.get("DOCKER_NETWORK"), reason="DOCKER_NETWORK not set -- live Aurora")
def test_aurora_cluster_uses_one_backing_container(rds):
    import docker

    with _live_cluster(rds) as (cluster_id, _wid, _rid, writer, reader, cluster):
        containers = docker.from_env().containers.list(
            all=True,
            filters={"label": ["ministack=rds", f"cluster_id={cluster_id}"]},
        )
        assert len(containers) == 1
        assert writer["Endpoint"] == reader["Endpoint"]
        assert cluster["Endpoint"] == cluster["ReaderEndpoint"]
        assert cluster["Endpoint"] == writer["Endpoint"]["Address"]
        assert cluster["Port"] == writer["Endpoint"]["Port"]


@pytest.mark.skipif(not os.environ.get("DOCKER_NETWORK"), reason="DOCKER_NETWORK not set -- live Aurora")
def test_aurora_delete_member_keeps_shared_data(rds, rds_data):
    import docker
    import pymysql

    with _live_cluster(rds) as (
        cluster_id,
        writer_id,
        reader_id,
        writer,
        _reader,
        cluster,
    ):
        with _aurora_connect(writer["Endpoint"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE TABLE durable_rows (id INT PRIMARY KEY)")
                cursor.execute("INSERT INTO durable_rows VALUES (7)")
        rds.delete_db_instance(
            DBInstanceIdentifier=reader_id,
            SkipFinalSnapshot=True,
        )
        with _aurora_connect(writer["Endpoint"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id FROM durable_rows")
                assert cursor.fetchone() == (7,)

        containers = docker.from_env().containers.list(
            all=True,
            filters={"label": ["ministack=rds", f"cluster_id={cluster_id}"]},
        )
        assert len(containers) == 1
        container = containers[0]
        original_container_id = container.id

        rds.delete_db_instance(
            DBInstanceIdentifier=writer_id,
            SkipFinalSnapshot=True,
        )
        empty_cluster = rds.describe_db_clusters(
            DBClusterIdentifier=cluster_id,
        )["DBClusters"][0]
        assert empty_cluster["Status"] == "available"
        assert empty_cluster["DBClusterMembers"] == []

        container.reload()
        assert container.status == "exited"
        with pytest.raises((pymysql.err.OperationalError, OSError)):
            _aurora_connect(writer["Endpoint"])
        with pytest.raises(ClientError) as exc_info:
            rds_data.execute_statement(
                resourceArn=cluster["DBClusterArn"],
                secretArn=(
                    "arn:aws:secretsmanager:us-east-1:000000000000:secret:unused"
                ),
                sql="SELECT 1",
            )
        assert exc_info.value.response["Error"]["Code"] == "DatabaseUnavailableException"

        replacement_id = f"{cluster_id}-replacement"
        try:
            rds.create_db_instance(
                DBInstanceIdentifier=replacement_id,
                DBClusterIdentifier=cluster_id,
                DBInstanceClass="db.r6g.large",
                Engine="aurora-mysql",
            )
            replacement = _wait_for_instance(rds, replacement_id)
            container.reload()
            assert container.status == "running"
            assert container.id == original_container_id
            with _aurora_connect(replacement["Endpoint"]) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT id FROM durable_rows")
                    assert cursor.fetchone() == (7,)
        finally:
            try:
                rds.delete_db_instance(
                    DBInstanceIdentifier=replacement_id,
                    SkipFinalSnapshot=True,
                )
            except ClientError as e:
                if e.response["Error"]["Code"] != "DBInstanceNotFound":
                    raise
    containers = docker.from_env().containers.list(
        all=True,
        filters={"label": ["ministack=rds", f"cluster_id={cluster_id}"]},
    )
    assert containers == []
