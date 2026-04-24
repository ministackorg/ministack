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
    assert inst["DBInstanceStatus"] == "available"
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
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFoundFault"

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
    # engine-default filter should return empty when no defaults are tracked
    resp2 = rds.describe_db_cluster_parameters(
        DBClusterParameterGroupName="test-cparam-persist", Source="engine-default"
    )
    assert len(resp2["Parameters"]) == 0


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
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFoundFault"


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
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFoundFault"


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
        rds.stop_db_instance(DBInstanceIdentifier="start-test")
        stopped = rds.describe_db_instances(DBInstanceIdentifier="start-test")["DBInstances"][0]
        assert stopped["DBInstanceStatus"] == "stopped"

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
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFoundFault"


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
        resp = rds.stop_db_instance(DBInstanceIdentifier="stop-test")
        assert resp["DBInstance"]["DBInstanceStatus"] == "stopped"

        desc = rds.describe_db_instances(DBInstanceIdentifier="stop-test")["DBInstances"][0]
        assert desc["DBInstanceStatus"] == "stopped"
    finally:
        rds.delete_db_instance(DBInstanceIdentifier="stop-test", SkipFinalSnapshot=True)


def test_rds_stop_db_instance_not_found(rds):
    """StopDBInstance fails for a non-existent instance."""
    with pytest.raises(ClientError) as exc:
        rds.stop_db_instance(DBInstanceIdentifier="ghost-instance-2")
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFoundFault"


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
