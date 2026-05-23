import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError
from conftest import ENDPOINT


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
