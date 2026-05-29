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
    finally:
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
        _delete_instance(west, instance_id)


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
