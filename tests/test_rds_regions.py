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
