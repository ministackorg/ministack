import time

import pytest
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Instances (created as cluster members, as the docdb API requires)
# ---------------------------------------------------------------------------

def _make_cluster(docdb, cluster_id, **kwargs):
    params = dict(
        DBClusterIdentifier=cluster_id,
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="password123",
    )
    params.update(kwargs)
    return docdb.create_db_cluster(**params)["DBCluster"]


def test_docdb_create_instance(docdb):
    _make_cluster(docdb, "test-docdb-cluster-a")
    docdb.create_db_instance(
        DBInstanceIdentifier="test-docdb",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="test-docdb-cluster-a",
    )
    resp = docdb.describe_db_instances(DBInstanceIdentifier="test-docdb")
    instances = resp["DBInstances"]
    assert len(instances) == 1
    assert instances[0]["DBInstanceIdentifier"] == "test-docdb"
    assert instances[0]["Engine"] == "docdb"
    assert instances[0]["DBClusterIdentifier"] == "test-docdb-cluster-a"
    assert instances[0]["Endpoint"]["Port"] == 27017 or instances[0]["Endpoint"]["Port"] >= 27117


def test_docdb_cluster_member_shares_endpoint(docdb):
    _make_cluster(docdb, "member-endpoint-cluster")
    a = docdb.create_db_instance(
        DBInstanceIdentifier="member-a",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="member-endpoint-cluster",
    )["DBInstance"]
    b = docdb.create_db_instance(
        DBInstanceIdentifier="member-b",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="member-endpoint-cluster",
    )["DBInstance"]
    assert a["Endpoint"] == b["Endpoint"]

    cluster = docdb.describe_db_clusters(DBClusterIdentifier="member-endpoint-cluster")["DBClusters"][0]
    members = sorted(m["DBInstanceIdentifier"] for m in cluster["DBClusterMembers"])
    assert members == ["member-a", "member-b"]
    writers = [m for m in cluster["DBClusterMembers"] if m["IsClusterWriter"]]
    assert len(writers) == 1
    assert writers[0]["DBInstanceIdentifier"] == "member-a"


def test_docdb_engines(docdb):
    resp = docdb.describe_db_engine_versions(Engine="docdb")
    versions = {v["EngineVersion"]: v for v in resp["DBEngineVersions"]}
    assert set(versions) >= {"5.0.0", "8.0.0"}
    assert all(v["Engine"] == "docdb" for v in resp["DBEngineVersions"])
    assert versions["5.0.0"]["DBParameterGroupFamily"] == "docdb5.0"
    assert versions["8.0.0"]["DBParameterGroupFamily"] == "docdb8.0"


def test_docdb_unsupported_engine_version_rejected(docdb):
    with pytest.raises(ClientError) as exc:
        docdb.create_db_cluster(
            DBClusterIdentifier="bad-version-cluster",
            Engine="docdb",
            MasterUsername="root",
            MasterUserPassword="password123",
            EngineVersion="6.0.0",
        )
    assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"


def test_docdb_delete_instance(docdb):
    _make_cluster(docdb, "delete-instance-cluster")
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-del-v2",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="delete-instance-cluster",
    )
    # The real DocDB DeleteDBInstance takes only the identifier (no final
    # snapshot options — cluster snapshots are the only snapshots).
    docdb.delete_db_instance(DBInstanceIdentifier="docdb-del-v2")
    with pytest.raises(ClientError) as exc:
        docdb.describe_db_instances(DBInstanceIdentifier="docdb-del-v2")
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


def test_docdb_modify_instance(docdb):
    _make_cluster(docdb, "modify-instance-cluster")
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-mod-v2",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="modify-instance-cluster",
    )
    docdb.modify_db_instance(
        DBInstanceIdentifier="docdb-mod-v2",
        DBInstanceClass="db.t3.large",
        ApplyImmediately=True,
    )
    inst = docdb.describe_db_instances(DBInstanceIdentifier="docdb-mod-v2")["DBInstances"][0]
    assert inst["DBInstanceClass"] == "db.t3.large"


def test_docdb_deletion_protection(docdb):
    # Deletion protection is cluster-level on real DocumentDB; instances
    # inherit it and DeleteDBInstance is refused while it is on.
    _make_cluster(docdb, "protected-cluster", DeletionProtection=True)
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-protected",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="protected-cluster",
    )
    try:
        with pytest.raises(ClientError) as exc:
            docdb.delete_db_instance(DBInstanceIdentifier="docdb-protected")
        assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
    finally:
        docdb.modify_db_cluster(
            DBClusterIdentifier="protected-cluster",
            DeletionProtection=False,
            ApplyImmediately=True,
        )
        docdb.delete_db_instance(DBInstanceIdentifier="docdb-protected")


def test_docdb_tags(docdb):
    _make_cluster(docdb, "tag-cluster", Tags=[{"Key": "env", "Value": "dev"}])
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-tag-v2",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="tag-cluster",
    )
    arn = docdb.describe_db_clusters(DBClusterIdentifier="tag-cluster")["DBClusters"][0]["DBClusterArn"]

    tags = docdb.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert any(t["Key"] == "env" and t["Value"] == "dev" for t in tags)

    docdb.add_tags_to_resource(ResourceName=arn, Tags=[{"Key": "team", "Value": "dba"}])
    tags2 = docdb.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert any(t["Key"] == "team" and t["Value"] == "dba" for t in tags2)

    docdb.remove_tags_from_resource(ResourceName=arn, TagKeys=["env"])
    tags3 = docdb.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert not any(t["Key"] == "env" for t in tags3)
    assert any(t["Key"] == "team" for t in tags3)


def test_docdb_orderable_options(docdb):
    resp = docdb.describe_orderable_db_instance_options(Engine="docdb", EngineVersion="8.0.0")
    options = resp["OrderableDBInstanceOptions"]
    assert options
    assert all(o["EngineVersion"] == "8.0.0" for o in options)


# ---------------------------------------------------------------------------
# Cluster snapshots (metadata-only)
# ---------------------------------------------------------------------------

def test_docdb_cluster_snapshot_crud(docdb):
    _make_cluster(docdb, "snapshot-cluster")

    snap = docdb.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="snap-1",
        DBClusterIdentifier="snapshot-cluster",
    )["DBClusterSnapshot"]
    assert snap["Status"] == "available"
    assert snap["DBClusterIdentifier"] == "snapshot-cluster"
    assert snap["EngineVersion"] in ("5.0.0", "8.0.0")

    with pytest.raises(ClientError) as exc:
        docdb.create_db_cluster_snapshot(
            DBClusterSnapshotIdentifier="snap-1",
            DBClusterIdentifier="snapshot-cluster",
        )
    assert exc.value.response["Error"]["Code"] == "DBClusterSnapshotAlreadyExistsFault"

    desc = docdb.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier="snap-1")
    assert len(desc["DBClusterSnapshots"]) == 1

    by_cluster = docdb.describe_db_cluster_snapshots(DBClusterIdentifier="snapshot-cluster")
    assert any(s["DBClusterSnapshotIdentifier"] == "snap-1" for s in by_cluster["DBClusterSnapshots"])

    docdb.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier="snap-1")
    with pytest.raises(ClientError) as exc:
        docdb.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier="snap-1")
    assert exc.value.response["Error"]["Code"] == "DBClusterSnapshotNotFoundFault"


def test_docdb_cluster_snapshot_attributes(docdb):
    _make_cluster(docdb, "attribute-cluster")
    docdb.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="snap-attrs",
        DBClusterIdentifier="attribute-cluster",
    )

    attrs = docdb.describe_db_cluster_snapshot_attributes(DBClusterSnapshotIdentifier="snap-attrs")
    result = attrs["DBClusterSnapshotAttributesResult"]
    restore = next(a for a in result["DBClusterSnapshotAttributes"] if a["AttributeName"] == "restore")
    assert "000000000000" in restore["AttributeValues"]

    mod = docdb.modify_db_cluster_snapshot_attribute(
        DBClusterSnapshotIdentifier="snap-attrs",
        AttributeName="restore",
        ValuesToAdd=["111111111111"],
    )
    result = mod["DBClusterSnapshotAttributesResult"]
    restore = next(
        a for a in result["DBClusterSnapshotAttributes"]
        if a["AttributeName"] == "restore"
    )
    assert set(restore["AttributeValues"]) >= {"000000000000", "111111111111"}

    mod2 = docdb.modify_db_cluster_snapshot_attribute(
        DBClusterSnapshotIdentifier="snap-attrs",
        AttributeName="restore",
        ValuesToRemove=["111111111111"],
    )
    result = mod2["DBClusterSnapshotAttributesResult"]
    restore = next(
        a for a in result["DBClusterSnapshotAttributes"]
        if a["AttributeName"] == "restore"
    )
    assert "111111111111" not in restore["AttributeValues"]

    docdb.delete_db_cluster_snapshot(DBClusterSnapshotIdentifier="snap-attrs")


# ---------------------------------------------------------------------------
# Cluster parameter groups
# ---------------------------------------------------------------------------

def test_docdb_cluster_parameter_group_crud(docdb):
    docdb.create_db_cluster_parameter_group(
        DBClusterParameterGroupName="pg-crud",
        DBParameterGroupFamily="docdb5.0",
        Description="crud test group",
    )
    groups = docdb.describe_db_cluster_parameter_groups(DBClusterParameterGroupName="pg-crud")
    pg = groups["DBClusterParameterGroups"][0]
    assert pg["DBParameterGroupFamily"] == "docdb5.0"
    assert pg["Description"] == "crud test group"

    docdb.delete_db_cluster_parameter_group(DBClusterParameterGroupName="pg-crud")
    with pytest.raises(ClientError) as exc:
        docdb.describe_db_cluster_parameter_groups(DBClusterParameterGroupName="pg-crud")
    assert exc.value.response["Error"]["Code"] == "DBParameterGroupNotFound"


def test_docdb_cluster_parameters_modify_reset(docdb):
    docdb.create_db_cluster_parameter_group(
        DBClusterParameterGroupName="pg-mod",
        DBParameterGroupFamily="docdb8.0",
        Description="modify/reset test",
    )
    docdb.modify_db_cluster_parameter_group(
        DBClusterParameterGroupName="pg-mod",
        Parameters=[{"ParameterName": "ttl_monitor_enabled", "ParameterValue": "false"}],
    )

    user_params = docdb.describe_db_cluster_parameters(
        DBClusterParameterGroupName="pg-mod", Source="user")
    names = [p["ParameterName"] for p in user_params["Parameters"]]
    assert "ttl_monitor_enabled" in names

    all_params = docdb.describe_db_cluster_parameters(DBClusterParameterGroupName="pg-mod")
    defaults = [p for p in all_params["Parameters"] if p["Source"] == "engine-default"]
    assert defaults

    docdb.reset_db_cluster_parameter_group(
        DBClusterParameterGroupName="pg-mod",
        ResetAllParameters=True,
    )
    after = docdb.describe_db_cluster_parameters(
        DBClusterParameterGroupName="pg-mod", Source="user")
    assert after["Parameters"] == []

    docdb.delete_db_cluster_parameter_group(DBClusterParameterGroupName="pg-mod")


# ---------------------------------------------------------------------------
# Failover / restore / maintenance / certificates / events
# ---------------------------------------------------------------------------

def test_docdb_delete_cluster_refused_while_members_exist(docdb):
    _make_cluster(docdb, "occupied-cluster")
    docdb.create_db_instance(
        DBInstanceIdentifier="occupied-member",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="occupied-cluster",
    )
    with pytest.raises(ClientError) as exc:
        docdb.delete_db_cluster(DBClusterIdentifier="occupied-cluster")
    assert exc.value.response["Error"]["Code"] == "InvalidDBClusterStateFault"

    # AWS ordering: delete the members first, then the cluster.
    docdb.delete_db_instance(DBInstanceIdentifier="occupied-member")
    docdb.delete_db_cluster(DBClusterIdentifier="occupied-cluster")
    with pytest.raises(ClientError) as exc:
        docdb.describe_db_clusters(DBClusterIdentifier="occupied-cluster")
    assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"


def test_docdb_failover_rotates_writer(docdb):
    _make_cluster(docdb, "failover-cluster")
    docdb.create_db_instance(
        DBInstanceIdentifier="fo-writer",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="failover-cluster",
        PromotionTier=1,
    )
    docdb.create_db_instance(
        DBInstanceIdentifier="fo-reader-tier1",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="failover-cluster",
        PromotionTier=2,
    )
    docdb.create_db_instance(
        DBInstanceIdentifier="fo-reader-tier0",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="failover-cluster",
        PromotionTier=1,
    )

    resp = docdb.failover_db_cluster(DBClusterIdentifier="failover-cluster")
    cluster = resp["DBCluster"]
    # Response carries the transitional status; endpoints are stable.
    assert cluster["Status"] == "failing-over"
    writers = [m for m in cluster["DBClusterMembers"] if m["IsClusterWriter"]]
    assert len(writers) == 1
    assert writers[0]["DBInstanceIdentifier"] == "fo-reader-tier0"

    stored = docdb.describe_db_clusters(DBClusterIdentifier="failover-cluster")["DBClusters"][0]
    stored_writer = next(m for m in stored["DBClusterMembers"] if m["IsClusterWriter"])
    assert stored_writer["DBInstanceIdentifier"] == "fo-reader-tier0"

    explicit = docdb.failover_db_cluster(
        DBClusterIdentifier="failover-cluster",
        TargetDBInstanceIdentifier="fo-writer",
    )["DBCluster"]
    writer = next(m for m in explicit["DBClusterMembers"] if m["IsClusterWriter"])
    assert writer["DBInstanceIdentifier"] == "fo-writer"


def test_docdb_restore_cluster_from_snapshot(docdb):
    src = _make_cluster(
        docdb, "restore-source",
        EngineVersion="8.0.0",
    )
    docdb.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="snap-restore",
        DBClusterIdentifier="restore-source",
    )

    restored = docdb.restore_db_cluster_from_snapshot(
        DBClusterIdentifier="restored-cluster",
        Engine="docdb",
        SnapshotIdentifier="snap-restore",
    )["DBCluster"]
    assert restored["Status"] == "available"
    assert restored["EngineVersion"] == src["EngineVersion"] == "8.0.0"
    assert restored["MasterUsername"] == src["MasterUsername"] == "root"
    assert restored["DBClusterMembers"] == []

    desc = docdb.describe_db_clusters(DBClusterIdentifier="restored-cluster")
    assert desc["DBClusters"][0]["DBClusterIdentifier"] == "restored-cluster"


def test_docdb_pending_maintenance_actions(docdb):
    arn = docdb.describe_db_clusters(DBClusterIdentifier="failover-cluster")["DBClusters"][0]["DBClusterArn"]
    applied = docdb.apply_pending_maintenance_action(
        ResourceIdentifier=arn,
        ApplyAction="system-update",
        OptInType="immediately",
    )
    details = applied["ResourcePendingMaintenanceActions"]["PendingMaintenanceActionDetails"]
    assert any(a["Action"] == "system-update" for a in details)

    described = docdb.describe_pending_maintenance_actions(ResourceIdentifier=arn)
    resources = described["PendingMaintenanceActions"]
    assert any(
        r["ResourceIdentifier"] == arn
        and any(a["Action"] == "system-update" for a in r["PendingMaintenanceActionDetails"])
        for r in resources
    )


def test_docdb_describe_certificates_and_events(docdb):
    certs = docdb.describe_certificates()["Certificates"]
    assert len(certs) == 1
    assert certs[0]["CertificateIdentifier"] == "rds-ca-rsa2048-g1"

    events = docdb.describe_events()["Events"]
    assert events == []


# ---------------------------------------------------------------------------
# Docker image helper tests (always run, no container required)
# ---------------------------------------------------------------------------

def test_docker_image_for_docdb_versions():
    from ministack.services.documentdb import _docker_image_for_docdb

    image, env, port, data_path = _docker_image_for_docdb("5.0.0", "root", "secret", "admin")
    assert image.endswith("mongo:5.0")
    assert env["MONGO_INITDB_ROOT_USERNAME"] == "root"
    assert env["MONGO_INITDB_ROOT_PASSWORD"] == "secret"
    assert port == 27017
    assert data_path == "/data/db"

    image8, _, _, _ = _docker_image_for_docdb("8.0.0", "root", "secret", "admin")
    assert image8.endswith("mongo:8.0")


def test_docker_image_for_docdb_unknown_version_falls_back():
    from ministack.services.documentdb import _docker_image_for_docdb

    image, _, _, _ = _docker_image_for_docdb("9.9.9", "root", "secret")
    assert image.endswith("mongo:5.0")


def test_docker_image_prefix_honored(monkeypatch):
    monkeypatch.setenv("MINISTACK_IMAGE_PREFIX", "mirror.example.com/")
    from ministack.services.documentdb import _docker_image_for_docdb

    image, _, _, _ = _docker_image_for_docdb("8.0.0", "root", "secret")
    assert image == "mirror.example.com/mongo:8.0"


# ---------------------------------------------------------------------------
# Pymongo integration (Docker + pymongo gated)
# ---------------------------------------------------------------------------

def _docker_available():
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


def _pymongo_available():
    try:
        import pymongo
        return True
    except ImportError:
        return False


def test_docdb_pymongo_shared_endpoint(docdb):
    """Cluster members share one mongo endpoint: writes via A are visible via B."""
    if not _pymongo_available():
        pytest.skip("pymongo not installed")
    if not _docker_available():
        pytest.skip("Docker not available for DocDB container launch")

    import pymongo

    _make_cluster(docdb, "smoke-cluster")
    for member_id in ("smoke-a", "smoke-b"):
        docdb.create_db_instance(
            DBInstanceIdentifier=member_id,
            DBInstanceClass="db.t3.medium",
            Engine="docdb",
            DBClusterIdentifier="smoke-cluster",
        )

    deadline = time.time() + 120
    while time.time() < deadline:
        instances = docdb.describe_db_instances(
            Filters=[{"Name": "db-cluster-id", "Values": ["smoke-cluster"]}])["DBInstances"]
        identifiers = {i["DBInstanceIdentifier"] for i in instances}
        if {"smoke-a", "smoke-b"} <= identifiers and all(
            i["DBInstanceStatus"] == "available" for i in instances
        ):
            break
        time.sleep(2)
    else:
        raise TimeoutError("DocDB members not available after 120s")

    ep_a = next(i["Endpoint"] for i in instances if i["DBInstanceIdentifier"] == "smoke-a")
    ep_b = next(i["Endpoint"] for i in instances if i["DBInstanceIdentifier"] == "smoke-b")
    assert ep_a == ep_b

    client_a = pymongo.MongoClient(
        ep_a["Address"], int(ep_a["Port"]),
        username="root", password="password123",
        serverSelectionTimeoutMS=60000, directConnection=True,
    )
    client_b = pymongo.MongoClient(
        ep_b["Address"], int(ep_b["Port"]),
        username="root", password="password123",
        serverSelectionTimeoutMS=60000, directConnection=True,
    )
    try:
        client_a.smoketest.items.insert_one({"k": 1, "msg": "visible through member B"})
        found = client_b.smoketest.items.find_one({"k": 1})
        assert found is not None
        assert found["msg"] == "visible through member B"
    finally:
        client_a.close()
        client_b.close()

    for member_id in ("smoke-a", "smoke-b"):
        docdb.delete_db_instance(DBInstanceIdentifier=member_id)
    docdb.delete_db_cluster(DBClusterIdentifier="smoke-cluster")


def test_docdb_pymongo_v8_connects(docdb):
    """A DocDB 8.0.0 cluster member accepts wire connections (mongo:8.0)."""
    if not _pymongo_available():
        pytest.skip("pymongo not installed")
    if not _docker_available():
        pytest.skip("Docker not available for DocDB container launch")

    import pymongo

    _make_cluster(docdb, "v8-cluster", EngineVersion="8.0.0")
    docdb.create_db_instance(
        DBInstanceIdentifier="v8-member",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        DBClusterIdentifier="v8-cluster",
    )
    inst = docdb.describe_db_instances(DBInstanceIdentifier="v8-member")["DBInstances"][0]
    assert inst["EngineVersion"] == "8.0.0"
    ep = inst["Endpoint"]

    client = pymongo.MongoClient(
        ep["Address"], int(ep["Port"]),
        username="root", password="password123",
        serverSelectionTimeoutMS=120000, directConnection=True,
    )
    try:
        db = client["v8check"]
        coll = db["items"]
        coll.insert_one({"k": 8})
        found = coll.find_one({"k": 8})
        assert found is not None
    finally:
        client.close()
        docdb.delete_db_instance(DBInstanceIdentifier="v8-member")
        docdb.delete_db_cluster(DBClusterIdentifier="v8-cluster")
