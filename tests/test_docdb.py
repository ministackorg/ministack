import time
import uuid as _uuid_mod

import pytest
from botocore.exceptions import ClientError


def _wait_available(client, db_id, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        instance = client.describe_db_instances(DBInstanceIdentifier=db_id)["DBInstances"][0]
        if instance["DBInstanceStatus"] == "available":
            return instance
        time.sleep(2)
    raise TimeoutError(f"DocumentDB instance {db_id} not available after {timeout}s")


def _docdb_cluster(docdb, cluster_id, **kwargs):
    return docdb.create_db_cluster(DBClusterIdentifier=cluster_id, Engine="docdb", MasterUsername="mainuser",
                                   MasterUserPassword="Passw0rd123", **kwargs)["DBCluster"]


def test_docdb_cluster_defaults(docdb):
    cluster_id = f"docdb-{_uuid_mod.uuid4().hex[:8]}"
    try:
        cluster = _docdb_cluster(docdb, cluster_id)
        assert cluster["Engine"] == "docdb"
        assert cluster["EngineVersion"] == "8.0.0"
        assert cluster["Port"] == 27017
        assert cluster["DBClusterParameterGroup"] == "default.docdb8.0"
        assert cluster["Endpoint"].endswith(".us-east-1.docdb.amazonaws.com")
        assert cluster["ReaderEndpoint"].startswith(f"{cluster_id}.cluster-ro-")
    finally:
        docdb.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)


def test_docdb_engine_versions(docdb):
    cluster_id = f"docdb-{_uuid_mod.uuid4().hex[:8]}"
    try:
        cluster = _docdb_cluster(docdb, cluster_id, EngineVersion="5.0.0")
        assert cluster["DBClusterParameterGroup"] == "default.docdb5.0"
    finally:
        docdb.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
    with pytest.raises(ClientError) as exc:
        _docdb_cluster(docdb, f"docdb-{_uuid_mod.uuid4().hex[:8]}", EngineVersion="9.9.9")
    assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
    assert exc.value.response["Error"]["Message"] == "Cannot find version 9.9.9 for docdb"


def test_docdb_instance_lifecycle_and_tags(docdb, rds):
    cluster_id = f"docdb-{_uuid_mod.uuid4().hex[:8]}"
    db_id = f"{cluster_id}-1"
    _docdb_cluster(docdb, cluster_id, Tags=[{"Key": "team", "Value": "a"}])
    try:
        instance = docdb.create_db_instance(DBInstanceIdentifier=db_id, DBInstanceClass="db.r6g.large",
                                            Engine="docdb", DBClusterIdentifier=cluster_id)["DBInstance"]
        assert instance["Engine"] == "docdb"
        assert instance["DBClusterIdentifier"] == cluster_id
        _wait_available(docdb, db_id)
        members = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["DBClusterMembers"]
        assert [m["DBInstanceIdentifier"] for m in members] == [db_id]
        assert db_id in [i["DBInstanceIdentifier"] for i in docdb.describe_db_instances()["DBInstances"]]

        # One control plane: the rds client sees the cluster, and the engine filter selects it.
        assert cluster_id in [c["DBClusterIdentifier"] for c in rds.describe_db_clusters()["DBClusters"]]
        filtered = docdb.describe_db_clusters(Filters=[{"Name": "engine", "Values": ["docdb"]}])["DBClusters"]
        assert cluster_id in [c["DBClusterIdentifier"] for c in filtered]

        arn = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["DBClusterArn"]
        docdb.add_tags_to_resource(ResourceName=arn, Tags=[{"Key": "env", "Value": "dev"}])
        tags = docdb.list_tags_for_resource(ResourceName=arn)["TagList"]
        assert {"Key": "team", "Value": "a"} in tags and {"Key": "env", "Value": "dev"} in tags

        docdb.stop_db_cluster(DBClusterIdentifier=cluster_id)
        assert docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["Status"] == "stopped"
        docdb.start_db_cluster(DBClusterIdentifier=cluster_id)
        deadline = time.time() + 120
        while docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["Status"] != "available":
            assert time.time() < deadline
            time.sleep(1)
        docdb.delete_db_instance(DBInstanceIdentifier=db_id)
    finally:
        deadline = time.time() + 60
        while docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["DBClusterMembers"]:
            if time.time() > deadline:
                break
            time.sleep(1)
        docdb.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
    with pytest.raises(ClientError) as exc:
        docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)
    assert exc.value.response["Error"]["Code"] == "DBClusterNotFoundFault"


@pytest.mark.data_plane
def test_docdb_instance_serves_the_mongodb_protocol(docdb):
    pymongo = pytest.importorskip("pymongo")
    cluster_id = f"docdb-{_uuid_mod.uuid4().hex[:8]}"
    db_id = f"{cluster_id}-1"
    _docdb_cluster(docdb, cluster_id)
    try:
        docdb.create_db_instance(DBInstanceIdentifier=db_id, DBInstanceClass="db.r6g.large",
                                 Engine="docdb", DBClusterIdentifier=cluster_id)
        endpoint = _wait_available(docdb, db_id, timeout=300)["Endpoint"]
        client = pymongo.MongoClient(endpoint["Address"], int(endpoint["Port"]), username="mainuser",
                                     password="Passw0rd123", tls=True, tlsAllowInvalidCertificates=True,
                                     directConnection=True, serverSelectionTimeoutMS=60000)
        client.app.items.insert_one({"k": "v"})
        assert client.app.items.find_one({"k": "v"}, {"_id": 0}) == {"k": "v"}
        client.close()
    finally:
        docdb.delete_db_instance(DBInstanceIdentifier=db_id)
        deadline = time.time() + 60
        while docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["DBClusterMembers"]:
            if time.time() > deadline:
                break
            time.sleep(1)
        docdb.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)


def test_docdb_and_rds_share_the_cluster_listing(docdb, rds):
    docdb_id = f"docdb-{_uuid_mod.uuid4().hex[:8]}"
    rds_id = f"aurora-{_uuid_mod.uuid4().hex[:8]}"
    _docdb_cluster(docdb, docdb_id)
    rds.create_db_cluster(DBClusterIdentifier=rds_id, Engine="aurora-postgresql",
                          MasterUsername="mainuser", MasterUserPassword="Passw0rd123")
    try:
        assert rds.describe_db_clusters(DBClusterIdentifier=rds_id)["DBClusters"][0]["Engine"] == "aurora-postgresql"
        listed = {c["DBClusterIdentifier"]: c["Engine"] for c in docdb.describe_db_clusters()["DBClusters"]}
        assert listed[docdb_id] == "docdb" and listed[rds_id] == "aurora-postgresql"
        only_docdb = docdb.describe_db_clusters(Filters=[{"Name": "engine", "Values": ["docdb"]}])["DBClusters"]
        assert docdb_id in [c["DBClusterIdentifier"] for c in only_docdb]
        assert rds_id not in [c["DBClusterIdentifier"] for c in only_docdb]
    finally:
        docdb.delete_db_cluster(DBClusterIdentifier=docdb_id, SkipFinalSnapshot=True)
        rds.delete_db_cluster(DBClusterIdentifier=rds_id, SkipFinalSnapshot=True)


def test_docdb_cluster_parameter_group_must_exist(docdb):
    with pytest.raises(ClientError) as exc:
        _docdb_cluster(docdb, f"docdb-{_uuid_mod.uuid4().hex[:8]}", DBClusterParameterGroupName="missing-group")
    assert exc.value.response["Error"]["Code"] == "DBClusterParameterGroupNotFound"
