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


def test_docdb_create_instance(docdb):
    docdb.create_db_instance(
        DBInstanceIdentifier="test-docdb",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="password123",
        DBName="admin",
        AllocatedStorage=20,
    )
    resp = docdb.describe_db_instances(DBInstanceIdentifier="test-docdb")
    instances = resp["DBInstances"]
    assert len(instances) == 1
    assert instances[0]["DBInstanceIdentifier"] == "test-docdb"
    assert instances[0]["Engine"] == "docdb"
    assert "Address" in instances[0]["Endpoint"]
    assert instances[0]["Endpoint"]["Port"] in (27017, 27117, 27118)  # allocated or internal


def test_docdb_engines(docdb):
    resp = docdb.describe_db_engine_versions(Engine="docdb")
    assert len(resp["DBEngineVersions"]) > 0
    assert all(v["Engine"] == "docdb" for v in resp["DBEngineVersions"])


def test_docdb_cluster(docdb):
    docdb.create_db_cluster(
        DBClusterIdentifier="test-docdb-cluster",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="password123",
    )
    resp = docdb.describe_db_clusters(DBClusterIdentifier="test-docdb-cluster")
    assert resp["DBClusters"][0]["DBClusterIdentifier"] == "test-docdb-cluster"


def test_docdb_create_instance_v2(docdb):
    resp = docdb.create_db_instance(
        DBInstanceIdentifier="docdb-ci-v2",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="pass123",
        AllocatedStorage=20,
    )
    inst = resp["DBInstance"]
    assert inst["DBInstanceIdentifier"] == "docdb-ci-v2"
    assert inst["DBInstanceStatus"] == "available"
    assert inst["Engine"] == "docdb"
    assert "Address" in inst["Endpoint"]
    assert "Port" in inst["Endpoint"]


def test_docdb_describe_instances_v2(docdb):
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-di-v2a",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-di-v2b",
        DBInstanceClass="db.t3.large",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="pass",
        AllocatedStorage=20,
    )
    resp = docdb.describe_db_instances()
    ids = [i["DBInstanceIdentifier"] for i in resp["DBInstances"]]
    assert "docdb-di-v2a" in ids
    assert "docdb-di-v2b" in ids

    resp2 = docdb.describe_db_instances(DBInstanceIdentifier="docdb-di-v2a")
    assert len(resp2["DBInstances"]) == 1
    assert resp2["DBInstances"][0]["Engine"] == "docdb"


def test_docdb_delete_instance_v2(docdb):
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-del-v2",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    docdb.delete_db_instance(DBInstanceIdentifier="docdb-del-v2", SkipFinalSnapshot=True)
    with pytest.raises(ClientError) as exc:
        docdb.describe_db_instances(DBInstanceIdentifier="docdb-del-v2")
    assert exc.value.response["Error"]["Code"] == "DBInstanceNotFound"


def test_docdb_modify_instance_v2(docdb):
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-mod-v2",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="pass",
        AllocatedStorage=20,
    )
    docdb.modify_db_instance(
        DBInstanceIdentifier="docdb-mod-v2",
        DBInstanceClass="db.t3.large",
        AllocatedStorage=50,
        ApplyImmediately=True,
    )
    resp = docdb.describe_db_instances(DBInstanceIdentifier="docdb-mod-v2")
    inst = resp["DBInstances"][0]
    assert inst["DBInstanceClass"] == "db.t3.large"
    assert inst["AllocatedStorage"] == 50


def test_docdb_create_cluster_v2(docdb):
    resp = docdb.create_db_cluster(
        DBClusterIdentifier="docdb-cc-v2",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="pass123",
    )
    cluster = resp["DBCluster"]
    assert cluster["DBClusterIdentifier"] == "docdb-cc-v2"
    assert cluster["Status"] == "available"
    assert cluster["Engine"] == "docdb"
    assert "DBClusterArn" in cluster

    desc = docdb.describe_db_clusters(DBClusterIdentifier="docdb-cc-v2")
    assert desc["DBClusters"][0]["DBClusterIdentifier"] == "docdb-cc-v2"


def test_docdb_engine_versions_v2(docdb):
    dv = docdb.describe_db_engine_versions(Engine="docdb")
    assert len(dv["DBEngineVersions"]) > 0
    assert all(v["Engine"] == "docdb" for v in dv["DBEngineVersions"])


def test_docdb_snapshot_v2(docdb):
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-snap-v2",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="pass",
        AllocatedStorage=10,
    )
    resp = docdb.create_db_snapshot(
        DBSnapshotIdentifier="docdb-snap-v2-s1",
        DBInstanceIdentifier="docdb-snap-v2",
    )
    snap = resp["DBSnapshot"]
    assert snap["DBSnapshotIdentifier"] == "docdb-snap-v2-s1"
    assert snap["Status"] == "available"

    desc = docdb.describe_db_snapshots(DBSnapshotIdentifier="docdb-snap-v2-s1")
    assert len(desc["DBSnapshots"]) == 1

    docdb.delete_db_snapshot(DBSnapshotIdentifier="docdb-snap-v2-s1")
    with pytest.raises(ClientError) as exc:
        docdb.describe_db_snapshots(DBSnapshotIdentifier="docdb-snap-v2-s1")
    assert exc.value.response["Error"]["Code"] == "DBSnapshotNotFound"


def test_docdb_tags_v2(docdb):
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-tag-v2",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="pass",
        AllocatedStorage=10,
        Tags=[{"Key": "env", "Value": "dev"}],
    )
    arn = docdb.describe_db_instances(DBInstanceIdentifier="docdb-tag-v2")["DBInstances"][0]["DBInstanceArn"]

    tags = docdb.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert any(t["Key"] == "env" and t["Value"] == "dev" for t in tags)

    docdb.add_tags_to_resource(ResourceName=arn, Tags=[{"Key": "team", "Value": "dba"}])
    tags2 = docdb.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert any(t["Key"] == "team" and t["Value"] == "dba" for t in tags2)

    docdb.remove_tags_from_resource(ResourceName=arn, TagKeys=["env"])
    tags3 = docdb.list_tags_for_resource(ResourceName=arn)["TagList"]
    assert not any(t["Key"] == "env" for t in tags3)
    assert any(t["Key"] == "team" for t in tags3)


def test_docdb_deletion_protection(docdb):
    docdb.create_db_instance(
        DBInstanceIdentifier="docdb-protected",
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="password",
        AllocatedStorage=20,
        DeletionProtection=True,
    )
    try:
        with pytest.raises(ClientError) as exc:
            docdb.delete_db_instance(DBInstanceIdentifier="docdb-protected")
        assert exc.value.response["Error"]["Code"] == "InvalidParameterCombination"
    finally:
        docdb.modify_db_instance(
            DBInstanceIdentifier="docdb-protected",
            DeletionProtection=False,
            ApplyImmediately=True,
        )
        docdb.delete_db_instance(DBInstanceIdentifier="docdb-protected", SkipFinalSnapshot=True)


# --- Docker image helper tests (always run, no container required) ---

def test_docker_image_for_docdb_basic():
    from ministack.services.documentdb import _docker_image_for_docdb
    image, env, port, data_path = _docker_image_for_docdb("5.0.0", "root", "secret", "admin")
    assert "mongo:" in image
    assert env["MONGO_INITDB_ROOT_USERNAME"] == "root"
    assert env["MONGO_INITDB_ROOT_PASSWORD"] == "secret"
    assert port == 27017
    assert data_path == "/data/db"


# --- Pymongo smoke test (Docker + pymongo gated) ---

def _wait_for_docdb(docdb_client, db_id, timeout=90):
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = docdb_client.describe_db_instances(DBInstanceIdentifier=db_id)
        inst = resp["DBInstances"][0]
        if inst["DBInstanceStatus"] == "available":
            return inst
        time.sleep(2)
    raise TimeoutError(f"DocDB instance {db_id} not available after {timeout}s")


def test_docdb_pymongo_smoke(docdb):
    """If Docker and pymongo are available, create a real instance and exercise the wire protocol."""
    try:
        import pymongo
    except ImportError:
        pytest.skip("pymongo not installed")

    # Best-effort docker presence check (service will skip container if absent)
    try:
        import docker
        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker not available for DocDB container launch")

    db_id = "docdb-pymongo-smoke"
    docdb.create_db_instance(
        DBInstanceIdentifier=db_id,
        DBInstanceClass="db.t3.medium",
        Engine="docdb",
        MasterUsername="root",
        MasterUserPassword="password123",
    )

    try:
        inst = _wait_for_docdb(docdb, db_id)
        ep = inst["Endpoint"]
        host = ep["Address"]
        port = int(ep["Port"])
        user = inst["MasterUsername"]
        pwd = "password123"  # we know what we passed

        # Connect and do a trivial round-trip
        client = pymongo.MongoClient(
            host, port,
            username=user, password=pwd,
            serverSelectionTimeoutMS=15000,
            directConnection=True,
        )
        try:
            db = client["smoketest"]
            coll = db["items"]
            coll.insert_one({"k": 1, "msg": "hello from pymongo via docdb"})
            found = coll.find_one({"k": 1})
            assert found is not None
            assert found["msg"].startswith("hello")
        finally:
            client.close()
    finally:
        try:
            docdb.delete_db_instance(DBInstanceIdentifier=db_id, SkipFinalSnapshot=True)
        except Exception:
            pass
