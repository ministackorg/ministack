"""Live Aurora MySQL shared-container coverage.

These tests require MiniStack to have Docker socket access and its
``DOCKER_NETWORK`` configured so the returned MySQL endpoint is reachable.
"""

import contextlib
import os
import time
import uuid

import pytest
from botocore.exceptions import ClientError

pytestmark = pytest.mark.skipif(
    not os.environ.get("DOCKER_NETWORK"),
    reason="DOCKER_NETWORK not set -- skipping live Aurora MySQL tests",
)

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


def _connect(endpoint, user="admin", password=PASSWORD, database=DATABASE):
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


def test_aurora_writer_data_is_visible_through_reader(rds):
    with _live_cluster(rds) as (_cid, _wid, _rid, writer, reader, _cluster):
        with _connect(writer["Endpoint"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE TABLE shared_rows (id INT PRIMARY KEY, value VARCHAR(32))")
                cursor.execute("INSERT INTO shared_rows VALUES (1, 'writer-data')")
        with _connect(reader["Endpoint"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, value FROM shared_rows")
                assert cursor.fetchall() == ((1, "writer-data"),)


def test_aurora_user_and_grant_are_visible_through_reader(rds):
    with _live_cluster(rds) as (_cid, _wid, _rid, writer, reader, _cluster):
        app_user = f"app_{uuid.uuid4().hex[:8]}"
        app_password = "AppPassword123!"
        with _connect(writer["Endpoint"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE TABLE granted_rows (id INT PRIMARY KEY)")
                cursor.execute(f"CREATE USER `{app_user}`@'%%' IDENTIFIED BY %s", (app_password,))
                cursor.execute(f"GRANT SELECT ON `{DATABASE}`.* TO `{app_user}`@'%'")
        with _connect(reader["Endpoint"], app_user, app_password) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM granted_rows")
                assert cursor.fetchone() == (0,)


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
        with _connect(writer["Endpoint"]) as conn:
            with conn.cursor() as cursor:
                cursor.execute("CREATE TABLE durable_rows (id INT PRIMARY KEY)")
                cursor.execute("INSERT INTO durable_rows VALUES (7)")
        rds.delete_db_instance(
            DBInstanceIdentifier=reader_id,
            SkipFinalSnapshot=True,
        )
        with _connect(writer["Endpoint"]) as conn:
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
            _connect(writer["Endpoint"])
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
            with _connect(replacement["Endpoint"]) as conn:
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


# P2 adds FailoverDBCluster coverage here: role flip plus post-failover write.
