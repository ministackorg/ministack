"""
Tests for RDS Data API service emulator.
Since no real DB containers are available in CI, these tests focus on:
- API routing (requests reach the handler, not 404)
- Parameter validation (missing resourceArn, missing sql, etc.)
- Transaction lifecycle error paths
- Invalid resource ARN handling
"""

import json
import os
import urllib.request
import uuid

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
REGION = "us-east-1"
ACCOUNT_ID = "000000000000"

FAKE_CLUSTER_ARN = f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:cluster:nonexistent-cluster"
FAKE_SECRET_ARN = f"arn:aws:secretsmanager:{REGION}:{ACCOUNT_ID}:secret:nonexistent-secret"


def _raw_post(path, body):
    """Send a raw POST to the MiniStack endpoint (bypassing boto3 since
    rds-data uses REST paths like /Execute)."""
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{ENDPOINT}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    # 30s instead of 10s: under CI xdist contention the server's first
    # call into _resolve_cluster triggers a lazy `from ministack.services
    # import rds` whose import-time block can exceed 10s on the shared
    # 2-core Linux runner. Handler itself is sub-ms once rds is loaded,
    # so 30s leaves a wide margin without making real failures slow.
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def _regional_client(service, region, access_key_id="test"):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        aws_access_key_id=access_key_id,
        aws_secret_access_key="test",
        region_name=region,
        config=Config(region_name=region, retries={"mode": "standard"}),
    )


# ── Routing tests ──────────────────────────────────────────

def test_execute_route_exists():
    """POST /Execute reaches the rds-data handler (not a 404)."""
    status, body = _raw_post("/Execute", {})
    # Should get a 400 (missing params), not 404
    assert status == 400
    assert "BadRequestException" in str(body) or "resourceArn" in str(body)


def test_begin_transaction_route_exists():
    """POST /BeginTransaction reaches the rds-data handler."""
    status, body = _raw_post("/BeginTransaction", {})
    assert status == 400


def test_commit_transaction_route_exists():
    """POST /CommitTransaction reaches the rds-data handler."""
    status, body = _raw_post("/CommitTransaction", {})
    assert status == 400


def test_rollback_transaction_route_exists():
    """POST /RollbackTransaction reaches the rds-data handler."""
    status, body = _raw_post("/RollbackTransaction", {})
    assert status == 400


def test_batch_execute_route_exists():
    """POST /BatchExecute reaches the rds-data handler."""
    status, body = _raw_post("/BatchExecute", {})
    assert status == 400


# ── Parameter validation ───────────────────────────────────

def test_execute_missing_resource_arn():
    status, body = _raw_post("/Execute", {
        "secretArn": FAKE_SECRET_ARN,
        "sql": "SELECT 1",
    })
    assert status == 400
    assert "resourceArn" in body.get("message", body.get("Message", ""))


def test_execute_missing_secret_arn():
    status, body = _raw_post("/Execute", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "sql": "SELECT 1",
    })
    assert status == 400
    assert "secretArn" in body.get("message", body.get("Message", ""))


def test_execute_missing_sql():
    status, body = _raw_post("/Execute", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "secretArn": FAKE_SECRET_ARN,
    })
    assert status == 400
    assert "sql" in body.get("message", body.get("Message", ""))


def test_batch_execute_missing_sql():
    status, body = _raw_post("/BatchExecute", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "secretArn": FAKE_SECRET_ARN,
    })
    assert status == 400
    assert "sql" in body.get("message", body.get("Message", ""))


# ── Invalid ARN ────────────────────────────────────────────

def test_execute_nonexistent_cluster():
    """ExecuteStatement with a non-existent cluster ARN returns an error."""
    status, body = _raw_post("/Execute", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "secretArn": FAKE_SECRET_ARN,
        "sql": "SELECT 1",
    })
    assert status == 400
    assert "not found" in body.get("message", body.get("Message", "")).lower()


def test_rds_data_resolves_cluster_arn_by_arn_region(rds, sm):
    east_rds = _regional_client("rds", "us-east-1")
    west_rds = _regional_client("rds", "us-west-2")
    east_data = _regional_client("rds-data", "us-east-1")
    cluster_id = f"rds-data-region-{uuid.uuid4().hex[:8]}"
    secret_arn = sm.create_secret(
        Name=f"rds-data-region-secret-{uuid.uuid4().hex[:8]}",
        SecretString='{"username":"admin","password":"testpass123"}',
    )["ARN"]

    try:
        east_rds.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="testpass123",
        )
        west_rds.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="testpass123",
        )
        east_arn = east_rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["DBClusterArn"]
        west_arn = west_rds.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]["DBClusterArn"]

        east_data.execute_statement(
            resourceArn=west_arn,
            secretArn=secret_arn,
            sql="CREATE DATABASE west_only_db",
        )
        east_resp = east_data.execute_statement(
            resourceArn=east_arn,
            secretArn=secret_arn,
            sql="SHOW DATABASES",
        )
        west_resp = east_data.execute_statement(
            resourceArn=west_arn,
            secretArn=secret_arn,
            sql="SHOW DATABASES",
        )

        east_names = [r[0]["stringValue"] for r in east_resp.get("records", [])]
        west_names = [r[0]["stringValue"] for r in west_resp.get("records", [])]
        assert "west_only_db" not in east_names
        assert "west_only_db" in west_names
    finally:
        for client in (east_rds, west_rds):
            try:
                client.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
            except ClientError:
                pass


def test_execute_rejects_foreign_account_cluster_arn():
    """ExecuteStatement should not resolve a cluster ARN from another account."""
    owner_account = "111111111111"
    owner = _regional_client("rds", REGION, access_key_id=owner_account)
    cluster_id = f"rds-data-foreign-account-{uuid.uuid4().hex[:8]}"

    try:
        cluster = owner.create_db_cluster(
            DBClusterIdentifier=cluster_id,
            Engine="aurora-mysql",
            MasterUsername="admin",
            MasterUserPassword="password123",
        )["DBCluster"]

        assert f":{owner_account}:" in cluster["DBClusterArn"]
        status, body = _raw_post("/Execute", {
            "resourceArn": cluster["DBClusterArn"],
            "secretArn": FAKE_SECRET_ARN,
            "sql": "SELECT 1",
        })
        assert status == 400
        assert "not found" in body.get("message", body.get("Message", "")).lower()
    finally:
        try:
            owner.delete_db_cluster(DBClusterIdentifier=cluster_id, SkipFinalSnapshot=True)
        except ClientError:
            pass


def test_cluster_id_from_arn_includes_account_region_and_resource_tail():
    from ministack.services import rds_data

    assert (
        rds_data._cluster_id_from_arn(
            "arn:aws:rds:us-east-1:000000000000:cluster:cluster-id:tail",
        )
        == "000000000000:us-east-1:cluster:cluster-id:tail"
    )


def test_rds_data_member_lookup_uses_resource_arn_region():
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import rds, rds_data

    original_account = get_account_id()
    original_region = get_region()
    cluster_id = f"rds-data-member-{uuid.uuid4().hex[:8]}"
    east_arn = f"arn:aws:rds:us-east-1:{ACCOUNT_ID}:cluster:{cluster_id}"
    west_arn = f"arn:aws:rds:us-west-2:{ACCOUNT_ID}:cluster:{cluster_id}"
    east_instance = {
        "DBInstanceIdentifier": f"{cluster_id}-east",
        "DBClusterIdentifier": cluster_id,
        "Engine": "aurora-mysql",
        "Endpoint": {"Address": "east.example", "Port": 3306},
    }
    west_instance = {
        "DBInstanceIdentifier": f"{cluster_id}-west",
        "DBClusterIdentifier": cluster_id,
        "Engine": "aurora-mysql",
        "Endpoint": {"Address": "west.example", "Port": 3306},
    }

    try:
        set_request_account_id(ACCOUNT_ID)
        rds.reset()
        rds._clusters.set_scoped(
            ACCOUNT_ID,
            "us-east-1",
            cluster_id,
            {"DBClusterIdentifier": cluster_id, "DBClusterArn": east_arn, "Engine": "aurora-mysql"},
        )
        rds._clusters.set_scoped(
            ACCOUNT_ID,
            "us-west-2",
            cluster_id,
            {"DBClusterIdentifier": cluster_id, "DBClusterArn": west_arn, "Engine": "aurora-mysql"},
        )
        rds._instances.set_scoped(ACCOUNT_ID, "us-east-1", east_instance["DBInstanceIdentifier"], east_instance)
        rds._instances.set_scoped(ACCOUNT_ID, "us-west-2", west_instance["DBInstanceIdentifier"], west_instance)

        set_request_region("us-east-1")
        instance, engine = rds_data._resolve_cluster(west_arn)

        assert instance is west_instance
        assert engine == "aurora-mysql"
    finally:
        rds.reset()
        set_request_account_id(original_account)
        set_request_region(original_region)


def test_rds_data_db_arn_lookup_rejects_foreign_account():
    from ministack.core.responses import (
        get_account_id,
        set_request_account_id,
    )
    from ministack.services import rds, rds_data

    original_account = get_account_id()
    db_id = f"rds-data-db-{uuid.uuid4().hex[:8]}"
    db_arn = f"arn:aws:rds:us-east-1:111111111111:db:{db_id}"
    instance = {
        "DBInstanceIdentifier": db_id,
        "Engine": "aurora-mysql",
        "Endpoint": {"Address": "foreign.example", "Port": 3306},
    }

    try:
        rds.reset()
        rds._instances.set_scoped("111111111111", "us-east-1", db_id, instance)
        set_request_account_id(ACCOUNT_ID)

        assert rds_data._resolve_cluster(db_arn) == (None, None)
    finally:
        rds.reset()
        set_request_account_id(original_account)


def test_begin_transaction_nonexistent_cluster():
    """BeginTransaction with a non-existent cluster ARN returns an error."""
    status, body = _raw_post("/BeginTransaction", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "secretArn": FAKE_SECRET_ARN,
    })
    assert status == 400
    assert "not found" in body.get("message", body.get("Message", "")).lower()


def test_batch_execute_nonexistent_cluster():
    status, body = _raw_post("/BatchExecute", {
        "resourceArn": FAKE_CLUSTER_ARN,
        "secretArn": FAKE_SECRET_ARN,
        "sql": "INSERT INTO t VALUES (1)",
    })
    assert status == 400
    assert "not found" in body.get("message", body.get("Message", "")).lower()


# ── Transaction lifecycle (error paths) ────────────────────

def test_commit_missing_transaction_id():
    status, body = _raw_post("/CommitTransaction", {})
    assert status == 400
    assert "transactionId" in body.get("message", body.get("Message", ""))


def test_rollback_missing_transaction_id():
    status, body = _raw_post("/RollbackTransaction", {})
    assert status == 400
    assert "transactionId" in body.get("message", body.get("Message", ""))


def test_commit_nonexistent_transaction():
    status, body = _raw_post("/CommitTransaction", {
        "transactionId": "nonexistent-txn-id",
    })
    assert status == 404
    assert "not found" in body.get("message", body.get("Message", "")).lower()


def test_rollback_nonexistent_transaction():
    status, body = _raw_post("/RollbackTransaction", {
        "transactionId": "nonexistent-txn-id",
    })
    assert status == 404
    assert "not found" in body.get("message", body.get("Message", "")).lower()


# ── Invalid JSON ───────────────────────────────────────────

def test_execute_invalid_json():
    """Malformed JSON body returns BadRequestException."""
    req = urllib.request.Request(
        f"{ENDPOINT}/Execute",
        data=b"not-json{{{",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        resp = urllib.request.urlopen(req, timeout=10)
        status = resp.status
        body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        status = e.code
        body = json.loads(e.read())
    assert status == 400
    assert "Invalid JSON" in body.get("message", body.get("Message", ""))


# ── Parameter conversion (unit tests) ─────────────────────

def test_convert_parameters_all_types():
    """_convert_parameters handles all RDS Data API value types."""
    from ministack.services.rds_data import _convert_parameters

    params = [
        {"name": "s", "value": {"stringValue": "hello"}},
        {"name": "n", "value": {"longValue": 42}},
        {"name": "d", "value": {"doubleValue": 3.14}},
        {"name": "b", "value": {"booleanValue": True}},
        {"name": "null_val", "value": {"isNull": True}},
        {"name": "blob", "value": {"blobValue": "AQID"}},  # base64 of b'\x01\x02\x03'
    ]
    result = _convert_parameters(params)
    assert result["s"] == "hello"
    assert result["n"] == 42
    assert result["d"] == 3.14
    assert result["b"] is True
    assert result["null_val"] is None
    assert result["blob"] == b"\x01\x02\x03"


def test_convert_parameters_empty():
    """_convert_parameters returns empty dict for empty/None input."""
    from ministack.services.rds_data import _convert_parameters

    assert _convert_parameters([]) == {}
    assert _convert_parameters(None) == {}


def test_convert_parameters_missing_name_skipped():
    """Parameters without a name are skipped."""
    from ministack.services.rds_data import _convert_parameters

    params = [
        {"value": {"stringValue": "no-name"}},
        {"name": "valid", "value": {"stringValue": "ok"}},
    ]
    result = _convert_parameters(params)
    assert len(result) == 1
    assert result["valid"] == "ok"


def test_convert_parameters_empty_value():
    """Parameter with empty value object returns None."""
    from ministack.services.rds_data import _convert_parameters

    result = _convert_parameters([{"name": "x", "value": {}}])
    assert result["x"] is None


def test_substitute_named_params_distinct_numeric_tokens():
    """:1 and :10 are distinct placeholders, not substring shadows (#957 bug 2).

    A naive substring replace of ":1" first corrupts ":10" into "<val>0". Token
    matching keeps them distinct regardless of supply order."""
    from ministack.services.rds_data import _substitute_named_params

    params = {"1": "ONE", "10": "TEN"}
    assert _substitute_named_params("SELECT :1 AS one, :10 AS ten", params) == \
        "SELECT %(1)s AS one, %(10)s AS ten"
    # Order of the names in the dict must not change the result.
    assert _substitute_named_params("SELECT :10 AS ten, :1 AS one", {"10": "TEN", "1": "ONE"}) == \
        "SELECT %(10)s AS ten, %(1)s AS one"


def test_substitute_named_params_preserves_cast_and_unknowns():
    """A ::type cast is left intact and a :word that is not a supplied parameter
    passes through unchanged instead of becoming a bad placeholder."""
    from ministack.services.rds_data import _substitute_named_params

    assert _substitute_named_params("SELECT :val::jsonb", {"val": "{}"}) == "SELECT %(val)s::jsonb"
    assert _substitute_named_params("SELECT :foo", {"bar": 1}) == "SELECT :foo"
    assert _substitute_named_params("SELECT 1", {}) == "SELECT 1"
    # A longer unrelated :token must not be partially eaten by a shorter param
    # name. Substring replace of ":id" would corrupt ":identity"; token matching
    # leaves it whole because it is not a supplied parameter.
    assert _substitute_named_params("SELECT :id, :identity", {"id": 1}) == "SELECT %(id)s, :identity"


# ── Stub mode tests ────────────────────────────────────────

def _setup_stub_cluster(rds, sm):
    """Create an RDS cluster (no real DB container) and a secret for stub testing."""
    import uuid as _uuid
    cluster_id = f"stub-test-{_uuid.uuid4().hex[:8]}"
    rds.create_db_cluster(
        DBClusterIdentifier=cluster_id,
        Engine="aurora-mysql",
        MasterUsername="admin",
        MasterUserPassword="testpass123",
    )
    secret_arn = sm.create_secret(
        Name=f"stub-secret-{_uuid.uuid4().hex[:8]}",
        SecretString='{"username":"admin","password":"testpass123"}',
    )["ARN"]
    cluster_arn = f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:cluster:{cluster_id}"
    return cluster_arn, secret_arn


def _exec(cluster_arn, secret_arn, sql):
    """Execute a SQL statement via the stub and return (status, body)."""
    return _raw_post("/Execute", {
        "resourceArn": cluster_arn,
        "secretArn": secret_arn,
        "sql": sql,
    })


def test_rds_data_stub_create_and_query_databases(rds, sm):
    """CREATE DATABASE via stub, then query information_schema.schemata."""
    cluster_arn, secret_arn = _setup_stub_cluster(rds, sm)

    status, _ = _exec(cluster_arn, secret_arn, "CREATE DATABASE myappdb")
    assert status == 200

    status, body = _exec(
        cluster_arn, secret_arn,
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('myappdb')",
    )
    assert status == 200
    names = [r[0]["stringValue"] for r in body.get("records", [])]
    assert "myappdb" in names


def test_rds_data_stub_create_and_query_users(rds, sm):
    """CREATE USER via stub, then query mysql.user."""
    cluster_arn, secret_arn = _setup_stub_cluster(rds, sm)

    status, _ = _exec(cluster_arn, secret_arn, "CREATE USER 'appuser'@'%' IDENTIFIED BY 'pass'")
    assert status == 200

    status, body = _exec(
        cluster_arn, secret_arn,
        "SELECT User FROM mysql.user WHERE User='appuser'",
    )
    assert status == 200
    names = [r[0]["stringValue"] for r in body.get("records", [])]
    assert "appuser" in names


def test_rds_data_stub_grant_and_show_grants(rds, sm):
    """GRANT privileges, then SHOW GRANTS FOR."""
    cluster_arn, secret_arn = _setup_stub_cluster(rds, sm)

    _exec(cluster_arn, secret_arn, "CREATE USER 'grantee'@'%' IDENTIFIED BY 'pass'")
    status, _ = _exec(
        cluster_arn, secret_arn,
        "GRANT ALL PRIVILEGES ON mydb.* TO 'grantee'@'%'",
    )
    assert status == 200

    status, body = _exec(cluster_arn, secret_arn, "SHOW GRANTS FOR 'grantee'")
    assert status == 200
    grants = [r[0]["stringValue"] for r in body.get("records", [])]
    assert any("GRANT" in g and "grantee" in g for g in grants)


def test_rds_data_stub_drop_database(rds, sm):
    """CREATE then DROP DATABASE, verify gone from queries."""
    cluster_arn, secret_arn = _setup_stub_cluster(rds, sm)

    _exec(cluster_arn, secret_arn, "CREATE DATABASE dropme")
    _exec(cluster_arn, secret_arn, "DROP DATABASE dropme")

    status, body = _exec(
        cluster_arn, secret_arn,
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'dropme'",
    )
    assert status == 200
    names = [r[0]["stringValue"] for r in body.get("records", [])]
    assert "dropme" not in names


def test_rds_data_stub_drop_user(rds, sm):
    """CREATE then DROP USER, verify gone from queries."""
    cluster_arn, secret_arn = _setup_stub_cluster(rds, sm)

    _exec(cluster_arn, secret_arn, "CREATE USER 'tempuser'@'%' IDENTIFIED BY 'pass'")
    _exec(cluster_arn, secret_arn, "DROP USER 'tempuser'@'%'")

    status, body = _exec(
        cluster_arn, secret_arn,
        "SELECT User FROM mysql.user WHERE User='tempuser'",
    )
    assert status == 200
    # Should return no records (empty records list from _stub_success)
    records = body.get("records", [])
    names = [r[0]["stringValue"] for r in records] if records else []
    assert "tempuser" not in names


def test_rds_data_real_endpoint_connection_failure_is_transient(monkeypatch):
    """Endpoint-backed clusters must not acknowledge writes through stub mode."""
    from ministack.services import rds_data

    rds_data.reset()
    resource_arn = f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:cluster:real-cluster"
    instance = {
        "Endpoint": {"Address": "10.0.0.10", "Port": 3306},
        "_docker_container_id": "container-id",
        "Engine": "aurora-mysql",
    }

    monkeypatch.setattr(rds_data, "_resolve_cluster", lambda _arn: (instance, "aurora-mysql"))
    monkeypatch.setattr(rds_data, "_get_secret_credentials", lambda _arn: ("admin", "pw"))

    def _raise_connection_error(*_args, **_kwargs):
        raise OSError("Connection refused")

    monkeypatch.setattr(rds_data, "_connect", _raise_connection_error)

    status, headers, body = rds_data._execute_statement({
        "resourceArn": resource_arn,
        "secretArn": FAKE_SECRET_ARN,
        "sql": "CREATE USER 'lost_write'@'%' IDENTIFIED BY 'pw'",
    })
    payload = json.loads(body)

    assert status == 504
    assert headers["x-amzn-errortype"] == "DatabaseUnavailableException"
    assert payload["__type"] == "DatabaseUnavailableException"
    assert "lost_write" not in rds_data._stub_users.get("real-cluster", set())


def test_rds_data_non_container_endpoint_keeps_stub_mode(monkeypatch):
    """Control-plane-only instances still use the lightweight SQL stub."""
    from ministack.services import rds_data

    rds_data.reset()
    resource_arn = f"arn:aws:rds:{REGION}:{ACCOUNT_ID}:cluster:stub-cluster"
    instance = {
        "Endpoint": {"Address": "localhost", "Port": 3306},
        "_docker_container_id": None,
        "Engine": "aurora-mysql",
    }
    monkeypatch.setattr(rds_data, "_resolve_cluster", lambda _arn: (instance, "aurora-mysql"))

    status, _headers, body = rds_data._execute_statement({
        "resourceArn": resource_arn,
        "secretArn": FAKE_SECRET_ARN,
        "sql": "CREATE USER 'stub_user'@'%' IDENTIFIED BY 'pw'",
    })
    payload = json.loads(body)

    assert status == 200
    assert payload["records"] == []
    assert "stub_user" in rds_data._stub_users.get(
        rds_data._cluster_id_from_arn(resource_arn),
        set(),
    )


def test_rds_data_lock_wait_timeout_is_not_connection_error():
    """MySQL lock wait timeout is a SQL error, not endpoint unavailability."""
    from ministack.services import rds_data

    assert not rds_data._is_connection_error(
        Exception("(1205, 'Lock wait timeout exceeded; try restarting transaction')")
    )


def test_rds_cluster_status_tracks_member_readiness():
    """Parent clusters remain creating until their member instance is available."""
    from ministack.services import rds

    cluster_id = "readiness-cluster"
    instance_id = "readiness-instance"
    rds._clusters[cluster_id] = {
        "DBClusterIdentifier": cluster_id,
        "Status": "available",
        "DBClusterMembers": [],
    }
    rds._instances[instance_id] = {
        "DBInstanceIdentifier": instance_id,
        "DBClusterIdentifier": cluster_id,
        "DBInstanceStatus": "creating",
        "PromotionTier": 1,
    }

    try:
        rds._register_instance_in_cluster(rds._instances[instance_id])
        assert rds._clusters[cluster_id]["Status"] == "creating"

        rds._instances[instance_id]["DBInstanceStatus"] = "available"
        rds._refresh_cluster_status(cluster_id)
        assert rds._clusters[cluster_id]["Status"] == "available"
    finally:
        rds._instances.pop(instance_id, None)
        rds._clusters.pop(cluster_id, None)


def test_rds_cluster_endpoints_sync_to_backing_instance():
    """Aurora cluster endpoints track the registered local writer instance."""
    from ministack.services import rds

    cluster_id = "endpoint-sync-cluster"
    instance_id = "endpoint-sync-instance"
    rds._clusters[cluster_id] = {
        "DBClusterIdentifier": cluster_id,
        "Status": "available",
        "Endpoint": "original.cluster.local",
        "ReaderEndpoint": "original-ro.cluster.local",
        "Port": 3306,
        "DBClusterMembers": [],
    }
    rds._instances[instance_id] = {
        "DBInstanceIdentifier": instance_id,
        "DBClusterIdentifier": cluster_id,
        "DBInstanceStatus": "available",
        "Endpoint": {
            "Address": "10.0.0.12",
            "Port": 3306,
        },
        "PromotionTier": 1,
    }

    try:
        rds._register_instance_in_cluster(rds._instances[instance_id])

        cluster = rds._clusters[cluster_id]
        assert cluster["Endpoint"] == "10.0.0.12"
        assert cluster["ReaderEndpoint"] == "10.0.0.12"
        assert cluster["Port"] == 3306
    finally:
        rds._instances.pop(instance_id, None)
        rds._clusters.pop(cluster_id, None)


def test_rds_data_secret_credentials_parsing():
    """_get_secret_credentials extracts username and password from secret."""
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import rds_data, secretsmanager
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    # Create a secret with JSON credentials
    secretsmanager._secrets["test-cred-secret"] = {
        "ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:test-cred",
        "Name": "test-cred-secret",
        "Versions": {
            "v1": {
                "Stages": ["AWSCURRENT"],
                "SecretString": '{"username":"app_rw","password":"p@ss123"}',
            }
        },
    }
    user, pw = rds_data._get_secret_credentials(
        "arn:aws:secretsmanager:us-east-1:000000000000:secret:test-cred")
    assert user == "app_rw"
    assert pw == "p@ss123"
    # Clean up
    del secretsmanager._secrets["test-cred-secret"]


def test_rds_data_secret_credentials_no_username():
    """_get_secret_credentials returns None username for password-only secret."""
    from ministack.core.responses import set_request_account_id, set_request_region
    from ministack.services import rds_data, secretsmanager
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    secretsmanager._secrets["pw-only-secret"] = {
        "ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:pw-only",
        "Name": "pw-only-secret",
        "Versions": {
            "v1": {
                "Stages": ["AWSCURRENT"],
                "SecretString": '{"password":"just-a-password"}',
            }
        },
    }
    user, pw = rds_data._get_secret_credentials(
        "arn:aws:secretsmanager:us-east-1:000000000000:secret:pw-only")
    assert user is None
    assert pw == "just-a-password"
    del secretsmanager._secrets["pw-only-secret"]


def test_rds_data_secret_credentials_use_secret_arn_region():
    """_get_secret_credentials resolves ARN-scoped secrets outside the request region."""
    from ministack.core.responses import get_region, set_request_account_id, set_request_region
    from ministack.services import rds_data, secretsmanager

    original_region = get_region()
    set_request_account_id("test")
    set_request_region("us-east-1")
    secretsmanager._secrets["cross-region-cred"] = {
        "ARN": "arn:aws:secretsmanager:us-east-1:000000000000:secret:cross-region-cred",
        "Name": "cross-region-cred",
        "Versions": {
            "v1": {
                "Stages": ["AWSCURRENT"],
                "SecretString": '{"username":"app_rw","password":"p@ss123"}',
            }
        },
    }
    try:
        set_request_region("us-west-2")
        user, pw = rds_data._get_secret_credentials(
            "arn:aws:secretsmanager:us-east-1:000000000000:secret:cross-region-cred"
        )
        assert user == "app_rw"
        assert pw == "p@ss123"
    finally:
        set_request_region("us-east-1")
        secretsmanager._secrets.pop("cross-region-cred", None)
        set_request_region(original_region)


def test_rds_data_secret_credentials_reject_cross_account_secret_arn():
    from ministack.core.responses import (
        get_account_id,
        get_region,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import rds_data, secretsmanager

    original_account = get_account_id()
    original_region = get_region()
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    secretsmanager._secrets.set_scoped("111111111111", "us-east-1", "cross-account-cred", {
        "ARN": "arn:aws:secretsmanager:us-east-1:111111111111:secret:cross-account-cred",
        "Name": "cross-account-cred",
        "Versions": {
            "v1": {
                "Stages": ["AWSCURRENT"],
                "SecretString": '{"username":"app_rw","password":"p@ss123"}',
            }
        },
    })
    try:
        assert rds_data._get_secret_credentials(
            "arn:aws:secretsmanager:us-east-1:111111111111:secret:cross-account-cred"
        ) == (None, None)
    finally:
        secretsmanager._secrets.pop_scoped("111111111111", "us-east-1", "cross-account-cred", None)
        set_request_account_id(original_account)
        set_request_region(original_region)
