"""
Integration tests for the Aurora DSQL control-plane emulator, plus
validator unit tests and live data-plane (wire proxy) tests.
"""

import asyncio
import re
import socket
import struct
import threading
import time

import pytest
from botocore.exceptions import ClientError

from ministack.core import pgproxy

ID_RE = re.compile(r"^[a-z0-9]{26}$")


def _create(dsql, **kwargs):
    resp = dsql.create_cluster(**kwargs)
    return resp["identifier"]


def _cleanup(dsql, identifier):
    try:
        dsql.update_cluster(identifier=identifier, deletionProtectionEnabled=False)
    except ClientError:
        pass
    try:
        dsql.delete_cluster(identifier=identifier)
    except ClientError:
        pass


def _wait_active(dsql, identifier, timeout=60):
    """Poll GetCluster until the cluster is ACTIVE (backend may spin up async)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        cluster = dsql.get_cluster(identifier=identifier)
        if cluster["status"] == "ACTIVE":
            return cluster
        time.sleep(1)
    raise TimeoutError(f"cluster {identifier} not ACTIVE after {timeout}s")


class TestClusterLifecycle:
    def test_create_get_list_update_delete(self, dsql):
        resp = dsql.create_cluster()
        identifier = resp["identifier"]
        assert ID_RE.match(identifier)
        assert resp["arn"].endswith(f":cluster/{identifier}")
        assert ":dsql:us-east-1:" in resp["arn"]
        # CREATING while the backend spins up (Docker), ACTIVE without it.
        assert resp["status"] in ("CREATING", "ACTIVE")
        assert resp["deletionProtectionEnabled"] is True  # AWS default: on
        assert resp["creationTime"] is not None
        assert resp["endpoint"].startswith("localhost:")

        got = _wait_active(dsql, identifier)
        assert got["identifier"] == identifier
        assert got["tags"] == {}

        listed = dsql.list_clusters()
        assert any(c["identifier"] == identifier for c in listed["clusters"])
        summary = next(c for c in listed["clusters"] if c["identifier"] == identifier)
        assert summary["arn"] == resp["arn"]

        updated = dsql.update_cluster(identifier=identifier, deletionProtectionEnabled=True)
        assert updated["identifier"] == identifier
        assert updated["status"] == "ACTIVE"
        assert dsql.get_cluster(identifier=identifier)["deletionProtectionEnabled"] is True
        dsql.update_cluster(identifier=identifier, deletionProtectionEnabled=False)

        deleted = dsql.delete_cluster(identifier=identifier)
        assert deleted["identifier"] == identifier
        assert deleted["status"] == "DELETING"

        with pytest.raises(ClientError) as exc:
            dsql.get_cluster(identifier=identifier)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_with_multi_region_properties_echoed(self, dsql):
        identifier = _create(
            dsql,
            multiRegionProperties={"witnessRegion": "us-west-2", "clusters": []},
        )
        try:
            got = dsql.get_cluster(identifier=identifier)
            assert got["multiRegionProperties"] == {
                "witnessRegion": "us-west-2",
                "clusters": [],
            }
        finally:
            _cleanup(dsql, identifier)

    def test_encryption_details_default_aws_owned(self, dsql):
        resp = dsql.create_cluster()
        try:
            enc = resp["encryptionDetails"]
            assert enc["encryptionType"] == "AWS_OWNED_KMS_KEY"
            assert enc["encryptionStatus"] == "ENABLED"
            assert ":key/aws/dsql" in enc["kmsKeyArn"]
        finally:
            _cleanup(dsql, resp["identifier"])

    def test_encryption_details_customer_managed(self, dsql, kms_client):
        key_arn = kms_client.create_key()["KeyMetadata"]["Arn"]
        resp = dsql.create_cluster(kmsEncryptionKey=key_arn)
        identifier = resp["identifier"]
        try:
            enc = resp["encryptionDetails"]
            assert enc["encryptionType"] == "CUSTOMER_MANAGED_KMS_KEY"
            assert enc["kmsKeyArn"] == key_arn
            assert enc["encryptionStatus"] == "ENABLED"

            # UpdateCluster with a new key updates encryptionDetails
            new_key = kms_client.create_key()["KeyMetadata"]["Arn"]
            dsql.update_cluster(identifier=identifier, kmsEncryptionKey=new_key)
            got = dsql.get_cluster(identifier=identifier)
            assert got["encryptionDetails"]["kmsKeyArn"] == new_key
            assert got["encryptionDetails"]["encryptionType"] == "CUSTOMER_MANAGED_KMS_KEY"
        finally:
            _cleanup(dsql, identifier)

    def test_kms_key_validated_against_kms_service(self, dsql, kms_client):
        bogus = "arn:aws:kms:us-east-1:000000000000:key/12345678-1234-1234-1234-123456789012"
        with pytest.raises(ClientError) as exc:
            dsql.create_cluster(kmsEncryptionKey=bogus)
        assert exc.value.response["Error"]["Code"] == "ValidationException"

        # A real key (by id, ARN, or alias) is accepted.
        key = kms_client.create_key()["KeyMetadata"]
        kms_client.create_alias(AliasName="alias/dsql-test", TargetKeyId=key["KeyId"])
        for ref in (key["KeyId"], key["Arn"], "alias/dsql-test"):
            resp = dsql.create_cluster(kmsEncryptionKey=ref)
            identifier = resp["identifier"]
            try:
                enc = resp["encryptionDetails"]
                assert enc["encryptionType"] == "CUSTOMER_MANAGED_KMS_KEY"
                # bare ids/aliases expand to a full key ARN
                assert enc["kmsKeyArn"].startswith("arn:aws:kms:")
            finally:
                _cleanup(dsql, identifier)

        # UpdateCluster validates too.
        identifier = _create(dsql)
        try:
            with pytest.raises(ClientError) as exc:
                dsql.update_cluster(identifier=identifier, kmsEncryptionKey=bogus)
            assert exc.value.response["Error"]["Code"] == "ValidationException"
        finally:
            _cleanup(dsql, identifier)

    def test_kms_aws_managed_alias_accepted(self, dsql):
        # AWS-managed aliases exist by definition — no KMS store entry needed.
        resp = dsql.create_cluster(kmsEncryptionKey="alias/aws/dsql")
        try:
            assert resp["encryptionDetails"]["encryptionType"] == "CUSTOMER_MANAGED_KMS_KEY"
        finally:
            _cleanup(dsql, resp["identifier"])

    def test_endpoints_get_unique_ports(self, dsql):
        a = dsql.create_cluster()
        b = dsql.create_cluster()
        try:
            assert a["endpoint"] != b["endpoint"]
            assert a["endpoint"].split(":")[1].isdigit()
        finally:
            _cleanup(dsql, a["identifier"])
            _cleanup(dsql, b["identifier"])

    def test_deletion_protection_blocks_delete(self, dsql):
        identifier = _create(dsql, deletionProtectionEnabled=True)
        try:
            with pytest.raises(ClientError) as exc:
                dsql.delete_cluster(identifier=identifier)
            err = exc.value.response["Error"]
            assert err["Code"] == "ValidationException"
            assert "deletion protection" in err["Message"].lower()
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400

            # Disable protection, then delete succeeds.
            dsql.update_cluster(identifier=identifier, deletionProtectionEnabled=False)
            deleted = dsql.delete_cluster(identifier=identifier)
            assert deleted["status"] == "DELETING"
        finally:
            _cleanup(dsql, identifier)

    def test_unknown_cluster_id(self, dsql):
        unknown = "a" * 26
        for call in (
            lambda: dsql.get_cluster(identifier=unknown),
            lambda: dsql.update_cluster(identifier=unknown, deletionProtectionEnabled=True),
            lambda: dsql.delete_cluster(identifier=unknown),
        ):
            with pytest.raises(ClientError) as exc:
                call()
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

    def test_client_token_idempotency(self, dsql):
        token = "test-idempotency-token-12345"
        first = dsql.create_cluster(clientToken=token)
        second = dsql.create_cluster(clientToken=token)
        try:
            assert second["identifier"] == first["identifier"]
            assert second["arn"] == first["arn"]
        finally:
            _cleanup(dsql, first["identifier"])

    def test_list_clusters_pagination(self, dsql):
        ids = {_create(dsql), _create(dsql)}
        try:
            seen = []
            kwargs = {"maxResults": 1}
            pages = 0
            while True:
                resp = dsql.list_clusters(**kwargs)
                assert len(resp["clusters"]) <= 1
                seen.extend(c["identifier"] for c in resp["clusters"])
                pages += 1
                token = resp.get("nextToken")
                if not token:
                    break
                kwargs["nextToken"] = token
            assert ids.issubset(set(seen))
            assert pages >= 2
        finally:
            for identifier in ids:
                _cleanup(dsql, identifier)

    def test_list_clusters_invalid_next_token(self, dsql):
        with pytest.raises(ClientError) as exc:
            dsql.list_clusters(nextToken="not-a-valid-token")
        assert exc.value.response["Error"]["Code"] == "ValidationException"

    def test_list_clusters_max_results_out_of_range(self):
        # botocore validates maxResults (model min 1 / max 100) client-side,
        # so exercise the server-side validation with a raw HTTP call.
        import urllib.error
        import urllib.request

        for bad in (0, 101):
            req = urllib.request.Request(
                f"http://127.0.0.1:4566/cluster?max-results={bad}",
                headers={"Host": "dsql.us-east-1.localhost"},
            )
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(req, timeout=10)
            assert exc.value.code == 400, bad
            assert exc.value.headers["x-amzn-errortype"] == "ValidationException"

    def test_update_cluster_empty_body_rejected(self, dsql):
        identifier = _create(dsql)
        try:
            with pytest.raises(ClientError) as exc:
                dsql.update_cluster(identifier=identifier)
            err = exc.value.response["Error"]
            assert err["Code"] == "ValidationException"
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400
        finally:
            _cleanup(dsql, identifier)

    def test_delete_cluster_client_token_idempotency(self, dsql):
        identifier = _create(dsql)
        try:
            dsql.update_cluster(identifier=identifier, deletionProtectionEnabled=False)
            token = "delete-idempotency-token-abc"
            first = dsql.delete_cluster(identifier=identifier, clientToken=token)
            assert first["identifier"] == identifier
            assert first["status"] == "DELETING"

            # Second delete with same token returns idempotent success
            second = dsql.delete_cluster(identifier=identifier, clientToken=token)
            assert second["identifier"] == identifier
            assert second["status"] == "DELETING"
            assert second["arn"] == first["arn"]
            assert second["creationTime"] == first["creationTime"]
        finally:
            _cleanup(dsql, identifier)


class TestTags:
    def test_tags_via_create_and_tag_ops(self, dsql):
        resp = dsql.create_cluster(tags={"env": "test", "team": "data"})
        identifier = resp["identifier"]
        arn = resp["arn"]
        try:
            got = dsql.get_cluster(identifier=identifier)
            assert got["tags"] == {"env": "test", "team": "data"}

            listed = dsql.list_tags_for_resource(resourceArn=arn)
            assert listed["tags"] == {"env": "test", "team": "data"}

            dsql.tag_resource(resourceArn=arn, tags={"owner": "alice", "env": "prod"})
            listed = dsql.list_tags_for_resource(resourceArn=arn)
            assert listed["tags"] == {"env": "prod", "team": "data", "owner": "alice"}

            dsql.untag_resource(resourceArn=arn, tagKeys=["team", "owner"])
            listed = dsql.list_tags_for_resource(resourceArn=arn)
            assert listed["tags"] == {"env": "prod"}
        finally:
            _cleanup(dsql, identifier)

    def test_tag_ops_unknown_arn(self, dsql):
        unknown_arn = "arn:aws:dsql:us-east-1:000000000000:cluster/" + "z" * 26
        with pytest.raises(ClientError) as exc:
            dsql.list_tags_for_resource(resourceArn=unknown_arn)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        with pytest.raises(ClientError) as exc:
            dsql.tag_resource(resourceArn=unknown_arn, tags={"k": "v"})
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

        with pytest.raises(ClientError) as exc:
            dsql.untag_resource(resourceArn=unknown_arn, tagKeys=["k"])
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"


class TestClusterPolicy:
    POLICY = '{"Version":"2012-10-17","Statement":[]}'

    def test_policy_put_get_delete(self, dsql):
        identifier = _create(dsql)
        try:
            with pytest.raises(ClientError) as exc:
                dsql.get_cluster_policy(identifier=identifier)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

            put = dsql.put_cluster_policy(identifier=identifier, policy=self.POLICY)
            assert put["policyVersion"] == "1"

            got = dsql.get_cluster_policy(identifier=identifier)
            assert got["policy"] == self.POLICY
            assert got["policyVersion"] == "1"

            put2 = dsql.put_cluster_policy(
                identifier=identifier, policy=self.POLICY, expectedPolicyVersion="1"
            )
            assert put2["policyVersion"] == "2"

            deleted = dsql.delete_cluster_policy(identifier=identifier)
            assert deleted["policyVersion"] == "2"

            with pytest.raises(ClientError) as exc:
                dsql.get_cluster_policy(identifier=identifier)
            assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"
        finally:
            _cleanup(dsql, identifier)

    def test_policy_version_conflict(self, dsql):
        identifier = _create(dsql)
        try:
            dsql.put_cluster_policy(identifier=identifier, policy=self.POLICY)
            with pytest.raises(ClientError) as exc:
                dsql.put_cluster_policy(
                    identifier=identifier, policy=self.POLICY, expectedPolicyVersion="99"
                )
            assert exc.value.response["Error"]["Code"] == "ConflictException"
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        finally:
            _cleanup(dsql, identifier)

    def test_policy_on_unknown_cluster(self, dsql):
        unknown = "z" * 26
        with pytest.raises(ClientError) as exc:
            dsql.put_cluster_policy(identifier=unknown, policy=self.POLICY)
        assert exc.value.response["Error"]["Code"] == "ResourceNotFoundException"

    def test_create_cluster_with_policy(self, dsql):
        resp = dsql.create_cluster(policy=self.POLICY)
        identifier = resp["identifier"]
        try:
            got = dsql.get_cluster_policy(identifier=identifier)
            assert got["policy"] == self.POLICY
            assert got["policyVersion"] == "1"
        finally:
            _cleanup(dsql, identifier)

    def test_delete_policy_with_expected_version_success(self, dsql):
        identifier = _create(dsql)
        try:
            dsql.put_cluster_policy(identifier=identifier, policy=self.POLICY)
            deleted = dsql.delete_cluster_policy(
                identifier=identifier, expectedPolicyVersion="1"
            )
            assert deleted["policyVersion"] == "1"
        finally:
            _cleanup(dsql, identifier)

    def test_delete_policy_expected_version_conflict(self, dsql):
        identifier = _create(dsql)
        try:
            dsql.put_cluster_policy(identifier=identifier, policy=self.POLICY)
            with pytest.raises(ClientError) as exc:
                dsql.delete_cluster_policy(
                    identifier=identifier, expectedPolicyVersion="99"
                )
            assert exc.value.response["Error"]["Code"] == "ConflictException"
            assert exc.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409
        finally:
            _cleanup(dsql, identifier)


class TestBotoParity:
    """Waiters, paginators, and structured error members (botocore model)."""

    def test_cluster_active_and_not_exists_waiters(self, dsql):
        identifier = _create(dsql)
        dsql.get_waiter("cluster_active").wait(
            identifier=identifier, WaiterConfig={"Delay": 1, "MaxAttempts": 60}
        )
        assert dsql.get_cluster(identifier=identifier)["status"] == "ACTIVE"

        dsql.update_cluster(identifier=identifier, deletionProtectionEnabled=False)
        dsql.delete_cluster(identifier=identifier)
        dsql.get_waiter("cluster_not_exists").wait(
            identifier=identifier, WaiterConfig={"Delay": 1, "MaxAttempts": 10}
        )

    def test_list_clusters_paginator(self, dsql):
        ids = {_create(dsql), _create(dsql)}
        try:
            paginator = dsql.get_paginator("list_clusters")
            seen = []
            for page in paginator.paginate(PaginationConfig={"PageSize": 1}):
                seen.extend(c["identifier"] for c in page["clusters"])
            assert ids.issubset(set(seen))
            assert len(seen) >= 2
        finally:
            for identifier in ids:
                _cleanup(dsql, identifier)

    def test_error_members_not_found(self, dsql):
        unknown = "z" * 26
        with pytest.raises(ClientError) as exc:
            dsql.get_cluster(identifier=unknown)
        resp = exc.value.response
        assert resp["Error"]["Code"] == "ResourceNotFoundException"
        # botocore parses modeled error members at the response top level
        assert resp["resourceId"] == unknown
        assert resp["resourceType"] == "cluster"

    def test_error_members_deletion_protection(self, dsql):
        identifier = _create(dsql, deletionProtectionEnabled=True)
        try:
            with pytest.raises(ClientError) as exc:
                dsql.delete_cluster(identifier=identifier)
            resp = exc.value.response
            assert resp["Error"]["Code"] == "ValidationException"
            assert resp["reason"] == "deletionProtectionEnabled"
        finally:
            _cleanup(dsql, identifier)

    def test_update_delete_response_shape(self, dsql):
        identifier = _create(dsql)
        try:
            updated = dsql.update_cluster(
                identifier=identifier, deletionProtectionEnabled=False
            )
            assert set(updated) == {
                "identifier", "arn", "status", "creationTime", "ResponseMetadata",
            }
        finally:
            deleted = dsql.delete_cluster(identifier=identifier)
            assert set(deleted) == {
                "identifier", "arn", "status", "creationTime", "ResponseMetadata",
            }


class TestAuthTokens:
    def test_generate_db_connect_auth_token(self, dsql):
        token = dsql.generate_db_connect_auth_token("localhost", "us-east-1")
        assert isinstance(token, str)
        assert "Action=DbConnect" in token

    def test_generate_db_connect_admin_auth_token(self, dsql):
        token = dsql.generate_db_connect_admin_auth_token("localhost", "us-east-1")
        assert isinstance(token, str)
        assert "Action=DbConnectAdmin" in token


# ---------------------------------------------------------------------------
# Validator unit tests (no server, no Postgres needed)
# ---------------------------------------------------------------------------


class TestClassifyStatement:
    @pytest.mark.parametrize(
        "sql,expected",
        [
            ("CREATE TABLE t (a int)", "ddl"),
            ("alter table t add column b int", "ddl"),
            ("DROP INDEX idx", "ddl"),
            ("INSERT INTO t VALUES (1)", "dml"),
            ("update t set a = 1", "dml"),
            ("DELETE FROM t", "dml"),
            ("BEGIN", "tcl_begin"),
            ("START TRANSACTION", "tcl_begin"),
            ("COMMIT", "tcl_end"),
            ("END", "tcl_end"),
            ("ROLLBACK", "tcl_end"),
            ("ABORT", "tcl_end"),
            ("SELECT 1", "other"),
            ("SET search_path TO public", "other"),
            ("SHOW server_version", "other"),
            ("GRANT SELECT ON t TO r", "other"),
            ("REVOKE SELECT ON t FROM r", "other"),
            # Leading comments must not hide the statement keyword.
            ("-- migration 004\nCREATE TABLE t (a int)", "ddl"),
            ("/* flyway */ INSERT INTO t VALUES (1)", "dml"),
            ("/* a /* nested */ b */ COMMIT", "tcl_end"),
            ("\n\n-- one\n-- two\nBEGIN", "tcl_begin"),
            # CTE-leading writes are still writes.
            ("WITH x AS (SELECT 1) INSERT INTO t SELECT 1", "dml"),
            ("WITH x AS (SELECT 1) UPDATE t SET a = 1", "dml"),
            ("WITH x AS (SELECT 1) DELETE FROM t", "dml"),
            ("WITH RECURSIVE x AS (SELECT 1) INSERT INTO t SELECT 1", "dml"),
            ("WITH x (a, b) AS (SELECT 1, 2) INSERT INTO t SELECT 1", "dml"),
            ("WITH x AS MATERIALIZED (SELECT 1) INSERT INTO t SELECT 1", "dml"),
            ("WITH a AS (SELECT 1), b AS (SELECT 2) INSERT INTO t SELECT 1", "dml"),
            ("WITH x AS (SELECT 1) SELECT * FROM x", "other"),
            # EXPLAIN ANALYZE executes; plain EXPLAIN only plans.
            ("EXPLAIN ANALYZE INSERT INTO t VALUES (1)", "dml"),
            ("EXPLAIN (ANALYZE, BUFFERS) DELETE FROM t", "dml"),
            ("EXPLAIN INSERT INTO t VALUES (1)", "other"),
            ("EXPLAIN SELECT 1", "other"),
        ],
    )
    def test_classify(self, sql, expected):
        assert pgproxy.classify_statement(sql) == expected


class TestSqlLexing:
    """Statement splitting and comment stripping must not be fooled by
    literals — a ';' or ')' inside a string used to truncate the text a
    validation rule saw, so the rule passed on the fragment."""

    @pytest.mark.parametrize(
        "sql,expected",
        [
            ("SELECT 1; SELECT 2", ["SELECT 1", "SELECT 2"]),
            ("SELECT 'a;b'", ["SELECT 'a;b'"]),
            ("SELECT 'it''s; fine'", ["SELECT 'it''s; fine'"]),
            ("SELECT $$a;b$$", ["SELECT $$a;b$$"]),
            ("SELECT $tag$a;b$tag$", ["SELECT $tag$a;b$tag$"]),
            ('SELECT "we;ird"', ['SELECT "we;ird"']),
            ("SELECT 1 -- c;omment\n; SELECT 2", ["SELECT 1 -- c;omment", "SELECT 2"]),
            ("SELECT 1 /* c;omment */; SELECT 2", ["SELECT 1 /* c;omment */", "SELECT 2"]),
            ("SELECT E'a\\';b'", ["SELECT E'a\\';b'"]),
            ("SELECT 1;", ["SELECT 1"]),
            ("  ;  ", []),
        ],
    )
    def test_split_statements(self, sql, expected):
        assert pgproxy.split_statements(sql) == expected

    @pytest.mark.parametrize(
        "sql,expected",
        [
            ("-- x\nSELECT 1", "SELECT 1"),
            ("/* x */SELECT 1", "SELECT 1"),
            ("/* a /* b */ c */ SELECT 1", "SELECT 1"),
            ("  \n\t-- a\n  /* b */\nSELECT 1", "SELECT 1"),
            ("SELECT 1 -- trailing", "SELECT 1 -- trailing"),
            ("'-- not a comment'", "'-- not a comment'"),
        ],
    )
    def test_strip_leading_comments(self, sql, expected):
        assert pgproxy.strip_leading_comments(sql) == expected

    def test_semicolon_in_literal_does_not_hide_a_bad_column(self):
        # The fragment left by a naive ';' split had unbalanced parens, so the
        # column-type rule silently passed and `serial` got through.
        err = pgproxy.validate(
            "CREATE TABLE t (a text DEFAULT 'x;y', b serial)"
        )
        assert isinstance(err, pgproxy.DsqlError)
        assert "serial" in err.message

    def test_paren_in_literal_does_not_hide_a_bad_column(self):
        err = pgproxy.validate("CREATE TABLE t (a text DEFAULT ')(', b serial)")
        assert isinstance(err, pgproxy.DsqlError)
        assert "serial" in err.message

    @pytest.mark.parametrize(
        "sql",
        [
            "-- migration\nCREATE EXTENSION pgcrypto",
            "/* tooling */ TRUNCATE t",
            "\n-- a\n/* b */ CREATE TEMP TABLE t (a int)",
        ],
    )
    def test_leading_comment_does_not_bypass_the_denylist(self, sql):
        assert isinstance(pgproxy.validate(sql), pgproxy.DsqlError)


class TestDenylist:
    @pytest.mark.parametrize(
        "sql",
        [
            "CREATE DATABASE foo",
            "CREATE TYPE mood AS ENUM ('sad')",
            "CREATE EXTENSION pgcrypto",
            "CREATE TRIGGER trg AFTER INSERT ON t EXECUTE FUNCTION f()",
            "CREATE PROCEDURE p() LANGUAGE sql AS $$ SELECT 1 $$",
            "CREATE TABLESPACE ts LOCATION '/tmp/x'",
            "CREATE TEMP TABLE t (a int)",
            "CREATE TEMPORARY TABLE t (a int)",
            "CREATE UNLOGGED TABLE t (a int)",
            "COPY t FROM STDIN",
            "LISTEN chan",
            "NOTIFY chan",
            "UNLISTEN chan",
            "TRUNCATE t",
            "DO $$ BEGIN NULL; END $$",
            "SAVEPOINT sp",
            "LOCK TABLE t IN ACCESS EXCLUSIVE MODE",
            "CREATE FUNCTION f() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 1; END $$",
            "CREATE FUNCTION f() RETURNS int LANGUAGE C AS 'x'",
            "CREATE MATERIALIZED VIEW mv AS SELECT 1",
            "CREATE TABLE t (a int) PARTITION BY RANGE (a)",
            "CREATE TABLE p (a int, EXCLUDE (a WITH =))",
            "CREATE RULE r AS ON DELETE TO t DO INSTEAD NOTHING",
            "CREATE AGGREGATE a (int) (sfunc = f, stype = int)",
            "CREATE DOMAIN d AS int",
            "CREATE CAST (int AS text) WITH FUNCTION f(int)",
            "CREATE COLLATION c (locale = 'en_US')",
            "CREATE OPERATOR + (leftarg = int, rightarg = int, function = f)",
            "CREATE PUBLICATION p FOR ALL TABLES",
            "CREATE SUBSCRIPTION s CONNECTION 'x' PUBLICATION p",
            "CREATE FOREIGN TABLE ft (a int) SERVER s",
            "CREATE SERVER s FOREIGN DATA WRAPPER w",
            "VACUUM t",
            "VACUUM FULL ANALYZE t",
            "CLUSTER t USING idx",
            "REINDEX TABLE t",
            "ALTER SYSTEM SET work_mem = '4MB'",
            "PREPARE TRANSACTION 'gid'",
            "COMMIT PREPARED 'gid'",
            "ROLLBACK PREPARED 'gid'",
        ],
    )
    def test_denied(self, sql):
        err = pgproxy.validate(sql, pgproxy.TxnState())
        assert isinstance(err, pgproxy.DsqlError), sql
        assert err.sqlstate == "0A000"
        assert "is not supported" in err.message

    @pytest.mark.parametrize(
        "sql",
        [
            "CREATE TABLE t (a int, b int REFERENCES other (id))",
            "CREATE TABLE t (a int, FOREIGN KEY (a) REFERENCES other (id))",
            "ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES o (id)",
        ],
    )
    def test_foreign_keys_denied(self, sql):
        err = pgproxy.validate(sql, pgproxy.TxnState())
        assert isinstance(err, pgproxy.DsqlError), sql
        assert err.sqlstate == "0A000"
        assert "foreign key" in err.message

    @pytest.mark.parametrize(
        "sql",
        [
            "ALTER TABLE t ADD COLUMN c int DEFAULT 0",
            "ALTER TABLE t ADD COLUMN c text NOT NULL",
        ],
    )
    def test_add_column_default_not_null_denied(self, sql):
        err = pgproxy.validate(sql, pgproxy.TxnState())
        assert isinstance(err, pgproxy.DsqlError), sql
        assert err.sqlstate == "0A000"

    def test_create_function_language_sql_allowed(self):
        sql = "CREATE FUNCTION f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$"
        assert pgproxy.validate(sql, pgproxy.TxnState()) is None


class TestColumnTypes:
    @pytest.mark.parametrize(
        "typ",
        [
            "serial", "bigserial", "smallserial",
            "money", "xml", "inet", "cidr", "macaddr", "macaddr8",
            "bit", "bit varying", "varbit",
            "point", "line", "lseg", "box", "path", "polygon", "circle",
            "hstore",
            "int[]", "text[]",
        ],
    )
    def test_unsupported_types_denied(self, typ):
        err = pgproxy.validate(
            f"CREATE TABLE t (id int, c {typ})", pgproxy.TxnState()
        )
        assert isinstance(err, pgproxy.DsqlError), typ
        assert err.sqlstate == "0A000"
        assert 'type "' in err.message and "is not supported" in err.message

    def test_unsupported_type_in_alter_add_column(self):
        err = pgproxy.validate(
            "ALTER TABLE t ADD COLUMN c serial", pgproxy.TxnState()
        )
        assert isinstance(err, pgproxy.DsqlError)
        assert err.sqlstate == "0A000"
        assert '"serial"' in err.message

    @pytest.mark.parametrize(
        "typ",
        [
            "smallint", "int2", "integer", "int", "int4", "bigint", "int8",
            "real", "float4", "double precision", "float8",
            "numeric", "decimal", "dec", "numeric(18,6)", "dec(10,2)",
            "char(1)", "character(1)", "bpchar", "varchar(255)",
            "character varying(10)", "text",
            "date", "time", "time with time zone", "timetz",
            "timestamp", "timestamp with time zone", "timestamptz",
            "interval", "interval year", "interval day to second",
            "interval year to month", "interval second", "boolean", "bool", "bytea", "uuid", "json", "jsonb",
        ],
    )
    def test_supported_types_allowed(self, typ):
        assert pgproxy.validate(
            f"CREATE TABLE t (id int, c {typ})", pgproxy.TxnState()
        ) is None

    def test_table_constraints_skipped(self):
        sql = "CREATE TABLE t (id int PRIMARY KEY, c text UNIQUE, CHECK (id > 0))"
        assert pgproxy.validate(sql, pgproxy.TxnState()) is None

    def test_storage_modifier_allowed(self):
        assert pgproxy.validate(
            "CREATE TABLE t (id int, j jsonb STORAGE PLAIN)", pgproxy.TxnState()
        ) is None
        assert pgproxy.validate(
            "ALTER TABLE t ADD COLUMN j jsonb STORAGE EXTERNAL", pgproxy.TxnState()
        ) is None
        assert pgproxy.validate(
            "ALTER TABLE t ADD COLUMN j jsonb STORAGE DEFAULT", pgproxy.TxnState()
        ) is None

    def test_bogus_interval_fields_rejected(self):
        err = pgproxy.validate(
            "CREATE TABLE t (id int, c interval foo)", pgproxy.TxnState()
        )
        assert isinstance(err, pgproxy.DsqlError)
        assert err.sqlstate == "0A000"


class TestAlterTableSubset:
    """ALTER TABLE action subset per the AWS alter-table-syntax-support doc."""

    @pytest.mark.parametrize(
        "sql",
        [
            "ALTER TABLE t ALTER COLUMN c TYPE text",
            "ALTER TABLE t ALTER COLUMN c SET DATA TYPE text",
            "ALTER TABLE t ALTER COLUMN c SET NOT NULL",
            "ALTER TABLE t ADD CONSTRAINT pk PRIMARY KEY (id)",
            "ALTER TABLE t ADD PRIMARY KEY (id)",
            "ALTER TABLE t ADD CONSTRAINT u UNIQUE (a)",
            "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0)",  # missing NOT VALID
            "ALTER TABLE t ALTER COLUMN c ADD GENERATED BY DEFAULT AS IDENTITY",  # no CACHE
        ],
    )
    def test_unsupported_actions_denied(self, sql):
        err = pgproxy.validate(sql, pgproxy.TxnState())
        assert isinstance(err, pgproxy.DsqlError), sql
        assert err.sqlstate == "0A000"

    @pytest.mark.parametrize(
        "sql",
        [
            "ALTER TABLE t DROP COLUMN c",
            "ALTER TABLE t DROP COLUMN IF EXISTS c CASCADE",
            "ALTER TABLE t ALTER COLUMN c SET DEFAULT 0",
            "ALTER TABLE t ALTER COLUMN c DROP DEFAULT",
            "ALTER TABLE t ALTER COLUMN c DROP NOT NULL",
            "ALTER TABLE t ALTER COLUMN c SET STORAGE PLAIN",
            "ALTER TABLE t RENAME COLUMN a TO b",
            "ALTER TABLE t RENAME TO t2",
            "ALTER TABLE t SET SCHEMA app",
            "ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0) NOT VALID",
            "ALTER TABLE t ADD CONSTRAINT u UNIQUE USING INDEX idx",
            "ALTER TABLE t DROP CONSTRAINT c",
            "ALTER TABLE t ALTER COLUMN c ADD GENERATED BY DEFAULT AS IDENTITY (CACHE 1)",
            "ALTER TABLE t ADD COLUMN c integer",
            "ALTER TABLE t ADD COLUMN j jsonb STORAGE PLAIN",
            "ALTER TABLE t ADD COLUMN c bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 1)",
            "ALTER TABLE t ADD COLUMN cl text GENERATED ALWAYS AS (lower(email)) STORED",
        ],
    )
    def test_supported_actions_allowed(self, sql):
        assert pgproxy.validate(sql, pgproxy.TxnState()) is None, sql

    @pytest.mark.parametrize(
        "sql,expected",
        [
            ("ALTER TABLE t DROP COLUMN c", ["c"]),
            ("ALTER TABLE t DROP c", ["c"]),
            ("ALTER TABLE t DROP COLUMN IF EXISTS c CASCADE", ["c"]),
            ("ALTER TABLE t DROP COLUMN a, DROP COLUMN b", ["a", "b"]),
            ("ALTER TABLE t DROP a, DROP COLUMN b, DROP IF EXISTS c", ["a", "b", "c"]),
            ('ALTER TABLE t DROP COLUMN "MixedCase"', ["MixedCase"]),
            # Actions that merely start with DROP are not column drops.
            ("ALTER TABLE t DROP CONSTRAINT c", []),
            ("ALTER TABLE t ALTER COLUMN c DROP DEFAULT", []),
            ("ALTER TABLE t ALTER COLUMN c DROP NOT NULL", []),
            ("ALTER TABLE t ALTER COLUMN c DROP EXPRESSION", []),
            ("ALTER TABLE t ALTER COLUMN c DROP IDENTITY", []),
            ("ALTER TABLE t ADD COLUMN c int", []),
            ("DROP TABLE t", []),
        ],
    )
    def test_dropped_columns(self, sql, expected):
        assert pgproxy.dropped_columns(sql) == expected

    def test_drop_column_still_passes_text_only_validation(self):
        # The primary-key restriction needs a catalog probe, so validate()
        # stays clean and the rule lives in the proxy's statement planner.
        assert pgproxy.validate("ALTER TABLE t DROP COLUMN c") is None

    def test_validate_constraint_async_rewrite(self):
        result = pgproxy.validate(
            "ALTER TABLE ASYNC t VALIDATE CONSTRAINT c", pgproxy.TxnState()
        )
        assert isinstance(result, pgproxy.Rewrite)
        assert result.sql == "ALTER TABLE t VALIDATE CONSTRAINT c"
        assert result.job_type == "VALIDATE_CONSTRAINT"
        assert result.object_name == "public.t"


class TestIdentityAndSequences:
    @pytest.mark.parametrize(
        "sql",
        [
            "CREATE TABLE t (id bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 1) PRIMARY KEY)",
            "CREATE TABLE t (id bigint GENERATED ALWAYS AS IDENTITY (CACHE 65536))",
            "CREATE TABLE t (id bigint GENERATED BY DEFAULT AS IDENTITY)",
            "CREATE SEQUENCE s CACHE 1",
            "CREATE SEQUENCE s CACHE 65536",
            "CREATE SEQUENCE s CACHE 1000000",
            "CREATE SEQUENCE s",
        ],
    )
    def test_allowed(self, sql):
        assert pgproxy.validate(sql, pgproxy.TxnState()) is None, sql

    @pytest.mark.parametrize(
        "sql",
        [
            # identity is bigint-only
            "CREATE TABLE t (id int GENERATED BY DEFAULT AS IDENTITY (CACHE 1))",
            "CREATE TABLE t (id integer GENERATED ALWAYS AS IDENTITY (CACHE 65536))",
            # CACHE must be 1 or >= 65536
            "CREATE TABLE t (id bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 100))",
            "CREATE SEQUENCE s CACHE 100",
            "CREATE SEQUENCE s CACHE 65535",
        ],
    )
    def test_denied(self, sql):
        err = pgproxy.validate(sql, pgproxy.TxnState())
        assert isinstance(err, pgproxy.DsqlError), sql
        assert err.sqlstate == "0A000"


class TestIndexLimits:
    @pytest.mark.parametrize(
        "sql",
        [
            "CREATE INDEX i ON t USING gin (data)",
            "CREATE INDEX i ON t USING gist (coords)",
            "CREATE INDEX i ON t USING brin (created_at)",
            "CREATE INDEX i ON t USING hash (a)",
            "CREATE INDEX ASYNC i ON t USING gin (data)",
            "CREATE INDEX CONCURRENTLY i ON t (a)",
            "CREATE INDEX i ON t (a) WHERE a > 0",  # partial index
            "CREATE INDEX ASYNC i ON t (a) WHERE a > 0",
            "CREATE INDEX i ON t (lower(email))",  # expression index
            "CREATE INDEX ASYNC i ON t ((data->>'city'))",
        ],
    )
    def test_unsupported_index_forms_denied(self, sql):
        err = pgproxy.validate(sql, pgproxy.TxnState())
        assert isinstance(err, pgproxy.DsqlError), sql
        assert err.sqlstate == "0A000"

    @pytest.mark.parametrize(
        "sql",
        [
            "CREATE INDEX i ON t (a)",
            "CREATE INDEX i ON t USING btree (a)",
            "CREATE UNIQUE INDEX i ON t (a NULLS LAST) INCLUDE (b)",
            "CREATE INDEX ASYNC i ON t (a, b DESC) INCLUDE (c)",
        ],
    )
    def test_supported_index_forms_allowed(self, sql):
        result = pgproxy.validate(sql, pgproxy.TxnState())
        assert not isinstance(result, pgproxy.DsqlError), sql

    def test_more_than_8_columns_denied(self):
        for kw in ("", "ASYNC "):
            err = pgproxy.validate(
                f"CREATE INDEX {kw}i ON t (a,b,c,d,e,f,g,h,i)", pgproxy.TxnState()
            )
            assert isinstance(err, pgproxy.DsqlError), kw
            assert err.sqlstate == "0A000"
            assert "8 columns" in err.message

    def test_exactly_8_columns_allowed(self):
        assert pgproxy.validate(
            "CREATE INDEX i ON t (a,b,c,d,e,f,g,h)", pgproxy.TxnState()
        ) is None
        result = pgproxy.validate(
            "CREATE INDEX ASYNC i ON t (a,b,c,d,e,f,g,h)", pgproxy.TxnState()
        )
        assert isinstance(result, pgproxy.Rewrite)


class TestWaitForJob:
    def test_documented_form(self):
        sql = "SELECT sys.wait_for_job(job_id) 'abc123'"
        assert pgproxy.match_wait_for_job(sql) == "abc123"

    def test_conventional_form(self):
        sql = "SELECT sys.wait_for_job('abc123')"
        assert pgproxy.match_wait_for_job(sql) == "abc123"

    def test_call_form(self):
        assert pgproxy.match_wait_for_job("CALL sys.wait_for_job('abc123')") == "abc123"

    def test_no_match(self):
        assert pgproxy.match_wait_for_job("SELECT 1") is None
        assert pgproxy.match_wait_for_job("SELECT sys.wait_for_job(job_id)") is None


class TestReset:
    def test_reset_stops_proxies_and_clears_jobs(self):
        """reset() runs in a worker thread (no running loop) — it must still
        schedule proxy shutdown on the captured app loop and drop job state."""
        from ministack.services import dsql as dsql_mod

        loop = asyncio.new_event_loop()
        thread = threading.Thread(target=loop.run_forever, daemon=True)
        thread.start()
        old_loop = dsql_mod._main_loop
        dsql_mod._main_loop = loop
        try:
            port = _free_port()
            asyncio.run_coroutine_threadsafe(
                pgproxy.start_proxy("resettest", port, "127.0.0.1", 1), loop
            ).result(timeout=5)
            pgproxy._jobs["resettest"] = [{"job_id": "x"}]
            assert "resettest" in pgproxy._proxies

            dsql_mod.reset()  # called from this thread: no running loop here

            deadline = time.time() + 5
            while time.time() < deadline and "resettest" in pgproxy._proxies:
                time.sleep(0.05)
            assert "resettest" not in pgproxy._proxies
            assert pgproxy.get_jobs("resettest") == []
        finally:
            dsql_mod._main_loop = old_loop
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)


class TestContainerCap:
    def test_cap_exceeded_logs_and_degrades_to_stub(self, monkeypatch, caplog):
        """When the backend port window is exhausted, creating another cluster
        logs a warning naming DSQL_BASE_PORT and goes metadata-only."""
        import json
        import logging

        from ministack.services import dsql as dsql_mod

        monkeypatch.setattr(dsql_mod, "_backends_enabled", lambda: True)
        monkeypatch.setattr(dsql_mod, "_BACKEND_PORT_WINDOW", 0)

        with caplog.at_level(logging.WARNING, logger="dsql"):
            status, _, body = dsql_mod._create_cluster({})

        assert status == 200
        cluster = json.loads(body)
        identifier = cluster["identifier"]
        try:
            assert cluster["status"] == "ACTIVE"  # stub, not CREATING
            assert dsql_mod._clusters[identifier]["_has_backend"] is False
            assert any(
                "DSQL_BASE_PORT" in r.message and "metadata-only" in r.message
                for r in caplog.records
                if r.name == "dsql" and r.levelno == logging.WARNING
            )
        finally:
            dsql_mod._clusters.clear()

    def test_containers_disabled_means_stub_even_with_docker(self, monkeypatch):
        """Default posture: Docker available but DSQL_STRICT unset/off —
        clusters are metadata-only stubs and no backend is started."""
        import json

        from ministack.services import dsql as dsql_mod

        monkeypatch.setattr(dsql_mod, "DSQL_STRICT", False)
        monkeypatch.setattr(dsql_mod, "_docker_available", lambda: True)
        assert dsql_mod._backends_enabled() is False

        status, _, body = dsql_mod._create_cluster({})
        assert status == 200
        cluster = json.loads(body)
        identifier = cluster["identifier"]
        try:
            assert cluster["status"] == "ACTIVE"  # stub, not CREATING
            assert dsql_mod._clusters[identifier]["_has_backend"] is False
        finally:
            dsql_mod._clusters.clear()

    def test_backends_enabled_requires_flag_and_docker(self, monkeypatch):
        from ministack.services import dsql as dsql_mod

        monkeypatch.setattr(dsql_mod, "_docker_available", lambda: True)
        monkeypatch.setattr(dsql_mod, "DSQL_STRICT", False)
        assert dsql_mod._backends_enabled() is False
        monkeypatch.setattr(dsql_mod, "DSQL_STRICT", True)
        assert dsql_mod._backends_enabled() is True
        monkeypatch.setattr(dsql_mod, "_docker_available", lambda: False)
        assert dsql_mod._backends_enabled() is False



class TestIndexAsync:
    def test_rewrite_strips_async(self):
        assert pgproxy.rewrite_index_async(
            "CREATE UNIQUE INDEX ASYNC IF NOT EXISTS idx ON t (a)"
        ) == "CREATE UNIQUE INDEX IF NOT EXISTS idx ON t (a)"

    def test_validate_returns_rewrite(self):
        result = pgproxy.validate(
            "CREATE INDEX ASYNC idx ON t (a)", pgproxy.TxnState()
        )
        assert isinstance(result, pgproxy.Rewrite)
        assert result.kind == "index_async"
        assert result.sql == "CREATE INDEX idx ON t (a)"
        assert result.object_name == "public.idx"
        assert result.table == "t"

    def test_named_index_takes_table_schema(self):
        result = pgproxy.validate(
            "CREATE INDEX ASYNC idx ON app.t (a)", pgproxy.TxnState()
        )
        assert isinstance(result, pgproxy.Rewrite)
        assert result.object_name == "app.idx"

    def test_schema_qualified_index_name_kept(self):
        result = pgproxy.validate(
            "CREATE INDEX ASYNC app.idx ON app.t (a)", pgproxy.TxnState()
        )
        assert isinstance(result, pgproxy.Rewrite)
        assert result.object_name == "app.idx"

    def test_unnamed_index_auto_named(self):
        result = pgproxy.validate(
            "CREATE INDEX ASYNC ON public.t (a, b)", pgproxy.TxnState()
        )
        assert isinstance(result, pgproxy.Rewrite)
        assert result.object_name == "public.t_a_b_idx"

    def test_unnamed_index_default_schema(self):
        result = pgproxy.validate(
            "CREATE INDEX ASYNC ON t (email)", pgproxy.TxnState()
        )
        assert isinstance(result, pgproxy.Rewrite)
        assert result.object_name == "public.t_email_idx"

    def test_plain_create_index_not_rewritten(self):
        result = pgproxy.validate(
            "CREATE INDEX idx ON t (a)", pgproxy.TxnState()
        )
        assert result is None
        assert pgproxy.is_plain_create_index("CREATE INDEX idx ON t (a)")
        assert not pgproxy.is_plain_create_index("CREATE INDEX ASYNC idx ON t (a)")


class TestTxnDiscipline:
    def test_second_ddl_in_txn_rejected(self):
        txn = pgproxy.TxnState()
        txn.apply("BEGIN")
        txn.apply("CREATE TABLE a (x int)")
        err = pgproxy.validate("CREATE TABLE b (x int)", txn)
        assert isinstance(err, pgproxy.DsqlError)
        assert err.sqlstate == "25006"
        assert "only one DDL" in err.message

    def test_ddl_after_dml_rejected(self):
        txn = pgproxy.TxnState()
        txn.apply("BEGIN")
        txn.apply("INSERT INTO t VALUES (1)")
        err = pgproxy.validate("CREATE TABLE b (x int)", txn)
        assert isinstance(err, pgproxy.DsqlError)
        assert err.sqlstate == "25006"
        assert "cannot be mixed" in err.message

    def test_dml_after_ddl_rejected(self):
        txn = pgproxy.TxnState()
        txn.apply("BEGIN")
        txn.apply("CREATE TABLE a (x int)")
        err = pgproxy.validate("INSERT INTO a VALUES (1)", txn)
        assert isinstance(err, pgproxy.DsqlError)
        assert err.sqlstate == "25006"
        assert "cannot be mixed" in err.message

    def test_commit_resets_state(self):
        txn = pgproxy.TxnState()
        txn.apply("BEGIN")
        txn.apply("CREATE TABLE a (x int)")
        txn.apply("COMMIT")
        assert pgproxy.validate("CREATE TABLE b (x int)", txn) is None

    def test_autocommit_sequential_ddl_fine(self):
        txn = pgproxy.TxnState()
        assert pgproxy.validate("CREATE TABLE a (x int)", txn) is None
        txn.apply("CREATE TABLE a (x int)")
        assert pgproxy.validate("CREATE TABLE b (x int)", txn) is None

    def test_select_insert_outside_txn_fine(self):
        txn = pgproxy.TxnState()
        assert pgproxy.validate("SELECT 1", txn) is None
        assert pgproxy.validate("INSERT INTO t VALUES (1)", txn) is None


class TestTxnLimits:
    def test_single_insert_over_3000_rows_rejected(self):
        sql = "INSERT INTO t VALUES " + ",".join(f"({i})" for i in range(3001))
        err = pgproxy.validate(sql, pgproxy.TxnState())
        assert isinstance(err, pgproxy.DsqlError)
        assert err.sqlstate == "25006"
        assert "3,000" in err.message

    def test_exactly_3000_rows_allowed(self):
        sql = "INSERT INTO t VALUES " + ",".join(f"({i})" for i in range(3000))
        assert pgproxy.validate(sql, pgproxy.TxnState()) is None

    def test_cumulative_rows_across_txn(self):
        txn = pgproxy.TxnState()
        txn.apply("BEGIN")
        batch = "INSERT INTO t VALUES " + ",".join(f"({i})" for i in range(2000))
        assert pgproxy.validate(batch, txn) is None
        txn.apply(batch)
        err = pgproxy.validate(batch, txn)  # 2000 + 2000 > 3000
        assert isinstance(err, pgproxy.DsqlError)
        assert "3,000" in err.message

    def test_oversized_statement_rejected(self):
        sql = "INSERT INTO t VALUES ('" + "x" * (10 * 1024 * 1024 + 1) + "')"
        err = pgproxy.validate(sql, pgproxy.TxnState())
        assert isinstance(err, pgproxy.DsqlError)
        assert "10 MiB" in err.message

    def test_duration_limit(self):
        txn = pgproxy.TxnState()
        txn.apply("BEGIN")
        txn.started_at -= 301  # simulate a 5+ minute old transaction
        err = pgproxy.validate("INSERT INTO t VALUES (1)", txn)
        assert isinstance(err, pgproxy.DsqlError)
        assert "5 minute" in err.message

    def test_values_tuple_count(self):
        assert pgproxy._values_tuple_count("INSERT INTO t VALUES (1, '(a'), (2)") == 2
        assert pgproxy._values_tuple_count("INSERT INTO t SELECT * FROM s") == 0
        assert pgproxy._values_tuple_count("UPDATE t SET a = 1") == 0


# ---------------------------------------------------------------------------
# Live proxy tests (need Docker — the backend is a real postgres container)
# ---------------------------------------------------------------------------


def _docker_daemon_available():
    try:
        import docker

        docker.from_env().ping()
        return True
    except Exception:
        return False


requires_docker = pytest.mark.skipif(
    not _docker_daemon_available(), reason="Docker daemon not available"
)


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def pg_backend():
    """Run a trust-auth postgres container on an ephemeral published port."""
    docker = pytest.importorskip("docker")
    psycopg2 = pytest.importorskip("psycopg2")
    client = docker.from_env()
    container = client.containers.run(
        image="postgres:16-alpine",
        detach=True,
        environment={
            "POSTGRES_USER": "postgres",
            "POSTGRES_DB": "postgres",
            "POSTGRES_HOST_AUTH_METHOD": "trust",
        },
        ports={"5432/tcp": None},  # docker-assigned host port
    )
    try:
        container.reload()
        port = int(
            container.attrs["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"]
        )
        deadline = time.time() + 60
        while time.time() < deadline:
            try:
                conn = psycopg2.connect(
                    host="127.0.0.1",
                    port=port,
                    user="postgres",
                    dbname="postgres",
                    connect_timeout=2,
                )
                conn.close()
                break
            except psycopg2.OperationalError:
                time.sleep(0.5)
        else:
            pytest.fail(f"postgres container not ready on port {port}")
        yield "127.0.0.1", port
    finally:
        container.remove(force=True, v=True)


@pytest.fixture(scope="module")
def dsql_proxy(pg_backend):
    """Run a pgproxy in a background event loop in front of pg_backend."""
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, daemon=True)
    thread.start()
    proxy_port = _free_port()
    asyncio.run_coroutine_threadsafe(
        pgproxy.start_proxy("livetest", proxy_port, pg_backend[0], pg_backend[1]),
        loop,
    ).result(timeout=10)
    yield proxy_port
    asyncio.run_coroutine_threadsafe(pgproxy.stop_all_proxies(), loop).result(timeout=10)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)


def _pg_connect(port, autocommit=True):
    psycopg2 = pytest.importorskip("psycopg2")
    # Any user/password must be accepted (IAM-token posture).
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=port,
        user="admin",
        password="anything",
        dbname="postgres",
        connect_timeout=10,
    )
    conn.autocommit = autocommit
    return conn


@requires_docker
class TestContainersE2E:
    """End-to-end for DSQL_STRICT=1: _create_cluster spins up a real
    Postgres container behind the wire proxy, reachable over SQL. Runs
    in-process (flag monkeypatched on) wherever a Docker daemon exists."""

    def test_env_flag_spins_up_real_backend(self, monkeypatch):
        import json

        psycopg2 = pytest.importorskip("psycopg2")
        pytest.importorskip("docker")

        from ministack.services import dsql as dsql_mod

        monkeypatch.setattr(dsql_mod, "DSQL_STRICT", True)
        # _start_backend stashes the test's ephemeral loop in this global;
        # put the original back afterwards (all consumers guard is_running(),
        # but don't leak a closed loop into other tests).
        old_main_loop = dsql_mod._main_loop

        async def _run():
            status, _, body = dsql_mod._create_cluster({})
            assert status == 200
            cluster = json.loads(body)
            identifier = cluster["identifier"]
            try:
                # Backend container + proxy spin up in the background.
                assert cluster["status"] == "CREATING"
                deadline = time.time() + 120
                while dsql_mod._clusters[identifier]["status"] != "ACTIVE":
                    assert time.time() < deadline, "backend did not start in time"
                    await asyncio.sleep(0.5)
                assert dsql_mod._clusters[identifier]["_has_backend"] is True

                def _select_one():
                    conn = psycopg2.connect(
                        host="127.0.0.1",
                        port=int(cluster["endpoint"].split(":")[1]),
                        user="admin",
                        password="anything",
                        dbname="postgres",
                        connect_timeout=10,
                    )
                    try:
                        conn.autocommit = True
                        with conn.cursor() as cur:
                            cur.execute("SELECT 1")
                            return cur.fetchone()
                    finally:
                        conn.close()

                assert await asyncio.to_thread(_select_one) == (1,)
            finally:
                await dsql_mod._teardown_backend(identifier)
                dsql_mod._clusters.pop(identifier, None)

        try:
            asyncio.run(_run())
        finally:
            dsql_mod._main_loop = old_main_loop


class _WireClient:
    """Raw Postgres wire-protocol client.

    psycopg2 interpolates parameters client-side and always sends the simple
    'Q' protocol, so it cannot reach the extended (Parse/Bind/Execute) path
    that pgjdbc, pgx, asyncpg and psycopg3 use. These tests speak the protocol
    directly so both paths are covered.
    """

    def __init__(self, port, user="admin", database="postgres"):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=15)
        params = f"user\0{user}\0database\0{database}\0\0".encode()
        payload = struct.pack("!I", 196608) + params
        self.sock.sendall(struct.pack("!I", len(payload) + 4) + payload)
        self._until_ready()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _recv(self, n):
        buf = b""
        while len(buf) < n:
            chunk = self.sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError("proxy closed the connection")
            buf += chunk
        return buf

    def _until_ready(self):
        frames = []
        while True:
            type_byte = self._recv(1)
            (length,) = struct.unpack("!I", self._recv(4))
            payload = self._recv(length - 4)
            frames.append((type_byte, payload))
            if type_byte == b"Z":
                return frames

    @staticmethod
    def _frame(type_byte, payload):
        return type_byte + struct.pack("!I", len(payload) + 4) + payload

    def simple(self, sql):
        """Simple query protocol — what psql and psycopg2 send."""
        self.sock.sendall(self._frame(b"Q", sql.encode() + b"\0"))
        return _WireResult(self._until_ready())

    def extended(self, sql):
        """Parse/Bind/Describe/Execute/Sync — what prepared statements send."""
        msg = self._frame(b"P", b"\0" + sql.encode() + b"\0" + struct.pack("!H", 0))
        msg += self._frame(b"B", b"\0\0" + struct.pack("!HHH", 0, 0, 0))
        msg += self._frame(b"D", b"P\0")
        msg += self._frame(b"E", b"\0" + struct.pack("!I", 0))
        msg += self._frame(b"S", b"")
        self.sock.sendall(msg)
        return _WireResult(self._until_ready())

    def close(self):
        try:
            self.sock.sendall(self._frame(b"X", b""))
        except OSError:
            pass
        self.sock.close()


class _WireResult:
    """Decoded response: error state, rows, command tag, txn status byte."""

    def __init__(self, frames):
        self.sqlstate = None
        self.message = None
        self.rows = []
        self.tag = None
        self.txn_status = None
        for type_byte, payload in frames:
            if type_byte == b"E":
                for part in payload.split(b"\0"):
                    if part[:1] == b"C":
                        self.sqlstate = part[1:].decode()
                    elif part[:1] == b"M":
                        self.message = part[1:].decode("utf-8", "replace")
            elif type_byte == b"C":
                self.tag = payload[:-1].decode("utf-8", "replace")
            elif type_byte == b"D":
                (n,) = struct.unpack("!H", payload[:2])
                values, off = [], 2
                for _ in range(n):
                    (vlen,) = struct.unpack("!i", payload[off:off + 4])
                    off += 4
                    if vlen < 0:
                        values.append(None)
                    else:
                        values.append(payload[off:off + vlen].decode("utf-8", "replace"))
                        off += vlen
                self.rows.append(tuple(values))
            elif type_byte == b"Z":
                self.txn_status = payload.decode()

    @property
    def ok(self):
        return self.sqlstate is None


@requires_docker
class TestExtendedProtocol:
    """The extended protocol must enforce the same DSQL subset as 'Q'.

    Relaying Parse unvalidated let every prepared-statement client — pgjdbc,
    pgx, asyncpg, psycopg3, and the ORMs on top of them — past the validator.
    """

    @pytest.mark.parametrize(
        "sql,expected",
        [
            ("CREATE TABLE xp_serial (id serial)", "serial"),
            ("CREATE TABLE xp_fk (id int, p int REFERENCES xp_serial (id))",
             "foreign key"),
            ("CREATE EXTENSION pgcrypto", "CREATE EXTENSION"),
            ("CREATE TEMP TABLE xp_tmp (id int)", "temporary"),
            ("TRUNCATE xp_base", "TRUNCATE"),
            ("CREATE MATERIALIZED VIEW xp_mv AS SELECT 1", "materialized view"),
            ("CREATE TABLE xp_part (id int) PARTITION BY RANGE (id)",
             "partitioning"),
            ("CREATE TABLE xp_bad (id int, ts money)", "money"),
        ],
    )
    def test_unsupported_sql_is_rejected_over_parse(self, dsql_proxy, sql, expected):
        with _WireClient(dsql_proxy) as c:
            result = c.extended(sql)
            assert not result.ok, f"{sql} was accepted over the extended protocol"
            assert result.sqlstate == "0A000"
            assert expected in result.message

    def test_rejected_parse_leaves_the_connection_usable(self, dsql_proxy):
        """After an error the client must be able to keep working (the proxy
        has to honour skip-until-Sync, then answer the Sync)."""
        with _WireClient(dsql_proxy) as c:
            assert not c.extended("CREATE TABLE xp_reuse (id serial)").ok
            result = c.extended("SELECT 1")
            assert result.ok and result.rows == [("1",)]

    def test_valid_ddl_still_passes_over_parse(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            assert c.extended("CREATE TABLE xp_good (id bigint, name text)").ok
            assert c.extended("INSERT INTO xp_good VALUES (1, 'a')").ok
            result = c.extended("SELECT name FROM xp_good WHERE id = 1")
            assert result.rows == [("a",)]

    def test_transaction_limits_apply_over_parse(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE xp_rows (id int)")
            big = "INSERT INTO xp_rows VALUES " + ", ".join(
                f"({i})" for i in range(3100)
            )
            result = c.extended(big)
            assert result.sqlstate == "25006"
            assert "3,000 row" in result.message

    def test_for_update_rules_apply_over_parse(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE xp_lock (id int PRIMARY KEY, v int)")
            result = c.extended("SELECT * FROM xp_lock WHERE id > 1 FOR UPDATE")
            assert result.sqlstate == "0A000"
            assert "equality predicates" in result.message

    def test_create_index_async_returns_a_job_over_parse(self, dsql_proxy):
        """DSQL-only syntax has to be rewritten on this path too, or the
        backend answers with a bare syntax error."""
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE xp_idx (id int, name text)")
            result = c.extended("CREATE INDEX ASYNC xp_i ON xp_idx (name)")
            assert result.ok, result.message
            assert len(result.rows) == 1
            job_id = result.rows[0][0]
            assert ID_RE.match(job_id)
            jobs = c.extended(
                f"SELECT job_id, status FROM sys.jobs WHERE job_id = '{job_id}'"
            )
            assert jobs.rows == [(job_id, "completed")]

    def test_sys_jobs_is_emulated_over_parse(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            result = c.extended("SELECT job_id FROM sys.jobs")
            assert result.ok, result.message

    def test_txn_state_survives_begin_and_commit_over_parse(self, dsql_proxy):
        """Transaction state comes from the backend's ReadyForQuery byte, so a
        BEGIN or COMMIT sent over either protocol keeps the validator honest."""
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE xp_txn (id int)")
            assert c.extended("BEGIN").ok
            assert c.simple("CREATE TABLE xp_txn_a (id int)").ok
            # Second DDL in the same transaction: DSQL allows only one.
            result = c.simple("CREATE TABLE xp_txn_b (id int)")
            assert result.sqlstate == "25006"
            c.simple("ROLLBACK")

    def test_commit_over_parse_does_not_strand_txn_state(self, dsql_proxy):
        """A stranded in_txn made the validator reject valid autocommit DDL —
        a false rejection, worse than a missed one."""
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE xp_strand (id int)")
            assert c.simple("BEGIN").ok
            assert c.extended("INSERT INTO xp_strand VALUES (1)").ok
            assert c.extended("COMMIT").ok
            assert c.simple("CREATE TABLE xp_strand_a (id int)").ok
            assert c.simple("CREATE TABLE xp_strand_b (id int)").ok
            assert c.simple("INSERT INTO xp_strand VALUES (2)").ok

    def test_ready_for_query_status_tracks_the_backend(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            assert c.extended("BEGIN").txn_status == "T"
            # Rejected by the proxy, but the block really is in a transaction.
            assert c.simple("TRUNCATE xp_base").txn_status == "E"
            assert c.simple("ROLLBACK").txn_status == "I"


@requires_docker
class TestDropColumn:
    """Aurora DSQL gained ALTER TABLE ... DROP COLUMN on 2026-08-03, including
    several columns in one statement, but dropping a primary key column is not
    supported. The rule needs a catalog probe, so it runs in the proxy."""

    def test_drop_non_pk_column(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE dc_a (id int PRIMARY KEY, x int, y int)")
            assert c.simple("ALTER TABLE dc_a DROP COLUMN x").ok
            cols = c.simple(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'dc_a' ORDER BY column_name"
            )
            assert cols.rows == [("id",), ("y",)]

    def test_drop_several_non_pk_columns_in_one_statement(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE dc_b (id int PRIMARY KEY, x int, y int, z int)")
            assert c.simple("ALTER TABLE dc_b DROP COLUMN x, DROP COLUMN y").ok
            cols = c.simple(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'dc_b' ORDER BY column_name"
            )
            assert cols.rows == [("id",), ("z",)]

    def test_drop_pk_column_rejected(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE dc_c (id int PRIMARY KEY, x int)")
            result = c.simple("ALTER TABLE dc_c DROP COLUMN id")
            assert result.sqlstate == "0A000"
            assert "primary key" in result.message
            # The column must still be there.
            assert c.simple("SELECT id FROM dc_c").ok

    def test_drop_pk_column_rejected_over_extended_protocol(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE dc_d (id int PRIMARY KEY, x int)")
            result = c.extended("ALTER TABLE dc_d DROP COLUMN id")
            assert result.sqlstate == "0A000"
            assert "primary key" in result.message

    def test_pk_column_in_a_multi_drop_rejects_the_whole_statement(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE dc_e (id int PRIMARY KEY, x int)")
            result = c.simple("ALTER TABLE dc_e DROP COLUMN x, DROP COLUMN id")
            assert result.sqlstate == "0A000"
            assert "primary key" in result.message
            # Nothing was dropped — the statement never reached the backend.
            assert c.simple("SELECT id, x FROM dc_e").ok

    def test_composite_pk_columns_are_all_protected(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE dc_f (a int, b int, x int, PRIMARY KEY (a, b))")
            assert c.simple("ALTER TABLE dc_f DROP COLUMN b").sqlstate == "0A000"
            assert c.simple("ALTER TABLE dc_f DROP COLUMN x").ok

    def test_other_drop_actions_are_unaffected(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple(
                "CREATE TABLE dc_g (id int PRIMARY KEY, x int DEFAULT 1 NOT NULL)"
            )
            assert c.simple("ALTER TABLE dc_g ALTER COLUMN x DROP DEFAULT").ok
            assert c.simple("ALTER TABLE dc_g ALTER COLUMN x DROP NOT NULL").ok

    def test_table_without_a_primary_key(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE dc_h (x int, y int)")
            assert c.simple("ALTER TABLE dc_h DROP COLUMN x").ok


@requires_docker
class TestTransactionAbortSemantics:
    """A statement the proxy rejects must poison the transaction block the way
    a real error does — otherwise the following statements still commit."""

    def test_rejection_aborts_the_block(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE ab_t (id int)")
            assert c.simple("BEGIN").ok
            assert c.simple("INSERT INTO ab_t VALUES (1)").ok
            assert c.simple("TRUNCATE ab_t").sqlstate == "0A000"

            blocked = c.simple("INSERT INTO ab_t VALUES (2)")
            assert blocked.sqlstate == "25P02"
            assert blocked.txn_status == "E"

            # Postgres reports ROLLBACK when a failed block is committed.
            assert c.simple("COMMIT").tag == "ROLLBACK"
            assert c.simple("SELECT count(*) FROM ab_t").rows == [("0",)]

    def test_rollback_clears_the_abort(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE ab_r (id int)")
            c.simple("BEGIN")
            assert c.simple("CREATE TEMP TABLE nope (id int)").sqlstate == "0A000"
            assert c.simple("ROLLBACK").ok
            assert c.simple("INSERT INTO ab_r VALUES (1)").ok
            assert c.simple("SELECT count(*) FROM ab_r").rows == [("1",)]

    def test_abort_applies_to_the_extended_protocol_too(self, dsql_proxy):
        with _WireClient(dsql_proxy) as c:
            c.simple("CREATE TABLE ab_x (id int)")
            c.simple("BEGIN")
            assert c.extended("TRUNCATE ab_x").sqlstate == "0A000"
            assert c.extended("INSERT INTO ab_x VALUES (1)").sqlstate == "25P02"
            assert c.simple("COMMIT").tag == "ROLLBACK"
            assert c.simple("SELECT count(*) FROM ab_x").rows == [("0",)]


@requires_docker
class TestLiveProxy:
    def test_create_insert_select_round_trip(self, dsql_proxy):
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute(
                "CREATE TABLE rt_live (id int, name text, amt numeric(18,6), "
                "flag boolean, ts timestamp with time zone, u uuid, j jsonb)"
            )
            cur.execute(
                "INSERT INTO rt_live VALUES (1, 'alice', 12.5, true, now(), "
                "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{\"k\": 1}')"
            )
            cur.execute("SELECT id, name, flag FROM rt_live")
            rows = cur.fetchall()
            assert rows == [(1, "alice", True)]
            cur.execute("DROP TABLE rt_live")
        finally:
            conn.close()

    def test_create_index_async_returns_job(self, dsql_proxy):
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE idx_live (a int)")
            cur.execute("INSERT INTO idx_live VALUES (1)")
            cur.execute("CREATE INDEX ASYNC idx_live_a ON idx_live (a)")
            job_id = cur.fetchone()[0]
            assert re.match(r"^[a-z0-9]{26}$", job_id)

            cur.execute("SELECT job_id, status, job_type FROM sys.jobs")
            jobs = cur.fetchall()
            assert (job_id, "completed", "INDEX_BUILD") in jobs
        finally:
            conn.close()

        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT job_id, status FROM sys.jobs WHERE job_id = '{job_id}'"
            )
            assert cur.fetchall() == [(job_id, "completed")]

            cur.execute(f"SELECT sys.wait_for_job(job_id) '{job_id}'")
            assert cur.fetchone()[0] is True
            cur.execute("SELECT sys.wait_for_job(job_id) 'nonexistent'")
            assert cur.fetchone()[0] is False

            # The rewritten (sync) index really exists on the backend.
            cur.execute(
                "SELECT indexname FROM pg_indexes WHERE tablename = 'idx_live'"
            )
            assert "idx_live_a" in {r[0] for r in cur.fetchall()}
            cur.execute("DROP TABLE idx_live")
        finally:
            conn.close()

    def test_plain_create_index_empty_table_ok(self, dsql_proxy):
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE ci_live (a int)")
            cur.execute("CREATE INDEX ci_live_a ON ci_live (a)")
            cur.execute("DROP TABLE ci_live")
        finally:
            conn.close()

    def test_plain_create_index_nonempty_table_rejected(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE cn_live (a int)")
            cur.execute("INSERT INTO cn_live VALUES (1)")
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute("CREATE INDEX cn_live_a ON cn_live (a)")
            assert exc.value.pgcode == "0A000"
            # exact message published in the AWS DSQL troubleshooting docs
            assert "use CREATE INDEX ASYNC instead" in str(exc.value)
            cur.execute("DROP TABLE cn_live")
        finally:
            conn.close()

    def test_serial_column_rejected(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute("CREATE TABLE ser_live (id serial)")
            assert exc.value.pgcode == "0A000"
            assert '"serial"' in str(exc.value)
        finally:
            conn.close()

    def test_create_extension_rejected(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute("CREATE EXTENSION pgcrypto")
            assert exc.value.pgcode == "0A000"
            assert "is not supported" in str(exc.value)
        finally:
            conn.close()

    def test_foreign_key_rejected(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute(
                    "CREATE TABLE fk_live (a int REFERENCES other_table (id))"
                )
            assert exc.value.pgcode == "0A000"
            assert "foreign key" in str(exc.value)
        finally:
            conn.close()

    def test_index_form_rejections(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE form_live (a int, data jsonb)")
            for sql, fragment in [
                ("CREATE INDEX fi1 ON form_live USING gin (data)", "btree"),
                ("CREATE INDEX fi2 ON form_live (a) WHERE a > 0", "partial"),
                ("CREATE INDEX fi3 ON form_live (lower(data::text))", "expression"),
                ("CREATE MATERIALIZED VIEW mv_live AS SELECT 1", "materialized"),
                ("CREATE TABLE part_live (a int) PARTITION BY RANGE (a)", "partition"),
            ]:
                with pytest.raises(psycopg2.Error) as exc:
                    cur.execute(sql)
                assert exc.value.pgcode == "0A000", sql
                assert fragment in str(exc.value), sql
            cur.execute("DROP TABLE form_live")
        finally:
            conn.close()

    def test_alter_column_type_rejected(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE alt_live (a int)")
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute("ALTER TABLE alt_live ALTER COLUMN a TYPE text")
            assert exc.value.pgcode == "0A000"
            assert "ALTER COLUMN TYPE" in str(exc.value)
            cur.execute("DROP TABLE alt_live")
        finally:
            conn.close()

    def test_check_constraint_not_valid_and_async_validate(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE chk_live (a int)")
            cur.execute("INSERT INTO chk_live VALUES (1)")
            # CHECK via ALTER TABLE requires NOT VALID
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute(
                    "ALTER TABLE chk_live ADD CONSTRAINT chk_a CHECK (a > 0)"
                )
            assert exc.value.pgcode == "0A000"
            assert "NOT VALID" in str(exc.value)
            cur.execute(
                "ALTER TABLE chk_live ADD CONSTRAINT chk_a CHECK (a > 0) NOT VALID"
            )
            # Async validation returns a job_id and registers a job
            cur.execute("ALTER TABLE ASYNC chk_live VALIDATE CONSTRAINT chk_a")
            job_id = cur.fetchone()[0]
            cur.execute(
                "SELECT job_type, status FROM sys.jobs WHERE job_id = %s" % repr(job_id)
            )
            assert cur.fetchone() == ("VALIDATE_CONSTRAINT", "completed")
            # Backend actually validated: violating rows are now rejected
            with pytest.raises(psycopg2.Error):
                cur.execute("INSERT INTO chk_live VALUES (-1)")
            cur.execute("DROP TABLE chk_live")
        finally:
            conn.close()

    def test_max_24_indexes_per_table(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE idx_live (a int)")
            for i in range(24):
                cur.execute(f"CREATE INDEX ASYNC idx_live_{i} ON idx_live (a)")
                cur.fetchone()  # consume the job_id row
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute("CREATE INDEX ASYNC idx_live_24 ON idx_live (a)")
            assert exc.value.pgcode == "0A000"
            assert "24 indexes" in str(exc.value)
            cur.execute("DROP TABLE idx_live")
        finally:
            conn.close()

    def test_oc001_stale_catalog(self, dsql_proxy):
        """A txn that outlives another session's DDL gets 40001 OC001 once."""
        psycopg2 = pytest.importorskip("psycopg2")
        stale = _pg_connect(dsql_proxy, autocommit=False)
        other = _pg_connect(dsql_proxy)
        try:
            stale.cursor().execute("SELECT 1")  # opens txn, caches catalog
            other.cursor().execute("CREATE TABLE oc_live (a int)")  # bumps it
            with pytest.raises(psycopg2.Error) as exc:
                stale.cursor().execute("SELECT 1")
            assert exc.value.pgcode == "40001"
            assert "OC001" in str(exc.value)
            # Retry on the same session sees the fresh catalog.
            stale.cursor().execute("SELECT 1")
            other.cursor().execute("DROP TABLE oc_live")
        finally:
            stale.close()
            other.close()

    def test_for_update_rules(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE fu_live (id int primary key, v text)")
            cur.execute("INSERT INTO fu_live VALUES (1, 'x')")
            # equality on the PK: fine
            cur.execute("SELECT * FROM fu_live WHERE id = 1 FOR UPDATE")
            cur.fetchall()
            for sql, fragment in [
                ("SELECT * FROM fu_live FOR UPDATE", "equality predicates"),
                ("SELECT * FROM fu_live WHERE id > 1 FOR UPDATE", "equality predicates"),
                ("SELECT * FROM fu_live WHERE v = 'x' FOR UPDATE", "equality predicates"),
                (
                    "SELECT * FROM fu_live a JOIN fu_live b ON a.id = b.id "
                    "WHERE a.id = 1 FOR UPDATE",
                    "single table",
                ),
            ]:
                with pytest.raises(psycopg2.Error) as exc:
                    cur.execute(sql)
                assert exc.value.pgcode == "0A000", sql
                assert fragment in str(exc.value), sql
            cur.execute("DROP TABLE fu_live")
        finally:
            conn.close()

    def test_row_limit_enforced(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE rl_live (a int)")
            big = "INSERT INTO rl_live VALUES " + ",".join(
                f"({i})" for i in range(3001)
            )
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute(big)
            assert exc.value.pgcode == "25006"
            assert "3,000" in str(exc.value)
            cur.execute("SELECT count(*) FROM rl_live")
            assert cur.fetchone()[0] == 0
            cur.execute("DROP TABLE rl_live")
        finally:
            conn.close()

    def test_mixed_ddl_dml_in_txn_rejected(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy, autocommit=False)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE tx_live (a int)")  # psycopg2 sends BEGIN
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute("INSERT INTO tx_live VALUES (1)")
            assert exc.value.pgcode == "25006"
            assert "cannot be mixed" in str(exc.value)
            conn.rollback()
        finally:
            conn.close()
        # Rollback discarded the CREATE TABLE (PG transactional DDL) — the
        # DROP is just a safety net if the backend behaved differently.
        conn = _pg_connect(dsql_proxy)
        try:
            conn.cursor().execute("DROP TABLE IF EXISTS tx_live")
        finally:
            conn.close()

    def test_two_ddl_in_txn_rejected(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy, autocommit=False)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE dd1_live (a int)")
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute("CREATE TABLE dd2_live (a int)")
            assert exc.value.pgcode == "25006"
            assert "only one DDL" in str(exc.value)
            conn.rollback()
        finally:
            conn.close()
        conn = _pg_connect(dsql_proxy)
        try:
            conn.cursor().execute("DROP TABLE IF EXISTS dd1_live")
        finally:
            conn.close()

    def test_autocommit_sequential_ddl_fine(self, dsql_proxy):
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            cur.execute("CREATE TABLE ac1_live (a int)")
            cur.execute("CREATE TABLE ac2_live (a int)")
            cur.execute("DROP TABLE ac1_live")
            cur.execute("DROP TABLE ac2_live")
        finally:
            conn.close()

    def test_multi_statement_batch_txn_rules(self, dsql_proxy):
        psycopg2 = pytest.importorskip("psycopg2")
        conn = _pg_connect(dsql_proxy)
        try:
            cur = conn.cursor()
            with pytest.raises(psycopg2.Error) as exc:
                cur.execute(
                    "CREATE TABLE ba1_live (a int); CREATE TABLE ba2_live (a int)"
                )
            assert exc.value.pgcode == "25006"
            # Nothing in the batch was forwarded.
            cur.execute(
                "SELECT tablename FROM pg_tables WHERE tablename LIKE 'ba%_live'"
            )
            assert cur.fetchall() == []
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Container lifecycle smoke test (needs a Docker daemon)
# ---------------------------------------------------------------------------


@requires_docker
def test_cluster_data_plane_end_to_end(dsql):
    """CreateCluster -> poll ACTIVE -> psycopg2 through the endpoint."""
    psycopg2 = pytest.importorskip("psycopg2")
    resp = dsql.create_cluster()
    identifier = resp["identifier"]
    try:
        deadline = time.time() + 180
        cluster = None
        while time.time() < deadline:
            cluster = dsql.get_cluster(identifier=identifier)
            if cluster["status"] == "ACTIVE":
                break
            time.sleep(2)
        else:
            pytest.fail(f"cluster {identifier} not ACTIVE after 180s")

        port = int(cluster["endpoint"].rsplit(":", 1)[1])
        deadline = time.time() + 60
        while True:
            try:
                conn = psycopg2.connect(
                    host="127.0.0.1",
                    port=port,
                    user="admin",
                    password="anything",
                    dbname="postgres",
                    connect_timeout=5,
                )
                break
            except psycopg2.OperationalError:
                if time.time() > deadline:
                    raise
                time.sleep(1)
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("CREATE TABLE e2e (id int, name text)")
            cur.execute("INSERT INTO e2e VALUES (1, 'x')")
            cur.execute("SELECT name FROM e2e WHERE id = 1")
            assert cur.fetchone()[0] == "x"
            cur.execute("DROP TABLE e2e")
        finally:
            conn.close()
    finally:
        _cleanup(dsql, identifier)
