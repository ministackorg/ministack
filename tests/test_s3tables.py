"""S3 Tables service tests — round-trip coverage for the 14 control-plane
operations the service ships in 1.3.50, plus a multi-tenancy isolation check.

Operations covered:
  CreateTableBucket, ListTableBuckets, GetTableBucket, DeleteTableBucket
  CreateNamespace, ListNamespaces, GetNamespace, DeleteNamespace
  CreateTable, ListTables, GetTable, DeleteTable
  GetTableMetadataLocation, UpdateTableMetadataLocation

Shapes verified against `botocore.data.s3tables.2024-12-01.service-2`.
"""

import contextlib
import json
import os
import urllib.error
import urllib.parse
import urllib.request
import uuid as _uuid_mod

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

_ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")


def _iceberg_json(path, method="GET", payload=None, region_name=None, authorization=None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"}
    if authorization:
        headers["Authorization"] = authorization
    elif region_name:
        headers["Authorization"] = (
            "AWS4-HMAC-SHA256 "
            f"Credential=test/20260604/{region_name}/s3tables/aws4_request, "
            "SignedHeaders=host, Signature=test"
        )
    req = urllib.request.Request(
        f"{_ENDPOINT}{path}",
        data=data,
        method=method,
        headers=headers,
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read().decode("utf-8") or "{}")


def _make_s3tables_client(access_key="test", region_name="us-east-1"):
    return boto3.client(
        "s3tables",
        endpoint_url=_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key="test",
        region_name=region_name,
        config=Config(retries={"mode": "standard"}),
    )


@pytest.fixture(scope="session")
def s3tables():
    return _make_s3tables_client()


# ── Table bucket lifecycle ──────────────────────────────────

def test_s3tables_create_list_get_delete_bucket(s3tables):
    name = f"tb-bucket-{_uuid_mod.uuid4().hex[:8]}"

    created = s3tables.create_table_bucket(name=name)
    assert "arn" in created
    arn = created["arn"]
    assert name in arn
    assert arn.startswith("arn:aws:s3tables:")

    listed = s3tables.list_table_buckets()
    names = {b.get("name") for b in listed.get("tableBuckets", [])}
    assert name in names, f"created bucket {name!r} not in ListTableBuckets"

    got = s3tables.get_table_bucket(tableBucketARN=arn)
    assert got.get("name") == name
    assert got.get("arn") == arn

    s3tables.delete_table_bucket(tableBucketARN=arn)
    with pytest.raises(ClientError) as exc:
        s3tables.get_table_bucket(tableBucketARN=arn)
    assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")


def test_s3tables_table_bucket_provisions_backing_s3_bucket(s3tables):
    """Creating a table bucket must automatically provision a same-named S3 bucket
    so that data-plane writes (Parquet files, manifests) have storage to land in —
    mirroring real AWS where S3 Tables manages underlying storage transparently."""
    import boto3
    from botocore.config import Config
    name = f"tb-backing-{_uuid_mod.uuid4().hex[:8]}"
    arn = s3tables.create_table_bucket(name=name)["arn"]
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=_ENDPOINT,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name="us-east-1",
            config=Config(retries={"mode": "standard"}),
        )
        # The backing bucket must be accessible via the S3 API
        s3_client.head_bucket(Bucket=name)

        # And must accept object writes (simulating a Parquet data file)
        s3_client.put_object(Bucket=name, Key="ns/tbl/data/part-0.parquet", Body=b"parquet")
        obj = s3_client.get_object(Bucket=name, Key="ns/tbl/data/part-0.parquet")
        assert obj["Body"].read() == b"parquet"
        # Deleting the table bucket must also remove the backing S3 bucket
        s3tables.delete_table_bucket(tableBucketARN=arn)
        from botocore.exceptions import ClientError as _CE
        with pytest.raises(_CE) as exc:
            s3_client.head_bucket(Bucket=name)
        assert exc.value.response["Error"]["Code"] in ("404", "NoSuchBucket")
    except Exception:
        try:
            s3tables.delete_table_bucket(tableBucketARN=arn)
        except Exception:
            pass
        raise


def test_s3tables_get_bucket_missing_returns_not_found(s3tables):
    fake_arn = "arn:aws:s3tables:us-east-1:000000000000:bucket/does-not-exist-xyz"
    with pytest.raises(ClientError) as exc:
        s3tables.get_table_bucket(tableBucketARN=fake_arn)
    assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")


def test_s3tables_bucket_arn_scope_does_not_fallback_to_local_bucket(s3tables):
    bucket_name = f"tb-scope-{_uuid_mod.uuid4().hex[:8]}"
    arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=arn, namespace=[ns])
        s3tables.create_table(
            tableBucketARN=arn,
            namespace=ns,
            name=table,
            format="ICEBERG",
            metadata={"iceberg": {"schema": {"fields": [{"name": "id", "type": "long"}]}}},
        )

        wrong_region = arn.replace(":us-east-1:", ":us-west-2:")
        wrong_account = arn.replace(":000000000000:", ":111111111111:")
        wrong_service = arn.replace(":s3tables:", ":s3:")
        wrong_resource = arn.replace(":bucket/", ":table/")
        for bad_ref in (wrong_region, wrong_account, wrong_service, wrong_resource):
            with pytest.raises(ClientError) as exc:
                s3tables.get_table_bucket(tableBucketARN=bad_ref)
            assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")

            with pytest.raises(ClientError) as exc:
                s3tables.create_namespace(tableBucketARN=bad_ref, namespace=[f"ns_{_uuid_mod.uuid4().hex[:6]}"])
            assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")

            with pytest.raises(ClientError) as exc:
                s3tables.list_tables(tableBucketARN=bad_ref)
            assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")

            with pytest.raises(ClientError) as exc:
                s3tables.get_table(tableBucketARN=bad_ref, namespace=ns, name=table)
            assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")
    finally:
        try:
            s3tables.delete_table(tableBucketARN=arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=arn)


# ── Namespace lifecycle ─────────────────────────────────────

def test_s3tables_create_list_get_delete_namespace(s3tables):
    bucket_name = f"tb-ns-{_uuid_mod.uuid4().hex[:8]}"
    arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    try:
        ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
        created = s3tables.create_namespace(tableBucketARN=arn, namespace=[ns])
        assert created.get("namespace") == [ns] or created.get("namespace") == ns

        listed = s3tables.list_namespaces(tableBucketARN=arn)
        ns_values = []
        for entry in listed.get("namespaces", []):
            n = entry.get("namespace")
            ns_values.append(n[0] if isinstance(n, list) else n)
        assert ns in ns_values

        got = s3tables.get_namespace(tableBucketARN=arn, namespace=ns)
        got_ns = got.get("namespace")
        assert (got_ns[0] if isinstance(got_ns, list) else got_ns) == ns

        s3tables.delete_namespace(tableBucketARN=arn, namespace=ns)
        with pytest.raises(ClientError) as exc:
            s3tables.get_namespace(tableBucketARN=arn, namespace=ns)
        assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")
    finally:
        s3tables.delete_table_bucket(tableBucketARN=arn)


# ── Table lifecycle ─────────────────────────────────────────

def test_s3tables_create_list_get_delete_table(s3tables):
    bucket_name = f"tb-tbl-{_uuid_mod.uuid4().hex[:8]}"
    arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=arn, namespace=[ns])
        table = f"t_{_uuid_mod.uuid4().hex[:6]}"
        created = s3tables.create_table(
            tableBucketARN=arn, namespace=ns, name=table, format="ICEBERG",
            metadata={
                "iceberg": {
                    "schema": {
                        "fields": [
                            {"name": "id", "type": "long", "required": True},
                            {"name": "value", "type": "string"},
                        ]
                    }
                }
            },
        )
        assert "tableARN" in created
        table_arn = created["tableARN"]
        assert ns in table_arn and table in table_arn

        listed = s3tables.list_tables(tableBucketARN=arn)
        table_names = {t.get("name") for t in listed.get("tables", [])}
        assert table in table_names

        got = s3tables.get_table(tableBucketARN=arn, namespace=ns, name=table)
        assert got.get("name") == table
        assert got.get("format") == "ICEBERG"

        s3tables.delete_table(tableBucketARN=arn, namespace=ns, name=table)
        with pytest.raises(ClientError) as exc:
            s3tables.get_table(tableBucketARN=arn, namespace=ns, name=table)
        assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")
    finally:
        try:
            s3tables.delete_namespace(tableBucketARN=arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=arn)


# ── Metadata location round-trip ────────────────────────────

def test_s3tables_get_update_table_metadata_location(s3tables):
    bucket_name = f"tb-md-{_uuid_mod.uuid4().hex[:8]}"
    arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=arn, namespace=[ns])
        s3tables.create_table(
            tableBucketARN=arn, namespace=ns, name=table, format="ICEBERG",
            metadata={"iceberg": {"schema": {"fields": [{"name": "id", "type": "long"}]}}},
        )

        got = s3tables.get_table_metadata_location(
            tableBucketARN=arn, namespace=ns, name=table)
        assert "metadataLocation" in got
        token = got.get("versionToken", "")

        new_loc = f"s3://{bucket_name}/{ns}/{table}/metadata/v1.metadata.json"
        updated = s3tables.update_table_metadata_location(
            tableBucketARN=arn, namespace=ns, name=table,
            versionToken=token, metadataLocation=new_loc,
        )
        assert updated.get("metadataLocation") == new_loc

        got2 = s3tables.get_table_metadata_location(
            tableBucketARN=arn, namespace=ns, name=table)
        assert got2.get("metadataLocation") == new_loc
    finally:
        try:
            s3tables.delete_table(tableBucketARN=arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=arn)


# ── Multi-tenancy isolation ─────────────────────────────────

def test_s3tables_buckets_are_account_scoped(s3tables):
    """Same bucket name under two different account IDs must not collide.

    Multi-tenancy is enforced by the SigV4 access-key-derived account ID; we
    swap clients with 12-digit access keys and assert ListTableBuckets returns
    only the caller's buckets."""
    acct_a = "111111111111"
    acct_b = "222222222222"
    name = f"shared-{_uuid_mod.uuid4().hex[:6]}"

    client_a = _make_s3tables_client(access_key=acct_a)
    client_b = _make_s3tables_client(access_key=acct_b)

    arn_a = client_a.create_table_bucket(name=name)["arn"]
    arn_b = client_b.create_table_bucket(name=name)["arn"]
    try:
        assert acct_a in arn_a
        assert acct_b in arn_b
        assert arn_a != arn_b

        names_a = {b.get("name") for b in client_a.list_table_buckets().get("tableBuckets", [])}
        names_b = {b.get("name") for b in client_b.list_table_buckets().get("tableBuckets", [])}
        assert name in names_a
        assert name in names_b

        # Cross-account access must not see the other tenant's bucket.
        with pytest.raises(ClientError):
            client_a.get_table_bucket(tableBucketARN=arn_b)
        with pytest.raises(ClientError):
            client_b.get_table_bucket(tableBucketARN=arn_a)
    finally:
        try:
            client_a.delete_table_bucket(tableBucketARN=arn_a)
        except Exception:
            pass
        try:
            client_b.delete_table_bucket(tableBucketARN=arn_b)
        except Exception:
            pass


def test_s3tables_buckets_are_region_scoped():
    name = f"regional-{_uuid_mod.uuid4().hex[:6]}"
    east = _make_s3tables_client(region_name="us-east-1")
    west = _make_s3tables_client(region_name="us-west-2")

    east_arn = east.create_table_bucket(name=name)["arn"]
    west_arn = west.create_table_bucket(name=name)["arn"]
    try:
        assert ":us-east-1:" in east_arn
        assert ":us-west-2:" in west_arn
        assert east_arn != west_arn

        east_arns = {b.get("arn") for b in east.list_table_buckets().get("tableBuckets", [])}
        west_arns = {b.get("arn") for b in west.list_table_buckets().get("tableBuckets", [])}
        assert east_arn in east_arns
        assert west_arn not in east_arns
        assert west_arn in west_arns
        assert east_arn not in west_arns

        with pytest.raises(ClientError) as exc:
            east.get_table_bucket(tableBucketARN=west_arn)
        assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")
    finally:
        try:
            east.delete_table_bucket(tableBucketARN=east_arn)
        except Exception:
            pass
        try:
            west.delete_table_bucket(tableBucketARN=west_arn)
        except Exception:
            pass


def test_s3tables_iceberg_catalog_spans_control_plane_regions():
    west = _make_s3tables_client(region_name="us-west-2")
    bucket_name = f"iceberg-west-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = west.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    iceberg_table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        west.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        west.create_table(
            tableBucketARN=bucket_arn,
            namespace=ns,
            name=table,
            format="ICEBERG",
            metadata={"iceberg": {"schema": {"fields": [{"name": "id", "type": "long"}]}}},
        )

        namespaces = _iceberg_json("/iceberg/v1/catalog/namespaces")
        assert [ns] in namespaces.get("namespaces", [])

        tables = _iceberg_json(f"/iceberg/v1/catalog/namespaces/{ns}/tables")
        assert {"namespace": [ns], "name": table} in tables.get("identifiers", [])

        loaded = _iceberg_json(f"/iceberg/v1/catalog/namespaces/{ns}/tables/{table}")
        assert loaded.get("metadata-location", "").startswith(f"s3://{bucket_name}/{ns}/{table}/")

        bearer_loaded = _iceberg_json(
            f"/iceberg/v1/catalog/namespaces/{ns}/tables/{table}",
            authorization="Bearer test-token",
        )
        assert bearer_loaded.get("metadata-location", "").startswith(f"s3://{bucket_name}/{ns}/{table}/")

        _iceberg_json(
            f"/iceberg/v1/catalog/namespaces/{ns}/tables",
            method="POST",
            payload={
                "name": iceberg_table,
                "schema": {"type": "struct", "fields": [{"id": 1, "name": "id", "type": "long"}]},
            },
        )
        got = west.get_table(tableBucketARN=bucket_arn, namespace=ns, name=iceberg_table)
        assert got["name"] == iceberg_table
    finally:
        for candidate in (table, iceberg_table):
            try:
                west.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=candidate)
            except Exception:
                pass
        try:
            west.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        west.delete_table_bucket(tableBucketARN=bucket_arn)


def test_s3tables_iceberg_catalog_prefers_signed_region_for_duplicate_names():
    east = _make_s3tables_client(region_name="us-east-1")
    west = _make_s3tables_client(region_name="us-west-2")
    east_bucket = f"iceberg-east-{_uuid_mod.uuid4().hex[:6]}"
    west_bucket = f"iceberg-west-{_uuid_mod.uuid4().hex[:6]}"
    east_arn = east.create_table_bucket(name=east_bucket)["arn"]
    west_arn = west.create_table_bucket(name=west_bucket)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        for client, bucket_arn in ((east, east_arn), (west, west_arn)):
            client.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
            client.create_table(
                tableBucketARN=bucket_arn,
                namespace=ns,
                name=table,
                format="ICEBERG",
                metadata={"iceberg": {"schema": {"fields": [{"name": "id", "type": "long"}]}}},
            )

        east_loaded = _iceberg_json(
            f"/iceberg/v1/catalog/namespaces/{ns}/tables/{table}",
            region_name="us-east-1",
        )
        west_loaded = _iceberg_json(
            f"/iceberg/v1/catalog/namespaces/{ns}/tables/{table}",
            region_name="us-west-2",
        )
        assert east_loaded.get("metadata-location", "").startswith(f"s3://{east_bucket}/{ns}/{table}/")
        assert west_loaded.get("metadata-location", "").startswith(f"s3://{west_bucket}/{ns}/{table}/")

        with pytest.raises(urllib.error.HTTPError) as exc:
            _iceberg_json(
                f"/iceberg/v1/catalog/namespaces/{ns}/tables/{table}",
                region_name="us-east-2",
            )
        assert exc.value.code == 404
    finally:
        for client, bucket_arn in ((east, east_arn), (west, west_arn)):
            try:
                client.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
            except Exception:
                pass
            try:
                client.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
            except Exception:
                pass
            try:
                client.delete_table_bucket(tableBucketARN=bucket_arn)
            except Exception:
                pass


def test_iceberg_rest_error_uses_spec_envelope():
    """The /iceberg REST surface must return the Iceberg REST OpenAPI ErrorModel
    ({"error": {message, type, code}}), not the AWS {"__type"} shape. A LoadTable
    on a missing table must be a proper NoSuchTableException so spec-compliant
    writers (DuckDB, Spark) proceed to create it instead of aborting."""
    with pytest.raises(urllib.error.HTTPError) as exc:
        _iceberg_json("/iceberg/v1/catalog/namespaces/nope/tables/missing")
    assert exc.value.code == 404
    body = json.loads(exc.value.read().decode("utf-8"))
    assert "__type" not in body
    assert body["error"]["type"] == "NoSuchTableException"
    assert body["error"]["code"] == 404
    assert body["error"]["message"]


def test_s3tables_iceberg_routes_without_prefix_are_unknown(s3tables):
    """Every route but /v1/config takes the table bucket ARN prefix; real AWS
    answers one without it 404 <UnknownOperationException/>."""
    bucket_arn = s3tables.create_table_bucket(name=f"tb-noprefix-{_uuid_mod.uuid4().hex[:6]}")["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(
            tableBucketARN=bucket_arn, namespace=ns, name=table, format="ICEBERG",
            metadata={"iceberg": {"schema": {"fields": [{"name": "id", "type": "long"}]}}},
        )
        for method, path in (
            ("GET", "/iceberg/v1/namespaces"),
            ("GET", f"/iceberg/v1/namespaces/{ns}/tables"),
            ("GET", f"/iceberg/v1/namespaces/{ns}/tables/{table}"),
            ("POST", f"/iceberg/v1/namespaces/{ns}/tables"),
            ("POST", "/iceberg/v1/transactions/commit"),
        ):
            req = urllib.request.Request(f"{_ENDPOINT}{path}", method=method, data=b"{}" if method == "POST" else None)
            with pytest.raises(urllib.error.HTTPError) as exc:
                urllib.request.urlopen(req)
            assert (exc.value.code, exc.value.read()) == (404, b"<UnknownOperationException/>")
        loaded = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        assert loaded["metadata-location"].startswith(f"s3://{bucket_arn.rsplit('/', 1)[1]}/{ns}/{table}/")
    finally:
        s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)


def test_s3tables_iceberg_config_returns_s3_defaults():
    """GET /iceberg/v1/config hands back the url-encoded table bucket ARN as the
    prefix in defaults, as AWS does, plus the S3 connection defaults."""
    arn = "arn:aws:s3tables:us-east-1:000000000000:bucket/cfg-bucket"
    resp = _iceberg_json(f"/iceberg/v1/config?warehouse={_arn_prefix(arn)}")
    defaults = resp["defaults"]
    assert defaults["prefix"] == _arn_prefix(arn)
    assert resp["overrides"] == {}
    for key in ("s3.endpoint", "s3.access-key-id", "s3.secret-access-key", "s3.path-style-access", "client.region"):
        assert key in defaults


@pytest.mark.parametrize("query", ["", "?warehouse=arn:aws:s3tables:us-east-1:000000000000:bucket"])
def test_s3tables_iceberg_config_needs_a_table_bucket_arn(query):
    status, body = _iceberg_call(f"/iceberg/v1/config{query}")
    assert (status, body) == (400, {"error": {
        "message": "The specified table bucket ARN is not valid.", "type": "bad_request", "code": 400}})


def test_s3tables_iceberg_load_table_includes_region(s3tables):
    """LoadTable response config must include s3.region and client.region so
    DuckDB / Spark can route S3 data-file requests to the right region."""
    bucket_name = f"tb-region-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(tableBucketARN=bucket_arn, namespace=ns, name=table, format="ICEBERG")

        resp = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        config = resp.get("config", {})
        assert "s3.region" in config, "s3.region missing from LoadTable config"
        assert "client.region" in config, "client.region missing from LoadTable config"
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)


def test_s3tables_iceberg_transactions_commit(s3tables):
    """POST /iceberg/v1/transactions/commit — DuckDB uses this endpoint for
    atomic multi-table commits when writing data."""
    bucket_name = f"tb-txn-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(tableBucketARN=bucket_arn, namespace=ns, name=table, format="ICEBERG")

        snapshot_id = 1234567890
        resp = _iceberg_json(
            f"/iceberg/v1/{_arn_prefix(bucket_arn)}/transactions/commit",
            method="POST",
            payload={
                "table-changes": [
                    {
                        "identifier": {"namespace": [ns], "name": table},
                        "requirements": [],
                        "updates": [
                            {
                                "action": "add-snapshot",
                                "snapshot": {
                                    "snapshot-id": snapshot_id,
                                    "sequence-number": 1,
                                    "timestamp-ms": 1700000000000,
                                    "manifest-list": f"s3://{bucket_name}/{ns}/{table}/metadata/snap.avro",
                                    "summary": {"operation": "append"},
                                },
                            },
                            {
                                "action": "set-snapshot-ref",
                                "ref-name": "main",
                                "type": "branch",
                                "snapshot-id": snapshot_id,
                            },
                        ],
                    }
                ]
            },
        )
        # A successful commit returns 200 with an empty body (or minimal body)
        assert resp is not None

        # Verify the snapshot was recorded in the table metadata
        loaded = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        metadata = loaded.get("metadata", {})
        snap_ids = [s.get("snapshot-id") for s in metadata.get("snapshots", [])]
        assert snapshot_id in snap_ids, f"snapshot {snapshot_id} not found after commit"
        assert metadata.get("current-snapshot-id") == snapshot_id
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)



def test_s3tables_iceberg_upgrade_format_version(s3tables):
    """A commit carrying the Iceberg REST ``upgrade-format-version`` action
    (what Spark sends for ``.tableProperty("format-version", "3")`` on an
    existing table) moves the table's metadata to that version; re-asserting
    the current version is a no-op and a downgrade is refused with 400.
    Real S3 Tables supports Iceberg format version 3."""
    bucket_name = f"tb-fv-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(tableBucketARN=bucket_arn, namespace=ns, name=table, format="ICEBERG")

        loaded = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        assert loaded["metadata"]["format-version"] == 2

        resp = _iceberg_json(
            f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}", method="POST",
            payload={"requirements": [],
                     "updates": [{"action": "upgrade-format-version", "format-version": 3}]},
        )
        assert resp["metadata"]["format-version"] == 3
        loaded = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        assert loaded["metadata"]["format-version"] == 3

        # Re-asserting the version the table already has is a no-op commit.
        resp = _iceberg_json(
            f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}", method="POST",
            payload={"requirements": [],
                     "updates": [{"action": "upgrade-format-version", "format-version": 3}]},
        )
        assert resp["metadata"]["format-version"] == 3

        # A downgrade is refused, and the table keeps its version.
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            _iceberg_json(
                f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}", method="POST",
                payload={"requirements": [],
                         "updates": [{"action": "upgrade-format-version", "format-version": 2}]},
            )
        assert exc_info.value.code == 400
        assert b"downgrade" in exc_info.value.read()
        loaded = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        assert loaded["metadata"]["format-version"] == 3
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)


def test_s3tables_iceberg_create_table_rejects_resend_instead_of_wiping_data(s3tables):
    """CreateTable is not idempotent per the Iceberg REST spec -- a resent create
    (e.g. a client retry after a lost response to a create that already landed)
    must be rejected with a conflict, matching the S3 Tables control-plane path's
    existing 409 behaviour. Before this guard, a resend silently replaced the
    table's metadata with a fresh empty one, discarding any snapshots/schema
    already committed -- worse than a crash, since nothing signals data was lost."""
    bucket_name = f"tb-createretry-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])

        _iceberg_json(
            f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables",
            method="POST",
            payload={"name": table, "schema": {"type": "struct", "schema-id": 0, "fields": []}},
        )
        # Commit a snapshot so there's real state a resent create could destroy.
        snapshot_id = 42
        _iceberg_json(
            f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}",
            method="POST",
            payload={
                "requirements": [],
                "updates": [
                    {
                        "action": "add-snapshot",
                        "snapshot": {
                            "snapshot-id": snapshot_id,
                            "sequence-number": 1,
                            "timestamp-ms": 1700000000000,
                            "manifest-list": f"s3://{bucket_name}/{ns}/{table}/metadata/snap.avro",
                            "summary": {"operation": "append"},
                        },
                    },
                ],
            },
        )

        # Resend the identical create, simulating a retry of a create whose first
        # response was lost.
        try:
            _iceberg_json(
                f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables",
                method="POST",
                payload={"name": table, "schema": {"type": "struct", "schema-id": 0, "fields": []}},
            )
            assert False, "expected a 409 conflict on resent CreateTable"
        except urllib.error.HTTPError as e:
            assert e.code == 409
            body = json.loads(e.read().decode("utf-8"))
            assert body["error"]["type"] == "AlreadyExistsException"

        # The original snapshot must still be there -- not wiped by the resend.
        loaded = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        snap_ids = [s.get("snapshot-id") for s in loaded["metadata"]["snapshots"]]
        assert snapshot_id in snap_ids, "resent create-table must not wipe existing table state"
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)



def test_s3tables_iceberg_add_schema_advances_last_column_id(s3tables):
    """Each add-schema commit must bump last-column-id to the new schema's highest
    field ID, so a client allocating the *next* column's ID (e.g. DuckDB's
    ALTER TABLE ... ADD COLUMN) doesn't collide with a previous add-schema's fields."""
    bucket_name = f"tb-lcid-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(tableBucketARN=bucket_arn, namespace=ns, name=table, format="ICEBERG")

        _iceberg_json(
            f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}",
            method="POST",
            payload={
                "requirements": [],
                "updates": [
                    {
                        "action": "add-schema",
                        "schema": {
                            "type": "struct", "schema-id": 1,
                            "fields": [{"id": 1, "name": "colA", "required": False, "type": "string"}],
                        },
                    },
                    {"action": "set-current-schema", "schema-id": 1},
                ],
            },
        )
        first = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        assert first["metadata"]["last-column-id"] == 1

        _iceberg_json(
            f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}",
            method="POST",
            payload={
                "requirements": [],
                "updates": [
                    {
                        "action": "add-schema",
                        "schema": {
                            "type": "struct", "schema-id": 2,
                            "fields": [
                                {"id": 1, "name": "colA", "required": False, "type": "string"},
                                {"id": 2, "name": "colB", "required": False, "type": "long"},
                            ],
                        },
                    },
                    {"action": "set-current-schema", "schema-id": 2},
                ],
            },
        )
        second = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        metadata = second["metadata"]
        assert metadata["last-column-id"] == 2

        current_schema = next(s for s in metadata["schemas"] if s["schema-id"] == metadata["current-schema-id"])
        field_ids = [f["id"] for f in current_schema["fields"]]
        assert field_ids == [1, 2], "field IDs must not collide across separate add-schema commits"
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)



def test_s3tables_iceberg_add_spec_uses_real_wire_action_name(s3tables):
    """The Iceberg REST spec's real action name for adding a partition spec is
    "add-spec" (what duckdb-iceberg actually sends) — not "add-partition-spec",
    a non-standard name only some hand-rolled callers use. Both must work."""
    bucket_name = f"tb-spec-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(tableBucketARN=bucket_arn, namespace=ns, name=table, format="ICEBERG")

        _iceberg_json(
            f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}",
            method="POST",
            payload={
                "requirements": [],
                "updates": [
                    {
                        "action": "add-spec",
                        "spec": {
                            "spec-id": 1,
                            "fields": [{"source-id": 1, "field-id": 1000, "name": "day_col", "transform": "day"}],
                        },
                    },
                    {"action": "set-default-spec", "spec-id": 1},
                ],
            },
        )
        loaded = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        metadata = loaded["metadata"]
        spec_ids = [s["spec-id"] for s in metadata["partition-specs"]]
        assert 1 in spec_ids, "add-spec must append the new partition spec"
        assert metadata["default-spec-id"] == 1
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)



def test_s3tables_iceberg_create_table_honors_requested_partition_spec(s3tables):
    """The create-table endpoint's response must echo back the partition spec the
    client actually asked for, not a hardcoded empty one -- clients (e.g.
    duckdb-iceberg) initialize their own local table state from this response, so an
    empty echo here gets asserted right back at the server on the very next commit,
    permanently losing the partition spec even though it was in the original request."""
    bucket_name = f"tb-createspec-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])

        created = _iceberg_json(
            f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables",
            method="POST",
            payload={
                "name": table,
                "schema": {
                    "type": "struct", "schema-id": 0,
                    "fields": [{"id": 1, "name": "ts", "required": False, "type": "timestamp"}],
                },
                "partition-spec": {
                    "spec-id": 0,
                    "fields": [{"source-id": 1, "field-id": 1000, "name": "day_ts", "transform": "day"}],
                },
            },
        )
        metadata = created["metadata"]
        spec = next(s for s in metadata["partition-specs"] if s["spec-id"] == metadata["default-spec-id"])
        assert spec["fields"], "create-table response must echo back the requested partition spec, not an empty one"
        assert spec["fields"][0]["transform"] == "day"
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)


def test_s3tables_iceberg_add_schema_is_idempotent_by_schema_id(s3tables):
    """A resent add-schema commit for an already-registered schema-id (Spark
    re-declares its current schema on every write) must not append a second
    entry -- Iceberg's own schemasById() crashes on load with "Multiple entries
    with same key: <id>=table {...}" if the schemas list has a duplicate id."""
    bucket_name = f"tb-schemadup-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(tableBucketARN=bucket_arn, namespace=ns, name=table, format="ICEBERG")

        payload = {
            "requirements": [],
            "updates": [
                {
                    "action": "add-schema",
                    "schema": {
                        "type": "struct", "schema-id": 0,
                        "fields": [{"id": 1, "name": "colA", "required": False, "type": "string"}],
                    },
                },
                {"action": "set-current-schema", "schema-id": 0},
            ],
        }
        # Send the identical commit twice, simulating Spark re-declaring its
        # current schema on a second, unrelated write.
        for _ in range(2):
            _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}", method="POST", payload=payload)

        loaded = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        schema_ids = [s.get("schema-id") for s in loaded["metadata"]["schemas"]]
        assert schema_ids.count(0) == 1, "resent add-schema must not duplicate the schema-id entry"
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)


def test_s3tables_iceberg_add_spec_is_idempotent_by_spec_id(s3tables):
    """A resent add-spec commit for an already-registered spec-id must not
    append a second entry -- Iceberg's own specsById() crashes the same way
    schemasById() does on a duplicate id."""
    bucket_name = f"tb-specdup-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(tableBucketARN=bucket_arn, namespace=ns, name=table, format="ICEBERG")

        payload = {
            "requirements": [],
            "updates": [
                {
                    "action": "add-spec",
                    "spec": {
                        "spec-id": 1,
                        "fields": [{"source-id": 1, "field-id": 1000, "name": "day_col", "transform": "day"}],
                    },
                },
                {"action": "set-default-spec", "spec-id": 1},
            ],
        }
        for _ in range(2):
            _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}", method="POST", payload=payload)

        loaded = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        spec_ids = [s.get("spec-id") for s in loaded["metadata"]["partition-specs"]]
        assert spec_ids.count(1) == 1, "resent add-spec must not duplicate the spec-id entry"
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)


def test_s3tables_iceberg_add_sort_order_is_idempotent_by_order_id(s3tables):
    """A resent add-sort-order commit for an already-registered order-id must
    not append a second entry -- Iceberg's own sortOrdersById() crashes the
    same way schemasById() does on a duplicate id."""
    bucket_name = f"tb-orderdup-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(tableBucketARN=bucket_arn, namespace=ns, name=table, format="ICEBERG")

        payload = {
            "requirements": [],
            "updates": [
                {
                    "action": "add-sort-order",
                    "sort-order": {
                        "order-id": 1,
                        "fields": [{"source-id": 1, "transform": "identity", "direction": "asc", "null-order": "nulls-first"}],
                    },
                },
                {"action": "set-default-sort-order", "sort-order-id": 1},
            ],
        }
        for _ in range(2):
            _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}", method="POST", payload=payload)

        loaded = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        order_ids = [o.get("order-id") for o in loaded["metadata"]["sort-orders"]]
        assert order_ids.count(1) == 1, "resent add-sort-order must not duplicate the order-id entry"
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)


def test_s3tables_deletes_answer_204(s3tables):
    """DeleteNamespace / DeleteTable / DeleteTableBucket answer 204 No Content."""
    arn = s3tables.create_table_bucket(name="del-status-bkt")["arn"]
    s3tables.create_namespace(tableBucketARN=arn, namespace=["ns204"])
    s3tables.create_table(tableBucketARN=arn, namespace="ns204", name="t204",
                          format="ICEBERG")
    r = s3tables.delete_table(tableBucketARN=arn, namespace="ns204", name="t204")
    assert r["ResponseMetadata"]["HTTPStatusCode"] == 204
    r = s3tables.delete_namespace(tableBucketARN=arn, namespace="ns204")
    assert r["ResponseMetadata"]["HTTPStatusCode"] == 204
    r = s3tables.delete_table_bucket(tableBucketARN=arn)
    assert r["ResponseMetadata"]["HTTPStatusCode"] == 204


def test_s3tables_iceberg_load_table_serves_the_numerically_latest_metadata(s3tables, s3):
    """LoadTable's scan for the newest metadata.json must order vN numerically:
    lexically v10 sorts before v2, so from the 10th commit on a lexical scan
    serves v9 forever while the table keeps advancing."""
    bucket_name = f"tb-vsort-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns, table = "vsortns", "vsorttbl"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(tableBucketARN=bucket_arn, namespace=ns, name=table, format="ICEBERG")

        base = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")["metadata"]
        for v in range(1, 12):
            doc = dict(base, **{"last-sequence-number": v})
            s3.put_object(
                Bucket=bucket_name,
                Key=f"{ns}/{table}/metadata/v{v}.metadata.json",
                Body=json.dumps(doc).encode(),
            )

        resp = _iceberg_json(f"/iceberg/v1/{_arn_prefix(bucket_arn)}/namespaces/{ns}/tables/{table}")
        assert resp["metadata-location"].endswith("/v11.metadata.json"), resp["metadata-location"]
        assert resp["metadata"]["last-sequence-number"] == 11
    finally:
        try:
            s3tables.delete_table(tableBucketARN=bucket_arn, namespace=ns, name=table)
        except Exception:
            pass
        try:
            s3tables.delete_namespace(tableBucketARN=bucket_arn, namespace=ns)
        except Exception:
            pass
        s3tables.delete_table_bucket(tableBucketARN=bucket_arn)


def test_s3tables_iceberg_prefixed_lookup_scopes_to_the_bucket(s3tables):
    """Two table buckets holding a same-named table must resolve per the
    ``{account}:s3tablescatalog/{bucket}`` prefix — without the filter the
    first match wins and the wrong table's schema comes back."""
    ns, table = "scopens", "orders"
    arns = {}
    try:
        for b in ("tb-scope-a", "tb-scope-b"):
            arns[b] = s3tables.create_table_bucket(name=b)["arn"]
            s3tables.create_namespace(tableBucketARN=arns[b], namespace=[ns])
            s3tables.create_table(
                tableBucketARN=arns[b], namespace=ns, name=table, format="ICEBERG",
                metadata={"iceberg": {"schema": {"fields": [
                    {"name": f"col_{b[-1]}", "type": "string"}]}}},
            )
        for b in ("tb-scope-a", "tb-scope-b"):
            resp = _iceberg_json(
                f"/iceberg/v1/000000000000:s3tablescatalog/{b}/namespaces/{ns}/tables/{table}")
            fields = resp["metadata"]["schemas"][0]["fields"]
            assert [f["name"] for f in fields] == [f"col_{b[-1]}"], (b, fields)
    finally:
        for b, arn in arns.items():
            try:
                s3tables.delete_table(tableBucketARN=arn, namespace=ns, name=table)
                s3tables.delete_namespace(tableBucketARN=arn, namespace=ns)
            except Exception:
                pass
            try:
                s3tables.delete_table_bucket(tableBucketARN=arn)
            except Exception:
                pass


def test_iceberg_unknown_path_answers_404_envelope():
    """A path neither catalog serves must be an Iceberg error envelope, not an
    empty 200 a client would read as success."""
    import urllib.error

    try:
        _iceberg_json("/iceberg/v9/definitely/not/a/route")
        raise AssertionError("expected HTTP error")
    except urllib.error.HTTPError as e:
        assert e.code == 404
        doc = json.loads(e.read())
        assert doc["error"]["type"] == "NotFoundException"


def _iceberg_call(path, method="GET", payload=None):
    try:
        return 200, _iceberg_json(path, method, payload)
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8") or "{}")


def _arn_prefix(arn):
    return urllib.parse.quote(arn, safe="")


def test_s3tables_iceberg_rest_scopes_each_catalog_to_its_warehouse_arn(s3tables):
    """Two catalogs, each with its table-bucket ARN as warehouse; the REST
    {prefix} "is always your url-encoded table bucket ARN"."""
    suffix = _uuid_mod.uuid4().hex[:8]
    arns = [s3tables.create_table_bucket(name=f"wh-{n}-{suffix}")["arn"] for n in ("std", "qual")]
    try:
        for arn in arns:
            p = _arn_prefix(arn)
            assert _iceberg_call(f"/iceberg/v1/{p}/namespaces", "POST", {"namespace": ["ns"]})[0] == 200
            status, created = _iceberg_call(f"/iceberg/v1/{p}/namespaces/ns/tables", "POST", {
                "name": "t", "schema": {"type": "struct", "schema-id": 0,
                                        "fields": [{"id": 1, "name": "id", "type": "long", "required": False}]}})
            assert status == 200, created
        for arn in arns:
            p = _arn_prefix(arn)
            bucket = arn.rsplit("/", 1)[-1]
            status, loaded = _iceberg_call(f"/iceberg/v1/{p}/namespaces/ns/tables/t")
            assert status == 200
            assert loaded["metadata-location"].startswith(f"s3://{bucket}/")
            assert _iceberg_call(f"/iceberg/v1/{p}/namespaces/ns/tables")[1]["identifiers"] == [
                {"namespace": ["ns"], "name": "t"}]
    finally:
        for arn in arns:
            with contextlib.suppress(Exception):
                s3tables.delete_table(tableBucketARN=arn, namespace="ns", name="t")
                s3tables.delete_namespace(tableBucketARN=arn, namespace="ns")
                s3tables.delete_table_bucket(tableBucketARN=arn)


def test_s3tables_iceberg_rest_sequence_numbers_and_requirements(s3tables):
    arn = s3tables.create_table_bucket(name=f"seq-{_uuid_mod.uuid4().hex[:8]}")["arn"]
    p = _arn_prefix(arn)
    table = f"/iceberg/v1/{p}/namespaces/ns/tables/t"
    try:
        _iceberg_call(f"/iceberg/v1/{p}/namespaces", "POST", {"namespace": ["ns"]})
        _iceberg_call(f"/iceberg/v1/{p}/namespaces/ns/tables", "POST", {
            "name": "t", "format-version": 2, "schema": {"type": "struct", "schema-id": 0, "fields": [
                {"id": 1, "name": "id", "type": "long", "required": False}]}})

        def append(seq, snap_id, parent, expected_ref):
            snapshot = {"snapshot-id": snap_id, "sequence-number": seq, "timestamp-ms": 1_700_000_000_000 + seq,
                        "manifest-list": f"s3://x/{snap_id}.avro", "summary": {"operation": "append"}}
            if parent is not None:
                snapshot["parent-snapshot-id"] = parent
            return _iceberg_call(table, "POST", {
                "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": expected_ref}],
                "updates": [{"action": "add-snapshot", "snapshot": snapshot},
                            {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch",
                             "snapshot-id": snap_id}]})

        for seq in (1, 2, 3):                      # sequential Spark appends
            status, body = append(seq, 100 + seq, (99 + seq) if seq > 1 else None, (99 + seq) if seq > 1 else None)
            assert status == 200, body
            assert body["metadata"]["last-sequence-number"] == seq
        assert _iceberg_call(table)[1]["metadata"]["last-sequence-number"] == 3

        # A writer that loaded the table before the last commit is refused.
        status, body = append(3, 200, 102, 102)
        assert status == 409
        assert body["error"]["type"] == "CommitFailedException"
        assert body["error"]["message"] == "Requirement failed: branch main has changed: expected id 102 != 103"

        # A snapshot not newer than the table's last sequence number is refused.
        status, body = _iceberg_call(table, "POST", {"updates": [{"action": "add-snapshot", "snapshot": {
            "snapshot-id": 300, "sequence-number": 3, "parent-snapshot-id": 103, "timestamp-ms": 1_800_000_000_000,
            "manifest-list": "s3://x/300.avro", "summary": {"operation": "append"}}}]})
        assert status == 400
        assert body["error"]["message"] == "Cannot add snapshot with sequence number 3 older than last sequence number 3"
    finally:
        with contextlib.suppress(Exception):
            s3tables.delete_table(tableBucketARN=arn, namespace="ns", name="t")
            s3tables.delete_namespace(tableBucketARN=arn, namespace="ns")
            s3tables.delete_table_bucket(tableBucketARN=arn)


def test_iceberg_route_splits_paths_as_the_router():
    """The router and the IAM checks read one split: raw segments, the {prefix} decoded only afterwards."""
    from ministack.services.s3tables import _iceberg_route, iceberg_lacks_prefix

    bucket = "arn%3Aaws%3As3tables%3Aus-east-1%3A000000000000%3Abucket%2Fb"
    assert _iceberg_route("GET", "/iceberg/v1/config") == ("config", [], [])
    assert _iceberg_route("GET", f"/iceberg/v1/{bucket}/namespaces/ns") == ("namespaces", [bucket], ["ns"])
    # a decoded ARN spans two segments; an encoded keyword is a segment like any other
    assert _iceberg_route("GET", "/iceberg/v1/arn:aws:s3tables:us-east-1:000000000000:bucket/b/namespaces") == (
        "namespaces", ["arn:aws:s3tables:us-east-1:000000000000:bucket", "b"], [])
    assert _iceberg_route("GET", f"/iceberg/v1/%6Eamespaces/{bucket}/namespaces") == (
        "namespaces", ["%6Eamespaces", bucket], [])
    assert _iceberg_route("GET", f"/iceberg/v1/{bucket}/%6Eamespaces") is None
    # tables/rename after a namespace is updateTable on a table named rename
    assert _iceberg_route("POST", f"/iceberg/v1/{bucket}/tables/rename") == ("rename", [bucket], [])
    assert _iceberg_route("POST", f"/iceberg/v1/{bucket}/namespaces/ns/tables/rename") == (
        "namespaces", [bucket], ["ns", "tables", "rename"])
    assert _iceberg_route("POST", f"/iceberg/v1/{bucket}/transactions/commit") == (
        "transactions", [bucket], ["commit"])
    for method, path in (("GET", "/iceberg/v1/namespaces"), ("GET", "/iceberg/v1/namespaces/ns/tables/t"),
                         ("POST", "/iceberg/v1/tables/rename"), ("POST", "/iceberg/v1/transactions/commit")):
        assert iceberg_lacks_prefix(method, path)
    # a first segment before "namespaces" is the {prefix}, whatever it reads
    for method, path in (("GET", "/iceberg/v1/config"), ("GET", f"/iceberg/v1/{bucket}/namespaces"),
                         ("POST", f"/iceberg/v1/{bucket}/tables/rename"), ("GET", "/iceberg/v1/tables/namespaces"),
                         ("GET", "/iceberg/v1/transactions/namespaces/ns"),
                         ("POST", f"/iceberg/v1/{bucket}/transactions/commit")):
        assert not iceberg_lacks_prefix(method, path)


def test_iceberg_iam_checks_name_the_bucket_the_prefix_names():
    """A bucket ARN or account:s3tablescatalog/bucket names a table bucket; any other {prefix} names none."""
    from ministack.services.s3tables import iceberg_iam_checks, iceberg_prefix_error

    bucket = "arn:aws:s3tables:us-east-1:000000000000:bucket/b"
    for prefix in (_arn_prefix(bucket), "000000000000:s3tablescatalog/b"):
        assert iceberg_iam_checks("GET", f"/iceberg/v1/{prefix}/namespaces", b"", {}) == [
            ("s3tables:ListNamespaces", bucket)]
        assert iceberg_prefix_error("GET", f"/iceberg/v1/{prefix}/namespaces") is None
    no_arn = "The specified table bucket ARN is not valid."
    for prefix, message in (("catalog", no_arn), ("%6Eamespaces/000000000000:s3tablescatalog/b", no_arn),
                            (_arn_prefix(f"{bucket}/x"), "The specified bucket name isn't valid. Specify a different "
                                                         "bucket name, and then try again.")):
        assert iceberg_iam_checks("POST", f"/iceberg/v1/{prefix}/namespaces", b"{}", {}) is None
        status, _, body = iceberg_prefix_error("POST", f"/iceberg/v1/{prefix}/namespaces")
        assert (status, json.loads(body)) == (400, {"error": {"message": message, "type": "bad_request", "code": 400}})
    # a rename body whose source is no object names no table
    assert iceberg_iam_checks("POST", f"/iceberg/v1/{_arn_prefix(bucket)}/tables/rename", b'{"source": "t"}', {}) == [
        ("s3tables:RenameTable", f"{bucket}/table/*")]


@pytest.fixture
def iceberg_auth_catalog(s3tables):
    """A table bucket with namespace ns and tables t and u, removed with whatever a test left in it."""
    name = f"irc-{_uuid_mod.uuid4().hex[:8]}"
    bucket = s3tables.create_table_bucket(name=name)["arn"]
    s3tables.create_namespace(tableBucketARN=bucket, namespace=["ns"])
    tables = {t: s3tables.create_table(tableBucketARN=bucket, namespace="ns", name=t, format="ICEBERG")["tableARN"]
              for t in ("t", "u")}
    yield {"bucket": bucket, "name": name, "prefix": _arn_prefix(bucket), **tables}
    for namespace in s3tables.list_namespaces(tableBucketARN=bucket)["namespaces"]:
        ns = namespace["namespace"][0]
        for table in s3tables.list_tables(tableBucketARN=bucket, namespace=ns)["tables"]:
            s3tables.delete_table(tableBucketARN=bucket, namespace=ns, name=table["name"])
        s3tables.delete_namespace(tableBucketARN=bucket, namespace=ns)
    s3tables.delete_table_bucket(tableBucketARN=bucket)


@contextlib.contextmanager
def _iceberg_contract_signer(user):
    """A second access key of an ``iam_user`` for signed Iceberg REST calls, deleted before the fixture's cleanup."""
    from botocore.credentials import Credentials
    from conftest import make_client

    iam = make_client("iam")
    key = iam.create_access_key(UserName=user.name)["AccessKey"]
    try:
        yield Credentials(key["AccessKeyId"], key["SecretAccessKey"])
    finally:
        iam.delete_access_key(UserName=user.name, AccessKeyId=key["AccessKeyId"])


def _iceberg_signed(credentials, method, path, query=None, body=None, service="s3tables", region="us-east-1",
                    presigned=False):
    """One SigV4-signed Iceberg REST call over HTTP: status, body (parsed when JSON, None when empty) and headers."""
    from botocore.auth import S3SigV4Auth, SigV4Auth, SigV4QueryAuth
    from botocore.awsrequest import AWSRequest

    request = AWSRequest(method=method, url=f"{_ENDPOINT}/iceberg/v1/{path}", params=query or {},
                         data=json.dumps(body).encode() if body is not None else None)
    if presigned:
        SigV4QueryAuth(credentials, service, region, expires=300).add_auth(request)
    else:
        (S3SigV4Auth if service == "s3" else SigV4Auth)(credentials, service, region).add_auth(request)
    prepared = request.prepare()
    try:
        with urllib.request.urlopen(urllib.request.Request(
                prepared.url, data=prepared.body, method=method, headers=dict(prepared.headers))) as response:
            status, content, headers = response.status, response.read(), response.headers
    except urllib.error.HTTPError as error:
        status, content, headers = error.code, error.read(), error.headers
    headers = {k.lower(): v for k, v in headers.items()}
    if content and "json" in headers.get("content-type", ""):
        content = json.loads(content)
    return status, content or None, headers


def _iceberg_fill(value, names):
    """A path, query or body template with {name} placeholders filled from ``names``."""
    text = json.dumps(value)
    for key, name in names.items():
        text = text.replace("{" + key + "}", name)
    return json.loads(text)


def _iceberg_denial(user, action, resource, reason=None):
    """The Iceberg error body of an s3tables identity-policy denial."""
    reason = reason or f"because no identity-based policy allows the s3tables:{action} action"
    return {"error": {"message": f"User: {user.arn} is not authorized to perform: s3tables:{action} "
                                 f"on resource: {resource} {reason}", "type": "forbidden", "code": 403}}


# Iceberg REST routes on the S3 Tables catalog: method, path, query, body, the actions AWS lists for each (on the
# table bucket or table), and MiniStack's status once they are allowed.
_UPDATE_TABLE_T = [("UpdateTableMetadataLocation", "{t}"), ("PutTableData", "{t}"), ("GetTableData", "{t}")]
_S3TABLES_ICEBERG_ROUTES = {
    "config": ("GET", "config", {"warehouse": "{bucket}"}, None, [("GetTableBucket", "{bucket}")], 200),
    "list-namespaces": ("GET", "{prefix}/namespaces", None, None, [("ListNamespaces", "{bucket}")], 200),
    "create-namespace": ("POST", "{prefix}/namespaces", None, {"namespace": ["{new}"]},
                         [("CreateNamespace", "{bucket}")], 200),
    "load-namespace": ("GET", "{prefix}/namespaces/ns", None, None, [("GetNamespace", "{bucket}")], 200),
    "drop-namespace": ("DELETE", "{prefix}/namespaces/ns", None, None, [("DeleteNamespace", "{bucket}")], 404),
    "list-tables": ("GET", "{prefix}/namespaces/ns/tables", None, None, [("ListTables", "{bucket}")], 200),
    "create-table": ("POST", "{prefix}/namespaces/ns/tables", None, {"name": "{new}"},
                     [("CreateTable", "{bucket}"), ("PutTableData", "{bucket}/table/*")], 200),
    "load-table": ("GET", "{prefix}/namespaces/ns/tables/t", None, None,
                   [("GetTableMetadataLocation", "{t}"), ("GetTableData", "{t}")], 200),
    "update-table": ("POST", "{prefix}/namespaces/ns/tables/t", None, {"requirements": [], "updates": []},
                     _UPDATE_TABLE_T, 200),
    "drop-table": ("DELETE", "{prefix}/namespaces/ns/tables/t", None, None, [("DeleteTable", "{t}")], 404),
    "table-exists": ("HEAD", "{prefix}/namespaces/ns/tables/t", None, None, [("GetTable", "{t}")], 200),
    "rename-table": ("POST", "{prefix}/tables/rename", None, {"source": {"namespace": ["ns"], "name": "t"},
                                                               "destination": {"namespace": ["ns"], "name": "{new}"}},
                     [("RenameTable", "{t}")], 404),
    "commit-transaction": ("POST", "{prefix}/transactions/commit", None, {"table-changes": [
        {"identifier": {"namespace": ["ns"], "name": "t"}, "requirements": [], "updates": []}]}, _UPDATE_TABLE_T, 200),
}


@pytest.mark.auth_contract
@pytest.mark.parametrize("service", ["s3tables", "glue", "s3", "sts"])
@pytest.mark.parametrize("route", list(_S3TABLES_ICEBERG_ROUTES))
def test_s3tables_iceberg_route_contract(auth_enabled, iam_user, iceberg_auth_catalog, route, service):
    """Each Iceberg route on a table bucket needs exactly its s3tables actions, whatever service it is signed for."""
    method, path, query, body, actions, served = _S3TABLES_ICEBERG_ROUTES[route]
    actions = _iceberg_fill(actions, iceberg_auth_catalog)
    allowed = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": f"s3tables:{action}", "Resource": resource} for action, resource in actions]})
    nobody = iam_user()
    with _iceberg_contract_signer(allowed) as signer, _iceberg_contract_signer(nobody) as refused_signer:
        calls = [_iceberg_signed(credentials, method, *_iceberg_fill([path, query, body], {
            **iceberg_auth_catalog, "new": f"by-{label}"}), service=service)
            for label, credentials in (("allowed", signer), ("nobody", refused_signer))]
    assert calls[0][0] == served
    if not auth_enabled:
        assert calls[1][0] == served
        return
    status, error, headers = calls[1]
    assert (status, headers["content-type"], headers["x-amzn-errortype"]) == (
        403, "application/json", "IrcForbiddenResponse")
    assert error == (None if method == "HEAD" else _iceberg_denial(nobody, *actions[0]))


@pytest.mark.auth_contract
@pytest.mark.parametrize("method,path,body,action,resource,reason", [
    ("GET", "config?warehouse={absent}", None, "GetTableBucket", "{absent}", None),
    ("POST", "{prefix}/namespaces", {"namespace": ["ns2"]}, "CreateNamespace", "{bucket}",
     "with an explicit deny in an identity-based policy"),
    ("GET", "{prefix}/namespaces/ns/tables/u", None, "GetTableMetadataLocation", "{u}", None),
    ("GET", "{prefix}/namespaces/ns/tables/absent", None, "GetTableMetadataLocation", "{bucket}/table/*", None),
], ids=["config-absent-bucket", "explicit-deny", "other-table", "absent-table"])
def test_s3tables_iceberg_authorization_contract(auth_enabled, iam_user, iceberg_auth_catalog,
                                                 method, path, body, action, resource, reason):
    """A denial names the bucket or table the call names, table/* for a table that does not exist, and a Deny."""
    catalog = iceberg_auth_catalog
    user = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Resource": catalog["bucket"],
         "Action": ["s3tables:GetTableBucket", "s3tables:ListNamespaces", "s3tables:GetNamespace"]},
        {"Effect": "Allow", "Action": "s3tables:GetTableMetadataLocation", "Resource": catalog["t"]},
        {"Effect": "Deny", "Action": "s3tables:CreateNamespace", "Resource": catalog["bucket"]},
    ]})
    names = {**catalog, "absent": catalog["bucket"].rsplit("/", 1)[0] + "/absent"}
    path, query = _iceberg_fill(path, names), None
    if "?warehouse=" in path:
        path, warehouse = path.split("?warehouse=")
        query = {"warehouse": warehouse}
    with _iceberg_contract_signer(user) as signer:
        assert _iceberg_signed(signer, "GET", "config", {"warehouse": catalog["bucket"]})[0] == 200
        assert _iceberg_signed(signer, "GET", f"{catalog['prefix']}/namespaces")[1] == {"namespaces": [["ns"]]}
        status, error, headers = _iceberg_signed(signer, method, path, query, body)
    if not auth_enabled:
        assert status != 403
        return
    assert (status, headers["content-type"]) == (403, "application/json")
    assert error == _iceberg_denial(user, action, _iceberg_fill(resource, names), reason)


@pytest.mark.auth_contract
def test_s3tables_iceberg_later_action_contract(auth_enabled, iam_user, iceberg_auth_catalog):
    """loadTable's second action answers Access Denied."""
    catalog = iceberg_auth_catalog
    user = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3tables:GetTableMetadataLocation", "Resource": catalog["t"]}]})
    with _iceberg_contract_signer(user) as signer:
        loaded = _iceberg_signed(signer, "GET", f"{catalog['prefix']}/namespaces/ns/tables/t")
    if not auth_enabled:
        assert loaded[0] == 200
        return
    assert loaded[:2] == (403, {"error": {"message": "Access Denied", "type": "forbidden", "code": 403}})


@pytest.mark.auth_contract
@pytest.mark.parametrize("service", ["s3tables", "glue"])
def test_s3tables_iceberg_wrong_secret_contract(auth_enabled, iam_user, iceberg_auth_catalog, service):
    """Iceberg REST calls check their signature, for S3 Tables and for the Glue catalog alike, in JSON."""
    from botocore.credentials import Credentials

    catalog = iceberg_auth_catalog
    warehouse = catalog["bucket"] if service == "s3tables" else "000000000000"
    action = "s3tables:GetTableBucket" if service == "s3tables" else "glue:GetCatalog"
    user = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": action, "Resource": "*"}]})
    with _iceberg_contract_signer(user) as signer:
        assert _iceberg_signed(signer, "GET", "config", {"warehouse": warehouse}, service=service)[0] == 200
        status, _, headers = _iceberg_signed(Credentials(signer.access_key, "wrong-secret"), "GET", "config",
                                             {"warehouse": warehouse}, service=service)
    if not auth_enabled:
        assert status == 200
        return
    assert (status, headers["x-amzn-errortype"], headers["content-type"]) == (
        403, "InvalidSignatureException", "application/json")


@pytest.mark.auth_contract
def test_s3tables_iceberg_calls_naming_no_table_bucket_contract(auth_enabled, iam_user):
    """An unprefixed route answers 404 whatever its signature; config without a table bucket answers 400."""
    from botocore.credentials import Credentials

    with _iceberg_contract_signer(iam_user()) as signer:
        assert _iceberg_signed(signer, "GET", "config")[0] == 400
        wrong = Credentials(signer.access_key, "wrong-secret")
        assert _iceberg_signed(wrong, "GET", "namespaces")[0] == 404
        assert _iceberg_signed(wrong, "HEAD", "namespaces/ns")[0] == 404
        status, _, headers = _iceberg_signed(wrong, "GET", "config")
        read_as_prefix = _iceberg_signed(wrong, "GET", "tables/namespaces", service="glue")
    assert (status, headers.get("x-amzn-errortype")) == (
        (403, "InvalidSignatureException") if auth_enabled else (400, None))
    # "tables" before "namespaces" is the {prefix}, so that call is authenticated however it is signed
    assert read_as_prefix[0] == (403 if auth_enabled else 200)


@pytest.mark.auth_contract
@pytest.mark.parametrize("method,prefix,message,served", [
    ("GET", "somewhere", "The specified table bucket ARN is not valid.", [200, 200, 200]),
    ("POST", "%256Eamespaces/000000000000:s3tablescatalog/{name}", "The specified table bucket ARN is not valid.",
     [200, 409, 409]),
    ("POST", "{bucket}/x", "The specified bucket name isn't valid. Specify a different bucket name, and then try "
                           "again.", [404, 404, 404]),
], ids=["list-namespaces", "encoded-namespaces-segment", "bucket-arn-and-more"])
def test_s3tables_iceberg_prefix_naming_no_table_bucket_contract(auth_enabled, iam_user, iceberg_auth_catalog,
                                                                 s3tables, method, prefix, message, served):
    """A {prefix} that is no table bucket answers 400 after the credential check, before any policy, as on AWS."""
    from botocore.credentials import Credentials

    catalog = iceberg_auth_catalog
    prefix = _iceberg_fill(prefix, catalog)
    prefix = _arn_prefix(prefix) if prefix.startswith("arn:") else prefix
    everything = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3tables:*", "Resource": "*"}]})
    body = {"namespace": ["created"]} if method == "POST" else None
    with _iceberg_contract_signer(iam_user()) as nobody, _iceberg_contract_signer(everything) as signer:
        calls = [_iceberg_signed(credentials, method, f"{prefix}/namespaces", body=body)
                 for credentials in (nobody, signer, Credentials(signer.access_key, "wrong-secret"))]
    if not auth_enabled:
        assert [status for status, _, _ in calls] == served
        return
    for status, error, _ in calls[:2]:
        assert (status, error) == (400, {"error": {"message": message, "type": "bad_request", "code": 400}})
    assert (calls[2][0], calls[2][2]["x-amzn-errortype"]) == (403, "InvalidSignatureException")
    assert [n["namespace"] for n in s3tables.list_namespaces(tableBucketARN=catalog["bucket"])["namespaces"]] == [
        ["ns"]]


@pytest.mark.auth_contract
@pytest.mark.parametrize("prefix", ["{prefix}/%6Eamespaces", "000000000000:s3tablescatalog/{name}/namespaces"],
                         ids=["encoded-namespaces-keyword", "account-catalog-prefix"])
def test_s3tables_iceberg_prefix_forms_served_are_authorized_contract(auth_enabled, iam_user, iceberg_auth_catalog,
                                                                     prefix):
    """A path MiniStack serves as listNamespaces is authorized as one, whichever form names the bucket."""
    catalog = iceberg_auth_catalog
    path = _iceberg_fill(prefix, catalog)
    allowed = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3tables:ListNamespaces", "Resource": catalog["bucket"]}]})
    nobody = iam_user()
    with _iceberg_contract_signer(allowed) as signer, _iceberg_contract_signer(nobody) as refused_signer:
        listed = _iceberg_signed(signer, "GET", path)
        refused = _iceberg_signed(refused_signer, "GET", path)
    assert listed[:2] == (200, {"namespaces": [["ns"]]})
    if not auth_enabled:
        assert refused[:2] == listed[:2]
        return
    assert refused[:2] == (403, _iceberg_denial(nobody, "ListNamespaces", catalog["bucket"]))


@pytest.mark.auth_contract
def test_s3tables_iceberg_presigned_region_contract(auth_enabled, iam_user, iceberg_auth_catalog):
    """Under AUTH a presigned call serves its credential region, whose bucket and table the checks read."""
    catalog = iceberg_auth_catalog
    west = _make_s3tables_client(region_name="us-west-2")
    west_arn = west.create_table_bucket(name=catalog["name"])["arn"]
    west.create_namespace(tableBucketARN=west_arn, namespace=["ns"])
    secret = west.create_table(tableBucketARN=west_arn, namespace="ns", name="secret", format="ICEBERG")["tableARN"]
    reads = ["s3tables:GetTableMetadataLocation", "s3tables:GetTableData"]
    user = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3tables:ListNamespaces", "Resource": catalog["bucket"]},
        {"Effect": "Allow", "Action": reads, "Resource": "*"},
        {"Effect": "Deny", "Action": reads, "Resource": secret}]})
    by_name = f"000000000000:s3tablescatalog/{catalog['name']}/namespaces"
    table = f"{_arn_prefix(west_arn)}/namespaces/ns/tables/secret"
    try:
        with _iceberg_contract_signer(user) as signer:
            calls = {(region, kind): _iceberg_signed(signer, "GET", path, region=region, presigned=True)
                     for region in ("us-east-1", "us-west-2") for kind, path in (("list", by_name), ("table", table))}
    finally:
        west.delete_table(tableBucketARN=west_arn, namespace="ns", name="secret")
        west.delete_namespace(tableBucketARN=west_arn, namespace="ns")
        west.delete_table_bucket(tableBucketARN=west_arn)
    if not auth_enabled:
        assert [status for status, _, _ in calls.values()] == [200, 200, 200, 200]
        return
    # signed for us-east-1, neither call reaches the us-west-2 bucket
    assert calls["us-east-1", "list"][:2] == (200, {"namespaces": [["ns"]]})
    assert calls["us-east-1", "table"][0] == 404
    assert calls["us-west-2", "list"][:2] == (403, _iceberg_denial(user, "ListNamespaces", west_arn))
    assert calls["us-west-2", "table"][:2] == (403, _iceberg_denial(
        user, "GetTableMetadataLocation", secret, "with an explicit deny in an identity-based policy"))


@pytest.mark.auth_contract
@pytest.mark.parametrize("warehouse", ["1:s3tablescatalog/{name}", "000000000000:s3tablescatalog/{name}/x"],
                         ids=["short-account", "trailing-segment"])
def test_s3tables_iceberg_config_catalog_warehouse_contract(auth_enabled, iam_user, iceberg_auth_catalog, warehouse):
    """getConfig serves any account:s3tablescatalog/ warehouse; one naming no bucket needs GetTableBucket on *."""
    warehouse = _iceberg_fill(warehouse, iceberg_auth_catalog)
    nobody = iam_user()
    allowed = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3tables:GetTableBucket", "Resource": "*"}]})
    with _iceberg_contract_signer(allowed) as signer, _iceberg_contract_signer(nobody) as refused_signer:
        served = _iceberg_signed(signer, "GET", "config", {"warehouse": warehouse})
        refused = _iceberg_signed(refused_signer, "GET", "config", {"warehouse": warehouse})
    assert served[0] == 200
    if not auth_enabled:
        assert refused[0] == 200
        return
    assert refused[:2] == (403, _iceberg_denial(nobody, "GetTableBucket", "*"))


@pytest.mark.auth_contract
def test_s3tables_iceberg_table_named_rename_contract(auth_enabled, iam_user, iceberg_auth_catalog, s3tables):
    """A commit to a table named rename is updateTable on that table, not renameTable."""
    catalog = iceberg_auth_catalog
    table = s3tables.create_table(tableBucketARN=catalog["bucket"], namespace="ns", name="rename",
                                  format="ICEBERG")["tableARN"]
    path = f"{catalog['prefix']}/namespaces/ns/tables/rename"
    renamer = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": "s3tables:RenameTable", "Resource": "*"}]})
    updater = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": f"s3tables:{action}", "Resource": table} for action, _ in _UPDATE_TABLE_T]})
    commit = {"requirements": [], "updates": []}
    with _iceberg_contract_signer(renamer) as refused_signer, _iceberg_contract_signer(updater) as signer:
        assert _iceberg_signed(signer, "POST", path, body=commit)[0] == 200
        refused = _iceberg_signed(refused_signer, "POST", path, body=commit)
    if not auth_enabled:
        assert refused[0] == 200
        return
    assert refused[:2] == (403, _iceberg_denial(renamer, "UpdateTableMetadataLocation", table))


@pytest.mark.auth_contract
@pytest.mark.parametrize("body", ["t", "t-and-u", "no-changes", "no-object"])
def test_s3tables_iceberg_transaction_commit_contract(auth_enabled, iam_user, iceberg_auth_catalog, body):
    """transactions/commit needs updateTable's actions on each table it changes, on * when it names none."""
    catalog = iceberg_auth_catalog
    change = {"identifier": {"namespace": ["ns"], "name": "t"}, "requirements": [], "updates": []}
    bodies = {"t": {"table-changes": [change]},
              "t-and-u": {"table-changes": [change, {**change, "identifier": {"namespace": ["ns"], "name": "u"}}]},
              "no-changes": {}, "no-object": "table-changes"}
    allowed = iam_user(policy={"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Action": f"s3tables:{action}", "Resource": catalog["t"]}
        for action, _ in _UPDATE_TABLE_T]})
    nobody = iam_user()
    path = f"{catalog['prefix']}/transactions/commit"
    with _iceberg_contract_signer(allowed) as signer, _iceberg_contract_signer(nobody) as refused_signer:
        committed = _iceberg_signed(signer, "POST", path, body=bodies[body])
        refused = _iceberg_signed(refused_signer, "POST", path, body=bodies[body])
    if not auth_enabled:
        assert (committed[0], refused[0]) == ((500, 500) if body == "no-object" else (200, 200))
        return
    first = "*" if body in ("no-changes", "no-object") else catalog["t"]
    assert refused[:2] == (403, _iceberg_denial(nobody, "UpdateTableMetadataLocation", first))
    if body == "t":
        assert committed[0] == 200
    elif body == "t-and-u":
        assert committed[:2] == (403, {"error": {"message": "Access Denied", "type": "forbidden", "code": 403}})
    else:
        assert committed[:2] == (403, _iceberg_denial(allowed, "UpdateTableMetadataLocation", "*"))


def test_s3tables_bucket_subresources_defaults_and_updates(s3tables):
    """Documented defaults until set: SSE-S3, STANDARD, unreferenced file removal 3/10 days."""
    arn = s3tables.create_table_bucket(name=f"sub-{_uuid_mod.uuid4().hex[:8]}",
                                       tags={"team": "data"})["arn"]
    got = s3tables.get_table_bucket(tableBucketARN=arn)
    assert set(got) - {"ResponseMetadata"} <= {"arn", "name", "ownerAccountId", "createdAt", "tableBucketId", "type"}
    assert got["type"] == "customer" and got["tableBucketId"]

    assert s3tables.get_table_bucket_encryption(tableBucketARN=arn)["encryptionConfiguration"] == {
        "sseAlgorithm": "AES256"}
    s3tables.put_table_bucket_encryption(tableBucketARN=arn, encryptionConfiguration={
        "sseAlgorithm": "aws:kms", "kmsKeyArn": "arn:aws:kms:us-east-1:000000000000:key/k"})
    assert s3tables.get_table_bucket_encryption(tableBucketARN=arn)["encryptionConfiguration"]["sseAlgorithm"] == "aws:kms"
    s3tables.delete_table_bucket_encryption(tableBucketARN=arn)
    assert s3tables.get_table_bucket_encryption(tableBucketARN=arn)["encryptionConfiguration"] == {
        "sseAlgorithm": "AES256"}

    removal = s3tables.get_table_bucket_maintenance_configuration(tableBucketARN=arn)["configuration"][
        "icebergUnreferencedFileRemoval"]
    assert removal == {"status": "enabled", "settings": {"icebergUnreferencedFileRemoval": {
        "unreferencedDays": 3, "nonCurrentDays": 10}}}
    s3tables.put_table_bucket_maintenance_configuration(
        tableBucketARN=arn, type="icebergUnreferencedFileRemoval",
        value={"status": "enabled", "settings": {"icebergUnreferencedFileRemoval": {"unreferencedDays": 4}}})
    removal = s3tables.get_table_bucket_maintenance_configuration(tableBucketARN=arn)["configuration"][
        "icebergUnreferencedFileRemoval"]
    assert removal["settings"]["icebergUnreferencedFileRemoval"] == {"unreferencedDays": 4, "nonCurrentDays": 10}

    with pytest.raises(ClientError) as exc:
        s3tables.get_table_bucket_policy(tableBucketARN=arn)
    assert exc.value.response["Error"]["Code"] == "NotFoundException"
    s3tables.put_table_bucket_policy(tableBucketARN=arn, resourcePolicy='{"Version":"2012-10-17","Statement":[]}')
    assert s3tables.get_table_bucket_policy(tableBucketARN=arn)["resourcePolicy"].startswith('{"Version"')
    s3tables.delete_table_bucket_policy(tableBucketARN=arn)

    assert s3tables.get_table_bucket_storage_class(tableBucketARN=arn)["storageClassConfiguration"] == {
        "storageClass": "STANDARD"}
    s3tables.put_table_bucket_storage_class(tableBucketARN=arn,
                                            storageClassConfiguration={"storageClass": "INTELLIGENT_TIERING"})
    assert s3tables.get_table_bucket_storage_class(tableBucketARN=arn)["storageClassConfiguration"][
        "storageClass"] == "INTELLIGENT_TIERING"

    s3tables.put_table_bucket_metrics_configuration(tableBucketARN=arn)
    assert s3tables.get_table_bucket_metrics_configuration(tableBucketARN=arn)["id"]
    s3tables.delete_table_bucket_metrics_configuration(tableBucketARN=arn)

    assert s3tables.list_tags_for_resource(resourceArn=arn)["tags"] == {"team": "data"}
    s3tables.tag_resource(resourceArn=arn, tags={"env": "dev"})
    s3tables.untag_resource(resourceArn=arn, tagKeys=["team"])
    assert s3tables.list_tags_for_resource(resourceArn=arn)["tags"] == {"env": "dev"}


def test_s3tables_table_subresources_and_rename(s3tables):
    arn = s3tables.create_table_bucket(name=f"tsub-{_uuid_mod.uuid4().hex[:8]}")["arn"]
    for ns in ("a", "b"):
        s3tables.create_namespace(tableBucketARN=arn, namespace=[ns])
    created = s3tables.create_table(tableBucketARN=arn, namespace="a", name="t", format="ICEBERG",
                                    tags={"k": "v"})
    table = s3tables.get_table(tableBucketARN=arn, namespace="a", name="t")
    assert table["type"] == "customer" and table["versionToken"] == created["versionToken"]
    assert "tableBucketARN" not in table

    config = s3tables.get_table_maintenance_configuration(tableBucketARN=arn, namespace="a", name="t")["configuration"]
    assert config["icebergCompaction"] == {"status": "enabled", "settings": {"icebergCompaction": {
        "targetFileSizeMB": 512, "strategy": "auto"}}}
    assert config["icebergSnapshotManagement"] == {"status": "enabled", "settings": {"icebergSnapshotManagement": {
        "minSnapshotsToKeep": 1, "maxSnapshotAgeHours": 120}}}
    s3tables.put_table_maintenance_configuration(
        tableBucketARN=arn, namespace="a", name="t", type="icebergCompaction",
        value={"status": "disabled", "settings": {"icebergCompaction": {"targetFileSizeMB": 256}}})
    compaction = s3tables.get_table_maintenance_configuration(
        tableBucketARN=arn, namespace="a", name="t")["configuration"]["icebergCompaction"]
    assert compaction["status"] == "disabled"
    assert compaction["settings"]["icebergCompaction"]["targetFileSizeMB"] == 256

    status = s3tables.get_table_maintenance_job_status(tableBucketARN=arn, namespace="a", name="t")["status"]
    assert {v["status"] for v in status.values()} == {"Not_Yet_Run"}
    assert s3tables.get_table_encryption(tableBucketARN=arn, namespace="a", name="t")[
        "encryptionConfiguration"] == {"sseAlgorithm": "AES256"}
    assert s3tables.get_table_storage_class(tableBucketARN=arn, namespace="a", name="t")[
        "storageClassConfiguration"] == {"storageClass": "STANDARD"}

    with pytest.raises(ClientError) as exc:
        s3tables.get_table_policy(tableBucketARN=arn, namespace="a", name="t")
    assert exc.value.response["Error"]["Code"] == "NotFoundException"
    s3tables.put_table_policy(tableBucketARN=arn, namespace="a", name="t", resourcePolicy="{}")
    assert s3tables.get_table_policy(tableBucketARN=arn, namespace="a", name="t")["resourcePolicy"] == "{}"

    assert s3tables.list_tags_for_resource(resourceArn=created["tableARN"])["tags"] == {"k": "v"}

    s3tables.rename_table(tableBucketARN=arn, namespace="a", name="t", newNamespaceName="b", newName="t2")
    renamed = s3tables.get_table(tableBucketARN=arn, namespace="b", name="t2")
    assert renamed["tableARN"] == created["tableARN"] and renamed["namespace"] == ["b"]
    with pytest.raises(ClientError) as exc:
        s3tables.get_table(tableBucketARN=arn, namespace="a", name="t")
    assert exc.value.response["Error"]["Code"] == "NotFoundException"


def test_s3tables_create_table_keeps_iceberg_types(s3tables):
    bucket = s3tables.create_table_bucket(name=f"types-{_uuid_mod.uuid4().hex[:8]}")["arn"]
    s3tables.create_namespace(tableBucketARN=bucket, namespace=["ns"])
    primitives = ["decimal(19, 2)", "decimal(19,2)", "timestamp", "timestamptz", "time", "uuid", "fixed[16]", "binary",
                  "STRING"]
    stored = ["decimal(19, 2)", "decimal(19, 2)", "timestamp", "timestamptz", "time", "uuid", "fixed[16]", "binary",
              "string"]
    nested = {"type": "list", "element-id": 3, "element": "string", "element-required": False}
    try:
        s3tables.create_table(
            tableBucketARN=bucket, namespace="ns", name="flat", format="ICEBERG",
            metadata={"iceberg": {"schema": {"fields": [
                {"name": f"c{i}", "type": t, "required": False} for i, t in enumerate(primitives)]}}},
        )
        s3tables.create_table(
            tableBucketARN=bucket, namespace="ns", name="nested", format="ICEBERG",
            metadata={"iceberg": {"schemaV2": {"type": "struct", "fields": [
                {"id": 1, "name": "amount", "type": "decimal(9,2)", "required": True},
                {"id": 2, "name": "tags", "type": nested, "required": False}]}}},
        )
        prefix = _arn_prefix(bucket)
        flat = _iceberg_json(f"/iceberg/v1/{prefix}/namespaces/ns/tables/flat")["metadata"]
        assert [f["type"] for f in flat["schemas"][0]["fields"]] == stored
        fields = _iceberg_json(f"/iceberg/v1/{prefix}/namespaces/ns/tables/nested")["metadata"]["schemas"][0]["fields"]
        assert [f["type"] for f in fields] == ["decimal(9, 2)", nested]
    finally:
        for name in ("flat", "nested"):
            with contextlib.suppress(ClientError):
                s3tables.delete_table(tableBucketARN=bucket, namespace="ns", name=name)
        s3tables.delete_namespace(tableBucketARN=bucket, namespace="ns")
        s3tables.delete_table_bucket(tableBucketARN=bucket)


@pytest.mark.parametrize("kind", ["timestamp_ns", "varchar", "decimal(x,2)"])
def test_s3tables_create_table_refuses_an_invalid_iceberg_type(s3tables, kind):
    bucket = s3tables.create_table_bucket(name=f"badtype-{_uuid_mod.uuid4().hex[:8]}")["arn"]
    s3tables.create_namespace(tableBucketARN=bucket, namespace=["ns"])
    try:
        with pytest.raises(ClientError) as exc:
            s3tables.create_table(
                tableBucketARN=bucket, namespace="ns", name="t", format="ICEBERG",
                metadata={"iceberg": {"schema": {"fields": [{"name": "c", "type": kind}]}}},
            )
        error = exc.value.response
        assert error["ResponseMetadata"]["HTTPStatusCode"] == 400
        assert (error["Error"]["Code"], error["Error"]["Message"]) == (
            "BadRequestException", "The specified metadata is not valid")
        assert s3tables.list_tables(tableBucketARN=bucket)["tables"] == []
    finally:
        s3tables.delete_namespace(tableBucketARN=bucket, namespace="ns")
        s3tables.delete_table_bucket(tableBucketARN=bucket)
