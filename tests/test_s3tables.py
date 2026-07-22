"""S3 Tables service tests — round-trip coverage for the 14 control-plane
operations the service ships in 1.3.50, plus a multi-tenancy isolation check.

Operations covered:
  CreateTableBucket, ListTableBuckets, GetTableBucket, DeleteTableBucket
  CreateNamespace, ListNamespaces, GetNamespace, DeleteNamespace
  CreateTable, ListTables, GetTable, DeleteTable
  GetTableMetadataLocation, UpdateTableMetadataLocation

Shapes verified against `botocore.data.s3tables.2024-12-01.service-2`.
"""

import json
import os
import urllib.error
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


def test_s3tables_iceberg_catalog_no_prefix_url_format(s3tables):
    """S3 Tables uses /iceberg/v1/namespaces/... (no catalog prefix in path,
    warehouse in query param) — the format DuckDB sends with ENDPOINT_TYPE s3_tables
    or an explicit ENDPOINT pointing at the catalog root."""
    bucket_name = f"tb-noprefix-{_uuid_mod.uuid4().hex[:6]}"
    bucket_arn = s3tables.create_table_bucket(name=bucket_name)["arn"]
    ns = f"ns_{_uuid_mod.uuid4().hex[:6]}"
    table = f"t_{_uuid_mod.uuid4().hex[:6]}"
    try:
        s3tables.create_namespace(tableBucketARN=bucket_arn, namespace=[ns])
        s3tables.create_table(
            tableBucketARN=bucket_arn,
            namespace=ns,
            name=table,
            format="ICEBERG",
            metadata={"iceberg": {"schema": {"fields": [{"name": "id", "type": "long"}]}}},
        )

        # List namespaces — no prefix
        resp = _iceberg_json("/iceberg/v1/namespaces")
        ns_names = [
            (n[0] if isinstance(n, list) else n)
            for n in resp.get("namespaces", [])
        ]
        assert ns in ns_names

        # List tables — no prefix
        resp = _iceberg_json(f"/iceberg/v1/namespaces/{ns}/tables")
        assert {"namespace": [ns], "name": table} in resp.get("identifiers", [])

        # Load table — no prefix
        resp = _iceberg_json(f"/iceberg/v1/namespaces/{ns}/tables/{table}")
        assert resp.get("metadata-location", "").startswith(f"s3://{bucket_name}/{ns}/{table}/")
        assert resp.get("metadata", {}).get("table-uuid")
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


def test_s3tables_iceberg_config_returns_s3_defaults():
    """GET /iceberg/v1/config must return S3 connection defaults so clients
    don't need out-of-band credential / endpoint configuration."""
    resp = _iceberg_json("/iceberg/v1/config")
    defaults = resp.get("defaults", {})
    assert "s3.endpoint" in defaults, "s3.endpoint missing from config defaults"
    assert "s3.access-key-id" in defaults, "s3.access-key-id missing from config defaults"
    assert "s3.secret-access-key" in defaults, "s3.secret-access-key missing from config defaults"
    assert "s3.path-style-access" in defaults, "s3.path-style-access missing from config defaults"
    assert "client.region" in defaults, "client.region missing from config defaults"


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

        resp = _iceberg_json(f"/iceberg/v1/namespaces/{ns}/tables/{table}")
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
            "/iceberg/v1/transactions/commit",
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
        loaded = _iceberg_json(f"/iceberg/v1/namespaces/{ns}/tables/{table}")
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
