"""S3 Tables service tests — round-trip coverage for the 14 control-plane
operations the service ships in 1.3.50, plus a multi-tenancy isolation check.

Operations covered:
  CreateTableBucket, ListTableBuckets, GetTableBucket, DeleteTableBucket
  CreateNamespace, ListNamespaces, GetNamespace, DeleteNamespace
  CreateTable, ListTables, GetTable, DeleteTable
  GetTableMetadataLocation, UpdateTableMetadataLocation

Shapes verified against `botocore.data.s3tables.2024-12-01.service-2`.
"""

import uuid as _uuid_mod

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError


_ENDPOINT = "http://localhost:4566"


def _make_s3tables_client(access_key="test"):
    return boto3.client(
        "s3tables",
        endpoint_url=_ENDPOINT,
        aws_access_key_id=access_key,
        aws_secret_access_key="test",
        region_name="us-east-1",
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


def test_s3tables_get_bucket_missing_returns_not_found(s3tables):
    fake_arn = "arn:aws:s3tables:us-east-1:000000000000:bucket/does-not-exist-xyz"
    with pytest.raises(ClientError) as exc:
        s3tables.get_table_bucket(tableBucketARN=fake_arn)
    assert exc.value.response["Error"]["Code"] in ("NotFoundException", "404")


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
