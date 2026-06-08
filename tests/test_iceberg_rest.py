"""Unit tests for the Iceberg REST catalog emulator (services/iceberg_rest.py).

These exercise the dispatcher directly with seeded state in the underlying
glue / s3tables / s3 modules, without booting a full ASGI server. The
production code path is identical — `handle_request` is what the gateway
calls — so we get full coverage of the four read-path endpoints + the
HEAD/TableExists + the fall-through error cases.
"""

import asyncio
import json

import pytest

import ministack.core.responses as _responses
from ministack.services import glue, iceberg_rest, s3, s3tables


def _call(method, path, query_params=None):
    status, headers, body = asyncio.run(
        iceberg_rest.handle_request(method, path, {}, b"", query_params or {})
    )
    payload = json.loads(body) if body else None
    return status, headers, payload


@pytest.fixture(autouse=True)
def _reset():
    # Pin the per-request account so AccountScopedDict lookups land in the
    # same bucket whether the caller is the test or production code.
    _responses._request_account_id.set("000000000000")
    _responses._request_region.set("us-east-1")
    glue.reset()
    s3tables.reset()
    # `s3.reset` doesn't exist; clear the in-memory bucket dict directly.
    s3._buckets.clear()
    yield
    glue.reset()
    s3tables.reset()
    s3._buckets.clear()


# ── /v1/config ────────────────────────────────────────────────


def test_config_echoes_warehouse_as_prefix_and_emits_s3_overrides():
    status, _, payload = _call("GET", "/iceberg/v1/config", {"warehouse": ["000000000000"]})
    assert status == 200
    assert payload["defaults"]["prefix"] == "000000000000"
    overrides = payload["overrides"]
    # DuckDB / pyiceberg use these to reach MiniStack's S3 for data files —
    # without them, the client falls back to ambient AWS S3 creds and fails.
    assert overrides["s3.endpoint"].startswith("http://")
    assert overrides["s3.path-style-access"] == "true"
    for required in ("s3.access-key-id", "s3.secret-access-key", "s3.region"):
        assert required in overrides


def test_config_without_warehouse_returns_empty_defaults():
    _, _, payload = _call("GET", "/iceberg/v1/config")
    assert payload["defaults"] == {}
    assert "overrides" in payload


# ── /v1/{prefix}/namespaces ───────────────────────────────────


def test_list_namespaces_unions_glue_and_s3tables():
    glue._databases["db_glue"] = {"Name": "db_glue"}
    s3tables._namespaces["ns_s3tables"] = {"namespace": ["ns_s3tables"]}

    _, _, payload = _call("GET", "/iceberg/v1/000000000000/namespaces")
    flattened = sorted(ns[0] for ns in payload["namespaces"])
    assert flattened == ["db_glue", "ns_s3tables"]


def test_get_namespace_stub_returns_minimal_shape():
    _, _, payload = _call(
        "GET", "/iceberg/v1/000000000000/namespaces/anything"
    )
    assert payload == {"namespace": ["anything"], "properties": {}}


# ── /v1/{prefix}/namespaces/{ns}/tables ───────────────────────


def test_list_tables_hides_non_iceberg_glue_tables():
    """A Glue table without ``Parameters['metadata_location']`` is a plain
    Hive/CSV/Parquet table and MUST NOT appear in the Iceberg REST listing,
    or DuckDB will try to load it as Iceberg and explode."""
    glue._databases["db"] = {"Name": "db"}
    glue._tables["db/iceberg_table"] = {
        "Name": "iceberg_table",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": "s3://lake/iceberg_table/metadata/v0.metadata.json"},
    }
    glue._tables["db/csv_table"] = {
        "Name": "csv_table",
        "DatabaseName": "db",
        "Parameters": {},  # no metadata_location
    }

    _, _, payload = _call("GET", "/iceberg/v1/000000000000/namespaces/db/tables")
    names = sorted(t["name"] for t in payload["identifiers"])
    assert names == ["iceberg_table"]


def test_list_tables_unions_glue_and_s3tables():
    glue._databases["shared_ns"] = {"Name": "shared_ns"}
    glue._tables["shared_ns/glue_iceberg"] = {
        "Name": "glue_iceberg",
        "DatabaseName": "shared_ns",
        "Parameters": {"metadata_location": "s3://lake/glue_iceberg/metadata/v0.metadata.json"},
    }
    s3tables._tables["bucket\x00shared_ns\x00s3t_table"] = {
        "name": "s3t_table", "namespace": ["shared_ns"],
    }

    _, _, payload = _call("GET", "/iceberg/v1/000000000000/namespaces/shared_ns/tables")
    names = sorted(t["name"] for t in payload["identifiers"])
    assert names == ["glue_iceberg", "s3t_table"]


# ── LoadTable hot path ───────────────────────────────────────


_META_JSON = {
    "format-version": 2,
    "table-uuid": "11111111-2222-3333-4444-555555555555",
    "location": "s3://lake/dim_application",
    "schemas": [{"schema-id": 0, "type": "struct", "fields": []}],
    "current-schema-id": 0,
    "current-snapshot-id": 42,
    "properties": {},
}


def _seed_glue_table_with_metadata(metadata_dict=None):
    """Seed a Glue Iceberg table + its metadata.json on internal S3."""
    metadata_dict = metadata_dict if metadata_dict is not None else _META_JSON
    key = "dim_application/metadata/00003-deadbeef.metadata.json"
    s3._buckets["lake"] = {"objects": {key: {"body": json.dumps(metadata_dict).encode()}}}
    glue._databases["db"] = {"Name": "db"}
    glue._tables["db/dim_application"] = {
        "Name": "dim_application",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": f"s3://lake/{key}"},
    }


def test_load_table_glue_path_inlines_metadata_verbatim():
    _seed_glue_table_with_metadata()
    status, _, payload = _call(
        "GET", "/iceberg/v1/000000000000/namespaces/db/tables/dim_application"
    )
    assert status == 200
    assert payload["metadata-location"].endswith("/00003-deadbeef.metadata.json")
    assert payload["metadata"] == _META_JSON  # passthrough — no transformation
    assert "s3.endpoint" in payload["config"]


def test_load_table_404s_when_metadata_object_missing():
    """Glue table exists and points at an S3 URI, but the metadata.json
    object isn't there. Returning 200 with metadata={} would let DuckDB
    treat it as a real-but-empty table and silently produce wrong results."""
    glue._databases["db"] = {"Name": "db"}
    glue._tables["db/orphan"] = {
        "Name": "orphan",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": "s3://lake/orphan/metadata/v0.metadata.json"},
    }
    status, _, payload = _call(
        "GET", "/iceberg/v1/000000000000/namespaces/db/tables/orphan"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchTableException"
    assert payload["error"]["code"] == 404


def test_load_table_404s_when_metadata_json_unparseable():
    key = "broken/metadata/v0.metadata.json"
    s3._buckets["lake"] = {"objects": {key: {"body": b"<<not json>>"}}}
    glue._databases["db"] = {"Name": "db"}
    glue._tables["db/broken"] = {
        "Name": "broken",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": f"s3://lake/{key}"},
    }
    status, _, payload = _call(
        "GET", "/iceberg/v1/000000000000/namespaces/db/tables/broken"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchTableException"


def test_load_table_s3tables_fallback_when_not_in_glue():
    s3tables._tables["bucket\x00ns\x00t"] = {
        "name": "t",
        "namespace": ["ns"],
        "metadataLocation": "s3://bucket/ns/t/metadata/v0.metadata.json",
        "_iceberg_metadata": {"format-version": 2, "snapshots": []},
    }
    status, _, payload = _call(
        "GET", "/iceberg/v1/000000000000/namespaces/ns/tables/t"
    )
    assert status == 200
    assert payload["metadata"]["format-version"] == 2


def test_load_table_404s_on_unknown_table_in_both_backends():
    status, _, payload = _call(
        "GET", "/iceberg/v1/000000000000/namespaces/nope/tables/missing"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchTableException"


# ── HEAD / TableExists ───────────────────────────────────────


def test_head_returns_200_when_glue_iceberg_table_present():
    _seed_glue_table_with_metadata()
    status, _, _ = _call(
        "HEAD", "/iceberg/v1/000000000000/namespaces/db/tables/dim_application"
    )
    assert status == 200


def test_head_returns_404_when_glue_table_lacks_metadata_location():
    glue._databases["db"] = {"Name": "db"}
    glue._tables["db/csv"] = {
        "Name": "csv", "DatabaseName": "db", "Parameters": {},
    }
    status, _, _ = _call(
        "HEAD", "/iceberg/v1/000000000000/namespaces/db/tables/csv"
    )
    assert status == 404


# ── Dispatcher fall-throughs ─────────────────────────────────


def test_unknown_subpath_returns_error_envelope():
    """Anything under /v1/{prefix}/ that isn't ``namespaces/...`` falls out
    of the dispatcher and gets the 501 catch-all. The shape is what matters
    for clients — Iceberg-REST-spec error envelope, not silent 200."""
    status, _, payload = _call("GET", "/iceberg/v1/000000000000/junk")
    assert status == 501
    assert "error" in payload
    assert payload["error"]["type"] == "UnsupportedOperationException"


def test_post_to_load_table_returns_501_unsupported():
    """Read-only PR: writes return 501 with a clear envelope rather than
    silently succeeding or erroring as 404."""
    status, _, payload = _call(
        "POST", "/iceberg/v1/000000000000/namespaces/db/tables/t"
    )
    assert status == 501
    assert payload["error"]["type"] == "UnsupportedOperationException"


def test_missing_v1_prefix_404s():
    status, _, _ = _call("GET", "/iceberg/v2/config")
    assert status == 404
