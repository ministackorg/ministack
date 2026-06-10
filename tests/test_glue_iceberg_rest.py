"""Unit tests for Glue's Iceberg REST catalog data plane (services/glue.py).

Mirrors AWS Glue's `glue.<region>.amazonaws.com/iceberg` endpoint: prefix
shape `/v1/catalogs/{catalog}/...`, told apart from the S3 Tables Iceberg
REST endpoint (services/s3tables.py) by SigV4 signing name — exactly how
the two separate services are distinguished on real AWS.

These exercise `glue.handle_request` directly with seeded state, without a
running server — the same direct-handler pattern as test_ecs_metadata.py.
"""

import asyncio
import importlib
import json

import pytest

import ministack.core.responses as _responses
from ministack.core.router import detect_service


def _svc(name):
    """Resolve a service module through sys.modules at call time.

    NOT a module-level `from ministack.services import glue, s3` — the
    persistence regression tests cold-reimport service modules
    (`sys.modules.pop` + fresh import), so an import-time binding here can
    end up pointing at a stale module object while glue.py's own lazy
    `from ministack.services import s3` resolves the fresh one. State
    seeded on the stale object is then invisible to production code (the
    exact failure mode this replaced: 404 where 200 was expected, only
    when test_persistence.py ran first in the same worker)."""
    return importlib.import_module(f"ministack.services.{name}")


def _call(method, path, query_params=None):
    status, headers, body = asyncio.run(
        _svc("glue").handle_request(method, path, {}, b"", query_params or {})
    )
    payload = json.loads(body) if body else None
    return status, headers, payload


@pytest.fixture(autouse=True)
def _reset():
    # Pin the per-request account so AccountScopedDict lookups land in the
    # same bucket whether the caller is the test or production code.
    _responses._request_account_id.set("000000000000")
    _responses._request_region.set("us-east-1")
    _svc("glue").reset()
    _svc("s3")._buckets.clear()
    yield
    _svc("glue").reset()
    _svc("s3")._buckets.clear()


# ── Routing ──────────────────────────────────────────────────


def test_glue_signed_iceberg_request_routes_to_glue_service():
    """DuckDB signs Iceberg REST requests with the `glue` credential scope.
    The router's existing scope dispatch must land them on the glue module —
    no path-based special case required."""
    headers = {
        "authorization": (
            "AWS4-HMAC-SHA256 Credential=test/20260610/us-east-1/glue/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        )
    }
    assert detect_service("GET", "/iceberg/v1/config", headers, {}) == "glue"


def test_s3tables_signed_iceberg_request_still_routes_to_s3tables():
    """The S3 Tables Iceberg REST surface is a distinct AWS service; signing
    as `s3tables` must keep landing on services/s3tables.py."""
    headers = {
        "authorization": (
            "AWS4-HMAC-SHA256 Credential=test/20260610/us-east-1/s3tables/aws4_request, "
            "SignedHeaders=host, Signature=abc"
        )
    }
    assert detect_service("GET", "/iceberg/v1/config", headers, {}) == "s3tables"


# ── /iceberg/v1/config ───────────────────────────────────────


def test_config_returns_glue_catalogs_prefix_and_s3_overrides():
    status, headers, payload = _call(
        "GET", "/iceberg/v1/config", {"warehouse": ["000000000000"]}
    )
    assert status == 200
    assert headers["Content-Type"] == "application/json"
    # Glue's prefix shape — subsequent client URLs become
    # /iceberg/v1/catalogs/000000000000/namespaces/...
    assert payload["defaults"]["prefix"] == "catalogs/000000000000"
    overrides = payload["overrides"]
    assert overrides["s3.endpoint"].startswith("http://")
    assert overrides["s3.path-style-access"] == "true"
    assert "s3.region" in overrides
    # Fixed creds, never echoed from the host env — emitting ambient
    # AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY into a response body would
    # leak real credentials from a developer's shell.
    assert overrides["s3.access-key-id"] == "test"
    assert overrides["s3.secret-access-key"] == "test"


def test_config_without_warehouse_returns_empty_defaults():
    _, _, payload = _call("GET", "/iceberg/v1/config")
    assert payload["defaults"] == {}
    assert "overrides" in payload


# ── Namespaces ───────────────────────────────────────────────


def test_list_namespaces_returns_glue_databases():
    _svc("glue")._databases["db_a"] = {"Name": "db_a"}
    _svc("glue")._databases["db_b"] = {"Name": "db_b"}
    _, _, payload = _call("GET", "/iceberg/v1/catalogs/000000000000/namespaces")
    assert payload["namespaces"] == [["db_a"], ["db_b"]]


def test_get_namespace_404s_when_database_missing():
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/nope"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchNamespaceException"


def test_get_namespace_returns_shape_when_database_exists():
    _svc("glue")._databases["db"] = {"Name": "db"}
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db"
    )
    assert status == 200
    assert payload == {"namespace": ["db"], "properties": {}}


# ── ListTables ───────────────────────────────────────────────


def test_list_tables_404s_when_namespace_missing():
    """Real Iceberg REST returns NoSuchNamespaceException for ListTables on
    an unknown namespace — not an empty 200 list."""
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/nope/tables"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchNamespaceException"


def test_list_tables_hides_non_iceberg_glue_tables():
    """A Glue table without `Parameters['metadata_location']` is a plain
    Hive/CSV/Parquet table and must not appear on the Iceberg surface."""
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/iceberg_table"] = {
        "Name": "iceberg_table",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": "s3://lake/t/metadata/v0.metadata.json"},
    }
    _svc("glue")._tables["db/csv_table"] = {
        "Name": "csv_table", "DatabaseName": "db", "Parameters": {},
    }
    _, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables"
    )
    assert [t["name"] for t in payload["identifiers"]] == ["iceberg_table"]
    assert payload["identifiers"][0]["namespace"] == ["db"]


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


def _seed_glue_table_with_metadata():
    key = "dim_application/metadata/00003-deadbeef.metadata.json"
    _svc("s3")._buckets["lake"] = {"objects": {key: {"body": json.dumps(_META_JSON).encode()}}}
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/dim_application"] = {
        "Name": "dim_application",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": f"s3://lake/{key}"},
    }


def test_load_table_inlines_metadata_json_verbatim():
    _seed_glue_table_with_metadata()
    status, _, payload = _call(
        "GET",
        "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/dim_application",
    )
    assert status == 200
    assert payload["metadata-location"].endswith("/00003-deadbeef.metadata.json")
    assert payload["metadata"] == _META_JSON  # passthrough — no transformation
    assert "s3.endpoint" in payload["config"]


def test_load_table_404s_when_metadata_object_missing():
    """Glue table exists and points at an S3 URI, but the metadata.json
    object isn't there. 200-with-empty-metadata would make DuckDB treat it
    as a real-but-empty table and silently return wrong results."""
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/orphan"] = {
        "Name": "orphan",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": "s3://lake/orphan/metadata/v0.metadata.json"},
    }
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/orphan"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchTableException"
    assert payload["error"]["code"] == 404


def test_load_table_404s_when_metadata_json_unparseable():
    key = "broken/metadata/v0.metadata.json"
    _svc("s3")._buckets["lake"] = {"objects": {key: {"body": b"<<not json>>"}}}
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/broken"] = {
        "Name": "broken",
        "DatabaseName": "db",
        "Parameters": {"metadata_location": f"s3://lake/{key}"},
    }
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/broken"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchTableException"


def test_load_table_404s_on_unknown_table():
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/missing"
    )
    assert status == 404
    assert payload["error"]["type"] == "NoSuchTableException"


def test_load_table_404s_on_non_iceberg_table():
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/csv"] = {"Name": "csv", "DatabaseName": "db", "Parameters": {}}
    status, _, payload = _call(
        "GET", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/csv"
    )
    assert status == 404


# ── HEAD / TableExists ───────────────────────────────────────


def test_head_returns_200_for_iceberg_table():
    _seed_glue_table_with_metadata()
    status, _, _ = _call(
        "HEAD",
        "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/dim_application",
    )
    assert status == 200


def test_head_returns_404_for_non_iceberg_table():
    _svc("glue")._databases["db"] = {"Name": "db"}
    _svc("glue")._tables["db/csv"] = {"Name": "csv", "DatabaseName": "db", "Parameters": {}}
    status, _, _ = _call(
        "HEAD", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/csv"
    )
    assert status == 404


# ── Fall-throughs ────────────────────────────────────────────


def test_post_to_table_returns_501_unsupported():
    """Read-only surface: writes get an explicit 501 envelope instead of a
    silent success that never persisted anything."""
    status, _, payload = _call(
        "POST", "/iceberg/v1/catalogs/000000000000/namespaces/db/tables/t"
    )
    assert status == 501
    assert payload["error"]["type"] == "UnsupportedOperationException"


def test_non_catalogs_prefix_returns_501():
    """Bare-warehouse prefixes (`/v1/{warehouse}/namespaces`) are the
    S3 Tables shape, not Glue's — reject rather than guess."""
    status, _, payload = _call(
        "GET", "/iceberg/v1/000000000000/namespaces"
    )
    assert status == 501


def test_unknown_version_prefix_404s():
    status, _, _ = _call("GET", "/iceberg/v2/config")
    assert status == 404


def test_glue_json_rpc_surface_unaffected():
    """The X-Amz-Target JSON RPC surface must keep working alongside the
    Iceberg branch — same module, two protocols."""
    status, _, body = asyncio.run(
        _svc("glue").handle_request(
            "POST", "/",
            {"x-amz-target": "AWSGlue.CreateDatabase"},
            json.dumps({"DatabaseInput": {"Name": "rpc_db"}}).encode(),
            {},
        )
    )
    assert status == 200
    assert "rpc_db" in _svc("glue")._databases
