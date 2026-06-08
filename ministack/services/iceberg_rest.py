"""
Apache Iceberg REST Catalog endpoint.

Serves the read-path subset of the Iceberg REST Catalog OpenAPI spec under
``/iceberg/v1/*`` so that clients like DuckDB's ``iceberg`` extension can
ATTACH to MiniStack as if it were AWS Glue's Iceberg REST endpoint:

    ATTACH '000000000000' AS glue_catalog (
        TYPE iceberg,
        ENDPOINT 'localhost:4566/iceberg',
        AUTHORIZATION_TYPE 'sigv4'
    );

Backends, in lookup order:
  1. Glue Data Catalog — any Glue table whose ``Parameters["metadata_location"]``
     points at an Iceberg metadata.json on internal S3. This is the path
     Trino/Spark write through and what real AWS Glue's Iceberg REST exposes.
  2. S3 Tables — falls back to ``services.s3tables`` for tables created
     through the s3tables control plane (Spark with ``catalog.type=rest``).

Endpoints implemented (DuckDB read-path only):
  GET /iceberg/v1/config?warehouse=<id>
  GET /iceberg/v1/{prefix}/namespaces
  GET /iceberg/v1/{prefix}/namespaces/{namespace}/tables
  GET /iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}        (LoadTable)
  HEAD /iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}       (TableExists)

Write paths (commit / create / drop) and the views API are intentionally
absent; if/when needed they belong in a follow-up.

DuckDB hardcodes ``https://`` on the Iceberg endpoint with no escape hatch,
so callers must run MiniStack with ``USE_SSL=1`` (see ``core/tls.py``) and
trust the self-signed cert (``SSL_CERT_FILE`` / ``CURL_CA_BUNDLE`` /
DuckDB's ``ca_cert_file``). SigV4 signatures are accepted but not verified —
same loose stance as every other MiniStack endpoint.
"""

import json
import logging
import os
from urllib.parse import unquote

# Sibling service modules are imported lazily inside the helpers below to
# match the codebase's lazy-load convention (see appsync / cognito /
# stepfunctions for the same pattern). Eager top-level imports would force
# glue / s3 / s3tables to load the moment iceberg_rest is touched by the
# router — defeating the lazy `_get_module` design in `app.py`.

logger = logging.getLogger("iceberg_rest")

_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_GATEWAY_PORT = os.environ.get("GATEWAY_PORT", "4566")


# ── Response helpers ──────────────────────────────────────────
# Iceberg REST uses plain ``application/json`` (not the AWS x-amz-json-1.0
# flavor) and its error envelope is ``{"error": {"message", "type", "code"}}``.

def _json(data: dict, status: int = 200):
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    return status, {"Content-Type": "application/json"}, body


def _error(message: str, error_type: str, status: int):
    return _json(
        {"error": {"message": message, "type": error_type, "code": status}},
        status=status,
    )


# ── S3 overrides emitted to clients ───────────────────────────
# DuckDB / pyiceberg / Spark all consume these key names; with them, the
# client knows how to reach data files referenced by the catalog. Without
# them, the client falls back to ambient credentials (real AWS S3) and
# fails. The endpoint points at MiniStack itself — its S3 service answers
# on the same gateway port.

def _s3_overrides() -> dict:
    return {
        "s3.endpoint": f"http://{_MINISTACK_HOST}:{_GATEWAY_PORT}",
        "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        "s3.path-style-access": "true",
        "s3.region": os.environ.get("MINISTACK_REGION", "us-east-1"),
    }


# ── S3 URI parsing ────────────────────────────────────────────

def _parse_s3_uri(uri: str):
    """Split ``s3://bucket/key/parts`` → ``(bucket, key)``. Returns ``(None, None)`` on malformed input."""
    if not uri or not uri.startswith("s3://"):
        return None, None
    rest = uri[len("s3://"):]
    if "/" not in rest:
        return None, None
    bucket, key = rest.split("/", 1)
    return bucket, key


def _fetch_metadata_json(metadata_location: str) -> dict | None:
    """Read the metadata.json at the given ``s3://`` URI from MiniStack's S3.

    Returns ``None`` if the URI is malformed, the bucket/object is missing,
    or the body isn't valid JSON. The caller decides whether to 404 or
    return an empty ``metadata`` block.
    """
    bucket, key = _parse_s3_uri(metadata_location)
    if not bucket or not key:
        return None
    from ministack.services import s3 as _s3
    data = _s3._get_object_data(bucket, key)
    if data is None:
        return None
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        logger.warning("metadata.json at %s did not parse as JSON: %s", metadata_location, exc)
        return None


# ── Glue-backed lookups ───────────────────────────────────────
# Glue stores Iceberg metadata pointer in ``Table.Parameters["metadata_location"]``
# (the same place Trino, Spark with the Glue catalog, and the real AWS Glue
# Iceberg REST endpoint look). A Glue table without that parameter is a
# non-Iceberg table (Hive/CSV/Parquet without the Iceberg manifest layer)
# and we hide it from the Iceberg REST surface.

def _glue_namespaces() -> list[str]:
    from ministack.services import glue as _glue
    return list(_glue._databases.keys())


def _glue_iceberg_tables(db_name: str) -> list[str]:
    from ministack.services import glue as _glue
    out = []
    prefix = f"{db_name}/"
    for key, table in _glue._tables.items():
        if not key.startswith(prefix):
            continue
        params = table.get("Parameters") or {}
        if params.get("metadata_location"):
            out.append(table["Name"])
    return out


def _glue_load_table(db_name: str, table_name: str):
    from ministack.services import glue as _glue
    table = _glue._tables.get(f"{db_name}/{table_name}")
    if table is None:
        return None
    metadata_location = (table.get("Parameters") or {}).get("metadata_location")
    if not metadata_location:
        return None
    # If the metadata.json can't be read (missing object, malformed URI,
    # not valid JSON), surface a 404 rather than an HTTP 200 with an empty
    # ``metadata`` block — DuckDB would otherwise treat that as a real but
    # empty table and produce a silent-wrong query result.
    metadata = _fetch_metadata_json(metadata_location)
    if metadata is None:
        return None
    return {
        "metadata-location": metadata_location,
        "metadata": metadata,
        "config": _s3_overrides(),
    }


# ── S3 Tables-backed lookups ──────────────────────────────────
# Fallback so existing Spark-against-s3tables setups keep working through
# this new dispatcher. The s3tables module already manages the in-memory
# metadata blob; we just adapt its shape and bolt our overrides on.

def _s3tables_namespaces() -> list[str]:
    out = []
    for ns in _s3tables._namespaces.values():
        name = ns.get("namespace")
        if isinstance(name, list) and name:
            out.append(name[0])
    return out


def _s3tables_tables(namespace: str) -> list[str]:
    out = []
    for tbl in _s3tables._tables.values():
        tbl_ns = tbl.get("namespace")
        tbl_ns_name = tbl_ns[0] if isinstance(tbl_ns, list) and tbl_ns else ""
        if tbl_ns_name == namespace:
            out.append(tbl["name"])
    return out


def _s3tables_load_table(namespace: str, table_name: str):
    for tbl in _s3tables._tables.values():
        tbl_ns = tbl.get("namespace")
        tbl_ns_name = tbl_ns[0] if isinstance(tbl_ns, list) and tbl_ns else ""
        if tbl_ns_name == namespace and tbl.get("name") == table_name:
            return {
                "metadata-location": tbl.get("metadataLocation", ""),
                "metadata": tbl.get("_iceberg_metadata", {}),
                "config": _s3_overrides(),
            }
    return None


# ── Endpoint implementations ──────────────────────────────────

def _config(query_params: dict):
    """``GET /v1/config?warehouse=<id>``.

    DuckDB calls this once on ATTACH. Echo the warehouse back as the prefix
    (clients use it to build subsequent URLs) and hand over the S3 overrides
    so it can reach MiniStack's S3 for data files.
    """
    warehouse = _qp(query_params, "warehouse")
    defaults = {"prefix": warehouse} if warehouse else {}
    return _json({"defaults": defaults, "overrides": _s3_overrides()})


def _list_namespaces():
    namespaces = sorted(set(_glue_namespaces()) | set(_s3tables_namespaces()))
    return _json({"namespaces": [[ns] for ns in namespaces]})


def _list_tables(namespace: str):
    tables = sorted(set(_glue_iceberg_tables(namespace)) | set(_s3tables_tables(namespace)))
    identifiers = [{"namespace": [namespace], "name": t} for t in tables]
    return _json({"identifiers": identifiers})


def _load_table(namespace: str, table_name: str):
    payload = _glue_load_table(namespace, table_name) or _s3tables_load_table(namespace, table_name)
    if payload is None:
        return _error(
            f"Table does not exist: {namespace}.{table_name}",
            "NoSuchTableException",
            404,
        )
    return _json(payload)


def _table_exists(namespace: str, table_name: str):
    glue_table = _glue._tables.get(f"{namespace}/{table_name}")
    if glue_table and (glue_table.get("Parameters") or {}).get("metadata_location"):
        return 200, {}, b""
    for tbl in _s3tables._tables.values():
        tbl_ns = tbl.get("namespace")
        tbl_ns_name = tbl_ns[0] if isinstance(tbl_ns, list) and tbl_ns else ""
        if tbl_ns_name == namespace and tbl.get("name") == table_name:
            return 200, {}, b""
    return 404, {}, b""


# ── Query-param helper ────────────────────────────────────────

def _qp(query_params: dict, key: str, default: str = "") -> str:
    val = query_params.get(key, default)
    if isinstance(val, list):
        return val[0] if val else default
    return val


# ── Dispatcher ────────────────────────────────────────────────

async def handle_request(method, path, headers, body, query_params):
    parts = [unquote(p) for p in path.strip("/").split("/") if p]
    # parts: ["iceberg", "v1", ...]
    if len(parts) < 2 or parts[0] != "iceberg" or parts[1] != "v1":
        return _error(f"Unknown Iceberg REST path: {path}", "NotFoundException", 404)

    # GET /v1/config — singleton; warehouse comes from query string.
    if len(parts) == 3 and parts[2] == "config" and method == "GET":
        return _config(query_params)

    # All remaining endpoints live under /v1/{prefix}/...
    if len(parts) < 4:
        return _error(f"Unknown Iceberg REST path: {path}", "NotFoundException", 404)

    # parts[2] = prefix (warehouse, echoed back from /v1/config). We don't
    # validate it — MiniStack treats any non-empty prefix as the catalog
    # identifier and routes by namespace/table within.
    if parts[3] == "namespaces":
        if len(parts) == 4 and method == "GET":
            return _list_namespaces()
        if len(parts) == 5 and method == "GET":
            # GetNamespace — minimal stub. DuckDB doesn't require properties.
            return _json({"namespace": [parts[4]], "properties": {}})
        if len(parts) >= 6 and parts[5] == "tables":
            namespace = parts[4]
            if len(parts) == 6 and method == "GET":
                return _list_tables(namespace)
            if len(parts) == 7:
                table_name = parts[6]
                if method == "GET":
                    return _load_table(namespace, table_name)
                if method == "HEAD":
                    return _table_exists(namespace, table_name)

    return _error(
        f"Operation not supported: {method} {path}",
        "UnsupportedOperationException",
        501,
    )


# ── Persistence hooks (none — state is borrowed from glue/s3tables) ──

def get_state():
    return {}


def restore_state(data):
    pass


def reset():
    pass
