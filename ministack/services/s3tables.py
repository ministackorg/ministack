"""
S3 Tables Service Emulator.

Provides the ``s3tables`` control plane: table buckets, namespaces, and tables
in Apache Iceberg format.  Data files are stored in MiniStack's S3 service;
table metadata (schemas, snapshots, manifests) is held in memory.

Also exposes an **Iceberg REST catalog** endpoint at ``/iceberg`` so that Spark
jobs configured with ``spark.sql.catalog.*.type=rest`` and
``spark.sql.catalog.*.uri=http://<ministack>/iceberg`` can create, load, and
commit Iceberg tables without any external catalog server.

REST API paths from botocore s3tables service model:
  PUT    /buckets                                          CreateTableBucket
  GET    /buckets                                          ListTableBuckets
  GET    /buckets/{arn}                                    GetTableBucket
  DELETE /buckets/{arn}                                    DeleteTableBucket
  PUT    /namespaces/{arn}                                 CreateNamespace
  GET    /namespaces/{arn}                                 ListNamespaces
  GET    /namespaces/{arn}/{namespace}                     GetNamespace
  DELETE /namespaces/{arn}/{namespace}                     DeleteNamespace
  PUT    /tables/{arn}/{namespace}                         CreateTable
  GET    /tables/{arn}                                     ListTables
  GET    /get-table?tableBucketARN=&namespace=&name=       GetTable
  DELETE /tables/{arn}/{namespace}/{name}                  DeleteTable
  GET    /tables/{arn}/{namespace}/{name}/metadata-location GetTableMetadataLocation
  PUT    /tables/{arn}/{namespace}/{name}/metadata-location UpdateTableMetadataLocation
"""

import copy
import json
import logging
import os
import re
import time
from urllib.parse import unquote

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_iso,
)

logger = logging.getLogger("s3tables")

_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_GATEWAY_PORT = os.environ.get("GATEWAY_PORT", "4566")
_SIGV4_CREDENTIAL_REGION_RE = re.compile(r"Credential=[^/]+/[^/]+/([^/]+)/")

# ── In-memory state ────────────────────────────────────────

_table_buckets = AccountRegionScopedDict()
_namespaces = AccountRegionScopedDict()        # "bucket_arn\x00namespace" -> ns dict
_tables = AccountRegionScopedDict()            # "bucket_arn\x00namespace\x00table" -> table dict


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "table_buckets": copy.deepcopy(_table_buckets),
        "namespaces": copy.deepcopy(_namespaces),
        "tables": copy.deepcopy(_tables),
    }


def restore_state(data):
    _table_buckets.update(data.get("table_buckets", {}))
    _namespaces.update(data.get("namespaces", {}))
    _tables.update(data.get("tables", {}))


def reset():
    _table_buckets.clear()
    _namespaces.clear()
    _tables.clear()


if PERSIST_STATE:
    _saved = load_state("s3tables")
    if _saved:
        restore_state(_saved)


# ── Helpers ────────────────────────────────────────────────

def _bucket_arn(name):
    return f"arn:aws:s3tables:{get_region()}:{get_account_id()}:bucket/{name}"


def _table_arn(bucket_arn, namespace, table_name):
    return f"{bucket_arn}/table/{namespace}/{table_name}"


def _bucket_name_from_arn(bucket_arn):
    try:
        spec = parse_arn(bucket_arn)
    except ArnParseError:
        return None
    if (
        spec.partition != "aws"
        or spec.service != "s3tables"
        or spec.region != get_region()
        or spec.account_id != get_account_id()
    ):
        return None
    prefix = "bucket/"
    if not spec.resource.startswith(prefix):
        return None
    name = spec.resource[len(prefix):]
    if not name or "/" in name:
        return None
    return name


def _canonical_bucket_arn(bucket_ref):
    if not isinstance(bucket_ref, str) or not bucket_ref:
        return None
    if not bucket_ref.startswith("arn:"):
        return _bucket_arn(bucket_ref)
    name = _bucket_name_from_arn(bucket_ref)
    return _bucket_arn(name) if name else None


def _ns_key(bucket_arn, namespace):
    return f"{bucket_arn}\x00{namespace}"


def _table_key(bucket_arn, namespace, table_name):
    return f"{bucket_arn}\x00{namespace}\x00{table_name}"


def _find_bucket_by_arn(arn):
    arn = _canonical_bucket_arn(arn)
    if not arn:
        return None
    for b in _table_buckets.values():
        if b["arn"] == arn:
            return b
    return None


def _existing_bucket_arn(bucket_ref):
    arn = _canonical_bucket_arn(bucket_ref)
    if not arn or not _find_bucket_by_arn(arn):
        return None
    return arn


def _bucket_not_found(bucket_ref):
    return error_response_json("NotFoundException", f"Table bucket not found: {bucket_ref}", 404)


def _account_values(store):
    return [value for key, value in store.all_items() if key[0] == get_account_id()]


def _iceberg_values(store, predicate, allow_cross_region):
    values = _account_values(store) if allow_cross_region else store.values()
    return [value for value in values if predicate(value)]


def _namespace_name(value):
    return value["namespace"][0] if isinstance(value.get("namespace"), list) else value.get("namespace", "")


def _set_bucket_region_value(store, bucket_arn, key, value):
    spec = parse_arn(bucket_arn)
    store.set_scoped(spec.account_id, spec.region, key, value)


def _to_iceberg_type(kind):
    return {"string": "string", "int": "int", "long": "long", "boolean": "boolean",
            "date": "date", "timestamp": "timestamptz", "float": "float", "double": "double"
            }.get(kind, "string")


def _initial_iceberg_metadata(table_name, schema_fields, location):
    table_uuid = new_uuid()
    fields = []
    for i, f in enumerate(schema_fields):
        fields.append({"id": i + 1, "name": f["name"], "required": f.get("required", False),
                        "type": _to_iceberg_type(f.get("type", "string"))})
    schema = {"type": "struct", "schema-id": 0, "fields": fields}
    return {
        "format-version": 3, "table-uuid": table_uuid, "location": location,
        "last-sequence-number": 0, "last-updated-ms": int(time.time() * 1000),
        "last-column-id": len(schema_fields), "current-schema-id": 0,
        "schemas": [schema], "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999, "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {}, "current-snapshot-id": -1, "refs": {},
        "snapshots": [], "statistics": [], "snapshot-log": [], "metadata-log": [],
    }


# ── S3 Tables control plane ───────────────────────────────

def _create_table_bucket(data):
    name = data.get("name", "")
    if not name:
        return error_response_json("ValidationException", "name is required", 400)
    if name in _table_buckets:
        return error_response_json("ConflictException", f"Table bucket {name} already exists", 409)
    arn = _bucket_arn(name)
    _table_buckets[name] = {"arn": arn, "name": name, "ownerAccountId": get_account_id(),
                             "createdAt": now_iso(), "tableCount": 0}
    logger.info("S3Tables: created table bucket %s", name)
    return json_response({"arn": arn})


def _list_table_buckets():
    return json_response({"tableBuckets": list(_table_buckets.values())})


def _get_table_bucket(arn):
    bucket = _find_bucket_by_arn(arn)
    if not bucket:
        return _bucket_not_found(arn)
    return json_response(bucket)


def _delete_table_bucket(arn):
    raw_arn = arn
    arn = _canonical_bucket_arn(arn)
    if not arn:
        return _bucket_not_found(raw_arn)
    name = None
    for n, b in _table_buckets.items():
        if b["arn"] == arn:
            name = n
            break
    if not name:
        return _bucket_not_found(arn)
    for key in list(_tables.keys()):
        if key.startswith(arn + "\x00"):
            del _tables[key]
    for key in list(_namespaces.keys()):
        if key.startswith(arn + "\x00"):
            del _namespaces[key]
    del _table_buckets[name]
    return json_response({})


def _create_namespace(bucket_arn, data):
    raw_bucket_arn = bucket_arn
    bucket_arn = _existing_bucket_arn(bucket_arn)
    if not bucket_arn:
        return _bucket_not_found(raw_bucket_arn)
    ns_list = data.get("namespace", [])
    namespace = ns_list[0] if isinstance(ns_list, list) and ns_list else ns_list
    if not namespace:
        return error_response_json("ValidationException", "namespace is required", 400)
    key = _ns_key(bucket_arn, namespace)
    if key in _namespaces:
        return error_response_json("ConflictException", f"Namespace {namespace} already exists", 409)
    _namespaces[key] = {"namespace": [namespace], "createdAt": now_iso(),
                         "createdBy": get_account_id(), "ownerAccountId": get_account_id(),
                         "tableBucketARN": bucket_arn}
    logger.info("S3Tables: created namespace %s", namespace)
    return json_response({"namespace": [namespace], "tableBucketARN": bucket_arn})


def _list_namespaces(bucket_arn):
    raw_bucket_arn = bucket_arn
    bucket_arn = _existing_bucket_arn(bucket_arn)
    if not bucket_arn:
        return _bucket_not_found(raw_bucket_arn)
    result = [ns for ns in _namespaces.values() if ns.get("tableBucketARN") == bucket_arn]
    return json_response({"namespaces": result})


def _get_namespace(bucket_arn, namespace):
    raw_bucket_arn = bucket_arn
    bucket_arn = _existing_bucket_arn(bucket_arn)
    if not bucket_arn:
        return _bucket_not_found(raw_bucket_arn)
    key = _ns_key(bucket_arn, namespace)
    ns = _namespaces.get(key)
    if not ns:
        return error_response_json("NotFoundException", f"Namespace {namespace} not found", 404)
    return json_response(ns)


def _delete_namespace(bucket_arn, namespace):
    raw_bucket_arn = bucket_arn
    bucket_arn = _existing_bucket_arn(bucket_arn)
    if not bucket_arn:
        return _bucket_not_found(raw_bucket_arn)
    key = _ns_key(bucket_arn, namespace)
    if key not in _namespaces:
        return error_response_json("NotFoundException", f"Namespace {namespace} not found", 404)
    del _namespaces[key]
    return json_response({})


def _create_table(bucket_arn, namespace, data):
    raw_bucket_arn = bucket_arn
    bucket_arn = _existing_bucket_arn(bucket_arn)
    if not bucket_arn:
        return _bucket_not_found(raw_bucket_arn)
    table_name = data.get("name", "")
    fmt = data.get("format", "ICEBERG")
    if not table_name:
        return error_response_json("ValidationException", "name is required", 400)
    key = _table_key(bucket_arn, namespace, table_name)
    if key in _tables:
        return error_response_json("ConflictException", f"Table {table_name} already exists", 409)

    schema_fields = []
    metadata = data.get("metadata", {})
    iceberg_meta = metadata.get("iceberg", {})
    schema_def = iceberg_meta.get("schema", {})
    for f in schema_def.get("field", schema_def.get("fields", [])):
        schema_fields.append({"name": f["name"], "type": f.get("type", "string"),
                               "required": f.get("required", False)})

    bucket_name = bucket_arn.rsplit("/", 1)[-1]
    location = f"s3://{bucket_name}/{namespace}/{table_name}"
    iceberg_metadata = _initial_iceberg_metadata(table_name, schema_fields, location)
    metadata_location = f"s3://{bucket_name}/{namespace}/{table_name}/metadata/v0.metadata.json"
    arn = _table_arn(bucket_arn, namespace, table_name)

    _tables[key] = {
        "name": table_name, "tableARN": arn, "namespace": [namespace],
        "tableBucketARN": bucket_arn, "format": fmt,
        "createdAt": now_iso(), "modifiedAt": now_iso(),
        "ownerAccountId": get_account_id(),
        "metadataLocation": metadata_location, "warehouseLocation": location,
        "_iceberg_metadata": iceberg_metadata, "_metadata_version": 0,
        "_schema_fields": schema_fields,
    }

    for b in _table_buckets.values():
        if b["arn"] == bucket_arn:
            b["tableCount"] = b.get("tableCount", 0) + 1
            break

    logger.info("S3Tables: created table %s/%s", namespace, table_name)
    return json_response({"tableARN": arn, "versionToken": new_uuid()[:8]})


def _list_tables(bucket_arn, namespace=None):
    raw_bucket_arn = bucket_arn
    bucket_arn = _existing_bucket_arn(bucket_arn)
    if not bucket_arn:
        return _bucket_not_found(raw_bucket_arn)
    result = []
    for key, table in _tables.items():
        if not key.startswith(bucket_arn + "\x00"):
            continue
        table_ns = table["namespace"][0] if isinstance(table["namespace"], list) else table["namespace"]
        if namespace and table_ns != namespace:
            continue
        result.append({"name": table["name"], "tableARN": table["tableARN"],
                        "namespace": table["namespace"], "format": table["format"],
                        "createdAt": table["createdAt"]})
    return json_response({"tables": result})


def _get_table(bucket_arn, namespace, table_name):
    raw_bucket_arn = bucket_arn
    bucket_arn = _existing_bucket_arn(bucket_arn)
    if not bucket_arn:
        return _bucket_not_found(raw_bucket_arn)
    key = _table_key(bucket_arn, namespace, table_name)
    table = _tables.get(key)
    if not table:
        return error_response_json("NotFoundException", f"Table {table_name} not found", 404)
    return json_response({k: v for k, v in table.items() if not k.startswith("_")})


def _delete_table(bucket_arn, namespace, table_name):
    raw_bucket_arn = bucket_arn
    bucket_arn = _existing_bucket_arn(bucket_arn)
    if not bucket_arn:
        return _bucket_not_found(raw_bucket_arn)
    key = _table_key(bucket_arn, namespace, table_name)
    if key not in _tables:
        return error_response_json("NotFoundException", f"Table {table_name} not found", 404)
    del _tables[key]
    return json_response({})


def _get_table_metadata_location(bucket_arn, namespace, table_name):
    raw_bucket_arn = bucket_arn
    bucket_arn = _existing_bucket_arn(bucket_arn)
    if not bucket_arn:
        return _bucket_not_found(raw_bucket_arn)
    key = _table_key(bucket_arn, namespace, table_name)
    table = _tables.get(key)
    if not table:
        return error_response_json("NotFoundException", f"Table {table_name} not found", 404)
    return json_response({"metadataLocation": table["metadataLocation"],
                           "versionToken": new_uuid()[:8]})


def _update_table_metadata_location(bucket_arn, namespace, table_name, data):
    raw_bucket_arn = bucket_arn
    bucket_arn = _existing_bucket_arn(bucket_arn)
    if not bucket_arn:
        return _bucket_not_found(raw_bucket_arn)
    key = _table_key(bucket_arn, namespace, table_name)
    table = _tables.get(key)
    if not table:
        return error_response_json("NotFoundException", f"Table {table_name} not found", 404)
    new_loc = data.get("metadataLocation", "")
    table["metadataLocation"] = new_loc
    table["modifiedAt"] = now_iso()
    return json_response({"metadataLocation": new_loc, "name": table_name,
                           "versionToken": new_uuid()[:8]})


# ── Iceberg REST catalog (data plane for Spark) ───────────

def _iceberg_config():
    return json_response({"defaults": {}, "overrides": {}})


def _iceberg_allows_cross_region(headers):
    return _SIGV4_CREDENTIAL_REGION_RE.search(headers.get("authorization", "")) is None


def _iceberg_list_namespaces(allow_cross_region):
    result = []
    for ns in _iceberg_values(_namespaces, lambda _ns: True, allow_cross_region):
        result.append(ns.get("namespace", []))
    return json_response({"namespaces": result})


def _iceberg_get_namespace(namespace, allow_cross_region):
    if _iceberg_values(_namespaces, lambda ns: _namespace_name(ns) == namespace, allow_cross_region):
        return json_response({"namespace": [namespace], "properties": {}})
    return error_response_json("NotFoundException", f"Namespace {namespace} not found", 404)


def _iceberg_list_tables(namespace, allow_cross_region):
    result = []
    for table in _iceberg_values(_tables, lambda table: _namespace_name(table) == namespace, allow_cross_region):
        result.append({"namespace": [namespace], "name": table["name"]})
    return json_response({"identifiers": result})


def _iceberg_load_table(namespace, table_name, allow_cross_region):
    matches = _iceberg_values(
        _tables,
        lambda table: _namespace_name(table) == namespace and table["name"] == table_name,
        allow_cross_region,
    )
    if matches:
        table = matches[0]
        return json_response({
            "metadata-location": table.get("metadataLocation", ""),
            "metadata": table.get("_iceberg_metadata", {}),
            "config": {
                "s3.access-key-id": "test", "s3.secret-access-key": "test",
                "s3.endpoint": f"http://{_MINISTACK_HOST}:{_GATEWAY_PORT}", "s3.path-style-access": "true",
            },
        })
    return error_response_json("NotFoundException", f"Table {namespace}.{table_name} not found", 404)


def _iceberg_commit_table(namespace, table_name, data, allow_cross_region):
    matches = _iceberg_values(
        _tables,
        lambda table: _namespace_name(table) == namespace and table["name"] == table_name,
        allow_cross_region,
    )
    if matches:
        table = matches[0]
        metadata = table.get("_iceberg_metadata", {})
        for update in data.get("updates", []):
            action = update.get("action", "")
            if action == "add-snapshot":
                snapshot = update.get("snapshot", {})
                metadata.setdefault("snapshots", []).append(snapshot)
                metadata["current-snapshot-id"] = snapshot.get("snapshot-id", -1)
                metadata["last-updated-ms"] = int(time.time() * 1000)
                metadata["last-sequence-number"] = metadata.get("last-sequence-number", 0) + 1
            elif action == "set-snapshot-ref":
                metadata.setdefault("refs", {})[update.get("ref-name", "main")] = {
                    "snapshot-id": update.get("snapshot-id", -1),
                    "type": update.get("type", "branch")}
            elif action == "add-schema":
                metadata.setdefault("schemas", []).append(update.get("schema", {}))
            elif action == "set-current-schema":
                metadata["current-schema-id"] = update.get("schema-id", 0)
            elif action == "add-partition-spec":
                metadata.setdefault("partition-specs", []).append(update.get("spec", {}))
            elif action == "set-default-spec":
                metadata["default-spec-id"] = update.get("spec-id", 0)
            elif action == "add-sort-order":
                metadata.setdefault("sort-orders", []).append(update.get("sort-order", {}))
            elif action == "set-default-sort-order":
                metadata["default-sort-order-id"] = update.get("sort-order-id", 0)
            elif action == "set-properties":
                metadata.setdefault("properties", {}).update(update.get("updates", {}))
            elif action == "remove-properties":
                for r in update.get("removals", []):
                    metadata.get("properties", {}).pop(r, None)
            elif action == "set-location":
                metadata["location"] = update.get("location", "")

        table["_metadata_version"] = table.get("_metadata_version", 0) + 1
        v = table["_metadata_version"]
        bucket_name = table.get("tableBucketARN", "").rsplit("/", 1)[-1]
        new_loc = f"s3://{bucket_name}/{namespace}/{table_name}/metadata/v{v}.metadata.json"
        table["metadataLocation"] = new_loc
        table["modifiedAt"] = now_iso()
        return json_response({"metadata-location": new_loc, "metadata": metadata})

    return error_response_json("NotFoundException", f"Table {namespace}.{table_name} not found", 404)


def _iceberg_create_table(namespace, data, allow_cross_region):
    table_name = data.get("name", "")
    schema = data.get("schema", {})
    bucket_arn = None
    matches = _iceberg_values(_namespaces, lambda ns: _namespace_name(ns) == namespace, allow_cross_region)
    if matches:
        bucket_arn = matches[0].get("tableBucketARN")
    if not bucket_arn:
        return error_response_json("NotFoundException", f"Namespace {namespace} not found", 404)

    schema_fields = [{"name": f.get("name", ""), "type": f.get("type", "string") if isinstance(f.get("type"), str) else "string",
                       "required": f.get("required", False)} for f in schema.get("fields", [])]

    bucket_name = bucket_arn.rsplit("/", 1)[-1]
    location = data.get("location", f"s3://{bucket_name}/{namespace}/{table_name}")
    iceberg_metadata = _initial_iceberg_metadata(table_name, schema_fields, location)
    if schema:
        iceberg_metadata["schemas"] = [schema]
    metadata_location = f"s3://{bucket_name}/{namespace}/{table_name}/metadata/v0.metadata.json"
    arn = _table_arn(bucket_arn, namespace, table_name)
    key = _table_key(bucket_arn, namespace, table_name)

    table = {
        "name": table_name, "tableARN": arn, "namespace": [namespace],
        "tableBucketARN": bucket_arn, "format": "ICEBERG",
        "createdAt": now_iso(), "modifiedAt": now_iso(),
        "ownerAccountId": get_account_id(),
        "metadataLocation": metadata_location, "warehouseLocation": location,
        "_iceberg_metadata": iceberg_metadata, "_metadata_version": 0,
        "_schema_fields": schema_fields,
    }
    _set_bucket_region_value(_tables, bucket_arn, key, table)
    return json_response({"metadata-location": metadata_location, "metadata": iceberg_metadata})


# ── Iceberg REST router ───────────────────────────────────

async def _handle_iceberg_request(method, path, headers, body, query_params):
    parts = [p for p in path.strip("/").split("/") if p]
    allow_cross_region = _iceberg_allows_cross_region(headers)
    # parts: ["iceberg", "v1", ...]
    if len(parts) < 2:
        return None

    if parts[1] == "v1" and len(parts) == 3 and parts[2] == "config" and method == "GET":
        return _iceberg_config()

    if len(parts) < 4:
        return None

    # parts[2] = prefix (catalog name), parts[3] = "namespaces"
    if parts[3] == "namespaces":
        if len(parts) == 4 and method == "GET":
            return _iceberg_list_namespaces(allow_cross_region)
        if len(parts) == 5 and method == "GET":
            return _iceberg_get_namespace(parts[4], allow_cross_region)
        if len(parts) >= 6 and parts[5] == "tables":
            namespace = parts[4]
            if len(parts) == 6:
                if method == "GET":
                    return _iceberg_list_tables(namespace, allow_cross_region)
                if method == "POST":
                    data = json.loads(body) if body else {}
                    return _iceberg_create_table(namespace, data, allow_cross_region)
            if len(parts) == 7:
                table_name = parts[6]
                if method == "GET":
                    return _iceberg_load_table(namespace, table_name, allow_cross_region)
                if method == "POST":
                    data = json.loads(body) if body else {}
                    return _iceberg_commit_table(namespace, table_name, data, allow_cross_region)
                if method == "HEAD":
                    if _iceberg_values(
                        _tables,
                        lambda table: _namespace_name(table) == namespace and table["name"] == table_name,
                        allow_cross_region,
                    ):
                        return 200, {}, b""
                    return 404, {}, b""
    return None


# ── S3 Tables control plane REST router ────────────────────

async def handle_request(method, path, headers, body, query_params):
    # Iceberg REST catalog
    if path.startswith("/iceberg"):
        return await _handle_iceberg_request(method, path, headers, body, query_params)

    data = json.loads(body) if body else {}
    clean = path.rstrip("/")
    parts = [unquote(p) for p in clean.split("/") if p]

    # PUT /buckets -> CreateTableBucket
    # GET /buckets -> ListTableBuckets
    if parts == ["buckets"]:
        if method == "PUT":
            return _create_table_bucket(data)
        if method == "GET":
            return _list_table_buckets()

    # GET|DELETE /buckets/{arn...} -> GetTableBucket|DeleteTableBucket
    if len(parts) >= 2 and parts[0] == "buckets":
        arn = "/".join(parts[1:])
        if not arn.startswith("arn:"):
            arn = f"arn:aws:s3tables:{get_region()}:{get_account_id()}:bucket/{arn}"
        # Check for sub-resource paths
        if parts[-1] in ("encryption", "maintenance", "metrics", "policy", "storage-class"):
            return json_response({})  # stub
        if method == "GET":
            return _get_table_bucket(arn)
        if method == "DELETE":
            return _delete_table_bucket(arn)

    # PUT /namespaces/{arn...} -> CreateNamespace
    # GET /namespaces/{arn...} -> ListNamespaces
    # GET /namespaces/{arn...}/{namespace} -> GetNamespace
    # DELETE /namespaces/{arn...}/{namespace} -> DeleteNamespace
    if len(parts) >= 2 and parts[0] == "namespaces":
        # The ARN is URL-encoded and contains slashes, so we need to reconstruct it
        # Pattern: /namespaces/arn:aws:s3tables:region:account:bucket/name[/namespace]
        remaining = "/".join(parts[1:])
        # Try to split ARN from namespace: ARN ends at "bucket/name"
        # arn:aws:s3tables:region:account:bucket/bucketname
        arn, namespace = _split_arn_and_suffix(remaining, "bucket")
        if namespace:
            if method == "GET":
                return _get_namespace(arn, namespace)
            if method == "DELETE":
                return _delete_namespace(arn, namespace)
        else:
            if method == "PUT":
                return _create_namespace(arn, data)
            if method == "GET":
                return _list_namespaces(arn)

    # PUT /tables/{arn...}/{namespace} -> CreateTable
    # GET /tables/{arn...} -> ListTables
    # DELETE /tables/{arn...}/{namespace}/{name} -> DeleteTable
    # GET /tables/{arn...}/{namespace}/{name}/metadata-location -> GetTableMetadataLocation
    # PUT /tables/{arn...}/{namespace}/{name}/metadata-location -> UpdateTableMetadataLocation
    if len(parts) >= 2 and parts[0] == "tables":
        remaining = "/".join(parts[1:])
        arn, suffix = _split_arn_and_suffix(remaining, "bucket")
        if not suffix:
            # GET /tables/{arn} -> ListTables
            namespace = query_params.get("namespace", [""])[0] if isinstance(query_params.get("namespace"), list) else query_params.get("namespace", "")
            return _list_tables(arn, namespace or None)
        # suffix could be "namespace", "namespace/table", or "namespace/table/metadata-location"
        suffix_parts = suffix.split("/")
        if len(suffix_parts) == 1:
            # PUT /tables/{arn}/{namespace} -> CreateTable
            if method == "PUT":
                return _create_table(arn, suffix_parts[0], data)
        elif len(suffix_parts) == 2:
            # DELETE /tables/{arn}/{namespace}/{name}
            if method == "DELETE":
                return _delete_table(arn, suffix_parts[0], suffix_parts[1])
        elif len(suffix_parts) == 3 and suffix_parts[2] == "metadata-location":
            if method == "GET":
                return _get_table_metadata_location(arn, suffix_parts[0], suffix_parts[1])
            if method == "PUT":
                return _update_table_metadata_location(arn, suffix_parts[0], suffix_parts[1], data)

    # GET /get-table?tableBucketARN=&namespace=&name= -> GetTable
    if parts == ["get-table"] and method == "GET":
        def _qp(name):
            v = query_params.get(name, [""])[0] if isinstance(query_params.get(name), list) else query_params.get(name, "")
            return v
        return _get_table(_qp("tableBucketARN"), _qp("namespace"), _qp("name"))

    return error_response_json("UnknownOperationException",
                                f"Unknown S3Tables operation: {method} {path}", 400)


def _split_arn_and_suffix(path_str, resource_type):
    """Split 'arn:aws:s3tables:region:account:bucket/name/extra/stuff' into (arn, 'extra/stuff').

    The ARN pattern is: arn:aws:s3tables:{region}:{account}:{resource_type}/{name}
    Everything after the resource name is the suffix.
    """
    # Find the ARN prefix pattern
    idx = path_str.find(f":{resource_type}/")
    if idx == -1:
        # Maybe the whole thing is an ARN with no suffix
        if f":{resource_type}/" in path_str or path_str.endswith(f":{resource_type}"):
            return path_str, ""
        return path_str, ""

    # arn:...:bucket/name — find the end of the bucket name
    after_type = path_str[idx + len(f":{resource_type}/"):]
    # The bucket name is the next segment before any '/'
    slash_idx = after_type.find("/")
    if slash_idx == -1:
        # No suffix, whole thing is the ARN
        return path_str, ""
    arn = path_str[:idx + len(f":{resource_type}/") + slash_idx]
    suffix = after_type[slash_idx + 1:]
    return arn, suffix
