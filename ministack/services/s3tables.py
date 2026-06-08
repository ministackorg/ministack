"""
S3 Tables Service Emulator.

Provides the ``s3tables`` control plane: table buckets, namespaces, and tables
in Apache Iceberg format.  Data files are stored in MiniStack's S3 service;
table metadata (schemas, snapshots, manifests) is held in memory.

The Iceberg REST catalog data plane (``/iceberg/v1/*``) is served by
``services/iceberg_rest.py`` since v1.3.60. It reads from this module's
``_namespaces`` and ``_tables`` for s3tables-backed Iceberg tables, and
falls back to Glue table parameters for the Glue-backed Iceberg path
that DuckDB's iceberg extension uses.

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
import time
from urllib.parse import unquote

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    now_iso,
)

logger = logging.getLogger("s3tables")

# ── In-memory state ────────────────────────────────────────

_table_buckets = AccountScopedDict()
_namespaces = AccountScopedDict()        # "bucket_arn\x00namespace" -> ns dict
_tables = AccountScopedDict()            # "bucket_arn\x00namespace\x00table" -> table dict


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


def _ns_key(bucket_arn, namespace):
    return f"{bucket_arn}\x00{namespace}"


def _table_key(bucket_arn, namespace, table_name):
    return f"{bucket_arn}\x00{namespace}\x00{table_name}"


def _find_bucket_by_arn(arn):
    for b in _table_buckets.values():
        if b["arn"] == arn:
            return b
    return None


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
        return error_response_json("NotFoundException", f"Table bucket not found: {arn}", 404)
    return json_response(bucket)


def _delete_table_bucket(arn):
    name = None
    for n, b in _table_buckets.items():
        if b["arn"] == arn:
            name = n
            break
    if not name:
        return error_response_json("NotFoundException", f"Table bucket not found: {arn}", 404)
    for key in list(_tables.keys()):
        if key.startswith(arn + "\x00"):
            del _tables[key]
    for key in list(_namespaces.keys()):
        if key.startswith(arn + "\x00"):
            del _namespaces[key]
    del _table_buckets[name]
    return json_response({})


def _create_namespace(bucket_arn, data):
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
    result = [ns for ns in _namespaces.values() if ns.get("tableBucketARN") == bucket_arn]
    return json_response({"namespaces": result})


def _get_namespace(bucket_arn, namespace):
    key = _ns_key(bucket_arn, namespace)
    ns = _namespaces.get(key)
    if not ns:
        return error_response_json("NotFoundException", f"Namespace {namespace} not found", 404)
    return json_response(ns)


def _delete_namespace(bucket_arn, namespace):
    key = _ns_key(bucket_arn, namespace)
    if key not in _namespaces:
        return error_response_json("NotFoundException", f"Namespace {namespace} not found", 404)
    del _namespaces[key]
    return json_response({})


def _create_table(bucket_arn, namespace, data):
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
    key = _table_key(bucket_arn, namespace, table_name)
    table = _tables.get(key)
    if not table:
        return error_response_json("NotFoundException", f"Table {table_name} not found", 404)
    return json_response({k: v for k, v in table.items() if not k.startswith("_")})


def _delete_table(bucket_arn, namespace, table_name):
    key = _table_key(bucket_arn, namespace, table_name)
    if key not in _tables:
        return error_response_json("NotFoundException", f"Table {table_name} not found", 404)
    del _tables[key]
    return json_response({})


def _get_table_metadata_location(bucket_arn, namespace, table_name):
    key = _table_key(bucket_arn, namespace, table_name)
    table = _tables.get(key)
    if not table:
        return error_response_json("NotFoundException", f"Table {table_name} not found", 404)
    return json_response({"metadataLocation": table["metadataLocation"],
                           "versionToken": new_uuid()[:8]})


def _update_table_metadata_location(bucket_arn, namespace, table_name, data):
    key = _table_key(bucket_arn, namespace, table_name)
    table = _tables.get(key)
    if not table:
        return error_response_json("NotFoundException", f"Table {table_name} not found", 404)
    new_loc = data.get("metadataLocation", "")
    table["metadataLocation"] = new_loc
    table["modifiedAt"] = now_iso()
    return json_response({"metadataLocation": new_loc, "name": table_name,
                           "versionToken": new_uuid()[:8]})


# Iceberg REST data plane lives in ``services/iceberg_rest.py`` since
# v1.3.60. It reaches into the s3tables in-memory dicts (``_namespaces``,
# ``_tables``) directly for s3tables-backed Iceberg tables, and looks
# up Glue tables by ``Parameters["metadata_location"]`` for the
# Glue-backed path the DuckDB iceberg extension uses.


# ── S3 Tables control plane REST router ────────────────────

async def handle_request(method, path, headers, body, query_params):
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
