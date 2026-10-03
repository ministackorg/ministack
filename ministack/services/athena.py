# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Athena Service Emulator.
JSON-based API via X-Amz-Target (AmazonAthena).
Uses DuckDB to actually execute SQL queries against S3 data (CSV/JSON/Parquet).
Supports: StartQueryExecution, GetQueryExecution, GetQueryResults,
          StopQueryExecution, ListQueryExecutions,
          CreateWorkGroup, DeleteWorkGroup, GetWorkGroup, ListWorkGroups, UpdateWorkGroup,
          CreateNamedQuery, DeleteNamedQuery, GetNamedQuery, ListNamedQueries,
          BatchGetNamedQuery, BatchGetQueryExecution,
          CreateDataCatalog, GetDataCatalog, ListDataCatalogs, DeleteDataCatalog, UpdateDataCatalog,
          CreatePreparedStatement, GetPreparedStatement, DeletePreparedStatement, ListPreparedStatements,
          GetTableMetadata, ListTableMetadata,
          TagResource, UntagResource, ListTagsForResource.
"""
import asyncio
import copy
import csv
import glob
import io
import json
import logging
import os
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from urllib.parse import urlparse

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    set_request_region,
)

logger = logging.getLogger("athena")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
S3_DATA_DIR = os.environ.get("S3_DATA_DIR", "/tmp/ministack-data/s3")
ATHENA_ENGINE = os.environ.get("ATHENA_ENGINE", "auto")  # "auto" | "duckdb" | "mock"
ATHENA_DATA_DIR = S3_DATA_DIR


def get_athena_engine():
    """Resolve the effective SQL engine. Reads module-level ATHENA_ENGINE which
    can be overridden at runtime via POST /_ministack/config."""
    engine = ATHENA_ENGINE
    if engine == "auto":
        engine = "duckdb" if _duckdb_available else "mock"
    logger.debug("Athena engine: %s (ATHENA_ENGINE=%s)", engine, ATHENA_ENGINE)
    return engine


_executions = AccountRegionScopedDict()
# Per-account-and-region workgroups / data catalogs. AWS's "primary" workgroup
# and "AwsDataCatalog" exist in every region — we lazily seed them per scope on
# first access so requests never share workgroup or catalog state.
_workgroups = AccountRegionScopedDict()
_named_queries = AccountRegionScopedDict()
_data_catalogs = AccountRegionScopedDict()


def _ensure_default_workgroup():
    if "primary" not in _workgroups:
        _workgroups["primary"] = {
            "Name": "primary",
            "State": "ENABLED",
            "Description": "Primary workgroup",
            "CreationTime": int(time.time()),
            "Configuration": {
                "ResultConfiguration": {"OutputLocation": "s3://athena-results/"},
                # AUTO is the default selection; the effective version is what Athena resolved it to.
                # https://docs.aws.amazon.com/athena/latest/APIReference/API_EngineVersion.html
                "EngineVersion": {
                    "SelectedEngineVersion": "AUTO",
                    "EffectiveEngineVersion": "Athena engine version 3",
                },
            },
        }


def _ensure_default_data_catalog():
    if "AwsDataCatalog" not in _data_catalogs:
        _data_catalogs["AwsDataCatalog"] = {
            "Name": "AwsDataCatalog",
            "Description": "AWS Glue Data Catalog",
            "Type": "GLUE",
            "Parameters": {},
        }
_prepared_statements = AccountRegionScopedDict()  # "workgroup/name" -> statement dict
_tags = AccountScopedDict()  # arn -> {key: value, ...}


def get_state():
    return copy.deepcopy(
        {
            "_executions": _executions,
            "_workgroups": _workgroups,
            "_named_queries": _named_queries,
            "_data_catalogs": _data_catalogs,
            "_prepared_statements": _prepared_statements,
            "_tags": _tags,
        }
    )


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    # Scoped dicts are mutated in-place — no module-level reassignment.
    _executions.clear()
    _workgroups.clear()
    _named_queries.clear()
    _data_catalogs.clear()
    _prepared_statements.clear()
    _tags.clear()

    legacy_workgroup_regions = _restore_regional_store(
        _workgroups, data.get("_workgroups", {})
    )
    for store, key, workgroup_fields in (
        (_executions, "_executions", ("WorkGroup",)),
        (_named_queries, "_named_queries", ("WorkGroup",)),
        (
            _prepared_statements,
            "_prepared_statements",
            ("WorkGroupName", "WorkGroup"),
        ),
    ):
        _restore_workgroup_child_store(
            store,
            data.get(key, {}),
            legacy_workgroup_regions,
            workgroup_fields,
        )
    _restore_regional_store(_data_catalogs, data.get("_data_catalogs", {}))
    _tags.update(data.get("_tags", {}))


def _legacy_region_for_value(store, key, value):
    """Infer an ARN region, falling back to Athena's configured boot region."""
    request_region = get_region()
    try:
        set_request_region(REGION)
        return store._region_for_legacy_value(key, value)
    finally:
        set_request_region(request_region)


def _restore_regional_store(store, restored):
    """Restore current state verbatim and legacy state deterministically."""
    if isinstance(restored, AccountRegionScopedDict):
        store.update(restored)
        return {}

    restored_regions = {}
    if isinstance(restored, AccountScopedDict):
        items = restored._data.items()
    else:
        account_id = get_account_id()
        items = (((account_id, key), value) for key, value in restored.items())

    for (account_id, key), value in items:
        region = _legacy_region_for_value(store, key, value)
        store.set_scoped(account_id, region, key, value)
        restored_regions[(account_id, key)] = region
    return restored_regions


def _restore_workgroup_child_store(
    store, restored, workgroup_regions, workgroup_fields
):
    """Place legacy child records beside their referenced workgroup."""
    if isinstance(restored, AccountRegionScopedDict):
        store.update(restored)
        return

    if isinstance(restored, AccountScopedDict):
        items = restored._data.items()
    else:
        account_id = get_account_id()
        items = (((account_id, key), value) for key, value in restored.items())

    for (account_id, key), value in items:
        workgroup = next(
            (value.get(field) for field in workgroup_fields if value.get(field)),
            None,
        )
        if not workgroup and store is _prepared_statements:
            workgroup = str(key).partition("/")[0]
        workgroup = workgroup or "primary"
        region = workgroup_regions.get((account_id, workgroup))
        if region is None:
            region = _legacy_region_for_value(store, key, value)
        store.set_scoped(account_id, region, key, value)



try:
    import duckdb  # noqa: F401 — the import is the availability probe

    _duckdb_available = True
except ImportError:
    _duckdb_available = False



_DUCKDB_TYPE_MAP = {
    "BOOLEAN": "boolean",
    "TINYINT": "tinyint",
    "SMALLINT": "smallint",
    "INTEGER": "integer",
    "INT": "integer",
    "BIGINT": "bigint",
    "HUGEINT": "bigint",
    "FLOAT": "float",
    "REAL": "float",
    "DOUBLE": "double",
    "DECIMAL": "decimal",
    "VARCHAR": "varchar",
    "BLOB": "varbinary",
    "DATE": "date",
    "TIME": "time",
    "TIMESTAMP": "timestamp",
    "TIMESTAMP WITH TIME ZONE": "timestamp",
    "INTERVAL": "varchar",
    "LIST": "array",
    "STRUCT": "row",
    "MAP": "map",
}


def _arn_workgroup(name):
    return f"arn:aws:athena:{get_region()}:{get_account_id()}:workgroup/{name}"


def _arn_datacatalog(name):
    return f"arn:aws:athena:{get_region()}:{get_account_id()}:datacatalog/{name}"


def _is_taggable_athena_resource(resource):
    for prefix in ("workgroup/", "datacatalog/"):
        if resource.startswith(prefix):
            name = resource[len(prefix):]
            return bool(name) and "/" not in name
    return False


def _validate_tag_resource_arn(arn):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return error_response_json(
            "InvalidRequestException",
            f"Invalid ResourceARN: {arn}",
            400,
        )
    if (
        spec.partition != "aws"
        or spec.service != "athena"
        or spec.region != get_region()
        or spec.account_id != get_account_id()
        or not _is_taggable_athena_resource(spec.resource)
    ):
        return error_response_json(
            "InvalidRequestException",
            f"Invalid ResourceARN: {arn}",
            400,
        )
    return None


async def handle_request(method, path, headers, body, query_params):
    # AWS pre-provisions "primary" workgroup + "AwsDataCatalog" in every
    # account. Seed them lazily per-tenant on first access.
    _ensure_default_workgroup()
    _ensure_default_data_catalog()

    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "StartQueryExecution": _start_query_execution,
        "GetQueryExecution": _get_query_execution,
        "GetQueryResults": _get_query_results,
        "StopQueryExecution": _stop_query_execution,
        "ListQueryExecutions": _list_query_executions,
        "CreateWorkGroup": _create_workgroup,
        "DeleteWorkGroup": _delete_workgroup,
        "GetWorkGroup": _get_workgroup,
        "ListWorkGroups": _list_workgroups,
        "UpdateWorkGroup": _update_workgroup,
        "CreateNamedQuery": _create_named_query,
        "DeleteNamedQuery": _delete_named_query,
        "GetNamedQuery": _get_named_query,
        "ListNamedQueries": _list_named_queries,
        "BatchGetNamedQuery": _batch_get_named_query,
        "BatchGetQueryExecution": _batch_get_query_execution,
        # Data Catalogs
        "CreateDataCatalog": _create_data_catalog,
        "GetDataCatalog": _get_data_catalog,
        "ListDataCatalogs": _list_data_catalogs,
        "DeleteDataCatalog": _delete_data_catalog,
        "UpdateDataCatalog": _update_data_catalog,
        # Prepared Statements
        "CreatePreparedStatement": _create_prepared_statement,
        "GetPreparedStatement": _get_prepared_statement,
        "DeletePreparedStatement": _delete_prepared_statement,
        "ListPreparedStatements": _list_prepared_statements,
        # Databases and Table Metadata (Glue Data Catalog)
        "GetDatabase": _get_database,
        "ListDatabases": _list_databases,
        "GetTableMetadata": _get_table_metadata,
        "ListTableMetadata": _list_table_metadata,
        # Tags
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsForResource": _list_tags_for_resource,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json(
            "InvalidAction", f"Unknown Athena action: {action}", 400
        )
    return handler(data)


# ---- SQL scanning ----

_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_NUMBER_RE = re.compile(r"[0-9]+")


def _sql_tokens(sql):
    """``(kind, text, start, end)`` per token: ``name`` (lower-cased), ``qname``, ``str``, ``number``, ``punct``."""
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch.isspace():
            i += 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j + 1
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
        elif ch in "'\"`":
            j = i + 1
            while j < n and not (sql[j] == ch and sql[j + 1:j + 2] != ch):
                j += 2 if sql[j] == ch else 1
            end = min(j + 1, n)
            if ch == "'":
                yield "str", sql[i:end], i, end
            else:
                yield "qname", sql[i + 1:j].replace(ch * 2, ch), i, end
            i = end
        elif m := _NAME_RE.match(sql, i):
            yield "name", m.group(0).lower(), i, m.end()
            i = m.end()
        elif m := _NUMBER_RE.match(sql, i):
            yield "number", m.group(0), i, m.end()
            i = m.end()
        else:
            yield "punct", ch, i, i + 1
            i += 1


class _Cursor:
    def __init__(self, text):
        self.text = text
        self.tokens = list(_sql_tokens(text))
        self.i = 0

    def reset(self):
        self.i = 0

    def at_end(self):
        return self.i >= len(self.tokens)

    def peek(self):
        return self.tokens[self.i] if self.i < len(self.tokens) else (None, None, None, None)

    def advance(self):
        self.i += 1

    def keywords(self, *words):
        """Consume the bare-name keyword run ``words`` if it is next; else leave the cursor."""
        for offset, word in enumerate(words):
            if self.i + offset >= len(self.tokens) or self.tokens[self.i + offset][:2] != ("name", word):
                return False
        self.i += len(words)
        return True

    def punct(self, char):
        if self.peek()[:2] == ("punct", char):
            self.advance()
            return True
        return False

    def skip_group(self):
        """After ``punct("(")``: consume through the matching ``)``."""
        depth = 1
        while not self.at_end() and depth > 0:
            if self.punct("("):
                depth += 1
            elif self.punct(")"):
                depth -= 1
            else:
                self.advance()

    def name(self):
        kind, text = self.peek()[:2]
        if kind == "name":
            self.advance()
            return text
        return None

    def identifier(self):
        """A bare (lower-cased) or quoted (verbatim) identifier."""
        kind, text = self.peek()[:2]
        if kind in ("name", "qname"):
            self.advance()
            return text
        return None

    def number(self):
        kind, text = self.peek()[:2]
        if kind == "number":
            self.advance()
            return int(text)
        return None

    def string(self):
        """A ``'...'`` literal's value: ``''`` is a quote, and Hive's ``\\t``, ``\\n`` and ``\\\\`` are unescaped."""
        kind, text = self.peek()[:2]
        if kind != "str" or len(text) < 2 or not text.endswith("'"):
            return None
        self.advance()
        value = text[1:-1].replace("''", "'")
        return re.sub(r"\\(.)", lambda m: {"t": "\t", "n": "\n"}.get(m.group(1), m.group(1)), value)

    def dotted_name(self, max_parts):
        """The next dotted name, right-aligned in a ``max_parts`` tuple (``t`` → ``(None, None, "t")``)."""
        parts = []
        while True:
            part = self.identifier()
            if part is None:
                return None
            parts.append(part)
            if not self.punct("."):
                break
        if len(parts) > max_parts:
            return None
        return (None,) * (max_parts - len(parts)) + tuple(parts)


@dataclass(frozen=True)
class _TableReference:
    """One ``FROM``/``JOIN`` target and where its text sits in the query."""

    database: str | None  # None when the reference names only the table
    table: str
    start: int
    end: int
    aliased: bool


# After a table reference, these keywords start the next clause; anything else is an alias.
_CLAUSE_KEYWORDS = frozenset(
    "where on join inner left right full cross natural using group order limit offset having "
    "union except intersect window qualify fetch with".split()
)


def _table_references(query):
    """The ``FROM``/``JOIN`` targets of ``query``, in order; CTE names shadow tables."""
    cursor = _Cursor(query)
    ctes = _cte_names(cursor)
    cursor.reset()
    references = []
    while not cursor.at_end():
        if not (cursor.keywords("from") or cursor.keywords("join")):
            cursor.advance()
            continue
        start = cursor.peek()[2]
        name = cursor.dotted_name(3)
        if name is None:
            continue
        _, database, table = name  # the catalog qualifier is dropped: Glue is the only catalog here
        if database is None and table in ctes:
            continue
        end = cursor.tokens[cursor.i - 1][3]
        next_kind, next_text = cursor.peek()[:2]
        aliased = next_kind == "qname" or (next_kind == "name" and next_text not in _CLAUSE_KEYWORDS)
        references.append(_TableReference(database, table, start, end, aliased))
    return references


def _cte_names(cursor):
    """Names bound by a top-level ``WITH name AS (...)``."""
    names = set()
    if not cursor.keywords("with"):
        return names
    while True:
        name = cursor.identifier()
        if name is None or not cursor.keywords("as") or not cursor.punct("("):
            return names
        names.add(name)
        cursor.skip_group()
        if not cursor.punct(","):
            return names


# ---- Table DDL ----

_TEXT_INPUT_FORMAT = "org.apache.hadoop.mapred.TextInputFormat"
_LAZY_SIMPLE_SERDE = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
# STORED AS <format> → (InputFormat, OutputFormat, SerDe), as Athena writes them to Glue.
_STORED_AS = {
    "textfile": (_TEXT_INPUT_FORMAT, "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat", _LAZY_SIMPLE_SERDE),
    "parquet": (
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    ),
    "orc": (
        "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
        "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
    ),
    "avro": (
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
        "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
        "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
    ),
}
# A ROW FORMAT SERDE table with no STORED AS is text written through IgnoreKeyTextOutputFormat.
_SERDE_TEXT_OUTPUT_FORMAT = "org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat"
# ROW FORMAT DELIMITED <clause> TERMINATED BY / DEFINED AS 'c' → SerDe parameter.
_DELIMITED = {
    ("fields", "terminated", "by"): "field.delim",
    ("escaped", "by"): "escape.delim",
    ("collection", "items", "terminated", "by"): "collection.delim",
    ("map", "keys", "terminated", "by"): "mapkey.delim",
    ("lines", "terminated", "by"): "line.delim",
    ("null", "defined", "as"): "serialization.null.format",
}


@dataclass(frozen=True)
class _CreateTable:
    database: str | None  # None when the statement names only the table
    table: str
    external: bool
    columns: list = field(default_factory=list)  # Glue column dicts: Name, Type, optional Comment
    partition_keys: list = field(default_factory=list)
    comment: str | None = None
    location: str | None = None
    input_format: str = _TEXT_INPUT_FORMAT
    output_format: str = _STORED_AS["textfile"][1]
    serde: str = _LAZY_SIMPLE_SERDE
    serde_parameters: dict = field(default_factory=lambda: {"serialization.format": "1"})
    table_properties: dict = field(default_factory=dict)
    bucket_columns: list = field(default_factory=list)
    number_of_buckets: int = -1
    if_not_exists: bool = False

    @property
    def iceberg(self):
        return self.table_properties.get("table_type", "").lower() == "iceberg"


@dataclass(frozen=True)
class _DropTable:
    database: str | None
    table: str
    if_exists: bool = False


def _parse_ddl(query):
    """The ``_CreateTable`` or ``_DropTable`` a statement is; None sends it to the query engine."""
    cursor = _Cursor(query)
    if cursor.tokens and cursor.tokens[-1][:2] == ("punct", ";"):
        cursor.tokens.pop()
    if any(token[:2] == ("punct", ";") for token in cursor.tokens):
        return None
    if cursor.keywords("drop", "table"):
        if_exists = cursor.keywords("if", "exists")
        name = cursor.dotted_name(2)
        if name is None or not cursor.at_end():
            return None
        return _DropTable(*name, if_exists=if_exists)
    if not cursor.keywords("create"):
        return None
    external = cursor.keywords("external")
    if not cursor.keywords("table"):
        return None
    if_not_exists = cursor.keywords("if", "not", "exists")
    name = cursor.dotted_name(2)
    columns = _column_list(cursor) if name else []
    if not columns:
        return None
    clauses = {}
    stored_as = row_format_serde = None
    serde_parameters = {"serialization.format": "1"}
    while not cursor.at_end():
        if cursor.keywords("comment"):
            clauses["comment"] = cursor.string()
        elif cursor.keywords("partitioned", "by"):
            clauses["partition_keys"] = _column_list(cursor) or None
        elif cursor.keywords("clustered", "by"):
            bucket_columns = _name_list(cursor)
            buckets = cursor.number() if bucket_columns and cursor.keywords("into") else None
            if buckets is None or not cursor.keywords("buckets"):
                return None
            clauses["bucket_columns"], clauses["number_of_buckets"] = bucket_columns, buckets
        elif cursor.keywords("row", "format", "delimited"):
            delimited = _delimited(cursor)
            if delimited is None:
                return None
            serde_parameters.update(delimited)
        elif cursor.keywords("row", "format", "serde"):
            row_format_serde = cursor.string()
            if row_format_serde is None:
                return None
            if cursor.keywords("with", "serdeproperties"):
                properties = _property_list(cursor)
                if properties is None:
                    return None
                serde_parameters.update(properties)
        elif cursor.keywords("with", "serdeproperties"):
            properties = _property_list(cursor)
            if properties is None:
                return None
            serde_parameters.update(properties)
        elif cursor.keywords("stored", "as", "inputformat"):
            input_format = cursor.string()
            output_format = cursor.string() if cursor.keywords("outputformat") else None
            if input_format is None or output_format is None:
                return None
            stored_as = (input_format, output_format, None)
        elif cursor.keywords("stored", "as"):
            stored_as = _STORED_AS.get(cursor.name() or "")
            if stored_as is None:
                return None
        elif cursor.keywords("location"):
            clauses["location"] = cursor.string()
        elif cursor.keywords("tblproperties"):
            clauses["table_properties"] = _property_list(cursor)
        else:
            return None
        if any(value is None for value in clauses.values()):
            return None
    if stored_as is None:
        input_format, output_format, serde = _STORED_AS["textfile"]
        if row_format_serde:
            output_format = _SERDE_TEXT_OUTPUT_FORMAT
    else:
        input_format, output_format, serde = stored_as
    return _CreateTable(
        *name, external=external, columns=columns, input_format=input_format, output_format=output_format,
        serde=row_format_serde or serde or _LAZY_SIMPLE_SERDE, serde_parameters=serde_parameters,
        if_not_exists=if_not_exists, **clauses,
    )


def _column_list(cursor):
    """``( name type [COMMENT 'text'] , … )`` → Glue column dicts; [] when malformed."""
    if not cursor.punct("("):
        return []
    columns = []
    while True:
        name = cursor.identifier()
        if name is None:
            return []
        type_start = type_end = None
        comment = None
        depth = 0
        while not cursor.at_end():
            kind, text, start, end = cursor.peek()
            if depth == 0 and kind == "punct" and text in ",)":
                break
            if depth == 0 and kind == "name" and text == "comment":
                cursor.advance()
                comment = cursor.string()
                if comment is None:
                    return []
                continue
            if kind == "punct" and text in "(<":
                depth += 1
            elif kind == "punct" and text in ")>":
                depth -= 1
            if type_start is None:
                type_start = start
            type_end = end
            cursor.advance()
        if type_start is None:
            return []
        column = {"Name": name, "Type": cursor.text[type_start:type_end].lower()}
        if comment is not None:
            column["Comment"] = comment
        columns.append(column)
        if cursor.punct(","):
            continue
        if cursor.punct(")"):
            return columns
        return []


def _name_list(cursor):
    """``( a , b )`` → ``["a", "b"]``; [] when malformed."""
    if not cursor.punct("("):
        return []
    names = []
    while (name := cursor.identifier()) is not None:
        names.append(name)
        if cursor.punct(")"):
            return names
        if not cursor.punct(","):
            return []
    return []


def _property_list(cursor):
    """``( 'k' = 'v' , … )`` → dict; None when malformed."""
    if not cursor.punct("("):
        return None
    properties = {}
    while True:
        key = cursor.string()
        value = cursor.string() if key is not None and cursor.punct("=") else None
        if value is None:
            return None
        properties[key] = value
        if cursor.punct(")"):
            return properties
        if not cursor.punct(","):
            return None


def _delimited(cursor):
    """The SerDe parameters a ``ROW FORMAT DELIMITED`` clause sets; None when malformed."""
    parameters = {}
    while True:
        for words, parameter in _DELIMITED.items():
            if cursor.keywords(*words):
                value = cursor.string()
                if value is None:
                    return None
                parameters[parameter] = value
                break
        else:
            break
    if "field.delim" in parameters:
        parameters["serialization.format"] = parameters["field.delim"]
    return parameters


def _create_table_input(ddl):
    """The Glue ``TableInput`` Athena writes for ``ddl``."""
    parameters = {"EXTERNAL": "TRUE", "transient_lastDdlTime": str(int(time.time())), **ddl.table_properties}
    if ddl.comment is not None:
        parameters["comment"] = ddl.comment
    storage = {
        "Columns": ddl.columns,
        "InputFormat": ddl.input_format,
        "OutputFormat": ddl.output_format,
        "Compressed": False,
        "NumberOfBuckets": ddl.number_of_buckets,
        "SerdeInfo": {"SerializationLibrary": ddl.serde, "Parameters": ddl.serde_parameters},
        "BucketColumns": ddl.bucket_columns,
        "SortColumns": [],
        "Parameters": {},
        "SkewedInfo": {"SkewedColumnNames": [], "SkewedColumnValues": [], "SkewedColumnValueLocationMaps": {}},
        "StoredAsSubDirectories": False,
    }
    if ddl.location:
        storage["Location"] = ddl.location.rstrip("/")
    return {
        "Name": ddl.table,
        "Owner": "hadoop",
        "TableType": "EXTERNAL_TABLE",
        "Parameters": parameters,
        "StorageDescriptor": storage,
        "PartitionKeys": ddl.partition_keys,
    }


def _data_format(table_data):
    """``classification`` when set, else the format its InputFormat or SerDe names, else ``csv``."""
    classification = (table_data.get("Parameters") or {}).get("classification")
    if classification:
        return classification
    storage = table_data.get("StorageDescriptor") or {}
    classes = f"{storage.get('InputFormat', '')} {(storage.get('SerdeInfo') or {}).get('SerializationLibrary', '')}".lower()
    return next((fmt for fmt in ("parquet", "orc", "avro", "json") if fmt in classes), "csv")


# ---- Query Execution ----


def _start_query_execution(data):
    query = data.get("QueryString", "")
    query_id = new_uuid()
    workgroup = data.get("WorkGroup", "primary")
    output_location = data.get("ResultConfiguration", {}).get(
        "OutputLocation"
    ) or _workgroups.get(workgroup, {}).get("Configuration", {}).get(
        "ResultConfiguration", {}
    ).get("OutputLocation", "s3://athena-results/")
    db = data.get("QueryExecutionContext", {}).get("Database", "default")
    catalog = data.get("QueryExecutionContext", {}).get("Catalog", "AwsDataCatalog")
    # Athena rejects any malformed statement here; with no Trino parser, only this DDL rule is
    # checked at submission and other errors fail the execution instead.
    ddl = _parse_ddl(query)
    if isinstance(ddl, _CreateTable) and not ddl.external and not ddl.iceberg:
        return error_response_json(
            "InvalidRequestException", "External keyword required for table type HIVE", 400,
            extra={"AthenaErrorCode": "MALFORMED_QUERY"},
        )

    execution = {
        "QueryExecutionId": query_id,
        "Query": query,
        "StatementType": _detect_statement_type(query),
        "ResultConfiguration": {"OutputLocation": f"{output_location}{query_id}.csv"},
        "QueryExecutionContext": {"Database": db, "Catalog": catalog},
        "Status": {
            "State": "QUEUED",
            "SubmissionDateTime": int(time.time()),
            "CompletionDateTime": None,
            "StateChangeReason": "",
        },
        "Statistics": {
            "EngineExecutionTimeInMillis": 0,
            "DataScannedInBytes": 0,
            "DataManifestLocation": "",
            "TotalExecutionTimeInMillis": 0,
            "QueryQueueTimeInMillis": 0,
            "QueryPlanningTimeInMillis": 0,
            "ServiceProcessingTimeInMillis": 0,
        },
        "WorkGroup": workgroup,
        "EngineVersion": {
            "SelectedEngineVersion": "Athena engine version 3",
            "EffectiveEngineVersion": "Athena engine version 3",
        },
        "_results": None,
        "_column_types": None,
        "_error": None,
    }
    _executions[query_id] = execution

    asyncio.create_task(_execute_query(query_id, query, db, ddl))

    return json_response({"QueryExecutionId": query_id})


async def _execute_query(query_id, query, database, ddl):
    execution = _executions.get(query_id)
    if not execution:
        return

    execution["Status"]["State"] = "RUNNING"
    start = time.time()

    try:
        engine = get_athena_engine()

        if ddl is not None:
            results = _run_ddl(ddl, database)
        elif engine == "duckdb":
            results = await _run_duckdb(query, database)
        else:
            results = _mock_query_results(query)

        execution["_results"] = {"columns": results["columns"], "rows": results["rows"]}
        execution["_column_types"] = results.get(
            "column_types", ["varchar"] * len(results["columns"])
        )
        execution["Status"]["State"] = "SUCCEEDED"
        elapsed_ms = int((time.time() - start) * 1000)
        execution["Statistics"]["EngineExecutionTimeInMillis"] = elapsed_ms
        execution["Statistics"]["TotalExecutionTimeInMillis"] = elapsed_ms + 50
        execution["Statistics"]["QueryPlanningTimeInMillis"] = min(20, elapsed_ms)
        execution["Statistics"]["ServiceProcessingTimeInMillis"] = 30
        execution["Statistics"]["DataScannedInBytes"] = sum(
            len(str(row)) for row in results.get("rows", [])
        )

        await _save_query_results(query_id)
    except Exception as e:
        logger.error("Athena query %s failed: %s", query_id, e)
        execution["Status"]["State"] = "FAILED"
        execution["Status"]["StateChangeReason"] = str(e)[:2000]
        execution["_error"] = str(e)

    execution["Status"]["CompletionDateTime"] = int(time.time())


async def _run_duckdb(query, database):
    """Run a DuckDB query off the event loop.

    DuckDB's ``conn.execute()`` is a blocking C-extension call — running it
    directly on the asyncio loop stalls every other in-flight request for
    the duration of the query. Offload to a worker thread via
    ``asyncio.to_thread`` so multiple concurrent Athena queries on the
    single-process server stay non-blocking.
    """
    import duckdb

    rewritten = await _rewrite_data_paths(query, database)

    def _execute_blocking():
        conn = duckdb.connect(":memory:")
        try:
            result = conn.execute(rewritten)
            columns = []
            column_types = []
            if result.description:
                for desc in result.description:
                    columns.append(desc[0])
                    raw_type = desc[1] if len(desc) > 1 else "VARCHAR"
                    if isinstance(raw_type, str):
                        type_key = raw_type.upper().split("(")[0].strip()
                    else:
                        type_key = str(raw_type).upper().split("(")[0].strip()
                    athena_type = _DUCKDB_TYPE_MAP.get(type_key, "varchar")
                    column_types.append(athena_type)
            rows = result.fetchall()
            return {
                "columns": columns,
                "column_types": column_types,
                "rows": [list(r) for r in rows],
            }
        finally:
            conn.close()

    return await asyncio.to_thread(_execute_blocking)


async def _rewrite_data_paths(query, database):
    """Replace each Glue table reference with a DuckDB relation over its local S3 data."""
    from ministack.services import glue as glue_svc

    account_id = get_account_id()
    edits = []
    for ref in _table_references(query):
        db_name = ref.database or database or "default"

        # Read directly from glue's internal store rather than going through
        # the HTTP handler. The store is account-scoped via AccountScopedDict
        # and reads the current request's contextvar, so multi-tenancy is
        # preserved without crafting synthetic Authorization headers.
        table_data = glue_svc._tables.get(f"{db_name}/{ref.table}")
        if not table_data:
            continue
        s3_location = (table_data.get("StorageDescriptor") or {}).get("Location")
        if not s3_location:
            continue

        p = urlparse(s3_location)
        stripped = f"{p.netloc}{p.path}".rstrip("/")
        local_dir = f"{ATHENA_DATA_DIR}/{account_id}/{stripped}"
        if next(glob.iglob(f"{local_dir}/**/*", recursive=True), None):
            relation = f"'{local_dir}/**/*.{_data_format(table_data)}'"  # DuckDB reads the files, or reports why not
        else:
            relation = _empty_relation(table_data)
        alias = "" if ref.aliased else f' AS "{ref.table}"'
        edits.append((ref.start, ref.end, relation + alias))

    for span_start, span_end, replacement in reversed(edits):
        query = query[:span_start] + replacement + query[span_end:]
    return _rewrite_s3_paths(query)


_DUCKDB_TYPE_BY_ATHENA = {
    "boolean": "BOOLEAN", "tinyint": "TINYINT", "smallint": "SMALLINT", "int": "INTEGER", "integer": "INTEGER",
    "bigint": "BIGINT", "float": "FLOAT", "real": "FLOAT", "double": "DOUBLE", "date": "DATE",
    "timestamp": "TIMESTAMP", "binary": "BLOB", "varbinary": "BLOB",
}


def _empty_relation(table_data):
    """Zero rows with the table's Glue columns and partition keys; complex types read as VARCHAR."""
    columns = ((table_data.get("StorageDescriptor") or {}).get("Columns") or []) + (table_data.get("PartitionKeys") or [])
    if not columns:
        return "(SELECT 1 WHERE FALSE)"
    projected = []
    for column in columns:
        base = str(column.get("Type", "string")).lower().split("(")[0].strip()
        duck_type = "DECIMAL" if base == "decimal" else _DUCKDB_TYPE_BY_ATHENA.get(base, "VARCHAR")
        name = str(column.get("Name", "")).replace('"', '""')
        projected.append(f'CAST(NULL AS {duck_type}) AS "{name}"')
    return f"(SELECT {', '.join(projected)} WHERE FALSE)"


def _run_ddl(ddl, database):
    from ministack.services import glue as glue_svc

    db_name = ddl.database or database or "default"
    if isinstance(ddl, _DropTable):
        if f"{db_name}/{ddl.table}" not in glue_svc._tables:
            if ddl.if_exists:
                return {"columns": [], "rows": [], "column_types": []}
            raise ValueError(f"Table '{db_name}.{ddl.table}' does not exist")
        glue_svc._delete_table({"DatabaseName": db_name, "Name": ddl.table})
        return {"columns": [], "rows": [], "column_types": []}
    if ddl.iceberg:
        raise ValueError("NOT_SUPPORTED: Iceberg tables are not supported")
    if db_name not in glue_svc._databases:
        raise ValueError(f"Schema '{db_name}' does not exist")
    if f"{db_name}/{ddl.table}" in glue_svc._tables:
        if ddl.if_not_exists:
            return {"columns": [], "rows": [], "column_types": []}
        raise ValueError(f"Table '{db_name}.{ddl.table}' already exists")
    status, _, body = glue_svc._create_table({"DatabaseName": db_name, "TableInput": _create_table_input(ddl)})
    if status >= 300:
        raise ValueError(json.loads(body).get("message", "Glue CreateTable failed"))
    return {"columns": [], "rows": [], "column_types": []}


def _rewrite_s3_paths(query):
    """Replace s3://bucket/key references with local file paths.
    Handles: quoted strings, read_csv/read_parquet/read_json function args,
    and FROM clauses with s3 paths.
    """

    def replace_s3(match):
        prefix = match.group(1)
        s3_uri = match.group(2)
        suffix = match.group(3)
        stripped = s3_uri
        if stripped.startswith("s3://"):
            stripped = stripped[5:]
        elif stripped.startswith("s3a://"):
            stripped = stripped[6:]
        parts = stripped.split("/", 1)
        account_id = get_account_id()
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        local_path = os.path.join(ATHENA_DATA_DIR, account_id, bucket, key)
        return f"{prefix}{local_path}{suffix}"

    result = re.sub(
        r"""(["'])(s3a?://[^"']+)(["'])""",
        replace_s3,
        query,
    )
    result = re.sub(
        r"(FROM\s+)(s3a?://\S+)(\s|;|$)",
        replace_s3,
        result,
        flags=re.IGNORECASE,
    )
    return result


async def _save_query_results(query_id):
    from ministack.services import s3 as s3_svc
    execution = _executions.get(query_id)
    results = execution.get('_results', [])

    output = io.StringIO()
    writer = csv.writer(output)
    # Real Athena writes the column names as the first row of the result
    # CSV; downstream consumers (Glue crawlers, csv readers using
    # header=True, BI tools) rely on it.
    writer.writerow(results['columns'])
    writer.writerows(results['rows'])
    csv_content = output.getvalue().encode("utf-8")

    col_types = execution.get('_column_types', [])
    metadata_text = ",".join(results['columns']) + "\n" + ",".join(col_types)
    metadata_content = metadata_text.encode("utf-8")

    output_location = execution["ResultConfiguration"]["OutputLocation"]
    p = urlparse(output_location)
    bucket_name = p.netloc
    key_prefix = p.path.lstrip("/").rstrip("/")
    # Athena writes <id>.csv and <id>.csv.metadata under the OutputLocation
    # prefix. If the prefix is empty (output_location == "s3://bucket/"),
    # the files land at bucket root.
    csv_key = f"{key_prefix}/{query_id}.csv" if key_prefix else f"{query_id}.csv"
    meta_key = f"{csv_key}.metadata"

    # Write directly to the S3 service's in-memory store via the public
    # internal helper, which is account-scoped through the request's
    # contextvar — no synthetic Authorization header needed and no
    # round-trip through the HTTP handler.
    def _upload(key, data, content_type):
        resp = s3_svc._put_object(
            bucket_name,
            key,
            data,
            {"content-type": content_type, "content-length": str(len(data))},
        )
        # _put_object returns (status, headers, body) on error; a successful
        # write returns the same tuple with status 200. Surface any failure
        # onto the execution so callers see the cause via GetQueryExecution.
        if isinstance(resp, tuple) and resp[0] >= 300:
            try:
                root = ET.fromstring(resp[2])
                code = root.findtext("Code", "UnknownError")
                message = root.findtext("Message", "An unexpected S3 error occurred")
            except Exception:
                code, message = "InternalError", "Failed to parse S3 error response"
            execution["Status"]["State"] = "FAILED"
            execution["Status"]["StateChangeReason"] = (
                f"An error occurred while writing to S3. {code}: {message}"
            )
            return False
        return True

    if _upload(csv_key, csv_content, "text/csv"):
        _upload(meta_key, metadata_content, "text/plain")


def _mock_query_results(query):
    query_upper = query.strip().upper()
    if query_upper.startswith("SELECT"):
        match = re.match(r"SELECT\s+'([^']*)'", query.strip(), re.IGNORECASE)
        if match:
            val = match.group(1)
            return {"columns": [val], "column_types": ["varchar"], "rows": [[val]]}
        alias_pattern = re.findall(
            r"""(?:(\d+(?:\.\d+)?)|'([^']*)')\s+AS\s+(\w+)""",
            query.strip(),
            re.IGNORECASE,
        )
        if alias_pattern:
            cols = [m[2] for m in alias_pattern]
            types = ["integer" if m[0] else "varchar" for m in alias_pattern]
            vals = [m[0] if m[0] else m[1] for m in alias_pattern]
            return {"columns": cols, "column_types": types, "rows": [vals]}
        return {
            "columns": ["result"],
            "column_types": ["varchar"],
            "rows": [["mock_value"]],
        }
    return {"columns": [], "column_types": [], "rows": []}


def _detect_statement_type(query):
    first = next((text for kind, text, _, _ in _sql_tokens(query) if kind != "punct" or text != "("), "")
    if first in ("select", "with", "insert", "delete", "update", "merge"):
        return "DML"
    if first in ("create", "drop", "alter"):
        return "DDL"
    return "UTILITY"


def _get_query_execution(data):
    query_id = data.get("QueryExecutionId")
    execution = _executions.get(query_id)
    if not execution:
        return error_response_json(
            "InvalidRequestException", f"Query {query_id} not found", 400
        )
    return json_response({"QueryExecution": _execution_out(execution)})


def _get_query_results(data):
    query_id = data.get("QueryExecutionId")
    max_results = data.get("MaxResults", 1000)
    next_token = data.get("NextToken")
    execution = _executions.get(query_id)
    if not execution:
        return error_response_json(
            "InvalidRequestException", f"Query {query_id} not found", 400
        )

    state = execution["Status"]["State"]
    if state == "FAILED":
        return error_response_json(
            "InvalidRequestException",
            f"Query has failed: {execution['Status'].get('StateChangeReason', 'Unknown')}",
            400,
        )
    if state != "SUCCEEDED":
        return error_response_json(
            "InvalidRequestException", f"Query is in state {state}", 400
        )

    results = execution.get("_results") or {"columns": [], "rows": []}
    columns = results.get("columns", [])
    rows = results.get("rows", [])
    column_types = execution.get("_column_types") or ["varchar"] * len(columns)

    start_idx = 0
    if next_token:
        try:
            start_idx = int(next_token)
        except ValueError:
            pass

    page_rows = rows[start_idx : start_idx + max_results]

    result_rows = []
    # Only a DML result's first page carries Athena's header row of column names.
    if execution.get("StatementType") == "DML" and start_idx == 0:
        result_rows.append({"Data": [{"VarCharValue": col} for col in columns]})
    for row in page_rows:
        result_rows.append(
            {"Data": [{"VarCharValue": str(v) if v is not None else ""} for v in row]}
        )

    column_info = []
    for i, col in enumerate(columns):
        ctype = column_types[i] if i < len(column_types) else "varchar"
        precision, scale = _type_precision_scale(ctype)
        column_info.append(
            {
                "CatalogName": "hive",
                "SchemaName": "",
                "TableName": "",
                "Name": col,
                "Label": col,
                "Type": ctype,
                "Precision": precision,
                "Scale": scale,
                "Nullable": "UNKNOWN",
                "CaseSensitive": ctype == "varchar",
            }
        )

    response = {
        "ResultSet": {
            "Rows": result_rows,
            "ResultSetMetadata": {"ColumnInfo": column_info},
        },
        "UpdateCount": 0,
    }

    end_idx = start_idx + max_results
    if end_idx < len(rows):
        response["NextToken"] = str(end_idx)

    return json_response(response)


def _type_precision_scale(athena_type):
    if athena_type in ("integer", "int"):
        return 10, 0
    if athena_type == "bigint":
        return 19, 0
    if athena_type == "smallint":
        return 5, 0
    if athena_type == "tinyint":
        return 3, 0
    if athena_type == "double":
        return 17, 0
    if athena_type == "float":
        return 7, 0
    if athena_type == "boolean":
        return 0, 0
    if athena_type == "decimal":
        return 38, 0
    return 0, 0


def _stop_query_execution(data):
    query_id = data.get("QueryExecutionId")
    execution = _executions.get(query_id)
    if execution and execution["Status"]["State"] in ("QUEUED", "RUNNING"):
        execution["Status"]["State"] = "CANCELLED"
        execution["Status"]["StateChangeReason"] = "Query was cancelled by user"
        execution["Status"]["CompletionDateTime"] = int(time.time())
    return json_response({})


def _list_query_executions(data):
    workgroup = data.get("WorkGroup", "primary")
    ids = [qid for qid, ex in _executions.items() if ex.get("WorkGroup") == workgroup]
    return json_response({"QueryExecutionIds": ids})


# ---- WorkGroups ----


def _create_workgroup(data):
    name = data.get("Name")
    if name in _workgroups:
        return error_response_json(
            "InvalidRequestException", f"WorkGroup {name} already exists", 400
        )
    _workgroups[name] = {
        "Name": name,
        "State": "ENABLED",
        "Description": data.get("Description", ""),
        "CreationTime": int(time.time()),
        "Configuration": data.get("Configuration", {}),
    }
    tags = data.get("Tags", [])
    if tags:
        arn = _arn_workgroup(name)
        _tags[arn] = {t["Key"]: t["Value"] for t in tags}
    return json_response({})


def _delete_workgroup(data):
    name = data.get("WorkGroup")
    if name == "primary":
        return error_response_json(
            "InvalidRequestException", "Cannot delete primary workgroup", 400
        )
    _workgroups.pop(name, None)
    _tags.pop(_arn_workgroup(name), None)
    return json_response({})


def _get_workgroup(data):
    name = data.get("WorkGroup")
    wg = _workgroups.get(name)
    if not wg:
        return error_response_json(
            "InvalidRequestException", f"WorkGroup {name} not found", 400
        )
    out = dict(wg)
    out.setdefault("WorkGroupConfiguration", out.get("Configuration", {}))
    return json_response({"WorkGroup": out})


def _list_workgroups(data):
    return json_response(
        {
            "WorkGroups": [
                {
                    "Name": wg["Name"],
                    "State": wg["State"],
                    "Description": wg.get("Description", ""),
                    "CreationTime": wg.get("CreationTime", 0),
                }
                for wg in _workgroups.values()
            ]
        }
    )


def _update_workgroup(data):
    name = data.get("WorkGroup")
    wg = _workgroups.get(name)
    if not wg:
        return error_response_json(
            "InvalidRequestException", f"WorkGroup {name} not found", 400
        )
    if "ConfigurationUpdates" in data:
        updates = data["ConfigurationUpdates"]
        config = wg.setdefault("Configuration", {})
        if "ResultConfigurationUpdates" in updates:
            rc = config.setdefault("ResultConfiguration", {})
            rcu = updates["ResultConfigurationUpdates"]
            if "OutputLocation" in rcu:
                rc["OutputLocation"] = rcu["OutputLocation"]
            if "EncryptionConfiguration" in rcu:
                rc["EncryptionConfiguration"] = rcu["EncryptionConfiguration"]
            if rcu.get("RemoveOutputLocation"):
                rc.pop("OutputLocation", None)
            if rcu.get("RemoveEncryptionConfiguration"):
                rc.pop("EncryptionConfiguration", None)
        for ck in (
            "EnforceWorkGroupConfiguration",
            "PublishCloudWatchMetricsEnabled",
            "BytesScannedCutoffPerQuery",
            "RequesterPaysEnabled",
            "EngineVersion",
        ):
            if ck in updates:
                config[ck] = updates[ck]
    if "Description" in data:
        wg["Description"] = data["Description"]
    if "State" in data:
        wg["State"] = data["State"]
    return json_response({})


# ---- Named Queries ----


def _create_named_query(data):
    query_id = new_uuid()
    _named_queries[query_id] = {
        "NamedQueryId": query_id,
        "Name": data.get("Name", ""),
        "Description": data.get("Description", ""),
        "Database": data.get("Database", "default"),
        "QueryString": data.get("QueryString", ""),
        "WorkGroup": data.get("WorkGroup", "primary"),
    }
    return json_response({"NamedQueryId": query_id})


def _delete_named_query(data):
    _named_queries.pop(data.get("NamedQueryId"), None)
    return json_response({})


def _get_named_query(data):
    query_id = data.get("NamedQueryId")
    nq = _named_queries.get(query_id)
    if not nq:
        return error_response_json(
            "InvalidRequestException", f"Named query {query_id} not found", 400
        )
    return json_response({"NamedQuery": nq})


def _list_named_queries(data):
    workgroup = data.get("WorkGroup")
    if workgroup:
        ids = [qid for qid, nq in _named_queries.items() if nq.get("WorkGroup") == workgroup]
    else:
        ids = list(_named_queries.keys())
    return json_response({"NamedQueryIds": ids})


def _batch_get_named_query(data):
    ids = data.get("NamedQueryIds", [])
    queries = [_named_queries[qid] for qid in ids if qid in _named_queries]
    unprocessed = [
        {
            "NamedQueryId": qid,
            "ErrorCode": "INTERNAL_FAILURE",
            "ErrorMessage": "Not found",
        }
        for qid in ids
        if qid not in _named_queries
    ]
    return json_response(
        {"NamedQueries": queries, "UnprocessedNamedQueryIds": unprocessed}
    )


def _batch_get_query_execution(data):
    ids = data.get("QueryExecutionIds", [])
    execs = [_execution_out(_executions[qid]) for qid in ids if qid in _executions]
    unprocessed = [
        {
            "QueryExecutionId": qid,
            "ErrorCode": "INTERNAL_FAILURE",
            "ErrorMessage": "Not found",
        }
        for qid in ids
        if qid not in _executions
    ]
    return json_response(
        {"QueryExecutions": execs, "UnprocessedQueryExecutionIds": unprocessed}
    )


# ---- Data Catalogs ----


def _create_data_catalog(data):
    name = data.get("Name")
    if not name:
        return error_response_json("InvalidRequestException", "Name is required", 400)
    if name in _data_catalogs:
        return error_response_json(
            "InvalidRequestException", f"Data catalog {name} already exists", 400
        )
    catalog_type = data.get("Type", "HIVE")
    if catalog_type not in ("HIVE", "LAMBDA", "GLUE"):
        return error_response_json(
            "InvalidRequestException", f"Invalid catalog type: {catalog_type}", 400
        )
    _data_catalogs[name] = {
        "Name": name,
        "Description": data.get("Description", ""),
        "Type": catalog_type,
        "Parameters": data.get("Parameters", {}),
    }
    tags = data.get("Tags", [])
    if tags:
        arn = _arn_datacatalog(name)
        _tags[arn] = {t["Key"]: t["Value"] for t in tags}
    return json_response({})


def _get_data_catalog(data):
    name = data.get("Name")
    catalog = _data_catalogs.get(name)
    if not catalog:
        return error_response_json(
            "InvalidRequestException", f"Data catalog {name} not found", 400
        )
    return json_response({"DataCatalog": catalog})


def _list_data_catalogs(data):
    summaries = [
        {"CatalogName": c["Name"], "Type": c["Type"]} for c in _data_catalogs.values()
    ]
    return json_response({"DataCatalogsSummary": summaries})


def _delete_data_catalog(data):
    name = data.get("Name")
    if name == "AwsDataCatalog":
        return error_response_json(
            "InvalidRequestException", "Cannot delete the default AWS data catalog", 400
        )
    if name not in _data_catalogs:
        return error_response_json(
            "InvalidRequestException", f"Data catalog {name} not found", 400
        )
    del _data_catalogs[name]
    _tags.pop(_arn_datacatalog(name), None)
    return json_response({})


def _update_data_catalog(data):
    name = data.get("Name")
    catalog = _data_catalogs.get(name)
    if not catalog:
        return error_response_json(
            "InvalidRequestException", f"Data catalog {name} not found", 400
        )
    if "Description" in data:
        catalog["Description"] = data["Description"]
    if "Type" in data:
        catalog["Type"] = data["Type"]
    if "Parameters" in data:
        catalog["Parameters"] = data["Parameters"]
    return json_response({})


# ---- Prepared Statements ----


def _create_prepared_statement(data):
    name = data.get("StatementName")
    workgroup = data.get("WorkGroup", "primary")
    query = data.get("QueryStatement", "")
    if not name:
        return error_response_json(
            "InvalidRequestException", "StatementName is required", 400
        )
    key = f"{workgroup}/{name}"
    if key in _prepared_statements:
        return error_response_json(
            "InvalidRequestException",
            f"Prepared statement {name} already exists in {workgroup}",
            400,
        )
    _prepared_statements[key] = {
        "StatementName": name,
        "WorkGroupName": workgroup,
        "QueryStatement": query,
        "Description": data.get("Description", ""),
        "LastModifiedTime": int(time.time()),
    }
    return json_response({})


def _get_prepared_statement(data):
    name = data.get("StatementName")
    workgroup = data.get("WorkGroup") or data.get("WorkGroupName", "primary")
    key = f"{workgroup}/{name}"
    stmt = _prepared_statements.get(key)
    if not stmt:
        return error_response_json(
            "ResourceNotFoundException",
            f"Prepared statement {name} not found in {workgroup}",
            400,
        )
    return json_response({"PreparedStatement": stmt})


def _delete_prepared_statement(data):
    name = data.get("StatementName")
    workgroup = data.get("WorkGroup") or data.get("WorkGroupName", "primary")
    key = f"{workgroup}/{name}"
    if key not in _prepared_statements:
        return error_response_json(
            "ResourceNotFoundException", f"Prepared statement {name} not found", 400
        )
    del _prepared_statements[key]
    return json_response({})


def _list_prepared_statements(data):
    workgroup = data.get("WorkGroup") or data.get("WorkGroupName", "primary")
    stmts = [
        {"StatementName": s["StatementName"], "LastModifiedTime": s["LastModifiedTime"]}
        for k, s in _prepared_statements.items()
        if s.get("WorkGroupName") == workgroup
    ]
    return json_response({"PreparedStatements": stmts})


# ---- Databases and Table Metadata (Glue Data Catalog) ----


def _athena_database(db):
    database = {"Name": db["Name"], "Parameters": db.get("Parameters") or {}}
    if db.get("Description"):
        database["Description"] = db["Description"]
    return database


def _get_database(data):
    from ministack.services import glue as glue_svc
    if data.get("CatalogName", "AwsDataCatalog") not in _data_catalogs:
        return error_response_json("MetadataException", f"Catalog {data.get('CatalogName')} not found", 400)
    db = glue_svc._databases.get(data.get("DatabaseName", ""))
    if not db:
        return error_response_json("MetadataException", f"Database {data.get('DatabaseName')} not found", 400)
    return json_response({"Database": _athena_database(db)})


def _list_databases(data):
    from ministack.services import glue as glue_svc
    if data.get("CatalogName", "AwsDataCatalog") not in _data_catalogs:
        return error_response_json("MetadataException", f"Catalog {data.get('CatalogName')} not found", 400)
    databases = [_athena_database(db) for db in glue_svc._databases.values()]
    token = data.get("NextToken") or "0"
    if not token.isdigit():
        return error_response_json("InvalidRequestException", "Invalid NextToken", 400)
    start = int(token)
    end = start + int(data.get("MaxResults") or 50)
    response = {"DatabaseList": databases[start:end]}
    if end < len(databases):
        response["NextToken"] = str(end)
    return json_response(response)


def _glue_col(c):
    col = {"Name": c.get("Name", ""), "Type": c.get("Type", "string")}
    if c.get("Comment"):
        col["Comment"] = c["Comment"]
    return col


def _glue_table_to_metadata(t):
    # Athena tables are Glue Data Catalog tables; surface the real columns and
    # partition keys rather than empty lists.
    sd = t.get("StorageDescriptor", {}) or {}
    return {
        "Name": t.get("Name", ""),
        "CreateTime": int(time.time()),
        "LastAccessTime": int(time.time()),
        "TableType": t.get("TableType", "EXTERNAL_TABLE"),
        "Columns": [_glue_col(c) for c in sd.get("Columns", [])],
        "PartitionKeys": [_glue_col(c) for c in t.get("PartitionKeys", [])],
        "Parameters": t.get("Parameters", {}) or {"classification": "csv"},
    }


def _get_table_metadata(data):
    from ministack.services import glue as glue_svc
    db = data.get("DatabaseName", "default")
    table = data.get("TableName", "")
    t = glue_svc._tables.get(f"{db}/{table}")
    if t:
        return json_response({"TableMetadata": _glue_table_to_metadata(t)})
    # Unknown table — keep the prior lenient empty shape.
    return json_response({"TableMetadata": {
        "Name": table, "CreateTime": int(time.time()),
        "LastAccessTime": int(time.time()), "TableType": "EXTERNAL_TABLE",
        "Columns": [], "PartitionKeys": [], "Parameters": {"classification": "csv"},
    }})


def _list_table_metadata(data):
    from ministack.services import glue as glue_svc
    db = data.get("DatabaseName", "default")
    tables = [_glue_table_to_metadata(t) for k, t in glue_svc._tables.items()
              if k.startswith(f"{db}/")]
    return json_response({"TableMetadataList": tables})


# ---- Tags ----


def _tag_resource(data):
    arn = data.get("ResourceARN", "")
    validation_error = _validate_tag_resource_arn(arn)
    if validation_error:
        return validation_error
    tags = data.get("Tags", [])
    tag_dict = _tags.setdefault(arn, {})
    for t in tags:
        tag_dict[t["Key"]] = t["Value"]
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceARN", "")
    validation_error = _validate_tag_resource_arn(arn)
    if validation_error:
        return validation_error
    keys = data.get("TagKeys", [])
    tag_dict = _tags.get(arn, {})
    for k in keys:
        tag_dict.pop(k, None)
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("ResourceARN", "")
    validation_error = _validate_tag_resource_arn(arn)
    if validation_error:
        return validation_error
    tag_dict = _tags.get(arn, {})
    tags = [{"Key": k, "Value": v} for k, v in tag_dict.items()]
    return json_response({"Tags": tags})


# ---- Helpers ----


def _execution_out(ex):
    return {k: v for k, v in ex.items() if not k.startswith("_")}


def reset():
    _executions.clear()
    _named_queries.clear()
    _prepared_statements.clear()
    _workgroups.clear()
    _data_catalogs.clear()
    _tags.clear()
    # "primary" workgroup and "AwsDataCatalog" are seeded lazily per-account
    # on next access via _ensure_default_workgroup() / _ensure_default_data_catalog().
