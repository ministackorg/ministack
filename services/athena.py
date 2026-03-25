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

import os
import re
import json
import time
import logging
import threading

from core.responses import json_response, error_response_json, new_uuid

logger = logging.getLogger("athena")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"
S3_DATA_DIR = os.environ.get("S3_DATA_DIR", "/tmp/localstack-data/s3")

_executions: dict = {}
_workgroups: dict = {
    "primary": {
        "Name": "primary",
        "State": "ENABLED",
        "Description": "Primary workgroup",
        "CreationTime": time.time(),
        "Configuration": {"ResultConfiguration": {"OutputLocation": "s3://athena-results/"}},
    }
}
_named_queries: dict = {}
_data_catalogs: dict = {
    "AwsDataCatalog": {
        "Name": "AwsDataCatalog",
        "Description": "AWS Glue Data Catalog",
        "Type": "GLUE",
        "Parameters": {},
    }
}
_prepared_statements: dict = {}  # "workgroup/name" -> statement dict
_tags: dict = {}  # arn -> {key: value, ...}

try:
    import duckdb
    _duckdb_available = True
    logger.info("Athena: DuckDB available — real SQL execution enabled")
except ImportError:
    _duckdb_available = False
    logger.warning("Athena: DuckDB not available — install with: pip install duckdb")

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
    return f"arn:aws:athena:{REGION}:{ACCOUNT_ID}:workgroup/{name}"


def _arn_datacatalog(name):
    return f"arn:aws:athena:{REGION}:{ACCOUNT_ID}:datacatalog/{name}"


async def handle_request(method, path, headers, body, query_params):
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
        # Table Metadata
        "GetTableMetadata": _get_table_metadata,
        "ListTableMetadata": _list_table_metadata,
        # Tags
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "ListTagsForResource": _list_tags_for_resource,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown Athena action: {action}", 400)
    return handler(data)


# ---- Query Execution ----

def _start_query_execution(data):
    query = data.get("QueryString", "")
    query_id = new_uuid()
    workgroup = data.get("WorkGroup", "primary")
    output_location = (
        data.get("ResultConfiguration", {}).get("OutputLocation")
        or _workgroups.get(workgroup, {}).get("Configuration", {})
            .get("ResultConfiguration", {}).get("OutputLocation", "s3://athena-results/")
    )
    db = data.get("QueryExecutionContext", {}).get("Database", "default")
    catalog = data.get("QueryExecutionContext", {}).get("Catalog", "AwsDataCatalog")

    execution = {
        "QueryExecutionId": query_id,
        "Query": query,
        "StatementType": _detect_statement_type(query),
        "ResultConfiguration": {"OutputLocation": f"{output_location}{query_id}.csv"},
        "QueryExecutionContext": {"Database": db, "Catalog": catalog},
        "Status": {
            "State": "QUEUED",
            "SubmissionDateTime": time.time(),
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
        "EngineVersion": {"SelectedEngineVersion": "Athena engine version 3",
                          "EffectiveEngineVersion": "Athena engine version 3"},
        "_results": None,
        "_column_types": None,
        "_error": None,
    }
    _executions[query_id] = execution

    thread = threading.Thread(target=_execute_query, args=(query_id, query, db), daemon=True)
    thread.start()

    return json_response({"QueryExecutionId": query_id})


def _execute_query(query_id, query, database):
    execution = _executions.get(query_id)
    if not execution:
        return

    execution["Status"]["State"] = "RUNNING"
    start = time.time()

    try:
        if _duckdb_available:
            results = _run_duckdb(query, database)
        else:
            results = _mock_query_results(query)

        execution["_results"] = {"columns": results["columns"], "rows": results["rows"]}
        execution["_column_types"] = results.get("column_types", ["varchar"] * len(results["columns"]))
        execution["Status"]["State"] = "SUCCEEDED"
        elapsed_ms = int((time.time() - start) * 1000)
        execution["Statistics"]["EngineExecutionTimeInMillis"] = elapsed_ms
        execution["Statistics"]["TotalExecutionTimeInMillis"] = elapsed_ms + 50
        execution["Statistics"]["QueryPlanningTimeInMillis"] = min(20, elapsed_ms)
        execution["Statistics"]["ServiceProcessingTimeInMillis"] = 30
        execution["Statistics"]["DataScannedInBytes"] = sum(
            len(str(row)) for row in results.get("rows", [])
        )
    except Exception as e:
        logger.error(f"Athena query {query_id} failed: {e}")
        execution["Status"]["State"] = "FAILED"
        execution["Status"]["StateChangeReason"] = str(e)[:2000]
        execution["_error"] = str(e)

    execution["Status"]["CompletionDateTime"] = time.time()


def _run_duckdb(query, database):
    import duckdb

    conn = duckdb.connect(":memory:")
    rewritten = _rewrite_s3_paths(query)

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
        conn.close()
        return {"columns": columns, "column_types": column_types,
                "rows": [list(r) for r in rows]}
    except Exception as e:
        conn.close()
        raise e


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
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        local_path = os.path.join(S3_DATA_DIR, bucket, key)
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


def _mock_query_results(query):
    query_upper = query.strip().upper()
    if query_upper.startswith("SELECT"):
        match = re.match(r"SELECT\s+'([^']*)'", query.strip(), re.IGNORECASE)
        if match:
            val = match.group(1)
            return {"columns": [val], "column_types": ["varchar"], "rows": [[val]]}
        alias_pattern = re.findall(
            r"""(?:(\d+(?:\.\d+)?)|'([^']*)')\s+AS\s+(\w+)""",
            query.strip(), re.IGNORECASE,
        )
        if alias_pattern:
            cols = [m[2] for m in alias_pattern]
            types = ["integer" if m[0] else "varchar" for m in alias_pattern]
            vals = [m[0] if m[0] else m[1] for m in alias_pattern]
            return {"columns": cols, "column_types": types, "rows": [vals]}
        return {"columns": ["result"], "column_types": ["varchar"],
                "rows": [["mock_value"]]}
    return {"columns": [], "column_types": [], "rows": []}


def _detect_statement_type(query):
    q = query.strip().upper()
    if q.startswith("SELECT") or q.startswith("WITH"):
        return "DML"
    if q.startswith(("CREATE", "DROP", "ALTER")):
        return "DDL"
    if q.startswith(("INSERT", "DELETE", "UPDATE", "MERGE")):
        return "DML"
    return "UTILITY"


def _get_query_execution(data):
    query_id = data.get("QueryExecutionId")
    execution = _executions.get(query_id)
    if not execution:
        return error_response_json("InvalidRequestException", f"Query {query_id} not found", 400)
    return json_response({"QueryExecution": _execution_out(execution)})


def _get_query_results(data):
    query_id = data.get("QueryExecutionId")
    max_results = data.get("MaxResults", 1000)
    next_token = data.get("NextToken")
    execution = _executions.get(query_id)
    if not execution:
        return error_response_json("InvalidRequestException", f"Query {query_id} not found", 400)

    state = execution["Status"]["State"]
    if state == "FAILED":
        return error_response_json("InvalidRequestException",
            f"Query has failed: {execution['Status'].get('StateChangeReason', 'Unknown')}", 400)
    if state != "SUCCEEDED":
        return error_response_json("InvalidRequestException",
            f"Query is in state {state}", 400)

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

    page_rows = rows[start_idx:start_idx + max_results]

    result_rows = []
    result_rows.append({"Data": [{"VarCharValue": col} for col in columns]})
    for row in page_rows:
        result_rows.append({
            "Data": [{"VarCharValue": str(v) if v is not None else ""} for v in row]
        })

    column_info = []
    for i, col in enumerate(columns):
        ctype = column_types[i] if i < len(column_types) else "varchar"
        precision, scale = _type_precision_scale(ctype)
        column_info.append({
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
        })

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
        execution["Status"]["CompletionDateTime"] = time.time()
    return json_response({})


def _list_query_executions(data):
    workgroup = data.get("WorkGroup", "primary")
    ids = [qid for qid, ex in _executions.items() if ex.get("WorkGroup") == workgroup]
    return json_response({"QueryExecutionIds": ids})


# ---- WorkGroups ----

def _create_workgroup(data):
    name = data.get("Name")
    if name in _workgroups:
        return error_response_json("InvalidRequestException", f"WorkGroup {name} already exists", 400)
    _workgroups[name] = {
        "Name": name,
        "State": "ENABLED",
        "Description": data.get("Description", ""),
        "CreationTime": time.time(),
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
        return error_response_json("InvalidRequestException", "Cannot delete primary workgroup", 400)
    _workgroups.pop(name, None)
    _tags.pop(_arn_workgroup(name), None)
    return json_response({})


def _get_workgroup(data):
    name = data.get("WorkGroup")
    wg = _workgroups.get(name)
    if not wg:
        return error_response_json("InvalidRequestException", f"WorkGroup {name} not found", 400)
    out = dict(wg)
    out.setdefault("WorkGroupConfiguration", out.get("Configuration", {}))
    return json_response({"WorkGroup": out})


def _list_workgroups(data):
    return json_response({
        "WorkGroups": [{"Name": wg["Name"], "State": wg["State"],
                        "Description": wg.get("Description", ""),
                        "CreationTime": wg.get("CreationTime", 0)}
                       for wg in _workgroups.values()]
    })


def _update_workgroup(data):
    name = data.get("WorkGroup")
    wg = _workgroups.get(name)
    if not wg:
        return error_response_json("InvalidRequestException", f"WorkGroup {name} not found", 400)
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
        for ck in ("EnforceWorkGroupConfiguration", "PublishCloudWatchMetricsEnabled",
                    "BytesScannedCutoffPerQuery", "RequesterPaysEnabled",
                    "EngineVersion"):
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
        return error_response_json("InvalidRequestException", f"Named query {query_id} not found", 400)
    return json_response({"NamedQuery": nq})


def _list_named_queries(data):
    workgroup = data.get("WorkGroup", "primary")
    ids = [qid for qid, nq in _named_queries.items() if nq.get("WorkGroup") == workgroup]
    return json_response({"NamedQueryIds": ids})


def _batch_get_named_query(data):
    ids = data.get("NamedQueryIds", [])
    queries = [_named_queries[qid] for qid in ids if qid in _named_queries]
    unprocessed = [{"NamedQueryId": qid, "ErrorCode": "INTERNAL_FAILURE", "ErrorMessage": "Not found"}
                   for qid in ids if qid not in _named_queries]
    return json_response({"NamedQueries": queries, "UnprocessedNamedQueryIds": unprocessed})


def _batch_get_query_execution(data):
    ids = data.get("QueryExecutionIds", [])
    execs = [_execution_out(_executions[qid]) for qid in ids if qid in _executions]
    unprocessed = [{"QueryExecutionId": qid, "ErrorCode": "INTERNAL_FAILURE", "ErrorMessage": "Not found"}
                   for qid in ids if qid not in _executions]
    return json_response({"QueryExecutions": execs, "UnprocessedQueryExecutionIds": unprocessed})


# ---- Data Catalogs ----

def _create_data_catalog(data):
    name = data.get("Name")
    if not name:
        return error_response_json("InvalidRequestException", "Name is required", 400)
    if name in _data_catalogs:
        return error_response_json("InvalidRequestException",
            f"Data catalog {name} already exists", 400)
    catalog_type = data.get("Type", "HIVE")
    if catalog_type not in ("HIVE", "LAMBDA", "GLUE"):
        return error_response_json("InvalidRequestException",
            f"Invalid catalog type: {catalog_type}", 400)
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
        return error_response_json("InvalidRequestException",
            f"Data catalog {name} not found", 400)
    return json_response({"DataCatalog": catalog})


def _list_data_catalogs(data):
    summaries = [{"CatalogName": c["Name"], "Type": c["Type"]}
                 for c in _data_catalogs.values()]
    return json_response({"DataCatalogsSummary": summaries})


def _delete_data_catalog(data):
    name = data.get("Name")
    if name == "AwsDataCatalog":
        return error_response_json("InvalidRequestException",
            "Cannot delete the default AWS data catalog", 400)
    if name not in _data_catalogs:
        return error_response_json("InvalidRequestException",
            f"Data catalog {name} not found", 400)
    del _data_catalogs[name]
    _tags.pop(_arn_datacatalog(name), None)
    return json_response({})


def _update_data_catalog(data):
    name = data.get("Name")
    catalog = _data_catalogs.get(name)
    if not catalog:
        return error_response_json("InvalidRequestException",
            f"Data catalog {name} not found", 400)
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
        return error_response_json("InvalidRequestException", "StatementName is required", 400)
    key = f"{workgroup}/{name}"
    if key in _prepared_statements:
        return error_response_json("InvalidRequestException",
            f"Prepared statement {name} already exists in {workgroup}", 400)
    _prepared_statements[key] = {
        "StatementName": name,
        "WorkGroupName": workgroup,
        "QueryStatement": query,
        "Description": data.get("Description", ""),
        "LastModifiedTime": time.time(),
    }
    return json_response({})


def _get_prepared_statement(data):
    name = data.get("StatementName")
    workgroup = data.get("WorkGroup") or data.get("WorkGroupName", "primary")
    key = f"{workgroup}/{name}"
    stmt = _prepared_statements.get(key)
    if not stmt:
        return error_response_json("ResourceNotFoundException",
            f"Prepared statement {name} not found in {workgroup}", 400)
    return json_response({"PreparedStatement": stmt})


def _delete_prepared_statement(data):
    name = data.get("StatementName")
    workgroup = data.get("WorkGroup") or data.get("WorkGroupName", "primary")
    key = f"{workgroup}/{name}"
    if key not in _prepared_statements:
        return error_response_json("ResourceNotFoundException",
            f"Prepared statement {name} not found", 400)
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


# ---- Table Metadata (stubs) ----

def _get_table_metadata(data):
    catalog = data.get("CatalogName", "AwsDataCatalog")
    db = data.get("DatabaseName", "default")
    table = data.get("TableName", "")
    return json_response({
        "TableMetadata": {
            "Name": table,
            "CreateTime": time.time(),
            "LastAccessTime": time.time(),
            "TableType": "EXTERNAL_TABLE",
            "Columns": [],
            "PartitionKeys": [],
            "Parameters": {"classification": "csv"},
        }
    })


def _list_table_metadata(data):
    return json_response({"TableMetadataList": []})


# ---- Tags ----

def _tag_resource(data):
    arn = data.get("ResourceARN", "")
    tags = data.get("Tags", [])
    tag_dict = _tags.setdefault(arn, {})
    for t in tags:
        tag_dict[t["Key"]] = t["Value"]
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceARN", "")
    keys = data.get("TagKeys", [])
    tag_dict = _tags.get(arn, {})
    for k in keys:
        tag_dict.pop(k, None)
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("ResourceARN", "")
    tag_dict = _tags.get(arn, {})
    tags = [{"Key": k, "Value": v} for k, v in tag_dict.items()]
    return json_response({"Tags": tags})


# ---- Helpers ----

def _execution_out(ex):
    return {k: v for k, v in ex.items() if not k.startswith("_")}


def reset():
    import time as _time
    global _workgroups, _data_catalogs
    _executions.clear()
    _named_queries.clear()
    _prepared_statements.clear()
    _workgroups = {
        "primary": {
            "Name": "primary",
            "State": "ENABLED",
            "Description": "Primary workgroup",
            "CreationTime": _time.time(),
            "Configuration": {"ResultConfiguration": {"OutputLocation": "s3://athena-results/"}},
        }
    }
    _data_catalogs = {
        "AwsDataCatalog": {
            "Name": "AwsDataCatalog",
            "Description": "AWS Glue Data Catalog",
            "Type": "GLUE",
            "Parameters": {},
        }
    }
