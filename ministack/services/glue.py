"""
Glue Service Emulator.
JSON-based API via X-Amz-Target (AWSGlue).
Supports full Data Catalog: Databases, Tables, Partitions, Connections, Crawlers, Jobs, JobRuns.
Also: SecurityConfigurations, Classifiers, PartitionIndexes, CrawlerMetrics, Tags,
      Triggers, Workflows.
Job execution: when Docker is available and the job command is ``glueetl`` or
``gluestreaming``, runs the script inside an ``amazon/aws-glue-libs`` container
with Spark + awsglue.  Falls back to plain ``python3`` subprocess for non-Spark
scripts or when Docker is unavailable.
Crawlers transition through RUNNING state with a configurable timer.
"""

import contextvars
import copy
import fnmatch
import json
import logging
import os
import subprocess
import tempfile
import threading
import time

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("glue")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
CRAWLER_RUN_SECONDS = int(os.environ.get("GLUE_CRAWLER_RUN_SECONDS", "5"))
S3_DATA_DIR = os.environ.get("S3_DATA_DIR", "/tmp/ministack-data/s3")
DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")

# Glue Docker image — maps GlueVersion to the amazon/aws-glue-libs tag.
# Users can override via GLUE_DOCKER_IMAGE env var.
_GLUE_VERSION_IMAGES = {
    "4.0": "amazon/aws-glue-libs:glue_libs_4.0.0_image_01",
    "3.0": "amazon/aws-glue-libs:glue_libs_3.0.0_image_01",
}
GLUE_DOCKER_IMAGE_OVERRIDE = os.environ.get("GLUE_DOCKER_IMAGE", "")

_docker = None
_ministack_network = None


def _get_docker():
    global _docker
    if _docker is None:
        try:
            import docker
            _docker = docker.from_env()
        except Exception:
            pass
    return _docker


def _get_ministack_network(docker_client):
    """Detect the Docker network MiniStack is running on (if containerised)."""
    global _ministack_network
    if _ministack_network is not None:
        return _ministack_network or None
    if DOCKER_NETWORK:
        _ministack_network = DOCKER_NETWORK
        return DOCKER_NETWORK
    try:
        self_container = docker_client.containers.get(
            os.environ.get("HOSTNAME", ""))
        nets = list(
            self_container.attrs["NetworkSettings"]["Networks"].keys())
        if nets:
            _ministack_network = nets[0]
            return nets[0]
    except Exception:
        pass
    _ministack_network = ""
    return None


def _glue_image_for_version(glue_version):
    """Return the Docker image for a given GlueVersion."""
    if GLUE_DOCKER_IMAGE_OVERRIDE:
        return GLUE_DOCKER_IMAGE_OVERRIDE
    return _GLUE_VERSION_IMAGES.get(glue_version, _GLUE_VERSION_IMAGES.get("4.0"))


def _is_spark_job(job):
    """True if the job uses glueetl/gluestreaming (Spark-based)."""
    cmd_name = job.get("Command", {}).get("Name", "")
    return cmd_name in ("glueetl", "gluestreaming")

_databases = AccountScopedDict()
_tables = AccountScopedDict()       # "db_name/table_name" -> table dict
_partitions = AccountScopedDict()   # "db_name/table_name" -> [partition, ...]
_partition_indexes = AccountScopedDict()  # "db_name/table_name" -> [index, ...]
_connections = AccountScopedDict()
_crawlers = AccountScopedDict()
_jobs = AccountScopedDict()
_job_runs = AccountScopedDict()     # job_name -> [run, ...]
_tags = AccountScopedDict()         # arn -> {key: value, ...}
_security_configs = AccountScopedDict()
_classifiers = AccountScopedDict()
_triggers = AccountScopedDict()     # trigger_name -> trigger dict
_workflows = AccountScopedDict()    # workflow_name -> workflow dict
_workflow_runs = AccountScopedDict() # workflow_name -> [run, ...]
_user_defined_functions = AccountScopedDict()  # "db_name/function_name" -> udf dict

_ALL_STATE = {
    "databases": _databases,
    "tables": _tables,
    "partitions": _partitions,
    "partition_indexes": _partition_indexes,
    "connections": _connections,
    "crawlers": _crawlers,
    "jobs": _jobs,
    "job_runs": _job_runs,
    "tags": _tags,
    "security_configs": _security_configs,
    "classifiers": _classifiers,
    "triggers": _triggers,
    "workflows": _workflows,
    "workflow_runs": _workflow_runs,
    "user_defined_functions": _user_defined_functions,
}


def get_state():
    return copy.deepcopy(_ALL_STATE)


def restore_state(data):
    for key, store in _ALL_STATE.items():
        store.clear()
        store.update(data.get(key, {}))


try:
    _restored = load_state("glue")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


def _arn(resource_type, name):
    return f"arn:aws:glue:{get_region()}:{get_account_id()}:{resource_type}/{name}"


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        # Databases
        "CreateDatabase": _create_database,
        "DeleteDatabase": _delete_database,
        "GetDatabase": _get_database,
        "GetDatabases": _get_databases,
        "UpdateDatabase": _update_database,
        # Tables
        "CreateTable": _create_table,
        "DeleteTable": _delete_table,
        "GetTable": _get_table,
        "GetTables": _get_tables,
        "UpdateTable": _update_table,
        "BatchDeleteTable": _batch_delete_table,
        # Partitions
        "CreatePartition": _create_partition,
        "DeletePartition": _delete_partition,
        "GetPartition": _get_partition,
        "GetPartitions": _get_partitions,
        "BatchCreatePartition": _batch_create_partition,
        "BatchGetPartition": _batch_get_partition,
        "BatchUpdatePartition": _batch_update_partition,
        # Partition Indexes
        "CreatePartitionIndex": _create_partition_index,
        "GetPartitionIndexes": _get_partition_indexes,
        # Connections
        "CreateConnection": _create_connection,
        "DeleteConnection": _delete_connection,
        "GetConnection": _get_connection,
        "GetConnections": _get_connections,
        # Crawlers
        "CreateCrawler": _create_crawler,
        "DeleteCrawler": _delete_crawler,
        "GetCrawler": _get_crawler,
        "GetCrawlers": _get_crawlers,
        "UpdateCrawler": _update_crawler,
        "StartCrawler": _start_crawler,
        "StopCrawler": _stop_crawler,
        "GetCrawlerMetrics": _get_crawler_metrics,
        # Jobs
        "CreateJob": _create_job,
        "DeleteJob": _delete_job,
        "GetJob": _get_job,
        "GetJobs": _get_jobs,
        "UpdateJob": _update_job,
        "StartJobRun": _start_job_run,
        "GetJobRun": _get_job_run,
        "GetJobRuns": _get_job_runs,
        "BatchStopJobRun": _batch_stop_job_run,
        # Security Configurations
        "CreateSecurityConfiguration": _create_security_configuration,
        "DeleteSecurityConfiguration": _delete_security_configuration,
        "GetSecurityConfiguration": _get_security_configuration,
        "GetSecurityConfigurations": _get_security_configurations,
        # Classifiers
        "CreateClassifier": _create_classifier,
        "GetClassifier": _get_classifier,
        "GetClassifiers": _get_classifiers,
        "DeleteClassifier": _delete_classifier,
        # Triggers
        "CreateTrigger": _create_trigger,
        "GetTrigger": _get_trigger,
        "DeleteTrigger": _delete_trigger,
        "UpdateTrigger": _update_trigger,
        "StartTrigger": _start_trigger,
        "StopTrigger": _stop_trigger,
        "ListTriggers": _list_triggers,
        "BatchGetTriggers": _batch_get_triggers,
        "GetTriggers": _get_triggers,
        # Workflows
        "CreateWorkflow": _create_workflow,
        "GetWorkflow": _get_workflow,
        "DeleteWorkflow": _delete_workflow,
        "UpdateWorkflow": _update_workflow,
        "StartWorkflowRun": _start_workflow_run,
        # User Defined Functions
        "CreateUserDefinedFunction": _create_user_defined_function,
        "UpdateUserDefinedFunction": _update_user_defined_function,
        "DeleteUserDefinedFunction": _delete_user_defined_function,
        "GetUserDefinedFunction": _get_user_defined_function,
        "GetUserDefinedFunctions": _get_user_defined_functions,
        # Tags
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "GetTags": _get_tags,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown Glue action: {action}", 400)
    return handler(data)


# ---- Databases ----

def _create_database(data):
    db_input = data.get("DatabaseInput", {})
    name = db_input.get("Name")
    if not name:
        return error_response_json("InvalidInputException", "DatabaseInput.Name is required", 400)
    if name in _databases:
        return error_response_json("AlreadyExistsException", f"Database {name} already exists", 400)
    _databases[name] = {
        "Name": name,
        "Description": db_input.get("Description", ""),
        "LocationUri": db_input.get("LocationUri"),
        "Parameters": db_input.get("Parameters", {}),
        "CreateTime": int(time.time()),
        "CatalogId": get_account_id(),
    }
    if data.get("Tags"):
        _tags[_arn("database", name)] = dict(data["Tags"])
    return json_response({})


def _delete_database(data):
    name = data.get("Name")
    if name not in _databases:
        return error_response_json("EntityNotFoundException", f"Database {name} not found", 400)
    del _databases[name]
    _tags.pop(_arn("database", name), None)
    keys_to_del = [k for k in _tables if k.startswith(f"{name}/")]
    for k in keys_to_del:
        del _tables[k]
        _partitions.pop(k, None)
        _partition_indexes.pop(k, None)
    return json_response({})


def _get_database(data):
    name = data.get("Name")
    db = _databases.get(name)
    if not db:
        return error_response_json("EntityNotFoundException", f"Database {name} not found", 400)
    return json_response({"Database": db})


def _get_databases(data):
    return json_response({"DatabaseList": list(_databases.values())})


def _update_database(data):
    name = data.get("Name")
    db_input = data.get("DatabaseInput", {})
    if name not in _databases:
        return error_response_json("EntityNotFoundException", f"Database {name} not found", 400)
    safe_keys = {"Description", "LocationUri", "Parameters"}
    for k in safe_keys:
        if k in db_input:
            _databases[name][k] = db_input[k]
    return json_response({})


# ---- Tables ----

def _create_table(data):
    db_name = data.get("DatabaseName")
    if db_name not in _databases:
        return error_response_json("EntityNotFoundException", f"Database {db_name} not found.", 400)
    table_input = data.get("TableInput", {})
    name = table_input.get("Name")
    key = f"{db_name}/{name}"
    if key in _tables:
        return error_response_json("AlreadyExistsException", f"Table {name} already exists", 400)
    _tables[key] = {
        "Name": name,
        "DatabaseName": db_name,
        "Description": table_input.get("Description", ""),
        "Owner": table_input.get("Owner", ""),
        "CreateTime": int(time.time()),
        "UpdateTime": int(time.time()),
        "LastAccessTime": int(time.time()),
        "StorageDescriptor": table_input.get("StorageDescriptor", {}),
        "PartitionKeys": table_input.get("PartitionKeys", []),
        "TableType": table_input.get("TableType", "EXTERNAL_TABLE"),
        "Parameters": table_input.get("Parameters", {}),
        "ViewOriginalText": table_input.get("ViewOriginalText"),
        "ViewExpandedText": table_input.get("ViewExpandedText"),
        "ViewDefinition": table_input.get("ViewDefinition"),
        "IsMultiDialectView": table_input.get("IsMultiDialectView"),
        "IsRegisteredWithLakeFormation": False,
        "CatalogId": get_account_id(),
        # AWS Glue exposes a monotonically-increasing VersionId per table for
        # optimistic concurrency on UpdateTable. Stored as a string per the
        # botocore Table output shape.
        "VersionId": "1",
    }
    return json_response({})


def _delete_table(data):
    db_name = data.get("DatabaseName")
    name = data.get("Name")
    key = f"{db_name}/{name}"
    if key not in _tables:
        return error_response_json("EntityNotFoundException", f"Table {name} not found.", 400)
    _tables.pop(key, None)
    _partitions.pop(key, None)
    _partition_indexes.pop(key, None)
    return json_response({})


def _get_table(data):
    db_name = data.get("DatabaseName")
    name = data.get("Name")
    key = f"{db_name}/{name}"
    table = _tables.get(key)
    if not table:
        return error_response_json("EntityNotFoundException", f"Table {name} not found in {db_name}", 400)
    return json_response({"Table": table})


def _get_tables(data):
    db_name = data.get("DatabaseName")
    expression = data.get("Expression", "")
    tables = [t for k, t in _tables.items() if k.startswith(f"{db_name}/")]
    if expression:
        tables = [t for t in tables if _simple_glob_match(expression, t["Name"])]
    return json_response({"TableList": tables})


def _update_table(data):
    db_name = data.get("DatabaseName")
    table_input = data.get("TableInput", {})
    name = table_input.get("Name")
    key = f"{db_name}/{name}"
    if key not in _tables:
        return error_response_json("EntityNotFoundException", f"Table {name} not found", 400)
    # Optimistic-concurrency check: if the caller passes VersionId, it must
    # match the table's current VersionId. Real AWS Glue rejects stale writes
    # with ConcurrentModificationException. Issue #1183.
    requested_version = data.get("VersionId")
    current_version = _tables[key].get("VersionId", "1")
    if requested_version is not None and str(requested_version) != current_version:
        return error_response_json(
            "ConcurrentModificationException",
            f"Table {name} was modified by another process. Expected VersionId={current_version}, got {requested_version}.",
            400,
        )
    safe_keys = {"Description", "Owner", "StorageDescriptor", "PartitionKeys",
                 "TableType", "Parameters", "ViewOriginalText", "ViewExpandedText",
                 "ViewDefinition", "IsMultiDialectView"}
    for k in safe_keys:
        if k in table_input:
            _tables[key][k] = table_input[k]
    _tables[key]["UpdateTime"] = int(time.time())
    try:
        _tables[key]["VersionId"] = str(int(current_version) + 1)
    except (TypeError, ValueError):
        _tables[key]["VersionId"] = "1"
    return json_response({})


def _batch_delete_table(data):
    db_name = data.get("DatabaseName")
    names = data.get("TablesToDelete", [])
    errors = []
    for name in names:
        key = f"{db_name}/{name}"
        if key not in _tables:
            errors.append({"TableName": name, "ErrorDetail": {
                "ErrorCode": "EntityNotFoundException", "ErrorMessage": "Table not found"}})
        else:
            del _tables[key]
            _partitions.pop(key, None)
            _partition_indexes.pop(key, None)
    return json_response({"Errors": errors})


# ---- Partitions ----

def _create_partition(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    partition_input = data.get("PartitionInput", {})
    key = f"{db_name}/{table_name}"
    if key not in _partitions:
        _partitions[key] = []

    values = partition_input.get("Values", [])
    for existing in _partitions[key]:
        if existing.get("Values") == values:
            return error_response_json("AlreadyExistsException",
                f"Partition with values {values} already exists", 400)

    _partitions[key].append({
        **partition_input,
        "DatabaseName": db_name,
        "TableName": table_name,
        "CreationTime": int(time.time()),
        "LastAccessTime": int(time.time()),
        "CatalogId": get_account_id(),
    })
    return json_response({})


def _delete_partition(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    values = data.get("PartitionValues", [])
    key = f"{db_name}/{table_name}"
    if key in _partitions:
        _partitions[key] = [p for p in _partitions[key] if p.get("Values") != values]
    return json_response({})


def _get_partition(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    values = data.get("PartitionValues", [])
    key = f"{db_name}/{table_name}"
    for p in _partitions.get(key, []):
        if p.get("Values") == values:
            return json_response({"Partition": p})
    return error_response_json("EntityNotFoundException", "Partition not found", 400)


def _get_partitions(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    key = f"{db_name}/{table_name}"
    return json_response({"Partitions": _partitions.get(key, [])})


def _batch_create_partition(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    key = f"{db_name}/{table_name}"
    if key not in _partitions:
        _partitions[key] = []
    errors = []
    for pi in data.get("PartitionInputList", []):
        values = pi.get("Values", [])
        dupe = any(p.get("Values") == values for p in _partitions[key])
        if dupe:
            errors.append({"PartitionValues": values, "ErrorDetail": {
                "ErrorCode": "AlreadyExistsException",
                "ErrorMessage": "Partition already exists"}})
        else:
            _partitions[key].append({
                **pi,
                "DatabaseName": db_name,
                "TableName": table_name,
                "CreationTime": int(time.time()),
                "CatalogId": get_account_id(),
            })
    return json_response({"Errors": errors})


def _batch_get_partition(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    key = f"{db_name}/{table_name}"
    entries = data.get("PartitionsToGet", [])
    partitions = []
    unprocessed = []
    all_parts = _partitions.get(key, [])
    for entry in entries:
        values = entry.get("Values", [])
        found = None
        for p in all_parts:
            if p.get("Values") == values:
                found = p
                break
        if found:
            partitions.append(found)
        else:
            unprocessed.append(entry)
    return json_response({"Partitions": partitions, "UnprocessedKeys": unprocessed})


def _batch_update_partition(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    key = f"{db_name}/{table_name}"
    if key not in _tables:
        return error_response_json("EntityNotFoundException",
            f"Table {table_name} not found in {db_name}", 400)
    parts = _partitions.get(key, [])
    errors = []
    for entry in data.get("Entries", []):
        values = entry.get("PartitionValueList", [])
        partition_input = entry.get("PartitionInput", {})
        target = None
        for p in parts:
            if p.get("Values") == values:
                target = p
                break
        if target is None:
            errors.append({"PartitionValueList": values, "ErrorDetail": {
                "ErrorCode": "EntityNotFoundException",
                "ErrorMessage": "Partition not found"}})
            continue
        creation_time = target.get("CreationTime")
        target.clear()
        target.update({
            **partition_input,
            "DatabaseName": db_name,
            "TableName": table_name,
            "CreationTime": creation_time,
            "LastAccessTime": int(time.time()),
            "CatalogId": get_account_id(),
        })
    return json_response({"Errors": errors})


# ---- Partition Indexes ----

def _create_partition_index(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    index_input = data.get("PartitionIndex", {})
    key = f"{db_name}/{table_name}"
    if key not in _partition_indexes:
        _partition_indexes[key] = []
    raw_keys = index_input.get("Keys", [])
    key_schema = [{"Name": k} if isinstance(k, str) else k for k in raw_keys]
    _partition_indexes[key].append({
        "IndexName": index_input.get("IndexName", ""),
        "Keys": key_schema,
        "IndexStatus": "ACTIVE",
    })
    return json_response({})


def _get_partition_indexes(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    key = f"{db_name}/{table_name}"
    return json_response({"PartitionIndexDescriptorList": _partition_indexes.get(key, [])})


# ---- Connections ----

def _create_connection(data):
    conn_input = data.get("ConnectionInput", {})
    name = conn_input.get("Name")
    _connections[name] = {**conn_input, "CreationTime": int(time.time()), "LastUpdatedTime": int(time.time())}
    return json_response({})


def _delete_connection(data):
    name = data.get("ConnectionName")
    if name not in _connections:
        return error_response_json("EntityNotFoundException", f"Connection {name} not found.", 400)
    _connections.pop(name, None)
    return json_response({})


def _get_connection(data):
    name = data.get("Name")
    conn = _connections.get(name)
    if not conn:
        return error_response_json("EntityNotFoundException", f"Connection {name} not found", 400)
    return json_response({"Connection": conn})


def _get_connections(data):
    return json_response({"ConnectionList": list(_connections.values())})


# ---- Crawlers ----

def _create_crawler(data):
    name = data.get("Name")
    if name in _crawlers:
        return error_response_json("AlreadyExistsException", f"Crawler {name} already exists", 400)
    schedule = data.get("Schedule", "")
    schedule_struct = {"ScheduleExpression": schedule} if schedule else {}
    _crawlers[name] = {
        "Name": name,
        "Role": data.get("Role", ""),
        "DatabaseName": data.get("DatabaseName", ""),
        "Description": data.get("Description", ""),
        "Targets": data.get("Targets", {}),
        "Schedule": schedule_struct,
        "Classifiers": data.get("Classifiers", []),
        "TablePrefix": data.get("TablePrefix", ""),
        "SchemaChangePolicy": data.get("SchemaChangePolicy", {}),
        "RecrawlPolicy": data.get("RecrawlPolicy", {}),
        "LineageConfiguration": data.get("LineageConfiguration", {}),
        "State": "READY",
        "CrawlElapsedTime": 0,
        "CreationTime": int(time.time()),
        "LastUpdated": int(time.time()),
        "LastCrawl": None,
        "Version": 1,
        "Configuration": data.get("Configuration", ""),
        "CrawlerSecurityConfiguration": data.get("CrawlerSecurityConfiguration", ""),
    }
    return json_response({})


def _delete_crawler(data):
    name = data.get("Name")
    if name not in _crawlers:
        return error_response_json("EntityNotFoundException", f"Crawler {name} not found", 400)
    del _crawlers[name]
    return json_response({})


def _get_crawler(data):
    name = data.get("Name")
    crawler = _crawlers.get(name)
    if not crawler:
        return error_response_json("EntityNotFoundException", f"Crawler {name} not found", 400)
    return json_response({"Crawler": crawler})


def _get_crawlers(data):
    return json_response({"Crawlers": list(_crawlers.values())})


def _update_crawler(data):
    name = data.get("Name")
    if name not in _crawlers:
        return error_response_json("EntityNotFoundException", f"Crawler {name} not found", 400)
    crawler = _crawlers[name]
    updatable = {"Role", "DatabaseName", "Description", "Targets", "Schedule",
                 "Classifiers", "TablePrefix", "SchemaChangePolicy", "RecrawlPolicy",
                 "LineageConfiguration", "Configuration", "CrawlerSecurityConfiguration"}
    for k in updatable:
        if k in data:
            if k == "Schedule":
                sched = data[k]
                crawler["Schedule"] = {"ScheduleExpression": sched} if isinstance(sched, str) else sched
            else:
                crawler[k] = data[k]
    crawler["LastUpdated"] = int(time.time())
    crawler["Version"] = crawler.get("Version", 1) + 1
    return json_response({})


def _start_crawler(data):
    name = data.get("Name")
    if name not in _crawlers:
        return error_response_json("EntityNotFoundException", f"Crawler {name} not found", 400)
    crawler = _crawlers[name]
    if crawler["State"] == "RUNNING":
        return error_response_json("CrawlerRunningException",
            f"Crawler {name} is already running", 400)

    crawler["State"] = "RUNNING"
    crawler["CrawlElapsedTime"] = 0
    start_time = time.time()

    def _finish_crawl():
        if name in _crawlers and _crawlers[name]["State"] == "RUNNING":
            _crawlers[name]["State"] = "READY"
            _crawlers[name]["CrawlElapsedTime"] = int((time.time() - start_time) * 1000)
            _crawlers[name]["LastCrawl"] = {
                "Status": "SUCCEEDED",
                "LogGroup": f"/aws-glue/crawlers/{name}",
                "LogStream": new_uuid(),
                "MessagePrefix": "",
                "StartTime": start_time,
                "EndTime": int(time.time()),
            }
            logger.info("Glue: Crawler %s finished after %ss", name, CRAWLER_RUN_SECONDS)

    # threading.Timer (like threading.Thread) does NOT copy contextvars, so
    # without this snapshot _finish_crawl runs under the default account and the
    # account-scoped _crawlers guard never matches — the crawler would hang in
    # RUNNING forever for non-default accounts. See issue #639 / stepfunctions.
    ctx = contextvars.copy_context()
    timer = threading.Timer(CRAWLER_RUN_SECONDS, lambda: ctx.run(_finish_crawl))
    timer.daemon = True
    timer.start()

    logger.info("Glue: Crawler %s started (will run for %ss)", name, CRAWLER_RUN_SECONDS)
    return json_response({})


def _stop_crawler(data):
    name = data.get("Name")
    if name not in _crawlers:
        return error_response_json("EntityNotFoundException", f"Crawler {name} not found", 400)
    if _crawlers[name]["State"] != "RUNNING":
        return error_response_json("CrawlerNotRunningException",
            f"Crawler {name} is not running", 400)
    _crawlers[name]["State"] = "STOPPING"
    _crawlers[name]["State"] = "READY"
    return json_response({})


def _get_crawler_metrics(data):
    crawler_names = data.get("CrawlerNameList", list(_crawlers.keys()))
    metrics = []
    for name in crawler_names:
        crawler = _crawlers.get(name)
        if crawler:
            metrics.append({
                "CrawlerName": name,
                "TimeLeftSeconds": 0.0,
                "StillEstimating": False,
                "LastRuntimeSeconds": crawler.get("CrawlElapsedTime", 0) / 1000.0,
                "MedianRuntimeSeconds": crawler.get("CrawlElapsedTime", 0) / 1000.0,
                "TablesCreated": 0,
                "TablesUpdated": 0,
                "TablesDeleted": 0,
            })
    return json_response({"CrawlerMetricsList": metrics})


# ---- Jobs ----

def _create_job(data):
    name = data.get("Name")
    if not name:
        return error_response_json("InvalidInputException", "Name is required", 400)
    if name in _jobs:
        return error_response_json("AlreadyExistsException", f"Job {name} already exists", 400)
    _jobs[name] = {
        "Name": name,
        "Description": data.get("Description", ""),
        "Role": data.get("Role", ""),
        "Command": data.get("Command", {}),
        "DefaultArguments": data.get("DefaultArguments", {}),
        "NonOverridableArguments": data.get("NonOverridableArguments", {}),
        "Connections": data.get("Connections", {}),
        "MaxRetries": data.get("MaxRetries", 0),
        "Timeout": data.get("Timeout", 2880),
        "GlueVersion": data.get("GlueVersion", "3.0"),
        "NumberOfWorkers": data.get("NumberOfWorkers", 2),
        "WorkerType": data.get("WorkerType", "G.1X"),
        "MaxCapacity": data.get("MaxCapacity"),
        "SecurityConfiguration": data.get("SecurityConfiguration", ""),
        "Tags": data.get("Tags", {}),
        "CreatedOn": int(time.time()),
        "LastModifiedOn": int(time.time()),
    }
    _job_runs[name] = []
    return json_response({"Name": name})


def _delete_job(data):
    name = data.get("JobName")
    _jobs.pop(name, None)
    _job_runs.pop(name, None)
    return json_response({"JobName": name})


def _get_job(data):
    name = data.get("JobName")
    job = _jobs.get(name)
    if not job:
        return error_response_json("EntityNotFoundException", f"Job {name} not found", 400)
    return json_response({"Job": job})


def _get_jobs(data):
    return json_response({"Jobs": list(_jobs.values())})


def _update_job(data):
    name = data.get("JobName")
    job_update = data.get("JobUpdate", {})
    if name not in _jobs:
        return error_response_json("EntityNotFoundException", f"Job {name} not found", 400)
    updatable = {"Description", "Role", "Command", "DefaultArguments",
                 "NonOverridableArguments", "Connections", "MaxRetries", "Timeout",
                 "GlueVersion", "NumberOfWorkers", "WorkerType", "MaxCapacity",
                 "SecurityConfiguration"}
    for k in updatable:
        if k in job_update:
            _jobs[name][k] = job_update[k]
    _jobs[name]["LastModifiedOn"] = int(time.time())
    return json_response({"JobName": name})


def _resolve_script(script_location):
    """Resolve a script location to a local path. Supports local paths and s3:// URIs.

    For S3 URIs, first checks the on-disk S3_DATA_DIR (file-backed S3).
    If not found, fetches from MiniStack's in-memory S3 service to a temp file.
    """
    if not script_location:
        return None
    if os.path.exists(script_location):
        return script_location
    if script_location.startswith("s3://"):
        stripped = script_location[5:]
        parts = stripped.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        # Check on-disk first. Objects are persisted account-scoped at
        # DATA_DIR/<account>/<bucket>/<key> (see s3._object_disk_path), so the
        # account id MUST be part of the lookup path or it never matches.
        local_path = os.path.join(S3_DATA_DIR, get_account_id(), bucket, key)
        if os.path.exists(local_path):
            return local_path
        # Fetch from in-memory S3
        try:
            import ministack.services.s3 as _s3_svc
            s3_bucket = _s3_svc._buckets.get(bucket)
            if s3_bucket:
                obj = s3_bucket.get("objects", {}).get(key)
                if obj and obj.get("body"):
                    tmp_dir = os.path.join(tempfile.gettempdir(), "ministack-glue-scripts")
                    os.makedirs(tmp_dir, exist_ok=True)
                    tmp_path = os.path.join(tmp_dir, os.path.basename(key))
                    data = obj["body"]
                    if isinstance(data, memoryview):
                        data = bytes(data)
                    with open(tmp_path, "wb") as f:
                        f.write(data)
                    logger.info("Glue: resolved script from S3: s3://%s/%s -> %s", bucket, key, tmp_path)
                    return tmp_path
        except Exception as e:
            logger.debug("Glue: failed to fetch script from S3: %s", e)
    return None


def _start_job_run(data):
    job_name = data.get("JobName")
    if job_name not in _jobs:
        return error_response_json("EntityNotFoundException", f"Job {job_name} not found", 400)

    run_id = new_uuid()
    job = _jobs[job_name]
    args = {**job.get("DefaultArguments", {}), **data.get("Arguments", {})}

    run = {
        "Id": run_id,
        "JobName": job_name,
        "StartedOn": int(time.time()),
        "LastModifiedOn": int(time.time()),
        "CompletedOn": None,
        "JobRunState": "STARTING",
        "Arguments": args,
        "ErrorMessage": "",
        "PredecessorRuns": [],
        "AllocatedCapacity": job.get("MaxCapacity") or job.get("NumberOfWorkers", 2),
        "ExecutionTime": 0,
        "Timeout": job.get("Timeout", 2880),
        "MaxCapacity": job.get("MaxCapacity"),
        "WorkerType": job.get("WorkerType", "G.1X"),
        "NumberOfWorkers": job.get("NumberOfWorkers", 2),
        "SecurityConfiguration": job.get("SecurityConfiguration", ""),
        "GlueVersion": job.get("GlueVersion", "3.0"),
        "Attempt": 0,
    }

    if job_name not in _job_runs:
        _job_runs[job_name] = []
    _job_runs[job_name].append(run)

    def _execute():
        run["JobRunState"] = "RUNNING"
        run["LastModifiedOn"] = int(time.time())

        script_location = job.get("Command", {}).get("ScriptLocation", "")
        resolved = _resolve_script(script_location)

        docker_client = _get_docker()
        use_docker = False
        if _is_spark_job(job) and docker_client and resolved:
            image = _glue_image_for_version(job.get("GlueVersion", "4.0"))
            try:
                docker_client.images.get(image)
                use_docker = True
            except Exception:
                logger.info("Glue: image %s not available — stubbing job %s", image, job_name)

        if use_docker:
            _execute_spark_docker(run, job, job_name, args, resolved, docker_client)
        elif _is_spark_job(job):
            run["JobRunState"] = "SUCCEEDED"
        elif resolved and resolved.endswith(".py"):
            _execute_subprocess(run, job, args, resolved)
        else:
            run["JobRunState"] = "SUCCEEDED"

        run["CompletedOn"] = int(time.time())
        run["ExecutionTime"] = int(run["CompletedOn"] - run["StartedOn"])
        run["LastModifiedOn"] = int(time.time())

    # threading.Thread does NOT copy contextvars, so without this snapshot the
    # worker would run under the default account and fail to resolve the
    # account-scoped on-disk script (and AccountScopedDict lookups). Carry the
    # request's account/region into the thread. See issue #639 / stepfunctions.
    ctx = contextvars.copy_context()
    thread = threading.Thread(target=ctx.run, args=(_execute,), daemon=True)
    thread.start()

    return json_response({"JobRunId": run_id})


def _execute_subprocess(run, job, args, resolved):
    """Run a Glue script as a plain Python subprocess (non-Spark fallback)."""
    try:
        env = dict(os.environ)
        for k, v in args.items():
            env_key = k.lstrip("-")
            if env_key:
                env[env_key] = str(v)
        proc = subprocess.run(
            ["python3", resolved],
            capture_output=True, text=True,
            timeout=min(job.get("Timeout", 300), 600),
            env=env,
        )
        if proc.returncode == 0:
            run["JobRunState"] = "SUCCEEDED"
        else:
            run["JobRunState"] = "FAILED"
            run["ErrorMessage"] = proc.stderr[:2000] if proc.stderr else f"Exit code {proc.returncode}"
    except subprocess.TimeoutExpired:
        run["JobRunState"] = "TIMEOUT"
        run["ErrorMessage"] = "Job execution timed out"
    except Exception as e:
        run["JobRunState"] = "FAILED"
        run["ErrorMessage"] = str(e)[:2000]


def _execute_spark_docker(run, job, job_name, args, script_path, docker_client):
    """Run a Glue Spark job inside an amazon/aws-glue-libs Docker container."""
    glue_version = job.get("GlueVersion", "4.0")
    image = _glue_image_for_version(glue_version)
    container_name = f"ministack-glue-{job_name}-{run['Id'][:8]}"

    # Remove stale container with same name
    try:
        existing = docker_client.containers.get(container_name)
        existing.remove(force=True)
    except Exception:
        pass

    ms_network = _get_ministack_network(docker_client)

    # Determine MiniStack's S3 endpoint from inside the container.
    # If on a Docker network, use the ministack container's IP; otherwise localhost.
    ministack_host = os.environ.get("MINISTACK_HOST", "")
    ministack_port = os.environ.get("EDGE_PORT", "4566")
    if ms_network and not ministack_host:
        # Try to resolve from HOSTNAME
        try:
            ms_container = docker_client.containers.get(
                os.environ.get("HOSTNAME", ""))
            ms_container.reload()
            nets = ms_container.attrs.get("NetworkSettings", {}).get("Networks", {})
            ip = nets.get(ms_network, {}).get("IPAddress", "")
            if ip:
                ministack_host = ip
        except Exception:
            pass
    if not ministack_host:
        ministack_host = "host.docker.internal"

    s3_endpoint = f"http://{ministack_host}:{ministack_port}"

    # Build Spark submit arguments from Glue job arguments.
    # Glue args use --key value; spark-submit uses --conf key=value for Spark conf.
    spark_args = []
    for k, v in args.items():
        spark_args.extend([k, str(v)])

    # Extra py files (Glue --extra-py-files)
    extra_py = args.get("--extra-py-files", "")

    # Build environment for the container
    container_env = {
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        "AWS_DEFAULT_REGION": get_region(),
        "AWS_REGION": get_region(),
        "DISABLE_SSL": "true",
    }

    # Build the spark-submit command.
    # The aws-glue-libs image has /home/glue_user/spark/bin/spark-submit.
    cmd = [
        "spark-submit",
        "--master", "local[*]",
        "--conf", f"spark.hadoop.fs.s3a.endpoint={s3_endpoint}",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", f"spark.hadoop.fs.s3a.access.key={container_env['AWS_ACCESS_KEY_ID']}",
        "--conf", f"spark.hadoop.fs.s3a.secret.key={container_env['AWS_SECRET_ACCESS_KEY']}",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
    ]

    # Add extra-py-files if present
    if extra_py:
        cmd.extend(["--py-files", extra_py])

    # Add Spark/Iceberg conf from job arguments
    conf_arg = args.get("--conf", "")
    if conf_arg:
        for conf in conf_arg.split(" --conf "):
            conf = conf.strip()
            if conf:
                cmd.extend(["--conf", conf])

    # The script path inside the container
    container_script = f"/tmp/{os.path.basename(script_path)}"
    cmd.append(container_script)

    # Append Glue job arguments (--key value pairs) after the script
    cmd.extend(spark_args)

    container_kwargs = {
        "image": image,
        "name": container_name,
        "command": cmd,
        "environment": container_env,
        "detach": True,
        "labels": {"ministack": "glue", "job_name": job_name},
    }

    if ms_network:
        container_kwargs["network"] = ms_network

    logger.info(
        "Glue: starting Spark container for %s (image=%s, network=%s)",
        job_name, image, ms_network or "host",
    )

    try:
        container = docker_client.containers.create(**container_kwargs)
        # Copy script into container (avoids Docker-in-Docker volume mount issues)
        import io
        import tarfile
        script_data = open(script_path, "rb").read()
        tar_buf = io.BytesIO()
        with tarfile.open(fileobj=tar_buf, mode="w") as tar:
            info = tarfile.TarInfo(name=os.path.basename(script_path))
            info.size = len(script_data)
            tar.addfile(info, io.BytesIO(script_data))
        tar_buf.seek(0)
        container.put_archive("/tmp", tar_buf)
        container.start()
    except Exception as e:
        logger.warning("Glue: failed to start Spark container for %s: %s", job_name, e)
        run["JobRunState"] = "FAILED"
        run["ErrorMessage"] = f"Docker container start failed: {e}"[:2000]
        try:
            container.remove(force=True)
        except Exception:
            pass
        return

    # Wait for container to finish
    try:
        result = container.wait(timeout=min(job.get("Timeout", 2880) * 60, 3600))
        exit_code = result.get("StatusCode", -1)
        logs = container.logs(tail=200).decode("utf-8", errors="replace")

        if exit_code == 0:
            run["JobRunState"] = "SUCCEEDED"
            logger.info("Glue: Spark job %s completed successfully", job_name)
        else:
            run["JobRunState"] = "FAILED"
            run["ErrorMessage"] = logs[-2000:] if logs else f"Exit code {exit_code}"
            logger.warning("Glue: Spark job %s failed (exit %d)", job_name, exit_code)
    except Exception as e:
        run["JobRunState"] = "FAILED"
        run["ErrorMessage"] = f"Container execution error: {e}"[:2000]
        logger.warning("Glue: Spark container for %s error: %s", job_name, e)
    finally:
        try:
            container.remove(force=True)
        except Exception:
            pass


def _get_job_run(data):
    job_name = data.get("JobName")
    run_id = data.get("RunId")
    for run in _job_runs.get(job_name, []):
        if run["Id"] == run_id:
            return json_response({"JobRun": run})
    return error_response_json("EntityNotFoundException", f"Job run {run_id} not found", 400)


def _get_job_runs(data):
    job_name = data.get("JobName")
    return json_response({"JobRuns": _job_runs.get(job_name, [])})


def _batch_stop_job_run(data):
    job_name = data.get("JobName")
    run_ids = data.get("JobRunIds", [])
    errors = []
    successful = []
    for run_id in run_ids:
        found = False
        for run in _job_runs.get(job_name, []):
            if run["Id"] == run_id:
                if run["JobRunState"] in ("STARTING", "RUNNING"):
                    run["JobRunState"] = "STOPPED"
                    run["CompletedOn"] = int(time.time())
                    run["LastModifiedOn"] = int(time.time())
                    successful.append({"JobName": job_name, "JobRunId": run_id})
                else:
                    errors.append({"JobName": job_name, "JobRunId": run_id,
                        "ErrorDetail": {"ErrorCode": "InvalidInputException",
                            "ErrorMessage": f"Run {run_id} is in state {run['JobRunState']}"}})
                found = True
                break
        if not found:
            errors.append({"JobName": job_name, "JobRunId": run_id,
                "ErrorDetail": {"ErrorCode": "EntityNotFoundException",
                    "ErrorMessage": "Run not found"}})
    return json_response({"SuccessfulSubmissions": successful, "Errors": errors})


# ---- Security Configurations ----

def _create_security_configuration(data):
    name = data.get("Name")
    if not name:
        return error_response_json("InvalidInputException", "Name is required", 400)
    if name in _security_configs:
        return error_response_json("AlreadyExistsException",
            f"Security configuration {name} already exists", 400)
    _security_configs[name] = {
        "Name": name,
        "CreatedTimeStamp": int(time.time()),
        "EncryptionConfiguration": data.get("EncryptionConfiguration", {}),
    }
    return json_response({"Name": name, "CreatedTimestamp": _security_configs[name]["CreatedTimeStamp"]})


def _delete_security_configuration(data):
    name = data.get("Name")
    if name not in _security_configs:
        return error_response_json("EntityNotFoundException",
            f"Security configuration {name} not found", 400)
    del _security_configs[name]
    return json_response({})


def _get_security_configuration(data):
    name = data.get("Name")
    config = _security_configs.get(name)
    if not config:
        return error_response_json("EntityNotFoundException",
            f"Security configuration {name} not found", 400)
    return json_response({"SecurityConfiguration": config})


def _get_security_configurations(data):
    return json_response({"SecurityConfigurations": list(_security_configs.values())})


# ---- Classifiers ----

def _create_classifier(data):
    grok = data.get("GrokClassifier")
    xml_cls = data.get("XMLClassifier")
    json_cls = data.get("JsonClassifier")
    csv_cls = data.get("CsvClassifier")

    classifier = grok or xml_cls or json_cls or csv_cls
    if not classifier:
        return error_response_json("InvalidInputException",
            "Must provide one of GrokClassifier, XMLClassifier, JsonClassifier, CsvClassifier", 400)

    name = classifier.get("Name")
    if not name:
        return error_response_json("InvalidInputException", "Classifier name is required", 400)
    if name in _classifiers:
        return error_response_json("AlreadyExistsException",
            f"Classifier {name} already exists", 400)

    cls_type = "GrokClassifier" if grok else "XMLClassifier" if xml_cls else "JsonClassifier" if json_cls else "CsvClassifier"
    _classifiers[name] = {
        cls_type: {**classifier, "CreationTime": int(time.time()), "LastUpdated": int(time.time()), "Version": 1},
    }
    return json_response({})


def _get_classifier(data):
    name = data.get("Name")
    cls = _classifiers.get(name)
    if not cls:
        return error_response_json("EntityNotFoundException", f"Classifier {name} not found", 400)
    return json_response({"Classifier": cls})


def _get_classifiers(data):
    return json_response({"Classifiers": list(_classifiers.values())})


def _delete_classifier(data):
    name = data.get("Name")
    if name not in _classifiers:
        return error_response_json("EntityNotFoundException", f"Classifier {name} not found", 400)
    del _classifiers[name]
    return json_response({})


# ---- Triggers ----

def _create_trigger(data):
    name = data.get("Name")
    if not name:
        return error_response_json("InvalidInputException", "Name is required", 400)
    if name in _triggers:
        return error_response_json("AlreadyExistsException", f"Trigger {name} already exists", 400)

    trigger_type = data.get("Type", "ON_DEMAND")
    _triggers[name] = {
        "Name": name,
        "Type": trigger_type,
        "State": "CREATED",
        "Schedule": data.get("Schedule", ""),
        "Predicate": data.get("Predicate", {}),
        "Actions": data.get("Actions", []),
        "Description": data.get("Description", ""),
        "WorkflowName": data.get("WorkflowName", ""),
        "Tags": data.get("Tags", {}),
        "CreatedOn": int(time.time()),
        "LastModifiedOn": int(time.time()),
    }
    if data.get("StartOnCreation", False):
        _triggers[name]["State"] = "ACTIVATED"
    if data.get("Tags"):
        arn = _arn("trigger", name)
        _tags[arn] = dict(data["Tags"])
    return json_response({"Name": name})


def _get_trigger(data):
    name = data.get("Name")
    trigger = _triggers.get(name)
    if not trigger:
        return error_response_json("EntityNotFoundException", f"Trigger {name} not found", 400)
    return json_response({"Trigger": trigger})


def _delete_trigger(data):
    name = data.get("Name")
    if name not in _triggers:
        return error_response_json("EntityNotFoundException", f"Trigger {name} not found", 400)
    del _triggers[name]
    _tags.pop(_arn("trigger", name), None)
    return json_response({"Name": name})


def _update_trigger(data):
    name = data.get("Name")
    if name not in _triggers:
        return error_response_json("EntityNotFoundException", f"Trigger {name} not found", 400)
    trigger_update = data.get("TriggerUpdate", {})
    updatable = {"Schedule", "Predicate", "Actions", "Description"}
    for k in updatable:
        if k in trigger_update:
            _triggers[name][k] = trigger_update[k]
    _triggers[name]["LastModifiedOn"] = int(time.time())
    return json_response({"Trigger": _triggers[name]})


def _start_trigger(data):
    name = data.get("Name")
    if name not in _triggers:
        return error_response_json("EntityNotFoundException", f"Trigger {name} not found", 400)
    _triggers[name]["State"] = "ACTIVATED"
    _triggers[name]["LastModifiedOn"] = int(time.time())
    return json_response({"Name": name})


def _stop_trigger(data):
    name = data.get("Name")
    if name not in _triggers:
        return error_response_json("EntityNotFoundException", f"Trigger {name} not found", 400)
    _triggers[name]["State"] = "DEACTIVATED"
    _triggers[name]["LastModifiedOn"] = int(time.time())
    return json_response({"Name": name})


def _list_triggers(data):
    dependent_job = data.get("DependentJobName", "")
    names = []
    for name, trigger in _triggers.items():
        if dependent_job:
            actions = trigger.get("Actions", [])
            if not any(a.get("JobName") == dependent_job for a in actions):
                continue
        names.append(name)
    return json_response({"TriggerNames": sorted(names)})


def _batch_get_triggers(data):
    requested = data.get("TriggerNames", [])
    found = [_triggers[n] for n in requested if n in _triggers]
    not_found = [n for n in requested if n not in _triggers]
    return json_response({"Triggers": found, "TriggersNotFound": not_found})


def _get_triggers(data):
    dependent_job = data.get("DependentJobName", "")
    triggers = []
    for trigger in _triggers.values():
        if dependent_job:
            actions = trigger.get("Actions", [])
            if not any(a.get("JobName") == dependent_job for a in actions):
                continue
        triggers.append(trigger)
    return json_response({"Triggers": triggers})


# ---- Workflows ----

def _create_workflow(data):
    name = data.get("Name")
    if not name:
        return error_response_json("InvalidInputException", "Name is required", 400)
    if name in _workflows:
        return error_response_json("AlreadyExistsException", f"Workflow {name} already exists", 400)
    _workflows[name] = {
        "Name": name,
        "Description": data.get("Description", ""),
        "DefaultRunProperties": data.get("DefaultRunProperties", {}),
        "CreatedOn": int(time.time()),
        "LastModifiedOn": int(time.time()),
        "MaxConcurrentRuns": data.get("MaxConcurrentRuns", 0),
    }
    if data.get("Tags"):
        _tags[_arn("workflow", name)] = dict(data["Tags"])
    _workflow_runs[name] = []
    return json_response({"Name": name})


def _get_workflow(data):
    name = data.get("Name")
    wf = _workflows.get(name)
    if not wf:
        return error_response_json("EntityNotFoundException", f"Workflow {name} not found", 400)
    result = dict(wf)
    runs = _workflow_runs.get(name, [])
    if runs:
        result["LastRun"] = runs[-1]
    return json_response({"Workflow": result})


def _delete_workflow(data):
    name = data.get("Name")
    if name not in _workflows:
        return error_response_json("EntityNotFoundException", f"Workflow {name} not found", 400)
    del _workflows[name]
    _workflow_runs.pop(name, None)
    _tags.pop(_arn("workflow", name), None)
    return json_response({"Name": name})


def _update_workflow(data):
    name = data.get("Name")
    if name not in _workflows:
        return error_response_json("EntityNotFoundException", f"Workflow {name} not found", 400)
    for k in ("Description", "DefaultRunProperties", "MaxConcurrentRuns"):
        if k in data:
            _workflows[name][k] = data[k]
    _workflows[name]["LastModifiedOn"] = int(time.time())
    return json_response({"Name": name})


def _start_workflow_run(data):
    name = data.get("Name")
    if name not in _workflows:
        return error_response_json("EntityNotFoundException", f"Workflow {name} not found", 400)
    run_id = new_uuid()
    run = {
        "WorkflowRunId": run_id,
        "Name": name,
        "Status": "RUNNING",
        "StartedOn": int(time.time()),
        "CompletedOn": None,
        "Statistics": {
            "TotalActions": 0, "RunningActions": 0, "StoppedActions": 0,
            "SucceededActions": 0, "FailedActions": 0, "TimeoutActions": 0,
        },
        "WorkflowRunProperties": dict(_workflows[name].get("DefaultRunProperties", {})),
    }
    _workflow_runs.setdefault(name, []).append(run)
    return json_response({"RunId": run_id})


# ---- User Defined Functions ----

def _udf_key(db_name: str, func_name: str) -> str:
    return f"{db_name}/{func_name}"


def _udf_record(db_name: str, fn_input: dict) -> dict:
    """Build a UserDefinedFunction record matching the botocore output shape:
    UserDefinedFunction { FunctionName, DatabaseName, ClassName, OwnerName,
    OwnerType, CreateTime, ResourceUris, CatalogId }."""
    return {
        "FunctionName": fn_input.get("FunctionName"),
        "DatabaseName": db_name,
        "ClassName": fn_input.get("ClassName"),
        "OwnerName": fn_input.get("OwnerName"),
        "OwnerType": fn_input.get("OwnerType"),
        "CreateTime": int(time.time()),
        "ResourceUris": fn_input.get("ResourceUris", []),
        "CatalogId": get_account_id(),
    }


def _create_user_defined_function(data):
    db_name = data.get("DatabaseName")
    if db_name not in _databases:
        return error_response_json("EntityNotFoundException", f"Database {db_name} not found.", 400)
    fn_input = data.get("FunctionInput") or {}
    func_name = fn_input.get("FunctionName")
    if not func_name:
        return error_response_json("InvalidInputException", "FunctionInput.FunctionName is required", 400)
    if not fn_input.get("ClassName"):
        return error_response_json("InvalidInputException", "FunctionInput.ClassName is required", 400)
    key = _udf_key(db_name, func_name)
    if key in _user_defined_functions:
        return error_response_json("AlreadyExistsException", f"User-defined function {func_name} already exists", 400)
    _user_defined_functions[key] = _udf_record(db_name, fn_input)
    return json_response({})


def _update_user_defined_function(data):
    db_name = data.get("DatabaseName")
    func_name = data.get("FunctionName")
    key = _udf_key(db_name, func_name)
    if key not in _user_defined_functions:
        return error_response_json("EntityNotFoundException", f"User-defined function {func_name} not found in {db_name}", 400)
    fn_input = data.get("FunctionInput") or {}
    existing = _user_defined_functions[key]
    for field in ("ClassName", "OwnerName", "OwnerType", "ResourceUris"):
        if field in fn_input:
            existing[field] = fn_input[field]
    # AWS allows renaming the function via FunctionInput.FunctionName.
    new_name = fn_input.get("FunctionName")
    if new_name and new_name != func_name:
        existing["FunctionName"] = new_name
        _user_defined_functions[_udf_key(db_name, new_name)] = existing
        del _user_defined_functions[key]
    return json_response({})


def _delete_user_defined_function(data):
    db_name = data.get("DatabaseName")
    func_name = data.get("FunctionName")
    key = _udf_key(db_name, func_name)
    if key not in _user_defined_functions:
        return error_response_json("EntityNotFoundException", f"User-defined function {func_name} not found in {db_name}", 400)
    del _user_defined_functions[key]
    return json_response({})


def _get_user_defined_function(data):
    db_name = data.get("DatabaseName")
    func_name = data.get("FunctionName")
    key = _udf_key(db_name, func_name)
    udf = _user_defined_functions.get(key)
    if not udf:
        return error_response_json("EntityNotFoundException", f"User-defined function {func_name} not found in {db_name}", 400)
    return json_response({"UserDefinedFunction": udf})


def _get_user_defined_functions(data):
    db_name = data.get("DatabaseName")
    pattern = data.get("Pattern") or ""
    # Real AWS accepts DatabaseName="*" or omitted to span all databases in the
    # catalog. Botocore marks DatabaseName as optional.
    if db_name and db_name != "*":
        items = [u for k, u in _user_defined_functions.items() if k.startswith(f"{db_name}/")]
    else:
        items = list(_user_defined_functions.values())
    if pattern:
        items = [u for u in items if _simple_glob_match(pattern, u.get("FunctionName", ""))]
    return json_response({"UserDefinedFunctions": items})


# ---- Tags ----

def _tag_resource(data):
    arn = data.get("ResourceArn", "")
    _tags[arn] = {**_tags.get(arn, {}), **data.get("TagsToAdd", {})}
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceArn", "")
    for key in data.get("TagsToRemove", []):
        _tags.get(arn, {}).pop(key, None)
    return json_response({})


def _get_tags(data):
    arn = data.get("ResourceArn", "")
    return json_response({"Tags": _tags.get(arn, {})})


# ---- Helpers ----

def _simple_glob_match(pattern, name):
    """Very simple glob matching: * matches anything."""
    return fnmatch.fnmatch(name, pattern)


def reset():
    _databases.clear()
    _tables.clear()
    _partitions.clear()
    _partition_indexes.clear()
    _connections.clear()
    _crawlers.clear()
    _jobs.clear()
    _job_runs.clear()
    _tags.clear()
    _security_configs.clear()
    _classifiers.clear()
    _triggers.clear()
    _workflows.clear()
    _workflow_runs.clear()
