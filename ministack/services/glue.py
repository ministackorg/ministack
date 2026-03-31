"""
Glue Service Emulator.
JSON-based API via X-Amz-Target (AWSGlue).
Supports full Data Catalog: Databases, Tables, Partitions, Connections, Crawlers, Jobs, JobRuns.
Also: SecurityConfigurations, Classifiers, PartitionIndexes, CrawlerMetrics, Tags,
      Triggers, Workflows.
Job execution runs Python scripts via subprocess in background threads.
Crawlers transition through RUNNING state with a configurable timer.
"""

import json
import logging
import os
import subprocess
import tempfile
import threading
import time

from ministack.core.responses import error_response_json, json_response, new_uuid

logger = logging.getLogger("glue")

ACCOUNT_ID = "000000000000"
REGION = "us-east-1"
CRAWLER_RUN_SECONDS = int(os.environ.get("GLUE_CRAWLER_RUN_SECONDS", "5"))
S3_DATA_DIR = os.environ.get("S3_DATA_DIR", "/tmp/ministack-data/s3")

_databases: dict = {}
_tables: dict = {}       # "db_name/table_name" -> table dict
_partitions: dict = {}   # "db_name/table_name" -> [partition, ...]
_partition_indexes: dict = {}  # "db_name/table_name" -> [index, ...]
_connections: dict = {}
_crawlers: dict = {}
_jobs: dict = {}
_job_runs: dict = {}     # job_name -> [run, ...]
_tags: dict = {}         # arn -> {key: value, ...}
_security_configs: dict = {}
_classifiers: dict = {}
_triggers: dict = {}     # trigger_name -> trigger dict
_workflows: dict = {}    # workflow_name -> workflow dict
_workflow_runs: dict = {} # workflow_name -> [run, ...]


def _arn(resource_type, name):
    return f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:{resource_type}/{name}"


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
        "LocationUri": db_input.get("LocationUri", ""),
        "Parameters": db_input.get("Parameters", {}),
        "CreateTime": time.time(),
        "CatalogId": ACCOUNT_ID,
    }
    return json_response({})


def _delete_database(data):
    name = data.get("Name")
    if name not in _databases:
        return error_response_json("EntityNotFoundException", f"Database {name} not found", 400)
    del _databases[name]
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
        "CreateTime": time.time(),
        "UpdateTime": time.time(),
        "LastAccessTime": time.time(),
        "StorageDescriptor": table_input.get("StorageDescriptor", {}),
        "PartitionKeys": table_input.get("PartitionKeys", []),
        "TableType": table_input.get("TableType", "EXTERNAL_TABLE"),
        "Parameters": table_input.get("Parameters", {}),
        "IsRegisteredWithLakeFormation": False,
        "CatalogId": ACCOUNT_ID,
    }
    return json_response({})


def _delete_table(data):
    db_name = data.get("DatabaseName")
    name = data.get("Name")
    key = f"{db_name}/{name}"
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
    safe_keys = {"Description", "Owner", "StorageDescriptor", "PartitionKeys",
                 "TableType", "Parameters", "ViewOriginalText", "ViewExpandedText"}
    for k in safe_keys:
        if k in table_input:
            _tables[key][k] = table_input[k]
    _tables[key]["UpdateTime"] = time.time()
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
        "CreationTime": time.time(),
        "LastAccessTime": time.time(),
        "CatalogId": ACCOUNT_ID,
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
                "CreationTime": time.time(),
                "CatalogId": ACCOUNT_ID,
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


# ---- Partition Indexes ----

def _create_partition_index(data):
    db_name = data.get("DatabaseName")
    table_name = data.get("TableName")
    index_input = data.get("PartitionIndex", {})
    key = f"{db_name}/{table_name}"
    if key not in _partition_indexes:
        _partition_indexes[key] = []
    _partition_indexes[key].append({
        "IndexName": index_input.get("IndexName", ""),
        "Keys": index_input.get("Keys", []),
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
    _connections[name] = {**conn_input, "CreationTime": time.time(), "LastUpdatedTime": time.time()}
    return json_response({})


def _delete_connection(data):
    _connections.pop(data.get("ConnectionName"), None)
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
        "CreationTime": time.time(),
        "LastUpdated": time.time(),
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
    crawler["LastUpdated"] = time.time()
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
                "EndTime": time.time(),
            }
            logger.info(f"Glue: Crawler {name} finished after {CRAWLER_RUN_SECONDS}s")

    timer = threading.Timer(CRAWLER_RUN_SECONDS, _finish_crawl)
    timer.daemon = True
    timer.start()

    logger.info(f"Glue: Crawler {name} started (will run for {CRAWLER_RUN_SECONDS}s)")
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
        "CreatedOn": time.time(),
        "LastModifiedOn": time.time(),
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
    _jobs[name]["LastModifiedOn"] = time.time()
    return json_response({"JobName": name})


def _resolve_script(script_location):
    """Resolve a script location to a local path. Supports local paths and s3:// URIs."""
    if not script_location:
        return None
    if os.path.exists(script_location):
        return script_location
    if script_location.startswith("s3://"):
        stripped = script_location[5:]
        parts = stripped.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        local_path = os.path.join(S3_DATA_DIR, bucket, key)
        if os.path.exists(local_path):
            return local_path
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
        "StartedOn": time.time(),
        "LastModifiedOn": time.time(),
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
        run["LastModifiedOn"] = time.time()

        script_location = job.get("Command", {}).get("ScriptLocation", "")
        resolved = _resolve_script(script_location)
        if resolved and resolved.endswith(".py"):
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
        else:
            run["JobRunState"] = "SUCCEEDED"

        run["CompletedOn"] = time.time()
        run["ExecutionTime"] = int(run["CompletedOn"] - run["StartedOn"])
        run["LastModifiedOn"] = time.time()

    thread = threading.Thread(target=_execute, daemon=True)
    thread.start()

    return json_response({"JobRunId": run_id})


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
                    run["CompletedOn"] = time.time()
                    run["LastModifiedOn"] = time.time()
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
        "CreatedTimeStamp": time.time(),
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
        cls_type: {**classifier, "CreationTime": time.time(), "LastUpdated": time.time(), "Version": 1},
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
        "CreatedOn": time.time(),
        "LastModifiedOn": time.time(),
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
    _triggers[name]["LastModifiedOn"] = time.time()
    return json_response({"Trigger": _triggers[name]})


def _start_trigger(data):
    name = data.get("Name")
    if name not in _triggers:
        return error_response_json("EntityNotFoundException", f"Trigger {name} not found", 400)
    _triggers[name]["State"] = "ACTIVATED"
    _triggers[name]["LastModifiedOn"] = time.time()
    return json_response({"Name": name})


def _stop_trigger(data):
    name = data.get("Name")
    if name not in _triggers:
        return error_response_json("EntityNotFoundException", f"Trigger {name} not found", 400)
    _triggers[name]["State"] = "DEACTIVATED"
    _triggers[name]["LastModifiedOn"] = time.time()
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
        "CreatedOn": time.time(),
        "LastModifiedOn": time.time(),
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
    _workflows[name]["LastModifiedOn"] = time.time()
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
        "StartedOn": time.time(),
        "CompletedOn": None,
        "Statistics": {
            "TotalActions": 0, "RunningActions": 0, "StoppedActions": 0,
            "SucceededActions": 0, "FailedActions": 0, "TimeoutActions": 0,
        },
        "WorkflowRunProperties": dict(_workflows[name].get("DefaultRunProperties", {})),
    }
    _workflow_runs.setdefault(name, []).append(run)
    return json_response({"RunId": run_id})


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
    import fnmatch
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
