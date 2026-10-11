# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Amazon Managed Service for Apache Flink (kinesisanalyticsv2) control plane.
JSON 1.1 via X-Amz-Target (prefix: KinesisAnalytics_20180523).

Verified against botocore kinesisanalyticsv2/2018-05-23/service-2.json:
  metadata.targetPrefix = "KinesisAnalytics_20180523"
  metadata.signingName  = "kinesisanalytics"
  metadata.protocol     = "json", jsonVersion "1.1"

Supports the operations a CDK deploy and a lifecycle script use:
  CreateApplication, DescribeApplication, UpdateApplication, DeleteApplication,
  StartApplication, StopApplication,
  CreateApplicationSnapshot, ListApplicationSnapshots,
  DescribeApplicationSnapshot, DeleteApplicationSnapshot,
  AddApplicationCloudWatchLoggingOption, DeleteApplicationCloudWatchLoggingOption,
  TagResource, UntagResource, ListTagsForResource.

This is the control plane: applications move through the model's statuses
and keep snapshot records, but no Flink job runs.

Records keep the request shape of ApplicationConfiguration; DescribeApplication
renders the Description shape from it. Stores are per account and region.
"""

import base64
import copy
import hashlib
import json
import logging
import time

from ministack.core.responses import (
    AccountRegionScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("kinesisanalyticsv2")

_applications = AccountRegionScopedDict()  # ApplicationName -> record
_snapshots = AccountRegionScopedDict()     # ApplicationName -> {SnapshotName -> snapshot}
_tags = AccountRegionScopedDict()          # ApplicationARN -> {Key: Value}

_SUPPORTED_RUNTIME_PREFIX = "FLINK-"
# Statuses in which the application is busy with an operation.
_TRANSITIONAL = ("STARTING", "STOPPING", "FORCE_STOPPING", "UPDATING", "DELETING")
# What a transitional status settles into on the next read.
_SETTLES_TO = {
    "STARTING": "RUNNING",
    "UPDATING": "RUNNING",
    "STOPPING": "READY",
    "FORCE_STOPPING": "READY",
}

# Defaults AWS reports for ConfigurationType DEFAULT (Managed Service for
# Apache Flink developer guide: checkpointing, parallelism and monitoring).
_CHECKPOINT_DEFAULTS = {"CheckpointingEnabled": True, "CheckpointInterval": 60000,
                        "MinPauseBetweenCheckpoints": 5000}
_MONITORING_DEFAULTS = {"MetricsLevel": "APPLICATION", "LogLevel": "INFO"}
_PARALLELISM_DEFAULTS = {"Parallelism": 1, "ParallelismPerKPU": 1, "AutoScalingEnabled": True}


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def get_state():
    return copy.deepcopy({
        "applications": _applications,
        "snapshots": _snapshots,
        "tags": _tags,
    })


def load_persisted_state(data):
    if not data:
        return
    for store, key in ((_applications, "applications"), (_snapshots, "snapshots"), (_tags, "tags")):
        store.clear()
        store.update(data.get(key) or {})
    # No Flink job survives a restart, so a running or busy application
    # comes back READY and can be started again.
    for record in _applications.all_values():
        record["ApplicationStatus"] = "READY"


def reset():
    _applications.clear()
    _snapshots.clear()
    _tags.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _arn(name):
    return f"arn:aws:kinesisanalytics:{get_region()}:{get_account_id()}:application/{name}"


def _error(code, message, status=400):
    return error_response_json(code, message, status)


def _not_found(name):
    return _error("ResourceNotFoundException", f"Application '{name}' does not exist.")


def _new_token():
    return new_uuid().replace("-", "")[:16]


def _bump_version(record):
    record["ApplicationVersionId"] += 1
    record["ConditionalToken"] = _new_token()
    record["LastUpdateTimestamp"] = time.time()


def _check_concurrency(record, data):
    """ConcurrentModificationException on a stale version id or conditional token."""
    version = data.get("CurrentApplicationVersionId")
    if version is not None and version != record["ApplicationVersionId"]:
        return _error("ConcurrentModificationException",
                      "The provided application version ID does not match the most recent version "
                      "for the application.")
    token = data.get("ConditionalToken")
    if token is not None and token != record["ConditionalToken"]:
        return _error("ConcurrentModificationException",
                      "The provided conditional token does not match the current conditional token "
                      "for the application.")
    return None


def _settle(record):
    """No Flink job runs, so a transitional status settles on the next read
    and a script polling DescribeApplication sees each status once."""
    target = _SETTLES_TO.get(record["ApplicationStatus"])
    if target:
        record["ApplicationStatus"] = target


def _get(name):
    record = _applications.get(name)
    if record is not None:
        _settle(record)
    return record


# ---------------------------------------------------------------------------
# Rendering: request-shape configuration -> Description shapes
# ---------------------------------------------------------------------------

def _code_description(code_config, code_stats=None):
    content = code_config.get("CodeContent") or {}
    described = {}
    if "S3ContentLocation" in content:
        loc = content["S3ContentLocation"]
        described["S3ApplicationCodeLocationDescription"] = {
            k: loc[k] for k in ("BucketARN", "FileKey", "ObjectVersion") if k in loc
        }
        # AWS reports the size and MD5 of the S3 object too.
        described.update(code_stats or {})
    if "TextContent" in content:
        described["TextContent"] = content["TextContent"]
    if "ZipFileContent" in content:
        raw = base64.b64decode(content["ZipFileContent"])
        described["CodeMD5"] = hashlib.md5(raw).hexdigest()
        described["CodeSize"] = len(raw)
    return {"CodeContentType": code_config.get("CodeContentType", "ZIPFILE"),
            "CodeContentDescription": described}


def _with_defaults(section, defaults):
    out = {"ConfigurationType": section.get("ConfigurationType", "DEFAULT")}
    if out["ConfigurationType"] == "DEFAULT":
        out.update(defaults)
    else:
        out.update({k: section[k] for k in defaults if k in section})
    return out


def _flink_description(flink):
    parallelism = _with_defaults(flink.get("ParallelismConfiguration") or {}, _PARALLELISM_DEFAULTS)
    parallelism["CurrentParallelism"] = parallelism.get("Parallelism", 1)
    return {
        "CheckpointConfigurationDescription":
            _with_defaults(flink.get("CheckpointConfiguration") or {}, _CHECKPOINT_DEFAULTS),
        "MonitoringConfigurationDescription":
            _with_defaults(flink.get("MonitoringConfiguration") or {}, _MONITORING_DEFAULTS),
        "ParallelismConfigurationDescription": parallelism,
    }


def _configuration_description(record):
    config = record.get("_Configuration") or {}
    described = {}
    if "ApplicationCodeConfiguration" in config:
        described["ApplicationCodeConfigurationDescription"] = _code_description(
            config["ApplicationCodeConfiguration"], record.get("_CodeStats"))
    described["FlinkApplicationConfigurationDescription"] = _flink_description(
        config.get("FlinkApplicationConfiguration") or {})
    groups = (config.get("EnvironmentProperties") or {}).get("PropertyGroups")
    if groups is not None:
        described["EnvironmentPropertyDescriptions"] = {"PropertyGroupDescriptions": copy.deepcopy(groups)}
    snapshots = config.get("ApplicationSnapshotConfiguration") or {}
    described["ApplicationSnapshotConfigurationDescription"] = {
        "SnapshotsEnabled": bool(snapshots.get("SnapshotsEnabled", False))}
    rollback = config.get("ApplicationSystemRollbackConfiguration") or {}
    described["ApplicationSystemRollbackConfigurationDescription"] = {
        "RollbackEnabled": bool(rollback.get("RollbackEnabled", False))}
    described["ApplicationEncryptionConfigurationDescription"] = _encryption_description(record)
    if record.get("_RunConfiguration"):
        run = record["_RunConfiguration"]
        described["RunConfigurationDescription"] = {
            "ApplicationRestoreConfigurationDescription": copy.deepcopy(
                run.get("ApplicationRestoreConfiguration") or {"ApplicationRestoreType": "RESTORE_FROM_LATEST_SNAPSHOT"}),
            "FlinkRunConfigurationDescription": copy.deepcopy(
                run.get("FlinkRunConfiguration") or {"AllowNonRestoredState": False}),
        }
    return described


def _encryption_description(record):
    """AWS reports an AWS-owned key when none is configured."""
    configured = (record.get("_Configuration") or {}).get("ApplicationEncryptionConfiguration")
    return copy.deepcopy(configured) if configured else {"KeyType": "AWS_OWNED_KEY"}


# The maintenance window AWS reported for a new application (us-east-1).
_MAINTENANCE_WINDOW = {"ApplicationMaintenanceWindowStartTime": "03:00",
                       "ApplicationMaintenanceWindowEndTime": "11:00"}


def _refresh_code_stats(record):
    """Size and MD5 of the application's S3 code, as DescribeApplication reports them."""
    loc = (((record.get("_Configuration") or {}).get("ApplicationCodeConfiguration") or {})
           .get("CodeContent") or {}).get("S3ContentLocation")
    record.pop("_CodeStats", None)
    if not loc:
        return
    try:
        from ministack.services import s3 as _s3

        data = _s3._get_object_data(loc.get("BucketARN", "").split(":::", 1)[-1], loc.get("FileKey", ""),
                                    loc.get("ObjectVersion") or None)
    except Exception:
        data = None
    if data is not None:
        record["_CodeStats"] = {"CodeMD5": hashlib.md5(data).hexdigest(), "CodeSize": len(data)}


def application_detail(record):
    """The ApplicationDetail shape for a stored record."""
    detail = {k: copy.deepcopy(v) for k, v in record.items() if not k.startswith("_")}
    detail["ApplicationConfigurationDescription"] = _configuration_description(record)
    detail["CloudWatchLoggingOptionDescriptions"] = copy.deepcopy(record.get("_LoggingOptions", []))
    detail["ApplicationMaintenanceConfigurationDescription"] = dict(_MAINTENANCE_WINDOW)
    return detail


# ---------------------------------------------------------------------------
# Applications
# ---------------------------------------------------------------------------

def _logging_option(option, option_id):
    return {
        "CloudWatchLoggingOptionId": option_id,
        "LogStreamARN": option["LogStreamARN"],
        **({"RoleARN": option["RoleARN"]} if option.get("RoleARN") else {}),
    }


def create_application(data):
    """CreateApplication; also used by the CloudFormation provisioner."""
    name = data.get("ApplicationName")
    runtime = data.get("RuntimeEnvironment")
    role = data.get("ServiceExecutionRole")
    if not name or not runtime or not role:
        return _error("InvalidArgumentException",
                      "ApplicationName, RuntimeEnvironment and ServiceExecutionRole are required.")
    if not runtime.startswith(_SUPPORTED_RUNTIME_PREFIX):
        return _error("InvalidArgumentException",
                      f"MiniStack supports Apache Flink applications only; RuntimeEnvironment {runtime} "
                      "(SQL and Studio applications) is not implemented.")
    if name in _applications:
        return _error("ResourceInUseException", f"Application '{name}' already exists.")
    now = time.time()
    record = {
        "ApplicationARN": _arn(name),
        "ApplicationName": name,
        "RuntimeEnvironment": runtime,
        "ServiceExecutionRole": role,
        "ApplicationStatus": "READY",
        "ApplicationVersionId": 1,
        "CreateTimestamp": now,
        "LastUpdateTimestamp": now,
        "ConditionalToken": _new_token(),
        "ApplicationMode": data.get("ApplicationMode", "STREAMING"),
        "_Configuration": copy.deepcopy(data.get("ApplicationConfiguration") or {}),
        "_LoggingOptions": [_logging_option(o, f"1.{i + 1}")
                            for i, o in enumerate(data.get("CloudWatchLoggingOptions") or [])],
    }
    _refresh_code_stats(record)
    if data.get("ApplicationDescription"):
        record["ApplicationDescription"] = data["ApplicationDescription"]
    _applications[name] = record
    _snapshots[name] = {}
    tags = {t["Key"]: t.get("Value", "") for t in data.get("Tags") or [] if "Key" in t}
    if tags:
        _tags[record["ApplicationARN"]] = tags
    logger.info("CreateApplication %s (%s)", name, runtime)
    return json_response({"ApplicationDetail": application_detail(record)})


def _describe_application(data):
    record = _get(data.get("ApplicationName"))
    if record is None:
        return _not_found(data.get("ApplicationName"))
    return json_response({"ApplicationDetail": application_detail(record)})


def _strip_update_suffix(update):
    """Turn an ApplicationConfigurationUpdate into ApplicationConfiguration
    form: every member is named after its target plus "Update"."""
    if not isinstance(update, dict):
        return update
    out = {}
    for key, value in update.items():
        if key == "EnvironmentPropertyUpdates":
            out["EnvironmentProperties"] = copy.deepcopy(value)
            continue
        target = key[: -len("Update")] if key.endswith("Update") else key
        out[target] = _strip_update_suffix(value)
    return out


def _merge(base, patch):
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict) and key != "EnvironmentProperties":
            _merge(base[key], value)
        else:
            base[key] = copy.deepcopy(value)


def update_application(data):
    """UpdateApplication; also used by the CloudFormation provisioner."""
    name = data.get("ApplicationName")
    record = _get(name)
    if record is None:
        return _not_found(name)
    if record["ApplicationStatus"] not in ("READY", "RUNNING"):
        return _error("ResourceInUseException",
                      f"Application cannot be updated in '{record['ApplicationStatus']}' state")
    conflict = _check_concurrency(record, data)
    if conflict:
        return conflict
    if data.get("ApplicationConfigurationUpdate"):
        _merge(record["_Configuration"], _strip_update_suffix(data["ApplicationConfigurationUpdate"]))
    if data.get("ServiceExecutionRoleUpdate"):
        record["ServiceExecutionRole"] = data["ServiceExecutionRoleUpdate"]
    if data.get("RuntimeEnvironmentUpdate"):
        record["RuntimeEnvironment"] = data["RuntimeEnvironmentUpdate"]
    for option in data.get("CloudWatchLoggingOptionUpdates") or []:
        for existing in record["_LoggingOptions"]:
            if existing["CloudWatchLoggingOptionId"] == option.get("CloudWatchLoggingOptionId"):
                if option.get("LogStreamARNUpdate"):
                    existing["LogStreamARN"] = option["LogStreamARNUpdate"]
    if data.get("RunConfigurationUpdate"):
        run = record.setdefault("_RunConfiguration", {})
        _merge(run, _strip_update_suffix(data["RunConfigurationUpdate"]))
    previous_version = record["ApplicationVersionId"]
    _bump_version(record)
    _refresh_code_stats(record)
    operation_id = _new_token()
    if record["ApplicationStatus"] == "RUNNING":
        _restart_for_update(record, previous_version)
    logger.info("UpdateApplication %s -> version %d", name, record["ApplicationVersionId"])
    return json_response({"ApplicationDetail": application_detail(record), "OperationId": operation_id})


def replace_from_template(name, props):
    """CloudFormation update: the template's ApplicationConfiguration replaces
    the stored one (UpdateApplication takes a diff, a template carries the
    whole desired state). A running application restarts on it."""
    record = _get(name)
    if record is None:
        raise ValueError(f"Application {name} is not found.")
    record["_Configuration"] = copy.deepcopy(props.get("ApplicationConfiguration") or {})
    for key in ("RuntimeEnvironment", "ServiceExecutionRole"):
        if props.get(key):
            record[key] = props[key]
    if props.get("ApplicationDescription") is not None:
        record["ApplicationDescription"] = props["ApplicationDescription"]
    if props.get("RunConfiguration") is not None:
        record["_RunConfiguration"] = copy.deepcopy(props["RunConfiguration"])
    previous_version = record["ApplicationVersionId"]
    _bump_version(record)
    _refresh_code_stats(record)
    if record["ApplicationStatus"] == "RUNNING":
        _restart_for_update(record, previous_version)


def delete_application(data, check_timestamp=True):
    """DeleteApplication; also used by the CloudFormation provisioner."""
    name = data.get("ApplicationName")
    record = _get(name)
    if record is None:
        return _not_found(name)
    if check_timestamp:
        stamp = data.get("CreateTimestamp")
        if stamp is None or abs(float(stamp) - record["CreateTimestamp"]) >= 1:
            return _error("ConcurrentModificationException",
                          f"The application with name: '{name}' has a different create timestamp than the "
                          "one you provided. Please call DescribeApplication to fetch valid create timestamp.")
    if record["ApplicationStatus"] in _TRANSITIONAL:
        return _error("ResourceInUseException",
                      f"Application {name} is in {record['ApplicationStatus']} status and cannot be deleted.")
    del _applications[name]
    _snapshots.pop(name, None)
    _tags.pop(record["ApplicationARN"], None)
    logger.info("DeleteApplication %s", name)
    return json_response({})


# ---------------------------------------------------------------------------
# Start / stop
# ---------------------------------------------------------------------------

def _snapshots_enabled(record):
    return bool((record["_Configuration"].get("ApplicationSnapshotConfiguration") or {}).get("SnapshotsEnabled"))


def _restart_for_update(record, previous_version):
    """A running application restarts on its new configuration. With snapshots
    enabled, Managed Flink first takes an UPDATEAPPLICATION-<app>-<ms> snapshot
    carrying the version before the update (observed on AWS)."""
    record["ApplicationStatus"] = "UPDATING"
    snapshot_name = None
    if _snapshots_enabled(record):
        snapshot_name = f"UPDATEAPPLICATION-{record['ApplicationName']}-{int(time.time() * 1000)}"
    if snapshot_name:
        _add_snapshot(record, snapshot_name)["ApplicationVersionId"] = previous_version


def _start_application(data):
    name = data.get("ApplicationName")
    record = _get(name)
    if record is None:
        return _not_found(name)
    if record["ApplicationStatus"] != "READY":
        return _error("ResourceInUseException",
                      f"Application cannot be started in '{record['ApplicationStatus']}' state")
    run = copy.deepcopy(data.get("RunConfiguration") or {})
    restore = run.get("ApplicationRestoreConfiguration") or {}
    if restore.get("ApplicationRestoreType") == "RESTORE_FROM_CUSTOM_SNAPSHOT":
        snap = (_snapshots.get(name) or {}).get(restore.get("SnapshotName", ""))
        if not snap:
            return _error("InvalidArgumentException",
                          f"The snapshot name {restore.get('SnapshotName')} provided for restore configuration "
                          "does not exist.")
    record["_RunConfiguration"] = run
    record["ApplicationStatus"] = "STARTING"
    logger.info("StartApplication %s: control plane only, so the Flink job does not run", name)
    return json_response({"OperationId": _new_token()})


def _stop_application(data):
    name = data.get("ApplicationName")
    record = _get(name)
    if record is None:
        return _not_found(name)
    force = bool(data.get("Force"))
    status = record["ApplicationStatus"]
    if status != "RUNNING" and not (force and status in ("STARTING", "UPDATING")):
        return _error("ResourceInUseException", f"Application cannot be Stopped in '{status}' state")
    take_snapshot = _snapshots_enabled(record) and not force
    record["ApplicationStatus"] = "FORCE_STOPPING" if force else "STOPPING"
    # Managed Flink takes a snapshot on a graceful stop when snapshots are enabled.
    snapshot_name = f"STOP-{name}-{int(time.time() * 1000)}" if take_snapshot else None
    if snapshot_name:
        _add_snapshot(record, snapshot_name)
    return json_response({"OperationId": _new_token()})


# ---------------------------------------------------------------------------
# Snapshots
# ---------------------------------------------------------------------------

def _add_snapshot(record, snapshot_name):
    snap = {
        "SnapshotName": snapshot_name,
        "SnapshotStatus": "READY",
        "ApplicationVersionId": record["ApplicationVersionId"],
        "SnapshotCreationTimestamp": time.time(),
        "RuntimeEnvironment": record["RuntimeEnvironment"],
        "ApplicationEncryptionConfigurationDescription": _encryption_description(record),
    }
    _snapshots.setdefault(record["ApplicationName"], {})[snapshot_name] = snap
    return snap


def _snapshot_view(snap):
    return {k: v for k, v in snap.items() if not k.startswith("_")}


def _create_application_snapshot(data):
    name = data.get("ApplicationName")
    snapshot_name = data.get("SnapshotName")
    record = _get(name)
    if record is None:
        return _not_found(name)
    # AWS accepts a manual snapshot with SnapshotsEnabled false; it only needs RUNNING.
    if record["ApplicationStatus"] != "RUNNING":
        return _error("InvalidRequestException",
                      f"Application {name} is in {record['ApplicationStatus']} status. Snapshot creation "
                      "is only allowed when application is in RUNNING status")
    if snapshot_name in (_snapshots.get(name) or {}):
        return _error("ResourceInUseException",
                      f"Provided Snapshot name {snapshot_name} already exists for application {name}")
    # No job runs, so the snapshot is a record without saved state.
    _add_snapshot(record, snapshot_name)
    return json_response({})


def _list_application_snapshots(data):
    name = data.get("ApplicationName")
    if _get(name) is None:
        return _not_found(name)
    # Oldest first, as AWS lists them.
    snaps = sorted((_snapshots.get(name) or {}).values(), key=lambda s: s["SnapshotCreationTimestamp"])
    limit = int(data.get("Limit") or 50)
    start = int(data.get("NextToken") or 0)
    page = snaps[start:start + limit]
    out = {"SnapshotSummaries": [_snapshot_view(s) for s in page]}
    if start + limit < len(snaps):
        out["NextToken"] = str(start + limit)
    return json_response(out)


def _describe_application_snapshot(data):
    name = data.get("ApplicationName")
    if _get(name) is None:
        return _not_found(name)
    snap = (_snapshots.get(name) or {}).get(data.get("SnapshotName"))
    if not snap:
        return _error("ResourceNotFoundException",
                      f"Snapshot '{data.get('SnapshotName')}' does not exist for application {name}.")
    return json_response({"SnapshotDetails": _snapshot_view(snap)})


def _delete_application_snapshot(data):
    name = data.get("ApplicationName")
    if _get(name) is None:
        return _not_found(name)
    snaps = _snapshots.get(name) or {}
    snap = snaps.get(data.get("SnapshotName"))
    if not snap:
        return _error("ResourceNotFoundException",
                      f"Snapshot '{data.get('SnapshotName')}' does not exist for application {name}.")
    stamp = data.get("SnapshotCreationTimestamp")
    if stamp is None or abs(float(stamp) - snap["SnapshotCreationTimestamp"]) >= 1:
        return _error("InvalidArgumentException",
                      f"Snapshot '{data['SnapshotName']}' has a different SnapshotCreationTimestamp than the one "
                      f"you provided '{int(float(stamp or 0) * 1000)}'. Please call DescribeApplicationSnapshot "
                      "to fetch valid SnapshotCreationTimestamp.")
    del snaps[data["SnapshotName"]]
    return json_response({})


# ---------------------------------------------------------------------------
# CloudWatch logging options
# ---------------------------------------------------------------------------

def add_logging_option(data):
    """AddApplicationCloudWatchLoggingOption; also used by CloudFormation."""
    name = data.get("ApplicationName")
    record = _get(name)
    if record is None:
        return _not_found(name)
    conflict = _check_concurrency(record, data)
    if conflict:
        return conflict
    option = data.get("CloudWatchLoggingOption") or {}
    if not option.get("LogStreamARN"):
        return _error("InvalidArgumentException", "CloudWatchLoggingOption.LogStreamARN is required.")
    if record["_LoggingOptions"]:
        return _error("InvalidArgumentException", "Kinesis Analytics currently supports only 1 logging option.")
    _bump_version(record)
    # The id carries the version the add creates ("5.1" for an add that made version 5 on AWS).
    record["_LoggingOptions"].append(_logging_option(option, f"{record['ApplicationVersionId']}.1"))
    return json_response(_logging_response(record, with_options=True))


def delete_logging_option(data):
    """DeleteApplicationCloudWatchLoggingOption; also used by CloudFormation."""
    name = data.get("ApplicationName")
    record = _get(name)
    if record is None:
        return _not_found(name)
    conflict = _check_concurrency(record, data)
    if conflict:
        return conflict
    option_id = data.get("CloudWatchLoggingOptionId")
    remaining = [o for o in record["_LoggingOptions"] if o["CloudWatchLoggingOptionId"] != option_id]
    if len(remaining) == len(record["_LoggingOptions"]):
        return _error("ResourceNotFoundException", f"Logging option {option_id} is not found.")
    record["_LoggingOptions"] = remaining
    _bump_version(record)
    return json_response(_logging_response(record, with_options=False))


def _logging_response(record, with_options):
    """AWS answers an add with the options and a delete with the ARN and version only."""
    out = {"ApplicationARN": record["ApplicationARN"], "ApplicationVersionId": record["ApplicationVersionId"]}
    if with_options:
        out["CloudWatchLoggingOptionDescriptions"] = copy.deepcopy(record["_LoggingOptions"])
    return out


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _application_for_arn(arn):
    name = arn.split("application/", 1)[-1] if "application/" in arn else ""
    record = _applications.get(name)
    return record if record is not None and record["ApplicationARN"] == arn else None


def _tag_resource(data):
    arn = data.get("ResourceARN", "")
    if _application_for_arn(arn) is None:
        return _error("ResourceNotFoundException", f"Resource {arn} is not found.")
    tags = _tags.get(arn) or {}
    tags.update({t["Key"]: t.get("Value", "") for t in data.get("Tags") or [] if "Key" in t})
    _tags[arn] = tags
    return json_response({})


def _untag_resource(data):
    arn = data.get("ResourceARN", "")
    if _application_for_arn(arn) is None:
        return _error("ResourceNotFoundException", f"Resource {arn} is not found.")
    tags = _tags.get(arn) or {}
    for key in data.get("TagKeys") or []:
        tags.pop(key, None)
    _tags[arn] = tags
    return json_response({})


def _list_tags_for_resource(data):
    arn = data.get("ResourceARN", "")
    if _application_for_arn(arn) is None:
        return _error("ResourceNotFoundException", f"Resource {arn} is not found.")
    return json_response({"Tags": [{"Key": k, "Value": v} for k, v in (_tags.get(arn) or {}).items()]})


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

_HANDLERS = {
    "CreateApplication": create_application,
    "DescribeApplication": _describe_application,
    "UpdateApplication": update_application,
    "DeleteApplication": delete_application,
    "StartApplication": _start_application,
    "StopApplication": _stop_application,
    "CreateApplicationSnapshot": _create_application_snapshot,
    "ListApplicationSnapshots": _list_application_snapshots,
    "DescribeApplicationSnapshot": _describe_application_snapshot,
    "DeleteApplicationSnapshot": _delete_application_snapshot,
    "AddApplicationCloudWatchLoggingOption": add_logging_option,
    "DeleteApplicationCloudWatchLoggingOption": delete_logging_option,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return _error("SerializationException", "Invalid JSON")
    handler = _HANDLERS.get(action)
    if handler is None:
        return _error("UnsupportedOperationException",
                      f"MiniStack does not implement kinesisanalyticsv2 {action or 'request'}.")
    return handler(data)
