# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
CloudFormation drift detection: compare a stack resource's expected properties
(the resolved template properties the stack record keeps) with what the
owning service's store holds now.

Each supported type has a reader in ``_DRIFT_READERS``: a function that looks
the resource up by its physical id and returns its current properties in the
template's shape (``None`` when the record is gone, which is ``DELETED``), plus
the set of properties it can read. Only properties the template sets
explicitly and the reader covers are compared, as the drift user guide says
("CloudFormation only determines drift for property values that are
explicitly set"). A reader with an empty set is an existence check. A type
without a reader is ``NOT_CHECKED``.
"""

import copy
import json
import logging

import ministack.services.cloudwatch_logs as _cw_logs
import ministack.services.dynamodb as _dynamodb
import ministack.services.ecr as _ecr
import ministack.services.eventbridge as _eb
import ministack.services.iam as _iam
import ministack.services.kinesis as _kinesis
import ministack.services.lambda_svc as _lambda_svc
import ministack.services.s3 as _s3
import ministack.services.secretsmanager as _sm
import ministack.services.sns as _sns
import ministack.services.sqs as _sqs
import ministack.services.ssm as _ssm
import ministack.services.stepfunctions as _sfn

from .provisioners import _STACK_TAG_PROPERTY, _tag_map

logger = logging.getLogger("cloudformation")

# The stack statuses the drift user guide lists for drift detection.
DRIFT_DETECTABLE_STATUSES = frozenset({
    "CREATE_COMPLETE", "UPDATE_COMPLETE", "UPDATE_ROLLBACK_COMPLETE",
    "UPDATE_ROLLBACK_FAILED",
})

RESOURCE_DRIFT_STATUSES = ("IN_SYNC", "MODIFIED", "DELETED", "NOT_CHECKED",
                           "UNKNOWN", "UNSUPPORTED")


def _json_or(value):
    """A policy-like attribute a service keeps as a JSON string, parsed; the
    template writes it as an object."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


# ---------------------------------------------------------------------------
# Readers: physical id + expected properties -> current properties or None
# ---------------------------------------------------------------------------

def _read_sqs_queue(physical_id, props):
    queue = _sqs._queues.get(physical_id)
    if queue is None:
        return None
    attrs = queue.get("attributes", {})
    out = {"QueueName": queue.get("name"), "Tags": queue.get("tags", {}),
           "FifoQueue": bool(queue.get("is_fifo"))}
    for key in ("VisibilityTimeout", "MaximumMessageSize", "MessageRetentionPeriod",
                "DelaySeconds", "ReceiveMessageWaitTimeSeconds",
                "ContentBasedDeduplication"):
        if key in attrs:
            out[key] = attrs[key]
    for key in ("RedrivePolicy", "RedriveAllowPolicy"):
        if attrs.get(key):
            out[key] = _json_or(attrs[key])
    return out


def _read_ssm_parameter(physical_id, props):
    record = _ssm._parameters.get(physical_id)
    if record is None:
        return None
    out = {
        "Name": record.get("Name"),
        "Type": record.get("Type"),
        "Value": record.get("OriginalValue", record.get("Value")),
        "Description": record.get("Description", ""),
        "AllowedPattern": record.get("AllowedPattern", ""),
        "Tier": record.get("Tier"),
        "DataType": record.get("DataType"),
        "Tags": _ssm._tags.get(record.get("ARN") or _ssm._param_arn(physical_id), {}),
    }
    return {k: v for k, v in out.items() if v is not None}


def _read_sns_topic(physical_id, props):
    topic = _sns._topics.get(physical_id)
    if topic is None:
        return None
    out = {"TopicName": topic.get("name"), "Tags": topic.get("tags", {})}
    if "DisplayName" in topic.get("attributes", {}):
        out["DisplayName"] = topic["attributes"]["DisplayName"]
    return out


def _read_lambda_function(physical_id, props):
    func = _lambda_svc._functions.get(physical_id)
    if func is None:
        return None
    config = func.get("config", {})
    out = {"FunctionName": config.get("FunctionName"), "Tags": func.get("tags", {})}
    for key in ("Runtime", "Handler", "Role", "Timeout", "MemorySize", "Description",
                "Architectures", "PackageType"):
        if key in config:
            out[key] = config[key]
    variables = (config.get("Environment") or {}).get("Variables")
    if variables is not None:
        out["Environment"] = {"Variables": variables}
    if config.get("EphemeralStorage"):
        out["EphemeralStorage"] = {"Size": config["EphemeralStorage"].get("Size")}
    if config.get("TracingConfig"):
        out["TracingConfig"] = {"Mode": config["TracingConfig"].get("Mode")}
    return out


def _read_iam_role(physical_id, props):
    role = _iam._roles.get(physical_id)
    if role is None:
        return None
    return {
        "RoleName": role.get("RoleName"),
        "Path": role.get("Path", "/"),
        "Description": role.get("Description", ""),
        "MaxSessionDuration": role.get("MaxSessionDuration"),
        "AssumeRolePolicyDocument": _json_or(role.get("AssumeRolePolicyDocument")),
        "Tags": _tag_map(role.get("Tags")),
    }


def _read_dynamodb_table(physical_id, props):
    table = _dynamodb._tables.get(physical_id)
    if table is None:
        return None
    billing = (table.get("BillingModeSummary") or {}).get("BillingMode", "PROVISIONED")
    out = {
        "TableName": table.get("TableName"),
        "BillingMode": billing,
        "KeySchema": table.get("KeySchema", []),
        "AttributeDefinitions": table.get("AttributeDefinitions", []),
        "DeletionProtectionEnabled": table.get("DeletionProtectionEnabled", False),
        "Tags": _tag_map(_dynamodb._tags.get(table.get("TableArn"), [])),
    }
    throughput = table.get("ProvisionedThroughput") or {}
    if billing == "PROVISIONED" and throughput:
        out["ProvisionedThroughput"] = {
            k: throughput[k] for k in ("ReadCapacityUnits", "WriteCapacityUnits")
            if k in throughput
        }
    stream = table.get("StreamSpecification") or {}
    if stream.get("StreamViewType"):
        out["StreamSpecification"] = {"StreamViewType": stream["StreamViewType"]}
    return out


def _read_s3_bucket(physical_id, props):
    if physical_id not in _s3._buckets:
        return None
    out = {"BucketName": physical_id}
    status = _s3._bucket_versioning.get(physical_id)
    if status:
        out["VersioningConfiguration"] = {"Status": status}
    return out


def _read_log_group(physical_id, props):
    group = _cw_logs._log_groups.get(physical_id)
    if group is None:
        return None
    out = {"LogGroupName": physical_id, "Tags": group.get("tags", {})}
    if group.get("retentionInDays") is not None:
        out["RetentionInDays"] = group["retentionInDays"]
    if group.get("logGroupClass"):
        out["LogGroupClass"] = group["logGroupClass"]
    return out


def _read_secret(physical_id, props):
    secret = _sm._secrets.get(physical_id)
    if secret is None:
        return None
    return {
        "Name": secret.get("Name"),
        "Description": secret.get("Description", ""),
        "Tags": _tag_map(secret.get("Tags")),
    }


def _exists_in(store_getter):
    """An existence check: the record is looked up by the physical id."""
    def read(physical_id, props):
        return {} if physical_id in store_getter() else None
    return read


def _read_events_rule(physical_id, props):
    key = _eb._rule_key(physical_id, props.get("EventBusName", "default"))
    return {} if key in _eb._rules else None


def _read_sns_subscription(physical_id, props):
    return {} if physical_id in _sns._sub_arn_to_topic else None


# type -> (reader, the properties it reads). An empty set is an existence check.
_DRIFT_READERS = {
    "AWS::SQS::Queue": (_read_sqs_queue, frozenset({
        "QueueName", "VisibilityTimeout", "MaximumMessageSize", "MessageRetentionPeriod",
        "DelaySeconds", "ReceiveMessageWaitTimeSeconds", "ContentBasedDeduplication",
        "FifoQueue", "RedrivePolicy", "RedriveAllowPolicy", "Tags"})),
    "AWS::SSM::Parameter": (_read_ssm_parameter, frozenset({
        "Name", "Type", "Value", "Description", "AllowedPattern", "Tier", "DataType",
        "Tags"})),
    "AWS::SNS::Topic": (_read_sns_topic, frozenset({"TopicName", "DisplayName", "Tags"})),
    "AWS::Lambda::Function": (_read_lambda_function, frozenset({
        "FunctionName", "Runtime", "Handler", "Role", "Timeout", "MemorySize",
        "Description", "Architectures", "PackageType", "Environment",
        "EphemeralStorage", "TracingConfig", "Tags"})),
    "AWS::IAM::Role": (_read_iam_role, frozenset({
        "RoleName", "Path", "Description", "MaxSessionDuration",
        "AssumeRolePolicyDocument", "Tags"})),
    "AWS::DynamoDB::Table": (_read_dynamodb_table, frozenset({
        "TableName", "BillingMode", "KeySchema", "AttributeDefinitions",
        "ProvisionedThroughput", "StreamSpecification", "DeletionProtectionEnabled",
        "Tags"})),
    "AWS::S3::Bucket": (_read_s3_bucket, frozenset({"BucketName", "VersioningConfiguration"})),
    "AWS::Logs::LogGroup": (_read_log_group, frozenset({
        "LogGroupName", "RetentionInDays", "LogGroupClass", "Tags"})),
    "AWS::SecretsManager::Secret": (_read_secret, frozenset({"Name", "Description", "Tags"})),
    "AWS::Kinesis::Stream": (_exists_in(lambda: _kinesis._streams), frozenset()),
    "AWS::ECR::Repository": (_exists_in(lambda: _ecr._repositories), frozenset()),
    "AWS::StepFunctions::StateMachine": (_exists_in(lambda: _sfn._state_machines), frozenset()),
    "AWS::Events::Rule": (_read_events_rule, frozenset()),
    "AWS::SNS::Subscription": (_read_sns_subscription, frozenset()),
}


def supports_drift(resource_type: str) -> bool:
    return resource_type in _DRIFT_READERS


# ---------------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------------

def _coerce_like(expected, actual):
    """``actual`` in the JSON types of ``expected``: services keep numbers and
    booleans as strings (SQS attributes), templates write them either way."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        return {k: _coerce_like(expected.get(k), v) for k, v in actual.items()}
    if isinstance(expected, list) and isinstance(actual, list):
        sample = expected[0] if expected else None
        return [_coerce_like(sample, v) for v in actual]
    if isinstance(expected, bool) and isinstance(actual, str):
        if actual.lower() in ("true", "false"):
            return actual.lower() == "true"
    elif isinstance(expected, int) and not isinstance(expected, bool) and isinstance(actual, str):
        try:
            return int(actual)
        except ValueError:
            return actual
    elif isinstance(expected, str) and isinstance(actual, (int, float)) \
            and not isinstance(actual, bool):
        return str(actual)
    elif isinstance(expected, str) and isinstance(actual, bool):
        return str(actual).lower()
    return actual


def _scalar_text(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def _same(expected, actual) -> bool:
    if isinstance(expected, dict) and isinstance(actual, dict):
        return (expected.keys() == actual.keys()
                and all(_same(expected[k], actual[k]) for k in expected))
    if isinstance(expected, list) and isinstance(actual, list):
        return len(expected) == len(actual) and not _list_differences(expected, actual, "")
    if isinstance(expected, (dict, list)) or isinstance(actual, (dict, list)):
        return False
    return _scalar_text(expected) == _scalar_text(actual)


def _list_differences(expected: list, actual: list, path: str) -> list:
    """Arrays compare as sets of elements: an element found on both sides
    matches wherever it sits. Of the rest, elements with the same ``Key`` (tags)
    or at the same position are compared in depth; what is left on the actual
    side was added, what is left on the expected side was removed."""
    diffs = []
    unmatched_actual = list(range(len(actual)))
    leftover_expected = []
    for i, exp in enumerate(expected):
        hit = next((j for j in unmatched_actual if _same(exp, actual[j])), None)
        if hit is None:
            leftover_expected.append(i)
        else:
            unmatched_actual.remove(hit)
    for i in leftover_expected:
        exp = expected[i]
        partner = None
        if isinstance(exp, dict) and "Key" in exp:
            partner = next((j for j in unmatched_actual
                            if isinstance(actual[j], dict)
                            and actual[j].get("Key") == exp["Key"]), None)
        elif i in unmatched_actual:
            partner = i
        if partner is None:
            diffs.append(_difference(f"{path}/{i}", exp, None, "REMOVE"))
            continue
        unmatched_actual.remove(partner)
        diffs.extend(_differences(exp, actual[partner], f"{path}/{i}"))
    for j in unmatched_actual:
        diffs.append(_difference(f"{path}/{j}", None, actual[j], "ADD"))
    return diffs


def _difference(path, expected, actual, kind) -> dict:
    return {
        "PropertyPath": path,
        "ExpectedValue": _scalar_text(expected),
        "ActualValue": _scalar_text(actual),
        "DifferenceType": kind,
    }


def _differences(expected, actual, path: str) -> list:
    """``PropertyDifference`` entries, with the JSON-pointer paths the API
    reference's example uses (``/RedrivePolicy/maxReceiveCount``)."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        diffs = []
        for key, exp in expected.items():
            sub = f"{path}/{key}"
            if key not in actual:
                diffs.append(_difference(sub, exp, None, "REMOVE"))
            else:
                diffs.extend(_differences(exp, actual[key], sub))
        for key in actual.keys() - expected.keys():
            diffs.append(_difference(f"{path}/{key}", None, actual[key], "ADD"))
        return diffs
    if isinstance(expected, list) and isinstance(actual, list):
        return _list_differences(expected, actual, path)
    if _same(expected, actual):
        return []
    return [_difference(path, expected, actual, "NOT_EQUAL")]


def _tags_in_shape(tags: dict, like):
    """A ``{key: value}`` tag map in the shape the template uses: a map, or a
    ``[{Key, Value}]`` list in the order of ``like``, extra keys last."""
    if isinstance(like, dict):
        return dict(tags)
    order = [t.get("Key") for t in (like or []) if isinstance(t, dict)]
    keys = [k for k in order if k in tags] + sorted(k for k in tags if k not in order)
    return [{"Key": k, "Value": tags[k]} for k in keys]


def _expected_properties(resource_type: str, props: dict, supported, stack_tags) -> dict:
    """The properties drift detection checks: the explicitly set ones the
    reader covers. The tag property also carries the stack-level tags
    ("CloudFormation also detects drift on stack-level tags"); the
    ``aws:``-prefixed ones are the service's, not the template's."""
    expected = {k: copy.deepcopy(v) for k, v in (props or {}).items() if k in supported}
    spec = _STACK_TAG_PROPERTY.get(resource_type)
    if spec and spec[0] in supported:
        prop = spec[0]
        own = props.get(prop)
        merged = {**_tag_map(stack_tags), **_tag_map(own)}
        merged = {k: v for k, v in merged.items() if not k.startswith("aws:")}
        if merged:
            like = own if own is not None else ({} if spec[1] == "map" else [])
            expected[prop] = _tags_in_shape(merged, like)
        else:
            expected.pop(prop, None)
    return expected


def detect_resource_drift(stack: dict, logical_id: str, record: dict, timestamp: str) -> dict:
    """The ``StackResourceDrift`` of one resource that has a reader."""
    rtype = record.get("ResourceType", "")
    physical_id = record.get("PhysicalResourceId", "")
    reader, supported = _DRIFT_READERS[rtype]
    drift = {
        "StackId": stack.get("StackId", ""),
        "LogicalResourceId": logical_id,
        "PhysicalResourceId": physical_id,
        "ResourceType": rtype,
        "Timestamp": timestamp,
    }
    props = record.get("Properties") or {}
    try:
        current = reader(physical_id, props)
    except Exception as exc:  # the resource could not be read: UNKNOWN, as documented
        logger.warning("Drift detection of %s (%s) failed: %s", logical_id, rtype, exc)
        drift["StackResourceDriftStatus"] = "UNKNOWN"
        drift["DriftStatusReason"] = f"Drift detection failed: {exc}"
        return drift
    if current is None:
        drift["StackResourceDriftStatus"] = "DELETED"
        return drift
    expected = _expected_properties(rtype, props, supported, stack.get("Tags") or [])
    actual = {}
    for key, exp in expected.items():
        if key not in current:
            continue
        value = current[key]
        spec = _STACK_TAG_PROPERTY.get(rtype)
        if spec and key == spec[0]:
            tags = {k: v for k, v in _tag_map(value).items() if not k.startswith("aws:")}
            value = _tags_in_shape(tags, exp)
        actual[key] = _coerce_like(exp, copy.deepcopy(value))
    differences = _differences(expected, actual, "")
    drift["ExpectedProperties"] = json.dumps(expected, sort_keys=True, default=str)
    drift["ActualProperties"] = json.dumps(actual, sort_keys=True, default=str)
    if differences:
        drift["StackResourceDriftStatus"] = "MODIFIED"
        drift["PropertyDifferences"] = differences
    else:
        drift["StackResourceDriftStatus"] = "IN_SYNC"
    return drift


def record_resource_drift(record: dict, drift: dict) -> None:
    """Keep the result on the resource record: ``DescribeStackResourceDrifts``
    lists it and the resource's ``DriftInformation`` reports it."""
    record["_drift"] = drift
    record["DriftInformation"] = {
        "StackResourceDriftStatus": drift["StackResourceDriftStatus"],
        "LastCheckTimestamp": drift["Timestamp"],
    }
