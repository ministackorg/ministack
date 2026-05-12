"""
AWS Cost and Usage Report (CUR) service emulator.

JSON 1.1 protocol with X-Amz-Target prefix ``AWSOrigamiServiceGateway``.

Implemented:
  DeleteReportDefinition, DescribeReportDefinitions, ListTagsForResource,
  ModifyReportDefinition, PutReportDefinition, TagResource, UntagResource.

Deferred:
  None.
"""

import copy
import json
import logging

from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
)

logger = logging.getLogger("cur")

_report_definitions = AccountScopedDict()  # report_name -> report_definition
_report_tags = AccountScopedDict()  # report_name -> {tag_key: tag_value}


def reset():
    _report_definitions.clear()
    _report_tags.clear()


def get_state():
    return {
        "report_definitions": copy.deepcopy(_report_definitions),
        "report_tags": copy.deepcopy(_report_tags),
    }


def restore_state(data):
    if not data:
        return
    _report_definitions.clear()
    _report_definitions.update(data.get("report_definitions") or {})
    _report_tags.clear()
    _report_tags.update(data.get("report_tags") or {})


def load_persisted_state(data):
    restore_state(data)


try:
    _restored = load_state("cur")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore persisted CUR state; continuing with fresh store")


def _json(status: int, body: dict):
    return status, {"Content-Type": "application/x-amz-json-1.1"}, json.dumps(body).encode()


def _delete_report_definition(payload: dict):
    report_name = payload.get("ReportName")

    if not isinstance(report_name, str) or not report_name.strip():
        return error_response_json("ValidationException", "ReportName is required", 400)

    if report_name not in _report_definitions:
        return error_response_json(
            "ValidationException",
            f"Report definition not found: {report_name}",
            400,
        )

    _report_tags.pop(report_name, None)
    del _report_definitions[report_name]
    return _json(200, {})


def _describe_report_definitions(_payload: dict):
    return _json(
        200,
        {
            "ReportDefinitions": list(_report_definitions.values()),
        },
    )


def _list_tags_for_resource(payload: dict):
    report_name = payload.get("ReportName")
    if not isinstance(report_name, str) or not report_name.strip():
        return error_response_json("ValidationException", "ReportName is required", 400)

    if report_name not in _report_definitions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Report definition not found: {report_name}",
            400,
        )

    tag_dict = _report_tags.get(report_name, {})
    tags = [{"Key": k, "Value": v} for k, v in tag_dict.items()]
    return _json(200, {"Tags": tags})


def _modify_report_definition(payload: dict):
    report_name = payload.get("ReportName")
    report = payload.get("ReportDefinition")

    if not isinstance(report_name, str) or not report_name.strip():
        return error_response_json("ValidationException", "ReportName is required", 400)

    if not isinstance(report, dict):
        return error_response_json("ValidationException", "ReportDefinition is required", 400)

    if report_name not in _report_definitions:
        return error_response_json(
            "ValidationException",
            f"Report definition not found: {report_name}",
            400,
        )

    # AWS uses the request ReportName as the identity of the report to update.
    # Keep that stable even when a skeleton payload omits or changes
    # ReportDefinition.ReportName.
    updated = copy.deepcopy(report)
    updated["ReportName"] = report_name

    _report_definitions[report_name] = updated
    return _json(200, {})


def _put_report_definition(payload: dict):
    report = payload.get("ReportDefinition")
    if not isinstance(report, dict):
        return error_response_json(
            "ValidationException",
            "ReportDefinition is required",
            400,
        )

    report_name = report.get("ReportName")
    if not isinstance(report_name, str) or not report_name.strip():
        return error_response_json(
            "ValidationException",
            "ReportDefinition.ReportName is required",
            400,
        )

    if report_name in _report_definitions:
        return error_response_json(
            "DuplicateReportNameException",
            f"Report definition already exists: {report_name}",
            400,
        )

    _report_definitions[report_name] = copy.deepcopy(report)
    _report_tags.setdefault(report_name, {})
    return _json(200, {})


def _tag_resource(payload: dict):
    report_name = payload.get("ReportName")
    if not isinstance(report_name, str) or not report_name.strip():
        return error_response_json("ValidationException", "ReportName is required", 400)

    if report_name not in _report_definitions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Report definition not found: {report_name}",
            400,
        )

    tags = payload.get("Tags")
    if not isinstance(tags, list):
        return error_response_json("ValidationException", "Tags is required", 400)

    # Validate each tag has Key and Value.
    for tag in tags:
        if not isinstance(tag, dict):
            return error_response_json("ValidationException", "Tag must be a dict", 400)
        if "Key" not in tag or "Value" not in tag:
            return error_response_json(
                "ValidationException",
                "Each tag must have Key and Value",
                400,
            )

    # Merge tags into the report's tag dict.
    tag_dict = _report_tags.setdefault(report_name, {})
    for tag in tags:
        tag_dict[tag["Key"]] = tag["Value"]

    return _json(200, {})


def _untag_resource(payload: dict):
    report_name = payload.get("ReportName")
    if not isinstance(report_name, str) or not report_name.strip():
        return error_response_json("ValidationException", "ReportName is required", 400)

    if report_name not in _report_definitions:
        return error_response_json(
            "ResourceNotFoundException",
            f"Report definition not found: {report_name}",
            400,
        )

    tag_keys = payload.get("TagKeys")
    if not isinstance(tag_keys, list):
        return error_response_json("ValidationException", "TagKeys is required", 400)

    # Remove tags by key.
    tag_dict = _report_tags.get(report_name, {})
    for key in tag_keys:
        tag_dict.pop(key, None)

    return _json(200, {})


_DISPATCH = {
    "DeleteReportDefinition": _delete_report_definition,
    "DescribeReportDefinitions": _describe_report_definitions,
    "ListTagsForResource": _list_tags_for_resource,
    "ModifyReportDefinition": _modify_report_definition,
    "PutReportDefinition": _put_report_definition,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("X-Amz-Target") or headers.get("x-amz-target") or ""
    action = target.split(".", 1)[1] if "." in target else target
    if not action:
        return error_response_json("InvalidAction", "missing X-Amz-Target", 400)

    body_text = body.decode("utf-8") if isinstance(body, bytes) else (body or "")
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "invalid JSON body", 400)

    fn = _DISPATCH.get(action)
    if fn is None:
        return error_response_json(
            "InvalidAction",
            f"Operation '{action}' not implemented",
            400,
        )

    return fn(payload)
