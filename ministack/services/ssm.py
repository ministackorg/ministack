# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
SSM Parameter Store and Run Command Emulator.
JSON-based API via X-Amz-Target (AmazonSSM).
Supports: PutParameter, GetParameter, GetParameters, GetParametersByPath,
          DeleteParameter, DeleteParameters, DescribeParameters,
          GetParameterHistory, LabelParameterVersion,
          AddTagsToResource, RemoveTagsFromResource, ListTagsForResource,
          SendCommand, GetCommandInvocation, ListCommands,
          DescribeInstanceInformation,
          CreateDocument, DeleteDocument, DescribeDocument, GetDocument,
          ListDocuments,
          StartAutomationExecution, GetAutomationExecution,
          DescribeAutomationExecutions, StopAutomationExecution.

Automation executions run the document's steps on a background thread:
aws:sleep waits its real duration and aws:executeAwsApi/aws:runCommand
dispatch into the emulated service; a step whose action MiniStack cannot
execute fails the execution rather than reporting a fabricated Success.
"""

import base64
import copy
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import yaml

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import spawn_background
from ministack.core.iam_evaluator import request_caller_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("ssm")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
DEFAULT_PAGE_SIZE = 10


_parameters = AccountRegionScopedDict()
_parameter_history = AccountRegionScopedDict()
_tags = AccountRegionScopedDict()
_commands = AccountRegionScopedDict()
_service_settings = AccountRegionScopedDict()
_documents = AccountRegionScopedDict()
_automation_executions = AccountRegionScopedDict()

# The botocore model's SettingIds with the (value, LastModifiedDate) AWS reports before any update.
_SERVICE_SETTING_DEFAULTS = {
    "/ssm/parameter-store/high-throughput-enabled": ("false", 1555532571.138),
    "/ssm/parameter-store/default-parameter-tier": ("Standard", 1565131339.38),
    "/ssm/documents/console/public-sharing-permission": ("Enable", 1612382724.747),
    "/ssm/managed-instance/activation-tier": ("standard", 1579134984.656),
    "/ssm/managed-instance/default-ec2-instance-management-role": ("$None", 1636561263.576),
    "/ssm/automation/customer-script-log-destination": ("None", 1572919724.731),
    "/ssm/automation/customer-script-log-group-name": ("/aws/ssm/automation/executeScript", 1572898415.256),
    "/ssm/automation/enable-adaptive-concurrency": ("False", 1632330231.776),
    "/ssm/appmanager/appmanager-enabled": ("False", 1604983265.572),
    "/ssm/opsinsights/opscenter": ("Disabled", 1622077352.094),
}
# SettingValues the UpdateServiceSetting API reference lists; an id not here takes any string.
_SERVICE_SETTING_VALUES = {
    "/ssm/parameter-store/high-throughput-enabled": ("true", "false"),
    "/ssm/parameter-store/default-parameter-tier": ("Standard", "Advanced", "Intelligent-Tiering"),
    "/ssm/documents/console/public-sharing-permission": ("Enable", "Disable"),
    "/ssm/managed-instance/activation-tier": ("standard", "advanced"),
    "/ssm/appmanager/appmanager-enabled": ("True", "False"),
    "/ssm/opsinsights/opscenter": ("Enabled", "Disabled"),
}
_DEFAULT_TIER_SETTING = "/ssm/parameter-store/default-parameter-tier"


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "parameters": copy.deepcopy(_parameters),
        "parameter_history": copy.deepcopy(_parameter_history),
        "tags": copy.deepcopy(_tags),
        "commands": copy.deepcopy(_commands),
        "service_settings": copy.deepcopy(_service_settings),
        "documents": copy.deepcopy(_documents),
        "automation_executions": copy.deepcopy(_automation_executions),
    }


def _legacy_region_for_parameter_history(account_id: str, name: str) -> str:
    exact_regions = {
        region
        for (stored_account_id, region, stored_name), _param in _parameters.all_items()
        if stored_account_id == account_id and stored_name == name
    }
    if len(exact_regions) == 1:
        return next(iter(exact_regions))

    candidates = set(_parameter_name_candidates(name))
    regions = {
        region
        for (stored_account_id, region, stored_name), _param in _parameters.all_items()
        if stored_account_id == account_id and stored_name in candidates
    }
    if len(regions) == 1:
        return next(iter(regions))
    return get_region()


def _restore_parameter_history(data) -> None:
    if isinstance(data, AccountRegionScopedDict):
        _parameter_history.update(data)
        return
    if isinstance(data, AccountScopedDict):
        for (account_id, name), history in data._data.items():
            region = _legacy_region_for_parameter_history(account_id, name)
            _parameter_history.set_scoped(account_id, region, name, history)
        return
    if isinstance(data, dict):
        for key, history in data.items():
            if isinstance(key, tuple) and len(key) == 3:
                account_id, region, name = key
            elif isinstance(key, tuple) and len(key) == 2:
                account_id, name = key
                region = _legacy_region_for_parameter_history(account_id, name)
            else:
                account_id = get_account_id()
                name = key
                region = _legacy_region_for_parameter_history(account_id, name)
            _parameter_history.set_scoped(account_id, region, name, history)


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if data:
        _parameters.update(data.get("parameters", {}))
        _restore_parameter_history(data.get("parameter_history", {}))
        _tags.update(data.get("tags", {}))
        _commands.update(data.get("commands", {}))
        _service_settings.update(data.get("service_settings", {}))
        _documents.update(data.get("documents", {}))
        _automation_executions.update(data.get("automation_executions", {}))


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _now_epoch() -> float:
    return datetime.now(timezone.utc).timestamp()


def _parameter_resource(name: str) -> str:
    return f"parameter/{name.lstrip('/')}"


def _param_arn(name: str) -> str:
    return f"arn:aws:ssm:{get_region()}:{get_account_id()}:{_parameter_resource(name)}"


def _parameter_name_candidates(name: str) -> list[str]:
    if not name:
        return []
    candidates = [name]
    alternate = name.lstrip("/") if name.startswith("/") else f"/{name}"
    if alternate and alternate not in candidates:
        candidates.append(alternate)
    return candidates


def _parameter_name_from_arn(value: str) -> tuple[str, str, str, bool] | None:
    try:
        spec = parse_arn(value)
    except ArnParseError:
        return None
    if spec.partition != "aws" or spec.service != "ssm" or not spec.region:
        return None
    if spec.resource.startswith("parameter/"):
        name = spec.resource[len("parameter/"):]
        legacy_no_slash = False
    elif spec.resource.startswith("parameter"):
        name = spec.resource[len("parameter"):]
        legacy_no_slash = True
    else:
        return None
    if not name:
        return None
    return spec.account_id, spec.region or get_region(), name, legacy_no_slash


def _lookup_parameter(name_or_arn: str, *, allow_arn_region: bool = False, flexible_name: bool = False):
    if not name_or_arn:
        return None, None
    if name_or_arn.startswith("arn:"):
        parsed = _parameter_name_from_arn(name_or_arn)
        if not parsed:
            return None, None
        account_id, region, arn_name, legacy_no_slash = parsed
        if account_id != get_account_id() or (not allow_arn_region and region != get_region()):
            return None, None
        matches = []
        candidates = [arn_name] if legacy_no_slash else _parameter_name_candidates(arn_name)
        for candidate in candidates:
            param = _parameters.get_scoped(account_id, region, candidate)
            if param:
                matches.append((candidate, param))
        for candidate, param in matches:
            if param.get("ARN") == name_or_arn:
                return candidate, param
        if legacy_no_slash:
            return None, None
        if matches:
            return matches[0]
        return None, None

    candidates = _parameter_name_candidates(name_or_arn) if flexible_name else [name_or_arn]
    for candidate in candidates:
        param = _parameters.get(candidate)
        if param:
            return candidate, param
    return None, None


def _parameter_tag_arn(resource_type: str, resource_id: str) -> str | None:
    if resource_type != "Parameter":
        return resource_id
    if resource_id.startswith("arn:"):
        _, param = _lookup_parameter(resource_id)
        if not param:
            return None
        if resource_id in _tags:
            return resource_id
        return param["ARN"]
    _, param = _lookup_parameter(resource_id, flexible_name=True)
    if param:
        normalized_id = resource_id if resource_id.startswith("/") else f"/{resource_id}"
        slash_normalized_arn = _param_arn(normalized_id)
        if slash_normalized_arn in _tags:
            return slash_normalized_arn
        return param["ARN"]
    if not resource_id.startswith("/"):
        resource_id = "/" + resource_id
    return _param_arn(resource_id)




def _encode_next_token(index: int) -> str:
    return base64.b64encode(str(index).encode()).decode()


def _decode_next_token(token: str) -> int:
    try:
        return int(base64.b64decode(token).decode())
    except Exception:
        return 0


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "PutParameter": _put_parameter,
        "GetParameter": _get_parameter,
        "GetParameters": _get_parameters,
        "GetParametersByPath": _get_parameters_by_path,
        "DeleteParameter": _delete_parameter,
        "DeleteParameters": _delete_parameters,
        "DescribeParameters": _describe_parameters,
        "GetParameterHistory": _get_parameter_history,
        "LabelParameterVersion": _label_parameter_version,
        "AddTagsToResource": _add_tags_to_resource,
        "RemoveTagsFromResource": _remove_tags_from_resource,
        "ListTagsForResource": _list_tags_for_resource,
        "SendCommand": _send_command,
        "GetCommandInvocation": _get_command_invocation,
        "ListCommands": _list_commands,
        "DescribeInstanceInformation": _describe_instance_information,
        "GetServiceSetting": _get_service_setting,
        "UpdateServiceSetting": lambda d: _update_service_setting(d, request_caller_arn(headers, query_params)),
        "ResetServiceSetting": _reset_service_setting,
        "CreateDocument": _create_document,
        "DeleteDocument": _delete_document,
        "DescribeDocument": _describe_document,
        "GetDocument": _get_document,
        "ListDocuments": _list_documents,
        "StartAutomationExecution": lambda d: _start_automation_execution(d, request_caller_arn(headers, query_params)),
        "GetAutomationExecution": _get_automation_execution,
        "DescribeAutomationExecutions": _describe_automation_executions,
        "StopAutomationExecution": _stop_automation_execution,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


_RESERVED_PREFIX_MSG = 'Parameter name: can\'t be prefixed with "aws" or "ssm" (case-insensitive).'
_RESERVED_SSM_PATH_MSG = (
    'Parameter name: can\'t be prefixed with "ssm" (case-insensitive). If formed as a path, it can consist '
    "of sub-paths divided by slash symbol; each sub-path can be formed as a mix of letters, numbers and "
    "the following 3 symbols .-_"
)
_TAGS_WITH_OVERWRITE_MSG = (
    "Invalid request: tags and overwrite can't be used together. To create a parameter "
    "with tags, please remove overwrite flag. To update tags for an existing parameter, "
    "please use AddTagsToResource or RemoveTagsFromResource."
)


def _reserved_name_error(name: str):
    """PutParameter refuses names whose first characters are ``aws`` or ``ssm``."""
    is_path = name.startswith("/")
    bare = name[1:] if is_path else name
    lowered = bare.lower()
    if is_path and lowered.startswith("aws"):
        return error_response_json("AccessDeniedException", f"No access to reserved parameter name: {bare}.", 400)
    if lowered.startswith(("aws", "ssm")):
        msg = _RESERVED_SSM_PATH_MSG if is_path else _RESERVED_PREFIX_MSG
        return error_response_json("ValidationException", msg, 400)
    return None


def _put_parameter(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)

    reserved = _reserved_name_error(name)
    if reserved:
        return reserved

    overwrite = data.get("Overwrite", False)
    if overwrite and data.get("Tags"):
        return error_response_json("ValidationException", _TAGS_WITH_OVERWRITE_MSG, 400)

    value = data.get("Value", "")

    existing = _parameters.get(name)
    alternate_existing = [
        candidate
        for candidate in _parameter_name_candidates(name)
        if candidate != name and candidate in _parameters
    ]
    if alternate_existing and not existing:
        return error_response_json(
            "ParameterAlreadyExists",
            "The parameter already exists. To overwrite this value, set the overwrite option in the request to true.",
            400,
        )
    if existing and not overwrite:
        return error_response_json(
            "ParameterAlreadyExists",
            "The parameter already exists. To overwrite this value, set the overwrite option in the request to true.",
            400,
        )

    param_type = data.get("Type")
    if not param_type:
        if not existing:
            return error_response_json(
                "ValidationException", "A parameter type is required when you create a parameter.", 400
            )
        param_type = existing["Type"]

    version = (existing["Version"] + 1) if existing else 1
    arn = existing.get("ARN") if existing else _param_arn(name)
    now = _now_epoch()

    stored_value = value
    if param_type == "SecureString":
        # "If you don't specify a key ID, the system uses the default key
        # associated with your Amazon Web Services account" — an AWS managed
        # key, whose alias has the form aws/<service-name> and which the API
        # names with the alias/ prefix (KMS concepts: alias/aws/s3 is the S3
        # one). An empty KeyId on the request is the same as none.
        key_id = data.get("KeyId") or "alias/aws/ssm"
        stored_value = f"ENCRYPTED:{base64.b64encode(value.encode()).decode()}"
    else:
        key_id = ""

    record = {
        "Name": name,
        "Value": stored_value,
        "OriginalValue": value,
        "Type": param_type,
        "KeyId": key_id,
        "Version": version,
        "ARN": arn,
        "LastModifiedDate": now,
        "DataType": data.get("DataType", "text"),
        "Description": data.get("Description", existing.get("Description", "") if existing else ""),
        "Tier": data.get("Tier") or _default_tier(data, value, existing),
        "AllowedPattern": data.get("AllowedPattern", ""),
        "Policies": data.get("Policies", []),
        "Labels": [],
    }

    _parameters[name] = record

    history_entry = {
        "Name": name,
        "Value": stored_value,
        "OriginalValue": value,
        "Type": param_type,
        "KeyId": key_id,
        "Version": version,
        "LastModifiedDate": now,
        "LastModifiedUser": f"arn:aws:iam::{get_account_id()}:root",
        "Description": record["Description"],
        "AllowedPattern": record["AllowedPattern"],
        "Tier": record["Tier"],
        "Policies": record["Policies"],
        "DataType": record["DataType"],
        "Labels": [],
    }

    if name not in _parameter_history:
        _parameter_history[name] = []
    _parameter_history[name].append(history_entry)

    if data.get("Tags"):
        _tags[arn] = {t["Key"]: t["Value"] for t in data["Tags"]}

    logger.info("SSM PutParameter: %s v%s type=%s", name, version, param_type)
    return json_response({"Version": version, "Tier": record["Tier"]})


def resolve_parameter_value(name_or_arn, version=None, decrypt=False):
    """Return an SSM parameter's value by name or ARN, or None.

    Used by other services (e.g. ECS `secrets[].valueFrom`, CloudFormation
    dynamic references) that need to read a parameter value in-process without
    going through the HTTP API. Accepts a bare name (``/path/name`` or
    ``name``) or a full ARN (``arn:aws:ssm:region:acct:parameter/path/name``).
    ``version`` selects an earlier version from the parameter's history;
    ``decrypt`` returns a SecureString's plaintext, as GetParameter with
    WithDecryption does.
    """
    if not name_or_arn:
        return None
    name, param = _lookup_parameter(name_or_arn, allow_arn_region=True, flexible_name=True)
    if not param and version is None:
        param = _public_parameter(name_or_arn)
    if not param:
        return None
    if version is not None and str(version) != str(param.get("Version")):
        history = _parameter_history.get(name) or []
        param = next((h for h in history if str(h.get("Version")) == str(version)), None)
        if not param:
            return None
    if decrypt:
        return param.get("OriginalValue", param.get("Value"))
    return param.get("Value")


def _split_selector(name_or_arn):
    """Split a trailing ":version" / ":label"; parameter names cannot contain ":"."""
    if not name_or_arn:
        return name_or_arn, None
    base_colons = 5 if name_or_arn.startswith("arn:") else 0
    if name_or_arn.count(":") <= base_colons:
        return name_or_arn, None
    base, selector = name_or_arn.rsplit(":", 1)
    return base, selector


# Public latest-AMI parameters every account can read, pointed at the stub images in ec2.py.
_PUBLIC_AMI_PARAMETERS = {
    "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64": "ami-0c2023a1b2c3d4e5f",
    "/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2": "ami-0abcdef1234567890",
}


def _public_parameter(name):
    """A public parameter as AWS serves it, with no account in the ARN."""
    image_id = _PUBLIC_AMI_PARAMETERS.get(name)
    if image_id is None:
        return None
    return {"Name": name, "Type": "String", "Value": image_id, "Version": 1, "LastModifiedDate": 1728518400,
            "ARN": f"arn:aws:ssm:{get_region()}::parameter{name}"}


def _select_parameter(name_or_arn):
    """(param, error) for GetParameter / GetParameters, honouring name:version and name:label."""
    base, selector = _split_selector(name_or_arn)
    name, param = _lookup_parameter(base)
    if not param and selector is None:
        param = _public_parameter(base)
    if not param:
        return None, "ParameterNotFound"
    if selector is None:
        return param, None
    history = _parameter_history.get(name) or []
    if selector.isdigit():
        entry = next((h for h in history if str(h.get("Version")) == selector), None)
    else:
        entry = next((h for h in history if selector in (h.get("Labels") or [])), None)
    if entry is None:
        return None, "ParameterVersionNotFound"
    return {**entry, "Name": param["Name"], "ARN": param["ARN"], "Selector": f":{selector}"}, None


def _get_parameter(data):
    name = data.get("Name")
    param, error = _select_parameter(name)
    if error == "ParameterVersionNotFound":
        return error_response_json(
            "ParameterVersionNotFound",
            f"Systems Manager could not find version {_split_selector(name)[1]} of {_split_selector(name)[0]}.",
            400,
        )
    if not param:
        return error_response_json("ParameterNotFound", f"Parameter {name} not found", 400)
    with_decryption = data.get("WithDecryption", False)
    return json_response({"Parameter": _param_out(param, with_decryption)})


def _get_parameters(data):
    names = data.get("Names", [])
    with_decryption = data.get("WithDecryption", False)
    params = []
    invalid = []
    for name in names:
        p, _ = _select_parameter(name)
        if p:
            params.append(_param_out(p, with_decryption))
        else:
            invalid.append(name)
    return json_response({"Parameters": params, "InvalidParameters": invalid})


def _get_parameters_by_path(data):
    path = data.get("Path", "/")
    recursive = data.get("Recursive", False)
    with_decryption = data.get("WithDecryption", False)
    max_results = data.get("MaxResults", DEFAULT_PAGE_SIZE)
    next_token = data.get("NextToken")

    if not path.endswith("/"):
        path_prefix = path + "/"
    else:
        path_prefix = path

    all_results = []
    for name, param in sorted(_parameters.items()):
        if name == path:
            continue
        if not name.startswith(path_prefix) and not (name.startswith(path) and path == "/"):
            continue
        if recursive:
            matches = True
        else:
            suffix = name[len(path_prefix):]
            matches = "/" not in suffix
        if matches:
            all_results.append(param)

    start = 0
    if next_token:
        start = _decode_next_token(next_token)

    page = all_results[start:start + max_results]
    out = [_param_out(p, with_decryption) for p in page]

    resp = {"Parameters": out}
    if start + max_results < len(all_results):
        resp["NextToken"] = _encode_next_token(start + max_results)
    return json_response(resp)


def _delete_parameter(data):
    name = data.get("Name")
    if isinstance(name, str) and name.startswith("arn:"):
        return error_response_json("ValidationException", "Parameter name must not be an ARN", 400)
    key, param = _lookup_parameter(name)
    if not param:
        return error_response_json("ParameterNotFound", f"Parameter {name} not found", 400)
    tag_arn = _parameter_tag_arn("Parameter", key)
    del _parameters[key]
    _parameter_history.pop(key, None)
    for arn in {param["ARN"], tag_arn}:
        if arn:
            _tags.pop(arn, None)
    return json_response({})


def _delete_parameters(data):
    names = data.get("Names", [])
    deleted = []
    invalid = []
    for name in names:
        if isinstance(name, str) and name.startswith("arn:"):
            invalid.append(name)
            continue
        key, param = _lookup_parameter(name)
        if param:
            tag_arn = _parameter_tag_arn("Parameter", key)
            del _parameters[key]
            _parameter_history.pop(key, None)
            for arn in {param["ARN"], tag_arn}:
                if arn:
                    _tags.pop(arn, None)
            deleted.append(name)
        else:
            invalid.append(name)
    return json_response({"DeletedParameters": deleted, "InvalidParameters": invalid})


def _describe_parameters(data):
    filters = data.get("ParameterFilters", [])
    string_filters = data.get("Filters", [])
    max_results = data.get("MaxResults", DEFAULT_PAGE_SIZE)
    next_token = data.get("NextToken")

    candidates = list(_parameters.values())

    for f in filters:
        key = f.get("Key", "")
        option = f.get("Option", "Equals")
        values = f.get("Values", [])
        candidates = [p for p in candidates if _apply_filter(p, key, option, values)]

    for f in string_filters:
        key = f.get("Key", "")
        values = f.get("Values", [])
        if key == "Name" and values:
            candidates = [p for p in candidates if p["Name"] in values]
        elif key == "Type" and values:
            candidates = [p for p in candidates if p["Type"] in values]

    candidates.sort(key=lambda p: p["Name"])

    start = 0
    if next_token:
        start = _decode_next_token(next_token)

    page = candidates[start:start + max_results]
    results = []
    for param in page:
        desc = {
            "Name": param["Name"],
            "Type": param["Type"],
            "Version": param["Version"],
            "LastModifiedDate": param["LastModifiedDate"],
            "LastModifiedUser": f"arn:aws:iam::{get_account_id()}:root",
            "ARN": param["ARN"],
            "DataType": param["DataType"],
            "Description": param.get("Description", ""),
            "Tier": param.get("Tier", "Standard"),
            "AllowedPattern": param.get("AllowedPattern", ""),
        }
        # ParameterMetadata.KeyId is "the alias of the KMS key used to encrypt
        # the parameter. Applies to SecureString parameters only", so it is
        # reported for those and omitted for the rest. A caller that reads it
        # back (Terraform's aws_ssm_parameter) plans the same in-place update
        # on every run while the member is missing.
        if param.get("KeyId"):
            desc["KeyId"] = param["KeyId"]
        if param.get("Policies"):
            desc["Policies"] = param["Policies"]
        results.append(desc)

    resp = {"Parameters": results}
    if start + max_results < len(candidates):
        resp["NextToken"] = _encode_next_token(start + max_results)
    return json_response(resp)


def _apply_filter(param, key, option, values):
    if not values:
        return True

    if key == "Name":
        target = param["Name"]
        if option == "Equals":
            return target in values
        elif option == "Contains":
            return any(v in target for v in values)
        elif option == "BeginsWith":
            return any(target.startswith(v) for v in values)
    elif key == "Type":
        return param["Type"] in values
    elif key == "KeyId":
        return param.get("KeyId", "") in values
    elif key == "Path":
        name = param["Name"]
        for v in values:
            prefix = v if v.endswith("/") else v + "/"
            if name.startswith(prefix):
                return True
        return False
    elif key == "DataType":
        return param.get("DataType", "text") in values
    elif key == "Tier":
        return param.get("Tier", "Standard") in values
    elif key == "Label":
        labels = param.get("Labels", [])
        return any(v in labels for v in values)

    return True


def _get_parameter_history(data):
    name = data.get("Name")
    if name not in _parameter_history:
        return error_response_json("ParameterNotFound", f"Parameter {name} not found", 400)

    with_decryption = data.get("WithDecryption", False)
    max_results = data.get("MaxResults", 50)
    next_token = data.get("NextToken")

    history = _parameter_history[name]

    start = 0
    if next_token:
        start = _decode_next_token(next_token)

    page = history[start:start + max_results]
    results = []
    for entry in page:
        out = {
            "Name": entry["Name"],
            "Type": entry["Type"],
            "Version": entry["Version"],
            "LastModifiedDate": entry["LastModifiedDate"],
            "LastModifiedUser": entry.get("LastModifiedUser", f"arn:aws:iam::{get_account_id()}:root"),
            "Description": entry.get("Description", ""),
            "DataType": entry.get("DataType", "text"),
            "Tier": entry.get("Tier", "Standard"),
            "Labels": entry.get("Labels", []),
            "Policies": entry.get("Policies", []),
        }
        # ParameterHistory carries the same member as ParameterMetadata.
        if entry.get("KeyId"):
            out["KeyId"] = entry["KeyId"]
        if with_decryption or entry["Type"] != "SecureString":
            out["Value"] = entry.get("OriginalValue", entry["Value"])
        else:
            out["Value"] = entry["Value"]
        results.append(out)

    resp = {"Parameters": results}
    if start + max_results < len(history):
        resp["NextToken"] = _encode_next_token(start + max_results)
    return json_response(resp)


def _label_parameter_version(data):
    name = data.get("Name")
    version = data.get("ParameterVersion")
    labels = data.get("Labels", [])

    if name not in _parameter_history:
        return error_response_json("ParameterNotFound", f"Parameter {name} not found", 400)

    history = _parameter_history[name]
    if version is None:
        version = _parameters[name]["Version"]

    target = None
    for entry in history:
        if entry["Version"] == version:
            target = entry
            break

    if target is None:
        return error_response_json(
            "ParameterVersionNotFound",
            f"Version {version} of parameter {name} not found",
            400,
        )

    invalid_labels = []
    for label in labels:
        if len(label) > 100 or label.startswith("aws:") or label.startswith("ssm:"):
            invalid_labels.append(label)
            continue
        for entry in history:
            if label in entry.get("Labels", []) and entry["Version"] != version:
                entry["Labels"].remove(label)
        if label not in target.get("Labels", []):
            target.setdefault("Labels", []).append(label)

    if version == _parameters[name]["Version"]:
        _parameters[name]["Labels"] = target.get("Labels", [])

    return json_response({"InvalidLabels": invalid_labels, "ParameterVersion": version})


def _add_tags_to_resource(data):
    resource_type = data.get("ResourceType", "Parameter")
    resource_id = data.get("ResourceId", "")
    new_tags = data.get("Tags", [])

    arn = _parameter_tag_arn(resource_type, resource_id)
    if arn is None:
        return error_response_json("ParameterNotFound", f"Parameter {resource_id} not found", 400)

    if arn not in _tags:
        _tags[arn] = {}
    for tag in new_tags:
        _tags[arn][tag["Key"]] = tag["Value"]

    return json_response({})


def _remove_tags_from_resource(data):
    resource_type = data.get("ResourceType", "Parameter")
    resource_id = data.get("ResourceId", "")
    tag_keys = data.get("TagKeys", [])

    arn = _parameter_tag_arn(resource_type, resource_id)
    if arn is None:
        return error_response_json("ParameterNotFound", f"Parameter {resource_id} not found", 400)

    if arn in _tags:
        for key in tag_keys:
            _tags[arn].pop(key, None)

    return json_response({})


def _list_tags_for_resource(data):
    resource_type = data.get("ResourceType", "Parameter")
    resource_id = data.get("ResourceId", "")

    arn = _parameter_tag_arn(resource_type, resource_id)
    if arn is None:
        return error_response_json("ParameterNotFound", f"Parameter {resource_id} not found", 400)

    tag_dict = _tags.get(arn, {})
    tag_list = [{"Key": k, "Value": v} for k, v in tag_dict.items()]

    return json_response({"TagList": tag_list})


def _param_out(param, with_decryption=False):
    if with_decryption or param["Type"] != "SecureString":
        value = param.get("OriginalValue", param["Value"])
    else:
        value = param["Value"]

    out = {
        "Name": param["Name"],
        "Type": param["Type"],
        "Value": value,
        "Version": param["Version"],
        "ARN": param["ARN"],
        "LastModifiedDate": param["LastModifiedDate"],
        "DataType": param.get("DataType", "text"),
    }
    if param.get("Selector"):
        out["Selector"] = param["Selector"]
    return out


# ── Run Command ────────────────────────────────────────────
#
# An invocation is asynchronous, as on AWS: SendCommand answers Pending and the caller polls
# GetCommandInvocation until it reaches a terminal state. For AWS-RunShellScript against an
# instance with a box behind it, the script runs in that box and the invocation carries its real
# exit code and output, so a health probe can genuinely fail. A document we cannot run completes
# as Success with empty output rather than never converging.

_RUN_COMMAND_PLUGIN = "aws:runShellScript"
# GetCommandInvocation: "The first 24,000 characters written by the plugin to stdout" and
# "The first 8,000 characters written by the plugin to stderr".
_STDOUT_LIMIT = 24000
_STDERR_LIMIT = 8000
_SHELL_DOCUMENTS = {"AWS-RunShellScript"}


def _managed_instance_ids() -> set:
    """Instances Run Command can reach.

    An agent has to be answering, which AWS folds into InvalidInstanceId along with the node's
    state ("SSM Agent isn't running" is one of the causes it lists). A metadata-only instance has
    nothing running on it, so real SSM would refuse it too. `instance_is_managed` covers both:
    it requires the instance to be running and to have a box with a shell behind it.
    """
    from ministack.services import ec2 as ec2_svc

    return {
        instance_id
        for instance_id in ec2_svc._instances
        if ec2_svc.instance_is_managed(instance_id)
    }


def _shell_argv(data) -> list:
    """The argv for an AWS-RunShellScript invocation, from its `commands` parameter."""
    commands = (data.get("Parameters") or {}).get("commands") or []
    if isinstance(commands, str):
        commands = [commands]
    script = "\n".join(str(c) for c in commands)
    return ["/bin/sh", "-c", script]


def _finish_invocation(command_id, instance_id, argv):
    """Run one invocation to completion and publish its result.

    On its own thread: the script is the caller's, and a readiness probe calling back into
    MiniStack is the point of the feature, so it must not take a slot from the shared pool.
    """
    from ministack.services import ec2 as ec2_svc

    record = _commands.get(command_id)
    if not record:
        return
    invocation = (record.get("Invocations") or {}).get(instance_id)
    if invocation is None:
        return
    invocation["Status"] = "InProgress"
    invocation["StatusDetails"] = "In Progress"
    invocation["ExecutionStartDateTime"] = _now_iso()
    exit_code, stdout, stderr = ec2_svc.exec_in_instance_blocking(instance_id, argv)
    invocation["ResponseCode"] = exit_code
    invocation["StandardOutputContent"] = stdout[:_STDOUT_LIMIT]
    invocation["StandardErrorContent"] = stderr[:_STDERR_LIMIT]
    invocation["ExecutionEndDateTime"] = _now_iso()
    # Status flips last: a poller that sees a terminal state must already see the exit code,
    # the output and the end time.
    # "For a plugin, this indicates that the result code wasn't zero."
    invocation["StatusDetails"] = "Success" if exit_code == 0 else "Failed"
    invocation["Status"] = invocation["StatusDetails"]
    _refresh_command_status(record)


def _refresh_command_status(record) -> None:
    """Roll the invocations up into the parent command."""
    invocations = list((record.get("Invocations") or {}).values())
    terminal = [i for i in invocations
                if i["Status"] in ("Success", "Failed", "Cancelled", "TimedOut")]
    failed = [i for i in invocations if i["Status"] == "Failed"]
    record["CompletedCount"] = len(terminal)
    record["ErrorCount"] = len(failed)
    if len(terminal) < len(invocations):
        record["Status"] = "InProgress"
        record["StatusDetails"] = "In Progress"
    elif failed:
        record["Status"] = "Failed"
        record["StatusDetails"] = "Failed"
    else:
        record["Status"] = "Success"
        record["StatusDetails"] = "Success"


def _command_target_ids(data) -> list:
    """The instances a SendCommand addresses: InstanceIds, or a Targets entry naming them."""
    instance_ids = list(data.get("InstanceIds") or [])
    for target in data.get("Targets") or []:
        if target.get("Key") in ("InstanceIds", "instanceids"):
            instance_ids.extend(target.get("Values") or [])
    return instance_ids


def _public_command(record) -> dict:
    """The Command shape, without the per-instance invocations kept alongside it."""
    return {k: v for k, v in record.items() if k != "Invocations"}


def _send_command(data):
    document = data.get("DocumentName") or ""
    if not document:
        return error_response_json("ValidationException", "DocumentName is required", 400)

    instance_ids = _command_target_ids(data)
    if not instance_ids:
        return error_response_json(
            "ValidationException", "InstanceIds or a Targets entry is required", 400
        )
    live = _managed_instance_ids()
    unknown = [i for i in instance_ids if i not in live]
    if unknown:
        return error_response_json(
            "InvalidInstanceId",
            f"Instances [[{', '.join(unknown)}]] not in a valid state for account "
            f"{get_account_id()}",
            400,
        )

    command_id = new_uuid()
    now = _now_epoch()
    started = _now_iso()
    record = {
        "CommandId": command_id,
        "DocumentName": document,
        "DocumentVersion": data.get("DocumentVersion", "$DEFAULT"),
        "Comment": data.get("Comment", ""),
        "ExpiresAfter": now + 3600,
        "Parameters": data.get("Parameters", {}),
        "InstanceIds": instance_ids,
        "Targets": data.get("Targets", []),
        "RequestedDateTime": now,
        # SendCommand answers before the command has been delivered — the doc's own sample
        # response is Pending. The caller polls GetCommandInvocation from here.
        "Status": "Pending",
        "StatusDetails": "Pending",
        "OutputS3Region": get_region(),
        "OutputS3BucketName": data.get("OutputS3BucketName", ""),
        "OutputS3KeyPrefix": data.get("OutputS3KeyPrefix", ""),
        "MaxConcurrency": data.get("MaxConcurrency", "50"),
        "MaxErrors": data.get("MaxErrors", "0"),
        "TargetCount": len(instance_ids),
        "CompletedCount": 0,
        "ErrorCount": 0,
        "DeliveryTimedOutCount": 0,
        "ServiceRole": data.get("ServiceRoleArn", ""),
        "TimeoutSeconds": data.get("TimeoutSeconds", 3600),
        "Invocations": {
            instance_id: {
                "CommandId": command_id,
                "InstanceId": instance_id,
                "Comment": data.get("Comment", ""),
                "DocumentName": document,
                "DocumentVersion": data.get("DocumentVersion", "$DEFAULT"),
                "PluginName": _RUN_COMMAND_PLUGIN,
                # "If the response code is -1, then the command hasn't started running on the
                # managed node, or it wasn't received by the node."
                "ResponseCode": -1,
                # These three are strings in the SSM model, not timestamps, and are empty until
                # the plugin starts.
                "ExecutionStartDateTime": "",
                "ExecutionElapsedTime": "PT0.001S",
                "ExecutionEndDateTime": "",
                "Status": "Pending",
                "StatusDetails": "Pending",
                "StandardOutputContent": "",
                "StandardOutputUrl": "",
                "StandardErrorContent": "",
                "StandardErrorUrl": "",
            }
            for instance_id in instance_ids
        },
    }
    _commands[command_id] = record
    if document in _SHELL_DOCUMENTS:
        argv = _shell_argv(data)
        for instance_id in instance_ids:
            spawn_background(_finish_invocation, command_id, instance_id, argv,
                             thread_name=f"ministack-ssm-{command_id[:8]}")
    else:
        # A document we cannot run still has to converge, or a caller polling for a terminal
        # state never returns.
        for instance_id in instance_ids:
            invocation = record["Invocations"][instance_id]
            invocation.update({"ResponseCode": 0, "Status": "Success",
                               "StatusDetails": "Success",
                               "ExecutionStartDateTime": started,
                               "ExecutionEndDateTime": started})
        _refresh_command_status(record)
    return json_response({"Command": _public_command(record)})


def _get_command_invocation(data):
    command_id = data.get("CommandId") or ""
    instance_id = data.get("InstanceId") or ""
    invocation = ((_commands.get(command_id) or {}).get("Invocations") or {}).get(instance_id)
    if not invocation:
        # AWS resolves the invocation before the command, so an unknown command id lands here too.
        return error_response_json(
            "InvocationDoesNotExist",
            f"Invocation not found for {command_id}, {instance_id}",
            400,
        )
    return json_response(dict(invocation))


def _list_commands(data):
    command_id = data.get("CommandId")
    instance_id = data.get("InstanceId")
    # An absent command id narrows to nothing rather than failing, which is what AWS answers.
    commands = [
        _public_command(record)
        for record in _commands.values()
        if (not command_id or record["CommandId"] == command_id)
        and (not instance_id or instance_id in record.get("InstanceIds", []))
    ]
    commands.sort(key=lambda c: c["RequestedDateTime"], reverse=True)

    start = _decode_next_token(data.get("NextToken", "")) if data.get("NextToken") else 0
    limit = data.get("MaxResults") or DEFAULT_PAGE_SIZE
    page = commands[start:start + limit]
    out = {"Commands": page}
    if start + limit < len(commands):
        out["NextToken"] = _encode_next_token(start + limit)
    return json_response(out)


def _describe_instance_information(data):
    from ministack.services import ec2 as ec2_svc

    now = _now_epoch()
    managed = _managed_instance_ids()
    entries = []
    for instance_id, inst in sorted(ec2_svc._instances.items()):
        if instance_id not in managed:
            continue
        private_ip = inst.get("PrivateIpAddress", "")
        entries.append({
            "InstanceId": instance_id,
            "PingStatus": "Online",
            "LastPingDateTime": now,
            "AgentVersion": "3.2.1377.0",
            "IsLatestVersion": True,
            "PlatformType": "Linux",
            "PlatformName": "Amazon Linux",
            "PlatformVersion": "2023",
            "ResourceType": "EC2Instance",
            "IPAddress": private_ip,
            "ComputerName": inst.get("PrivateDnsName", ""),
        })

    start = _decode_next_token(data.get("NextToken", "")) if data.get("NextToken") else 0
    limit = data.get("MaxResults") or DEFAULT_PAGE_SIZE
    page = entries[start:start + limit]
    out = {"InstanceInformationList": page}
    if start + limit < len(entries):
        out["NextToken"] = _encode_next_token(start + limit)
    return json_response(out)


def _service_setting_id(data):
    value = data.get("SettingId") or ""
    if value.startswith("arn:"):
        resource = value.split(":", 5)[-1]
        value = resource[len("servicesetting"):] if resource.startswith("servicesetting/") else ""
    return value if value in _SERVICE_SETTING_DEFAULTS else None


def _service_setting_out(setting_id):
    default_value, default_date = _SERVICE_SETTING_DEFAULTS[setting_id]
    stored = _service_settings.get(setting_id)
    return {
        "SettingId": setting_id,
        "SettingValue": stored["value"] if stored else default_value,
        "LastModifiedDate": stored["modified"] if stored else default_date,
        "LastModifiedUser": stored["user"] if stored else "System",
        "ARN": f"arn:aws:ssm:{get_region()}:{get_account_id()}:servicesetting{setting_id}",
        "Status": "Customized" if stored else "Default",
    }


def _default_tier(data, value, existing):
    """Tier of a put without one: the default-parameter-tier setting; an Advanced parameter stays Advanced."""
    if existing and existing.get("Tier") == "Advanced":
        return "Advanced"
    setting = _service_settings.get(_DEFAULT_TIER_SETTING)
    tier = setting["value"] if setting else "Standard"
    if tier == "Intelligent-Tiering":
        needs_advanced = len(value.encode()) > 4096 or data.get("Policies") or len(_parameters) > 10000
        return "Advanced" if needs_advanced else "Standard"
    return tier


def _service_setting_not_found(data):
    return error_response_json(
        "ServiceSettingNotFound", f"ServiceSetting {data.get('SettingId')} does not exist.", 400
    )


def _get_service_setting(data):
    setting_id = _service_setting_id(data)
    if setting_id is None:
        return _service_setting_not_found(data)
    return json_response({"ServiceSetting": _service_setting_out(setting_id)})


def _update_service_setting(data, caller):
    setting_id = _service_setting_id(data)
    if setting_id is None:
        return _service_setting_not_found(data)
    value = data.get("SettingValue")
    accepted = _SERVICE_SETTING_VALUES.get(setting_id)
    if accepted and value not in accepted:
        return error_response_json(
            "ValidationException", f"Invalid setting value, allowed value pattern: {'|'.join(accepted)}", 400
        )
    _service_settings[setting_id] = {"value": value, "modified": _now_epoch(), "user": caller}
    return json_response({})


def _reset_service_setting(data):
    setting_id = _service_setting_id(data)
    if setting_id is None:
        return _service_setting_not_found(data)
    _service_settings.pop(setting_id, None)
    return json_response({"ServiceSetting": _service_setting_out(setting_id)})


# ── Documents ─────────────────────────────────────────────

_DOCUMENT_TYPES = {
    "Command", "Policy", "Automation", "Session", "Package",
    "ApplicationConfiguration", "ApplicationConfigurationSchema",
    "DeploymentStrategy", "ChangeCalendar", "Automation.ChangeTemplate",
    "ProblemAnalysis", "ProblemAnalysisTemplate", "CloudFormation",
    "ConformancePackTemplate", "QuickSetup",
}

_AUTOMATION_STEP_ACTIONS = {
    "aws:approve", "aws:assertAwsResourceProperty", "aws:branch",
    "aws:changeInstanceState", "aws:copyImage", "aws:createImage",
    "aws:createStack", "aws:createTags", "aws:deleteImage", "aws:deleteStack",
    "aws:executeAutomation", "aws:executeAwsApi", "aws:executeScript",
    "aws:executeStateMachine", "aws:invokeLambdaFunction", "aws:invokeWebhook",
    "aws:pause", "aws:runCommand", "aws:runInstances", "aws:sleep",
    "aws:updateServiceSetting", "aws:waitForAwsResourceProperty",
}

_PARAM_FIELD_OUT = {
    "name": "Name", "type": "Type", "description": "Description",
    "default": "DefaultValue", "allowedValues": "AllowedValues",
    "allowedPattern": "AllowedPattern", "displayType": "DisplayType",
}

_PARAM_REF = re.compile(r"\{\{\s*([A-Za-z0-9_.\-]+)\s*\}\}")


def _parse_document_content(content, fmt):
    if fmt == "JSON":
        try:
            return json.loads(content), None
        except json.JSONDecodeError as e:
            return None, error_response_json(
                "InvalidDocumentContent",
                f"JSON not well-formed. at Line: {e.lineno}, Column: {e.colno}", 400)
    try:
        return yaml.safe_load(content), None
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        line = mark.line + 1 if mark else 1
        col = mark.column + 1 if mark else 1
        return None, error_response_json(
            "InvalidDocumentContent",
            f"YAML not well-formed. at Line: {line}, Column: {col}", 400)


def _canonical_json(value, depth=1):
    """JSON in the pretty layout AWS serves in GetDocument Content."""
    if isinstance(value, dict):
        if not value:
            return "{ }"
        inner = ",\n".join(
            f'{"  " * depth}{json.dumps(k)} : {_canonical_json(v, depth + 1)}'
            for k, v in value.items())
        return "{\n" + inner + "\n" + "  " * (depth - 1) + "}"
    if isinstance(value, list):
        if not value:
            return "[ ]"
        return "[ " + ", ".join(_canonical_json(v, depth) for v in value) + " ]"
    return json.dumps(value)


def _document_description(doc, for_describe=False):
    desc = {
        "Hash": doc["Hash"],
        "HashType": "Sha256",
        "Name": doc["Name"],
        "Owner": doc["Owner"],
        "CreatedDate": doc["CreatedDate"],
        "Status": doc["Status"],
        "DocumentVersion": doc["DocumentVersion"],
        "PlatformTypes": doc["PlatformTypes"],
        "DocumentType": doc["DocumentType"],
        "SchemaVersion": doc["SchemaVersion"],
        "LatestVersion": doc["DocumentVersion"],
        "DefaultVersion": doc["DocumentVersion"],
        "DocumentFormat": doc["DocumentFormat"],
        "Tags": doc["Tags"],
    }
    if doc.get("Description"):
        desc["Description"] = doc["Description"]
    if doc.get("Parameters"):
        desc["Parameters"] = doc["Parameters"]
    if doc.get("AssumeRole"):
        desc["AssumeRole"] = doc["AssumeRole"]
    if for_describe:
        desc["DocumentId"] = doc["DocumentId"]
        desc["Category"] = []
        desc["CategoryEnum"] = []
    return desc


def _doc_missing(name):
    return error_response_json(
        "InvalidDocument", f"Document with name {name} does not exist.", 400)


def _activate_document(name):
    time.sleep(1.5)
    doc = _documents.get(name)
    if doc and doc["Status"] == "Creating":
        doc["Status"] = "Active"


def _create_document(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    if name.lower().startswith("aws-"):
        return error_response_json(
            "ValidationException", f"Invalid document name {name}", 400)
    content = data.get("Content")
    if not content:
        return error_response_json("ValidationException", "Content is required", 400)
    doc_type = data.get("DocumentType", "Command")
    if doc_type not in _DOCUMENT_TYPES:
        return error_response_json(
            "ValidationException",
            "1 validation error detected: Value at 'documentType' failed to satisfy "
            "constraint: Member is not a valid documentType", 400)
    fmt = data.get("DocumentFormat", "JSON")
    parsed, err = _parse_document_content(content, fmt)
    if err:
        return err
    parsed = parsed if isinstance(parsed, dict) else {}
    parsed = json.loads(json.dumps(parsed, default=str))
    if doc_type == "Automation":
        steps = parsed.get("mainSteps")
        if not steps:
            return error_response_json(
                "InvalidDocumentContent", 'Missing steps in "mainSteps".', 400)
        for step in steps:
            action = step.get("action", "") if isinstance(step, dict) else ""
            if action not in _AUTOMATION_STEP_ACTIONS:
                return error_response_json(
                    "InvalidDocumentContent", f"Unknown action: {action}", 400)
    if _documents.get(name) is not None:
        return error_response_json(
            "DocumentAlreadyExists",
            f"Document with same name {name} already exists", 400)
    parameters = [
        {"Name": pname, **{_PARAM_FIELD_OUT[k]: v for k, v in p.items() if k in _PARAM_FIELD_OUT}}
        for pname, p in (parsed.get("parameters") or {}).items()
        if isinstance(p, dict)
    ]
    doc = {
        "Name": name,
        "Content": content,
        "ParsedContent": parsed,
        "DocumentType": doc_type,
        "DocumentFormat": fmt,
        "DocumentVersion": "1",
        "Status": "Creating",
        "CreatedDate": _now_epoch(),
        "Owner": get_account_id(),
        "Hash": hashlib.sha256(content.encode()).hexdigest(),
        "DocumentId": new_uuid(),
        "Description": parsed.get("description", ""),
        "SchemaVersion": str(parsed.get("schemaVersion", "0.3")),
        "Parameters": parameters,
        "PlatformTypes": parsed.get("platformTypes") or ["Windows", "Linux", "MacOS"],
        "AssumeRole": parsed.get("assumeRole", ""),
        "Tags": data.get("Tags", []),
    }
    _documents[name] = doc
    spawn_background(_activate_document, name)
    return json_response({"DocumentDescription": _document_description(doc)})


def _delete_document(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    if _documents.pop(name, None) is None:
        return error_response_json(
            "InvalidDocument", f"Document {name} does not exist in your account", 400)
    return json_response({})


def _describe_document(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    doc = _documents.get(name)
    if doc is None:
        return _doc_missing(name)
    return json_response({"Document": _document_description(doc, for_describe=True)})


def _get_document(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)
    doc = _documents.get(name)
    if doc is None:
        return _doc_missing(name)
    version = data.get("DocumentVersion")
    if version and version != doc["DocumentVersion"]:
        return error_response_json(
            "InvalidDocumentVersion",
            f"Document version: {version} is invalid, latest version is "
            f"{doc['DocumentVersion']}", 400)
    fmt = data.get("DocumentFormat", "JSON")
    if fmt not in ("YAML", "JSON", "TEXT"):
        return error_response_json(
            "ValidationException",
            "1 validation error detected: Value at 'documentFormat' failed to "
            "satisfy constraint: Member must satisfy enum value set: "
            "[YAML, JSON, TEXT]", 400)
    return json_response({
        "AllowInvalidContent": False,
        "Content": doc["Content"] if fmt in ("YAML", "TEXT")
                   else _canonical_json(doc["ParsedContent"]),
        "CreatedDate": doc["CreatedDate"],
        "DocumentFormat": fmt,
        "DocumentType": doc["DocumentType"],
        "DocumentVersion": doc["DocumentVersion"],
        "Name": name,
        "Status": doc["Status"],
    })


def _list_documents(data):
    names = None
    owner_self = True
    filters = list(data.get("Filters", []))
    filters += [{"Key": f.get("Key"), "Values": [f.get("Value")]}
                for f in data.get("DocumentFilterList", [])]
    for flt in filters:
        if flt.get("Key") == "Name":
            names = set(flt.get("Values", []))
        elif flt.get("Key") == "Owner":
            owner_self = "Self" in flt.get("Values", [])
    items = [
        {
            "Name": d["Name"],
            "Owner": d["Owner"],
            "DocumentVersion": d["DocumentVersion"],
            "DocumentType": d["DocumentType"],
            "SchemaVersion": d["SchemaVersion"],
            "DocumentFormat": d["DocumentFormat"],
            "PlatformTypes": d["PlatformTypes"],
            "CreatedDate": d["CreatedDate"],
            "Tags": d["Tags"],
        }
        for d in _documents.values()
        if owner_self and (names is None or d["Name"] in names)
    ]
    return json_response({"DocumentIdentifiers": items})


# ── Automation executions ─────────────────────────────────

# Ops an aws:executeAwsApi step may dispatch to, keyed by model Api name.
_AUTOMATION_SSM_APIS = {
    "GetParameter": _get_parameter,
    "GetParameters": _get_parameters,
    "GetParametersByPath": _get_parameters_by_path,
    "PutParameter": _put_parameter,
    "DeleteParameter": _delete_parameter,
    "GetServiceSetting": _get_service_setting,
}

_ISO8601_DURATION = re.compile(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?$")


def _step_failure_message(inner):
    return (f"Step fails when it is Execute/Cancelling action. {inner}. "
            "Please refer to Automation Service Troubleshooting Guide for "
            "more diagnosis details.")


def _sleep_seconds(duration):
    m = _ISO8601_DURATION.match(duration or "")
    if not m:
        return 0.0
    hours, minutes, seconds = m.groups()
    return int(hours or 0) * 3600 + int(minutes or 0) * 60 + float(seconds or 0)


def _execute_step(step, record):
    action = step["Action"]
    inputs = {k: json.loads(v) for k, v in step["Inputs"].items()}
    if action == "aws:sleep":
        time.sleep(_sleep_seconds(inputs.get("Duration")))
        return None
    if action == "aws:executeAwsApi":
        if inputs.get("Service") != "ssm":
            return "aws:executeAwsApi is emulated for Service ssm only"
        api = inputs.get("Api", "")
        fn = _AUTOMATION_SSM_APIS.get(api)
        if fn is None:
            return f"MiniStack does not emulate ssm:{api} in aws:executeAwsApi"
        status, _headers, body = fn(
            {k: v for k, v in inputs.items() if k not in ("Service", "Api")})
        payload = json.loads(body)
        if status >= 400 or "__type" in payload:
            return (f"An error occurred ({payload.get('__type')}) when calling "
                    f"the {api} operation: {payload.get('message', '')}")
        return None
    if action == "aws:runCommand":
        status, _headers, body = _send_command({
            "DocumentName": inputs.get("DocumentName", "AWS-RunShellScript"),
            "InstanceIds": inputs.get("InstanceIds", []),
            "Parameters": inputs.get("Parameters", {}),
        })
        payload = json.loads(body)
        if status >= 400 or "__type" in payload:
            return (f"An error occurred ({payload.get('__type')}) when calling "
                    f"the SendCommand operation: {payload.get('message', '')}")
        command = _commands.get(payload["Command"]["CommandId"])
        while (command and command["Status"] in ("Pending", "InProgress")
               and record["Status"] == "InProgress"):
            time.sleep(0.2)
            _refresh_command_status(command)
        if command and command["Status"] != "Success" and record["Status"] == "InProgress":
            return (f"Command {command['CommandId']} finished with status "
                    f"{command['Status']}")
        return None
    return f"{action} is not emulated"


def _run_automation(execution_id):
    record = _automation_executions.get(execution_id)
    if record is None:
        return
    record["Status"] = "InProgress"
    failure = None
    for step in record["StepExecutions"]:
        if record["Status"] != "InProgress":
            break
        step["StepStatus"] = "InProgress"
        step["Inputs"] = step.pop("PendingInputs", {})
        step["Outputs"] = {}
        step["ExecutionStartTime"] = _now_epoch()
        if step["Action"] in ("aws:executeAwsApi", "aws:runCommand"):
            step["ThrottlingDetected"] = False
        record["CurrentStepName"] = step["StepName"]
        record["CurrentAction"] = step["Action"]
        failure = _execute_step(step, record)
        step["ExecutionEndTime"] = _now_epoch()
        if record["Status"] != "InProgress":
            step["StepStatus"] = "Cancelled"
            break
        if failure:
            step["StepStatus"] = "Failed"
            step["FailureMessage"] = _step_failure_message(failure)
            step["FailureDetails"] = {
                "FailureStage": "Invocation",
                "FailureType": "Verification",
                "Details": {"VerificationErrorMessage": [failure]},
            }
            break
        step["StepStatus"] = "Success"
        failure = None
    record.pop("CurrentStepName", None)
    record.pop("CurrentAction", None)
    if record["Status"] == "InProgress":
        record["ExecutionEndTime"] = _now_epoch()
        if failure:
            record["Status"] = "Failed"
            record["FailureMessage"] = _step_failure_message(failure)
        else:
            record["Status"] = "Success"


def _step_out(step):
    return {k: v for k, v in step.items() if k != "PendingInputs"}


def _automation_execution_out(record):
    out = {
        "AutomationExecutionId": record["AutomationExecutionId"],
        "AutomationExecutionStatus": record["Status"],
        "DocumentName": record["DocumentName"],
        "DocumentVersion": record["DocumentVersion"],
        "ExecutedBy": record["ExecutedBy"],
        "ExecutionStartTime": record["ExecutionStartTime"],
        "Mode": record["Mode"],
        "Outputs": {},
        "Parameters": record["Parameters"],
        "ResolvedTargets": {"ParameterValues": [], "Truncated": False},
        "StepExecutions": [_step_out(s) for s in record["StepExecutions"]],
        "StepExecutionsTruncated": False,
        "Targets": record["Targets"],
    }
    if record["Status"] == "InProgress":
        out["CurrentAction"] = record.get("CurrentAction", "")
        out["CurrentStepName"] = record.get("CurrentStepName", "")
    if record.get("ExecutionEndTime") is not None:
        out["ExecutionEndTime"] = record["ExecutionEndTime"]
    if record.get("FailureMessage"):
        out["FailureMessage"] = record["FailureMessage"]
    return out


def _render_param_refs(value, resolved):
    if isinstance(value, str):
        return _PARAM_REF.sub(
            lambda m: str(resolved.get(m.group(1), m.group(0))), value)
    if isinstance(value, list):
        return [_render_param_refs(v, resolved) for v in value]
    if isinstance(value, dict):
        return {k: _render_param_refs(v, resolved) for k, v in value.items()}
    return value


def _start_automation_execution(data, caller):
    doc_name = data.get("DocumentName") or ""
    if not doc_name:
        return error_response_json("ValidationException", "DocumentName is required", 400)
    doc = _documents.get(doc_name)
    if doc is None:
        return error_response_json(
            "AutomationDefinitionNotFoundException",
            "Invalid input for field document name, expected valid document "
            "in account.", 400)
    declared = {p["Name"]: p for p in doc.get("Parameters", [])}
    passed = data.get("Parameters") or {}
    unknown = sorted(set(passed) - set(declared))
    if unknown:
        return error_response_json(
            "InvalidAutomationExecutionParametersException",
            f"Undefined execution inputs: [{', '.join(unknown)}].", 400)
    resolved = {}
    for pname, pdef in declared.items():
        if pname in passed:
            v = passed[pname]
            resolved[pname] = v[0] if isinstance(v, list) and len(v) == 1 else v
        elif "DefaultValue" in pdef:
            resolved[pname] = pdef["DefaultValue"]
    steps = [
        {
            "StepName": s.get("name", ""),
            "Action": s.get("action", ""),
            "StepExecutionId": new_uuid(),
            "StepStatus": "Pending",
            "Inputs": {},
            "PendingInputs": {
                k: json.dumps(_render_param_refs(v, resolved))
                for k, v in (s.get("inputs") or {}).items()
            },
            "IsStepBlocked": False,
            "OverriddenParameters": {},
        }
        for s in (doc["ParsedContent"].get("mainSteps") or [])
        if isinstance(s, dict)
    ]
    execution_id = new_uuid()
    _automation_executions[execution_id] = {
        "AutomationExecutionId": execution_id,
        "DocumentName": doc_name,
        "DocumentVersion": doc["DocumentVersion"],
        "Status": "Pending",
        "ExecutionStartTime": _now_epoch(),
        "ExecutedBy": caller,
        "Parameters": passed,
        "Mode": data.get("Mode", "Auto"),
        "Targets": data.get("Targets", []),
        "StepExecutions": steps,
    }
    spawn_background(_run_automation, execution_id)
    return json_response({"AutomationExecutionId": execution_id})


def _get_automation_execution(data):
    execution_id = data.get("AutomationExecutionId") or ""
    if not execution_id:
        return error_response_json(
            "ValidationException", "AutomationExecutionId is required", 400)
    record = _automation_executions.get(execution_id)
    if record is None:
        return error_response_json(
            "AutomationExecutionNotFoundException",
            "The input automation execution does not exist.", 400)
    return json_response({"AutomationExecution": _automation_execution_out(record)})


def _describe_automation_executions(data):
    exec_ids = statuses = name_prefix = None
    for flt in data.get("Filters", []):
        key, values = flt.get("Key"), flt.get("Values", [])
        if key == "ExecutionId":
            exec_ids = set(values)
        elif key == "ExecutionStatus":
            statuses = set(values)
        elif key == "DocumentNamePrefix":
            name_prefix = values[0] if values else ""
    items = []
    for r in _automation_executions.values():
        if exec_ids is not None and r["AutomationExecutionId"] not in exec_ids:
            continue
        if statuses is not None and r["Status"] not in statuses:
            continue
        if name_prefix is not None and not r["DocumentName"].startswith(name_prefix):
            continue
        meta = {
            "AutomationExecutionId": r["AutomationExecutionId"],
            "AutomationExecutionStatus": r["Status"],
            "AutomationType": "Local",
            "DocumentName": r["DocumentName"],
            "DocumentVersion": r["DocumentVersion"],
            "ExecutedBy": r["ExecutedBy"],
            "ExecutionStartTime": r["ExecutionStartTime"],
            "LogFile": "",
            "Mode": r["Mode"],
            "Outputs": {},
            "ResolvedTargets": {"ParameterValues": [], "Truncated": False},
            "Targets": r["Targets"],
        }
        if r.get("ExecutionEndTime") is not None:
            meta["ExecutionEndTime"] = r["ExecutionEndTime"]
        if r.get("FailureMessage"):
            meta["FailureMessage"] = r["FailureMessage"]
        items.append(meta)
    return json_response({"AutomationExecutionMetadataList": items})


def _stop_automation_execution(data):
    execution_id = data.get("AutomationExecutionId") or ""
    if not execution_id:
        return error_response_json(
            "ValidationException", "AutomationExecutionId is required", 400)
    record = _automation_executions.get(execution_id)
    if record is None:
        return error_response_json(
            "AutomationExecutionNotFoundException",
            "The input automation execution does not exist.", 400)
    if record["Status"] in ("Pending", "InProgress"):
        record["Status"] = "Cancelled"
        record["ExecutionEndTime"] = _now_epoch()
        for step in record["StepExecutions"]:
            if step["StepStatus"] == "InProgress":
                step["StepStatus"] = "Cancelled"
                step["ExecutionEndTime"] = _now_epoch()
    return json_response({})


def reset():
    _service_settings.clear()
    _parameters.clear()
    _parameter_history.clear()
    _tags.clear()
    _commands.clear()
    _documents.clear()
    _automation_executions.clear()
