"""
SSM Parameter Store Emulator.
JSON-based API via X-Amz-Target (AmazonSSM).
Supports: PutParameter, GetParameter, GetParameters, GetParametersByPath,
          DeleteParameter, DeleteParameters, DescribeParameters,
          GetParameterHistory, LabelParameterVersion,
          AddTagsToResource, RemoveTagsFromResource, ListTagsForResource.
"""

import base64
import copy
import os
import json
import logging
import time
from datetime import datetime, timezone

from ministack.core.responses import AccountScopedDict, get_account_id, error_response_json, json_response, new_uuid, get_region

logger = logging.getLogger("ssm")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")
DEFAULT_PAGE_SIZE = 10

from ministack.core.persistence import load_state, PERSIST_STATE

_parameters = AccountScopedDict()
_parameter_history = AccountScopedDict()
_tags = AccountScopedDict()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "parameters": copy.deepcopy(_parameters),
        "parameter_history": copy.deepcopy(_parameter_history),
        "tags": copy.deepcopy(_tags),
    }


def restore_state(data):
    if data:
        _parameters.update(data.get("parameters", {}))
        _parameter_history.update(data.get("parameter_history", {}))
        _tags.update(data.get("tags", {}))


try:
    _restored = load_state("ssm")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _now_epoch() -> float:
    return datetime.now(timezone.utc).timestamp()


def _param_arn(name: str) -> str:
    return f"arn:aws:ssm:{get_region()}:{get_account_id()}:parameter{name}"


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
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)
    return handler(data)


def _put_parameter(data):
    name = data.get("Name")
    if not name:
        return error_response_json("ValidationException", "Name is required", 400)

    param_type = data.get("Type", "String")
    value = data.get("Value", "")
    overwrite = data.get("Overwrite", False)

    if name in _parameters and not overwrite:
        return error_response_json(
            "ParameterAlreadyExists",
            "The parameter already exists. To overwrite this value, set the overwrite option in the request to true.",
            400,
        )

    version = (_parameters[name]["Version"] + 1) if name in _parameters else 1
    arn = _param_arn(name)
    now = _now_epoch()

    stored_value = value
    if param_type == "SecureString":
        key_id = data.get("KeyId", "alias/aws/ssm")
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
        "Description": data.get("Description", _parameters.get(name, {}).get("Description", "")),
        "Tier": data.get("Tier", "Standard"),
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


def _get_parameter(data):
    name = data.get("Name")
    param = _parameters.get(name)
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
        p = _parameters.get(name)
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
    if name not in _parameters:
        return error_response_json("ParameterNotFound", f"Parameter {name} not found", 400)
    del _parameters[name]
    _parameter_history.pop(name, None)
    arn = _param_arn(name)
    _tags.pop(arn, None)
    return json_response({})


def _delete_parameters(data):
    names = data.get("Names", [])
    deleted = []
    invalid = []
    for name in names:
        if name in _parameters:
            del _parameters[name]
            _parameter_history.pop(name, None)
            _tags.pop(_param_arn(name), None)
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

    if resource_type == "Parameter":
        if not resource_id.startswith("/"):
            resource_id = "/" + resource_id
        arn = _param_arn(resource_id)
    else:
        arn = resource_id

    if arn not in _tags:
        _tags[arn] = {}
    for tag in new_tags:
        _tags[arn][tag["Key"]] = tag["Value"]

    return json_response({})


def _remove_tags_from_resource(data):
    resource_type = data.get("ResourceType", "Parameter")
    resource_id = data.get("ResourceId", "")
    tag_keys = data.get("TagKeys", [])

    if resource_type == "Parameter":
        if not resource_id.startswith("/"):
            resource_id = "/" + resource_id
        arn = _param_arn(resource_id)
    else:
        arn = resource_id

    if arn in _tags:
        for key in tag_keys:
            _tags[arn].pop(key, None)

    return json_response({})


def _list_tags_for_resource(data):
    resource_type = data.get("ResourceType", "Parameter")
    resource_id = data.get("ResourceId", "")

    if resource_type == "Parameter":
        if not resource_id.startswith("/"):
            resource_id = "/" + resource_id
        arn = _param_arn(resource_id)
    else:
        arn = resource_id

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


def reset():
    _parameters.clear()
    _parameter_history.clear()
    _tags.clear()
