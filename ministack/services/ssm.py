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
import json
import logging
import os
import time
from datetime import datetime, timezone

from ministack.core.arn import ArnParseError, parse_arn
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

from ministack.core.persistence import PERSIST_STATE, load_state

_parameters = AccountRegionScopedDict()
_parameter_history = AccountRegionScopedDict()
_tags = AccountRegionScopedDict()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "parameters": copy.deepcopy(_parameters),
        "parameter_history": copy.deepcopy(_parameter_history),
        "tags": copy.deepcopy(_tags),
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


def restore_state(data):
    if data:
        _parameters.update(data.get("parameters", {}))
        _restore_parameter_history(data.get("parameter_history", {}))
        _tags.update(data.get("tags", {}))


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


try:
    _restored = load_state("ssm")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


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

    version = (existing["Version"] + 1) if existing else 1
    arn = existing.get("ARN") if existing else _param_arn(name)
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
        "Description": data.get("Description", existing.get("Description", "") if existing else ""),
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


def resolve_parameter_value(name_or_arn):
    """Return an SSM parameter's value by name or ARN, or None.

    Used by other services (e.g. ECS `secrets[].valueFrom`) that need to read a
    parameter value in-process without going through the HTTP API. Accepts a
    bare name (``/path/name`` or ``name``) or a full ARN
    (``arn:aws:ssm:region:acct:parameter/path/name``).
    """
    if not name_or_arn:
        return None
    _, param = _lookup_parameter(name_or_arn, allow_arn_region=True, flexible_name=True)
    return param.get("Value") if param else None


def _get_parameter(data):
    name = data.get("Name")
    _, param = _lookup_parameter(name)
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
        _, p = _lookup_parameter(name)
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


def reset():
    _parameters.clear()
    _parameter_history.clear()
    _tags.clear()
