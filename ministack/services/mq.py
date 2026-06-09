"""AmazonMQ Service Emulator."""

import copy
import json
import logging
import re
import time
from urllib.parse import unquote

from ministack.core.persistence import load_state
from ministack.core.responses import AccountScopedDict, get_account_id, get_region, new_uuid

logger = logging.getLogger("mq")

_brokers: AccountScopedDict = AccountScopedDict()
_name_index: AccountScopedDict = AccountScopedDict()
_tags: AccountScopedDict = AccountScopedDict()
_users: AccountScopedDict = AccountScopedDict()

SUPPORTED_ENGINES = {
    "RABBITMQ": {
        "storage_types": ["EBS"],
        "deployment_modes": ["SINGLE_INSTANCE", "CLUSTER_MULTI_AZ"],
        "versions": ["4.2", "3.13"],
        "host_instance_types": [
            "mq.m5.2xlarge", "mq.m5.4xlarge", "mq.m5.large", "mq.m5.xlarge",
            "mq.m7g.12xlarge", "mq.m7g.16xlarge", "mq.m7g.2xlarge", "mq.m7g.4xlarge",
            "mq.m7g.8xlarge", "mq.m7g.large", "mq.m7g.medium", "mq.m7g.xlarge",
        ],
    },
    "ACTIVEMQ": {
        "storage_types": ["EBS", "EFS"],
        "deployment_modes": ["SINGLE_INSTANCE", "ACTIVE_STANDBY_MULTI_AZ"],
        "versions": ["5.19", "5.18"],
        "host_instance_types": [
            "mq.m5.2xlarge", "mq.m5.4xlarge", "mq.m5.large", "mq.m5.xlarge", "mq.t3.micro"
        ],
    },
}

_BROKER_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,50}$")
_INVALID_PASSWORD_CHARS_RE = re.compile(r"[,:=]")

_JSON_CT = {"Content-Type": "application/json"}
_HTTP_TO_EXCEPTION = {
    400: "BadRequestException",
    403: "ForbiddenException",
    404: "NotFoundException",
    409: "ConflictException",
    500: "InternalServerErrorException",
}


def get_state() -> dict:
    return {
        "brokers": copy.deepcopy(_brokers._data),
        "name_index": copy.deepcopy(_name_index._data),
        "tags": copy.deepcopy(_tags._data),
        "users": copy.deepcopy(_users._data),
    }


def restore_state(data: dict) -> None:
    if not data:
        return
    _brokers._data.update(data.get("brokers", {}))
    _name_index._data.update(data.get("name_index", {}))
    _tags._data.update(data.get("tags", {}))
    _users._data.update(data.get("users", {}))


try:
    _restored = load_state("mq")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore mq state; starting fresh")


def _ok(data: dict) -> tuple:
    return 200, dict(_JSON_CT), json.dumps(data, ensure_ascii=False).encode("utf-8")

def _no_content() -> tuple:
    return 204, {}, b""

def _err(http_status: int, error_attribute: str, message: str) -> tuple:
    exc_type = _HTTP_TO_EXCEPTION.get(http_status, "BadRequestException")
    body = json.dumps(
        {"errorAttribute": error_attribute, "message": message, "__type": exc_type},
        ensure_ascii=False,
    ).encode("utf-8")
    return http_status, {**_JSON_CT, "x-amzn-errortype": exc_type}, body


def _make_broker_arn(broker_id: str) -> str:
    return f"arn:aws:mq:{get_region()}:{get_account_id()}:broker:{broker_id}"


def _valid_host_instance_types(engine_type: str | None) -> set[str]:
    if engine_type:
        return set(SUPPORTED_ENGINES.get(engine_type, {}).get("host_instance_types", []))
    out = set()
    for cfg in SUPPORTED_ENGINES.values():
        out.update(cfg["host_instance_types"])
    return out

def _valid_storage_types(engine_type: str | None) -> set[str]:
    if engine_type:
        return set(SUPPORTED_ENGINES.get(engine_type, {}).get("storage_types", []))
    out = set()
    for cfg in SUPPORTED_ENGINES.values():
        out.update(cfg["storage_types"])
    return out


def _parse_max_results(query_params: dict, *, default: int, minimum: int, maximum: int = 100, reject: set[int] | None = None):
    raw = query_params.get("maxResults")
    if raw is None:
        return default, None
    if isinstance(raw, list):
        raw = raw[-1] if raw else None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None, _err(400, "MaxResults", f"maxResults must be an integer from {minimum} to {maximum}.")
    if value < minimum or value > maximum or (reject and value in reject):
        return None, _err(400, "MaxResults", f"maxResults must be an integer from {minimum} to {maximum}.")
    return value, None


def _parse_next_token(query_params: dict):
    raw = query_params.get("nextToken")
    if raw is None:
        return 0, None
    if isinstance(raw, list):
        raw = raw[-1] if raw else None
    try:
        offset = int(raw)
    except (TypeError, ValueError):
        return 0, _err(400, "NextToken", "nextToken is invalid.")
    if offset < 0:
        return 0, _err(400, "NextToken", "nextToken is invalid.")
    return offset, None


def _paginate(items: list, offset: int, max_results: int):
    page = items[offset : offset + max_results]
    next_token = str(offset + max_results) if (offset + max_results) < len(items) else None
    return page, next_token


def _resource_exists(resource_arn: str) -> bool:
    return any(b.get("brokerArn") == resource_arn for b in _brokers.values())


def _get_broker_or_404(broker_id: str):
    broker = _brokers.get(broker_id)
    if broker is None:
        return None, _err(404, "BrokerId", f"Broker '{broker_id}' does not exist.")
    return broker, None

def _ensure_activemq_broker(broker: dict):
    if broker.get("engineType") != "ACTIVEMQ":
        return _err(400, "BrokerId", "This operation is supported only for ActiveMQ brokers.")
    return None

def _validate_password(password: str):
    if len(password) < 4 or _INVALID_PASSWORD_CHARS_RE.search(password):
        return _err(400, "Password", "Password must be at least 4 characters and cannot contain ',', ':' or '='.")
    return None

def _create_broker(body: dict) -> tuple:
    engine_type = str(body.get("engineType", "")).strip().upper()
    if engine_type not in SUPPORTED_ENGINES:
        return _err(400, "EngineType", f"Unsupported engine type '{engine_type}'.")

    broker_name = str(body.get("brokerName", "")).strip()
    if not broker_name:
        return _err(400, "BrokerName", "brokerName is required.")
    if not _BROKER_NAME_RE.fullmatch(broker_name):
        return _err(400, "BrokerName", "brokerName is invalid.")
    if broker_name in _name_index:
        return _err(409, "BrokerName", f"A broker with the name '{broker_name}' already exists.")

    versions = SUPPORTED_ENGINES[engine_type]["versions"]
    engine_version = body.get("engineVersion") or versions[0]
    if engine_version not in versions:
        return _err(400, "EngineVersion", "Engine version is invalid.")

    deployment_mode = str(body.get("deploymentMode", "SINGLE_INSTANCE")).upper()
    if deployment_mode not in SUPPORTED_ENGINES[engine_type]["deployment_modes"]:
        return _err(400, "DeploymentMode", "Deployment mode is invalid.")

    host_instance_type = body.get("hostInstanceType", "mq.m5.large")
    if host_instance_type not in _valid_host_instance_types(engine_type):
        return _err(400, "HostInstanceType", "Host instance type is invalid.")

    storage_type = body.get("storageType", "EBS")
    if storage_type and storage_type not in _valid_storage_types(engine_type):
        return _err(400, "StorageType", f"Invalid storage type: '{storage_type}'.")

    broker_id = new_uuid()
    broker_arn = _make_broker_arn(broker_id)
    created = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())

    _brokers[broker_id] = {
        "brokerId": broker_id,
        "brokerName": broker_name,
        "brokerArn": broker_arn,
        "brokerState": "RUNNING",
        "engineType": engine_type,
        "engineVersion": engine_version,
        "deploymentMode": deployment_mode,
        "hostInstanceType": host_instance_type,
        "publiclyAccessible": bool(body.get("publiclyAccessible", False)),
        "autoMinorVersionUpgrade": bool(body.get("autoMinorVersionUpgrade", True)),
        "created": created,
        "_createdAt": time.time_ns(),
        "brokerInstances": [{"consoleURL": "https://localhost:15671", "endpoints": ["amqps://localhost:5671"], "ipAddress": "127.0.0.1"}],
    }
    _name_index[broker_name] = broker_id

    return _ok({"brokerId": broker_id, "brokerArn": broker_arn})


def _list_brokers(query_params: dict) -> tuple:
    max_results, max_err = _parse_max_results(query_params, default=5, minimum=5, maximum=100, reject={4})
    if max_err:
        return max_err
    offset, token_err = _parse_next_token(query_params)
    if token_err:
        return token_err

    brokers_list = sorted(
        [
            {
                "brokerId": b["brokerId"],
                "brokerName": b["brokerName"],
                "brokerArn": b["brokerArn"],
                "brokerState": b["brokerState"],
                "deploymentMode": b["deploymentMode"],
                "engineType": b["engineType"],
                "engineVersion": b["engineVersion"],
                "hostInstanceType": b["hostInstanceType"],
                "created": b["created"],
                "_createdAt": b.get("_createdAt", 0),
            }
            for b in _brokers.values()
        ],
        key=lambda x: x["_createdAt"],
        reverse=True,
    )
    for row in brokers_list:
        row.pop("_createdAt", None)

    page, next_token = _paginate(brokers_list, offset, max_results)
    out = {"brokerSummaries": page}
    if next_token is not None:
        out["nextToken"] = next_token
    return _ok(out)


def _describe_broker(broker_id: str) -> tuple:
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    out = copy.deepcopy(broker)
    out.pop("_createdAt", None)
    return _ok(out)


def _delete_broker(broker_id: str) -> tuple:
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    del _brokers[broker_id]
    _name_index.pop(broker["brokerName"], None)
    return _ok({"brokerId": broker_id})


def _update_broker(broker_id: str, body: dict) -> tuple:
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err

    engine_type = broker["engineType"]
    if "engineVersion" in body and body["engineVersion"] not in SUPPORTED_ENGINES[engine_type]["versions"]:
        return _err(400, "EngineVersion", "Engine version is invalid.")
    if "hostInstanceType" in body and body["hostInstanceType"] not in _valid_host_instance_types(engine_type):
        return _err(400, "HostInstanceType", "Host instance type is invalid.")

    field_map = {
        "authenticationStrategy": "authenticationStrategy",
        "autoMinorVersionUpgrade": "autoMinorVersionUpgrade",
        "configuration": "configuration",
        "engineVersion": "engineVersion",
        "hostInstanceType": "hostInstanceType",
        "ldapServerMetadata": "ldapServerMetadata",
        "logs": "logs",
        "maintenanceWindowStartTime": "maintenanceWindowStartTime",
        "securityGroups": "securityGroups",
        "dataReplicationMode": "pendingDataReplicationMode",
    }

    out = {"brokerId": broker_id}
    for src, dst in field_map.items():
        if src in body:
            broker[dst] = copy.deepcopy(body[src])
            out[dst] = copy.deepcopy(body[src])

    return _ok(out)


def _reboot_broker(broker_id: str) -> tuple:
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    if broker.get("brokerState") != "RUNNING":
        return _err(400, "BrokerState", "You can reboot only a broker with RUNNING status.")
    return _ok({})

def _list_broker_engine_types(query_params: dict) -> tuple:
    engine_type = query_params.get("engineType")
    if isinstance(engine_type, list):
        engine_type = engine_type[-1] if engine_type else None
    if engine_type is not None:
        engine_type = str(engine_type).upper()
        if engine_type not in SUPPORTED_ENGINES:
            return _err(400, "EngineType", f"Invalid engine type: '{engine_type}'.")

    max_results, max_err = _parse_max_results(query_params, default=20, minimum=5, maximum=100, reject={4})
    if max_err:
        return max_err
    offset, token_err = _parse_next_token(query_params)
    if token_err:
        return token_err

    items = []
    for eng, cfg in SUPPORTED_ENGINES.items():
        if engine_type and eng != engine_type:
            continue
        items.append({"engineType": eng, "engineVersions": [{"name": v} for v in cfg["versions"]]})

    page, next_token = _paginate(items, offset, max_results)
    out = {"brokerEngineTypes": page, "maxResults": max_results}
    if next_token is not None:
        out["nextToken"] = next_token
    return _ok(out)

def _list_broker_instance_options(query_params: dict) -> tuple:
    max_results, max_err = _parse_max_results(query_params, default=20, minimum=1, maximum=100, reject={4})
    if max_err:
        return max_err
    offset, token_err = _parse_next_token(query_params)
    if token_err:
        return token_err

    engine_type = query_params.get("engineType")
    if isinstance(engine_type, list):
        engine_type = engine_type[-1] if engine_type else None
    engine_type = str(engine_type).upper() if engine_type else None
    if engine_type and engine_type not in SUPPORTED_ENGINES:
        return _err(400, "EngineType", f"Invalid engine type: '{engine_type}'.")

    host_instance_type = query_params.get("hostInstanceType")
    if isinstance(host_instance_type, list):
        host_instance_type = host_instance_type[-1] if host_instance_type else None
    if host_instance_type and host_instance_type not in _valid_host_instance_types(engine_type):
        return _err(400, "HostInstanceType", f"Invalid host instance type: '{host_instance_type}'.")

    storage_type = query_params.get("storageType")
    if isinstance(storage_type, list):
        storage_type = storage_type[-1] if storage_type else None
    storage_type = str(storage_type).upper() if storage_type else None
    if storage_type and storage_type not in _valid_storage_types(engine_type):
        return _err(400, "StorageType", f"Invalid storage type: '{storage_type}'.")

    filtered = []
    for eng, cfg in SUPPORTED_ENGINES.items():
        if engine_type and eng != engine_type:
            continue
        for host in cfg["host_instance_types"]:
            if host_instance_type and host != host_instance_type:
                continue
            for stor in cfg["storage_types"]:
                if storage_type and stor != storage_type:
                    continue
                filtered.append(
                    {
                        "availabilityZones": [{"name": "us-east-1a"}, {"name": "us-east-1b"}],
                        "engineType": eng,
                        "hostInstanceType": host,
                        "storageType": stor,
                        "supportedEngineVersions": [{"name": v} for v in cfg["versions"]],
                        "supportedDeploymentModes": list(cfg["deployment_modes"]),
                    }
                )

    page, next_token = _paginate(filtered, offset, max_results)
    out = {"brokerInstanceOptions": page, "maxResults": max_results}
    if next_token is not None:
        out["nextToken"] = next_token
    return _ok(out)

def _list_tags(resource_arn: str) -> tuple:
    if not _resource_exists(resource_arn):
        return _err(404, "ResourceArn", f"Resource '{resource_arn}' does not exist.")
    return _ok({"tags": dict(_tags.get(resource_arn, {}))})


def _create_tags(resource_arn: str, body: dict) -> tuple:
    if not _resource_exists(resource_arn):
        return _err(404, "ResourceArn", f"Resource '{resource_arn}' does not exist.")
    tags = body.get("tags") if isinstance(body, dict) else None
    if not isinstance(tags, dict):
        return _err(400, "Tags", "tags must be an object.")
    _tags.setdefault(resource_arn, {}).update({str(k): str(v) for k, v in tags.items()})
    return _no_content()


def _delete_tags(resource_arn: str, query_params: dict) -> tuple:
    if not _resource_exists(resource_arn):
        return _err(404, "ResourceArn", f"Resource '{resource_arn}' does not exist.")
    tag_keys = query_params.get("tagKeys")
    if tag_keys is None:
        return _err(400, "TagKeys", "tagKeys is required.")
    if isinstance(tag_keys, str):
        tag_keys = [tag_keys]
    tags = _tags.setdefault(resource_arn, {})
    for key in tag_keys:
        tags.pop(str(key), None)
    return _no_content()

def _create_user(broker_id: str, username: str, body: dict) -> tuple:
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    engine_err = _ensure_activemq_broker(broker)
    if engine_err:
        return engine_err

    users_map = _users.setdefault(broker_id, {})
    if username in users_map:
        return _err(409, "Username", f"User '{username}' already exists.")

    password = str(body.get("password", ""))
    pw_err = _validate_password(password)
    if pw_err:
        return pw_err

    users_map[username] = {
        "username": username,
        "password": password,
        "consoleAccess": bool(body.get("consoleAccess", False)),
        "groups": list(body.get("groups") or []),
        "replicationUser": bool(body.get("replicationUser", False)),
        "_createdAt": time.time_ns(),
    }
    return _ok({})


def _delete_user(broker_id: str, username: str) -> tuple:
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    engine_err = _ensure_activemq_broker(broker)
    if engine_err:
        return engine_err

    users_map = _users.setdefault(broker_id, {})
    if username not in users_map:
        return _err(404, "Username", f"User '{username}' does not exist.")

    del users_map[username]
    return _ok({})


def _list_users(broker_id: str, query_params: dict) -> tuple:
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    engine_err = _ensure_activemq_broker(broker)
    if engine_err:
        return engine_err

    max_results, max_err = _parse_max_results(query_params, default=20, minimum=1, maximum=100, reject={4})
    if max_err:
        return max_err
    offset, token_err = _parse_next_token(query_params)
    if token_err:
        return token_err

    users_map = _users.setdefault(broker_id, {})
    users_list = sorted(
        [
            {
                "username": u["username"],
                "consoleAccess": bool(u.get("consoleAccess", False)),
                "groups": list(u.get("groups", [])),
                "replicationUser": bool(u.get("replicationUser", False)),
                "_createdAt": u.get("_createdAt", 0),
            }
            for u in users_map.values()
        ],
        key=lambda x: x["_createdAt"],
        reverse=True,
    )
    for row in users_list:
        row.pop("_createdAt", None)

    page, next_token = _paginate(users_list, offset, max_results)
    out = {"brokerId": broker_id, "maxResults": max_results, "users": page}
    if next_token is not None:
        out["nextToken"] = next_token
    return _ok(out)


def _update_user(broker_id: str, username: str, body: dict) -> tuple:
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    engine_err = _ensure_activemq_broker(broker)
    if engine_err:
        return engine_err

    users_map = _users.setdefault(broker_id, {})
    user = users_map.get(username)
    if user is None:
        return _err(404, "Username", f"User '{username}' does not exist.")

    if "password" in body:
        pw_err = _validate_password(str(body.get("password", "")))
        if pw_err:
            return pw_err
        user["password"] = str(body["password"])

    if "consoleAccess" in body:
        user["consoleAccess"] = bool(body["consoleAccess"])
    if "groups" in body:
        user["groups"] = list(body.get("groups") or [])
    if "replicationUser" in body:
        user["replicationUser"] = bool(body["replicationUser"])

    return _ok({})

_BROKER_ID_RE = re.compile(r"^/v1/brokers/([^/]+)$")
_BROKER_REBOOT_RE = re.compile(r"^/v1/brokers/([^/]+)/reboot$")
_BROKER_USERS_RE = re.compile(r"^/v1/brokers/([^/]+)/users$")
_BROKER_USER_RE = re.compile(r"^/v1/brokers/([^/]+)/users/([^/]+)$")
_TAGS_RE = re.compile(r"^/v1/tags/(.+)$")


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    method = method.upper()

    if path == "/v1/broker-instance-options" and method == "GET":
        return _list_broker_instance_options(query_params)

    if path == "/v1/broker-engine-types" and method == "GET":
        return _list_broker_engine_types(query_params)

    m = _TAGS_RE.match(path)
    if m:
        resource_arn = unquote(m.group(1))
        if method == "GET":
            return _list_tags(resource_arn)
        if method == "POST":
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                return _err(400, "RequestBody", "Invalid JSON in request body.")
            return _create_tags(resource_arn, payload)
        if method == "DELETE":
            return _delete_tags(resource_arn, query_params)

    m = _BROKER_USERS_RE.match(path)
    if m and method == "GET":
        return _list_users(m.group(1), query_params)

    m = _BROKER_USER_RE.match(path)
    if m:
        broker_id, username = m.group(1), unquote(m.group(2))
        if method == "POST":
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                return _err(400, "RequestBody", "Invalid JSON in request body.")
            return _create_user(broker_id, username, payload)
        if method == "DELETE":
            return _delete_user(broker_id, username)
        if method == "PUT":
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                return _err(400, "RequestBody", "Invalid JSON in request body.")
            return _update_user(broker_id, username, payload)

    if method == "POST" and path == "/v1/brokers":
        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            return _err(400, "RequestBody", "Invalid JSON in request body.")
        return _create_broker(payload)

    if method == "GET" and path == "/v1/brokers":
        return _list_brokers(query_params)

    m = _BROKER_REBOOT_RE.match(path)
    if m and method == "POST":
        return _reboot_broker(m.group(1))

    m = _BROKER_ID_RE.match(path)
    if m:
        broker_id = m.group(1)
        if method == "GET":
            return _describe_broker(broker_id)
        if method == "PUT":
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                return _err(400, "RequestBody", "Invalid JSON in request body.")
            return _update_broker(broker_id, payload)
        if method == "DELETE":
            return _delete_broker(broker_id)

    return _err(400, "Action", f"Unknown action: {method} {path}")


def reset() -> None:
    _brokers._data.clear()
    _name_index._data.clear()
    _tags._data.clear()
    _users._data.clear()
