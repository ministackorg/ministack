"""AmazonMQ Service Emulator.

This module provides a mock implementation of AWS MQ (Message Broker) service,
supporting both RabbitMQ and ActiveMQ engines with configurable deployment modes,
host instance types, and storage backends.

Key Components:
- Broker lifecycle management (create, describe, update, delete, reboot)
- User management for ActiveMQ brokers
- Tag management for brokers
- Broker metadata and configuration queries
- Persistence of broker state across service restarts

State Management:
- _brokers: Maps broker_id → broker configuration dict
- _name_index: Maps broker_name → broker_id (prevents duplicate names)
- _tags: Maps broker_arn → tags dict
- _users: Maps broker_id → {username → user_config} (ActiveMQ only)

All state is persisted to disk via ministack.core.persistence.
"""

import copy
import json
import logging
import re
import time
from urllib.parse import unquote

from ministack.core.persistence import load_state
from ministack.core.responses import AccountScopedDict, get_account_id, get_region, new_uuid

logger = logging.getLogger("mq")


# ============================================================================
# Module State
# ============================================================================
# These module-level dicts are scoped per AWS account/region via AccountScopedDict.
# Invariants to maintain:
#   - _name_index[name] must exist iff _brokers[id].brokerName == name
#   - _tags[arn] exists iff _brokers[id] with matching arn exists
#   - _users[id] exists iff _brokers[id].engineType == "ACTIVEMQ"

_brokers: AccountScopedDict = AccountScopedDict()
_name_index: AccountScopedDict = AccountScopedDict()
_tags: AccountScopedDict = AccountScopedDict()
_users: AccountScopedDict = AccountScopedDict()


# ============================================================================
# Configuration & Supported Engines
# ============================================================================

SUPPORTED_ENGINES = {
    "RABBITMQ": {
        "storage_types": ["EBS"],
        "deployment_modes": ["SINGLE_INSTANCE", "CLUSTER_MULTI_AZ"],
        "versions": ["4.2", "3.13"],
        # output of
        # aws mq describe-broker-instance-options \
        #   --engine-type RABBITMQ \
        #   --query 'BrokerInstanceOptions[].HostInstanceType' \
        #   --output json
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
        # output of
        # aws mq describe-broker-instance-options \
        #   --engine-type ACTIVEMQ \
        #   --query 'BrokerInstanceOptions[].HostInstanceType' \
        #   --output json
        "host_instance_types": [
            "mq.m5.2xlarge", "mq.m5.4xlarge", "mq.m5.large", "mq.m5.xlarge", "mq.t3.micro"
        ],
    },
}


# ============================================================================
# HTTP & Error Constants
# ============================================================================

HTTP_STATUS_TO_EXCEPTION = {
    400: "BadRequestException",
    403: "ForbiddenException",
    404: "NotFoundException",
    409: "ConflictException",
    500: "InternalServerErrorException",
}

HTTP_JSON_HEADERS = {"Content-Type": "application/json"}


# ============================================================================
# Error Messages
# ============================================================================
# Centralized error messages to prevent duplication and improve consistency.

ERROR_MESSAGES = {
    # Broker name validation
    "BROKER_NAME_REQUIRED": "brokerName is required.",
    "BROKER_NAME_INVALID": "brokerName is invalid.",
    "BROKER_NAME_EXISTS": "A broker with the name '{}' already exists.",
    "BROKER_NOT_FOUND": "Broker '{}' does not exist.",

    # Engine configuration
    "ENGINE_TYPE_UNSUPPORTED": "Unsupported engine type '{}'.",
    "ENGINE_VERSION_INVALID": "Engine version is invalid.",
    "DEPLOYMENT_MODE_INVALID": "Deployment mode is invalid.",

    # Instance & storage types
    "HOST_INSTANCE_TYPE_INVALID": "Host instance type is invalid.",
    "STORAGE_TYPE_INVALID": "Invalid storage type: '{}'.",

    # User management
    "USER_ALREADY_EXISTS": "User '{}' already exists.",
    "USER_NOT_FOUND": "User '{}' does not exist.",
    "PASSWORD_INVALID": "Password must be at least 4 characters and cannot contain ',', ':' or '='.",
    "ACTIVEMQ_ONLY_OP": "This operation is supported only for ActiveMQ brokers.",

    # Broker state
    "BROKER_NOT_RUNNING": "You can reboot only a broker with RUNNING status.",

    # Tagging
    "RESOURCE_NOT_FOUND": "Resource '{}' does not exist.",
    "TAG_KEYS_REQUIRED": "tagKeys is required.",

    # Pagination
    "MAX_RESULTS_INVALID": "maxResults must be an integer from 5 to 100.",
    "NEXT_TOKEN_INVALID": "nextToken is invalid.",

    # Request parsing
    "JSON_INVALID": "Invalid JSON in request body.",
    "TAGS_INVALID": "tags must be an object.",

    # Generic
    "ACTION_UNKNOWN": "Unknown action: {} {}",
}


# ============================================================================
# Validation Patterns (Regex)
# ============================================================================
# All regex patterns with documentation for their purpose.

# Broker name: 1-50 alphanumeric characters, hyphens, underscores
BROKER_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,50}$")

# Broker ID extraction from path: /v1/brokers/{id}
BROKER_ID_PATH_PATTERN = re.compile(r"^/v1/brokers/([^/]+)$")

# Broker reboot from path: /v1/brokers/{id}/reboot
BROKER_REBOOT_PATH_PATTERN = re.compile(r"^/v1/brokers/([^/]+)/reboot$")

# Users list from path: /v1/brokers/{id}/users
BROKER_USERS_PATH_PATTERN = re.compile(r"^/v1/brokers/([^/]+)/users$")

# User operations from path: /v1/brokers/{id}/users/{username}
BROKER_USER_PATH_PATTERN = re.compile(r"^/v1/brokers/([^/]+)/users/([^/]+)$")

# Tag operations from path: /v1/tags/{arn}
TAGS_PATH_PATTERN = re.compile(r"^/v1/tags/(.+)$")

# Password: cannot contain comma, colon, or equals (AWS MQ limitation)
INVALID_PASSWORD_CHARS_PATTERN = re.compile(r"[,:=]")


# ============================================================================
# Service Implementation
# ============================================================================

# ============================================================================
# Validation Helpers
# ============================================================================
# Centralized validation functions to reduce code duplication and improve
# testability. All validation functions return None on success or an error
# tuple (status, headers, body) on failure.


def _validate_broker_config(body: dict) -> tuple | None:
    """Validate all required broker configuration fields.

    Returns None if valid, or error tuple if invalid.
    """
    engine_type = str(body.get("engineType", "")).strip().upper()
    if engine_type not in SUPPORTED_ENGINES:
        return _err(400, "EngineType", ERROR_MESSAGES["ENGINE_TYPE_UNSUPPORTED"].format(engine_type))

    broker_name = str(body.get("brokerName", "")).strip()
    if not broker_name:
        return _err(400, "BrokerName", ERROR_MESSAGES["BROKER_NAME_REQUIRED"])
    if not BROKER_NAME_PATTERN.fullmatch(broker_name):
        return _err(400, "BrokerName", ERROR_MESSAGES["BROKER_NAME_INVALID"])
    if broker_name in _name_index:
        return _err(409, "BrokerName", ERROR_MESSAGES["BROKER_NAME_EXISTS"].format(broker_name))

    engine_versions = SUPPORTED_ENGINES[engine_type]["versions"]
    engine_version = body.get("engineVersion") or engine_versions[0]
    if engine_version not in engine_versions:
        return _err(400, "EngineVersion", ERROR_MESSAGES["ENGINE_VERSION_INVALID"])

    deployment_mode = str(body.get("deploymentMode", "SINGLE_INSTANCE")).upper()
    if deployment_mode not in SUPPORTED_ENGINES[engine_type]["deployment_modes"]:
        return _err(400, "DeploymentMode", ERROR_MESSAGES["DEPLOYMENT_MODE_INVALID"])

    host_instance_type = body.get("hostInstanceType", "mq.m5.large")
    if host_instance_type not in _valid_host_instance_types(engine_type):
        return _err(400, "HostInstanceType", ERROR_MESSAGES["HOST_INSTANCE_TYPE_INVALID"])

    storage_type = body.get("storageType", "EBS")
    if storage_type and storage_type not in _valid_storage_types(engine_type):
        return _err(400, "StorageType", ERROR_MESSAGES["STORAGE_TYPE_INVALID"].format(storage_type))

    return None


def _validate_broker_update_fields(broker: dict, body: dict) -> tuple | None:
    """Validate broker update fields.

    Returns None if valid, or error tuple if invalid.
    Only validates fields that are actually being updated.
    """
    engine_type = broker["engineType"]

    if "engineVersion" in body:
        engine_versions = SUPPORTED_ENGINES[engine_type]["versions"]
        if body["engineVersion"] not in engine_versions:
            return _err(400, "EngineVersion", ERROR_MESSAGES["ENGINE_VERSION_INVALID"])

    if "hostInstanceType" in body:
        if body["hostInstanceType"] not in _valid_host_instance_types(engine_type):
            return _err(400, "HostInstanceType", ERROR_MESSAGES["HOST_INSTANCE_TYPE_INVALID"])

    return None


def _validate_query_parameter_filter(
    engine_type: str | None,
    host_instance_type: str | None,
    storage_type: str | None,
) -> tuple | None:
    """Validate query parameters for broker instance options filtering.

    Returns None if valid, or error tuple if invalid.
    """
    if engine_type and engine_type not in SUPPORTED_ENGINES:
        return _err(400, "EngineType", ERROR_MESSAGES["ENGINE_TYPE_UNSUPPORTED"].format(engine_type))

    if host_instance_type and host_instance_type not in _valid_host_instance_types(engine_type):
        return _err(400, "HostInstanceType", ERROR_MESSAGES["HOST_INSTANCE_TYPE_INVALID"])

    if storage_type and storage_type not in _valid_storage_types(engine_type):
        return _err(400, "StorageType", ERROR_MESSAGES["STORAGE_TYPE_INVALID"].format(storage_type))

    return None


# ============================================================================
# State & Persistence
# ============================================================================


def get_state() -> dict:
    """Retrieve current broker state (used for persistence)."""
    return {
        "brokers": copy.deepcopy(_brokers),
        "name_index": copy.deepcopy(_name_index),
        "tags": copy.deepcopy(_tags),
        "users": copy.deepcopy(_users),
    }


def restore_state(data: dict) -> None:
    if not data:
        return
    _brokers.update(data.get("brokers", {}))
    _name_index.update(data.get("name_index", {}))
    _tags.update(data.get("tags", {}))
    _users.update(data.get("users", {}))


try:
    _restored = load_state("mq")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore mq state; starting fresh")


# ============================================================================
# Response Builders
# ============================================================================


def _ok(data: dict) -> tuple:
    """Return 200 OK with JSON-encoded response body."""
    return 200, dict(HTTP_JSON_HEADERS), json.dumps(data, ensure_ascii=False).encode("utf-8")


def _no_content() -> tuple:
    """Return 204 No Content."""
    return 204, {}, b""


def _err(http_status: int, error_attribute: str, message: str) -> tuple:
    """Return error response with appropriate HTTP status and AWS error format."""
    exc_type = HTTP_STATUS_TO_EXCEPTION.get(http_status, "BadRequestException")
    body = json.dumps(
        {"errorAttribute": error_attribute, "message": message, "__type": exc_type},
        ensure_ascii=False,
    ).encode("utf-8")
    return http_status, {**HTTP_JSON_HEADERS, "x-amzn-errortype": exc_type}, body


# ============================================================================
# Broker Metadata & Utilities
# ============================================================================


def _make_broker_arn(broker_id: str) -> str:
    """Construct an ARN for a broker given its ID."""
    return f"arn:aws:mq:{get_region()}:{get_account_id()}:broker:{broker_id}"


def _valid_host_instance_types(engine_type: str | None) -> set[str]:
    """Get set of valid host instance types for an engine type."""
    if engine_type:
        return set(SUPPORTED_ENGINES.get(engine_type, {}).get("host_instance_types", []))
    out = set()
    for cfg in SUPPORTED_ENGINES.values():
        out.update(cfg["host_instance_types"])
    return out

def _valid_storage_types(engine_type: str | None) -> set[str]:
    """Get set of valid storage types for an engine type."""
    if engine_type:
        return set(SUPPORTED_ENGINES.get(engine_type, {}).get("storage_types", []))
    out = set()
    for cfg in SUPPORTED_ENGINES.values():
        out.update(cfg["storage_types"])
    return out


def _parse_max_results(query_params: dict, *, default: int = 20):
    """Parse and validate maxResults pagination parameter."""
    raw = query_params.get("maxResults")
    if not raw:
        return default, None
    if isinstance(raw, list):
        raw = raw[-1] if raw else None
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return None, _err(400, "MaxResults", ERROR_MESSAGES["MAX_RESULTS_INVALID"])
    if value < 5 or value > 100:
        return None, _err(400, "MaxResults", ERROR_MESSAGES["MAX_RESULTS_INVALID"])
    return value, None

# ============================================================================
# Pagination & Query Parsing
# ============================================================================


def _parse_next_token(query_params: dict):
    """Parse and validate nextToken pagination parameter."""
    raw = query_params.get("nextToken")
    if not raw:
        return 0, None
    if isinstance(raw, list):
        raw = raw[-1] if raw else None
    try:
        offset = int(raw)
    except (TypeError, ValueError):
        return 0, _err(400, "NextToken", ERROR_MESSAGES["NEXT_TOKEN_INVALID"])
    if offset < 0:
        return 0, _err(400, "NextToken", ERROR_MESSAGES["NEXT_TOKEN_INVALID"])
    return offset, None


def _paginate(items: list, offset: int, max_results: int):
    """Paginate a list of items returning page and optional nextToken."""
    page = items[offset : offset + max_results]
    next_token = str(offset + max_results) if (offset + max_results) < len(items) else None
    return page, next_token


# ============================================================================
# Broker Lookup & Guards
# ============================================================================


def _resource_exists(resource_arn: str) -> bool:
    """Check if a broker with given ARN exists."""
    return any(b.get("brokerArn") == resource_arn for b in _brokers.values())


def _get_broker_or_404(broker_id: str):
    """Retrieve broker or return 404 error tuple."""
    broker = _brokers.get(broker_id)
    if not broker:
        return None, _err(404, "BrokerId", ERROR_MESSAGES["BROKER_NOT_FOUND"].format(broker_id))
    return broker, None


# ============================================================================
# Broker Guard Functions
# ============================================================================


def _ensure_activemq_broker(broker: dict):
    """Verify broker is ActiveMQ engine type; return error if not."""
    if broker.get("engineType") != "ACTIVEMQ":
        return _err(400, "BrokerId", ERROR_MESSAGES["ACTIVEMQ_ONLY_OP"])
    return None

def _validate_password(password: str):
    """Validate password meets AWS MQ requirements."""
    if len(password) < 4 or INVALID_PASSWORD_CHARS_PATTERN.search(password):
        return _err(400, "Password", ERROR_MESSAGES["PASSWORD_INVALID"])
    return None


# ============================================================================
# Broker CRUD Operations
# ============================================================================


def _create_broker(body: dict) -> tuple:
    """Create a new MQ broker with specified configuration."""
    # Validate all configuration first
    config_err = _validate_broker_config(body)
    if config_err:
        return config_err

    # Extract validated configuration
    engine_type = str(body.get("engineType", "")).strip().upper()
    broker_name = str(body.get("brokerName", "")).strip()
    engine_versions = SUPPORTED_ENGINES[engine_type]["versions"]
    engine_version = body.get("engineVersion") or engine_versions[0]
    deployment_mode = str(body.get("deploymentMode", "SINGLE_INSTANCE")).upper()
    host_instance_type = body.get("hostInstanceType", "mq.m5.large")
    storage_type = body.get("storageType", "EBS")

    # Create broker in state
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
    _tags[broker_arn] = dict(body.get("tags") or {})
    _users[broker_id] = {}

    return _ok({"brokerId": broker_id, "brokerArn": broker_arn})


def _list_brokers(query_params: dict) -> tuple:
    """List all brokers with pagination support."""
    max_results, max_err = _parse_max_results(query_params)
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
    if next_token:
        out["nextToken"] = next_token
    return _ok(out)


def _describe_broker(broker_id: str) -> tuple:
    """Describe a specific broker in detail."""
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    out = copy.deepcopy(broker)
    out.pop("_createdAt", None)
    out["tags"] = dict(_tags.get(broker["brokerArn"], {}))
    return _ok(out)


def _delete_broker(broker_id: str) -> tuple:
    """Delete a broker and all associated data."""
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    del _brokers[broker_id]
    _name_index.pop(broker["brokerName"], None)
    _tags.pop(broker["brokerArn"], None)
    _users.pop(broker_id, None)
    return _ok({"brokerId": broker_id})


def _update_broker(broker_id: str, body: dict) -> tuple:
    """Update broker configuration fields."""
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err

    # Validate update fields
    validation_err = _validate_broker_update_fields(broker, body)
    if validation_err:
        return validation_err

    # Apply updates
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
    """Reboot a broker (soft restart)."""
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    if broker.get("brokerState") != "RUNNING":
        return _err(400, "BrokerState", ERROR_MESSAGES["BROKER_NOT_RUNNING"])
    return _ok({})

# ============================================================================
# Broker Metadata & Configuration Queries
# ============================================================================


def _list_broker_engine_types(query_params: dict) -> tuple:
    """List supported MQ broker engine types and versions."""
    # Parse engine type parameter
    engine_type = query_params.get("engineType")
    if isinstance(engine_type, list):
        engine_type = engine_type[-1] if engine_type else None
    if engine_type:
        engine_type = str(engine_type).upper()
        if engine_type not in SUPPORTED_ENGINES:
            return _err(400, "EngineType", ERROR_MESSAGES["ENGINE_TYPE_UNSUPPORTED"].format(engine_type))

    # Parse pagination parameters
    max_results, max_err = _parse_max_results(query_params)
    if max_err:
        return max_err
    offset, token_err = _parse_next_token(query_params)
    if token_err:
        return token_err

    # Build filtered engine list
    items = []
    for eng, cfg in SUPPORTED_ENGINES.items():
        if engine_type and eng != engine_type:
            continue
        items.append({"engineType": eng, "engineVersions": [{"name": v} for v in cfg["versions"]]})

    # Paginate and return
    page, next_token = _paginate(items, offset, max_results)
    out = {"brokerEngineTypes": page, "maxResults": max_results}
    if next_token:
        out["nextToken"] = next_token
    return _ok(out)

def _list_broker_instance_options(query_params: dict) -> tuple:
    """List supported instance and storage type combinations."""
    max_results, max_err = _parse_max_results(query_params)
    if max_err:
        return max_err
    offset, token_err = _parse_next_token(query_params)
    if token_err:
        return token_err

    # Parse and normalize filter parameters
    engine_type = query_params.get("engineType")
    if isinstance(engine_type, list):
        engine_type = engine_type[-1] if engine_type else None
    engine_type = str(engine_type).upper() if engine_type else None

    host_instance_type = query_params.get("hostInstanceType")
    if isinstance(host_instance_type, list):
        host_instance_type = host_instance_type[-1] if host_instance_type else None

    storage_type = query_params.get("storageType")
    if isinstance(storage_type, list):
        storage_type = storage_type[-1] if storage_type else None
    storage_type = str(storage_type).upper() if storage_type else None

    # Validate filters
    validation_err = _validate_query_parameter_filter(engine_type, host_instance_type, storage_type)
    if validation_err:
        return validation_err

    # Build filtered options list
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
    if next_token:
        out["nextToken"] = next_token
    return _ok(out)

# ============================================================================
# Tag Management
# ============================================================================


def _list_tags(resource_arn: str) -> tuple:
    """List all tags for a broker resource."""
    if not _resource_exists(resource_arn):
        return _err(404, "ResourceArn", ERROR_MESSAGES["RESOURCE_NOT_FOUND"].format(resource_arn))
    return _ok({"tags": dict(_tags.get(resource_arn, {}))})


def _create_tags(resource_arn: str, body: dict) -> tuple:
    """Add or update tags on a broker resource."""
    if not _resource_exists(resource_arn):
        return _err(404, "ResourceArn", ERROR_MESSAGES["RESOURCE_NOT_FOUND"].format(resource_arn))
    tags = body.get("tags") if isinstance(body, dict) else None
    if not isinstance(tags, dict):
        return _err(400, "Tags", ERROR_MESSAGES["TAGS_INVALID"])
    _tags.setdefault(resource_arn, {}).update({str(k): str(v) for k, v in tags.items()})
    return _no_content()


def _delete_tags(resource_arn: str, query_params: dict) -> tuple:
    """Delete specific tags from a broker resource."""
    if not _resource_exists(resource_arn):
        return _err(404, "ResourceArn", ERROR_MESSAGES["RESOURCE_NOT_FOUND"].format(resource_arn))
    tag_keys = query_params.get("tagKeys")
    if not tag_keys:
        return _err(400, "TagKeys", ERROR_MESSAGES["TAG_KEYS_REQUIRED"])
    if isinstance(tag_keys, str):
        tag_keys = [tag_keys]
    tags = _tags.setdefault(resource_arn, {})
    for key in tag_keys:
        tags.pop(str(key), None)
    return _no_content()

# ============================================================================
# User Management (ActiveMQ only)
# ============================================================================


def _create_user(broker_id: str, username: str, body: dict) -> tuple:
    """Create a user account on an ActiveMQ broker."""
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    engine_err = _ensure_activemq_broker(broker)
    if engine_err:
        return engine_err

    users_map = _users.setdefault(broker_id, {})
    if username in users_map:
        return _err(409, "Username", ERROR_MESSAGES["USER_ALREADY_EXISTS"].format(username))

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
    """Delete a user account from an ActiveMQ broker."""
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    engine_err = _ensure_activemq_broker(broker)
    if engine_err:
        return engine_err

    users_map = _users.setdefault(broker_id, {})
    if username not in users_map:
        return _err(404, "Username", ERROR_MESSAGES["USER_NOT_FOUND"].format(username))

    del users_map[username]
    return _ok({})


def _list_users(broker_id: str, query_params: dict) -> tuple:
    """List all users for an ActiveMQ broker."""
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    engine_err = _ensure_activemq_broker(broker)
    if engine_err:
        return engine_err

    max_results, max_err = _parse_max_results(query_params)
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
    if next_token:
        out["nextToken"] = next_token
    return _ok(out)


def _update_user(broker_id: str, username: str, body: dict) -> tuple:
    """Update a user account on an ActiveMQ broker."""
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err
    engine_err = _ensure_activemq_broker(broker)
    if engine_err:
        return engine_err

    users_map = _users.setdefault(broker_id, {})
    user = users_map.get(username)
    if not user:
        return _err(404, "Username", ERROR_MESSAGES["USER_NOT_FOUND"].format(username))

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

def _describe_user(broker_id: str, username: str) -> tuple:
    """Describe a specific user account on an ActiveMQ broker."""
    broker, err = _get_broker_or_404(broker_id)
    if err:
        return err

    engine_err = _ensure_activemq_broker(broker)
    if engine_err:
        return engine_err

    users_map = _users.setdefault(broker_id, {})
    user = users_map.get(username)
    if not user:
        return _err(404, "Username", ERROR_MESSAGES["USER_NOT_FOUND"].format(username))

    out = {
        "brokerId": broker_id,
        "consoleAccess": bool(user.get("consoleAccess", False)),
        "groups": list(user.get("groups", [])),
        "pending": {},
        "username": user["username"],
        "replicationUser": bool(user.get("replicationUser", False)),
    }
    return _ok(out)




# ============================================================================
# Request Routing & Handlers
# ============================================================================


async def handle_request(method: str, path: str, headers: dict, body: bytes, query_params: dict) -> tuple:
    """Main request handler that routes to appropriate API endpoint handler."""
    method = method.upper()

    if path == "/v1/broker-instance-options" and method == "GET":
        return _list_broker_instance_options(query_params)

    if path == "/v1/broker-engine-types" and method == "GET":
        return _list_broker_engine_types(query_params)

    m = TAGS_PATH_PATTERN.match(path)
    if m:
        resource_arn = unquote(m.group(1))
        if method == "GET":
            return _list_tags(resource_arn)
        if method == "POST":
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                return _err(400, "RequestBody", ERROR_MESSAGES["JSON_INVALID"])
            return _create_tags(resource_arn, payload)
        if method == "DELETE":
            return _delete_tags(resource_arn, query_params)

    m = BROKER_USERS_PATH_PATTERN.match(path)
    if m and method == "GET":
        return _list_users(m.group(1), query_params)

    m = BROKER_USER_PATH_PATTERN.match(path)
    if m:
        broker_id, username = m.group(1), unquote(m.group(2))
        if method == "POST":
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                return _err(400, "RequestBody", ERROR_MESSAGES["JSON_INVALID"])
            return _create_user(broker_id, username, payload)
        if method == "DELETE":
            return _delete_user(broker_id, username)
        if method == "PUT":
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                return _err(400, "RequestBody", ERROR_MESSAGES["JSON_INVALID"])
            return _update_user(broker_id, username, payload)
        if method == "GET":
            return _describe_user(broker_id, username)

    if method == "POST" and path == "/v1/brokers":
        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            return _err(400, "RequestBody", ERROR_MESSAGES["JSON_INVALID"])
        return _create_broker(payload)

    if method == "GET" and path == "/v1/brokers":
        return _list_brokers(query_params)

    m = BROKER_REBOOT_PATH_PATTERN.match(path)
    if m and method == "POST":
        return _reboot_broker(m.group(1))

    m = BROKER_ID_PATH_PATTERN.match(path)
    if m:
        broker_id = m.group(1)
        if method == "GET":
            return _describe_broker(broker_id)
        if method == "PUT":
            try:
                payload = json.loads(body) if body else {}
            except json.JSONDecodeError:
                return _err(400, "RequestBody", ERROR_MESSAGES["JSON_INVALID"])
            return _update_broker(broker_id, payload)
        if method == "DELETE":
            return _delete_broker(broker_id)

    return _err(400, "Action", ERROR_MESSAGES["ACTION_UNKNOWN"].format(method, path))


def reset() -> None:
    """Clear all MQ service state (used for testing)."""
    _brokers.clear()
    _name_index.clear()
    _tags.clear()
    _users.clear()
