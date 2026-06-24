"""
Amazon MSK (Managed Streaming for Apache Kafka) Service Emulator.
REST/JSON protocol — signing name: kafka. Endpoint prefix: kafka.

Operations (verified against botocore kafka-2018-11-14):
  Clusters:
    POST   /v1/clusters                                   CreateCluster
    GET    /v1/clusters                                   ListClusters
    GET    /v1/clusters/{clusterArn}                      DescribeCluster
    DELETE /v1/clusters/{clusterArn}                      DeleteCluster
    GET    /v1/clusters/{clusterArn}/bootstrap-brokers    GetBootstrapBrokers
    GET    /v1/clusters/{clusterArn}/nodes                ListNodes
  Configurations:
    POST   /v1/configurations                             CreateConfiguration
    GET    /v1/configurations                             ListConfigurations
    GET    /v1/configurations/{arn}                       DescribeConfiguration
    GET    /v1/configurations/{arn}/revisions             ListConfigurationRevisions
    GET    /v1/configurations/{arn}/revisions/{revision}  DescribeConfigurationRevision
  SCRAM:
    POST   /v1/clusters/{clusterArn}/scram-secrets        BatchAssociateScramSecret
    PATCH  /v1/clusters/{clusterArn}/scram-secrets        BatchDisassociateScramSecret
    GET    /v1/clusters/{clusterArn}/scram-secrets        ListScramSecrets
  Tags:
    POST   /v1/tags/{resourceArn}                         TagResource
    DELETE /v1/tags/{resourceArn}                         UntagResource
    GET    /v1/tags/{resourceArn}                         ListTagsForResource

Data plane (Kafka wire protocol) is not emulated. GetBootstrapBrokers honors
MINISTACK_MSK_BOOTSTRAP — when set, clients route directly to that broker
(Redpanda, real Kafka, KRaft Kafka). Unset → placeholder endpoint that fails
to connect with a clear error, control-plane tests still work.
"""

import base64
import copy
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.persistence import load_state
from ministack.core.responses import (
    AccountRegionScopedDict,
    get_account_id,
    get_region,
)

logger = logging.getLogger("msk")

_BOOTSTRAP_PLAIN = os.environ.get("MINISTACK_MSK_BOOTSTRAP", "")
_BOOTSTRAP_TLS = os.environ.get("MINISTACK_MSK_BOOTSTRAP_TLS", "")
_BOOTSTRAP_SASL_SCRAM = os.environ.get("MINISTACK_MSK_BOOTSTRAP_SASL_SCRAM", "")
_BOOTSTRAP_SASL_IAM = os.environ.get("MINISTACK_MSK_BOOTSTRAP_SASL_IAM", "")

# ---------------------------------------------------------------------------
# State (region-scoped via AccountRegionScopedDict)
# ---------------------------------------------------------------------------

_clusters = AccountRegionScopedDict()         # clusterArn -> ClusterInfo dict
_configurations = AccountRegionScopedDict()   # configArn -> Configuration dict
_config_revisions = AccountRegionScopedDict() # configArn -> [revision dicts]
_scram_secrets = AccountRegionScopedDict()    # clusterArn -> [secretArn]
_tags = AccountRegionScopedDict()             # resourceArn -> {key: value}


def reset():
    _clusters.clear()
    _configurations.clear()
    _config_revisions.clear()
    _scram_secrets.clear()
    _tags.clear()


def get_state():
    return copy.deepcopy({
        "clusters": _clusters,
        "configurations": _configurations,
        "config_revisions": _config_revisions,
        "scram_secrets": _scram_secrets,
        "tags": _tags,
    })


def restore_state(data):
    if not data:
        return
    _clusters.update(data.get("clusters", {}))
    _configurations.update(data.get("configurations", {}))
    _config_revisions.update(data.get("config_revisions", {}))
    _scram_secrets.update(data.get("scram_secrets", {}))
    _tags.update(data.get("tags", {}))


try:
    _restored = load_state("msk")
    if _restored:
        restore_state(_restored)
except Exception:
    logger.exception("Failed to restore msk state; continuing fresh")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def _to_camel(key: str) -> str:
    """PascalCase → camelCase for a single key. Single-acronym keys (ARN, TLS)
    aren't used inside MSK shapes, so the simple lowercase-first transform
    matches every botocore locationName for kafka-2018-11-14."""
    if not key:
        return key
    return key[0].lower() + key[1:]


def _camelize(obj):
    """Recursively convert all dict keys from PascalCase to camelCase. Leaves
    lists and scalars untouched."""
    if isinstance(obj, dict):
        return {_to_camel(k): _camelize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_camelize(v) for v in obj]
    return obj


def _json(payload: dict, status: int = 200) -> tuple:
    return status, {"Content-Type": "application/json"}, json.dumps(_camelize(payload)).encode()


def _error(code: str, message: str, status: int) -> tuple:
    body = json.dumps({
        "InvalidParameter": None,
        "Message": message,
        "__type": code,
    }).encode()
    return status, {"Content-Type": "application/json"}, body


def _not_found(message: str) -> tuple:
    return _error("NotFoundException", message, 404)


def _bad_request(message: str) -> tuple:
    return _error("BadRequestException", message, 400)


def _conflict(message: str) -> tuple:
    return _error("ConflictException", message, 409)


# ---------------------------------------------------------------------------
# ARN + ID helpers
# ---------------------------------------------------------------------------


def _cluster_arn(name: str, cluster_id: str) -> str:
    return f"arn:aws:kafka:{get_region()}:{get_account_id()}:cluster/{name}/{cluster_id}"


def _config_arn(config_id: str) -> str:
    return f"arn:aws:kafka:{get_region()}:{get_account_id()}:configuration/{config_id}"


def _validate_kafka_arn(arn: str, resource_type: str) -> tuple | None:
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return _bad_request(f"Invalid ARN: {arn}")
    if spec.service != "kafka":
        return _bad_request(f"Invalid ARN: {arn}")
    if spec.account_id != get_account_id() or spec.region != get_region():
        return _not_found(f"Resource {arn} not found.")

    if resource_type == "cluster":
        parts = spec.resource.split("/")
        if len(parts) != 3 or parts[0] != "cluster" or not parts[1] or not parts[2]:
            return _bad_request(f"Invalid cluster ARN: {arn}")
    elif resource_type == "configuration":
        parts = spec.resource.split("/")
        if len(parts) != 2 or parts[0] != "configuration" or not parts[1]:
            return _bad_request(f"Invalid configuration ARN: {arn}")
    else:
        return _bad_request(f"Invalid ARN: {arn}")

    return None


def _validate_existing_cluster_arn(arn: str) -> tuple | None:
    validation_error = _validate_kafka_arn(arn, "cluster")
    if validation_error:
        return validation_error
    if arn not in _clusters:
        return _not_found(f"Cluster {arn} not found.")
    return None


def _validate_existing_configuration_arn(arn: str) -> tuple | None:
    validation_error = _validate_kafka_arn(arn, "configuration")
    if validation_error:
        return validation_error
    if arn not in _configurations:
        return _not_found(f"Configuration {arn} not found.")
    return None


def _validate_existing_taggable_arn(arn: str) -> tuple | None:
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return _bad_request(f"Invalid ARN: {arn}")
    if spec.service != "kafka":
        return _bad_request(f"Invalid ARN: {arn}")

    if spec.resource.startswith("cluster/"):
        return _validate_existing_cluster_arn(arn)
    if spec.resource.startswith("configuration/"):
        return _validate_existing_configuration_arn(arn)
    return _bad_request(f"Invalid ARN: {arn}")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bootstrap_strings(cluster_name: str) -> dict:
    """Return per-auth-mode bootstrap broker strings.

    If MINISTACK_MSK_BOOTSTRAP* env vars are set, return them so Kafka clients
    connect to the real broker. Otherwise return a deterministic placeholder
    endpoint — connections will fail, signaling data plane is not emulated.
    """
    placeholder = f"b-1.{cluster_name}.ministack.local:9092"
    return {
        "BootstrapBrokerString": _BOOTSTRAP_PLAIN or placeholder,
        "BootstrapBrokerStringTls": _BOOTSTRAP_TLS or placeholder.replace(":9092", ":9094"),
        "BootstrapBrokerStringSaslScram": _BOOTSTRAP_SASL_SCRAM or placeholder.replace(":9092", ":9096"),
        "BootstrapBrokerStringSaslIam": _BOOTSTRAP_SASL_IAM or placeholder.replace(":9092", ":9098"),
        "BootstrapBrokerStringPublicTls": "",
        "BootstrapBrokerStringPublicSaslScram": "",
        "BootstrapBrokerStringPublicSaslIam": "",
        "BootstrapBrokerStringVpcConnectivityTls": "",
        "BootstrapBrokerStringVpcConnectivitySaslScram": "",
        "BootstrapBrokerStringVpcConnectivitySaslIam": "",
    }


# ---------------------------------------------------------------------------
# Cluster handlers
# ---------------------------------------------------------------------------


def _create_cluster(body) -> tuple:
    try:
        req = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _bad_request("Body is not valid JSON.")
    if not isinstance(req, dict):
        return _bad_request("Body must be a JSON object.")
    name = req.get("clusterName")
    if not name or not isinstance(name, str):
        return _bad_request("clusterName is required.")
    if not re.match(r"^[A-Za-z][A-Za-z0-9-]{0,63}$", name):
        return _bad_request("clusterName must match ^[A-Za-z][A-Za-z0-9-]{0,63}$.")
    # Name must be unique per account+region
    for existing in _clusters.values():
        if existing["ClusterName"] == name:
            return _conflict(f"Cluster {name} already exists.")
    cluster_id = uuid.uuid4().hex[:8]
    arn = _cluster_arn(name, cluster_id)
    now = _now_iso()
    info = {
        "ClusterArn": arn,
        "ClusterName": name,
        "CreationTime": now,
        "CurrentVersion": "K3AEGXETSR30VB",
        "State": "ACTIVE",
        "BrokerNodeGroupInfo": req.get("brokerNodeGroupInfo", {}),
        "ClientAuthentication": req.get("clientAuthentication", {}),
        "EncryptionInfo": req.get("encryptionInfo", {}),
        "EnhancedMonitoring": req.get("enhancedMonitoring", "DEFAULT"),
        "OpenMonitoring": req.get("openMonitoring", {}),
        "LoggingInfo": req.get("loggingInfo", {}),
        "NumberOfBrokerNodes": req.get("numberOfBrokerNodes", 3),
        "CurrentBrokerSoftwareInfo": {
            "KafkaVersion": (req.get("kafkaVersion") or "3.6.0"),
            "ConfigurationArn": (req.get("configurationInfo") or {}).get("arn"),
            "ConfigurationRevision": (req.get("configurationInfo") or {}).get("revision"),
        },
        "Tags": req.get("tags", {}),
        "ZookeeperConnectString": "",
        "ZookeeperConnectStringTls": "",
    }
    _clusters[arn] = info
    if info["Tags"]:
        _tags[arn] = dict(info["Tags"])
    return _json({
        "ClusterArn": arn,
        "ClusterName": name,
        "State": "ACTIVE",
    })


def _list_clusters(query_params) -> tuple:
    name_filter = (query_params.get("clusterNameFilter") or [None])[0]
    summaries = []
    for arn, info in _clusters.items():
        if name_filter and not info["ClusterName"].startswith(name_filter):
            continue
        summaries.append(info)
    return _json({
        "ClusterInfoList": summaries,
        "NextToken": None,
    })


def _describe_cluster(arn: str) -> tuple:
    validation_error = _validate_existing_cluster_arn(arn)
    if validation_error:
        return validation_error
    info = _clusters.get(arn)
    return _json({"ClusterInfo": info})


def _delete_cluster(arn: str) -> tuple:
    validation_error = _validate_existing_cluster_arn(arn)
    if validation_error:
        return validation_error
    info = _clusters.get(arn)
    _clusters.pop(arn, None)
    _scram_secrets.pop(arn, None)
    _tags.pop(arn, None)
    return _json({"ClusterArn": arn, "State": "DELETING"})


def _get_bootstrap_brokers(arn: str) -> tuple:
    validation_error = _validate_existing_cluster_arn(arn)
    if validation_error:
        return validation_error
    info = _clusters.get(arn)
    return _json(_bootstrap_strings(info["ClusterName"]))


def _list_nodes(arn: str) -> tuple:
    validation_error = _validate_existing_cluster_arn(arn)
    if validation_error:
        return validation_error
    info = _clusters.get(arn)
    n = info.get("NumberOfBrokerNodes", 3)
    bootstrap = _bootstrap_strings(info["ClusterName"])
    nodes = []
    for i in range(1, n + 1):
        nodes.append({
            "NodeARN": f"{arn}/node/{i}",
            "NodeType": "BROKER",
            "InstanceType": (info.get("BrokerNodeGroupInfo") or {}).get("instanceType", "kafka.m5.large"),
            "BrokerNodeInfo": {
                "BrokerId": float(i),
                "ClientSubnet": "",
                "ClientVpcIpAddress": "",
                "CurrentBrokerSoftwareInfo": info["CurrentBrokerSoftwareInfo"],
                "Endpoints": [bootstrap["BootstrapBrokerString"].split(",")[0]],
            },
        })
    return _json({"NodeInfoList": nodes, "NextToken": None})


# ---------------------------------------------------------------------------
# Configuration handlers
# ---------------------------------------------------------------------------


def _create_configuration(body) -> tuple:
    try:
        req = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _bad_request("Body is not valid JSON.")
    if not isinstance(req, dict):
        return _bad_request("Body must be a JSON object.")
    name = req.get("name")
    if not name:
        return _bad_request("name is required.")
    if "serverProperties" not in req:
        return _bad_request("serverProperties is required.")
    for existing in _configurations.values():
        if existing["Name"] == name:
            return _conflict(f"Configuration {name} already exists.")
    config_id = f"{uuid.uuid4().hex[:8]}-{uuid.uuid4().hex[:4]}-2"
    arn = _config_arn(config_id)
    now = _now_iso()
    revision = {
        "Revision": 1,
        "CreationTime": now,
        "Description": req.get("description", ""),
        "ServerProperties": req["serverProperties"],
    }
    config = {
        "Arn": arn,
        "CreationTime": now,
        "Description": req.get("description", ""),
        "KafkaVersions": req.get("kafkaVersions", []),
        "LatestRevision": {
            "CreationTime": now,
            "Description": revision["Description"],
            "Revision": 1,
        },
        "Name": name,
        "State": "ACTIVE",
    }
    _configurations[arn] = config
    _config_revisions[arn] = [revision]
    return _json({
        "Arn": arn,
        "CreationTime": now,
        "LatestRevision": config["LatestRevision"],
        "Name": name,
        "State": "ACTIVE",
    })


def _list_configurations(query_params) -> tuple:
    return _json({
        "Configurations": list(_configurations.values()),
        "NextToken": None,
    })


def _describe_configuration(arn: str) -> tuple:
    validation_error = _validate_existing_configuration_arn(arn)
    if validation_error:
        return validation_error
    cfg = _configurations.get(arn)
    return _json(cfg)


def _list_configuration_revisions(arn: str) -> tuple:
    validation_error = _validate_existing_configuration_arn(arn)
    if validation_error:
        return validation_error
    revs = _config_revisions.get(arn, [])
    summaries = [{
        "CreationTime": r["CreationTime"],
        "Description": r["Description"],
        "Revision": r["Revision"],
    } for r in revs]
    return _json({"Revisions": summaries, "NextToken": None})


def _describe_configuration_revision(arn: str, revision: str) -> tuple:
    validation_error = _validate_existing_configuration_arn(arn)
    if validation_error:
        return validation_error
    try:
        rev_n = int(revision)
    except ValueError:
        return _bad_request(f"Revision {revision} is not a valid integer.")
    for r in _config_revisions.get(arn, []):
        if r["Revision"] == rev_n:
            # ServerProperties is a blob on the wire — base64-encoded bytes
            sp = r["ServerProperties"]
            if isinstance(sp, bytes):
                sp = base64.b64encode(sp).decode()
            return _json({
                "Arn": arn,
                "CreationTime": r["CreationTime"],
                "Description": r["Description"],
                "Revision": r["Revision"],
                "ServerProperties": sp,
            })
    return _not_found(f"Configuration {arn} revision {revision} not found.")


# ---------------------------------------------------------------------------
# SCRAM secrets
# ---------------------------------------------------------------------------


def _batch_associate_scram(arn: str, body) -> tuple:
    validation_error = _validate_existing_cluster_arn(arn)
    if validation_error:
        return validation_error
    try:
        req = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _bad_request("Body is not valid JSON.")
    secrets = req.get("secretArnList", [])
    if not isinstance(secrets, list):
        return _bad_request("secretArnList must be an array.")
    current = list(_scram_secrets.get(arn, []))
    unprocessed = []
    for s in secrets:
        if s in current:
            unprocessed.append({"ErrorCode": "InvalidParameter",
                                 "ErrorMessage": "Secret already associated.",
                                 "SecretArn": s})
        else:
            current.append(s)
    _scram_secrets[arn] = current
    return _json({"ClusterArn": arn, "UnprocessedScramSecrets": unprocessed})


def _batch_disassociate_scram(arn: str, body) -> tuple:
    validation_error = _validate_existing_cluster_arn(arn)
    if validation_error:
        return validation_error
    try:
        req = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _bad_request("Body is not valid JSON.")
    secrets = req.get("secretArnList", [])
    current = list(_scram_secrets.get(arn, []))
    unprocessed = []
    for s in secrets:
        if s not in current:
            unprocessed.append({"ErrorCode": "InvalidParameter",
                                 "ErrorMessage": "Secret is not associated.",
                                 "SecretArn": s})
        else:
            current.remove(s)
    _scram_secrets[arn] = current
    return _json({"ClusterArn": arn, "UnprocessedScramSecrets": unprocessed})


def _list_scram_secrets(arn: str) -> tuple:
    validation_error = _validate_existing_cluster_arn(arn)
    if validation_error:
        return validation_error
    return _json({
        "SecretArnList": list(_scram_secrets.get(arn, [])),
        "NextToken": None,
    })


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------


def _tag_resource(arn: str, body) -> tuple:
    validation_error = _validate_existing_taggable_arn(arn)
    if validation_error:
        return validation_error
    try:
        req = json.loads(body or b"{}")
    except json.JSONDecodeError:
        return _bad_request("Body is not valid JSON.")
    tags = req.get("tags", {})
    if not isinstance(tags, dict):
        return _bad_request("tags must be an object.")
    current = dict(_tags.get(arn, {}))
    current.update(tags)
    _tags[arn] = current
    return 204, {"Content-Type": "application/json"}, b""


def _untag_resource(arn: str, query_params) -> tuple:
    validation_error = _validate_existing_taggable_arn(arn)
    if validation_error:
        return validation_error
    keys = query_params.get("tagKeys", []) if isinstance(query_params, dict) else []
    if isinstance(keys, str):
        keys = [keys]
    current = dict(_tags.get(arn, {}))
    for k in keys:
        current.pop(k, None)
    _tags[arn] = current
    return 204, {"Content-Type": "application/json"}, b""


def _list_tags_for_resource(arn: str) -> tuple:
    validation_error = _validate_existing_taggable_arn(arn)
    if validation_error:
        return validation_error
    # Tag keys/values are user data — pass through without camelization.
    return 200, {"Content-Type": "application/json"}, json.dumps({
        "tags": dict(_tags.get(arn, {})),
    }).encode()


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

# Path params hold an ARN, which itself contains slashes — non-greedy until
# the literal suffix anchor.
_CLUSTER_RE = re.compile(r"^/v1/clusters/(.+)$")
_BOOTSTRAP_RE = re.compile(r"^/v1/clusters/(.+?)/bootstrap-brokers$")
_NODES_RE = re.compile(r"^/v1/clusters/(.+?)/nodes$")
_SCRAM_RE = re.compile(r"^/v1/clusters/(.+?)/scram-secrets$")
_CONFIG_RE = re.compile(r"^/v1/configurations/(.+)$")
_CONFIG_REVS_RE = re.compile(r"^/v1/configurations/(.+?)/revisions$")
_CONFIG_REV_RE = re.compile(r"^/v1/configurations/(.+?)/revisions/([^/]+)$")
_TAGS_RE = re.compile(r"^/v1/tags/(.+)$")


async def handle_request(method, path, headers, body, query_params):
    logger.debug("%s %s", method, path)

    if path == "/v1/clusters":
        if method == "POST":
            return _create_cluster(body)
        if method == "GET":
            return _list_clusters(query_params)
        return _bad_request(f"Unsupported method {method} for /v1/clusters.")

    if path == "/v1/configurations":
        if method == "POST":
            return _create_configuration(body)
        if method == "GET":
            return _list_configurations(query_params)
        return _bad_request(f"Unsupported method {method} for /v1/configurations.")

    m = _BOOTSTRAP_RE.match(path)
    if m and method == "GET":
        return _get_bootstrap_brokers(unquote(m.group(1)))

    m = _NODES_RE.match(path)
    if m and method == "GET":
        return _list_nodes(unquote(m.group(1)))

    m = _SCRAM_RE.match(path)
    if m:
        arn = unquote(m.group(1))
        if method == "POST":
            return _batch_associate_scram(arn, body)
        if method == "PATCH":
            return _batch_disassociate_scram(arn, body)
        if method == "GET":
            return _list_scram_secrets(arn)

    m = _CONFIG_REV_RE.match(path)
    if m and method == "GET":
        return _describe_configuration_revision(unquote(m.group(1)), m.group(2))

    m = _CONFIG_REVS_RE.match(path)
    if m and method == "GET":
        return _list_configuration_revisions(unquote(m.group(1)))

    m = _CONFIG_RE.match(path)
    if m and method == "GET":
        return _describe_configuration(unquote(m.group(1)))

    m = _CLUSTER_RE.match(path)
    if m:
        arn = unquote(m.group(1))
        if method == "GET":
            return _describe_cluster(arn)
        if method == "DELETE":
            return _delete_cluster(arn)

    m = _TAGS_RE.match(path)
    if m:
        arn = unquote(m.group(1))
        if method == "POST":
            return _tag_resource(arn, body)
        if method == "DELETE":
            return _untag_resource(arn, query_params)
        if method == "GET":
            return _list_tags_for_resource(arn)

    return _bad_request(f"No route for {method} {path}.")
