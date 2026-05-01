"""
Amazon OpenSearch Service emulator.

Management plane: rest-json on /2021-01-01/* (botocore service-2.json).
Data plane: in-memory by default (stub endpoint), optional real
``opensearchproject/opensearch`` container per domain when
``OPENSEARCH_DATAPLANE=1`` (same pattern as ElastiCache and RDS).
Optional ``opensearchproject/opensearch-dashboards`` sidecar per domain when
``OPENSEARCH_DASHBOARDS=1``.

Operations:
- CreateDomain, DescribeDomain, DescribeDomains, DeleteDomain, ListDomainNames
- UpdateDomainConfig, DescribeDomainConfig, DescribeDomainChangeProgress
- ListVersions, GetCompatibleVersions
- AddTags, ListTags, RemoveTags

State is account-scoped via AccountScopedDict so per-tenant isolation matches
the rest of ministack.
"""

import copy
import json
import logging
import os
import re
import threading
import time

from ministack.core.responses import (
    AccountScopedDict,
    apply_image_prefix,
    get_account_id,
    get_region,
    new_uuid,
)

logger = logging.getLogger("opensearch")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DOCKER_NETWORK = os.environ.get("DOCKER_NETWORK", "")
DATAPLANE_ENABLED = os.environ.get("OPENSEARCH_DATAPLANE", "0") == "1"
DASHBOARDS_ENABLED = os.environ.get("OPENSEARCH_DASHBOARDS", "0") == "1"
BASE_PORT = int(os.environ.get("OPENSEARCH_BASE_PORT", "14571"))
DASHBOARDS_BASE_PORT = int(os.environ.get("OPENSEARCH_DASHBOARDS_BASE_PORT", "15601"))
DEFAULT_IMAGE = os.environ.get("OPENSEARCH_IMAGE", "opensearchproject/opensearch:2.15.0")
DASHBOARDS_IMAGE = os.environ.get(
    "OPENSEARCH_DASHBOARDS_IMAGE", "opensearchproject/opensearch-dashboards:2.15.0"
)
ENDPOINT_OVERRIDE = os.environ.get("MINISTACK_OPENSEARCH_ENDPOINT", "")

# Versions returned by ListVersions / used by GetCompatibleVersions. Mirrors the
# subset AWS still supports for new domains (managed-service availability page,
# 2026-Q1).
_SUPPORTED_VERSIONS = [
    "OpenSearch_2.15", "OpenSearch_2.13", "OpenSearch_2.11", "OpenSearch_2.9",
    "OpenSearch_2.7", "OpenSearch_2.5", "OpenSearch_2.3", "OpenSearch_1.3",
    "OpenSearch_1.2", "OpenSearch_1.1", "OpenSearch_1.0",
    "Elasticsearch_7.10", "Elasticsearch_7.9", "Elasticsearch_7.8",
    "Elasticsearch_7.7", "Elasticsearch_7.4", "Elasticsearch_7.1",
    "Elasticsearch_6.8", "Elasticsearch_6.7", "Elasticsearch_6.5",
    "Elasticsearch_6.4", "Elasticsearch_6.3", "Elasticsearch_6.2",
    "Elasticsearch_6.0", "Elasticsearch_5.6", "Elasticsearch_5.5",
    "Elasticsearch_5.3", "Elasticsearch_5.1",
]

_DEFAULT_VERSION = "OpenSearch_2.15"

# AWS allows lowercase letters, digits, hyphens; first character must be
# lowercase letter; 3-28 chars.
_NAME_RE = re.compile(r"^[a-z][a-z0-9\-]{2,27}$")

# ---------------------------------------------------------------------------
# State (account-scoped)
# ---------------------------------------------------------------------------

_domains = AccountScopedDict()        # name -> DomainStatus dict (+ private _* fields)
_tags = AccountScopedDict()           # arn -> [{Key, Value}, ...]
_change_progress = AccountScopedDict()  # name -> change progress record

# Port counter and Docker handle are process-global (data plane is shared).
_port_counter = [BASE_PORT]
_dashboards_port_counter = [DASHBOARDS_BASE_PORT]
_state_lock = threading.Lock()
_docker_client = None


def reset():
    _domains.clear()
    _tags.clear()
    _change_progress.clear()
    docker = _get_docker()
    if docker is not None:
        for c in docker.containers.list(
            all=True,
            filters={"label": "com.ministack.service=opensearch"},
        ):
            try:
                c.stop(timeout=2)
                c.remove(force=True)
            except Exception:
                pass


def get_state():
    return {
        "domains": copy.deepcopy(_domains),
        "tags": copy.deepcopy(_tags),
        "change_progress": copy.deepcopy(_change_progress),
    }


def restore_state(data):
    if not data:
        return
    for store, key in (
        (_domains, "domains"),
        (_tags, "tags"),
        (_change_progress, "change_progress"),
    ):
        store.clear()
        for k, v in (data.get(key) or {}).items():
            store[k] = v


# ---------------------------------------------------------------------------
# Wire helpers
# ---------------------------------------------------------------------------

def _json(status, body):
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _error(status, code, message):
    return _json(status, {"__type": code, "Message": message})


def _arn(name):
    return f"arn:aws:es:{get_region()}:{get_account_id()}:domain/{name}"


def _engine_type(version: str) -> str:
    return "Elasticsearch" if version.lower().startswith("elasticsearch") else "OpenSearch"


def _public(d: dict) -> dict:
    """Strip internal _-prefixed bookkeeping fields before serialising."""
    return {k: v for k, v in d.items() if not k.startswith("_")}


def _now() -> int:
    return int(time.time())


# ---------------------------------------------------------------------------
# Docker container management
# ---------------------------------------------------------------------------

def _get_docker():
    global _docker_client
    if _docker_client is None:
        try:
            import docker
            _docker_client = docker.from_env()
        except Exception:
            pass
    return _docker_client


def _container_name(domain_name: str) -> str:
    return f"ministack-opensearch-{domain_name}"


def _dashboards_container_name(domain_name: str) -> str:
    return f"ministack-opensearch-dashboards-{domain_name}"


def _spawn_dataplane(domain_name: str, engine_version: str):
    """Spawn an OpenSearch container.

    Returns ``(host, port, container_id, dashboards_endpoint, dashboards_cid)``.
    On any failure returns the stub endpoint shape so the management plane
    still works without a real cluster.
    """
    stub_host = f"{domain_name}.ministack.local"
    stub_port = 9200
    if ENDPOINT_OVERRIDE:
        host, _, port_s = ENDPOINT_OVERRIDE.partition(":")
        return host or stub_host, int(port_s or stub_port), None, None, None

    if not DATAPLANE_ENABLED:
        return stub_host, stub_port, None, None, None

    docker = _get_docker()
    if docker is None:
        logger.warning(
            "OpenSearch: OPENSEARCH_DATAPLANE=1 but Docker unavailable; "
            "falling back to stub endpoint"
        )
        return stub_host, stub_port, None, None, None

    with _state_lock:
        host_port = _port_counter[0]
        _port_counter[0] += 1
        dash_host_port = _dashboards_port_counter[0]
        _dashboards_port_counter[0] += 1

    image = apply_image_prefix(DEFAULT_IMAGE)
    labels = {
        "com.ministack.service": "opensearch",
        "com.ministack.domain": domain_name,
        "com.docker.compose.project": "ministack",
    }
    env_vars = {
        "discovery.type": "single-node",
        # Disable security plugin so test code can talk to the cluster
        # without bootstrapping a CA chain. Real AWS requires HTTPS+IAM;
        # matching that here would block every smoke test.
        "DISABLE_SECURITY_PLUGIN": "true",
        "OPENSEARCH_INITIAL_ADMIN_PASSWORD": "MinIstack-Admin-1!",
    }
    run_kwargs = dict(
        image=image, detach=True,
        ports={"9200/tcp": host_port},
        name=_container_name(domain_name),
        labels=labels,
        environment=env_vars,
        ulimits=[{"Name": "memlock", "Soft": -1, "Hard": -1}],
    )
    if DOCKER_NETWORK:
        run_kwargs["network"] = DOCKER_NETWORK

    cid = None
    endpoint_host, endpoint_port = "localhost", host_port
    try:
        container = docker.containers.run(**run_kwargs)
        cid = container.id
        if DOCKER_NETWORK:
            container.reload()
            networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})
            ip = networks.get(DOCKER_NETWORK, {}).get("IPAddress", "")
            if ip:
                endpoint_host = ip
                endpoint_port = 9200
        logger.info(
            "OpenSearch: started container %s at %s:%s (image=%s)",
            domain_name, endpoint_host, endpoint_port, image,
        )
    except Exception as e:
        logger.warning("OpenSearch: Docker failed for %s: %s", domain_name, e)
        return stub_host, stub_port, None, None, None

    dash_endpoint, dash_cid = None, None
    if DASHBOARDS_ENABLED:
        try:
            dash_kwargs = dict(
                image=apply_image_prefix(DASHBOARDS_IMAGE),
                detach=True,
                ports={"5601/tcp": dash_host_port},
                name=_dashboards_container_name(domain_name),
                labels={**labels, "com.ministack.role": "dashboards"},
                environment={
                    "OPENSEARCH_HOSTS": f"http://{_container_name(domain_name)}:9200",
                    "DISABLE_SECURITY_DASHBOARDS_PLUGIN": "true",
                },
            )
            if DOCKER_NETWORK:
                dash_kwargs["network"] = DOCKER_NETWORK
            dash_container = docker.containers.run(**dash_kwargs)
            dash_cid = dash_container.id
            dash_endpoint = f"localhost:{dash_host_port}"
            logger.info(
                "OpenSearch Dashboards: started %s on port %s",
                domain_name, dash_host_port,
            )
        except Exception as e:
            logger.warning("OpenSearch Dashboards: failed for %s: %s", domain_name, e)

    return endpoint_host, endpoint_port, cid, dash_endpoint, dash_cid


def _teardown_dataplane(rec: dict) -> None:
    docker = _get_docker()
    if docker is None:
        return
    for cid in (rec.get("_ContainerId"), rec.get("_DashboardsContainerId")):
        if not cid:
            continue
        try:
            c = docker.containers.get(cid)
            c.stop(timeout=2)
            c.remove(force=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
# DomainStatus / DomainConfig builders
# ---------------------------------------------------------------------------

def _default_cluster_config(override=None):
    base = {
        "InstanceType": "m5.large.search",
        "InstanceCount": 1,
        "DedicatedMasterEnabled": False,
        "ZoneAwarenessEnabled": False,
        "WarmEnabled": False,
        "ColdStorageOptions": {"Enabled": False},
    }
    if override:
        base.update(override)
    return base


def _default_ebs_options(override=None):
    base = {"EBSEnabled": True, "VolumeType": "gp3", "VolumeSize": 10}
    if override:
        base.update(override)
    return base


def _new_domain_record(name, payload):
    engine_version = payload.get("EngineVersion", _DEFAULT_VERSION)
    cluster_cfg = _default_cluster_config(payload.get("ClusterConfig"))
    ebs_opts = _default_ebs_options(payload.get("EBSOptions"))
    access_policies = payload.get("AccessPolicies", "")

    host, port, cid, dash_endpoint, dash_cid = _spawn_dataplane(name, engine_version)
    endpoint = f"{host}:{port}"

    rec = {
        "DomainId": f"{get_account_id()}/{name}",
        "DomainName": name,
        "ARN": _arn(name),
        "Created": True,
        "Deleted": False,
        "Endpoint": endpoint,
        "Processing": False,
        "UpgradeProcessing": False,
        "EngineVersion": engine_version,
        "ClusterConfig": cluster_cfg,
        "EBSOptions": ebs_opts,
        "AccessPolicies": access_policies,
        "SnapshotOptions": {"AutomatedSnapshotStartHour": 0},
        "VPCOptions": payload.get("VPCOptions", {}),
        "CognitoOptions": payload.get("CognitoOptions", {"Enabled": False}),
        "EncryptionAtRestOptions": payload.get(
            "EncryptionAtRestOptions", {"Enabled": False}
        ),
        "NodeToNodeEncryptionOptions": payload.get(
            "NodeToNodeEncryptionOptions", {"Enabled": False}
        ),
        "AdvancedOptions": payload.get("AdvancedOptions", {}),
        "ServiceSoftwareOptions": {
            "CurrentVersion": engine_version,
            "NewVersion": "",
            "UpdateAvailable": False,
            "Cancellable": False,
            "UpdateStatus": "COMPLETED",
            "Description": "",
            "AutomatedUpdateDate": 0,
            "OptionalDeployment": True,
        },
        "DomainEndpointOptions": payload.get(
            "DomainEndpointOptions",
            {"EnforceHTTPS": True, "TLSSecurityPolicy": "Policy-Min-TLS-1-2-2019-07"},
        ),
        "AdvancedSecurityOptions": payload.get(
            "AdvancedSecurityOptions",
            {"Enabled": False, "InternalUserDatabaseEnabled": False},
        ),
        "AutoTuneOptions": {"State": "DISABLED"},
        "ChangeProgressDetails": {},
        "OffPeakWindowOptions": {"Enabled": False},
        "SoftwareUpdateOptions": {"AutoSoftwareUpdateEnabled": False},
        "_CreatedTime": _now(),
        "_UpdatedTime": _now(),
        "_ContainerId": cid,
        "_DashboardsContainerId": dash_cid,
    }
    if dash_endpoint:
        rec["DashboardEndpoint"] = dash_endpoint
    return rec


def _option_status(create_ts, update_ts, version=1, state="Active"):
    return {
        "CreationDate": create_ts,
        "UpdateDate": update_ts,
        "UpdateVersion": version,
        "State": state,
        "PendingDeletion": False,
    }


def _domain_config(rec: dict) -> dict:
    """Wrap each top-level option in {Options, Status} per AWS DomainConfig shape."""
    create = rec.get("_CreatedTime", _now())
    update = rec.get("_UpdatedTime", create)
    status = _option_status(create, update)

    def wrap(value):
        return {"Options": value, "Status": dict(status)}

    return {
        "EngineVersion": wrap(rec["EngineVersion"]),
        "ClusterConfig": wrap(rec["ClusterConfig"]),
        "EBSOptions": wrap(rec["EBSOptions"]),
        "AccessPolicies": wrap(rec["AccessPolicies"]),
        "SnapshotOptions": wrap(rec["SnapshotOptions"]),
        "VPCOptions": wrap(rec["VPCOptions"]),
        "CognitoOptions": wrap(rec["CognitoOptions"]),
        "EncryptionAtRestOptions": wrap(rec["EncryptionAtRestOptions"]),
        "NodeToNodeEncryptionOptions": wrap(rec["NodeToNodeEncryptionOptions"]),
        "AdvancedOptions": wrap(rec["AdvancedOptions"]),
        "DomainEndpointOptions": wrap(rec["DomainEndpointOptions"]),
        "AdvancedSecurityOptions": wrap(rec["AdvancedSecurityOptions"]),
        "AutoTuneOptions": wrap(rec["AutoTuneOptions"]),
        "ChangeProgressDetails": rec.get("ChangeProgressDetails", {}),
        "OffPeakWindowOptions": wrap(rec["OffPeakWindowOptions"]),
        "SoftwareUpdateOptions": wrap(rec["SoftwareUpdateOptions"]),
    }


# ---------------------------------------------------------------------------
# Path matching
# ---------------------------------------------------------------------------

_DOMAIN_RE = re.compile(r"^/2021-01-01/(?:opensearch/)?domain(?:/(?P<name>[^/]+))?/?$")
_DOMAIN_INFO_RE = re.compile(r"^/2021-01-01/(?:opensearch/)?domain-info/?$")
_DOMAIN_CONFIG_RE = re.compile(r"^/2021-01-01/(?:opensearch/)?domain/(?P<name>[^/]+)/config/?$")
_DOMAIN_PROGRESS_RE = re.compile(r"^/2021-01-01/(?:opensearch/)?domain/(?P<name>[^/]+)/progress/?$")
_VERSIONS_RE = re.compile(r"^/2021-01-01/(?:opensearch/)?versions/?$")
_COMPAT_RE = re.compile(r"^/2021-01-01/(?:opensearch/)?compatibleVersions/?$")
_TAGS_RE = re.compile(r"^/2021-01-01/tags/?$")
_TAGS_REMOVAL_RE = re.compile(r"^/2021-01-01/tags-removal/?$")


def _qp(query_params, key) -> str:
    if not query_params:
        return ""
    v = query_params.get(key)
    if isinstance(v, list):
        return v[0] if v else ""
    return v or ""


# ---------------------------------------------------------------------------
# Operation handlers
# ---------------------------------------------------------------------------

def _create_domain(payload):
    name = payload.get("DomainName")
    if not name:
        return _error(400, "ValidationException", "DomainName is required")
    if not _NAME_RE.match(name):
        return _error(400, "ValidationException",
                      "DomainName must start with a lowercase letter, contain only "
                      "lowercase letters, digits, and hyphens, and be 3-28 characters")
    if name in _domains:
        return _error(409, "ResourceAlreadyExistsException",
                      f"Domain already exists: {name}")
    rec = _new_domain_record(name, payload)
    _domains[name] = rec
    tag_list = payload.get("TagList") or []
    if tag_list:
        _tags[_arn(name)] = list(tag_list)
    return _json(200, {"DomainStatus": _public(rec)})


def _describe_domain(name):
    rec = _domains.get(name)
    if not rec:
        return _error(404, "ResourceNotFoundException", f"Domain not found: {name}")
    return _json(200, {"DomainStatus": _public(rec)})


def _delete_domain(name):
    rec = _domains.pop(name, None)
    if not rec:
        return _error(404, "ResourceNotFoundException", f"Domain not found: {name}")
    _teardown_dataplane(rec)
    _tags.pop(_arn(name), None)
    _change_progress.pop(name, None)
    out = dict(rec)
    out["Deleted"] = True
    return _json(200, {"DomainStatus": _public(out)})


def _list_domain_names(query_params):
    engine_filter = _qp(query_params, "engineType")
    out = []
    for name, rec in _domains.items():
        etype = _engine_type(rec.get("EngineVersion", _DEFAULT_VERSION))
        if engine_filter and etype != engine_filter:
            continue
        out.append({"DomainName": name, "EngineType": etype})
    return _json(200, {"DomainNames": out})


def _describe_domains(payload):
    names = payload.get("DomainNames") or []
    statuses = []
    for n in names:
        rec = _domains.get(n)
        if rec:
            statuses.append(_public(rec))
    return _json(200, {"DomainStatusList": statuses})


def _update_domain_config(name, payload):
    rec = _domains.get(name)
    if not rec:
        return _error(404, "ResourceNotFoundException", f"Domain not found: {name}")
    updatable = {
        "ClusterConfig", "EBSOptions", "AccessPolicies", "SnapshotOptions",
        "VPCOptions", "CognitoOptions", "AdvancedOptions",
        "DomainEndpointOptions", "AdvancedSecurityOptions",
        "EncryptionAtRestOptions", "NodeToNodeEncryptionOptions",
        "AutoTuneOptions", "OffPeakWindowOptions", "SoftwareUpdateOptions",
    }
    for k, v in payload.items():
        if k in updatable:
            rec[k] = v
    rec["_UpdatedTime"] = _now()

    # Real AWS marks the domain Processing while config rollout happens; we
    # complete immediately because the data plane (if any) doesn't actually
    # re-provision.
    _change_progress[name] = {
        "ChangeId": new_uuid(),
        "StartTime": _now(),
        "Status": "COMPLETED",
        "PendingProperties": [],
        "CompletedProperties": list(payload.keys()),
        "TotalNumberOfStages": 0,
        "ConfigChangeStatus": "Completed",
    }
    return _json(200, {"DomainConfig": _domain_config(rec)})


def _describe_domain_config(name):
    rec = _domains.get(name)
    if not rec:
        return _error(404, "ResourceNotFoundException", f"Domain not found: {name}")
    return _json(200, {"DomainConfig": _domain_config(rec)})


def _describe_domain_change_progress(name, query_params):
    rec = _domains.get(name)
    if not rec:
        return _error(404, "ResourceNotFoundException", f"Domain not found: {name}")
    progress = _change_progress.get(name) or {
        "ChangeId": "",
        "StartTime": rec.get("_CreatedTime", _now()),
        "Status": "COMPLETED",
        "PendingProperties": [],
        "CompletedProperties": [],
        "TotalNumberOfStages": 0,
        "ConfigChangeStatus": "Completed",
    }
    return _json(200, {"ChangeProgressStatus": progress})


def _list_versions(query_params):
    return _json(200, {"Versions": list(_SUPPORTED_VERSIONS)})


def _get_compatible_versions(query_params):
    domain = _qp(query_params, "domainName")
    if domain:
        rec = _domains.get(domain)
        if not rec:
            return _error(404, "ResourceNotFoundException", f"Domain not found: {domain}")
        source = rec["EngineVersion"]
        targets = [v for v in _SUPPORTED_VERSIONS
                   if _engine_type(v) == _engine_type(source) and v != source]
        return _json(200, {"CompatibleVersions": [
            {"SourceVersion": source, "TargetVersions": targets}
        ]})
    out = []
    for source in _SUPPORTED_VERSIONS:
        targets = [v for v in _SUPPORTED_VERSIONS
                   if _engine_type(v) == _engine_type(source) and v != source]
        out.append({"SourceVersion": source, "TargetVersions": targets})
    return _json(200, {"CompatibleVersions": out})


def _add_tags(payload):
    arn = payload.get("ARN")
    tag_list = payload.get("TagList") or []
    if not arn:
        return _error(400, "ValidationException", "ARN is required")
    existing = _tags.get(arn) or []
    by_key = {t["Key"]: t for t in existing}
    for t in tag_list:
        by_key[t["Key"]] = t
    _tags[arn] = list(by_key.values())
    return _json(200, {})


def _list_tags(query_params):
    arn = _qp(query_params, "arn")
    if not arn:
        return _error(400, "ValidationException", "arn query parameter is required")
    return _json(200, {"TagList": list(_tags.get(arn) or [])})


def _remove_tags(payload):
    arn = payload.get("ARN")
    keys = set(payload.get("TagKeys") or [])
    if not arn:
        return _error(400, "ValidationException", "ARN is required")
    existing = _tags.get(arn) or []
    _tags[arn] = [t for t in existing if t["Key"] not in keys]
    return _json(200, {})


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body_bytes, query_params):
    body_text = body_bytes.decode("utf-8") if body_bytes else ""
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return _error(400, "InvalidPayloadException", "Request body is not valid JSON")

    p = path.rstrip("/")

    if method == "GET" and p == "/2021-01-01/domain":
        return _list_domain_names(query_params)

    if method == "GET" and _VERSIONS_RE.match(path):
        return _list_versions(query_params)

    if method == "GET" and _COMPAT_RE.match(path):
        return _get_compatible_versions(query_params)

    if _TAGS_REMOVAL_RE.match(path) and method == "POST":
        return _remove_tags(payload)
    if _TAGS_RE.match(path):
        if method == "POST":
            return _add_tags(payload)
        if method == "GET":
            return _list_tags(query_params)

    if method == "POST" and _DOMAIN_INFO_RE.match(path):
        return _describe_domains(payload)

    m = _DOMAIN_CONFIG_RE.match(path)
    if m:
        name = m.group("name")
        if method == "GET":
            return _describe_domain_config(name)
        if method == "POST":
            return _update_domain_config(name, payload)

    m = _DOMAIN_PROGRESS_RE.match(path)
    if method == "GET" and m:
        return _describe_domain_change_progress(m.group("name"), query_params)

    m = _DOMAIN_RE.match(path)
    if method == "POST" and m and m.group("name") is None:
        return _create_domain(payload)
    if m and m.group("name"):
        name = m.group("name")
        if method == "GET":
            return _describe_domain(name)
        if method == "DELETE":
            return _delete_domain(name)

    return _error(400, "InvalidAction",
                  f"OpenSearch operation not implemented: {method} {path}")
