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

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountScopedDict,
    apply_image_prefix,
    get_account_id,
    get_region,
    new_uuid,
)

logger = logging.getLogger("opensearch")

_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")

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
# versions AWS OpenSearch Service currently supports for new domains
# (managed-service availability page).
_SUPPORTED_VERSIONS = [
    "OpenSearch_3.5", "OpenSearch_3.3", "OpenSearch_3.1",
    "OpenSearch_2.19", "OpenSearch_2.17", "OpenSearch_2.15",
    "OpenSearch_2.13", "OpenSearch_2.11", "OpenSearch_2.9",
    "OpenSearch_2.7", "OpenSearch_2.5", "OpenSearch_2.3", "OpenSearch_1.3",
    "OpenSearch_1.2", "OpenSearch_1.1", "OpenSearch_1.0",
    "Elasticsearch_7.10", "Elasticsearch_7.9", "Elasticsearch_7.8",
    "Elasticsearch_7.7", "Elasticsearch_7.4", "Elasticsearch_7.1",
    "Elasticsearch_6.8", "Elasticsearch_6.7", "Elasticsearch_6.5",
    "Elasticsearch_6.4", "Elasticsearch_6.3", "Elasticsearch_6.2",
    "Elasticsearch_6.0", "Elasticsearch_5.6", "Elasticsearch_5.5",
    "Elasticsearch_5.3", "Elasticsearch_5.1",
    "Elasticsearch_2.3", "Elasticsearch_1.5",
]

_DEFAULT_VERSION = "OpenSearch_3.5"

_MODELED_DOMAIN_PROPERTIES = (
    "EngineVersion",
    "ClusterConfig",
    "EBSOptions",
    "AccessPolicies",
    "SnapshotOptions",
    "CognitoOptions",
    "EncryptionAtRestOptions",
    "NodeToNodeEncryptionOptions",
    "AdvancedOptions",
    "DomainEndpointOptions",
    "AdvancedSecurityOptions",
    "VPCOptions",
    "AutoTuneOptions",
    "OffPeakWindowOptions",
    "SoftwareUpdateOptions",
)

# AWS allows lowercase letters, digits, hyphens; first character must be
# lowercase letter; 3-28 chars.
_NAME_RE = re.compile(r"^[a-z][a-z0-9\-]{2,27}$")

# ---------------------------------------------------------------------------
# State (account-scoped)
# ---------------------------------------------------------------------------

_domains = AccountScopedDict()        # name -> DomainStatus dict (+ private _* fields)
_tags = AccountScopedDict()           # arn -> [{Key, Value}, ...]
_change_progress = AccountScopedDict()  # name -> change progress record
_packages = AccountScopedDict()       # PackageID -> PackageDetails (+ private _* fields)
_domain_packages = AccountScopedDict()  # "PackageID:DomainName" -> DomainPackageDetails

# Port counter and Docker handle are process-global (data plane is shared).
_port_counter = [BASE_PORT]
_dashboards_port_counter = [DASHBOARDS_BASE_PORT]
_state_lock = threading.Lock()
_docker_client = None


class OpenSearchServiceError(ValueError):
    """Actionable service error shared by HTTP and internal callers."""

    def __init__(self, status, code, message):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def reset():
    _domains.clear()
    _tags.clear()
    _change_progress.clear()
    _packages.clear()
    _domain_packages.clear()
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
        "packages": copy.deepcopy(_packages),
        "domain_packages": copy.deepcopy(_domain_packages),
    }


def restore_state(data):
    if not data:
        return
    for store, key in (
        (_domains, "domains"),
        (_tags, "tags"),
        (_change_progress, "change_progress"),
        (_packages, "packages"),
        (_domain_packages, "domain_packages"),
    ):
        store.clear()
        for k, v in (data.get(key) or {}).items():
            store[k] = v


# ---------------------------------------------------------------------------
# Wire helpers
# ---------------------------------------------------------------------------

def _json(status, body, extra_headers=None):
    headers = {"Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    return status, headers, json.dumps(body).encode()


def _error(status, code, message):
    return _json(status, {"__type": code, "Message": message}, {"x-amzn-errortype": code})


def _arn(name):
    return f"arn:aws:es:{get_region()}:{get_account_id()}:domain/{name}"


def _resolve_taggable_opensearch_arn(arn):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None, _error(400, "ValidationException", f"Invalid ARN: {arn}")

    if (
        spec.partition != "aws"
        or spec.service != "es"
        or spec.region != get_region()
        or spec.account_id != get_account_id()
    ):
        return None, _error(400, "ValidationException", f"Invalid ARN: {arn}")

    prefix = "domain/"
    if not spec.resource.startswith(prefix):
        return None, _error(400, "ValidationException", f"Invalid ARN: {arn}")

    name = spec.resource[len(prefix):]
    rec = _domains.get(name)
    if not rec or rec.get("ARN") != arn:
        return None, _error(404, "ResourceNotFoundException", f"Domain not found: {name}")
    return arn, None


def _engine_type(version: str) -> str:
    return "Elasticsearch" if version.lower().startswith("elasticsearch") else "OpenSearch"


def _public(d: dict) -> dict:
    """Strip internal _-prefixed bookkeeping fields before serialising."""
    out = copy.deepcopy({k: v for k, v in d.items() if not k.startswith("_")})
    # MasterUserOptions is an input-only shape and may contain a plaintext
    # password. AWS does not return it from DescribeDomain, and neither should
    # MiniStack's raw JSON response.
    advanced_security = out.get("AdvancedSecurityOptions")
    if isinstance(advanced_security, dict):
        advanced_security.pop("MasterUserOptions", None)
    if not out.get("VPCOptions"):
        out.pop("VPCOptions", None)
    return out


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
    endpoint_host, endpoint_port = _MINISTACK_HOST, host_port
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
            dash_endpoint = f"{_MINISTACK_HOST}:{dash_host_port}"
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


def _teardown_named_dataplane(domain_name: str) -> None:
    """Best-effort cleanup for a create that failed before returning a record."""
    docker = _get_docker()
    if docker is None:
        return
    for name in (_container_name(domain_name), _dashboards_container_name(domain_name)):
        try:
            c = docker.containers.get(name)
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


def _normalise_vpc_options(options):
    if not isinstance(options, dict):
        return None

    subnet_ids = list(options.get("SubnetIds") or [])
    security_group_ids = list(options.get("SecurityGroupIds") or [])
    if not subnet_ids and not security_group_ids:
        return None

    out = dict(options)
    out["SubnetIds"] = subnet_ids
    out["SecurityGroupIds"] = security_group_ids
    out.setdefault("VPCId", "vpc-ministack")
    out.setdefault(
        "AvailabilityZones",
        [f"{get_region()}{chr(ord('a') + idx)}" for idx, _ in enumerate(subnet_ids or ["default"])],
    )
    return out


def _set_vpc_options(rec: dict, options) -> None:
    vpc_options = _normalise_vpc_options(options)
    endpoint = rec.get("_Endpoint") or rec.get("Endpoint")
    endpoints = rec.get("Endpoints") or {}
    endpoint = endpoint or endpoints.get("vpc")

    if vpc_options:
        rec["VPCOptions"] = vpc_options
        if endpoint:
            rec["_Endpoint"] = endpoint
            rec["Endpoints"] = {"vpc": endpoint}
        rec.pop("Endpoint", None)
        return

    rec.pop("VPCOptions", None)
    rec.pop("Endpoints", None)
    if endpoint:
        rec["_Endpoint"] = endpoint
        rec["Endpoint"] = endpoint


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
        "SnapshotOptions": payload.get(
            "SnapshotOptions", {"AutomatedSnapshotStartHour": 0}
        ),
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
        "AutoTuneOptions": payload.get("AutoTuneOptions", {"State": "DISABLED"}),
        "ChangeProgressDetails": {},
        "OffPeakWindowOptions": payload.get(
            "OffPeakWindowOptions", {"Enabled": False}
        ),
        "SoftwareUpdateOptions": payload.get(
            "SoftwareUpdateOptions", {"AutoSoftwareUpdateEnabled": False}
        ),
        "_Endpoint": endpoint,
        "_CreatedTime": _now(),
        "_UpdatedTime": _now(),
        "_ContainerId": cid,
        "_DashboardsContainerId": dash_cid,
    }
    _set_vpc_options(rec, payload.get("VPCOptions"))
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

    config = {
        "EngineVersion": wrap(rec["EngineVersion"]),
        "ClusterConfig": wrap(rec["ClusterConfig"]),
        "EBSOptions": wrap(rec["EBSOptions"]),
        "AccessPolicies": wrap(rec["AccessPolicies"]),
        "SnapshotOptions": wrap(rec["SnapshotOptions"]),
        "CognitoOptions": wrap(rec["CognitoOptions"]),
        "EncryptionAtRestOptions": wrap(rec["EncryptionAtRestOptions"]),
        "NodeToNodeEncryptionOptions": wrap(rec["NodeToNodeEncryptionOptions"]),
        "AdvancedOptions": wrap(rec["AdvancedOptions"]),
        "DomainEndpointOptions": wrap(rec["DomainEndpointOptions"]),
        "AdvancedSecurityOptions": wrap({
            k: copy.deepcopy(v)
            for k, v in rec["AdvancedSecurityOptions"].items()
            if k != "MasterUserOptions"
        }),
        "AutoTuneOptions": wrap(rec["AutoTuneOptions"]),
        "ChangeProgressDetails": rec.get("ChangeProgressDetails", {}),
        "OffPeakWindowOptions": wrap(rec["OffPeakWindowOptions"]),
        "SoftwareUpdateOptions": wrap(rec["SoftwareUpdateOptions"]),
    }
    if rec.get("VPCOptions"):
        config["VPCOptions"] = wrap(rec["VPCOptions"])
    return config


def _normalise_tags(tag_list):
    """Return an exact, detached tag set with the last value winning by key."""
    by_key = {}
    for tag in tag_list or []:
        detached = copy.deepcopy(tag)
        by_key[detached.get("Key", "")] = detached
    return list(by_key.values())


def _desired_modeled_configuration(payload):
    """Expand a full create-shaped payload into the desired domain state."""
    return {
        "EngineVersion": copy.deepcopy(payload.get("EngineVersion", _DEFAULT_VERSION)),
        "ClusterConfig": _default_cluster_config(copy.deepcopy(payload.get("ClusterConfig"))),
        "EBSOptions": _default_ebs_options(copy.deepcopy(payload.get("EBSOptions"))),
        "AccessPolicies": copy.deepcopy(payload.get("AccessPolicies", "")),
        "SnapshotOptions": copy.deepcopy(payload.get(
            "SnapshotOptions", {"AutomatedSnapshotStartHour": 0}
        )),
        "CognitoOptions": copy.deepcopy(payload.get("CognitoOptions", {"Enabled": False})),
        "EncryptionAtRestOptions": copy.deepcopy(payload.get(
            "EncryptionAtRestOptions", {"Enabled": False}
        )),
        "NodeToNodeEncryptionOptions": copy.deepcopy(payload.get(
            "NodeToNodeEncryptionOptions", {"Enabled": False}
        )),
        "AdvancedOptions": copy.deepcopy(payload.get("AdvancedOptions", {})),
        "DomainEndpointOptions": copy.deepcopy(payload.get(
            "DomainEndpointOptions",
            {"EnforceHTTPS": True, "TLSSecurityPolicy": "Policy-Min-TLS-1-2-2019-07"},
        )),
        "AdvancedSecurityOptions": copy.deepcopy(payload.get(
            "AdvancedSecurityOptions",
            {"Enabled": False, "InternalUserDatabaseEnabled": False},
        )),
        "VPCOptions": _normalise_vpc_options(copy.deepcopy(payload.get("VPCOptions"))),
        "AutoTuneOptions": copy.deepcopy(payload.get("AutoTuneOptions", {"State": "DISABLED"})),
        "OffPeakWindowOptions": copy.deepcopy(payload.get(
            "OffPeakWindowOptions", {"Enabled": False}
        )),
        "SoftwareUpdateOptions": copy.deepcopy(payload.get(
            "SoftwareUpdateOptions", {"AutoSoftwareUpdateEnabled": False}
        )),
    }


def create_domain_record(payload, compatibility_properties=None):
    """Create a domain through the shared management-plane lifecycle.

    Internal callers receive the live private record. HTTP handlers pass it
    through ``_public`` before serialising it.
    """
    detached_payload = copy.deepcopy(payload or {})
    name = detached_payload.get("DomainName")
    if not name:
        raise OpenSearchServiceError(400, "ValidationException", "DomainName is required")
    if not _NAME_RE.match(name):
        raise OpenSearchServiceError(
            400,
            "ValidationException",
            "DomainName must start with a lowercase letter, contain only "
            "lowercase letters, digits, and hyphens, and be 3-28 characters",
        )
    if name in _domains:
        raise OpenSearchServiceError(
            409, "ResourceAlreadyExistsException", f"Domain already exists: {name}"
        )

    rec = None
    try:
        rec = _new_domain_record(name, detached_payload)
        if compatibility_properties is not None:
            rec["_CloudFormationCompatibility"] = copy.deepcopy(
                compatibility_properties
            )
        _domains[name] = rec
        tags = _normalise_tags(detached_payload.get("TagList"))
        if tags:
            _tags[rec["ARN"]] = tags
        else:
            _tags.pop(rec["ARN"], None)
        return rec
    except Exception:
        if rec is not None:
            _teardown_dataplane(rec)
        _teardown_named_dataplane(name)
        _domains.pop(name, None)
        _tags.pop(_arn(name), None)
        _change_progress.pop(name, None)
        raise


def update_domain_from_cloudformation(name, payload, compatibility_properties):
    """Apply a complete CloudFormation desired state to an existing domain."""
    rec = _domains.get(name)
    if not rec:
        raise OpenSearchServiceError(
            404, "ResourceNotFoundException", f"Domain not found: {name}"
        )

    detached_payload = copy.deepcopy(payload or {})
    desired = _desired_modeled_configuration(detached_payload)
    detached_compatibility = copy.deepcopy(compatibility_properties or {})
    desired_tags = _normalise_tags(detached_payload.get("TagList"))
    current_tags = _normalise_tags(_tags.get(rec["ARN"]) or [])

    changed_properties = []
    for key in _MODELED_DOMAIN_PROPERTIES:
        wanted = desired[key]
        current = rec.get(key)
        if key == "VPCOptions":
            current = rec.get("VPCOptions")
        if current != wanted:
            changed_properties.append(key)
            if key == "VPCOptions":
                _set_vpc_options(rec, wanted)
            else:
                rec[key] = copy.deepcopy(wanted)
                if key == "EngineVersion":
                    rec["ServiceSoftwareOptions"]["CurrentVersion"] = wanted

    if rec.get("_CloudFormationCompatibility", {}) != detached_compatibility:
        rec["_CloudFormationCompatibility"] = detached_compatibility
        changed_properties.append("CompatibilityProperties")

    if current_tags != desired_tags:
        if desired_tags:
            _tags[rec["ARN"]] = desired_tags
        else:
            _tags.pop(rec["ARN"], None)

    if changed_properties:
        rec["_UpdatedTime"] = _now()
        _change_progress[name] = {
            "ChangeId": new_uuid(),
            "StartTime": _now(),
            "Status": "COMPLETED",
            "PendingProperties": [],
            "CompletedProperties": changed_properties,
            "TotalNumberOfStages": 0,
            "ConfigChangeStatus": "Completed",
        }
    return rec


def delete_domain_record(name, missing_ok=False):
    """Delete all domain-owned state and containers, optionally idempotently."""
    rec = _domains.pop(name, None)
    if not rec and not missing_ok:
        raise OpenSearchServiceError(
            404, "ResourceNotFoundException", f"Domain not found: {name}"
        )
    if rec:
        _teardown_dataplane(rec)
        arn = rec.get("ARN", _arn(name))
    else:
        arn = _arn(name)
    _teardown_named_dataplane(name)
    _tags.pop(arn, None)
    _change_progress.pop(name, None)
    return rec


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
# Package management. The literal sub-paths (update/describe/associate/
# dissociate) are matched before the generic ``packages/{PackageID}`` so those
# segments aren't captured as an id.
_PACKAGES_RE = re.compile(r"^/2021-01-01/packages/?$")
_PACKAGES_UPDATE_RE = re.compile(r"^/2021-01-01/packages/update/?$")
_PACKAGES_DESCRIBE_RE = re.compile(r"^/2021-01-01/packages/describe/?$")
_PACKAGES_ASSOCIATE_RE = re.compile(
    r"^/2021-01-01/packages/associate/(?P<pid>[^/]+)/(?P<domain>[^/]+)/?$")
_PACKAGES_DISSOCIATE_RE = re.compile(
    r"^/2021-01-01/packages/dissociate/(?P<pid>[^/]+)/(?P<domain>[^/]+)/?$")
_PACKAGE_RE = re.compile(r"^/2021-01-01/packages/(?P<pid>[^/]+)/?$")
_DOMAIN_PACKAGES_RE = re.compile(
    r"^/2021-01-01/(?:opensearch/)?domain/(?P<name>[^/]+)/packages/?$")


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
    try:
        rec = create_domain_record(payload)
    except OpenSearchServiceError as exc:
        return _error(exc.status, exc.code, exc.message)
    return _json(200, {"DomainStatus": _public(rec)})


def _describe_domain(name):
    rec = _domains.get(name)
    if not rec:
        return _error(404, "ResourceNotFoundException", f"Domain not found: {name}")
    return _json(200, {"DomainStatus": _public(rec)})


def _delete_domain(name):
    try:
        rec = delete_domain_record(name)
    except OpenSearchServiceError as exc:
        return _error(exc.status, exc.code, exc.message)
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
            if k == "VPCOptions":
                _set_vpc_options(rec, v)
            else:
                rec[k] = copy.deepcopy(v)
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
    arn, err = _resolve_taggable_opensearch_arn(arn)
    if err:
        return err
    existing = _tags.get(arn) or []
    by_key = {t["Key"]: t for t in existing}
    for t in tag_list:
        by_key[t["Key"]] = t
    _tags[arn] = copy.deepcopy(list(by_key.values()))
    return _json(200, {})


def _list_tags(query_params):
    arn = _qp(query_params, "arn")
    if not arn:
        return _error(400, "ValidationException", "arn query parameter is required")
    arn, err = _resolve_taggable_opensearch_arn(arn)
    if err:
        return err
    return _json(200, {"TagList": copy.deepcopy(list(_tags.get(arn) or []))})


def _remove_tags(payload):
    arn = payload.get("ARN")
    keys = set(payload.get("TagKeys") or [])
    if not arn:
        return _error(400, "ValidationException", "ARN is required")
    arn, err = _resolve_taggable_opensearch_arn(arn)
    if err:
        return err
    existing = _tags.get(arn) or []
    _tags[arn] = [t for t in existing if t["Key"] not in keys]
    return _json(200, {})


# ---------------------------------------------------------------------------
# Packages
# ---------------------------------------------------------------------------
# CRUD + domain association, keyed by the service-generated PackageID. Like the
# domain change-progress model, status transitions complete synchronously:
# a package is AVAILABLE the moment it is created/updated, and an association
# is ACTIVE the moment it is made, so a client's poll succeeds on first call.

def _new_package_id() -> str:
    return f"F{new_uuid().replace('-', '')[:10].upper()}"


def _public_package(rec: dict) -> dict:
    """Strip internal _-prefixed fields. PackageSource is an input-only shape
    (AWS does not echo it from DescribePackages), so it is stored privately."""
    return {k: v for k, v in rec.items() if not k.startswith("_")}


def _create_package(payload):
    name = payload.get("PackageName")
    ptype = payload.get("PackageType")
    source = payload.get("PackageSource")
    if not name:
        return _error(400, "ValidationException", "PackageName is required")
    if not ptype:
        return _error(400, "ValidationException", "PackageType is required")
    if source is None:
        return _error(400, "ValidationException", "PackageSource is required")
    now = _now()
    pid = _new_package_id()
    rec = {
        "PackageID": pid,
        "PackageName": name,
        "PackageType": ptype,
        "PackageDescription": payload.get("PackageDescription", ""),
        "PackageStatus": "AVAILABLE",
        "CreatedAt": now,
        "LastUpdatedAt": now,
        "AvailablePackageVersion": "1",
        "_PackageSource": source,
    }
    _packages[pid] = rec
    return _json(200, {"PackageDetails": _public_package(rec)})


def _update_package(payload):
    pid = payload.get("PackageID")
    rec = _packages.get(pid)
    if not rec:
        return _error(404, "ResourceNotFoundException", f"Package not found: {pid}")
    if "PackageSource" in payload:
        rec["_PackageSource"] = payload["PackageSource"]
    if "PackageDescription" in payload:
        rec["PackageDescription"] = payload["PackageDescription"]
    try:
        rec["AvailablePackageVersion"] = str(int(rec.get("AvailablePackageVersion", "1")) + 1)
    except (TypeError, ValueError):
        rec["AvailablePackageVersion"] = "2"
    rec["LastUpdatedAt"] = _now()
    rec["PackageStatus"] = "AVAILABLE"
    return _json(200, {"PackageDetails": _public_package(rec)})


def _describe_packages(payload):
    results = list(_packages.values())
    for f in payload.get("Filters") or []:
        fname = f.get("Name")
        fvals = f.get("Value") or []
        if not fvals:
            continue
        if fname in ("PackageID", "PackageName", "PackageStatus", "PackageType"):
            results = [p for p in results if p.get(fname) in fvals]
    results = sorted(results, key=lambda p: p["PackageID"])
    start = int(payload["NextToken"]) if payload.get("NextToken") else 0
    max_results = payload.get("MaxResults")
    if max_results:
        page = results[start:start + max_results]
        resp = {"PackageDetailsList": [_public_package(p) for p in page]}
        if start + max_results < len(results):
            resp["NextToken"] = str(start + max_results)
    else:
        resp = {"PackageDetailsList": [_public_package(p) for p in results[start:]]}
    return _json(200, resp)


def _delete_package(pid):
    rec = _packages.pop(pid, None)
    if not rec:
        return _error(404, "ResourceNotFoundException", f"Package not found: {pid}")
    # Cascade: drop any domain associations this package still had.
    for key in [k for k in _domain_packages.keys() if k.split(":", 1)[0] == pid]:
        _domain_packages.pop(key, None)
    out = dict(rec)
    out["PackageStatus"] = "DELETED"
    return _json(200, {"PackageDetails": _public_package(out)})


def _associate_package(pid, domain_name):
    rec = _packages.get(pid)
    if not rec:
        return _error(404, "ResourceNotFoundException", f"Package not found: {pid}")
    if domain_name not in _domains:
        return _error(404, "ResourceNotFoundException", f"Domain not found: {domain_name}")
    detail = {
        "PackageID": pid,
        "PackageName": rec["PackageName"],
        "PackageType": rec["PackageType"],
        "DomainName": domain_name,
        "DomainPackageStatus": "ACTIVE",
        "PackageVersion": rec["AvailablePackageVersion"],
        "LastUpdated": _now(),
        "ReferencePath": f"packages/{rec['PackageName']}",
    }
    _domain_packages[f"{pid}:{domain_name}"] = detail
    return _json(200, {"DomainPackageDetails": detail})


def _dissociate_package(pid, domain_name):
    detail = _domain_packages.pop(f"{pid}:{domain_name}", None)
    if not detail:
        return _error(404, "ResourceNotFoundException",
                      f"Package {pid} is not associated with domain {domain_name}")
    out = dict(detail)
    out["DomainPackageStatus"] = "DISSOCIATING"
    return _json(200, {"DomainPackageDetails": out})


def _list_packages_for_domain(domain_name, query_params):
    if domain_name not in _domains:
        return _error(404, "ResourceNotFoundException", f"Domain not found: {domain_name}")
    details = sorted(
        (v for k, v in _domain_packages.items() if k.split(":", 1)[-1] == domain_name),
        key=lambda d: d["PackageID"],
    )
    start = int(_qp(query_params, "nextToken")) if _qp(query_params, "nextToken") else 0
    mr = _qp(query_params, "maxResults")
    if mr:
        limit = int(mr)
        page = details[start:start + limit]
        resp = {"DomainPackageDetailsList": page}
        if start + limit < len(details):
            resp["NextToken"] = str(start + limit)
    else:
        resp = {"DomainPackageDetailsList": details[start:]}
    return _json(200, resp)


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

async def handle_request(method, path, headers, body_bytes, query_params):
    body_text = body_bytes.decode("utf-8") if body_bytes else ""
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        # Botocore opensearch model has no InvalidPayloadException — JSON
        # parsing failures map to the generic ValidationException (400).
        return _error(400, "ValidationException", "Request body is not valid JSON")

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

    # Packages — literal sub-paths before the generic packages/{PackageID}.
    if method == "POST" and _PACKAGES_UPDATE_RE.match(path):
        return _update_package(payload)
    if method == "POST" and _PACKAGES_DESCRIBE_RE.match(path):
        return _describe_packages(payload)
    m = _PACKAGES_ASSOCIATE_RE.match(path)
    if method == "POST" and m:
        return _associate_package(m.group("pid"), m.group("domain"))
    m = _PACKAGES_DISSOCIATE_RE.match(path)
    if method == "POST" and m:
        return _dissociate_package(m.group("pid"), m.group("domain"))
    m = _PACKAGE_RE.match(path)
    if method == "DELETE" and m:
        return _delete_package(m.group("pid"))
    if method == "POST" and _PACKAGES_RE.match(path):
        return _create_package(payload)
    m = _DOMAIN_PACKAGES_RE.match(path)
    if method == "GET" and m:
        return _list_packages_for_domain(m.group("name"), query_params)

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
