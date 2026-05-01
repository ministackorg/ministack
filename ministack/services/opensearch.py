"""
Amazon OpenSearch Service emulator (rest-json).

Endpoints (botocore service-2.json, 2021-01-01):
- GET    /2021-01-01/domain                          ListDomainNames
- POST   /2021-01-01/opensearch/domain               CreateDomain
- GET    /2021-01-01/opensearch/domain/{Name}        DescribeDomain
- DELETE /2021-01-01/opensearch/domain/{Name}        DeleteDomain
- POST   /2021-01-01/opensearch/domain-info          DescribeDomains

Account-scoped storage so multi-tenant tests stay isolated.
"""

import copy
import json
import logging
import re
import time

from ministack.core.responses import (
    AccountScopedDict,
    get_account_id,
    get_region,
    new_uuid,
)

logger = logging.getLogger("opensearch")

_domains = AccountScopedDict()  # name -> DomainStatus dict


def reset():
    _domains.clear()


def get_state():
    return {"domains": copy.deepcopy(_domains)}


def restore_state(data):
    if not data:
        return
    _domains.clear()
    for k, v in (data.get("domains") or {}).items():
        _domains[k] = v


def _json(status, body):
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _error(status, code, message):
    # opensearch errors are rest-json with __type at top level + Message
    return _json(status, {"__type": code, "Message": message})


def _arn(name):
    return f"arn:aws:es:{get_region()}:{get_account_id()}:domain/{name}"


def _domain_status(name, engine_version="OpenSearch_2.11", cluster_config=None,
                   ebs_options=None, access_policies=None, tags=None):
    cluster_config = cluster_config or {
        "InstanceType": "m5.large.search",
        "InstanceCount": 1,
        "DedicatedMasterEnabled": False,
        "ZoneAwarenessEnabled": False,
        "WarmEnabled": False,
        "ColdStorageOptions": {"Enabled": False},
    }
    ebs_options = ebs_options or {
        "EBSEnabled": True,
        "VolumeType": "gp3",
        "VolumeSize": 10,
    }
    return {
        "DomainId": f"{get_account_id()}/{name}",
        "DomainName": name,
        "ARN": _arn(name),
        "Created": True,
        "Deleted": False,
        "Endpoint": f"search-{name}-{new_uuid()[:8]}.{get_region()}.es.amazonaws.com",
        "Processing": False,
        "UpgradeProcessing": False,
        "EngineVersion": engine_version,
        "ClusterConfig": cluster_config,
        "EBSOptions": ebs_options,
        "AccessPolicies": access_policies or "",
        "SnapshotOptions": {"AutomatedSnapshotStartHour": 0},
        "VPCOptions": {},
        "CognitoOptions": {"Enabled": False},
        "EncryptionAtRestOptions": {"Enabled": False},
        "NodeToNodeEncryptionOptions": {"Enabled": False},
        "AdvancedOptions": {},
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
        "DomainEndpointOptions": {
            "EnforceHTTPS": True,
            "TLSSecurityPolicy": "Policy-Min-TLS-1-2-2019-07",
        },
        "AdvancedSecurityOptions": {"Enabled": False, "InternalUserDatabaseEnabled": False},
        "AutoTuneOptions": {"State": "DISABLED"},
        "ChangeProgressDetails": {},
        "OffPeakWindowOptions": {"Enabled": False},
        "SoftwareUpdateOptions": {"AutoSoftwareUpdateEnabled": False},
        "_CreatedTime": time.time(),
        "_Tags": tags or [],
    }


def _engine_type(engine_version: str) -> str:
    return "Elasticsearch" if engine_version.lower().startswith("elasticsearch") else "OpenSearch"


def _public_status(d: dict) -> dict:
    """Strip internal underscore-prefixed fields before returning to the wire."""
    return {k: v for k, v in d.items() if not k.startswith("_")}


_DOMAIN_PATH = re.compile(r"^/2021-01-01/(?:opensearch/)?domain(?:/(?P<name>[^/]+))?/?$")
_DOMAIN_INFO_PATH = re.compile(r"^/2021-01-01/(?:opensearch/)?domain-info/?$")


async def handle_request(method, path, headers, body_bytes, query_params):
    body_text = body_bytes.decode("utf-8") if body_bytes else ""
    try:
        payload = json.loads(body_text) if body_text else {}
    except json.JSONDecodeError:
        return _error(400, "InvalidPayloadException", "Request body is not valid JSON")

    # ListDomainNames — GET /2021-01-01/domain
    if method == "GET" and path.rstrip("/") == "/2021-01-01/domain":
        engine_filter = (query_params.get("engineType") or [""])[0] if query_params else ""
        out = []
        for name, d in _domains.items():
            etype = _engine_type(d.get("EngineVersion", "OpenSearch_2.11"))
            if engine_filter and etype != engine_filter:
                continue
            out.append({"DomainName": name, "EngineType": etype})
        return _json(200, {"DomainNames": out})

    # CreateDomain — POST /2021-01-01/opensearch/domain
    m = _DOMAIN_PATH.match(path)
    if method == "POST" and m and m.group("name") is None:
        name = payload.get("DomainName")
        if not name:
            return _error(400, "ValidationException", "DomainName is required")
        if name in _domains:
            return _error(409, "ResourceAlreadyExistsException",
                          f"Domain already exists: {name}")
        rec = _domain_status(
            name,
            engine_version=payload.get("EngineVersion", "OpenSearch_2.11"),
            cluster_config=payload.get("ClusterConfig"),
            ebs_options=payload.get("EBSOptions"),
            access_policies=payload.get("AccessPolicies"),
            tags=payload.get("TagList") or [],
        )
        _domains[name] = rec
        return _json(200, {"DomainStatus": _public_status(rec)})

    # DescribeDomain — GET /2021-01-01/opensearch/domain/{Name}
    if method == "GET" and m and m.group("name"):
        name = m.group("name")
        rec = _domains.get(name)
        if not rec:
            return _error(404, "ResourceNotFoundException",
                          f"Domain not found: {name}")
        return _json(200, {"DomainStatus": _public_status(rec)})

    # DeleteDomain — DELETE /2021-01-01/opensearch/domain/{Name}
    if method == "DELETE" and m and m.group("name"):
        name = m.group("name")
        rec = _domains.pop(name, None)
        if not rec:
            return _error(404, "ResourceNotFoundException",
                          f"Domain not found: {name}")
        rec = dict(rec)
        rec["Deleted"] = True
        return _json(200, {"DomainStatus": _public_status(rec)})

    # DescribeDomains — POST /2021-01-01/opensearch/domain-info
    if method == "POST" and _DOMAIN_INFO_PATH.match(path):
        names = payload.get("DomainNames") or []
        statuses = []
        for n in names:
            rec = _domains.get(n)
            if rec:
                statuses.append(_public_status(rec))
        return _json(200, {"DomainStatusList": statuses})

    return _error(400, "InvalidAction",
                  f"OpenSearch operation not implemented: {method} {path}")
