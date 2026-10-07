# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
Amazon OpenSearch Serverless (AOSS) emulator.

Control plane: JSON 1.0 via ``X-Amz-Target: OpenSearchServerless.<Op>``
(botocore ``opensearchserverless`` 2021-11-01, signing name ``aoss``).
Data plane: each collection's ``collectionEndpoint`` is served through the
gateway (``<id>.<region>.aoss.<MINISTACK_HOST>:<port>``) and proxied to a real
``opensearchproject/opensearch`` container when ``OPENSEARCH_DATAPLANE=1``.

Operations:
- CreateCollection, BatchGetCollection, ListCollections, UpdateCollection, DeleteCollection
- Create/Get/Update/Delete/ListSecurityPolicy (encryption, network)
- Create/Get/Update/Delete/ListAccessPolicy (data)
- GetPoliciesStats
- TagResource, UntagResource, ListTagsForResource
"""

import base64
import copy
import fnmatch
import http.client
import json
import logging
import os
import random
import re
import string
import time

from ministack.core import container_reaper, persistence
from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.concurrency import resource_lock, run_offloop, spawn_background
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    set_request_account_id,
    set_request_region,
)

logger = logging.getLogger("opensearchserverless")

_MINISTACK_HOST = os.environ.get("MINISTACK_HOST", "localhost")
_ENGINE_READY_TIMEOUT = 300.0
_PROXY_TIMEOUT = 120.0
_CONTAINER_LABEL = "opensearchserverless"
# OpenSearch's default data path (opensearchproject/opensearch image).
_ENGINE_DATA_DIR = "/usr/share/opensearch/data"

_NAME_RE = re.compile(r"^[a-z][a-z0-9-]+$")
_COLLECTION_TYPES = ("SEARCH", "TIMESERIES", "VECTORSEARCH")
_SECURITY_POLICY_TYPES = ("encryption", "network")
_ACCESS_POLICY_TYPES = ("data",)

# Data access policy permissions (AWS docs: "Data access control for Amazon
# OpenSearch Serverless", supported policy permissions).
_DATA_PERMISSIONS = {
    "collection": {
        "aoss:*", "aoss:CreateCollectionItems", "aoss:DeleteCollectionItems",
        "aoss:UpdateCollectionItems", "aoss:DescribeCollectionItems",
    },
    "index": {
        "aoss:*", "aoss:ReadDocument", "aoss:WriteDocument", "aoss:CreateIndex",
        "aoss:DeleteIndex", "aoss:UpdateIndex", "aoss:DescribeIndex",
    },
    "model": {
        "aoss:*", "aoss:CreateMLResource", "aoss:DeleteMLResource",
        "aoss:UpdateMLResource", "aoss:DescribeMLResource", "aoss:ExecuteMLResource",
    },
}

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

_collections = AccountRegionScopedDict()        # id -> record (+ private _* fields)
_security_policies = AccountRegionScopedDict()  # "<type>/<name>" -> record
_access_policies = AccountRegionScopedDict()    # "<type>/<name>" -> record
_tags = AccountScopedDict()                     # collection ARN -> [{key, value}]
_docker_client = None


def reset():
    _collections.clear()
    _security_policies.clear()
    _access_policies.clear()
    _tags.clear()
    docker = _get_docker()
    if docker is None:
        return
    try:
        containers = docker.containers.list(
            all=True, filters={"label": f"com.ministack.service={_CONTAINER_LABEL}"}
        )
    except Exception:
        return
    container_reaper.drop_containers(containers, force=True)


def get_state():
    return {
        "collections": copy.deepcopy(_collections),
        "security_policies": copy.deepcopy(_security_policies),
        "access_policies": copy.deepcopy(_access_policies),
        "tags": copy.deepcopy(_tags),
    }


def load_persisted_state(data):
    if not data:
        return
    _collections.update(data.get("collections", {}))
    _security_policies.update(data.get("security_policies", {}))
    _access_policies.update(data.get("access_policies", {}))
    _tags.update(data.get("tags", {}))
    original_account, original_region = get_account_id(), get_region()
    try:
        for (account_id, region, _cid), rec in list(_collections.all_items()):
            set_request_account_id(account_id)
            set_request_region(region)
            # Engine containers do not survive a restart; provision afresh.
            # _VolumeName is kept: PERSIST_STATE gives the fresh container the
            # same named volume, so its data survives the restart.
            rec.pop("_ContainerId", None)
            rec.pop("_Engine", None)
            rec["status"] = "CREATING"
            _start_provisioning(rec)
    finally:
        set_request_account_id(original_account)
        set_request_region(original_region)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _error(code, message, status=400):
    return error_response_json(code, message, status)


class _Invalid(Exception):
    def __init__(self, message, code="ValidationException"):
        super().__init__(message)
        self.code = code
        self.message = message


def _validate_name(value, field, max_len):
    if not isinstance(value, str) or not value:
        raise _Invalid(f"1 validation error detected: Value null at '{field}' failed to satisfy constraint: Member must not be null")
    if not (3 <= len(value) <= max_len) or not _NAME_RE.match(value):
        raise _Invalid(
            f"1 validation error detected: Value '{value}' at '{field}' failed to satisfy constraint: "
            f"Member must satisfy regular expression pattern: [a-z][a-z0-9-]+ and length {3}-{max_len}"
        )


def _validate_enum(value, field, allowed):
    if value not in allowed:
        raise _Invalid(
            f"1 validation error detected: Value '{value}' at '{field}' failed to satisfy constraint: "
            f"Member must satisfy enum value set: [{', '.join(allowed)}]"
        )


def _policy_version(modified_ms, revision):
    # AWS docs example: "MTY2MzY5MzIxNzgyNl8x" = base64("1663693217826_1").
    return base64.b64encode(f"{modified_ms}_{revision}".encode()).decode()


def _collection_arn(collection_id):
    return f"arn:aws:aoss:{get_region()}:{get_account_id()}:collection/{collection_id}"


def _new_collection_id():
    alphabet = string.ascii_lowercase + string.digits
    while True:
        cid = "".join(random.choices(alphabet, k=20))
        if not any(key[2] == cid for key, _ in _collections.all_items()):
            return cid


def _collection_endpoint(collection_id):
    from ministack.core import tls

    scheme = "https" if tls.use_ssl_enabled() else "http"
    port = os.environ.get("GATEWAY_PORT") or os.environ.get("EDGE_PORT") or "4566"
    return f"{scheme}://{collection_id}.{get_region()}.aoss.{_MINISTACK_HOST}:{port}"


def _pattern_matches(pattern, value):
    """AOSS resource patterns support a trailing ``*`` wildcard."""
    return fnmatch.fnmatchcase(value, pattern)


def _page(items, data, max_default=100):
    try:
        start = int(data.get("nextToken") or 0)
    except (TypeError, ValueError):
        raise _Invalid("Invalid nextToken")
    limit = data.get("maxResults") or max_default
    page = items[start:start + limit]
    token = str(start + limit) if start + limit < len(items) else None
    return page, token


# ---------------------------------------------------------------------------
# Policy documents
# ---------------------------------------------------------------------------

def _parse_policy_document(raw):
    if not isinstance(raw, str) or not raw.strip():
        raise _Invalid("Policy document must not be empty")
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise _Invalid(f"Policy json is invalid, error: [{e.msg}]")


def _validate_rules(rules, allowed_types, need_permission):
    if not isinstance(rules, list) or not rules:
        raise _Invalid("Policy json is invalid, error: [Rules must be a non-empty list]")
    for rule in rules:
        if not isinstance(rule, dict):
            raise _Invalid("Policy json is invalid, error: [Rule must be an object]")
        rtype = rule.get("ResourceType")
        if rtype not in allowed_types:
            raise _Invalid(f"Policy json is invalid, error: [ResourceType {rtype!r} is not supported]")
        resources = rule.get("Resource")
        if not isinstance(resources, list) or not resources:
            raise _Invalid("Policy json is invalid, error: [Resource must be a non-empty list]")
        for resource in resources:
            if not isinstance(resource, str) or not resource.startswith(f"{rtype}/") or resource == f"{rtype}/":
                raise _Invalid(f"Policy json is invalid, error: [Resource {resource!r} must start with '{rtype}/']")
        if need_permission:
            perms = rule.get("Permission")
            if not isinstance(perms, list) or not perms:
                raise _Invalid("Policy json is invalid, error: [Permission must be a non-empty list]")
            for perm in perms:
                if perm not in _DATA_PERMISSIONS[rtype]:
                    raise _Invalid(f"Policy json is invalid, error: [Permission {perm!r} is not valid for ResourceType {rtype}]")


def _validate_encryption_policy(doc, own_name):
    if not isinstance(doc, dict):
        raise _Invalid("Policy json is invalid, error: [Encryption policy must be a JSON object]")
    _validate_rules(doc.get("Rules"), ("collection",), False)
    if not doc.get("AWSOwnedKey") and not doc.get("KmsARN"):
        raise _Invalid("Policy json is invalid, error: [Either AWSOwnedKey must be true or KmsARN must be specified]")
    # AWS docs: identical resource patterns may not appear in two encryption policies.
    patterns = {r for rule in doc["Rules"] for r in rule["Resource"]}
    for key, rec in _security_policies.items():
        if not key.startswith("encryption/") or rec["name"] == own_name:
            continue
        for rule in rec["policy"].get("Rules", []):
            clash = patterns.intersection(rule.get("Resource", []))
            if clash:
                raise _Invalid(
                    f"Policy json is invalid, error: [Resource {sorted(clash)[0]} is already used in "
                    f"encryption policy {rec['name']}]"
                )


def _validate_network_policy(doc):
    if not isinstance(doc, list) or not doc:
        raise _Invalid("Policy json is invalid, error: [Network policy must be a non-empty JSON array]")
    for statement in doc:
        if not isinstance(statement, dict):
            raise _Invalid("Policy json is invalid, error: [Network policy rule must be an object]")
        _validate_rules(statement.get("Rules"), ("collection", "dashboard"), False)
        public = statement.get("AllowFromPublic")
        if not public and not statement.get("SourceVPCEs") and not statement.get("SourceServices"):
            raise _Invalid(
                "Policy json is invalid, error: [AllowFromPublic must be true or SourceVPCEs/SourceServices specified]"
            )


def _validate_data_policy(doc):
    if not isinstance(doc, list) or not doc:
        raise _Invalid("Policy json is invalid, error: [Data access policy must be a non-empty JSON array]")
    for statement in doc:
        if not isinstance(statement, dict):
            raise _Invalid("Policy json is invalid, error: [Data access policy rule must be an object]")
        _validate_rules(statement.get("Rules"), tuple(_DATA_PERMISSIONS), True)
        principals = statement.get("Principal")
        if not isinstance(principals, list) or not principals:
            raise _Invalid("Policy json is invalid, error: [Principal must be a non-empty list]")
        for principal in principals:
            if not isinstance(principal, str) or "*" in principal:
                raise _Invalid(f"Policy json is invalid, error: [Principal {principal!r} is invalid; wildcards are not supported]")


def _policy_detail(rec, include_policy=True):
    out = {k: copy.deepcopy(v) for k, v in rec.items() if not k.startswith("_")}
    if not include_policy:
        out.pop("policy", None)
    return out


def _put_policy(store, kind, data, validate):
    ptype, name = data.get("type"), data.get("name")
    _validate_enum(ptype, "type", _SECURITY_POLICY_TYPES if kind == "security" else _ACCESS_POLICY_TYPES)
    _validate_name(name, "name", 32)
    key = f"{ptype}/{name}"
    if key in store:
        raise _Invalid(f"Policy with name {name} and type {ptype} already exists", "ConflictException")
    doc = _parse_policy_document(data.get("policy"))
    validate(ptype, name, doc)
    now = _now_ms()
    rec = {
        "type": ptype,
        "name": name,
        "policyVersion": _policy_version(now, 1),
        "description": data.get("description", ""),
        "policy": doc,
        "createdDate": now,
        "lastModifiedDate": now,
        "_revision": 1,
    }
    store[key] = rec
    return rec


def _update_policy(store, kind, data, validate):
    ptype, name = data.get("type"), data.get("name")
    _validate_enum(ptype, "type", _SECURITY_POLICY_TYPES if kind == "security" else _ACCESS_POLICY_TYPES)
    _validate_name(name, "name", 32)
    rec = store.get(f"{ptype}/{name}")
    if rec is None:
        raise _Invalid(f"Policy with name {name} and type {ptype} is not found", "ResourceNotFoundException")
    if data.get("policyVersion") != rec["policyVersion"]:
        raise _Invalid(
            "Policy version specified in the request refers to an older version and policy has since changed"
        )
    changed = False
    if "policy" in data and data["policy"] is not None:
        doc = _parse_policy_document(data["policy"])
        validate(ptype, name, doc)
        if doc != rec["policy"]:
            rec["policy"] = doc
            changed = True
    if "description" in data and data["description"] != rec["description"]:
        rec["description"] = data["description"]
        changed = True
    if changed:
        now = _now_ms()
        rec["_revision"] += 1
        rec["lastModifiedDate"] = now
        rec["policyVersion"] = _policy_version(now, rec["_revision"])
    return rec


def _list_policies(store, data, detail_key):
    ptype = data.get("type")
    filters = data.get("resource") or []
    items = []
    for key in sorted(store.keys()):
        rec = store[key]
        if rec["type"] != ptype:
            continue
        if filters:
            rules = rec["policy"] if isinstance(rec["policy"], list) else [rec["policy"]]
            resources = [r for st in rules for rule in st.get("Rules", []) for r in rule.get("Resource", [])]
            if not any(_pattern_matches(f, r) or _pattern_matches(r, f) for f in filters for r in resources):
                continue
        items.append(_policy_detail(rec, include_policy=False))
    page, token = _page(items, data)
    out = {detail_key: page}
    if token:
        out["nextToken"] = token
    return out


def _security_validator(ptype, name, doc):
    if ptype == "encryption":
        _validate_encryption_policy(doc, name)
    else:
        _validate_network_policy(doc)


def _access_validator(_ptype, _name, doc):
    _validate_data_policy(doc)


def _create_security_policy(data):
    return json_response({"securityPolicyDetail": _policy_detail(
        _put_policy(_security_policies, "security", data, _security_validator))})


def _get_security_policy(data):
    _validate_enum(data.get("type"), "type", _SECURITY_POLICY_TYPES)
    rec = _security_policies.get(f"{data.get('type')}/{data.get('name')}")
    if rec is None:
        raise _Invalid(f"Policy with name {data.get('name')} and type {data.get('type')} is not found",
                       "ResourceNotFoundException")
    return json_response({"securityPolicyDetail": _policy_detail(rec)})


def _update_security_policy(data):
    return json_response({"securityPolicyDetail": _policy_detail(
        _update_policy(_security_policies, "security", data, _security_validator))})


def _delete_security_policy(data):
    _validate_enum(data.get("type"), "type", _SECURITY_POLICY_TYPES)
    if _security_policies.pop(f"{data.get('type')}/{data.get('name')}", None) is None:
        raise _Invalid(f"Policy with name {data.get('name')} and type {data.get('type')} is not found",
                       "ResourceNotFoundException")
    return json_response({})


def _list_security_policies(data):
    _validate_enum(data.get("type"), "type", _SECURITY_POLICY_TYPES)
    return json_response(_list_policies(_security_policies, data, "securityPolicySummaries"))


def _create_access_policy(data):
    return json_response({"accessPolicyDetail": _policy_detail(
        _put_policy(_access_policies, "access", data, _access_validator))})


def _get_access_policy(data):
    _validate_enum(data.get("type"), "type", _ACCESS_POLICY_TYPES)
    rec = _access_policies.get(f"{data.get('type')}/{data.get('name')}")
    if rec is None:
        raise _Invalid(f"Policy with name {data.get('name')} and type {data.get('type')} is not found",
                       "ResourceNotFoundException")
    return json_response({"accessPolicyDetail": _policy_detail(rec)})


def _update_access_policy(data):
    return json_response({"accessPolicyDetail": _policy_detail(
        _update_policy(_access_policies, "access", data, _access_validator))})


def _delete_access_policy(data):
    _validate_enum(data.get("type"), "type", _ACCESS_POLICY_TYPES)
    if _access_policies.pop(f"{data.get('type')}/{data.get('name')}", None) is None:
        raise _Invalid(f"Policy with name {data.get('name')} and type {data.get('type')} is not found",
                       "ResourceNotFoundException")
    return json_response({})


def _list_access_policies(data):
    _validate_enum(data.get("type"), "type", _ACCESS_POLICY_TYPES)
    return json_response(_list_policies(_access_policies, data, "accessPolicySummaries"))


def _get_policies_stats(_data):
    enc = sum(1 for k in _security_policies.keys() if k.startswith("encryption/"))
    net = sum(1 for k in _security_policies.keys() if k.startswith("network/"))
    dat = len(_access_policies)
    return json_response({
        "AccessPolicyStats": {"DataPolicyCount": dat},
        "SecurityPolicyStats": {"EncryptionPolicyCount": enc, "NetworkPolicyCount": net},
        "SecurityConfigStats": {"SamlConfigCount": 0},
        "LifecyclePolicyStats": {"RetentionPolicyCount": 0},
        "TotalPolicyCount": enc + net + dat,
    })


# ---------------------------------------------------------------------------
# Collections
# ---------------------------------------------------------------------------

def _matching_encryption_key(name):
    """The KMS key of the most specific encryption policy rule matching ``name``."""
    best = None
    for key, rec in _security_policies.items():
        if not key.startswith("encryption/"):
            continue
        for rule in rec["policy"].get("Rules", []):
            for resource in rule.get("Resource", []):
                pattern = resource[len("collection/"):]
                if not _pattern_matches(pattern, name):
                    continue
                exact = "*" not in pattern
                rank = (exact, len(pattern))
                if best is None or rank > best[0]:
                    doc = rec["policy"]
                    best = (rank, doc.get("KmsARN") or "auto")
    return best[1] if best else None


def _collection_detail(rec, fields):
    return {k: copy.deepcopy(rec[k]) for k in fields if rec.get(k) is not None}


_DETAIL_FIELDS = (
    "id", "name", "status", "type", "description", "arn", "kmsKeyArn", "standbyReplicas",
    "deletionProtection", "vectorOptions", "createdDate", "lastModifiedDate",
    "collectionEndpoint", "dashboardEndpoint", "failureCode", "failureMessage",
)
_CREATE_FIELDS = (
    "id", "name", "status", "type", "description", "arn", "kmsKeyArn", "standbyReplicas",
    "deletionProtection", "vectorOptions", "createdDate", "lastModifiedDate",
)
_SUMMARY_FIELDS = ("id", "name", "status", "arn", "kmsKeyArn")


def _find_by_name(name):
    for rec in _collections.values():
        if rec["name"] == name:
            return rec
    return None


def _create_collection(data):
    name = data.get("name")
    _validate_name(name, "name", 64)
    ctype = data.get("type", "TIMESERIES")
    _validate_enum(ctype, "type", _COLLECTION_TYPES)
    standby = data.get("standbyReplicas", "ENABLED")
    _validate_enum(standby, "standbyReplicas", ("ENABLED", "DISABLED"))
    deletion = data.get("deletionProtection", "DISABLED")
    _validate_enum(deletion, "deletionProtection", ("ENABLED", "DISABLED"))
    if data.get("collectionGroupName"):
        raise _Invalid("Collection groups are not supported by MiniStack")
    if len(data.get("description") or "") > 1000:
        raise _Invalid("Description must be at most 1000 characters")
    tags = data.get("tags") or []
    _validate_tags(tags)
    if _find_by_name(name):
        raise _Invalid(f"A collection with name {name} already exists", "ConflictException")

    encryption = data.get("encryptionConfig") or {}
    kms = encryption.get("kmsKeyArn") or ("auto" if encryption.get("aWSOwnedKey") else None)
    if kms is None:
        kms = _matching_encryption_key(name)
    if kms is None:
        raise _Invalid(
            f"No matching security policy of encryption type found for collection name: {name}. "
            "Please create security policy of encryption type for this collection."
        )

    now = _now_ms()
    cid = _new_collection_id()
    endpoint = _collection_endpoint(cid)
    rec = {
        "id": cid,
        "name": name,
        "status": "CREATING",
        "type": ctype,
        "description": data.get("description", ""),
        "arn": _collection_arn(cid),
        "kmsKeyArn": kms,
        "standbyReplicas": standby,
        "deletionProtection": deletion,
        "vectorOptions": copy.deepcopy(data.get("vectorOptions")),
        "createdDate": now,
        "lastModifiedDate": now,
        "collectionEndpoint": endpoint,
        "dashboardEndpoint": f"{endpoint}/_dashboards",
    }
    _collections[cid] = rec
    if tags:
        _tags[rec["arn"]] = _merge_tags([], tags)
    _start_provisioning(rec)
    return json_response({"createCollectionDetail": _collection_detail(rec, _CREATE_FIELDS)})


def _batch_get_collection(data):
    ids, names = data.get("ids"), data.get("names")
    if bool(ids) == bool(names):
        raise _Invalid("You must provide either ids or names, but not both")
    details, errors = [], []
    for value in ids or names:
        rec = _collections.get(value) if ids else _find_by_name(value)
        if rec is None:
            err = {"id": value} if ids else {"name": value}
            err.update({"errorCode": "NOT_FOUND", "errorMessage": "The specified Collection is not found."})
            errors.append(err)
        else:
            details.append(_collection_detail(rec, _DETAIL_FIELDS))
    return json_response({"collectionDetails": details, "collectionErrorDetails": errors})


def _list_collections(data):
    filters = data.get("collectionFilters") or {}
    items = []
    for rec in sorted(_collections.values(), key=lambda r: r["createdDate"]):
        if filters.get("name") and rec["name"] != filters["name"]:
            continue
        if filters.get("status") and rec["status"] != filters["status"]:
            continue
        items.append(_collection_detail(rec, _SUMMARY_FIELDS))
    page, token = _page(items, data)
    out = {"collectionSummaries": page}
    if token:
        out["nextToken"] = token
    return json_response(out)


def _update_collection(data):
    cid = data.get("id")
    rec = _collections.get(cid)
    if rec is None:
        raise _Invalid(f"Collection with id {cid} is not found", "ResourceNotFoundException")
    if rec["status"] != "ACTIVE":
        raise _Invalid(f"Collection with id {cid} is not in ACTIVE state", "ConflictException")
    if "description" in data:
        rec["description"] = data["description"]
    if "deletionProtection" in data:
        _validate_enum(data["deletionProtection"], "deletionProtection", ("ENABLED", "DISABLED"))
        rec["deletionProtection"] = data["deletionProtection"]
    if "vectorOptions" in data:
        rec["vectorOptions"] = copy.deepcopy(data["vectorOptions"])
    rec["lastModifiedDate"] = _now_ms()
    return json_response({"updateCollectionDetail": _collection_detail(rec, (
        "id", "name", "status", "type", "description", "vectorOptions", "arn",
        "createdDate", "lastModifiedDate", "deletionProtection",
    ))})


def _delete_collection(data):
    cid = data.get("id")
    with resource_lock("opensearchserverless", cid or ""):
        rec = _collections.get(cid)
        if rec is None:
            raise _Invalid(f"Collection with id {cid} is not found", "ResourceNotFoundException")
        if rec["status"] not in ("ACTIVE", "FAILED", "UPDATE_FAILED"):
            raise _Invalid(f"Collection with id {cid} is in {rec['status']} state", "ConflictException")
        if rec.get("deletionProtection") == "ENABLED":
            raise _Invalid(f"Collection with id {cid} has deletion protection enabled")
        _collections.pop(cid, None)
        rec["_deleting"] = True
    _tags.pop(rec["arn"], None)
    _remove_container(rec.get("_ContainerId"))
    _remove_volume(rec.get("_VolumeName"))
    return json_response({"deleteCollectionDetail": {
        "id": rec["id"], "name": rec["name"], "status": "DELETING",
        "deletionProtection": rec.get("deletionProtection", "DISABLED"),
    }})


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def _validate_tags(tags):
    if not isinstance(tags, list) or len(tags) > 50:
        raise _Invalid("Tags must be a list of at most 50 items")
    for tag in tags:
        if not isinstance(tag, dict) or not tag.get("key") or "value" not in tag:
            raise _Invalid("Each tag must have a key and a value")


def _merge_tags(existing, new):
    by_key = {t["key"]: t for t in existing}
    for tag in new:
        by_key[tag["key"]] = {"key": tag["key"], "value": tag["value"]}
    return list(by_key.values())


def _resolve_tag_arn(arn):
    try:
        spec = parse_arn(arn or "")
    except ArnParseError:
        raise _Invalid(f"Invalid resource ARN: {arn}")
    if spec.service != "aoss" or spec.account_id != get_account_id() or spec.region != get_region():
        raise _Invalid(f"Invalid resource ARN: {arn}")
    if not spec.resource.startswith("collection/"):
        raise _Invalid(f"Invalid resource ARN: {arn}")
    rec = _collections.get(spec.resource[len("collection/"):])
    if rec is None or rec["arn"] != arn:
        raise _Invalid(f"Resource {arn} is not found", "ResourceNotFoundException")
    return arn


def _tag_resource(data):
    arn = _resolve_tag_arn(data.get("resourceArn"))
    tags = data.get("tags") or []
    _validate_tags(tags)
    merged = _merge_tags(_tags.get(arn) or [], tags)
    if len(merged) > 50:
        raise _Invalid("A resource can have at most 50 tags", "ServiceQuotaExceededException")
    _tags[arn] = merged
    return json_response({})


def _untag_resource(data):
    arn = _resolve_tag_arn(data.get("resourceArn"))
    keys = set(data.get("tagKeys") or [])
    _tags[arn] = [t for t in _tags.get(arn) or [] if t["key"] not in keys]
    return json_response({})


def _list_tags_for_resource(data):
    arn = _resolve_tag_arn(data.get("resourceArn"))
    return json_response({"tags": copy.deepcopy(_tags.get(arn) or [])})


# ---------------------------------------------------------------------------
# Engine containers
# ---------------------------------------------------------------------------

def _get_docker():
    global _docker_client
    if _docker_client is None:
        from ministack.services import opensearch

        _docker_client = opensearch._get_docker()
    return _docker_client


def _dataplane_enabled():
    from ministack.services import opensearch

    return opensearch.DATAPLANE_ENABLED


def _remove_container(container_id):
    if container_id:
        from ministack.services import opensearch

        opensearch._remove_container_by_id(container_id)


def _volume_name(cid):
    return f"ministack-aoss-{get_region()}-{cid}-data"


def _remove_volume(volume_name):
    """Best-effort teardown of a collection's persisted data volume."""
    if not volume_name:
        return
    docker = _get_docker()
    if docker is None:
        return
    try:
        docker.volumes.get(volume_name).remove()
    except Exception as e:
        logger.warning("OpenSearch Serverless: failed to remove volume %s: %s", volume_name, e)


def _wait_for_engine(host, port, deadline):
    while time.monotonic() < deadline:
        try:
            conn = http.client.HTTPConnection(host, port, timeout=2)
            conn.request("GET", "/")
            ok = conn.getresponse().status == 200
            conn.close()
            if ok:
                return True
        except (OSError, http.client.HTTPException):
            pass
        time.sleep(1)
    return False


def _start_provisioning(rec):
    spawn_background(_provision, rec, thread_name=f"ministack-aoss-start-{rec['id']}")


def _provision(rec):
    """Bring a collection to ACTIVE, starting its engine container when enabled."""
    cid = rec["id"]
    container_id, engine, failure = None, None, None
    volume_name = None
    if _dataplane_enabled():
        docker = _get_docker()
        if docker is None:
            failure = "Docker is not available to run the collection's OpenSearch engine"
        else:
            from ministack.services import opensearch

            labels = {
                **container_reaper.own_labels(_CONTAINER_LABEL),
                "com.ministack.service": _CONTAINER_LABEL,
                "com.ministack.collection": cid,
                "com.ministack.region": get_region(),
            }
            name = f"ministack-aoss-{get_region()}-{cid}"
            try:
                if persistence.PERSIST_STATE:
                    # Same collection id survives a restart (persisted state
                    # is keyed by it), so the volume it names is the one a
                    # prior boot already created — a warm boot reattaches it.
                    volume_name = rec.get("_VolumeName") or _volume_name(cid)
                    container_id, host, port = opensearch.start_engine_container(
                        docker, name, labels,
                        volumes={volume_name: {"bind": _ENGINE_DATA_DIR, "mode": "rw"}},
                    )
                else:
                    container_id, host, port = opensearch.start_engine_container(docker, name, labels)
                if _wait_for_engine(host, port, time.monotonic() + _ENGINE_READY_TIMEOUT):
                    engine = f"{host}:{port}"
                else:
                    failure = "The collection's OpenSearch engine did not become ready"
            except Exception as e:
                logger.warning("OpenSearch Serverless: engine start failed for %s: %s", cid, e)
                failure = f"The collection's OpenSearch engine failed to start: {e}"
    with resource_lock("opensearchserverless", cid):
        live = _collections.get(cid)
        orphaned = live is not rec or rec.get("_deleting")
        if not orphaned:
            rec["_ContainerId"] = container_id
            rec["_Engine"] = engine
            if volume_name:
                rec["_VolumeName"] = volume_name
            if failure:
                rec["status"] = "FAILED"
                rec["failureCode"] = "INTERNAL_ERROR"
                rec["failureMessage"] = failure
            else:
                rec["status"] = "ACTIVE"
            rec["lastModifiedDate"] = _now_ms()
    if orphaned or failure:
        _remove_container(container_id)


def _live_container_ids():
    return {rec.get("_ContainerId") for _k, rec in _collections.all_items() if rec.get("_ContainerId")}


container_reaper.register_live_ids(_CONTAINER_LABEL, _live_container_ids)


# ---------------------------------------------------------------------------
# Data plane
# ---------------------------------------------------------------------------

# Host of a collection endpoint: <collection-id>.<region>.aoss.<suffix>[:port]
_DATAPLANE_HOST_RE = re.compile(r"^(?P<id>[a-z0-9]{3,40})\.[a-z0-9-]+\.aoss\.")

# AWS docs "Supported operations and plugins in Amazon OpenSearch Serverless" (serverless-genref.html):
# everything else is proxied, these answer 404 as they do on real AOSS.
_DENIED_FIRST_SEGMENTS = {"_refresh", "_cluster", "_nodes", "_reindex", "_snapshot"}

# Write operations that carry a caller-chosen document id; AWS docs "Choosing
# a collection type" refuse these on a TIMESERIES collection.
_TIMESERIES_RESTRICTED = {("POST", "_create"), ("PUT", "_create"), ("POST", "_update"), ("POST", "_doc"), ("PUT", "_doc")}


def _dataplane_denied(path, segments, query_params):
    if path == "":
        return True
    if segments[0] in _DENIED_FIRST_SEGMENTS:
        return True
    if path == "_cat/nodes":
        return True
    if len(segments) == 2 and segments[1] == "_refresh":
        return True
    if path == "_search/scroll" or "scroll" in (query_params or {}):
        return True
    return False


def _operation_supported(method, raw_path, collection_type, query_params=None):
    path = raw_path.strip("/")
    segments = path.split("/") if path else [""]
    if _dataplane_denied(path, segments, query_params):
        return False
    if (
        collection_type == "TIMESERIES" and len(segments) == 3
        and not segments[0].startswith("_") and segments[2]
    ):
        return (method, segments[1]) not in _TIMESERIES_RESTRICTED
    return True


def _aoss_error(status, reason, error_type):
    body = {"status": status, "request-id": new_uuid(), "error": {"reason": reason, "type": error_type}}
    return status, {"Content-Type": "application/json"}, json.dumps(body).encode()


def _signed_for_aoss(headers, query_params):
    auth = headers.get("authorization", "")
    credential = auth if auth else " ".join(query_params.get("X-Amz-Credential", []))
    match = re.search(r"[^/\s=]+/\d{8}/[^/]+/([^/]+)/aws4_request", credential)
    return bool(match) and match.group(1) == "aoss"


_HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
    "transfer-encoding", "upgrade", "content-length",
}


def _proxy(engine, method, target, headers, body):
    host, _, port = engine.rpartition(":")
    forward = {
        k: v for k, v in headers.items()
        if k not in _HOP_BY_HOP and k != "host" and k != "authorization" and not k.startswith("x-amz-")
    }
    conn = http.client.HTTPConnection(host, int(port), timeout=_PROXY_TIMEOUT)
    try:
        conn.request(method, target, body=body if body else None, headers=forward)
        resp = conn.getresponse()
        data = resp.read()
        out_headers = {
            k: v for k, v in resp.getheaders() if k.lower() not in _HOP_BY_HOP
        }
        return resp.status, out_headers, data
    finally:
        conn.close()


def _find_collection_anywhere(collection_id):
    """Collection ids are globally unique, like the endpoint hostnames built from them."""
    return next((rec for (_a, _r, cid), rec in _collections.all_items() if cid == collection_id), None)


async def handle_dataplane(method, host, raw_path, query_string, headers, body, query_params):
    """Serve a request addressed to a collection endpoint, or None if ``host`` is not one."""
    match = _DATAPLANE_HOST_RE.match((host or "").split(":")[0].lower())
    if not match:
        return None
    rec = _find_collection_anywhere(match.group("id"))
    if rec is None:
        return _aoss_error(404, "404 Not Found", "NotFound")
    # AWS docs "Troubleshooting Amazon OpenSearch Serverless": every request must be
    # SigV4-signed for service `aoss` and carry x-amz-content-sha256, else 403.
    if not _signed_for_aoss(headers, query_params) or not headers.get("x-amz-content-sha256"):
        return _aoss_error(403, "403 Forbidden", "Forbidden")
    # 404 for unsupported APIs such as _refresh: langchain-ai/langchainjs#2302 quoting AWS.
    if not _operation_supported(method, raw_path, rec["type"], query_params):
        return _aoss_error(404, "404 Not Found", "NotFound")
    engine = rec.get("_Engine")
    if rec["status"] != "ACTIVE" or not engine:
        return _aoss_error(503, "503 Service Unavailable", "ServiceUnavailable")
    target = raw_path + (f"?{query_string}" if query_string else "")
    try:
        status, resp_headers, data = await run_offloop(_proxy, engine, method, target, headers, body)
    except (OSError, http.client.HTTPException) as e:
        logger.warning("OpenSearch Serverless: engine for %s unreachable: %s", rec["id"], e)
        return _aoss_error(503, "503 Service Unavailable", "ServiceUnavailable")
    return status, resp_headers, data


# ---------------------------------------------------------------------------
# Control-plane dispatcher
# ---------------------------------------------------------------------------

_HANDLERS = {
    "CreateCollection": _create_collection,
    "BatchGetCollection": _batch_get_collection,
    "ListCollections": _list_collections,
    "UpdateCollection": _update_collection,
    "DeleteCollection": _delete_collection,
    "CreateSecurityPolicy": _create_security_policy,
    "GetSecurityPolicy": _get_security_policy,
    "UpdateSecurityPolicy": _update_security_policy,
    "DeleteSecurityPolicy": _delete_security_policy,
    "ListSecurityPolicies": _list_security_policies,
    "CreateAccessPolicy": _create_access_policy,
    "GetAccessPolicy": _get_access_policy,
    "UpdateAccessPolicy": _update_access_policy,
    "DeleteAccessPolicy": _delete_access_policy,
    "ListAccessPolicies": _list_access_policies,
    "GetPoliciesStats": _get_policies_stats,
    "TagResource": _tag_resource,
    "UntagResource": _untag_resource,
    "ListTagsForResource": _list_tags_for_resource,
}

# Container teardown talks to Docker; run those off the event loop.
_OFFLOOP = {"DeleteCollection"}


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".", 1)[1] if target.startswith("OpenSearchServerless.") else ""
    try:
        data = json.loads(body) if body else {}
    except json.JSONDecodeError:
        return _error("SerializationException", "Invalid JSON in request body")
    if not isinstance(data, dict):
        return _error("SerializationException", "Request body must be a JSON object")
    handler = _HANDLERS.get(action)
    if handler is None:
        return _error("UnknownOperationException", f"Unknown operation: {action or target}")
    try:
        if action in _OFFLOOP:
            return await run_offloop(handler, data)
        return handler(data)
    except _Invalid as e:
        return _error(e.code, e.message)
