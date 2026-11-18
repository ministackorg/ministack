# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""
ECR (Elastic Container Registry) Emulator.
JSON-based API via X-Amz-Target (prefix: AmazonEC2ContainerRegistry_V20150921).
"""

import asyncio
import base64
import copy
import hashlib
import json
import logging
import os
import re
import time

from ministack.core.arn import ArnParseError, parse_arn
from ministack.core.responses import (
    AccountRegionScopedDict,
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
    request_scope,
    set_request_account_id,
    set_request_region,
)

logger = logging.getLogger("ecr")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_repositories = AccountRegionScopedDict()
_images = AccountRegionScopedDict()
_lifecycle_policies = AccountRegionScopedDict()
_repo_policies = AccountRegionScopedDict()
# Docker Registry HTTP API V2 backing storage.
# _layer_blobs[repo] = {digest: bytes}     — finalised layer + config bytes addressable by digest.
# _manifest_blobs[repo] = {digest: bytes}  — raw manifest bytes addressable by digest (for HEAD/GET by digest).
# _uploads[repo] = {upload_uuid: bytearray} — in-flight chunked uploads (cleared on PUT).
_layer_blobs = AccountRegionScopedDict()
_manifest_blobs = AccountRegionScopedDict()
_uploads = AccountRegionScopedDict()
# Pull-through cache rules (ecrRepositoryPrefix -> rule, in creation order) and the registry policy ("policy").
_ptc_rules = AccountRegionScopedDict()
_registry_policies = AccountRegionScopedDict()
_creation_templates = AccountRegionScopedDict()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "repositories": copy.deepcopy(_repositories),
        "images": copy.deepcopy(_images),
        "lifecycle_policies": copy.deepcopy(_lifecycle_policies),
        "repo_policies": copy.deepcopy(_repo_policies),
        "layer_blobs": copy.deepcopy(_layer_blobs),
        "manifest_blobs": copy.deepcopy(_manifest_blobs),
        "pull_through_cache_rules": copy.deepcopy(_ptc_rules),
        "registry_policies": copy.deepcopy(_registry_policies),
        "creation_templates": copy.deepcopy(_creation_templates),
        # _uploads is intentionally NOT persisted — in-flight upload sessions
        # do not survive a restart, matching real registry behaviour where
        # the client retries the upload from scratch after a 404 on PATCH.
    }


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not data:
        return

    restored_repositories = data.get("repositories", {})
    legacy_repo_regions = {}
    if isinstance(restored_repositories, AccountScopedDict):
        legacy_repo_regions = {
            (account_id, name): _repositories._region_for_legacy_value(
                name, repository
            )
            for (account_id, name), repository in restored_repositories._data.items()
        }
    elif isinstance(restored_repositories, dict):
        account_id = get_account_id()
        legacy_repo_regions = {
            (account_id, name): _repositories._region_for_legacy_value(
                name, repository
            )
            for name, repository in restored_repositories.items()
        }

    _repositories.update(restored_repositories)
    _ptc_rules.update(data.get("pull_through_cache_rules", {}))
    _registry_policies.update(data.get("registry_policies", {}))
    _creation_templates.update(data.get("creation_templates", {}))
    for store, key in (
        (_images, "images"),
        (_lifecycle_policies, "lifecycle_policies"),
        (_repo_policies, "repo_policies"),
        (_layer_blobs, "layer_blobs"),
        (_manifest_blobs, "manifest_blobs"),
    ):
        _restore_repository_child_store(
            store, data.get(key, {}), legacy_repo_regions
        )


def _restore_repository_child_store(store, restored, legacy_repo_regions):
    """Adopt legacy name-keyed child state into its repository's region."""
    if isinstance(restored, AccountRegionScopedDict):
        store.update(restored)
        return

    if isinstance(restored, AccountScopedDict):
        entries = restored._data.items()
    elif isinstance(restored, dict):
        account_id = get_account_id()
        entries = (
            ((account_id, name), value) for name, value in restored.items()
        )
    else:
        store.update(restored)
        return

    for (account_id, name), value in entries:
        region = legacy_repo_regions.get((account_id, name), get_region())
        store.set_scoped(account_id, region, name, value)




def _repo_arn(name):
    return f"arn:aws:ecr:{get_region()}:{get_account_id()}:repository/{name}"


def _registry_id():
    return get_account_id()


def _repo_uri(name):
    return f"{get_account_id()}.dkr.ecr.{get_region()}.amazonaws.com/{name}"


def _image_digest(manifest):
    raw = manifest.encode() if isinstance(manifest, str) else manifest
    return "sha256:" + hashlib.sha256(raw).hexdigest()


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    if action in _PTC_WIRE_ACTIONS:
        try:
            data = _PtcReader(body.decode("utf-8", "replace") if isinstance(body, (bytes, bytearray))
                              else body or "").parse()
        except _PtcWire as exc:
            return exc.response
    else:
        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            return error_response_json("SerializationException", "Invalid JSON", 400)

    handlers = {
        "CreateRepository": _create_repository,
        "DescribeRepositories": _describe_repositories,
        "DeleteRepository": _delete_repository,
        "ListImages": _list_images,
        "PutImage": _put_image,
        "BatchGetImage": _batch_get_image,
        "BatchDeleteImage": _batch_delete_image,
        "GetAuthorizationToken": _get_authorization_token,
        "GetRepositoryPolicy": _get_repository_policy,
        "SetRepositoryPolicy": _set_repository_policy,
        "DeleteRepositoryPolicy": _delete_repository_policy,
        "PutLifecyclePolicy": _put_lifecycle_policy,
        "GetLifecyclePolicy": _get_lifecycle_policy,
        "DeleteLifecyclePolicy": _delete_lifecycle_policy,
        "DescribeImages": _describe_images,
        "ListTagsForResource": _list_tags_for_resource,
        "TagResource": _tag_resource,
        "UntagResource": _untag_resource,
        "PutImageTagMutability": _put_image_tag_mutability,
        "PutImageScanningConfiguration": _put_image_scanning_configuration,
        "DescribeRegistry": _describe_registry,
        "GetDownloadUrlForLayer": _get_download_url_for_layer,
        "BatchCheckLayerAvailability": _batch_check_layer_availability,
        "InitiateLayerUpload": _initiate_layer_upload,
        "UploadLayerPart": _upload_layer_part,
        "CompleteLayerUpload": _complete_layer_upload,
        "CreatePullThroughCacheRule": _create_pull_through_cache_rule,
        "UpdatePullThroughCacheRule": _update_pull_through_cache_rule,
        "DeletePullThroughCacheRule": _delete_pull_through_cache_rule,
        "DescribePullThroughCacheRules": _describe_pull_through_cache_rules,
        "ValidatePullThroughCacheRule": _validate_pull_through_cache_rule,
        "PutRegistryPolicy": _put_registry_policy,
        "GetRegistryPolicy": _get_registry_policy,
        "DeleteRegistryPolicy": _delete_registry_policy,
        "CreateRepositoryCreationTemplate": _create_creation_template,
        "UpdateRepositoryCreationTemplate": _update_creation_template,
        "DeleteRepositoryCreationTemplate": _delete_creation_template,
        "DescribeRepositoryCreationTemplates": _describe_creation_templates,
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)

    if action in _PTC_WIRE_ACTIONS:
        if isinstance(data, list):
            return _ptc_serialization_error()
        if data is None:
            return _ptc_serialization_error("Request payload does not contain the input model")
        if not isinstance(data, dict):
            return _ptc_serialization_error("Unexpected value type in payload")
        wrong = _ptc_member_type_error(action, data)
        if wrong:
            return wrong
        if action not in _PTC_ACTIONS:
            return handler(data)
        if action == "ValidatePullThroughCacheRule":
            return await asyncio.to_thread(_ptc_in_registry, handler, data)
        return _ptc_in_registry(handler, data)
    if action in _RCT_ACTIONS:
        if not isinstance(data, dict):
            return _ptc_serialization_error(None if isinstance(data, list) else
                                            "Request payload does not contain the input model" if data is None
                                            else "Unexpected value type in payload")
        return handler(data)
    if action == "BatchGetImage":
        return _batch_get_image(data)
    return handler(data)


def _create_repository(data):
    name = data.get("repositoryName", "")
    if not name:
        return error_response_json("InvalidParameterException", "repositoryName is required", 400)
    if name in _repositories:
        return error_response_json("RepositoryAlreadyExistsException",
                                   f"The repository with name '{name}' already exists in the registry with id '{_registry_id()}'", 400)

    repo = {
        "repositoryArn": _repo_arn(name),
        "registryId": _registry_id(),
        "repositoryName": name,
        "repositoryUri": _repo_uri(name),
        "createdAt": int(time.time()),
        "imageTagMutability": data.get("imageTagMutability", "MUTABLE"),
        "imageScanningConfiguration": data.get("imageScanningConfiguration", {"scanOnPush": False}),
        "encryptionConfiguration": data.get("encryptionConfiguration", {"encryptionType": "AES256"}),
        "tags": data.get("tags", []),
    }
    if data.get("imageTagMutabilityExclusionFilters"):
        repo["imageTagMutabilityExclusionFilters"] = data["imageTagMutabilityExclusionFilters"]
    _repositories[name] = repo
    _images[name] = []
    return json_response({"repository": _repo_shape(repo)})


def _paged(data, key, items):
    try:
        start = max(int(data.get("nextToken") or 0), 0)
    except (TypeError, ValueError):
        start = 0
    end = start + (data.get("maxResults") or 100)
    body = {key: items[start:end]}
    if end < len(items):
        body["nextToken"] = str(end)
    return json_response(body)


def _describe_repositories(data):
    names = data.get("repositoryNames", [])

    if names:
        repos = []
        for n in names:
            if n not in _repositories:
                return error_response_json("RepositoryNotFoundException",
                                           f"The repository with name '{n}' does not exist in the registry with id '{_registry_id()}'", 400)
            repos.append(_repositories[n])
        return json_response({"repositories": [_repo_shape(r) for r in repos]})
    return _paged(data, "repositories", [_repo_shape(r) for r in _repositories.values()])


def _delete_repository(data):
    name = data.get("repositoryName", "")
    force = data.get("force", False)

    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)

    if not force and _images.get(name):
        return error_response_json("RepositoryNotEmptyException",
                                   f"The repository with name '{name}' in registry with id '{_registry_id()}' "
                                   "cannot be deleted because it still contains images", 400)

    repo = _repositories.pop(name)
    _images.pop(name, None)
    _lifecycle_policies.pop(name, None)
    _repo_policies.pop(name, None)
    return json_response({"repository": _repo_shape(repo)})


def _put_image(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)

    manifest = data.get("imageManifest", "")
    manifest_type = data.get("imageManifestMediaType",
                             "application/vnd.docker.distribution.manifest.v2+json")
    tag = data.get("imageTag")
    digest = data.get("imageDigest") or _image_digest(manifest)

    repo = _repositories[name]
    if tag and repo.get("imageTagMutability") == "IMMUTABLE":
        for img in _images[name]:
            if tag in img.get("imageTags", []):
                return error_response_json("ImageTagAlreadyExistsException",
                                           f"The image tag '{tag}' already exists", 400)

    if tag:
        for img in _images[name]:
            tags = img.get("imageTags", [])
            if tag in tags:
                tags.remove(tag)

    image = {
        "registryId": _registry_id(),
        "repositoryName": name,
        "imageId": {"imageDigest": digest},
        "imageManifest": manifest,
        "imageManifestMediaType": manifest_type,
        "imageTags": [tag] if tag else [],
        "imagePushedAt": int(time.time()),
        "imageDigest": digest,
    }
    if tag:
        image["imageId"]["imageTag"] = tag

    existing = next((img for img in _images[name] if img["imageDigest"] == digest), None)
    if existing:
        if tag:
            existing.setdefault("imageTags", [])
            if tag not in existing["imageTags"]:
                existing["imageTags"].append(tag)
            existing["imageId"]["imageTag"] = tag
        image = existing
    else:
        _images[name].append(image)

    return json_response({"image": {
        "registryId": _registry_id(),
        "repositoryName": name,
        "imageId": image["imageId"],
        "imageManifest": manifest,
        "imageManifestMediaType": manifest_type,
    }})


_REPO_NAME_PATTERN = r"[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*"
_REPO_NAME_RE = re.compile(_REPO_NAME_PATTERN)


def _batch_get_image_refused(data):
    """BatchGetImage's request errors: a malformed imageIds, a repositoryName off the pattern, no imageIds."""
    name, image_ids = data.get("repositoryName"), data.get("imageIds")
    if isinstance(image_ids, dict):
        return _ptc_serialization_error("Start of structure or map found where not expected")
    if image_ids is not None and not isinstance(image_ids, list):
        return _ptc_serialization_error("Unexpected field type")
    for item in image_ids or []:
        if isinstance(item, list):
            return _ptc_serialization_error()
        if not isinstance(item, dict):
            return _ptc_serialization_error("Unexpected value type in payload")
    if isinstance(name, str) and not _REPO_NAME_RE.fullmatch(name):
        return _invalid("Invalid parameter at 'repositoryName' failed to satisfy constraint: "
                        f"'must satisfy regular expression '{_REPO_NAME_PATTERN}''")
    if not image_ids:
        return _invalid("Invalid parameter at 'imageIds' failed to satisfy constraint: "
                        "'Member must have length greater than or equal to 1'")
    return None


def _batch_get_image(data):
    """BatchGetImage; the API call never fills a pull-through cache (AWS capture: RepositoryNotFound)."""
    refused = _batch_get_image_refused(data)
    if refused:
        return refused
    name = data.get("repositoryName", "")
    image_ids = data.get("imageIds", [])
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)

    found = []
    failures = []

    for iid in image_ids:
        match = _find_image(name, iid)
        if match:
            found.append({
                "registryId": _registry_id(),
                "repositoryName": name,
                "imageId": match["imageId"],
                "imageManifest": match.get("imageManifest", "{}"),
                "imageManifestMediaType": match.get("imageManifestMediaType",
                    "application/vnd.docker.distribution.manifest.v2+json"),
            })
        else:
            failures.append({
                "imageId": iid,
                "failureCode": "ImageNotFound",
                "failureReason": "Requested image not found",
            })

    return json_response({"images": found, "failures": failures})


def _batch_delete_image(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)

    image_ids = data.get("imageIds", [])
    deleted = []
    failures = []

    for iid in image_ids:
        match = _find_image(name, iid)
        if match:
            _images[name].remove(match)
            deleted.append(match["imageId"])
        else:
            failures.append({
                "imageId": iid,
                "failureCode": "ImageNotFound",
                "failureReason": "Requested image not found",
            })

    return json_response({"imageIds": deleted, "failures": failures})


def _list_images(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)

    tag_status = data.get("filter", {}).get("tagStatus")
    result = []
    for img in _images.get(name, []):
        tags = img.get("imageTags", [])
        if tag_status == "TAGGED" and not tags:
            continue
        if tag_status == "UNTAGGED" and tags:
            continue
        result.append(img["imageId"])

    return _paged(data, "imageIds", result)


def _describe_images(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)

    image_ids = data.get("imageIds")
    images = _images.get(name, [])

    if image_ids:
        filtered = []
        for iid in image_ids:
            match = _find_image(name, iid)
            if match:
                filtered.append(match)
        images = filtered

    details = []
    for img in images:
        manifest = img.get("imageManifest", "{}")
        details.append({
            "registryId": _registry_id(),
            "repositoryName": name,
            "imageDigest": img["imageDigest"],
            "imageTags": img.get("imageTags", []),
            "imageSizeInBytes": len(manifest),
            "imagePushedAt": img.get("imagePushedAt", int(time.time())),
            "imageManifestMediaType": img.get("imageManifestMediaType",
                "application/vnd.docker.distribution.manifest.v2+json"),
            "artifactMediaType": img.get("imageManifestMediaType",
                "application/vnd.docker.distribution.manifest.v2+json"),
        })

    if image_ids:
        return json_response({"imageDetails": details})
    return _paged(data, "imageDetails", details)


def _get_authorization_token(data):
    token = base64.b64encode(b"AWS:ministack-auth-token").decode()
    return json_response({
        "authorizationData": [{
            "authorizationToken": token,
            "expiresAt": int(time.time()) + 43200,
            "proxyEndpoint": f"https://{get_account_id()}.dkr.ecr.{get_region()}.amazonaws.com",
        }]
    })


def repository_policy_for_arn(resource_arn: str) -> tuple[str, str] | None:
    """The repository policy and owner account, for the identity check."""
    name = resource_arn.partition(":repository/")[2]
    if name not in _repositories:
        return None
    return _repo_policies.get(name) or "", get_account_id()


def _get_repository_policy(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    if name not in _repo_policies:
        return error_response_json("RepositoryPolicyNotFoundException",
                                   f"Repository policy does not exist for the repository with name '{name}' "
                                   f"in the registry with id '{_registry_id()}'", 400)
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "policyText": _repo_policies[name],
    })


def _set_repository_policy(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    policy = data.get("policyText", "")
    _repo_policies[name] = policy
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "policyText": policy,
    })


def _delete_repository_policy(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    if name not in _repo_policies:
        return error_response_json("RepositoryPolicyNotFoundException",
                                   f"Repository policy does not exist for the repository with name '{name}' "
                                   f"in the registry with id '{_registry_id()}'", 400)
    policy = _repo_policies.pop(name)
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "policyText": policy,
    })


def _put_lifecycle_policy(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    policy = data.get("lifecyclePolicyText", "")
    _lifecycle_policies[name] = policy
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "lifecyclePolicyText": policy,
    })


def _get_lifecycle_policy(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    if name not in _lifecycle_policies:
        return error_response_json("LifecyclePolicyNotFoundException",
                                   f"Lifecycle policy does not exist for the repository with name '{name}' "
                                   f"in the registry with id '{_registry_id()}'", 400)
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "lifecyclePolicyText": _lifecycle_policies[name],
        "lastEvaluatedAt": int(time.time()),
    })


def _delete_lifecycle_policy(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    if name not in _lifecycle_policies:
        return error_response_json("LifecyclePolicyNotFoundException",
                                   f"Lifecycle policy does not exist for the repository with name '{name}' "
                                   f"in the registry with id '{_registry_id()}'", 400)
    policy = _lifecycle_policies.pop(name)
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "lifecyclePolicyText": policy,
        "lastEvaluatedAt": int(time.time()),
    })


def _list_tags_for_resource(data):
    arn = data.get("resourceArn", "")
    repo = _find_repo_by_arn(arn)
    if not repo:
        return error_response_json("RepositoryNotFoundException", "Repository not found", 400)
    return json_response({"tags": repo.get("tags", [])})


def _tag_resource(data):
    arn = data.get("resourceArn", "")
    repo = _find_repo_by_arn(arn)
    if not repo:
        return error_response_json("RepositoryNotFoundException", "Repository not found", 400)
    new_tags = data.get("tags", [])
    existing = {t["Key"]: t for t in repo.get("tags", [])}
    for t in new_tags:
        existing[t["Key"]] = t
    repo["tags"] = list(existing.values())
    return json_response({})


def _untag_resource(data):
    arn = data.get("resourceArn", "")
    repo = _find_repo_by_arn(arn)
    if not repo:
        return error_response_json("RepositoryNotFoundException", "Repository not found", 400)
    keys = set(data.get("tagKeys", []))
    repo["tags"] = [t for t in repo.get("tags", []) if t["Key"] not in keys]
    return json_response({})


def _put_image_tag_mutability(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    mutability = data.get("imageTagMutability", "MUTABLE")
    _repositories[name]["imageTagMutability"] = mutability
    filters = data.get("imageTagMutabilityExclusionFilters")
    if filters:
        _repositories[name]["imageTagMutabilityExclusionFilters"] = filters
    else:
        _repositories[name].pop("imageTagMutabilityExclusionFilters", None)
    resp = {
        "registryId": _registry_id(),
        "repositoryName": name,
        "imageTagMutability": mutability,
    }
    if filters:
        resp["imageTagMutabilityExclusionFilters"] = filters
    return json_response(resp)


def _put_image_scanning_configuration(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    config = data.get("imageScanningConfiguration", {"scanOnPush": False})
    _repositories[name]["imageScanningConfiguration"] = config
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "imageScanningConfiguration": config,
    })


def _describe_registry(data):
    return json_response({
        "registryId": _registry_id(),
        "replicationConfiguration": {"rules": []},
    })


def _get_download_url_for_layer(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    layer_digest = data.get("layerDigest", "")
    return json_response({
        "downloadUrl": f"https://{get_account_id()}.dkr.ecr.{get_region()}.amazonaws.com/v2/{name}/blobs/{layer_digest}",
        "layerDigest": layer_digest,
    })


def _batch_check_layer_availability(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    digests = data.get("layerDigests", [])
    layers = [{"layerDigest": d, "layerAvailability": "UNAVAILABLE", "layerSize": 0} for d in digests]
    return json_response({"layers": layers, "failures": []})


def _initiate_layer_upload(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'", 400)
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "uploadId": new_uuid(),
        "partSize": 10485760,
    })


def _upload_layer_part(data):
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": data.get("repositoryName", ""),
        "uploadId": data.get("uploadId", ""),
        "lastByteReceived": 0,
    })


def _complete_layer_upload(data):
    name = data.get("repositoryName", "")
    digests = data.get("layerDigests", [])
    layer_digest = digests[0] if digests else "sha256:" + new_uuid().replace("-", "")
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "uploadId": data.get("uploadId", ""),
        "layerDigest": layer_digest,
    })


def _find_image(repo_name, image_id):
    digest = image_id.get("imageDigest")
    tag = image_id.get("imageTag")
    for img in _images.get(repo_name, []):
        if digest and img["imageDigest"] == digest:
            return img
        if tag and tag in img.get("imageTags", []):
            return img
    return None


def _find_repo_by_arn(arn):
    name = _repo_name_from_arn(arn)
    if not name:
        return None
    repo = _repositories.get(name)
    if not repo or repo.get("repositoryArn") != arn:
        return None
    return repo


def _repo_name_from_arn(arn):
    try:
        spec = parse_arn(arn)
    except ArnParseError:
        return None
    if (
        spec.partition != "aws"
        or spec.service != "ecr"
        or spec.account_id != get_account_id()
        or spec.region != get_region()
    ):
        return None
    prefix = "repository/"
    if not spec.resource.startswith(prefix):
        return None
    name = spec.resource[len(prefix):]
    return name or None


def _repo_shape(repo):
    return {
        "repositoryArn": repo["repositoryArn"],
        "registryId": repo["registryId"],
        "repositoryName": repo["repositoryName"],
        "repositoryUri": repo["repositoryUri"],
        "createdAt": repo["createdAt"],
        "imageTagMutability": repo.get("imageTagMutability", "MUTABLE"),
        **({"imageTagMutabilityExclusionFilters": repo["imageTagMutabilityExclusionFilters"]}
           if repo.get("imageTagMutabilityExclusionFilters") else {}),
        "imageScanningConfiguration": repo.get("imageScanningConfiguration", {"scanOnPush": False}),
        "encryptionConfiguration": repo.get("encryptionConfiguration", {"encryptionType": "AES256"}),
    }


def reset():
    _ptc_rules.clear()
    _creation_templates.clear()
    _registry_policies.clear()
    _repositories.clear()
    _images.clear()
    _lifecycle_policies.clear()
    _repo_policies.clear()
    _layer_blobs.clear()
    _manifest_blobs.clear()
    _uploads.clear()


# ──────────────────────────────────────────────────────────────────────────
# Docker Registry HTTP API V2 (RFC: distribution.github.io/distribution/spec/api/)
# Real AWS ECR exposes both the AWS API (CreateRepository, DescribeImages, etc.)
# and the registry V2 protocol from the same hostname. `docker push` and `docker
# pull` speak only V2. The two paths share `_repositories` and `_images` so a
# `docker push foo:latest` becomes visible to `aws ecr describe-images
# --repository-name foo` and vice-versa.
# ──────────────────────────────────────────────────────────────────────────

_REGISTRY_API_VERSION_HEADER = ("Docker-Distribution-API-Version", "registry/2.0")
_DEFAULT_MANIFEST_MEDIA_TYPE = "application/vnd.docker.distribution.manifest.v2+json"
_UPLOAD_PART_SIZE = 10 * 1024 * 1024  # advisory chunk size; clients ignore for chunked uploads
_ECR_REGISTRY_HOST_RE = re.compile(
    r"^(\d{12})\.dkr\.ecr\.([a-z0-9-]+)\."
)


def _registry_error(status, code, message, detail=None):
    body = {
        "errors": [
            {"code": code, "message": message, "detail": detail if detail is not None else {}}
        ]
    }
    return (
        status,
        {
            "Content-Type": "application/json",
            _REGISTRY_API_VERSION_HEADER[0]: _REGISTRY_API_VERSION_HEADER[1],
        },
        json.dumps(body).encode(),
    )


def _registry_ok(status, headers, body):
    out = dict(headers or {})
    out.setdefault(_REGISTRY_API_VERSION_HEADER[0], _REGISTRY_API_VERSION_HEADER[1])
    return (status, out, body if body is not None else b"")


def _content_digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _parse_v2_path(path: str):
    """Split a `/v2/...` path into (verb, name, ref).

    Verbs: ``ping``, ``catalog``, ``blob_uploads``, ``blob_upload_session``,
    ``blob``, ``manifest``, ``tags_list``. Returns ``None`` if the path does
    not match a known V2 endpoint shape.

    Repository names may contain slashes (`my/team/svc`), so we locate the
    first occurrence of a registry keyword (``blobs``, ``manifests``, ``tags``)
    and treat everything before it as the name.
    """
    if path in ("/v2", "/v2/"):
        return ("ping", "", "")
    if path == "/v2/_catalog":
        return ("catalog", "", "")
    if not path.startswith("/v2/"):
        return None

    rest = path[len("/v2/"):]
    parts = rest.split("/")
    keyword_idx = None
    for i, p in enumerate(parts):
        if p in ("blobs", "manifests", "tags"):
            keyword_idx = i
            break
    if keyword_idx is None or keyword_idx == 0:
        return None

    name = "/".join(parts[:keyword_idx])
    keyword = parts[keyword_idx]
    tail = parts[keyword_idx + 1:]

    if keyword == "blobs":
        if tail and tail[0] == "uploads":
            # /v2/<name>/blobs/uploads/                  → blob_uploads (start)
            # /v2/<name>/blobs/uploads/<uuid>            → blob_upload_session
            uuid = "/".join(tail[1:]).rstrip("/")
            if uuid:
                return ("blob_upload_session", name, uuid)
            return ("blob_uploads", name, "")
        # /v2/<name>/blobs/<digest>
        digest = "/".join(tail).rstrip("/")
        if not digest:
            return None
        return ("blob", name, digest)

    if keyword == "manifests":
        ref = "/".join(tail).rstrip("/")
        if not ref:
            return None
        return ("manifest", name, ref)

    if keyword == "tags":
        if tail and tail[0] == "list":
            return ("tags_list", name, "")
        return None

    return None


def _ensure_repo_exists(name):
    if name in _repositories:
        return None
    return _registry_error(
        404,
        "NAME_UNKNOWN",
        f"repository name not known to registry: {name}",
        {"name": name},
    )


def _query_first(query_params, key):
    val = query_params.get(key)
    if isinstance(val, list):
        return val[0] if val else None
    return val


def _registry_scope_hint(value):
    if not isinstance(value, str):
        return None
    match = _ECR_REGISTRY_HOST_RE.match(value.lower())
    return match.groups() if match else None


def _select_registry_request_scope(name, headers, query_params):
    """Pin an unsigned Registry V2 request to its repository's tenant scope."""
    scope_hint = _registry_scope_hint(headers.get("host"))
    if scope_hint is None:
        scope_hint = _registry_scope_hint(_query_first(query_params, "ns"))
    if scope_hint is not None:
        account_id, region = scope_hint
        set_request_account_id(account_id)
        set_request_region(region)
        return

    if not name:
        return

    # Preserve the ambient request scope when it already resolves the repo.
    if name in _repositories:
        return

    account_id = get_account_id()
    matches = [
        region
        for (stored_account, region, stored_name), _repository
        in _repositories.all_items()
        if stored_account == account_id and stored_name == name
    ]
    if len(matches) == 1:
        set_request_region(matches[0])


def _v2_ping():
    return _registry_ok(200, {"Content-Type": "application/json"}, b"{}")


def _v2_catalog(query_params):
    n_raw = _query_first(query_params, "n")
    try:
        n = int(n_raw) if n_raw is not None else None
    except (TypeError, ValueError):
        n = None
    repos = sorted(_repositories.keys())
    if n is not None and n >= 0:
        repos = repos[:n]
    return _registry_ok(
        200,
        {"Content-Type": "application/json"},
        json.dumps({"repositories": repos}).encode(),
    )


def _v2_start_blob_upload(name, query_params, body):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err

    mount = _query_first(query_params, "mount")
    src = _query_first(query_params, "from")
    # Cross-repo blob mount: if the requested digest already exists in `from`,
    # link it into `name` and return 201 immediately. Otherwise fall through
    # to a normal upload session (per the spec).
    if mount:
        src_blobs = _layer_blobs.get(src, {}) if src else {}
        if mount in src_blobs:
            _layer_blobs.setdefault(name, {})[mount] = src_blobs[mount]
            return _registry_ok(
                201,
                {
                    "Location": f"/v2/{name}/blobs/{mount}",
                    "Docker-Content-Digest": mount,
                    "Content-Length": "0",
                },
                b"",
            )

    # Single-shot upload: POST /v2/<name>/blobs/uploads/?digest=sha256:... with body.
    digest = _query_first(query_params, "digest")
    if digest and body:
        actual = _content_digest(body if isinstance(body, bytes) else body.encode())
        if actual != digest:
            return _registry_error(
                400,
                "DIGEST_INVALID",
                "provided digest did not match uploaded content",
                {"expected": digest, "actual": actual},
            )
        _layer_blobs.setdefault(name, {})[digest] = bytes(body)
        return _registry_ok(
            201,
            {
                "Location": f"/v2/{name}/blobs/{digest}",
                "Docker-Content-Digest": digest,
                "Content-Length": "0",
            },
            b"",
        )

    upload_uuid = new_uuid()
    _uploads.setdefault(name, {})[upload_uuid] = bytearray()
    return _registry_ok(
        202,
        {
            "Location": f"/v2/{name}/blobs/uploads/{upload_uuid}",
            "Range": "0-0",
            "Content-Length": "0",
            "Docker-Upload-UUID": upload_uuid,
        },
        b"",
    )


def _v2_get_blob_upload(name, upload_uuid):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    buf = _uploads.get(name, {}).get(upload_uuid)
    if buf is None:
        return _registry_error(
            404,
            "BLOB_UPLOAD_UNKNOWN",
            "blob upload unknown to registry",
            {"name": name, "uuid": upload_uuid},
        )
    end = max(0, len(buf) - 1)
    return _registry_ok(
        204,
        {
            "Location": f"/v2/{name}/blobs/uploads/{upload_uuid}",
            "Range": f"0-{end}",
            "Docker-Upload-UUID": upload_uuid,
        },
        b"",
    )


def _v2_patch_blob_upload(name, upload_uuid, body):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    buf = _uploads.get(name, {}).get(upload_uuid)
    if buf is None:
        return _registry_error(
            404,
            "BLOB_UPLOAD_UNKNOWN",
            "blob upload unknown to registry",
            {"name": name, "uuid": upload_uuid},
        )
    if body:
        buf.extend(body if isinstance(body, (bytes, bytearray)) else body.encode())
    end = max(0, len(buf) - 1)
    return _registry_ok(
        202,
        {
            "Location": f"/v2/{name}/blobs/uploads/{upload_uuid}",
            "Range": f"0-{end}",
            "Content-Length": "0",
            "Docker-Upload-UUID": upload_uuid,
        },
        b"",
    )


def _v2_complete_blob_upload(name, upload_uuid, query_params, body):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    digest = _query_first(query_params, "digest")
    if not digest:
        return _registry_error(
            400, "DIGEST_INVALID", "digest query parameter is required", {}
        )

    buf = _uploads.get(name, {}).get(upload_uuid)
    if buf is None:
        return _registry_error(
            404,
            "BLOB_UPLOAD_UNKNOWN",
            "blob upload unknown to registry",
            {"name": name, "uuid": upload_uuid},
        )
    # The PUT may carry a final chunk in its body.
    if body:
        buf.extend(body if isinstance(body, (bytes, bytearray)) else body.encode())

    final = bytes(buf)
    actual = _content_digest(final)
    if actual != digest:
        return _registry_error(
            400,
            "DIGEST_INVALID",
            "uploaded blob digest did not match provided digest",
            {"expected": digest, "actual": actual},
        )

    _layer_blobs.setdefault(name, {})[digest] = final
    _uploads.get(name, {}).pop(upload_uuid, None)

    return _registry_ok(
        201,
        {
            "Location": f"/v2/{name}/blobs/{digest}",
            "Docker-Content-Digest": digest,
            "Content-Length": "0",
        },
        b"",
    )


def _v2_cancel_blob_upload(name, upload_uuid):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    sessions = _uploads.get(name, {})
    if upload_uuid not in sessions:
        return _registry_error(
            404,
            "BLOB_UPLOAD_UNKNOWN",
            "blob upload unknown to registry",
            {"name": name, "uuid": upload_uuid},
        )
    sessions.pop(upload_uuid, None)
    return _registry_ok(204, {}, b"")


def _v2_head_blob(name, digest):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    blob = _layer_blobs.get(name, {}).get(digest)
    if blob is None:
        return _registry_error(
            404,
            "BLOB_UNKNOWN",
            "blob unknown to registry",
            {"digest": digest},
        )
    return _registry_ok(
        200,
        {
            "Content-Length": str(len(blob)),
            "Docker-Content-Digest": digest,
            "Content-Type": "application/octet-stream",
        },
        b"",
    )


def _v2_get_blob(name, digest):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    blob = _layer_blobs.get(name, {}).get(digest)
    if blob is None:
        return _registry_error(
            404,
            "BLOB_UNKNOWN",
            "blob unknown to registry",
            {"digest": digest},
        )
    return _registry_ok(
        200,
        {
            "Content-Length": str(len(blob)),
            "Docker-Content-Digest": digest,
            "Content-Type": "application/octet-stream",
        },
        blob,
    )


def _v2_delete_blob(name, digest):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    blobs = _layer_blobs.get(name, {})
    if digest not in blobs:
        return _registry_error(
            404,
            "BLOB_UNKNOWN",
            "blob unknown to registry",
            {"digest": digest},
        )
    blobs.pop(digest, None)
    return _registry_ok(202, {}, b"")


def _resolve_manifest(name, ref):
    """Return (digest, manifest_bytes, media_type) or (None, None, None)."""
    if ref.startswith("sha256:"):
        blob = _manifest_blobs.get(name, {}).get(ref)
        if blob is None:
            return None, None, None
        # Look up the matching image record for media-type accuracy.
        for img in _images.get(name, []):
            if img.get("imageDigest") == ref:
                return ref, blob, img.get("imageManifestMediaType", _DEFAULT_MANIFEST_MEDIA_TYPE)
        return ref, blob, _DEFAULT_MANIFEST_MEDIA_TYPE
    # Tag lookup.
    for img in _images.get(name, []):
        if ref in img.get("imageTags", []):
            digest = img["imageDigest"]
            blob = _manifest_blobs.get(name, {}).get(digest)
            if blob is None:
                # Pre-V2 PutImage path: the manifest was stored as a string.
                manifest_str = img.get("imageManifest", "")
                blob = manifest_str.encode() if isinstance(manifest_str, str) else manifest_str
            return digest, blob, img.get("imageManifestMediaType", _DEFAULT_MANIFEST_MEDIA_TYPE)
    return None, None, None


# ── Images ECS runs from this registry ──────────────────────

_INDEX_MEDIA_TYPES = frozenset((
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.index.v1+json",
))


def _held_image(image):
    """(account, region, repository, reference) when ``image`` names a repository this MiniStack holds, else None."""
    host, _, rest = (image or "").partition("/")
    match = _ECR_REGISTRY_HOST_RE.match(host.lower() + ".")
    if not match or not rest:
        return None
    if "@" in rest:
        repo, ref = rest.split("@", 1)
    elif ":" in rest.rsplit("/", 1)[-1]:
        repo, ref = rest.rsplit(":", 1)
    else:
        repo, ref = rest, "latest"
    account, region = match.groups()
    if _repositories.get_scoped(account, region, repo) is None:
        return None
    return account, region, repo, ref


_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")


def held_image_readable(image, principal_arn):
    """Under AUTH=true, a repository in another account is readable only when its policy allows the
    caller to pull (ecr:BatchGetImage and ecr:GetDownloadUrlForLayer, the ECR guide's pull example); its own
    account always can."""
    from ministack.app import AUTH
    held = _held_image(image)
    if held is None or not AUTH or held[0] == get_account_id():
        return True
    from ministack.core.iam_evaluator import EvalContext, resource_policy_allows
    account, region, repo, _ref = held
    policy = _repo_policies.get_scoped(account, region, repo)
    return all(resource_policy_allows(policy, EvalContext(
        principal_arn=principal_arn, principal_account=principal_arn.split(":")[4],
        principal_type="AssumedRole" if ":role/" in principal_arn else "Root", action=action,
        resource_arn=f"arn:aws:ecr:{region}:{account}:repository/{repo}", region=region), same_account=False)
        for action in ("ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"))


def held_image_digest(image):
    """The manifest digest of an image this MiniStack holds, or None (not held, or no such tag)."""
    held = _held_image(image)
    if held is None:
        return None
    account, region, repo, ref = held
    with request_scope(account, region):
        return _resolve_manifest(repo, ref)[0]


def held_image_archive(image, architecture):
    """A ``docker load`` archive of an image this MiniStack holds, for ``architecture`` (amd64, arm64, ...).

    Returns ``(archive bytes, config digest)``, or None when the image is not held here or cannot be
    assembled (missing blobs, or a layer compression Docker's loaders may not take)."""
    import gzip
    import io
    import tarfile

    held = _held_image(image)
    if held is None:
        return None
    account, region, repo, ref = held
    with request_scope(account, region):
        digest, blob, media_type = _resolve_manifest(repo, ref)
        blobs = _layer_blobs.get(repo, {})
        manifests = _manifest_blobs.get(repo, {})
    if blob is None:
        return None
    try:
        manifest = json.loads(blob)
        if manifest.get("mediaType", media_type) in _INDEX_MEDIA_TYPES or "manifests" in manifest:
            chosen = next((m for m in manifest.get("manifests", [])
                           if (m.get("platform") or {}).get("architecture") == architecture
                           and (m.get("platform") or {}).get("os", "linux") == "linux"), None)
            if chosen is None or chosen.get("digest") not in manifests:
                return None
            manifest = json.loads(manifests[chosen["digest"]])
        config_digest = manifest["config"]["digest"]
        if not _DIGEST_RE.fullmatch(config_digest):
            return None
        config = blobs[config_digest]
        layers = []
        for layer in manifest.get("layers", []):
            data = blobs[layer["digest"]]
            if "zstd" in layer.get("mediaType", ""):
                return None
            layers.append(gzip.decompress(data) if data[:2] == b"\x1f\x8b" else data)
    except (KeyError, TypeError, ValueError, OSError):
        return None

    def add(tar, name, data):
        info = tarfile.TarInfo(name)
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))

    tag = None if "@" in image.rsplit("/", 1)[-1] else image if ":" in image.rsplit("/", 1)[-1] else f"{image}:latest"
    config_name = f"{config_digest.split(':', 1)[1]}.json"
    layer_names = [f"{hashlib.sha256(data).hexdigest()}/layer.tar" for data in layers]
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w") as tar:
        add(tar, config_name, config)
        for name, data in zip(layer_names, layers):
            add(tar, name, data)
        add(tar, "manifest.json", json.dumps([{"Config": config_name, "RepoTags": [tag] if tag else [],
                                                "Layers": layer_names}]).encode())
    return out.getvalue(), config_digest


_DOCKER_ARCHITECTURES = {"x86_64": "amd64", "aarch64": "arm64"}


def _ptc_fetch_platform(image, architecture, docker_client):
    """Fetch *architecture*'s child of a cached index that does not hold it yet. True when it was stored."""
    held = _held_image(image)
    if held is None:
        return False
    account, region, repo, ref = held
    with request_scope(account, region):
        _digest, blob, _media_type = _resolve_manifest(repo, ref)
        child = next((m.get("digest") for m in (_ptc_json(blob).get("manifests") or [])
                      if (m.get("platform") or {}).get("architecture") == architecture
                      and (m.get("platform") or {}).get("os", "linux") == "linux"), None)
        if child is None:
            return False
        try:
            return _ptc_fill_child(repo, child, f"arn:aws:iam::{account}:root", docker_client)
        except PullThroughMiss:
            return False


def load_held_image(docker_client, image, architecture=None):
    """Load an image this MiniStack holds into ``docker_client`` for ``architecture`` (the daemon's when None).

    Returns the reference to run it by, or None when it is not held here or has no image for that platform."""
    if not architecture:
        reported = (docker_client.info() or {}).get("Architecture", "")
        architecture = _DOCKER_ARCHITECTURES.get(reported, reported)
    built = held_image_archive(image, architecture)
    if built is None and _ptc_fetch_platform(image, architecture, docker_client):
        built = held_image_archive(image, architecture)
    if built is None:
        return None
    archive, config_digest = built
    loaded = _transfer_client(docker_client).images.load(archive)
    if "@" not in image.rsplit("/", 1)[-1]:
        return image
    # An untagged load is named by its ID: the config digest on the classic store, the manifest digest on containerd's.
    return loaded[0].id if loaded else config_digest


def _v2_put_manifest(name, ref, headers, body):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    if not body:
        return _registry_error(400, "MANIFEST_INVALID", "manifest body is empty", {})

    raw = body if isinstance(body, bytes) else body.encode()
    digest = _content_digest(raw)
    media_type = headers.get("content-type") or _DEFAULT_MANIFEST_MEDIA_TYPE

    # If `ref` is a tag, validate against repo immutability.
    repo = _repositories[name]
    is_tag_ref = not ref.startswith("sha256:")
    tag = ref if is_tag_ref else None

    if is_tag_ref and repo.get("imageTagMutability") == "IMMUTABLE":
        for img in _images.get(name, []):
            if tag in img.get("imageTags", []) and img.get("imageDigest") != digest:
                return _registry_error(
                    400,
                    "TAG_INVALID",
                    f"tag '{tag}' is immutable",
                    {"tag": tag},
                )

    _manifest_blobs.setdefault(name, {})[digest] = raw

    # Detach the tag from any other image record currently holding it,
    # then attach it (or upsert a fresh record) for this manifest.
    if tag:
        for img in _images.get(name, []):
            tags = img.get("imageTags", [])
            if tag in tags:
                tags.remove(tag)

    existing = next(
        (img for img in _images.get(name, []) if img.get("imageDigest") == digest),
        None,
    )
    if existing:
        if tag:
            existing.setdefault("imageTags", [])
            if tag not in existing["imageTags"]:
                existing["imageTags"].append(tag)
            existing["imageId"]["imageTag"] = tag
        existing["imageManifest"] = raw.decode("utf-8", errors="replace")
        existing["imageManifestMediaType"] = media_type
    else:
        record = {
            "registryId": _registry_id(),
            "repositoryName": name,
            "imageId": {"imageDigest": digest},
            "imageManifest": raw.decode("utf-8", errors="replace"),
            "imageManifestMediaType": media_type,
            "imageTags": [tag] if tag else [],
            "imagePushedAt": int(time.time()),
            "imageDigest": digest,
            "imageSizeInBytes": len(raw),
        }
        if tag:
            record["imageId"]["imageTag"] = tag
        _images.setdefault(name, []).append(record)

    return _registry_ok(
        201,
        {
            "Location": f"/v2/{name}/manifests/{digest}",
            "Docker-Content-Digest": digest,
            "Content-Length": "0",
        },
        b"",
    )


def _v2_get_manifest(name, ref):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    digest, blob, media_type = _resolve_manifest(name, ref)
    if blob is None:
        return _registry_error(
            404,
            "MANIFEST_UNKNOWN",
            "manifest unknown to registry",
            {"name": name, "reference": ref},
        )
    return _registry_ok(
        200,
        {
            "Content-Length": str(len(blob)),
            "Content-Type": media_type,
            "Docker-Content-Digest": digest,
        },
        blob,
    )


def _v2_head_manifest(name, ref):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    digest, blob, media_type = _resolve_manifest(name, ref)
    if blob is None:
        return _registry_error(
            404,
            "MANIFEST_UNKNOWN",
            "manifest unknown to registry",
            {"name": name, "reference": ref},
        )
    return _registry_ok(
        200,
        {
            "Content-Length": str(len(blob)),
            "Content-Type": media_type,
            "Docker-Content-Digest": digest,
        },
        b"",
    )


def _v2_delete_manifest(name, ref):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    if not ref.startswith("sha256:"):
        return _registry_error(
            400,
            "UNSUPPORTED",
            "manifest delete by tag is not supported; delete by digest",
            {"reference": ref},
        )
    digest = ref
    blobs = _manifest_blobs.get(name, {})
    if digest not in blobs:
        return _registry_error(
            404,
            "MANIFEST_UNKNOWN",
            "manifest unknown to registry",
            {"name": name, "reference": ref},
        )
    blobs.pop(digest, None)
    images = _images.get(name, [])
    _images[name] = [img for img in images if img.get("imageDigest") != digest]
    return _registry_ok(202, {}, b"")


def _v2_list_tags(name, query_params):
    err = _ensure_repo_exists(name)
    if err is not None:
        return err
    n_raw = _query_first(query_params, "n")
    last = _query_first(query_params, "last")
    tags = sorted(
        {tag for img in _images.get(name, []) for tag in img.get("imageTags", [])}
    )
    if last:
        tags = [t for t in tags if t > last]
    try:
        n = int(n_raw) if n_raw is not None else None
    except (TypeError, ValueError):
        n = None
    if n is not None and n >= 0:
        tags = tags[:n]
    return _registry_ok(
        200,
        {"Content-Type": "application/json"},
        json.dumps({"name": name, "tags": tags}).encode(),
    )


async def handle_registry_request(method, path, headers, body, query_params):
    """Entry point for Docker Registry HTTP API V2 requests (`/v2/...`).

    Real ECR serves this protocol from the same hostname as the AWS API.
    `app.py` routes any path beginning with `/v2/` (excluding the SES v2
    `/v2/email` carve-out) here before generic service detection picks it up.
    """
    parsed = _parse_v2_path(path)
    if parsed is None:
        return _registry_error(
            404,
            "UNSUPPORTED",
            f"path not handled by registry: {path}",
            {"path": path},
        )

    verb, name, ref = parsed
    method = method.upper()
    headers = {k.lower(): v for k, v in (headers or {}).items()}

    _select_registry_request_scope(name, headers, query_params)

    if verb == "ping":
        if method not in ("GET", "HEAD"):
            return _registry_error(405, "UNSUPPORTED", "method not allowed", {})
        return _v2_ping()

    if verb == "catalog":
        if method != "GET":
            return _registry_error(405, "UNSUPPORTED", "method not allowed", {})
        return _v2_catalog(query_params)

    if verb == "blob_uploads":
        if method != "POST":
            return _registry_error(405, "UNSUPPORTED", "method not allowed", {})
        return _v2_start_blob_upload(name, query_params, body)

    if verb == "blob_upload_session":
        if method == "PATCH":
            return _v2_patch_blob_upload(name, ref, body)
        if method == "PUT":
            return _v2_complete_blob_upload(name, ref, query_params, body)
        if method == "GET":
            return _v2_get_blob_upload(name, ref)
        if method == "DELETE":
            return _v2_cancel_blob_upload(name, ref)
        return _registry_error(405, "UNSUPPORTED", "method not allowed", {})

    if verb == "blob":
        if method == "HEAD":
            return _v2_head_blob(name, ref)
        if method == "GET":
            return _v2_get_blob(name, ref)
        if method == "DELETE":
            return _v2_delete_blob(name, ref)
        return _registry_error(405, "UNSUPPORTED", "method not allowed", {})

    if verb == "manifest" and method in ("GET", "HEAD") and ref.startswith("sha256:") \
            and _ptc_match(name)[0] is not None:
        try:
            await asyncio.to_thread(_ptc_fill_child, name, ref, f"arn:aws:iam::{get_account_id()}:root")
        except PullThroughMiss:
            pass  # answered as MANIFEST_UNKNOWN below, like any digest the cache does not hold

    if verb == "manifest" and method in ("GET", "HEAD") and not ref.startswith("sha256:") \
            and _ptc_match(name)[0] is not None:
        try:
            await asyncio.to_thread(_ptc_fill, name, ref, f"arn:aws:iam::{get_account_id()}:root")
        except PullThroughMiss:
            if name not in _repositories or _resolve_manifest(name, ref)[1] is None:
                return _registry_error(404, "MANIFEST_UNKNOWN", "manifest unknown to registry",
                                       {"name": name, "reference": ref})

    if verb == "manifest":
        if method == "PUT":
            return _v2_put_manifest(name, ref, headers, body)
        if method == "GET":
            return _v2_get_manifest(name, ref)
        if method == "HEAD":
            return _v2_head_manifest(name, ref)
        if method == "DELETE":
            return _v2_delete_manifest(name, ref)
        return _registry_error(405, "UNSUPPORTED", "method not allowed", {})

    if verb == "tags_list":
        if method != "GET":
            return _registry_error(405, "UNSUPPORTED", "method not allowed", {})
        return _v2_list_tags(name, query_params)

    return _registry_error(404, "UNSUPPORTED", f"unhandled V2 verb: {verb}", {})


# ── Pull-through cache rules ────────────────────────────────

_PTC_MAX_RULES = 50
_PTC_ROLE_SERVICE = "pullthroughcache.ecr.amazonaws.com"
_PTC_ROLE_NAME = "AWSServiceRoleForECRPullThroughCache"
_PTC_ROLE_POLICY = "arn:aws:iam::aws:policy/aws-service-role/AWSECRPullThroughCache_ServiceRolePolicy"
_PTC_PREFIX_PATTERN = r"^([a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(\/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*\/?|ROOT)$"
_PTC_PREFIX_RE = re.compile(_PTC_PREFIX_PATTERN)
_PTC_CREDENTIAL_PATTERN = (
    r"^arn:aws(-\w+)*:secretsmanager:[a-zA-Z0-9-:]+:secret:ecr\-pullthroughcache\/[a-zA-Z0-9\/_+=.@-]+$")
_PTC_CREDENTIAL_RE = re.compile(_PTC_CREDENTIAL_PATTERN, re.ASCII)
_REGISTRY_ID_RE = re.compile(r"[0-9]{12}")
_PTC_ECR_URL_RE = re.compile(r"^([0-9]{12})\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com$")
# The value set AWS's own refusal lists (botocore's model omits ecr-public-premium).
_PTC_UPSTREAM_REGISTRIES = (
    "ecr-public", "chainguard", "k8s", "azure-container-registry", "docker-hub", "ecr-public-premium",
    "github-container-registry", "ecr", "gitlab-container-registry", "quay",
)
# The upstreams AWS supports (ECR guide, pull-through-cache); each group name is the upstreamRegistry value.
_PTC_UPSTREAM_URL_RE = re.compile(
    r"^(?:(?P<ecr_public>public\.ecr\.aws)|(?P<quay>quay\.io)|(?P<k8s>registry\.k8s\.io)"
    r"|(?P<docker_hub>registry-1\.docker\.io)|(?P<github_container_registry>ghcr\.io)"
    r"|(?P<gitlab_container_registry>registry\.gitlab\.com)|(?P<chainguard>cgr\.dev)"
    r"|(?P<azure_container_registry>[a-z0-9]+\.azurecr\.io)"
    r"|(?P<ecr>[0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com))$")
_PTC_SECRET_UPSTREAMS = frozenset((
    "docker-hub", "github-container-registry", "gitlab-container-registry", "chainguard", "azure-container-registry"))
_PTC_ACTIONS = frozenset((
    "CreatePullThroughCacheRule", "UpdatePullThroughCacheRule", "DeletePullThroughCacheRule",
    "DescribePullThroughCacheRules", "ValidatePullThroughCacheRule",
))
_PTC_WIRE_ACTIONS = _PTC_ACTIONS | {"PutRegistryPolicy", "GetRegistryPolicy", "DeleteRegistryPolicy"}
_PTC_STRING_MEMBERS = ("policyText", "registryId", "ecrRepositoryPrefix", "upstreamRegistryUrl", "upstreamRegistry",
                       "credentialArn", "customRoleArn", "upstreamRepositoryPrefix", "nextToken")

_MSG_NEEDS_SECRET = ("The specified upstream registry requires authentication. Specify a valid Secrets Manager ARN "
                     "containing the upstream registry credentials and try again.")
_MSG_NEEDS_NO_AUTH = ("The specified upstream registry doesn't require authentication. Verify the upstream registry "
                      "URL and the credential ARN value and try again.")
_MSG_NEEDS_ROLE = ("Invalid input: The specified upstream registry requires authentication. "
                   "Specify a valid IAM role ARN and try again.")
_MSG_TYPE_MISMATCH = ("Invalid input: Upstream registry URL and provided credential type do not match. "
                      "Please verify input and try again.")
_MSG_ONE_CREDENTIAL = "Invalid input: Please set either credential arn or role arn to update the credentials"
_MSG_UNAUTHENTICATED_RULE = "Invalid input: Please create an authenticated pull through cache rule to update the credentials"
_MSG_SECRET_REGION = ("Please verify the secret exists in the same region and account that the pullthrough cache rule "
                      "will be created in, and try again.")
_MSG_SECRET_NOT_FOUND = ("The ARN of the secret specified in the pull through cache rule was not found. Update the pull "
                         "through cache rule with a valid secret ARN and try again.")
_MSG_SECRET_UNREADABLE = "The secret is unable to be accessed. Verify the resource permissions for the secret and try again."
_MSG_SECRET_UNDECRYPTABLE = ("The secret is accessible but is unable to be decrypted. Verify the resource permisisons "
                             "and try again.")
def _invalid(message):
    return error_response_json("InvalidParameterException", message, 400)


def _access_denied(message):
    return error_response_json("AccessDeniedException", message, 400, message_key="Message")


def _param(name, constraint):
    return f"Invalid parameter at '{name}' failed to satisfy constraint: '{constraint}'"


def _ptc_serialization_error(message=None):
    if message:
        return error_response_json("SerializationException", message, 400, message_key="Message")
    return 400, {"Content-Type": "application/x-amz-json-1.1", "x-amzn-errortype": "SerializationException"}, \
        json.dumps({"__type": "SerializationException"}).encode()


class _PtcWire(Exception):
    def __init__(self, response):
        super().__init__()
        self.response = response


class _PtcReader:
    """Strict JSON reader for the pull-through cache rule actions: AWS's error positions and duplicate keys."""

    _NUMBER = re.compile(r"-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?")

    def __init__(self, text):
        self.text = text
        self.pos = 0

    def _fail_at(self, index):
        if index >= len(self.text):
            raise _PtcWire(_ptc_serialization_error("Unexpected end of payload"))
        position = len(self.text[:index].encode("utf-8", "replace"))
        raise _PtcWire(_ptc_serialization_error(f"Unexpected Character in input string at byte {position}"))

    def _skip(self):
        while self.pos < len(self.text) and self.text[self.pos] in " \t\r\n":
            self.pos += 1

    def parse(self):
        self._skip()
        if self.pos >= len(self.text):
            raise _PtcWire(_ptc_serialization_error())
        value = self._value(0)
        self._skip()
        if self.pos < len(self.text):
            self._fail_at(self.pos)
        return value

    def _value(self, depth):
        if depth > 64:
            raise _PtcWire(_ptc_serialization_error())
        self._skip()
        if self.pos >= len(self.text):
            self._fail_at(self.pos)
        ch = self.text[self.pos]
        if ch == "{":
            return self._object(depth)
        if ch == "[":
            return self._array(depth)
        if ch == '"':
            return self._string()
        for literal, value in (("true", True), ("false", False), ("null", None)):
            if ch == literal[0]:
                for offset, expected in enumerate(literal):
                    if self.pos + offset >= len(self.text) or self.text[self.pos + offset] != expected:
                        self._fail_at(self.pos + offset)
                self.pos += len(literal)
                return value
        match = self._NUMBER.match(self.text, self.pos)
        if not match:
            self._fail_at(self.pos)
        self.pos = match.end()
        token = match.group(0)
        if match.group(1) or match.group(2):
            return float(token)
        try:
            return int(token)
        except ValueError:
            return -10 ** 40 if token.startswith("-") else 10 ** 40  # past int()'s digit limit: out of any range

    def _string(self):
        start, index = self.pos, self.pos + 1
        while index < len(self.text):
            if self.text[index] == "\\":
                index += 2
                continue
            if self.text[index] == '"':
                self.pos = index + 1
                try:
                    return json.loads(self.text[start:index + 1])
                except ValueError:
                    self._fail_at(start)
            index += 1
        self._fail_at(len(self.text))

    def _array(self, depth):
        self.pos += 1
        items = []
        self._skip()
        if self.pos < len(self.text) and self.text[self.pos] == "]":
            self.pos += 1
            return items
        while True:
            items.append(self._value(depth + 1))
            self._skip()
            if self.pos >= len(self.text):
                self._fail_at(self.pos)
            ch = self.text[self.pos]
            self.pos += 1
            if ch == "]":
                return items
            if ch != ",":
                self._fail_at(self.pos - 1)

    def _object(self, depth):
        self.pos += 1
        result = {}
        self._skip()
        if self.pos < len(self.text) and self.text[self.pos] == "}":
            self.pos += 1
            return result
        while True:
            self._skip()
            if self.pos >= len(self.text) or self.text[self.pos] != '"':
                self._fail_at(self.pos)
            key = self._string()
            if key in result:
                raise _PtcWire(_ptc_serialization_error("Duplicate key found in payload"))
            self._skip()
            if self.pos >= len(self.text) or self.text[self.pos] != ":":
                self._fail_at(self.pos)
            self.pos += 1
            result[key] = self._value(depth + 1)
            self._skip()
            if self.pos >= len(self.text):
                self._fail_at(self.pos)
            ch = self.text[self.pos]
            self.pos += 1
            if ch == "}":
                return result
            if ch != ",":
                self._fail_at(self.pos - 1)


def _ptc_member_type_error(action, data):
    """The SerializationException a JSON value of the wrong type gets, before any validation."""
    def wrong(value, target):
        if isinstance(value, bool):
            return f"{'TRUE' if value else 'FALSE'}_VALUE cannot be converted to {target}"
        if isinstance(value, int):
            return f"NUMBER_VALUE cannot be converted to {target}"
        if isinstance(value, float):
            return f"DECIMAL_VALUE cannot be converted to {target}"
        if isinstance(value, str):
            return f"STRING_VALUE cannot be converted to {target}"
        if isinstance(value, dict):
            return "Start of structure or map found where not expected"
        return ""

    def refuse(message):
        return error_response_json("SerializationException", message, 400, message_key="Message") if message \
            else (400, {"Content-Type": "application/x-amz-json-1.1", "x-amzn-errortype": "SerializationException"},
                  json.dumps({"__type": "SerializationException"}).encode())

    for member in _PTC_STRING_MEMBERS:
        value = data.get(member)
        if value is not None and not isinstance(value, str):
            return refuse(wrong(value, "String"))
    value = data.get("maxResults")
    if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
        return refuse(wrong(value, "Integer") if not isinstance(value, float) else "")
    value = data.get("ecrRepositoryPrefixes")
    if value is not None:
        if not isinstance(value, list):
            return refuse("Start of structure or map found where not expected" if isinstance(value, dict)
                          else "Unexpected field type")
        for item in value:
            if item is not None and not isinstance(item, str):
                return refuse(wrong(item, "String"))
    return None


def _ptc_in_registry(handler, data):
    """Run a rule action in the registry ``registryId`` names. Under AUTH=false another account's registry is
    simply that account's; under AUTH=true the handler refuses it."""
    from ministack.app import AUTH
    registry_id = data.get("registryId")
    if not AUTH and registry_id and _REGISTRY_ID_RE.fullmatch(registry_id) and registry_id != get_account_id():
        with request_scope(registry_id, get_region()):
            return handler(data)
    return handler(data)


def _ptc_registry_errors(action, data):
    registry_id = data.get("registryId")
    if registry_id is not None and not _REGISTRY_ID_RE.fullmatch(registry_id):
        return _invalid(_param("registryId", "must satisfy regular expression [0-9]{12}"))
    return None


def _ptc_other_registry(action, data):
    registry_id = data.get("registryId")
    if registry_id is not None and registry_id != get_account_id():
        from ministack.core.iam_evaluator import caller_arn
        return _access_denied(f"User: {caller_arn()} is not authorized to perform: ecr:{action} on resource: * "
                              f"because no resource-based policy allows the ecr:{action} action")
    return None


def _ptc_role_errors(data):
    role = data.get("customRoleArn")
    if not role:
        data.pop("customRoleArn", None)
        return None
    try:
        spec = parse_arn(role)
    except ArnParseError:
        spec = None
    if spec is None or spec.service != "iam" or not spec.resource.startswith("role/"):
        return _invalid("Invalid role ARN supplied in request.")
    if spec.account_id != data.get("registryId", get_account_id()):
        return _access_denied("Cross-account pass role is not allowed.")
    return None


def _ptc_prefix_errors(name, value):
    if value is None:
        return [_param(name, "Member must not be null")]
    errors = []
    if not _PTC_PREFIX_RE.fullmatch(value):
        errors.append(_param(name, f"Member must satisfy regular expression pattern: {_PTC_PREFIX_PATTERN}"))
    if len(value) < 2:
        errors.append(_param(name, "Member must have length greater than or equal to 2"))
    elif len(value) > 30:
        errors.append(_param(name, "Member must have length less than or equal to 30"))
    return errors


def _ptc_credential_errors(value):
    if value is None:
        return []
    errors = []
    if not _PTC_CREDENTIAL_RE.fullmatch(value):
        errors.append(_param("credentialArn", f"Member must satisfy regular expression pattern: {_PTC_CREDENTIAL_PATTERN}"))
    if len(value) < 50:
        errors.append(_param("credentialArn", "Member must have length greater than or equal to 50"))
    elif len(value) > 612:
        errors.append(_param("credentialArn", "Member must have length less than or equal to 612"))
    return errors


def _ptc_role_length_errors(value):
    if value is not None and len(value) > 2048:
        return [_param("customRoleArn", "Member must have length less than or equal to 2048")]
    return []


def _ptc_normalise(prefix):
    return prefix[:-1] if prefix.endswith("/") else prefix


def _ptc_rule_missing(prefix):
    return error_response_json(
        "PullThroughCacheRuleNotFoundException",
        f"A pull-through cache rule with repository prefix '{prefix}' does not exist in the registry with id "
        f"'{get_account_id()}'", 400)


def _ptc_service_role_arn():
    return f"arn:aws:iam::{get_account_id()}:role/aws-service-role/{_PTC_ROLE_SERVICE}/{_PTC_ROLE_NAME}"


def _ptc_caller_ctx(credential):
    from ministack.core.iam_evaluator import EvalContext, caller_arn
    caller = caller_arn()
    return EvalContext(principal_arn=caller, principal_account=caller.split(":")[4],
                       principal_type="Root" if caller.endswith(":root") else
                       "AssumedRole" if ":assumed-role/" in caller else "User",
                       action="secretsmanager:GetSecretValue", resource_arn=credential, region=get_region(),
                       service_context={"aws:calledvia": ["ecr.amazonaws.com"]})


def _ptc_caller_identity_denied(credential):
    """Under AUTH=true, whether the caller's identity policies refuse its GetSecretValue through ECR."""
    from ministack.app import AUTH
    if not AUTH:
        return False
    from ministack.core.iam_evaluator import _verified_key, enforce
    denied = enforce(_verified_key.get(), "secretsmanager:GetSecretValue", "secretsmanager", get_region(),
                     credential, service_context={"aws:calledvia": ["ecr.amazonaws.com"]})
    return denied is not None and hasattr(denied, "principal_arn")


def _ptc_caller_policy_denied(credential):
    """Under AUTH=true, whether the secret's resource policy explicitly denies the caller's read through ECR."""
    from ministack.app import AUTH
    if not AUTH:
        return False
    from ministack.core.iam_evaluator import evaluate_resource_policy
    from ministack.services import secretsmanager
    found = secretsmanager.secret_policy_for_arn(credential)
    return bool(found and found[0]) and evaluate_resource_policy(found[0], _ptc_caller_ctx(credential)).decision == "Deny"


def _ptc_secret_errors(credential):
    """The region check and the caller's read of the rule's secret, as Create and Update make them."""
    from ministack.services import kms, secretsmanager
    try:
        spec = parse_arn(credential)
    except ArnParseError:
        return _invalid(_MSG_SECRET_REGION)
    if spec.region != get_region() or spec.account_id != get_account_id():
        return _invalid(_MSG_SECRET_REGION)
    if spec.partition != "aws":
        return error_response_json("SecretNotFoundException", _MSG_SECRET_NOT_FOUND, 400)
    if _ptc_caller_identity_denied(credential):
        return error_response_json("UnableToAccessSecretException", _MSG_SECRET_UNREADABLE, 400)
    state, key_id, _ = secretsmanager.pull_through_secret(credential)
    if state == "missing" and len(spec.resource) - len("secret:") > 519:
        # No secret ARN is that long (a 512-character name, a dash and a 6-character suffix).
        return error_response_json("UnableToAccessSecretException", _MSG_SECRET_UNREADABLE, 400)
    if state == "missing":
        return error_response_json("SecretNotFoundException", _MSG_SECRET_NOT_FOUND, 400)
    if state == "unreadable":
        return error_response_json("UnableToAccessSecretException", _MSG_SECRET_UNREADABLE, 400)
    if key_id and kms.held_key_state(key_id) in ("Disabled", "PendingDeletion"):
        return error_response_json("UnableToDecryptSecretValueException", _MSG_SECRET_UNDECRYPTABLE, 400)
    if _ptc_caller_policy_denied(credential):
        return error_response_json("UnableToAccessSecretException", _MSG_SECRET_UNREADABLE, 400)
    return None


def _ptc_upstream_for_url(url):
    match = _PTC_UPSTREAM_URL_RE.fullmatch(url) if isinstance(url, str) else None
    return match.lastgroup.replace("_", "-") if match else None


def _ptc_authenticated(rule):
    return "credentialArn" in rule or "customRoleArn" in rule


def _ptc_echo(rule, out):
    for member in ("credentialArn", "customRoleArn", "upstreamRepositoryPrefix"):
        if member in rule:
            out[member] = rule[member]
    return out


def _create_pull_through_cache_rule(data):
    action = "CreatePullThroughCacheRule"
    from ministack.app import AUTH
    refused = (_ptc_registry_errors(action, data) or _ptc_role_errors(data)
               or (AUTH and _ptc_other_registry(action, data)))
    if refused:
        return refused
    errors = _ptc_role_length_errors(data.get("customRoleArn"))
    url = data.get("upstreamRegistryUrl")
    if url is None:
        errors.append(_param("upstreamRegistryUrl", "Member must not be null"))
    upstream_registry = data.get("upstreamRegistry")
    if upstream_registry is not None and upstream_registry not in _PTC_UPSTREAM_REGISTRIES:
        errors.append(_param("upstreamRegistry",
                             f"Member must satisfy enum value set: [{', '.join(_PTC_UPSTREAM_REGISTRIES)}]"))
    upstream_prefix = data.get("upstreamRepositoryPrefix")
    if upstream_prefix is not None:
        errors += _ptc_prefix_errors("upstreamRepositoryPrefix", upstream_prefix)
    prefix = data.get("ecrRepositoryPrefix")
    errors += _ptc_prefix_errors("ecrRepositoryPrefix", prefix)
    credential = data.get("credentialArn")
    errors += _ptc_credential_errors(credential)
    if errors:
        return _invalid("; ".join(errors))
    prefix = _ptc_normalise(prefix)

    from ministack.services import iam
    iam.ensure_service_linked_role(_PTC_ROLE_SERVICE, _PTC_ROLE_NAME, _PTC_ROLE_POLICY)

    if credential is not None:
        refused = _ptc_secret_errors(credential)
        if refused:
            return refused
    if not url.strip() or len(url) >= 2048:
        return 500, {"Content-Type": "application/x-amz-json-1.1", "x-amzn-errortype": "InternalFailure"}, \
            json.dumps({"__type": "InternalFailure"}).encode()
    upstream = _ptc_upstream_for_url(url)
    if upstream is None:
        return error_response_json(
            "UnsupportedUpstreamRegistryException",
            f"The upstream registry URL {url} is invalid. Specify a valid upstream registry URL and try again", 400)
    role = data.get("customRoleArn")
    if upstream == "ecr":
        url_account, url_region = _PTC_ECR_URL_RE.fullmatch(url).groups()
        if url_account == get_account_id() and url_region == get_region():
            return error_response_json("UnsupportedUpstreamRegistryException",
                                       "Upstream registry URL cannot be the same as the cache registry.", 400)
        if credential is not None:
            return _invalid(_MSG_TYPE_MISMATCH)
        if role is None:
            return _invalid(_MSG_NEEDS_ROLE)
        if url_account == get_account_id():
            return _invalid(_MSG_TYPE_MISMATCH)
    elif upstream in _PTC_SECRET_UPSTREAMS:
        if credential is None:
            return error_response_json("UnsupportedUpstreamRegistryException", _MSG_NEEDS_SECRET, 400)
        if role is not None:
            return _invalid(_MSG_TYPE_MISMATCH)
    elif credential is not None or role is not None:
        return error_response_json("UnsupportedUpstreamRegistryException", _MSG_NEEDS_NO_AUTH, 400)

    if prefix == "ROOT" and upstream_prefix is not None:
        return _invalid("Invalid input: Must specify ecr-repository-prefix when specify upstream-repository-prefix.")
    if len(_ptc_rules) >= _PTC_MAX_RULES:
        return error_response_json(
            "LimitExceededException",
            f"Registry id '{get_account_id()}' has the maximum number of pull-through cache rules allowed, "
            f"which is {_PTC_MAX_RULES}.", 400)
    if prefix in _ptc_rules:
        return error_response_json(
            "PullThroughCacheRuleAlreadyExistsException",
            f"A pull-through cache rule with repository prefix '{prefix}' already exists in the registry with id "
            f"'{get_account_id()}'", 400)

    now = int(time.time())
    rule = {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": url, "upstreamRegistry": upstream,
            "createdAt": now, "updatedAt": now}
    if credential is not None:
        rule["credentialArn"] = credential
    if role is not None:
        rule["customRoleArn"] = role
    if upstream_prefix is not None:
        rule["upstreamRepositoryPrefix"] = upstream_prefix
    _ptc_rules[prefix] = rule
    out = {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": url, "createdAt": now, "registryId": get_account_id()}
    if _ptc_authenticated(rule):
        out["upstreamRegistry"] = upstream
    return json_response(_ptc_echo(rule, out))


def _update_pull_through_cache_rule(data):
    action = "UpdatePullThroughCacheRule"
    from ministack.app import AUTH
    refused = (_ptc_registry_errors(action, data) or _ptc_role_errors(data)
               or (AUTH and _ptc_other_registry(action, data)))
    if refused:
        return refused
    errors = _ptc_role_length_errors(data.get("customRoleArn"))
    prefix = data.get("ecrRepositoryPrefix")
    errors += _ptc_prefix_errors("ecrRepositoryPrefix", prefix)
    credential = data.get("credentialArn")
    errors += _ptc_credential_errors(credential)
    if errors:
        return _invalid("; ".join(errors))
    prefix = _ptc_normalise(prefix)
    role = data.get("customRoleArn")
    if credential is None and role is None:
        return _invalid(_MSG_ONE_CREDENTIAL)
    if credential is not None:
        refused = _ptc_secret_errors(credential)
        if refused:
            return refused
    if credential is not None and role is not None:
        return _invalid(_MSG_ONE_CREDENTIAL)
    rule = _ptc_rules.get(prefix)
    if rule is None:
        return _ptc_rule_missing(prefix)
    if not _ptc_authenticated(rule):
        return _invalid(_MSG_UNAUTHENTICATED_RULE)
    if "credentialArn" in rule and role is not None:
        return _invalid("Invalid input: " + _MSG_NEEDS_SECRET)
    if "customRoleArn" in rule and credential is not None:
        return _invalid(_MSG_NEEDS_ROLE)
    now = int(time.time())
    rule["credentialArn" if credential is not None else "customRoleArn"] = credential or role
    rule["updatedAt"] = now
    return json_response(_ptc_echo(rule, {"ecrRepositoryPrefix": prefix, "registryId": get_account_id(),
                                          "updatedAt": now}))


def _ptc_prefix_action(action, data):
    """The checks Delete and Validate share: registryId, then ecrRepositoryPrefix. Returns (error, rule)."""
    from ministack.app import AUTH
    refused = _ptc_registry_errors(action, data) or (AUTH and _ptc_other_registry(action, data))
    if refused:
        return refused, None
    errors = _ptc_prefix_errors("ecrRepositoryPrefix", data.get("ecrRepositoryPrefix"))
    if errors:
        return _invalid("; ".join(errors)), None
    prefix = _ptc_normalise(data["ecrRepositoryPrefix"])
    rule = _ptc_rules.get(prefix)
    return (_ptc_rule_missing(prefix), None) if rule is None else (None, rule)


def _delete_pull_through_cache_rule(data):
    refused, rule = _ptc_prefix_action("DeletePullThroughCacheRule", data)
    if refused:
        return refused
    del _ptc_rules[rule["ecrRepositoryPrefix"]]
    return json_response(_ptc_echo(rule, {
        "ecrRepositoryPrefix": rule["ecrRepositoryPrefix"], "upstreamRegistryUrl": rule["upstreamRegistryUrl"],
        "createdAt": rule["createdAt"], "registryId": get_account_id()}))


def _ptc_rule_shape(rule):
    item = {"ecrRepositoryPrefix": rule["ecrRepositoryPrefix"], "upstreamRegistryUrl": rule["upstreamRegistryUrl"],
            "createdAt": rule["createdAt"], "registryId": get_account_id()}
    if _ptc_authenticated(rule):
        item["upstreamRegistry"] = rule["upstreamRegistry"]
        item["updatedAt"] = rule["updatedAt"]
    return _ptc_echo(rule, item)


def _describe_pull_through_cache_rules(data):
    action = "DescribePullThroughCacheRules"
    from ministack.app import AUTH
    refused = _ptc_registry_errors(action, data) or (AUTH and _ptc_other_registry(action, data))
    if refused:
        return refused
    errors = []
    max_results = data.get("maxResults")
    if max_results is not None:
        if max_results < 1:
            errors.append(_param("maxResults", "Member must have value greater than or equal to 1"))
        elif max_results > 1000:
            errors.append(_param("maxResults", "Member must have value less than or equal to 1000"))
    prefixes = data.get("ecrRepositoryPrefixes")
    if prefixes is not None:
        if any(item is None for item in prefixes):
            errors.append(_param("ecrRepositoryPrefix", "must not be null"))
        elif not prefixes:
            errors.append(_param("ecrRepositoryPrefixes", "Member must have length greater than or equal to 1"))
        elif len(prefixes) > 100:
            errors.append(_param("ecrRepositoryPrefixes", "Member must have length less than or equal to 100"))
        elif any(_ptc_prefix_errors("ecrRepositoryPrefixes", item) for item in prefixes):
            errors.append(_param("ecrRepositoryPrefixes",
                                 "Member must satisfy constraint: [Member must have length less than or equal to 30, "
                                 "Member must have length greater than or equal to 2, "
                                 f"Member must satisfy regular expression pattern: {_PTC_PREFIX_PATTERN}]"))
    if errors:
        return _invalid("; ".join(errors))
    if prefixes is not None:
        if max_results is not None:
            return _invalid(_param("MaxResults", "Cannot provide MaxResults if providing RepositoryPrefixes"))
        if "nextToken" in data:
            return _invalid(_param("NextToken", "Cannot provide NextToken if providing RepositoryPrefixes"))
    rules = list(_ptc_rules.values())
    if prefixes is not None:
        wanted = list(dict.fromkeys(_ptc_normalise(item) for item in prefixes))
        for item in wanted:
            if item not in _ptc_rules:
                return _ptc_rule_missing(item)
        return json_response({"pullThroughCacheRules": [_ptc_rule_shape(r) for r in rules
                                                         if r["ecrRepositoryPrefix"] in wanted]})
    offset = 0
    token = data.get("nextToken")
    if token:
        try:
            offset = int(base64.b64decode(token, validate=True).decode().removeprefix("ptc:"), 10)
        except (ValueError, UnicodeError):
            offset = -1
        if offset < 0:
            return _invalid(_param("NextToken", "Invalid next token provided"))
    size = max_results or 100
    out = {"pullThroughCacheRules": [_ptc_rule_shape(r) for r in rules[offset:offset + size]]}
    if offset + size < len(rules):
        out["nextToken"] = base64.b64encode(f"ptc:{offset + size}".encode()).decode()
    return json_response(out)


def _ptc_parse_secret(value):
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return None
    if isinstance(parsed, dict) and isinstance(parsed.get("username"), str) \
            and isinstance(parsed.get("accessToken"), str):
        return parsed["username"], parsed["accessToken"]
    return None


def _validate_pull_through_cache_rule(data):
    refused, rule = _ptc_prefix_action("ValidatePullThroughCacheRule", data)
    if refused:
        return refused
    failure = _ptc_validate_failure(rule)
    out = {"ecrRepositoryPrefix": rule["ecrRepositoryPrefix"], "registryId": get_account_id()}
    if failure is None or failure[1] != _MSG_PTC_SECRET_UNREADABLE:
        out["upstreamRegistryUrl"] = rule["upstreamRegistryUrl"]
        out = _ptc_echo(rule, out)
    else:
        out = _ptc_echo({k: v for k, v in rule.items() if k != "credentialArn"}, out)
    out["isValid"] = failure is None
    out["failure"] = "" if failure is None else json.dumps(
        {"failureCode": failure[0], "failureReason": failure[1]}, separators=(",", ":"))
    return json_response(out)


_MSG_PTC_SECRET_UNREADABLE = (
    "The secret is unable to be accessed. Verify the resource permissions for the secret and try again.")
_MSG_PTC_PARSE_SECRET = "Unable to parse secret value. Secret must contain 'username' and 'accessToken' keys."
_MSG_PTC_ASSUME_ROLE = "Unable to access the IAM role. Please verify trust policy and try again."


def _ptc_validate_failure(rule):
    """(failureCode, failureReason) from the rule's own secret or role, else None. Nothing is sent anywhere."""
    from ministack.services import iam, kms, secretsmanager
    if rule.get("credentialArn") is not None:
        credential = rule["credentialArn"]
        state, key_id, value = secretsmanager.pull_through_secret(credential)
        if (state != "ok" or (key_id and kms.held_key_state(key_id) in ("Disabled", "PendingDeletion"))
                or _ptc_caller_identity_denied(credential) or _ptc_caller_policy_denied(credential)
                or _ptc_service_role_denied(credential)):
            return "UNABLE_TO_ACCESS_SECRET", _MSG_PTC_SECRET_UNREADABLE
        if _ptc_parse_secret(value) is None:
            return "UNABLE_TO_PARSE_SECRET", _MSG_PTC_PARSE_SECRET
    elif "customRoleArn" in rule and not iam.role_trusts_service(rule["customRoleArn"], _PTC_ROLE_SERVICE):
        return "UNABLE_TO_ASSUME_ROLE", _MSG_PTC_ASSUME_ROLE
    return None


# ── Registry policy ────────────────────────────────────────

_REGISTRY_POLICY_KEY = "policy"
_REGISTRY_POLICY_VERSIONS = ("2012-10-17", "2008-10-17")
_REGISTRY_POLICY_STATEMENT_KEYS = frozenset((
    "Sid", "Effect", "Principal", "NotPrincipal", "Action", "NotAction", "Resource", "NotResource", "Condition"))
_ECR_IAM_ONLY_ACTIONS = ("BatchImportUpstreamImage", "ReplicateImage")
_ecr_actions_cache = []


def _ecr_actions():
    if not _ecr_actions_cache:
        import botocore.session
        model = botocore.session.get_session().get_service_model("ecr")
        _ecr_actions_cache.extend(name.lower() for name in (*model.operation_names, *_ECR_IAM_ONLY_ACTIONS))
    return _ecr_actions_cache


def _registry_policy_action_known(action):
    if not isinstance(action, str) or not action:
        return False
    if action == "*":
        return True
    service, _, name = action.partition(":")
    if service.lower() != "ecr" or not name or "[" in name:
        return False
    pattern = re.compile(re.escape(name.lower()).replace(r"\*", ".*").replace(r"\?", "."))
    return any(pattern.fullmatch(known) for known in _ecr_actions())


def _registry_policy_principal(value):
    """The normalised Principal value, or None when it is not one AWS accepts."""
    if value == "*":
        return value
    if not isinstance(value, dict) or not value or set(value) - {"AWS", "Service"}:
        return None
    out = {}
    for key, entries in value.items():
        many = isinstance(entries, list)
        items = entries if many else [entries]
        if not items or not all(isinstance(e, str) and e for e in items):
            return None
        if key == "AWS":
            normalised = []
            for entry in items:
                if re.fullmatch(r"[0-9]{12}", entry):
                    entry = f"arn:aws:iam::{entry}:root"
                elif entry != "*" and not re.fullmatch(r"arn:aws[a-z-]*:(iam|sts)::[0-9]{12}:\S+", entry):
                    return None
                normalised.append(entry)
            items = normalised
        out[key] = items if many else items[0]
    return out


def _registry_policy_document(text):
    """The policy as AWS stores it, or None when AWS refuses it."""
    def no_constant(_value):
        raise ValueError("not JSON")

    try:
        doc = json.loads(text, parse_constant=no_constant)
    except (TypeError, ValueError, RecursionError):
        return None
    if not isinstance(doc, dict) or set(doc) - {"Version", "Id", "Statement"}:
        return None
    version = doc.get("Version", "2008-10-17")
    statements = doc.get("Statement")
    if version not in _REGISTRY_POLICY_VERSIONS or statements is None:
        return None
    statements = [statements] if isinstance(statements, dict) else statements
    if not isinstance(statements, list) or not statements:
        return None
    if "Id" in doc and not isinstance(doc["Id"], str):
        return None
    stored = []
    for statement in statements:
        if not isinstance(statement, dict) or set(statement) - _REGISTRY_POLICY_STATEMENT_KEYS:
            return None
        if statement.get("Effect") not in ("Allow", "Deny"):
            return None
        out = {}
        for key, value in statement.items():
            if key in ("Principal", "NotPrincipal"):
                value = _registry_policy_principal(value)
                if value is None:
                    return None
            elif key in ("Action", "NotAction"):
                items = value if isinstance(value, list) else [value]
                if not items or not all(_registry_policy_action_known(a) for a in items):
                    return None
                value = items[0] if len(items) == 1 else items
            elif key in ("Resource", "NotResource"):
                items = value if isinstance(value, list) else [value]
                if not items or not all(isinstance(r, str) and (r == "*" or r.startswith("arn:")) for r in items):
                    return None
            elif key == "Condition" and not isinstance(value, dict):
                return None
            elif key == "Sid" and not isinstance(value, str):
                return None
            out[key] = value
        if not ({"Principal", "NotPrincipal"} & set(out)) or not ({"Action", "NotAction"} & set(out)):
            return None
        stored.append(out)
    return {"Version": version, **({"Id": doc["Id"]} if "Id" in doc else {}), "Statement": stored}


def _put_registry_policy(data):
    text = data.get("policyText")
    if text is None:
        return _invalid(_param("PolicyText", "Cannot be null"))
    if not text:
        return _invalid(_param("PolicyText", "Must not be empty"))
    if len(text) > 10240:
        return _invalid(_param("PolicyText", "Must have length less than or equal to 10240"))
    if _registry_policy_document(text) is None:
        return _invalid(_param("PolicyText", "Invalid registry policy provided"))
    _registry_policies[_REGISTRY_POLICY_KEY] = text
    return json_response({"registryId": get_account_id(), "policyText": text})


def _registry_policy_missing():
    return error_response_json("RegistryPolicyNotFoundException",
                               f"Registry policy does not exist in the registry with id '{get_account_id()}'", 400)


def _get_registry_policy(data):
    stored = _registry_policies.get(_REGISTRY_POLICY_KEY)
    if stored is None:
        return _registry_policy_missing()
    return json_response({"registryId": get_account_id(), "policyText": stored})


def _delete_registry_policy(data):
    stored = _registry_policies.pop(_REGISTRY_POLICY_KEY, None)
    if stored is None:
        return _registry_policy_missing()
    return json_response({"registryId": get_account_id(), "policyText": stored})


# ── Repository creation templates ─────────

_RCT_ACTIONS = frozenset((
    "CreateRepositoryCreationTemplate", "UpdateRepositoryCreationTemplate",
    "DeleteRepositoryCreationTemplate", "DescribeRepositoryCreationTemplates",
))
_RCT_APPLIED_FOR = ("REPLICATION", "PULL_THROUGH_CACHE", "CREATE_ON_PUSH")
_RCT_MUTABILITY = ("IMMUTABLE", "MUTABLE", "MUTABLE_WITH_EXCLUSION", "IMMUTABLE_WITH_EXCLUSION")
_RCT_ENCRYPTION = ("AES256", "KMS", "KMS_DSSE")
_RCT_KMS_RE = re.compile(r"^$|arn:aws[a-z0-9-]*:kms:[a-z0-9-]+:[0-9]{12}:key\/[a-z0-9-]+")
_RCT_FILTER_RE = re.compile(r"^[0-9a-zA-Z._*-]{1,128}$")
_RCT_SETTINGS = ("description", "encryptionConfiguration", "resourceTags", "imageTagMutability",
                 "imageTagMutabilityExclusionFilters", "repositoryPolicy", "lifecyclePolicy", "appliedFor",
                 "customRoleArn")


def _rct_prefix_errors(value):
    if value is None:
        return [_param("prefix", "Member must not be null")]
    if not isinstance(value, str):
        return [_param("prefix", "Member must be a string")]
    errors = []
    if not _PTC_PREFIX_RE.fullmatch(value):
        errors.append(_param("prefix", f"Member must satisfy regular expression pattern: {_PTC_PREFIX_PATTERN}"))
    if not 1 <= len(value) <= 256:
        errors.append(_param("prefix", "Member must have length between 1 and 256"))
    return errors


def _rct_max(errors, data, name, limit):
    value = data.get(name)
    if value is None:
        return
    if not isinstance(value, str):
        errors.append(_param(name, "Member must be a string"))
    elif len(value) > limit:
        errors.append(_param(name, f"Member must have length less than or equal to {limit}"))


def _rct_settings_errors(data, creating):
    errors = []
    applied = data.get("appliedFor")
    if applied is None:
        if creating:
            errors.append(_param("appliedFor", "Member must not be null"))
    elif not isinstance(applied, list) or any(v not in _RCT_APPLIED_FOR for v in applied):
        errors.append(_param("appliedFor", f"Member must satisfy enum value set: [{', '.join(_RCT_APPLIED_FOR)}]"))
    _rct_max(errors, data, "description", 256)
    _rct_max(errors, data, "repositoryPolicy", 10240)
    _rct_max(errors, data, "lifecyclePolicy", 30720)
    _rct_max(errors, data, "customRoleArn", 2048)
    mutability = data.get("imageTagMutability")
    if mutability is not None and mutability not in _RCT_MUTABILITY:
        errors.append(_param("imageTagMutability",
                             f"Member must satisfy enum value set: [{', '.join(_RCT_MUTABILITY)}]"))
    filters = data.get("imageTagMutabilityExclusionFilters")
    if filters is not None:
        if not isinstance(filters, list) or not 1 <= len(filters) <= 5:
            errors.append(_param("imageTagMutabilityExclusionFilters", "Member must have length between 1 and 5"))
        elif any(not isinstance(f, dict) or f.get("filterType") != "WILDCARD"
                 or not isinstance(f.get("filter"), str) or not _RCT_FILTER_RE.fullmatch(f["filter"])
                 for f in filters):
            errors.append(_param("imageTagMutabilityExclusionFilters",
                                 "Member must have filterType WILDCARD and a filter matching ^[0-9a-zA-Z._*-]{1,128}$"))
    encryption = data.get("encryptionConfiguration")
    if encryption is not None:
        if not isinstance(encryption, dict) or encryption.get("encryptionType") not in _RCT_ENCRYPTION:
            errors.append(_param("encryptionConfiguration.encryptionType",
                                 f"Member must satisfy enum value set: [{', '.join(_RCT_ENCRYPTION)}]"))
        else:
            key = encryption.get("kmsKey")
            if key is not None and (not isinstance(key, str) or len(key) > 2048 or not _RCT_KMS_RE.fullmatch(key)):
                errors.append(_param("encryptionConfiguration.kmsKey",
                                     f"Member must satisfy regular expression pattern: {_RCT_KMS_RE.pattern}"))
    tags = data.get("resourceTags")
    if tags is not None and (not isinstance(tags, list) or any(
            not isinstance(t, dict) or not isinstance(t.get("Key"), str) or not isinstance(t.get("Value"), str)
            for t in tags)):
        errors.append(_param("resourceTags", "Member must have a Key and a Value"))
    return errors


def _rct_refused(errors):
    return _invalid(errors[0]) if errors else None


def _rct_missing(prefix):
    return error_response_json("TemplateNotFoundException",
                               f"A repository creation template with prefix '{prefix}' is not found "
                               f"in the registry with id '{get_account_id()}'", 400)


def _rct_shape(template):
    return {k: copy.deepcopy(v) for k, v in template.items() if v is not None}


def _rct_answer(template):
    return json_response({"registryId": get_account_id(), "repositoryCreationTemplate": _rct_shape(template)})


def _create_creation_template(data):
    prefix = data.get("prefix")
    refused = _rct_refused(_rct_prefix_errors(prefix) + _rct_settings_errors(data, creating=True))
    if refused:
        return refused
    if prefix in _creation_templates:
        return error_response_json("TemplateAlreadyExistsException",
                                   f"A repository creation template with prefix '{prefix}' already exists "
                                   f"in the registry with id '{get_account_id()}'", 400)
    now = int(time.time())
    template = {"prefix": prefix, **{k: copy.deepcopy(data[k]) for k in _RCT_SETTINGS if data.get(k) is not None},
                "createdAt": now, "updatedAt": now}
    template["appliedFor"] = list(dict.fromkeys(template["appliedFor"]))
    _creation_templates[prefix] = template
    return _rct_answer(template)


def _update_creation_template(data):
    prefix = data.get("prefix")
    refused = _rct_refused(_rct_prefix_errors(prefix) + _rct_settings_errors(data, creating=False))
    if refused:
        return refused
    template = _creation_templates.get(prefix)
    if template is None:
        return _rct_missing(prefix)
    for key in _RCT_SETTINGS:
        if data.get(key) is not None:
            template[key] = copy.deepcopy(data[key])
    template["appliedFor"] = list(dict.fromkeys(template["appliedFor"]))
    template["updatedAt"] = int(time.time())
    return _rct_answer(template)


def _delete_creation_template(data):
    prefix = data.get("prefix")
    refused = _rct_refused(_rct_prefix_errors(prefix))
    if refused:
        return refused
    template = _creation_templates.pop(prefix, None)
    if template is None:
        return _rct_missing(prefix)
    return _rct_answer(template)


def _describe_creation_templates(data):
    prefixes = data.get("prefixes")
    errors = []
    if prefixes is not None:
        if not isinstance(prefixes, list):
            errors.append(_param("prefixes", "Member must be a list"))
        else:
            for value in prefixes:
                errors += _rct_prefix_errors(value)
    limit = data.get("maxResults")
    if limit is not None and (isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000):
        errors.append(_param("maxResults", "Member must have value between 1 and 1000"))
    refused = _rct_refused(errors)
    if refused:
        return refused
    wanted = set(prefixes) if prefixes else None
    items = [_rct_shape(t) for p, t in sorted(_creation_templates.items()) if wanted is None or p in wanted]
    try:
        start = max(int(data.get("nextToken") or 0), 0)
    except (TypeError, ValueError):
        return _invalid(_param("nextToken", "Invalid pagination token"))
    end = start + (limit or 100)
    body = {"registryId": get_account_id(), "repositoryCreationTemplates": items[start:end]}
    if end < len(items):
        body["nextToken"] = str(end)
    return json_response(body)


def _rct_for(repo, applied_for):
    """The template a repository ECR creates for *applied_for* takes: the longest prefix covering *repo*, else ROOT."""
    best = None
    for prefix, template in _creation_templates.items():
        if applied_for not in template.get("appliedFor", []):
            continue
        base = prefix.rstrip("/")
        if prefix != "ROOT" and repo.startswith(base + "/") and (best is None or len(base) > len(best[0])):
            best = (base, template)
    if best:
        return best[1]
    root = _creation_templates.get("ROOT")
    return root if root and applied_for in root.get("appliedFor", []) else None


def _rct_create_repository(repo, applied_for):
    """Create *repo* the way ECR does on its own behalf: with the matching template's settings, else the defaults."""
    template = _rct_for(repo, applied_for) or {}
    record = {
        "repositoryArn": _repo_arn(repo), "registryId": _registry_id(), "repositoryName": repo,
        "repositoryUri": _repo_uri(repo), "createdAt": int(time.time()),
        "imageTagMutability": template.get("imageTagMutability", "MUTABLE"),
        "imageScanningConfiguration": {"scanOnPush": False},
        "encryptionConfiguration": copy.deepcopy(template.get("encryptionConfiguration")
                                                 or {"encryptionType": "AES256"}),
        "tags": copy.deepcopy(template.get("resourceTags") or []),
    }
    if template.get("imageTagMutabilityExclusionFilters"):
        record["imageTagMutabilityExclusionFilters"] = copy.deepcopy(template["imageTagMutabilityExclusionFilters"])
    _repositories[repo] = record
    _images[repo] = []
    if template.get("repositoryPolicy"):
        _repo_policies[repo] = template["repositoryPolicy"]
    if template.get("lifecyclePolicy"):
        _lifecycle_policies[repo] = template["lifecyclePolicy"]


# ── Pull-through cache: the upstream image ECS runs ─────────

def _ptc_match(name):
    """``(rule, upstream path)`` of the longest rule prefix covering repository *name*, or (None, None)."""
    best = None
    for prefix, rule in _ptc_rules.items():
        if prefix != "ROOT" and name.startswith(prefix + "/") and (best is None or len(prefix) > len(best[0])):
            best = (prefix, rule)
    if best is None:
        root = _ptc_rules.get("ROOT")
        return (root, name) if root is not None else (None, None)
    prefix, rule = best
    rest = name[len(prefix) + 1:]
    upstream_prefix = rule.get("upstreamRepositoryPrefix")
    return rule, f"{upstream_prefix}/{rest}" if upstream_prefix else rest


def _ptc_import_allowed(account, region, repo, principal_arn):
    """Under AUTH=true, another account may pull through this registry's cache only when the registry or repository
    policy allows it ecr:BatchImportUpstreamImage, plus ecr:CreateRepository from the registry policy when the
    repository does not exist yet (ECR guide, pull-through-cache-iam)."""
    from ministack.app import AUTH
    if not AUTH or not principal_arn or principal_arn.split(":")[4] == account:
        return True
    from ministack.core.iam_evaluator import EvalContext, resource_policy_allows

    def allows(policy, action):
        return policy is not None and resource_policy_allows(policy, EvalContext(
            principal_arn=principal_arn, principal_account=principal_arn.split(":")[4],
            principal_type="AssumedRole" if ":role/" in principal_arn else "Root", action=action,
            resource_arn=f"arn:aws:ecr:{region}:{account}:repository/{repo}", region=region), same_account=False)
    registry = _registry_policies.get(_REGISTRY_POLICY_KEY)
    if repo not in _repositories:
        return allows(registry, "ecr:CreateRepository") and allows(registry, "ecr:BatchImportUpstreamImage")
    return allows(registry, "ecr:BatchImportUpstreamImage") or allows(_repo_policies.get(repo),
                                                                       "ecr:BatchImportUpstreamImage")


# ── Pull-through cache fill ─────────────────────────────────
# A tag miss under a rule's prefix is filled once and then served like a pushed image. The upstream image is
# obtained the way EC2 obtains one (the host daemon's copy, else a daemon pull); nothing is pushed anywhere and
# the rule's secret is never sent. An ECR upstream is copied in-process from MiniStack's own ECR.

_PTC_LAYER_MEDIA_TYPE = "application/vnd.docker.image.rootfs.diff.tar.gzip"
_PTC_CONFIG_MEDIA_TYPE = "application/vnd.docker.container.image.v1+json"
_PTC_DOCKER_HINT = "mount the Docker socket (/var/run/docker.sock) so MiniStack can fill pull-through cache repositories"


class PullThroughMiss(Exception):
    """A rule covers the image but it cannot be filled. *kind* is "unavailable", "secret" or "repository"."""

    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind


_ptc_docker_client = None


def _ptc_docker():
    """A client for whole-image transfers (pull, save, load): no read timeout, since how long one takes depends on
    the image's size and the host, not on a fixed limit."""
    global _ptc_docker_client
    if _ptc_docker_client is None:
        try:
            import docker
            _ptc_docker_client = docker.from_env(timeout=None)
        except Exception:
            return None
    return _ptc_docker_client


def _transfer_client(client):
    """*client* for a whole-image transfer: a real Docker client is swapped for the untimed one, a stand-in kept."""
    try:
        import docker
    except ImportError:
        return client
    if isinstance(client, docker.DockerClient):
        return _ptc_docker() or client
    return client


def _ptc_service_role_session():
    return f"arn:aws:sts::{get_account_id()}:assumed-role/{_PTC_ROLE_NAME}/ecr-pullthroughcache"


def _ptc_service_role_denied(credential):
    """Under AUTH=true, whether the service-linked role cannot read the rule's secret: a Deny for it in the
    secret's resource policy, or a customer managed key whose policy does not name it for kms:Decrypt."""
    from ministack.app import AUTH
    if not AUTH:
        return False
    from ministack.core.iam_evaluator import EvalContext, evaluate_resource_policy
    from ministack.services import kms, secretsmanager
    session = _ptc_service_role_session()
    found = secretsmanager.secret_policy_for_arn(credential)
    if found and found[0] and evaluate_resource_policy(found[0], EvalContext(
            principal_arn=session, principal_type="AssumedRole", principal_account=get_account_id(),
            action="secretsmanager:GetSecretValue", resource_arn=credential, region=get_region())).decision == "Deny":
        return True
    _, key_id, _ = secretsmanager.pull_through_secret(credential)
    return bool(key_id) and not kms.key_policy_names(key_id, "Decrypt", session)


def _ptc_store(repo, tag, manifest_bytes, media_type, blobs, manifests=None):
    """Store a manifest, its child manifests and its blobs under cache repository *repo*, tagged *tag*; a new
    repository takes the matching PULL_THROUGH_CACHE creation template, else AWS's defaults."""
    if repo not in _repositories:
        _rct_create_repository(repo, "PULL_THROUGH_CACHE")
    digest = _content_digest(manifest_bytes)
    _layer_blobs.setdefault(repo, {}).update(blobs)
    _manifest_blobs.setdefault(repo, {}).update(manifests or {})
    _manifest_blobs.setdefault(repo, {})[digest] = manifest_bytes
    images = _images.setdefault(repo, [])
    for img in images:
        if tag in img.get("imageTags", []) and img.get("imageDigest") != digest:
            img["imageTags"].remove(tag)
    record = next((img for img in images if img.get("imageDigest") == digest), None)
    if record is None:
        layers = json.loads(manifest_bytes).get("layers") or []
        record = {"registryId": _registry_id(), "repositoryName": repo, "imageId": {"imageDigest": digest},
                  "imageManifest": manifest_bytes.decode("utf-8", errors="replace"),
                  "imageManifestMediaType": media_type, "imageTags": [], "imagePushedAt": int(time.time()),
                  "imageDigest": digest,
                  "imageSizeInBytes": sum(layer.get("size", 0) for layer in layers) or len(manifest_bytes)}
        images.append(record)
    if tag not in record["imageTags"]:
        record["imageTags"].append(tag)
    record["imageId"]["imageTag"] = tag
    return digest


def _ptc_saved_blobs(save_stream):
    """(manifest bytes, media type, blobs, child manifests) of a ``docker save`` stream.

    An OCI-layout export (Docker's containerd image store) holds the upstream's own index, manifests and compressed
    layers: they are kept byte for byte, so the digests are the upstream's. A classic export holds one platform's
    config and uncompressed layers: each layer is gzipped and addressed by the gzip's digest, so the manifest
    digest is this host's, not the upstream's."""
    import gzip
    import io
    import tarfile

    buf = io.BytesIO()
    for chunk in save_stream:
        buf.write(chunk)
    buf.seek(0)
    blobs, layers = {}, []
    with tarfile.open(fileobj=buf, mode="r") as tar:
        upstream = _ptc_oci_blobs(tar)
        if upstream is not None:
            return upstream

        def read(name):
            member = tar.extractfile(name)
            if member is None:
                raise PullThroughMiss("unavailable", f"the daemon's image archive has no {name}")
            return member.read()

        entry = json.loads(read("manifest.json"))[0]
        config = read(entry["Config"])
        config_digest = _content_digest(config)
        blobs[config_digest] = config
        for name in entry.get("Layers", []):
            raw = read(name)
            data = raw if raw[:2] == b"\x1f\x8b" else gzip.compress(raw, mtime=0)
            digest = _content_digest(data)
            blobs[digest] = data
            layers.append({"mediaType": _PTC_LAYER_MEDIA_TYPE, "size": len(data), "digest": digest})
    manifest = {"schemaVersion": 2, "mediaType": _DEFAULT_MANIFEST_MEDIA_TYPE,
                "config": {"mediaType": _PTC_CONFIG_MEDIA_TYPE, "size": len(config), "digest": config_digest},
                "layers": layers}
    return json.dumps(manifest).encode(), _DEFAULT_MANIFEST_MEDIA_TYPE, blobs, {}


def _ptc_oci_blobs(tar):
    """The upstream's bytes from an OCI-layout export, or None when the export is not one or misses the image."""
    names = set(tar.getnames())
    if "index.json" not in names or "oci-layout" not in names:
        return None

    def blob(digest):
        name = f"blobs/sha256/{str(digest).split(':', 1)[-1]}"
        member = tar.extractfile(name) if name in names else None
        data = member.read() if member is not None else None
        return data if data is not None and _content_digest(data) == digest else None

    try:
        top = json.loads(tar.extractfile("index.json").read())["manifests"][0]
        top_bytes = blob(top["digest"])
        if top_bytes is None:
            return None
        parsed = json.loads(top_bytes)
        media_type = parsed.get("mediaType") or top.get("mediaType") or _DEFAULT_MANIFEST_MEDIA_TYPE
        manifests, singles = {}, []
        if "manifests" in parsed:
            for child in parsed["manifests"]:
                data = blob(child.get("digest"))
                if data is not None:
                    manifests[child["digest"]] = data
                    singles.append(json.loads(data))
        else:
            singles.append(parsed)
        blobs, complete = {}, False
        for single in singles:
            refs = [single.get("config") or {}] + list(single.get("layers") or [])
            found = {ref.get("digest"): blob(ref.get("digest")) for ref in refs}
            blobs.update({d: data for d, data in found.items() if data is not None})
            complete = complete or (bool(single.get("config")) and all(v is not None for v in found.values()))
    except (KeyError, IndexError, TypeError, ValueError, AttributeError):
        return None
    return (top_bytes, media_type, blobs, manifests) if complete else None


def _ptc_daemon_image(client, ref):
    """The daemon's image for *ref*: its local copy, else a pull (EC2's order). Raises on failure."""
    try:
        return client.images.get(ref)
    except Exception:
        image = client.images.pull(ref)
        return image[0] if isinstance(image, list) else image


def _ptc_fill_child(repo, digest, principal_arn, docker_client=None):
    """Fetch child manifest *digest* of an index the cache holds for *repo* but not the child, through the daemon by
    digest, and store it with its blobs. Returns True when stored; False when *digest* is no such child."""
    if not _DIGEST_RE.fullmatch(str(digest)) or digest in _manifest_blobs.get(repo, {}):
        return False
    rule, path = _ptc_match(repo)
    if rule is None or rule["upstreamRegistry"] == "ecr" or repo not in _repositories:
        return False
    listed = any(child.get("digest") == digest
                 for data in list(_manifest_blobs.get(repo, {}).values())
                 for child in (_ptc_json(data).get("manifests") or []))
    if not listed or not _ptc_import_allowed(get_account_id(), get_region(), repo, principal_arn):
        return False
    if rule.get("credentialArn") is not None and _ptc_service_role_denied(rule["credentialArn"]):
        raise PullThroughMiss("secret", "unable to retrieve the pull through cache rule's upstream registry "
                                        "credentials")
    client = _transfer_client(docker_client) if docker_client is not None else _ptc_docker()
    if client is None:
        logger.warning("ECR pull-through cache: no Docker daemon to fetch %s@%s; %s", repo, digest, _PTC_DOCKER_HINT)
        raise PullThroughMiss("unavailable", "Unable to reach the upstream registry.")
    ref = f"{rule['upstreamRegistryUrl']}/{path}@{digest}"
    try:
        saved = _ptc_saved_blobs(_ptc_daemon_image(client, ref).save())
    except PullThroughMiss:
        raise
    except Exception as exc:
        logger.warning("ECR pull-through cache: the daemon could not pull %s (%s)", ref, exc)
        raise PullThroughMiss("unavailable", "Unable to reach the upstream registry.") from exc
    manifest, _media_type, blobs, children = saved
    data = manifest if _content_digest(manifest) == digest else children.get(digest)
    if data is None:
        logger.warning("ECR pull-through cache: the daemon's export of %s does not keep the upstream manifest "
                       "(a classic image store); only the host's platform is cached", ref)
        raise PullThroughMiss("unavailable", "Unable to reach the upstream registry.")
    _layer_blobs.setdefault(repo, {}).update(blobs)
    _manifest_blobs.setdefault(repo, {})[digest] = data
    return True


def _ptc_json(data):
    try:
        parsed = json.loads(data)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _ptc_fill_from_daemon(rule, repo, path, tag, docker_client):
    if rule.get("credentialArn") is not None and _ptc_service_role_denied(rule["credentialArn"]):
        raise PullThroughMiss("secret", "unable to retrieve the pull through cache rule's upstream registry "
                                        "credentials")
    client = _transfer_client(docker_client) if docker_client is not None else _ptc_docker()
    if client is None:
        logger.warning("ECR pull-through cache: no Docker daemon to fill %s; %s", repo, _PTC_DOCKER_HINT)
        raise PullThroughMiss("unavailable", "Unable to reach the upstream registry.")
    ref = f"{rule['upstreamRegistryUrl']}/{path}:{tag}"
    try:
        image = _ptc_daemon_image(client, ref)
    except Exception as exc:
        logger.warning("ECR pull-through cache: %s is not on the host and the daemon could not pull it (%s)",
                       ref, exc)
        raise PullThroughMiss("unavailable", "Unable to reach the upstream registry.") from exc
    manifest, media_type, blobs, children = _ptc_saved_blobs(image.save())
    return _ptc_store(repo, tag, manifest, media_type, blobs, children)


def _ptc_fill_from_ecr(rule, repo, path, tag):
    """Copy *path*:*tag* from the upstream account and region's own MiniStack ECR, as the rule's role."""
    from ministack.app import AUTH
    from ministack.services import iam
    role = rule.get("customRoleArn")
    if not role or not iam.role_trusts_service(role, _PTC_ROLE_SERVICE):
        raise PullThroughMiss("unavailable", "Unable to reach the upstream registry.")
    up_account, up_region = _PTC_ECR_URL_RE.fullmatch(rule["upstreamRegistryUrl"]).groups()
    missing = PullThroughMiss("repository", f"The repository with name '{repo}' does not exist in the registry "
                                            f"with id '{get_account_id()}'")
    with request_scope(up_account, up_region):
        if path not in _repositories:
            raise missing
        if AUTH:
            from ministack.core.iam_evaluator import EvalContext, resource_policy_allows
            policy = _repo_policies.get(path)
            if not all(policy is not None and resource_policy_allows(policy, EvalContext(
                    principal_arn=role, principal_account=role.split(":")[4], principal_type="AssumedRole",
                    action=action, resource_arn=f"arn:aws:ecr:{up_region}:{up_account}:repository/{path}",
                    region=up_region), same_account=False)
                    for action in ("ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer")):
                raise PullThroughMiss("unavailable", "Unable to reach the upstream registry.")
        digest, manifest, media_type = _resolve_manifest(path, tag)
        if manifest is None:
            raise PullThroughMiss("unavailable", "Unable to reach the upstream registry.")
        held_blobs, held_manifests = _layer_blobs.get(path, {}), _manifest_blobs.get(path, {})
        blobs, children = {}, {}
        try:
            parsed = json.loads(manifest)
            singles = [parsed]
            for child in parsed.get("manifests") or []:
                if child.get("digest") in held_manifests:
                    children[child["digest"]] = held_manifests[child["digest"]]
                    singles.append(json.loads(held_manifests[child["digest"]]))
            for single in singles:
                for ref in [single.get("config") or {}] + list(single.get("layers") or []):
                    if ref.get("digest") in held_blobs:
                        blobs[ref["digest"]] = held_blobs[ref["digest"]]
        except (TypeError, ValueError, AttributeError):
            raise PullThroughMiss("unavailable", "Unable to reach the upstream registry.")
    _manifest_blobs.setdefault(repo, {}).update(children)
    return _ptc_store(repo, tag, manifest, media_type, blobs)


def fill_pull_through(image, principal_arn=None, docker_client=None):
    """Fill a pull-through cache miss for *image* (``<registry>/<repo>:<tag>`` of this MiniStack) and return the
    stored manifest digest; None when *image* is not a tag miss a rule covers for *principal_arn*.
    Raises PullThroughMiss when a rule covers it but it cannot be filled."""
    host, _, rest = (image or "").partition("/")
    match = _ECR_REGISTRY_HOST_RE.match(host.lower() + ".")
    if not match or not rest or "@" in rest:
        return None
    repo, tag = rest.rsplit(":", 1) if ":" in rest.rsplit("/", 1)[-1] else (rest, "latest")
    account, region = match.groups()
    with request_scope(account, region):
        return _ptc_fill(repo, tag, principal_arn or f"arn:aws:iam::{account}:root", docker_client)


def _ptc_fill(repo, tag, principal_arn, docker_client=None):
    """fill_pull_through within the cache registry's request scope."""
    if repo in _repositories and _find_image(repo, {"imageTag": tag}):
        return None
    rule, path = _ptc_match(repo)
    if rule is None or not _ptc_import_allowed(get_account_id(), get_region(), repo, principal_arn):
        return None
    if rule["upstreamRegistry"] == "ecr":
        return _ptc_fill_from_ecr(rule, repo, path, tag)
    return _ptc_fill_from_daemon(rule, repo, path, tag, docker_client)
