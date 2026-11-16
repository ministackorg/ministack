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
    get_account_id,
    get_region,
    new_uuid,
    request_scope,
    set_request_account_id,
    set_request_region,
)
from ministack.services import ecr_pull_through
from ministack.services.ecr_registry_policy import error_response_json, json_response

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


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "repositories": copy.deepcopy(_repositories),
        "images": copy.deepcopy(_images),
        "lifecycle_policies": copy.deepcopy(_lifecycle_policies),
        "repo_policies": copy.deepcopy(_repo_policies),
        "layer_blobs": copy.deepcopy(_layer_blobs),
        "manifest_blobs": copy.deepcopy(_manifest_blobs),
        **ecr_pull_through.get_state(),
        # _uploads is intentionally NOT persisted — in-flight upload sessions
        # do not survive a restart, matching real registry behaviour where
        # the client retries the upload from scratch after a 404 on PATCH.
    }


def load_persisted_state(data):
    return _restore_state(data)


def _restore_state(data):
    if not data:
        return

    ecr_pull_through.load_persisted_state(data)
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


def _cache_scoped(name):
    """Whether repository *name* is a pull-through cache repository or lies on a path a rule covers. The answers
    captured for those paths (r07, r15) apply only there; every other repository keeps its v1.5.39 answer."""
    return isinstance(name, str) and (ecr_pull_through.is_cache_repository(name) or _on_rule_path(name))


def _on_rule_path(name):
    """Whether a rule's prefix covers *name* (a trailing '/' or a '//' upstream path included)."""
    return any(prefix == "ROOT" or name.startswith(prefix + "/") for prefix in ecr_pull_through._rules)


def _repo_not_found_message(name, captured_everywhere=False):
    """The registry-id wording is captured for BatchGetImage and DescribeRepositories on a path no rule covers
    (r07-pull-no-rule, r15-get-no-rule--repo: *captured_everywhere*) and, for the other operations, on rule paths."""
    if captured_everywhere or _cache_scoped(name):
        return f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'"
    return f"The repository with name '{name}' does not exist"


def _repo_uri(name):
    return f"{get_account_id()}.dkr.ecr.{get_region()}.amazonaws.com/{name}"


def _image_digest(manifest):
    raw = manifest.encode() if isinstance(manifest, str) else manifest
    return "sha256:" + hashlib.sha256(raw).hexdigest()


async def handle_request(method, path, headers, body, query_params):
    target = headers.get("x-amz-target", "")
    action = target.split(".")[-1] if "." in target else ""

    if action in ecr_pull_through.ACTIONS:
        # Pull-through cache actions read the raw body: AWS refuses malformed JSON and member types its own way.
        return await asyncio.to_thread(ecr_pull_through.handle_action, action, body)

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
    }

    handler = handlers.get(action)
    if not handler:
        return error_response_json("InvalidAction", f"Unknown action: {action}", 400)

    if action in _CROSS_ACCOUNT_ACTIONS:
        owner = data.get("registryId")
        if (isinstance(owner, str) and owner != get_account_id() and re.fullmatch(r"[0-9]{12}", owner)
                and _owner_serves_cache(owner, data.get("repositoryName"))):
            return await _other_registry(action, owner, data, handler)
    if action in ("BatchGetImage", "GetDownloadUrlForLayer"):
        # Only a request on a cache repository can fetch from an upstream registry: it leaves the event loop.
        return await ecr_pull_through.run_fetching(
            data.get("repositoryName"), lambda: _action_fetches(action, data), handler, data)
    return handler(data)


def _action_fetches(action, data):
    """Whether *action* would contact an upstream registry: an image or layer it asks for is not held."""
    name = data.get("repositoryName")
    if action == "BatchGetImage":
        return ecr_pull_through.images_need_fetch(name, data.get("imageIds"))
    digest = data.get("layerDigest")
    return ecr_pull_through.layers_need_fetch(name, [digest])


_CROSS_ACCOUNT_ACTIONS = frozenset({"BatchGetImage", "DescribeImages"})


def _owner_serves_cache(owner, name):
    """Whether *name* is a cache repository, or on a rule path, in registry *owner*. Only those are captured as
    refused without a repository policy (r11-a2-pull-no-grant, r11-a2-describe-images-a1); any other registryId
    is ignored as on v1.5.39."""
    with request_scope(owner, get_region()):
        return _cache_scoped(name)


async def _other_registry(action, owner, data, handler):
    """Run *handler* in the registry of account *owner* when the repository policy there grants the caller
    the action; otherwise refuse as AWS does (a registry policy alone grants nothing)."""
    from ministack.core.iam_evaluator import EvalContext, caller_arn, resource_policy_allows

    name = data.get("repositoryName", "")
    region = get_region()
    resource = f"arn:aws:ecr:{region}:{owner}:repository/{name}"
    caller = caller_arn()
    ctx = EvalContext(principal_arn=caller, principal_account=get_account_id(),
                      principal_type="Root" if caller.endswith(":root") else
                      "AssumedRole" if ":assumed-role/" in caller else "User",
                      action=f"ecr:{action}", resource_arn=resource, region=region)
    policy = _repo_policies.get_scoped(owner, region, name)
    if not policy or not resource_policy_allows(policy, ctx, same_account=False):
        message = (f"User: {caller} is not authorized to perform: ecr:{action} on resource: {resource} "
                   f"because no resource-based policy allows the ecr:{action} action")
        return error_response_json("AccessDeniedException", message, 400, message_key="Message")

    def run():
        with request_scope(owner, region):
            return handler(data)

    if action in ("BatchGetImage", "GetDownloadUrlForLayer"):
        # These can fill from an upstream registry: keep the blocking fetch off the event loop.
        with request_scope(owner, region):
            fetches = _action_fetches(action, data)
        if fetches:
            return await ecr_pull_through.run_in_fill_pool(run)
    return run()


def _policy_not_found(kind, name):
    """The wording of r07-cache-repo-policy and r07-cache-lifecycle on a cache repository, else v1.5.39's."""
    if _cache_scoped(name):
        return (f"{kind} policy does not exist for the repository with name '{name}' "
                f"in the registry with id '{_registry_id()}'")
    return f"{kind} policy does not exist for '{name}'"


def _create_repository(data):
    name = data.get("repositoryName", "")
    if not name:
        return error_response_json("InvalidParameterException", "repositoryName is required", 400)
    if name in _repositories:
        # r07-post-create-repo-again (a rule path); any other repository keeps the v1.5.39 text.
        suffix = f" in the registry with id '{_registry_id()}'" if _cache_scoped(name) else ""
        return error_response_json("RepositoryAlreadyExistsException",
                                   f"The repository with name '{name}' already exists{suffix}", 400)

    repo = {
        "repositoryArn": _repo_arn(name),
        "registryId": _registry_id(),
        "repositoryName": name,
        "repositoryUri": _repo_uri(name),
        # Fractional seconds on rule paths (r07-post-create-repo-under-prefix).
        # Fractional seconds for every repository: r07-pre-pull-into-existing--repo, r18-root-pull-into-plain--repo
        # (repositories CreateRepository made before any rule covered them).
        "createdAt": round(time.time(), 3),
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
                                           _repo_not_found_message(n, captured_everywhere=True), 400)
            repos.append(_repositories[n])
        return json_response({"repositories": [_repo_shape(r) for r in repos]})
    return _paged(data, "repositories", [_repo_shape(r) for r in _repositories.values()])


def _delete_repository(data):
    name = data.get("repositoryName", "")
    force = data.get("force", False)

    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   _repo_not_found_message(name), 400)

    if not force and _images.get(name):
        if _cache_scoped(name):  # r07-cache-delete-repo-no-force
            message = (f"The repository with name '{name}' in registry with id '{_registry_id()}' "
                       "cannot be deleted because it still contains images")
        else:
            message = f"The repository with name '{name}' is not empty"
        return error_response_json("RepositoryNotEmptyException", message, 400)

    repo = _repositories.pop(name)
    _images.pop(name, None)
    _lifecycle_policies.pop(name, None)
    _repo_policies.pop(name, None)
    ecr_pull_through.forget_repository(name)
    _layer_blobs.pop(name, None)
    _manifest_blobs.pop(name, None)
    return json_response({"repository": _repo_shape(repo)})


def _put_image(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   _repo_not_found_message(name), 400)

    manifest = data.get("imageManifest", "")
    manifest_type = data.get("imageManifestMediaType",
                             "application/vnd.docker.distribution.manifest.v2+json")
    tag = data.get("imageTag")
    digest = data.get("imageDigest") or _image_digest(manifest)

    missing_layers = ecr_pull_through.cache_only_layers(name, manifest)
    if missing_layers:
        return error_response_json(
            "LayersNotFoundException",
            f"Layers with digests '[{','.join(missing_layers)}]' required for pushing image into repository with "
            f"name '{name}' in the registry with id '{_registry_id()}' do not exist", 400)

    repo = _repositories[name]
    if tag and _cache_scoped(name) and any(  # r07-cache-put-image-upstream-tag
            img["imageDigest"] == digest and tag in img.get("imageTags", []) for img in _images[name]):
        return error_response_json(
            "ImageAlreadyExistsException",
            f"Image with digest '{digest}' and tag '{tag}' already exists in the repository with name "
            f"'{name}' in registry with id '{_registry_id()}'", 400)
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


def _image_ids_wire_error(image_ids):
    """The SerializationException AWS gives a JSON type that is not the member's shape (same answers as
    ecr_pull_through's member checks); None when *image_ids* is a list of objects (or absent)."""
    serialization_error = ecr_pull_through._serialization_error
    if image_ids is None:
        return None
    if isinstance(image_ids, dict):
        return serialization_error("Start of structure or map found where not expected")
    if not isinstance(image_ids, list):
        return serialization_error("Unexpected field type")
    for item in image_ids:
        if isinstance(item, list):
            return serialization_error()
        if not isinstance(item, dict):
            return serialization_error("Unexpected field type")
    return None


def _batch_get_image(data):
    name = data.get("repositoryName", "")
    if not isinstance(name, str):
        # v1.5.39 answer for a name that cannot be a repository (null, a number, a list, an object).
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist", 400)
    image_ids = data.get("imageIds", [])
    wire_error = _image_ids_wire_error(image_ids)
    if wire_error is not None:
        return wire_error
    image_ids = image_ids or []
    if _cache_scoped(name):
        # r07-pull-prefix-bad-name, r07-pull-prefix-no-image-ids: captured on rule paths only; any other
        # repository is not validated, as on v1.5.39.
        if not _REPO_NAME_RE.fullmatch(name):
            return error_response_json(
                "InvalidParameterException",
                f"Invalid parameter at 'repositoryName' failed to satisfy constraint: "
                f"'must satisfy regular expression '{_REPO_NAME_PATTERN}''", 400)
        if not image_ids:
            return error_response_json(
                "InvalidParameterException",
                "Invalid parameter at 'imageIds' failed to satisfy constraint: "
                "'Member must have length greater than or equal to 1'", 400)

    # A pull-through cache rule fills what is not held (the repository too) from its upstream registry.
    fills = None
    if name not in _repositories or any(_find_image(name, iid) is None for iid in image_ids):
        fills = ecr_pull_through.fill(name, image_ids)
    if name not in _repositories:
        if fills is None:
            return error_response_json(
                "RepositoryNotFoundException",
                _repo_not_found_message(name, captured_everywhere=True), 400)  # r07-pull-no-rule
        return json_response({"images": [], "failures": [
            {"imageId": iid, "failureCode": fail[0], "failureReason": fail[1]}
            for iid, fail in zip(image_ids, fills)]})

    found = []
    failures = []
    for index, iid in enumerate(image_ids):
        match = _find_image(name, iid)
        if match:
            found.append({
                "registryId": _registry_id(),
                "repositoryName": name,
                # r07-pull-by-digest: a cache repository answers a digest request with the digest alone.
                "imageId": {"imageDigest": match["imageDigest"]}
                           if iid.get("imageDigest") and not iid.get("imageTag") and ecr_pull_through.is_cache_repository(name)
                           else match["imageId"],
                "imageManifest": match.get("imageManifest", "{}"),
                "imageManifestMediaType": match.get("imageManifestMediaType",
                    "application/vnd.docker.distribution.manifest.v2+json"),
            })
        else:
            code, reason = (fills[index] if fills else None) or ("ImageNotFound", "Requested image not found")
            failures.append({"imageId": iid, "failureCode": code, "failureReason": reason})

    return json_response({"images": found, "failures": failures})


def _batch_delete_image(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   _repo_not_found_message(name), 400)

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
                                   _repo_not_found_message(name), 400)

    tag_status = data.get("filter", {}).get("tagStatus")
    result = []
    for img in _images.get(name, []):
        if img.get("cacheOnly"):
            continue
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
        # r16-pull-e2-app--images: the registry-id wording on a path no rule covers (the rule create was refused).
        return error_response_json("RepositoryNotFoundException",
                                   _repo_not_found_message(name, captured_everywhere=True), 400)

    image_ids = data.get("imageIds")
    # Images an ECR-upstream fill copied are served to pulls but never listed.
    images = [img for img in _images.get(name, []) if not img.get("cacheOnly")]

    if image_ids:
        filtered = []
        for iid in image_ids:
            match = _find_image(name, iid)
            if match and match.get("cacheOnly"):
                return error_response_json(
                    "ImageNotFoundException",
                    f"The image with imageId {{imageDigest:'{iid.get('imageDigest') or 'null'}', "
                    f"imageTag:'{iid.get('imageTag') or 'null'}'}} does not exist within the repository "
                    f"with name '{name}' in the registry with id '{_registry_id()}'", 400)
            if match:
                filtered.append(match)
        images = filtered

    details = []
    for img in images:
        manifest = img.get("imageManifest", "{}")
        detail = {
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
        }
        # Rows filled from an upstream registry carry their own size, status and artifact type.
        if "detailOverrides" in img and not detail["imageTags"]:
            del detail["imageTags"]
        for key, value in img.get("detailOverrides", {}).items():
            if value is None:
                detail.pop(key, None)
            else:
                detail[key] = value
        details.append(detail)

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
                                   _repo_not_found_message(name), 400)
    if name not in _repo_policies:
        return error_response_json("RepositoryPolicyNotFoundException", _policy_not_found("Repository", name), 400)
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "policyText": _repo_policies[name],
    })


def _set_repository_policy(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   _repo_not_found_message(name), 400)
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
                                   _repo_not_found_message(name), 400)
    if name not in _repo_policies:
        return error_response_json("RepositoryPolicyNotFoundException", _policy_not_found("Repository", name), 400)
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
                                   _repo_not_found_message(name), 400)
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
                                   _repo_not_found_message(name), 400)
    if name not in _lifecycle_policies:
        return error_response_json("LifecyclePolicyNotFoundException", _policy_not_found("Lifecycle", name), 400)
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
                                   _repo_not_found_message(name), 400)
    if name not in _lifecycle_policies:
        return error_response_json("LifecyclePolicyNotFoundException", _policy_not_found("Lifecycle", name), 400)
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
                                   _repo_not_found_message(name), 400)
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
                                   _repo_not_found_message(name), 400)
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


def _unexpected_field_type():
    """The SerializationException of a member whose JSON type is not its shape's, as _image_ids_wire_error."""
    return ecr_pull_through._serialization_error("Unexpected field type")


def _get_download_url_for_layer(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   _repo_not_found_message(name), 400)
    layer_digest = data.get("layerDigest", "")
    if _cache_scoped(name) and layer_digest is not None and not isinstance(layer_digest, str):
        return _unexpected_field_type()
    ecr_pull_through.ensure_layer(name, layer_digest)
    if ecr_pull_through.layer_fetch_blocked(name, layer_digest):
        # v20 line 196: the on-demand layer fetch failed.
        return error_response_json(
            "LayersNotFoundException",
            "The specified layers could not be found, or the specified layer is not valid for this repository.",
            400)
    return json_response({
        "downloadUrl": f"https://{get_account_id()}.dkr.ecr.{get_region()}.amazonaws.com/v2/{name}/blobs/{layer_digest}",
        "layerDigest": layer_digest,
    })


def _batch_check_layer_availability(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   _repo_not_found_message(name), 400)
    digests = data.get("layerDigests", [])
    cache = _cache_scoped(name)
    if cache and digests is not None and (
            not isinstance(digests, list) or not all(isinstance(d, str) for d in digests)):
        return _unexpected_field_type()
    layers = [{"layerDigest": d, "layerAvailability": "UNAVAILABLE", **({} if cache else {"layerSize": 0})}
              for d in digests or []]  # r07-layer-availability: AWS omits layerSize for an UNAVAILABLE layer
    return json_response({"layers": layers, "failures": []})


def _initiate_layer_upload(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   _repo_not_found_message(name), 400)
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
    if not isinstance(image_id, dict):
        return None
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
    _repositories.clear()
    _images.clear()
    _lifecycle_policies.clear()
    _repo_policies.clear()
    _layer_blobs.clear()
    _manifest_blobs.clear()
    _uploads.clear()
    ecr_pull_through.reset()


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
    ecr_pull_through.ensure_layer(name, digest)  # HEAD runs the same on-demand fetch as GET
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
    ecr_pull_through.ensure_layer(name, digest)
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


def load_held_image(docker_client, image, architecture=None):
    """Load an image this MiniStack holds into ``docker_client`` for ``architecture`` (the daemon's when None).

    Returns the reference to run it by, or None when it is not held here or has no image for that platform."""
    if not architecture:
        reported = (docker_client.info() or {}).get("Architecture", "")
        architecture = _DOCKER_ARCHITECTURES.get(reported, reported)
    built = held_image_archive(image, architecture)
    if built is None:
        return None
    archive, config_digest = built
    loaded = docker_client.images.load(archive)
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


def _v2_fill_manifest(name, ref):
    """A V2 manifest GET or HEAD fills from the pull-through cache rule the same way BatchGetImage does.

    Returns the fill failure (code, reason) when the upstream refused the rule's credentials, else None."""
    if name in _repositories and _resolve_manifest(name, ref)[1] is not None:
        return None
    image_id = {"imageDigest": ref} if ref.startswith("sha256:") else {"imageTag": ref}
    results = ecr_pull_through.fill(name, [image_id])
    if results and results[0] is not None and results[0][0] == "UpstreamAccessDenied":
        return results[0]
    return None


def _v2_manifest_error(status, code, message, head):
    """A manifest-path error: ``{"errors":[{"code","message"}]}`` with no detail member (empty for HEAD)."""
    body = json.dumps({"errors": [{"code": code, "message": message}]}, separators=(",", ":")).encode() + b"\n"
    return (
        status,
        {
            "Content-Type": "application/json; charset=utf-8",
            "Content-Length": str(len(body)),
            _REGISTRY_API_VERSION_HEADER[0]: _REGISTRY_API_VERSION_HEADER[1],
        },
        b"" if head else body,
    )


_V2_AUTH_SCHEMES = frozenset({"basic", "bearer", "aws4-hmac-sha256"})


def _v2_request_signed(headers):
    """Whether a registry request carries credentials of a scheme MiniStack issues or verifies: Basic (the
    GetAuthorizationToken token), Bearer, or a SigV4 signature, each followed by a non-empty credential."""
    scheme, _, credential = (headers.get("authorization") or "").strip().partition(" ")
    return scheme.lower() in _V2_AUTH_SCHEMES and bool(credential.strip())


def _v2_unsigned_fill_refusal(name, ref, head):
    """Under AUTH=true an unsigned manifest request that would start a fill is answered 401 (r15-noauth)."""
    from ministack.app import AUTH

    if not AUTH or ecr_pull_through.match_rule(name) is None or not ecr_pull_through.fillable(name):
        return None
    if name in _repositories and _resolve_manifest(name, ref)[1] is not None:
        return None
    body = b"Not Authorized\n"
    realm = f"https://{get_account_id()}.dkr.ecr.{get_region()}.amazonaws.com/"
    return (
        401,
        {
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Length": str(len(body)),
            _REGISTRY_API_VERSION_HEADER[0]: _REGISTRY_API_VERSION_HEADER[1],
            "Www-Authenticate": f'Basic realm="{realm}",service="ecr.amazonaws.com"',
        },
        b"" if head else body,
    )


def _v2_bad_name_answer(head):
    """The router-level 404 AWS gives a manifest request whose repository name breaks the name pattern."""
    body = b"404 page not found\n"
    return (
        404,
        {
            "Content-Type": "text/plain; charset=utf-8",
            "Content-Length": str(len(body)),
            _REGISTRY_API_VERSION_HEADER[0]: _REGISTRY_API_VERSION_HEADER[1],
            "X-Content-Type-Options": "nosniff",
        },
        b"" if head else body,
    )


def _v2_manifest(name, ref, head, unsigned=False):
    if name not in _repositories and not _REPO_NAME_RE.fullmatch(name):
        # r15-get-bad-name-no-rule: a name that is no repository and breaks the pattern is a router 404. A
        # repository CreateRepository made keeps answering as on v1.5.39 (MiniStack does not validate names there).
        return _v2_bad_name_answer(head)
    if unsigned:
        refused = _v2_unsigned_fill_refusal(name, ref, head)
        if refused is not None:
            return refused
    denied = _v2_fill_manifest(name, ref)
    if denied is not None:
        return _v2_manifest_error(403, "DENIED", denied[1], head)
    if name not in _repositories and ecr_pull_through.fillable(name):
        # A rule covers the name but the fill brought nothing (missing tag or upstream repository).
        return _v2_manifest_error(404, "MANIFEST_UNKNOWN", "Requested image not found", head)
    if name not in _repositories:
        return _v2_manifest_error(
            404,
            "NAME_UNKNOWN",
            f"The repository with name '{name}' does not exist in the registry with id '{_registry_id()}'",
            head,
        )
    digest, blob, media_type = _resolve_manifest(name, ref)
    if blob is None:
        # r33-pre-trailing-v2-get: a repository CreateRepository made on a rule path, rule or not fillable.
        if _cache_scoped(name) or ecr_pull_through.fillable(name):
            return _v2_manifest_error(404, "MANIFEST_UNKNOWN", "Requested image not found", head)
        # A pushed repository no rule covers: no capture, so the answer of v1.5.39.
        return _registry_error(404, "MANIFEST_UNKNOWN", "manifest unknown to registry",
                               {"name": name, "reference": ref})
    headers = {"Content-Length": str(len(blob)), "Content-Type": media_type}
    if head or not ecr_pull_through.is_cache_repository(name):
        # AWS answers a manifest GET on a cache repository without Docker-Content-Digest (v20 line 192).
        headers["Docker-Content-Digest"] = digest
    return _registry_ok(200, headers, b"" if head else blob)


def _v2_get_manifest(name, ref, unsigned=False):
    return _v2_manifest(name, ref, head=False, unsigned=unsigned)


def _v2_head_manifest(name, ref, unsigned=False):
    return _v2_manifest(name, ref, head=True, unsigned=unsigned)


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
    if ecr_pull_through.is_cache_repository(name):
        # v20 line 193: a cache repository answers text/plain with a compact body.
        return _registry_ok(
            200,
            {"Content-Type": "text/plain; charset=utf-8"},
            json.dumps({"name": name, "tags": tags}, separators=(",", ":")).encode(),
        )
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
            return await ecr_pull_through.run_fetching(
                name, lambda: ecr_pull_through.layers_need_fetch(name, [ref]), _v2_head_blob, name, ref)
        if method == "GET":
            return await ecr_pull_through.run_fetching(
                name, lambda: ecr_pull_through.layers_need_fetch(name, [ref]), _v2_get_blob, name, ref)
        if method == "DELETE":
            return _v2_delete_blob(name, ref)
        return _registry_error(405, "UNSUPPORTED", "method not allowed", {})

    if verb == "manifest":
        if method == "PUT":
            return _v2_put_manifest(name, ref, headers, body)
        if method == "GET":
            return await ecr_pull_through.run_fetching(
                name, lambda: ecr_pull_through.manifest_needs_fetch(name, ref), _v2_get_manifest, name, ref, not _v2_request_signed(headers))
        if method == "HEAD":
            return await ecr_pull_through.run_fetching(
                name, lambda: ecr_pull_through.manifest_needs_fetch(name, ref), _v2_head_manifest, name, ref, not _v2_request_signed(headers))
        if method == "DELETE":
            return _v2_delete_manifest(name, ref)
        return _registry_error(405, "UNSUPPORTED", "method not allowed", {})

    if verb == "tags_list":
        if method != "GET":
            return _registry_error(405, "UNSUPPORTED", "method not allowed", {})
        return _v2_list_tags(name, query_params)

    return _registry_error(404, "UNSUPPORTED", f"unhandled V2 verb: {verb}", {})
