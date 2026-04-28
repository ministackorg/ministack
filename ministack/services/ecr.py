"""
ECR (Elastic Container Registry) Emulator.
JSON-based API via X-Amz-Target (prefix: AmazonEC2ContainerRegistry_V20150921).
"""

import base64
import copy
import hashlib
import json
import logging
import os
import time

from ministack.core.persistence import PERSIST_STATE, load_state
from ministack.core.responses import (
    AccountScopedDict,
    error_response_json,
    get_account_id,
    get_region,
    json_response,
    new_uuid,
)

logger = logging.getLogger("ecr")

REGION = os.environ.get("MINISTACK_REGION", "us-east-1")

_repositories = AccountScopedDict()
_images = AccountScopedDict()
_lifecycle_policies = AccountScopedDict()
_repo_policies = AccountScopedDict()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "repositories": copy.deepcopy(_repositories),
        "images": copy.deepcopy(_images),
        "lifecycle_policies": copy.deepcopy(_lifecycle_policies),
        "repo_policies": copy.deepcopy(_repo_policies),
    }


def restore_state(data):
    if data:
        _repositories.update(data.get("repositories", {}))
        _images.update(data.get("images", {}))
        _lifecycle_policies.update(data.get("lifecycle_policies", {}))
        _repo_policies.update(data.get("repo_policies", {}))


try:
    _restored = load_state("ecr")
    if _restored:
        restore_state(_restored)
except Exception:
    import logging
    logging.getLogger(__name__).exception(
        "Failed to restore persisted state; continuing with fresh store"
    )


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

    return handler(data)


def _create_repository(data):
    name = data.get("repositoryName", "")
    if not name:
        return error_response_json("InvalidParameterException", "repositoryName is required", 400)
    if name in _repositories:
        return error_response_json("RepositoryAlreadyExistsException",
                                   f"The repository with name '{name}' already exists", 400)

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
    _repositories[name] = repo
    _images[name] = []
    return json_response({"repository": _repo_shape(repo)})


def _describe_repositories(data):
    names = data.get("repositoryNames", [])
    max_results = data.get("maxResults", 1000)

    if names:
        repos = []
        for n in names:
            if n not in _repositories:
                return error_response_json("RepositoryNotFoundException",
                                           f"The repository with name '{n}' does not exist", 400)
            repos.append(_repositories[n])
    else:
        repos = list(_repositories.values())

    return json_response({"repositories": [_repo_shape(r) for r in repos[:max_results]]})


def _delete_repository(data):
    name = data.get("repositoryName", "")
    force = data.get("force", False)

    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist", 400)

    if not force and _images.get(name):
        return error_response_json("RepositoryNotEmptyException",
                                   f"The repository with name '{name}' is not empty", 400)

    repo = _repositories.pop(name)
    _images.pop(name, None)
    _lifecycle_policies.pop(name, None)
    _repo_policies.pop(name, None)
    return json_response({"repository": _repo_shape(repo)})


def _put_image(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist", 400)

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


def _batch_get_image(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist", 400)

    image_ids = data.get("imageIds", [])
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
                                   f"The repository with name '{name}' does not exist", 400)

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
                                   f"The repository with name '{name}' does not exist", 400)

    tag_status = data.get("filter", {}).get("tagStatus")
    result = []
    for img in _images.get(name, []):
        tags = img.get("imageTags", [])
        if tag_status == "TAGGED" and not tags:
            continue
        if tag_status == "UNTAGGED" and tags:
            continue
        result.append(img["imageId"])

    return json_response({"imageIds": result})


def _describe_images(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist", 400)

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

    return json_response({"imageDetails": details})


def _get_authorization_token(data):
    token = base64.b64encode(b"AWS:ministack-auth-token").decode()
    return json_response({
        "authorizationData": [{
            "authorizationToken": token,
            "expiresAt": int(time.time()) + 43200,
            "proxyEndpoint": f"https://{get_account_id()}.dkr.ecr.{get_region()}.amazonaws.com",
        }]
    })


def _get_repository_policy(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist", 400)
    if name not in _repo_policies:
        return error_response_json("RepositoryPolicyNotFoundException",
                                   f"Repository policy does not exist for '{name}'", 400)
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "policyText": _repo_policies[name],
    })


def _set_repository_policy(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist", 400)
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
                                   f"The repository with name '{name}' does not exist", 400)
    if name not in _repo_policies:
        return error_response_json("RepositoryPolicyNotFoundException",
                                   f"Repository policy does not exist for '{name}'", 400)
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
                                   f"The repository with name '{name}' does not exist", 400)
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
                                   f"The repository with name '{name}' does not exist", 400)
    if name not in _lifecycle_policies:
        return error_response_json("LifecyclePolicyNotFoundException",
                                   f"Lifecycle policy does not exist for '{name}'", 400)
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
                                   f"The repository with name '{name}' does not exist", 400)
    if name not in _lifecycle_policies:
        return error_response_json("LifecyclePolicyNotFoundException",
                                   f"Lifecycle policy does not exist for '{name}'", 400)
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
                                   f"The repository with name '{name}' does not exist", 400)
    mutability = data.get("imageTagMutability", "MUTABLE")
    _repositories[name]["imageTagMutability"] = mutability
    return json_response({
        "registryId": _registry_id(),
        "repositoryName": name,
        "imageTagMutability": mutability,
    })


def _put_image_scanning_configuration(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist", 400)
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
                                   f"The repository with name '{name}' does not exist", 400)
    layer_digest = data.get("layerDigest", "")
    return json_response({
        "downloadUrl": f"https://{get_account_id()}.dkr.ecr.{get_region()}.amazonaws.com/v2/{name}/blobs/{layer_digest}",
        "layerDigest": layer_digest,
    })


def _batch_check_layer_availability(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist", 400)
    digests = data.get("layerDigests", [])
    layers = [{"layerDigest": d, "layerAvailability": "UNAVAILABLE", "layerSize": 0} for d in digests]
    return json_response({"layers": layers, "failures": []})


def _initiate_layer_upload(data):
    name = data.get("repositoryName", "")
    if name not in _repositories:
        return error_response_json("RepositoryNotFoundException",
                                   f"The repository with name '{name}' does not exist", 400)
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
    for repo in _repositories.values():
        if repo["repositoryArn"] == arn:
            return repo
    return None


def _repo_shape(repo):
    return {
        "repositoryArn": repo["repositoryArn"],
        "registryId": repo["registryId"],
        "repositoryName": repo["repositoryName"],
        "repositoryUri": repo["repositoryUri"],
        "createdAt": repo["createdAt"],
        "imageTagMutability": repo.get("imageTagMutability", "MUTABLE"),
        "imageScanningConfiguration": repo.get("imageScanningConfiguration", {"scanOnPush": False}),
        "encryptionConfiguration": repo.get("encryptionConfiguration", {"encryptionType": "AES256"}),
    }


def reset():
    _repositories.clear()
    _images.clear()
    _lifecycle_policies.clear()
    _repo_policies.clear()
