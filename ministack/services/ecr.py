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

from ministack.core.arn import ArnParseError, parse_arn
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
# Docker Registry HTTP API V2 backing storage.
# _layer_blobs[repo] = {digest: bytes}     — finalised layer + config bytes addressable by digest.
# _manifest_blobs[repo] = {digest: bytes}  — raw manifest bytes addressable by digest (for HEAD/GET by digest).
# _uploads[repo] = {upload_uuid: bytearray} — in-flight chunked uploads (cleared on PUT).
_layer_blobs = AccountScopedDict()
_manifest_blobs = AccountScopedDict()
_uploads = AccountScopedDict()


# ── Persistence ────────────────────────────────────────────

def get_state():
    return {
        "repositories": copy.deepcopy(_repositories),
        "images": copy.deepcopy(_images),
        "lifecycle_policies": copy.deepcopy(_lifecycle_policies),
        "repo_policies": copy.deepcopy(_repo_policies),
        "layer_blobs": copy.deepcopy(_layer_blobs),
        "manifest_blobs": copy.deepcopy(_manifest_blobs),
        # _uploads is intentionally NOT persisted — in-flight upload sessions
        # do not survive a restart, matching real registry behaviour where
        # the client retries the upload from scratch after a 404 on PATCH.
    }


def restore_state(data):
    if data:
        _repositories.update(data.get("repositories", {}))
        _images.update(data.get("images", {}))
        _lifecycle_policies.update(data.get("lifecycle_policies", {}))
        _repo_policies.update(data.get("repo_policies", {}))
        _layer_blobs.update(data.get("layer_blobs", {}))
        _manifest_blobs.update(data.get("manifest_blobs", {}))


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
