"""ECR pull-through cache: Validate upstream checks and the first-pull fill (issue 2617).

Runs the service in-process with a fake upstream registry behind ``_http_get``: the real upstreams are
the network. Bodies and messages come from the AWS captures named in each test.
"""

import asyncio
import hashlib
import http.client
import json
import logging
import pathlib

import pytest

from ministack.core.responses import get_account_id
from ministack.services import ecr, iam, secretsmanager

# ecr_pull_through is imported by the pull_through fixture, not here, so the file collects where it does not exist.

pytestmark = pytest.mark.serial  # shares the registry-wide rule quota, registry policy and service-linked role

PREFIX = "msc2617-r07-pub"
PULL = f"{PREFIX}/docker/library/alpine"
DENIED = {"failureCode": "UPSTREAM_REGISTRY_ACCESS_DENIED",
          "failureReason": "Failed to get upstream bearer token. Verify the credentials provided and try again."}
LAYERS_NOT_FOUND = "The specified layers could not be found, or the specified layer is not valid for this repository."
NO_ROLE = {"failureCode": "UNABLE_TO_ASSUME_ROLE",
           "failureReason": "Unable to access the IAM role. Please verify trust policy and try again."}


def _digest(raw):
    return "sha256:" + hashlib.sha256(raw).hexdigest()


LAYER = b"layer-bytes"
CONFIG = b'{"architecture":"amd64"}'
CHILD = json.dumps({
    "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
    "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": _digest(CONFIG), "size": len(CONFIG)},
    "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": _digest(LAYER), "size": len(LAYER)}],
}, indent=2).encode()
INDEX = json.dumps({
    "schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json",
    "manifests": [{"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": _digest(CHILD),
                   "size": len(CHILD), "platform": {"architecture": "amd64", "os": "linux"}}],
}).encode()
BLOBS = {_digest(INDEX): (INDEX, "application/vnd.oci.image.index.v1+json"),
         _digest(CHILD): (CHILD, "application/vnd.oci.image.manifest.v1+json"),
         _digest(LAYER): (LAYER, "application/octet-stream"), _digest(CONFIG): (CONFIG, "application/json")}
# The captured r15 alpine:3.20 index (9226 bytes) and its amd64-less child (1023 bytes), served for the upstream
# repository path docker/library/alpine-cap
_FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "ecr_pull_through"
CAP_INDEX = (_FIXTURES / "alpine-3.20-index.json").read_bytes()
CAP_CHILD = (_FIXTURES / "alpine-3.20-child.json").read_bytes()
CAP_INDEX_DIGEST = "sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc"
CAP_CHILD_DIGEST = "sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e"
CAP_BLOBS = {CAP_INDEX_DIGEST: (CAP_INDEX, "application/vnd.oci.image.index.v1+json"),
             CAP_CHILD_DIGEST: (CAP_CHILD, "application/vnd.oci.image.manifest.v1+json")}
CAP_PULL = f"{PREFIX}/docker/library/alpine-cap"
TAGS = {"3.20": _digest(INDEX)}
CAP_TAGS = {"3.20": CAP_INDEX_DIGEST}


class Upstream:
    """A registry that issues bearer tokens, refuses credentials listed in ``deny`` and serves ``BLOBS``."""

    def __init__(self):
        self.deny = False
        self.down = False
        self.urls = []
        self.token_answer = None  # bytes served as the token answer instead of {"token": "t"}
        self.manifest_answer = None  # bytes served for every manifest instead of the stored ones
        self.truncate = False  # raise http.client.IncompleteRead on every GET after the challenge

    def __call__(self, url, headers):
        self.urls.append(url)
        if self.down:
            raise OSError("no route")
        if url.endswith("/v2/"):
            return 401, {"www-authenticate": 'Bearer realm="https://auth.test/token",service="reg"'}, b""
        if url.startswith("https://auth.test/token"):
            if self.truncate:
                raise http.client.IncompleteRead(b"")
            if self.token_answer is not None:
                return 200, {}, self.token_answer
            return (401, {}, b"") if self.deny else (200, {}, json.dumps({"token": "t"}).encode())
        assert headers.get("Authorization") == "Bearer t"
        if self.truncate:
            raise http.client.IncompleteRead(b"")
        path, _, ref = url.partition("/v2/")[2].rpartition("/")
        if path.startswith("docker/library/alpine-cap/"):
            ref = CAP_TAGS.get(ref, ref)
            if self.manifest_answer is not None and "/manifests" in path + "/":
                return 200, {"content-type": "application/vnd.oci.image.index.v1+json"}, self.manifest_answer
            if ref in CAP_BLOBS:
                return 200, {"content-type": CAP_BLOBS[ref][1]}, CAP_BLOBS[ref][0]
            return 404, {}, b""
        if not path.startswith("library/alpine/") and not path.startswith("docker/library/alpine/"):
            return 404, {}, b""
        ref = TAGS.get(ref, ref)
        if ref in BLOBS:
            return 200, {"content-type": BLOBS[ref][1]}, BLOBS[ref][0]
        return 404, {}, b""


class Daemon:
    """The host Docker daemon pulling from the fake ``Upstream``: it holds the first (single-platform) child of
    an index with its config and layers, like a daemon of one platform. ``absent`` models no daemon."""

    def __init__(self, registry):
        self.registry = registry
        self.absent = False
        self.pulls = []

    def __call__(self, host, path, reference, credentials):
        self.pulls.append((host, path, reference))
        if self.absent:
            return "unavailable", "", b"", {}
        from ministack.services import ecr_pull_through

        outcome, token = ecr_pull_through._bearer_token(host, credentials, f"repository:{path}:pull")
        if outcome != "ok":
            return ("denied" if credentials else "notfound") if outcome == "denied" else outcome, "", b"", {}

        def get(ref):
            try:
                return self.registry(f"https://{host}/v2/{path}/manifests/{ref}", {"Authorization": "Bearer " + token})
            except (OSError, http.client.HTTPException):
                return None

        answer = get(reference)
        if answer is None:
            return "unavailable", "", b"", {}
        status, headers, raw = answer
        if status != 200:
            return ("notfound" if status == 404 else "unavailable"), "", b"", {}
        media_type = headers.get("content-type", "")
        blobs = {}
        manifest = json.loads(raw) if _pull_through().manifest_is_wellformed(raw) else {}
        manifest = manifest if isinstance(manifest, dict) else {}
        held = manifest
        if "manifests" in manifest and manifest["manifests"]:
            child_digest = manifest["manifests"][0]["digest"]
            child = get(child_digest)
            if child is not None and child[0] == 200:
                blobs[child_digest] = child[2]
                held = json.loads(child[2])
        for descriptor in [d for d in [held.get("config") or {}, *(held.get("layers") or [])] if "digest" in d]:
            blob = self.registry(f"https://{host}/v2/{path}/blobs/{descriptor['digest']}", {"Authorization": "Bearer " + token})
            if blob[0] == 200:
                blobs[descriptor["digest"]] = blob[2]
        return "ok", media_type, raw, blobs


def _pull_through():
    from ministack.services import ecr_pull_through

    return ecr_pull_through


@pytest.fixture
def pull_through():
    """Bind the module as this file's ``ecr_pull_through`` (imported here, not at the top)."""
    global ecr_pull_through
    from ministack.services import ecr_pull_through

    return ecr_pull_through


@pytest.fixture
def upstream(monkeypatch, pull_through):
    fake = Upstream()
    monkeypatch.setattr(pull_through, "_http_get", fake)
    fake.daemon = Daemon(fake)
    monkeypatch.setattr(pull_through, "_daemon_pull", fake.daemon)
    ecr.reset()
    yield fake
    ecr.reset()


def _action(action, body):
    status, headers, raw = asyncio.run(ecr.handle_request(
        "POST", "/", {"x-amz-target": f"AmazonEC2ContainerRegistry_V20150921.{action}"}, json.dumps(body).encode(), {}))
    return status, json.loads(raw)


def _secret(value):
    name = "ecr-pullthroughcache/msc-2617-r03-secrets-ok"
    secretsmanager._secrets.pop(name, None)
    return secretsmanager.create_secret_in_process(name, value)


def _rule(prefix, url, **extra):
    status, body = _action("CreatePullThroughCacheRule",
                           {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": url, **extra})
    assert status == 200, body
    return body


@pytest.mark.parametrize("key,secret", [
    ("r03-validate-cred-ok", '{"username":"u","accessToken":"x"}'),
    ("r03-validate-cred-emptyvals", '{"username":"","accessToken":""}'),
    ("r03-validate-cred-extra", '{"username":"u","accessToken":"x","extra":"y"}'),
])
def test_validate_refused_credentials(upstream, key, secret):
    arn = _secret(secret)
    _rule("msc2617-r03-s-ok", "registry-1.docker.io", credentialArn=arn)
    upstream.deny = True
    status, body = _action("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r03-s-ok"})
    assert status == 200
    assert body == {"credentialArn": arn, "ecrRepositoryPrefix": "msc2617-r03-s-ok", "failure": json.dumps(DENIED, separators=(",", ":")),
                    "isValid": False, "registryId": get_account_id(), "upstreamRegistryUrl": "registry-1.docker.io"}


def test_validate_accepted_credentials_is_valid(upstream):
    arn = _secret('{"username":"u","accessToken":"x"}')
    _rule("msc2617-r03-s-ok", "registry-1.docker.io", credentialArn=arn)
    status, body = _action("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r03-s-ok"})
    assert (status, body["isValid"], body["failure"]) == (200, True, "")


@pytest.mark.parametrize("trust", [None, "other.amazonaws.com"])
def test_validate_ecr_rule_role_not_assumable(upstream, trust):
    # r12-upd-role-missing-validate (no such role) and r12-upd-role-bad-trust-validate (wrong principal)
    name = "msc-2617-r12-ecr-upstream-role-ptc-badtrust"
    arn = f"arn:aws:iam::{get_account_id()}:role/{name}"
    iam._roles.pop(name, None)
    if trust:
        iam._roles[name] = {"RoleName": name, "Arn": arn, "AssumeRolePolicyDocument": json.dumps(
            {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": trust}, "Action": "sts:AssumeRole"}]})}
    _rule("msc2617-r12-e1", "444455556666.dkr.ecr.us-east-2.amazonaws.com", customRoleArn=arn)
    status, body = _action("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r12-e1"})
    assert status == 200
    assert body == {"customRoleArn": arn, "ecrRepositoryPrefix": "msc2617-r12-e1", "failure": json.dumps(NO_ROLE, separators=(",", ":")),
                    "isValid": False, "registryId": get_account_id(),
                    "upstreamRegistryUrl": "444455556666.dkr.ecr.us-east-2.amazonaws.com"}
    iam._roles.pop(name, None)


def test_pull_fills_repository_and_serves_upstream_bytes(upstream):
    # r07-before-pull-repo, r07-pull-first, r07-pull-second, r07-pull-first--repo, r07-cache-tags
    _rule(PREFIX, "public.ecr.aws")
    assert _action("DescribeRepositories", {"repositoryNames": [PULL]})[0] == 400
    for _ in range(2):
        status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})
        assert status == 200
        assert body["failures"] == []
        assert body["images"] == [{"registryId": get_account_id(), "repositoryName": PULL,
                                   "imageId": {"imageDigest": _digest(INDEX), "imageTag": "3.20"},
                                   "imageManifest": INDEX.decode(),
                                   "imageManifestMediaType": "application/vnd.oci.image.index.v1+json"}]
    repo = _action("DescribeRepositories", {"repositoryNames": [PULL]})[1]["repositories"][0]
    assert (repo["imageTagMutability"], repo["imageScanningConfiguration"], repo["encryptionConfiguration"], repo.get("tags", [])) == (
        "MUTABLE", {"scanOnPush": False}, {"encryptionType": "AES256"}, [])
    assert _action("ListTagsForResource", {"resourceArn": repo["repositoryArn"]})[1] == {"tags": []}
    assert _action("GetRepositoryPolicy", {"repositoryName": PULL})[1]["__type"] == "RepositoryPolicyNotFoundException"
    assert any(u.endswith("/v2/docker/library/alpine/manifests/3.20") for u in upstream.urls)


def test_pull_maps_path_without_prefix_for_docker_hub(upstream):
    # r07-pull-dh-fake-cred maps <prefix>/library/alpine to library/alpine upstream
    arn = _secret('{"username":"u","accessToken":"x"}')
    _rule("msc2617-r07-dh", "registry-1.docker.io", credentialArn=arn)
    status, body = _action("BatchGetImage", {"repositoryName": "msc2617-r07-dh/library/alpine", "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["failures"] == []
    assert any("https://registry-1.docker.io/v2/library/alpine/manifests/3.20" == u for u in upstream.urls)


def test_pull_by_digest_child_and_layer_after_fill(upstream):
    # r07-pull-by-digest, r07-pull-index-child, r07-layer-download-url
    _rule(PREFIX, "public.ecr.aws")
    _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})
    for digest, manifest in ((_digest(INDEX), INDEX), (_digest(CHILD), CHILD)):
        status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageDigest": digest}]})
        assert status == 200 and body["failures"] == []
        assert body["images"][0]["imageId"] == {"imageDigest": digest}
        assert body["images"][0]["imageManifest"] == manifest.decode()
    status, body = _action("GetDownloadUrlForLayer", {"repositoryName": PULL, "layerDigest": _digest(LAYER)})
    assert status == 200 and body["layerDigest"] == _digest(LAYER) and body["downloadUrl"]
    assert ecr._layer_blobs[PULL][_digest(LAYER)] == LAYER


def test_layer_fetch_with_unreadable_secret_leaves_layer_missing(upstream, monkeypatch):
    # ledger line 168 (uncaptured user decision layer-fetch-secret-unreadable); fill path: r07-pull-index-child
    _rule(PREFIX, "public.ecr.aws")
    _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})
    ecr._layer_blobs.setdefault(PULL, {}).pop(_digest(LAYER), None)
    # the fetch-time rule's credentialArn decides, not the one stored at fill time (origins-after-repository-delete)
    ecr_pull_through.match_rule(PULL)[1]["credentialArn"] = "arn:aws:secretsmanager:us-east-1:000000000000:secret:x"
    monkeypatch.setattr(ecr_pull_through, "_pull_secret_unreadable", lambda credential: True)
    status, body = _action("GetDownloadUrlForLayer", {"repositoryName": PULL, "layerDigest": _digest(LAYER)})
    # choices.txt: LayersNotFoundException on GetDownloadUrlForLayer, text as in the captured layer refusals
    assert status == 400 and body == {"__type": "LayersNotFoundException", "message": LAYERS_NOT_FOUND}
    assert _digest(LAYER) not in ecr._layer_blobs[PULL]
    status, headers, _ = ecr._v2_get_blob(PULL, _digest(LAYER))
    assert status == 404


def test_describe_images_index_and_child_rows(upstream):
    # r07-pull-after-batch-delete--images (row shape; AWS lists them once its import finishes)
    _rule(PREFIX, "public.ecr.aws")
    _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})
    rows = {r["imageDigest"]: r for r in _action("DescribeImages", {"repositoryName": PULL})[1]["imageDetails"]}
    size = len(CONFIG) + len(LAYER)
    child, index = rows[_digest(CHILD)], rows[_digest(INDEX)]
    assert child == {"artifactMediaType": "application/vnd.oci.image.config.v1+json", "imageDigest": _digest(CHILD),
                     "imageManifestMediaType": "application/vnd.oci.image.manifest.v1+json", "imagePushedAt": child["imagePushedAt"],
                     "imageSizeInBytes": size, "imageStatus": "ACTIVE", "registryId": get_account_id(), "repositoryName": PULL}
    assert index["imageTags"] == ["3.20"] and index["imageSizeInBytes"] == size
    assert index["imageManifestMediaType"] == "application/vnd.oci.image.index.v1+json"
    assert "artifactMediaType" not in index


FAILURE_MISSING = {"failureCode": "ImageNotFound", "failureReason": "Requested image not found"}


def test_missing_tag_is_a_failure_and_mixed_request_returns_both(upstream):
    # r07-pull-missing-tag, r07-pull-mixed-tags
    _rule(PREFIX, "public.ecr.aws")
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "nope"}]})
    assert (status, body) == (200, {"images": [], "failures": [{"imageId": {"imageTag": "nope"}, **FAILURE_MISSING}]})
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}, {"imageTag": "nope"}]})
    assert status == 200 and len(body["images"]) == 1
    assert body["failures"] == [{"imageId": {"imageTag": "nope"}, **FAILURE_MISSING}]


def test_missing_upstream_repository_leaves_no_repository(upstream):
    # r07-pull-missing-upstream-repo, --repo, --images
    _rule(PREFIX, "public.ecr.aws")
    name = f"{PREFIX}/docker/library/msc2617-no-such-image"
    status, body = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "latest"}]})
    assert (status, body) == (200, {"images": [], "failures": [{"imageId": {"imageTag": "latest"}, **FAILURE_MISSING}]})
    for action, params in (("DescribeRepositories", {"repositoryNames": [name]}), ("DescribeImages", {"repositoryName": name})):
        status, body = _action(action, params)
        assert status == 400 and body == {
            "__type": "RepositoryNotFoundException",
            "message": f"The repository with name '{name}' does not exist in the registry with id '{get_account_id()}'"}


def test_upstream_rejects_credentials(upstream):
    # r07-pull-dh-fake-cred, --repo, --images
    arn = _secret('{"username":"u","accessToken":"x"}')
    _rule("msc2617-r07-dh", "registry-1.docker.io", credentialArn=arn)
    upstream.deny = True
    name = "msc2617-r07-dh/library/alpine"
    status, body = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["images"] == []
    assert body["failures"] == [{"imageId": {"imageTag": "3.20"}, "failureCode": "UpstreamAccessDenied", "failureReason":
                                 "The authentication to the upstream registry failed. Verify that the credentials provided in the pull through cache rule are correct, and try again."}]
    assert _action("DescribeRepositories", {"repositoryNames": [name]})[0] == 400


def test_ecr_upstream_without_grant_is_image_not_found(upstream):
    # r12-pull-e1, r12-pull-e1--repo
    _rule("msc2617-r12-e1", "444455556666.dkr.ecr.us-east-2.amazonaws.com",
          customRoleArn=f"arn:aws:iam::{get_account_id()}:role/r")
    name = "msc2617-r12-e1/msc-2617-r12-ecr-upstream-role-src/app"
    status, body = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "v1"}]})
    assert (status, body) == (200, {"images": [], "failures": [{"imageId": {"imageTag": "v1"}, **FAILURE_MISSING}]})
    assert _action("DescribeRepositories", {"repositoryNames": [name]})[0] == 400


@pytest.mark.parametrize("name", ["msc2617-r07-norule/docker/library/alpine", PREFIX])
def test_no_rule_or_bare_prefix_is_repository_not_found(upstream, name):
    # r07-pull-no-rule, r07-pull-prefix-only
    _rule(PREFIX, "public.ecr.aws")
    status, body = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 400 and body["__type"] == "RepositoryNotFoundException"
    assert body["message"] == f"The repository with name '{name}' does not exist in the registry with id '{get_account_id()}'"


def test_pull_keeps_shape_checks(upstream):
    # r07-pull-prefix-bad-name, r07-pull-trailing-slash, r07-pull-prefix-no-image-ids (all on a rule path)
    _rule(PREFIX, "public.ecr.aws")
    pattern = "[a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*(/[a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*)*"
    for name in (f"{PREFIX}/Docker/Library/Alpine", f"{PREFIX}/"):
        status, body = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "3.20"}]})
        assert status == 400 and body["__type"] == "InvalidParameterException"
        assert body["message"] == f"Invalid parameter at 'repositoryName' failed to satisfy constraint: 'must satisfy regular expression '{pattern}''"
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": []})
    assert status == 400 and body["message"] == (
        "Invalid parameter at 'imageIds' failed to satisfy constraint: 'Member must have length greater than or equal to 1'")


def _pull(name=PULL, tag="3.20", **extra):
    return _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": tag}], **extra})


def test_cache_repository_behaves_as_a_normal_repository(upstream):
    # r07-cache-put-image-new-tag, r07-cache-put-image-upstream-tag, r07-cache-put-image-mutability
    _rule(PREFIX, "public.ecr.aws")
    _pull()
    registry = get_account_id()
    status, body = _action("PutImage", {"repositoryName": PULL, "imageManifest": INDEX.decode(),
                                        "imageTag": "msc2617-manual"})
    assert status == 200 and body["image"]["imageId"] == {"imageDigest": _digest(INDEX), "imageTag": "msc2617-manual"}
    status, body = _action("PutImage", {"repositoryName": PULL, "imageManifest": INDEX.decode(), "imageTag": "3.20"})
    assert (status, body["__type"], body["message"]) == (
        400, "ImageAlreadyExistsException",
        f"Image with digest '{_digest(INDEX)}' and tag '3.20' already exists in the repository with name "
        f"'{PULL}' in registry with id '{registry}'")
    status, body = _action("PutImageTagMutability", {"repositoryName": PULL, "imageTagMutability": "IMMUTABLE"})
    assert (status, body) == (200, {"imageTagMutability": "IMMUTABLE", "registryId": registry, "repositoryName": PULL})
    status, body = _action("PutImage", {"repositoryName": PULL, "imageManifest": INDEX.decode(), "imageTag": "3.20"})
    # r07-cache-put-image-upstream-tag-immutable
    assert (status, body["__type"], body["message"]) == (
        400, "ImageAlreadyExistsException",
        f"Image with digest '{_digest(INDEX)}' and tag '3.20' already exists in the repository with name "
        f"'{PULL}' in registry with id '{registry}'")


def test_rule_recreated_with_another_upstream_pulls_from_it(upstream):
    # r07-rule-pub-again, r07-pull-after-rule-recreated-other-upstream
    _rule(PREFIX, "public.ecr.aws")
    _pull()
    assert _action("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": PREFIX})[0] == 200
    _rule(PREFIX, "quay.io")
    upstream.urls.clear()
    status, body = _pull(tag="3.19")
    assert status == 200 and body["images"] == []
    assert body["failures"] == [{"imageId": {"imageTag": "3.19"}, **FAILURE_MISSING}]
    assert upstream.urls and all("quay.io" in u or "auth.test" in u for u in upstream.urls)


def test_cross_account_pull_without_grant_is_access_denied(upstream):
    # r11-a2-pull-no-grant, r11-a1-repo-after-no-grant, r11-a2-pull-registry-policy-only, r11-a2-describe-images-a1
    from ministack.core.responses import get_region, request_scope

    _rule(PREFIX, "public.ecr.aws")
    owner, region = get_account_id(), get_region()
    _action("PutRegistryPolicy", {"policyText": json.dumps({"Statement": {
        "Effect": "Allow", "Principal": {"AWS": "222222222222"}, "Action": "ecr:BatchGetImage", "Resource": "*"}})})
    resource = f"arn:aws:ecr:{region}:{owner}:repository/{PULL}"
    with request_scope("222222222222", region):
        for action, body in (("BatchGetImage", {"imageIds": [{"imageTag": "3.20"}]}), ("DescribeImages", {})):
            status, out = _action(action, {"registryId": owner, "repositoryName": PULL, **body})
            assert status == 400 and out["__type"] == "AccessDeniedException"
            assert out["Message"] == (
                f"User: arn:aws:iam::222222222222:root is not authorized to perform: ecr:{action} on resource: "
                f"{resource} because no resource-based policy allows the ecr:{action} action")
    status, body = _action("DescribeRepositories", {"repositoryNames": [PULL]})
    assert status == 400 and body["message"] == f"The repository with name '{PULL}' does not exist in the registry with id '{owner}'"


def test_pull_path_messages_name_the_registry_and_timestamps_carry_milliseconds(upstream):
    # r07-cache-repo-policy, r07-cache-lifecycle, r07-post-create-repo-again, r07-cache-delete-repo-no-force,
    # r07-post-pull-into-created--repo
    _rule(PREFIX, "public.ecr.aws")
    _pull()
    registry = get_account_id()
    tail = f"in the registry with id '{registry}'"
    assert _action("GetRepositoryPolicy", {"repositoryName": PULL})[1]["message"] == (
        f"Repository policy does not exist for the repository with name '{PULL}' {tail}")
    assert _action("GetLifecyclePolicy", {"repositoryName": PULL})[1]["message"] == (
        f"Lifecycle policy does not exist for the repository with name '{PULL}' {tail}")
    assert _action("CreateRepository", {"repositoryName": PULL})[1]["message"] == (
        f"The repository with name '{PULL}' already exists {tail}")
    assert _action("DeleteRepository", {"repositoryName": PULL})[1]["message"] == (
        f"The repository with name '{PULL}' in registry with id '{registry}' cannot be deleted because it still "
        "contains images")
    created = _action("DescribeRepositories", {"repositoryNames": [PULL]})[1]["repositories"][0]["createdAt"]
    assert isinstance(created, float) and round(created, 3) == created


def test_pull_missing_tag_on_ecr_upstream_and_k8s_fill(upstream):
    # r12-pull-e1-missing-tag, r07-pull-k8s
    _rule("msc2617-r12-e1", "444455556666.dkr.ecr.us-east-2.amazonaws.com",
          customRoleArn=f"arn:aws:iam::{get_account_id()}:role/r")
    name = "msc2617-r12-e1/msc-2617-r12-ecr-upstream-role-src/app"
    out = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "nope"}]})
    assert out == (200, {"images": [], "failures": [{"imageId": {"imageTag": "nope"}, **FAILURE_MISSING}]})
    _rule("msc2617-r07-k8s", "registry.k8s.io")
    status, body = _action("BatchGetImage", {"repositoryName": "msc2617-r07-k8s/docker/library/alpine",
                                             "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["failures"] == []
    assert any(u.startswith("https://registry.k8s.io/v2/") for u in upstream.urls)


def test_pull_with_upstream_unreachable_creates_no_repository(upstream):
    # ANSWERS daemon-unavailable (no capture key)
    _rule(PREFIX, "public.ecr.aws")
    upstream.down = True
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["images"] == []
    assert body["failures"] == [{"imageId": {"imageTag": "3.20"}, **UNAVAILABLE}]
    assert _action("DescribeRepositories", {"repositoryNames": [PULL]})[0] == 400


def test_describe_rules_in_creation_order_with_pages(upstream):
    # r05-desc-all, r05-desc-page-1, r05-desc-page-2 (order is creation order, ANSWERS describe-order)
    names = ["msc2617-r05-a", "msc2617-r05-c", "msc2617-r05-a/x", "msc2617-r05-b"]
    for n in names:
        _rule(n, "public.ecr.aws")
    got = _action("DescribePullThroughCacheRules", {})[1]["pullThroughCacheRules"]
    assert [r["ecrRepositoryPrefix"] for r in got] == names
    status, page = _action("DescribePullThroughCacheRules", {"maxResults": 1})
    assert [r["ecrRepositoryPrefix"] for r in page["pullThroughCacheRules"]] == names[:1] and page["nextToken"]
    page = _action("DescribePullThroughCacheRules", {"maxResults": 1, "nextToken": page["nextToken"]})[1]
    assert [r["ecrRepositoryPrefix"] for r in page["pullThroughCacheRules"]] == names[1:2]


def _v2(method, path, headers=None, signed=True):
    """A registry V2 request; signed requests carry an Authorization header as the captures did."""
    sent = {"Authorization": "AWS4-HMAC-SHA256 x"} if signed else {}
    sent.update(headers or {})
    return asyncio.run(ecr.handle_registry_request(method, path, sent, b"", {}))


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("accept", [
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.oci.image.index.v1+json",
])
def test_v2_first_pull_fills_like_batch_get_image(upstream, method, accept):
    # r15-head-first, r15-get-after-head, r15-get-accept-*: Accept is ignored; the captured index is answered.
    # v20 line 192: a cache repository answers GET without Docker-Content-Digest, HEAD with it.
    _rule(PREFIX, "public.ecr.aws")
    status, headers, raw = _v2(method, f"/v2/{CAP_PULL}/manifests/3.20", {"Accept": accept})
    assert status == 200
    assert headers["Content-Type"] == "application/vnd.oci.image.index.v1+json"
    assert headers["Content-Length"] == "9226"
    if method == "HEAD":
        assert headers["Docker-Content-Digest"] == CAP_INDEX_DIGEST
        assert raw == b""
    else:
        assert "Docker-Content-Digest" not in headers
        assert raw == CAP_INDEX
    assert _action("DescribeRepositories", {"repositoryNames": [CAP_PULL]})[0] == 200
    status, body = _action("BatchGetImage", {"repositoryName": CAP_PULL, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["images"][0]["imageId"] == {"imageDigest": CAP_INDEX_DIGEST, "imageTag": "3.20"}


def test_v2_child_digest_returns_child_manifest(upstream):
    # r15-get-by-child-digest (no digest header on GET), r15-head-by-child-digest (header on HEAD)
    _rule(PREFIX, "public.ecr.aws")
    _v2("GET", f"/v2/{CAP_PULL}/manifests/3.20")
    status, headers, raw = _v2("GET", f"/v2/{CAP_PULL}/manifests/{CAP_CHILD_DIGEST}")
    assert status == 200 and raw == CAP_CHILD and "Docker-Content-Digest" not in headers
    assert headers["Content-Type"] == "application/vnd.oci.image.manifest.v1+json"
    assert headers["Content-Length"] == "1023"
    status, headers, raw = _v2("HEAD", f"/v2/{CAP_PULL}/manifests/{CAP_CHILD_DIGEST}")
    assert (status, raw, headers["Docker-Content-Digest"], headers["Content-Length"]) == (
        200, b"", CAP_CHILD_DIGEST, "1023")


def test_v2_manifest_get_on_a_pushed_repository_keeps_its_digest_header(upstream):
    # AGENTS.md:79-83: no capture for plain repositories, so today's header stays
    _action("CreateRepository", {"repositoryName": "plain"})
    assert _action("PutImage", {"repositoryName": "plain", "imageManifest": CHILD.decode(), "imageTag": "t"})[0] == 200
    status, headers, raw = _v2("GET", "/v2/plain/manifests/t")
    assert status == 200 and headers["Docker-Content-Digest"] == _digest(CHILD)


def test_v2_tags_list_of_a_cache_repository_is_compact_text_plain(upstream):
    # r15-tags-list-after-fill: Content-Type text/plain, compact body (the empty tag list is a known difference)
    _rule(PREFIX, "public.ecr.aws")
    _v2("GET", f"/v2/{CAP_PULL}/manifests/3.20")
    status, headers, raw = _v2("GET", f"/v2/{CAP_PULL}/tags/list")
    assert status == 200 and headers["Content-Type"] == "text/plain; charset=utf-8"
    assert raw == json.dumps({"name": CAP_PULL, "tags": ["3.20"]}, separators=(",", ":")).encode()
    _action("CreateRepository", {"repositoryName": "plain"})
    status, headers, raw = _v2("GET", "/v2/plain/tags/list")
    assert headers["Content-Type"] == "application/json"


def test_v2_without_rule_stays_name_unknown(upstream):
    assert _v2("GET", f"/v2/{PULL}/manifests/3.20")[0] == 404


ACCT = "111122223333"
ERR_JSON = "application/json; charset=utf-8"


def _v2_error(result, status, code, message):
    got_status, headers, raw = result
    assert got_status == status and headers["Content-Type"] == ERR_JSON
    expected = json.dumps({"errors": [{"code": code, "message": message}]}, separators=(",", ":")).encode() + b"\n"
    assert raw == expected
    assert headers["Content-Length"] == str(len(expected))


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize("prefix_rule", [True, False])
@pytest.mark.parametrize("name", [f"{PREFIX}/Docker/Library/Alpine", f"{PREFIX}/a%2F..%2Fb"])
def test_v2_bad_repository_name_is_the_router_404_and_never_fills(upstream, method, prefix_rule, name):
    # r15-get-bad-name-rule, r15-get-bad-name-no-rule (line 202); HEAD and a decoded 'a/../b' follow from the pattern
    if prefix_rule:
        _rule(PREFIX, "public.ecr.aws")
    status, headers, raw = _v2(method, f"/v2/{name}/manifests/3.20")
    assert status == 404
    assert headers["Content-Type"] == "text/plain; charset=utf-8"
    assert {k.lower(): v for k, v in headers.items()}["docker-distribution-api-version"] == "registry/2.0"
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert headers["Content-Length"] == "19"
    assert raw == (b"404 page not found\n" if method == "GET" else b"")
    assert upstream.urls == [] and upstream.daemon.pulls == []
    assert _action("DescribeRepositories", {})[1]["repositories"] == []


@pytest.mark.parametrize("name", ["msc2617-r15-norule/docker/library/alpine", PREFIX])
def test_v2_no_rule_or_bare_prefix_is_name_unknown(upstream, name):
    # r15-get-no-rule, r15-get-no-rule--repo, r15-get-prefix-only, r15-head-no-rule, r15-head-prefix-only
    _rule(PREFIX, "public.ecr.aws")
    message = f"The repository with name '{name}' does not exist in the registry with id '{get_account_id()}'"
    _v2_error(_v2("GET", f"/v2/{name}/manifests/3.20"), 404, "NAME_UNKNOWN", message)
    status, headers, raw = _v2("HEAD", f"/v2/{name}/manifests/3.20")
    assert (status, raw) == (404, b"")
    assert _action("DescribeRepositories", {"repositoryNames": [name]})[0] == 400


def test_v2_rule_with_missing_tag_is_manifest_unknown_and_creates_nothing(upstream):
    # r15-get-missing-tag-new-repo, r15-head-missing-tag-new-repo
    _rule(PREFIX, "public.ecr.aws")
    _v2_error(_v2("GET", f"/v2/{PULL}/manifests/nope"), 404, "MANIFEST_UNKNOWN", "Requested image not found")
    assert _v2("HEAD", f"/v2/{PULL}/manifests/nope")[2] == b""
    assert _action("DescribeRepositories", {"repositoryNames": [PULL]})[0] == 400


def test_v2_upstream_authentication_failure_is_denied(upstream):
    # r15-secret-dh, r15-rule-dh, r15-get-dh-fake-cred, r15-head-dh-fake-cred, r15-get-dh-fake-cred--repo
    arn = _secret('{"username":"u","accessToken":"x"}')
    _rule("msc2617-r15-dh", "registry-1.docker.io", credentialArn=arn)
    upstream.deny = True
    path = "/v2/msc2617-r15-dh/library/alpine/manifests/3.20"
    _v2_error(_v2("GET", path), 403, "DENIED",
              "The authentication to the upstream registry failed. Verify that the credentials provided in "
              "the pull through cache rule are correct, and try again.")
    status, _, raw = _v2("HEAD", path)
    assert (status, raw) == (403, b"")
    assert _action("DescribeRepositories", {"repositoryNames": ["msc2617-r15-dh/library/alpine"]})[0] == 400


@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_v2_fills_a_pre_created_repository(upstream, method):
    # r15-pre-create-repo, r15-pre-rule-over-repo, r15-get-into-existing-repo, r15-head-into-existing-repo
    assert _action("CreateRepository", {"repositoryName": PULL})[0] == 200
    _rule(PREFIX, "public.ecr.aws")
    status, headers, raw = _v2(method, f"/v2/{PULL}/manifests/3.20")
    assert status == 200 and headers.get("Docker-Content-Digest") == (_digest(INDEX) if method == "HEAD" else None)


def test_v2_after_rule_delete_and_batch_delete(upstream):
    # r15-cache-batch-delete, r15-get-after-batch-delete, r15-delete-rule-pub, r15-get-after-rule-delete-*
    _rule(PREFIX, "public.ecr.aws")
    assert _v2("GET", f"/v2/{PULL}/manifests/3.20")[0] == 200
    assert _action("BatchDeleteImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})[0] == 200
    assert _v2("GET", f"/v2/{PULL}/manifests/3.20")[0] == 200
    assert _action("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": PREFIX})[0] == 200
    assert _v2("GET", f"/v2/{PULL}/manifests/3.20")[0] == 200
    _v2_error(_v2("GET", f"/v2/{PULL}/manifests/3.19"), 404, "MANIFEST_UNKNOWN", "Requested image not found")
    other = f"{PREFIX}/docker/library/debian"
    _v2_error(_v2("GET", f"/v2/{other}/manifests/stable"), 404, "NAME_UNKNOWN",
              f"The repository with name '{other}' does not exist in the registry with id '{get_account_id()}'")


def test_root_rule_maps_name_unchanged(upstream):
    # r18-rules-before, r18-before-hello--repo, r18-before-hello-short--repo, r18-root-create
    name = "docker/library/alpine"
    status, body = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "3.20"}]})
    assert (status, body) == (400, {
        "__type": "RepositoryNotFoundException",
        "message": f"The repository with name '{name}' does not exist in the registry with id '{get_account_id()}'"})
    _rule("ROOT", "public.ecr.aws")
    status, body = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["failures"] == []
    assert _action("DescribeRepositories", {"repositoryNames": [name]})[0] == 200


def test_root_with_upstream_prefix_is_refused(upstream):
    # r18-root-upp-create, --after, r18-root-upp-pull-short
    status, body = _action("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": "ROOT", "upstreamRegistryUrl": "public.ecr.aws", "upstreamRepositoryPrefix": "docker"})
    assert status == 400 and body["__type"] == "InvalidParameterException"
    assert body["message"] == "Invalid input: Must specify ecr-repository-prefix when specify upstream-repository-prefix."
    assert _action("DescribePullThroughCacheRules", {})[1]["pullThroughCacheRules"] == []
    status, body = _action("BatchGetImage", {"repositoryName": "library/alpine", "imageIds": [{"imageTag": "3.20"}]})
    assert body == {
        "__type": "RepositoryNotFoundException",
        "message": f"The repository with name 'library/alpine' does not exist in the registry with id '{get_account_id()}'"}


def test_upstream_repository_prefix_maps_in_front_of_the_rest(upstream):
    # r17-upp-pub-docker-library, --after, r17-upp-pub-docker, r17-pull-u1-short/full
    _rule("msc2617-r17-u1", "public.ecr.aws", upstreamRepositoryPrefix="docker/library")
    short = "msc2617-r17-u1/alpine"
    status, body = _action("BatchGetImage", {"repositoryName": short, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["failures"] == []
    assert any("/docker/library/alpine/" in url for url in upstream.urls)
    full = "msc2617-r17-u1/docker/library/alpine"
    status, body = _action("BatchGetImage", {"repositoryName": full, "imageIds": [{"imageTag": "3.20"}]})
    assert body["failures"] == [{"imageId": {"imageTag": "3.20"}, **FAILURE_MISSING}]
    assert _action("DescribeRepositories", {"repositoryNames": [full]})[0] == 400


def test_longest_matching_prefix_wins_and_does_not_fall_back(upstream):
    # r17-nest-p-parent, r17-nest-p-child
    _rule("msc2617-r17-np", "public.ecr.aws", upstreamRepositoryPrefix="docker/library")
    _rule("msc2617-r17-np/pub", "registry.k8s.io")
    status, body = _action("BatchGetImage", {"repositoryName": "msc2617-r17-np/pub/alpine",
                                             "imageIds": [{"imageTag": "3.20"}]})
    assert body["failures"] == [{"imageId": {"imageTag": "3.20"}, **FAILURE_MISSING}]
    assert any(u.startswith("https://registry.k8s.io/") for u in upstream.urls)
    assert not any("public.ecr.aws" in u for u in upstream.urls)
    assert _action("DescribeRepositories", {"repositoryNames": ["msc2617-r17-np/pub/alpine"]})[0] == 400


ROLE = "msc2617-r16-ptc-fill"
SRC = "msc2617-r16-src/app"


def _ecr_upstream(grant=True):
    """An upstream registry in account 444455556666 holding SRC:v1, and a cache role in the caller's account."""
    from ministack.core.responses import get_region, request_scope

    role_arn = f"arn:aws:iam::{get_account_id()}:role/{ROLE}"
    iam._roles.pop(ROLE, None)
    iam._roles[ROLE] = {"RoleName": ROLE, "Arn": role_arn, "AssumeRolePolicyDocument": json.dumps(
        {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal":
         {"Service": "pullthroughcache.ecr.amazonaws.com"}, "Action": "sts:AssumeRole"}]})}
    manifest = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                           "config": {"digest": _digest(CONFIG), "size": len(CONFIG), "mediaType": "x"},
                           "layers": [{"digest": _digest(LAYER), "size": len(LAYER), "mediaType": "y"}]})
    with request_scope("444455556666", get_region()):
        assert _action("CreateRepository", {"repositoryName": SRC})[0] == 200
        ecr._images[SRC].append({
            "registryId": "444455556666", "repositoryName": SRC, "imageDigest": _digest(manifest.encode()),
            "imageTags": ["v1"], "imageManifest": manifest, "imageId": {"imageDigest": _digest(manifest.encode()),
                                                                          "imageTag": "v1"},
            "imageManifestMediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "imageSizeInBytes": 1, "imagePushedAt": 1})
        ecr._manifest_blobs.setdefault(SRC, {})[_digest(manifest.encode())] = manifest.encode()
        ecr._layer_blobs.setdefault(SRC, {})[_digest(LAYER)] = LAYER
        if grant:
            ecr._repo_policies[SRC] = json.dumps({"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"AWS": role_arn}, "Action": ["ecr:BatchGetImage"]}]})
    _rule("msc2617-r16-e1", f"444455556666.dkr.ecr.{get_region()}.amazonaws.com", customRoleArn=role_arn)
    return manifest


def test_ecr_to_ecr_fill_and_held_tag_is_not_refreshed(upstream):
    # r16-iam-role, r16-iam-role-policy, r16-a2-src-repo, r16-pull-e1-src, r16-pull-e1-src-again,
    # r16-a2-src-put-v1-again, r16-a2-src-put-v2
    from ministack.core.responses import get_region, request_scope

    manifest = _ecr_upstream()
    name = f"msc2617-r16-e1/{SRC}"
    status, body = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "v1"}]})
    assert status == 200 and body["failures"] == []
    assert body["images"][0]["imageManifest"] == manifest
    assert body["images"][0]["imageId"]["imageDigest"] == _digest(manifest.encode())
    repo = _action("DescribeRepositories", {"repositoryNames": [name]})[1]["repositories"][0]
    assert (repo["imageTagMutability"], repo["encryptionConfiguration"]) == ("MUTABLE", {"encryptionType": "AES256"})
    with request_scope("444455556666", get_region()):
        moved = manifest.replace('"size": 1', '"size": 1 ') + " "
        for img in ecr._images[SRC]:
            img["imageTags"] = []
        ecr._images[SRC].append({"registryId": "444455556666", "repositoryName": SRC, "imageDigest": _digest(moved.encode()),
                                 "imageTags": ["v1", "v2"], "imageManifest": moved,
                                 "imageManifestMediaType": "application/vnd.docker.distribution.manifest.v2+json"})
    again = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "v1"}, {"imageTag": "v2"}]})[1]
    assert [i["imageId"]["imageTag"] for i in again["images"]] == ["v1", "v2"]
    assert again["images"][0]["imageId"]["imageDigest"] == _digest(manifest.encode())
    missing = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "msc2617-nope"}]})[1]
    assert missing["failures"][0] == {"imageId": {"imageTag": "msc2617-nope"}, **FAILURE_MISSING}


def test_ecr_upstream_refused_without_repository_policy_grant(upstream):
    # r16-pull-e1-src-first (no grant yet): ImageNotFound, no repository
    _ecr_upstream(grant=False)
    name = f"msc2617-r16-e1/{SRC}"
    status, body = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "v1"}]})
    assert body["failures"] == [{"imageId": {"imageTag": "v1"}, **FAILURE_MISSING}]
    assert _action("DescribeRepositories", {"repositoryNames": [name]})[0] == 400


def test_create_repository_made_repository_is_filled_and_keeps_settings(upstream):
    # r25-pre-a-create-repo, r25-pre-rule, r25-pre-a-bgi-first (+--repo), r07-pre-rule-over-repo, r07-pre-pull-into-existing
    created = _action("CreateRepository", {"repositoryName": PULL, "imageTagMutability": "IMMUTABLE"})[1]["repository"]
    _rule(PREFIX, "public.ecr.aws")
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["failures"] == []
    assert body["images"][0]["imageId"]["imageDigest"] == _digest(INDEX)
    repo = _action("DescribeRepositories", {"repositoryNames": [PULL]})[1]["repositories"][0]
    assert repo["createdAt"] == created["createdAt"] and repo["imageTagMutability"] == "IMMUTABLE"
    assert _v2("GET", f"/v2/{PULL}/manifests/3.20")[0] == 200


def test_unheld_tag_is_not_filled_after_the_only_rule_is_deleted_on_batch_get_image(upstream):
    # r25-del-rule, r25-del-bgi-fill, r25-del-bgi-fill--repo, r25-del-v2-fill, r25-del-t0-bgirepo-bgi-held
    _rule(PREFIX, "public.ecr.aws")
    assert _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})[0] == 200
    assert _action("DeletePullThroughCacheRule", {"ecrRepositoryPrefix": PREFIX})[0] == 200
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.19"}]})
    assert status == 200 and body["images"] == [] and body["failures"] == [
        {"imageId": {"imageTag": "3.19"}, **FAILURE_MISSING}]
    held = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})[1]
    assert held["images"][0]["imageId"]["imageDigest"] == _digest(INDEX)


def test_ecr_upstream_fill_is_cache_only_and_served_but_not_listed(upstream):
    # r26-list-t0m-e1-describe, r26-list-t0m-e1-list-images, r26-pull-e1-ns-app--images,
    # r26-list-t180m-e1-describe-by-tag
    manifest = _ecr_upstream()
    name = f"msc2617-r16-e1/{SRC}"
    assert _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "v1"}]})[0] == 200
    assert _action("DescribeImages", {"repositoryName": name}) == (200, {"imageDetails": []})
    assert _action("DescribeImages", {"repositoryName": name, "filter": {"tagStatus": "TAGGED"}})[1] == {"imageDetails": []}
    assert _action("ListImages", {"repositoryName": name}) == (200, {"imageIds": []})
    status, body = _action("DescribeImages", {"repositoryName": name, "imageIds": [{"imageTag": "v1"}]})
    assert status == 400 and body["__type"] == "ImageNotFoundException"
    assert body["message"] == (
        f"The image with imageId {{imageDigest:'null', imageTag:'v1'}} does not exist within the repository "
        f"with name '{name}' in the registry with id '{get_account_id()}'")
    again = _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "v1"}]})[1]
    assert again["images"][0]["imageManifest"] == manifest


def test_upstream_repository_prefix_is_kept_verbatim_for_mapping(upstream):
    # r26-create-e5-upp-trailing-slash, r26-validate-e5
    created = _rule("msc2617-r26-e5", "public.ecr.aws", upstreamRepositoryPrefix="ns/")
    assert created["upstreamRepositoryPrefix"] == "ns/"
    assert ecr_pull_through.match_rule("msc2617-r26-e5/app")[2] == "ns//app"


@pytest.mark.parametrize("upp,key", [("docker/library/", "r33-t0-both-trailing-v2-get"), ("docker/", "r33-t0-one-level-v2-get")])
def test_v2_get_under_a_trailing_slash_prefix_is_name_unknown(upstream, upp, key):
    # r33-t0-both-trailing-v2-get, r33-t0-one-level-v2-get: a '//' upstream path never fills; AWS answers 404 NAME_UNKNOWN
    _rule("msc2617-r33-t2", "public.ecr.aws", upstreamRepositoryPrefix=upp)
    _v2_error(_v2("GET", "/v2/msc2617-r33-t2/alpine/manifests/3.20"), 404, "NAME_UNKNOWN",
              f"The repository with name 'msc2617-r33-t2/alpine' does not exist in the registry with id '{get_account_id()}'")


def test_put_image_over_ecr_upstream_cache_layers_is_layers_not_found(upstream):
    # r26-put-own-tag-into-e1-cache: 400 LayersNotFoundException, layers then config
    manifest = _ecr_upstream()
    name = f"msc2617-r16-e1/{SRC}"
    assert _action("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "v1"}]})[0] == 200
    status, body = _action("PutImage", {"repositoryName": name, "imageManifest": manifest, "imageTag": "mine"})
    assert status == 400 and body["__type"] == "LayersNotFoundException"
    assert body["message"] == (
        f"Layers with digests '[{_digest(LAYER)},{_digest(CONFIG)}]' required for pushing image into repository "
        f"with name '{name}' in the registry with id '{get_account_id()}' do not exist")


def test_filled_rows_carry_millisecond_imagePushedAt(upstream):
    # r07-pull-first, r07-pull-after-batch-delete--images: index and child rows hold round(time.time(), 3)
    _rule(PREFIX, "public.ecr.aws")
    _pull()
    rows = _action("DescribeImages", {"repositoryName": PULL})[1]["imageDetails"]
    assert len(rows) == 2
    for row in rows:
        assert round(row["imagePushedAt"], 3) == row["imagePushedAt"]


def test_v2_head_error_sends_the_get_body_length(upstream):
    # r15-head-no-rule: 404, the GET body's Content-Length, empty body
    name = "msc2617-r15-norule/docker/library/alpine"
    get = _v2("GET", f"/v2/{name}/manifests/3.20")
    head = _v2("HEAD", f"/v2/{name}/manifests/3.20")
    assert head[0] == get[0] == 404 and head[2] == b""
    assert head[1]["Content-Length"] == get[1]["Content-Length"] == str(len(get[2]))


def _two_platform_index(monkeypatch):
    layer2, config2 = b"layer-two", b'{"architecture":"arm64"}'
    child2 = json.dumps({
        "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": _digest(config2), "size": len(config2)},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": _digest(layer2), "size": len(layer2)}],
    }).encode()
    index = json.dumps({
        "schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": _digest(CHILD), "size": len(CHILD),
             "platform": {"architecture": "amd64", "os": "linux"}},
            {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": _digest(child2), "size": len(child2),
             "platform": {"architecture": "arm64", "os": "linux"}}],
    }).encode()
    for raw, media in ((index, "application/vnd.oci.image.index.v1+json"),
                       (child2, "application/vnd.oci.image.manifest.v1+json"),
                       (layer2, "application/octet-stream"), (config2, "application/json")):
        monkeypatch.setitem(BLOBS, _digest(raw), (raw, media))
    monkeypatch.setitem(TAGS, "multi", _digest(index))
    return index, child2, layer2


def test_a_fill_keeps_only_the_platform_the_daemon_holds_and_pulls_a_child_by_digest_on_demand(upstream, monkeypatch):
    # ANSWERS multi-arch-through-daemon (index bytes kept, one platform held) and child-platform-on-demand
    index, child2, layer2 = _two_platform_index(monkeypatch)
    _rule(PREFIX, "public.ecr.aws")
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "multi"}]})
    assert status == 200 and not body["failures"]
    assert body["images"][0]["imageManifest"] == index.decode()
    held = {img["imageDigest"] for img in ecr._images[PULL]}
    assert held == {_digest(index), _digest(CHILD)} and len(upstream.daemon.pulls) == 1
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageDigest": _digest(child2)}]})
    assert status == 200 and not body["failures"] and body["images"][0]["imageManifest"] == child2.decode()
    assert upstream.daemon.pulls[-1] == ("public.ecr.aws", "docker/library/alpine", _digest(child2))
    assert ecr._layer_blobs[PULL][_digest(layer2)] == layer2


def test_a_digest_no_stored_index_names_is_image_not_found_without_a_daemon_call(upstream, monkeypatch):
    _two_platform_index(monkeypatch)
    _rule(PREFIX, "public.ecr.aws")
    _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "multi"}]})
    pulls = len(upstream.daemon.pulls)
    stranger = _digest(b"stranger")
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageDigest": stranger}]})
    assert status == 200 and body["failures"] == [
        {"imageId": {"imageDigest": stranger}, "failureCode": "ImageNotFound", "failureReason": "Requested image not found"}]
    assert len(upstream.daemon.pulls) == pulls


def test_no_daemon_answers_upstream_unavailable_logs_the_socket_hint_and_creates_nothing(upstream, caplog):
    # ANSWERS daemon-unavailable; the failureReason is MiniStack's own text (choices.txt)
    from ministack.services import ecr_pull_through as ept

    _rule(PREFIX, "public.ecr.aws")
    upstream.daemon.absent = True
    status, body = _pull()
    assert status == 200 and body["images"] == [] and body["failures"][0]["failureCode"] == "UpstreamUnavailable"
    assert body["failures"][0]["failureReason"] == "Unable to reach the upstream registry."
    assert PULL not in ecr._repositories
    status, _, raw = ecr._v2_get_manifest(PULL, "3.20")
    assert status == 404 and json.loads(raw)["errors"][0]["code"] == "MANIFEST_UNKNOWN"
    assert ept._DOCKER_HINT.startswith("no Docker daemon answered; mount the Docker socket")


def test_the_real_daemon_client_logs_the_socket_hint_when_no_daemon_answers(monkeypatch, caplog, pull_through):
    import docker

    def refuse(**_):
        raise docker.errors.DockerException("Error while fetching server API version")

    monkeypatch.setattr(docker, "from_env", refuse)
    with caplog.at_level(logging.WARNING):
        assert ecr_pull_through._daemon_pull("public.ecr.aws", "a/b", "1", None) == ("unavailable", "", b"", {})
    assert "mount the Docker socket" in caplog.text


class _FakeApi:
    def __init__(self, pull_events=(), error=None, image=b""):
        self.pull_events, self.error, self.image = pull_events, error, image

    def pull(self, *args, **kwargs):
        self.pulled = (args, kwargs)
        if self.error:
            raise self.error
        return iter(self.pull_events)

    def get_image(self, ref):
        yield self.image


def _tar(members):
    import io
    import tarfile

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        for name, data in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _daemon_with(monkeypatch, api):
    import docker

    class Client:
        def ping(self):
            return True

    client = Client()
    client.api = api
    monkeypatch.setattr(docker, "from_env", lambda **_: client)


def test_the_daemon_client_reads_the_oci_layout_index_and_blobs(monkeypatch, pull_through):
    hexed = lambda raw: hashlib.sha256(raw).hexdigest()  # noqa: E731
    members = {"index.json": json.dumps({"manifests": [{"mediaType": "application/vnd.oci.image.index.v1+json",
                                                         "digest": _digest(INDEX), "size": len(INDEX)}]}).encode(),
               f"blobs/sha256/{hexed(INDEX)}": INDEX, f"blobs/sha256/{hexed(CHILD)}": CHILD,
               f"blobs/sha256/{hexed(LAYER)}": LAYER}
    _daemon_with(monkeypatch, _FakeApi(image=_tar(members)))
    outcome, media_type, raw, blobs = ecr_pull_through._daemon_pull("public.ecr.aws", "a/b", "3.20", ("u", "t"))
    assert (outcome, media_type, raw) == ("ok", "application/vnd.oci.image.index.v1+json", INDEX)
    assert blobs[_digest(CHILD)] == CHILD and blobs[_digest(LAYER)] == LAYER


def test_the_daemon_client_rebuilds_a_docker_manifest_when_the_daemon_holds_no_manifest(monkeypatch, pull_through):
    members = {"manifest.json": json.dumps([{"Config": "c.json", "Layers": ["l/layer.tar"]}]).encode(),
               "c.json": CONFIG, "l/layer.tar": LAYER}
    _daemon_with(monkeypatch, _FakeApi(image=_tar(members)))
    outcome, media_type, raw, blobs = ecr_pull_through._daemon_pull("public.ecr.aws", "a/b", "1", None)
    manifest = json.loads(raw)
    assert outcome == "ok" and media_type == "application/vnd.docker.distribution.manifest.v2+json"
    assert manifest["config"]["digest"] == _digest(CONFIG) and manifest["layers"][0]["digest"] == _digest(LAYER)
    assert blobs == {_digest(CONFIG): CONFIG, _digest(LAYER): LAYER}


@pytest.mark.parametrize("message,credentials,outcome", [
    ("manifest for a/b:1 not found: manifest unknown", None, "notfound"),
    ("pull access denied for a/b, repository does not exist or may require authorization", None, "notfound"),
    ("unauthorized: authentication required", ("u", "t"), "denied"),
    ("net/http: TLS handshake timeout", None, "unavailable"),
])
def test_the_daemon_client_maps_a_pull_error_to_an_outcome(monkeypatch, message, credentials, outcome, pull_through):
    _daemon_with(monkeypatch, _FakeApi(pull_events=[{"status": "Pulling"}, {"error": message}]))
    assert ecr_pull_through._daemon_pull("public.ecr.aws", "a/b", "1", credentials)[0] == outcome


UNAVAILABLE = {"failureCode": "UpstreamUnavailable", "failureReason": "Unable to reach the upstream registry."}
UNABLE_TO_VALIDATE = {
    "failureCode": "UNABLE_TO_VALIDATE",
    "failureReason": "Failed to get response from upstream registry authorization endpoint. "
                     "The upstream registry may be unavailable. Try again."}


@pytest.mark.parametrize("raw", [
    b"[]", b"3", b'"x"', b"null", b'{"config":[]}', b'{"layers":{}}', b'{"layers":[3]}', b'{"config":{"size":"1"}}',
    b'{"layers":[{"size":1.5}]}', b'{"manifests":[null]}', b'{"layers":[{"size":true}]}',
])
def test_malformed_upstream_manifest_stores_nothing_and_logs_once(upstream, caplog, raw):
    # v20 line 194 (ANSWERS upstream-manifest-malformed): BatchGetImage 200 failure, V2 GET 404, HEAD 404 empty
    _rule(PREFIX, "public.ecr.aws")
    upstream.manifest_answer = raw
    with caplog.at_level(logging.WARNING, logger="ecr"):
        status, body = _action("BatchGetImage", {"repositoryName": CAP_PULL, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["images"] == []
    assert body["failures"] == [{"imageId": {"imageTag": "3.20"}, **UNAVAILABLE}]
    assert [r.getMessage() for r in caplog.records if "malformed upstream manifest" in r.getMessage()]
    assert len([r for r in caplog.records if "malformed upstream manifest" in r.getMessage()]) == 1
    _v2_error(_v2("GET", f"/v2/{CAP_PULL}/manifests/3.20"), 404, "MANIFEST_UNKNOWN", "Requested image not found")
    status, headers, body = _v2("HEAD", f"/v2/{CAP_PULL}/manifests/3.20")
    assert (status, body) == (404, b"")
    assert CAP_PULL not in ecr._repositories and CAP_PULL not in ecr_pull_through._origins
    assert CAP_PULL not in ecr._manifest_blobs and CAP_PULL not in ecr._layer_blobs
    assert _action("DescribeRepositories", {"repositoryNames": [CAP_PULL]})[0] == 400


def test_http_exception_from_the_upstream_is_no_answer(upstream):
    # v20 line 194: an http.client.HTTPException counts as no answer, not a 500
    _rule(PREFIX, "public.ecr.aws")
    upstream.truncate = True
    status, body = _action("BatchGetImage", {"repositoryName": CAP_PULL, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["failures"] == [{"imageId": {"imageTag": "3.20"}, **UNAVAILABLE}]
    _v2_error(_v2("GET", f"/v2/{CAP_PULL}/manifests/3.20"), 404, "MANIFEST_UNKNOWN", "Requested image not found")
    assert CAP_PULL not in ecr._repositories


@pytest.mark.parametrize("answer", [b"[]", b'"t"', b"{}", b'{"token":3}', b'{"token":null,"access_token":[]}', b"not json",
                                    b'{"token":""}', b'{"token":"","access_token":""}'])
def test_malformed_token_answer_is_no_answer_for_validate_and_fill(upstream, answer):
    # v20 line 195: no JSON object / no string token or access_token -> UNABLE_TO_VALIDATE and UpstreamUnavailable;
    # ledger 214 (MiniStack contract, uncaptured: ledger 195, 188): an empty string is no answer either
    arn = _secret('{"username":"u","accessToken":"x"}')
    _rule("msc2617-r03-s-ok", "registry-1.docker.io", credentialArn=arn)
    upstream.token_answer = answer
    status, body = _action("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": "msc2617-r03-s-ok"})
    assert status == 200 and body["isValid"] is False
    assert body["failure"] == json.dumps(UNABLE_TO_VALIDATE, separators=(",", ":"))
    status, body = _action("BatchGetImage", {"repositoryName": "msc2617-r03-s-ok/library/alpine",
                                             "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["failures"] == [{"imageId": {"imageTag": "3.20"}, **UNAVAILABLE}]


def test_token_answer_with_access_token_only_is_accepted(upstream):
    _rule(PREFIX, "public.ecr.aws")
    upstream.token_answer = b'{"access_token":"t"}'
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["failures"] == []


def _drop_layer():
    ecr._layer_blobs.get(PULL, {}).pop(_digest(LAYER), None)


@pytest.mark.parametrize("failure", ["down", "notfound", "mismatch", "truncated"])
def test_failed_on_demand_layer_fetch_is_layers_not_found_and_blob_unknown(upstream, monkeypatch, caplog, failure):
    # v20 line 196 (ANSWERS child-platform-on-demand)
    _rule(PREFIX, "public.ecr.aws")
    _pull()
    _drop_layer()
    if failure == "down":
        upstream.down = True
    elif failure == "notfound":
        monkeypatch.delitem(BLOBS, _digest(LAYER))
    elif failure == "mismatch":
        monkeypatch.setitem(BLOBS, _digest(LAYER), (b"other bytes", "application/octet-stream"))
    else:
        upstream.truncate = True
    with caplog.at_level(logging.WARNING, logger="ecr"):
        status, body = _action("GetDownloadUrlForLayer", {"repositoryName": PULL, "layerDigest": _digest(LAYER)})
    assert status == 400 and body["__type"] == "LayersNotFoundException"
    assert body["message"] == LAYERS_NOT_FOUND
    assert any("could not fetch layer" in r.getMessage() for r in caplog.records)
    for method in ("GET", "HEAD"):
        status, _, raw = _v2(method, f"/v2/{PULL}/blobs/{_digest(LAYER)}")
        assert status == 404
        assert method == "HEAD" or json.loads(raw)["errors"][0]["code"] == "BLOB_UNKNOWN"
    assert _digest(LAYER) not in ecr._layer_blobs.get(PULL, {})


def test_layer_not_named_by_a_stored_manifest_keeps_today_answer(upstream):
    _rule(PREFIX, "public.ecr.aws")
    _pull()
    status, body = _action("GetDownloadUrlForLayer", {"repositoryName": PULL, "layerDigest": "sha256:" + "0" * 64})
    assert status == 200 and body["layerDigest"] == "sha256:" + "0" * 64


def test_cross_account_batch_get_image_with_grant_fills_off_the_event_loop(upstream, monkeypatch):
    # v20 line 197 (ANSWERS fill-off-event-loop), r11-a2-pull-with-grant-again: the grant check passes, then the
    # handler and its fill run in a worker thread
    import threading

    from ministack.core.responses import get_region, request_scope

    owner, region = get_account_id(), get_region()
    assert _action("CreateRepository", {"repositoryName": PULL})[0] == 200
    _rule(PREFIX, "public.ecr.aws")
    policy = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::222222222222:root"},
        "Action": ["ecr:BatchGetImage"]}]})
    assert _action("SetRepositoryPolicy", {"repositoryName": PULL, "policyText": policy})[0] == 200
    seen = []
    real_fill = ecr_pull_through.fill

    def fill(name, image_ids):
        seen.append((threading.current_thread() is threading.main_thread(), name))
        return real_fill(name, image_ids)

    monkeypatch.setattr(ecr_pull_through, "fill", fill)
    with request_scope("222222222222", region):
        status, body = _action("BatchGetImage", {"registryId": owner, "repositoryName": PULL,
                                                 "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["failures"] == [] and body["images"][0]["imageId"]["imageDigest"] == _digest(INDEX)
    assert seen == [(False, PULL)]


def test_an_empty_token_is_skipped_for_a_non_empty_access_token(upstream):
    """MiniStack contract, uncaptured: ledger 214 (from ledger 195): an empty token is no answer, so the
    access_token member is tried."""
    _rule(PREFIX, "public.ecr.aws")
    upstream.token_answer = b'{"token":"","access_token":"t"}'
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}]})
    assert status == 200 and body["failures"] == []


@pytest.mark.parametrize("bad_id", ["x", 5, None])
def test_an_image_id_that_is_not_an_object_is_a_serialization_error(upstream, bad_id):
    """MiniStack contract, uncaptured: ledger 210 (as amended by 218). An imageIds item that is not an object is
    the SerializationException a JSON type outside the member's shape gets, on every repository, with no fill."""
    _rule(PREFIX, "public.ecr.aws")
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [bad_id]})
    assert (status, body) == (400, {"__type": "SerializationException", "Message": "Unexpected field type"})
    assert upstream.daemon.pulls == [] and PULL not in ecr._repositories


@pytest.mark.parametrize("bad_id", [{}, {"imageTag": 5}, {"imageDigest": 5}, {"imageTag": None, "imageDigest": None},
                                    {"imageTag": "a b"}, {"imageTag": "-x"}, {"imageTag": "t" * 129},
                                    {"imageDigest": "sha256:abc"}, {"imageDigest": "sha256:" + "G" * 64},
                                    {"imageDigest": "sha256:" + "A" * 64}, {"imageDigest": "md5:" + "a" * 64}])
def test_an_image_id_that_cannot_name_an_upstream_image_starts_no_fill(upstream, bad_id):
    """MiniStack contract, uncaptured: ledger 210 and 215. Such an id is the ImageNotFound failure a missing tag
    gets on a repository with no rule (r15-get-missing-tag-new-repo for the V2 answer), echoed back, with no
    daemon call and no repository."""
    _rule(PREFIX, "public.ecr.aws")
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [bad_id]})
    assert status == 200 and body["images"] == []
    assert body["failures"] == [{"imageId": bad_id, "failureCode": "ImageNotFound",
                                 "failureReason": "Requested image not found"}]
    assert upstream.daemon.pulls == [] and PULL not in ecr._repositories


def test_an_invalid_tag_on_the_ecr_upstream_path_is_image_not_found(upstream):
    """MiniStack contract, uncaptured: ledger 210."""
    role = f"arn:aws:iam::{get_account_id()}:role/msc-2617-r12-ecr-upstream-role-ptc"
    _rule("msc2617-r12-e1", "444455556666.dkr.ecr.us-east-2.amazonaws.com", customRoleArn=role)
    status, body = _action("BatchGetImage", {"repositoryName": "msc2617-r12-e1/x", "imageIds": [{}, {"imageTag": "a b"}]})
    assert status == 200 and [f["failureCode"] for f in body["failures"]] == ["ImageNotFound", "ImageNotFound"]


def test_a_valid_id_beside_an_invalid_one_is_still_filled(upstream):
    """MiniStack contract, uncaptured: ledger 210: the invalid id fails alone."""
    _rule(PREFIX, "public.ecr.aws")
    status, body = _action("BatchGetImage", {"repositoryName": PULL, "imageIds": [{"imageTag": "3.20"}, {}]})
    assert status == 200 and len(body["images"]) == 1
    assert body["failures"] == [{"imageId": {}, "failureCode": "ImageNotFound", "failureReason": "Requested image not found"}]


def test_a_tag_moved_by_a_concurrent_fill_leaves_the_tag_on_one_image(upstream):
    """MiniStack contract, uncaptured: ledger 217. The tag-move in ``_store`` is reachable only when two fills of
    one tag overlap (the fill takes no lock and the daemon call waits on the network): the later store takes the
    tag from the earlier image, as a push of the same tag does."""
    _rule(PREFIX, "public.ecr.aws")
    _pull()
    first = _digest(INDEX)
    assert "3.20" in next(img for img in ecr._images[PULL] if img["imageDigest"] == first)["imageTags"]
    ecr_pull_through._store(PULL, ecr_pull_through._origins[PULL], CHILD, "application/vnd.oci.image.manifest.v1+json",
                            "3.20", {})
    held = {img["imageDigest"]: img.get("imageTags", []) for img in ecr._images[PULL]}
    assert held[first] == [] and held[_digest(CHILD)] == ["3.20"]


class _Throttled:
    """A daemon API whose pull answers *answers* in turn (an error event or an exception), then succeeds."""

    def __init__(self, answers, image):
        self.answers, self.image, self.calls = list(answers), image, 0

    def pull(self, *args, **kwargs):
        self.calls += 1
        answer = self.answers.pop(0) if self.answers else None
        if isinstance(answer, Exception):
            raise answer
        return iter([{"error": answer}] if answer else [{"status": "Pulling"}])

    def get_image(self, ref):
        yield self.image



def _ok_image():
    members = {"manifest.json": json.dumps([{"Config": "c.json", "Layers": ["l/layer.tar"]}]).encode(),
               "c.json": CONFIG, "l/layer.tar": LAYER}
    return _tar(members)


@pytest.mark.parametrize("message", ["toomanyrequests: You have reached your pull rate limit", "Too Many Requests"])
def test_the_daemon_fill_retries_a_rate_limited_pull_after_1_2_and_4_seconds(monkeypatch, pull_through, message):
    """MiniStack contract, uncaptured: ledger 187 and 212 (the daemon gives no Retry-After): up to 3 retries,
    waiting 1 s, 2 s and 4 s, only when the daemon error names toomanyrequests or too many requests."""
    slept = []
    monkeypatch.setattr(ecr_pull_through.time, "sleep", slept.append)
    api = _Throttled([message, message, message], _ok_image())
    _daemon_with(monkeypatch, api)
    assert ecr_pull_through._daemon_pull("registry-1.docker.io", "library/alpine", "3.20", None)[0] == "ok"
    assert slept == [1.0, 2.0, 4.0] and api.calls == 4


def test_the_daemon_fill_gives_up_after_three_retries_as_unavailable(monkeypatch, pull_through):
    """MiniStack contract, uncaptured: ledger 187 and 212: a fourth rate limit is UpstreamUnavailable."""
    slept = []
    monkeypatch.setattr(ecr_pull_through.time, "sleep", slept.append)
    api = _Throttled(["toomanyrequests: slow down"] * 9, b"")
    _daemon_with(monkeypatch, api)
    assert ecr_pull_through._daemon_pull("registry-1.docker.io", "library/alpine", "3.20", None) == ("unavailable", "", b"", {})
    assert slept == [1.0, 2.0, 4.0] and api.calls == 4


def test_the_daemon_fill_retries_a_429_api_error(monkeypatch, pull_through):
    """MiniStack contract, uncaptured: ledger 212: a daemon API error with status 429 is a rate limit."""
    class Err(Exception):
        status_code = 429

    slept = []
    monkeypatch.setattr(ecr_pull_through.time, "sleep", slept.append)
    api = _Throttled([Err("x")], _ok_image())
    _daemon_with(monkeypatch, api)
    assert ecr_pull_through._daemon_pull("registry-1.docker.io", "library/alpine", "3.20", None)[0] == "ok"
    assert slept == [1.0]


@pytest.mark.parametrize("message", ["manifest unknown", "net/http: TLS handshake timeout", "unauthorized: authentication required"])
def test_the_daemon_fill_does_not_retry_other_errors(monkeypatch, pull_through, message):
    """MiniStack contract, uncaptured: ledger 212: only a rate limit is retried."""
    slept = []
    monkeypatch.setattr(ecr_pull_through.time, "sleep", slept.append)
    api = _Throttled([message], b"")
    _daemon_with(monkeypatch, api)
    ecr_pull_through._daemon_pull("registry-1.docker.io", "library/alpine", "3.20", ("u", "t"))
    assert slept == [] and api.calls == 1


def test_docker_hub_pull_access_denied_for_a_missing_repository_is_not_found(monkeypatch, pull_through):
    """MiniStack contract, uncaptured: ledger 215: the not-found words are checked before the denied words, so
    Docker Hub's "pull access denied ... repository does not exist" is notfound even with credentials."""
    message = "pull access denied for a/b, repository does not exist or may require 'docker login'"
    _daemon_with(monkeypatch, _FakeApi(pull_events=[{"error": message}]))
    assert ecr_pull_through._daemon_pull("registry-1.docker.io", "a/b", "1", ("u", "t"))[0] == "notfound"


def test_a_no_such_host_daemon_error_is_unavailable_and_not_retried(monkeypatch, pull_through):
    """MiniStack contract, uncaptured: ledger 226: a DNS failure is UpstreamUnavailable, not a missing image."""
    slept = []
    monkeypatch.setattr(ecr_pull_through.time, "sleep", slept.append)
    api = _Throttled(["Get https://r.test/v2/: dial tcp: lookup r.test: no such host"], b"")
    _daemon_with(monkeypatch, api)
    assert ecr_pull_through._daemon_pull("registry-1.docker.io", "a/b", "1", None)[0] == "unavailable"
    assert slept == [] and api.calls == 1


def test_a_digest_holding_429_in_a_denied_error_is_access_denied_and_not_retried(monkeypatch, pull_through):
    """MiniStack contract, uncaptured: ledger 226: a bare 429 inside the text is not a rate limit."""
    slept = []
    monkeypatch.setattr(ecr_pull_through.time, "sleep", slept.append)
    api = _Throttled(["denied: requested access to sha256:ab429cd is denied"], b"")
    _daemon_with(monkeypatch, api)
    assert ecr_pull_through._daemon_pull("registry-1.docker.io", "a/b", "1", ("u", "t"))[0] == "denied"
    assert slept == [] and api.calls == 1


def test_fill_from_ecr_takes_a_tag_held_by_another_image():
    """MiniStack contract, uncaptured: ledger 217: ``_fill_from_ecr`` moves a tag through the same helper as ``_store``."""
    images = [{"imageDigest": "sha256:a", "imageTags": ["t"]}, {"imageDigest": "sha256:b", "imageTags": ["t", "u"]}]
    ecr_pull_through._move_tag(images, "sha256:b", "t")
    assert images[0]["imageTags"] == [] and images[1]["imageTags"] == ["t", "u"]
