import hashlib
import json
import os
import uuid as _uuid_mod

import boto3
import pytest
import requests
from botocore.exceptions import ClientError

from ministack.core.responses import (
    get_account_id,
    get_region,
    set_request_account_id,
    set_request_region,
)
from ministack.services import ecr as ecr_svc

ENDPOINT = os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566")
REGION = os.environ.get("MINISTACK_REGION", "us-east-1")


def _ecr_client(region, access_key="test"):
    return boto3.client(
        "ecr",
        endpoint_url=ENDPOINT,
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key="test",
    )


def test_ecr_create_repository(ecr):
    resp = ecr.create_repository(repositoryName="test-app")
    repo = resp["repository"]
    assert repo["repositoryName"] == "test-app"
    assert "repositoryUri" in repo
    assert "repositoryArn" in repo
    assert repo["imageTagMutability"] == "MUTABLE"

def test_ecr_create_duplicate_repository(ecr):
    import botocore.exceptions
    try:
        ecr.create_repository(repositoryName="test-app")
        assert False, "Should have raised"
    except botocore.exceptions.ClientError as e:
        assert "RepositoryAlreadyExistsException" in str(e)

def test_ecr_describe_repositories(ecr):
    resp = ecr.describe_repositories()
    names = [r["repositoryName"] for r in resp["repositories"]]
    assert "test-app" in names

def test_ecr_describe_repositories_by_name(ecr):
    resp = ecr.describe_repositories(repositoryNames=["test-app"])
    assert len(resp["repositories"]) == 1
    assert resp["repositories"][0]["repositoryName"] == "test-app"

def test_ecr_describe_nonexistent_repository(ecr):
    import botocore.exceptions
    try:
        ecr.describe_repositories(repositoryNames=["nonexistent"])
        assert False, "Should have raised"
    except botocore.exceptions.ClientError as e:
        assert "RepositoryNotFoundException" in str(e)

def test_ecr_put_image(ecr):
    manifest = '{"schemaVersion": 2, "config": {"digest": "sha256:abc123"}}'
    resp = ecr.put_image(
        repositoryName="test-app",
        imageManifest=manifest,
        imageTag="v1.0.0",
    )
    assert resp["image"]["repositoryName"] == "test-app"
    assert resp["image"]["imageId"]["imageTag"] == "v1.0.0"
    assert "imageDigest" in resp["image"]["imageId"]

def test_ecr_list_images(ecr):
    resp = ecr.list_images(repositoryName="test-app")
    assert len(resp["imageIds"]) >= 1
    tags = [iid.get("imageTag") for iid in resp["imageIds"]]
    assert "v1.0.0" in tags

def test_ecr_describe_images(ecr):
    resp = ecr.describe_images(repositoryName="test-app")
    assert len(resp["imageDetails"]) >= 1
    detail = resp["imageDetails"][0]
    assert "imageDigest" in detail
    assert "v1.0.0" in detail.get("imageTags", [])

def test_ecr_batch_get_image(ecr):
    resp = ecr.batch_get_image(
        repositoryName="test-app",
        imageIds=[{"imageTag": "v1.0.0"}],
    )
    assert len(resp["images"]) == 1
    assert resp["images"][0]["imageId"]["imageTag"] == "v1.0.0"
    assert len(resp["failures"]) == 0

def test_ecr_batch_get_image_not_found(ecr):
    resp = ecr.batch_get_image(
        repositoryName="test-app",
        imageIds=[{"imageTag": "nonexistent"}],
    )
    assert len(resp["images"]) == 0
    assert len(resp["failures"]) == 1

def test_ecr_batch_delete_image(ecr):
    ecr.put_image(
        repositoryName="test-app",
        imageManifest='{"schemaVersion": 2, "delete": "me"}',
        imageTag="to-delete",
    )
    resp = ecr.batch_delete_image(
        repositoryName="test-app",
        imageIds=[{"imageTag": "to-delete"}],
    )
    assert len(resp["imageIds"]) == 1
    assert len(resp["failures"]) == 0

def test_ecr_get_authorization_token(ecr):
    resp = ecr.get_authorization_token()
    assert len(resp["authorizationData"]) == 1
    assert "authorizationToken" in resp["authorizationData"][0]
    assert "proxyEndpoint" in resp["authorizationData"][0]

def test_ecr_lifecycle_policy(ecr):
    policy = '{"rules": [{"rulePriority": 1, "selection": {"tagStatus": "untagged", "countType": "sinceImagePushed", "countUnit": "days", "countNumber": 14}, "action": {"type": "expire"}}]}'
    ecr.put_lifecycle_policy(repositoryName="test-app", lifecyclePolicyText=policy)
    resp = ecr.get_lifecycle_policy(repositoryName="test-app")
    assert resp["lifecyclePolicyText"] == policy
    ecr.delete_lifecycle_policy(repositoryName="test-app")
    import botocore.exceptions
    try:
        ecr.get_lifecycle_policy(repositoryName="test-app")
        assert False, "Should have raised"
    except botocore.exceptions.ClientError as e:
        assert "LifecyclePolicyNotFoundException" in str(e)

def test_ecr_repository_policy(ecr):
    policy = '{"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*", "Action": "ecr:GetDownloadUrlForLayer"}]}'
    ecr.set_repository_policy(repositoryName="test-app", policyText=policy)
    resp = ecr.get_repository_policy(repositoryName="test-app")
    assert resp["policyText"] == policy
    ecr.delete_repository_policy(repositoryName="test-app")
    import botocore.exceptions
    try:
        ecr.get_repository_policy(repositoryName="test-app")
        assert False, "Should have raised"
    except botocore.exceptions.ClientError as e:
        assert "RepositoryPolicyNotFoundException" in str(e)

def test_ecr_image_tag_mutability(ecr):
    ecr.put_image_tag_mutability(repositoryName="test-app", imageTagMutability="IMMUTABLE")
    resp = ecr.describe_repositories(repositoryNames=["test-app"])
    assert resp["repositories"][0]["imageTagMutability"] == "IMMUTABLE"
    ecr.put_image_tag_mutability(repositoryName="test-app", imageTagMutability="MUTABLE")

def test_ecr_image_scanning_configuration(ecr):
    ecr.put_image_scanning_configuration(
        repositoryName="test-app",
        imageScanningConfiguration={"scanOnPush": True},
    )
    resp = ecr.describe_repositories(repositoryNames=["test-app"])
    assert resp["repositories"][0]["imageScanningConfiguration"]["scanOnPush"] is True

def test_ecr_tag_resource(ecr):
    resp = ecr.describe_repositories(repositoryNames=["test-app"])
    arn = resp["repositories"][0]["repositoryArn"]
    ecr.tag_resource(resourceArn=arn, tags=[{"Key": "env", "Value": "dev"}])
    tags_resp = ecr.list_tags_for_resource(resourceArn=arn)
    tag_keys = [t["Key"] for t in tags_resp["tags"]]
    assert "env" in tag_keys
    ecr.untag_resource(resourceArn=arn, tagKeys=["env"])
    tags_resp = ecr.list_tags_for_resource(resourceArn=arn)
    tag_keys = [t["Key"] for t in tags_resp["tags"]]
    assert "env" not in tag_keys

def test_ecr_delete_repository_not_empty(ecr):
    import botocore.exceptions
    try:
        ecr.delete_repository(repositoryName="test-app")
        assert False, "Should have raised"
    except botocore.exceptions.ClientError as e:
        assert "RepositoryNotEmptyException" in str(e)

def test_ecr_delete_repository_force(ecr):
    ecr.create_repository(repositoryName="to-force-delete")
    ecr.put_image(
        repositoryName="to-force-delete",
        imageManifest='{"schemaVersion": 2}',
        imageTag="latest",
    )
    resp = ecr.delete_repository(repositoryName="to-force-delete", force=True)
    assert resp["repository"]["repositoryName"] == "to-force-delete"

def test_ecr_describe_registry(ecr):
    resp = ecr.describe_registry()
    assert "registryId" in resp
    assert "replicationConfiguration" in resp


def test_ecr_repositories_are_region_scoped():
    east = _ecr_client("us-east-1")
    west = _ecr_client("us-west-2")
    same_name = "regional-same-" + _uuid_mod.uuid4().hex[:8]
    east_only = "regional-east-" + _uuid_mod.uuid4().hex[:8]
    west_only = "regional-west-" + _uuid_mod.uuid4().hex[:8]

    east_repo = east.create_repository(repositoryName=same_name)["repository"]
    west_repo = west.create_repository(repositoryName=same_name)["repository"]
    east.create_repository(repositoryName=east_only)
    west.create_repository(repositoryName=west_only)

    assert f":us-east-1:000000000000:repository/{same_name}" in east_repo["repositoryArn"]
    assert f":us-west-2:000000000000:repository/{same_name}" in west_repo["repositoryArn"]
    with pytest.raises(ClientError) as exc_info:
        east.create_repository(repositoryName=same_name)
    assert exc_info.value.response["Error"]["Code"] == (
        "RepositoryAlreadyExistsException"
    )

    east_names = {
        repo["repositoryName"] for repo in east.describe_repositories()["repositories"]
    }
    west_names = {
        repo["repositoryName"] for repo in west.describe_repositories()["repositories"]
    }
    assert east_only in east_names
    assert west_only not in east_names
    assert west_only in west_names
    assert east_only not in west_names

    with pytest.raises(ClientError) as exc_info:
        east.describe_repositories(repositoryNames=[west_only])
    assert exc_info.value.response["Error"]["Code"] == "RepositoryNotFoundException"

    catalog = requests.get(f"{ENDPOINT}/v2/_catalog")
    assert catalog.status_code == 200
    assert west_only not in catalog.json()["repositories"]

    west_namespace = "000000000000.dkr.ecr.us-west-2.amazonaws.com"
    catalog = requests.get(
        f"{ENDPOINT}/v2/_catalog", headers={"Host": west_namespace}
    )
    assert catalog.status_code == 200
    assert west_only in catalog.json()["repositories"]
    assert east_only not in catalog.json()["repositories"]
    catalog = requests.get(
        f"{ENDPOINT}/v2/_catalog", params={"ns": west_namespace}
    )
    assert catalog.status_code == 200
    assert west_only in catalog.json()["repositories"]
    assert east_only not in catalog.json()["repositories"]


# ──────────────────────────────────────────────────────────────────────────
# Docker Registry HTTP API V2 — `docker push` / `docker pull` wire protocol.
# Issue #606: paths starting with /v2/ were routing to S3 (which returned 405).
# ──────────────────────────────────────────────────────────────────────────


def _digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _v2_single_shot_upload(repo, payload, *, headers=None, namespace=None):
    digest = _digest(payload)
    params = {"digest": digest}
    if namespace is not None:
        params["ns"] = namespace
    response = requests.post(
        f"{ENDPOINT}/v2/{repo}/blobs/uploads/",
        params=params,
        data=payload,
        headers=headers,
    )
    return response, digest


def test_v2_version_probe():
    r = requests.get(f"{ENDPOINT}/v2/")
    assert r.status_code == 200
    assert r.headers.get("Docker-Distribution-API-Version") == "registry/2.0"


def test_v2_unknown_repo_returns_404(ecr):
    r = requests.head(
        f"{ENDPOINT}/v2/no-such-repo-{_uuid_mod.uuid4().hex[:8]}/blobs/sha256:" + "0" * 64
    )
    assert r.status_code == 404


def test_v2_registry_scope_uses_host_ns_and_unique_region_fallback():
    west = _ecr_client("us-west-2")
    europe = _ecr_client("eu-west-1")
    account = "000000000000"

    host_repo = "v2-host-region-" + _uuid_mod.uuid4().hex[:8]
    host_account = "111111111111"
    host_account_west = _ecr_client("us-west-2", access_key=host_account)
    host_account_west.create_repository(repositoryName=host_repo)
    west_host = {"Host": f"{host_account}.dkr.ecr.us-west-2.amazonaws.com"}
    response, digest = _v2_single_shot_upload(
        host_repo, b"host-region", headers=west_host
    )
    assert response.status_code == 201, response.text
    response = requests.get(
        f"{ENDPOINT}/v2/{host_repo}/blobs/{digest}", headers=west_host
    )
    assert response.status_code == 200
    assert response.content == b"host-region"

    # Unique fallback never crosses accounts, so the same request without
    # the host hint stays in the default account and cannot see this repo.
    response = requests.get(f"{ENDPOINT}/v2/{host_repo}/blobs/{digest}")
    assert response.status_code == 404

    # Plain localhost requests model an EKS/containerd mirror without an
    # original-host hint. A unique repository match selects its region.
    unique_repo = "v2-unique-region-" + _uuid_mod.uuid4().hex[:8]
    west.create_repository(repositoryName=unique_repo)
    response, unique_digest = _v2_single_shot_upload(
        unique_repo, b"unique-region"
    )
    assert response.status_code == 201, response.text
    response = requests.get(f"{ENDPOINT}/v2/{unique_repo}/blobs/{unique_digest}")
    assert response.status_code == 200
    assert response.content == b"unique-region"

    ns_repo = "v2-ns-region-" + _uuid_mod.uuid4().hex[:8]
    europe.create_repository(repositoryName=ns_repo)
    namespace = f"{account}.dkr.ecr.eu-west-1.amazonaws.com"
    response, ns_digest = _v2_single_shot_upload(
        ns_repo, b"namespace-region", namespace=namespace
    )
    assert response.status_code == 201, response.text
    response = requests.get(
        f"{ENDPOINT}/v2/{ns_repo}/blobs/{ns_digest}",
        params={"ns": namespace},
    )
    assert response.status_code == 200
    assert response.content == b"namespace-region"


def test_v2_registry_scope_prefers_ambient_and_rejects_ambiguous_fallback():
    east = _ecr_client("us-east-1")
    west = _ecr_client("us-west-2")
    europe = _ecr_client("eu-west-1")
    account = "000000000000"

    ambient_repo = "v2-ambient-region-" + _uuid_mod.uuid4().hex[:8]
    east.create_repository(repositoryName=ambient_repo)
    west.create_repository(repositoryName=ambient_repo)
    response, digest = _v2_single_shot_upload(ambient_repo, b"ambient-region")
    assert response.status_code == 201, response.text
    east_host = {"Host": f"{account}.dkr.ecr.us-east-1.amazonaws.com"}
    west_host = {"Host": f"{account}.dkr.ecr.us-west-2.amazonaws.com"}
    assert requests.get(
        f"{ENDPOINT}/v2/{ambient_repo}/blobs/{digest}", headers=east_host
    ).status_code == 200
    assert requests.get(
        f"{ENDPOINT}/v2/{ambient_repo}/blobs/{digest}", headers=west_host
    ).status_code == 404

    ambiguous_repo = "v2-ambiguous-region-" + _uuid_mod.uuid4().hex[:8]
    west.create_repository(repositoryName=ambiguous_repo)
    europe.create_repository(repositoryName=ambiguous_repo)
    response, _digest_value = _v2_single_shot_upload(
        ambiguous_repo, b"ambiguous-region"
    )
    assert response.status_code == 404
    assert response.json()["errors"][0]["code"] == "NAME_UNKNOWN"


def test_v2_chunked_blob_upload_then_manifest_then_describe_images(ecr):
    repo = "v2-pushflow-" + _uuid_mod.uuid4().hex[:8]
    ecr.create_repository(repositoryName=repo)

    # Two layers — first chunked, second single-shot.
    layer_a = b"layer-a-bytes" * 100
    layer_b = b"layer-b-bytes" * 50
    digest_a = _digest(layer_a)
    digest_b = _digest(layer_b)

    # Chunked upload of layer A.
    r = requests.post(f"{ENDPOINT}/v2/{repo}/blobs/uploads/")
    assert r.status_code == 202, r.text
    upload_url = r.headers["Location"]
    assert upload_url.startswith("/v2/")

    # PATCH first half.
    half = len(layer_a) // 2
    r = requests.patch(f"{ENDPOINT}{upload_url}", data=layer_a[:half])
    assert r.status_code == 202, r.text
    # PATCH second half.
    upload_url = r.headers["Location"]
    r = requests.patch(f"{ENDPOINT}{upload_url}", data=layer_a[half:])
    assert r.status_code == 202

    # PUT to finalise with digest query.
    upload_url = r.headers["Location"]
    r = requests.put(f"{ENDPOINT}{upload_url}", params={"digest": digest_a})
    assert r.status_code == 201, r.text
    assert r.headers["Docker-Content-Digest"] == digest_a

    # Single-shot upload of layer B.
    r = requests.post(
        f"{ENDPOINT}/v2/{repo}/blobs/uploads/",
        params={"digest": digest_b},
        data=layer_b,
    )
    assert r.status_code == 201
    assert r.headers["Docker-Content-Digest"] == digest_b

    # HEAD + GET round-trip layer A bytes.
    r = requests.head(f"{ENDPOINT}/v2/{repo}/blobs/{digest_a}")
    assert r.status_code == 200
    assert r.headers["Docker-Content-Digest"] == digest_a
    assert int(r.headers["Content-Length"]) == len(layer_a)

    r = requests.get(f"{ENDPOINT}/v2/{repo}/blobs/{digest_a}")
    assert r.status_code == 200
    assert r.content == layer_a

    # PUT manifest by tag.
    manifest = json.dumps({
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "size": len(layer_b),
            "digest": digest_b,
        },
        "layers": [{
            "mediaType": "application/vnd.docker.image.rootfs.diff.tar.gzip",
            "size": len(layer_a),
            "digest": digest_a,
        }],
    }).encode()
    manifest_digest = _digest(manifest)

    r = requests.put(
        f"{ENDPOINT}/v2/{repo}/manifests/v1.0.0",
        data=manifest,
        headers={"Content-Type": "application/vnd.docker.distribution.manifest.v2+json"},
    )
    assert r.status_code == 201, r.text
    assert r.headers["Docker-Content-Digest"] == manifest_digest

    # GET manifest by tag.
    r = requests.get(f"{ENDPOINT}/v2/{repo}/manifests/v1.0.0")
    assert r.status_code == 200
    assert r.content == manifest
    assert r.headers["Docker-Content-Digest"] == manifest_digest
    assert r.headers["Content-Type"] == (
        "application/vnd.docker.distribution.manifest.v2+json"
    )

    # GET manifest by digest.
    r = requests.get(f"{ENDPOINT}/v2/{repo}/manifests/{manifest_digest}")
    assert r.status_code == 200
    assert r.content == manifest

    # HEAD manifest by tag.
    r = requests.head(f"{ENDPOINT}/v2/{repo}/manifests/v1.0.0")
    assert r.status_code == 200
    assert r.headers["Docker-Content-Digest"] == manifest_digest

    # /v2/<repo>/tags/list returns the tag.
    r = requests.get(f"{ENDPOINT}/v2/{repo}/tags/list")
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == repo
    assert "v1.0.0" in body["tags"]

    # AWS ECR DescribeImages now sees the pushed image.
    desc = ecr.describe_images(repositoryName=repo)
    digests = [img["imageDigest"] for img in desc["imageDetails"]]
    assert manifest_digest in digests
    pushed = next(img for img in desc["imageDetails"] if img["imageDigest"] == manifest_digest)
    assert "v1.0.0" in pushed.get("imageTags", [])


def test_v2_digest_mismatch_returns_400(ecr):
    repo = "v2-bad-digest-" + _uuid_mod.uuid4().hex[:8]
    ecr.create_repository(repositoryName=repo)

    payload = b"actual-bytes"
    wrong_digest = "sha256:" + "f" * 64

    r = requests.post(f"{ENDPOINT}/v2/{repo}/blobs/uploads/")
    assert r.status_code == 202
    upload_url = r.headers["Location"]
    r = requests.patch(f"{ENDPOINT}{upload_url}", data=payload)
    assert r.status_code == 202
    upload_url = r.headers["Location"]
    r = requests.put(f"{ENDPOINT}{upload_url}", params={"digest": wrong_digest})
    assert r.status_code == 400
    body = r.json()
    assert body["errors"][0]["code"] == "DIGEST_INVALID"


def test_v2_cross_repo_blob_mount(ecr):
    src = "v2-mount-src-" + _uuid_mod.uuid4().hex[:8]
    dst = "v2-mount-dst-" + _uuid_mod.uuid4().hex[:8]
    ecr.create_repository(repositoryName=src)
    ecr.create_repository(repositoryName=dst)

    layer = b"shared-layer" * 200
    digest = _digest(layer)

    r = requests.post(
        f"{ENDPOINT}/v2/{src}/blobs/uploads/",
        params={"digest": digest},
        data=layer,
    )
    assert r.status_code == 201

    # Mount into dst without re-uploading.
    r = requests.post(
        f"{ENDPOINT}/v2/{dst}/blobs/uploads/",
        params={"mount": digest, "from": src},
    )
    assert r.status_code == 201, r.text
    assert r.headers["Docker-Content-Digest"] == digest

    r = requests.get(f"{ENDPOINT}/v2/{dst}/blobs/{digest}")
    assert r.status_code == 200
    assert r.content == layer


def test_v2_repo_named_after_a_path_does_not_route_to_s3(ecr):
    """Regression for #606: /v2/<name>/blobs/uploads/ used to hit S3."""
    repo = "review-service-" + _uuid_mod.uuid4().hex[:8]
    ecr.create_repository(repositoryName=repo)

    # The exact request `docker push` makes first.
    r = requests.post(f"{ENDPOINT}/v2/{repo}/blobs/uploads/")
    # Before the fix this was a 405 from S3. Now it is a 202 from ECR.
    assert r.status_code == 202
    assert "Location" in r.headers
    assert r.headers.get("Docker-Distribution-API-Version") == "registry/2.0"


def test_v2_catalog_lists_repos(ecr):
    name = "v2-catalog-" + _uuid_mod.uuid4().hex[:8]
    ecr.create_repository(repositoryName=name)
    r = requests.get(f"{ENDPOINT}/v2/_catalog")
    assert r.status_code == 200
    assert name in r.json()["repositories"]


def test_v2_cancel_upload(ecr):
    repo = "v2-cancel-" + _uuid_mod.uuid4().hex[:8]
    ecr.create_repository(repositoryName=repo)
    r = requests.post(f"{ENDPOINT}/v2/{repo}/blobs/uploads/")
    assert r.status_code == 202
    upload_url = r.headers["Location"]
    r = requests.delete(f"{ENDPOINT}{upload_url}")
    assert r.status_code == 204
    # Subsequent PATCH on a cancelled upload must 404.
    r = requests.patch(f"{ENDPOINT}{upload_url}", data=b"after-cancel")
    assert r.status_code == 404


def test_v2_apis_path_still_routes_to_apigwv2():
    """Regression: `/v2/apis/...` is the API Gateway v2 management API, not ECR.
    Earlier iteration of the registry routing hijacked every `/v2/*` path and
    broke 60+ apigwv2 tests in CI."""
    apigwv2 = boto3.client(
        "apigatewayv2",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    api = apigwv2.create_api(Name="v2-routing-regression-" + _uuid_mod.uuid4().hex[:8], ProtocolType="HTTP")
    assert "ApiId" in api
    apigwv2.delete_api(ApiId=api["ApiId"])


def test_v2_email_path_still_routes_to_ses(ses):
    """The SES v2 carve-out (`/v2/email/...`) must NOT be hijacked by ECR."""
    # boto3 SES v2 client uses /v2/email/identities under the hood — verify the
    # client still works after our /v2/ → ECR routing change. We just call it;
    # if the route were broken we'd get a 4xx from the ECR registry handler.
    sesv2 = boto3.client(
        "sesv2",
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    resp = sesv2.list_email_identities()
    assert "EmailIdentities" in resp


# ---------------------------------------------------------------------------
# ARN-parser / tag-scope in-process unit tests. Folded from
# test_ecr_arn_parser.py (drive ecr_svc directly).
# ---------------------------------------------------------------------------


def _body(response):
    return json.loads(response[2].decode("utf-8"))


def _reset_ecr_state():
    for store in (
        ecr_svc._repositories,
        ecr_svc._images,
        ecr_svc._lifecycle_policies,
        ecr_svc._repo_policies,
        ecr_svc._layer_blobs,
        ecr_svc._manifest_blobs,
        ecr_svc._uploads,
    ):
        store.clear()


@pytest.fixture(autouse=True)
def ecr_module_state():
    original_account = get_account_id()
    original_region = get_region()
    _reset_ecr_state()
    set_request_account_id("000000000000")
    set_request_region("us-east-1")
    yield
    _reset_ecr_state()
    set_request_account_id(original_account)
    set_request_region(original_region)


def _create_repository(name):
    response = ecr_svc._create_repository({"repositoryName": name})
    assert response[0] == 200, _body(response)
    return _body(response)["repository"]


def _assert_repository_not_found(response):
    assert response[0] == 400
    assert _body(response)["__type"] == "RepositoryNotFoundException"


def test_ecr_tag_apis_require_request_scope_and_stored_repository_arn_match():
    repo_name = "arn-parser-region-scope"
    stored = _create_repository(repo_name)
    stored_arn = stored["repositoryArn"]
    assert stored_arn == f"arn:aws:ecr:us-east-1:000000000000:repository/{repo_name}"

    seed_response = ecr_svc._tag_resource({
        "resourceArn": stored_arn,
        "tags": [{"Key": "env", "Value": "east"}],
    })
    assert seed_response[0] == 200, _body(seed_response)

    set_request_region("us-west-2")
    fabricated_request_region_arn = f"arn:aws:ecr:us-west-2:000000000000:repository/{repo_name}"
    for resource_arn in (stored_arn, fabricated_request_region_arn):
        _assert_repository_not_found(ecr_svc._tag_resource({
            "resourceArn": resource_arn,
            "tags": [{"Key": "bad", "Value": "tag"}],
        }))
        _assert_repository_not_found(ecr_svc._list_tags_for_resource({"resourceArn": resource_arn}))
        _assert_repository_not_found(ecr_svc._untag_resource({
            "resourceArn": resource_arn,
            "tagKeys": ["env"],
        }))

    set_request_region("us-east-1")
    assert _body(ecr_svc._list_tags_for_resource({"resourceArn": stored_arn}))["tags"] == [
        {"Key": "env", "Value": "east"}
    ]


def test_ecr_child_stores_are_region_scoped_and_reset_clears_all_regions():
    name = "direct-region-scope"
    _create_repository(name)
    ecr_svc._images[name].append({"scope": "east"})
    ecr_svc._lifecycle_policies[name] = "east-lifecycle"
    ecr_svc._repo_policies[name] = "east-policy"
    ecr_svc._layer_blobs[name] = {"sha256:east": b"east-layer"}
    ecr_svc._manifest_blobs[name] = {"sha256:east": b"east-manifest"}
    ecr_svc._uploads[name] = {"east-upload": bytearray(b"east")}

    set_request_region("us-west-2")
    _create_repository(name)
    ecr_svc._images[name].append({"scope": "west"})
    ecr_svc._lifecycle_policies[name] = "west-lifecycle"
    ecr_svc._repo_policies[name] = "west-policy"
    ecr_svc._layer_blobs[name] = {"sha256:west": b"west-layer"}
    ecr_svc._manifest_blobs[name] = {"sha256:west": b"west-manifest"}
    ecr_svc._uploads[name] = {"west-upload": bytearray(b"west")}

    assert ecr_svc._images[name] == [{"scope": "west"}]
    assert ecr_svc._repo_policies[name] == "west-policy"
    set_request_region("us-east-1")
    assert ecr_svc._images[name] == [{"scope": "east"}]
    assert ecr_svc._repo_policies[name] == "east-policy"

    ecr_svc.reset()
    for store in (
        ecr_svc._repositories,
        ecr_svc._images,
        ecr_svc._lifecycle_policies,
        ecr_svc._repo_policies,
        ecr_svc._layer_blobs,
        ecr_svc._manifest_blobs,
        ecr_svc._uploads,
    ):
        assert not store.has_any()


@pytest.mark.parametrize(
    "resource_arn",
    [
        "not-an-arn",
        "arn:aws-cn:ecr:us-east-1:000000000000:repository/arn-parser-scope",
        "arn:aws:ecr:us-west-2:000000000000:repository/arn-parser-scope",
        "arn:aws:ecr:us-east-1:111111111111:repository/arn-parser-scope",
        "arn:aws:sns:us-east-1:000000000000:repository/arn-parser-scope",
        "arn:aws:ecr:us-east-1:000000000000:image/arn-parser-scope",
        "arn:aws:ecr:us-east-1:000000000000:repository/",
    ],
)
def test_ecr_tag_apis_reject_invalid_or_out_of_scope_repository_arns(resource_arn):
    repo_name = "arn-parser-scope"
    valid_arn = _create_repository(repo_name)["repositoryArn"]

    seed_response = ecr_svc._tag_resource({
        "resourceArn": valid_arn,
        "tags": [{"Key": "existing", "Value": "tag"}],
    })
    assert seed_response[0] == 200, _body(seed_response)

    _assert_repository_not_found(ecr_svc._tag_resource({
        "resourceArn": resource_arn,
        "tags": [{"Key": "bad", "Value": "tag"}],
    }))
    _assert_repository_not_found(ecr_svc._list_tags_for_resource({"resourceArn": resource_arn}))
    _assert_repository_not_found(ecr_svc._untag_resource({
        "resourceArn": resource_arn,
        "tagKeys": ["existing"],
    }))

    tags = _body(ecr_svc._list_tags_for_resource({"resourceArn": valid_arn}))["tags"]
    assert tags == [{"Key": "existing", "Value": "tag"}]
