import hashlib
import json
import os
import uuid as _uuid_mod

import boto3
import pytest
import requests
from botocore.exceptions import ClientError
from conftest import inprocess_iam_user, routed_request

from ministack.core.responses import (
    get_account_id,
    get_region,
    request_scope,
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


def _policy_repository(effect, principal):
    """A repository whose policy has one statement, without a Resource, on ecr:ListImages."""
    name = f"rp-{_uuid_mod.uuid4().hex[:8]}"
    _create_repository(name)
    ecr_svc._set_repository_policy({"repositoryName": name, "policyText": json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": effect, "Principal": {"AWS": principal}, "Action": "ecr:ListImages"}]})})
    return name


def _list_images(key, name, field="__type"):
    status, _headers, body = routed_request(
        key, "ecr", "POST", "/", json.dumps({"repositoryName": name}).encode(),
        {"content-type": "application/x-amz-json-1.1",
         "x-amz-target": "AmazonEC2ContainerRegistry_V20150921.ListImages"})
    return status, json.loads(body).get(field)


_ALLOW_ECR = {"Statement": [{"Effect": "Allow", "Action": "ecr:*", "Resource": "*"}]}


def test_repository_policy_naming_the_user_allows_without_identity_policy(auth):
    key, user = inprocess_iam_user()
    assert _list_images(key, _policy_repository("Allow", user)) == (200, None)


def test_repository_policy_naming_the_account_needs_an_identity_policy(auth):
    name = _policy_repository("Allow", "arn:aws:iam::000000000000:root")
    key, _user = inprocess_iam_user()
    assert _list_images(key, name) == (403, "AccessDeniedException")
    key, _user = inprocess_iam_user(_ALLOW_ECR)
    assert _list_images(key, name) == (200, None)


def test_repository_policy_deny_beats_an_identity_allow(auth):
    key, user = inprocess_iam_user(_ALLOW_ECR)
    name = _policy_repository("Deny", user)
    assert _list_images(key, name) == (403, "AccessDeniedException")
    assert _list_images(key, name, "message") == (403, (
        f"User: {user} is not authorized to perform: ecr:ListImages on resource: "
        f"arn:aws:ecr:us-east-1:000000000000:repository/{name} with an explicit deny in a resource-based policy"))


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



def test_ecr_held_image_archive_loads_as_its_repository_uri():
    """ECS loads an image this MiniStack's ECR holds as a `docker load` archive under its repositoryUri
    (layers inflated, the platform picked from an index) instead of asking the real ECR."""
    import gzip
    import hashlib
    import io
    import json as _json
    import tarfile

    from ministack.core.responses import request_scope
    from ministack.services import ecr

    def digest(data):
        return "sha256:" + hashlib.sha256(data).hexdigest()

    layer = b"layer-bytes"
    gz_layer = gzip.compress(layer)
    config = b'{"rootfs": {"type": "layers"}}'
    image = {"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
             "config": {"digest": digest(config)},
             "layers": [{"digest": digest(gz_layer), "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip"}]}
    image_raw = _json.dumps(image).encode()
    index = _json.dumps({"mediaType": "application/vnd.oci.image.index.v1+json", "manifests": [
        {"digest": digest(image_raw), "platform": {"architecture": "arm64", "os": "linux"}}]}).encode()
    with request_scope("111122223333", "us-east-2"):
        ecr._repositories["held"] = {"repositoryName": "held"}
        ecr._layer_blobs["held"] = {digest(config): config, digest(gz_layer): gz_layer}
        ecr._manifest_blobs["held"] = {digest(image_raw): image_raw, digest(index): index}
        ecr._images["held"] = [{"imageDigest": digest(index), "imageTags": ["v1"],
                                "imageManifestMediaType": "application/vnd.oci.image.index.v1+json"}]
    try:
        uri = "111122223333.dkr.ecr.us-east-2.amazonaws.com/held:v1"
        assert ecr.held_image_digest(uri) == digest(index)
        assert ecr.held_image_archive(uri, "amd64") is None  # no amd64 image in the index
        archive, config_digest = ecr.held_image_archive(uri, "arm64")
        assert config_digest == digest(config)
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            manifest = _json.loads(tar.extractfile("manifest.json").read())[0]
            assert manifest["RepoTags"] == [uri]
            assert tar.extractfile(manifest["Layers"][0]).read() == layer
            assert all(".." not in n and not n.startswith("/") for n in tar.getnames())
        assert ecr.held_image_digest("111122223333.dkr.ecr.us-east-2.amazonaws.com/other:v1") is None
        assert ecr.held_image_digest("public.ecr.aws/held:v1") is None
        # A stored config digest that is not sha256:<64 hex> is never written into the archive.
        bad = dict(image, config={"digest": "sha256:../../x"})
        with request_scope("111122223333", "us-east-2"):
            ecr._manifest_blobs["held"][digest(index)] = _json.dumps({"manifests": [
                {"digest": "sha256:" + "0" * 64, "platform": {"architecture": "arm64"}}]}).encode()
            ecr._manifest_blobs["held"]["sha256:" + "0" * 64] = _json.dumps(bad).encode()
        assert ecr.held_image_archive(uri, "arm64") is None
    finally:
        with request_scope("111122223333", "us-east-2"):
            for store in (ecr._repositories, ecr._layer_blobs, ecr._manifest_blobs, ecr._images):
                store.pop("held", None)


# ── Pull-through cache rules and the registry policy ───────

def _ptc_prefix():
    return f"ptc{_uuid_mod.uuid4().hex[:8]}"


def _ptc_call(action, data):
    import asyncio
    headers = {"x-amz-target": f"AmazonEC2ContainerRegistry_V20150921.{action}"}
    status, _headers, body = asyncio.run(ecr_svc.handle_request("POST", "/", headers, json.dumps(data).encode(), {}))
    return status, json.loads(body) if body else {}


@pytest.fixture
def ptc_rules():
    """The in-process registry's rules and registry policy, as they were before the test."""
    created = set(ecr_svc._ptc_rules)
    yield
    for prefix in set(ecr_svc._ptc_rules) - created:
        del ecr_svc._ptc_rules[prefix]
    ecr_svc._registry_policies.pop("policy", None)


def _ptc_secret(value):
    from ministack.services import secretsmanager
    return secretsmanager.create_secret_in_process(f"ecr-pullthroughcache/{_uuid_mod.uuid4().hex[:8]}", value)


def test_ptc_rule_lifecycle(ecr):
    prefix = _ptc_prefix()
    created = ecr.create_pull_through_cache_rule(ecrRepositoryPrefix=prefix + "/", upstreamRegistryUrl="quay.io")
    assert (created["ecrRepositoryPrefix"], created["upstreamRegistryUrl"], created["registryId"]) == (
        prefix, "quay.io", "000000000000")
    assert "upstreamRegistry" not in created
    rules = ecr.describe_pull_through_cache_rules(ecrRepositoryPrefixes=[prefix])["pullThroughCacheRules"]
    assert [r["ecrRepositoryPrefix"] for r in rules] == [prefix]
    with pytest.raises(ClientError) as exc:
        ecr.create_pull_through_cache_rule(ecrRepositoryPrefix=prefix, upstreamRegistryUrl="quay.io")
    assert exc.value.response["Error"]["Message"] == (
        f"A pull-through cache rule with repository prefix '{prefix}' already exists in the registry with id "
        f"'000000000000'")
    assert ecr.delete_pull_through_cache_rule(ecrRepositoryPrefix=prefix)["ecrRepositoryPrefix"] == prefix
    with pytest.raises(ClientError) as exc:
        ecr.delete_pull_through_cache_rule(ecrRepositoryPrefix=prefix)
    assert exc.value.response["Error"]["Code"] == "PullThroughCacheRuleNotFoundException"


def test_ptc_rule_creates_the_service_linked_role(ecr):
    prefix = _ptc_prefix()
    ecr.create_pull_through_cache_rule(ecrRepositoryPrefix=prefix, upstreamRegistryUrl="registry.k8s.io")
    iam = boto3.client("iam", endpoint_url=ENDPOINT, region_name=REGION,
                       aws_access_key_id="test", aws_secret_access_key="test")
    role = iam.get_role(RoleName="AWSServiceRoleForECRPullThroughCache")["Role"]
    assert role["Path"] == "/aws-service-role/pullthroughcache.ecr.amazonaws.com/"
    policy = "arn:aws:iam::aws:policy/aws-service-role/AWSECRPullThroughCache_ServiceRolePolicy"
    attached = iam.list_attached_role_policies(RoleName="AWSServiceRoleForECRPullThroughCache")["AttachedPolicies"]
    assert [p["PolicyArn"] for p in attached] == [policy]
    document = iam.get_policy_version(PolicyArn=policy, VersionId="v1")["PolicyVersion"]["Document"]
    assert [s["Sid"] for s in document["Statement"]] == ["ECR", "SecretsManager"]
    ecr.delete_pull_through_cache_rule(ecrRepositoryPrefix=prefix)


@pytest.mark.parametrize("kwargs, code, message", [
    ({"upstreamRegistryUrl": "registry-1.docker.io"}, "UnsupportedUpstreamRegistryException",
     "The specified upstream registry requires authentication. Specify a valid Secrets Manager ARN containing the "
     "upstream registry credentials and try again."),
    ({"upstreamRegistryUrl": "example.com"}, "UnsupportedUpstreamRegistryException",
     "The upstream registry URL example.com is invalid. Specify a valid upstream registry URL and try again"),
    ({"upstreamRegistryUrl": "000000000000.dkr.ecr.us-east-1.amazonaws.com",
      "customRoleArn": "arn:aws:iam::000000000000:role/r"}, "UnsupportedUpstreamRegistryException",
     "Upstream registry URL cannot be the same as the cache registry."),
    ({"upstreamRegistryUrl": "000000000000.dkr.ecr.us-west-2.amazonaws.com",
      "customRoleArn": "arn:aws:iam::000000000000:role/r"}, "InvalidParameterException",
     "Invalid input: Upstream registry URL and provided credential type do not match. Please verify input and try "
     "again."),
    ({"upstreamRegistryUrl": "111111111111.dkr.ecr.us-east-1.amazonaws.com"}, "InvalidParameterException",
     "Invalid input: The specified upstream registry requires authentication. Specify a valid IAM role ARN and "
     "try again."),
    ({"upstreamRegistryUrl": "quay.io", "customRoleArn": "arn:aws:iam::111111111111:role/r"},
     "AccessDeniedException", "Cross-account pass role is not allowed."),
    ({"upstreamRegistryUrl": "registry-1.docker.io",
      "credentialArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:ecr-pullthroughcache/none-AbCdEf"},
     "SecretNotFoundException",
     "The ARN of the secret specified in the pull through cache rule was not found. Update the pull through cache "
     "rule with a valid secret ARN and try again."),
])
def test_ptc_create_refusals(ecr, kwargs, code, message):
    with pytest.raises(ClientError) as exc:
        ecr.create_pull_through_cache_rule(ecrRepositoryPrefix=_ptc_prefix(), **kwargs)
    assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (code, message)


@pytest.mark.parametrize("body, message", [
    (b"", None),
    (b"null", "Request payload does not contain the input model"),
    (b"not json", "Unexpected Character in input string at byte 1"),
    (b'{"ecrRepositoryPrefix": "', "Unexpected end of payload"),
    (b'{"ecrRepositoryPrefix":"aa","ecrRepositoryPrefix":"bb","upstreamRegistryUrl":"quay.io"}',
     "Duplicate key found in payload"),
])
def test_ptc_payload_refusals(body, message):
    import asyncio
    status, _headers, raw = asyncio.run(ecr_svc.handle_request(
        "POST", "/", {"x-amz-target": "AmazonEC2ContainerRegistry_V20150921.CreatePullThroughCacheRule"}, body, {}))
    answer = json.loads(raw)
    assert (status, answer["__type"], answer.get("Message")) == (400, "SerializationException", message)


def test_ptc_describe_pages_in_creation_order(ptc_rules):
    prefixes = [_ptc_prefix() for _ in range(3)]
    for prefix in prefixes:
        _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    seen, token = [], None
    while True:
        _status, page = _ptc_call("DescribePullThroughCacheRules",
                                  {"maxResults": 1, **({"nextToken": token} if token else {})})
        seen += [r["ecrRepositoryPrefix"] for r in page["pullThroughCacheRules"]]
        token = page.get("nextToken")
        if not token:
            break
    assert [p for p in seen if p in prefixes] == prefixes


def test_ptc_update_switches_the_secret(ptc_rules):
    prefix = _ptc_prefix()
    first, second = (_ptc_secret(json.dumps({"username": "u", "accessToken": t})) for t in ("a", "b"))
    status, _ = _ptc_call("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "registry-1.docker.io", "credentialArn": first})
    assert status == 200
    status, body = _ptc_call("UpdatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "credentialArn": second})
    assert (status, body["credentialArn"]) == (200, second)
    _status, body = _ptc_call("DescribePullThroughCacheRules", {"ecrRepositoryPrefixes": [prefix]})
    rule = body["pullThroughCacheRules"][0]
    assert (rule["credentialArn"], rule["upstreamRegistry"]) == (second, "docker-hub")


def test_ptc_validate_checks_the_secret(ptc_rules):
    prefix = _ptc_prefix()
    arn = _ptc_secret(json.dumps({"username": "alice", "accessToken": "token"}))
    _ptc_call("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "registry-1.docker.io", "credentialArn": arn})
    _status, body = _ptc_call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix})
    assert (body["isValid"], body["failure"], body["credentialArn"]) == (True, "", arn)
    from ministack.services import secretsmanager
    secretsmanager._secrets[secretsmanager.find_secret(arn)["Name"]]["Versions"] = {
        "v": {"SecretString": "not json", "Stages": ["AWSCURRENT"]}}
    _status, body = _ptc_call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix})
    assert (body["isValid"], json.loads(body["failure"]), body["credentialArn"]) == (False, {
        "failureCode": "UNABLE_TO_PARSE_SECRET",
        "failureReason": "Unable to parse secret value. Secret must contain 'username' and 'accessToken' keys."}, arn)
    secretsmanager._secrets.pop(secretsmanager.find_secret(arn)["Name"])
    _status, body = _ptc_call("ValidatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix})
    assert body == {"ecrRepositoryPrefix": prefix, "registryId": "000000000000", "isValid": False,
                    "failure": '{"failureCode":"UNABLE_TO_ACCESS_SECRET","failureReason":"The secret is unable to be '
                               'accessed. Verify the resource permissions for the secret and try again."}'}


def test_ptc_validate_checks_the_role_trust():
    from ministack.services import iam
    arn, trusted = "arn:aws:iam::000000000000:role/r", {"Statement": [{"Effect": "Allow", "Principal": {
        "Service": "pullthroughcache.ecr.amazonaws.com"}, "Action": "sts:AssumeRole"}]}
    iam._roles["r"] = {"Arn": arn, "AssumeRolePolicyDocument": json.dumps(trusted)}
    try:
        assert iam.role_trusts_service(arn, "pullthroughcache.ecr.amazonaws.com")
        assert ecr_svc._ptc_validate_failure({"customRoleArn": arn}) is None
        trusted["Statement"][0]["Effect"] = "Deny"
        iam._roles["r"]["AssumeRolePolicyDocument"] = json.dumps(trusted)
        assert ecr_svc._ptc_validate_failure({"customRoleArn": arn}) == (
            "UNABLE_TO_ASSUME_ROLE", "Unable to access the IAM role. Please verify trust policy and try again.")
        assert ecr_svc._ptc_validate_failure({"customRoleArn": "arn:aws:iam::000000000000:role/none"})[0] == (
            "UNABLE_TO_ASSUME_ROLE")
    finally:
        iam._roles.pop("r", None)


def test_ptc_sends_no_credentials():
    import ast
    import inspect
    names = {n.attr for n in ast.walk(ast.parse(inspect.getsource(ecr_svc))) if isinstance(n, ast.Attribute)}
    assert "login" not in names
    banned = {"urllib", "http", "socket", "requests", "httpx", "ssl", "aiohttp"}
    tree = ast.parse(inspect.getsource(ecr_svc))
    imported = {(a.name if isinstance(n, ast.Import) else n.module or "").split(".")[0]
                for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom)) for a in n.names}
    assert not imported & banned


def test_registry_policy_lifecycle(ecr):
    text = json.dumps({"Version": "2012-10-17", "Statement": [{
        "Sid": "pull", "Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::111111111111:root"},
        "Action": ["ecr:CreateRepository", "ecr:BatchImportUpstreamImage"], "Resource": "*"}]})
    assert ecr.put_registry_policy(policyText=text)["policyText"] == text
    assert ecr.get_registry_policy()["policyText"] == text
    assert ecr.delete_registry_policy()["policyText"] == text
    with pytest.raises(ClientError) as exc:
        ecr.get_registry_policy()
    assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
        "RegistryPolicyNotFoundException", "Registry policy does not exist in the registry with id '000000000000'")
    with pytest.raises(ClientError) as exc:
        ecr.put_registry_policy(policyText='{"Version": "2012-10-17", "Statement": []}')
    assert exc.value.response["Error"]["Message"] == (
        "Invalid parameter at 'PolicyText' failed to satisfy constraint: 'Invalid registry policy provided'")


def test_ptc_rules_survive_persistence(ptc_rules):
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    state = ecr_svc.get_state()
    assert state["pull_through_cache_rules"].get_scoped("000000000000", "us-east-1", prefix)[
        "ecrRepositoryPrefix"] == prefix


# ── Pull-through cache: the first-pull fill ─────────────────

_CACHE = "000000000000.dkr.ecr.us-east-1.amazonaws.com"
_PTC_ROLE_ARN = ("arn:aws:iam::000000000000:role/aws-service-role/pullthroughcache.ecr.amazonaws.com/"
                 "AWSServiceRoleForECRPullThroughCache")


def _docker_save(config, layer):
    import io
    import tarfile
    out = io.BytesIO()
    entries = (("cfg.json", config), ("l1/layer.tar", layer),
               ("manifest.json", json.dumps([{"Config": "cfg.json", "RepoTags": [], "Layers": ["l1/layer.tar"]}]).encode()))
    with tarfile.open(fileobj=out, mode="w") as tar:
        for name, data in entries:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return out.getvalue()


class _PtcDaemon:
    """A host daemon holding the *local* refs, able to pull the *pullable* ones; records every call."""

    def __init__(self, local=(), pullable=()):
        self.local, self.pullable, self.calls = set(local), set(pullable), []
        self.images = self
        self.config = json.dumps({"architecture": "amd64", "os": "linux"}).encode()
        self.archive = _docker_save(self.config, b"layer-bytes")

    def _image(self):
        from types import SimpleNamespace
        return SimpleNamespace(save=lambda: iter([self.archive]))

    def get(self, ref):
        self.calls.append(("get", ref))
        if ref not in self.local:
            raise RuntimeError("No such image")
        return self._image()

    def pull(self, ref, **kwargs):
        self.calls.append(("pull", ref, kwargs))
        if ref not in self.pullable:
            raise RuntimeError("pull access denied")
        return self._image()


def _ptc_repo(image):
    rest = image.split("/", 1)[1]
    return rest.rsplit(":", 1)[0] if ":" in rest.rsplit("/", 1)[-1] else rest


def _drop_repository(name):
    if name in ecr_svc._repositories:
        ecr_svc._delete_repository({"repositoryName": name, "force": True})
    for store in (ecr_svc._layer_blobs, ecr_svc._manifest_blobs):
        store.pop(name, None)


@pytest.mark.parametrize("rule, image, upstream", [
    ({"upstreamRegistryUrl": "registry-1.docker.io"}, "{p}/permitio/pdp-v2:0.9.9",
     "registry-1.docker.io/permitio/pdp-v2:0.9.9"),
    ({"upstreamRegistryUrl": "public.ecr.aws"}, "{p}/docker/library/alpine", "public.ecr.aws/docker/library/alpine:latest"),
    ({"upstreamRegistryUrl": "public.ecr.aws", "upstreamRepositoryPrefix": "mirror"}, "{p}/a:1",
     "public.ecr.aws/mirror/a:1"),
    ({"upstreamRegistryUrl": "quay.io"}, "{p}/deep/x:1", "quay.io/deep/x:1"),
])
def test_ptc_fill_takes_the_host_image_like_ec2(ptc_rules, rule, image, upstream):
    prefix = _ptc_prefix()
    body = {"ecrRepositoryPrefix": prefix, **rule}
    if rule["upstreamRegistryUrl"] == "registry-1.docker.io":
        body["credentialArn"] = _ptc_secret(json.dumps({"username": "u", "accessToken": "t"}))
    assert _ptc_call("CreatePullThroughCacheRule", body)[0] == 200
    image = f"{_CACHE}/{image.format(p=prefix)}"
    daemon = _PtcDaemon(local={upstream})
    try:
        digest = ecr_svc.fill_pull_through(image, docker_client=daemon)
        assert daemon.calls == [("get", upstream)]
        repo = ecr_svc._repositories[_ptc_repo(image)]
        assert (repo["imageTagMutability"], repo["imageScanningConfiguration"], repo["encryptionConfiguration"]) == (
            "MUTABLE", {"scanOnPush": False}, {"encryptionType": "AES256"})
        assert ecr_svc.held_image_digest(image) == digest
        _archive, config_digest = ecr_svc.held_image_archive(image, "amd64")
        assert config_digest == "sha256:" + hashlib.sha256(daemon.config).hexdigest()
        assert ecr_svc.fill_pull_through(image, docker_client=daemon) is None
    finally:
        _drop_repository(_ptc_repo(image))


def test_ptc_fill_pulls_through_the_daemon_without_the_secret(ptc_rules):
    prefix = _ptc_prefix()
    arn = _ptc_secret(json.dumps({"username": "alice", "accessToken": "token"}))
    _ptc_call("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "registry-1.docker.io", "credentialArn": arn})
    image, upstream = f"{_CACHE}/{prefix}/library/alpine:3", "registry-1.docker.io/library/alpine:3"
    daemon = _PtcDaemon(pullable={upstream})
    try:
        assert ecr_svc.fill_pull_through(image, docker_client=daemon)
        assert daemon.calls == [("get", upstream), ("pull", upstream, {})]
    finally:
        _drop_repository(_ptc_repo(image))


def test_ptc_fill_unavailable_creates_no_repository(ptc_rules, monkeypatch, caplog):
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    image = f"{_CACHE}/{prefix}/org/app:1"
    with pytest.raises(ecr_svc.PullThroughMiss) as miss:
        ecr_svc.fill_pull_through(image, docker_client=_PtcDaemon())
    assert (miss.value.kind, str(miss.value)) == ("unavailable", "Unable to reach the upstream registry.")
    monkeypatch.setattr(ecr_svc, "_ptc_docker", lambda: None)
    with pytest.raises(ecr_svc.PullThroughMiss):
        ecr_svc.fill_pull_through(image)
    assert f"{prefix}/org/app" not in ecr_svc._repositories
    assert "mount the Docker socket" in caplog.text


def test_ptc_fill_ignores_what_no_rule_covers(ptc_rules):
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    daemon = _PtcDaemon(local={"quay.io/org/app:1"})
    for image in (f"{_CACHE}/other{prefix}/org/app:1", f"{_CACHE}/{prefix}/org/app@sha256:" + "a" * 64,
                  "quay.io/org/app:1"):
        assert ecr_svc.fill_pull_through(image, docker_client=daemon) is None
    assert daemon.calls == []


def test_ptc_other_account_needs_the_registry_policy_under_auth(ptc_rules, monkeypatch):
    import ministack.app as app
    monkeypatch.setattr(app, "AUTH", True)
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    image, puller = f"{_CACHE}/{prefix}/org/app:1", "arn:aws:iam::111111111111:root"
    daemon = _PtcDaemon(local={"quay.io/org/app:1"})
    try:
        assert ecr_svc.fill_pull_through(image, puller, daemon) is None
        statement = {"Effect": "Allow", "Principal": {"AWS": puller}, "Action": ["ecr:BatchImportUpstreamImage"],
                     "Resource": f"arn:aws:ecr:us-east-1:000000000000:repository/{prefix}/*"}
        _ptc_call("PutRegistryPolicy", {"policyText": json.dumps({"Version": "2012-10-17", "Statement": [statement]})})
        assert ecr_svc.fill_pull_through(image, puller, daemon) is None
        statement["Action"].append("ecr:CreateRepository")
        _ptc_call("PutRegistryPolicy", {"policyText": json.dumps({"Version": "2012-10-17", "Statement": [statement]})})
        assert ecr_svc.fill_pull_through(image, puller, daemon)
    finally:
        _drop_repository(f"{prefix}/org/app")


def test_ptc_service_linked_role_reads_the_secret_under_auth(ptc_rules, monkeypatch):
    import ministack.app as app
    from ministack.services import kms, secretsmanager
    prefix = _ptc_prefix()
    arn = _ptc_secret(json.dumps({"username": "u", "accessToken": "t"}))
    _ptc_call("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "registry-1.docker.io", "credentialArn": arn})
    image = f"{_CACHE}/{prefix}/library/alpine:3"
    daemon = _PtcDaemon(local={"registry-1.docker.io/library/alpine:3"})
    monkeypatch.setattr(app, "AUTH", True)
    deny = {"Version": "2012-10-17", "Statement": [{"Effect": "Deny", "Principal": {"AWS": _PTC_ROLE_ARN},
                                                     "Action": "secretsmanager:GetSecretValue", "Resource": "*"}]}
    secretsmanager._resource_policies[arn] = json.dumps(deny)
    key_id = None
    try:
        assert ecr_svc._ptc_service_role_denied(arn)
        with pytest.raises(ecr_svc.PullThroughMiss) as miss:
            ecr_svc.fill_pull_through(image, docker_client=daemon)
        assert miss.value.kind == "secret"
        secretsmanager._resource_policies.pop(arn)
        assert not ecr_svc._ptc_service_role_denied(arn)
        key_id = json.loads(kms._create_key({})[2])["KeyMetadata"]["KeyId"]
        secretsmanager._secrets[secretsmanager.find_secret(arn)["Name"]]["KmsKeyId"] = key_id
        assert ecr_svc._ptc_service_role_denied(arn)
        rec = kms._keys[key_id]
        rec["Policy"] = json.dumps({"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::000000000000:root"}, "Action": "kms:*",
             "Resource": "*"},
            {"Effect": "Allow", "Principal": {"AWS": _PTC_ROLE_ARN}, "Action": "kms:Decrypt", "Resource": "*"}]})
        assert not ecr_svc._ptc_service_role_denied(arn)
        assert ecr_svc.fill_pull_through(image, docker_client=daemon)
        monkeypatch.setattr(app, "AUTH", False)
        rec["Policy"] = ""
        assert not ecr_svc._ptc_service_role_denied(arn)
    finally:
        secretsmanager._resource_policies.pop(arn, None)
        if key_id:
            kms._keys.pop(key_id, None)
        _drop_repository(f"{prefix}/library/alpine")


def test_ptc_fill_copies_an_ecr_upstream_in_process(ptc_rules):
    from ministack.services import iam
    prefix, source = _ptc_prefix(), f"src{_uuid_mod.uuid4().hex[:8]}/app"
    role = "arn:aws:iam::000000000000:role/ptc-copy"
    iam._roles["ptc-copy"] = {"Arn": role, "AssumeRolePolicyDocument": json.dumps({"Statement": [{
        "Effect": "Allow", "Principal": {"Service": "pullthroughcache.ecr.amazonaws.com"}, "Action": "sts:AssumeRole"}]})}
    config = b'{"architecture":"amd64","os":"linux"}'
    config_digest = "sha256:" + hashlib.sha256(config).hexdigest()
    manifest = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
                           "config": {"mediaType": "application/vnd.docker.container.image.v1+json",
                                      "size": len(config), "digest": config_digest}, "layers": []}).encode()
    with request_scope("000000000002", "us-east-1"):
        _create_repository(source)
        ecr_svc._layer_blobs.setdefault(source, {})[config_digest] = config
        ecr_svc._v2_put_manifest(source, "v1", {}, manifest)
    _ptc_call("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "000000000002.dkr.ecr.us-east-1.amazonaws.com",
        "customRoleArn": role})
    image = f"{_CACHE}/{prefix}/{source}:v1"
    try:
        digest = ecr_svc.fill_pull_through(image)
        assert digest == "sha256:" + hashlib.sha256(manifest).hexdigest()
        assert ecr_svc._layer_blobs[f"{prefix}/{source}"] == {config_digest: config}
        with pytest.raises(ecr_svc.PullThroughMiss) as miss:
            ecr_svc.fill_pull_through(f"{_CACHE}/{prefix}/nope:v1")
        assert (miss.value.kind, str(miss.value)) == (
            "repository", f"The repository with name '{prefix}/nope' does not exist in the registry with id "
                          f"'000000000000'")
    finally:
        iam._roles.pop("ptc-copy", None)
        _drop_repository(f"{prefix}/{source}")
        with request_scope("000000000002", "us-east-1"):
            _drop_repository(source)


def test_ptc_fill_copies_a_same_account_ecr_upstream_without_a_role(ptc_rules):
    prefix, source = _ptc_prefix(), f"src{_uuid_mod.uuid4().hex[:8]}/app"
    config = b'{"architecture":"arm64","os":"linux"}'
    config_digest = "sha256:" + hashlib.sha256(config).hexdigest()
    manifest = json.dumps({"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
                           "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                                      "size": len(config), "digest": config_digest}, "layers": []}).encode()
    with request_scope("000000000000", "us-west-2"):
        _create_repository(source)
        ecr_svc._layer_blobs.setdefault(source, {})[config_digest] = config
        ecr_svc._v2_put_manifest(source, "edge", {}, manifest)
    status, created = _ptc_call("CreatePullThroughCacheRule", {
        "ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "000000000000.dkr.ecr.us-west-2.amazonaws.com",
        "upstreamRepositoryPrefix": source.split("/")[0]})
    try:
        assert (status, created["ecrRepositoryPrefix"]) == (200, prefix)
        digest = ecr_svc.fill_pull_through(f"{_CACHE}/{prefix}/app:edge")
        assert digest == "sha256:" + hashlib.sha256(manifest).hexdigest()
        assert ecr_svc._layer_blobs[f"{prefix}/app"] == {config_digest: config}
    finally:
        _drop_repository(f"{prefix}/app")
        with request_scope("000000000000", "us-west-2"):
            _drop_repository(source)


def test_ptc_batch_get_image_does_not_fill(ptc_rules, monkeypatch):
    """The BatchGetImage API never fills the cache; AWS answers RepositoryNotFound (capture 2026-11-18)."""
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    name = f"{prefix}/org/app"
    daemon = _PtcDaemon(local={"quay.io/org/app:1"})
    monkeypatch.setattr(ecr_svc, "_ptc_docker", lambda: daemon)
    status, body = _ptc_call("BatchGetImage", {"repositoryName": name, "imageIds": [{"imageTag": "1"}]})
    assert (status, body["__type"], body["message"]) == (400, "RepositoryNotFoundException",
        f"The repository with name '{name}' does not exist in the registry with id '000000000000'")
    assert name not in ecr_svc._repositories and not daemon.calls


def test_ptc_registry_api_fills_or_answers_manifest_unknown(ptc_rules, monkeypatch):
    import asyncio
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    name = f"{prefix}/org/app"
    monkeypatch.setattr(ecr_svc, "_ptc_docker", lambda: _PtcDaemon(local={"quay.io/org/app:1"}))

    def get(ref):
        return asyncio.run(ecr_svc.handle_registry_request("GET", f"/v2/{name}/manifests/{ref}", {}, b"", {}))
    try:
        status, _headers, body = get("2")
        assert (status, json.loads(body)["errors"][0]["code"]) == (404, "MANIFEST_UNKNOWN")
        assert name not in ecr_svc._repositories
        status, headers, _body = get("1")
        assert status == 200 and headers["Docker-Content-Digest"] == ecr_svc.held_image_digest(f"{_CACHE}/{name}:1")
    finally:
        _drop_repository(name)


# ── Repository creation templates ─────────

def test_repository_creation_template_lifecycle(ecr):
    prefix = f"rct{_uuid_mod.uuid4().hex[:8]}"
    created = ecr.create_repository_creation_template(
        prefix=prefix, appliedFor=["PULL_THROUGH_CACHE"], description="cache", imageTagMutability="IMMUTABLE",
        resourceTags=[{"Key": "team", "Value": "a"}])
    try:
        template = created["repositoryCreationTemplate"]
        assert created["registryId"] == "000000000000"
        assert (template["prefix"], template["appliedFor"], template["imageTagMutability"], template["resourceTags"]) \
            == (prefix, ["PULL_THROUGH_CACHE"], "IMMUTABLE", [{"Key": "team", "Value": "a"}])
        with pytest.raises(ClientError) as exc:
            ecr.create_repository_creation_template(prefix=prefix, appliedFor=["PULL_THROUGH_CACHE"])
        assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
            "TemplateAlreadyExistsException",
            f"A repository creation template with prefix '{prefix}' already exists in the registry with id '000000000000'")

        described = ecr.describe_repository_creation_templates(prefixes=[prefix])["repositoryCreationTemplates"]
        assert [t["prefix"] for t in described] == [prefix]

        updated = ecr.update_repository_creation_template(prefix=prefix, imageTagMutability="MUTABLE",
                                                          appliedFor=["PULL_THROUGH_CACHE", "REPLICATION"])
        assert (updated["repositoryCreationTemplate"]["imageTagMutability"],
                updated["repositoryCreationTemplate"]["appliedFor"],
                updated["repositoryCreationTemplate"]["description"]) == (
            "MUTABLE", ["PULL_THROUGH_CACHE", "REPLICATION"], "cache")
    finally:
        deleted = ecr.delete_repository_creation_template(prefix=prefix)
    assert deleted["repositoryCreationTemplate"]["prefix"] == prefix
    for call in (lambda: ecr.delete_repository_creation_template(prefix=prefix),
                 lambda: ecr.update_repository_creation_template(prefix=prefix, imageTagMutability="MUTABLE")):
        with pytest.raises(ClientError) as exc:
            call()
        assert (exc.value.response["Error"]["Code"], exc.value.response["Error"]["Message"]) == (
            "TemplateNotFoundException",
            f"A repository creation template with prefix '{prefix}' is not found in the registry with id '000000000000'")
    assert ecr.describe_repository_creation_templates(prefixes=[prefix])["repositoryCreationTemplates"] == []


@pytest.mark.parametrize("body", [
    {"prefix": "rctnoapplied"},
    {"prefix": "rctbad", "appliedFor": ["SOMETIMES"]},
    {"prefix": "Bad_Prefix", "appliedFor": ["PULL_THROUGH_CACHE"]},
    {"prefix": "rctbad", "appliedFor": ["PULL_THROUGH_CACHE"], "imageTagMutability": "SOMETIMES"},
    {"prefix": "rctbad", "appliedFor": ["PULL_THROUGH_CACHE"], "encryptionConfiguration": {"encryptionType": "XOR"}},
    {"prefix": "rctbad", "appliedFor": ["PULL_THROUGH_CACHE"], "description": "d" * 257},
])
def test_repository_creation_template_refuses_invalid_input(body):
    status, answer = _ptc_call("CreateRepositoryCreationTemplate", body)
    assert (status, answer["__type"]) == (400, "InvalidParameterException")
    assert body["prefix"] not in ecr_svc._creation_templates


@pytest.fixture
def creation_templates():
    created = set(ecr_svc._creation_templates)
    yield
    for prefix in set(ecr_svc._creation_templates) - created:
        del ecr_svc._creation_templates[prefix]


def test_ptc_fill_creates_the_repository_from_the_matching_template(ptc_rules, creation_templates):
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    policy = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": "*",
                                                                 "Action": "ecr:BatchGetImage"}]})
    lifecycle = json.dumps({"rules": [{"rulePriority": 1, "selection": {
        "tagStatus": "any", "countType": "imageCountMoreThan", "countNumber": 5}, "action": {"type": "expire"}}]})
    kms = {"encryptionType": "KMS", "kmsKey": "arn:aws:kms:us-east-1:000000000000:key/1234abcd-12ab"}
    assert _ptc_call("CreateRepositoryCreationTemplate", {
        "prefix": prefix, "appliedFor": ["PULL_THROUGH_CACHE"], "imageTagMutability": "IMMUTABLE",
        "encryptionConfiguration": kms, "resourceTags": [{"Key": "cache", "Value": "yes"}],
        "repositoryPolicy": policy, "lifecyclePolicy": lifecycle})[0] == 200
    assert _ptc_call("CreateRepositoryCreationTemplate", {
        "prefix": f"{prefix}/org/push", "appliedFor": ["CREATE_ON_PUSH"], "imageTagMutability": "MUTABLE"})[0] == 200
    assert _ptc_call("CreateRepositoryCreationTemplate", {
        "prefix": f"{prefix}/other", "appliedFor": ["PULL_THROUGH_CACHE"]})[0] == 200
    image = f"{_CACHE}/{prefix}/org/push/app:1"
    try:
        assert ecr_svc.fill_pull_through(image, docker_client=_PtcDaemon(local={f"quay.io/org/push/app:1"}))
        name = _ptc_repo(image)
        repo = ecr_svc._repositories[name]
        assert (repo["imageTagMutability"], repo["encryptionConfiguration"], repo["tags"]) == (
            "IMMUTABLE", kms, [{"Key": "cache", "Value": "yes"}])
        assert (ecr_svc._repo_policies[name], ecr_svc._lifecycle_policies[name]) == (policy, lifecycle)
    finally:
        _drop_repository(_ptc_repo(image))


def test_ptc_fill_without_a_matching_template_takes_the_defaults(ptc_rules, creation_templates):
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    _ptc_call("CreateRepositoryCreationTemplate", {
        "prefix": prefix, "appliedFor": ["REPLICATION"], "imageTagMutability": "IMMUTABLE"})
    image = f"{_CACHE}/{prefix}/org/app:1"
    try:
        assert ecr_svc.fill_pull_through(image, docker_client=_PtcDaemon(local={"quay.io/org/app:1"}))
        name = _ptc_repo(image)
        repo = ecr_svc._repositories[name]
        assert (repo["imageTagMutability"], repo["encryptionConfiguration"], repo["tags"]) == (
            "MUTABLE", {"encryptionType": "AES256"}, [])
        assert name not in ecr_svc._repo_policies and name not in ecr_svc._lifecycle_policies
    finally:
        _drop_repository(_ptc_repo(image))


# ── Pull-through cache: an OCI-layout export keeps the upstream's bytes ─────────

def _oci_blob(data):
    return "sha256:" + hashlib.sha256(data).hexdigest(), data


def _oci_image(arch, layer_bytes):
    import gzip
    cfg_d, cfg = _oci_blob(json.dumps({"architecture": arch, "os": "linux"}).encode())
    lay_d, lay = _oci_blob(gzip.compress(layer_bytes, mtime=0))
    man_d, man = _oci_blob(json.dumps({
        "schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"mediaType": "application/vnd.oci.image.config.v1+json", "digest": cfg_d, "size": len(cfg)},
        "layers": [{"mediaType": "application/vnd.oci.image.layer.v1.tar+gzip", "digest": lay_d,
                    "size": len(lay)}]}).encode())
    return man_d, man, {cfg_d: cfg, lay_d: lay}


def _oci_save(top_digest, top_bytes, blobs):
    import io
    import tarfile
    out = io.BytesIO()
    entries = [("oci-layout", b'{"imageLayoutVersion":"1.0.0"}'),
               ("index.json", json.dumps({"schemaVersion": 2, "manifests": [
                   {"mediaType": "application/vnd.oci.image.index.v1+json", "digest": top_digest,
                    "size": len(top_bytes)}]}).encode())]
    entries += [(f"blobs/sha256/{d.split(':')[1]}", data) for d, data in {top_digest: top_bytes, **blobs}.items()]
    with tarfile.open(fileobj=out, mode="w") as tar:
        for name, data in entries:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return out.getvalue()


class _OciDaemon(_PtcDaemon):
    """A daemon on the containerd image store: it exports the upstream index and the platforms it holds."""

    def __init__(self, upstream):
        super().__init__(local={f"{upstream}:1"})
        self.upstream = upstream
        self.amd_d, amd, self.amd_blobs = _oci_image("amd64", b"amd-layer")
        self.arm_d, arm, self.arm_blobs = _oci_image("arm64", b"arm-layer")
        self.index_d, self.index = _oci_blob(json.dumps({
            "schemaVersion": 2, "mediaType": "application/vnd.oci.image.index.v1+json", "manifests": [
                {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": self.amd_d, "size": len(amd),
                 "platform": {"architecture": "amd64", "os": "linux"}},
                {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": self.arm_d, "size": len(arm),
                 "platform": {"architecture": "arm64", "os": "linux"}}]}).encode())
        self.archive = _oci_save(self.index_d, self.index, {self.amd_d: amd, **self.amd_blobs})
        self.arm_archive = _oci_save(self.arm_d, arm, self.arm_blobs)
        self.pullable = {f"{upstream}@{self.arm_d}"}

    def pull(self, ref, **kwargs):
        image = super().pull(ref, **kwargs)
        from types import SimpleNamespace
        return SimpleNamespace(save=lambda: iter([self.arm_archive])) if ref.endswith(self.arm_d) else image

    def load(self, archive):
        self.loaded = getattr(self, "loaded", []) + [archive]
        return []


def test_ptc_fill_keeps_the_upstream_index_and_fetches_other_platforms_by_digest(ptc_rules):
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    image = f"{_CACHE}/{prefix}/org/app:1"
    daemon = _OciDaemon("quay.io/org/app")
    name = _ptc_repo(image)
    try:
        assert ecr_svc.fill_pull_through(image, docker_client=daemon) == daemon.index_d
        assert ecr_svc._manifest_blobs[name][daemon.index_d] == daemon.index
        assert ecr_svc._images[name][0]["imageManifestMediaType"] == "application/vnd.oci.image.index.v1+json"
        assert ecr_svc.held_image_archive(image, "amd64")[1] == next(iter(daemon.amd_blobs))
        assert ecr_svc.held_image_archive(image, "arm64") is None

        assert ecr_svc.load_held_image(daemon, image, "arm64") == image
        assert daemon.loaded
        assert daemon.calls[-1][:2] == ("pull", f"quay.io/org/app@{daemon.arm_d}")
        assert ecr_svc.held_image_archive(image, "arm64")[1] == next(iter(daemon.arm_blobs))
    finally:
        _drop_repository(name)


def test_ptc_child_fetch_by_digest_answers_batch_get_image(ptc_rules):
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    image = f"{_CACHE}/{prefix}/org/app:1"
    daemon = _OciDaemon("quay.io/org/app")
    name = _ptc_repo(image)
    try:
        ecr_svc.fill_pull_through(image, docker_client=daemon)
        unlisted = "sha256:" + "0" * 64
        assert ecr_svc._ptc_fill_child(name, unlisted, None, daemon) is False
        assert ecr_svc._ptc_fill_child(name, daemon.arm_d, None, daemon) is True
        assert ecr_svc._manifest_blobs[name][daemon.arm_d]
        assert ecr_svc._ptc_fill_child(name, daemon.arm_d, None, daemon) is False
    finally:
        _drop_repository(name)


def test_ptc_child_fetch_from_a_classic_store_is_a_miss(ptc_rules):
    prefix = _ptc_prefix()
    _ptc_call("CreatePullThroughCacheRule", {"ecrRepositoryPrefix": prefix, "upstreamRegistryUrl": "quay.io"})
    image = f"{_CACHE}/{prefix}/org/app:1"
    daemon = _OciDaemon("quay.io/org/app")
    name = _ptc_repo(image)
    try:
        ecr_svc.fill_pull_through(image, docker_client=daemon)
        daemon.arm_archive = _docker_save(b'{"architecture":"arm64","os":"linux"}', b"arm-layer")
        with pytest.raises(ecr_svc.PullThroughMiss):
            ecr_svc._ptc_fill_child(name, daemon.arm_d, None, daemon)
        assert daemon.arm_d not in ecr_svc._manifest_blobs[name]
    finally:
        _drop_repository(name)



# ── Captured error wording and BatchGetImage request errors ─────────

def test_missing_repository_and_policy_errors_name_the_registry(ecr):
    name = f"wording-{_uuid_mod.uuid4().hex[:8]}"
    with pytest.raises(ClientError) as exc:
        ecr.batch_get_image(repositoryName=name, imageIds=[{"imageTag": "1"}])
    assert exc.value.response["Error"]["Message"] == \
        f"The repository with name '{name}' does not exist in the registry with id '000000000000'"
    ecr.create_repository(repositoryName=name)
    try:
        with pytest.raises(ClientError) as exc:
            ecr.create_repository(repositoryName=name)
        assert exc.value.response["Error"]["Message"] == \
            f"The repository with name '{name}' already exists in the registry with id '000000000000'"
        for call, kind in ((ecr.get_repository_policy, "Repository"), (ecr.get_lifecycle_policy, "Lifecycle")):
            with pytest.raises(ClientError) as exc:
                call(repositoryName=name)
            assert exc.value.response["Error"]["Message"] == (
                f"{kind} policy does not exist for the repository with name '{name}' "
                "in the registry with id '000000000000'")
        ecr.put_image(repositoryName=name, imageTag="v1", imageManifest=json.dumps({
            "schemaVersion": 2, "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
            "config": {"mediaType": "application/vnd.docker.container.image.v1+json", "size": 2,
                       "digest": "sha256:" + "a" * 64}, "layers": []}))
        with pytest.raises(ClientError) as exc:
            ecr.delete_repository(repositoryName=name)
        assert exc.value.response["Error"]["Message"] == (
            f"The repository with name '{name}' in registry with id '000000000000' "
            "cannot be deleted because it still contains images")
    finally:
        ecr.delete_repository(repositoryName=name, force=True)


@pytest.mark.parametrize("body, code, message", [
    ({"repositoryName": "Bad_Name", "imageIds": [{"imageTag": "1"}]}, "InvalidParameterException",
     "Invalid parameter at 'repositoryName' failed to satisfy constraint: 'must satisfy regular expression "
     "'[a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*(/[a-z0-9]+((\\.|_|__|-+)[a-z0-9]+)*)*''"),
    ({"repositoryName": "batch-ok", "imageIds": []}, "InvalidParameterException",
     "Invalid parameter at 'imageIds' failed to satisfy constraint: 'Member must have length greater than or equal to 1'"),
    ({"repositoryName": "batch-ok", "imageIds": {"imageTag": "1"}}, "SerializationException",
     "Start of structure or map found where not expected"),
    ({"repositoryName": "batch-ok", "imageIds": 5}, "SerializationException", "Unexpected field type"),
    ({"repositoryName": "batch-ok", "imageIds": ["x"]}, "SerializationException", "Unexpected value type in payload"),
])
def test_batch_get_image_refuses_malformed_requests(body, code, message):
    status, answer = _ptc_call("BatchGetImage", body)
    assert (status, answer["__type"], answer.get("message") or answer.get("Message")) == (400, code, message)
