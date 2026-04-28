import io
import json
import os
import time
import uuid as _uuid_mod
import zipfile
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError


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
