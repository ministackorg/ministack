import pytest
from botocore.exceptions import ClientError

# ========== CodeBuild ==========

def test_codebuild_create_project(codebuild):
    resp = codebuild.create_project(
        name="test-project",
        source={"type": "NO_SOURCE", "buildspec": "version: 0.2\nphases:\n  build:\n    commands:\n      - echo Hello"},
        artifacts={"type": "NO_ARTIFACTS"},
        environment={
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        },
        serviceRole="arn:aws:iam::000000000000:role/codebuild-role",
    )
    project = resp["project"]
    assert project["name"] == "test-project"
    assert project["arn"].startswith("arn:aws:codebuild:")
    assert "created" in project


def test_codebuild_create_duplicate_project(codebuild):
    with pytest.raises(ClientError) as exc:
        codebuild.create_project(
            name="test-project",
            source={"type": "NO_SOURCE"},
            artifacts={"type": "NO_ARTIFACTS"},
            environment={"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL"},
            serviceRole="arn:aws:iam::000000000000:role/codebuild-role",
        )
    assert "ResourceAlreadyExistsException" in str(exc.value)


def test_codebuild_batch_get_projects(codebuild):
    resp = codebuild.batch_get_projects(names=["test-project", "nonexistent"])
    assert len(resp["projects"]) == 1
    assert resp["projects"][0]["name"] == "test-project"
    assert "nonexistent" in resp["projectsNotFound"]


def test_codebuild_list_projects(codebuild):
    resp = codebuild.list_projects()
    assert "test-project" in resp["projects"]


def test_codebuild_update_project(codebuild):
    resp = codebuild.update_project(
        name="test-project",
        description="updated description",
    )
    assert resp["project"]["description"] == "updated description"


def test_codebuild_start_build(codebuild):
    resp = codebuild.start_build(projectName="test-project")
    build = resp["build"]
    assert build["projectName"] == "test-project"
    assert build["buildStatus"] == "SUCCEEDED"
    assert build["arn"].startswith("arn:aws:codebuild:")
    assert "phases" in build


def test_codebuild_batch_get_builds(codebuild):
    start_resp = codebuild.start_build(projectName="test-project")
    build_id = start_resp["build"]["id"]
    resp = codebuild.batch_get_builds(ids=[build_id, "nonexistent:fake"])
    assert len(resp["builds"]) == 1
    assert resp["builds"][0]["id"] == build_id
    assert "nonexistent:fake" in resp["buildsNotFound"]


def test_codebuild_list_builds_for_project(codebuild):
    resp = codebuild.list_builds_for_project(projectName="test-project")
    assert len(resp["ids"]) >= 1


def test_codebuild_list_builds(codebuild):
    resp = codebuild.list_builds()
    assert len(resp["ids"]) >= 1


def test_codebuild_stop_build(codebuild):
    start_resp = codebuild.start_build(projectName="test-project")
    build_id = start_resp["build"]["id"]
    resp = codebuild.stop_build(id=build_id)
    assert resp["build"]["buildStatus"] == "STOPPED"


def test_codebuild_batch_delete_builds(codebuild):
    start_resp = codebuild.start_build(projectName="test-project")
    build_id = start_resp["build"]["id"]
    resp = codebuild.batch_delete_builds(ids=[build_id])
    assert build_id in resp["buildsDeleted"]


def test_codebuild_delete_project(codebuild):
    codebuild.delete_project(name="test-project")
    resp = codebuild.list_projects()
    assert "test-project" not in resp["projects"]


def test_codebuild_delete_nonexistent_project(codebuild):
    with pytest.raises(ClientError) as exc:
        codebuild.delete_project(name="nonexistent")
    assert "ResourceNotFoundException" in str(exc.value)
