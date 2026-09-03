import json
import os
import time

import boto3
import pytest
from botocore.config import Config
from botocore.exceptions import ClientError

from ministack.services import codebuild

# ========== CodeBuild ==========


def _client(region):
    return boto3.client(
        "codebuild",
        endpoint_url=os.environ.get("MINISTACK_ENDPOINT", "http://localhost:4566"),
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"mode": "standard"}),
    )


def _project_args(name, description):
    return {
        "name": name,
        "description": description,
        "source": {"type": "NO_SOURCE"},
        "artifacts": {"type": "NO_ARTIFACTS"},
        "environment": {
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        },
        "serviceRole": "arn:aws:iam::000000000000:role/codebuild-role",
    }


def _ensure_codebuild_project(codebuild, name):
    if codebuild.batch_get_projects(names=[name])["projects"]:
        return
    codebuild.create_project(
        name=name,
        source={"type": "NO_SOURCE"},
        artifacts={"type": "NO_ARTIFACTS"},
        environment={
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
        },
        serviceRole="arn:aws:iam::000000000000:role/codebuild-role",
    )


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


def test_codebuild_batch_get_projects_by_arn(codebuild):
    arn = codebuild.batch_get_projects(names=["test-project"])["projects"][0]["arn"]
    resp = codebuild.batch_get_projects(names=[arn])
    assert len(resp["projects"]) == 1
    assert resp["projects"][0]["name"] == "test-project"
    assert resp["projectsNotFound"] == []


@pytest.mark.parametrize(
    "identifier_template",
    [
        "arn:aws:codebuild:us-east-1:000000000000:build/{name}",
        "arn:aws:codebuild:project/{name}",
        "arn:aws-us-gov:codebuild:us-east-1:000000000000:project/{name}",
        "arn:aws:lambda:us-east-1:000000000000:project/{name}",
        "arn:aws:codebuild:us-west-2:000000000000:project/{name}",
        "arn:aws:codebuild:us-east-1:111111111111:project/{name}",
    ],
)
def test_codebuild_batch_get_projects_does_not_tail_resolve_out_of_scope_arns(
    codebuild,
    identifier_template,
):
    name = "arn-parser-project"
    _ensure_codebuild_project(codebuild, name)

    identifier = identifier_template.format(name=name)
    resp = codebuild.batch_get_projects(names=[identifier])
    assert resp["projects"] == []
    assert resp["projectsNotFound"] == [identifier]


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


def test_codebuild_batch_delete_builds_not_deleted_shape(codebuild):
    """An id that matches no build comes back as a BuildNotDeleted structure
    ({id, statusCode}), not a bare string — SDK parsers crash on the string."""
    resp = codebuild.batch_delete_builds(ids=["test-project:00000000-0000-0000-0000-000000000000"])
    assert resp["buildsDeleted"] == []
    entry = resp["buildsNotDeleted"][0]
    assert entry["id"] == "test-project:00000000-0000-0000-0000-000000000000"
    assert entry["statusCode"]


def test_codebuild_delete_project(codebuild):
    codebuild.delete_project(name="test-project")
    resp = codebuild.list_projects()
    assert "test-project" not in resp["projects"]


def test_codebuild_delete_nonexistent_project(codebuild):
    with pytest.raises(ClientError) as exc:
        codebuild.delete_project(name="nonexistent")
    assert "ResourceNotFoundException" in str(exc.value)


def test_codebuild_projects_and_builds_are_region_scoped():
    east = _client("us-east-1")
    west = _client("us-west-2")
    name = "same-name-regional-project"

    east.create_project(**_project_args(name, "east"))
    west.create_project(**_project_args(name, "west"))
    try:
        east_project = east.batch_get_projects(names=[name])["projects"][0]
        west_project = west.batch_get_projects(names=[name])["projects"][0]
        assert east_project["description"] == "east"
        assert west_project["description"] == "west"
        assert ":us-east-1:" in east_project["arn"]
        assert ":us-west-2:" in west_project["arn"]
        assert name in east.list_projects()["projects"]
        assert name in west.list_projects()["projects"]

        east_build = east.start_build(projectName=name)["build"]
        west_build = west.start_build(projectName=name)["build"]
        assert ":us-east-1:" in east_build["arn"]
        assert ":us-west-2:" in west_build["arn"]
        assert east_build["id"] in east.list_builds()["ids"]
        assert east_build["id"] not in west.list_builds()["ids"]
        assert west_build["id"] in west.list_builds()["ids"]
        assert west_build["id"] not in east.list_builds()["ids"]
    finally:
        east.delete_project(name=name)
        west.delete_project(name=name)


def test_codebuild_restore_legacy_state_uses_resource_arn_region():
    from ministack.core.responses import (
        AccountScopedDict,
        set_request_account_id,
        set_request_region,
    )
    from ministack.services import codebuild as service

    account_id = "111111111111"
    resource_region = "us-west-2"
    boot_region = "us-east-1"
    project_name = "legacy-project"
    build_id = f"{project_name}:legacy-build"
    projects = AccountScopedDict()
    builds = AccountScopedDict()

    set_request_account_id(account_id)
    set_request_region(boot_region)
    projects[project_name] = {
        "name": project_name,
        "arn": (
            f"arn:aws:codebuild:{resource_region}:{account_id}:"
            f"project/{project_name}"
        ),
    }
    builds[build_id] = {
        "id": build_id,
        "arn": (
            f"arn:aws:codebuild:{resource_region}:{account_id}:build/{build_id}"
        ),
    }

    service.reset()
    try:
        service.restore_state({"projects": projects, "builds": builds})
        assert service._projects.get_scoped(
            account_id, resource_region, project_name
        )["name"] == project_name
        assert service._builds.get_scoped(
            account_id, resource_region, build_id
        )["id"] == build_id
        assert service._projects.get_scoped(
            account_id, boot_region, project_name
        ) is None
        assert service._builds.get_scoped(
            account_id, boot_region, build_id
        ) is None
    finally:
        service.reset()


# ========== CodeBuild execution (MINISTACK_CODEBUILD_EXECUTE=1) ==========
#
# Execution hands the buildspec to the official CodeBuild local agent, so the
# Docker client is faked here: these cover the build bookkeeping around the
# agent (phase records, status mapping, the metadata-only default), not
# Docker itself.


class _FakeContainer:
    def __init__(self, log_lines, exit_code=0):
        self._log_lines = log_lines
        self._exit_code = exit_code
        self.removed = False

    def logs(self, **_kwargs):
        return iter(line.encode("utf-8") for line in self._log_lines)

    def wait(self):
        return {"StatusCode": self._exit_code}

    def remove(self, **_kwargs):
        self.removed = True


class _FakeContainers:
    def __init__(self, container):
        self._container = container
        self.image = None
        self.kwargs = None

    def run(self, image, **kwargs):
        self.image = image
        self.kwargs = kwargs
        return self._container


class _FakeDocker:
    def __init__(self, container):
        self.containers = _FakeContainers(container)


EXECUTION_BUILDSPEC = "version: 0.2\nphases:\n  build:\n    commands:\n      - echo hi\n"


def _execution_project(buildspec=EXECUTION_BUILDSPEC):
    return {
        "name": "demo",
        "arn": "arn:aws:codebuild:us-east-1:000000000000:project/demo",
        "source": {"type": "NO_SOURCE", "buildspec": buildspec},
        "artifacts": {"type": "NO_ARTIFACTS"},
        "environment": {
            "type": "LINUX_CONTAINER",
            "image": "aws/codebuild/standard:7.0",
            "computeType": "BUILD_GENERAL1_SMALL",
            "privilegedMode": True,
            "environmentVariables": [{"name": "FOO", "value": "bar"}],
        },
        "serviceRole": "arn:aws:iam::000000000000:role/codebuild-role",
    }


def _seed_execution_build(project, build_id="demo:0001"):
    build = codebuild._make_build_record(project, build_id)
    codebuild._builds[build_id] = build
    return build


def test_start_build_is_metadata_only_by_default(monkeypatch):
    """Execution is opt-in: without the flag no container is ever created."""
    calls = []
    monkeypatch.setattr(codebuild, "_get_docker", lambda: calls.append(1))
    monkeypatch.setattr(codebuild, "EXECUTE_BUILDS", False)

    project = _execution_project()
    codebuild._projects["demo"] = project

    status, _headers, body = codebuild._start_build({"projectName": "demo"})

    assert status == 200
    assert json.loads(body)["build"]["buildStatus"] == "SUCCEEDED"
    assert calls == []


def test_execute_build_records_phases_from_agent_log(monkeypatch, tmp_path):
    container = _FakeContainer([
        "Phase complete: INSTALL State: SUCCEEDED",
        "[Container] running command echo hi",
        "Phase complete: BUILD State: SUCCEEDED",
    ])
    docker = _FakeDocker(container)
    monkeypatch.setattr(codebuild, "_get_docker", lambda: docker)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project()
    build = _seed_execution_build(project)

    codebuild._execute_build("demo:0001", project)

    assert build["buildStatus"] == "SUCCEEDED"
    assert build["currentPhase"] == "COMPLETED"

    phases = {p["phaseType"]: p.get("phaseStatus") for p in build["phases"]}
    assert phases["INSTALL"] == "SUCCEEDED"
    assert phases["BUILD"] == "SUCCEEDED"
    assert phases["COMPLETED"] == "SUCCEEDED"
    assert all("endTime" in p for p in build["phases"])

    # The build runs in the project's image; the agent image only orchestrates.
    assert docker.containers.image == codebuild.AGENT_IMAGE
    assert docker.containers.kwargs["environment"]["IMAGE_NAME"] == "aws/codebuild/standard:7.0"
    assert docker.containers.kwargs["environment"]["DOCKER_PRIVILEGED_MODE"] == "true"
    assert container.removed is True

    buildspec_path = docker.containers.kwargs["environment"]["BUILDSPEC"]
    assert os.path.isfile(buildspec_path)
    with open(buildspec_path, encoding="utf-8") as fh:
        assert fh.read() == EXECUTION_BUILDSPEC

    env_file = os.path.join(str(tmp_path), "demo_0001", "env", "env.list")
    with open(env_file, encoding="utf-8") as fh:
        env_lines = fh.read().splitlines()
    assert "FOO=bar" in env_lines
    assert "CODEBUILD_BUILD_ID=demo:0001" in env_lines


def test_execute_build_writes_agent_output_to_cloudwatch_logs(monkeypatch, tmp_path):
    from ministack.services import cloudwatch_logs as cwl

    container = _FakeContainer([
        "[Container] running command echo hi",
        "Phase complete: BUILD State: SUCCEEDED",
    ])
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project()
    build = _seed_execution_build(project, "demo:0005")

    codebuild._execute_build("demo:0005", project)

    group = cwl._log_groups[build["logs"]["groupName"]]
    stream = group["streams"][build["logs"]["streamName"]]
    messages = [event["message"] for event in stream["events"]]

    assert "[Container] running command echo hi" in messages
    assert "Phase complete: BUILD State: SUCCEEDED" in messages
    assert stream["firstEventTimestamp"] is not None
    assert stream["lastEventTimestamp"] >= stream["firstEventTimestamp"]


def test_execute_build_reports_failed_exit_code(monkeypatch, tmp_path):
    container = _FakeContainer(["Phase complete: BUILD State: FAILED"], exit_code=1)
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project()
    build = _seed_execution_build(project, "demo:0002")

    codebuild._execute_build("demo:0002", project)

    assert build["buildStatus"] == "FAILED"
    phases = {p["phaseType"]: p.get("phaseStatus") for p in build["phases"]}
    assert phases["BUILD"] == "FAILED"


def test_execute_build_without_buildspec_never_starts_agent(monkeypatch, tmp_path):
    docker = _FakeDocker(_FakeContainer([]))
    monkeypatch.setattr(codebuild, "_get_docker", lambda: docker)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project(buildspec="")
    build = _seed_execution_build(project, "demo:0003")

    codebuild._execute_build("demo:0003", project)

    assert build["buildStatus"] == "FAILED"
    assert docker.containers.image is None


def test_execute_build_times_out_per_project_timeout(monkeypatch, tmp_path):
    """A build past timeoutInMinutes is stopped and reported, not left running."""
    class _HangingContainer(_FakeContainer):
        def logs(self, **_kwargs):
            # Ends only once the timeout fired and "removed" the container.
            while not self.removed:
                time.sleep(0.01)
            return iter(())

    container = _HangingContainer([])
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))
    monkeypatch.setattr(codebuild, "_timeout_seconds", lambda _project: 0.2)

    project = _execution_project()
    build = _seed_execution_build(project, "demo:0006")

    codebuild._execute_build("demo:0006", project)

    assert build["buildStatus"] == "TIMED_OUT"
    assert "TIMED_OUT" in [p.get("phaseStatus") for p in build["phases"]]


def test_timeout_seconds_uses_the_project_value(monkeypatch):
    assert codebuild._timeout_seconds({"timeoutInMinutes": 5}) == 300
    assert codebuild._timeout_seconds({}) == 3600
    assert codebuild._timeout_seconds({"timeoutInMinutes": "bogus"}) == 3600


def test_env_file_points_the_build_at_ministack(monkeypatch, tmp_path):
    """A build's AWS calls should reach this emulator, not real AWS."""
    monkeypatch.setattr(codebuild, "_aws_endpoint", lambda: "http://172.17.0.1:4566")

    project = _execution_project()
    build = _seed_execution_build(project, "demo:0007")
    env_file = tmp_path / "env.list"

    codebuild._write_env_file(str(env_file), project, build)

    lines = env_file.read_text(encoding="utf-8").splitlines()
    assert "AWS_ENDPOINT_URL=http://172.17.0.1:4566" in lines
    assert "AWS_ACCESS_KEY_ID=test" in lines


def test_env_file_keeps_an_endpoint_the_project_declared(monkeypatch, tmp_path):
    monkeypatch.setattr(codebuild, "_aws_endpoint", lambda: "http://172.17.0.1:4566")

    project = _execution_project()
    project["environment"]["environmentVariables"].append(
        {"name": "AWS_ENDPOINT_URL", "value": "https://real.aws"}
    )
    build = _seed_execution_build(project, "demo:0008")
    env_file = tmp_path / "env.list"

    codebuild._write_env_file(str(env_file), project, build)

    lines = env_file.read_text(encoding="utf-8").splitlines()
    assert "AWS_ENDPOINT_URL=https://real.aws" in lines
    assert "AWS_ENDPOINT_URL=http://172.17.0.1:4566" not in lines


def test_restored_in_flight_builds_are_not_left_running():
    """A build cannot survive a restart, so it must not restore as IN_PROGRESS."""
    codebuild.restore_state({
        "projects": {},
        "builds": {"demo:0010": {
            "id": "demo:0010",
            "projectName": "demo",
            "buildStatus": "IN_PROGRESS",
            "currentPhase": "BUILD",
        }},
    })

    restored = codebuild._builds["demo:0010"]
    assert restored["buildStatus"] == "FAULT"
    assert restored["currentPhase"] == "COMPLETED"
    assert "endTime" in restored


def test_stopped_build_is_not_overwritten_by_the_worker(monkeypatch, tmp_path):
    """StopBuild removes the container, which is not a build failure.

    Without this the worker's `container.wait()` raises on the removed container
    and reports FAULT over the user's STOPPED.
    """
    class _RemovedContainer(_FakeContainer):
        def logs(self, **_kwargs):
            yield b"Phase complete: BUILD State: SUCCEEDED"
            raise RuntimeError("container was removed")

        def wait(self):
            raise RuntimeError("container was removed")

    container = _RemovedContainer([])
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "_container_for_build", lambda _bid: container)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))
    monkeypatch.setattr(codebuild, "EXECUTE_BUILDS", True)

    project = _execution_project()
    codebuild._projects["demo"] = project
    build = _seed_execution_build(project, "demo:0011")

    codebuild._stop_build({"id": "demo:0011"})
    assert build["buildStatus"] == "STOPPED"

    codebuild._execute_build("demo:0011", project)

    assert build["buildStatus"] == "STOPPED"
    assert build["currentPhase"] == "COMPLETED"
    # The registry must not leak once the worker is done.
    assert codebuild._stop_requested("demo:0011") is False


def test_execute_build_without_docker_reports_fault(monkeypatch, tmp_path):
    monkeypatch.setattr(codebuild, "_get_docker", lambda: None)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project()
    build = _seed_execution_build(project, "demo:0004")

    codebuild._execute_build("demo:0004", project)

    assert build["buildStatus"] == "FAULT"


def test_execute_build_agent_exit_zero_without_phases_is_fault(monkeypatch, tmp_path):
    """The agent exits 0 even when it started nothing (e.g. denied the Docker
    socket under SELinux). A zero exit with no 'Phase complete' line means no
    build phase ran, so the outcome is FAULT — not a silent SUCCEEDED."""
    container = _FakeContainer([
        "permission denied while trying to connect to the Docker daemon socket",
    ])
    docker = _FakeDocker(container)
    monkeypatch.setattr(codebuild, "_get_docker", lambda: docker)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _execution_project()
    build = _seed_execution_build(project)

    codebuild._execute_build("demo:0001", project)

    assert build["buildStatus"] == "FAULT"
    assert container.removed is True
