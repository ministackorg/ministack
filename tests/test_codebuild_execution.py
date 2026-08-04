"""Tests for the opt-in CodeBuild build execution path.

Execution hands the buildspec to the official CodeBuild local agent, so the
Docker client is faked here: these cover the build bookkeeping around the agent
(phase records, status mapping, the metadata-only default), not Docker itself.
"""

import json
import os

from ministack.services import codebuild


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


BUILDSPEC = "version: 0.2\nphases:\n  build:\n    commands:\n      - echo hi\n"


def _project(buildspec=BUILDSPEC):
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


def _seed_build(project, build_id="demo:0001"):
    build = codebuild._make_build_record(project, build_id)
    codebuild._builds[build_id] = build
    return build


def test_start_build_is_metadata_only_by_default(monkeypatch):
    """Execution is opt-in: without the flag no container is ever created."""
    calls = []
    monkeypatch.setattr(codebuild, "_get_docker", lambda: calls.append(1))
    monkeypatch.setattr(codebuild, "EXECUTE_BUILDS", False)

    project = _project()
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
    monkeypatch.setattr(codebuild, "SOURCE_PATH", "")

    project = _project()
    build = _seed_build(project)

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
        assert fh.read() == BUILDSPEC

    env_file = os.path.join(str(tmp_path), "demo_0001", "env", "env.list")
    with open(env_file, encoding="utf-8") as fh:
        env_lines = fh.read().splitlines()
    assert "FOO=bar" in env_lines
    assert "CODEBUILD_BUILD_ID=demo:0001" in env_lines


def test_execute_build_reports_failed_exit_code(monkeypatch, tmp_path):
    container = _FakeContainer(["Phase complete: BUILD State: FAILED"], exit_code=1)
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _project()
    build = _seed_build(project, "demo:0002")

    codebuild._execute_build("demo:0002", project)

    assert build["buildStatus"] == "FAILED"
    phases = {p["phaseType"]: p.get("phaseStatus") for p in build["phases"]}
    assert phases["BUILD"] == "FAILED"


def test_execute_build_without_buildspec_never_starts_agent(monkeypatch, tmp_path):
    docker = _FakeDocker(_FakeContainer([]))
    monkeypatch.setattr(codebuild, "_get_docker", lambda: docker)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _project(buildspec="")
    build = _seed_build(project, "demo:0003")

    codebuild._execute_build("demo:0003", project)

    assert build["buildStatus"] == "FAILED"
    assert docker.containers.image is None


def test_execute_build_without_docker_reports_fault(monkeypatch, tmp_path):
    monkeypatch.setattr(codebuild, "_get_docker", lambda: None)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _project()
    build = _seed_build(project, "demo:0004")

    codebuild._execute_build("demo:0004", project)

    assert build["buildStatus"] == "FAULT"
