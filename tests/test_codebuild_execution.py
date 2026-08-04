"""Tests for the opt-in CodeBuild build execution path.

Execution hands the buildspec to the official CodeBuild local agent, so the
Docker client is faked here: these cover the build bookkeeping around the agent
(phase records, status mapping, the metadata-only default), not Docker itself.
"""

import json
import os
import time

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


def test_execute_build_writes_agent_output_to_cloudwatch_logs(monkeypatch, tmp_path):
    from ministack.services import cloudwatch_logs as cwl

    container = _FakeContainer([
        "[Container] running command echo hi",
        "Phase complete: BUILD State: SUCCEEDED",
    ])
    monkeypatch.setattr(codebuild, "_get_docker", lambda: _FakeDocker(container))
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _project()
    build = _seed_build(project, "demo:0005")

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

    project = _project()
    build = _seed_build(project, "demo:0006")

    codebuild._execute_build("demo:0006", project)

    assert build["buildStatus"] == "FAILED"
    assert "TIMED_OUT" in [p.get("phaseStatus") for p in build["phases"]]


def test_timeout_seconds_uses_the_project_value(monkeypatch):
    assert codebuild._timeout_seconds({"timeoutInMinutes": 5}) == 300
    assert codebuild._timeout_seconds({}) == 3600
    assert codebuild._timeout_seconds({"timeoutInMinutes": "bogus"}) == 3600


def test_env_file_points_the_build_at_ministack(monkeypatch, tmp_path):
    """A build's AWS calls should reach this emulator, not real AWS."""
    monkeypatch.setattr(codebuild, "_aws_endpoint", lambda: "http://172.17.0.1:4566")

    project = _project()
    build = _seed_build(project, "demo:0007")
    env_file = tmp_path / "env.list"

    codebuild._write_env_file(str(env_file), project, build)

    lines = env_file.read_text(encoding="utf-8").splitlines()
    assert "AWS_ENDPOINT_URL=http://172.17.0.1:4566" in lines
    assert "AWS_ACCESS_KEY_ID=test" in lines


def test_env_file_keeps_an_endpoint_the_project_declared(monkeypatch, tmp_path):
    monkeypatch.setattr(codebuild, "_aws_endpoint", lambda: "http://172.17.0.1:4566")

    project = _project()
    project["environment"]["environmentVariables"].append(
        {"name": "AWS_ENDPOINT_URL", "value": "https://real.aws"}
    )
    build = _seed_build(project, "demo:0008")
    env_file = tmp_path / "env.list"

    codebuild._write_env_file(str(env_file), project, build)

    lines = env_file.read_text(encoding="utf-8").splitlines()
    assert "AWS_ENDPOINT_URL=https://real.aws" in lines
    assert "AWS_ENDPOINT_URL=http://172.17.0.1:4566" not in lines


def test_log_stream_is_capped(monkeypatch, tmp_path):
    from ministack.services import cloudwatch_logs as cwl

    monkeypatch.setattr(codebuild, "MAX_LOG_EVENTS", 5)

    project = _project()
    build = _seed_build(project, "demo:0009")
    emit = codebuild._log_sink(build)
    for i in range(20):
        emit(f"line {i}")

    stream = cwl._log_groups[build["logs"]["groupName"]]["streams"][build["logs"]["streamName"]]
    messages = [event["message"] for event in stream["events"]]
    assert len(messages) == 5
    assert messages[-1] == "line 19"


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


def test_execute_build_without_docker_reports_fault(monkeypatch, tmp_path):
    monkeypatch.setattr(codebuild, "_get_docker", lambda: None)
    monkeypatch.setattr(codebuild, "WORKSPACE", str(tmp_path))

    project = _project()
    build = _seed_build(project, "demo:0004")

    codebuild._execute_build("demo:0004", project)

    assert build["buildStatus"] == "FAULT"
