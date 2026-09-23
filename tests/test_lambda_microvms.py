# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Lambda MicroVM collection operations."""

import asyncio
import io
import json
import zipfile

from ministack.core.responses import request_scope
from ministack.services import lambda_microvms


def _create_body(name):
    return json.dumps({
        "baseImageArn": "arn:aws:lambda:us-east-1:aws:microvm-image:base",
        "buildRoleArn": "arn:aws:iam::000000000000:role/build",
        "name": name,
        "codeArtifact": {"uri": "s3://bucket/artifact.zip"},
    }).encode()


def _request(method, path, body=b"", query_params=None):
    return asyncio.run(lambda_microvms.handle_request(
        method, path, {}, body, query_params or {}))


class _FakeMicrovmContainer:
    def __init__(self, name, kwargs):
        self.id = f"cid-{name}"
        self.name = name
        self.kwargs = kwargs
        self.status = "running"
        self.pause_calls = 0
        self.unpause_calls = 0
        self.remove_calls = 0
        self.stop_calls = 0

    def reload(self):
        return None

    def pause(self):
        self.pause_calls += 1
        self.status = "paused"

    def unpause(self):
        self.unpause_calls += 1
        self.status = "running"

    def stop(self, timeout=2):
        self.stop_calls += 1
        self.status = "exited"

    def remove(self, force=False, v=False):
        self.remove_calls += 1


class _FakeMicrovmDocker:
    def __init__(self):
        self.containers_by_name = {}
        self.runs = []

        class _Images:
            @staticmethod
            def get(_ref):
                return type(
                    "Image",
                    (),
                    {"attrs": {"Config": {"Entrypoint": None, "Cmd": ["python", "server.py"]}}},
                )()

            @staticmethod
            def pull(_ref):
                return _Images.get(_ref)

            def build(self, path, tag, rm=True, forcerm=True):
                self.last_build = {"path": path, "tag": tag, "rm": rm, "forcerm": forcerm}
                return object(), []

        class _Containers:
            def __init__(self, owner):
                self.owner = owner

            def get(self, ref):
                for container in self.owner.containers_by_name.values():
                    if ref in (container.id, container.name):
                        return container
                raise LookupError(ref)

            def run(self, image, **kwargs):
                name = kwargs.get("name", f"build-{len(self.owner.runs)}")
                container = _FakeMicrovmContainer(name, {"image": image, **kwargs})
                self.owner.containers_by_name[container.name] = container
                self.owner.runs.append(container)
                return container

            def list(self, all=False, filters=None):
                return list(self.owner.containers_by_name.values())

        self.images = _Images()
        self.images.last_build = None
        self.containers = _Containers(self)


def test_list_microvm_images_returns_empty_collection():
    lambda_microvms.reset()
    try:
        with request_scope("000000000000", "us-east-1"):
            status, _, raw = _request("GET", "/2025-09-09/microvm-images")

        assert status == 200
        assert json.loads(raw) == {"items": []}
    finally:
        lambda_microvms.reset()


def test_docker_backend_builds_an_artifact_and_manages_the_built_image(monkeypatch):
    fake = _FakeMicrovmDocker()
    hook_calls = []
    monkeypatch.setattr(lambda_microvms, "MICROVM_BACKEND", "docker")
    monkeypatch.setattr(lambda_microvms, "_get_docker", lambda: fake)
    monkeypatch.setattr(lambda_microvms, "_docker_in_use", False)
    monkeypatch.setattr(
        lambda_microvms,
        "_s3_artifact_bytes",
        lambda _uri: _artifact_zip(),
    )
    monkeypatch.setattr(
        lambda_microvms,
        "_call_hook",
        lambda _record, _container, name, **_kwargs: hook_calls.append(name),
    )

    body = json.loads(_create_body("runner"))
    body["hooks"] = {
        "port": 9000,
        "microvmImageHooks": {"ready": "ENABLED", "validate": "ENABLED"},
        "microvmHooks": {
            "run": "ENABLED",
            "suspend": "ENABLED",
            "resume": "ENABLED",
            "terminate": "ENABLED",
        },
    }
    try:
        with request_scope("000000000000", "us-east-1"):
            status, _, _ = _request(
                "POST", "/2025-09-09/microvm-images", json.dumps(body).encode())
            assert status == 201
            assert fake.images.last_build["tag"].startswith("ministack-microvm:")
            assert hook_calls == ["ready", "validate"]

            status, _, raw = _request(
                "POST", "/2025-09-09/microvms",
                json.dumps({"imageIdentifier": "runner"}).encode(),
            )
            launched = json.loads(raw)
            assert status == 200
            assert launched["state"] == "RUNNING"
            assert len(fake.runs) == 2
            runtime = fake.runs[-1]
            assert runtime.kwargs["image"] == fake.images.last_build["tag"]
            assert "command" not in runtime.kwargs
            assert runtime.kwargs["labels"]["ministack"] == "lambda-microvm"
            assert hook_calls == ["ready", "validate", "run"]

            microvm_id = launched["microvmId"]
            status, _, _ = _request(
                "POST", f"/2025-09-09/microvms/{microvm_id}/suspend")
            assert status == 200
            assert runtime.pause_calls == 1
            assert hook_calls[-1] == "suspend"

            status, _, _ = _request(
                "POST", f"/2025-09-09/microvms/{microvm_id}/resume")
            assert status == 200
            assert runtime.unpause_calls == 1
            assert hook_calls[-1] == "resume"

            status, _, _ = _request(
                "DELETE", f"/2025-09-09/microvms/{microvm_id}")
            assert status == 200
            assert runtime.remove_calls == 1
            assert hook_calls[-1] == "terminate"
    finally:
        lambda_microvms.reset()


def _artifact_zip():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("Dockerfile", "FROM alpine:3\nCMD [\"python\", \"server.py\"]\n")
    return output.getvalue()


def test_list_microvm_images_filters_and_pages():
    lambda_microvms.reset()
    try:
        with request_scope("000000000000", "us-east-1"):
            for name in ("web", "worker", "worker-canary"):
                status, _, _ = _request(
                    "POST", "/2025-09-09/microvm-images", _create_body(name))
                assert status == 201

            status, _, raw = _request(
                "GET", "/2025-09-09/microvm-images",
                query_params={"nameFilter": ["worker"], "maxResults": ["1"]},
            )
            first_page = json.loads(raw)
            assert status == 200
            assert [item["name"] for item in first_page["items"]] == ["worker"]
            assert first_page["nextToken"] == "1"

            status, _, raw = _request(
                "GET", "/2025-09-09/microvm-images",
                query_params={
                    "nameFilter": ["worker"],
                    "maxResults": ["1"],
                    "nextToken": [first_page["nextToken"]],
                },
            )
            second_page = json.loads(raw)
            assert status == 200
            assert [item["name"] for item in second_page["items"]] == ["worker-canary"]
            assert "nextToken" not in second_page
    finally:
        lambda_microvms.reset()


def test_get_microvm_image_version_accepts_an_arn_path():
    lambda_microvms.reset()
    try:
        with request_scope("000000000000", "eu-west-1"):
            status, _, _ = _request(
                "POST", "/2025-09-09/microvm-images", _create_body("srea-gh-runner"))
            assert status == 201

            image_arn = (
                "arn:aws:lambda:eu-west-1:000000000000:"
                "microvm-image/srea-gh-runner"
            )
            status, _, raw = _request(
                "GET",
                f"/2025-09-09/microvm-images/{image_arn}/versions/1",
            )

        version = json.loads(raw)
        assert status == 200
        assert version["imageArn"] == image_arn
        assert version["imageVersion"] == "1"
        assert version["state"] == "SUCCESSFUL"
        assert version["status"] == "ACTIVE"
    finally:
        lambda_microvms.reset()


def test_get_and_update_microvm_image_support_the_publish_flow():
    lambda_microvms.reset()
    try:
        with request_scope("000000000000", "eu-west-1"):
            status, _, raw = _request(
                "POST", "/2025-09-09/microvm-images", _create_body("runner"))
            assert status == 201
            created = json.loads(raw)
            image_arn = created["imageArn"]

            status, _, raw = _request(
                "GET", f"/2025-09-09/microvm-images/{image_arn}")
            assert status == 200
            assert json.loads(raw)["state"] == "CREATED"

            status, _, raw = _request(
                "PUT",
                f"/2025-09-09/microvm-images/{image_arn}",
                _create_body("ignored-name"),
            )
            updated = json.loads(raw)
            assert status == 200
            assert updated["imageArn"] == image_arn
            assert updated["imageVersion"] == "2"
            assert updated["state"] == "UPDATED"

            status, _, raw = _request(
                "GET",
                f"/2025-09-09/microvm-images/{image_arn}/versions/2",
            )

        version = json.loads(raw)
        assert status == 200
        assert version["imageVersion"] == "2"
        assert version["state"] == "SUCCESSFUL"
        assert version["status"] == "ACTIVE"
    finally:
        lambda_microvms.reset()
