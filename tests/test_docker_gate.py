"""The product Docker capability gate is hard, including cleanup paths."""

import importlib

import pytest


@pytest.mark.parametrize(
    "module_name, getter",
    [
        ("ecs", "_get_docker"),
        ("codebuild", "_get_docker"),
        ("ec2", "_get_docker"),
        ("eks", "_get_docker"),
        ("elasticache", "_get_docker"),
        ("glue", "_get_docker"),
        ("mwaa", "_get_docker"),
        ("opensearch", "_get_docker"),
        ("rds", "_get_docker"),
        ("lambda_svc", "_get_docker_client"),
    ],
)
def test_disabled_gate_prevents_docker_client_creation(monkeypatch, module_name, getter):
    monkeypatch.setenv("MINISTACK_DOCKER_ENABLED", "0")
    module = importlib.import_module(f"ministack.services.{module_name}")
    assert getattr(module, getter)() is None


def test_disabled_gate_skips_app_reaper_client(monkeypatch):
    monkeypatch.setenv("MINISTACK_DOCKER_ENABLED", "false")
    app = importlib.import_module("ministack.app")
    assert app._reaper_docker_client() is None


def test_disabled_gate_skips_dsql_docker_preflight(monkeypatch):
    monkeypatch.setenv("MINISTACK_DOCKER_ENABLED", "0")
    dsql = importlib.import_module("ministack.services.dsql")
    assert dsql._docker_available() is False


def test_gate_defaults_to_enabled(monkeypatch):
    monkeypatch.delenv("MINISTACK_DOCKER_ENABLED", raising=False)
    from ministack.core.docker import docker_enabled

    assert docker_enabled()


def test_docker_available_is_gated_before_import(monkeypatch):
    monkeypatch.setenv("MINISTACK_DOCKER_ENABLED", "0")
    from ministack.core import docker as docker_gate

    assert docker_gate.docker_available() is False


@pytest.mark.parametrize("ping_works", [True, False])
def test_docker_available_probes_and_swallows_errors(monkeypatch, ping_works):
    monkeypatch.setenv("MINISTACK_DOCKER_ENABLED", "1")
    from ministack.core import docker as docker_gate

    calls = []

    class FakeClient:
        def ping(self):
            calls.append("ping")
            if not ping_works:
                raise RuntimeError("daemon unavailable")

        def close(self):
            calls.append("close")

    class FakeDocker:
        @staticmethod
        def from_env(timeout):
            calls.append(timeout)
            return FakeClient()

    monkeypatch.setitem(__import__("sys").modules, "docker", FakeDocker)
    assert docker_gate.docker_available() is ping_works
    assert calls == [5, "ping", "close"]
