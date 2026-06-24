"""Instance-scoped container cleanup (MINISTACK_INSTANCE_ID).

When MINISTACK_INSTANCE_ID is set, spawned containers are tagged with a
`ministack_instance=<id>` label and the boot/teardown cleanup filters on it,
so multiple MiniStack instances can share one Docker host without reaping each
other's containers. When unset, behaviour is the original host-global cleanup.
"""
import os

from ministack.services import eks as _eks
from ministack.services import lambda_svc as _lambda
from ministack import app as _app


# --- _instance_labels() helper (eks + lambda) -------------------------------

def test_instance_labels_empty_when_unset(monkeypatch):
    monkeypatch.delenv("MINISTACK_INSTANCE_ID", raising=False)
    assert _eks._instance_labels() == {}
    assert _lambda._instance_labels() == {}


def test_instance_labels_set_when_present(monkeypatch):
    monkeypatch.setenv("MINISTACK_INSTANCE_ID", "inst-x")
    assert _eks._instance_labels() == {"ministack_instance": "inst-x"}
    assert _lambda._instance_labels() == {"ministack_instance": "inst-x"}


# --- k3s spawn kwargs carry the instance label ------------------------------

def test_k3s_run_kwargs_labels_global_when_unset(monkeypatch):
    monkeypatch.delenv("MINISTACK_INSTANCE_ID", raising=False)
    kwargs = _eks._k3s_run_kwargs("mycluster", 16443)
    assert kwargs["labels"] == {"ministack": "eks", "cluster_name": "mycluster"}


def test_k3s_run_kwargs_labels_scoped_when_set(monkeypatch):
    monkeypatch.setenv("MINISTACK_INSTANCE_ID", "inst-x")
    kwargs = _eks._k3s_run_kwargs("mycluster", 16443)
    assert kwargs["labels"] == {
        "ministack": "eks",
        "cluster_name": "mycluster",
        "ministack_instance": "inst-x",
    }


# --- boot cleanup filters are instance-scoped -------------------------------

class _FakeContainers:
    def __init__(self, sink):
        self._sink = sink

    def list(self, all=False, filters=None):
        self._sink.append(filters)
        return []


class _FakeClient:
    def __init__(self, sink):
        self.containers = _FakeContainers(sink)


def _patch_docker(monkeypatch, sink):
    import docker
    # _stop_docker_containers bails early unless the socket path exists.
    monkeypatch.setattr(os.path, "exists", lambda _p: True)
    monkeypatch.setattr(docker, "from_env", lambda: _FakeClient(sink))


def test_stop_docker_containers_global_when_unset(monkeypatch):
    monkeypatch.delenv("MINISTACK_INSTANCE_ID", raising=False)
    captured = []
    _patch_docker(monkeypatch, captured)

    _app._stop_docker_containers()

    assert captured, "containers.list was never called"
    for f in captured:
        # exactly one service label, no instance scoping
        assert len(f["label"]) == 1
        assert f["label"][0].startswith("ministack=")
        assert all(not l.startswith("ministack_instance") for l in f["label"])


def test_stop_docker_containers_scoped_when_set(monkeypatch):
    monkeypatch.setenv("MINISTACK_INSTANCE_ID", "inst-x")
    captured = []
    _patch_docker(monkeypatch, captured)

    _app._stop_docker_containers()

    assert captured, "containers.list was never called"
    for f in captured:
        # every reap is scoped to this instance AND a single service label
        assert "ministack_instance=inst-x" in f["label"]
        assert any(l.startswith("ministack=") for l in f["label"])
