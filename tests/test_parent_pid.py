# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""--watch-parent-pid: a foreground MiniStack stops when the named process exits."""

import os
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_health(port: int, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/_ministack/health", timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise AssertionError(f"MiniStack on port {port} did not become healthy within {timeout}s")


def _spawn(port: int, parent_pid: str, log_path) -> subprocess.Popen:
    env = {**os.environ, "GATEWAY_PORT": str(port), "LOG_LEVEL": "INFO"}
    with open(log_path, "w") as log:
        return subprocess.Popen(
            [sys.executable, "-m", "ministack", "--watch-parent-pid", parent_pid],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )


def _stop(proc: subprocess.Popen) -> None:
    if proc.poll() is None:
        proc.kill()
        proc.wait(timeout=5)


@pytest.mark.serial
def test_parent_exit_shuts_ministack_down_gracefully(tmp_path):
    parent = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    port = _free_port()
    log_path = tmp_path / "ministack.log"
    proc = _spawn(port, str(parent.pid), log_path)
    try:
        _wait_health(port)
        assert proc.poll() is None, "MiniStack exited while its parent was still alive"

        parent.kill()  # SIGKILL: the parent gets no chance to clean up
        parent.wait(timeout=5)

        assert proc.wait(timeout=30) == 0
        log = log_path.read_text()
        assert "has exited; shutting down" in log
        assert "MiniStack shutting down" in log
    finally:
        _stop(proc)
        _stop(parent)


def test_watcher_signals_only_once_hypercorn_handles_signals(monkeypatch):
    """No SIGTERM before lifespan.startup, i.e. before hypercorn handles it gracefully."""
    import ministack.app as app

    parent_alive = {"value": True}
    monkeypatch.setattr(app, "_process_alive", lambda pid: parent_alive["value"])
    monkeypatch.setattr(app, "_PARENT_POLL_INTERVAL", 0.01)
    lifespan_started = threading.Event()
    monkeypatch.setattr(app, "_LIFESPAN_STARTED", lifespan_started)
    sent, fired = [], threading.Event()

    def fake_signal_self():
        sent.append("SIGTERM")
        fired.set()

    monkeypatch.setattr(app, "_signal_self", fake_signal_self)
    app._watch_parent_pid(12345)
    parent_alive["value"] = False
    time.sleep(0.2)
    assert sent == [], "signalled before hypercorn's handlers were in place"

    lifespan_started.set()
    assert fired.wait(5)
    assert sent == ["SIGTERM"]


def test_process_alive():
    import ministack.app as app

    assert app._process_alive(1)  # init/launchd: PermissionError for non-root, which counts as alive
    gone = subprocess.Popen([sys.executable, "-c", "pass"])
    gone.wait(timeout=10)
    assert not app._process_alive(gone.pid)


def test_parent_pid_that_is_not_running_is_rejected_at_startup(tmp_path):
    gone = subprocess.Popen([sys.executable, "-c", "pass"])
    gone.wait(timeout=10)
    proc = _spawn(_free_port(), str(gone.pid), tmp_path / "ministack.log")
    try:
        assert proc.wait(timeout=30) != 0
        assert "is not a running process" in (tmp_path / "ministack.log").read_text()
    finally:
        _stop(proc)


@pytest.mark.serial
@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_signals_still_shut_down_gracefully_with_a_parent_pid(tmp_path, signum):
    """Signals keep hypercorn's own graceful handling with the flag set."""
    parent = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    port = _free_port()
    log_path = tmp_path / "ministack.log"
    proc = _spawn(port, str(parent.pid), log_path)
    try:
        _wait_health(port)
        proc.send_signal(signum)
        assert proc.wait(timeout=30) == 0
        assert "MiniStack shutting down" in log_path.read_text()
    finally:
        _stop(proc)
        _stop(parent)


@pytest.mark.parametrize("value", ["not-a-pid", "0", "-5", "3000000000"])
def test_invalid_parent_pid_is_rejected_at_startup(tmp_path, value):
    port = _free_port()
    proc = _spawn(port, value, tmp_path / "ministack.log")
    try:
        assert proc.wait(timeout=30) != 0
        assert "--watch-parent-pid" in (tmp_path / "ministack.log").read_text()
    finally:
        _stop(proc)


@pytest.mark.parametrize("mode", ["--detach", "--stop"])
def test_watch_parent_pid_requires_foreground_mode(tmp_path, mode):
    log_path = tmp_path / "ministack.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "ministack", mode, "--watch-parent-pid", str(os.getpid())],
            stdout=log,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
    assert proc.wait(timeout=30) != 0
    assert "--watch-parent-pid requires foreground mode" in log_path.read_text()


def test_old_environment_variable_does_not_enable_watcher(tmp_path):
    port = _free_port()
    log_path = tmp_path / "ministack.log"
    with open(log_path, "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "ministack"],
            env={**os.environ, "GATEWAY_PORT": str(port), "MINISTACK_PARENT_PID": "0"},
            stdout=log,
            stderr=subprocess.STDOUT,
            cwd=REPO_ROOT,
        )
    try:
        _wait_health(port)
        assert proc.poll() is None
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=30) == 0
    finally:
        _stop(proc)
