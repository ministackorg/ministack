# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""MINISTACK_PARENT_PID: a foreground MiniStack stops when the named process exits."""

import os
import signal
import socket
import subprocess
import sys
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
    env = {**os.environ, "GATEWAY_PORT": str(port), "MINISTACK_PARENT_PID": parent_pid, "LOG_LEVEL": "INFO"}
    with open(log_path, "w") as log:
        return subprocess.Popen(
            [sys.executable, "-m", "ministack"],
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
        # The graceful path: lifespan shutdown is what removes MiniStack's containers.
        assert "MiniStack shutting down" in log
    finally:
        _stop(proc)
        _stop(parent)


@pytest.mark.serial
@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_signals_still_shut_down_gracefully_with_a_parent_pid(tmp_path, signum):
    """Hypercorn only installs its signal handlers when no shutdown trigger is
    given, so the trigger must handle SIGTERM/SIGINT itself or the lifespan
    shutdown, which removes the containers, is skipped."""
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
        assert "MINISTACK_PARENT_PID" in (tmp_path / "ministack.log").read_text()
    finally:
        _stop(proc)
