# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
"""Shared capability gate for MiniStack's optional Docker data plane."""

import os


def docker_enabled() -> bool:
    """Whether production code may import or connect to Docker.

    Docker remains enabled by default for backwards compatibility.  Explicitly
    setting ``MINISTACK_DOCKER_ENABLED`` to a conventional false value disables
    every Docker access point, including cleanup and background reapers.
    """
    return os.environ.get("MINISTACK_DOCKER_ENABLED", "1").strip().lower() not in {
        "0", "false", "no", "off", "disabled",
    }


def docker_available() -> bool:
    """Return whether an enabled, reachable Docker daemon is available.

    Docker is an optional test/runtime capability.  Keep the import and client
    construction behind the feature gate, and turn every client/daemon error
    into a simple negative result so callers can use this for skip conditions.
    """
    if not docker_enabled():
        return False
    try:
        import docker

        client = docker.from_env(timeout=5)
        try:
            client.ping()
        finally:
            close = getattr(client, "close", None)
            if close is not None:
                close()
        return True
    except Exception:
        return False
