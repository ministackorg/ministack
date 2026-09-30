# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
# Copies or substantial portions, including AI-assisted ports or rewrites, must retain this notice (see LICENSE).
"""Best-effort delivery of MiniStack's Aurora MySQL IAM auth plugin."""

import io
import logging
import os
import tarfile
from pathlib import Path

logger = logging.getLogger("rds")

PLUGIN_NAME = "AWSAuthenticationPlugin"
PLUGIN_FILE = "aws_auth_plugin.so"
DEFAULT_ARTIFACT_ROOT = "/opt/ministack/mysql-plugins"
# Read by aws_auth_plugin.cc on every IAM login.
CONFIG_PATH = "/etc/ministack/rds-iam.conf"


def iam_auth_plugin_enabled(engine_series=None):
    """Return whether a bundled artifact root merits Docker I/O."""
    root = Path(DEFAULT_ARTIFACT_ROOT)
    if engine_series:
        root = root / engine_series
    return root.is_dir()


def _container_arch(container):
    image = getattr(container, "image", None)
    attrs = getattr(image, "attrs", {}) if image is not None else {}
    arch = str(attrs.get("Architecture", "")).lower()
    aliases = {
        "x86_64": "amd64",
        "aarch64": "arm64",
        "arm64/v8": "arm64",
    }
    return aliases.get(arch, arch)


def _artifact_path(engine_series, container):
    root = Path(DEFAULT_ARTIFACT_ROOT)
    arch = _container_arch(container)
    if not engine_series or not arch:
        return None
    artifact = root / engine_series / arch / PLUGIN_FILE
    return artifact if artifact.is_file() else None


def _archive(artifact):
    payload = artifact.read_bytes()
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        member = tarfile.TarInfo(PLUGIN_FILE)
        member.mode = 0o755
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))
    return stream.getvalue()


def ensure_iam_auth_plugin(
    container,
    connection_factory,
    engine_series,
    resource_id,
):
    """Install the matching artifact, returning True only when it is loaded.

    Missing artifacts preserve stock behavior silently.
    Once an artifact is selected, every failure is reported once and swallowed
    so plugin fidelity can never fail RDS provisioning.
    """
    artifact = _artifact_path(engine_series, container)
    if artifact is None:
        return False

    connection = None
    cursor = None
    try:
        connection = connection_factory()
        if connection is None:
            raise RuntimeError("admin connection is unavailable")
        cursor = connection.cursor()
        cursor.execute("SET SESSION sql_log_bin = 0")
        cursor.execute(
            "SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS "
            "WHERE PLUGIN_NAME = %s AND PLUGIN_STATUS = 'ACTIVE'",
            (PLUGIN_NAME,),
        )
        if cursor.fetchone():
            return True

        cursor.execute(
            "SELECT name, dl FROM mysql.plugin WHERE name = %s",
            (PLUGIN_NAME,),
        )
        stale_registration = cursor.fetchone()

        cursor.execute("SELECT @@plugin_dir")
        plugin_dir_row = cursor.fetchone()
        if not plugin_dir_row or not plugin_dir_row[0]:
            raise RuntimeError("MySQL returned an empty plugin_dir")
        if not container.put_archive(plugin_dir_row[0], _archive(artifact)):
            raise RuntimeError("Docker rejected the plugin archive")

        if stale_registration:
            try:
                cursor.execute(f"UNINSTALL PLUGIN {PLUGIN_NAME}")
            except Exception as e:
                logger.warning(
                    "RDS: failed to uninstall stale %s registration for "
                    "%s; removing mysql.plugin row: %s",
                    PLUGIN_NAME,
                    resource_id,
                    e,
                )
                cursor.execute(
                    "DELETE FROM mysql.plugin WHERE name = %s",
                    (PLUGIN_NAME,),
                )

        cursor.execute(
            f"INSTALL PLUGIN {PLUGIN_NAME} SONAME '{PLUGIN_FILE}'"
        )
        cursor.execute(
            "SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS "
            "WHERE PLUGIN_NAME = %s AND PLUGIN_STATUS = 'ACTIVE'",
            (PLUGIN_NAME,),
        )
        if not cursor.fetchone():
            raise RuntimeError("plugin did not become ACTIVE after installation")
        logger.info(
            "RDS: installed %s for %s (MySQL %s, %s)",
            PLUGIN_NAME,
            resource_id,
            engine_series,
            _container_arch(container),
        )
        return True
    except Exception as e:
        logger.warning(
            "RDS: failed to install %s for %s: %s",
            PLUGIN_NAME,
            resource_id,
            e,
        )
        return False
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


def configure_iam_auth_broker(container, resource_kind, resource_identifier, broker_host, broker_port):
    """Point the loaded plugin at MiniStack's broker with a fresh capability.

    Re-issuing rotates the capability, so every ready path calls this again.
    """
    from ministack.core import rds_iam
    from ministack.core.responses import get_account_id, get_region

    try:
        capability = rds_iam.issue_capability(
            account_id=get_account_id(), region=get_region(),
            resource_kind=resource_kind, resource_identifier=resource_identifier,
        )
        config = f"{broker_host} {broker_port} {capability}\n".encode()
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w") as archive:
            directory = tarfile.TarInfo(os.path.dirname(CONFIG_PATH).lstrip("/"))
            directory.type = tarfile.DIRTYPE
            directory.mode = 0o755
            archive.addfile(directory)
            member = tarfile.TarInfo(CONFIG_PATH.lstrip("/"))
            member.mode = 0o644
            member.size = len(config)
            archive.addfile(member, io.BytesIO(config))
        if not container.put_archive("/", stream.getvalue()):
            raise RuntimeError("Docker rejected the plugin config")
        return True
    except Exception as e:
        logger.warning("RDS: failed to configure %s for %s: %s", PLUGIN_NAME, resource_identifier, e)
        return False


def get_state() -> dict:
    return {}


def load_persisted_state(data: dict) -> None:
    pass


def reset() -> None:
    pass
