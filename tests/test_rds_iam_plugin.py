import io
import logging
import tarfile
import types

from ministack.services import rds as rds_service
from ministack.services import rds_iam_plugin as plugin


class FakeCursor:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.executed = []
        self.closed = False

    def execute(self, statement, params=None):
        self.executed.append((statement, params))

    def fetchone(self):
        return next(self.rows)

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, rows):
        self.cursor_value = FakeCursor(rows)
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def close(self):
        self.closed = True


class FakeContainer:
    def __init__(self, arch="amd64", put_result=True):
        self.image = types.SimpleNamespace(attrs={"Architecture": arch})
        self.put_result = put_result
        self.archives = []

    def put_archive(self, path, data):
        self.archives.append((path, data))
        return self.put_result


def _write_artifact(tmp_path, series="8.0", arch="amd64"):
    artifact = tmp_path / series / arch / plugin.PLUGIN_FILE
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"plugin-payload")
    return artifact


def test_rds_iam_plugin_absent_is_silent_and_does_not_connect(
    monkeypatch, tmp_path, caplog,
):
    monkeypatch.setattr(plugin, "DEFAULT_ARTIFACT_ROOT", str(tmp_path))
    connections = []

    with caplog.at_level(logging.WARNING, logger="rds"):
        installed = plugin.ensure_iam_auth_plugin(
            FakeContainer(),
            lambda: connections.append(True),
            "8.0",
            "db-1",
        )

    assert installed is False
    assert connections == []
    assert caplog.records == []


def test_rds_iam_plugin_unsupported_series_avoids_docker_work(
    monkeypatch, tmp_path,
):
    _write_artifact(tmp_path, series="8.0")
    monkeypatch.setattr(plugin, "DEFAULT_ARTIFACT_ROOT", str(tmp_path))

    assert plugin.iam_auth_plugin_enabled("8.0") is True
    assert plugin.iam_auth_plugin_enabled("5.7") is False


def test_rds_iam_plugin_installs_matching_series_and_arch(monkeypatch, tmp_path):
    _write_artifact(tmp_path, series="8.4", arch="arm64")
    monkeypatch.setattr(plugin, "DEFAULT_ARTIFACT_ROOT", str(tmp_path))
    container = FakeContainer(arch="aarch64")
    connection = FakeConnection(
        [None, None, ("/usr/lib64/mysql/plugin",), (plugin.PLUGIN_NAME,)]
    )

    installed = plugin.ensure_iam_auth_plugin(
        container,
        lambda: connection,
        "8.4",
        "cluster-1",
    )

    assert installed is True
    assert connection.closed is True
    assert connection.cursor_value.closed is True
    assert connection.cursor_value.executed == [
        ("SET SESSION sql_log_bin = 0", None),
        (
            "SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS "
            "WHERE PLUGIN_NAME = %s AND PLUGIN_STATUS = 'ACTIVE'",
            (plugin.PLUGIN_NAME,),
        ),
        (
            "SELECT name, dl FROM mysql.plugin WHERE name = %s",
            (plugin.PLUGIN_NAME,),
        ),
        ("SELECT @@plugin_dir", None),
        (
            "INSTALL PLUGIN AWSAuthenticationPlugin "
            "SONAME 'aws_auth_plugin.so'",
            None,
        ),
        (
            "SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS "
            "WHERE PLUGIN_NAME = %s AND PLUGIN_STATUS = 'ACTIVE'",
            (plugin.PLUGIN_NAME,),
        ),
    ]
    assert len(container.archives) == 1
    archive_path, archive_data = container.archives[0]
    assert archive_path == "/usr/lib64/mysql/plugin"
    with tarfile.open(fileobj=io.BytesIO(archive_data), mode="r:") as archive:
        member = archive.getmember(plugin.PLUGIN_FILE)
        assert member.mode == 0o755
        assert archive.extractfile(member).read() == b"plugin-payload"


def test_rds_iam_plugin_is_idempotent(monkeypatch, tmp_path):
    _write_artifact(tmp_path)
    monkeypatch.setattr(plugin, "DEFAULT_ARTIFACT_ROOT", str(tmp_path))
    container = FakeContainer()
    connection = FakeConnection([("AWSAuthenticationPlugin",)])

    installed = plugin.ensure_iam_auth_plugin(
        container,
        lambda: connection,
        "8.0",
        "db-1",
    )

    assert installed is True
    assert container.archives == []
    assert connection.cursor_value.executed == [
        ("SET SESSION sql_log_bin = 0", None),
        (
            "SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS "
            "WHERE PLUGIN_NAME = %s AND PLUGIN_STATUS = 'ACTIVE'",
            (plugin.PLUGIN_NAME,),
        ),
    ]


def test_rds_iam_plugin_recovers_boot_failed_registration(
    monkeypatch, tmp_path, caplog,
):
    _write_artifact(tmp_path)
    monkeypatch.setattr(plugin, "DEFAULT_ARTIFACT_ROOT", str(tmp_path))
    container = FakeContainer()
    connection = FakeConnection(
        [
            None,
            (plugin.PLUGIN_NAME, plugin.PLUGIN_FILE),
            ("/usr/lib64/mysql/plugin",),
            (plugin.PLUGIN_NAME,),
        ]
    )
    original_execute = connection.cursor_value.execute

    def execute(statement, params=None):
        original_execute(statement, params)
        if statement == f"UNINSTALL PLUGIN {plugin.PLUGIN_NAME}":
            raise RuntimeError("plugin is registered but not loaded")

    connection.cursor_value.execute = execute

    with caplog.at_level(logging.WARNING, logger="rds"):
        assert plugin.ensure_iam_auth_plugin(
            container,
            lambda: connection,
            "8.0",
            "db-1",
        ) is True

    statements = connection.cursor_value.executed
    assert statements[0] == ("SET SESSION sql_log_bin = 0", None)
    assert statements.index(
        (f"UNINSTALL PLUGIN {plugin.PLUGIN_NAME}", None)
    ) < statements.index(
        ("DELETE FROM mysql.plugin WHERE name = %s", (plugin.PLUGIN_NAME,))
    )
    assert statements.index(
        ("DELETE FROM mysql.plugin WHERE name = %s", (plugin.PLUGIN_NAME,))
    ) < statements.index(
        (
            "INSTALL PLUGIN AWSAuthenticationPlugin "
            "SONAME 'aws_auth_plugin.so'",
            None,
        )
    )
    assert "removing mysql.plugin row" in caplog.text


def test_rds_iam_plugin_failure_warns_once_and_does_not_escape(
    monkeypatch, tmp_path, caplog,
):
    _write_artifact(tmp_path)
    monkeypatch.setattr(plugin, "DEFAULT_ARTIFACT_ROOT", str(tmp_path))
    container = FakeContainer(put_result=False)
    connection = FakeConnection(
        [None, None, ("/usr/lib64/mysql/plugin",)]
    )

    with caplog.at_level(logging.WARNING, logger="rds"):
        installed = plugin.ensure_iam_auth_plugin(
            container,
            lambda: connection,
            "8.0",
            "db-1",
        )

    assert installed is False
    warnings = [
        record for record in caplog.records
        if "failed to install AWSAuthenticationPlugin" in record.message
    ]
    assert len(warnings) == 1


def test_mysql_compatibility_wait_resolves_container_before_procedures(
    monkeypatch, tmp_path,
):
    monkeypatch.setattr(plugin, "DEFAULT_ARTIFACT_ROOT", str(tmp_path))
    connection = object()
    calls = []

    class ReadyContainer:
        status = "running"

        def reload(self):
            calls.append("reload")

    container = ReadyContainer()

    class Containers:
        def get(self, container_id):
            calls.append(("get", container_id))
            return container

    docker_client = types.SimpleNamespace(containers=Containers())

    def wait_for_ready(*args):
        calls.append("wait")
        assert args[-1]() is True
        return True

    def ensure_procedures(
        connection_factory, resource_id, engine, engine_series,
    ):
        calls.append(("procedures", resource_id))
        assert engine == "aurora-mysql"
        assert engine_series == "8.0"
        assert connection_factory() is connection
        return True

    monkeypatch.setattr(rds_service, "_get_docker", lambda: docker_client)
    monkeypatch.setattr(rds_service, "_wait_for_database_ready", wait_for_ready)
    monkeypatch.setattr(
        rds_service,
        "_mysql_endpoint_admin_connection",
        lambda *_args: connection,
    )
    monkeypatch.setattr(
        rds_service,
        "ensure_rds_compatibility_procedures",
        ensure_procedures,
    )

    assert rds_service._ensure_mysql_compatibility(
        "container-1",
        "127.0.0.1",
        3306,
        "password",
        "8.0.mysql_aurora.3.10.3",
        "db-1",
        wait_for_ready=True,
    ) == (True, False)
    assert calls == [
        ("get", "container-1"),
        ("procedures", "db-1"),
        "wait",
        "reload",
    ]


def test_mariadb_compatibility_skips_plugin_and_roles(monkeypatch):
    calls = []

    def enabled(engine_series):
        calls.append(("plugin-enabled", engine_series))
        return True

    def ensure_procedures(
        _connection_factory, resource_id, engine, engine_series,
    ):
        calls.append(("procedures", resource_id, engine, engine_series))
        return True

    monkeypatch.setattr(rds_service, "iam_auth_plugin_enabled", enabled)
    monkeypatch.setattr(
        rds_service,
        "ensure_rds_compatibility_procedures",
        ensure_procedures,
    )

    assert rds_service._ensure_mysql_compatibility(
        None,
        "127.0.0.1",
        3306,
        "password",
        "10.6.14",
        "db-1",
        engine="mariadb",
    ) == (True, False)
    assert calls == [("procedures", "db-1", "mariadb", None)]
