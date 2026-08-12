import io
import logging
import tarfile
import types

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
    monkeypatch.setenv("MINISTACK_MYSQL_IAM_PLUGIN_DIR", str(tmp_path))
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


def test_rds_iam_plugin_off_preserves_stock_behavior(monkeypatch, tmp_path):
    _write_artifact(tmp_path)
    monkeypatch.setenv("MINISTACK_MYSQL_IAM_PLUGIN_DIR", str(tmp_path))
    monkeypatch.setenv("MINISTACK_MYSQL_IAM_AUTH", "off")
    connections = []

    installed = plugin.ensure_iam_auth_plugin(
        FakeContainer(),
        lambda: connections.append(True),
        "8.0",
        "db-1",
    )

    assert installed is False
    assert connections == []


def test_rds_iam_plugin_unsupported_series_avoids_docker_work(
    monkeypatch, tmp_path,
):
    _write_artifact(tmp_path, series="8.0")
    monkeypatch.setenv("MINISTACK_MYSQL_IAM_PLUGIN_DIR", str(tmp_path))

    assert plugin.iam_auth_plugin_enabled("8.0") is True
    assert plugin.iam_auth_plugin_enabled("5.7") is False


def test_rds_iam_plugin_installs_matching_series_and_arch(monkeypatch, tmp_path):
    _write_artifact(tmp_path, series="8.4", arch="arm64")
    monkeypatch.setenv("MINISTACK_MYSQL_IAM_PLUGIN_DIR", str(tmp_path))
    container = FakeContainer(arch="aarch64")
    connection = FakeConnection([None, ("/usr/lib64/mysql/plugin",)])

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
        (
            "SELECT PLUGIN_NAME FROM INFORMATION_SCHEMA.PLUGINS "
            "WHERE PLUGIN_NAME = %s",
            (plugin.PLUGIN_NAME,),
        ),
        ("SELECT @@plugin_dir", None),
        (
            "INSTALL PLUGIN AWSAuthenticationPlugin "
            "SONAME 'aws_auth_plugin.so'",
            None,
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
    monkeypatch.setenv("MINISTACK_MYSQL_IAM_PLUGIN_DIR", str(tmp_path))
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
    assert len(connection.cursor_value.executed) == 1


def test_rds_iam_plugin_failure_warns_once_and_does_not_escape(
    monkeypatch, tmp_path, caplog,
):
    _write_artifact(tmp_path)
    monkeypatch.setenv("MINISTACK_MYSQL_IAM_PLUGIN_DIR", str(tmp_path))
    container = FakeContainer(put_result=False)
    connection = FakeConnection([None, ("/usr/lib64/mysql/plugin",)])

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


def test_rds_iam_plugin_unknown_mode_warns_once_and_uses_auto(
    monkeypatch, tmp_path, caplog,
):
    _write_artifact(tmp_path)
    monkeypatch.setenv("MINISTACK_MYSQL_IAM_PLUGIN_DIR", str(tmp_path))
    monkeypatch.setenv("MINISTACK_MYSQL_IAM_AUTH", "future-mode")
    plugin._WARNED_MODES.clear()

    with caplog.at_level(logging.WARNING, logger="rds"):
        assert plugin.iam_auth_plugin_enabled() is True
        assert plugin.iam_auth_plugin_enabled() is True

    warnings = [
        record for record in caplog.records
        if "unknown MINISTACK_MYSQL_IAM_AUTH" in record.message
    ]
    assert len(warnings) == 1
