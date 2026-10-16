"""RDS reboot lifecycle; direct handler tests do not require a running server."""
import threading
import time
from types import SimpleNamespace

import pytest

from ministack.services import rds


@pytest.fixture
def instance(monkeypatch):
    monkeypatch.setattr(rds, "_single_instance_response", lambda *args: args[-1].copy())
    value = {
        "DBInstanceIdentifier": "reboot-test", "DBInstanceStatus": "available",
        "Engine": "mysql", "MasterUsername": "root", "_MasterUserPassword": "test",
        "_docker_container_id": "owned", "_HostPort": 13306,
        "Endpoint": {"Address": "localhost", "Port": 13306},
    }
    rds._instances["reboot-test"] = value
    yield value
    rds._instances.pop("reboot-test", None)


def test_reboot_is_async_and_waits_for_sql(monkeypatch, instance):
    entered = threading.Event()
    release = threading.Event()
    done = threading.Event()
    calls = []
    class Container:
        status = "running"
        def stop(self, timeout):
            calls.append("stop")
        def start(self):
            calls.append("start")
        def reload(self):
            pass
    monkeypatch.setattr(rds, "_get_docker", lambda: SimpleNamespace(containers=SimpleNamespace(get=lambda ident: Container())))
    def ready(*args):
        calls.append("ready")
        entered.set()
        assert release.wait(5)
        return True
    monkeypatch.setattr(rds, "_wait_for_database_ready", ready)
    original = rds._finish_instance_reboot
    def finish(*args):
        try:
            original(*args)
        finally:
            done.set()
    monkeypatch.setattr(rds, "_finish_instance_reboot", finish)
    try:
        rds._reboot_db_instance({"DBInstanceIdentifier": "reboot-test"})
        assert entered.wait(5)
        assert instance["DBInstanceStatus"] == "rebooting"
        assert calls == ["stop", "start", "ready"]
    finally:
        release.set()
        assert done.wait(5)
    assert instance["DBInstanceStatus"] == "available"


@pytest.mark.data_plane
def test_reboot_preserves_mysql_data_and_breaks_sessions(monkeypatch, instance):
    import docker
    import pymysql

    import socket
    client = docker.from_env()
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    container = client.containers.run(
        "mysql:8.0", detach=True, ports={"3306/tcp": ("127.0.0.1", port)},
        environment={"MYSQL_ROOT_PASSWORD": "test", "MYSQL_ROOT_HOST": "%"},
    )
    connection = None
    try:
        container.reload()
        port = int(container.attrs["NetworkSettings"]["Ports"]["3306/tcp"][0]["HostPort"])
        instance["_HostPort"] = port
        instance["_docker_container_id"] = container.id
        def connect():
            return pymysql.connect(host="127.0.0.1", port=port, user="root", password="test", connect_timeout=2, read_timeout=2, autocommit=True)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            try:
                connection = connect()
                break
            except pymysql.Error:
                time.sleep(0.5)
        assert connection is not None
        with connection.cursor() as cursor:
            cursor.execute("CREATE DATABASE reboot_test")
            cursor.execute("CREATE TABLE reboot_test.evidence (value INT)")
            cursor.execute("INSERT INTO reboot_test.evidence VALUES (42)")
        monkeypatch.setattr(rds, "_get_docker", lambda: client)
        monkeypatch.setenv("RDS_REBOOT_DOWNTIME_SECONDS", "2")
        response = rds._reboot_db_instance({"DBInstanceIdentifier": "reboot-test"})
        assert response["DBInstanceStatus"] == "rebooting"
        deadline = time.monotonic() + 90
        while instance["DBInstanceStatus"] == "rebooting" and time.monotonic() < deadline:
            time.sleep(0.2)
        assert instance["DBInstanceStatus"] == "available"
        with pytest.raises(pymysql.Error):
            connection.ping(reconnect=False)
        with connect() as recovered:
            with recovered.cursor() as cursor:
                cursor.execute("SELECT value FROM reboot_test.evidence")
                assert cursor.fetchone() == (42,)
    finally:
        if connection:
            connection.close()
        container.remove(force=True, v=True)


def test_downtime_is_between_stop_and_start(monkeypatch, instance):
    calls = []
    container = SimpleNamespace(stop=lambda **kw: calls.append("stop"), start=lambda: calls.append("start"))
    monkeypatch.setattr(rds, "_get_docker", lambda: SimpleNamespace(containers=SimpleNamespace(get=lambda ident: container)))
    monkeypatch.setattr(rds.time, "sleep", lambda seconds: calls.append(seconds))
    monkeypatch.setattr(rds, "_wait_for_database_ready", lambda *a: True)
    instance["DBInstanceStatus"] = "rebooting"
    rds._finish_instance_reboot(instance, None, 7)
    assert calls == ["stop", 7, "start"]
    assert instance["DBInstanceStatus"] == "available"


def test_failed_restart_does_not_report_available(monkeypatch, instance):
    def fail():
        raise RuntimeError("Docker unavailable")
    monkeypatch.setattr(rds, "_get_docker", fail)
    instance["DBInstanceStatus"] = "rebooting"
    rds._finish_instance_reboot(instance, None, 0)
    assert instance["DBInstanceStatus"] == "failed"


def test_deleted_instance_is_not_resurrected(monkeypatch, instance):
    replacement = dict(instance)
    def ready(*args):
        rds._instances["reboot-test"] = replacement
        return True
    container = SimpleNamespace(stop=lambda **kw: None, start=lambda: None)
    monkeypatch.setattr(rds, "_get_docker", lambda: SimpleNamespace(containers=SimpleNamespace(get=lambda ident: container)))
    monkeypatch.setattr(rds, "_wait_for_database_ready", ready)
    instance["DBInstanceStatus"] = "rebooting"
    rds._finish_instance_reboot(instance, None, 0)
    assert rds._instances["reboot-test"] is replacement
    assert instance["DBInstanceStatus"] == "rebooting"


@pytest.mark.parametrize("delay", ["-1", "nan", "inf", "bad", "3601"])
def test_invalid_downtime_is_rejected(monkeypatch, instance, delay):
    monkeypatch.setenv("RDS_REBOOT_DOWNTIME_SECONDS", delay)
    rds._reboot_db_instance({"DBInstanceIdentifier": "reboot-test"})
    assert instance["DBInstanceStatus"] == "available"
