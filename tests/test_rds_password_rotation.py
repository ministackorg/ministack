import sys
import types


def test_rds_mysql_readiness_probe_requires_successful_query(monkeypatch):
    """MySQL readiness requires an executable query, not only connection auth."""
    from ministack.services import rds

    attempts = []

    class FakeCursor:
        def execute(self, sql, params=None):
            attempts.append(sql)
            raise RuntimeError("(2013, 'Lost connection to MySQL server during query')")

        def close(self):
            attempts.append("cursor.close")

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            attempts.append("connection.close")

    def connect(**kwargs):
        return FakeConnection()

    monkeypatch.setitem(sys.modules, "pymysql", types.SimpleNamespace(connect=connect))

    assert not rds._try_database_connect(
        "127.0.0.1",
        3306,
        "aurora-mysql",
        "admin",
        "old_pass",
        None,
    )
    assert attempts == ["SELECT 1", "cursor.close", "connection.close"]


def test_rds_cluster_password_rotation_alters_mysql_password(monkeypatch):
    """Password rotation assumes the instance already passed readiness."""
    from ministack.services import rds

    attempts = []

    class FakeCursor:
        def execute(self, sql, params=None):
            attempts.append((sql, params))

        def close(self):
            attempts.append(("cursor.close", None))

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            attempts.append(("connection.close", None))

    def connect(**kwargs):
        attempts.append(("connect", kwargs))
        return FakeConnection()

    monkeypatch.setitem(sys.modules, "pymysql", types.SimpleNamespace(connect=connect))
    instances = rds.AccountRegionScopedDict()
    instances["pw-retry-instance"] = {
        "DBClusterIdentifier": "pw-retry-cluster",
        "Engine": "aurora-mysql",
        "_internal_address": "127.0.0.1",
        "_internal_port": 3306,
    }
    monkeypatch.setattr(rds, "_instances", instances)

    assert rds._rotate_real_password(
        {"DBClusterIdentifier": "pw-retry-cluster"},
        "old_pass",
        "new_pass",
    )
    assert (
        "ALTER USER 'root'@'%%' IDENTIFIED BY %s",
        ("new_pass",),
    ) in attempts
