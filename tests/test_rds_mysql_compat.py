import logging

from ministack.services import rds_mysql_compat as compat


class FakeCursor:
    def __init__(self, routines=()):
        self.routines = routines
        self.executed = []
        self.closed = False

    def execute(self, statement, params=None):
        self.executed.append((statement, params))

    def fetchall(self):
        return [(name,) for name in self.routines]

    def close(self):
        self.closed = True


class FakeConnection:
    def __init__(self, routines=()):
        self.cursor_value = FakeCursor(routines)
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def close(self):
        self.closed = True


def test_rds_compatibility_procedures_create_complete_family():
    connection = FakeConnection()

    assert compat.ensure_rds_compatibility_procedures(
        lambda: connection,
        "cluster-1",
    ) is True

    statements = [statement for statement, _params in connection.cursor_value.executed]
    assert statements[0] == "SET SESSION sql_log_bin = 0"
    assert statements[1] == compat._CONFIG_TABLE_SQL
    assert statements[2] == compat._CONFIG_DEFAULT_SQL
    assert statements[3:5] == [
        f"CREATE ROLE IF NOT EXISTS `{role}`@'%'"
        for role in compat._PREDEFINED_ROLES
    ]
    assert statements[5].startswith("SELECT ROUTINE_NAME")
    assert statements[6:] == list(compat._PROCEDURES.values())
    assert connection.cursor_value.closed is True
    assert connection.closed is True


def test_rds_compatibility_procedures_are_idempotent():
    connection = FakeConnection(compat._PROCEDURES)

    assert compat.ensure_rds_compatibility_procedures(
        lambda: connection,
        "cluster-1",
    ) is True

    statements = [statement for statement, _params in connection.cursor_value.executed]
    assert statements[:3] == [
        "SET SESSION sql_log_bin = 0",
        compat._CONFIG_TABLE_SQL,
        compat._CONFIG_DEFAULT_SQL,
    ]
    assert statements[3:5] == [
        f"CREATE ROLE IF NOT EXISTS `{role}`@'%'"
        for role in compat._PREDEFINED_ROLES
    ]
    assert len(statements) == 6
    assert statements[5].startswith("SELECT ROUTINE_NAME")


def test_rds_compatibility_procedure_failure_warns_and_does_not_escape(caplog):
    def fail_connection():
        raise RuntimeError("database unavailable")

    with caplog.at_level(logging.WARNING, logger="rds"):
        assert compat.ensure_rds_compatibility_procedures(
            fail_connection,
            "cluster-1",
        ) is False

    assert len(caplog.records) == 1
    assert "failed to ensure Aurora compatibility objects" in caplog.text
