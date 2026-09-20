# Copyright (c) 2026 MiniStack Contributors. SPDX-License-Identifier: MIT
"""Pure unit tests; run with unittest to avoid the integration reset fixture."""

import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from ministack.services import rds_data


class ExecuteStatementRowCountsTest(unittest.TestCase):
    def execute(self, engine, count, status=None, rows=None, sql="SELECT 1",
                missing_status=False):
        cursor = SimpleNamespace(
            rowcount=count,
            description=[("value", 23, None, None, None, None, None)] if rows is not None else None,
            execute=Mock(), fetchall=Mock(return_value=rows), close=Mock(),
        )
        if not missing_status:
            cursor.statusmessage = status
        conn = Mock()
        conn.cursor.return_value = cursor
        with patch.object(rds_data, "_resolve_target", return_value=({"DBInstanceIdentifier": "unit"}, engine, {})), \
             patch.object(rds_data, "_validate_http_endpoint_enabled", return_value=None), \
             patch.object(rds_data, "_require_secret_credentials", return_value=("user", "password", None)), \
             patch.object(rds_data, "_has_real_endpoint", return_value=True), \
             patch.object(rds_data, "_connect", return_value=conn):
            code, _, body = rds_data._execute_statement({
                "resourceArn": "resource", "secretArn": "secret", "sql": sql,
                "includeResultMetadata": True,
            })
        self.assertEqual(code, 200, body)
        cursor.execute.assert_called_once_with(sql, None)
        cursor.close.assert_called_once()
        conn.close.assert_called_once()
        return json.loads(body)

    def test_select(self):
        for engine in ("postgres", "aurora-postgresql"):
            for rows in ([], [(1,)], [(1,), (2,)]):
                with self.subTest(engine=engine, rows=rows):
                    result = self.execute(engine, len(rows), f"SELECT {len(rows)}", rows,
                                          sql="/* comment */ WITH x AS (SELECT 1) SELECT * FROM x")
                    self.assertEqual(result["numberOfRecordsUpdated"], 0)
                    self.assertEqual(result["records"], [[{"longValue": row[0]}] for row in rows])
                    self.assertEqual(result["columnMetadata"][0]["name"], "value")

    def test_dml_with_and_without_returning(self):
        for command in ("INSERT", "UPDATE", "DELETE"):
            for count in (0, 2):
                for returning in (False, True):
                    with self.subTest(command=command, count=count, returning=returning):
                        rows = [(1,)] * count if returning else None
                        tag = f"INSERT 0 {count}" if command == "INSERT" else f"{command} {count}"
                        result = self.execute("postgres", count, tag, rows)
                        self.assertEqual(result["numberOfRecordsUpdated"], count)
                        self.assertEqual(len(result["records"]), count if returning else 0)

    def test_unknown_missing_and_malformed_tags_preserve_count(self):
        for status in (None, "", "   ", 123, b"SELECT 2", "SELECTED 2", "select 2", "CREATE TABLE AS", "FETCH 2"):
            with self.subTest(status=status):
                self.assertEqual(self.execute("postgres", 2, status)["numberOfRecordsUpdated"], 2)
        self.assertEqual(self.execute("postgres", 2, missing_status=True)["numberOfRecordsUpdated"], 2)

    def test_negative_rowcount_is_clamped(self):
        for engine in ("postgres", "aurora-postgresql", "mysql"):
            self.assertEqual(self.execute(engine, -1)["numberOfRecordsUpdated"], 0)

    def test_mysql_is_unchanged(self):
        for engine in ("mysql", "aurora-mysql", "mariadb"):
            self.assertEqual(self.execute(engine, 2, "SELECT 2", [(1,), (2,)])["numberOfRecordsUpdated"], 2)


if __name__ == "__main__":
    unittest.main()
