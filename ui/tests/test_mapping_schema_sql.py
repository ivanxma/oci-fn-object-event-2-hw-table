from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from myapp.services.mapping_service import RETIRED_OBJECT_CLEANUP_SQL, MAPPING_SCHEMA_SQL, MappingService, _migration_column, _sql_statements


class Cursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def execute(self, statement, parameters=None) -> None:
        self.executed.append((statement, parameters))

    def fetchone(self):
        return (1,)


class MappingSchemaSqlTest(unittest.TestCase):
    def test_control_schema_is_external_and_uses_validated_identifier(self) -> None:
        statements = _sql_statements(MAPPING_SCHEMA_SQL, "fndb")
        self.assertEqual(len(statements), 2)
        self.assertTrue(statements[0].startswith("CREATE DATABASE IF NOT EXISTS `fndb`"))
        self.assertIn("CREATE TABLE IF NOT EXISTS `fndb`.object_storage_mappings", statements[1])
        self.assertNotIn("__CONTROL_DATABASE__", "\n".join(statements))

    def test_legacy_function_objects_are_retired_by_external_sql(self) -> None:
        statements = _sql_statements(RETIRED_OBJECT_CLEANUP_SQL, "fndb")
        self.assertEqual(len(statements), 4)
        script = "\n".join(statements)
        self.assertIn("DROP COLUMN `invocation_mode`", script)
        self.assertIn("DROP TABLE IF EXISTS `fndb`.object_event", script)
        self.assertIn("DROP TABLE IF EXISTS `fndb`.event_tx_log", script)
        self.assertIn("DROP TABLE IF EXISTS `fndb`.event_errors", script)

    def test_all_migrations_are_external_single_statement_scripts(self) -> None:
        migrations = sorted(MAPPING_SCHEMA_SQL.parent.joinpath("mapping_migrations").glob("*.sql"))
        self.assertEqual(len(migrations), 4)
        self.assertEqual({_migration_column(path) for path in migrations}, {"worker_threads", "event_rule_id", "stream_id", "processing_mode"})
        for migration in migrations:
            self.assertEqual(len(_sql_statements(migration, "fndb")), 1)

    def test_schema_runner_executes_external_create_scripts_and_skips_existing_migrations(self) -> None:
        cursor = Cursor()
        with patch.dict(os.environ, {"CONTROL_DATABASE": "fndb"}, clear=False):
            MappingService(mysql=None)._ensure_schema(cursor)
        self.assertEqual(sum("CREATE " in statement for statement, _ in cursor.executed), 2)
        self.assertEqual(sum("information_schema.columns" in statement for statement, _ in cursor.executed), 5)
        cleanup = _sql_statements(RETIRED_OBJECT_CLEANUP_SQL, "fndb")
        self.assertEqual(len(cleanup), 4)
        self.assertTrue(all(statement in [sql for sql, _ in cursor.executed] for statement in cleanup))


if __name__ == "__main__":
    unittest.main()
