from __future__ import annotations

import os
import sys
import types
from pathlib import Path
from unittest.mock import patch

try:
    import mysql.connector  # type: ignore[import-not-found]
except ModuleNotFoundError:
    mysql_module = types.ModuleType("mysql")
    connector_module = types.ModuleType("mysql.connector")
    mysql_module.connector = connector_module
    sys.modules.update({"mysql": mysql_module, "mysql.connector": connector_module})

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "loader_core"))

from partition_loader import (  # noqa: E402
    CONTROL_MIGRATIONS_DIRECTORY,
    CONTROL_SCHEMA_SQL,
    control_migration,
    control_schema_statements,
)


def test_control_table_creation_is_external_and_complete() -> None:
    with patch.dict(os.environ, {"CONTROL_DATABASE": "stream_db"}, clear=False):
        statements = control_schema_statements()
    assert len(statements) == 5
    sql = "\n".join(statements)
    assert "CREATE DATABASE IF NOT EXISTS `stream_db`" in sql
    assert "CREATE TABLE IF NOT EXISTS `stream_db`.object_storage_mappings" in sql
    assert "CREATE TABLE IF NOT EXISTS `stream_db`.target_batch_sequences" in sql
    assert "CREATE TABLE IF NOT EXISTS `stream_db`.source_object_batches" in sql
    assert "CREATE TABLE IF NOT EXISTS `stream_db`.deployment_history" in sql
    assert "__CONTROL_DATABASE__" not in sql
    assert CONTROL_SCHEMA_SQL.is_file()


def test_control_compatibility_migrations_are_external() -> None:
    paths = sorted(CONTROL_MIGRATIONS_DIRECTORY.glob("*.sql"))
    with patch.dict(os.environ, {"CONTROL_DATABASE": "stream_db"}, clear=False):
        migrations = [control_migration(path) for path in paths]
    assert {column for column, _ in migrations} == {
        "worker_threads",
        "event_rule_id",
        "stream_id",
        "processing_mode",
    }
    assert all(statement.startswith("ALTER TABLE `stream_db`.object_storage_mappings") for _, statement in migrations)
