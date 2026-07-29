"""Persistence for Object Storage resource-to-table mappings in ``fndb``."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

from .naming import quote_identifier, validate_identifier


MAPPING_TABLE = "object_storage_mappings"
SQL_DIRECTORY = Path(__file__).resolve().parent.parent / "sql"
MAPPING_SCHEMA_SQL = SQL_DIRECTORY / "init_mapping_control.sql"
MAPPING_MIGRATIONS_DIRECTORY = SQL_DIRECTORY / "mapping_migrations"
_MIGRATION_COLUMN = re.compile(r"^\s*--\s*migration-column:\s*([A-Za-z_][A-Za-z0-9_]*)\s*$", re.MULTILINE)


def control_database() -> str:
    return validate_identifier(os.environ.get("CONTROL_DATABASE", "fndb"), "control database")


def _sql_statements(path: Path, database: str) -> tuple[str, ...]:
    """Load a tracked schema script after safely substituting its database identifier."""
    text = path.read_text(encoding="utf-8")
    text = text.replace("__CONTROL_DATABASE__", quote_identifier(database, "mapping database"))
    statements = tuple(
        statement.strip()
        for statement in re.sub(r"^\s*--.*$", "", text, flags=re.MULTILINE).split(";")
        if statement.strip()
    )
    if not statements:
        raise ValueError(f"Schema script {path.name} contains no SQL statements.")
    return statements


def _migration_column(path: Path) -> str:
    match = _MIGRATION_COLUMN.search(path.read_text(encoding="utf-8"))
    if not match:
        raise ValueError(f"Migration script {path.name} must declare '-- migration-column: <column>'.")
    return validate_identifier(match.group(1), "mapping migration column")


def _required_text(value: str | None, label: str, maximum: int) -> str:
    value = (value or "").strip()
    if not value:
        raise ValueError(f"{label} is required.")
    if len(value) > maximum:
        raise ValueError(f"{label} must be {maximum} characters or fewer.")
    return value


class MappingService:
    """CRUD operations for mappings, using the active server-side DB session."""

    def __init__(self, mysql) -> None:
        self.mysql = mysql

    @staticmethod
    def normalize(form: dict[str, Any]) -> dict[str, str]:
        """Validate browser input before it is used in a parameterized statement."""
        # The long-running Container Instance replaces the legacy detached
        # Function reinvocation path.  Keep the existing column for migration
        # compatibility, but do not create new mappings that depend on it.
        mode = (form.get("invocation_mode") or "SYNC").strip().upper()
        if mode != "SYNC":
            raise ValueError("Streaming consumer mappings must use SYNC loader mode; DETACHED Function mode is retired.")
        try:
            workers = int(form.get("worker_threads") or 4)
        except (TypeError, ValueError) as error:
            raise ValueError("Worker threads must be a whole number from 1 to 64.") from error
        if not 1 <= workers <= 64:
            raise ValueError("Worker threads must be from 1 to 64.")
        processing_mode = (form.get("processing_mode") or "FIFO").strip().upper()
        if processing_mode not in {"FIFO", "PARALLEL"}:
            raise ValueError("Processing mode must be FIFO or PARALLEL.")
        stream_id = _required_text(form.get("stream_id"), "OCI Stream", 255)
        if not stream_id.startswith("ocid1.stream."):
            raise ValueError("Stream identifier is invalid.")
        return {
            "compartment_name": _required_text(form.get("compartment_name"), "Compartment name", 255),
            "bucket_name": _required_text(form.get("bucket_name"), "Bucket name", 255),
            "resource_name_pattern": _required_text(form.get("resource_name_pattern"), "Resource name pattern", 1024),
            "target_database": validate_identifier((form.get("target_database") or "").strip(), "target database"),
            "target_table": validate_identifier((form.get("target_table") or "").strip().lstrip("."), "target table"),
            "invocation_mode": mode,
            "worker_threads": str(workers),
            "stream_id": stream_id,
            "processing_mode": processing_mode,
        }

    def _ensure_schema(self, cursor) -> None:
        database = control_database()
        for statement in _sql_statements(MAPPING_SCHEMA_SQL, database):
            cursor.execute(statement)
        for migration in sorted(MAPPING_MIGRATIONS_DIRECTORY.glob("*.sql")):
            column = _migration_column(migration)
            cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s", (database, MAPPING_TABLE, column))
            row = cursor.fetchone()
            count = row[0] if isinstance(row, tuple) else (next(iter(row.values())) if row else 0)
            if not row or not count:
                statements = _sql_statements(migration, database)
                if len(statements) != 1:
                    raise ValueError(f"Migration script {migration.name} must contain exactly one SQL statement.")
                cursor.execute(statements[0])

    def list_mappings(self) -> list[dict[str, Any]]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_schema(cursor)
            cursor.execute(
                f"SELECT id, compartment_name, bucket_name, resource_name_pattern, target_database, target_table, invocation_mode, worker_threads, event_rule_id, stream_id, processing_mode "
                f"FROM {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} ORDER BY compartment_name, bucket_name, resource_name_pattern"
            )
            return cursor.fetchall()

    def list_target_databases(self) -> list[str]:
        """Return databases available to the current authenticated connection."""
        return self.mysql.list_databases()

    def list_target_tables(self, database: str) -> list[str]:
        database = validate_identifier(database, "target database")
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """SELECT table_name FROM information_schema.tables
                   WHERE table_schema = %s AND table_type = 'BASE TABLE'
                   ORDER BY table_name""",
                (database,),
            )
            return [row[0] for row in cursor.fetchall()]

    def get_mapping(self, mapping_id: int) -> dict[str, Any] | None:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_schema(cursor)
            cursor.execute(
                f"SELECT id, compartment_name, bucket_name, resource_name_pattern, target_database, target_table, invocation_mode, worker_threads, event_rule_id, stream_id, processing_mode "
                f"FROM {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} WHERE id = %s",
                (mapping_id,),
            )
            return cursor.fetchone()

    def get_mapping_by_rule_id(self, rule_id: str) -> dict[str, Any] | None:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_schema(cursor)
            cursor.execute(
                f"SELECT id, compartment_name, bucket_name, resource_name_pattern, target_database, target_table, invocation_mode, worker_threads, event_rule_id, stream_id, processing_mode "
                f"FROM {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} WHERE event_rule_id = %s LIMIT 1",
                (rule_id,),
            )
            return cursor.fetchone()

    def add_mapping(self, values: dict[str, str]) -> int:
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            self._ensure_schema(cursor)
            cursor.execute(
                f"INSERT INTO {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} "
                "(compartment_name, bucket_name, resource_name_pattern, target_database, target_table, invocation_mode, worker_threads, stream_id, processing_mode) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                tuple(values[column] for column in ("compartment_name", "bucket_name", "resource_name_pattern", "target_database", "target_table", "invocation_mode", "worker_threads", "stream_id", "processing_mode")),
            )
            return int(cursor.lastrowid)

    def update_mapping(self, mapping_id: int, values: dict[str, str]) -> bool:
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            self._ensure_schema(cursor)
            cursor.execute(
                f"SELECT id FROM {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} WHERE id = %s",
                (mapping_id,),
            )
            if not cursor.fetchone():
                return False
            cursor.execute(
                f"UPDATE {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} "
                "SET compartment_name = %s, bucket_name = %s, resource_name_pattern = %s, target_database = %s, target_table = %s, invocation_mode = %s, worker_threads = %s, stream_id = %s, processing_mode = %s WHERE id = %s",
                (*tuple(values[column] for column in ("compartment_name", "bucket_name", "resource_name_pattern", "target_database", "target_table", "invocation_mode", "worker_threads", "stream_id", "processing_mode")), mapping_id),
            )
            return True

    def delete_mapping(self, mapping_id: int) -> bool:
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            self._ensure_schema(cursor)
            cursor.execute(
                f"DELETE FROM {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} WHERE id = %s",
                (mapping_id,),
            )
            return cursor.rowcount == 1

    def set_event_rule(self, mapping_id: int, rule_id: str | None) -> bool:
        """Store only the OCI rule identity; live rule details remain OCI-owned."""
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            self._ensure_schema(cursor)
            cursor.execute(
                f"UPDATE {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} "
                "SET event_rule_id = %s WHERE id = %s",
                (rule_id, mapping_id),
            )
            return cursor.rowcount == 1

    def clear_event_rule_reference(self, rule_id: str) -> int:
        """Clear mapping ownership after a rule has been removed from OCI."""
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            self._ensure_schema(cursor)
            cursor.execute(
                f"UPDATE {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} "
                "SET event_rule_id = NULL WHERE event_rule_id = %s",
                (rule_id,),
            )
            return int(cursor.rowcount)

    def exact_pattern_conflict(self, values: dict[str, str], *, exclude_mapping_id: int | None = None) -> int | None:
        """Return a conflicting exact pattern; broader wildcard overlap remains an operator constraint."""
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            self._ensure_schema(cursor)
            sql = (
                f"SELECT id FROM {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} "
                "WHERE compartment_name = %s AND bucket_name = %s AND resource_name_pattern = %s"
            )
            parameters: list[object] = [
                values["compartment_name"],
                values["bucket_name"],
                values["resource_name_pattern"],
            ]
            if exclude_mapping_id is not None:
                sql += " AND id <> %s"
                parameters.append(exclude_mapping_id)
            sql += " LIMIT 1"
            cursor.execute(sql, tuple(parameters))
            row = cursor.fetchone()
            return int(row[0]) if row else None
