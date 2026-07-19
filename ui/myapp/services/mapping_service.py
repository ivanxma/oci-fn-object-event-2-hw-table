"""Persistence for Object Storage resource-to-table mappings in ``fndb``."""

from __future__ import annotations

import os
from typing import Any

from .naming import quote_identifier, validate_identifier


MAPPING_TABLE = "object_storage_mappings"


def control_database() -> str:
    return validate_identifier(os.environ.get("CONTROL_DATABASE", "fndb"), "control database")


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
        mode = (form.get("invocation_mode") or "SYNC").strip().upper()
        if mode not in {"SYNC", "DETACHED"}:
            raise ValueError("Invocation mode must be SYNC or DETACHED.")
        queue_scope = (form.get("queue_scope") or "TABLE").strip().upper()
        if queue_scope not in {"TABLE", "MAPPING"}:
            raise ValueError("Queue scope must be TABLE or MAPPING.")
        try:
            workers = int(form.get("worker_threads") or 4)
        except (TypeError, ValueError) as error:
            raise ValueError("Worker threads must be a whole number from 1 to 64.") from error
        if not 1 <= workers <= 64:
            raise ValueError("Worker threads must be from 1 to 64.")
        return {
            "compartment_name": _required_text(form.get("compartment_name"), "Compartment name", 255),
            "bucket_name": _required_text(form.get("bucket_name"), "Bucket name", 255),
            "resource_name_pattern": _required_text(form.get("resource_name_pattern"), "Resource name pattern", 1024),
            "target_database": validate_identifier((form.get("target_database") or "").strip(), "target database"),
            "target_table": validate_identifier((form.get("target_table") or "").strip().lstrip("."), "target table"),
            "invocation_mode": mode,
            "worker_threads": str(workers),
            "queue_scope": queue_scope,
        }

    def _ensure_schema(self, cursor) -> None:
        database = control_database()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(database, 'mapping database')} CHARACTER SET utf8mb4")
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {quote_identifier(database, 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                compartment_name VARCHAR(255) NOT NULL,
                bucket_name VARCHAR(255) NOT NULL,
                resource_name_pattern VARCHAR(1024) NOT NULL,
                target_database VARCHAR(64) NOT NULL,
                target_table VARCHAR(64) NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                invocation_mode ENUM('SYNC','DETACHED') NOT NULL DEFAULT 'SYNC',
                worker_threads SMALLINT UNSIGNED NOT NULL DEFAULT 4,
                queue_scope ENUM('TABLE','MAPPING') NOT NULL DEFAULT 'TABLE',
                event_rule_id VARCHAR(255) NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )
        for column, definition in (
            ("invocation_mode", "ENUM('SYNC','DETACHED') NOT NULL DEFAULT 'SYNC'"),
            ("worker_threads", "SMALLINT UNSIGNED NOT NULL DEFAULT 4"),
            ("queue_scope", "ENUM('TABLE','MAPPING') NOT NULL DEFAULT 'TABLE'"),
            ("event_rule_id", "VARCHAR(255) NULL"),
        ):
            cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=%s AND table_name=%s AND column_name=%s", (database, MAPPING_TABLE, column))
            row = cursor.fetchone()
            count = row[0] if isinstance(row, tuple) else (next(iter(row.values())) if row else 0)
            if not row or not count:
                cursor.execute(f"ALTER TABLE {quote_identifier(database, 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} ADD COLUMN {quote_identifier(column, 'mapping column')} {definition}")

    def list_mappings(self) -> list[dict[str, Any]]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_schema(cursor)
            cursor.execute(
                f"SELECT id, compartment_name, bucket_name, resource_name_pattern, target_database, target_table, invocation_mode, worker_threads, queue_scope, event_rule_id "
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
                f"SELECT id, compartment_name, bucket_name, resource_name_pattern, target_database, target_table, invocation_mode, worker_threads, queue_scope, event_rule_id "
                f"FROM {quote_identifier(control_database(), 'mapping database')}.{quote_identifier(MAPPING_TABLE, 'mapping table')} WHERE id = %s",
                (mapping_id,),
            )
            return cursor.fetchone()

    def get_mapping_by_rule_id(self, rule_id: str) -> dict[str, Any] | None:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_schema(cursor)
            cursor.execute(
                f"SELECT id, compartment_name, bucket_name, resource_name_pattern, target_database, target_table, invocation_mode, worker_threads, queue_scope, event_rule_id "
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
                "(compartment_name, bucket_name, resource_name_pattern, target_database, target_table, invocation_mode, worker_threads, queue_scope) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                tuple(values[column] for column in ("compartment_name", "bucket_name", "resource_name_pattern", "target_database", "target_table", "invocation_mode", "worker_threads", "queue_scope")),
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
                "SET compartment_name = %s, bucket_name = %s, resource_name_pattern = %s, target_database = %s, target_table = %s, invocation_mode = %s, worker_threads = %s, queue_scope = %s WHERE id = %s",
                (*tuple(values[column] for column in ("compartment_name", "bucket_name", "resource_name_pattern", "target_database", "target_table", "invocation_mode", "worker_threads", "queue_scope")), mapping_id),
            )
            return True

    def has_nonterminal_queue_work(self, mapping: dict[str, Any], new_scope: str) -> bool:
        """Prevent a scope change from splitting an active ordered lane."""
        old_scope = str(mapping.get("queue_scope") or "TABLE").upper()
        if old_scope == new_scope:
            return False
        keys = [f"mapping:{int(mapping['id'])}", f"table:{mapping['target_database']}.{mapping['target_table']}"]
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name='event_work_queue'",
                (control_database(),),
            )
            if not cursor.fetchone()[0]:
                return False
            cursor.execute(
                f"""SELECT COUNT(*) FROM {quote_identifier(control_database(), 'mapping database')}.`event_work_queue`
                     WHERE binding_key IN (%s,%s)
                       AND status NOT IN ('SUCCESS','CANCELLED','DEAD_LETTER')""",
                tuple(keys),
            )
            return bool(cursor.fetchone()[0])

    def mapping_has_nonterminal_queue_work(self, mapping_id: int) -> bool:
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name='event_work_queue'",
                (control_database(),),
            )
            if not cursor.fetchone()[0]:
                return False
            cursor.execute(
                f"""SELECT COUNT(*) FROM {quote_identifier(control_database(), 'mapping database')}.`event_work_queue`
                     WHERE mapping_id=%s AND status NOT IN ('SUCCESS','CANCELLED','DEAD_LETTER')""",
                (mapping_id,),
            )
            return bool(cursor.fetchone()[0])

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
