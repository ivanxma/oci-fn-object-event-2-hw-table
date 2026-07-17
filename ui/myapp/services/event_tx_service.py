"""Read Event TX records grouped by registered target database/table."""

from __future__ import annotations

from typing import Any

from .mapping_service import control_database
from .naming import quote_identifier, validate_identifier


MAPPING_TABLE = "object_storage_mappings"
EVENT_LOG_TABLE = "event_tx_log"
OBJECT_EVENT_TABLE = "object_event"
EVENT_ERROR_TABLE = "event_errors"


class EventTransactionService:
    def __init__(self, mysql) -> None:
        self.mysql = mysql

    @staticmethod
    def _table_exists(cursor, table_name: str) -> bool:
        cursor.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (control_database(), table_name),
        )
        return cursor.fetchone() is not None

    def registered_tables(self) -> tuple[list[dict[str, Any]], bool]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if not self._table_exists(cursor, MAPPING_TABLE):
                return [], False
            event_log_exists = self._table_exists(cursor, EVENT_LOG_TABLE)
            control = quote_identifier(control_database(), "control database")
            if event_log_exists:
                cursor.execute(
                    f"""SELECT mapping.target_database, mapping.target_table, COUNT(DISTINCT mapping.id) AS mapping_count,
                              COUNT(DISTINCT event_log.id) AS event_count, MAX(event_log.created_at) AS last_event_at
                       FROM {control}.`object_storage_mappings` AS mapping
                       LEFT JOIN {control}.`event_tx_log` AS event_log
                         ON event_log.target_database = mapping.target_database
                        AND event_log.target_table = mapping.target_table
                       GROUP BY mapping.target_database, mapping.target_table
                       ORDER BY last_event_at DESC, mapping.target_database, mapping.target_table"""
                )
            else:
                cursor.execute(
                    f"""SELECT target_database, target_table, COUNT(*) AS mapping_count,
                              0 AS event_count, NULL AS last_event_at
                       FROM {control}.`object_storage_mappings`
                       GROUP BY target_database, target_table
                       ORDER BY target_database, target_table"""
                )
            return cursor.fetchall(), event_log_exists

    def recent_events(self, database: str, table: str, limit: int = 100) -> list[dict[str, Any]]:
        database = validate_identifier(database, "target database")
        table = validate_identifier(table, "target table")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if not self._table_exists(cursor, EVENT_LOG_TABLE):
                return []
            cursor.execute(
                f"""SELECT id, mapping_id, batch_num, event_action, event_status, bucket_name,
                              resource_name, object_version, message, created_at
                       FROM {quote_identifier(control_database(), 'control database')}.`event_tx_log`
                       WHERE target_database = %s AND target_table = %s
                       ORDER BY created_at DESC, id DESC LIMIT %s""",
                (database, table, limit),
            )
            return cursor.fetchall()

    def recent_events_all(self, limit: int) -> list[dict[str, Any]]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if not self._table_exists(cursor, EVENT_LOG_TABLE):
                return []
            cursor.execute(
                f"""SELECT id, mapping_id, target_database, target_table, batch_num, event_action,
                              event_status, bucket_name, resource_name, object_version, message, created_at
                       FROM {quote_identifier(control_database(), 'control database')}.`event_tx_log`
                       ORDER BY created_at DESC, id DESC LIMIT %s""",
                (limit,),
            )
            return cursor.fetchall()

    def audit_logs(self, limit: int) -> list[dict[str, Any]]:
        """Return the control-database transaction audit log."""
        return self.recent_events_all(limit)

    def error_logs(self, limit: int) -> list[dict[str, Any]]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if not self._table_exists(cursor, EVENT_ERROR_TABLE):
                return []
            cursor.execute(
                f"""SELECT id, event_log_id, mapping_id, target_database, target_table,
                              event_action, error_code, error_message, created_at
                       FROM {quote_identifier(control_database(), 'control database')}.`event_errors`
                       ORDER BY created_at DESC, id DESC LIMIT %s""",
                (limit,),
            )
            return cursor.fetchall()

    def object_event_tables(self) -> list[dict[str, str]]:
        """Return the object-event table only when it exists in the control database."""
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            cursor.execute(
                """SELECT table_schema AS database_name, table_name AS table_name
                     FROM information_schema.tables
                     WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'
                     ORDER BY table_schema""",
                (control_database(), OBJECT_EVENT_TABLE),
            )
            return cursor.fetchall()

    def object_event_columns(self, database: str) -> list[str]:
        database = validate_identifier(database, "object event database")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            cursor.execute(
                """SELECT column_name AS column_name
                     FROM information_schema.columns
                     WHERE table_schema = %s AND table_name = %s
                     ORDER BY ordinal_position""",
                (database, OBJECT_EVENT_TABLE),
            )
            return [row["column_name"] for row in cursor.fetchall()]

    def object_event_page(
        self,
        database: str,
        *,
        page: int,
        page_size: int,
        sort_column: str | None = None,
        sort_direction: str = "desc",
    ) -> tuple[list[str], list[dict[str, Any]], int, str, str]:
        """Read one safely sorted page from ``database.object_event``."""
        database = validate_identifier(database, "object event database")
        columns = self.object_event_columns(database)
        if not columns:
            raise ValueError("The selected object_event table is unavailable.")
        if sort_column not in columns:
            sort_column = "event_date" if "event_date" in columns else columns[0]
        direction = sort_direction.lower()
        if direction not in {"asc", "desc"}:
            raise ValueError("Object event sort direction must be ascending or descending.")
        page = max(page, 1)
        page_size = max(page_size, 1)
        qualified_table = f"{quote_identifier(database, 'object event database')}.{quote_identifier(OBJECT_EVENT_TABLE, 'object event table')}"
        order_by = quote_identifier(sort_column, "object event sort column")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            cursor.execute(f"SELECT COUNT(*) AS total FROM {qualified_table}")
            total = int(cursor.fetchone()["total"])
            cursor.execute(
                f"SELECT * FROM {qualified_table} ORDER BY {order_by} {direction.upper()} LIMIT %s OFFSET %s",
                (page_size, (page - 1) * page_size),
            )
            return columns, cursor.fetchall(), total, sort_column, direction

    def object_event_export(
        self, database: str, *, sort_column: str | None = None, sort_direction: str = "desc"
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Read the selected object-event table for its CSV export."""
        database = validate_identifier(database, "object event database")
        columns = self.object_event_columns(database)
        if not columns:
            raise ValueError("The selected object_event table is unavailable.")
        if sort_column not in columns:
            sort_column = "event_date" if "event_date" in columns else columns[0]
        direction = sort_direction.lower()
        if direction not in {"asc", "desc"}:
            raise ValueError("Object event sort direction must be ascending or descending.")
        qualified_table = f"{quote_identifier(database, 'object event database')}.{quote_identifier(OBJECT_EVENT_TABLE, 'object event table')}"
        order_by = quote_identifier(sort_column, "object event sort column")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            cursor.execute(f"SELECT * FROM {qualified_table} ORDER BY {order_by} {direction.upper()}")
            return columns, cursor.fetchall()
