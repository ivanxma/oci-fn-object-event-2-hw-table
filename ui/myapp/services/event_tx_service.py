"""Streaming processor transaction reporting from durable stream captures."""

from __future__ import annotations

import json
import os
from typing import Any

from .mapping_service import control_database
from .naming import quote_identifier, validate_identifier


MAPPING_TABLE = "object_storage_mappings"
STREAM_CAPTURE_TABLE = "stream_message_capture"


class EventTransactionService:
    def __init__(self, mysql, stream_data_database: str | None = None) -> None:
        self.mysql = mysql
        self.stream_data_database = validate_identifier(
            stream_data_database or os.environ.get("STREAM_DATA_DB_NAME", ""),
            "stream data database",
        )

    def _capture_exists(self, cursor) -> bool:
        cursor.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (self.stream_data_database, STREAM_CAPTURE_TABLE),
        )
        return cursor.fetchone() is not None

    @staticmethod
    def _event_action(event_type: Any) -> str:
        value = str(event_type or "").lower()
        if value.endswith("createobject"):
            return "CREATE"
        if value.endswith("updateobject"):
            return "UPDATE"
        if value.endswith("deleteobject"):
            return "DELETE"
        return str(event_type or "UNKNOWN")

    def _capture_rows(self, cursor, *, limit: int, offset: int = 0, database: str = "", table: str = "") -> list[dict[str, Any]]:
        capture = quote_identifier(self.stream_data_database, "stream data database")
        control = quote_identifier(control_database(), "control database")
        filters, values = [], []
        if database:
            filters.append("mapping.target_database=%s")
            values.append(database)
        if table:
            filters.append("mapping.target_table=%s")
            values.append(table)
        where = f"WHERE {' AND '.join(filters)}" if filters else ""
        cursor.execute(
            f"""SELECT capture.id, capture.stream_id, capture.partition_id, capture.stream_offset,
                       capture.payload, capture.status, capture.attempts, capture.last_error,
                       capture.received_at AS event_received_at, capture.completed_at AS event_completed_at,
                       CASE WHEN capture.completed_at IS NULL THEN NULL ELSE
                         TIMESTAMPDIFF(MICROSECOND,capture.received_at,capture.completed_at)/1000 END AS event_duration_ms,
                       mapping.id AS mapping_id, mapping.target_database, mapping.target_table,
                       mapping.processing_mode
                  FROM {capture}.`stream_message_capture` capture
                  LEFT JOIN {control}.`object_storage_mappings` mapping ON mapping.stream_id=capture.stream_id
                  {where} ORDER BY capture.id DESC LIMIT %s OFFSET %s""",
            (*values, limit, offset),
        )
        rows = cursor.fetchall()
        for row in rows:
            payload = row.get("payload")
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except json.JSONDecodeError:
                    payload = {}
            payload = payload if isinstance(payload, dict) else {}
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            details = data.get("additionalDetails") if isinstance(data.get("additionalDetails"), dict) else {}
            row["event_action"] = self._event_action(payload.get("eventType") or payload.get("type"))
            row["event_status"] = str(row.get("status") or "CAPTURED")
            row["bucket_name"] = details.get("bucketName") or data.get("bucketName")
            row["resource_name"] = data.get("resourceName") or details.get("resourceName")
            row["message"] = row.get("last_error") or f"Stream partition {row.get('partition_id')} · offset {row.get('stream_offset')}"
            row["batch_num"] = None
            row["processing_mode"] = str(row.get("processing_mode") or "UNKNOWN")
        return rows

    def registered_tables(self) -> tuple[list[dict[str, Any]], bool]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if not self._capture_exists(cursor):
                return [], False
            capture, control = quote_identifier(self.stream_data_database, "stream data database"), quote_identifier(control_database(), "control database")
            cursor.execute(
                f"""SELECT mapping.target_database, mapping.target_table, COUNT(DISTINCT mapping.id) mapping_count,
                           COUNT(capture.id) event_count, MAX(capture.received_at) last_event_at
                      FROM {control}.`object_storage_mappings` mapping
                      LEFT JOIN {capture}.`stream_message_capture` capture ON capture.stream_id=mapping.stream_id
                     GROUP BY mapping.target_database,mapping.target_table
                     ORDER BY last_event_at DESC,mapping.target_database,mapping.target_table"""
            )
            return cursor.fetchall(), True

    def _require_target(self, cursor, database: str, table: str) -> None:
        control = quote_identifier(control_database(), "control database")
        cursor.execute(f"SELECT 1 FROM {control}.`object_storage_mappings` WHERE target_database=%s AND target_table=%s LIMIT 1", (database, table))
        if cursor.fetchone() is None:
            raise ValueError("Select a registered target table.")

    def stage_tables(self, _database: str, _table: str) -> tuple[list[dict[str, Any]], bool]:
        return [], False

    def cleanup_stage_table(self, _database: str, _table: str, _stage_table: str) -> None:
        raise ValueError("No residual staging-table cleanup is required.")

    def cleanup_stage_tables(self, _database: str, _table: str) -> list[str]:
        return []

    def recent_events(self, database: str, table: str, limit: int = 100) -> list[dict[str, Any]]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            return self._capture_rows(cursor, limit=limit, database=validate_identifier(database, "target database"), table=validate_identifier(table, "target table")) if self._capture_exists(cursor) else []

    def registered_events_page(self, database: str, table: str, *, page: int, page_size: int) -> tuple[list[dict[str, Any]], int]:
        database, table = validate_identifier(database, "target database"), validate_identifier(table, "target table")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if not self._capture_exists(cursor):
                return [], 0
            capture, control = quote_identifier(self.stream_data_database, "stream data database"), quote_identifier(control_database(), "control database")
            cursor.execute(f"SELECT COUNT(*) total FROM {capture}.`stream_message_capture` capture INNER JOIN {control}.`object_storage_mappings` mapping ON mapping.stream_id=capture.stream_id WHERE mapping.target_database=%s AND mapping.target_table=%s", (database, table))
            total = int(cursor.fetchone()["total"])
            return self._capture_rows(cursor, limit=max(page_size, 1), offset=(max(page, 1)-1)*max(page_size, 1), database=database, table=table), total

    def recent_events_all(self, limit: int) -> list[dict[str, Any]]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            return self._capture_rows(cursor, limit=limit) if self._capture_exists(cursor) else []

    def audit_logs(self, limit: int) -> list[dict[str, Any]]:
        return self.recent_events_all(limit)

    def error_logs(self, limit: int) -> list[dict[str, Any]]:
        return [row for row in self.recent_events_all(limit) if row["event_status"] == "FAILED"]

    def error_log(self, error_id: int) -> dict[str, Any] | None:
        return next((row for row in self.recent_events_all(500) if int(row["id"]) == error_id and row["event_status"] == "FAILED"), None)

    def object_event_tables(self) -> list[dict[str, str]]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            return [{"database_name": self.stream_data_database, "table_name": STREAM_CAPTURE_TABLE}] if self._capture_exists(cursor) else []

    @staticmethod
    def object_event_columns(_database: str) -> list[str]:
        return ["event_time", "event_type", "bucket_name", "resource_name", "stream_partition", "stream_offset", "target", "message", "received_at", "completed_at", "duration_ms"]

    def object_event_page(self, database: str, *, page: int, page_size: int, sort_column: str | None = None, sort_direction: str = "desc") -> tuple[list[str], list[dict[str, Any]], int, str, str]:
        if validate_identifier(database, "stream data database") != self.stream_data_database:
            raise ValueError("Select the durable stream capture source.")
        columns = self.object_event_columns(database)
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if not self._capture_exists(cursor):
                return columns, [], 0, "received_at", "desc"
            capture = quote_identifier(self.stream_data_database, "stream data database")
            cursor.execute(f"SELECT COUNT(*) total FROM {capture}.`stream_message_capture`")
            total = int(cursor.fetchone()["total"])
            rows = self._capture_rows(cursor, limit=max(page_size, 1), offset=(max(page, 1)-1)*max(page_size, 1))
        for row in rows:
            row.update({"event_time": row["event_received_at"], "event_type": row["event_action"], "stream_partition": row["partition_id"], "stream_offset": row["stream_offset"], "target": f"{row.get('target_database') or '—'}.{row.get('target_table') or '—'}", "received_at": row["event_received_at"], "completed_at": row["event_completed_at"], "duration_ms": row["event_duration_ms"]})
        return columns, rows, total, "received_at", "desc"

    def object_event_export(self, database: str, **_kwargs) -> tuple[list[str], list[dict[str, Any]]]:
        columns, rows, _total, _sort, _direction = self.object_event_page(database, page=1, page_size=500)
        return columns, rows

    def target_table_page(self, database: str, table: str, *, page: int, page_size: int) -> tuple[list[str], list[dict[str, Any]], int]:
        database, table = validate_identifier(database, "target database"), validate_identifier(table, "target table")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._require_target(cursor, database, table)
            cursor.execute("SELECT column_name column_name, extra extra FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position", (database, table))
            columns = [row["column_name"] for row in cursor.fetchall() if "INVISIBLE" not in (row["extra"] or "").upper()]
            target = f"{quote_identifier(database, 'target database')}.{quote_identifier(table, 'target table')}"
            cursor.execute(f"SELECT COUNT(*) total FROM {target}")
            total = int(cursor.fetchone()["total"])
            cursor.execute(f"SELECT {', '.join(quote_identifier(column, 'target column') for column in columns)} FROM {target} LIMIT %s OFFSET %s", (max(page_size,1), (max(page,1)-1)*max(page_size,1)))
            return columns, cursor.fetchall(), total
