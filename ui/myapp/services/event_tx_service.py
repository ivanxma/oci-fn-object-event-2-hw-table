"""Streaming processor transaction reporting from durable stream captures."""

from __future__ import annotations

import json
import os
import re
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
        configured_staging = os.environ.get("STAGING_DATABASE", "stream_staging").strip()
        self.staging_database = validate_identifier(configured_staging, "staging database") if configured_staging else ""

    def _capture_exists(self, cursor) -> bool:
        staging_database = self.staging_database or database
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

    def _stage_snapshot(self, cursor, database: str, table: str) -> tuple[list[dict[str, Any]], bool]:
        """Return persistent loader staging tables and whether a load is active.

        Staging tables are intentionally persistent (rather than TEMPORARY) so a
        failed process can be diagnosed and cleaned up.  The loader names them
        ``<target prefix>_stage_<12 hex chars>``.  We discover names through
        information_schema and then apply the naming rule in Python; this keeps
        the SQL identifier handling safe even when a target table contains an
        underscore or a LIKE wildcard.
        """
        database = validate_identifier(database, "target database")
        table = validate_identifier(table, "target table")
        self._require_target(cursor, database, table)
        cursor.execute(
            """SELECT table_name, table_rows, data_length, index_length,
                      create_time, update_time
                 FROM information_schema.tables
                WHERE table_schema=%s AND table_name LIKE %s
                ORDER BY table_name""",
            (staging_database, f"{table[:45]}\\_stage\\_%"),
        )
        pattern = re.compile(rf"^{re.escape(table[:45])}_stage_[0-9a-f]{{12}}$")
        discovered = [row for row in cursor.fetchall() if pattern.fullmatch(str(row.get("table_name", "")))]

        control = quote_identifier(control_database(), "control database")
        cursor.execute(
            f"SELECT COUNT(*) AS total FROM {control}.`source_object_batches` "
            "WHERE target_database=%s AND target_table=%s AND lifecycle_state='LOADING'",
            (database, table),
        )
        active_batches = int(cursor.fetchone()["total"] or 0)
        active_captures = 0
        cursor.execute(
            f"SELECT stream_id FROM {control}.`object_storage_mappings` "
            "WHERE target_database=%s AND target_table=%s",
            (database, table),
        )
        stream_ids = [row.get("stream_id") for row in cursor.fetchall() if row.get("stream_id")]
        if stream_ids and self._capture_exists(cursor):
            capture = quote_identifier(self.stream_data_database, "stream data database")
            placeholders = ",".join(["%s"] * len(stream_ids))
            cursor.execute(
                f"SELECT COUNT(*) AS total FROM {capture}.`stream_message_capture` "
                f"WHERE stream_id IN ({placeholders}) AND status IN ('PROCESSING','CAPTURED')",
                tuple(stream_ids),
            )
            active_captures = int(cursor.fetchone()["total"] or 0)
        active = active_batches > 0 or active_captures > 0
        rows = []
        for row in discovered:
            rows.append({
                "table_name": row["table_name"],
                "table_rows": int(row.get("table_rows") or 0),
                "data_length": int(row.get("data_length") or 0),
                "index_length": int(row.get("index_length") or 0),
                "create_time": row.get("create_time"),
                "update_time": row.get("update_time"),
                "status": "ACTIVE LOAD" if active else "ORPHAN",
                "cleanup_allowed": not active,
                "active_batches": active_batches,
                "active_captures": active_captures,
            })
        return rows, active

    def stage_tables(self, database: str, table: str) -> tuple[list[dict[str, Any]], bool]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            return self._stage_snapshot(cursor, database, table)

    def cleanup_stage_table(self, database: str, table: str, stage_table: str) -> None:
        database = validate_identifier(database, "target database")
        table = validate_identifier(table, "target table")
        stage_table = validate_identifier(stage_table, "staging table")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            rows, blocked = self._stage_snapshot(cursor, database, table)
            match = next((row for row in rows if row["table_name"] == stage_table), None)
            if match is None:
                raise ValueError("The selected staging table no longer exists for this target.")
            if blocked or not match["cleanup_allowed"]:
                raise ValueError("Staging-table cleanup is blocked while this target has active messages or loads.")
            staging_database = self.staging_database or database
            target = f"{quote_identifier(staging_database, 'staging database')}.{quote_identifier(stage_table, 'staging table')}"
            cursor.execute(f"DROP TABLE IF EXISTS {target}")

    def cleanup_stage_tables(self, database: str, table: str) -> list[str]:
        database = validate_identifier(database, "target database")
        table = validate_identifier(table, "target table")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            rows, blocked = self._stage_snapshot(cursor, database, table)
            if blocked:
                raise ValueError("Staging-table cleanup is blocked while this target has active messages or loads.")
            names = [row["table_name"] for row in rows if row["cleanup_allowed"]]
            for name in names:
                staging_database = self.staging_database or database
                target = f"{quote_identifier(staging_database, 'staging database')}.{quote_identifier(name, 'staging table')}"
                cursor.execute(f"DROP TABLE IF EXISTS {target}")
            return names

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
