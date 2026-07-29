"""Read Event TX records grouped by registered target database/table."""

from __future__ import annotations

import json
import os
from typing import Any

from .mapping_service import control_database
from .naming import quote_identifier, validate_identifier


MAPPING_TABLE = "object_storage_mappings"
EVENT_LOG_TABLE = "event_tx_log"
OBJECT_EVENT_TABLE = "object_event"
EVENT_ERROR_TABLE = "event_errors"
SOURCE_BATCH_TABLE = "source_object_batches"
STREAM_CAPTURE_TABLE = "stream_message_capture"
STALE_LOADING_MINUTES = 10


class EventTransactionService:
    def __init__(self, mysql, stream_data_database: str | None = None) -> None:
        self.mysql = mysql
        self.stream_data_database = validate_identifier(
            stream_data_database or os.environ.get("STREAM_DATA_DB_NAME", "stream_data"), "stream data database"
        )

    @staticmethod
    def _table_exists(cursor, table_name: str) -> bool:
        cursor.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (control_database(), table_name),
        )
        return cursor.fetchone() is not None

    @staticmethod
    def _column_exists(cursor, table_name: str, column_name: str) -> bool:
        cursor.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_schema = %s AND table_name = %s AND column_name = %s",
            (control_database(), table_name, column_name),
        )
        return cursor.fetchone() is not None

    def _stream_capture_exists(self, cursor) -> bool:
        cursor.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (self.stream_data_database, STREAM_CAPTURE_TABLE),
        )
        return cursor.fetchone() is not None

    def _durable_capture_rows(self, cursor, *, limit: int, offset: int = 0, database: str = "", table: str = "") -> list[dict[str, Any]]:
        """Read the processor's authoritative capture lifecycle and mapping."""
        stream_data = quote_identifier(self.stream_data_database, "stream data database")
        control = quote_identifier(control_database(), "control database")
        filters, values = [], []
        if database:
            filters.append("mapping.target_database = %s")
            values.append(database)
        if table:
            filters.append("mapping.target_table = %s")
            values.append(table)
        where = f" WHERE {' AND '.join(filters)}" if filters else ""
        cursor.execute(
            f"""SELECT capture.id, capture.stream_id, capture.partition_id, capture.stream_offset,
                       capture.payload, capture.status, capture.attempts, capture.last_error,
                       capture.received_at AS event_received_at, capture.completed_at AS event_completed_at,
                       CASE WHEN capture.completed_at IS NULL THEN NULL
                            ELSE TIMESTAMPDIFF(MICROSECOND, capture.received_at, capture.completed_at) / 1000 END AS event_duration_ms,
                       mapping.id AS mapping_id, mapping.target_database, mapping.target_table, mapping.processing_mode
                  FROM {stream_data}.`stream_message_capture` AS capture
                  LEFT JOIN {control}.`object_storage_mappings` AS mapping ON mapping.stream_id = capture.stream_id
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
            event_type = payload.get("eventType") or payload.get("type") or "UNKNOWN"
            row["event_action"] = self._event_action(event_type) or str(event_type)
            row["event_status"] = str(row.get("status") or "CAPTURED")
            row["bucket_name"] = details.get("bucketName") or data.get("bucketName")
            row["resource_name"] = data.get("resourceName") or details.get("resourceName")
            row["message"] = row.get("last_error") or f"Stream partition {row.get('partition_id')} · offset {row.get('stream_offset')}"
            row["batch_num"] = None
            row["processing_mode"] = str(row.get("processing_mode") or "UNKNOWN")
        return rows

    def _transaction_mode_sql(self, cursor, *, include_object_event: bool = False, alias: str = "tx") -> str:
        """Return processor ordering mode, never retired Function invocation mode."""
        if not self._table_exists(cursor, MAPPING_TABLE) or not self._column_exists(cursor, MAPPING_TABLE, "processing_mode"):
            return "'UNKNOWN'"
        control = quote_identifier(control_database(), "control database")
        return f"COALESCE((SELECT mapping.processing_mode FROM {control}.`object_storage_mappings` AS mapping WHERE mapping.id = {alias}.mapping_id), 'UNKNOWN')"

    def _event_timing_sql(self, cursor, alias: str = "tx") -> tuple[str, str]:
        """Return optional Object Storage timing projection and join."""
        if not self._table_exists(cursor, OBJECT_EVENT_TABLE):
            return (
                ", NULL AS event_received_at, NULL AS event_completed_at, NULL AS event_duration_ms",
                "",
            )
        control = quote_identifier(control_database(), "control database")
        return (
            ", object_event.received_at AS event_received_at, object_event.completed_at AS event_completed_at, object_event.duration_ms AS event_duration_ms",
            f" LEFT JOIN {control}.`object_event` AS object_event ON object_event.id = {alias}.object_event_id",
        )

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

    @staticmethod
    def _stage_prefix(target_table: str) -> str:
        """Return the deterministic portion of a Function staging-table name."""
        return f"{target_table[:45]}_stage_"

    @classmethod
    def _is_stage_table_for_target(cls, target_table: str, stage_table: str) -> bool:
        prefix = cls._stage_prefix(target_table)
        suffix = stage_table[len(prefix):]
        return stage_table.startswith(prefix) and len(suffix) == 12 and all(char in "0123456789abcdef" for char in suffix.lower())

    def _require_registered_target(self, cursor, database: str, table: str) -> None:
        if not self._table_exists(cursor, MAPPING_TABLE):
            raise ValueError("No registered target tables are available.")
        control = quote_identifier(control_database(), "control database")
        cursor.execute(
            f"SELECT 1 FROM {control}.`object_storage_mappings` WHERE target_database = %s AND target_table = %s LIMIT 1",
            (database, table),
        )
        if cursor.fetchone() is None:
            raise ValueError("Select a registered target table.")

    def stage_tables(self, database: str, table: str) -> tuple[list[dict[str, Any]], bool]:
        """Return residual Function staging tables for one registered target.

        A true loading batch makes cleanup unavailable: the current loader does
        not persist a stage-table name, so it cannot safely identify which
        temporary table belongs to an in-flight invocation.
        """
        database = validate_identifier(database, "target database")
        table = validate_identifier(table, "target table")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._require_registered_target(cursor, database, table)
            loading = False
            if self._table_exists(cursor, SOURCE_BATCH_TABLE):
                control = quote_identifier(control_database(), "control database")
                cursor.execute(
                    f"SELECT 1 FROM {control}.`source_object_batches` WHERE target_database = %s AND target_table = %s AND lifecycle_state = 'LOADING' AND updated_at >= UTC_TIMESTAMP() - INTERVAL {STALE_LOADING_MINUTES} MINUTE LIMIT 1",
                    (database, table),
                )
                loading = cursor.fetchone() is not None
            cursor.execute(
                """SELECT table_name AS table_name, table_rows AS table_rows,
                          data_length AS data_length, index_length AS index_length,
                          create_time AS create_time, update_time AS update_time
                     FROM information_schema.tables
                    WHERE table_schema = %s AND table_type = 'BASE TABLE'
                    ORDER BY create_time, table_name""",
                (database,),
            )
            tables = [row for row in cursor.fetchall() if self._is_stage_table_for_target(table, row["table_name"])]
            return tables, loading

    def cleanup_stage_table(self, database: str, table: str, stage_table: str) -> None:
        """Drop a residual stage table after confirming its target and idle state."""
        database = validate_identifier(database, "target database")
        table = validate_identifier(table, "target table")
        stage_table = validate_identifier(stage_table, "staging table")
        if not self._is_stage_table_for_target(table, stage_table):
            raise ValueError("The selected table is not a staging table for this registered target.")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._require_registered_target(cursor, database, table)
            if self._table_exists(cursor, SOURCE_BATCH_TABLE):
                control = quote_identifier(control_database(), "control database")
                cursor.execute(
                    f"SELECT 1 FROM {control}.`source_object_batches` WHERE target_database = %s AND target_table = %s AND lifecycle_state = 'LOADING' AND updated_at >= UTC_TIMESTAMP() - INTERVAL {STALE_LOADING_MINUTES} MINUTE LIMIT 1",
                    (database, table),
                )
                if cursor.fetchone() is not None:
                    raise ValueError("Cleanup is unavailable while this target has a loading batch.")
            cursor.execute(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'",
                (database, stage_table),
            )
            if cursor.fetchone() is None:
                raise ValueError("The staging table no longer exists.")
            cursor.execute(
                f"DROP TABLE {quote_identifier(database, 'target database')}.{quote_identifier(stage_table, 'staging table')}"
            )

    def cleanup_stage_tables(self, database: str, table: str) -> list[str]:
        """Drop every residual staging table for one target after one confirmation."""
        database = validate_identifier(database, "target database")
        table = validate_identifier(table, "target table")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._require_registered_target(cursor, database, table)
            control = quote_identifier(control_database(), "control database")
            if self._table_exists(cursor, SOURCE_BATCH_TABLE):
                cursor.execute(
                    f"SELECT 1 FROM {control}.`source_object_batches` WHERE target_database = %s AND target_table = %s AND lifecycle_state = 'LOADING' AND updated_at >= UTC_TIMESTAMP() - INTERVAL {STALE_LOADING_MINUTES} MINUTE LIMIT 1",
                    (database, table),
                )
                if cursor.fetchone() is not None:
                    raise ValueError("Cleanup is unavailable while this target has a loading batch.")
            cursor.execute(
                """SELECT table_name AS table_name FROM information_schema.tables
                   WHERE table_schema = %s AND table_type = 'BASE TABLE'""",
                (database,),
            )
            names = [row["table_name"] for row in cursor.fetchall() if self._is_stage_table_for_target(table, row["table_name"])]
            for stage_table in names:
                cursor.execute(f"DROP TABLE {quote_identifier(database, 'target database')}.{quote_identifier(stage_table, 'staging table')}")
            return names

    def recent_events(self, database: str, table: str, limit: int = 100) -> list[dict[str, Any]]:
        database = validate_identifier(database, "target database")
        table = validate_identifier(table, "target table")
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if not self._table_exists(cursor, EVENT_LOG_TABLE):
                return []
            timing, timing_join = self._event_timing_sql(cursor)
            invocation_mode = self._transaction_mode_sql(cursor, include_object_event=bool(timing_join))
            cursor.execute(
                f"""SELECT tx.id, tx.mapping_id, tx.batch_num, tx.event_action, tx.event_status, tx.bucket_name,
                              tx.resource_name, tx.object_version, tx.message, tx.created_at, {invocation_mode} AS invocation_mode{timing}
                       FROM {quote_identifier(control_database(), 'control database')}.`event_tx_log`
                       AS tx{timing_join}
                       WHERE tx.target_database = %s AND tx.target_table = %s
                       ORDER BY tx.created_at DESC, tx.id DESC LIMIT %s""",
                (database, table, limit),
            )
            return cursor.fetchall()

    def registered_events_page(
        self, database: str, table: str, *, page: int, page_size: int
    ) -> tuple[list[dict[str, Any]], int]:
        """Return one bounded page of transaction records for a registered target."""
        database = validate_identifier(database, "target database")
        table = validate_identifier(table, "target table")
        page, page_size = max(page, 1), max(page_size, 1)
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if self._stream_capture_exists(cursor):
                stream_data = quote_identifier(self.stream_data_database, "stream data database")
                control = quote_identifier(control_database(), "control database")
                cursor.execute(
                    f"""SELECT COUNT(*) AS total FROM {stream_data}.`stream_message_capture` AS capture
                          INNER JOIN {control}.`object_storage_mappings` AS mapping ON mapping.stream_id = capture.stream_id
                         WHERE mapping.target_database = %s AND mapping.target_table = %s""",
                    (database, table),
                )
                total = int(cursor.fetchone()["total"])
                return self._durable_capture_rows(cursor, limit=page_size, offset=(page - 1) * page_size, database=database, table=table), total
            if not self._table_exists(cursor, EVENT_LOG_TABLE):
                return [], 0
            control = quote_identifier(control_database(), "control database")
            timing, timing_join = self._event_timing_sql(cursor)
            invocation_mode = self._transaction_mode_sql(cursor, include_object_event=bool(timing_join))
            cursor.execute(
                f"SELECT COUNT(*) AS total FROM {control}.`event_tx_log` WHERE target_database = %s AND target_table = %s",
                (database, table),
            )
            total = int(cursor.fetchone()["total"])
            cursor.execute(
                f"""SELECT tx.id, tx.mapping_id, tx.batch_num, tx.event_action, tx.event_status, tx.bucket_name,
                              tx.resource_name, tx.object_version, tx.message, tx.created_at, {invocation_mode} AS processing_mode{timing}
                       FROM {control}.`event_tx_log`
                       AS tx{timing_join}
                       WHERE tx.target_database = %s AND tx.target_table = %s
                       ORDER BY tx.created_at DESC, tx.id DESC LIMIT %s OFFSET %s""",
                (database, table, page_size, (page - 1) * page_size),
            )
            return cursor.fetchall(), total

    def target_table_page(
        self, database: str, table: str, *, page: int, page_size: int
    ) -> tuple[list[str], list[dict[str, Any]], int]:
        """Safely page visible rows from a registered target table for the dialog."""
        database = validate_identifier(database, "target database")
        table = validate_identifier(table, "target table")
        page, page_size = max(page, 1), max(page_size, 1)
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            control = quote_identifier(control_database(), "control database")
            cursor.execute(
                f"SELECT 1 FROM {control}.`object_storage_mappings` WHERE target_database = %s AND target_table = %s LIMIT 1",
                (database, table),
            )
            if cursor.fetchone() is None:
                raise ValueError("Select a registered target table.")
            cursor.execute(
                """SELECT column_name AS column_name, extra AS extra
                     FROM information_schema.columns
                     WHERE table_schema = %s AND table_name = %s
                     ORDER BY ordinal_position""",
                (database, table),
            )
            columns = [item["column_name"] for item in cursor.fetchall() if "INVISIBLE" not in (item["extra"] or "").upper()]
            if not columns:
                raise ValueError("The registered target has no visible columns.")
            target = f"{quote_identifier(database, 'target database')}.{quote_identifier(table, 'target table')}"
            cursor.execute(f"SELECT COUNT(*) AS total FROM {target}")
            total = int(cursor.fetchone()["total"])
            selected = ", ".join(quote_identifier(column, "target column") for column in columns)
            cursor.execute(f"SELECT {selected} FROM {target} LIMIT %s OFFSET %s", (page_size, (page - 1) * page_size))
            return columns, cursor.fetchall(), total

    def recent_events_all(self, limit: int) -> list[dict[str, Any]]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if self._stream_capture_exists(cursor):
                return self._durable_capture_rows(cursor, limit=limit)
            if not self._table_exists(cursor, EVENT_LOG_TABLE):
                return []
            timing, timing_join = self._event_timing_sql(cursor)
            invocation_mode = self._transaction_mode_sql(cursor, include_object_event=bool(timing_join))
            cursor.execute(
                f"""SELECT tx.id, tx.mapping_id, tx.target_database, tx.target_table, tx.batch_num, tx.event_action,
                              tx.event_status, tx.bucket_name, tx.resource_name, tx.object_version, tx.message, tx.created_at, {invocation_mode} AS processing_mode{timing}
                       FROM {quote_identifier(control_database(), 'control database')}.`event_tx_log`
                       AS tx{timing_join}
                       ORDER BY tx.created_at DESC, tx.id DESC LIMIT %s""",
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

    def error_log(self, error_id: int) -> dict[str, Any] | None:
        """Return one error record so a raw event can link to it directly."""
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if not self._table_exists(cursor, EVENT_ERROR_TABLE):
                return None
            cursor.execute(
                f"""SELECT id, event_log_id, mapping_id, target_database, target_table,
                              event_action, error_code, error_message, created_at
                       FROM {quote_identifier(control_database(), 'control database')}.`event_errors`
                       WHERE id = %s""",
                (error_id,),
            )
            return cursor.fetchone()

    def object_event_tables(self) -> list[dict[str, str]]:
        """Return the processor capture source instead of retired Function audit data."""
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if self._stream_capture_exists(cursor):
                return [{"database_name": self.stream_data_database, "table_name": STREAM_CAPTURE_TABLE}]
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
        if database == self.stream_data_database:
            return ["event_time", "event_type", "bucket_name", "resource_name", "stream_partition", "stream_offset", "target", "message", "received_at", "completed_at", "duration_ms"]
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            cursor.execute(
                """SELECT column_name AS column_name
                     FROM information_schema.columns
                     WHERE table_schema = %s AND table_name = %s AND column_name <> 'invocation_mode'
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
        """Read one safely sorted page from the active processor event source."""
        database = validate_identifier(database, "object event database")
        columns = self.object_event_columns(database)
        if not columns:
            raise ValueError("The selected object_event table is unavailable.")
        if database == self.stream_data_database:
            page = max(page, 1)
            page_size = max(page_size, 1)
            with self.mysql.connection() as conn:
                cursor = conn.cursor(dictionary=True, buffered=True)
                stream_data = quote_identifier(self.stream_data_database, "stream data database")
                cursor.execute(f"SELECT COUNT(*) AS total FROM {stream_data}.`{STREAM_CAPTURE_TABLE}`")
                total = int(cursor.fetchone()["total"])
                rows = self._durable_capture_rows(cursor, limit=page_size, offset=(page - 1) * page_size)
            for row in rows:
                row.update({
                    "event_time": row.get("event_received_at"),
                    "event_type": row.get("event_action") or "UNKNOWN",
                    "stream_partition": row.get("partition_id"),
                    "stream_offset": row.get("stream_offset"),
                    "target": f"{row.get('target_database') or '—'}.{row.get('target_table') or '—'}",
                    "received_at": row.get("event_received_at"),
                    "completed_at": row.get("event_completed_at"),
                    "duration_ms": row.get("event_duration_ms"),
                    "_lifecycle_status": str(row.get("event_status") or "CAPTURED"),
                    "_invocation_mode": str(row.get("processing_mode") or "UNKNOWN"),
                    "_error_id": None,
                })
            return columns, rows, total, "received_at", "desc"
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
            rows = cursor.fetchall()
            self._add_object_event_lifecycle(cursor, rows)
            return columns, rows, total, sort_column, direction

    def _add_object_event_lifecycle(self, cursor, rows: list[dict[str, Any]]) -> None:
        """Attach transaction lifecycle and its linked error to raw Object Storage events.

        Current Functions write the raw ``object_event`` id to ``event_tx_log``.
        The resource/action fallback keeps historical rows usable after upgrade.
        """
        event_log_exists = self._table_exists(cursor, EVENT_LOG_TABLE)
        error_log_exists = self._table_exists(cursor, EVENT_ERROR_TABLE)
        control = quote_identifier(control_database(), "control database")
        invocation_mode = f"{self._transaction_mode_sql(cursor)} AS invocation_mode"
        for row in rows:
            row["_lifecycle_status"] = self._raw_event_lifecycle(row)
            row["_error_id"] = None
            row["_processing_mode"] = "UNKNOWN"
            # Keep the legacy template field populated during the UI migration;
            # its value is now the mapping processing mode, never Sync/Detached.
            row["_invocation_mode"] = row["_processing_mode"]
            if not event_log_exists:
                continue
            error_join = f"LEFT JOIN {control}.`event_errors` AS err ON err.event_log_id = tx.id" if error_log_exists else ""
            error_id = "err.id AS error_id" if error_log_exists else "NULL AS error_id"
            cursor.execute(
                f"""SELECT tx.event_status, {error_id}, {invocation_mode}
                       FROM {control}.`event_tx_log` AS tx
                       {error_join}
                      WHERE tx.object_event_id = %s
                      ORDER BY tx.id DESC LIMIT 1""",
                (row["id"],),
            )
            linked = cursor.fetchone()
            if linked is None:
                action = self._event_action(row.get("event_type"))
                if action:
                    cursor.execute(
                        f"""SELECT tx.event_status, {error_id}, {invocation_mode}
                               FROM {control}.`event_tx_log` AS tx
                               {error_join}
                              WHERE tx.object_event_id IS NULL
                                AND tx.bucket_name <=> %s AND tx.resource_name <=> %s
                                AND tx.event_action = %s
                              ORDER BY tx.created_at DESC, tx.id DESC LIMIT 1""",
                        (row.get("bucket_name"), row.get("resource_name"), action),
                    )
                    linked = cursor.fetchone()
            if linked:
                row["_lifecycle_status"] = linked["event_status"]
                row["_error_id"] = linked["error_id"]
                row["_processing_mode"] = linked["invocation_mode"] or row["_processing_mode"]
                row["_invocation_mode"] = row["_processing_mode"]

    @staticmethod
    def _raw_event_lifecycle(row: dict[str, Any]) -> str:
        """Use persisted timing when detailed transaction history is unavailable."""
        return "COMPLETED" if row.get("completed_at") is not None else "RECEIVED"

    @staticmethod
    def _event_action(event_type: Any) -> str | None:
        value = str(event_type or "").lower()
        if value.endswith("createobject"):
            return "CREATE"
        if value.endswith("updateobject"):
            return "UPDATE"
        if value.endswith("deleteobject"):
            return "DELETE"
        return None

    def object_event_export(
        self, database: str, *, sort_column: str | None = None, sort_direction: str = "desc"
    ) -> tuple[list[str], list[dict[str, Any]]]:
        """Read the selected object-event table for its CSV export."""
        database = validate_identifier(database, "object event database")
        if database == self.stream_data_database:
            columns, rows, _total, _sort, _direction = self.object_event_page(
                database, page=1, page_size=500, sort_column=sort_column, sort_direction=sort_direction
            )
            return columns, rows
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
