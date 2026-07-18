"""Read Event TX records grouped by registered target database/table."""

from __future__ import annotations

from typing import Any

from .mapping_service import control_database
from .naming import quote_identifier, validate_identifier


MAPPING_TABLE = "object_storage_mappings"
EVENT_LOG_TABLE = "event_tx_log"
OBJECT_EVENT_TABLE = "object_event"
EVENT_ERROR_TABLE = "event_errors"
SOURCE_BATCH_TABLE = "source_object_batches"
STALE_LOADING_MINUTES = 10


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
            cursor.execute(
                f"""SELECT tx.id, tx.mapping_id, tx.batch_num, tx.event_action, tx.event_status, tx.bucket_name,
                              tx.resource_name, tx.object_version, tx.message, tx.created_at{timing}
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
            if not self._table_exists(cursor, EVENT_LOG_TABLE):
                return [], 0
            control = quote_identifier(control_database(), "control database")
            timing, timing_join = self._event_timing_sql(cursor)
            cursor.execute(
                f"SELECT COUNT(*) AS total FROM {control}.`event_tx_log` WHERE target_database = %s AND target_table = %s",
                (database, table),
            )
            total = int(cursor.fetchone()["total"])
            cursor.execute(
                f"""SELECT tx.id, tx.mapping_id, tx.batch_num, tx.event_action, tx.event_status, tx.bucket_name,
                              tx.resource_name, tx.object_version, tx.message, tx.created_at{timing}
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
            if not self._table_exists(cursor, EVENT_LOG_TABLE):
                return []
            timing, timing_join = self._event_timing_sql(cursor)
            cursor.execute(
                f"""SELECT tx.id, tx.mapping_id, tx.target_database, tx.target_table, tx.batch_num, tx.event_action,
                              tx.event_status, tx.bucket_name, tx.resource_name, tx.object_version, tx.message, tx.created_at{timing}
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
        for row in rows:
            row["_lifecycle_status"] = "RECEIVED"
            row["_error_id"] = None
            if not event_log_exists:
                continue
            error_join = f"LEFT JOIN {control}.`event_errors` AS err ON err.event_log_id = tx.id" if error_log_exists else ""
            error_id = "err.id AS error_id" if error_log_exists else "NULL AS error_id"
            cursor.execute(
                f"""SELECT tx.event_status, {error_id}
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
                        f"""SELECT tx.event_status, {error_id}
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
