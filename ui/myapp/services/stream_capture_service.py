"""Operator dashboard for durable captures and time-partitioned archive tables."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .naming import quote_identifier

ARCHIVE_REGISTRY_SQL = Path(__file__).parents[1] / "sql" / "init_stream_message_archive.sql"
ARCHIVE_PARTITION_SQL = Path(__file__).parents[1] / "sql" / "init_stream_message_archive_partition.sql"
VALID_CAPTURE_STATUSES = {"CAPTURED", "PROCESSING", "COMPLETED", "FAILED"}
VALID_ARCHIVE_GRANULARITIES = {"YEAR", "MONTH", "WEEK"}


class StreamCaptureService:
    def __init__(self, mysql, database: str):
        self.mysql = mysql
        self.database = quote_identifier(database, "stream data database")
        self.table = f"{self.database}.`stream_message_capture`"
        self.registry = f"{self.database}.`stream_message_archive_partitions`"

    @staticmethod
    def _identifier(value: int | str) -> int:
        try: value = int(value)
        except (TypeError, ValueError) as error: raise ValueError("Capture identifier is invalid.") from error
        if value < 1: raise ValueError("Capture identifier is invalid.")
        return value

    @staticmethod
    def _partition_spec(granularity: str, now: datetime | None = None) -> tuple[str, str, str]:
        granularity = granularity.upper()
        if granularity not in VALID_ARCHIVE_GRANULARITIES:
            raise ValueError("Archive partition must be Year, Month, or Week.")
        now = now or datetime.now(timezone.utc)
        if granularity == "YEAR": key, suffix = f"{now.year:04d}", f"y_{now.year:04d}"
        elif granularity == "MONTH": key, suffix = f"{now.year:04d}-{now.month:02d}", f"m_{now.year:04d}{now.month:02d}"
        else:
            iso_year, week, _ = now.isocalendar(); key, suffix = f"{iso_year:04d}-W{week:02d}", f"w_{iso_year:04d}w{week:02d}"
        return f"{granularity}-{key}", key, f"stream_message_archive_{suffix}"

    def _partition_table(self, table_name: str) -> str:
        if not table_name.startswith("stream_message_archive_"):
            raise ValueError("Archive partition is invalid.")
        return f"{self.database}.{quote_identifier(table_name, 'archive partition table')}"

    @staticmethod
    def _sql(path: Path) -> str:
        try: return path.read_text(encoding="utf-8").strip()
        except OSError as error: raise RuntimeError("Could not read the durable archive SQL file.") from error

    def _ensure_registry(self, cursor) -> None:
        cursor.execute(self._sql(ARCHIVE_REGISTRY_SQL).replace("__ARCHIVE_REGISTRY__", self.registry))

    def _partitions(self, cursor) -> list[dict[str, Any]]:
        self._ensure_registry(cursor)
        cursor.execute(f"SELECT partition_name, granularity, period_key, table_name, created_at FROM {self.registry} ORDER BY granularity, period_key DESC")
        return cursor.fetchall()

    def _ensure_partition(self, cursor, granularity: str) -> tuple[str, str]:
        name, period, table_name = self._partition_spec(granularity)
        self._ensure_registry(cursor)
        cursor.execute(f"SELECT table_name FROM {self.registry} WHERE partition_name=%s", (name,))
        row = cursor.fetchone()
        if row:
            return name, str(row["table_name"] if isinstance(row, dict) else row[0])
        table = self._partition_table(table_name)
        cursor.execute(self._sql(ARCHIVE_PARTITION_SQL).replace("__ARCHIVE_TABLE__", table))
        cursor.execute(f"INSERT INTO {self.registry} (partition_name, granularity, period_key, table_name) VALUES (%s,%s,%s,%s)", (name, granularity.upper(), period, table_name))
        return name, table_name

    def summary(self) -> dict[str, int]:
        counts = {status: 0 for status in VALID_CAPTURE_STATUSES}; counts["ARCHIVED"] = 0
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            try:
                cursor.execute(f"SELECT status, COUNT(*) AS total FROM {self.table} GROUP BY status")
                for row in cursor.fetchall():
                    if row["status"] in counts: counts[row["status"]] = int(row["total"])
                for partition in self._partitions(cursor):
                    cursor.execute(f"SELECT COUNT(*) AS total FROM {self._partition_table(partition['table_name'])}")
                    counts["ARCHIVED"] += int(cursor.fetchone()["total"])
            except Exception as error:
                if getattr(error, "errno", None) == 1146: return counts
                raise
        return counts

    def list_recent(self, *, limit: int = 200, status: str = "", stream_id: str = "") -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500)); status = status.upper()
        if status and status not in VALID_CAPTURE_STATUSES: raise ValueError("Durable message status filter is invalid.")
        clauses, values = [], []
        if status: clauses.append("status=%s"); values.append(status)
        if stream_id.strip(): clauses.append("stream_id=%s"); values.append(stream_id.strip()[:255])
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            try:
                cursor.execute(f"SELECT id, stream_id, partition_id, stream_offset, status, attempts, last_error, received_at, completed_at FROM {self.table}{where} ORDER BY id DESC LIMIT %s", (*values, limit))
                return cursor.fetchall()
            except Exception as error:
                if getattr(error, "errno", None) == 1146: return []
                raise

    def get(self, capture_id: int | str) -> dict[str, Any] | None:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True); cursor.execute(f"SELECT * FROM {self.table} WHERE id=%s", (self._identifier(capture_id),)); row = cursor.fetchone()
        return self._payload_text(row)

    @staticmethod
    def _payload_text(row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row and not isinstance(row.get("payload"), str): row["payload"] = json.dumps(row["payload"], indent=2, sort_keys=True, default=str)
        return row

    def list_archived(self, limit: int = 100) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        rows, partitions = [], []
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            for partition in self._partitions(cursor):
                table = self._partition_table(partition["table_name"])
                cursor.execute(f"SELECT COUNT(*) AS total FROM {table}"); partition["message_count"] = int(cursor.fetchone()["total"])
                partitions.append(partition)
                cursor.execute(f"SELECT id, capture_id, stream_id, partition_id, stream_offset, status, attempts, archived_at FROM {table} ORDER BY id DESC LIMIT %s", (limit,))
                rows.extend([{**row, "archive_partition": partition["partition_name"]} for row in cursor.fetchall()])
        return sorted(rows, key=lambda item: item["archived_at"], reverse=True)[:limit], partitions

    def get_archived(self, partition_name: str, archive_id: int | str) -> dict[str, Any] | None:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True); self._ensure_registry(cursor)
            cursor.execute(f"SELECT table_name FROM {self.registry} WHERE partition_name=%s", (partition_name,)); row = cursor.fetchone()
            if not row: return None
            table_name = row["table_name"]; cursor.execute(f"SELECT * FROM {self._partition_table(table_name)} WHERE id=%s", (self._identifier(archive_id),)); result = cursor.fetchone()
        return self._payload_text(result)

    def retry(self, capture_id: int | str) -> bool:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(); cursor.execute(f"UPDATE {self.table} SET status='CAPTURED', last_error=NULL, next_retry_at=NULL WHERE id=%s AND status='FAILED'", (self._identifier(capture_id),)); conn.commit(); return cursor.rowcount == 1

    def archive(self, capture_id: int | str, granularity: str) -> bool:
        capture_id = self._identifier(capture_id)
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True); _, table_name = self._ensure_partition(cursor, granularity); table = self._partition_table(table_name)
            cursor.execute(f"INSERT INTO {table} (capture_id, stream_id, partition_id, stream_offset, message_key, payload, status, attempts, last_error, received_at, completed_at) SELECT id, stream_id, partition_id, stream_offset, message_key, payload, status, attempts, last_error, received_at, completed_at FROM {self.table} WHERE id=%s AND status IN ('COMPLETED','FAILED')", (capture_id,))
            if cursor.rowcount != 1: conn.rollback(); return False
            cursor.execute(f"DELETE FROM {self.table} WHERE id=%s AND status IN ('COMPLETED','FAILED')", (capture_id,)); conn.commit(); return True

    def delete_archived(self, partition_name: str, archive_id: int | str) -> bool:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True); self._ensure_registry(cursor); cursor.execute(f"SELECT table_name FROM {self.registry} WHERE partition_name=%s", (partition_name,)); row = cursor.fetchone()
            if not row: return False
            cursor.execute(f"DELETE FROM {self._partition_table(row['table_name'])} WHERE id=%s", (self._identifier(archive_id),)); conn.commit(); return cursor.rowcount == 1

    def delete_archive_partition(self, partition_name: str) -> bool:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True); self._ensure_registry(cursor); cursor.execute(f"SELECT table_name FROM {self.registry} WHERE partition_name=%s", (partition_name,)); row = cursor.fetchone()
            if not row: return False
            cursor.execute(f"DROP TABLE {self._partition_table(row['table_name'])}"); cursor.execute(f"DELETE FROM {self.registry} WHERE partition_name=%s", (partition_name,)); conn.commit(); return True
