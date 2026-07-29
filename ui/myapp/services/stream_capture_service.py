"""Operator dashboard for durable OCI Streaming captures and their archive."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .naming import quote_identifier

ARCHIVE_SCHEMA_SQL = Path(__file__).parents[1] / "sql" / "init_stream_message_archive.sql"
VALID_CAPTURE_STATUSES = {"CAPTURED", "PROCESSING", "COMPLETED", "FAILED"}


class StreamCaptureService:
    def __init__(self, mysql, database: str):
        self.mysql = mysql
        quoted_database = quote_identifier(database, "stream data database")
        self.table = f"{quoted_database}.`stream_message_capture`"
        self.archive_table = f"{quoted_database}.`stream_message_archive`"

    @staticmethod
    def _identifier(capture_id: int | str) -> int:
        try:
            value = int(capture_id)
        except (TypeError, ValueError) as error:
            raise ValueError("Capture identifier is invalid.") from error
        if value < 1:
            raise ValueError("Capture identifier is invalid.")
        return value

    def _ensure_archive_table(self, cursor) -> None:
        try:
            statement = ARCHIVE_SCHEMA_SQL.read_text(encoding="utf-8").strip()
        except OSError as error:
            raise RuntimeError("Could not read the durable message archive SQL file.") from error
        cursor.execute(statement)

    def summary(self) -> dict[str, int]:
        counts = {status: 0 for status in VALID_CAPTURE_STATUSES}
        counts["ARCHIVED"] = 0
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            try:
                cursor.execute(f"SELECT status, COUNT(*) AS total FROM {self.table} GROUP BY status")
                for row in cursor.fetchall():
                    if row["status"] in counts:
                        counts[row["status"]] = int(row["total"])
                self._ensure_archive_table(cursor)
                cursor.execute(f"SELECT COUNT(*) AS total FROM {self.archive_table}")
                counts["ARCHIVED"] = int(cursor.fetchone()["total"])
            except Exception as error:
                if getattr(error, "errno", None) == 1146:
                    return counts
                raise
        return counts

    def list_recent(self, *, limit: int = 200, status: str = "", stream_id: str = "") -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        if status and status not in VALID_CAPTURE_STATUSES:
            raise ValueError("Durable message status filter is invalid.")
        stream_id = stream_id.strip()[:255]
        clauses, values = [], []
        if status:
            clauses.append("status=%s")
            values.append(status)
        if stream_id:
            clauses.append("stream_id=%s")
            values.append(stream_id)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            try:
                cursor.execute(f"SELECT id, stream_id, partition_id, stream_offset, status, attempts, last_error, received_at, completed_at FROM {self.table}{where} ORDER BY id DESC LIMIT %s", (*values, limit))
                return cursor.fetchall()
            except Exception as error:
                if getattr(error, "errno", None) == 1146:
                    return []
                raise

    def get(self, capture_id: int | str, *, archived: bool = False) -> dict[str, Any] | None:
        capture_id = self._identifier(capture_id)
        table = self.archive_table if archived else self.table
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            if archived:
                self._ensure_archive_table(cursor)
            cursor.execute(f"SELECT * FROM {table} WHERE id=%s", (capture_id,))
            row = cursor.fetchone()
        if row and not isinstance(row.get("payload"), str):
            row["payload"] = json.dumps(row["payload"], indent=2, sort_keys=True, default=str)
        return row

    def list_archived(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_archive_table(cursor)
            cursor.execute(f"SELECT id, capture_id, stream_id, partition_id, stream_offset, status, attempts, archived_at FROM {self.archive_table} ORDER BY id DESC LIMIT %s", (limit,))
            return cursor.fetchall()

    def retry(self, capture_id: int | str) -> bool:
        capture_id = self._identifier(capture_id)
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"UPDATE {self.table} SET status='CAPTURED', last_error=NULL, next_retry_at=NULL WHERE id=%s AND status='FAILED'", (capture_id,))
            conn.commit()
            return cursor.rowcount == 1

    def archive(self, capture_id: int | str) -> bool:
        """Move terminal messages to archive; never remove active consumer work."""
        capture_id = self._identifier(capture_id)
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            self._ensure_archive_table(cursor)
            cursor.execute(
                f"INSERT INTO {self.archive_table} (capture_id, stream_id, partition_id, stream_offset, message_key, payload, status, attempts, last_error, received_at, completed_at) "
                f"SELECT id, stream_id, partition_id, stream_offset, message_key, payload, status, attempts, last_error, received_at, completed_at FROM {self.table} "
                "WHERE id=%s AND status IN ('COMPLETED','FAILED')",
                (capture_id,),
            )
            if cursor.rowcount != 1:
                conn.rollback()
                return False
            cursor.execute(f"DELETE FROM {self.table} WHERE id=%s AND status IN ('COMPLETED','FAILED')", (capture_id,))
            conn.commit()
            return True

    def delete_archived(self, archive_id: int | str) -> bool:
        archive_id = self._identifier(archive_id)
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            self._ensure_archive_table(cursor)
            cursor.execute(f"DELETE FROM {self.archive_table} WHERE id=%s", (archive_id,))
            conn.commit()
            return cursor.rowcount == 1
