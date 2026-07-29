"""Read-only operator view of the consumer's durable capture queue."""
from __future__ import annotations
from typing import Any

class StreamCaptureService:
    def __init__(self, mysql): self.mysql = mysql
    def list_recent(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            try:
                cursor.execute("SELECT id, stream_id, partition_id, stream_offset, status, attempts, last_error, received_at, completed_at FROM stream_message_capture ORDER BY id DESC LIMIT %s", (limit,))
                return cursor.fetchall()
            except Exception as error:
                if getattr(error, "errno", None) == 1146:
                    return []
                raise

    def retry(self, capture_id: int) -> bool:
        if capture_id < 1:
            raise ValueError("Capture identifier is invalid.")
        with self.mysql.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE stream_message_capture SET status='CAPTURED', last_error=NULL WHERE id=%s AND status='FAILED'", (capture_id,))
            return cursor.rowcount == 1
