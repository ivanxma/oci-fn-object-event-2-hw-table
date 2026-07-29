"""Durable capture queue for OCI Streaming messages.

The unique stream/partition/offset key makes repeated delivery safe.  A
consumer captures the exact decoded payload before invoking the loader; retry
workers process CAPTURED/FAILED rows from MySQL, independent of retention.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
from typing import Any

DEFAULT_SCHEMA_SQL = Path(__file__).with_name("sql") / "init_stream_capture.sql"


def schema_sql_path() -> Path:
    """Return the packaged SQL file or an explicit operator-supplied override."""
    value = os.environ.get("CONSUMER_SCHEMA_SQL", "").strip()
    return Path(value) if value else DEFAULT_SCHEMA_SQL


def schema_statements(path: Path | None = None) -> list[str]:
    """Load simple semicolon-terminated initialization statements from SQL."""
    try:
        script = (path or schema_sql_path()).read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeError("Could not read the consumer schema SQL file.") from error
    executable_lines = [line for line in script.splitlines() if not line.lstrip().startswith("--")]
    statements = [statement.strip() for statement in "\n".join(executable_lines).split(";")]
    statements = [statement for statement in statements if statement]
    if len(statements) != 2:
        raise RuntimeError("Consumer schema SQL must contain the two initialization statements.")
    return statements

def ensure_schema(connection: Any) -> None:
    cursor = connection.cursor()
    for statement in schema_statements():
        # These statements are repository-owned schema initialization SQL, not
        # browser input; values remain parameterized everywhere else.
        cursor.execute(statement)

def checkpoint(connection: Any, *, stream_id: str, partition: str) -> str | None:
    cursor = connection.cursor()
    cursor.execute("SELECT cursor_value FROM stream_partition_checkpoint WHERE stream_id=%s AND partition_id=%s", (stream_id, partition))
    row = cursor.fetchone()
    return row[0] if row else None

def save_checkpoint(connection: Any, *, stream_id: str, partition: str, cursor_value: str) -> None:
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO stream_partition_checkpoint (stream_id, partition_id, cursor_value)
      VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE cursor_value=VALUES(cursor_value)""", (stream_id, partition, cursor_value))
    connection.commit()

def capture(connection: Any, *, stream_id: str, partition: str, offset: int, key: str, payload: dict[str, Any]) -> None:
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO stream_message_capture
      (stream_id, partition_id, stream_offset, message_key, payload)
      VALUES (%s,%s,%s,%s,%s)
      ON DUPLICATE KEY UPDATE received_at=received_at""", (stream_id, partition, offset, key, json.dumps(payload, separators=(",", ":"))))
    connection.commit()

def claim_next(connection: Any) -> dict[str, Any] | None:
    """Atomically claim one persisted message for loader processing."""
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT * FROM stream_message_capture WHERE status IN ('CAPTURED','FAILED') ORDER BY received_at, id LIMIT 1 FOR UPDATE")
    row = cursor.fetchone()
    if row:
        cursor.execute("UPDATE stream_message_capture SET status='PROCESSING', attempts=attempts+1, last_error=NULL WHERE id=%s", (row["id"],))
    connection.commit()
    return row

def complete(connection: Any, capture_id: int) -> None:
    cursor = connection.cursor()
    cursor.execute("UPDATE stream_message_capture SET status='COMPLETED', completed_at=UTC_TIMESTAMP(6) WHERE id=%s", (capture_id,))
    connection.commit()

def fail(connection: Any, capture_id: int, error: Exception) -> None:
    cursor = connection.cursor()
    cursor.execute("UPDATE stream_message_capture SET status='FAILED', last_error=%s WHERE id=%s", (str(error)[:65535], capture_id))
    connection.commit()
