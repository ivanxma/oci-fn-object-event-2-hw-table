"""Durable capture queue for OCI Streaming messages.

The unique stream/partition/offset key makes repeated delivery safe.  A
processor captures the exact decoded payload before invoking the loader; retry
workers process CAPTURED/FAILED rows from MySQL, independent of retention.
"""
from __future__ import annotations
import json
import os
from pathlib import Path
from typing import Any

DEFAULT_SCHEMA_SQL = Path(__file__).with_name("sql") / "init_stream_capture.sql"
RETRY_MIGRATION_SQL = Path(__file__).with_name("sql") / "migrate_stream_capture_retry.sql"
PROCESSING_MIGRATION_SQL = Path(__file__).with_name("sql") / "migrate_stream_capture_processing.sql"
RELEASE_MIGRATION_SQL = Path(__file__).with_name("sql") / "migrate_release_stamp.sql"
METRICS_MIGRATION_SQL = Path(__file__).with_name("sql") / "migrate_stream_capture_metrics.sql"


def schema_sql_path() -> Path:
    """Return the packaged SQL file or an explicit operator-supplied override."""
    value = os.environ.get("PROCESSOR_SCHEMA_SQL", "").strip()
    return Path(value) if value else DEFAULT_SCHEMA_SQL


def schema_statements(path: Path | None = None) -> list[str]:
    """Load simple semicolon-terminated initialization statements from SQL."""
    try:
        script = (path or schema_sql_path()).read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeError("Could not read the processor schema SQL file.") from error
    executable_lines = [line for line in script.splitlines() if not line.lstrip().startswith("--")]
    statements = [statement.strip() for statement in "\n".join(executable_lines).split(";")]
    statements = [statement for statement in statements if statement]
    if len(statements) != 3:
        raise RuntimeError("Processor schema SQL must contain the three initialization statements.")
    return statements


def migration_statements(path: Path = RETRY_MIGRATION_SQL) -> list[str]:
    """Load the tracked retry migration without accepting runtime SQL text."""
    try:
        script = path.read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeError("Could not read a processor migration SQL file.") from error
    statements = [statement.strip() for statement in "\n".join(line for line in script.splitlines() if not line.lstrip().startswith("--")).split(";")]
    return [statement for statement in statements if statement]

def ensure_schema(connection: Any) -> None:
    cursor = connection.cursor()
    for statement in schema_statements():
        # These statements are repository-owned schema initialization SQL, not
        # browser input; values remain parameterized everywhere else.
        cursor.execute(statement)
    for column, migration in (("next_retry_at", RETRY_MIGRATION_SQL), ("processing_started_at", PROCESSING_MIGRATION_SQL)):
        cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='stream_message_capture' AND column_name=%s", (column,))
        if cursor.fetchone()[0] == 0:
            for statement in migration_statements(migration):
                cursor.execute(statement)
    for table in ("stream_message_capture", "stream_event_tx_log"):
        cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name=%s AND column_name='processor_release_stamp'", (table,))
        if cursor.fetchone()[0] == 0:
            statement = next(item for item in migration_statements(RELEASE_MIGRATION_SQL) if item.startswith(f"ALTER TABLE {table}"))
            cursor.execute(statement)
    cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='stream_message_capture' AND column_name='rows_affected'")
    if cursor.fetchone()[0] == 0:
        for statement in migration_statements(METRICS_MIGRATION_SQL):
            cursor.execute(statement)


def retry_delay_seconds(attempts: int) -> int:
    """Bound retries so a poison FIFO record cannot create a busy loop."""
    return min(300, 2 ** min(9, max(1, int(attempts))))


def processing_lease_seconds(value: str | None = None) -> int:
    """Return the bounded lease used to recover after processor interruption."""
    try:
        seconds = int(value if value is not None else os.environ.get("PROCESSOR_PROCESSING_LEASE_SECONDS", "300"))
    except ValueError as error:
        raise ValueError("PROCESSOR_PROCESSING_LEASE_SECONDS must be a whole number.") from error
    if not 30 <= seconds <= 3600:
        raise ValueError("PROCESSOR_PROCESSING_LEASE_SECONDS must be from 30 to 3600.")
    return seconds

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
    from release import release_stamp
    cursor = connection.cursor()
    cursor.execute("""INSERT INTO stream_message_capture
      (stream_id, partition_id, stream_offset, message_key, payload, processor_release_stamp)
      VALUES (%s,%s,%s,%s,%s,%s)
      ON DUPLICATE KEY UPDATE received_at=received_at""", (stream_id, partition, offset, key, json.dumps(payload, separators=(",", ":")), release_stamp()))
    cursor.execute("""INSERT INTO stream_event_tx_log
      (capture_id, stream_id, partition_id, stream_offset, processor_release_stamp, event_status, attempts, received_at)
      SELECT id, stream_id, partition_id, stream_offset, processor_release_stamp, status, attempts, received_at
        FROM stream_message_capture WHERE stream_id=%s AND partition_id=%s AND stream_offset=%s
      ON DUPLICATE KEY UPDATE updated_at=updated_at""", (stream_id, partition, offset))
    connection.commit()


def decoded_payload(value: Any) -> dict[str, Any]:
    """Normalize Connector/Python's JSON result before loader invocation."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as error:
            raise RuntimeError("Durable capture payload is not valid JSON.") from error
    if not isinstance(value, dict):
        raise RuntimeError("Durable capture payload must be a JSON object.")
    return value


def _object_identity(payload: dict[str, Any]) -> tuple[str, str]:
    data = payload.get("data") or {}
    details = data.get("additionalDetails") or {}
    return (
        str(details.get("bucketName") or ""),
        str(data.get("resourceName") or details.get("objectName") or ""),
    )


def has_later_delete(connection: Any, row: dict[str, Any]) -> bool:
    """Return whether the same stream partition captured a later object delete.

    A create/update can legitimately become unreadable before it is processed
    when a later delete is already present in the ordered stream. Retrying that
    stale load forever prevents the delete from converging the mapped table.
    This check is intentionally partition- and offset-scoped so it cannot infer
    order between independent partitions.
    """
    payload = decoded_payload(row["payload"])
    event_type = str(payload.get("eventType") or "").lower()
    if not event_type.endswith(("createobject", "updateobject")):
        return False
    identity = _object_identity(payload)
    if not all(identity):
        return False
    cursor = connection.cursor()
    cursor.execute(
        """SELECT payload FROM stream_message_capture
           WHERE stream_id=%s AND partition_id=%s AND stream_offset>%s
           ORDER BY stream_offset""",
        (row["stream_id"], row["partition_id"], row["stream_offset"]),
    )
    for candidate in cursor.fetchall():
        later = decoded_payload(candidate[0] if not isinstance(candidate, dict) else candidate["payload"])
        if (
            str(later.get("eventType") or "").lower().endswith("deleteobject")
            and _object_identity(later) == identity
        ):
            return True
    return False


def claim_next(connection: Any, *, stream_id: str, partitions: list[str]) -> dict[str, Any] | None:
    """Atomically claim only a message owned by this stream processor."""
    if not partitions:
        raise ValueError("At least one assigned partition is required.")
    cursor = connection.cursor(dictionary=True)
    placeholders = ", ".join("%s" for _ in partitions)
    lease = processing_lease_seconds()
    cursor.execute(
        f"UPDATE stream_message_capture SET status='FAILED', last_error=COALESCE(last_error, 'Recovered after processor interruption.'), "
        "next_retry_at=UTC_TIMESTAMP(6), processing_started_at=NULL "
        f"WHERE stream_id=%s AND partition_id IN ({placeholders}) AND status='PROCESSING' "
        "AND processing_started_at < DATE_SUB(UTC_TIMESTAMP(6), INTERVAL %s SECOND)",
        (stream_id, *partitions, lease),
    )
    cursor.execute(
        f"SELECT * FROM stream_message_capture WHERE stream_id=%s AND partition_id IN ({placeholders}) "
        "AND status IN ('CAPTURED','FAILED') AND (next_retry_at IS NULL OR next_retry_at <= UTC_TIMESTAMP(6)) "
        "ORDER BY received_at, id LIMIT 1 FOR UPDATE",
        (stream_id, *partitions),
    )
    row = cursor.fetchone()
    if row:
        row["payload"] = decoded_payload(row["payload"])
        cursor.execute("UPDATE stream_message_capture SET status='PROCESSING', attempts=attempts+1, last_error=NULL, next_retry_at=NULL, processing_started_at=UTC_TIMESTAMP(6) WHERE id=%s", (row["id"],))
        cursor.execute("UPDATE stream_event_tx_log SET event_status='PROCESSING', attempts=attempts+1, message=NULL, completed_at=NULL WHERE capture_id=%s", (row["id"],))
    connection.commit()
    return row

def complete(connection: Any, capture_id: int, metrics: dict[str, Any] | None = None) -> None:
    metrics = metrics or {}
    values = (
        metrics.get("rows"),
        metrics.get("object_size_bytes"),
        metrics.get("loader_duration_ms"),
        metrics.get("exchange_duration_ms"),
        capture_id,
    )
    cursor = connection.cursor()
    cursor.execute(
        """UPDATE stream_message_capture
              SET status='COMPLETED', completed_at=UTC_TIMESTAMP(6),
                  rows_affected=%s, object_size_bytes=%s,
                  loader_duration_ms=%s, exchange_duration_ms=%s,
                  next_retry_at=NULL
            WHERE id=%s""",
        values,
    )
    cursor.execute(
        """UPDATE stream_event_tx_log
              SET event_status='COMPLETED', completed_at=UTC_TIMESTAMP(6),
                  rows_affected=%s, object_size_bytes=%s,
                  loader_duration_ms=%s, exchange_duration_ms=%s,
                  message=NULL
            WHERE capture_id=%s""",
        values,
    )
    connection.commit()

def fail(connection: Any, capture_id: int, error: Exception) -> None:
    cursor = connection.cursor()
    cursor.execute("SELECT attempts FROM stream_message_capture WHERE id=%s", (capture_id,))
    row = cursor.fetchone()
    delay = retry_delay_seconds(int(row[0]) if row else 1)
    cursor.execute("UPDATE stream_message_capture SET status='FAILED', last_error=%s, next_retry_at=DATE_ADD(UTC_TIMESTAMP(6), INTERVAL %s SECOND), processing_started_at=NULL WHERE id=%s", (str(error)[:65535], delay, capture_id))
    cursor.execute("UPDATE stream_event_tx_log SET event_status='FAILED', message=%s WHERE capture_id=%s", (str(error)[:65535], capture_id))
    connection.commit()
