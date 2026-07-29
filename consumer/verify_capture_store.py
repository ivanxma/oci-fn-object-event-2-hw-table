"""Bounded integration check for the consumer's durable MySQL capture queue.

It creates one unique verification stream record, proves deduplication and
retry/completion, then removes only that record and its checkpoint.
"""
from __future__ import annotations

import uuid

from database import connect
from message_store import capture, claim_next, complete, ensure_schema, fail
from vault_config import load_database_config, stream_data_database_config


def main() -> None:
    config = stream_data_database_config(load_database_config())
    stream_id = f"verification-{uuid.uuid4()}"
    connection = connect(config)
    try:
        ensure_schema(connection)
        capture(connection, stream_id=stream_id, partition="0", offset=1, key="verification", payload={"verification": True})
        capture(connection, stream_id=stream_id, partition="0", offset=1, key="verification", payload={"verification": True})
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM stream_message_capture WHERE stream_id=%s", (stream_id,))
        if cursor.fetchone()[0] != 1:
            raise RuntimeError("Capture idempotency check failed.")
        row = claim_next(connection, stream_id=stream_id, partitions=["0"])
        if not row:
            raise RuntimeError("Capture claim check failed.")
        fail(connection, int(row["id"]), RuntimeError("verification retry"))
        cursor.execute("SELECT next_retry_at > UTC_TIMESTAMP(6) FROM stream_message_capture WHERE stream_id=%s", (stream_id,))
        if cursor.fetchone()[0] != 1:
            raise RuntimeError("Capture retry backoff check failed.")
        # This verifier proves the manual-retry path without waiting for the
        # exponential schedule. It touches only its unique temporary record.
        cursor.execute("UPDATE stream_message_capture SET next_retry_at=UTC_TIMESTAMP(6) WHERE stream_id=%s", (stream_id,))
        connection.commit()
        retry = claim_next(connection, stream_id=stream_id, partitions=["0"])
        if not retry or int(retry["attempts"]) != 1:
            raise RuntimeError("Capture retry check failed.")
        complete(connection, int(retry["id"]))
        cursor.execute("SELECT status, attempts FROM stream_message_capture WHERE stream_id=%s", (stream_id,))
        status, attempts = cursor.fetchone()
        if status != "COMPLETED" or int(attempts) != 2:
            raise RuntimeError("Capture completion check failed.")
        capture(connection, stream_id=stream_id, partition="0", offset=2, key="recovery", payload={"verification": "recovery"})
        cursor.execute("UPDATE stream_message_capture SET status='PROCESSING', processing_started_at=DATE_SUB(UTC_TIMESTAMP(6), INTERVAL 301 SECOND) WHERE stream_id=%s AND stream_offset=2", (stream_id,))
        connection.commit()
        recovered = claim_next(connection, stream_id=stream_id, partitions=["0"])
        if not recovered or int(recovered["stream_offset"]) != 2:
            raise RuntimeError("Interrupted processing recovery check failed.")
        complete(connection, int(recovered["id"]))
        print("PASS: durable capture idempotency, scheduled retry, completion, and interrupted-processing recovery")
    finally:
        cleanup = connection.cursor()
        cleanup.execute("DELETE FROM stream_partition_checkpoint WHERE stream_id=%s", (stream_id,))
        cleanup.execute("DELETE FROM stream_message_capture WHERE stream_id=%s", (stream_id,))
        connection.commit()
        connection.close()


if __name__ == "__main__":
    main()
