"""OCI Streaming consumer startup contract for FIFO and parallel deployments.

The consumer captures each decoded message in MySQL before processing. This
makes retries/replays independent of Streaming retention and cursor lifetime.
"""
from __future__ import annotations
import base64
import json
import os
import time
from typing import Any

def validate_mode(mode: str, partitions: int, replicas: int) -> None:
    mode = mode.upper()
    if mode == "FIFO" and (partitions != 1 or replicas != 1):
        raise ValueError("FIFO requires exactly one stream partition and one consumer replica.")
    if mode == "PARALLEL" and (partitions < 2 or not 1 <= replicas <= partitions):
        raise ValueError("PARALLEL requires at least two partitions and replicas not exceeding partitions.")
    if mode not in {"FIFO", "PARALLEL"}:
        raise ValueError("PROCESSING_MODE must be FIFO or PARALLEL.")

def decode_stream_message(value: str) -> dict[str, Any]:
    """Decode OCI Streaming's base64 JSON value before durable capture."""
    try:
        decoded = base64.b64decode(value, validate=True).decode("utf-8")
        payload = json.loads(decoded)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("Stream message must be base64-encoded JSON.") from error
    if not isinstance(payload, dict):
        raise ValueError("Stream message JSON must be an object.")
    return payload

def capture_batch(connection: Any, stream_id: str, messages: list[Any], partition: str | None = None) -> int:
    """Persist a received OCI batch before the cursor may advance."""
    from message_store import capture
    for message in messages:
        capture(
            connection, stream_id=stream_id, partition=partition if partition is not None else str(message.partition),
            offset=int(message.offset), key=str(getattr(message, "key", "")),
            payload=decode_stream_message(str(message.value)),
        )
    return len(messages)

def assigned_partitions(mode: str, partitions: int, assignment: str) -> list[str]:
    """Validate explicit Container Instance partition ownership.

    OCI Container Instances do not assign Streaming partitions automatically.
    Each running consumer is therefore given a disjoint comma-separated list.
    """
    selected = [item.strip() for item in assignment.split(",") if item.strip()]
    if not selected or len(set(selected)) != len(selected):
        raise ValueError("CONSUMER_PARTITIONS must contain unique assigned partition numbers.")
    if any(not item.isdigit() or not 0 <= int(item) < partitions for item in selected):
        raise ValueError("CONSUMER_PARTITIONS contains an out-of-range partition.")
    if mode.upper() == "FIFO" and selected != ["0"]:
        raise ValueError("FIFO consumer must be assigned only partition 0.")
    return selected

def process_one(connection: Any, processor: Any) -> bool:
    """Run one durable capture through the loader; leave failure retryable."""
    from message_store import claim_next, complete, fail
    row = claim_next(connection)
    if row is None:
        return False
    try:
        processor(row["payload"])
        complete(connection, int(row["id"]))
        return True
    except Exception as error:
        fail(connection, int(row["id"]), error)
        return False


def is_expired_cursor_error(error: Exception) -> bool:
    """Identify the OCI response that requires a safe cursor recreation."""
    return (
        getattr(error, "status", None) == 400
        and getattr(error, "code", "") == "InvalidParameter"
        and "cursor is expired" in str(getattr(error, "message", error)).lower()
    )


def run_partition_once(connection: Any, *, oci: Any, client: Any, stream_id: str, partition: str, processor: Any) -> int:
    """Capture one Streaming poll, checkpoint only after commit, then process one row."""
    from message_store import checkpoint, save_checkpoint
    from stream_client import first_cursor, read_messages
    cursor = checkpoint(connection, stream_id=stream_id, partition=partition) or first_cursor(oci, client, stream_id, partition)
    try:
        messages, next_cursor = read_messages(client, stream_id, cursor)
    except Exception as error:
        # OCI cursors expire. Restart from trim horizon; the durable unique
        # stream/partition/offset capture key makes the replay idempotent.
        if not is_expired_cursor_error(error):
            raise
        cursor = first_cursor(oci, client, stream_id, partition)
        messages, next_cursor = read_messages(client, stream_id, cursor)
    if messages:
        capture_batch(connection, stream_id, list(messages), partition)
    # OCI supplies a next cursor even for an empty read. Persisting it avoids
    # retaining one stale cursor through a long idle period.
    if next_cursor:
        save_checkpoint(connection, stream_id=stream_id, partition=partition, cursor_value=next_cursor)
    process_one(connection, processor)
    return len(messages)

def main() -> None:
    from database import connect
    from message_store import ensure_schema
    from vault_config import apply_database_environment, load_database_config
    from stream_client import client_for_stream
    from loader import process_event
    mode = os.environ.get("PROCESSING_MODE", "").upper()
    stream_id = os.environ.get("OCI_STREAM_ID", "")
    if not stream_id.startswith("ocid1.stream."):
        raise ValueError("OCI_STREAM_ID is required.")
    validate_mode(mode, int(os.environ.get("EXPECTED_PARTITION_COUNT", "0")), int(os.environ.get("CONSUMER_REPLICA_COUNT", "0")))
    partitions = assigned_partitions(mode, int(os.environ["EXPECTED_PARTITION_COUNT"]), os.environ.get("CONSUMER_PARTITIONS", ""))
    database_config = load_database_config()
    apply_database_environment(database_config)
    connection = connect(database_config)
    try:
        ensure_schema(connection)
        connection.commit()
        oci, client = client_for_stream(stream_id, os.environ.get("OCI_REGION", ""))
        print(f"consumer-ready mode={mode} stream={stream_id} partitions={','.join(partitions)}", flush=True)
        while True:
            read_count = 0
            for partition in partitions:
                read_count += run_partition_once(connection, oci=oci, client=client, stream_id=stream_id, partition=partition, processor=process_event)
            if not read_count:
                time.sleep(float(os.environ.get("CONSUMER_POLL_SECONDS", "1")))
    finally:
        connection.close()

if __name__ == "__main__":
    main()
