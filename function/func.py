"""OCI Function: Object Storage event to partition-exchanged MySQL table."""

from __future__ import annotations

import io
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import oci
from fdk import response

from partition_loader import (
    Database,
    TargetTableError,
    allocate_or_get_batch,
    create_stage_table,
    drop_stage_table,
    ensure_control_tables,
    ensure_partition,
    event_source,
    load_csv_parallel,
    log_error,
    log_event,
    mark_error,
    mark_active,
    control_database,
    control_table,
    quote_identifier,
    resolve_mapping,
    target_definition,
    validate_and_exchange,
)


def _event_action(event: dict[str, Any]) -> str:
    event_type = str(event.get("eventType") or "").lower()
    if event_type.endswith("deleteobject"):
        return "DELETE"
    if event_type.endswith("updateobject"):
        return "UPDATE"
    if event_type.endswith("createobject"):
        return "CREATE"
    raise ValueError("Unsupported Object Storage event type.")


def _event_time(event: dict[str, Any]) -> datetime:
    value = event.get("eventTime")
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).replace(tzinfo=None)
        except ValueError:
            pass
    return datetime.now(UTC).replace(tzinfo=None)


def _object_name(event: dict[str, Any], source: dict[str, str]) -> str:
    details = (event.get("data") or {}).get("additionalDetails") or {}
    return str(details.get("objectName") or source["resource_name"])


def _write_event_audit(db: Database, event: dict[str, Any]) -> int:
    """Persist the received CloudEvent for the Event TX Object Storage Event view."""
    data = event.get("data") or {}
    details = data.get("additionalDetails") or {}
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(control_database(), 'control database')} CHARACTER SET utf8mb4")
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {control_table('object_event')} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                event_date DATETIME(6) NOT NULL,
                event_type VARCHAR(255) NOT NULL,
                event_message JSON NOT NULL,
                bucket_name VARCHAR(255) NULL,
                compartment_name VARCHAR(255) NULL,
                resource_name TEXT NULL,
                namespace VARCHAR(255) NULL,
                event_time DATETIME(6) NULL,
                received_at DATETIME(6) NOT NULL,
                completed_at DATETIME(6) NULL,
                duration_ms DECIMAL(16,3) NULL,
                KEY ix_object_event_time (event_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )
        for column, definition in (
            ("received_at", "DATETIME(6) NULL"),
            ("completed_at", "DATETIME(6) NULL"),
            ("duration_ms", "DECIMAL(16,3) NULL"),
        ):
            cursor.execute(
                """SELECT 1 FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = 'object_event' AND column_name = %s""",
                (control_database(), column),
            )
            if cursor.fetchone() is None:
                cursor.execute(f"ALTER TABLE {control_table('object_event')} ADD COLUMN {quote_identifier(column, 'event timing column')} {definition}")
        cursor.execute(
            f"""INSERT INTO {control_table('object_event')}
               (event_date, event_type, event_message, bucket_name, compartment_name, resource_name, namespace, event_time, received_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(6))""",
            (
                _event_time(event), str(event.get("eventType") or "unknown"), json.dumps(event, separators=(",", ":")),
                details.get("bucketName"), data.get("compartmentName"), data.get("resourceName"),
                details.get("namespace") or data.get("namespace"), _event_time(event),
            ),
        )
        return int(cursor.lastrowid)


class ObjectStorageRangeStream(io.RawIOBase):
    """A diskless, seek-free Object Storage reader using bounded HTTP ranges.

    OCI Functions can close one long-lived object response before a slow MySQL
    writer has consumed a large CSV.  Fetching modest byte ranges gives every
    response a short lifetime without accumulating the object in memory.
    """

    def __init__(self, client: Any, namespace: str, bucket: str, object_name: str, *, range_bytes: int) -> None:
        super().__init__()
        if range_bytes < 1024 * 1024:
            raise ValueError("OBJECT_STORAGE_RANGE_BYTES must be at least 1048576.")
        head = client.head_object(namespace, bucket, object_name)
        content_length = head.headers.get("content-length") or head.headers.get("Content-Length")
        if content_length is None:
            raise ValueError("Object Storage did not return Content-Length for the CSV object.")
        self._client, self._namespace, self._bucket, self._object_name = client, namespace, bucket, object_name
        self._length, self._range_bytes, self._position = int(content_length), range_bytes, 0
        self._body: Any | None = None
        self._body_remaining = 0

    def readable(self) -> bool:
        return True

    def _close_body(self) -> None:
        if self._body is not None:
            self._body.close()
            self._body = None
            self._body_remaining = 0

    def _open_range(self) -> None:
        end = min(self._position + self._range_bytes, self._length) - 1
        response = self._client.get_object(
            self._namespace, self._bucket, self._object_name,
            range=f"bytes={self._position}-{end}",
        )
        self._body = response.data.raw
        self._body_remaining = end - self._position + 1

    def readinto(self, buffer: bytearray) -> int:
        if self.closed:
            raise ValueError("I/O operation on closed file.")
        if self._position >= self._length:
            return 0
        written = 0
        view = memoryview(buffer)
        while written < len(view) and self._position < self._length:
            if self._body is None:
                self._open_range()
            chunk = self._body.read(min(len(view) - written, self._body_remaining))
            if not chunk:
                remaining = self._body_remaining
                self._close_body()
                if remaining:
                    raise OSError("Object Storage range response ended before its advertised byte range.")
                continue
            size = len(chunk)
            view[written:written + size] = chunk
            written += size
            self._position += size
            self._body_remaining -= size
            if self._body_remaining == 0:
                self._close_body()
        return written

    def close(self) -> None:
        self._close_body()
        super().close()


def _object_stream(event: dict[str, Any], source: dict[str, str]) -> ObjectStorageRangeStream:
    details = (event.get("data") or {}).get("additionalDetails") or {}
    namespace = str(details.get("namespace") or os.environ.get("OBJECT_STORAGE_NAMESPACE") or "")
    if not namespace:
        raise ValueError("Object Storage event must include a namespace or set OBJECT_STORAGE_NAMESPACE.")
    signer = oci.auth.signers.get_resource_principals_signer()
    # A large CSV can take longer to consume than the SDK's default read
    # timeout because ingestion pauses briefly while writer workers commit each
    # batch.  Keep the HTTP response open for the whole Function invocation.
    read_timeout = int(os.environ.get("OBJECT_STORAGE_READ_TIMEOUT_SECONDS", "300"))
    client = oci.object_storage.ObjectStorageClient(
        config={}, signer=signer, timeout=(10, read_timeout)
    )
    return ObjectStorageRangeStream(
        client, namespace, source["bucket_name"], _object_name(event, source),
        range_bytes=int(os.environ.get("OBJECT_STORAGE_RANGE_BYTES", str(32 * 1024 * 1024))),
    )


def _run_load(db: Database, event: dict[str, Any], source: dict[str, str], *, create: bool) -> dict[str, Any]:
    action, mapping, record, stage = "CREATE" if create else "UPDATE", None, None, None
    try:
        mapping = resolve_mapping(db, source)
        columns = target_definition(db, mapping)
        record = allocate_or_get_batch(db, mapping, source, create=create)
        ensure_partition(db, mapping, record["batch_num"])
        stage = create_stage_table(db, mapping, record["batch_num"])
        object_stream = _object_stream(event, source)
        # Decode a bounded range stream directly: no object copy is made in /tmp
        # or elsewhere on the Function filesystem.
        with io.TextIOWrapper(io.BufferedReader(object_stream), encoding="utf-8", newline="") as csv_stream:
            rows = load_csv_parallel(
                db, mapping, stage, record["batch_num"], columns, csv_stream,
                int(os.environ.get("BATCH_ROWS", "10000")), int(mapping.get("worker_threads") or os.environ.get("WRITER_WORKERS", "4")),
            )
        validate_and_exchange(db, mapping, stage, record["batch_num"])
        mark_active(db, record["id"])
        log_event(db, source, action, "SUCCESS", mapping, record["batch_num"], f"Loaded {rows} row(s) by partition exchange.")
        return {"action": action.lower(), "batch_num": record["batch_num"], "rows": rows, "target": f"{mapping['target_database']}.{mapping['target_table']}", "invocation_mode": mapping.get("invocation_mode", "SYNC"), "worker_threads": mapping.get("worker_threads", 4)}
    except Exception as error:
        if record is not None:
            try:
                mark_error(db, record["id"])
            except Exception:
                pass
        log_error(db, source, action, error, mapping, record.get("batch_num") if record else None)
        # The outer handler owns errors that occur before a load/delete helper is
        # selected.  Mark helper failures so it does not write a duplicate event
        # transaction record without the mapping and batch information.
        setattr(error, "event_logged", True)
        raise
    finally:
        if mapping is not None and stage is not None:
            try:
                drop_stage_table(db, mapping, stage)
            except Exception:
                # A failed cleanup is visible through the UI's staging-table
                # section and must not mask the load error.
                pass


def _run_delete(db: Database, event: dict[str, Any], source: dict[str, str]) -> dict[str, Any]:
    # Retain the established prototype's deletion semantics, including an idempotent no-op.
    from partition_loader import run_delete

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", encoding="utf-8") as event_file:
        json.dump(event, event_file)
        event_file.flush()
        try:
            return run_delete(Path(event_file.name))
        except Exception as error:
            setattr(error, "event_logged", True)
            raise


def handler(ctx: Any, data: io.BytesIO | None = None) -> response.Response:
    db: Database | None = None
    source: dict[str, str] | None = None
    mapping: dict[str, Any] | None = None
    try:
        event = json.loads(data.getvalue().decode("utf-8") if data else "{}")
        if not isinstance(event, dict):
            raise ValueError("Expected an Object Storage CloudEvent JSON object.")
        db = Database()
        ensure_control_tables(db)
        object_event_id = _write_event_audit(db, event)
        source = event_source(event)
        source["object_event_id"] = object_event_id
        action = _event_action(event)
        # Event Rules arrive synchronously.  A mapping can hand the same
        # CloudEvent back to this Function as a detached invocation, allowing
        # large objects to run beyond the 300-second synchronous ceiling.
        detached_worker = bool(event.get("_detached_worker")) or os.environ.get("DETACHED_WORKER", "false").lower() == "true"
        mapping = None if detached_worker else resolve_mapping(db, source)
        if mapping and mapping.get("invocation_mode", "SYNC") == "DETACHED":
            function_id = os.environ.get("FUNCTION_ID", "")
            if not function_id or os.environ.get("DETACHED_ENABLED", "false").lower() != "true":
                raise ValueError("Mapping requests DETACHED mode but detached execution is not enabled or FUNCTION_ID is missing.")
            # A Function invocation runs with a resource-principal identity;
            # instance principals are only available on Compute instances.
            signer = oci.auth.signers.get_resource_principals_signer()
            invoke_endpoint = os.environ.get("FUNCTION_INVOKE_ENDPOINT", "")
            if not invoke_endpoint:
                raise ValueError("Mapping requests DETACHED mode but FUNCTION_INVOKE_ENDPOINT is missing.")
            client = oci.functions.FunctionsInvokeClient(
                {"region": os.environ.get("OCI_REGION", "")},
                signer=signer,
                service_endpoint=invoke_endpoint,
            )
            worker_event = dict(event)
            worker_event["_detached_worker"] = True
            client.invoke_function(function_id=function_id, invoke_function_body=json.dumps(worker_event).encode(), fn_intent="cloudevent", fn_invoke_type="detached")
            return response.Response(ctx, response_data=json.dumps({"status": "detached_submitted", "mapping_id": mapping["id"], "worker_threads": mapping.get("worker_threads", 4)}), headers={"Content-Type": "application/json"}, status_code=202)
        result = _run_delete(db, event, source) if action == "DELETE" else _run_load(db, event, source, create=action == "CREATE")
        return response.Response(ctx, response_data=json.dumps({"status": "success", **result}), headers={"Content-Type": "application/json"}, status_code=200)
    except Exception as error:
        if db is not None and source is not None and not getattr(error, "event_logged", False):
            action = "UNKNOWN"
            if "event" in locals():
                try:
                    action = _event_action(event)
                except ValueError:
                    pass
            log_error(db, source, action, error, mapping)
        message = "Target table is not ready for partition exchange." if isinstance(error, TargetTableError) else str(error)
        return response.Response(ctx, response_data=json.dumps({"status": "error", "message": message}), headers={"Content-Type": "application/json"}, status_code=500)
