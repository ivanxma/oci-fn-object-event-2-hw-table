"""OCI Function: Object Storage event to partition-exchanged MySQL table."""

from __future__ import annotations

import io
import json
import os
import time
from datetime import UTC, datetime
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
    delete_object,
)
from work_queue import (
    LeaseHeartbeat,
    acquire_lane,
    block_entry,
    claim_next,
    complete_entry,
    defer_entry,
    enqueue_event,
    ensure_queue_tables,
    has_start_budget,
    invocation_owner,
    monotonic_deadline,
    queue_binding,
    release_lane,
    start_attempt,
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
                invocation_mode ENUM('SYNC','DETACHED') NULL,
                KEY ix_object_event_time (event_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )
        for column, definition in (
            ("received_at", "DATETIME(6) NULL"),
            ("completed_at", "DATETIME(6) NULL"),
            ("duration_ms", "DECIMAL(16,3) NULL"),
            ("invocation_mode", "ENUM('SYNC','DETACHED') NULL"),
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


def _set_object_event_mode(db: Database, object_event_id: int, invocation_mode: str) -> None:
    """Persist the mapping mode selected for this immutable event execution."""
    mode = str(invocation_mode or "").upper()
    if mode not in {"SYNC", "DETACHED"}:
        raise ValueError("Resolved mapping has an unsupported invocation mode.")
    with db.connection() as connection:
        connection.cursor().execute(
            f"UPDATE {control_table('object_event')} SET invocation_mode = %s WHERE id = %s",
            (mode, object_event_id),
        )


def _complete_duplicate_object_event(db: Database, object_event_id: int) -> None:
    """Close the raw audit row for an at-least-once duplicate delivery."""
    with db.connection() as connection:
        connection.cursor().execute(
            f"""UPDATE {control_table('object_event')}
                   SET completed_at=UTC_TIMESTAMP(6),
                       duration_ms=TIMESTAMPDIFF(MICROSECOND,received_at,UTC_TIMESTAMP(6))/1000
                 WHERE id=%s""",
            (object_event_id,),
        )


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


def _run_load(
    db: Database,
    event: dict[str, Any],
    source: dict[str, str],
    *,
    create: bool,
    mapping_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    action, mapping, record, stage = "CREATE" if create else "UPDATE", None, None, None
    try:
        mapping = mapping_override or resolve_mapping(db, source)
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


def _run_delete(
    db: Database,
    event: dict[str, Any],
    source: dict[str, Any],
    *,
    mapping_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    mapping = mapping_override or resolve_mapping(db, source)
    return delete_object(db, mapping, source)


def _invoke_detached(binding_key: str) -> None:
    function_id = os.environ.get("FUNCTION_ID", "")
    invoke_endpoint = os.environ.get("FUNCTION_INVOKE_ENDPOINT", "")
    if not function_id or not invoke_endpoint or os.environ.get("DETACHED_ENABLED", "false").lower() != "true":
        raise ValueError("Detached queue processing requires DETACHED_ENABLED, FUNCTION_ID, and FUNCTION_INVOKE_ENDPOINT.")
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.functions.FunctionsInvokeClient(
        {"region": os.environ.get("OCI_REGION", "")},
        signer=signer,
        service_endpoint=invoke_endpoint,
    )
    worker_event = {"_queue_worker": True, "_queue_binding_key": binding_key}
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            client.invoke_function(
                function_id=function_id,
                invoke_function_body=json.dumps(worker_event).encode(),
                fn_intent="cloudevent",
                fn_invoke_type="detached",
            )
            return
        except Exception as error:
            last_error = error
            status = int(getattr(error, "status", 0) or 0)
            if status not in {429, 500, 502, 503, 504} or attempt == 2:
                raise
            time.sleep(0.5 * (2**attempt))
    if last_error:
        raise last_error


def _mapping_from_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(entry["mapping_id"]),
        "target_database": entry["target_database"],
        "target_table": entry["target_table"],
        "invocation_mode": entry["invocation_mode"],
        "worker_threads": int(entry["worker_threads"]),
        "queue_scope": entry["queue_scope"],
    }


def _payload(entry: dict[str, Any]) -> dict[str, Any]:
    value = entry["event_payload"]
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("Queued event payload is not a JSON object.")
    return value


def _execute_queue_entry(db: Database, entry: dict[str, Any]) -> dict[str, Any]:
    event = _payload(entry)
    source = event_source(event)
    source["object_event_id"] = entry.get("object_event_id")
    source["invocation_mode"] = entry["invocation_mode"]
    mapping = _mapping_from_entry(entry)
    action = entry["event_action"]
    if action == "DELETE":
        return _run_delete(db, event, source, mapping_override=mapping)
    return _run_load(db, event, source, create=action == "CREATE", mapping_override=mapping)


def _log_queue_error(db: Database, entry: dict[str, Any], error: Exception) -> None:
    event = _payload(entry)
    source = event_source(event)
    source["object_event_id"] = entry.get("object_event_id")
    source["invocation_mode"] = entry["invocation_mode"]
    log_error(db, source, entry["event_action"], error, _mapping_from_entry(entry))


def _invocation_id(ctx: Any) -> str:
    for name in ("CallID", "call_id"):
        value = getattr(ctx, name, None)
        try:
            result = value() if callable(value) else value
        except Exception:
            result = None
        if result:
            return str(result)
    return invocation_owner()


def _drain_binding(db: Database, ctx: Any, binding_key: str, transport_mode: str) -> dict[str, Any]:
    owner = invocation_owner()
    lease_seconds = max(30, int(os.environ.get("QUEUE_LEASE_SECONDS", "90")))
    grace_seconds = max(0, int(os.environ.get("QUEUE_REORDER_GRACE_SECONDS", "30")))
    started, maximum = monotonic_deadline(transport_mode)
    if not acquire_lane(db, binding_key, owner, lease_seconds):
        return {"status": "queue_wakeup_coalesced", "binding_key": binding_key, "processed": 0}
    processed = 0
    needs_continuation = False
    stop_reason = "empty"
    try:
        while True:
            entry, reason = claim_next(db, binding_key, owner, lease_seconds, grace_seconds)
            if entry is None:
                stop_reason = reason
                if reason == "not_ready" and time.monotonic() - started + 5 < maximum:
                    time.sleep(min(5, max(1, grace_seconds)))
                    continue
                needs_continuation = reason in {"not_ready", "busy", "race"}
                break
            if reason == "late_event":
                error = ValueError(entry.get("last_error") or "Late queue event requires operator review.")
                _log_queue_error(db, entry, error)
                stop_reason = "blocked"
                break
            remaining = maximum - (time.monotonic() - started)
            if not has_start_budget(entry, remaining, transport_mode):
                if transport_mode == "DETACHED" and processed == 0:
                    attempt_id = start_attempt(db, entry, owner, _invocation_id(ctx), transport_mode)
                    error = ValueError("Object is projected to exceed the safe detached Function runtime and must be split or processed by an external job.")
                    _log_queue_error(db, entry, error)
                    block_entry(db, entry, owner, attempt_id, error)
                    stop_reason = "blocked_runtime_limit"
                    break
                defer_entry(db, entry, owner, "Deferred because the safe Function runtime budget is insufficient.")
                needs_continuation = True
                stop_reason = "runtime_budget"
                break
            attempt_id = start_attempt(db, entry, owner, _invocation_id(ctx), transport_mode)
            try:
                with LeaseHeartbeat(db, binding_key, int(entry["id"]), owner, lease_seconds):
                    _execute_queue_entry(db, entry)
                complete_entry(db, entry, owner, attempt_id)
                processed += 1
            except Exception as error:
                block_entry(db, entry, owner, attempt_id, error)
                stop_reason = "blocked"
                break
    finally:
        pending = release_lane(db, binding_key, owner)
        needs_continuation = needs_continuation or pending
    if needs_continuation and stop_reason != "blocked":
        _invoke_detached(binding_key)
    return {
        "status": "queue_drained" if stop_reason == "empty" else "queue_paused",
        "binding_key": binding_key,
        "processed": processed,
        "reason": stop_reason,
        "continuation_submitted": bool(needs_continuation and stop_reason != "blocked"),
    }


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
        ensure_queue_tables(db)
        if event.get("_queue_worker"):
            binding_key = str(event.get("_queue_binding_key") or "")
            if not binding_key:
                raise ValueError("Queue worker invocation is missing its binding key.")
            result = _drain_binding(db, ctx, binding_key, "DETACHED")
            return response.Response(ctx, response_data=json.dumps(result), headers={"Content-Type": "application/json"}, status_code=200)
        object_event_id = _write_event_audit(db, event)
        source = event_source(event)
        source["object_event_id"] = object_event_id
        action = _event_action(event)
        mapping = resolve_mapping(db, source)
        source["invocation_mode"] = str(mapping.get("invocation_mode") or "SYNC").upper()
        _set_object_event_mode(db, object_event_id, source["invocation_mode"])
        queued = enqueue_event(db, event, source, mapping, action, _event_time(event))
        if queued.get("object_event_id") and int(queued["object_event_id"]) != object_event_id:
            _complete_duplicate_object_event(db, object_event_id)
        _scope, binding_key = queue_binding(mapping)
        if source["invocation_mode"] == "DETACHED":
            _invoke_detached(binding_key)
            result = {"status": "queued", "queue_id": queued["id"], "binding_key": binding_key, "detached_submitted": True}
            status_code = 202
        else:
            result = _drain_binding(db, ctx, binding_key, "SYNC")
            result["queue_id"] = queued["id"]
            status_code = 200 if result["status"] == "queue_drained" else 202
        return response.Response(ctx, response_data=json.dumps(result), headers={"Content-Type": "application/json"}, status_code=status_code)
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
