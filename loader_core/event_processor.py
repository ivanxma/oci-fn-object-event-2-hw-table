"""Shared Object Storage event loader used by the OCI Container processor."""

from __future__ import annotations

import io
import os
import tempfile
from pathlib import Path
from typing import Any

import oci

from partition_loader import (
    Database,
    allocate_or_get_batch,
    create_stage_table,
    drop_stage_table,
    ensure_control_tables,
    ensure_partition,
    event_source,
    load_csv_parallel,
    mark_error,
    mark_active,
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


def _object_name(event: dict[str, Any], source: dict[str, str]) -> str:
    details = (event.get("data") or {}).get("additionalDetails") or {}
    return str(details.get("objectName") or source["resource_name"])


def _oci_signer() -> Any:
    """Use the deployment principal; VM validation opts into instance principal."""
    mode = os.environ.get("OCI_AUTH_MODE", "resource_principal").strip().lower()
    if mode == "resource_principal":
        return oci.auth.signers.get_resource_principals_signer()
    if mode == "instance_principal":
        return oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    raise ValueError("OCI_AUTH_MODE must be resource_principal or instance_principal.")


class ObjectStorageRangeStream(io.RawIOBase):
    """A diskless, seek-free Object Storage reader using bounded HTTP ranges.

    OCI Container processors can close one long-lived object response before a slow MySQL
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
    signer = _oci_signer()
    # A large CSV can take longer to consume than the SDK's default read
    # timeout because ingestion pauses briefly while writer workers commit each
    # batch.  Keep the HTTP response open for the whole processor operation.
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
        # or elsewhere on the processor filesystem.
        with io.TextIOWrapper(io.BufferedReader(object_stream), encoding="utf-8", newline="") as csv_stream:
            rows = load_csv_parallel(
                db, mapping, stage, record["batch_num"], columns, csv_stream,
                int(os.environ.get("BATCH_ROWS", "10000")), int(os.environ.get("WRITER_WORKERS") or mapping.get("worker_threads") or "4"),
            )
        validate_and_exchange(db, mapping, stage, record["batch_num"])
        mark_active(db, record["id"])
        return {"action": action.lower(), "batch_num": record["batch_num"], "rows": rows, "target": f"{mapping['target_database']}.{mapping['target_table']}", "processing_mode": mapping.get("processing_mode", "FIFO"), "worker_threads": mapping.get("worker_threads", 4)}
    except Exception as error:
        if record is not None:
            try:
                mark_error(db, record["id"])
            except Exception:
                pass
        raise
    finally:
        if mapping is not None and stage is not None:
            try:
                drop_stage_table(db, mapping, stage)
            except Exception:
                # A failed cleanup is visible through the UI's staging-table
                # section and must not mask the load error.
                pass


def _run_delete(db: Database, event: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    # Retain the established prototype's deletion semantics, including an idempotent no-op.
    from partition_loader import run_delete

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", encoding="utf-8") as event_file:
        import json
        json.dump(event, event_file)
        event_file.flush()
        return run_delete(Path(event_file.name))


def process_cloud_event(event: dict[str, Any]) -> dict[str, Any]:
    """Execute a CloudEvent without constructing an FDK HTTP response.

    The Streaming processor calls this directly after durable capture. Durable
    capture and ``stream_event_tx_log`` own transaction status and errors.
    """
    db: Database | None = None
    source: dict[str, str] | None = None
    try:
        if not isinstance(event, dict):
            raise ValueError("Expected an Object Storage CloudEvent JSON object.")
        db = Database()
        ensure_control_tables(db)
        source = event_source(event)
        action = _event_action(event)
        result = _run_delete(db, event, source) if action == "DELETE" else _run_load(db, event, source, create=action == "CREATE")
        return {"status": "success", **result}
    except Exception:
        raise
