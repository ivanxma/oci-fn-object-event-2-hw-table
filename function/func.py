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
                KEY ix_object_event_time (event_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )
        cursor.execute(
            f"""INSERT INTO {control_table('object_event')}
               (event_date, event_type, event_message, bucket_name, compartment_name, resource_name, namespace, event_time)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                _event_time(event), str(event.get("eventType") or "unknown"), json.dumps(event, separators=(",", ":")),
                details.get("bucketName"), data.get("compartmentName"), data.get("resourceName"),
                details.get("namespace") or data.get("namespace"), _event_time(event),
            ),
        )
        return int(cursor.lastrowid)


def _object_response(event: dict[str, Any], source: dict[str, str]):
    details = (event.get("data") or {}).get("additionalDetails") or {}
    namespace = str(details.get("namespace") or os.environ.get("OBJECT_STORAGE_NAMESPACE") or "")
    if not namespace:
        raise ValueError("Object Storage event must include a namespace or set OBJECT_STORAGE_NAMESPACE.")
    signer = oci.auth.signers.get_resource_principals_signer()
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    return client.get_object(namespace, source["bucket_name"], _object_name(event, source))


def _run_load(db: Database, event: dict[str, Any], source: dict[str, str], *, create: bool) -> dict[str, Any]:
    action, mapping, record = "CREATE" if create else "UPDATE", None, None
    try:
        mapping = resolve_mapping(db, source)
        columns = target_definition(db, mapping)
        record = allocate_or_get_batch(db, mapping, source, create=create)
        ensure_partition(db, mapping, record["batch_num"])
        stage = create_stage_table(db, mapping, record["batch_num"])
        object_response = _object_response(event, source)
        # Keep the SDK's streaming-body wrapper alive for the entire CSV read.
        # ``.raw`` is the wrapper's underlying HTTP response and can be closed
        # while a long-running loader is still consuming it.  The response body
        # itself is file-like, so it can be decoded directly without materialising
        # the object in the Function filesystem.
        with io.TextIOWrapper(object_response.data, encoding="utf-8", newline="") as csv_stream:
            rows = load_csv_parallel(
                db, mapping, stage, record["batch_num"], columns, csv_stream,
                int(os.environ.get("BATCH_ROWS", "1000")), int(os.environ.get("WRITER_WORKERS", "4")),
            )
        validate_and_exchange(db, mapping, stage, record["batch_num"])
        mark_active(db, record["id"])
        log_event(db, source, action, "SUCCESS", mapping, record["batch_num"], f"Loaded {rows} row(s) by partition exchange.")
        return {"action": action.lower(), "batch_num": record["batch_num"], "rows": rows, "target": f"{mapping['target_database']}.{mapping['target_table']}"}
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
            log_error(db, source, action, error)
        message = "Target table is not ready for partition exchange." if isinstance(error, TargetTableError) else str(error)
        return response.Response(ctx, response_data=json.dumps({"status": "error", "message": message}), headers={"Content-Type": "application/json"}, status_code=500)
