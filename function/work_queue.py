"""Durable, ordered MySQL work queue for Object Storage events."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime
from typing import Any

from partition_loader import Database, control_database, control_table, quote_identifier


QUEUE_TERMINAL_STATES = {"SUCCESS", "CANCELLED", "DEAD_LETTER"}
QUEUE_MUTABLE_STATES = {"PENDING", "RETRY_WAIT", "BLOCKED"}


def queue_binding(mapping: dict[str, Any]) -> tuple[str, str]:
    scope = str(mapping.get("queue_scope") or "TABLE").upper()
    if scope == "TABLE":
        return scope, f"table:{mapping['target_database']}.{mapping['target_table']}"
    if scope == "MAPPING":
        return scope, f"mapping:{int(mapping['id'])}"
    raise ValueError("Queue scope must be TABLE or MAPPING.")


def ensure_queue_tables(db: Database) -> None:
    with db.connection() as connection:
        cursor = connection.cursor()
        mapping_table = control_table("object_storage_mappings")
        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name='object_storage_mappings' AND column_name='queue_scope'",
            (control_database(),),
        )
        if not cursor.fetchone()[0]:
            cursor.execute(
                f"ALTER TABLE {mapping_table} ADD COLUMN queue_scope "
                "ENUM('TABLE','MAPPING') NOT NULL DEFAULT 'TABLE' AFTER worker_threads"
            )
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {control_table('queue_lane')} (
                binding_key VARCHAR(191) NOT NULL PRIMARY KEY,
                queue_scope ENUM('TABLE','MAPPING') NOT NULL,
                target_database VARCHAR(64) NOT NULL,
                target_table VARCHAR(64) NOT NULL,
                owner_token CHAR(36) NULL,
                lease_expires_at DATETIME(6) NULL,
                heartbeat_at DATETIME(6) NULL,
                generation BIGINT UNSIGNED NOT NULL DEFAULT 0,
                dispatch_requested BOOLEAN NOT NULL DEFAULT FALSE,
                last_completed_event_time DATETIME(6) NULL,
                last_completed_queue_id BIGINT UNSIGNED NULL,
                created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
                KEY ix_queue_lane_lease (lease_expires_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )
        for column, definition in (
            ("last_completed_event_time", "DATETIME(6) NULL"),
            ("last_completed_queue_id", "BIGINT UNSIGNED NULL"),
        ):
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=%s AND table_name='queue_lane' AND column_name=%s",
                (control_database(), column),
            )
            if not cursor.fetchone()[0]:
                cursor.execute(f"ALTER TABLE {control_table('queue_lane')} ADD COLUMN {quote_identifier(column, 'queue lane column')} {definition}")
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {control_table('event_work_queue')} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                event_id VARCHAR(255) NOT NULL,
                object_event_id BIGINT UNSIGNED NULL,
                mapping_id BIGINT UNSIGNED NOT NULL,
                queue_scope ENUM('TABLE','MAPPING') NOT NULL,
                binding_key VARCHAR(191) NOT NULL,
                target_database VARCHAR(64) NOT NULL,
                target_table VARCHAR(64) NOT NULL,
                compartment_name VARCHAR(255) NOT NULL,
                bucket_name VARCHAR(255) NOT NULL,
                resource_name VARCHAR(1024) NOT NULL,
                object_version VARCHAR(255) NOT NULL,
                event_action ENUM('CREATE','UPDATE','DELETE') NOT NULL,
                event_time DATETIME(6) NOT NULL,
                received_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                priority SMALLINT NOT NULL DEFAULT 100,
                status ENUM('PENDING','LEASED','RUNNING','RETRY_WAIT','BLOCKED','SUCCESS','CANCELLED','DEAD_LETTER') NOT NULL DEFAULT 'PENDING',
                attempt_count SMALLINT UNSIGNED NOT NULL DEFAULT 0,
                available_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                lease_token CHAR(36) NULL,
                lease_expires_at DATETIME(6) NULL,
                invocation_mode ENUM('SYNC','DETACHED') NOT NULL,
                worker_threads SMALLINT UNSIGNED NOT NULL,
                object_size_bytes BIGINT UNSIGNED NULL,
                event_payload JSON NOT NULL,
                last_error TEXT NULL,
                operator_note VARCHAR(1024) NULL,
                started_at DATETIME(6) NULL,
                completed_at DATETIME(6) NULL,
                created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
                UNIQUE KEY uq_queue_event (event_id),
                KEY ix_queue_binding_order (binding_key, status, event_time, received_at, id),
                KEY ix_queue_status_available (status, available_at),
                KEY ix_queue_mapping (mapping_id, created_at),
                KEY ix_queue_object_event (object_event_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {control_table('queue_attempt')} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                queue_id BIGINT UNSIGNED NOT NULL,
                attempt_number SMALLINT UNSIGNED NOT NULL,
                invocation_id VARCHAR(255) NULL,
                owner_token CHAR(36) NOT NULL,
                transport_mode ENUM('SYNC','DETACHED') NOT NULL,
                status ENUM('RUNNING','SUCCESS','ERROR','ABANDONED') NOT NULL,
                started_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                completed_at DATETIME(6) NULL,
                duration_ms DECIMAL(16,3) NULL,
                error_class VARCHAR(128) NULL,
                error_message TEXT NULL,
                UNIQUE KEY uq_queue_attempt (queue_id, attempt_number),
                KEY ix_attempt_status_time (status, started_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {control_table('queue_transition_audit')} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                queue_id BIGINT UNSIGNED NULL,
                binding_key VARCHAR(191) NULL,
                actor_type ENUM('FUNCTION','UI','SYSTEM') NOT NULL,
                actor_name VARCHAR(255) NOT NULL,
                action VARCHAR(64) NOT NULL,
                from_status VARCHAR(32) NULL,
                to_status VARCHAR(32) NULL,
                reason VARCHAR(1024) NULL,
                created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                KEY ix_queue_audit_entry (queue_id, created_at),
                KEY ix_queue_audit_binding (binding_key, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )


def _event_size(event: dict[str, Any]) -> int | None:
    data = event.get("data") or {}
    details = data.get("additionalDetails") or {}
    value = details.get("contentLength") or details.get("content-length") or data.get("contentLength")
    try:
        size = int(value)
        return size if size >= 0 else None
    except (TypeError, ValueError):
        return None


def enqueue_event(
    db: Database,
    event: dict[str, Any],
    source: dict[str, Any],
    mapping: dict[str, Any],
    action: str,
    event_time: datetime,
) -> dict[str, Any]:
    """Insert an event idempotently while locking its lane against idle release."""
    scope, binding_key = queue_binding(mapping)
    event_id = str(event.get("eventID") or event.get("id") or f"manual-{uuid.uuid4()}")
    payload = dict(event)
    payload["_object_event_id"] = source.get("object_event_id")
    with db.connection() as connection:
        cursor = connection.cursor(dictionary=True, buffered=True)
        cursor.execute(
            f"""INSERT INTO {control_table('queue_lane')}
               (binding_key, queue_scope, target_database, target_table)
               VALUES (%s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE target_database=VALUES(target_database), target_table=VALUES(target_table)""",
            (binding_key, scope, mapping["target_database"], mapping["target_table"]),
        )
        cursor.execute(
            f"SELECT binding_key FROM {control_table('queue_lane')} WHERE binding_key=%s FOR UPDATE",
            (binding_key,),
        )
        cursor.fetchone()
        cursor.execute(
            f"""INSERT INTO {control_table('event_work_queue')}
               (event_id, object_event_id, mapping_id, queue_scope, binding_key,
                target_database, target_table, compartment_name, bucket_name,
                resource_name, object_version, event_action, event_time, invocation_mode,
                worker_threads, object_size_bytes, event_payload)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)""",
            (
                event_id, source.get("object_event_id"), mapping["id"], scope, binding_key,
                mapping["target_database"], mapping["target_table"], source["compartment_name"],
                source["bucket_name"], source["resource_name"], source.get("object_version", ""),
                action, event_time, str(mapping.get("invocation_mode") or "SYNC").upper(),
                int(mapping.get("worker_threads") or 4), _event_size(event),
                json.dumps(payload, separators=(",", ":")),
            ),
        )
        queue_id = int(cursor.lastrowid)
        cursor.execute(
            f"UPDATE {control_table('queue_lane')} SET dispatch_requested=TRUE, generation=generation+1 WHERE binding_key=%s",
            (binding_key,),
        )
        cursor.execute(f"SELECT * FROM {control_table('event_work_queue')} WHERE id=%s", (queue_id,))
        queued = cursor.fetchone()
        cursor.execute(
            f"""INSERT INTO {control_table('queue_transition_audit')}
               (queue_id,binding_key,actor_type,actor_name,action,to_status,reason)
               VALUES (%s,%s,'FUNCTION','event-intake','ENQUEUE','PENDING','Object Storage event accepted into ordered queue.')""",
            (queue_id, binding_key),
        )
        return queued


def acquire_lane(db: Database, binding_key: str, owner_token: str, lease_seconds: int) -> bool:
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""UPDATE {control_table('queue_lane')}
                   SET owner_token=%s,
                       lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6), INTERVAL %s SECOND),
                       heartbeat_at=UTC_TIMESTAMP(6), dispatch_requested=FALSE
                 WHERE binding_key=%s
                   AND (owner_token IS NULL OR lease_expires_at < UTC_TIMESTAMP(6) OR owner_token=%s)""",
            (owner_token, lease_seconds, binding_key, owner_token),
        )
        return cursor.rowcount == 1


def claim_next(
    db: Database,
    binding_key: str,
    owner_token: str,
    lease_seconds: int,
    reorder_grace_seconds: int,
) -> tuple[dict[str, Any] | None, str]:
    """Claim the first non-terminal entry; never bypass blocked earlier work."""
    with db.connection() as connection:
        cursor = connection.cursor(dictionary=True, buffered=True)
        cursor.execute(
            f"SELECT owner_token,last_completed_event_time,last_completed_queue_id FROM {control_table('queue_lane')} WHERE binding_key=%s FOR UPDATE",
            (binding_key,),
        )
        lane = cursor.fetchone()
        if not lane or lane["owner_token"] != owner_token:
            return None, "lease_lost"
        cursor.execute(
            f"""UPDATE {control_table('event_work_queue')}
                   SET status='RETRY_WAIT', available_at=UTC_TIMESTAMP(6), lease_token=NULL,
                       lease_expires_at=NULL, last_error=COALESCE(last_error, 'Recovered after an expired worker lease.')
                 WHERE binding_key=%s AND status IN ('LEASED','RUNNING')
                   AND lease_expires_at < UTC_TIMESTAMP(6)""",
            (binding_key,),
        )
        cursor.execute(
            f"""SELECT * FROM {control_table('event_work_queue')}
                 WHERE binding_key=%s AND status NOT IN ('SUCCESS','CANCELLED','DEAD_LETTER')
                 ORDER BY event_time ASC, received_at ASC, priority ASC, id ASC LIMIT 1 FOR UPDATE""",
            (binding_key,),
        )
        entry = cursor.fetchone()
        if not entry:
            return None, "empty"
        watermark_time = lane.get("last_completed_event_time")
        watermark_id = int(lane.get("last_completed_queue_id") or 0)
        if watermark_time is not None and (
            entry["event_time"] < watermark_time
            or (entry["event_time"] == watermark_time and int(entry["id"]) < watermark_id)
        ):
            cursor.execute(
                f"UPDATE {control_table('event_work_queue')} SET status='BLOCKED',last_error=%s,completed_at=UTC_TIMESTAMP(6) WHERE id=%s",
                ("Late event is older than the completed lane watermark and requires operator review.", entry["id"]),
            )
            cursor.execute(
                f"""INSERT INTO {control_table('queue_transition_audit')}
                   (queue_id,binding_key,actor_type,actor_name,action,from_status,to_status,reason)
                   VALUES (%s,%s,'SYSTEM','queue-worker','BLOCK_LATE_EVENT',%s,'BLOCKED',%s)""",
                (entry["id"], binding_key, entry["status"], "Entry is older than the lane completion watermark."),
            )
            entry["status"] = "BLOCKED"
            entry["last_error"] = "Late event is older than the completed lane watermark and requires operator review."
            return entry, "late_event"
        if entry["status"] == "BLOCKED":
            return None, "blocked"
        if entry["status"] in {"LEASED", "RUNNING"}:
            return None, "busy"
        cursor.execute(
            "SELECT UTC_TIMESTAMP(6) >= DATE_ADD(%s, INTERVAL %s SECOND) AND UTC_TIMESTAMP(6) >= %s",
            (entry["received_at"], reorder_grace_seconds, entry["available_at"]),
        )
        if not bool(cursor.fetchone()[0]):
            return None, "not_ready"
        attempt = int(entry["attempt_count"] or 0) + 1
        cursor.execute(
            f"""UPDATE {control_table('event_work_queue')}
                   SET status='RUNNING', attempt_count=%s, lease_token=%s,
                       lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6), INTERVAL %s SECOND),
                       started_at=UTC_TIMESTAMP(6), completed_at=NULL, last_error=NULL
                 WHERE id=%s AND status IN ('PENDING','RETRY_WAIT')""",
            (attempt, owner_token, lease_seconds, entry["id"]),
        )
        if cursor.rowcount != 1:
            return None, "race"
        entry["attempt_count"] = attempt
        entry["status"] = "RUNNING"
        entry["lease_token"] = owner_token
        return entry, "claimed"


def start_attempt(
    db: Database,
    entry: dict[str, Any],
    owner_token: str,
    invocation_id: str,
    transport_mode: str,
) -> int:
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""INSERT INTO {control_table('queue_attempt')}
               (queue_id, attempt_number, invocation_id, owner_token, transport_mode, status)
               VALUES (%s,%s,%s,%s,%s,'RUNNING')""",
            (entry["id"], entry["attempt_count"], invocation_id[:255], owner_token, transport_mode),
        )
        attempt_id = int(cursor.lastrowid)
        cursor.execute(
            f"""INSERT INTO {control_table('queue_transition_audit')}
               (queue_id,binding_key,actor_type,actor_name,action,from_status,to_status,reason)
               VALUES (%s,%s,'FUNCTION',%s,'START','LEASED','RUNNING',%s)""",
            (entry["id"], entry["binding_key"], invocation_id[:255], f"Attempt {entry['attempt_count']} via {transport_mode}."),
        )
        return attempt_id


def heartbeat(db: Database, binding_key: str, queue_id: int, owner_token: str, lease_seconds: int) -> bool:
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""UPDATE {control_table('queue_lane')}
                   SET heartbeat_at=UTC_TIMESTAMP(6), lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6), INTERVAL %s SECOND)
                 WHERE binding_key=%s AND owner_token=%s""",
            (lease_seconds, binding_key, owner_token),
        )
        owned = cursor.rowcount == 1
        if owned:
            cursor.execute(
                f"""UPDATE {control_table('event_work_queue')}
                       SET lease_expires_at=DATE_ADD(UTC_TIMESTAMP(6), INTERVAL %s SECOND)
                     WHERE id=%s AND lease_token=%s AND status='RUNNING'""",
                (lease_seconds, queue_id, owner_token),
            )
        return owned


class LeaseHeartbeat:
    def __init__(self, db: Database, binding_key: str, queue_id: int, owner_token: str, lease_seconds: int) -> None:
        self.db, self.binding_key, self.queue_id = db, binding_key, queue_id
        self.owner_token, self.lease_seconds = owner_token, lease_seconds
        self.interval = max(5, min(20, lease_seconds // 3))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="queue-lease-heartbeat", daemon=True)

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                if not heartbeat(self.db, self.binding_key, self.queue_id, self.owner_token, self.lease_seconds):
                    return
            except Exception:
                # A later queue transition verifies the owner token. Transient
                # heartbeat errors are retried without leaking credentials.
                continue

    def __enter__(self) -> "LeaseHeartbeat":
        self._thread.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self._stop.set()
        self._thread.join(timeout=2)


def _finish_attempt(db: Database, attempt_id: int, status: str, error: Exception | None = None) -> None:
    with db.connection() as connection:
        connection.cursor().execute(
            f"""UPDATE {control_table('queue_attempt')}
                   SET status=%s, completed_at=UTC_TIMESTAMP(6),
                       duration_ms=TIMESTAMPDIFF(MICROSECOND, started_at, UTC_TIMESTAMP(6))/1000,
                       error_class=%s, error_message=%s
                 WHERE id=%s""",
            (status, type(error).__name__[:128] if error else None, str(error) if error else None, attempt_id),
        )


def complete_entry(db: Database, entry: dict[str, Any], owner_token: str, attempt_id: int) -> None:
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""UPDATE {control_table('event_work_queue')}
                   SET status='SUCCESS', completed_at=UTC_TIMESTAMP(6), lease_token=NULL,
                       lease_expires_at=NULL, last_error=NULL
                 WHERE id=%s AND lease_token=%s AND status='RUNNING'""",
            (entry["id"], owner_token),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Queue lease was lost before completion could be recorded.")
        cursor.execute(
            f"""UPDATE {control_table('queue_lane')}
                   SET last_completed_event_time=%s,last_completed_queue_id=%s
                 WHERE binding_key=%s AND owner_token=%s""",
            (entry["event_time"], entry["id"], entry["binding_key"], owner_token),
        )
        cursor.execute(
            f"""INSERT INTO {control_table('queue_transition_audit')}
               (queue_id,binding_key,actor_type,actor_name,action,from_status,to_status,reason)
               VALUES (%s,%s,'FUNCTION','queue-worker','COMPLETE','RUNNING','SUCCESS','Target mutation committed.')""",
            (entry["id"], entry["binding_key"]),
        )
    _finish_attempt(db, attempt_id, "SUCCESS")


def block_entry(db: Database, entry: dict[str, Any], owner_token: str, attempt_id: int, error: Exception) -> None:
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""UPDATE {control_table('event_work_queue')}
                   SET status='BLOCKED', completed_at=UTC_TIMESTAMP(6), lease_token=NULL,
                       lease_expires_at=NULL, last_error=%s
                 WHERE id=%s AND lease_token=%s AND status='RUNNING'""",
            (str(error), entry["id"], owner_token),
        )
        cursor.execute(
            f"""INSERT INTO {control_table('queue_transition_audit')}
               (queue_id,binding_key,actor_type,actor_name,action,from_status,to_status,reason)
               VALUES (%s,%s,'FUNCTION','queue-worker','BLOCK','RUNNING','BLOCKED',%s)""",
            (entry["id"], entry["binding_key"], str(error)[:1024]),
        )
    _finish_attempt(db, attempt_id, "ERROR", error)


def defer_entry(db: Database, entry: dict[str, Any], owner_token: str, reason: str) -> None:
    """Return an unstarted claim to the head of its lane for continuation."""
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""UPDATE {control_table('event_work_queue')}
                   SET status='RETRY_WAIT', available_at=UTC_TIMESTAMP(6), lease_token=NULL,
                       lease_expires_at=NULL, started_at=NULL, last_error=%s
                 WHERE id=%s AND lease_token=%s AND status='RUNNING'""",
            (reason, entry["id"], owner_token),
        )
        if cursor.rowcount != 1:
            raise RuntimeError("Queue lease was lost while deferring work.")
        cursor.execute(
            f"""INSERT INTO {control_table('queue_transition_audit')}
               (queue_id,binding_key,actor_type,actor_name,action,from_status,to_status,reason)
               VALUES (%s,%s,'FUNCTION','queue-worker','DEFER','RUNNING','RETRY_WAIT',%s)""",
            (entry["id"], entry["binding_key"], reason[:1024]),
        )


def release_lane(db: Database, binding_key: str, owner_token: str) -> bool:
    """Release only when no eligible work was inserted while the lane was locked."""
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"SELECT owner_token FROM {control_table('queue_lane')} WHERE binding_key=%s FOR UPDATE",
            (binding_key,),
        )
        row = cursor.fetchone()
        if not row or row[0] != owner_token:
            return False
        cursor.execute(
            f"""SELECT COUNT(*) FROM {control_table('event_work_queue')}
                 WHERE binding_key=%s AND status IN ('PENDING','RETRY_WAIT','LEASED','RUNNING')""",
            (binding_key,),
        )
        has_work = bool(cursor.fetchone()[0])
        cursor.execute(
            f"""UPDATE {control_table('queue_lane')}
                   SET owner_token=NULL, lease_expires_at=NULL, heartbeat_at=NULL,
                       dispatch_requested=%s WHERE binding_key=%s AND owner_token=%s""",
            (has_work, binding_key, owner_token),
        )
        return has_work


def pending_bindings(db: Database, *, limit: int = 20) -> list[str]:
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""SELECT DISTINCT binding_key FROM {control_table('event_work_queue')}
                 WHERE status IN ('PENDING','RETRY_WAIT') AND available_at <= UTC_TIMESTAMP(6)
                 ORDER BY binding_key LIMIT %s""",
            (limit,),
        )
        return [row[0] for row in cursor.fetchall()]


def predicted_seconds(entry: dict[str, Any]) -> float:
    size = int(entry.get("object_size_bytes") or 0)
    if size <= 0 or entry.get("event_action") == "DELETE":
        return float(os.environ.get("QUEUE_UNKNOWN_JOB_SECONDS", "60"))
    bytes_per_second = max(1.0, float(os.environ.get("QUEUE_EXPECTED_BYTES_PER_SECOND", str(4 * 1024 * 1024))))
    return size / bytes_per_second


def has_start_budget(entry: dict[str, Any], remaining_seconds: float, transport_mode: str = "DETACHED") -> bool:
    if transport_mode == "SYNC":
        reserve = int(os.environ.get("QUEUE_SYNC_RESERVE_SECONDS", "15"))
        minimum = int(os.environ.get("QUEUE_SYNC_MINIMUM_START_SECONDS", "15"))
    else:
        reserve = int(os.environ.get("QUEUE_SHUTDOWN_RESERVE_SECONDS", "120"))
        minimum = int(os.environ.get("QUEUE_MINIMUM_START_SECONDS", "180"))
    factor = float(os.environ.get("QUEUE_PREDICTION_SAFETY_FACTOR", "1.35"))
    required = max(minimum, predicted_seconds(entry) * factor) + reserve
    return remaining_seconds > required


def invocation_owner() -> str:
    return str(uuid.uuid4())


def monotonic_deadline(transport_mode: str) -> tuple[float, float]:
    maximum = int(os.environ.get("DETACHED_TIMEOUT_SECONDS" if transport_mode == "DETACHED" else "SYNC_TIMEOUT_SECONDS", "3600" if transport_mode == "DETACHED" else "300"))
    return time.monotonic(), float(maximum)
