"""Operational access to the durable Object Storage event queue."""

from __future__ import annotations

import fnmatch
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from .mapping_service import MappingService, control_database
from .naming import quote_identifier


def _table(name: str) -> str:
    return f"{quote_identifier(control_database(), 'control database')}.{quote_identifier(name, 'queue table')}"


def _queue_id(value: Any) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError("Queue identifier must be a positive whole number.") from error
    if result < 1:
        raise ValueError("Queue identifier must be a positive whole number.")
    return result


def _priority(value: Any) -> int:
    try:
        result = int(value if value not in (None, "") else 100)
    except (TypeError, ValueError) as error:
        raise ValueError("Priority must be a whole number from -1000 to 1000.") from error
    if not -1000 <= result <= 1000:
        raise ValueError("Priority must be from -1000 to 1000.")
    return result


def binding_for(mapping: dict[str, Any]) -> tuple[str, str]:
    scope = str(mapping.get("queue_scope") or "TABLE").upper()
    if scope == "TABLE":
        return scope, f"table:{mapping['target_database']}.{mapping['target_table']}"
    if scope == "MAPPING":
        return scope, f"mapping:{int(mapping['id'])}"
    raise ValueError("Queue scope must be TABLE or MAPPING.")


class QueueService:
    def __init__(self, mysql) -> None:
        self.mysql = mysql

    def _ensure_schema(self, cursor) -> None:
        # MappingService owns mapping upgrades, including queue_scope.
        MappingService(self.mysql)._ensure_schema(cursor)
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {_table('queue_lane')} (
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
                "SELECT COUNT(*) AS count FROM information_schema.columns WHERE table_schema=%s AND table_name='queue_lane' AND column_name=%s",
                (control_database(), column),
            )
            row = cursor.fetchone()
            count = row.get("count", 0) if isinstance(row, dict) else (row[0] if row else 0)
            if not count:
                cursor.execute(f"ALTER TABLE {_table('queue_lane')} ADD COLUMN {quote_identifier(column, 'queue lane column')} {definition}")
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {_table('event_work_queue')} (
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
            f"""CREATE TABLE IF NOT EXISTS {_table('queue_attempt')} (
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
            f"""CREATE TABLE IF NOT EXISTS {_table('queue_transition_audit')} (
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

    def dashboard(self, filters: dict[str, str], limit: int = 100) -> tuple[dict[str, int], list[dict[str, Any]], list[dict[str, Any]]]:
        conditions, parameters = [], []
        allowed_statuses = {"PENDING", "LEASED", "RUNNING", "RETRY_WAIT", "BLOCKED", "SUCCESS", "CANCELLED", "DEAD_LETTER"}
        if filters.get("status"):
            status = filters["status"].upper()
            if status not in allowed_statuses:
                raise ValueError("Unknown queue status filter.")
            conditions.append("q.status=%s")
            parameters.append(status)
        if filters.get("queue_scope"):
            scope = filters["queue_scope"].upper()
            if scope not in {"TABLE", "MAPPING"}:
                raise ValueError("Unknown queue scope filter.")
            conditions.append("q.queue_scope=%s")
            parameters.append(scope)
        if filters.get("binding_key"):
            conditions.append("q.binding_key=%s")
            parameters.append(filters["binding_key"][:191])
        if filters.get("resource_name"):
            conditions.append("q.resource_name LIKE %s")
            parameters.append(f"%{filters['resource_name'][:512]}%")
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_schema(cursor)
            cursor.execute(f"SELECT status, COUNT(*) AS count FROM {_table('event_work_queue')} GROUP BY status")
            counts = {row["status"]: int(row["count"]) for row in cursor.fetchall()}
            cursor.execute(
                "SELECT COUNT(*) AS count FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name='event_errors'",
                (control_database(),),
            )
            has_event_errors = bool(cursor.fetchone()["count"])
            event_error_id = (
                f"""(SELECT e.id FROM {_table('event_errors')} e
                        WHERE e.mapping_id=q.mapping_id
                          AND e.target_database=q.target_database
                          AND e.target_table=q.target_table
                          AND e.event_action=q.event_action
                          AND e.error_message=q.last_error
                          AND e.created_at BETWEEN q.created_at
                              AND DATE_ADD(COALESCE(q.completed_at,q.updated_at), INTERVAL 5 SECOND)
                        ORDER BY e.id DESC LIMIT 1)"""
                if has_event_errors
                else "NULL"
            )
            cursor.execute(
                f"""SELECT q.id, q.event_id, q.mapping_id, q.queue_scope, q.binding_key,
                            q.target_database, q.target_table, q.bucket_name, q.resource_name,
                            q.object_version, q.event_action, q.event_time, q.priority, q.status,
                            q.attempt_count, q.available_at, q.invocation_mode, q.worker_threads,
                            q.object_size_bytes, q.last_error, q.operator_note, q.started_at,
                            q.completed_at, q.created_at,a.transport_mode AS latest_transport_mode,
                            a.status AS latest_attempt_status,a.duration_ms AS latest_attempt_duration_ms,
                            {event_error_id} AS event_error_id
                     FROM {_table('event_work_queue')} q
                     LEFT JOIN {_table('queue_attempt')} a ON a.id=(SELECT MAX(a2.id) FROM {_table('queue_attempt')} a2 WHERE a2.queue_id=q.id)
                     {where}
                     ORDER BY q.created_at DESC, q.id DESC LIMIT %s""",
                (*parameters, limit),
            )
            entries = cursor.fetchall()
            cursor.execute(
                f"""SELECT l.binding_key, l.queue_scope, l.target_database, l.target_table,
                            l.owner_token, l.lease_expires_at, l.heartbeat_at, l.generation,
                            l.dispatch_requested,l.last_completed_event_time,l.last_completed_queue_id,
                            SUM(q.status IN ('PENDING','RETRY_WAIT')) AS pending_count,
                            SUM(q.status='RUNNING') AS running_count,
                            SUM(q.status='BLOCKED') AS blocked_count
                     FROM {_table('queue_lane')} l
                     LEFT JOIN {_table('event_work_queue')} q ON q.binding_key=l.binding_key
                     GROUP BY l.binding_key, l.queue_scope, l.target_database, l.target_table,
                              l.owner_token, l.lease_expires_at, l.heartbeat_at, l.generation,
                              l.dispatch_requested,l.last_completed_event_time,l.last_completed_queue_id
                     ORDER BY pending_count DESC, l.binding_key"""
            )
            lanes = cursor.fetchall()
        return counts, entries, lanes

    def get_entry(self, queue_id: int) -> dict[str, Any] | None:
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_schema(cursor)
            cursor.execute(f"SELECT * FROM {_table('event_work_queue')} WHERE id=%s", (_queue_id(queue_id),))
            return cursor.fetchone()

    def create_manual(self, form: dict[str, Any], username: str) -> tuple[int, str]:
        mapping_id = _queue_id(form.get("mapping_id"))
        action = str(form.get("event_action") or "").upper()
        if action not in {"CREATE", "UPDATE", "DELETE"}:
            raise ValueError("Event action must be CREATE, UPDATE, or DELETE.")
        resource = str(form.get("resource_name") or "").strip()
        if not resource or len(resource) > 1024:
            raise ValueError("Resource name is required and must be 1024 characters or fewer.")
        version = str(form.get("object_version") or f"manual-{uuid.uuid4()}").strip()[:255]
        note = str(form.get("operator_note") or "Manual queue entry.").strip()[:1024]
        priority = _priority(form.get("priority"))
        event_time_text = str(form.get("event_time") or "").strip()
        try:
            event_time = datetime.fromisoformat(event_time_text).replace(tzinfo=None) if event_time_text else datetime.now(timezone.utc).replace(tzinfo=None)
        except ValueError as error:
            raise ValueError("Event time must be a valid date and time.") from error
        mapping = MappingService(self.mysql).get_mapping(mapping_id)
        if not mapping:
            raise ValueError("The selected mapping does not exist.")
        if not fnmatch.fnmatchcase(resource, mapping["resource_name_pattern"]):
            raise ValueError("Resource name does not match the selected mapping pattern.")
        scope, binding_key = binding_for(mapping)
        event_id = f"manual-{uuid.uuid4()}"
        event_type = f"com.oraclecloud.objectstorage.{action.lower()}object"
        event = {
            "eventID": event_id,
            "eventTime": event_time.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
            "eventType": event_type,
            "data": {
                "compartmentName": mapping["compartment_name"],
                "resourceName": resource,
                "additionalDetails": {"bucketName": mapping["bucket_name"], "versionId": version},
            },
        }
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_schema(cursor)
            cursor.execute(
                f"""INSERT INTO {_table('queue_lane')} (binding_key,queue_scope,target_database,target_table)
                     VALUES (%s,%s,%s,%s)
                     ON DUPLICATE KEY UPDATE target_database=VALUES(target_database),target_table=VALUES(target_table)""",
                (binding_key, scope, mapping["target_database"], mapping["target_table"]),
            )
            cursor.execute(f"SELECT binding_key FROM {_table('queue_lane')} WHERE binding_key=%s FOR UPDATE", (binding_key,))
            cursor.fetchone()
            cursor.execute(
                f"""INSERT INTO {_table('event_work_queue')}
                   (event_id,mapping_id,queue_scope,binding_key,target_database,target_table,
                    compartment_name,bucket_name,resource_name,object_version,event_action,event_time,
                    priority,invocation_mode,worker_threads,event_payload,operator_note)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (event_id, mapping_id, scope, binding_key, mapping["target_database"], mapping["target_table"],
                 mapping["compartment_name"], mapping["bucket_name"], resource, version, action, event_time,
                 priority, mapping["invocation_mode"], mapping["worker_threads"], json.dumps(event), note),
            )
            queue_id = int(cursor.lastrowid)
            cursor.execute(
                f"INSERT INTO {_table('queue_transition_audit')} (queue_id,binding_key,actor_type,actor_name,action,to_status,reason) VALUES (%s,%s,'UI',%s,'CREATE','PENDING',%s)",
                (queue_id, binding_key, username[:255] or "unknown", note),
            )
            cursor.execute(f"UPDATE {_table('queue_lane')} SET dispatch_requested=TRUE,generation=generation+1 WHERE binding_key=%s", (binding_key,))
        return queue_id, binding_key

    def edit_entry(self, queue_id: int, form: dict[str, Any], username: str) -> None:
        queue_id = _queue_id(queue_id)
        priority = _priority(form.get("priority"))
        note = str(form.get("operator_note") or "").strip()[:1024]
        available_text = str(form.get("available_at") or "").strip()
        try:
            available_at = datetime.fromisoformat(available_text).replace(tzinfo=None) if available_text else datetime.now(timezone.utc).replace(tzinfo=None)
        except ValueError as error:
            raise ValueError("Available time must be a valid date and time.") from error
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_schema(cursor)
            cursor.execute(f"SELECT status,binding_key FROM {_table('event_work_queue')} WHERE id=%s FOR UPDATE", (queue_id,))
            entry = cursor.fetchone()
            if not entry:
                raise ValueError("Queue entry does not exist.")
            if entry["status"] not in {"PENDING", "RETRY_WAIT", "BLOCKED"}:
                raise ValueError("Only pending, retry-wait, or blocked entries can be edited.")
            cursor.execute(f"UPDATE {_table('event_work_queue')} SET priority=%s,available_at=%s,operator_note=%s WHERE id=%s", (priority, available_at, note, queue_id))
            cursor.execute(
                f"INSERT INTO {_table('queue_transition_audit')} (queue_id,binding_key,actor_type,actor_name,action,from_status,to_status,reason) VALUES (%s,%s,'UI',%s,'EDIT',%s,%s,%s)",
                (queue_id, entry["binding_key"], username[:255] or "unknown", entry["status"], entry["status"], note),
            )

    def transition(self, queue_ids: list[int], action: str, username: str, reason: str) -> tuple[int, list[str]]:
        action = action.upper()
        if action == "CANCEL":
            from_states, to_status = {"PENDING", "RETRY_WAIT", "BLOCKED"}, "CANCELLED"
        elif action == "RETRY":
            from_states, to_status = {"BLOCKED", "DEAD_LETTER"}, "RETRY_WAIT"
        else:
            raise ValueError("Unsupported queue transition.")
        changed, bindings = 0, []
        with self.mysql.connection() as conn:
            cursor = conn.cursor(dictionary=True, buffered=True)
            self._ensure_schema(cursor)
            for queue_id in dict.fromkeys(_queue_id(value) for value in queue_ids):
                cursor.execute(f"SELECT status,binding_key,object_event_id FROM {_table('event_work_queue')} WHERE id=%s FOR UPDATE", (queue_id,))
                entry = cursor.fetchone()
                if not entry or entry["status"] not in from_states:
                    continue
                cursor.execute(
                    f"UPDATE {_table('event_work_queue')} SET status=%s,available_at=UTC_TIMESTAMP(6),lease_token=NULL,lease_expires_at=NULL,last_error=NULL WHERE id=%s",
                    (to_status, queue_id),
                )
                if action == "CANCEL" and entry.get("object_event_id"):
                    cursor.execute(
                        f"""UPDATE {quote_identifier(control_database(), 'control database')}.`object_event`
                               SET completed_at=UTC_TIMESTAMP(6),
                                   duration_ms=TIMESTAMPDIFF(MICROSECOND,received_at,UTC_TIMESTAMP(6))/1000
                             WHERE id=%s""",
                        (entry["object_event_id"],),
                    )
                cursor.execute(
                    f"INSERT INTO {_table('queue_transition_audit')} (queue_id,binding_key,actor_type,actor_name,action,from_status,to_status,reason) VALUES (%s,%s,'UI',%s,%s,%s,%s,%s)",
                    (queue_id, entry["binding_key"], username[:255] or "unknown", action, entry["status"], to_status, reason[:1024]),
                )
                if action == "RETRY":
                    cursor.execute(f"UPDATE {_table('queue_lane')} SET dispatch_requested=TRUE,generation=generation+1 WHERE binding_key=%s", (entry["binding_key"],))
                    bindings.append(entry["binding_key"])
                changed += 1
        return changed, list(dict.fromkeys(bindings))
