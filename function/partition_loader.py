"""Generic, partition-exchange Object Storage event prototype helpers."""

from __future__ import annotations

import argparse
import csv
import fnmatch
import hashlib
import json
import os
import re
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, TextIO

import mysql.connector


IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
LOAD_LEASE_SECONDS = int(os.environ.get("LOAD_LEASE_SECONDS", "120"))


def control_database() -> str:
    value = os.environ.get("CONTROL_DATABASE", "fndb")
    if not IDENTIFIER.fullmatch(value):
        raise ValueError("CONTROL_DATABASE must be a valid MySQL identifier.")
    return value


def control_table(table: str) -> str:
    return f"{quote_identifier(control_database(), 'control database')}.{quote_identifier(table, 'control table')}"


class TargetTableError(ValueError):
    """The mapped target table cannot safely receive a partition exchange."""


def quote_identifier(value: str, label: str) -> str:
    if not IDENTIFIER.fullmatch(value or ""):
        raise ValueError(f"Invalid {label}: use a MySQL identifier beginning with a letter.")
    return f"`{value}`"


class Database:
    def __init__(self) -> None:
        self.args = {
            "host": os.environ.get("DB_HOST", "127.0.0.1"),
            "port": int(os.environ.get("DB_PORT", "3306")),
            "user": os.environ.get("DB_USER", ""),
            "password": os.environ.get("DB_PASSWORD", ""),
            "ssl_disabled": os.environ.get("DB_SSL_DISABLED", "false").lower() == "true",
            "connection_timeout": 15,
            "autocommit": False,
        }
        if not self.args["user"] or not self.args["password"]:
            raise ValueError("Set DB_USER and DB_PASSWORD in the Function configuration.")

    @contextmanager
    def connection(self):
        connection = mysql.connector.connect(**self.args)
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def event_source(event: dict[str, Any]) -> dict[str, str]:
    data = event.get("data") or {}
    details = data.get("additionalDetails") or {}
    compartment = str(data.get("compartmentName") or "")
    bucket = str(details.get("bucketName") or "")
    resource = str(data.get("resourceName") or details.get("objectName") or "")
    if not compartment or not bucket or not resource:
        raise ValueError("Event must contain data.compartmentName, bucketName, and resourceName/objectName.")
    return {
        "compartment_name": compartment,
        "bucket_name": bucket,
        "resource_name": resource,
        "object_version": str(details.get("versionId") or details.get("eTag") or event.get("eventID") or ""),
    }


def table_name(schema: str, table: str) -> str:
    return f"{quote_identifier(schema, 'target database')}.{quote_identifier(table, 'target table')}"


def stage_name(target_table: str) -> str:
    """Create a collision-resistant, MySQL-valid staging-table name."""
    return f"{target_table[:45]}_stage_{uuid.uuid4().hex[:12]}"


def partition_name(batch_num: int) -> str:
    return f"p_batch_{batch_num}"


def source_key(mapping_id: int, source: dict[str, str]) -> bytes:
    identity = f"{mapping_id}\x1f{source['bucket_name']}\x1f{source['resource_name']}"
    return hashlib.sha256(identity.encode("utf-8")).digest()


def ensure_control_tables(db: Database) -> None:
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(control_database(), 'control database')} CHARACTER SET utf8mb4")
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {control_table('target_batch_sequences')} (
                target_database VARCHAR(64) NOT NULL,
                target_table VARCHAR(64) NOT NULL,
                next_batch_num BIGINT UNSIGNED NOT NULL,
                PRIMARY KEY (target_database, target_table)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {control_table('source_object_batches')} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                mapping_id BIGINT UNSIGNED NOT NULL,
                bucket_name VARCHAR(255) NOT NULL,
                resource_name VARCHAR(1024) NOT NULL,
                target_database VARCHAR(64) NOT NULL,
                target_table VARCHAR(64) NOT NULL,
                batch_num BIGINT UNSIGNED NOT NULL,
                source_key BINARY(32) NOT NULL,
                object_version VARCHAR(255) NOT NULL,
                lifecycle_state VARCHAR(20) NOT NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                UNIQUE KEY uq_source_object (mapping_id, source_key)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {control_table('event_tx_log')} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                object_event_id BIGINT UNSIGNED NULL,
                mapping_id BIGINT UNSIGNED NULL,
                target_database VARCHAR(64) NULL,
                target_table VARCHAR(64) NULL,
                batch_num BIGINT UNSIGNED NULL,
                event_action VARCHAR(20) NOT NULL,
                event_status VARCHAR(20) NOT NULL,
                bucket_name VARCHAR(255) NULL,
                resource_name VARCHAR(1024) NULL,
                object_version VARCHAR(255) NULL,
                message TEXT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                KEY ix_event_object_event (object_event_id),
                KEY ix_event_target_time (target_database, target_table, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )
        cursor.execute(
            """SELECT 1 FROM information_schema.columns
                 WHERE table_schema = %s AND table_name = 'event_tx_log'
                   AND column_name = 'object_event_id'""",
            (control_database(),),
        )
        if cursor.fetchone() is None:
            cursor.execute(f"ALTER TABLE {control_table('event_tx_log')} ADD COLUMN object_event_id BIGINT UNSIGNED NULL AFTER id")
            cursor.execute(f"ALTER TABLE {control_table('event_tx_log')} ADD KEY ix_event_object_event (object_event_id)")
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {control_table('event_errors')} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                event_log_id BIGINT UNSIGNED NULL,
                mapping_id BIGINT UNSIGNED NULL,
                target_database VARCHAR(64) NULL,
                target_table VARCHAR(64) NULL,
                event_action VARCHAR(20) NOT NULL,
                error_code VARCHAR(64) NOT NULL,
                error_message TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                KEY ix_error_target_time (target_database, target_table, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )


def resolve_mapping(db: Database, source: dict[str, str]) -> dict[str, Any]:
    with db.connection() as connection:
        cursor = connection.cursor(dictionary=True, buffered=True)
        cursor.execute(
            f"""SELECT id, compartment_name, bucket_name, resource_name_pattern, target_database, target_table
               FROM {control_table('object_storage_mappings')}
               WHERE compartment_name = %s AND bucket_name = %s""",
            (source["compartment_name"], source["bucket_name"]),
        )
        matches = [row for row in cursor.fetchall() if fnmatch.fnmatchcase(source["resource_name"], row["resource_name_pattern"])]
    if not matches:
        raise ValueError("No Resource Mappings entry matches this compartment, bucket, and resource name.")
    return max(matches, key=lambda item: len(item["resource_name_pattern"]))


def target_definition(db: Database, mapping: dict[str, Any]) -> list[str]:
    """Inspect a pre-existing target and return its CSV-loadable columns."""
    database, table = mapping["target_database"], mapping["target_table"]
    with db.connection() as connection:
        cursor = connection.cursor(dictionary=True, buffered=True)
        cursor.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'",
            (database, table),
        )
        if cursor.fetchone() is None:
            raise TargetTableError(f"Mapped target table {database}.{table} does not exist.")
        cursor.execute(
            """SELECT column_name AS column_name, is_nullable AS is_nullable,
                      column_default AS column_default, extra AS extra,
                      generation_expression AS generation_expression
               FROM information_schema.columns
               WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""",
            (database, table),
        )
        columns = cursor.fetchall()
        batch = next((column for column in columns if column["column_name"].lower() == "batch_num"), None)
        if not batch or "INVISIBLE" not in (batch["extra"] or "").upper():
            raise TargetTableError(f"Mapped target table {database}.{table} must have an invisible batch_num column.")
        cursor.execute(
            """SELECT partition_method AS partition_method, partition_expression AS partition_expression
               FROM information_schema.partitions
               WHERE table_schema = %s AND table_name = %s AND partition_name IS NOT NULL LIMIT 1""",
            (database, table),
        )
        partition = cursor.fetchone()
        if not partition or not (partition["partition_method"] or "").upper().startswith("LIST") or "batch_num" not in (partition["partition_expression"] or "").lower():
            raise TargetTableError(f"Mapped target table {database}.{table} must use LIST partitioning by batch_num.")
        cursor.execute(
            """SELECT index_name AS index_name, column_name AS column_name FROM information_schema.statistics
               WHERE table_schema = %s AND table_name = %s AND non_unique = 0
               ORDER BY index_name, seq_in_index""",
            (database, table),
        )
        unique_indexes: dict[str, list[str]] = {}
        for index in cursor.fetchall():
            unique_indexes.setdefault(index["index_name"], []).append(index["column_name"].lower())
        if any("batch_num" not in columns for columns in unique_indexes.values()):
            raise TargetTableError(f"Every unique key on {database}.{table} must include batch_num.")
    load_columns = []
    for column in columns:
        if column["column_name"].lower() == "batch_num" or column["generation_expression"]:
            continue
        extra = (column["extra"] or "").upper()
        if "INVISIBLE" in extra or "AUTO_INCREMENT" in extra:
            continue
        load_columns.append(column["column_name"])
    if not load_columns:
        raise TargetTableError(f"Mapped target table {database}.{table} has no CSV-loadable columns.")
    return load_columns


def allocate_or_get_batch(db: Database, mapping: dict[str, Any], source: dict[str, str], *, create: bool) -> dict[str, Any]:
    with db.connection() as connection:
        cursor = connection.cursor(dictionary=True, buffered=True)
        cursor.execute(
            f"SELECT * FROM {control_table('source_object_batches')} WHERE mapping_id = %s AND source_key = %s FOR UPDATE",
            (mapping["id"], source_key(mapping["id"], source)),
        )
        record = cursor.fetchone()
        if record:
            if record["lifecycle_state"] == "LOADING":
                cursor.execute(
                    "SELECT TIMESTAMPDIFF(SECOND, %s, UTC_TIMESTAMP()) AS age_seconds",
                    (record["updated_at"],),
                )
                age_seconds = int(cursor.fetchone()["age_seconds"] or 0)
                if age_seconds < LOAD_LEASE_SECONDS:
                    raise ValueError("This source object already has a load in progress.")
                cursor.execute(
                    f"UPDATE {control_table('source_object_batches')} SET lifecycle_state = 'ERROR' WHERE id = %s",
                    (record["id"],),
                )
                record["lifecycle_state"] = "ERROR"
            if create and record["lifecycle_state"] == "ACTIVE":
                raise ValueError("This object already has an active batch; use the update scenario for a replacement.")
            cursor.execute(
                f"INSERT IGNORE INTO {control_table('target_batch_sequences')} (target_database, target_table, next_batch_num) VALUES (%s, %s, %s)",
                (mapping["target_database"], mapping["target_table"], record["batch_num"] + 1),
            )
            cursor.execute(
                f"""UPDATE {control_table('target_batch_sequences')}
                   SET next_batch_num = GREATEST(next_batch_num, %s)
                   WHERE target_database = %s AND target_table = %s""",
                (record["batch_num"] + 1, mapping["target_database"], mapping["target_table"]),
            )
            cursor.execute(
                f"UPDATE {control_table('source_object_batches')} SET lifecycle_state = 'LOADING', object_version = %s WHERE id = %s",
                (source["object_version"], record["id"]),
            )
            record["lifecycle_state"] = "LOADING"
            record["object_version"] = source["object_version"]
            return record
        if not create:
            raise ValueError("No batch exists for this object; run the create scenario first.")
        cursor.execute(
            f"""SELECT COALESCE(MAX(batch_num), 0) AS highest_batch
               FROM {control_table('source_object_batches')}
               WHERE target_database = %s AND target_table = %s""",
            (mapping["target_database"], mapping["target_table"]),
        )
        initial_batch = cursor.fetchone()["highest_batch"] + 1
        cursor.execute(
            f"INSERT IGNORE INTO {control_table('target_batch_sequences')} (target_database, target_table, next_batch_num) VALUES (%s, %s, %s)",
            (mapping["target_database"], mapping["target_table"], initial_batch),
        )
        cursor.execute(
            f"SELECT next_batch_num FROM {control_table('target_batch_sequences')} WHERE target_database = %s AND target_table = %s FOR UPDATE",
            (mapping["target_database"], mapping["target_table"]),
        )
        batch_num = cursor.fetchone()["next_batch_num"]
        cursor.execute(
            f"UPDATE {control_table('target_batch_sequences')} SET next_batch_num = next_batch_num + 1 WHERE target_database = %s AND target_table = %s",
            (mapping["target_database"], mapping["target_table"]),
        )
        cursor.execute(
            f"""INSERT INTO {control_table('source_object_batches')}
               (mapping_id, bucket_name, resource_name, target_database, target_table, batch_num, source_key, object_version, lifecycle_state)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'LOADING')""",
            (mapping["id"], source["bucket_name"], source["resource_name"], mapping["target_database"], mapping["target_table"], batch_num, source_key(mapping["id"], source), source["object_version"]),
        )
        return {"id": cursor.lastrowid, "batch_num": batch_num}


def ensure_partition(db: Database, mapping: dict[str, Any], batch_num: int) -> None:
    target = table_name(mapping["target_database"], mapping["target_table"])
    name = partition_name(batch_num)
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT 1 FROM information_schema.partitions WHERE table_schema = %s AND table_name = %s AND partition_name = %s",
            (mapping["target_database"], mapping["target_table"], name),
        )
        if cursor.fetchone() is None:
            cursor.execute(f"ALTER TABLE {target} ADD PARTITION (PARTITION {quote_identifier(name, 'partition name')} VALUES IN ({batch_num}))")


def create_stage_table(db: Database, mapping: dict[str, Any], batch_num: int) -> str:
    target = table_name(mapping["target_database"], mapping["target_table"])
    stage = stage_name(mapping["target_table"])
    stage_quoted = table_name(mapping["target_database"], stage)
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"CREATE TABLE {stage_quoted} LIKE {target}")
        cursor.execute(f"ALTER TABLE {stage_quoted} REMOVE PARTITIONING")
    return stage


def drop_stage_table(db: Database, mapping: dict[str, Any], stage: str) -> None:
    """Remove a per-batch staging table after exchange or failed processing."""
    with db.connection() as connection:
        connection.cursor().execute(
            f"DROP TABLE IF EXISTS {table_name(mapping['target_database'], stage)}"
        )


def csv_batches(csv_source: Path | TextIO, columns: list[str], batch_rows: int) -> Iterator[list[tuple[str, ...]]]:
    """Read CSV batches from either a path or an already-open text stream."""
    if isinstance(csv_source, Path):
        with csv_source.open(newline="", encoding="utf-8") as source:
            yield from csv_batches(source, columns, batch_rows)
        return
    source = csv_source
    reader = csv.DictReader(source)
    headers = reader.fieldnames or []
    header_by_folded_name: dict[str, str] = {}
    for header in headers:
        folded = header.casefold()
        if folded in header_by_folded_name:
            raise ValueError(f"CSV has duplicate column names when compared case-insensitively: {header_by_folded_name[folded]}, {header}.")
        header_by_folded_name[folded] = header
    expected_by_folded_name = {column.casefold(): column for column in columns}
    missing = [column for column in columns if column.casefold() not in header_by_folded_name]
    unknown = [header for header in headers if header.casefold() not in expected_by_folded_name]
    if missing or unknown:
        details = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if unknown:
            details.append(f"unknown: {', '.join(unknown)}")
        raise ValueError(f"CSV columns do not match target table ({'; '.join(details)}).")
    batch: list[tuple[str, ...]] = []
    for row in reader:
        batch.append(tuple((row.get(header_by_folded_name[column.casefold()]) or "").strip() for column in columns))
        if len(batch) >= batch_rows:
            yield batch
            batch = []
    if batch:
        yield batch


def insert_batch(db: Database, mapping: dict[str, Any], stage: str, batch_num: int, columns: list[str], rows: list[tuple[str, ...]]) -> int:
    names = ", ".join([quote_identifier("batch_num", "batch column"), *(quote_identifier(column, "target column") for column in columns)])
    placeholders = ", ".join(["%s"] * (len(columns) + 1))
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.executemany(
            f"INSERT INTO {table_name(mapping['target_database'], stage)} ({names}) VALUES ({placeholders})",
            [(batch_num, *row) for row in rows],
        )
    return len(rows)


def load_csv_parallel(db: Database, mapping: dict[str, Any], stage: str, batch_num: int, columns: list[str], csv_source: Path | TextIO, batch_rows: int, workers: int) -> int:
    pending, inserted = set(), 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        for rows in csv_batches(csv_source, columns, batch_rows):
            pending.add(executor.submit(insert_batch, db, mapping, stage, batch_num, columns, rows))
            if len(pending) >= workers * 2:
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                inserted += sum(job.result() for job in done)
        for job in pending:
            inserted += job.result()
    return inserted


def validate_and_exchange(db: Database, mapping: dict[str, Any], stage: str, batch_num: int) -> None:
    target = table_name(mapping["target_database"], mapping["target_table"])
    stage_quoted = table_name(mapping["target_database"], stage)
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {stage_quoted} WHERE batch_num <> %s", (batch_num,))
        if cursor.fetchone()[0]:
            raise ValueError("Stage table contains a row with an unexpected batch number.")
        cursor.execute(f"ALTER TABLE {target} EXCHANGE PARTITION {quote_identifier(partition_name(batch_num), 'partition name')} WITH TABLE {stage_quoted} WITHOUT VALIDATION")


def log_event(db: Database, source: dict[str, Any], action: str, status: str, mapping: dict[str, Any] | None = None, batch_num: int | None = None, message: str | None = None) -> int:
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""INSERT INTO {control_table('event_tx_log')}
               (object_event_id, mapping_id, target_database, target_table, batch_num, event_action, event_status, bucket_name, resource_name, object_version, message)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (source.get("object_event_id"), mapping.get("id") if mapping else None, mapping.get("target_database") if mapping else None, mapping.get("target_table") if mapping else None, batch_num, action, status, source["bucket_name"], source["resource_name"], source["object_version"], message),
        )
        if source.get("object_event_id"):
            cursor.execute(
                f"""UPDATE {control_table('object_event')}
                    SET completed_at = UTC_TIMESTAMP(6),
                        duration_ms = TIMESTAMPDIFF(MICROSECOND, received_at, UTC_TIMESTAMP(6)) / 1000
                  WHERE id = %s""",
                (source["object_event_id"],),
            )
        return cursor.lastrowid


def log_error(db: Database, source: dict[str, Any], action: str, error: Exception, mapping: dict[str, Any] | None = None, batch_num: int | None = None) -> None:
    try:
        event_log_id = log_event(db, source, action, "ERROR", mapping, batch_num, str(error))
        with db.connection() as connection:
            connection.cursor().execute(
                f"""INSERT INTO {control_table('event_errors')}
                   (event_log_id, mapping_id, target_database, target_table, event_action, error_code, error_message)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (event_log_id, mapping.get("id") if mapping else None, mapping.get("target_database") if mapping else None, mapping.get("target_table") if mapping else None, action, "TARGET_TABLE_NOT_FOUND" if isinstance(error, TargetTableError) else type(error).__name__.upper()[:64], str(error)),
            )
    except Exception:
        pass


def mark_active(db: Database, record_id: int) -> None:
    with db.connection() as connection:
        connection.cursor().execute(f"UPDATE {control_table('source_object_batches')} SET lifecycle_state = 'ACTIVE' WHERE id = %s", (record_id,))


def mark_error(db: Database, record_id: int) -> None:
    """Release a failed batch record so a later update event can retry it."""
    with db.connection() as connection:
        connection.cursor().execute(
            f"UPDATE {control_table('source_object_batches')} SET lifecycle_state = 'ERROR' WHERE id = %s",
            (record_id,),
        )


def run_load(event_path: Path, csv_path: Path, *, create: bool, batch_rows: int, workers: int) -> dict[str, Any]:
    event = json.loads(event_path.read_text(encoding="utf-8"))
    if not isinstance(event, dict):
        raise ValueError("Event JSON must be an object.")
    db, mapping, record, stage = Database(), None, None, None
    source, action = event_source(event), "CREATE" if create else "UPDATE"
    try:
        ensure_control_tables(db)
        mapping = resolve_mapping(db, source)
        columns = target_definition(db, mapping)
        record = allocate_or_get_batch(db, mapping, source, create=create)
        ensure_partition(db, mapping, record["batch_num"])
        stage = create_stage_table(db, mapping, record["batch_num"])
        rows = load_csv_parallel(db, mapping, stage, record["batch_num"], columns, csv_path, batch_rows, workers)
        validate_and_exchange(db, mapping, stage, record["batch_num"])
        mark_active(db, record["id"])
        log_event(db, source, action, "SUCCESS", mapping, record["batch_num"], f"Loaded {rows} row(s).")
        return {"event": action.lower(), "batch_num": record["batch_num"], "target": f"{mapping['target_database']}.{mapping['target_table']}", "rows": rows}
    except Exception as error:
        if record is not None:
            try:
                mark_error(db, record["id"])
            except Exception:
                pass
        log_error(db, source, action, error, mapping, record.get("batch_num") if record else None)
        raise
    finally:
        if mapping is not None and stage is not None:
            try:
                drop_stage_table(db, mapping, stage)
            except Exception:
                # A cleanup failure must not hide the original load outcome;
                # UUID stage names prevent a later event from colliding with it.
                pass


def run_delete(event_path: Path) -> dict[str, Any]:
    event = json.loads(event_path.read_text(encoding="utf-8"))
    if not isinstance(event, dict):
        raise ValueError("Event JSON must be an object.")
    db, mapping, record = Database(), None, None
    source, action = event_source(event), "DELETE"
    try:
        ensure_control_tables(db)
        mapping = resolve_mapping(db, source)
        target_definition(db, mapping)
        with db.connection() as connection:
            cursor = connection.cursor(dictionary=True, buffered=True)
            cursor.execute(f"SELECT * FROM {control_table('source_object_batches')} WHERE mapping_id = %s AND source_key = %s FOR UPDATE", (mapping["id"], source_key(mapping["id"], source)))
            record = cursor.fetchone()
            if not record or record["lifecycle_state"] == "DELETED":
                log_event(db, source, action, "SUCCESS", mapping, None, "Object batch already absent.")
                return {"event": "delete", "result": "already absent"}
            if record["lifecycle_state"] == "LOADING":
                raise ValueError("This source object has a load in progress; retry the delete after it completes or is recovered.")
            cursor.execute(f"ALTER TABLE {table_name(record['target_database'], record['target_table'])} TRUNCATE PARTITION {quote_identifier(partition_name(record['batch_num']), 'partition name')}")
            cursor.execute(f"UPDATE {control_table('source_object_batches')} SET lifecycle_state = 'DELETED' WHERE id = %s", (record["id"],))
        log_event(db, source, action, "SUCCESS", mapping, record["batch_num"], "Truncated mapped batch partition.")
        return {"event": "delete", "batch_num": record["batch_num"], "target": f"{record['target_database']}.{record['target_table']}"}
    except Exception as error:
        log_error(db, source, action, error, mapping, record.get("batch_num") if record else None)
        raise


def load_arguments(description: str, *, csv_required: bool) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--event", type=Path, required=True, help="Object Storage CloudEvent JSON file")
    if csv_required:
        parser.add_argument("--csv", type=Path, required=True, help="Local CSV fixture representing the version-pinned source object")
        parser.add_argument("--batch-rows", type=int, default=int(os.environ.get("PROTOTYPE_BATCH_ROWS", "1000")))
        parser.add_argument("--workers", type=int, default=int(os.environ.get("PROTOTYPE_WRITER_WORKERS", "4")))
    return parser
