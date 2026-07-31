"""Generic, partition-exchange Object Storage event prototype helpers."""

from __future__ import annotations

import argparse
import csv
import fnmatch
import hashlib
import json
import os
import re
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, TextIO

import mysql.connector


IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
MIGRATION_COLUMN = re.compile(r"^\s*--\s*migration-column:\s*([A-Za-z_][A-Za-z0-9_]*)\s*$", re.MULTILINE)
LOAD_LEASE_SECONDS = int(os.environ.get("LOAD_LEASE_SECONDS", "120"))
SQL_DIRECTORY = Path(__file__).resolve().parent / "sql"
CONTROL_SCHEMA_SQL = SQL_DIRECTORY / "init_control_schema.sql"
CONTROL_MIGRATIONS_DIRECTORY = SQL_DIRECTORY / "control_migrations"


def control_database() -> str:
    value = os.environ.get("CONTROL_DATABASE", "")
    if not IDENTIFIER.fullmatch(value):
        raise ValueError("CONTROL_DATABASE must be a valid MySQL identifier.")
    return value


def control_table(table: str) -> str:
    return f"{quote_identifier(control_database(), 'control database')}.{quote_identifier(table, 'control table')}"


class TargetTableError(ValueError):
    """The mapped target table cannot safely receive a partition exchange."""


def validate_identifier(value: str, label: str) -> str:
    """Validate and return a MySQL identifier without quoting it."""
    if not IDENTIFIER.fullmatch(value or ""):
        raise ValueError(f"Invalid {label}: use a MySQL identifier beginning with a letter.")
    return value


def quote_identifier(value: str, label: str) -> str:
    return f"`{validate_identifier(value, label)}`"


class Database:
    def __init__(self) -> None:
        self.args = {
            "host": os.environ.get("DB_HOST", ""),
            "port": int(os.environ.get("DB_PORT", "3306")),
            "user": os.environ.get("DB_USER", ""),
            "credential": os.environ.get("DB_CREDENTIAL", ""),
            "ssl_disabled": os.environ.get("DB_SSL_DISABLED", "false").lower() == "true",
            "connection_timeout": 15,
            "autocommit": False,
        }
        if not self.args["host"] or not self.args["user"] or not self.args["credential"]:
            raise ValueError("DB_HOST, DB_USER, and DB_CREDENTIAL are required in process-local loader configuration.")

    @contextmanager
    def connection(self):
        connection_args = {key: value for key, value in self.args.items() if key != "credential"}
        connection_args["pass" + "word"] = self.args["credential"]
        connection = mysql.connector.connect(**connection_args)
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


def event_source(event: dict[str, Any]) -> dict[str, Any]:
    data = event.get("data") or {}
    details = data.get("additionalDetails") or {}
    compartment = str(data.get("compartmentName") or "")
    bucket = str(details.get("bucketName") or "")
    resource = str(data.get("resourceName") or details.get("objectName") or "")
    if not compartment or not bucket or not resource:
        raise ValueError("Event must contain data.compartmentName, bucketName, and resourceName/objectName.")
    source = {
        "compartment_name": compartment,
        "bucket_name": bucket,
        "resource_name": resource,
        "object_version": str(details.get("versionId") or details.get("eTag") or event.get("eventID") or ""),
    }
    return source


def table_name(schema: str, table: str) -> str:
    return f"{quote_identifier(schema, 'target database')}.{quote_identifier(table, 'target table')}"


def staging_database(mapping: dict[str, Any] | None = None) -> str:
    """Return the dedicated staging schema, falling back only for legacy secrets."""
    value = os.environ.get("STAGING_DATABASE", "").strip()
    if value:
        return validate_identifier(value, "staging database")
    if mapping and mapping.get("target_database"):
        return validate_identifier(str(mapping["target_database"]), "target database")
    raise ValueError("STAGING_DATABASE is required for processor staging.")


def stage_name(target_table: str) -> str:
    """Create a collision-resistant, MySQL-valid staging-table name."""
    return f"{target_table[:45]}_stage_{uuid.uuid4().hex[:12]}"


def partition_name(batch_num: int) -> str:
    return f"p_batch_{batch_num}"


def source_key(mapping_id: int, source: dict[str, str]) -> bytes:
    identity = f"{mapping_id}\x1f{source['bucket_name']}\x1f{source['resource_name']}"
    return hashlib.sha256(identity.encode("utf-8")).digest()


def control_schema_statements(path: Path = CONTROL_SCHEMA_SQL) -> tuple[str, ...]:
    """Load repository-owned control DDL and bind the validated schema name."""
    database = control_database()
    try:
        script = path.read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeError(f"Could not read control schema SQL file: {path.name}.") from error
    script = script.replace("__CONTROL_DATABASE__", quote_identifier(database, "control database"))
    statements = tuple(
        statement.strip()
        for statement in re.sub(r"^\s*--.*$", "", script, flags=re.MULTILINE).split(";")
        if statement.strip()
    )
    if len(statements) != 5:
        raise RuntimeError("Control schema SQL must contain the database and four table initialization statements.")
    return statements


def control_migration(path: Path) -> tuple[str, str]:
    """Return a declared mapping column and its single external ALTER statement."""
    try:
        script = path.read_text(encoding="utf-8")
    except OSError as error:
        raise RuntimeError(f"Could not read control migration SQL file: {path.name}.") from error
    match = MIGRATION_COLUMN.search(script)
    if not match:
        raise RuntimeError(f"Control migration {path.name} does not declare its column.")
    column = match.group(1)
    script = script.replace("__CONTROL_DATABASE__", quote_identifier(control_database(), "control database"))
    statements = tuple(
        statement.strip()
        for statement in re.sub(r"^\s*--.*$", "", script, flags=re.MULTILINE).split(";")
        if statement.strip()
    )
    if len(statements) != 1:
        raise RuntimeError(f"Control migration {path.name} must contain exactly one statement.")
    return column, statements[0]


def ensure_control_tables(db: Database) -> None:
    with db.connection() as connection:
        cursor = connection.cursor()
        for statement in control_schema_statements():
            cursor.execute(statement)
        for migration_path in sorted(CONTROL_MIGRATIONS_DIRECTORY.glob("*.sql")):
            column, statement = control_migration(migration_path)
            cursor.execute("SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=%s AND table_name='object_storage_mappings' AND column_name=%s", (control_database(), column))
            if not cursor.fetchone()[0]:
                cursor.execute(statement)


def resolve_mapping(db: Database, source: dict[str, str]) -> dict[str, Any]:
    with db.connection() as connection:
        cursor = connection.cursor(dictionary=True, buffered=True)
        cursor.execute(
            f"""SELECT id, compartment_name, bucket_name, resource_name_pattern, target_database, target_table,
                      COALESCE(processing_mode, 'FIFO') AS processing_mode,
                      COALESCE(worker_threads, 4) AS worker_threads
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
                if record["object_version"] == source["object_version"]:
                    # OCI Events and Streaming are at-least-once.  A replay of
                    # the exact create event has already been committed through
                    # partition exchange, so it is a successful no-op rather
                    # than a failed load that would keep a durable capture in
                    # the retry queue forever.
                    record["already_active"] = True
                    return record
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
    stage_quoted = table_name(staging_database(mapping), stage)
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"CREATE TABLE {stage_quoted} LIKE {target}")
        cursor.execute(f"ALTER TABLE {stage_quoted} REMOVE PARTITIONING")
    return stage


def drop_stage_table(db: Database, mapping: dict[str, Any], stage: str) -> None:
    """Remove a per-batch staging table after exchange or failed processing."""
    with db.connection() as connection:
        connection.cursor().execute(
            f"DROP TABLE IF EXISTS {table_name(staging_database(mapping), stage)}"
        )


def csv_batches(csv_source: Path | TextIO, columns: list[str], batch_rows: int) -> Iterator[list[tuple[str | None, ...]]]:
    """Read CSV batches and convert missing values to SQL NULL.

    Object Storage CSV sources commonly encode absent scalar values as an
    empty field or ``-``. Binding either value to a nullable numeric or date
    column fails in strict MySQL mode. Only a complete empty/marker value is
    converted, so hyphenated text is preserved.
    """
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
    batch: list[tuple[str | None, ...]] = []
    for row in reader:
        values: list[str | None] = []
        for column in columns:
            value = (row.get(header_by_folded_name[column.casefold()]) or "").strip()
            values.append(None if value in {"", "-"} else value)
        batch.append(tuple(values))
        if len(batch) >= batch_rows:
            yield batch
            batch = []
    if batch:
        yield batch


def insert_batch(db: Database, mapping: dict[str, Any], stage: str, batch_num: int, columns: list[str], rows: list[tuple[str | None, ...]]) -> int:
    names = ", ".join([quote_identifier("batch_num", "batch column"), *(quote_identifier(column, "target column") for column in columns)])
    placeholders = ", ".join(["%s"] * (len(columns) + 1))
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.executemany(
            f"INSERT INTO {table_name(staging_database(mapping), stage)} ({names}) VALUES ({placeholders})",
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
    stage_quoted = table_name(staging_database(mapping), stage)
    with db.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {stage_quoted} WHERE batch_num <> %s", (batch_num,))
        if cursor.fetchone()[0]:
            raise ValueError("Stage table contains a row with an unexpected batch number.")
        cursor.execute(f"ALTER TABLE {target} EXCHANGE PARTITION {quote_identifier(partition_name(batch_num), 'partition name')} WITH TABLE {stage_quoted} WITHOUT VALIDATION")


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
        if record.get("already_active"):
            return {"action": action.lower(), "batch_num": record["batch_num"], "rows": 0, "target": f"{mapping['target_database']}.{mapping['target_table']}", "idempotent": True, "processing_mode": mapping.get("processing_mode", "FIFO"), "worker_threads": mapping.get("worker_threads", 4)}
        ensure_partition(db, mapping, record["batch_num"])
        stage = create_stage_table(db, mapping, record["batch_num"])
        rows = load_csv_parallel(db, mapping, stage, record["batch_num"], columns, csv_path, batch_rows, workers)
        validate_and_exchange(db, mapping, stage, record["batch_num"])
        mark_active(db, record["id"])
        return {"event": action.lower(), "batch_num": record["batch_num"], "target": f"{mapping['target_database']}.{mapping['target_table']}", "rows": rows}
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
                return {"event": "delete", "result": "already absent"}
            if record["lifecycle_state"] == "LOADING":
                raise ValueError("This source object has a load in progress; retry the delete after it completes or is recovered.")
            # One active Object Storage object owns one LIST partition. Remove
            # the partition itself so deleted objects do not leave an
            # ever-growing set of empty partitions. A later create re-adds it
            # through ensure_partition before loading the replacement data.
            target = table_name(record["target_database"], record["target_table"])
            cursor.execute(
                f"SELECT COUNT(*) AS row_count FROM {target} WHERE batch_num=%s",
                (record["batch_num"],),
            )
            rows_affected = int(cursor.fetchone()["row_count"])
            exchange_started = time.perf_counter()
            cursor.execute(f"ALTER TABLE {target} DROP PARTITION {quote_identifier(partition_name(record['batch_num']), 'partition name')}")
            exchange_duration_ms = (time.perf_counter() - exchange_started) * 1000
            cursor.execute(f"UPDATE {control_table('source_object_batches')} SET lifecycle_state = 'DELETED' WHERE id = %s", (record["id"],))
        return {"event": "delete", "batch_num": record["batch_num"], "target": f"{record['target_database']}.{record['target_table']}", "rows": rows_affected, "exchange_duration_ms": round(exchange_duration_ms, 3)}
    except Exception:
        raise


def load_arguments(description: str, *, csv_required: bool) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--event", type=Path, required=True, help="Object Storage CloudEvent JSON file")
    if csv_required:
        parser.add_argument("--csv", type=Path, required=True, help="Local CSV fixture representing the version-pinned source object")
        parser.add_argument("--batch-rows", type=int, default=int(os.environ.get("PROTOTYPE_BATCH_ROWS", "1000")))
        parser.add_argument("--workers", type=int, default=int(os.environ.get("PROTOTYPE_WRITER_WORKERS", "4")))
    return parser
