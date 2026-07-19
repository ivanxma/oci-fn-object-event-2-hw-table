#!/usr/bin/env python3
"""Initialize or reset a mapping-scoped performance-test target."""

from __future__ import annotations

import argparse
import os
import re

import mysql.connector


IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


def identifier(value: str, label: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise SystemExit(f"Invalid {label}: {value!r}")
    return value


def table_exists(cursor, database: str, table: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
        (database, table),
    )
    return bool(cursor.fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()
    control = identifier(os.environ["CONTROL_DATABASE"], "control database")
    target_db = identifier(os.environ["TARGET_DATABASE"], "target database")
    target_table = identifier(os.environ["TARGET_TABLE"], "target table")
    pattern = os.environ["RESOURCE_NAME_PATTERN"]
    event_rule_id = os.environ["EVENT_RULE_ID"]

    connection = mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        ssl_disabled=False,
    )
    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN (%s,%s)",
            (control, target_db),
        )
        available = {row[0] for row in cursor.fetchall()}
        missing = sorted({control, target_db} - available)
        if missing:
            raise SystemExit(
                "Missing pre-created database(s): "
                + ", ".join(missing)
                + ". Create them and grant the deployment user access before rerunning."
            )

        mapping_table = f"`{control}`.`object_storage_mappings`"
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {mapping_table} (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                compartment_name VARCHAR(255) NOT NULL,
                bucket_name VARCHAR(255) NOT NULL,
                resource_name_pattern VARCHAR(1024) NOT NULL,
                target_database VARCHAR(64) NOT NULL,
                target_table VARCHAR(64) NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                invocation_mode ENUM('SYNC','DETACHED') NOT NULL DEFAULT 'SYNC',
                worker_threads SMALLINT UNSIGNED NOT NULL DEFAULT 4,
                event_rule_id VARCHAR(255) NULL,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"""
        )

        if args.reset:
            # Reset mutable loader state but retain event_tx_log/event_errors.
            # Those tables are the operational audit history used by Event TX;
            # deleting them leaves completed object_event rows looking RECEIVED
            # and empties the Registered Table transaction list.
            if table_exists(cursor, control, "source_object_batches"):
                cursor.execute(
                    f"DELETE FROM `{control}`.`source_object_batches` WHERE target_database = %s AND target_table = %s",
                    (target_db, target_table),
                )
            if table_exists(cursor, control, "target_batch_sequences"):
                cursor.execute(
                    f"DELETE FROM `{control}`.`target_batch_sequences` WHERE target_database=%s AND target_table=%s",
                    (target_db, target_table),
                )
            cursor.execute(f"DROP TABLE IF EXISTS `{target_db}`.`{target_table}`")

        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS `{target_db}`.`{target_table}` (
              record_id BIGINT NOT NULL,
              event_ts DATETIME(6) NOT NULL,
              category VARCHAR(32) NOT NULL,
              payload VARCHAR(512) NOT NULL,
              amount DECIMAL(14,2) NOT NULL,
              batch_num BIGINT UNSIGNED NOT NULL INVISIBLE,
              PRIMARY KEY (record_id, batch_num),
              KEY ix_event_ts (event_ts)
            ) ENGINE=InnoDB
            PARTITION BY LIST (batch_num) (PARTITION p_seed VALUES IN (0))"""
        )
        cursor.execute(
            f"""SELECT id FROM {mapping_table}
                WHERE compartment_name=%s AND bucket_name=%s AND resource_name_pattern=%s
                ORDER BY id""",
            (os.environ["COMPARTMENT_NAME"], os.environ["BUCKET_NAME"], pattern),
        )
        ids = [int(row[0]) for row in cursor.fetchall()]
        if len(ids) > 1:
            raise SystemExit(f"Multiple mappings use the exact test pattern: {ids}")
        values = (
            target_db,
            target_table,
            os.environ.get("INVOCATION_MODE", "SYNC"),
            int(os.environ.get("WRITER_WORKERS", "4")),
            event_rule_id,
        )
        if ids:
            mapping_id = ids[0]
            cursor.execute(
                f"""UPDATE {mapping_table} SET target_database=%s, target_table=%s,
                    invocation_mode=%s, worker_threads=%s, event_rule_id=%s WHERE id=%s""",
                (*values, mapping_id),
            )
        else:
            cursor.execute(
                f"""INSERT INTO {mapping_table}
                    (compartment_name,bucket_name,resource_name_pattern,target_database,target_table,
                     invocation_mode,worker_threads,event_rule_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    os.environ["COMPARTMENT_NAME"],
                    os.environ["BUCKET_NAME"],
                    pattern,
                    *values,
                ),
            )
            mapping_id = int(cursor.lastrowid)
        connection.commit()
        print(f"Database setup complete: mapping_id={mapping_id}, target={target_db}.{target_table}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
