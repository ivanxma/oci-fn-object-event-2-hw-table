#!/usr/bin/env python3
"""Set the exact performance mapping's invocation mode."""

from __future__ import annotations

import os

import mysql.connector


mode = os.environ["INVOCATION_MODE"].upper()
if mode not in {"SYNC", "DETACHED"}:
    raise SystemExit("INVOCATION_MODE must be SYNC or DETACHED")
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
        f"""UPDATE `{os.environ['CONTROL_DATABASE']}`.`object_storage_mappings`
            SET invocation_mode=%s, worker_threads=%s
            WHERE compartment_name=%s AND bucket_name=%s AND resource_name_pattern=%s""",
        (
            mode,
            int(os.environ.get("WRITER_WORKERS", "4")),
            os.environ["COMPARTMENT_NAME"],
            os.environ["BUCKET_NAME"],
            os.environ["RESOURCE_NAME_PATTERN"],
        ),
    )
    cursor.execute(
        f"""SELECT COUNT(*) FROM `{os.environ['CONTROL_DATABASE']}`.`object_storage_mappings`
            WHERE compartment_name=%s AND bucket_name=%s AND resource_name_pattern=%s
              AND invocation_mode=%s AND worker_threads=%s""",
        (
            os.environ["COMPARTMENT_NAME"],
            os.environ["BUCKET_NAME"],
            os.environ["RESOURCE_NAME_PATTERN"],
            mode,
            int(os.environ.get("WRITER_WORKERS", "4")),
        ),
    )
    matched = int(cursor.fetchone()[0])
    if matched != 1:
        raise SystemExit(f"Expected one configured mapping; found {matched}")
    connection.commit()
    print(f"Mapping mode set to {mode}")
finally:
    connection.close()
