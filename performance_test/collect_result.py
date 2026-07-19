#!/usr/bin/env python3
"""Collect one completed create/delete test result as JSON."""

from __future__ import annotations

from datetime import datetime
import json
import os

import mysql.connector


def text(value):
    return value.isoformat(sep=" ") if isinstance(value, datetime) else value


connection = mysql.connector.connect(
    host=os.environ["DB_HOST"],
    port=int(os.environ.get("DB_PORT", "3306")),
    user=os.environ["DB_USER"],
    password=os.environ["DB_PASSWORD"],
    ssl_disabled=False,
)
try:
    cursor = connection.cursor(dictionary=True, buffered=True)
    control = os.environ["CONTROL_DATABASE"]
    object_name = os.environ["OBJECT_NAME"]
    cursor.execute(
        f"""SELECT event_type,event_time,received_at,completed_at,duration_ms
            FROM `{control}`.`object_event` WHERE resource_name=%s ORDER BY id""",
        (object_name,),
    )
    events = cursor.fetchall()
    cursor.execute(
        f"""SELECT event_action,event_status,message,batch_num,created_at
            FROM `{control}`.`event_tx_log` WHERE resource_name=%s ORDER BY id""",
        (object_name,),
    )
    transactions = cursor.fetchall()
    cursor.execute(
        """SELECT COUNT(*) AS count FROM information_schema.tables
           WHERE table_schema=%s AND table_name LIKE %s""",
        (os.environ["TARGET_DATABASE"], os.environ["TARGET_TABLE"] + "\\_stage\\_%"),
    )
    staging_count = int(cursor.fetchone()["count"])
    create_event = next((row for row in events if str(row["event_type"]).lower().endswith(("createobject", "updateobject"))), None)
    delete_event = next((row for row in events if str(row["event_type"]).lower().endswith("deleteobject")), None)
    create_tx = next((row for row in transactions if row["event_action"] in {"CREATE", "UPDATE"}), None)
    delete_tx = next((row for row in transactions if row["event_action"] == "DELETE"), None)
    file_bytes = int(os.environ["FILE_BYTES"])
    rows = int(os.environ["EXPECTED_ROWS"])
    duration_seconds = float(create_event["duration_ms"]) / 1000 if create_event and create_event["duration_ms"] is not None else None
    result = {
        "label": os.environ["CASE_LABEL"],
        "mode": os.environ["INVOCATION_MODE"],
        "worker_threads": int(os.environ.get("WRITER_WORKERS", "4")),
        "payload_bytes": int(os.environ.get("PAYLOAD_BYTES", "480")),
        "object_name": object_name,
        "file_bytes": file_bytes,
        "file_mib": file_bytes / 1048576,
        "rows": rows,
        "generation_seconds": float(os.environ["GENERATION_SECONDS"]),
        "upload_seconds": float(os.environ["UPLOAD_SECONDS"]),
        "create_status": create_tx["event_status"] if create_tx else None,
        "create_message": create_tx["message"] if create_tx else None,
        "event_time": text(create_event["event_time"]) if create_event else None,
        "received_at": text(create_event["received_at"]) if create_event else None,
        "completed_at": text(create_event["completed_at"]) if create_event else None,
        "delivery_latency_seconds": (create_event["received_at"] - create_event["event_time"]).total_seconds() if create_event else None,
        "processing_seconds": duration_seconds,
        "throughput_mib_per_second": (file_bytes / 1048576 / duration_seconds) if duration_seconds else None,
        "seconds_per_100_rows": (duration_seconds / rows * 100) if duration_seconds and rows else None,
        "delete_status": delete_tx["event_status"] if delete_tx else None,
        "delete_processing_seconds": float(delete_event["duration_ms"]) / 1000 if delete_event and delete_event["duration_ms"] is not None else None,
        "staging_tables_after_cleanup": staging_count,
    }
    print(json.dumps(result, separators=(",", ":"), default=text))
finally:
    connection.close()
