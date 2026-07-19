#!/usr/bin/env python3
"""Wait for one Object Storage event and verify the target row count."""

from __future__ import annotations

import os
import re
import time

import mysql.connector


IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")


def identifier(name: str) -> str:
    if not IDENTIFIER.fullmatch(name):
        raise SystemExit(f"Invalid database identifier: {name!r}")
    return name


def main() -> None:
    control = identifier(os.environ["CONTROL_DATABASE"])
    target_db = identifier(os.environ["TARGET_DATABASE"])
    target_table = identifier(os.environ["TARGET_TABLE"])
    object_name = os.environ["OBJECT_NAME"]
    action = os.environ["EXPECTED_ACTION"].upper()
    expected_rows = int(os.environ["EXPECTED_ROWS"])
    deadline = time.monotonic() + int(os.environ.get("EVENT_WAIT_SECONDS", "360"))
    connection = mysql.connector.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        ssl_disabled=False,
    )
    try:
        cursor = connection.cursor()
        while time.monotonic() < deadline:
            cursor.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=%s AND table_name='event_tx_log'",
                (control,),
            )
            if cursor.fetchone()[0]:
                cursor.execute(
                    f"""SELECT event_status, message FROM `{control}`.`event_tx_log`
                        WHERE resource_name=%s AND event_action=%s ORDER BY id DESC LIMIT 1""",
                    (object_name, action),
                )
                row = cursor.fetchone()
                if row and row[0] in {"SUCCESS", "ERROR"}:
                    if row[0] != "SUCCESS":
                        raise SystemExit(f"{action} event failed: {row[1] or 'no error message'}")
                    cursor.execute(f"SELECT COUNT(*) FROM `{target_db}`.`{target_table}`")
                    actual_rows = int(cursor.fetchone()[0])
                    if actual_rows != expected_rows:
                        raise SystemExit(
                            f"{action} succeeded but target has {actual_rows} rows; expected {expected_rows}."
                        )
                    print(f"{action} event succeeded; target_rows={actual_rows}")
                    return
            connection.rollback()
            time.sleep(3)
        raise SystemExit(f"Timed out waiting for terminal {action} status for {object_name}")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
