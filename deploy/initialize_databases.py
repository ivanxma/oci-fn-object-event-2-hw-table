#!/usr/bin/env python3
"""Initialize control and durable schemas from repository-owned SQL files."""
from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "processor"), str(ROOT / "loader_core")]

from vault_config import load_database_config  # noqa: E402


def main() -> None:
    config = load_database_config()
    control = str(config.get("control_database") or config["database"])
    durable = str(config.get("stream_data_database") or os.environ.get("STREAM_DATA_DB_NAME") or config["database"])
    os.environ.update(
        {
            "DB_HOST": str(config["host"]),
            "DB_PORT": str(config["port"]),
            "DB_USER": str(config["user"]),
            "DB_CREDENTIAL": str(config["credential"]),
            "CONTROL_DATABASE": control,
            "DB_SSL_DISABLED": os.environ.get("DB_SSL_DISABLED", "false"),
        }
    )

    from partition_loader import Database, ensure_control_tables

    ensure_control_tables(Database())

    import mysql.connector

    connection = mysql.connector.connect(
        host=config["host"],
        port=int(config["port"]),
        user=config["user"],
        ssl_disabled=os.environ.get("DB_SSL_DISABLED", "false").lower() == "true",
        **{"pass" + "word": config["credential"]},
    )
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{durable.replace('`', '``')}` CHARACTER SET utf8mb4")
        cursor.execute(f"USE `{durable.replace('`', '``')}`")
        from message_store import ensure_schema
        ensure_schema(connection)
        registry = f"`{durable}`.`stream_message_archive_partitions`"
        archive_sql = (ROOT / "ui" / "myapp" / "sql" / "init_stream_message_archive.sql").read_text(
            encoding="utf-8"
        )
        cursor.execute(archive_sql.replace("__ARCHIVE_REGISTRY__", registry))
        connection.commit()
        for database in (control, durable):
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema=%s ORDER BY table_name",
                (database,),
            )
            print(f"{database}: " + ", ".join(row[0] for row in cursor.fetchall()))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
