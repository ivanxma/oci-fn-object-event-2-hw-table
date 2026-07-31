#!/usr/bin/env python3
"""Verify DDL-only and DDL+data execution against an isolated temporary table."""
from __future__ import annotations

import os
import sys
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "processor"), str(ROOT / "ui")]

import mysql.connector  # noqa: E402
from myapp.services.import_service import ImportService  # noqa: E402
from vault_config import load_database_config  # noqa: E402


class ConnectionProvider:
    def __init__(self, config: dict[str, object], database: str) -> None:
        self.config = config
        self.database = database

    @contextmanager
    def connection(self):
        connection = mysql.connector.connect(
            host=self.config["host"],
            port=int(self.config["port"]),
            user=self.config["user"],
            password=self.config["credential"],
            database=self.database,
            ssl_disabled=os.environ.get("DB_SSL_DISABLED", "false").lower() == "true",
            allow_local_infile=True,
        )
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def create_database(self, _database: str) -> None:
        raise AssertionError("The verification must use an existing target database.")


def main() -> None:
    config = load_database_config()
    database = str(config["database"])
    table = f"verify_import_{uuid.uuid4().hex[:12]}"
    provider = ConnectionProvider(config, database)
    service = ImportService(provider)
    columns = [
        {"source_name": "a", "name": "a", "type": "BIGINT", "nullable": True},
        {"source_name": "b", "name": "b", "type": "BIGINT", "nullable": True},
    ]
    csv_path = Path(tempfile.mkstemp(prefix="ddl-mode-", suffix=".csv")[1])
    csv_path.write_text("a,b\n201,2\n203,4\n205,6\n", encoding="utf-8")
    try:
        service.load_data(
            csv_path, database, table, columns, [], False, ",",
            include_data=False, drop_existing=True,
        )
        with provider.connection() as connection:
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM `{database}`.`{table}`")
            ddl_only_rows = int(cursor.fetchone()[0])
        if ddl_only_rows != 0:
            raise AssertionError(f"DDL-only execution loaded {ddl_only_rows} unexpected row(s).")

        loaded = service.load_data(
            csv_path, database, table, columns, [], False, ",",
            include_data=True, drop_existing=True,
        )
        with provider.connection() as connection:
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM `{database}`.`{table}`")
            data_rows = int(cursor.fetchone()[0])
        if loaded != 3 or data_rows != 3:
            raise AssertionError(
                f"DDL+data expected 3 rows; loader={loaded}, table={data_rows}."
            )
        print("PASS: DDL-only=0 rows; DDL+data=3 rows")
    finally:
        try:
            with provider.connection() as connection:
                connection.cursor().execute(
                    f"DROP TABLE IF EXISTS `{database}`.`{table}`"
                )
        finally:
            csv_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
