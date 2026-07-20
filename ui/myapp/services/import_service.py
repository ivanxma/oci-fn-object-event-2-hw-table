"""Reviewed table creation and `LOAD DATA LOCAL INFILE` imports."""

from __future__ import annotations

from pathlib import Path

import mysql.connector

from .csv_service import iter_rows
from .naming import quote_identifier, validate_identifier

ALLOWED_TYPES = {"TINYINT", "INT", "BIGINT", "DECIMAL(38, 10)", "DATE", "DATETIME", "TEXT"}


class ImportExecutionError(ValueError):
    """A database error that is safe to present on the import review page."""


class ImportService:
    def __init__(self, mysql) -> None:
        self.mysql = mysql

    @staticmethod
    def _type(sql_type: str) -> str:
        sql_type = sql_type.strip().upper()
        if sql_type in ALLOWED_TYPES or (sql_type.startswith("VARCHAR(") and sql_type.endswith(")") and sql_type[8:-1].isdigit() and 1 <= int(sql_type[8:-1]) <= 65535):
            return sql_type
        raise ValueError("Use a supported MySQL type (VARCHAR(n), TEXT, BIGINT, INT, DECIMAL(38, 10), DATE, or DATETIME).")

    def ddl(self, database: str, table: str, columns: list[dict], primary_key: list[str], add_row_id: bool, partition_by_batch: bool = False) -> str:
        definitions = []
        if add_row_id:
            definitions.append("`ROW_ID` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT")
        if add_row_id:
            primary_key = ["ROW_ID"]
        elif primary_key:
            names = {column["name"] for column in columns}
            if not set(primary_key).issubset(names):
                raise ValueError("Primary key contains an unknown column.")
        key_columns = set(primary_key)
        for column in columns:
            nullable = column.get("nullable") and column["name"] not in key_columns
            definitions.append(f"{quote_identifier(column['name'], 'column name')} {self._type(column['type'])} {'NULL' if nullable else 'NOT NULL'}")
        if partition_by_batch:
            definitions.append("`batch_num` BIGINT UNSIGNED NOT NULL INVISIBLE")
            primary_key = [*primary_key, "batch_num"]
        if primary_key:
            definitions.append("PRIMARY KEY (" + ", ".join(quote_identifier(key, "primary-key column") for key in primary_key) + ")")
        suffix = " PARTITION BY LIST (`batch_num`) (PARTITION p0 VALUES IN (0))" if partition_by_batch else ""
        return f"CREATE TABLE IF NOT EXISTS {quote_identifier(database, 'database name')}.{quote_identifier(table, 'table name')} (\n  " + ",\n  ".join(definitions) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4" + suffix

    def create_database_statement(self, database: str) -> str:
        return f"CREATE DATABASE IF NOT EXISTS {quote_identifier(database, 'database name')} CHARACTER SET utf8mb4"

    def drop_table_statement(self, database: str, table: str) -> str:
        return f"DROP TABLE IF EXISTS {quote_identifier(database, 'database name')}.{quote_identifier(table, 'table name')}"

    def load_data(self, path: Path, database: str, table: str, columns: list[dict], primary_key: list[str], add_row_id: bool, delimiter: str, *, partition_by_batch: bool = False, create_database: bool = False, drop_table: bool = False, load_rows: bool = True) -> int:
        statement = None
        if load_rows:
            self._validate_primary_key_values(path, columns, primary_key, add_row_id)
            statement = self.load_data_statement(database, table, columns, delimiter, partition_by_batch)
        try:
            if create_database:
                self.mysql.create_database(database)
            with self.mysql.connection() as conn:
                cursor = conn.cursor()
                if drop_table:
                    cursor.execute(self.drop_table_statement(database, table))
                cursor.execute(self.ddl(database, table, columns, primary_key, add_row_id, partition_by_batch))
                if not load_rows:
                    return 0
                assert statement is not None
                cursor.execute(statement, (str(path),))
                return cursor.rowcount
        except mysql.connector.Error as error:
            raise ImportExecutionError(self._database_error_message(error)) from error

    @staticmethod
    def _validate_primary_key_values(path: Path, columns: list[dict], primary_key: list[str], add_row_id: bool) -> None:
        """Ensure every CSV-backed primary-key field has a value before loading."""
        if add_row_id or not primary_key:
            return
        source_by_target = {column["name"]: column.get("source_name", column["name"]) for column in columns}
        for key in primary_key:
            if key not in source_by_target:
                raise ValueError("Primary key contains an unknown column.")
        _, rows = iter_rows(path)
        for row_number, row in enumerate(rows, start=2):
            for key in primary_key:
                if not row[source_by_target[key]].strip():
                    raise ValueError(
                        f"CSV row {row_number} has no value for primary-key column '{key}'. "
                        "Primary-key values must not be null."
                    )

    @staticmethod
    def _database_error_message(error: mysql.connector.Error) -> str:
        """Turn Connector/Python errors into concise guidance without exposing credentials."""
        message = error.msg or "MySQL rejected the import."
        errno = error.errno
        if errno == 1062:
            return f"Import failed: duplicate primary-key value. {message}"
        if errno in {1048, 1263, 1366, 1367}:
            return f"Import failed: a value is missing or incompatible with the selected column type. {message}"
        if errno in {1148, 3948} or "local infile" in message.lower():
            return "Import failed: MySQL server has LOCAL INFILE disabled for this connection. Enable local_infile on the server and reconnect."
        return f"Import failed: MySQL error {errno or 'unknown'}: {message}"

    def load_data_statement(self, database: str, table: str, columns: list[dict], delimiter: str, partition_by_batch: bool = False) -> str:
        validate_identifier(database, "database name")
        validate_identifier(table, "table name")
        if delimiter not in {",", ";", "\t", "|"}:
            raise ValueError("Unsupported CSV delimiter.")
        column_vars = [f"@csv_{index}" for index in range(len(columns))]
        assignments = [f"{quote_identifier(column['name'], 'column name')} = NULLIF({variable}, '')" for column, variable in zip(columns, column_vars)]
        if partition_by_batch:
            assignments.append("`batch_num` = 0")
        escaped_delimiter = delimiter.replace("\\", "\\\\").replace("'", "\\'")
        statement = (
            "LOAD DATA LOCAL INFILE %s INTO TABLE " + f"{quote_identifier(database)}.{quote_identifier(table)} "
            + f"CHARACTER SET utf8mb4 FIELDS TERMINATED BY '{escaped_delimiter}' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\\\\' "
            + "LINES TERMINATED BY '\\n' IGNORE 1 LINES (" + ", ".join(column_vars) + ") SET " + ", ".join(assignments)
        )
        return statement
