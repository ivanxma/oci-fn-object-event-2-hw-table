"""Connector/Python access using credentials retained only in server memory."""

from __future__ import annotations

from contextlib import contextmanager

import mysql.connector
from mysql.connector.constants import ClientFlag

from .naming import quote_identifier


class MySQLService:
    def __init__(self, connection_session) -> None:
        self.state = connection_session

    def _args(self) -> dict:
        profile = self.state.profile
        host, port = profile["host"], profile["port"]
        if self.state.tunnel:
            host, port = "127.0.0.1", self.state.tunnel.local_bind_port
        return {
            "host": host, "port": port, "user": self.state.username, "password": self.state.password,
            "autocommit": False, "allow_local_infile": True, "ssl_disabled": False,
            "client_flags": [ClientFlag.SSL],
        }

    @contextmanager
    def connection(self):
        conn = mysql.connector.connect(**self._args())
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def health_check(self) -> None:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()

    def list_databases(self) -> list[str]:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SHOW DATABASES")
            return [row[0] for row in cursor.fetchall() if row[0] not in {"information_schema", "mysql", "performance_schema", "sys"}]

    def create_database(self, database: str) -> None:
        with self.connection() as conn:
            conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {quote_identifier(database, 'database name')} CHARACTER SET utf8mb4")
