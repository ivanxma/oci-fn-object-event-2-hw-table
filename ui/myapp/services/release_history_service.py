"""Append-only, secret-free deployment provenance stored in the control DB."""
from __future__ import annotations

from typing import Any

from .naming import quote_identifier


class ReleaseHistoryService:
    def __init__(self, mysql: Any, control_database: str) -> None:
        self.mysql = mysql
        self.table = f"{quote_identifier(control_database, 'control database')}.`deployment_history`"

    def record(self, *, component: str, deployment_name: str, deployment_id: str = "", mapping_id: str = "", release: dict[str, str]) -> None:
        if component not in {"UI", "PROCESSOR"}:
            raise ValueError("Unknown deployment component.")
        with self.mysql.connection() as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"""INSERT INTO {self.table}
                (component,deployment_name,deployment_id,mapping_id,release_version,git_sha,source_branch,build_utc,image_name,image_tag,image_digest,config_schema_version)
                VALUES (%s,%s,%s,NULLIF(%s,''),%s,%s,%s,%s,%s,%s,NULLIF(%s,''),%s)""",
                (component, deployment_name, deployment_id, mapping_id, release["release_version"], release["git_sha"], release["source_branch"], release["build_utc"], release["image_name"], release["image_tag"], release["image_digest"], release["config_schema_version"]),
            )
            connection.commit()

    def recent(self, limit: int = 20) -> list[dict[str, Any]]:
        with self.mysql.connection() as connection:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(f"SELECT * FROM {self.table} ORDER BY recorded_at DESC, id DESC LIMIT %s", (max(1, min(int(limit), 100)),))
            return list(cursor.fetchall())
