"""Read-only readiness check for application-owned database schemas."""

from __future__ import annotations

from .naming import validate_identifier


CONTROL_TABLES = frozenset(
    {
        "deployment_history",
        "object_storage_mappings",
        "source_object_batches",
        "target_batch_sequences",
    }
)
DURABLE_TABLES = frozenset(
    {
        "stream_event_tx_log",
        "stream_message_archive_partitions",
        "stream_message_capture",
        "stream_partition_checkpoint",
    }
)


def missing_application_objects(
    mysql,
    *,
    control_database: str,
    stream_data_database: str,
    staging_database: str,
) -> list[str]:
    """Return missing application-owned schemas/tables without changing data."""
    control = validate_identifier(control_database, "control database")
    durable = validate_identifier(stream_data_database, "stream data database")
    staging = validate_identifier(staging_database, "staging database")
    expected = {
        control: CONTROL_TABLES,
        durable: DURABLE_TABLES,
    }
    missing: list[str] = []
    with mysql.connection() as connection:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name IN (%s,%s,%s)",
            (control, durable, staging),
        )
        schemas = {str(row[0]) for row in cursor.fetchall()}
        for database in (control, durable, staging):
            if database not in schemas:
                missing.append(database)
        cursor.execute(
            "SELECT table_schema,table_name FROM information_schema.tables "
            "WHERE table_schema IN (%s,%s)",
            (control, durable),
        )
        tables: dict[str, set[str]] = {control: set(), durable: set()}
        for database, table in cursor.fetchall():
            tables.setdefault(str(database), set()).add(str(table))
        for database, required in expected.items():
            missing.extend(
                f"{database}.{table}"
                for table in sorted(required - tables.get(database, set()))
            )
    return missing
