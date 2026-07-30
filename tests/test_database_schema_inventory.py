from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_stream_data_tables_are_defined_in_external_sql() -> None:
    capture_sql = (ROOT / "processor" / "sql" / "init_stream_capture.sql").read_text(encoding="utf-8")
    archive_sql = (ROOT / "ui" / "myapp" / "sql" / "init_stream_message_archive.sql").read_text(encoding="utf-8")
    partition_sql = (ROOT / "ui" / "myapp" / "sql" / "init_stream_message_archive_partition.sql").read_text(encoding="utf-8")
    for table in ("stream_message_capture", "stream_partition_checkpoint", "stream_event_tx_log"):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in capture_sql
    assert "CREATE TABLE IF NOT EXISTS __ARCHIVE_REGISTRY__" in archive_sql
    assert "CREATE TABLE IF NOT EXISTS __ARCHIVE_TABLE__" in partition_sql


def test_control_table_names_are_not_created_inline_by_loader_python() -> None:
    source = (ROOT / "loader_core" / "partition_loader.py").read_text(encoding="utf-8")
    for table in ("object_storage_mappings", "target_batch_sequences", "source_object_batches"):
        assert f"CREATE TABLE IF NOT EXISTS {table}" not in source
