from __future__ import annotations

import unittest

from myapp.services.schema_inventory import missing_application_objects


class _Cursor:
    def __init__(self, result_sets):
        self.result_sets = iter(result_sets)
        self.rows = []

    def execute(self, _query, _params):
        self.rows = next(self.result_sets)

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, result_sets):
        self.cursor_value = _Cursor(result_sets)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def cursor(self):
        return self.cursor_value


class _MySQL:
    def __init__(self, result_sets):
        self.result_sets = result_sets

    def connection(self):
        return _Connection(self.result_sets)


class SchemaInventoryTest(unittest.TestCase):
    def test_complete_inventory_has_no_missing_objects(self):
        mysql = _MySQL(
            [
                [("stream_db",), ("stream_data",), ("stream_staging",)],
                [
                    ("stream_db", "deployment_history"),
                    ("stream_db", "object_storage_mappings"),
                    ("stream_db", "source_object_batches"),
                    ("stream_db", "target_batch_sequences"),
                    ("stream_data", "stream_event_tx_log"),
                    ("stream_data", "stream_message_archive_partitions"),
                    ("stream_data", "stream_message_capture"),
                    ("stream_data", "stream_partition_checkpoint"),
                ],
            ]
        )
        self.assertEqual(
            missing_application_objects(
                mysql,
                control_database="stream_db",
                stream_data_database="stream_data",
                staging_database="stream_staging",
            ),
            [],
        )

    def test_missing_inventory_names_schema_and_tables(self):
        mysql = _MySQL(
            [
                [("stream_db",), ("stream_data",)],
                [
                    ("stream_db", "object_storage_mappings"),
                    ("stream_data", "stream_message_capture"),
                ],
            ]
        )
        missing = missing_application_objects(
            mysql,
            control_database="stream_db",
            stream_data_database="stream_data",
            staging_database="stream_staging",
        )
        self.assertIn("stream_staging", missing)
        self.assertIn("stream_db.deployment_history", missing)
        self.assertIn("stream_data.stream_partition_checkpoint", missing)


if __name__ == "__main__":
    unittest.main()
