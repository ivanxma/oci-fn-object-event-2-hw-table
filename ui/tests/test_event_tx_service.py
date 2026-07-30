import os
import unittest
from contextlib import contextmanager
from unittest.mock import patch

from myapp.services.event_tx_service import EventTransactionService


class Cursor:
    def __init__(self):
        self.executed = []
        self.fetchone_values = iter([(1,), {"total": 0}])
        self.fetchall_values = iter([[], []])

    def execute(self, sql, values=None):
        self.executed.append((sql, values))

    def fetchone(self):
        return next(self.fetchone_values)

    def fetchall(self):
        return next(self.fetchall_values)


class Mysql:
    def __init__(self):
        self.cursor = Cursor()

    @contextmanager
    def connection(self):
        cursor = self.cursor

        class Connection:
            def cursor(self, **_kwargs):
                return cursor

        yield Connection()


class EventTransactionServiceTest(unittest.TestCase):
    def test_stage_snapshot_uses_configured_staging_database(self):
        mysql = Mysql()
        with patch.dict(
            os.environ,
            {"CONTROL_DATABASE": "stream_db", "STAGING_DATABASE": "staging_db"},
        ):
            rows, blocked = EventTransactionService(
                mysql, "stream_data"
            ).stage_tables("testdb", "employees")

        self.assertEqual(rows, [])
        self.assertFalse(blocked)
        stage_query = next(
            values
            for sql, values in mysql.cursor.executed
            if "information_schema.tables" in sql and "table_rows" in sql
        )
        self.assertEqual(stage_query, ("staging_db", "employees\\_stage\\_%"))

    def test_unmapped_capture_is_labeled_explicitly(self):
        class CaptureCursor:
            def execute(self, _sql, _values=None):
                pass

            def fetchall(self):
                return [
                    {
                        "id": 1,
                        "mapping_id": None,
                        "target_database": None,
                        "target_table": None,
                        "processing_mode": None,
                        "payload": {
                            "eventType": "com.oraclecloud.objectstorage.deleteobject",
                            "data": {
                                "resourceName": "retired/file.csv",
                                "additionalDetails": {"bucketName": "bucket"},
                            },
                        },
                        "status": "FAILED",
                        "last_error": "No Resource Mappings entry matches.",
                    }
                ]

        with patch.dict(os.environ, {"CONTROL_DATABASE": "stream_db"}):
            rows = EventTransactionService(
                Mysql(), "stream_data"
            )._capture_rows(CaptureCursor(), limit=10)
        self.assertEqual(rows[0]["processing_mode"], "UNMAPPED")
        self.assertEqual(rows[0]["target_label"], "Unmapped event")


if __name__ == "__main__":
    unittest.main()
