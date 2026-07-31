from __future__ import annotations

import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "loader_core"))
try:
    import mysql.connector  # type: ignore[import-not-found]
except ModuleNotFoundError:
    mysql_module = types.ModuleType("mysql")
    connector_module = types.ModuleType("mysql.connector")
    connector_module.connect = lambda **_kwargs: None
    mysql_module.connector = connector_module
    sys.modules.update({"mysql": mysql_module, "mysql.connector": connector_module})

import partition_loader


class _Cursor:
    def __init__(self):
        self.executed = []
        self.rows = iter(
            [
                {
                    "id": 11,
                    "mapping_id": 3,
                    "target_database": "testdb",
                    "target_table": "employees01",
                    "batch_num": 7,
                    "lifecycle_state": "ACTIVE",
                },
                {"row_count": 50},
            ]
        )

    def execute(self, statement, params=None):
        self.executed.append((statement, params))

    def fetchone(self):
        return next(self.rows)


class _Connection:
    def __init__(self, cursor):
        self.cursor_value = cursor

    def cursor(self, **_kwargs):
        return self.cursor_value


class _Context:
    def __init__(self, connection):
        self.connection_value = connection

    def __enter__(self):
        return self.connection_value

    def __exit__(self, *_args):
        return None


class _Database:
    def __init__(self, cursor):
        self.cursor = cursor

    def connection(self):
        return _Context(_Connection(self.cursor))


class PartitionLoaderDeleteTest(unittest.TestCase):
    def test_dictionary_cursor_count_is_used_before_partition_drop(self):
        event = {
            "eventType": "com.oraclecloud.objectstorage.deleteobject",
            "data": {
                "compartmentName": "HWDemo",
                "resourceName": "myfolder/emp/emp.csv",
                "additionalDetails": {"bucketName": "ivanma-bucket"},
            },
        }
        mapping = {
            "id": 3,
            "target_database": "testdb",
            "target_table": "employees01",
        }
        cursor = _Cursor()
        with tempfile.TemporaryDirectory() as directory:
            event_path = Path(directory) / "event.json"
            event_path.write_text(json.dumps(event), encoding="utf-8")
            with patch.dict("os.environ", {"CONTROL_DATABASE": "stream_db"}), patch.object(
                partition_loader, "Database", return_value=_Database(cursor)
            ), patch.object(
                partition_loader, "ensure_control_tables"
            ), patch.object(
                partition_loader, "resolve_mapping", return_value=mapping
            ), patch.object(
                partition_loader, "target_definition", return_value=["employee_id"]
            ), patch.object(
                partition_loader, "source_key", return_value=b"source-key"
            ):
                result = partition_loader.run_delete(event_path)

        self.assertEqual(result["rows"], 50)
        self.assertTrue(
            any("DROP PARTITION `p_batch_7`" in statement for statement, _ in cursor.executed)
        )
        self.assertTrue(
            any("lifecycle_state = 'DELETED'" in statement for statement, _ in cursor.executed)
        )


if __name__ == "__main__":
    unittest.main()
