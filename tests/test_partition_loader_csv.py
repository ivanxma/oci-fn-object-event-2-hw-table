from pathlib import Path
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / "loader_core"))

from partition_loader import csv_batches, staging_database


class PartitionLoaderCsvTest(unittest.TestCase):
    def source(self, content: str) -> Path:
        directory = Path(tempfile.mkdtemp(prefix="partition-loader-csv-"))
        self.addCleanup(lambda: shutil.rmtree(directory))
        source = directory / "employees.csv"
        source.write_text(content, encoding="utf-8")
        return source

    def test_headers_match_target_columns_case_insensitively(self):
        source = self.source("EMPLOYEE_ID,FIRST_NAME\n1,Jane\n")
        self.assertEqual(list(csv_batches(source, ["employee_id", "first_name"], 100)), [[("1", "Jane")]])

    def test_lone_dash_is_bound_as_null(self):
        source = self.source("EMPLOYEE_ID,MANAGER_ID\n1,-\n")
        self.assertEqual(list(csv_batches(source, ["employee_id", "manager_id"], 100)), [[("1", None)]])

    def test_empty_field_is_bound_as_null(self):
        source = self.source("EMPLOYEE_ID,MANAGER_ID\n1,\n")
        self.assertEqual(list(csv_batches(source, ["employee_id", "manager_id"], 100)), [[("1", None)]])

    def test_case_insensitive_duplicate_headers_are_rejected(self):
        source = self.source("ID,id\n1,2\n")
        with self.assertRaisesRegex(ValueError, "duplicate column names"):
            list(csv_batches(source, ["id"], 100))

    def test_staging_database_uses_validated_dedicated_schema(self):
        import os
        from unittest.mock import patch

        with patch.dict(os.environ, {"STAGING_DATABASE": "staging_db"}):
            self.assertEqual(staging_database(), "staging_db")
        with patch.dict(os.environ, {"STAGING_DATABASE": "invalid-name"}):
            with self.assertRaisesRegex(ValueError, "Invalid staging database"):
                staging_database()
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "STAGING_DATABASE is required"):
                staging_database()

if __name__ == "__main__":
    unittest.main()
