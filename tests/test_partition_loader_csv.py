from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "function"))

from partition_loader import csv_batches


def test_csv_headers_match_target_columns_case_insensitively(tmp_path):
    source = tmp_path / "employees.csv"
    source.write_text("EMPLOYEE_ID,FIRST_NAME\n1,Jane\n", encoding="utf-8")

    assert list(csv_batches(source, ["employee_id", "first_name"], 100)) == [[("1", "Jane")]]


def test_csv_case_insensitive_duplicate_headers_are_rejected(tmp_path):
    source = tmp_path / "duplicate.csv"
    source.write_text("ID,id\n1,2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate column names"):
        list(csv_batches(source, ["id"], 100))
