from __future__ import annotations

import importlib.util
import os
import sys
import types
import unittest
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import patch


def _load_policy_module():
    stub = types.ModuleType("partition_loader")
    stub.Database = object
    stub.control_database = lambda: "control"
    stub.control_table = lambda name: f"`control`.`{name}`"
    stub.quote_identifier = lambda value, _label: f"`{value}`"
    previous = sys.modules.get("partition_loader")
    sys.modules["partition_loader"] = stub
    try:
        path = Path(__file__).resolve().parents[1] / "function" / "work_queue.py"
        spec = importlib.util.spec_from_file_location("work_queue_policy_test", path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        return module
    finally:
        if previous is None:
            sys.modules.pop("partition_loader", None)
        else:
            sys.modules["partition_loader"] = previous


QUEUE = _load_policy_module()


class WorkQueuePolicyTest(unittest.TestCase):
    def test_table_binding_is_safe_default(self):
        mapping = {"id": 7, "target_database": "fntestdb", "target_table": "employees"}
        self.assertEqual(QUEUE.queue_binding(mapping), ("TABLE", "table:fntestdb.employees"))

    def test_mapping_binding_is_explicit(self):
        mapping = {"id": 7, "target_database": "fntestdb", "target_table": "employees", "queue_scope": "MAPPING"}
        self.assertEqual(QUEUE.queue_binding(mapping), ("MAPPING", "mapping:7"))

    def test_sync_and_detached_use_different_safety_reserves(self):
        entry = {"event_action": "CREATE", "object_size_bytes": None}
        environment = {
            "QUEUE_UNKNOWN_JOB_SECONDS": "60",
            "QUEUE_SYNC_RESERVE_SECONDS": "15",
            "QUEUE_SYNC_MINIMUM_START_SECONDS": "15",
            "QUEUE_SHUTDOWN_RESERVE_SECONDS": "120",
            "QUEUE_MINIMUM_START_SECONDS": "180",
            "QUEUE_PREDICTION_SAFETY_FACTOR": "1.35",
        }
        with patch.dict(os.environ, environment, clear=False):
            self.assertTrue(QUEUE.has_start_budget(entry, 299, "SYNC"))
            self.assertFalse(QUEUE.has_start_budget(entry, 299, "DETACHED"))
            self.assertTrue(QUEUE.has_start_budget(entry, 3599, "DETACHED"))

    def test_large_object_prediction_can_exceed_detached_budget(self):
        entry = {"event_action": "CREATE", "object_size_bytes": 20 * 1024 * 1024 * 1024}
        with patch.dict(os.environ, {"QUEUE_EXPECTED_BYTES_PER_SECOND": str(4 * 1024 * 1024)}, clear=False):
            self.assertFalse(QUEUE.has_start_budget(entry, 3600, "DETACHED"))

    def test_claim_next_reads_named_result_from_dictionary_cursor(self):
        entry = {
            "id": 11,
            "status": "PENDING",
            "attempt_count": 0,
            "received_at": datetime(2026, 7, 19, 12, 0, 0),
            "available_at": datetime(2026, 7, 19, 12, 0, 0),
            "event_time": datetime(2026, 7, 19, 12, 0, 0),
        }

        class Cursor:
            rowcount = 1

            def execute(self, sql, _params=()):
                if "SELECT owner_token" in sql:
                    self.result = {"owner_token": "owner", "last_completed_event_time": None, "last_completed_queue_id": None}
                elif "SELECT * FROM" in sql:
                    self.result = dict(entry)
                elif "AS is_ready" in sql:
                    self.result = {"is_ready": 1}

            def fetchone(self):
                return self.result

        class Connection:
            def cursor(self, **_kwargs):
                return Cursor()

        class Database:
            @contextmanager
            def connection(self):
                yield Connection()

        claimed, reason = QUEUE.claim_next(Database(), "table:fntestdb.employees", "owner", 90, 30)
        self.assertEqual(reason, "claimed")
        self.assertEqual(claimed["status"], "RUNNING")

    def test_unordered_late_entry_can_be_claimed(self):
        entry = {
            "id": 12,
            "status": "PENDING",
            "attempt_count": 0,
            "received_at": datetime(2026, 7, 19, 12, 0, 0),
            "available_at": datetime(2026, 7, 19, 12, 0, 0),
            "event_time": datetime(2026, 7, 19, 11, 0, 0),
            "order_required": False,
            "reorder_grace_seconds": 0,
        }

        class Cursor:
            rowcount = 1

            def execute(self, sql, _params=()):
                if "SELECT owner_token" in sql:
                    self.result = {
                        "owner_token": "owner",
                        "last_completed_event_time": datetime(2026, 7, 19, 12, 0, 0),
                        "last_completed_queue_id": 11,
                    }
                elif "SELECT * FROM" in sql:
                    self.result = dict(entry)
                elif "AS is_ready" in sql:
                    self.result = {"is_ready": 1}

            def fetchone(self):
                return self.result

        class Connection:
            def cursor(self, **_kwargs):
                return Cursor()

        class Database:
            @contextmanager
            def connection(self):
                yield Connection()

        claimed, reason = QUEUE.claim_next(Database(), "table:fntestdb.employees", "owner", 90, 30)
        self.assertEqual(reason, "claimed")
        self.assertEqual(claimed["id"], 12)

    def test_complete_entry_keeps_lane_watermark_monotonic(self):
        source = (Path(__file__).resolve().parents[1] / "function" / "work_queue.py").read_text(encoding="utf-8")
        self.assertIn("THEN %s ELSE last_completed_event_time END", source)


if __name__ == "__main__":
    unittest.main()
