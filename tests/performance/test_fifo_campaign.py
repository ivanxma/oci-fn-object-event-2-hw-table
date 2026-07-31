import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("run_fifo_campaign.py")
SPEC = importlib.util.spec_from_file_location("run_fifo_campaign", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FifoCampaignPureFunctionTest(unittest.TestCase):
    def test_generator_meets_size_and_writes_expected_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sample.csv"
            result = MODULE.generate_csv(path, 1024 * 1024, 1)
            self.assertGreaterEqual(result["bytes"], 1024 * 1024)
            self.assertGreater(result["rows"], 0)
            self.assertEqual(path.read_bytes().count(b"\n") - 1, result["rows"])

    def test_report_does_not_require_or_render_secret_values(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            metrics = directory / "metrics.json"
            report = directory / "report.html"
            metrics.write_text(
                json.dumps(
                    {
                        "run_id": "test",
                        "status": "PASS",
                        "started_utc": "start",
                        "finished_utc": "finish",
                        "total_seconds": 1,
                        "file_size_tests": [],
                        "concurrency_tests": [],
                        "failures": [],
                        "cleanup": {"remaining_managed_resources": 0, "errors": []},
                        "environment": {},
                    }
                )
            )
            MODULE.render_report(metrics, report)
            text = report.read_text()
            self.assertIn("Performance and scalability validation", text)
            self.assertNotIn("DB_SECRET_OCID", text)
            self.assertNotIn("super-secret-value", text)

    def test_heatwave_summary_reports_peaks_and_deltas(self):
        samples = [
            {
                "sampled_utc": "a",
                "threads_connected": 2,
                "threads_running": 1,
                "questions": 10,
                "bytes_received": 100,
                "bytes_sent": 200,
                "innodb_rows_inserted": 5,
                "innodb_buffer_pool_reads": 2,
                "innodb_buffer_pool_read_requests": 100,
                "created_tmp_tables": 1,
                "created_tmp_disk_tables": 0,
                "target_schema_bytes": 10,
            },
            {
                "sampled_utc": "b",
                "threads_connected": 7,
                "threads_running": 4,
                "questions": 30,
                "bytes_received": 400,
                "bytes_sent": 500,
                "innodb_rows_inserted": 15,
                "innodb_buffer_pool_reads": 4,
                "innodb_buffer_pool_read_requests": 200,
                "created_tmp_tables": 5,
                "created_tmp_disk_tables": 1,
                "target_schema_bytes": 100,
            },
        ]
        result = MODULE.summarize_heatwave_samples(samples)
        self.assertEqual(result["peak_threads_connected"], 7)
        self.assertEqual(result["questions_delta"], 20)
        self.assertEqual(result["peak_target_schema_bytes"], 100)
        self.assertEqual(result["buffer_pool_hit_ratio_percent"], 98.0)


if __name__ == "__main__":
    unittest.main()
