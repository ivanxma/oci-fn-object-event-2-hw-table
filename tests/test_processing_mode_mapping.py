from pathlib import Path
import unittest


class ProcessingModeMappingTest(unittest.TestCase):
    def test_processor_loader_uses_stream_processing_modes(self):
        root = Path(__file__).parents[1]
        source = (root / "loader_core" / "event_processor.py").read_text(encoding="utf-8")
        loader = (root / "loader_core" / "partition_loader.py").read_text(encoding="utf-8")

        self.assertNotIn("invocation_mode", source)
        self.assertNotIn("invocation_mode", loader)
        self.assertNotIn("event_tx_log", loader)
        self.assertNotIn("event_errors", loader)
        self.assertNotIn("object_event", loader)
        self.assertIn("processing_mode", loader)

    def test_object_delete_drops_its_batch_partition(self):
        root = Path(__file__).parents[1]
        loader = (root / "loader_core" / "partition_loader.py").read_text(encoding="utf-8")

        self.assertIn("DROP PARTITION", loader)
        self.assertNotIn("TRUNCATE PARTITION", loader)


if __name__ == "__main__":
    unittest.main()
