from __future__ import annotations

import unittest
from pathlib import Path


TEMPLATES = Path(__file__).resolve().parents[1] / "myapp" / "templates"


class UIConsistencySourceTest(unittest.TestCase):
    def test_event_tx_is_streaming_only(self) -> None:
        source = (TEMPLATES / "event_transactions.html").read_text(encoding="utf-8")
        self.assertIn("Processor-owned durable stream captures", source)
        self.assertIn("Processing mode is FIFO or Parallel", source)
        self.assertNotIn("event_tx_log", source)
        self.assertNotIn("object_event</code>", source)

    def test_event_tx_has_true_server_tabs(self) -> None:
        source = (TEMPLATES / "event_transactions.html").read_text(encoding="utf-8")
        self.assertEqual(source.count('data-server-tab="'), 4)
        self.assertIn('data-server-tab="object-events"', source)
        self.assertIn("window.location.assign(url)", source)

    def test_mapping_rule_tab_has_one_stable_label(self) -> None:
        source = (TEMPLATES / "mappings.html").read_text(encoding="utf-8")
        self.assertIn(">OCI Rules{% if active_tab == 'rules' %}", source)
        self.assertNotIn("else 'OCI'", source)


if __name__ == "__main__":
    unittest.main()
