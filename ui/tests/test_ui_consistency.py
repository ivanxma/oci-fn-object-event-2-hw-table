from __future__ import annotations

import unittest
from pathlib import Path


TEMPLATES = Path(__file__).resolve().parents[1] / "myapp" / "templates"
STYLESHEET = Path(__file__).resolve().parents[1] / "myapp" / "static" / "app.css"


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

    def test_registered_table_has_staging_cleanup_and_data_dialog(self) -> None:
        source = (TEMPLATES / "event_transactions.html").read_text(encoding="utf-8")
        self.assertIn('id="registered-stage-tables"', source)
        self.assertIn("cleanup_stage_tables", source)
        self.assertIn("stage_cleanup_blocked", source)
        self.assertIn('id="view-registered-table"', source)
        self.assertIn("registered_table_content", source)
        self.assertIn("dialog.showModal()", source)

    def test_mapping_rule_tab_has_one_stable_label(self) -> None:
        source = (TEMPLATES / "mappings.html").read_text(encoding="utf-8")
        self.assertIn(">OCI Rules{% if active_tab == 'rules' %}", source)
        self.assertNotIn("else 'OCI'", source)

    def test_settings_actions_align_with_their_fields(self) -> None:
        source = (TEMPLATES / "settings.html").read_text(encoding="utf-8")
        stylesheet = STYLESHEET.read_text(encoding="utf-8")
        self.assertEqual(source.count("form-actions field-aligned-actions"), 2)
        self.assertIn("Save configuration", source)
        self.assertIn("Create / update stream user", source)
        self.assertIn(".field-aligned-actions{align-items:flex-start;padding-top:21px}", stylesheet)
        self.assertIn(
            "@media(max-width:760px){.field-aligned-actions{padding-top:0}}",
            stylesheet,
        )


if __name__ == "__main__":
    unittest.main()
