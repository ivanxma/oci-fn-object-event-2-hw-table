from __future__ import annotations

import unittest
from pathlib import Path


UI_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = UI_ROOT / "myapp" / "templates"


class UIConsistencySourceTest(unittest.TestCase):
    def test_oci_rules_tab_has_one_stable_label(self) -> None:
        source = (TEMPLATES / "mappings.html").read_text(encoding="utf-8")
        self.assertIn(">OCI Rules{% if active_tab == 'rules' %}", source)
        self.assertNotIn("else 'OCI'", source)
        self.assertNotIn("OCI Rules OCI", source)

    def test_registered_and_object_event_downloads_share_icon(self) -> None:
        source = (TEMPLATES / "event_transactions.html").read_text(encoding="utf-8")
        download_path = 'd="M12 3v11m0 0 4-4m-4 4-4-4M5 17v3h14v-3"'
        self.assertGreaterEqual(source.count(download_path), 2)
        self.assertNotIn("⇩", source)

    def test_event_tables_use_shared_value_dialog(self) -> None:
        base = (TEMPLATES / "base.html").read_text(encoding="utf-8")
        event_page = (TEMPLATES / "event_transactions.html").read_text(encoding="utf-8")
        self.assertIn("window.tableValueDialog = { show: showValue }", base)
        self.assertNotIn("object-event-value-dialog", event_page)
        self.assertIn('data-column="Execution mode"', event_page)
        self.assertIn('data-column="Lifecycle"', event_page)
        self.assertIn("prepareRegisteredCells", event_page)
        self.assertIn("window.tableValueDialog?.show", event_page)

    def test_server_tables_default_to_show_ten_with_refresh(self) -> None:
        source = (TEMPLATES / "event_transactions.html").read_text(encoding="utf-8")
        self.assertGreaterEqual(source.count("server-table-controls"), 3)
        self.assertIn('name="object_page_size"', source)
        self.assertIn('value="{{ object_event_page_size }}"', source)
        base = (TEMPLATES / "base.html").read_text(encoding="utf-8")
        self.assertIn('value="10" aria-label="Rows to show"', base)
        self.assertIn("client-table-controls", base)


if __name__ == "__main__":
    unittest.main()
