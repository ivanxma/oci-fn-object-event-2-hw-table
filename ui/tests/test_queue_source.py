from __future__ import annotations

import unittest
from pathlib import Path


UI_ROOT = Path(__file__).resolve().parents[1]


class QueueSourceTest(unittest.TestCase):
    def test_queue_is_registered_and_navigable(self):
        app = (UI_ROOT / "myapp" / "app.py").read_text(encoding="utf-8")
        base = (UI_ROOT / "myapp" / "templates" / "base.html").read_text(encoding="utf-8")
        self.assertIn("app.register_blueprint(queue_bp)", app)
        self.assertIn("Queue</a>", base)

    def test_dashboard_exposes_required_controlled_actions(self):
        source = (UI_ROOT / "myapp" / "templates" / "queue_dashboard.html").read_text(encoding="utf-8")
        for endpoint in ("queue.edit_selected", "queue.retry_selected", "queue.cancel_selected", "queue.wake"):
            self.assertIn(endpoint, source)
        self.assertIn("Create queue entry", source)
        self.assertIn("Queue bindings and worker leases", source)

    def test_mapping_form_has_explicit_queue_scope(self):
        source = (UI_ROOT / "myapp" / "templates" / "mapping_form.html").read_text(encoding="utf-8")
        self.assertIn('name="queue_scope"', source)
        self.assertIn('value="TABLE"', source)
        self.assertIn('value="MAPPING"', source)


if __name__ == "__main__":
    unittest.main()

