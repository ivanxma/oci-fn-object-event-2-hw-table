import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class EventTxPaginationContractTest(unittest.TestCase):
    def test_all_event_tabs_use_server_side_page_controls(self):
        template = (ROOT / "myapp" / "templates" / "event_transactions.html").read_text()
        for prefix, page_size_name in (
            ("recent", "recent_page_size"),
            ("registered", "registered_page_size"),
            ("object", "object_event_page_size"),
            ("logs", "logs_page_size"),
        ):
            self.assertIn(f"'{prefix}'", template)
            self.assertIn(page_size_name, template)
        self.assertIn("data-event-refresh", template)
        self.assertIn("Page {{ page }} of {{ page_count }}", template)

    def test_route_persists_independent_page_sizes(self):
        route = (ROOT / "myapp" / "modules" / "event_tx_routes.py").read_text()
        for value in (
            "recent_page_size",
            "registered_page_size",
            "object_page_size",
            "logs_page_size",
        ):
            self.assertIn(value, route)

    def test_service_pages_recent_and_failed_captures(self):
        service = (ROOT / "myapp" / "services" / "event_tx_service.py").read_text()
        self.assertIn("def recent_events_page(", service)
        self.assertIn("def error_logs_page(", service)
        self.assertIn("offset=(max(page, 1) - 1) * max(page_size, 1)", service)


if __name__ == "__main__":
    unittest.main()
