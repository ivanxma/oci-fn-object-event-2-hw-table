from pathlib import Path
import unittest


ROUTE = (
    Path(__file__).resolve().parents[1]
    / "myapp"
    / "modules"
    / "flow_routes.py"
)
TEMPLATE = Path(__file__).resolve().parents[1] / "myapp" / "templates" / "flow.html"


class FlowPerformanceContractTest(unittest.TestCase):
    def test_oci_inventory_calls_are_concurrent(self):
        source = ROUTE.read_text(encoding="utf-8")
        self.assertIn("ThreadPoolExecutor(max_workers=len(loaders))", source)
        self.assertIn('executor.submit(loader)', source)

    def test_deleted_processors_are_not_expanded(self):
        source = ROUTE.read_text(encoding="utf-8")
        self.assertIn('list_deployments(state_filter="ACTIVE")', source)
        self.assertNotIn("orchestration.list_deployments()", source)

    def test_nested_details_are_limited_to_visible_mappings(self):
        source = ROUTE.read_text(encoding="utf-8")
        self.assertIn("mapping_ids =", source)
        self.assertIn("relevant =", source)
        self.assertIn("min(8, len(relevant))", source)

    def test_initial_screen_uses_background_topology_endpoint(self):
        route = ROUTE.read_text(encoding="utf-8")
        template = TEMPLATE.read_text(encoding="utf-8")
        self.assertIn('@flow_bp.get("/topology")', route)
        self.assertIn("initial_flows", route)
        self.assertGreaterEqual(template.lower().count("collecting info…"), 6)
        self.assertIn("url_for('flow.topology')", template)
        self.assertIn('fetch("', template)

    def test_empty_mapping_state_is_explicit_and_skips_oci(self):
        route = ROUTE.read_text(encoding="utf-8")
        template = TEMPLATE.read_text(encoding="utf-8")
        self.assertIn("if not mappings:", route)
        self.assertIn("return [], []", route)
        self.assertGreaterEqual(template.count("No mapping configured."), 5)
        self.assertIn("mappings.create_mapping", template)


if __name__ == "__main__":
    unittest.main()
