import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class ImportExecutionContractTest(unittest.TestCase):
    def test_ddl_only_never_executes_load_data(self):
        service = (ROOT / "myapp" / "services" / "import_service.py").read_text()
        routes = (ROOT / "myapp" / "modules" / "import_routes.py").read_text()
        self.assertIn("include_data: bool = True", service)
        self.assertIn("if not include_data:", service)
        self.assertIn("return 0", service)
        self.assertIn('include_data = review.get("sql_mode") == "DDL_DATA"', routes)
        self.assertIn("include_data=include_data", routes)
        self.assertIn("No CSV data was loaded.", routes)

    def test_confirmed_drop_is_applied_by_execution_path(self):
        service = (ROOT / "myapp" / "services" / "import_service.py").read_text()
        routes = (ROOT / "myapp" / "modules" / "import_routes.py").read_text()
        self.assertIn("drop_existing: bool = False", service)
        self.assertIn("DROP TABLE IF EXISTS", service)
        self.assertIn('drop_existing=review.get("drop_existing", False)', routes)

    def test_table_design_options_are_in_generate_sql_section(self):
        review = (ROOT / "myapp" / "templates" / "import_review.html").read_text()
        column_heading = review.index("<h2>Column definition</h2>")
        generate_heading = review.index("<strong>Generate import SQL</strong>")
        partition_option = review.index('id="partition-by-batch"')
        row_id_option = review.index('id="add-row-id"')
        self.assertLess(column_heading, generate_heading)
        self.assertLess(generate_heading, partition_option)
        self.assertLess(generate_heading, row_id_option)

    def test_sql_preview_hides_load_panel_for_ddl_only(self):
        preview = (ROOT / "myapp" / "templates" / "sql_preview.html").read_text()
        self.assertGreaterEqual(preview.count("{% if sql_mode == 'DDL_DATA' %}"), 2)
        self.assertIn("Executes only the reviewed database/table DDL.", preview)
        self.assertIn("Execute DDL only", preview)


if __name__ == "__main__":
    unittest.main()
