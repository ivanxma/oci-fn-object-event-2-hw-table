from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from myapp.app import create_app
from myapp.services.csv_service import inspect_csv
from myapp.services.import_service import ImportService


class _Cursor:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object]] = []
        self.rowcount = 2

    def execute(self, statement: str, parameters=None) -> None:
        self.statements.append((statement, parameters))


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _Cursor:
        return self._cursor


class _MySQL:
    def __init__(self) -> None:
        self.cursor = _Cursor()

    @contextmanager
    def connection(self):
        yield _Connection(self.cursor)

    def create_database(self, _database: str) -> None:
        raise AssertionError("Database creation was not requested")


class ImportDropRecreateTest(unittest.TestCase):
    def test_ddl_controls_are_aligned_below_the_column_definition(self) -> None:
        source = (Path(__file__).resolve().parents[1] / "myapp" / "templates" / "import_review.html").read_text(encoding="utf-8")
        self.assertLess(source.index("definition-table"), source.index("ddl-control-options"))
        self.assertLess(source.index("ddl-control-options"), source.index("Execution action"))
        self.assertIn("OCI event-ready table", source)
        self.assertIn("Add <code>ROW_ID</code>", source)
        self.assertIn("Drop the existing target table", source)

    def test_type_inference_uses_the_full_csv_not_only_preview_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "employees.csv"
            rows = ["employee_id,last_name"]
            rows.extend(f"{index},Short" for index in range(1, 29))
            rows.append("29,LongerName")
            path.write_text("\n".join(rows) + "\n", encoding="utf-8")
            result = inspect_csv(path)
        self.assertEqual(len(result["preview"]), 25)
        self.assertEqual(result["types"]["last_name"], "VARCHAR(10)")

    def test_drop_runs_before_create_and_load(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "employees.csv"
            path.write_text("employee_id,last_name\n1,Longname\n", encoding="utf-8")
            mysql = _MySQL()
            count = ImportService(mysql).load_data(
                path,
                "fntestdb",
                "employees",
                [
                    {"source_name": "employee_id", "name": "employee_id", "type": "BIGINT"},
                    {"source_name": "last_name", "name": "last_name", "type": "VARCHAR(64)"},
                ],
                [],
                True,
                ",",
                drop_table=True,
            )
        statements = [statement for statement, _parameters in mysql.cursor.statements]
        self.assertEqual(count, 2)
        self.assertTrue(statements[0].startswith("DROP TABLE IF EXISTS `fntestdb`.`employees`"))
        self.assertTrue(statements[1].startswith("CREATE TABLE IF NOT EXISTS `fntestdb`.`employees`"))
        self.assertTrue(statements[2].startswith("LOAD DATA LOCAL INFILE"))

    def test_ddl_only_does_not_validate_or_load_csv_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "employees.csv"
            path.write_text("employee_id,last_name\n,Longname\n", encoding="utf-8")
            mysql = _MySQL()
            count = ImportService(mysql).load_data(
                path,
                "fntestdb",
                "employees",
                [
                    {"source_name": "employee_id", "name": "employee_id", "type": "BIGINT"},
                    {"source_name": "last_name", "name": "last_name", "type": "VARCHAR(64)"},
                ],
                ["employee_id"],
                False,
                ",",
                drop_table=True,
                load_rows=False,
            )
        statements = [statement for statement, _parameters in mysql.cursor.statements]
        self.assertEqual(count, 0)
        self.assertEqual(len(statements), 2)
        self.assertTrue(statements[0].startswith("DROP TABLE IF EXISTS"))
        self.assertTrue(statements[1].startswith("CREATE TABLE IF NOT EXISTS"))

    def test_ddl_only_preview_omits_load_statement(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            csv_path = root / "employees.csv"
            csv_path.write_text("employee_id,last_name\n1,Longname\n", encoding="utf-8")
            app = create_app(
                {
                    "TESTING": True,
                    "SECRET_KEY": "test-only-key",
                    "SESSION_COOKIE_SECURE": False,
                    "PROFILE_STORE": str(root / "profiles.json"),
                    "UPLOAD_FOLDER": str(root / "uploads"),
                    "SSH_KEY_FOLDER": str(root / "keys"),
                }
            )
            client = app.test_client()
            store = app.extensions["session_store"]
            connection_id = store.create(
                {"name": "test", "mode": "direct", "host": "db", "port": 3306},
                "admin",
                "not-rendered",
            )
            store.get(connection_id).imports["job"] = {
                "path": str(csv_path),
                "database": "fntestdb",
                "table": "employees",
                "create_database": False,
                "headers": ["employee_id", "last_name"],
                "delimiter": ",",
                "preview": [{"employee_id": "1", "last_name": "Longname"}],
                "types": {"employee_id": "BIGINT", "last_name": "VARCHAR(8)"},
            }
            with client.session_transaction() as browser_session:
                browser_session["connection_id"] = connection_id

            definition = {
                "column_name_0": "employee_id",
                "column_type_0": "BIGINT",
                "column_name_1": "last_name",
                "column_type_1": "VARCHAR(64)",
                "add_row_id": "on",
                "import_action": "DDL_ONLY",
            }
            with patch("myapp.modules.common.MySQLService.health_check", return_value=None):
                preview = client.post("/imports/job/prepare", data=definition)
            self.assertEqual(preview.status_code, 200)
            self.assertIn(b"Complete generated script", preview.data)
            self.assertNotIn(b"Load CSV data", preview.data)
            self.assertIn(b"Only the reviewed DDL will run", preview.data)
            self.assertIn(b"Confirm and run DDL", preview.data)

            with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
                "myapp.modules.import_routes.ImportService.load_data", return_value=0
            ) as load_data:
                accepted = client.post("/imports/job/load", data={})
            self.assertEqual(accepted.status_code, 302)
            self.assertFalse(load_data.call_args.kwargs["load_rows"])

    def test_drop_preview_and_server_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            csv_path = root / "employees.csv"
            csv_path.write_text("employee_id,last_name\n1,Longname\n", encoding="utf-8")
            app = create_app(
                {
                    "TESTING": True,
                    "SECRET_KEY": "test-only-key",
                    "SESSION_COOKIE_SECURE": False,
                    "PROFILE_STORE": str(root / "profiles.json"),
                    "UPLOAD_FOLDER": str(root / "uploads"),
                    "SSH_KEY_FOLDER": str(root / "keys"),
                }
            )
            client = app.test_client()
            store = app.extensions["session_store"]
            connection_id = store.create(
                {"name": "test", "mode": "direct", "host": "db", "port": 3306},
                "admin",
                "not-rendered",
            )
            store.get(connection_id).imports["job"] = {
                "path": str(csv_path),
                "database": "fntestdb",
                "table": "employees",
                "create_database": False,
                "headers": ["employee_id", "last_name"],
                "delimiter": ",",
                "preview": [{"employee_id": "1", "last_name": "Longname"}],
                "types": {"employee_id": "BIGINT", "last_name": "VARCHAR(8)"},
            }
            with client.session_transaction() as browser_session:
                browser_session["connection_id"] = connection_id

            definition = {
                "column_name_0": "employee_id",
                "column_type_0": "BIGINT",
                "column_name_1": "last_name",
                "column_type_1": "VARCHAR(64)",
                "add_row_id": "on",
                "drop_table": "on",
            }
            with patch("myapp.modules.common.MySQLService.health_check", return_value=None):
                preview = client.post("/imports/job/prepare", data=definition)
            self.assertEqual(preview.status_code, 200)
            self.assertIn(b"Complete generated script", preview.data)
            self.assertIn(b"DROP TABLE IF EXISTS", preview.data)
            self.assertLess(preview.data.index(b"DROP TABLE IF EXISTS"), preview.data.index(b"CREATE TABLE IF NOT EXISTS"))
            self.assertLess(preview.data.index(b"CREATE TABLE IF NOT EXISTS"), preview.data.index(b"LOAD DATA LOCAL INFILE"))
            self.assertIn(b'name="confirm_drop_table" required', preview.data)

            with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
                "myapp.modules.import_routes.ImportService.load_data"
            ) as load_data:
                rejected = client.post("/imports/job/load", data={})
            self.assertEqual(rejected.status_code, 302)
            load_data.assert_not_called()

            with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
                "myapp.modules.import_routes.ImportService.load_data", return_value=1
            ) as load_data:
                accepted = client.post(
                    "/imports/job/load", data={"confirm_drop_table": "on"}
                )
            self.assertEqual(accepted.status_code, 302)
            self.assertTrue(load_data.call_args.kwargs["drop_table"])


if __name__ == "__main__":
    unittest.main()
