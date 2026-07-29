from __future__ import annotations

import tempfile
import unittest
import sys
import types
import io
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

try:
    import mysql.connector  # type: ignore[import-not-found]
except ModuleNotFoundError:
    mysql_module = types.ModuleType("mysql")
    connector_module = types.ModuleType("mysql.connector")
    constants_module = types.ModuleType("mysql.connector.constants")
    connector_module.Error = type("Error", (Exception,), {})
    connector_module.connect = lambda **_kwargs: None
    constants_module.ClientFlag = type("ClientFlag", (), {"SSL": 1})
    mysql_module.connector = connector_module
    sys.modules.update(
        {
            "mysql": mysql_module,
            "mysql.connector": connector_module,
            "mysql.connector.constants": constants_module,
        }
    )

from myapp.app import create_app
from myapp.services.event_rule_service import EventRuleRecord
from myapp.services.object_storage_upload_service import ObjectRecord
from myapp.services.streaming_service import StreamMessage, StreamRecord


MAPPING = {
    "id": 7,
    "compartment_name": "Operations",
    "bucket_name": "test-bucket",
    "resource_name_pattern": "stream-folder/emp*.csv",
    "target_database": "fntestdb",
    "target_table": "employees",
    "worker_threads": 4,
    "event_rule_id": "ocid1.eventrule.test",
}


class MappingPageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.app = create_app(
            {
                "TESTING": True,
                "SECRET_KEY": "test-only-key",
                "SESSION_COOKIE_SECURE": False,
                "PROFILE_STORE": str(root / "profiles.json"),
                "UPLOAD_FOLDER": str(root / "uploads"),
                "SSH_KEY_FOLDER": str(root / "keys"),
                "OCI_COMPARTMENT_ID": "ocid1.compartment.test",
                "OCI_REGION": "uk-london-1",
                "OCI_EVENT_RULE_MANAGEMENT_ENABLED": True,
            }
        )
        self.client = self.app.test_client()
        profile = {"name": "test", "mode": "direct", "host": "db", "port": 3306}
        connection_id = self.app.extensions["session_store"].create(profile, "admin", "not-rendered")
        with self.client.session_transaction() as browser_session:
            browser_session["connection_id"] = connection_id

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _base_patches(self):
        return (
            patch("myapp.modules.common.MySQLService.health_check", return_value=None),
            patch("myapp.modules.mapping_routes.MappingService.list_mappings", return_value=[MAPPING]),
        )

    def test_mapping_tab_renders_rule_ownership_without_mapping_timeout(self) -> None:
        health, mappings = self._base_patches()
        with health, mappings, patch(
            "myapp.modules.mapping_routes.MappingService.list_target_databases", return_value=["fntestdb"]
        ), patch("myapp.modules.mapping_routes.MappingService.list_target_tables", return_value=["employees"]):
            response = self.client.get("/mappings/?tab=mappings")
            form_response = self.client.get("/mappings/new")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Create and manage an OCI Events rule", form_response.data)
        self.assertIn(b"Managed", response.data)
        self.assertNotIn(b"Timeout</th>", response.data)
        self.assertIn(b"/mappings/edit-selected", response.data)
        self.assertIn(b"/mappings/delete-selected", response.data)
        self.assertNotIn(b"Delete this mapping and its managed", response.data)

    def test_rules_tab_reads_live_oci_rule(self) -> None:
        rule = EventRuleRecord(
            id="ocid1.eventrule.test",
            display_name="mapping-7",
            is_enabled=True,
            lifecycle_state="ACTIVE",
            condition='{"data":{"resourceName":"stream-folder/emp*.csv"}}',
            time_created=datetime.now(timezone.utc),
            mapping_id=7,
            managed=True,
        )
        health, mappings = self._base_patches()
        with health, mappings, patch(
            "myapp.modules.mapping_routes.EventRuleService.list_stream_rules", return_value=[rule]
        ):
            response = self.client.get("/mappings/?tab=rules")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Retrieved directly from OCI", response.data)
        self.assertIn(b"OCI Rules", response.data)
        self.assertIn(b"mapping-7", response.data)
        self.assertIn(b"Select all visible rules", response.data)
        self.assertIn(b"/mappings/rules/edit-selected", response.data)
        self.assertIn(b"/mappings/rules/disable-selected", response.data)
        self.assertIn(b"/mappings/rules/delete-selected", response.data)
        self.assertNotIn(b"Edit mapping/rule", response.data)

    def test_processor_deployment_tab_is_removed_from_resource_mappings(self) -> None:
        health, mappings = self._base_patches()
        with health, mappings:
            response = self.client.get("/mappings/?tab=deployment")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Processor Deployment", response.data)
        self.assertIn(b"Mappings stored", response.data)

    def test_selected_rule_edit_redirects_to_owning_mapping(self) -> None:
        rule = EventRuleRecord(
            id="ocid1.eventrule.test",
            display_name="mapping-7",
            is_enabled=True,
            lifecycle_state="ACTIVE",
            condition="{}",
            time_created=None,
            mapping_id=7,
            managed=True,
        )
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
            "myapp.modules.mapping_routes.EventRuleService.get_function_rule", return_value=rule
        ), patch("myapp.modules.mapping_routes.MappingService.get_mapping_by_rule_id", return_value=MAPPING):
            response = self.client.post(
                "/mappings/rules/edit-selected", data={"rule_id": "ocid1.eventrule.test"}
            )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/mappings/7/edit"))

    def test_selected_mapping_edit_requires_one_row_and_redirects(self) -> None:
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
            "myapp.modules.mapping_routes.MappingService.get_mapping", return_value=MAPPING
        ):
            response = self.client.post("/mappings/edit-selected", data={"mapping_id": "7"})
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/mappings/7/edit"))

    def test_multiple_selected_mappings_are_rejected_for_edit(self) -> None:
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None):
            response = self.client.post(
                "/mappings/edit-selected", data={"mapping_id": ["7", "8"]}
            )
        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as browser_session:
            flashes = browser_session.get("_flashes", [])
        self.assertTrue(any("exactly one mapping" in message for _category, message in flashes))

    def test_selected_mapping_delete_removes_managed_rule_then_mapping(self) -> None:
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
            "myapp.modules.mapping_routes.MappingService.get_mapping", return_value=MAPPING
        ), patch(
            "myapp.modules.mapping_routes.EventRuleService.delete_stream_rule"
        ) as delete_rule, patch(
            "myapp.modules.mapping_routes.MappingService.delete_mapping", return_value=True
        ) as delete_mapping:
            response = self.client.post("/mappings/delete-selected", data={"mapping_id": "7"})
        self.assertEqual(response.status_code, 302)
        delete_rule.assert_called_once_with("ocid1.eventrule.test")
        delete_mapping.assert_called_once_with(7)

    def test_selected_rules_can_be_disabled(self) -> None:
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
            "myapp.modules.mapping_routes.EventRuleService.set_rule_enabled"
        ) as disable:
            response = self.client.post(
                "/mappings/rules/disable-selected", data={"rule_id": "ocid1.eventrule.test"}
            )
        self.assertEqual(response.status_code, 302)
        disable.assert_called_once_with("ocid1.eventrule.test", enabled=False)

    def test_selected_rules_can_be_deleted_and_mapping_reference_cleared(self) -> None:
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
            "myapp.modules.mapping_routes.EventRuleService.delete_stream_rule"
        ) as delete, patch(
            "myapp.modules.mapping_routes.MappingService.clear_event_rule_reference", return_value=1
        ) as clear:
            response = self.client.post(
                "/mappings/rules/delete-selected", data={"rule_id": "ocid1.eventrule.test"}
            )
        self.assertEqual(response.status_code, 302)
        delete.assert_called_once_with("ocid1.eventrule.test")
        clear.assert_called_once_with("ocid1.eventrule.test")

    def test_object_storage_upload_tab_lists_mapping_objects(self) -> None:
        object_record = ObjectRecord(
            name="stream-folder/employees.csv",
            size=1024,
            etag="etag-test",
            time_created=None,
            time_modified=None,
            storage_tier="Standard",
        )
        health, mappings = self._base_patches()
        with health, mappings, patch(
            "myapp.modules.mapping_routes.ObjectStorageUploadService.list_mapping_objects",
            return_value=[object_record],
        ):
            response = self.client.get("/mappings/?tab=upload&mapping_id=7")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Object Storage Upload", response.data)
        self.assertIn(b"created automatically", response.data)
        self.assertIn(b"stream-folder/employees.csv", response.data)
        self.assertIn(b"Delete selected", response.data)

    def test_stream_server_tab_contains_only_server_list_and_creation(self) -> None:
        stream = StreamRecord("ocid1.stream.test", "test-stream", "ACTIVE", 1, 24, "endpoint", None)
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
            "myapp.modules.streaming_routes.StreamingService.list_streams", return_value=[stream]
        ):
            response = self.client.get("/streaming/?tab=server")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Create stream", response.data)
        self.assertIn(b"Open content", response.data)
        self.assertNotIn(b"Publish test message", response.data)
        self.assertNotIn(b"Read sample", response.data)

    def test_stream_content_tab_contains_message_actions_only(self) -> None:
        stream = StreamRecord("ocid1.stream.test", "test-stream", "ACTIVE", 1, 24, "endpoint", None)
        message = StreamMessage(0, "1", "key", "value", None)
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
            "myapp.modules.streaming_routes.StreamingService.list_streams", return_value=[stream]
        ), patch("myapp.modules.streaming_routes.StreamingService.read_messages", return_value=[message]):
            response = self.client.get("/streaming/?tab=content&stream_id=ocid1.stream.test")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Publish test message", response.data)
        self.assertIn(b"Read sample", response.data)
        self.assertNotIn(b">Create stream</button>", response.data)

    def test_mapping_scoped_csv_upload_uses_selected_mapping(self) -> None:
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
            "myapp.modules.mapping_routes.MappingService.get_mapping", return_value=MAPPING
        ), patch(
            "myapp.modules.mapping_routes.ObjectStorageUploadService.upload_csv",
            return_value="stream-folder/employees.csv",
        ) as upload:
            response = self.client.post(
                "/mappings/upload",
                data={
                    "mapping_id": "7",
                    "folder": "stream-folder",
                    "csv_file": (io.BytesIO(b"id,name\n1,Ada\n"), "employees.csv"),
                },
                content_type="multipart/form-data",
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(upload.call_args.kwargs["mapping"], MAPPING)
        self.assertEqual(upload.call_args.kwargs["folder"], "stream-folder")

    def test_mapping_scoped_objects_can_be_deleted(self) -> None:
        with patch("myapp.modules.common.MySQLService.health_check", return_value=None), patch(
            "myapp.modules.mapping_routes.MappingService.get_mapping", return_value=MAPPING
        ), patch(
            "myapp.modules.mapping_routes.ObjectStorageUploadService.delete_objects", return_value=1
        ) as delete:
            response = self.client.post(
                "/mappings/upload/delete-selected",
                data={"mapping_id": "7", "object_name": "stream-folder/employees.csv"},
            )
        self.assertEqual(response.status_code, 302)
        delete.assert_called_once_with(mapping=MAPPING, object_names=["stream-folder/employees.csv"])


if __name__ == "__main__":
    unittest.main()
