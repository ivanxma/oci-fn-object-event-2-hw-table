from __future__ import annotations

import json
import unittest

from myapp.services.event_rule_service import EVENT_TYPES, rule_condition
from myapp.services.mapping_service import MappingService
from myapp.services.object_storage_upload_service import default_folder, object_name_for_upload, static_prefix


def mapping_form(**overrides: str) -> dict[str, str]:
    values = {
        "compartment_name": "Operations",
        "bucket_name": "test-bucket",
        "resource_name_pattern": "performance/*.csv",
        "target_database": "fntestdb",
        "target_table": "perf_t_001",
        "worker_threads": "4",
        "stream_id": "ocid1.stream.oc1.uk-london-1.example",
        "processing_mode": "FIFO",
    }
    values.update(overrides)
    return values


class MappingOciManagementTest(unittest.TestCase):
    def test_mapping_has_no_per_mapping_timeout(self) -> None:
        normalized = MappingService.normalize(mapping_form(timeout_seconds="9999"))
        self.assertNotIn("timeout_seconds", normalized)
        self.assertEqual(normalized["worker_threads"], "4")

    def test_mapping_worker_bounds(self) -> None:
        for workers in ("0", "65", "invalid"):
            with self.subTest(workers=workers):
                with self.assertRaisesRegex(ValueError, "Worker threads"):
                    MappingService.normalize(mapping_form(worker_threads=workers))

    def test_mapping_requires_stream_and_known_processing_mode(self) -> None:
        with self.assertRaisesRegex(ValueError, "OCI Stream is required"):
            MappingService.normalize(mapping_form(stream_id=""))
        with self.assertRaisesRegex(ValueError, "Processing mode"):
            MappingService.normalize(mapping_form(processing_mode="UNORDERED"))
        self.assertEqual(MappingService.normalize(mapping_form(processing_mode="FIFO"))["processing_mode"], "FIFO")

    def test_rule_condition_has_bucket_pattern_compartment_and_lifecycle_events(self) -> None:
        condition = json.loads(
            rule_condition(
                compartment_id="ocid1.compartment.test",
                bucket_name="test-bucket",
                resource_pattern="stream-folder/emp*.csv",
            )
        )
        self.assertEqual(condition["eventType"], EVENT_TYPES)
        self.assertEqual(condition["data"]["compartmentId"], "ocid1.compartment.test")
        self.assertEqual(condition["data"]["resourceName"], "stream-folder/emp*.csv")
        self.assertEqual(condition["data"]["additionalDetails"]["bucketName"], "test-bucket")

    def test_upload_creates_virtual_folder_and_must_match_mapping(self) -> None:
        self.assertEqual(default_folder("testing/new-folder/*.csv"), "testing/new-folder")
        self.assertEqual(static_prefix("testing/new-folder/*.csv"), "testing/new-folder/")
        self.assertEqual(
            object_name_for_upload(
                folder="testing/new-folder",
                filename="employees.csv",
                resource_pattern="testing/new-folder/*.csv",
            ),
            "testing/new-folder/employees.csv",
        )
        with self.assertRaisesRegex(ValueError, "does not match"):
            object_name_for_upload(
                folder="wrong-folder",
                filename="employees.csv",
                resource_pattern="testing/new-folder/*.csv",
            )


if __name__ == "__main__":
    unittest.main()
