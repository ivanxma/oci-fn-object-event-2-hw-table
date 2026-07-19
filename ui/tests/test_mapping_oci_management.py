from __future__ import annotations

import json
import unittest

from myapp.services.event_rule_service import EVENT_TYPES, rule_condition
from myapp.services.function_configuration_service import normalize_function_configuration
from myapp.services.mapping_service import MappingService
from myapp.services.object_storage_upload_service import default_folder, object_name_for_upload, static_prefix


def mapping_form(**overrides: str) -> dict[str, str]:
    values = {
        "compartment_name": "HWDemo",
        "bucket_name": "test-bucket",
        "resource_name_pattern": "performance/*.csv",
        "target_database": "fntestdb",
        "target_table": "perf_t_001",
        "invocation_mode": "SYNC",
        "worker_threads": "4",
    }
    values.update(overrides)
    return values


def function_form(**overrides: str) -> dict[str, str]:
    values = {
        "memory_in_mbs": "1024",
        "sync_timeout_seconds": "300",
        "detached_timeout_seconds": "3600",
        "provisioned_concurrency": "0",
        "writer_workers": "4",
        "batch_rows": "10000",
        "object_storage_range_bytes": "33554432",
        "object_storage_read_timeout_seconds": "300",
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

    def test_rule_condition_has_bucket_pattern_compartment_and_lifecycle_events(self) -> None:
        condition = json.loads(
            rule_condition(
                compartment_id="ocid1.compartment.test",
                bucket_name="test-bucket",
                resource_pattern="detached-folder/emp*.csv",
            )
        )
        self.assertEqual(condition["eventType"], EVENT_TYPES)
        self.assertEqual(condition["data"]["compartmentId"], "ocid1.compartment.test")
        self.assertEqual(condition["data"]["resourceName"], "detached-folder/emp*.csv")
        self.assertEqual(condition["data"]["additionalDetails"]["bucketName"], "test-bucket")

    def test_function_configuration_validation(self) -> None:
        values = normalize_function_configuration(function_form())
        self.assertEqual(values["memory_in_mbs"], 1024)
        self.assertEqual(values["sync_timeout_seconds"], 300)
        self.assertEqual(values["detached_timeout_seconds"], 3600)

    def test_function_memory_requires_64_mb_increment(self) -> None:
        with self.assertRaisesRegex(ValueError, "multiple of 64 MB"):
            normalize_function_configuration(function_form(memory_in_mbs="1000"))

    def test_function_timeout_bounds_are_global(self) -> None:
        for field, value, message in (
            ("sync_timeout_seconds", "301", "Sync timeout"),
            ("detached_timeout_seconds", "3601", "Detached timeout"),
        ):
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, message):
                    normalize_function_configuration(function_form(**{field: value}))

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
