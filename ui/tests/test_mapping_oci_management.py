from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from myapp.services.event_rule_service import EVENT_TYPES, rule_condition
from myapp.services.function_configuration_service import FunctionConfigurationService, normalize_function_configuration
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
        "order_required": "on",
        "reorder_grace_seconds": "30",
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
        "db_host": "10.0.0.10",
        "db_port": "3306",
        "db_user": "fnuser",
        "control_database": "fndb",
        "db_ssl_mode": "REQUIRED",
    }
    values.update(overrides)
    return values


class MappingOciManagementTest(unittest.TestCase):
    def test_mapping_has_per_mapping_order_policy(self) -> None:
        normalized = MappingService.normalize(mapping_form(timeout_seconds="9999"))
        self.assertNotIn("timeout_seconds", normalized)
        self.assertEqual(normalized["worker_threads"], "4")
        self.assertTrue(normalized["order_required"])
        self.assertEqual(normalized["reorder_grace_seconds"], 30)

    def test_ordered_mapping_requires_at_least_30_seconds(self) -> None:
        with self.assertRaisesRegex(ValueError, "at least 30 seconds"):
            MappingService.normalize(mapping_form(reorder_grace_seconds="29"))

    def test_unordered_mapping_allows_zero_wait(self) -> None:
        values = mapping_form(reorder_grace_seconds="0")
        values.pop("order_required")
        normalized = MappingService.normalize(values)
        self.assertFalse(normalized["order_required"])
        self.assertEqual(normalized["reorder_grace_seconds"], 0)

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
        self.assertEqual(values["queue_reorder_grace_seconds"], 30)
        self.assertEqual(values["queue_expected_bytes_per_second"], 4 * 1024 * 1024)
        self.assertFalse(values["db_ssl_disabled"])

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

    def test_queue_wait_and_admission_configuration(self) -> None:
        values = normalize_function_configuration(
            function_form(
                detached_enabled="on",
                queue_reorder_grace_seconds="45",
                queue_sync_reserve_seconds="20",
                queue_sync_minimum_start_seconds="25",
                queue_shutdown_reserve_seconds="180",
                queue_minimum_start_seconds="240",
                queue_unknown_job_seconds="75",
                queue_expected_mib_per_second="14.5",
                queue_prediction_safety_factor="1.5",
                load_lease_seconds="180",
            )
        )
        self.assertTrue(values["detached_enabled"])
        self.assertEqual(values["queue_reorder_grace_seconds"], 45)
        self.assertEqual(values["queue_expected_bytes_per_second"], int(14.5 * 1024 * 1024))
        self.assertEqual(values["queue_prediction_safety_factor"], 1.5)
        self.assertEqual(values["load_lease_seconds"], 180)

    def test_queue_runtime_budget_must_fit_function_timeout(self) -> None:
        with self.assertRaisesRegex(ValueError, "Sync reserve plus minimum"):
            normalize_function_configuration(
                function_form(queue_sync_reserve_seconds="150", queue_sync_minimum_start_seconds="150")
            )
        with self.assertRaisesRegex(ValueError, "Detached reserve plus minimum"):
            normalize_function_configuration(
                function_form(queue_shutdown_reserve_seconds="1800", queue_minimum_start_seconds="1800")
            )

    def test_disabling_database_tls_requires_confirmation(self) -> None:
        with self.assertRaisesRegex(ValueError, "TLS mode must be Required or Disabled"):
            normalize_function_configuration(function_form(db_ssl_mode="PREFERRED"))
        with self.assertRaisesRegex(ValueError, "Confirm that database TLS"):
            normalize_function_configuration(function_form(db_ssl_mode="DISABLED"))
        values = normalize_function_configuration(
            function_form(db_ssl_mode="DISABLED", confirm_db_ssl_disabled="on")
        )
        self.assertTrue(values["db_ssl_disabled"])

    def test_function_update_preserves_or_replaces_password_without_returning_it(self) -> None:
        current = SimpleNamespace(
            display_name="function",
            lifecycle_state="ACTIVE",
            shape="GENERIC_X86",
            memory_in_mbs=1024,
            timeout_in_seconds=300,
            detached_mode_timeout_in_seconds=3600,
            provisioned_concurrency_config=None,
            image="image:1",
            time_updated=None,
            config={"DB_PASSWORD": "existing-secret", "UNRELATED": "preserved"},
        )

        class Models:
            NoneProvisionedConcurrencyConfig = staticmethod(lambda **values: SimpleNamespace(**values))
            ConstantProvisionedConcurrencyConfig = staticmethod(lambda **values: SimpleNamespace(**values))
            UpdateFunctionDetails = staticmethod(lambda **values: SimpleNamespace(**values))

        class Client:
            def __init__(self, function):
                self.function = function

            def get_function(self, _function_id):
                return SimpleNamespace(data=self.function)

            def update_function(self, _function_id, details):
                self.function.config = details.config
                self.function.memory_in_mbs = details.memory_in_mbs
                self.function.timeout_in_seconds = details.timeout_in_seconds
                self.function.detached_mode_timeout_in_seconds = details.detached_mode_timeout_in_seconds
                self.function.provisioned_concurrency_config = details.provisioned_concurrency_config
                return SimpleNamespace(data=self.function)

        service = FunctionConfigurationService(function_id="ocid1.fnfunc.test", region="uk-london-1")
        client = Client(current)
        oci = SimpleNamespace(functions=SimpleNamespace(models=Models))
        with patch.object(service, "_client", return_value=(oci, client)):
            result = service.update(function_form())
            self.assertEqual(client.function.config["DB_PASSWORD"], "existing-secret")
            self.assertEqual(client.function.config["UNRELATED"], "preserved")
            self.assertFalse(hasattr(result, "db_password"))
            service.update(function_form(new_db_password="replacement-secret"))
            self.assertEqual(client.function.config["DB_PASSWORD"], "replacement-secret")

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
