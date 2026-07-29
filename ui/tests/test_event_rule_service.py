from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from myapp.services.event_rule_service import EventRuleError, EventRuleService


class _Details:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _Client:
    def __init__(self):
        self.created = None

    def create_rule(self, details):
        self.created = details
        rule = SimpleNamespace(
            id="ocid1.eventrule.test", display_name=details.display_name,
            is_enabled=True, lifecycle_state="ACTIVE", condition=details.condition,
            time_created=None, freeform_tags=details.freeform_tags,
        )
        return SimpleNamespace(data=rule)


class EventRuleServiceTest(unittest.TestCase):
    def test_create_uses_oss_action_and_mapping_tags(self):
        service = EventRuleService(compartment_id="ocid1.compartment.test", region="uk-london-1", enabled=True)
        client = _Client()
        models = SimpleNamespace(
            CreateStreamingServiceActionDetails=_Details, ActionDetailsList=_Details,
            CreateRuleDetails=_Details,
        )
        oci = SimpleNamespace(events=SimpleNamespace(models=models))
        mapping = {"bucket_name": "bucket", "resource_name_pattern": "verify/*.csv", "target_database": "db", "target_table": "table", "stream_id": "ocid1.stream.test"}
        with patch.object(service, "_client", return_value=(oci, client)):
            result = service.ensure_mapping_rule(mapping_id=9, mapping=mapping)
        action = client.created.actions.actions[0]
        self.assertEqual(action.action_type, "OSS")
        self.assertEqual(action.stream_id, "ocid1.stream.test")
        self.assertEqual(result.mapping_id, 9)
        self.assertEqual(client.created.freeform_tags["managed-by"], "oci-object-event-2-table")

    def test_create_rejects_missing_stream_before_oci_mutation(self):
        service = EventRuleService(compartment_id="ocid1.compartment.test", region="uk-london-1", enabled=True)
        with patch.object(service, "_client", side_effect=AssertionError("OCI must not be called")):
            with self.assertRaisesRegex(EventRuleError, "valid OCI Stream"):
                service.ensure_mapping_rule(mapping_id=1, mapping={"bucket_name": "bucket", "resource_name_pattern": "x", "target_database": "db", "target_table": "table", "stream_id": ""})


if __name__ == "__main__":
    unittest.main()
