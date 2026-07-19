"""OCI Events rule management for Object Storage mapping entries."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any


EVENT_TYPES = [
    "com.oraclecloud.objectstorage.createobject",
    "com.oraclecloud.objectstorage.updateobject",
    "com.oraclecloud.objectstorage.deleteobject",
]
MANAGED_BY = "oci-object-event-2-table"


class EventRuleError(RuntimeError):
    """Raised when OCI Events rule management cannot complete safely."""


@dataclass(frozen=True)
class EventRuleRecord:
    id: str
    display_name: str
    is_enabled: bool
    lifecycle_state: str
    condition: str
    time_created: datetime | None
    mapping_id: int | None
    managed: bool


def rule_condition(*, compartment_id: str, bucket_name: str, resource_pattern: str) -> str:
    """Build the OCI Events condition for one mapping's bucket/object scope."""
    return json.dumps(
        {
            "eventType": EVENT_TYPES,
            "data": {
                "compartmentId": compartment_id,
                "resourceName": resource_pattern,
                "additionalDetails": {"bucketName": bucket_name},
            },
        },
        separators=(",", ":"),
    )


class EventRuleService:
    """Use the UI host instance principal to manage rules for one Function."""

    def __init__(
        self,
        *,
        function_id: str,
        compartment_id: str,
        region: str,
        enabled: bool,
        rule_prefix: str = "object-storage-heatwave",
    ) -> None:
        self.function_id = function_id.strip()
        self.compartment_id = compartment_id.strip()
        self.region = region.strip()
        self.enabled = enabled
        self.rule_prefix = (rule_prefix.strip() or "object-storage-heatwave")[:220]

    def _require_configuration(self) -> None:
        if not self.enabled:
            raise EventRuleError("OCI Events rule management is disabled for this UI deployment.")
        missing = [
            name
            for name, value in (
                ("OCI_FUNCTION_ID", self.function_id),
                ("OCI_COMPARTMENT_ID", self.compartment_id),
                ("OCI_REGION", self.region),
            )
            if not value
        ]
        if missing:
            raise EventRuleError(f"OCI Events rule management is missing: {', '.join(missing)}.")

    def _client(self):
        self._require_configuration()
        try:
            import oci

            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return oci, oci.events.EventsClient({"region": self.region}, signer=signer)
        except Exception as error:
            raise EventRuleError(
                f"Could not initialize the OCI Events client: {type(error).__name__}: {error}"
            ) from error

    @staticmethod
    def _mapping_id(tags: dict[str, str] | None) -> int | None:
        try:
            return int((tags or {}).get("mapping-id", ""))
        except (TypeError, ValueError):
            return None

    def _targets_function(self, rule: Any) -> bool:
        actions = getattr(getattr(rule, "actions", None), "actions", None) or []
        return any(
            str(getattr(action, "action_type", "")).upper() == "FAAS"
            and getattr(action, "function_id", None) == self.function_id
            for action in actions
        )

    def _record(self, rule: Any) -> EventRuleRecord:
        tags = getattr(rule, "freeform_tags", None) or {}
        return EventRuleRecord(
            id=rule.id,
            display_name=rule.display_name,
            is_enabled=bool(rule.is_enabled),
            lifecycle_state=str(rule.lifecycle_state or "UNKNOWN"),
            condition=str(rule.condition or ""),
            time_created=getattr(rule, "time_created", None),
            mapping_id=self._mapping_id(tags),
            managed=tags.get("managed-by") == MANAGED_BY,
        )

    def list_function_rules(self) -> list[EventRuleRecord]:
        """Read live rules from OCI and retain rules targeting this Function."""
        try:
            oci, client = self._client()
            summaries = oci.pagination.list_call_get_all_results(
                client.list_rules, compartment_id=self.compartment_id
            ).data
            records: list[EventRuleRecord] = []
            for summary in summaries:
                if str(getattr(summary, "lifecycle_state", "")).upper() == "DELETED":
                    continue
                try:
                    rule = client.get_rule(summary.id).data
                except oci.exceptions.ServiceError as error:
                    if error.status == 404:
                        continue
                    raise
                if str(getattr(rule, "lifecycle_state", "")).upper() == "DELETED":
                    continue
                if self._targets_function(rule):
                    records.append(self._record(rule))
            return sorted(records, key=lambda item: item.display_name.lower())
        except EventRuleError:
            raise
        except Exception as error:
            raise EventRuleError(
                f"Could not read OCI Events rules: {type(error).__name__}: {error}"
            ) from error

    def ensure_mapping_rule(
        self,
        *,
        mapping_id: int,
        mapping: dict[str, Any],
        existing_rule_id: str | None = None,
    ) -> EventRuleRecord:
        """Create or update the one managed OCI rule owned by a mapping."""
        condition = rule_condition(
            compartment_id=self.compartment_id,
            bucket_name=str(mapping["bucket_name"]),
            resource_pattern=str(mapping["resource_name_pattern"]),
        )
        tags = {"managed-by": MANAGED_BY, "mapping-id": str(mapping_id)}
        description = (
            f"Mapping {mapping_id}: {mapping['bucket_name']}/{mapping['resource_name_pattern']} "
            f"to {mapping['target_database']}.{mapping['target_table']}"
        )[:255]
        try:
            oci, client = self._client()
            action = oci.events.models.CreateFaaSActionDetails(
                action_type="FAAS",
                is_enabled=True,
                description="Object Storage CSV mapping",
                function_id=self.function_id,
            )
            actions = oci.events.models.ActionDetailsList(actions=[action])
            if existing_rule_id:
                try:
                    existing = client.get_rule(existing_rule_id).data
                except oci.exceptions.ServiceError as error:
                    if error.status != 404:
                        raise
                else:
                    if not self._targets_function(existing):
                        raise EventRuleError(
                            "The associated OCI rule no longer targets the configured Function; it was not overwritten."
                        )
                    details = oci.events.models.UpdateRuleDetails(
                        display_name=existing.display_name,
                        description=description,
                        is_enabled=True,
                        condition=condition,
                        actions=actions,
                        freeform_tags={**(existing.freeform_tags or {}), **tags},
                    )
                    return self._record(
                        client.update_rule(existing_rule_id, details).data
                    )

            details = oci.events.models.CreateRuleDetails(
                display_name=f"{self.rule_prefix}-mapping-{mapping_id}",
                description=description,
                is_enabled=True,
                condition=condition,
                compartment_id=self.compartment_id,
                actions=actions,
                freeform_tags=tags,
            )
            return self._record(client.create_rule(details).data)
        except EventRuleError:
            raise
        except Exception as error:
            raise EventRuleError(
                f"Could not create or update the OCI Events rule: {type(error).__name__}: {error}"
            ) from error

    def get_function_rule(self, rule_id: str) -> EventRuleRecord:
        """Return one live rule after verifying its FAAS action target."""
        try:
            _oci, client = self._client()
            rule = client.get_rule(rule_id).data
            if not self._targets_function(rule):
                raise EventRuleError("The selected OCI rule does not target the configured Function.")
            if str(getattr(rule, "lifecycle_state", "")).upper() == "DELETED":
                raise EventRuleError("The selected OCI rule has already been deleted.")
            return self._record(rule)
        except EventRuleError:
            raise
        except Exception as error:
            raise EventRuleError(
                f"Could not read the selected OCI Events rule: {type(error).__name__}: {error}"
            ) from error

    def set_rule_enabled(self, rule_id: str, *, enabled: bool) -> EventRuleRecord:
        """Enable or disable a rule only after verifying its Function target."""
        try:
            oci, client = self._client()
            self.get_function_rule(rule_id)
            details = oci.events.models.UpdateRuleDetails(is_enabled=enabled)
            return self._record(client.update_rule(rule_id, details).data)
        except EventRuleError:
            raise
        except Exception as error:
            action = "enable" if enabled else "disable"
            raise EventRuleError(
                f"Could not {action} the OCI Events rule: {type(error).__name__}: {error}"
            ) from error

    def delete_function_rule(self, rule_id: str) -> None:
        """Delete a rule only after verifying that it targets this Function."""
        try:
            oci, client = self._client()
            try:
                rule = client.get_rule(rule_id).data
            except oci.exceptions.ServiceError as error:
                if error.status == 404:
                    return
                raise
            if not self._targets_function(rule):
                raise EventRuleError(
                    "The selected OCI rule does not target the configured Function and was not deleted."
                )
            client.delete_rule(rule_id)
        except EventRuleError:
            raise
        except Exception as error:
            raise EventRuleError(
                f"Could not delete the OCI Events rule: {type(error).__name__}: {error}"
            ) from error
