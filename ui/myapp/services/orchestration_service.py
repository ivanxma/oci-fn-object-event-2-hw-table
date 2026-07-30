"""OCI Container Instance orchestration for explicit Stream partitions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import re


class OrchestrationError(RuntimeError):
    pass


def valid_deployment_id(value: str) -> bool:
    """OCI uses the computecontainerinstance OCID resource type."""
    return value.startswith("ocid1.computecontainerinstance.")


def validate_deployment(*, processing_mode: str, partitions: int, replicas: int) -> None:
    mode = processing_mode.upper()
    if mode == "FIFO" and (partitions != 1 or replicas != 1):
        raise ValueError("FIFO deployment requires one stream partition and one processor replica.")
    if mode == "PARALLEL" and (partitions < 2 or not 1 <= replicas <= partitions):
        raise ValueError("Parallel deployment requires at least two partitions and replicas no greater than partitions.")
    if mode not in {"FIFO", "PARALLEL"}:
        raise ValueError("Mapping processing mode must be FIFO or PARALLEL.")


def assigned_partitions(value: str, *, partition_count: int, mode: str) -> list[str]:
    values = [item.strip() for item in value.split(",") if item.strip()]
    if not values or len(set(values)) != len(values):
        raise ValueError("Choose one or more unique partition assignments.")
    if any(not item.isdigit() or int(item) >= partition_count for item in values):
        raise ValueError("A selected partition is outside the Stream partition range.")
    if mode.upper() == "FIFO" and values != ["0"]:
        raise ValueError("FIFO must use only Stream partition 0.")
    return values


@dataclass(frozen=True)
class DeploymentSettings:
    enabled: bool
    compartment_id: str
    region: str
    subnet_id: str
    availability_domain: str
    shape: str
    ocpus: float
    memory_gbs: float
    image_url: str
    db_secret_ocid: str
    db_host: str
    db_port: str
    db_user: str
    db_name: str
    stream_data_db_name: str
    control_database: str
    name_prefix: str = "object-storage-stream-processor"
    writer_workers: int = 4
    staging_database: str = "stream_staging"


@dataclass(frozen=True)
class ProcessorRuntime:
    """Non-secret values passed to a single Container Instance deployment."""
    image_url: str
    db_secret_ocid: str
    display_name: str
    writer_workers: int

    @classmethod
    def from_form(cls, form: Any, defaults: DeploymentSettings) -> "ProcessorRuntime":
        def value(name: str, fallback: str) -> str:
            return str(form.get(name, fallback)).strip()

        runtime = cls(
            image_url=value("image_url", defaults.image_url),
            db_secret_ocid=value("db_secret_ocid", defaults.db_secret_ocid),
            display_name=value("processor_name", ""),
            writer_workers=int(value("writer_workers", str(defaults.writer_workers))),
        )
        runtime.validate()
        return runtime

    def validate(self) -> None:
        required = ("image_url", "db_secret_ocid", "display_name")
        missing = [name for name in required if not str(getattr(self, name)).strip()]
        if missing:
            raise ValueError("Processor configuration is missing: " + ", ".join(missing) + ".")
        if any(char.isspace() for char in self.image_url) or ":" not in self.image_url:
            raise ValueError("Container image must be a valid OCI Registry image reference.")
        if not self.db_secret_ocid.startswith("ocid1.vaultsecret."):
            raise ValueError("Choose a valid OCI Vault secret.")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9._-]{0,254}", self.display_name):
            raise ValueError("Processor name must start with a letter and use letters, digits, dots, hyphens, or underscores.")
        if not 1 <= self.writer_workers <= 32:
            raise ValueError("Loader workers must be from 1 to 32.")


class ContainerOrchestrationService:
    def __init__(self, settings: DeploymentSettings) -> None:
        self.settings = settings

    def deployment_spec(self, *, mapping: dict[str, Any], stream_partitions: int, partition_assignment: str, runtime: ProcessorRuntime | None = None) -> dict[str, Any]:
        runtime = runtime or ProcessorRuntime(
            self.settings.image_url, self.settings.db_secret_ocid, f"{self.settings.name_prefix}-{str(mapping.get('processing_mode') or 'fifo').lower()}-p{partition_assignment}", self.settings.writer_workers,
        )
        runtime.validate()
        mode = str(mapping.get("processing_mode") or "FIFO").upper()
        assignments = assigned_partitions(partition_assignment, partition_count=stream_partitions, mode=mode)
        validate_deployment(processing_mode=mode, partitions=stream_partitions, replicas=1)
        required = ("compartment_id", "region", "subnet_id", "availability_domain", "shape")
        missing = [name for name in required if not str(getattr(self.settings, name)).strip()]
        if missing:
            raise ValueError("Container orchestration is missing: " + ", ".join(missing) + ".")
        if self.settings.ocpus <= 0 or self.settings.memory_gbs <= 0:
            raise ValueError("Container OCPUs and memory must both be greater than zero.")
        if not str(mapping.get("stream_id", "")).startswith("ocid1.stream."):
            raise ValueError("The mapping does not have a valid OCI Stream.")
        name = runtime.display_name
        return {
            "display_name": name,
            "compartment_id": self.settings.compartment_id,
            "availability_domain": self.settings.availability_domain,
            "shape": self.settings.shape,
            "ocpus": self.settings.ocpus,
            "memory_gbs": self.settings.memory_gbs,
            "subnet_id": self.settings.subnet_id,
            "container": {
                "display_name": name,
                "image_url": runtime.image_url,
                "is_resource_principal_disabled": False,
                "environment_variables": {
                    "OCI_STREAM_ID": str(mapping["stream_id"]), "PROCESSING_MODE": mode,
                    "EXPECTED_PARTITION_COUNT": str(stream_partitions), "PROCESSOR_REPLICA_COUNT": "1",
                    "PROCESSOR_PARTITIONS": ",".join(assignments), "DB_SECRET_OCID": runtime.db_secret_ocid,
                    "WRITER_WORKERS": str(runtime.writer_workers),
                },
            },
        }

    def _client(self):
        if not self.settings.enabled:
            raise OrchestrationError("OCI Container orchestration is disabled for this UI deployment.")
        try:
            import oci
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return oci, oci.container_instances.ContainerInstanceClient({"region": self.settings.region}, signer=signer)
        except Exception as error:
            raise OrchestrationError(f"Could not initialize OCI Container Instances: {type(error).__name__}: {error}") from error

    def list_deployments(self, *, state_filter: str = "ALL") -> list[dict[str, str]]:
        """List tagged Container Instances, optionally retaining ACTIVE records only."""
        state_filter = state_filter.upper()
        if state_filter not in {"ALL", "ACTIVE"}:
            raise ValueError("Managed Instance state filter must be All or Active only.")
        oci, client = self._client()
        try:
            records = oci.pagination.list_call_get_all_results(client.list_container_instances, compartment_id=self.settings.compartment_id).data
            return [
                {"id": str(item.id), "display_name": str(item.display_name), "lifecycle_state": str(item.lifecycle_state), "mapping_id": str((getattr(item, "freeform_tags", {}) or {}).get("mapping-id", ""))}
                for item in records
                if (getattr(item, "freeform_tags", {}) or {}).get("managed-by") == "oci-object-event-2-table"
                and (state_filter == "ALL" or str(getattr(item, "lifecycle_state", "")).upper() == "ACTIVE")
            ]
        except Exception as error:
            raise OrchestrationError(f"Could not list Container Instances: {type(error).__name__}: {error}") from error

    def create(self, *, mapping: dict[str, Any], stream_partitions: int, partition_assignment: str, runtime: ProcessorRuntime | None = None) -> dict[str, str]:
        spec = self.deployment_spec(mapping=mapping, stream_partitions=stream_partitions, partition_assignment=partition_assignment, runtime=runtime)
        try:
            oci, client = self._client()
            records = oci.pagination.list_call_get_all_results(
                client.list_container_instances, compartment_id=self.settings.compartment_id
            ).data
            duplicate = next(
                (
                    item for item in records
                    if (getattr(item, "freeform_tags", {}) or {}).get("managed-by") == "oci-object-event-2-table"
                    and str(getattr(item, "display_name", "")) == spec["display_name"]
                    and str(getattr(item, "lifecycle_state", "")).upper() not in {"DELETED", "DELETING"}
                ),
                None,
            )
            if duplicate is not None:
                raise ValueError(
                    "An active managed Container Instance already uses this processor name. "
                    "Choose a different name, or delete the old instance before reusing it."
                )
            models = oci.container_instances.models
            details = models.CreateContainerInstanceDetails(
                display_name=spec["display_name"], compartment_id=spec["compartment_id"], availability_domain=spec["availability_domain"],
                shape=spec["shape"], shape_config=models.CreateContainerInstanceShapeConfigDetails(ocpus=spec["ocpus"], memory_in_gbs=spec["memory_gbs"]),
                containers=[models.CreateContainerDetails(**spec["container"])],
                vnics=[models.CreateContainerVnicDetails(subnet_id=spec["subnet_id"], is_public_ip_assigned=False)],
                freeform_tags={"managed-by": "oci-object-event-2-table", "mapping-id": str(mapping.get("id", ""))},
            )
            result = client.create_container_instance(details).data
            return {"id": str(result.id), "display_name": str(result.display_name), "lifecycle_state": str(result.lifecycle_state)}
        except (ValueError, OrchestrationError):
            raise
        except Exception as error:
            raise OrchestrationError(f"Could not create Container Instance: {type(error).__name__}: {error}") from error

    def get_deployment(self, deployment_id: str) -> dict[str, Any]:
        """Return a managed Container Instance's non-secret deployment contract."""
        if not valid_deployment_id(deployment_id):
            raise ValueError("Container Instance identifier is invalid.")
        try:
            _, client = self._client()
            item = client.get_container_instance(deployment_id).data
            tags = getattr(item, "freeform_tags", {}) or {}
            if tags.get("managed-by") != "oci-object-event-2-table":
                raise OrchestrationError("Only Container Instances managed by this application can be viewed.")
            shape_config = getattr(item, "shape_config", None)
            containers = []
            allowed_environment = {
                "OCI_STREAM_ID", "PROCESSING_MODE", "EXPECTED_PARTITION_COUNT", "PROCESSOR_REPLICA_COUNT",
                "PROCESSOR_PARTITIONS", "DB_SECRET_OCID", "WRITER_WORKERS",
            }
            # Container Instance summaries do not include the image or environment
            # contract.  Fetch each nested Container resource explicitly.
            def field(value: Any, name: str, default: Any = None) -> Any:
                if isinstance(value, dict):
                    return value.get(name, default)
                return getattr(value, name, default)

            container_records = []
            for summary in getattr(item, "containers", []) or []:
                container = summary
                container_id = field(summary, "id", "")
                if container_id:
                    try:
                        container = client.get_container(container_id).data
                    except Exception:
                        # Keep the instance page usable if a nested resource is
                        # eventually-consistent; the refresh action can retry it.
                        container = summary
                container_records.append(container)
            for container in container_records:
                environment = field(container, "environment_variables", {}) or {}
                if not isinstance(environment, dict):
                    environment = {
                        str(field(value, "name", "")): field(value, "value", "")
                        for value in environment
                        if field(value, "name", "")
                    }
                visible_environment = []
                for key in sorted(environment):
                    if key not in allowed_environment:
                        continue
                    # The setting is intentionally visible, but the Vault OCID
                    # itself is treated as secret configuration on this page.
                    display_value = "Configured (secret OCID hidden)" if key == "DB_SECRET_OCID" else str(environment[key])
                    visible_environment.append({"name": key, "value": display_value})
                containers.append({
                    "display_name": str(field(container, "display_name", "")),
                    "image_url": str(field(container, "image_url", "")),
                    "resource_principal_enabled": not bool(field(container, "is_resource_principal_disabled", True)),
                    "environment": visible_environment,
                })
            primary_container = container_records[0] if container_records else None
            primary_environment = field(primary_container, "environment_variables", {}) or {}
            if not isinstance(primary_environment, dict):
                primary_environment = {str(field(value, "name", "")): field(value, "value", "") for value in primary_environment}
            return {
                "id": str(item.id), "display_name": str(item.display_name), "lifecycle_state": str(item.lifecycle_state),
                "mapping_id": str(tags.get("mapping-id", "")), "compartment_id": str(getattr(item, "compartment_id", "")),
                "availability_domain": str(getattr(item, "availability_domain", "")), "shape": str(getattr(item, "shape", "")),
                "ocpus": getattr(shape_config, "ocpus", ""), "memory_gbs": getattr(shape_config, "memory_in_gbs", ""),
                "containers": containers,
                "replacement": {
                    "mapping_id": str(tags.get("mapping-id", "")),
                    "partition_assignment": str(primary_environment.get("PROCESSOR_PARTITIONS", "")),
                    "image_url": str(field(primary_container, "image_url", "")),
                    "writer_workers": str(primary_environment.get("WRITER_WORKERS", "")),
                },
            }
        except (ValueError, OrchestrationError):
            raise
        except Exception as error:
            raise OrchestrationError(f"Could not load Container Instance details: {type(error).__name__}: {error}") from error

    def delete(self, deployment_id: str) -> None:
        """Delete only an instance carrying this application's management tag."""
        if not valid_deployment_id(deployment_id):
            raise ValueError("Container Instance identifier is invalid.")
        try:
            oci, client = self._client()
            records = oci.pagination.list_call_get_all_results(
                client.list_container_instances, compartment_id=self.settings.compartment_id
            ).data
            record = next((item for item in records if str(item.id) == deployment_id and (getattr(item, "freeform_tags", {}) or {}).get("managed-by") == "oci-object-event-2-table"), None)
            if record is None:
                raise OrchestrationError("Only Container Instances managed by this application can be deleted.")
            if str(getattr(record, "lifecycle_state", "")).upper() == "DELETED":
                raise ValueError("This managed Container Instance is already deleted.")
            client.delete_container_instance(deployment_id)
        except (ValueError, OrchestrationError):
            raise
        except Exception as error:
            raise OrchestrationError(f"Could not delete Container Instance: {type(error).__name__}: {error}") from error
