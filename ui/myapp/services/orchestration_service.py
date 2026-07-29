"""OCI Container Instance orchestration for explicit Stream partitions."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class OrchestrationError(RuntimeError):
    pass


def validate_deployment(*, processing_mode: str, partitions: int, replicas: int) -> None:
    mode = processing_mode.upper()
    if mode == "FIFO" and (partitions != 1 or replicas != 1):
        raise ValueError("FIFO deployment requires one stream partition and one consumer replica.")
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
    name_prefix: str = "object-storage-stream-consumer"


class ContainerOrchestrationService:
    def __init__(self, settings: DeploymentSettings) -> None:
        self.settings = settings

    def deployment_spec(self, *, mapping: dict[str, Any], stream_partitions: int, partition_assignment: str) -> dict[str, Any]:
        mode = str(mapping.get("processing_mode") or "FIFO").upper()
        assignments = assigned_partitions(partition_assignment, partition_count=stream_partitions, mode=mode)
        validate_deployment(processing_mode=mode, partitions=stream_partitions, replicas=1)
        required = ("compartment_id", "region", "subnet_id", "availability_domain", "shape", "image_url", "db_secret_ocid", "db_host", "db_port", "db_user", "db_name")
        missing = [name for name in required if not str(getattr(self.settings, name)).strip()]
        if missing:
            raise ValueError("Container orchestration is missing: " + ", ".join(missing) + ".")
        if self.settings.ocpus <= 0 or self.settings.memory_gbs <= 0:
            raise ValueError("Container OCPUs and memory must both be greater than zero.")
        if not str(mapping.get("stream_id", "")).startswith("ocid1.stream."):
            raise ValueError("The mapping does not have a valid OCI Stream.")
        suffix = "-".join(assignments)
        name = f"{self.settings.name_prefix}-{mode.lower()}-p{suffix}"[:255]
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
                "image_url": self.settings.image_url,
                "is_resource_principal_disabled": False,
                "environment_variables": {
                    "OCI_STREAM_ID": str(mapping["stream_id"]), "PROCESSING_MODE": mode,
                    "EXPECTED_PARTITION_COUNT": str(stream_partitions), "CONSUMER_REPLICA_COUNT": "1",
                    "CONSUMER_PARTITIONS": ",".join(assignments), "DB_SECRET_OCID": self.settings.db_secret_ocid,
                    "DB_HOST": self.settings.db_host, "DB_PORT": self.settings.db_port,
                    "DB_USER": self.settings.db_user, "DB_NAME": self.settings.db_name,
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

    def list_deployments(self) -> list[dict[str, str]]:
        """List only Container Instances created by this application's tag."""
        oci, client = self._client()
        try:
            records = oci.pagination.list_call_get_all_results(client.list_container_instances, compartment_id=self.settings.compartment_id).data
            return [
                {"id": str(item.id), "display_name": str(item.display_name), "lifecycle_state": str(item.lifecycle_state), "mapping_id": str((getattr(item, "freeform_tags", {}) or {}).get("mapping-id", ""))}
                for item in records
                if (getattr(item, "freeform_tags", {}) or {}).get("managed-by") == "oci-object-event-2-table"
            ]
        except Exception as error:
            raise OrchestrationError(f"Could not list Container Instances: {type(error).__name__}: {error}") from error

    def create(self, *, mapping: dict[str, Any], stream_partitions: int, partition_assignment: str) -> dict[str, str]:
        spec = self.deployment_spec(mapping=mapping, stream_partitions=stream_partitions, partition_assignment=partition_assignment)
        try:
            oci, client = self._client()
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
