"""Read and update non-secret OCI Function capacity settings."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class FunctionConfigurationError(RuntimeError):
    """Raised when Function configuration cannot be read or updated."""


@dataclass(frozen=True)
class FunctionConfiguration:
    display_name: str
    lifecycle_state: str
    shape: str
    memory_in_mbs: int
    sync_timeout_seconds: int
    detached_timeout_seconds: int
    provisioned_concurrency: int
    writer_workers: int
    batch_rows: int
    object_storage_range_bytes: int
    object_storage_read_timeout_seconds: int
    image: str
    time_updated: Any
    queue_lease_seconds: int = 90
    queue_reorder_grace_seconds: int = 30
    queue_shutdown_reserve_seconds: int = 120
    queue_minimum_start_seconds: int = 180


def _integer(form: dict[str, Any], name: str, label: str, minimum: int, maximum: int | None = None) -> int:
    try:
        value = int(form.get(name, ""))
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label} must be a whole number.") from error
    if value < minimum or (maximum is not None and value > maximum):
        limit = f" from {minimum} to {maximum}" if maximum is not None else f" at least {minimum}"
        raise ValueError(f"{label} must be{limit}.")
    return value


def _integer_default(form: dict[str, Any], name: str, label: str, default: int, minimum: int, maximum: int) -> int:
    values = dict(form)
    if values.get(name) in (None, ""):
        values[name] = str(default)
    return _integer(values, name, label, minimum, maximum)


def normalize_function_configuration(form: dict[str, Any]) -> dict[str, int]:
    values = {
        "memory_in_mbs": _integer(form, "memory_in_mbs", "Memory", 128, 3072),
        "sync_timeout_seconds": _integer(form, "sync_timeout_seconds", "Sync timeout", 1, 300),
        "detached_timeout_seconds": _integer(form, "detached_timeout_seconds", "Detached timeout", 5, 3600),
        "provisioned_concurrency": _integer(form, "provisioned_concurrency", "Provisioned concurrency", 0),
        "writer_workers": _integer(form, "writer_workers", "Default writer workers", 1, 64),
        "batch_rows": _integer(form, "batch_rows", "Batch rows", 100, 100000),
        "object_storage_range_bytes": _integer(
            form, "object_storage_range_bytes", "Object Storage range bytes", 1048576, 268435456
        ),
        "object_storage_read_timeout_seconds": _integer(
            form, "object_storage_read_timeout_seconds", "Object Storage read timeout", 1, 300
        ),
        "queue_lease_seconds": _integer_default(form, "queue_lease_seconds", "Queue lease", 90, 30, 600),
        "queue_reorder_grace_seconds": _integer_default(form, "queue_reorder_grace_seconds", "Queue reorder grace", 30, 0, 300),
        "queue_shutdown_reserve_seconds": _integer_default(form, "queue_shutdown_reserve_seconds", "Queue shutdown reserve", 120, 30, 600),
        "queue_minimum_start_seconds": _integer_default(form, "queue_minimum_start_seconds", "Queue minimum start budget", 180, 15, 900),
    }
    if values["memory_in_mbs"] % 64:
        raise ValueError("Memory must be a multiple of 64 MB.")
    return values


class FunctionConfigurationService:
    """Manage global Function settings with an OCI instance principal."""

    SAFE_CONFIG_KEYS = {
        "WRITER_WORKERS": "writer_workers",
        "BATCH_ROWS": "batch_rows",
        "OBJECT_STORAGE_RANGE_BYTES": "object_storage_range_bytes",
        "OBJECT_STORAGE_READ_TIMEOUT_SECONDS": "object_storage_read_timeout_seconds",
        "SYNC_TIMEOUT_SECONDS": "sync_timeout_seconds",
        "DETACHED_TIMEOUT_SECONDS": "detached_timeout_seconds",
        "QUEUE_LEASE_SECONDS": "queue_lease_seconds",
        "QUEUE_REORDER_GRACE_SECONDS": "queue_reorder_grace_seconds",
        "QUEUE_SHUTDOWN_RESERVE_SECONDS": "queue_shutdown_reserve_seconds",
        "QUEUE_MINIMUM_START_SECONDS": "queue_minimum_start_seconds",
    }

    def __init__(self, *, function_id: str, region: str) -> None:
        self.function_id = function_id.strip()
        self.region = region.strip()

    def _client(self):
        if not self.function_id or not self.region:
            raise FunctionConfigurationError("OCI_FUNCTION_ID and OCI_REGION are required.")
        try:
            import oci

            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return oci, oci.functions.FunctionsManagementClient({"region": self.region}, signer=signer)
        except Exception as error:
            raise FunctionConfigurationError(
                f"Could not initialize the OCI Functions client: {type(error).__name__}: {error}"
            ) from error

    @staticmethod
    def _configuration(function: Any) -> FunctionConfiguration:
        config = function.config or {}
        concurrency = function.provisioned_concurrency_config
        count = int(getattr(concurrency, "count", 0) or 0)
        return FunctionConfiguration(
            display_name=function.display_name,
            lifecycle_state=function.lifecycle_state,
            shape=function.shape,
            memory_in_mbs=int(function.memory_in_mbs),
            sync_timeout_seconds=int(function.timeout_in_seconds),
            detached_timeout_seconds=int(function.detached_mode_timeout_in_seconds),
            provisioned_concurrency=count,
            writer_workers=int(config.get("WRITER_WORKERS", 4)),
            batch_rows=int(config.get("BATCH_ROWS", 10000)),
            object_storage_range_bytes=int(config.get("OBJECT_STORAGE_RANGE_BYTES", 33554432)),
            object_storage_read_timeout_seconds=int(config.get("OBJECT_STORAGE_READ_TIMEOUT_SECONDS", 300)),
            image=function.image,
            time_updated=function.time_updated,
            queue_lease_seconds=int(config.get("QUEUE_LEASE_SECONDS", 90)),
            queue_reorder_grace_seconds=int(config.get("QUEUE_REORDER_GRACE_SECONDS", 30)),
            queue_shutdown_reserve_seconds=int(config.get("QUEUE_SHUTDOWN_RESERVE_SECONDS", 120)),
            queue_minimum_start_seconds=int(config.get("QUEUE_MINIMUM_START_SECONDS", 180)),
        )

    def get(self) -> FunctionConfiguration:
        try:
            _oci, client = self._client()
            return self._configuration(client.get_function(self.function_id).data)
        except FunctionConfigurationError:
            raise
        except Exception as error:
            raise FunctionConfigurationError(
                f"Could not read OCI Function configuration: {type(error).__name__}: {error}"
            ) from error

    def update(self, form: dict[str, Any]) -> FunctionConfiguration:
        values = normalize_function_configuration(form)
        try:
            oci, client = self._client()
            current = client.get_function(self.function_id).data
            config = dict(current.config or {})
            for environment_name, form_name in self.SAFE_CONFIG_KEYS.items():
                config[environment_name] = str(values[form_name])
            concurrency = (
                oci.functions.models.NoneProvisionedConcurrencyConfig(strategy="NONE")
                if values["provisioned_concurrency"] == 0
                else oci.functions.models.ConstantProvisionedConcurrencyConfig(
                    strategy="CONSTANT", count=values["provisioned_concurrency"]
                )
            )
            details = oci.functions.models.UpdateFunctionDetails(
                memory_in_mbs=values["memory_in_mbs"],
                timeout_in_seconds=values["sync_timeout_seconds"],
                detached_mode_timeout_in_seconds=values["detached_timeout_seconds"],
                provisioned_concurrency_config=concurrency,
                config=config,
            )
            updated = client.update_function(self.function_id, details).data
            return self._configuration(updated)
        except (FunctionConfigurationError, ValueError):
            raise
        except Exception as error:
            raise FunctionConfigurationError(
                f"Could not update OCI Function configuration: {type(error).__name__}: {error}"
            ) from error
