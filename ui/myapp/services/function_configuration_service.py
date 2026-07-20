"""Read and update OCI Function settings without returning stored secrets."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import re
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
    load_lease_seconds: int = 120
    detached_enabled: bool = False
    db_host: str = ""
    db_port: int = 3306
    db_user: str = ""
    control_database: str = "fndb"
    db_ssl_disabled: bool = False
    queue_lease_seconds: int = 90
    queue_reorder_grace_seconds: int = 30
    queue_sync_reserve_seconds: int = 15
    queue_sync_minimum_start_seconds: int = 15
    queue_shutdown_reserve_seconds: int = 120
    queue_minimum_start_seconds: int = 180
    queue_unknown_job_seconds: int = 60
    queue_expected_mib_per_second: float = 4.0
    queue_prediction_safety_factor: float = 1.35


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


def _decimal_default(
    form: dict[str, Any], name: str, label: str, default: str, minimum: Decimal, maximum: Decimal
) -> Decimal:
    raw = form.get(name)
    try:
        value = Decimal(str(default if raw in (None, "") else raw))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"{label} must be a number.") from error
    if not value.is_finite() or value < minimum or value > maximum:
        raise ValueError(f"{label} must be from {minimum} to {maximum}.")
    return value


def _required_text(form: dict[str, Any], name: str, label: str, maximum: int) -> str:
    value = str(form.get(name) or "").strip()
    if not value:
        raise ValueError(f"{label} is required.")
    if len(value) > maximum or any(ord(character) < 32 for character in value):
        raise ValueError(f"{label} must be {maximum} characters or fewer and contain no control characters.")
    return value


def _checked(form: dict[str, Any], name: str) -> bool:
    return str(form.get(name) or "").lower() in {"1", "true", "yes", "on"}


def normalize_function_configuration(form: dict[str, Any]) -> dict[str, Any]:
    db_ssl_mode = str(form.get("db_ssl_mode") or "REQUIRED").upper()
    if db_ssl_mode not in {"REQUIRED", "DISABLED"}:
        raise ValueError("Database TLS mode must be Required or Disabled.")
    expected_mib = _decimal_default(
        form, "queue_expected_mib_per_second", "Expected queue throughput", "4", Decimal("0.1"), Decimal("1024")
    )
    safety_factor = _decimal_default(
        form, "queue_prediction_safety_factor", "Queue prediction safety factor", "1.35", Decimal("1"), Decimal("10")
    )
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
        "load_lease_seconds": _integer_default(form, "load_lease_seconds", "Load lease", 120, 30, 3600),
        "detached_enabled": _checked(form, "detached_enabled"),
        "db_host": _required_text(form, "db_host", "Database host", 255),
        "db_port": _integer_default(form, "db_port", "Database port", 3306, 1, 65535),
        "db_user": _required_text(form, "db_user", "Database user", 128),
        "control_database": _required_text(form, "control_database", "Control database", 64),
        "db_ssl_disabled": db_ssl_mode == "DISABLED",
        "queue_lease_seconds": _integer_default(form, "queue_lease_seconds", "Queue lease", 90, 30, 3600),
        "queue_reorder_grace_seconds": _integer_default(form, "queue_reorder_grace_seconds", "Queue reorder grace", 30, 0, 3600),
        "queue_sync_reserve_seconds": _integer_default(form, "queue_sync_reserve_seconds", "Sync shutdown reserve", 15, 0, 299),
        "queue_sync_minimum_start_seconds": _integer_default(form, "queue_sync_minimum_start_seconds", "Sync minimum start budget", 15, 1, 299),
        "queue_shutdown_reserve_seconds": _integer_default(form, "queue_shutdown_reserve_seconds", "Detached shutdown reserve", 120, 0, 1800),
        "queue_minimum_start_seconds": _integer_default(form, "queue_minimum_start_seconds", "Detached minimum start budget", 180, 1, 1800),
        "queue_unknown_job_seconds": _integer_default(form, "queue_unknown_job_seconds", "Unknown job estimate", 60, 1, 3600),
        "queue_expected_bytes_per_second": int(expected_mib * 1024 * 1024),
        "queue_expected_mib_per_second": float(expected_mib),
        "queue_prediction_safety_factor": float(safety_factor),
    }
    if values["memory_in_mbs"] % 64:
        raise ValueError("Memory must be a multiple of 64 MB.")
    if not re.fullmatch(r"[A-Za-z0-9_$]+", values["control_database"]):
        raise ValueError("Control database may contain only letters, digits, underscore, and dollar sign.")
    if values["db_ssl_disabled"] and str(form.get("confirm_db_ssl_disabled") or "") != "on":
        raise ValueError("Confirm that database TLS should be disabled.")
    if values["queue_sync_reserve_seconds"] + values["queue_sync_minimum_start_seconds"] >= values["sync_timeout_seconds"]:
        raise ValueError("Sync reserve plus minimum start budget must be less than the Sync timeout.")
    if values["queue_shutdown_reserve_seconds"] + values["queue_minimum_start_seconds"] >= values["detached_timeout_seconds"]:
        raise ValueError("Detached reserve plus minimum start budget must be less than the Detached timeout.")
    return values


class FunctionConfigurationService:
    """Manage global Function settings with an OCI instance principal."""

    SAFE_CONFIG_KEYS = {
        "WRITER_WORKERS": "writer_workers",
        "BATCH_ROWS": "batch_rows",
        "OBJECT_STORAGE_RANGE_BYTES": "object_storage_range_bytes",
        "OBJECT_STORAGE_READ_TIMEOUT_SECONDS": "object_storage_read_timeout_seconds",
        "LOAD_LEASE_SECONDS": "load_lease_seconds",
        "SYNC_TIMEOUT_SECONDS": "sync_timeout_seconds",
        "DETACHED_TIMEOUT_SECONDS": "detached_timeout_seconds",
        "DETACHED_ENABLED": "detached_enabled",
        "DB_HOST": "db_host",
        "DB_PORT": "db_port",
        "DB_USER": "db_user",
        "CONTROL_DATABASE": "control_database",
        "DB_SSL_DISABLED": "db_ssl_disabled",
        "QUEUE_LEASE_SECONDS": "queue_lease_seconds",
        "QUEUE_REORDER_GRACE_SECONDS": "queue_reorder_grace_seconds",
        "QUEUE_SYNC_RESERVE_SECONDS": "queue_sync_reserve_seconds",
        "QUEUE_SYNC_MINIMUM_START_SECONDS": "queue_sync_minimum_start_seconds",
        "QUEUE_SHUTDOWN_RESERVE_SECONDS": "queue_shutdown_reserve_seconds",
        "QUEUE_MINIMUM_START_SECONDS": "queue_minimum_start_seconds",
        "QUEUE_UNKNOWN_JOB_SECONDS": "queue_unknown_job_seconds",
        "QUEUE_EXPECTED_BYTES_PER_SECOND": "queue_expected_bytes_per_second",
        "QUEUE_PREDICTION_SAFETY_FACTOR": "queue_prediction_safety_factor",
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
            load_lease_seconds=int(config.get("LOAD_LEASE_SECONDS", 120)),
            image=function.image,
            time_updated=function.time_updated,
            detached_enabled=str(config.get("DETACHED_ENABLED", "false")).lower() == "true",
            db_host=str(config.get("DB_HOST", "")),
            db_port=int(config.get("DB_PORT", 3306)),
            db_user=str(config.get("DB_USER", "")),
            control_database=str(config.get("CONTROL_DATABASE", "fndb")),
            db_ssl_disabled=str(config.get("DB_SSL_DISABLED", "false")).lower() == "true",
            queue_lease_seconds=int(config.get("QUEUE_LEASE_SECONDS", 90)),
            queue_reorder_grace_seconds=int(config.get("QUEUE_REORDER_GRACE_SECONDS", 30)),
            queue_sync_reserve_seconds=int(config.get("QUEUE_SYNC_RESERVE_SECONDS", 15)),
            queue_sync_minimum_start_seconds=int(config.get("QUEUE_SYNC_MINIMUM_START_SECONDS", 15)),
            queue_shutdown_reserve_seconds=int(config.get("QUEUE_SHUTDOWN_RESERVE_SECONDS", 120)),
            queue_minimum_start_seconds=int(config.get("QUEUE_MINIMUM_START_SECONDS", 180)),
            queue_unknown_job_seconds=int(config.get("QUEUE_UNKNOWN_JOB_SECONDS", 60)),
            queue_expected_mib_per_second=float(config.get("QUEUE_EXPECTED_BYTES_PER_SECOND", 4 * 1024 * 1024)) / (1024 * 1024),
            queue_prediction_safety_factor=float(config.get("QUEUE_PREDICTION_SAFETY_FACTOR", 1.35)),
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
                value = values[form_name]
                config[environment_name] = str(value).lower() if isinstance(value, bool) else str(value)
            new_password = str(form.get("new_db_password") or "")
            if new_password:
                if len(new_password) > 1024 or any(character in new_password for character in "\r\n\x00"):
                    raise ValueError("New database password must be 1,024 characters or fewer and contain no line breaks or NUL.")
                config["DB_PASSWORD"] = new_password
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
