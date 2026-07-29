"""Load the consumer database bundle from OCI Vault without logging it."""
from __future__ import annotations
import base64
import json
import os
from typing import Any

REQUIRED = {"host", "port", "user", "credential", "database"}


def oci_signer():
    """Return the deployment signer; VM smoke tests opt into instance principal."""
    import oci
    mode = os.environ.get("OCI_AUTH_MODE", "resource_principal").strip().lower()
    if mode == "resource_principal":
        return oci.auth.signers.get_resource_principals_signer()
    if mode == "instance_principal":
        return oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    raise ValueError("OCI_AUTH_MODE must be resource_principal or instance_principal.")

def parse_secret_content(encoded: str) -> dict[str, Any]:
    try:
        value = json.loads(base64.b64decode(encoded).decode("utf-8"))
    except Exception as error:
        raise ValueError("Vault database secret must contain base64 JSON.") from error
    if not isinstance(value, dict) or not REQUIRED <= value.keys():
        raise ValueError("Vault database secret is missing required connection fields.")
    return value


def database_config_from_secret(secret_value: str) -> dict[str, Any]:
    """Combine a Vault secret with non-secret runtime connection values."""
    values = {
        "host": os.environ.get("DB_HOST", "").strip(),
        "port": os.environ.get("DB_PORT", "3306").strip(),
        "user": os.environ.get("DB_USER", "").strip(),
        "database": os.environ.get("DB_NAME", "").strip(),
        "credential": secret_value,
    }
    if not all(values[key] for key in REQUIRED):
        raise ValueError("The Vault secret requires DB_HOST, DB_PORT, DB_USER, and DB_NAME.")
    return values

def load_database_config() -> dict[str, Any]:
    secret_id = os.environ.get("DB_SECRET_OCID", "")
    if not secret_id.startswith("ocid1.vaultsecret."):
        raise ValueError("DB_SECRET_OCID is required.")
    try:
        import oci
        signer = oci_signer()
        client = oci.secrets.SecretsClient({}, signer=signer)
        content = client.get_secret_bundle(secret_id).data.secret_bundle_content.content
        try:
            return parse_secret_content(content)
        except ValueError:
            try:
                secret_value = base64.b64decode(content).decode("utf-8")
            except Exception as error:
                raise ValueError("Vault database secret cannot be decoded.") from error
            return database_config_from_secret(secret_value)
    except ValueError:
        raise
    except Exception as error:
        raise RuntimeError(f"Could not load database credentials from Vault: {type(error).__name__}") from error


def stream_data_database_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return the dedicated durable-stream database configuration.

    ``DB_NAME`` remains the loader/control database for compatibility with the
    existing Object Storage processing logic.  Operators can isolate retained
    stream payloads, checkpoints, and retry state with ``STREAM_DATA_DB_NAME``.
    """
    database = os.environ.get("STREAM_DATA_DB_NAME", "").strip() or str(config["database"])
    return {**config, "database": database}


def apply_database_environment(config: dict[str, Any]) -> None:
    """Provide the existing loader its expected process-local configuration."""
    for key, value in {"DB_HOST": config["host"], "DB_PORT": str(config["port"]), "DB_USER": config["user"], "DB_CREDENTIAL": config["credential"]}.items():
        os.environ[key] = value
