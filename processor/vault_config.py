"""Load the processor database bundle from OCI Vault without logging it."""
from __future__ import annotations
import base64
import json
import os
from typing import Any

REQUIRED = {"host", "port", "user", "credential", "database", "control_database", "stream_data_database", "staging_database"}
OPTIONAL_DATABASES = set()


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
    if any(not isinstance(value.get(key), str) or not value[key].strip() for key in ("database", "control_database", "stream_data_database", "staging_database")):
        raise ValueError("Vault database secret contains an invalid database name.")
    names = {str(value[key]).strip() for key in ("database", "control_database", "stream_data_database", "staging_database")}
    if len(names) != 4:
        raise ValueError("Vault database secret requires separate default, control, stream-data, and staging databases.")
    return value


def load_database_config() -> dict[str, Any]:
    secret_id = os.environ.get("DB_SECRET_OCID", "")
    if not secret_id.startswith("ocid1.vaultsecret."):
        raise ValueError("DB_SECRET_OCID is required.")
    try:
        import oci
        signer = oci_signer()
        client = oci.secrets.SecretsClient({}, signer=signer)
        content = client.get_secret_bundle(secret_id).data.secret_bundle_content.content
        return parse_secret_content(content)
    except ValueError:
        raise
    except Exception as error:
        raise RuntimeError(f"Could not load database credentials from Vault: {type(error).__name__}") from error


def stream_data_database_config(config: dict[str, Any]) -> dict[str, Any]:
    """Return the dedicated durable-stream database configuration.

    The complete database configuration is mastered by the selected JSON Vault
    secret. Durable data uses ``stream_data_database`` when supplied.
    """
    database = str(config.get("stream_data_database") or config["database"])
    return {**config, "database": database}


def apply_database_environment(config: dict[str, Any]) -> None:
    """Provide the existing loader its expected process-local configuration."""
    values = {"DB_HOST": config["host"], "DB_PORT": str(config["port"]), "DB_USER": config["user"], "DB_CREDENTIAL": config["credential"]}
    if config.get("control_database"):
        values["CONTROL_DATABASE"] = str(config["control_database"])
    if config.get("staging_database"):
        values["STAGING_DATABASE"] = str(config["staging_database"])
    for key, value in values.items():
        os.environ[key] = value
