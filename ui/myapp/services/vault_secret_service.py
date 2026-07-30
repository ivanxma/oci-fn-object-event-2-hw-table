"""Read OCI Vault secret metadata for deployment configuration.

Secret list operations never read bundle content. Flow may read one selected
database bundle and returns only its non-sensitive host/port metadata; the
credential is never returned, rendered, or logged. A Container Instance receives
only the selected secret OCID and resolves its value with its resource principal.
"""
from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass


class VaultSecretError(RuntimeError):
    pass


@dataclass(frozen=True)
class VaultSecretRecord:
    id: str
    name: str
    lifecycle_state: str

@dataclass(frozen=True)
class VaultRecord:
    id: str
    name: str

@dataclass(frozen=True)
class VaultKeyRecord:
    id: str
    name: str


SECRET_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,254}$")


class VaultSecretService:
    def __init__(self, *, compartment_id: str, region: str, enabled: bool = True) -> None:
        self.compartment_id = compartment_id
        self.region = region
        self.enabled = enabled

    def _client(self):
        if not self.enabled:
            raise VaultSecretError("OCI Vault secret selection is disabled for this UI deployment.")
        if not self.compartment_id or not self.region:
            raise VaultSecretError("OCI compartment and region are required to list Vault secrets.")
        try:
            import oci
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return oci, oci.vault.VaultsClient({"region": self.region}, signer=signer)
        except Exception as error:
            raise VaultSecretError("Could not initialize OCI Vault access. Confirm the instance-principal configuration.") from error

    def _management_client(self, vault_id: str):
        """Use the selected Vault management endpoint for secret mutations."""
        try:
            import oci
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            vault_client = oci.key_management.KmsVaultClient({"region": self.region}, signer=signer)
            endpoint = vault_client.get_vault(vault_id).data.management_endpoint
            return oci, oci.vault.VaultsClient({"region": self.region}, signer=signer, service_endpoint=endpoint)
        except Exception as error:
            raise VaultSecretError("Could not initialize the selected Vault management endpoint.") from error

    def list_active_secrets(self) -> list[VaultSecretRecord]:
        try:
            oci, client = self._client()
            records = oci.pagination.list_call_get_all_results(
                client.list_secrets, compartment_id=self.compartment_id
            ).data
            return sorted(
                [
                    VaultSecretRecord(str(item.id), str(item.secret_name), str(item.lifecycle_state))
                    for item in records
                    if str(getattr(item, "lifecycle_state", "")).upper() == "ACTIVE"
                ],
                key=lambda item: (item.name.lower(), item.id),
            )
        except VaultSecretError:
            raise
        except Exception as error:
            # OCI request IDs, endpoints, and SDK responses are operational
            # diagnostics, not browser-facing UI content.  Secret metadata
            # listing needs the separate `read secrets` policy; reading the
            # bundle value alone does not grant this list operation.
            raise VaultSecretError(
                "Could not list OCI Vault secret metadata. Confirm the UI instance principal has "
                "'read secrets' permission in the selected compartment."
            ) from error

    def database_connection_metadata(self, secret_id: str) -> dict[str, str]:
        """Return only non-sensitive endpoint fields from a database JSON secret."""
        if not secret_id.startswith("ocid1.vaultsecret."):
            raise ValueError("Database secret identifier is invalid.")
        try:
            import oci
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            client = oci.secrets.SecretsClient({"region": self.region}, signer=signer)
            bundle = client.get_secret_bundle(secret_id, stage="CURRENT").data
            encoded = str(bundle.secret_bundle_content.content or "")
            payload = json.loads(base64.b64decode(encoded, validate=True))
            host = str(payload.get("host") or "").strip()
            port = str(payload.get("port") or "").strip()
            if not host or not port.isdigit() or not 1 <= int(port) <= 65535:
                raise ValueError("Database secret does not contain a valid host and port.")
            return {"host": host, "port": port}
        except ValueError:
            raise
        except Exception as error:
            raise VaultSecretError(
                "Could not read database endpoint metadata. Confirm the UI instance principal has "
                "'read secret-bundles' permission in the selected compartment."
            ) from error

    def list_active_vaults(self) -> list[VaultRecord]:
        try:
            import oci
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            client = oci.key_management.KmsVaultClient({"region": self.region}, signer=signer)
            records = oci.pagination.list_call_get_all_results(client.list_vaults, compartment_id=self.compartment_id).data
            return sorted([VaultRecord(str(item.id), str(item.display_name)) for item in records if str(getattr(item, "lifecycle_state", "")).upper() == "ACTIVE"], key=lambda item: item.name.lower())
        except Exception as error:
            raise VaultSecretError("Could not list OCI Vaults. Confirm the UI instance principal has 'read vaults' permission.") from error

    def list_active_keys(self, vault_id: str) -> list[VaultKeyRecord]:
        if not vault_id.startswith("ocid1.vault."):
            return []
        try:
            import oci
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            vault_client = oci.key_management.KmsVaultClient({"region": self.region}, signer=signer)
            endpoint = vault_client.get_vault(vault_id).data.management_endpoint
            client = oci.key_management.KmsManagementClient({"region": self.region}, signer=signer, service_endpoint=endpoint)
            records = oci.pagination.list_call_get_all_results(client.list_keys, compartment_id=self.compartment_id).data
            return sorted(
                [
                    VaultKeyRecord(str(item.id), str(item.display_name))
                    for item in records
                    if str(getattr(item, "lifecycle_state", "")).upper() == "ENABLED"
                    and str(getattr(item, "algorithm", "")).upper() == "AES"
                ],
                key=lambda item: item.name.lower(),
            )
        except Exception as error:
            raise VaultSecretError("Could not list Vault encryption keys. Confirm the UI instance principal has 'read keys' permission.") from error

    def create_database_secret(self, *, name: str, vault_id: str, key_id: str, host: str, port: str, user: str, credential: str, database: str, control_database: str, stream_data_database: str, staging_database: str) -> VaultSecretRecord:
        """Create a Vault JSON secret without retaining or returning its value."""
        name, vault_id, key_id = name.strip(), vault_id.strip(), key_id.strip()
        host, port, user, database = host.strip(), port.strip(), user.strip(), database.strip()
        control_database, stream_data_database, staging_database = control_database.strip(), stream_data_database.strip(), staging_database.strip()
        if not SECRET_NAME.fullmatch(name):
            raise ValueError("Secret name must start with a letter and use letters, digits, hyphens, or underscores.")
        if not vault_id.startswith("ocid1.vault.") or not key_id.startswith("ocid1.key."):
            raise ValueError("Choose valid OCI Vault and encryption key OCIDs.")
        if not host or not user or not database or not control_database or not stream_data_database or not staging_database or not credential:
            raise ValueError("Database host, user, credential, default, control, durable, and staging database names are required.")
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("Database port must be from 1 to 65535.")
        payload = json.dumps({"host": host, "port": int(port), "user": user, "credential": credential, "database": database, "control_database": control_database, "stream_data_database": stream_data_database, "staging_database": staging_database}, separators=(",", ":")).encode("utf-8")
        try:
            oci, client = self._client()
            content = oci.vault.models.Base64SecretContentDetails(name=f"{name}-v1", stage="CURRENT", content=base64.b64encode(payload).decode("ascii"))
            details = oci.vault.models.CreateSecretDetails(
                compartment_id=self.compartment_id, secret_name=name, vault_id=vault_id, key_id=key_id,
                description="Processor database connectivity configuration.", secret_content=content,
            )
            result = client.create_secret(details).data
            return VaultSecretRecord(str(result.id), str(result.secret_name), str(result.lifecycle_state))
        except (ValueError, VaultSecretError):
            raise
        except Exception as error:
            code = str(getattr(error, "code", "") or type(error).__name__)
            status = str(getattr(error, "status", "") or "unknown")
            raise VaultSecretError(f"Could not create OCI Vault database secret (OCI {code}, status {status}). Confirm the UI instance principal can manage secrets and use the selected Vault key.") from error

    def update_database_secret(self, *, secret_id: str, host: str, port: str, user: str, credential: str, database: str, control_database: str, stream_data_database: str, staging_database: str) -> None:
        if not secret_id.startswith("ocid1.vaultsecret."):
            raise ValueError("Choose an existing OCI Vault secret to update.")
        if not all((host.strip(), port.strip(), user.strip(), credential, database.strip(), control_database.strip(), stream_data_database.strip(), staging_database.strip())) or not port.strip().isdigit():
            raise ValueError("Complete all database connectivity fields before updating the secret.")
        payload = json.dumps({"host": host.strip(), "port": int(port), "user": user.strip(), "credential": credential, "database": database.strip(), "control_database": control_database.strip(), "stream_data_database": stream_data_database.strip(), "staging_database": staging_database.strip()}, separators=(",", ":")).encode("utf-8")
        try:
            oci, client = self._client()
            content = oci.vault.models.Base64SecretContentDetails(name="processor-db-config", stage="CURRENT", content=base64.b64encode(payload).decode("ascii"))
            client.update_secret(secret_id, oci.vault.models.UpdateSecretDetails(secret_content=content))
        except (ValueError, VaultSecretError):
            raise
        except Exception as error:
            code = str(getattr(error, "code", "") or type(error).__name__)
            status = str(getattr(error, "status", "") or "unknown")
            raise VaultSecretError(f"Could not update OCI Vault database secret (OCI {code}, status {status}). Confirm the UI instance principal can manage secrets.") from error
