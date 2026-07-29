"""Read OCI Vault secret metadata for deployment configuration.

The UI deliberately never reads secret contents.  A Container Instance receives
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

    def create_database_secret(self, *, name: str, vault_id: str, key_id: str, host: str, port: str, user: str, password: str, database: str) -> VaultSecretRecord:
        """Create a Vault JSON secret without retaining or returning its value."""
        name, vault_id, key_id = name.strip(), vault_id.strip(), key_id.strip()
        host, port, user, database = host.strip(), port.strip(), user.strip(), database.strip()
        if not SECRET_NAME.fullmatch(name):
            raise ValueError("Secret name must start with a letter and use letters, digits, hyphens, or underscores.")
        if not vault_id.startswith("ocid1.vault.") or not key_id.startswith("ocid1.key."):
            raise ValueError("Choose valid OCI Vault and encryption key OCIDs.")
        if not host or not user or not database or not password:
            raise ValueError("Database host, user, password, and database are required.")
        if not port.isdigit() or not 1 <= int(port) <= 65535:
            raise ValueError("Database port must be from 1 to 65535.")
        payload = json.dumps({"host": host, "port": int(port), "user": user, "credential": password, "database": database}, separators=(",", ":")).encode("utf-8")
        try:
            oci, client = self._client()
            content = oci.vault.models.Base64SecretContentDetails(content=base64.b64encode(payload).decode("ascii"))
            details = oci.vault.models.CreateSecretDetails(
                compartment_id=self.compartment_id, secret_name=name, vault_id=vault_id, key_id=key_id,
                description="Processor database connectivity configuration.", secret_content=content,
            )
            result = client.create_secret(details).data
            return VaultSecretRecord(str(result.id), str(result.secret_name), str(result.lifecycle_state))
        except (ValueError, VaultSecretError):
            raise
        except Exception as error:
            raise VaultSecretError("Could not create OCI Vault database secret. Confirm the UI instance principal can manage secrets and use the selected Vault key.") from error
