"""Read OCI Vault secret metadata for deployment configuration.

The UI deliberately never reads secret contents.  A Container Instance receives
only the selected secret OCID and resolves its value with its resource principal.
"""
from __future__ import annotations

from dataclasses import dataclass


class VaultSecretError(RuntimeError):
    pass


@dataclass(frozen=True)
class VaultSecretRecord:
    id: str
    name: str
    lifecycle_state: str


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
            raise VaultSecretError(f"Could not initialize OCI Vault: {type(error).__name__}: {error}") from error

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
            raise VaultSecretError(f"Could not list OCI Vault secret metadata: {type(error).__name__}: {error}") from error
