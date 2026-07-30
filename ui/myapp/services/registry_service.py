"""OCI Container Registry repository and image choices."""
from __future__ import annotations

from typing import Any

import oci


class RegistryError(RuntimeError):
    pass


class RegistryService:
    def __init__(self, *, compartment_id: str, region: str, namespace: str = "", region_key: str = "") -> None:
        self.compartment_id = compartment_id
        self.region = region
        self.namespace = namespace
        self.region_key = region_key

    def _client(self):
        try:
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return oci.artifacts.ArtifactsClient({"region": self.region}, signer=signer)
        except Exception as error:
            raise RegistryError(f"Could not authenticate to OCI Container Registry: {type(error).__name__}: {error}") from error

    def _namespace_value(self) -> str:
        if self.namespace.strip():
            return self.namespace.strip()
        try:
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return str(oci.object_storage.ObjectStorageClient({"region": self.region}, signer=signer).get_namespace().data)
        except Exception as error:
            raise RegistryError(f"Could not resolve the OCI Object Storage namespace for image references: {type(error).__name__}: {error}") from error

    @staticmethod
    def _field(value: Any, name: str, default: Any = "") -> Any:
        return value.get(name, default) if isinstance(value, dict) else getattr(value, name, default)

    def list_repositories(self) -> list[dict[str, str]]:
        try:
            client = self._client()
            rows = oci.pagination.list_call_get_all_results(
                client.list_container_repositories, compartment_id=self.compartment_id
            ).data
            return [
                {"id": str(self._field(row, "id")), "name": str(self._field(row, "display_name"))}
                for row in rows
                if str(self._field(row, "lifecycle_state")).upper() == "AVAILABLE"
            ]
        except RegistryError:
            raise
        except Exception as error:
            raise RegistryError(f"Could not list OCI Container Registry repositories: {type(error).__name__}: {error}") from error

    def list_images(self, repository_name: str) -> list[dict[str, str]]:
        if not repository_name.strip():
            return []
        try:
            client = self._client()
            rows = oci.pagination.list_call_get_all_results(
                client.list_container_images,
                compartment_id=self.compartment_id,
                repository_name=repository_name,
            ).data
            region_key = self.region_key.strip()
            namespace = self._namespace_value()
            prefix = f"{region_key}.ocir.io/{namespace}/{repository_name}:" if region_key and namespace else ""
            return [
                {
                    "id": str(self._field(row, "id")),
                    "version": str(self._field(row, "version")),
                    "digest": str(self._field(row, "digest")),
                    "image_url": f"{prefix}{self._field(row, 'version')}" if prefix and self._field(row, "version") else "",
                }
                for row in rows
                if str(self._field(row, "lifecycle_state")).upper() == "AVAILABLE"
                and self._field(row, "version")
                and not str(self._field(row, "version")).startswith("ui-")
            ]
        except Exception as error:
            raise RegistryError(f"Could not list images in OCI repository {repository_name}: {type(error).__name__}: {error}") from error
