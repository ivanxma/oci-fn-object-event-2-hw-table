"""OCI Container Registry repository and image choices."""
from __future__ import annotations

from datetime import datetime, timezone
import re
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

    @staticmethod
    def _release_time(row: Any, version: str) -> tuple[str, str]:
        """Return a sortable UTC timestamp and a compact label for an image.

        OCIR normally provides ``time_created``.  Release tags are also stamped
        by publish_release.sh, so use that as a useful fallback for old OCI SDK
        responses or repositories populated before OCI supplied the metadata.
        """
        value = RegistryService._field(row, "time_created", None)
        parsed: datetime | None = value if isinstance(value, datetime) else None
        if parsed is None and value:
            try:
                parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except ValueError:
                pass
        if parsed is None:
            match = re.match(r"^processor-(\d{8}T\d{6}Z)(?:-|$)|^ui-(\d{8}T\d{6}Z)(?:-|$)", version)
            if match:
                parsed = datetime.strptime(next(item for item in match.groups() if item), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        if parsed is None:
            return "", "Timestamp unavailable"
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        parsed = parsed.astimezone(timezone.utc)
        return parsed.isoformat(), parsed.strftime("%Y-%m-%d %H:%M:%S UTC")

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

    def list_images(self, repository_name: str, *, tag_prefix: str = "processor-") -> list[dict[str, Any]]:
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
            images = []
            for row in rows:
                version = str(self._field(row, "version"))
                if (str(self._field(row, "lifecycle_state")).upper() != "AVAILABLE" or not version
                        or (tag_prefix and not version.startswith(tag_prefix))):
                    continue
                time_created, release_timestamp = self._release_time(row, version)
                images.append({
                    "id": str(self._field(row, "id")), "version": version,
                    "digest": str(self._field(row, "digest")), "time_created": time_created,
                    "release_timestamp": release_timestamp,
                    "image_url": f"{prefix}{version}" if prefix else "",
                })
            # OCI does not promise list order.  An ISO UTC timestamp sorts
            # chronologically, and version gives deterministic ordering for
            # legacy tags with no timestamp.
            return sorted(images, key=lambda image: (image["time_created"], image["version"]), reverse=True)
        except Exception as error:
            raise RegistryError(f"Could not list images in OCI repository {repository_name}: {type(error).__name__}: {error}") from error

    def delete_image(self, *, repository_name: str, image_id: str) -> None:
        """Delete one available processor or UI image from the configured repository."""
        if not repository_name.strip() or not image_id.strip():
            raise ValueError("Choose a container image to delete.")
        try:
            images = self.list_images(repository_name, tag_prefix="")
            image = next((item for item in images if item["id"] == image_id), None)
            if image is None or not (image["version"].startswith("processor-") or image["version"].startswith("ui-")):
                raise ValueError("Choose an available Processor or UI image from the configured repository.")
            self._client().delete_container_image(image_id)
        except (RegistryError, ValueError):
            raise
        except Exception as error:
            raise RegistryError(f"Could not delete OCI Container Registry image: {type(error).__name__}: {error}") from error
