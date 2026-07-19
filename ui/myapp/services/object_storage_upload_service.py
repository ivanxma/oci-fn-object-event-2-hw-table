"""Mapping-scoped Object Storage test upload and cleanup operations."""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from typing import Any, BinaryIO

from werkzeug.utils import secure_filename


class ObjectStorageUploadError(RuntimeError):
    """Raised when a mapping-scoped Object Storage operation fails."""


@dataclass(frozen=True)
class ObjectRecord:
    name: str
    size: int
    etag: str
    time_created: Any
    time_modified: Any
    storage_tier: str


def default_folder(resource_pattern: str) -> str:
    """Return the literal folder portion of a mapping pattern when available."""
    folder = resource_pattern.rsplit("/", 1)[0] if "/" in resource_pattern else ""
    return "" if any(character in folder for character in "*?[") else folder


def static_prefix(resource_pattern: str) -> str:
    """Return the literal prefix before the first shell-style wildcard."""
    wildcard_positions = [resource_pattern.find(character) for character in "*?[" if character in resource_pattern]
    return resource_pattern[: min(wildcard_positions)] if wildcard_positions else resource_pattern


def object_name_for_upload(*, folder: str, filename: str, resource_pattern: str) -> str:
    folder = folder.strip().strip("/")
    if "\\" in folder or any(part in {".", ".."} for part in folder.split("/") if part):
        raise ValueError("Folder must be a relative Object Storage prefix without . or .. path segments.")
    filename = secure_filename(filename or "")
    if not filename or not filename.lower().endswith(".csv"):
        raise ValueError("Choose a CSV file with a .csv extension.")
    object_name = f"{folder}/{filename}" if folder else filename
    if not fnmatch.fnmatchcase(object_name, resource_pattern):
        raise ValueError(
            f"Object name {object_name!r} does not match this mapping's resource pattern {resource_pattern!r}."
        )
    return object_name


class ObjectStorageUploadService:
    """Use the UI host instance principal for mapping-scoped object operations."""

    def __init__(self, *, region: str, compartment_id: str, namespace: str = "") -> None:
        self.region = region.strip()
        self.compartment_id = compartment_id.strip()
        self.namespace = namespace.strip()

    def _client(self):
        if not self.region or not self.compartment_id:
            raise ObjectStorageUploadError("OCI_REGION and OCI_COMPARTMENT_ID are required for Object Storage upload.")
        try:
            import oci

            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            client = oci.object_storage.ObjectStorageClient({"region": self.region}, signer=signer)
            namespace = self.namespace or client.get_namespace(compartment_id=self.compartment_id).data
            return oci, client, namespace
        except Exception as error:
            raise ObjectStorageUploadError(
                f"Could not initialize the OCI Object Storage client: {type(error).__name__}: {error}"
            ) from error

    def upload_csv(self, *, mapping: dict[str, Any], folder: str, filename: str, stream: BinaryIO) -> str:
        object_name = object_name_for_upload(
            folder=folder,
            filename=filename,
            resource_pattern=str(mapping["resource_name_pattern"]),
        )
        try:
            _oci, client, namespace = self._client()
            stream.seek(0)
            client.put_object(
                namespace_name=namespace,
                bucket_name=str(mapping["bucket_name"]),
                object_name=object_name,
                put_object_body=stream,
                content_type="text/csv",
            )
            return object_name
        except (ObjectStorageUploadError, ValueError):
            raise
        except Exception as error:
            raise ObjectStorageUploadError(
                f"Could not upload {object_name!r} to OCI Object Storage: {type(error).__name__}: {error}"
            ) from error

    def list_mapping_objects(self, mapping: dict[str, Any], *, maximum: int = 500) -> list[ObjectRecord]:
        pattern = str(mapping["resource_name_pattern"])
        try:
            _oci, client, namespace = self._client()
            response = client.list_objects(
                namespace_name=namespace,
                bucket_name=str(mapping["bucket_name"]),
                prefix=static_prefix(pattern),
                fields="name,size,etag,timeCreated,timeModified,storageTier",
                limit=1000,
            )
            records = []
            for item in response.data.objects:
                if fnmatch.fnmatchcase(item.name, pattern):
                    records.append(
                        ObjectRecord(
                            name=item.name,
                            size=int(item.size or 0),
                            etag=str(item.etag or ""),
                            time_created=item.time_created,
                            time_modified=item.time_modified,
                            storage_tier=str(item.storage_tier or ""),
                        )
                    )
                if len(records) >= maximum:
                    break
            return records
        except ObjectStorageUploadError:
            raise
        except Exception as error:
            raise ObjectStorageUploadError(
                f"Could not list matching Object Storage files: {type(error).__name__}: {error}"
            ) from error

    def delete_objects(self, *, mapping: dict[str, Any], object_names: list[str]) -> int:
        pattern = str(mapping["resource_name_pattern"])
        if not object_names:
            raise ValueError("Select at least one Object Storage file.")
        if len(object_names) > 100:
            raise ValueError("Select no more than 100 Object Storage files at one time.")
        if any(not fnmatch.fnmatchcase(name, pattern) for name in object_names):
            raise ValueError("One or more selected objects are outside the mapping's resource pattern.")
        try:
            _oci, client, namespace = self._client()
            for name in dict.fromkeys(object_names):
                client.delete_object(
                    namespace_name=namespace,
                    bucket_name=str(mapping["bucket_name"]),
                    object_name=name,
                )
            return len(dict.fromkeys(object_names))
        except ObjectStorageUploadError:
            raise
        except Exception as error:
            raise ObjectStorageUploadError(
                f"Could not delete selected Object Storage files: {type(error).__name__}: {error}"
            ) from error
