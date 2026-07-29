"""OCI Streaming administration and safe test-message operations."""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any


class StreamingError(RuntimeError):
    pass


@dataclass(frozen=True)
class StreamRecord:
    id: str
    name: str
    lifecycle_state: str
    partitions: int
    retention_hours: int | None
    messages_endpoint: str
    stream_pool_id: str | None


@dataclass(frozen=True)
class StreamMessage:
    partition: int
    offset: str
    key: str
    value: str
    publish_timestamp: int | None


class StreamingService:
    """All calls are server-side and use the UI Compute instance principal."""

    def __init__(self, *, compartment_id: str, region: str, enabled: bool) -> None:
        self.compartment_id, self.region, self.enabled = compartment_id.strip(), region.strip(), enabled

    def _clients(self):
        if not self.enabled:
            raise StreamingError("OCI Streaming management is disabled for this UI deployment.")
        if not self.compartment_id or not self.region:
            raise StreamingError("OCI_COMPARTMENT_ID and OCI_REGION are required for Streaming management.")
        try:
            import oci
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            return oci, oci.streaming.StreamAdminClient({"region": self.region}, signer=signer), signer
        except Exception as error:
            raise StreamingError(f"Could not initialize OCI Streaming: {type(error).__name__}: {error}") from error

    @staticmethod
    def _record(stream: Any) -> StreamRecord:
        return StreamRecord(
            id=str(stream.id), name=str(stream.name), lifecycle_state=str(stream.lifecycle_state or "UNKNOWN"),
            partitions=int(stream.partitions or 0), retention_hours=getattr(stream, "retention_in_hours", None),
            messages_endpoint=str(getattr(stream, "messages_endpoint", "") or ""), stream_pool_id=getattr(stream, "stream_pool_id", None),
        )

    def list_streams(self) -> list[StreamRecord]:
        try:
            oci, client, _signer = self._clients()
            streams = oci.pagination.list_call_get_all_results(client.list_streams, compartment_id=self.compartment_id).data
            return sorted((self._record(item) for item in streams if str(item.lifecycle_state).upper() != "DELETED"), key=lambda item: item.name.lower())
        except StreamingError:
            raise
        except Exception as error:
            raise StreamingError(f"Could not list streams: {type(error).__name__}: {error}") from error

    def create_stream(self, *, name: str, partitions: int, retention_hours: int) -> StreamRecord:
        name = name.strip()
        if not name or len(name) > 256:
            raise ValueError("Stream name is required and must be 256 characters or fewer.")
        if not 1 <= partitions <= 50:
            raise ValueError("Partition count must be from 1 to 50.")
        if not 24 <= retention_hours <= 168:
            raise ValueError("Retention must be from 24 to 168 hours.")
        try:
            oci, client, _signer = self._clients()
            details = oci.streaming.models.CreateStreamDetails(compartment_id=self.compartment_id, name=name, partitions=partitions, retention_in_hours=retention_hours, freeform_tags={"managed-by": "oci-object-event-2-table"})
            return self._record(client.create_stream(details).data)
        except (StreamingError, ValueError):
            raise
        except Exception as error:
            raise StreamingError(f"Could not create stream: {type(error).__name__}: {error}") from error

    def publish_test_message(self, *, stream_id: str, payload: str, key: str = "ui-test") -> None:
        try:
            json.loads(payload)
        except json.JSONDecodeError as error:
            raise ValueError("Test message must be valid JSON.") from error
        try:
            oci, admin, signer = self._clients()
            stream = admin.get_stream(stream_id).data
            client = oci.streaming.StreamClient({"region": self.region}, stream.messages_endpoint, signer=signer)
            entry = oci.streaming.models.PutMessagesDetailsEntry(key=base64.b64encode(key.encode()).decode(), value=base64.b64encode(payload.encode()).decode())
            client.put_messages(stream_id, oci.streaming.models.PutMessagesDetails(messages=[entry]))
        except (StreamingError, ValueError):
            raise
        except Exception as error:
            raise StreamingError(f"Could not publish test message: {type(error).__name__}: {error}") from error

    @staticmethod
    def _decode(value: str | None) -> str:
        if not value:
            return ""
        try:
            return base64.b64decode(value).decode("utf-8")
        except (ValueError, UnicodeDecodeError):
            return "[binary or malformed base64]"

    def read_messages(self, *, stream_id: str, limit: int = 25) -> list[StreamMessage]:
        """Read a bounded, non-destructive sample from every partition.

        OCI Streaming messages cannot be individually deleted or purged.  This method
        deliberately creates transient trim-horizon cursors and never stores them.
        """
        if not stream_id.startswith("ocid1.stream."):
            raise ValueError("Select a valid OCI Stream.")
        if not 1 <= limit <= 100:
            raise ValueError("Message limit must be from 1 to 100.")
        try:
            oci, admin, signer = self._clients()
            stream = admin.get_stream(stream_id).data
            client = oci.streaming.StreamClient({"region": self.region}, stream.messages_endpoint, signer=signer)
            messages: list[StreamMessage] = []
            remaining = limit
            for partition in range(int(stream.partitions or 0)):
                if remaining <= 0:
                    break
                details = oci.streaming.models.CreateCursorDetails(
                    partition=str(partition), type="TRIM_HORIZON"
                )
                cursor = client.create_cursor(stream_id, details).data.value
                result = client.get_messages(stream_id, cursor, limit=remaining).data
                for item in result:
                    messages.append(StreamMessage(
                        partition=partition,
                        offset=str(getattr(item, "offset", "")),
                        key=self._decode(getattr(item, "key", None)),
                        value=self._decode(getattr(item, "value", None)),
                        publish_timestamp=getattr(item, "publish_timestamp", None),
                    ))
                    remaining -= 1
                    if remaining <= 0:
                        break
            return messages
        except (StreamingError, ValueError):
            raise
        except Exception as error:
            raise StreamingError(f"Could not read stream messages: {type(error).__name__}: {error}") from error
