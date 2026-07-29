"""OCI Streaming cursor operations used before durable message capture."""
from __future__ import annotations
from typing import Any

def client_for_stream(stream_id: str, region: str):
    import oci
    from vault_config import oci_signer
    signer = oci_signer()
    admin = oci.streaming.StreamAdminClient({"region": region}, signer=signer)
    stream = admin.get_stream(stream_id).data
    return oci, oci.streaming.StreamClient({"region": region}, stream.messages_endpoint, signer=signer)

def first_cursor(oci: Any, client: Any, stream_id: str, partition: str) -> str:
    details = oci.streaming.models.CreateCursorDetails(partition=partition, type="TRIM_HORIZON")
    return client.create_cursor(stream_id, details).data.value

def read_messages(client: Any, stream_id: str, cursor: str):
    response = client.get_messages(stream_id, cursor)
    return response.data, response.headers.get("opc-next-cursor")
