from __future__ import annotations

import base64
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from myapp.services.streaming_service import StreamingError, StreamingService


class StreamingServiceTest(unittest.TestCase):
    def test_decode_handles_utf8_and_invalid_content(self) -> None:
        self.assertEqual(StreamingService._decode(base64.b64encode(b'{"ok":true}').decode()), '{"ok":true}')
        self.assertEqual(StreamingService._decode("not base64"), "[binary or malformed base64]")

    def test_read_rejects_untrusted_stream_identifier_and_unbounded_limit(self) -> None:
        service = StreamingService(compartment_id="ocid1.compartment.test", region="uk-london-1", enabled=True)
        with self.assertRaisesRegex(ValueError, "valid OCI Stream"):
            service.read_messages(stream_id="wrong", limit=1)
        with self.assertRaisesRegex(ValueError, "Message limit"):
            service.read_messages(stream_id="ocid1.stream.test", limit=101)

    def test_list_wraps_oci_failure_without_exposing_sdk_details(self) -> None:
        service = StreamingService(compartment_id="ocid1.compartment.test", region="uk-london-1", enabled=True)
        oci = SimpleNamespace(pagination=SimpleNamespace(list_call_get_all_results=lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("sdk failure"))))
        with patch.object(service, "_clients", return_value=(oci, SimpleNamespace(list_streams=object()), None)):
            with self.assertRaisesRegex(StreamingError, "Could not list streams: RuntimeError"):
                service.list_streams()

    def test_disabled_service_fails_before_any_oci_call(self) -> None:
        with self.assertRaisesRegex(StreamingError, "disabled"):
            StreamingService(compartment_id="ocid1.compartment.test", region="uk-london-1", enabled=False).list_streams()


if __name__ == "__main__":
    unittest.main()
