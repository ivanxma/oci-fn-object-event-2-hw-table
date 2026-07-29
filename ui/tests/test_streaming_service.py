from __future__ import annotations

import base64
import unittest

from myapp.services.streaming_service import StreamingService


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


if __name__ == "__main__":
    unittest.main()
