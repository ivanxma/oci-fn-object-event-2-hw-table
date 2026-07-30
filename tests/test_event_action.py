from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / "loader_core"))

from event_processor import ObjectStorageRangeStream, _event_action


class EventActionTest(unittest.TestCase):
    def test_object_event_actions(self):
        self.assertEqual(_event_action({"eventType": "com.oraclecloud.objectstorage.createobject"}), "CREATE")
        self.assertEqual(_event_action({"eventType": "com.oraclecloud.objectstorage.updateobject"}), "UPDATE")
        self.assertEqual(_event_action({"eventType": "com.oraclecloud.objectstorage.deleteobject"}), "DELETE")

    def test_range_stream_preserves_head_error_during_partial_initialization(self):
        class Missing(Exception):
            status = 404

        class Client:
            @staticmethod
            def head_object(*_args):
                raise Missing("object is gone")

        with self.assertRaisesRegex(Missing, "object is gone"):
            ObjectStorageRangeStream(
                Client(), "namespace", "bucket", "object.csv",
                range_bytes=1024 * 1024,
            )


if __name__ == "__main__":
    unittest.main()
