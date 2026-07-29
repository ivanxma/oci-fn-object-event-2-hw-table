from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / "loader_core"))

from event_processor import _event_action


class EventActionTest(unittest.TestCase):
    def test_object_event_actions(self):
        self.assertEqual(_event_action({"eventType": "com.oraclecloud.objectstorage.createobject"}), "CREATE")
        self.assertEqual(_event_action({"eventType": "com.oraclecloud.objectstorage.updateobject"}), "UPDATE")
        self.assertEqual(_event_action({"eventType": "com.oraclecloud.objectstorage.deleteobject"}), "DELETE")


if __name__ == "__main__":
    unittest.main()
