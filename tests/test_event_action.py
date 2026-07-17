from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parents[1] / "function"))

from func import _event_action


def test_object_event_actions():
    assert _event_action({"eventType": "com.oraclecloud.objectstorage.createobject"}) == "CREATE"
    assert _event_action({"eventType": "com.oraclecloud.objectstorage.updateobject"}) == "UPDATE"
    assert _event_action({"eventType": "com.oraclecloud.objectstorage.deleteobject"}) == "DELETE"
