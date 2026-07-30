import unittest
import base64
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "processor"))

from stream_processor import assigned_partitions, capture_batch, decode_stream_message, is_expired_cursor_error, process_one, validate_mode
from vault_config import oci_signer, parse_secret_content, stream_data_database_config
from message_store import decoded_payload, ensure_schema, has_later_delete, migration_statements, processing_lease_seconds, retry_delay_seconds, schema_statements


class ProcessorModeTest(unittest.TestCase):
    def test_valid_modes(self):
        validate_mode("FIFO", 1, 1)
        validate_mode("PARALLEL", 4, 2)

    def test_invalid_modes(self):
        for values in (("FIFO", 2, 1), ("PARALLEL", 1, 1), ("PARALLEL", 2, 3)):
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    validate_mode(*values)

    def test_decodes_json_message(self):
        value = base64.b64encode(json.dumps({"eventType": "test"}).encode()).decode()
        self.assertEqual(decode_stream_message(value)["eventType"], "test")
        with self.assertRaises(ValueError):
            decode_stream_message("not base64")

    def test_decodes_durable_mysql_json_before_loader_invocation(self):
        self.assertEqual(decoded_payload('{"eventType":"test"}'), {"eventType": "test"})
        with self.assertRaisesRegex(RuntimeError, "valid JSON"):
            decoded_payload("not-json")
        with self.assertRaisesRegex(RuntimeError, "JSON object"):
            decoded_payload(["not", "an", "object"])

    def test_parses_vault_database_bundle(self):
        value = base64.b64encode(json.dumps({"host":"db","port":3306,"user":"u","credential":"p","database":"d"}).encode()).decode()
        self.assertEqual(parse_secret_content(value)["host"], "db")

    def test_rejects_unknown_oci_auth_mode(self):
        import os
        previous = os.environ.get("OCI_AUTH_MODE")
        os.environ["OCI_AUTH_MODE"] = "unknown"
        try:
            with self.assertRaisesRegex(ValueError, "OCI_AUTH_MODE"):
                oci_signer()
        finally:
            if previous is None:
                os.environ.pop("OCI_AUTH_MODE", None)
            else:
                os.environ["OCI_AUTH_MODE"] = previous

    def test_rejects_legacy_plain_text_vault_secret(self):
        encoded = base64.b64encode(b"not-json").decode()
        with self.assertRaisesRegex(ValueError, "base64 JSON"):
            parse_secret_content(encoded)

    def test_stream_data_database_can_be_separated_from_default_connection_database(self):
        self.assertEqual(
            stream_data_database_config({"database": "stream_db", "stream_data_database": "stream_data"})["database"],
            "stream_data",
        )

    def test_capture_batch_requires_valid_payload(self):
        class Message: partition = "0"; offset = 1; key = ""; value = "not-base64"
        with self.assertRaises(ValueError):
            capture_batch(None, "ocid1.stream.test", [Message()])

    def test_later_delete_is_matched_by_partition_offset_and_object(self):
        current = {
            "stream_id": "stream",
            "partition_id": "0",
            "stream_offset": 7,
            "payload": {
                "eventType": "com.oraclecloud.objectstorage.createobject",
                "data": {
                    "resourceName": "prefix/file.csv",
                    "additionalDetails": {"bucketName": "bucket"},
                },
            },
        }
        delete = {
            "eventType": "com.oraclecloud.objectstorage.deleteobject",
            "data": {
                "resourceName": "prefix/file.csv",
                "additionalDetails": {"bucketName": "bucket"},
            },
        }

        class Cursor:
            def execute(self, sql, values):
                self.sql, self.values = sql, values
            def fetchall(self):
                return [(json.dumps(delete),)]

        class Connection:
            value = Cursor()
            def cursor(self): return self.value

        connection = Connection()
        self.assertTrue(has_later_delete(connection, current))
        self.assertEqual(connection.value.values, ("stream", "0", 7))

    def test_explicit_partition_assignment(self):
        self.assertEqual(assigned_partitions("FIFO", 1, "0"), ["0"])
        self.assertEqual(assigned_partitions("PARALLEL", 4, "0,2"), ["0", "2"])
        with self.assertRaises(ValueError):
            assigned_partitions("FIFO", 1, "0,1")
        with self.assertRaises(ValueError):
            assigned_partitions("PARALLEL", 2, "2")

    def test_external_schema_sql_is_loaded_and_executed(self):
        statements = schema_statements()
        self.assertEqual(len(statements), 3)
        self.assertIn("stream_message_capture", statements[0])
        self.assertIn("next_retry_at", statements[0])
        self.assertIn("stream_partition_checkpoint", statements[1])
        class Cursor:
            def __init__(self): self.executed = []
            def execute(self, statement, _values=None): self.executed.append(statement)
            def fetchone(self): return (1,)
        class Connection:
            def __init__(self): self.value = Cursor()
            def cursor(self): return self.value
        connection = Connection()
        ensure_schema(connection)
        self.assertEqual(connection.value.executed[:3], statements)

    def test_retry_backoff_is_bounded_and_external_migration_is_loaded(self):
        self.assertEqual(retry_delay_seconds(1), 2)
        self.assertEqual(retry_delay_seconds(8), 256)
        self.assertEqual(retry_delay_seconds(999), 300)
        self.assertEqual(len(migration_statements()), 1)

    def test_processing_lease_is_bounded_for_restart_recovery(self):
        self.assertEqual(processing_lease_seconds("30"), 30)
        self.assertEqual(processing_lease_seconds("300"), 300)
        with self.assertRaises(ValueError):
            processing_lease_seconds("29")
        with self.assertRaises(ValueError):
            processing_lease_seconds("not-a-number")

    def test_recognizes_only_oci_expired_cursor_response(self):
        class ExpiredCursor(Exception):
            status = 400
            code = "InvalidParameter"
            message = "The cursor is expired. Create a new one."
        class OtherError(Exception):
            status = 401
            code = "NotAuthenticated"
            message = "Not authenticated"
        self.assertTrue(is_expired_cursor_error(ExpiredCursor()))
        self.assertFalse(is_expired_cursor_error(OtherError()))

    def test_processing_claim_is_scoped_to_stream_and_partition(self):
        import sys
        from unittest.mock import patch
        class Store:
            @staticmethod
            def claim_next(_connection, *, stream_id, partitions):
                self.assertEqual(stream_id, "ocid1.stream.test")
                self.assertEqual(partitions, ["0"])
                return None
            @staticmethod
            def complete(*_args):
                raise AssertionError("complete must not be called without a claim")
            @staticmethod
            def decoded_payload(_value):
                raise AssertionError("payload normalization must not run without a claim")
            @staticmethod
            def has_later_delete(*_args):
                raise AssertionError("supersession must not run without a claim")
            @staticmethod
            def fail(*_args):
                raise AssertionError("fail must not be called without a claim")
        with patch.dict(sys.modules, {"message_store": Store}):
            self.assertFalse(process_one(None, lambda _payload: None, stream_id="ocid1.stream.test", partitions=["0"]))

    def test_processing_boundary_normalizes_claimed_json(self):
        import sys
        from unittest.mock import patch
        received = []
        class Store:
            @staticmethod
            def claim_next(_connection, **_kwargs): return {"id": 9, "payload": '{"eventType":"test"}'}
            @staticmethod
            def decoded_payload(value): return json.loads(value) if isinstance(value, str) else value
            @staticmethod
            def has_later_delete(*_args): return False
            @staticmethod
            def complete(*_args): pass
            @staticmethod
            def fail(*_args): raise AssertionError("valid payload must not fail")
        with patch.dict(sys.modules, {"message_store": Store}):
            self.assertTrue(process_one(None, received.append, stream_id="ocid1.stream.test", partitions=["0"]))
        self.assertEqual(received, [{"eventType": "test"}])

    def test_object_not_found_is_completed_when_later_delete_supersedes_it(self):
        import sys
        from unittest.mock import patch
        calls = []

        class Missing(Exception):
            status = 404

        row = {
            "id": 9,
            "stream_id": "ocid1.stream.test",
            "partition_id": "0",
            "stream_offset": 4,
            "payload": {"eventType": "com.oraclecloud.objectstorage.createobject"},
        }

        class Store:
            @staticmethod
            def claim_next(*_args, **_kwargs): return row
            @staticmethod
            def decoded_payload(value): return value
            @staticmethod
            def has_later_delete(*_args): return True
            @staticmethod
            def complete(_connection, capture_id): calls.append(("complete", capture_id))
            @staticmethod
            def fail(*_args): raise AssertionError("superseded event must not fail")

        with patch.dict(sys.modules, {"message_store": Store}):
            self.assertTrue(
                process_one(
                    None,
                    lambda _payload: (_ for _ in ()).throw(Missing("gone")),
                    stream_id="ocid1.stream.test",
                    partitions=["0"],
                )
            )
        self.assertEqual(calls, [("complete", 9)])

    def test_object_not_found_remains_retryable_without_later_delete(self):
        import sys
        from unittest.mock import patch
        calls = []

        class Missing(Exception):
            status = 404

        class Store:
            @staticmethod
            def claim_next(*_args, **_kwargs):
                return {"id": 10, "payload": {"eventType": "com.oraclecloud.objectstorage.createobject"}}
            @staticmethod
            def decoded_payload(value): return value
            @staticmethod
            def has_later_delete(*_args): return False
            @staticmethod
            def complete(*_args): raise AssertionError("unsuperseded event must not complete")
            @staticmethod
            def fail(_connection, capture_id, _error): calls.append(("fail", capture_id))

        with patch.dict(sys.modules, {"message_store": Store}):
            self.assertFalse(
                process_one(
                    None,
                    lambda _payload: (_ for _ in ()).throw(Missing("gone")),
                    stream_id="ocid1.stream.test",
                    partitions=["0"],
                )
            )
        self.assertEqual(calls, [("fail", 10)])

    def test_loader_uses_shared_processing_path_without_fdk_response(self):
        import sys
        from types import SimpleNamespace
        from unittest.mock import patch
        from loader import process_event
        received = []
        with patch.dict(sys.modules, {"event_processor": SimpleNamespace(process_cloud_event=lambda event: received.append(event))}):
            process_event({"eventType": "test"})
        self.assertEqual(received, [{"eventType": "test"}])
