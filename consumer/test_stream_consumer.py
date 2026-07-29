import unittest
import base64
import json

from stream_consumer import assigned_partitions, capture_batch, decode_stream_message, is_expired_cursor_error, process_one, validate_mode
from vault_config import database_config_from_secret, oci_signer, parse_secret_content, stream_data_database_config
from message_store import decoded_payload, ensure_schema, migration_statements, processing_lease_seconds, retry_delay_seconds, schema_statements


class ConsumerModeTest(unittest.TestCase):
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

    def test_vault_secret_uses_non_secret_runtime_values(self):
        import os
        prior = {key: os.environ.get(key) for key in ("DB_HOST", "DB_PORT", "DB_USER", "DB_NAME")}
        os.environ.update({"DB_HOST": "db", "DB_PORT": "3306", "DB_USER": "streamuser", "DB_NAME": "stream_db"})
        try:
            values = database_config_from_secret("not-rendered")
            self.assertEqual(values["host"], "db")
            self.assertEqual(values["credential"], "not-rendered")
        finally:
            for key, value in prior.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_stream_data_database_can_be_separated_from_loader_database(self):
        import os
        previous = os.environ.get("STREAM_DATA_DB_NAME")
        os.environ["STREAM_DATA_DB_NAME"] = "stream_data"
        try:
            self.assertEqual(stream_data_database_config({"database": "stream_db"})["database"], "stream_data")
        finally:
            if previous is None:
                os.environ.pop("STREAM_DATA_DB_NAME", None)
            else:
                os.environ["STREAM_DATA_DB_NAME"] = previous

    def test_capture_batch_requires_valid_payload(self):
        class Message: partition = "0"; offset = 1; key = ""; value = "not-base64"
        with self.assertRaises(ValueError):
            capture_batch(None, "ocid1.stream.test", [Message()])

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
            def complete(*_args): pass
            @staticmethod
            def fail(*_args): raise AssertionError("valid payload must not fail")
        with patch.dict(sys.modules, {"message_store": Store}):
            self.assertTrue(process_one(None, received.append, stream_id="ocid1.stream.test", partitions=["0"]))
        self.assertEqual(received, [{"eventType": "test"}])

    def test_loader_uses_shared_processing_path_without_fdk_response(self):
        import sys
        from types import SimpleNamespace
        from unittest.mock import patch
        from loader import process_event
        received = []
        with patch.dict(sys.modules, {"func": SimpleNamespace(process_cloud_event=lambda event: received.append(event))}):
            process_event({"eventType": "test"})
        self.assertEqual(received, [{"eventType": "test"}])
