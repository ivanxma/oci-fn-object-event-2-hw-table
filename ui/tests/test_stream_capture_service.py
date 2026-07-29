import unittest

from myapp.services.stream_capture_service import StreamCaptureService


class Cursor:
    def __init__(self, insert_rows=1, delete_rows=1):
        self.executed = []
        self.insert_rows = insert_rows
        self.delete_rows = delete_rows
        self.rowcount = 0

    def execute(self, sql, values=None):
        self.executed.append((sql, values))
        if sql.startswith("INSERT INTO"):
            self.rowcount = self.insert_rows
        elif sql.startswith("DELETE FROM"):
            self.rowcount = self.delete_rows


class Connection:
    def __init__(self, cursor):
        self.cursor_value = cursor
        self.committed = False
        self.rolled_back = False

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def cursor(self, **_kwargs): return self.cursor_value
    def commit(self): self.committed = True
    def rollback(self): self.rolled_back = True


class Mysql:
    def __init__(self, connection): self.value = connection
    def connection(self): return self.value


class StreamCaptureServiceTest(unittest.TestCase):
    def test_archive_uses_external_schema_and_only_terminal_statuses(self):
        cursor = Cursor()
        connection = Connection(cursor)
        service = StreamCaptureService(Mysql(connection), "stream_data")
        self.assertTrue(service.archive(9))
        insert_sql = next(sql for sql, _ in cursor.executed if sql.startswith("INSERT INTO"))
        delete_sql = next(sql for sql, _ in cursor.executed if sql.startswith("DELETE FROM"))
        self.assertIn("status IN ('COMPLETED','FAILED')", insert_sql)
        self.assertIn("status IN ('COMPLETED','FAILED')", delete_sql)
        self.assertTrue(connection.committed)

    def test_archive_does_not_delete_when_message_is_not_terminal(self):
        cursor = Cursor(insert_rows=0)
        connection = Connection(cursor)
        self.assertFalse(StreamCaptureService(Mysql(connection), "stream_data").archive(9))
        self.assertFalse(any(sql.startswith("DELETE FROM") for sql, _ in cursor.executed))
        self.assertTrue(connection.rolled_back)

    def test_invalid_identifiers_are_rejected_before_sql(self):
        with self.assertRaisesRegex(ValueError, "invalid"):
            StreamCaptureService(Mysql(Connection(Cursor())), "stream_data").delete_archived("0")
