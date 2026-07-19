from contextlib import contextmanager

from function.partition_loader import log_event


class Cursor:
    def __init__(self):
        self.calls = []
        self.lastrowid = 17

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters))


class Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class Database:
    def __init__(self):
        self.cursor = Cursor()

    @contextmanager
    def connection(self):
        yield Connection(self.cursor)


def test_event_log_persists_source_mode_snapshot():
    database = Database()
    source = {
        "object_event_id": 9,
        "bucket_name": "bucket",
        "resource_name": "detached/file.csv",
        "object_version": "version",
        "invocation_mode": "DETACHED",
    }
    mapping = {
        "id": 3,
        "target_database": "target_db",
        "target_table": "target_table",
        "invocation_mode": "SYNC",
    }

    log_event(database, source, "CREATE", "SUCCESS", mapping, 4, "loaded")

    insert, parameters = database.cursor.calls[0]
    assert "invocation_mode" in insert
    assert parameters[-2] == "DETACHED"
