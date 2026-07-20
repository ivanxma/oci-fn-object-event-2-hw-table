from myapp.services.event_tx_service import EventTransactionService


class Cursor:
    def __init__(self, columns, tables=None):
        self.columns = columns
        self.tables = set(tables or {"object_event"})
        self.parameters = None

    def execute(self, _statement, parameters=None):
        self.parameters = parameters

    def fetchone(self):
        if len(self.parameters or ()) == 3:
            return (1,) if (self.parameters[1], self.parameters[2]) in self.columns else None
        if len(self.parameters or ()) == 2:
            return (1,) if self.parameters[1] in self.tables else None
        return None


def test_transaction_mode_uses_snapshots_not_current_mapping():
    cursor = Cursor({("event_tx_log", "invocation_mode"), ("object_event", "invocation_mode")})
    service = EventTransactionService(None)

    expression = service._transaction_mode_sql(cursor, include_object_event=True)

    assert expression == "COALESCE(tx.invocation_mode, object_event.invocation_mode, 'UNKNOWN')"
    assert "object_storage_mappings" not in expression


def test_completed_raw_event_does_not_fall_back_to_received():
    assert EventTransactionService._raw_event_lifecycle({"completed_at": "2026-07-19 15:00:00"}) == "COMPLETED"
    assert EventTransactionService._raw_event_lifecycle({"completed_at": None}) == "RECEIVED"


def test_event_tx_timing_prefers_the_matching_queue_attempt():
    cursor = Cursor(set(), {"object_event", "event_work_queue", "queue_attempt"})

    projection, joins = EventTransactionService(None)._event_timing_sql(cursor)

    assert "COALESCE(execution_attempt.duration_ms, object_event.duration_ms)" in projection
    assert "execution_attempt.attempt_number AS event_attempt_number" in projection
    assert "'ATTEMPT'" in projection
    assert "queue_attempt" in joins
    assert "attempt.started_at <= DATE_ADD(tx.created_at, INTERVAL 1 SECOND)" in joins
    assert "attempt.status = 'SUCCESS'" in joins
