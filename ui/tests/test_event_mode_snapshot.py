from myapp.services.event_tx_service import EventTransactionService


class Cursor:
    def __init__(self, columns):
        self.columns = columns
        self.parameters = None

    def execute(self, _statement, parameters=None):
        self.parameters = parameters

    def fetchone(self):
        if len(self.parameters or ()) == 3:
            return (1,) if (self.parameters[1], self.parameters[2]) in self.columns else None
        if len(self.parameters or ()) == 2:
            return (1,) if self.parameters[1] == "object_event" else None
        return None


def test_transaction_mode_uses_snapshots_not_current_mapping():
    cursor = Cursor({("event_tx_log", "invocation_mode"), ("object_event", "invocation_mode")})
    service = EventTransactionService(None)

    expression = service._transaction_mode_sql(cursor, include_object_event=True)

    assert expression == "COALESCE(tx.invocation_mode, object_event.invocation_mode, 'UNKNOWN')"
    assert "object_storage_mappings" not in expression
