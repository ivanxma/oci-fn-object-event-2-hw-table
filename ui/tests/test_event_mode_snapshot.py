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
            return (1,) if self.parameters[1] in {"object_event", "object_storage_mappings"} else None
        return None


def test_transaction_mode_uses_mapping_processing_mode():
    cursor = Cursor({("object_storage_mappings", "processing_mode")})
    service = EventTransactionService(None)

    expression = service._transaction_mode_sql(cursor, include_object_event=True)

    assert expression == "COALESCE((SELECT mapping.processing_mode FROM `fndb`.`object_storage_mappings` AS mapping WHERE mapping.id = tx.mapping_id), 'UNKNOWN')"


def test_completed_raw_event_does_not_fall_back_to_received():
    assert EventTransactionService._raw_event_lifecycle({"completed_at": "2026-07-19 15:00:00"}) == "COMPLETED"
    assert EventTransactionService._raw_event_lifecycle({"completed_at": None}) == "RECEIVED"
