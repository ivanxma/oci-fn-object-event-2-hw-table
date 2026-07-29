-- Idempotent retirement of obsolete mapping and audit objects.
ALTER TABLE __CONTROL_DATABASE__.object_storage_mappings
    DROP COLUMN `invocation_mode`;

DROP TABLE IF EXISTS __CONTROL_DATABASE__.object_event;
DROP TABLE IF EXISTS __CONTROL_DATABASE__.event_tx_log;
DROP TABLE IF EXISTS __CONTROL_DATABASE__.event_errors;
