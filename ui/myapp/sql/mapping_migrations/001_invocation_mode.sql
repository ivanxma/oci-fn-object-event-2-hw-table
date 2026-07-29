-- migration-column: invocation_mode
ALTER TABLE __CONTROL_DATABASE__.object_storage_mappings
    ADD COLUMN `invocation_mode` ENUM('SYNC','DETACHED') NOT NULL DEFAULT 'SYNC';
