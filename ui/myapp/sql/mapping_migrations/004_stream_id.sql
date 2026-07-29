-- migration-column: stream_id
ALTER TABLE __CONTROL_DATABASE__.object_storage_mappings
    ADD COLUMN `stream_id` VARCHAR(255) NULL;
