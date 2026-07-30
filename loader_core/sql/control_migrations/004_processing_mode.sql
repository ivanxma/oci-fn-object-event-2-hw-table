-- migration-column: processing_mode
ALTER TABLE __CONTROL_DATABASE__.object_storage_mappings
    ADD COLUMN processing_mode ENUM('FIFO','PARALLEL') NOT NULL DEFAULT 'FIFO';
