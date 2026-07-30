-- migration-column: worker_threads
ALTER TABLE __CONTROL_DATABASE__.object_storage_mappings
    ADD COLUMN worker_threads SMALLINT UNSIGNED NOT NULL DEFAULT 4;
