-- migration-column: event_rule_id
ALTER TABLE __CONTROL_DATABASE__.object_storage_mappings
    ADD COLUMN `event_rule_id` VARCHAR(255) NULL;
