-- Idempotent control schema for the Object Storage-to-Streaming mapping UI.
-- __CONTROL_DATABASE__ is replaced by a validated and quoted identifier at runtime.
CREATE DATABASE IF NOT EXISTS __CONTROL_DATABASE__ CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS __CONTROL_DATABASE__.object_storage_mappings (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    compartment_name VARCHAR(255) NOT NULL,
    bucket_name VARCHAR(255) NOT NULL,
    resource_name_pattern VARCHAR(1024) NOT NULL,
    target_database VARCHAR(64) NOT NULL,
    target_table VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    worker_threads SMALLINT UNSIGNED NOT NULL DEFAULT 4,
    event_rule_id VARCHAR(255) NULL,
    stream_id VARCHAR(255) NULL,
    processing_mode ENUM('FIFO','PARALLEL') NOT NULL DEFAULT 'FIFO',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
