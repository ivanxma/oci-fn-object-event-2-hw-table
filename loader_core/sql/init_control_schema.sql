-- Idempotent control schema used by the Object Storage event processor.
-- __CONTROL_DATABASE__ is replaced by a validated, quoted identifier.

CREATE DATABASE IF NOT EXISTS __CONTROL_DATABASE__ CHARACTER SET utf8mb4;

CREATE TABLE IF NOT EXISTS __CONTROL_DATABASE__.object_storage_mappings (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    compartment_name VARCHAR(255) NOT NULL,
    bucket_name VARCHAR(255) NOT NULL,
    resource_name_pattern VARCHAR(1024) NOT NULL,
    target_database VARCHAR(64) NOT NULL,
    target_table VARCHAR(64) NOT NULL,
    worker_threads SMALLINT UNSIGNED NOT NULL DEFAULT 4,
    event_rule_id VARCHAR(255) NULL,
    stream_id VARCHAR(255) NULL,
    processing_mode ENUM('FIFO','PARALLEL') NOT NULL DEFAULT 'FIFO',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS __CONTROL_DATABASE__.target_batch_sequences (
    target_database VARCHAR(64) NOT NULL,
    target_table VARCHAR(64) NOT NULL,
    next_batch_num BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (target_database, target_table)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS __CONTROL_DATABASE__.source_object_batches (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    mapping_id BIGINT UNSIGNED NOT NULL,
    bucket_name VARCHAR(255) NOT NULL,
    resource_name VARCHAR(1024) NOT NULL,
    target_database VARCHAR(64) NOT NULL,
    target_table VARCHAR(64) NOT NULL,
    batch_num BIGINT UNSIGNED NOT NULL,
    source_key BINARY(32) NOT NULL,
    object_version VARCHAR(255) NOT NULL,
    lifecycle_state VARCHAR(20) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_source_object (mapping_id, source_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
