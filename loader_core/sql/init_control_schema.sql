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

CREATE TABLE IF NOT EXISTS __CONTROL_DATABASE__.deployment_history (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    component ENUM('UI','PROCESSOR') NOT NULL,
    deployment_name VARCHAR(255) NOT NULL,
    deployment_id VARCHAR(255) NULL,
    mapping_id BIGINT UNSIGNED NULL,
    release_version VARCHAR(128) NOT NULL,
    git_sha VARCHAR(128) NOT NULL,
    source_branch VARCHAR(255) NOT NULL,
    build_utc VARCHAR(64) NOT NULL,
    image_name VARCHAR(255) NOT NULL,
    image_tag VARCHAR(255) NOT NULL,
    image_digest VARCHAR(255) NULL,
    config_schema_version VARCHAR(32) NOT NULL,
    recorded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    KEY ix_deployment_history_component (component, recorded_at),
    KEY ix_deployment_history_instance (deployment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
