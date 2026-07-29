-- Durable OCI Streaming capture and per-partition cursor checkpoint tables.
-- This script is idempotent and is run by consumer/message_store.py at startup.

CREATE TABLE IF NOT EXISTS stream_message_capture (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  stream_id VARCHAR(255) NOT NULL,
  partition_id VARCHAR(32) NOT NULL,
  stream_offset BIGINT NOT NULL,
  message_key TEXT NULL,
  payload JSON NOT NULL,
  status ENUM('CAPTURED','PROCESSING','COMPLETED','FAILED') NOT NULL DEFAULT 'CAPTURED',
  attempts INT UNSIGNED NOT NULL DEFAULT 0,
  last_error TEXT NULL,
  next_retry_at DATETIME(6) NULL,
  received_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  completed_at DATETIME(6) NULL,
  UNIQUE KEY uq_stream_message (stream_id, partition_id, stream_offset),
  KEY ix_capture_retry (status, next_retry_at, received_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS stream_partition_checkpoint (
  stream_id VARCHAR(255) NOT NULL,
  partition_id VARCHAR(32) NOT NULL,
  cursor_value MEDIUMTEXT NOT NULL,
  updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (stream_id, partition_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
