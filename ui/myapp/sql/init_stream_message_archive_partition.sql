-- Physical archive partition table. __ARCHIVE_TABLE__ is a trusted generated identifier.
CREATE TABLE IF NOT EXISTS __ARCHIVE_TABLE__ (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  capture_id BIGINT UNSIGNED NOT NULL,
  stream_id VARCHAR(255) NOT NULL,
  partition_id VARCHAR(32) NOT NULL,
  stream_offset BIGINT NOT NULL,
  message_key TEXT NULL,
  payload JSON NOT NULL,
  status VARCHAR(32) NOT NULL,
  attempts INT UNSIGNED NOT NULL,
  last_error TEXT NULL,
  received_at DATETIME(6) NOT NULL,
  completed_at DATETIME(6) NULL,
  archived_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  UNIQUE KEY uq_archived_capture (capture_id),
  KEY ix_archive_stream_time (stream_id, archived_at),
  KEY ix_archive_status_time (status, archived_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
