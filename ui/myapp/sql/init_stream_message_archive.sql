-- Registry of physical durable-message archive partitions.
CREATE TABLE IF NOT EXISTS stream_message_archive_partitions (
  partition_name VARCHAR(32) NOT NULL PRIMARY KEY,
  granularity ENUM('YEAR','MONTH','WEEK') NOT NULL,
  period_key VARCHAR(16) NOT NULL,
  table_name VARCHAR(64) NOT NULL UNIQUE,
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  KEY ix_archive_partition_period (granularity, period_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
