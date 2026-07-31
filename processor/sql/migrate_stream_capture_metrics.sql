-- Per-event throughput and phase metrics populated by the Processor.
ALTER TABLE stream_message_capture
  ADD COLUMN rows_affected BIGINT UNSIGNED NULL AFTER attempts,
  ADD COLUMN object_size_bytes BIGINT UNSIGNED NULL AFTER rows_affected,
  ADD COLUMN loader_duration_ms DECIMAL(15,3) NULL AFTER object_size_bytes,
  ADD COLUMN exchange_duration_ms DECIMAL(15,3) NULL AFTER loader_duration_ms;

ALTER TABLE stream_event_tx_log
  ADD COLUMN rows_affected BIGINT UNSIGNED NULL AFTER attempts,
  ADD COLUMN object_size_bytes BIGINT UNSIGNED NULL AFTER rows_affected,
  ADD COLUMN loader_duration_ms DECIMAL(15,3) NULL AFTER object_size_bytes,
  ADD COLUMN exchange_duration_ms DECIMAL(15,3) NULL AFTER loader_duration_ms;
