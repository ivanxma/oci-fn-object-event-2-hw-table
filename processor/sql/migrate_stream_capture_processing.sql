-- Idempotent migration applied only when the processing lease column is absent.
ALTER TABLE stream_message_capture
  ADD COLUMN processing_started_at DATETIME(6) NULL AFTER next_retry_at;
