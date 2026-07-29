-- Idempotent migration applied only when the retry column is absent.
-- Existing FAILED rows are eligible immediately after the migration, then use
-- the consumer's exponential retry schedule after their next failed attempt.
ALTER TABLE stream_message_capture
  ADD COLUMN next_retry_at DATETIME(6) NULL AFTER last_error;
