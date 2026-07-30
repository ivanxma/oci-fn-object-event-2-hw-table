ALTER TABLE stream_message_capture ADD COLUMN processor_release_stamp VARCHAR(255) NOT NULL DEFAULT 'unknown' AFTER payload;
ALTER TABLE stream_event_tx_log ADD COLUMN processor_release_stamp VARCHAR(255) NOT NULL DEFAULT 'unknown' AFTER stream_offset;
