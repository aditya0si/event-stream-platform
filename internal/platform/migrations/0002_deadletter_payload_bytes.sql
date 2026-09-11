-- 0002_deadletter_payload_bytes.sql — store a refused event's exact bytes.
--
-- The column was `jsonb`, and that was wrong in a way that only shows up on the path the
-- table exists to serve. Two reasons, either of which is sufficient:
--
--   1. jsonb rejects input that is not valid JSON. A payload that failed to *decode* is
--      precisely the case that reaches this table, so the column would refuse to store the
--      events it was built for — the insert would fail and the refusal could not be recorded
--      at all.
--
--   2. jsonb normalises what it stores: key order is not preserved, whitespace is not
--      preserved, duplicate keys are collapsed. A replayed event would then not be the event
--      that failed, and "replay the event unchanged" stops being true.
--
-- `bytea` preserves the bytes exactly, which is what a dead-letter record is for.
--
-- Forward-only, as ADR-011 requires: the applied 0001 is never edited, so a database that
-- already ran it reaches the same schema by the same path as a fresh one.
--
-- The conversion is safe for existing rows because it goes through the value's text form;
-- the table is empty in practice (the consumer that writes it did not exist when this
-- migration was written), and the USING clause states how a pre-existing row would convert
-- rather than leaving the migration to fail on one.

ALTER TABLE dead_letters
    ALTER COLUMN raw_payload TYPE bytea
    USING convert_to(raw_payload::text, 'UTF8');

-- The reason a refusal happened, kept beside the error text so that "why did this die" is
-- answerable by category as well as by reading a message. It matches the metric label
-- (decode | unknown_version | retries_exhausted), so a dashboard and a query agree.
ALTER TABLE dead_letters
    ADD COLUMN IF NOT EXISTS reason text NOT NULL DEFAULT 'unknown'
    CHECK (reason IN ('decode', 'unknown_version', 'retries_exhausted', 'unknown'));

-- Replay looks for what is still dead, newest last. The existing index on (state,
-- last_failed_at DESC) covers it; this one serves the other question an operator asks —
-- "what failed recently, in this topic" — without scanning the table.
CREATE INDEX IF NOT EXISTS dead_letters_topic_failed
    ON dead_letters (topic, last_failed_at DESC);
