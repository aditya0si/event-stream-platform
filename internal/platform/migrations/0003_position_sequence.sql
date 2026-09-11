-- 0003_position_sequence.sql — keep the vehicle's own counter in the history.
--
-- The ingest envelope carries `sequence`: a monotonic counter the vehicle maintains for its own
-- reports. It is not the ordering key — event_ts is (ADR-005) — but it is the only thing that
-- can answer "did we miss a report?", because timestamps cannot: a gap in time might be a slow
-- vehicle, a dropped message, or a device that stopped transmitting, and the sequence
-- distinguishes those.
--
-- The consumer writes it, so the fan-out frame it publishes carries it, and this column is what
-- lets a REBUILT frame — one served from the durable history to a reconnecting viewer rather
-- than from the bus — carry the same field as a live one. Without it, a resumed client would
-- receive frames whose sequence is silently absent, and anything downstream that used it to
-- detect gaps would see a gap at every reconnect.
--
-- Existing rows default to 0. They are demo data from before this column existed, and 0 is the
-- honest value: the sequence of those observations was never recorded. Forward-only, per
-- ADR-011 — 0001 and 0002 are untouched.

ALTER TABLE vehicle_positions
    ADD COLUMN IF NOT EXISTS sequence bigint NOT NULL DEFAULT 0;

-- The replay path reads "everything newer than this timestamp" and orders by (event_ts, id).
-- The existing (vehicle_id, event_ts DESC) and (route_id, event_ts DESC) indexes serve the
-- per-vehicle and per-route reads; this one serves the cross-vehicle resume scan, which is the
-- only query in the system that reads the history in event-time order across all vehicles.
CREATE INDEX IF NOT EXISTS vehicle_positions_event_ts_id
    ON vehicle_positions (event_ts, event_id);
