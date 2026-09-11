-- 0001_init.sql — the sink schema.
--
-- Everything here is derived from docs/DESIGN.md § D. Three tables carry the design's
-- weight, and the constraints on them are the mechanisms behind the guarantees, not
-- documentation of them:
--
--   1. processed_events.event_id is a PRIMARY KEY. That single constraint is what makes
--      processing idempotent under at-least-once delivery (ADR-002, ADR-006). A duplicate
--      insert conflicts, inserts nothing, and the surrounding transaction applies no
--      side effect.
--
--   2. vehicle_current's upsert is guarded on event_ts (see the consumer's SQL), so a
--      late event updates zero rows. History keeps it; current state refuses it
--      (ADR-005).
--
--   3. vehicle_positions records partition and offset, so any row can be traced back to
--      the exact log position that produced it. Without that, "why is this vehicle
--      here?" is unanswerable.

-- The append-only history. Every accepted observation lands here at most once, and the
-- record survives every later correction because nothing updates it.
CREATE TABLE vehicle_positions (
    event_id    uuid PRIMARY KEY,
    vehicle_id  text NOT NULL,
    route_id    text NOT NULL,
    lat         double precision NOT NULL,
    lon         double precision NOT NULL,
    speed_kph   real,
    bearing_deg real,
    event_ts    timestamptz NOT NULL,
    produced_at timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now(),
    partition   int    NOT NULL,
    "offset"    bigint NOT NULL,
    CONSTRAINT vehicle_positions_lat_range CHECK (lat BETWEEN -90 AND 90),
    CONSTRAINT vehicle_positions_lon_range CHECK (lon BETWEEN -180 AND 180),
    CONSTRAINT vehicle_positions_speed_nonneg CHECK (speed_kph IS NULL OR speed_kph >= 0)
);

-- The two access patterns this table exists to serve: one vehicle's recent track, and one
-- route's recent activity. Both are (id, time DESC) because a viewer asks for "latest
-- first" and Postgres can walk a composite index backwards.
CREATE INDEX vehicle_positions_vehicle_ts ON vehicle_positions (vehicle_id, event_ts DESC);
CREATE INDEX vehicle_positions_route_ts   ON vehicle_positions (route_id, event_ts DESC);

-- Latest known state per vehicle. Derived from the history above, and maintained in the
-- same transaction, so the two can never disagree by more than an unprocessed event.
CREATE TABLE vehicle_current (
    vehicle_id    text PRIMARY KEY,
    route_id      text NOT NULL,
    lat           double precision NOT NULL,
    lon           double precision NOT NULL,
    speed_kph     real,
    bearing_deg   real,
    last_event_ts timestamptz NOT NULL,
    last_event_id uuid NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- What has been processed, by whom, and from where.
--
-- This table is the deduplication record and the audit trail in one: a conflict on the
-- primary key means "already applied", and a row's partition/offset means "handled by
-- this consumer, from this log position". Both questions come up during an incident, so
-- both are answered by the same insert.
CREATE TABLE processed_events (
    event_id     uuid PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now(),
    consumer_id  text NOT NULL,
    partition    int  NOT NULL,
    "offset"     bigint NOT NULL
);

-- Retention is bounded on purpose. The rule enforced in the consumer (and asserted by a
-- test) is that this window may never be shorter than the log's retention: pruning dedup
-- records for events the log can still replay would let a replay re-apply them.
CREATE INDEX processed_events_processed_at ON processed_events (processed_at);

-- Poison events, with enough context to debug one without opening the broker.
CREATE TABLE dead_letters (
    event_id        uuid PRIMARY KEY,
    raw_payload     jsonb NOT NULL,
    error           text NOT NULL,
    attempts        int NOT NULL,
    first_failed_at timestamptz NOT NULL,
    last_failed_at  timestamptz NOT NULL,
    topic           text NOT NULL,
    partition       int NOT NULL,
    "offset"        bigint NOT NULL,
    state           text NOT NULL DEFAULT 'dead' CHECK (state IN ('dead', 'replayed')),
    replayed_at     timestamptz,
    -- A dead letter that has been replayed must say when. The reverse (a replayed_at with
    -- no state transition) is equally impossible. Stating both directions here means no
    -- code path can produce a row that is ambiguous about its own history.
    CONSTRAINT dead_letters_replay_consistency CHECK (
        (state = 'dead'     AND replayed_at IS NULL) OR
        (state = 'replayed' AND replayed_at IS NOT NULL)
    )
);
CREATE INDEX dead_letters_state ON dead_letters (state, last_failed_at DESC);

-- Observed envelope versions. Its purpose is to make schema drift visible: if a producer
-- ships v2 while a consumer only understands v1, this table is where the mismatch shows
-- up before it becomes an incident.
CREATE TABLE schema_versions (
    schema_version int  NOT NULL,
    event_type     text NOT NULL,
    first_seen_at  timestamptz NOT NULL DEFAULT now(),
    last_seen_at   timestamptz NOT NULL DEFAULT now(),
    event_count    bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (schema_version, event_type)
);

-- A view for the reconciliation test: the two tables must agree for every vehicle the
-- consumer has handled. Kept as a view rather than a materialization because it is a
-- verification aid, not a read path.
CREATE VIEW vehicle_state_drift AS
SELECT c.vehicle_id,
       c.last_event_ts       AS current_ts,
       h.max_event_ts        AS history_ts,
       h.event_count
FROM vehicle_current c
JOIN (
    SELECT vehicle_id, max(event_ts) AS max_event_ts, count(*) AS event_count
    FROM vehicle_positions
    GROUP BY vehicle_id
) h ON h.vehicle_id = c.vehicle_id
WHERE c.last_event_ts <> h.max_event_ts;
