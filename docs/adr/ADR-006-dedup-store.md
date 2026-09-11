# ADR-006: Deduplication is a Postgres primary key, in the same transaction as the effect

Status: accepted

## Context

Idempotent processing needs somewhere to record that an event was already handled (ADR-002). The
failure mode of that record *is* the system's correctness: if the record can be forgotten, the effect
can be applied twice; if it can be written without the effect, the event can be lost. There is no
neutral choice of storage — each option fails in a direction.

## Decision

Deduplicate in **Postgres**, with `processed_events.event_id` as a **PRIMARY KEY**, inserted with
`ON CONFLICT DO NOTHING` **inside the same transaction as every other effect** of handling that
event.

The transaction is the unit of idempotency. If it commits, the event was applied once and the dedup
record is durable alongside it. If it rolls back, nothing happened at all — no dedup record, no
effects — and the event is redelivered and retried safely.

```
BEGIN
  INSERT INTO processed_events (event_id, consumer_id, partition, "offset")
    VALUES (...) ON CONFLICT DO NOTHING
    -- 0 rows inserted => this event was already applied. COMMIT and count it as a duplicate.
  INSERT INTO vehicle_positions (...) ON CONFLICT DO NOTHING
  UPDATE vehicle_current ... WHERE excluded.last_event_ts > vehicle_current.last_event_ts
  UPDATE schema_versions ...
COMMIT
```

## Alternatives considered

| Option | Why not |
|---|---|
| **Redis `SETNX` on `event_id`** | The classic choice, and the one with the worst failure mode. Redis loses keys on eviction or restart, and the system then silently re-applies an event. Worse, the dedup state and the effect live in different systems, so no transaction can bind them: a crash between "set the key" and "write the effect" leaves the two disagreeing in one direction or the other, permanently. |
| **A `seen_events` table written in its own transaction, before the effects** | Introduces a window where an event is marked seen but not applied; a crash inside that window loses the event forever. The dedup record must share the effect's fate, which is exactly what one transaction provides. |
| **A Bloom filter** | Space-efficient and *approximately* correct — which, for an invariant, is a way of saying incorrect. Persisting it across restarts makes it a worse table with a false-positive rate attached. |

## Consequences

**Good.** "An event is applied at most once" is enforced by the same engine that stores the data, in
the strongest form it offers. No window, no eviction hazard, no cross-system protocol, and replay
inherits the same protection (a replayed event keeps its `event_id`).

**Bad.** Every event costs one dedup write, and the table grows with the stream. Both are bounded by
policy rather than hope: retention is documented; pruning may never outrun log retention (asserted by
a test, so tuning one constant cannot silently break the other); and the lookup is a single b-tree
probe on the primary key.

**If it ever becomes the bottleneck** — which would be a measurement from M7, not a guess — the first
remedy is partitioning the table by time, and the second is moving to a per-partition table so
writers contend less. Recorded here so the decision has a documented starting point instead of a
panic.
