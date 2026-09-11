# ADR-005: Per-vehicle ordering, and what "late" means

Status: accepted

## Context

Telemetry arrives out of order. A vehicle loses signal, buffers reports, and uploads them late; a
retransmission carries an older timestamp than an event already processed; two gateways relay the
same device's reports with different latencies. A viewer that renders positions must decide what
happens when an older report arrives after a newer one. A system that does not decide will still
behave — just unpredictably, which for a map means a vehicle that occasionally jumps backwards.

## Decision

**Ordering guarantee: per-vehicle, and nothing stronger.** The partition key is `vehicle_id`, so all
of a vehicle's events land on one partition in arrival order. Across vehicles there is no ordering
whatsoever, and the README says so rather than implying a global guarantee it cannot keep.

Note that "arrival order within a partition" is *not* "observation order." The payload carries
`event_ts` for the latter, and the two genuinely disagree — that disagreement is the subject of this
ADR.

**The late-data rule: an event whose `event_ts` is not newer than the vehicle's current state is
preserved in history and not applied to current state.**

- `vehicle_positions` (history) accepts it. The record of what a vehicle reported is append-only and
  ordered by arrival, not by observation; discarding a late report would corrupt the historical
  record to protect a derived view.
- `vehicle_current` (state) rejects it. The upsert carries
  `WHERE excluded.last_event_ts > vehicle_current.last_event_ts`; a late event updates zero rows and
  increments `consumer_events_total{result="late"}`.

The rule lives in the database, not in application code, so it holds for every writer — including
`cmd/replay` and any future one.

## Why not last-write-wins

"Whatever arrived most recently is the truth" is the tempting default and it fails exactly when it
matters: a retransmission of a ten-minute-old report would overwrite a current position with a stale
one, and the map would show a vehicle where it no longer is. **Arrival order is an artifact of the
network; observation time is a property of the event.** A viewer's job is to show the latter.

## Consequences

**Good.** The rule is one predicate, enforced in one place, with two testable outcomes. History stays
complete and current state stays monotonic by construction. A test drives a deliberately
out-of-order sequence and asserts both: the late event present in `vehicle_positions`, absent from
`vehicle_current`, and counted.

**Bad.** A vehicle with a genuinely skewed clock will hold its state until a report with a plausible
timestamp arrives. The ingest-side skew check (reports more than five minutes in the future are
rejected, see DESIGN.md § F) bounds the damage but does not eliminate it. This is listed as a known
limitation in the README rather than papered over.

## Explicit non-goals

No watermarks, no windowed aggregation, no event-time processing framework, no retraction protocol.
Those are streaming-engine problems, and this system's ordering rule is deliberately smaller than
Flink's. Claiming otherwise — or implying it by vocabulary — would be the overreach this document
exists to prevent.
