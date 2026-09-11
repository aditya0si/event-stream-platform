# event-stream-platform

An event-driven telemetry pipeline in Go: a partitioned log, idempotent consumers, a durable
Postgres sink, dead-letter handling with replay, and a live browser fan-out over SSE.

**Status: design phase.** The design and its decisions are committed; the implementation has not
started. [`docs/DESIGN.md`](docs/DESIGN.md) states the requirements, architecture, data model, event
flows, and failure table, and [`docs/adr/`](docs/adr/) records the nine decisions behind them.

There is no `cmd/` and no `internal/` yet, and this README will not claim otherwise. It will be
replaced with measured numbers — throughput, end-to-end latency, fan-out capacity, and the caveats
that belong beside them — only once those numbers have actually been measured, in the same way its
sibling repository [`tenant-api-platform`](https://github.com/aditya0si/tenant-api-platform) reports
its own.

## What it will be

| Stage | Component |
|---|---|
| Source | `cmd/simulate` — a deterministic fleet telemetry producer over committed route geometry |
| Ingest | `cmd/ingest` — HTTP batch endpoint with validation and an OpenAPI contract |
| Log | Redpanda (`telemetry.raw.v1`, partitioned by vehicle) |
| Processing | `cmd/consumer` — a consumer group with idempotent, deduplicated processing |
| Sink | Postgres — append-only history plus current state per vehicle |
| Recovery | `cmd/replay` — dead-letter inspection and replay that preserves original metadata |
| Fan-out | `cmd/gateway` — SSE with `Last-Event-ID` resume, and a live map viewer |

## Documented guarantees, none of which are "exactly-once"

- **Delivery is at-least-once; processing is idempotent.** The composition is called
  *effectively-once*, and it is stated that carefully because "exactly-once" would be a claim this
  system cannot support ([ADR-002](docs/adr/ADR-002-delivery-semantics.md)).
- **Ordering is per-vehicle, not global.** Nothing stronger is promised
  ([ADR-005](docs/adr/ADR-005-ordering-and-late-data.md)).
- **The live view is best-effort; the durable sink is not.** A slow browser is shed and resumes from
  the log rather than stalling ingest ([ADR-004](docs/adr/ADR-004-backpressure.md),
  [ADR-009](docs/adr/ADR-009-fanout-bus.md)).

## Why a broker here, when the sibling repository has none

`tenant-api-platform` deliberately uses a Postgres outbox and no broker, because an event there must
commit atomically with the row that caused it, and a broker client cannot join that transaction. Here
the event *is* the product: nothing local must commit alongside it, and the properties that matter are
a durable partitioned log, per-key ordering, consumer groups, and replay from an offset.
[ADR-001](docs/adr/ADR-001-broker-choice.md) makes the argument in full, because a reviewer moving
between the two repositories deserves the reasoning rather than an apparent flip-flop.

## License

MIT — see [LICENSE](LICENSE).
