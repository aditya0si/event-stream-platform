# event-stream-platform

An event-driven telemetry pipeline in Go: a partitioned log, idempotent consumers, a durable
Postgres sink, dead-letter handling with replay, and a live browser fan-out over SSE.

## Status

**M1 complete: the stack runs.** `docker compose up --build` brings up Postgres, Redis, Redpanda,
a one-shot migration container, and the ingest service; the migration applies the schema and creates
both topics with the partition counts the design requires (6 and 3, asserted rather than assumed).

What exists today, and what does not:

| Working now | Not yet |
|---|---|
| `cmd/migrate` — forward-only migrations plus topic provisioning, idempotent | `cmd/consumer` — the consumer group (M3) |
| `cmd/ingest` — operational surface: `/healthz`, `/readyz`, `/metrics` | `/v1/events` — the ingest endpoint itself (M2) |
| The event envelope and its schema versioning | `cmd/replay` — dead-letter inspection (M4) |
| Dependency health as metrics (`db_up`, `redis_up`, `broker_up`), kept fresh by a background prober | `cmd/gateway` — SSE fan-out and the viewer (M5) |
| CI: `gofmt`, `vet`, the migrations against a real broker, the suite, a build, and a job that starts the whole stack and smoke-tests it | Any benchmark number, which is why none appears below |

The design documents came first: [`docs/DESIGN.md`](docs/DESIGN.md) states the requirements,
architecture, data model, event flows, failure table, and explicit non-goals, and
[`docs/adr/`](docs/adr/) records the nine decisions behind them. This README will carry measured
numbers — throughput, end-to-end latency, fan-out capacity, and the caveats that belong beside
them — only once they have actually been measured, in the same way its sibling repository
[`tenant-api-platform`](https://github.com/aditya0si/tenant-api-platform) reports its own.

## Running it

```bash
docker compose up --build
```

That is the whole setup. Ports are offset from the sibling project so both stacks can run at once:
Postgres `5433`, Redis `6380`, Redpanda `19092` (external Kafka listener), ingest `8081`.

Against dependencies you already run:

```bash
go run ./cmd/migrate all            # schema + topics; idempotent
go run ./cmd/migrate status         # what is applied, what is pending
go run ./cmd/ingest                 # DATABASE_URL is required; everything else has a default
```

`make` targets wrap the same commands, and the raw commands above are listed in the Makefile for
hosts without it. [`.env.example`](.env.example) documents every variable.

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
