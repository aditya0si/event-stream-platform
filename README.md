# event-stream-platform

An event-driven telemetry pipeline in Go: a partitioned log, idempotent consumers, a durable
Postgres sink, dead-letter handling with replay, and a live browser fan-out over SSE.

## Status

**M5 complete: the live view, and a reconnect that loses nothing.** `docker compose up --build`
brings up Postgres, Redis, Redpanda, a one-shot migration container, the ingest service, the
consumer, and the SSE gateway.
`POST /v1/events` validates a batch, gives each event an identity, and publishes it to
`telemetry.raw.v1` **keyed by vehicle** — so one vehicle's events share a partition in produce
order, which is what the per-vehicle ordering guarantee
([ADR-005](docs/adr/ADR-005-ordering-and-late-data.md)) actually rests on. A consumer group reads
that log, applies each event to Postgres exactly once, commits its offsets only after the
transaction that applied them has committed, and records what it cannot apply in two places: a row
an operator can query, and a copy on `telemetry.dlq.v1` that survives the queryable store being
unavailable. `cmd/replay` puts a refusal back on the log it came from. `cmd/gateway` streams what
the consumer commits to browsers over SSE, and a reconnecting client gets exactly the frames it
missed, read from Postgres rather than reconstructed from the bus.

What exists today, and what does not:

| Working now | Not yet |
|---|---|
| `cmd/migrate` — forward-only migrations plus topic provisioning, idempotent | Viewer authentication — the browser's `EventSource` cannot set request headers, so a header key would authenticate nothing; the honest fix is a signed cookie or a short-lived stream token |
| `POST /v1/events` — batch validation, per-event rejection with the offending field, and 503 + `Retry-After` when the log is unreachable | `cmd/simulate` — the deterministic fleet producer the benchmarks will drive (M7) |
| `cmd/consumer` — a consumer group that deduplicates in the same transaction as its effects, keeps every observation in history while refusing to move current state backwards, and dead-letters what it cannot apply | Any benchmark number, which is why none appears below |
| `cmd/replay` — `list`, `show`, and `replay`, republishing an event's original bytes with provenance headers so the ordinary consumer processes it and the same deduplication guarantee applies | |
| `cmd/gateway` — SSE with `Last-Event-ID` resume, a live map viewer at `/`, a per-client buffer that sheds a slow viewer rather than stalling the fan-out, and a 503 + `Retry-After` refusal at its configured client limit | |
| The versioned envelope: an unknown `schema_version` is refused rather than guessed at, unknown fields are refused, and a producer's `event_id` survives so a retransmission stays detectable | |
| Operational surface on every process: `/healthz`, `/readyz`, `/metrics`, with dependency gauges kept fresh by a background prober | |
| CI: `gofmt`, `vet`, the migrations against a real broker, the race suite, a build, and a job that starts the whole stack and smoke-tests it — 48 checks, including a posted event applied to Postgres by the consumer, a record that cannot be decoded refused without stalling the stream, that refusal replayed, an event delivered to an already-connected browser over the bus, and a reconnect resuming exactly the missed frames | |

Three properties are decisions rather than accidents. Ingest **does not deduplicate**: it durably
records what arrived and lets the consumer's transaction refuse the second effect
([ADR-006](docs/adr/ADR-006-dedup-store.md)), because a dedupe cache at this layer would be a
second, uncoordinated dedupe — and the one that lies when it loses an entry. A
partially-acknowledged produce is reported as **failure** so the caller retries the batch: a
duplicate the pipeline provably absorbs is worth more than a silence nothing can detect. And a late
observation is **kept in history but refused by current state**
([ADR-005](docs/adr/ADR-005-ordering-and-late-data.md)): the record of what a vehicle reported is
append-only, while the map it drives cannot move backwards.

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
Postgres `5433`, Redis `6380`, Redpanda `19092` (external Kafka listener), ingest `8081`, gateway
`8082`. Open `http://localhost:8082/` for the live map.

Against dependencies you already run:

```bash
go run ./cmd/migrate all            # schema + topics; idempotent
go run ./cmd/migrate status         # what is applied, what is pending
go run ./cmd/ingest                 # DATABASE_URL is required; everything else has a default
go run ./cmd/consumer               # the consumer group; needs the broker, the database, and Redis
go run ./cmd/gateway                # the SSE gateway and the viewer; needs Postgres and Redis
```

`make` targets wrap the same commands, and the raw commands above are listed in the Makefile for
hosts without it. [`.env.example`](.env.example) documents every variable.

## Component map

Every row is implemented and running except the source, which is marked: the benchmark milestone
drives the system with `cmd/simulate` rather than by hand.

| Stage | Component |
|---|---|
| Source | `cmd/simulate` — a deterministic fleet telemetry producer over committed route geometry (M7, not yet) |
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
