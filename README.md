# event-stream-platform

An event-driven telemetry pipeline in Go: a partitioned log, idempotent consumers, a durable
Postgres sink, dead-letter handling with replay, and a live browser fan-out over SSE.

## Status

**M7 complete: the pipeline is measured, and the numbers are in this file.** `docker compose up
--build` brings up Postgres, Redis, Redpanda, a one-shot migration container, the ingest service,
the consumer, and the SSE gateway; `docker compose --profile demo up -d simulate` adds a
deterministic fleet so the viewer at `http://localhost:8082/` has something to draw.
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
| `POST /v1/events` — batch validation, per-event rejection with the offending field, and 503 + `Retry-After` when the log is unreachable | Viewer authentication — the browser's `EventSource` cannot set request headers, so the honest fix is a signed cookie or a short-lived token |
| `cmd/consumer` — a consumer group that deduplicates in the same transaction as its effects, keeps every observation in history while refusing to move current state backwards, and dead-letters what it cannot apply | A second gateway process, so NFR-3 above 1,000 clients is tested rather than assumed |
| `cmd/replay` — `list`, `show`, and `replay`, republishing an event's original bytes with provenance headers so the ordinary consumer processes it and the same deduplication guarantee applies | |
| `cmd/gateway` — SSE with `Last-Event-ID` resume, a live map viewer at `/`, a per-client buffer that sheds a slow viewer rather than stalling the fan-out, and a 503 + `Retry-After` refusal at its configured client limit | |
| The versioned envelope: an unknown `schema_version` is refused rather than guessed at, unknown fields are refused, and a producer's `event_id` survives so a retransmission stays detectable | |
| Operational surface on every process: `/healthz`, `/readyz`, `/metrics`, with dependency gauges kept fresh by a background prober | |
| CI: `gofmt`, `vet`, the migrations against a real broker, the race suite, a build, and a job that starts the whole stack and smoke-tests it — 86 checks, including a posted event applied to Postgres by the consumer, a record that cannot be decoded refused without stalling the stream, that refusal replayed, an event delivered to an already-connected browser over the bus, a reconnect resuming exactly the missed frames, a consumer killed with `SIGKILL` and then rewound to the start of the log, where every re-delivered event deduplicated instead of being applied twice, and every process stopped with `SIGTERM` to prove it drains and exits 0 rather than being killed at the end of its grace period | |

## Measured

Every number below comes from a committed artifact in [`load/`](load/), produced by the script
beside it. The host is one machine — an i7-13620H, 16 cores, 23.6 GB — running every process in
Docker, so these describe a single-host deployment and nothing else. Two runs of a simulator with
one seed produce identical payloads, so a difference between runs is a difference in the system.

| Requirement | Target | Measured | Artifact |
|---|---|---|---|
| NFR-1 ingest throughput | ≥ 2,000 events/s | **2,499.9 events/s** — 100,025 accepted, **0 rejected**, over 40 s | [`load/ingest-results.json`](load/ingest-results.json) |
| NFR-2 end-to-end latency | p95 ≤ 500 ms | **p95 22.77 ms** (p50 14.15, p99 25.54, max 46.78), 1,000 of 1,000 delivered | [`load/e2e-results.json`](load/e2e-results.json) |
| NFR-3 concurrent SSE clients | ≥ 200 on one gateway | **200 of 200**, 1,095,140 frames, RSS **+18.3 MiB** (~90 KiB per client), none shed | [`load/sse-results.json`](load/sse-results.json) |

How each was produced, because a throughput number without its method is a claim rather than a
measurement:

- **Ingest** — `k6 run load/ingest.js`, constant-arrival-rate 100 requests/s × 25 events, 40
  vehicles so events are keyed onto a bounded set of partitions. The threshold is on
  `events_accepted`, the count the *server* reports in its 202 body, not on what the load
  generator sent. That distinction found a real defect: the first version of the script sent the
  envelope shape the platform refuses, every batch returned 202, and only the accepted-count
  threshold exposed 100,025 rejections.
- **End-to-end latency** — `python scripts/load_e2e.py`, 1,000 events paced at 50/s, each timed
  from the POST that carried it to its arrival on a subscribed SSE stream: ingest → log →
  consumer transaction → Postgres → bus → gateway → client. It drains the consumer group before
  starting the clock, because a latency measurement taken during a backlog is a measurement of
  the backlog.
- **Fan-out** — `python scripts/load_sse.py`, 200 SSE clients via one asyncio event loop against
  a running producer, with the gateway's own `gateway_clients` gauge sampled while they were
  connected. The client count, the frames-received count and the gateway's gauge must agree
  before the run passes; a harness that reports its own belief while the system reports something
  else is measuring itself.

What these numbers do not say: nothing here is a production figure. Ingest at 2,499 events/s
went through one ingest process and one three-node-replica-free broker; the fan-out ceiling past
1,000 clients is untested; and the end-to-end figure holds at an offered rate far below the
ingest ceiling, which is the condition under which a latency number means anything.

**Fuzzing: 1,565,798 executions against the ingest parser and 2,267,015 against the log
decoder, zero crashers** (45 s per target, `go test -run=NONE -fuzz=... -fuzztime=45s`). The two
targets are the functions that read bytes this system did not produce: `event.Accept` parses what
a producer POSTs, and `event.Decode` parses what the consumer reads back off the log. The seeds
include the envelope shape that must be refused, so a change that starts accepting it fails a run
rather than passing quietly — and the accepted path asserts the invariants the pipeline leans on:
the id parses as a UUID, the schema version is one this build implements, and a marshal/decode
round trip preserves the identity the sink deduplicates on. No file was written under
`testdata/fuzz`, which is the result a clean run leaves behind; a file there would be a crash Go
had to reproduce.

**Test coverage: 45.5% of statements**, measured with `go test -covermode=atomic
-coverprofile=... ./...` against the same real Postgres, Redis and broker the suite requires.
What it says is which packages carry their weight: `httpserver` 96.2%, `config` 94.8%, `ingest`
89.5%, `gateway` 84.8%, `idgen` 83.3% and `simulator` 80.2% are where a change is likely to break
something subtle. The packages reading 0% are the wiring — `cmd/*` are `main` functions there is
no unit to test, and `reqid` and `testsupport` are exercised by the deployed smoke suite rather
than by the Go tests. One number hides that split, which is why it is stated rather than left in
a badge.

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
[`docs/adr/`](docs/adr/) records the nine decisions behind them. The numbers above came after, and
each is published with the method that produced it and the conditions it holds under — the same
discipline its sibling repository
[`tenant-api-platform`](https://github.com/aditya0si/tenant-api-platform) applies to its own.

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

Every row is implemented and running. `docker compose up --build` starts the service, and
`docker compose --profile demo up -d simulate` adds the fleet the viewer draws.

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
