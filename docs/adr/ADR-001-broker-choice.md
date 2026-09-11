# ADR-001: A real log — Redpanda, consumed with franz-go

Status: accepted

## Context

The sibling repository (`tenant-api-platform`) deliberately has **no broker**, and that decision has
its own ADR: an outbox row must commit in the same transaction as the business row, and a broker
client cannot join a Postgres transaction. A reviewer moving between the two repositories will notice
the difference, so it needs an argument rather than an inconsistency.

They are solving different problems:

| | `tenant-api-platform` | this repository |
|---|---|---|
| Trigger | a client request | a continuous stream |
| Coupling | the event must commit with the row that caused it | events are independent of any local write |
| Ordering | implied by transactions, per tenant | explicit, per vehicle, a property of the log |
| Scale axis | latency under concurrent clients | throughput and consumer parallelism |
| Recovery | replay the outbox | replay the log from an offset |

In the sibling repository the event is a *consequence* of a state change and must share its fate, so
the broker's inability to join the transaction is disqualifying. Here the event **is the product**:
nothing local must commit alongside it, and the constraint that forced the outbox design simply does
not exist. An outbox remains the right answer to "do not lose the notification that this row changed";
it is not the right answer to "durably buffer, partition, and replay thousands of messages per second
to many independent consumers."

## Decision

Use **Redpanda** as the log, produced and consumed with **franz-go**
(`github.com/twmb/franz-go`).

Redpanda speaks the Kafka wire protocol, so the real choice is "the Kafka API with a single-binary
server." Partitions, keys, consumer groups, offset commits, rebalances, retention, and replay from an
arbitrary offset are genuine, not emulated.

## Alternatives considered

| Option | Why not |
|---|---|
| **NATS + JetStream** | Lighter, Go-native, and genuinely good. It loses on vocabulary: "partition, key, consumer group, offset, rebalance" are the exact words of the streaming job descriptions this project is scoped against, and the Kafka wire protocol is the larger surface for both JDs and reader familiarity. The operational-cost argument is also weaker here than it appears — measured on this host, Redpanda idles at **193 MB and 0.5% CPU**. |
| **Apache Kafka** | The same API at a much higher operational cost (KRaft quorum, JVM tuning, multi-broker reality). It buys replication and scale this project neither has nor claims. Calling it "Kafka" while running one broker would be worse than useless: the README would promise a cluster and deliver a process. |
| **Redis Streams** | Already used in the sibling repository; a second appearance buys no new vocabulary and makes the two projects read as one project built twice. Consumer groups exist, but partition-level ordering, independent retention, and replay from arbitrary offsets are weaker. |
| **Postgres as the log** (`SKIP LOCKED`, `LISTEN/NOTIFY`) | This is the sibling repository's design; re-implementing it here would erase the reason a second repository exists. `LISTEN/NOTIFY` also cannot survive a subscriber being down — the notification is gone. A log that outlives its consumers is the requirement. |
| **A hand-rolled log** | The honest reason is time; the dishonest reason would be that building storage demonstrates more. It demonstrates *different* things, and this project's subject is delivery semantics, not storage. |

**franz-go over Sarama or segmentio/kafka-go.** franz-go is actively maintained, has no
ZooKeeper-era client in its dependency tree, threads `context` through every call, and exposes the
per-partition control this design needs — offsets are committed by hand, *after* the consumer's own
transaction commits (ADR-002). Sarama is legacy-shaped; kafka-go's manual-commit story is thinner.

## Consequences

**Good.** The properties this project exists to demonstrate — per-key ordering, consumer parallelism
bounded by partition count, replay from an offset, consumer lag as a first-class metric — are
properties of the log rather than things built on top of one. The concepts transfer directly to Kafka,
which is what JDs name.

**Bad.** A broker container, a second thing to health-check, and a hard dependency: ingest cannot
accept anything while it is down. Accepted, and turned into a feature — the failure table states
exactly what each process does when the broker is unreachable, and the tests exercise it.

**Revisit if.** The project ever needs multi-broker replication or a partition count beyond what one
node serves. Neither is true at this scale, and the README says so rather than implying production
readiness.
