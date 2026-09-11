# ADR-009: Redis Pub/Sub between the consumer and the gateway

Status: accepted

## Context

The consumer sinks events to Postgres and must also deliver them to N gateway processes, each holding
many SSE connections. Two constraints shape the choice: the consumer must not need to know how many
gateways exist, and gateways must scale (and restart) independently of consumers.

## Decision

The consumer **publishes to a Redis Pub/Sub channel**; gateways subscribe and fan out to their own
connections.

```
consumer ──PUBLISH telemetry:live──▶ Redis ──SUBSCRIBE──▶ gateway ──SSE──▶ browsers
   │
   └── same event already committed to Postgres
```

The publish is **deliberately not transactional** with the sink. If it fails, the event is already
durable in Postgres; the live view misses one frame and the next event corrects it. Making it
transactional would mean a *browser disconnection* could fail an *ingest* — inverting the priorities,
since the durable record is the product and the live view is a convenience.

## Why not Redis Streams

Streams are a log with consumer groups and offsets — which this system already has, one layer up.
Adding a second one introduces two offset systems, two retention policies, and a new question nobody
wants to answer during an incident: *when the Redis Stream and the Kafka log disagree, which is
authoritative?* The answer would be "the Kafka log," which makes the Stream a cache with extra steps
plus a consumer group nobody drains. Pub/Sub is the honest shape for a best-effort delivery bus: a
narrow waist between two components that need not know about each other.

## Why not HTTP push from consumer to gateway

The consumer would need to discover gateways, track their liveness, and retry failed deliveries — a
service registry and a delivery queue, both of which Redis already is, minus the maintenance.

## Consequences

**Good.** Gateways scale horizontally with no coordination; the consumer performs one command with no
subscriber knowledge; there is no bus state to manage, prune, or monitor for growth.

**Bad — and stated where it matters.** Pub/Sub has **no persistence**. A gateway that is down when a
message is published never sees it, and a subscriber whose buffer overruns can be dropped by Redis
itself. This is acceptable *because of what the bus carries*: one live frame. The event is already
durable in Postgres, and any client that notices a gap — or simply reconnects — resumes from
`Last-Event-ID` against the durable history (ADR-003).

**The guarantee lives where the data lives.** If the live view ever required a delivery guarantee,
the answer would be the sink's history plus SSE resume — never a more durable bus. That inversion is
the reason this ADR exists: it is tempting to reach for Streams to make the *demo* more reliable,
which would put the guarantee in the wrong component.
