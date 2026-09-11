# ADR-004: Backpressure — shed the slow subscriber, never grow without bound

Status: accepted

## Context

Every queue that "cannot overflow" is a queue whose limit nobody can find. In a live fan-out to
browsers there are two places memory can run away: the per-connection buffer inside the gateway, and
any channel carrying work between the consumer and the gateway. Neither has a natural maximum unless
one is chosen deliberately.

The requirement is not "handle any number of arbitrarily slow clients." It is: **stay up when clients
are slow, and be honest about what happens to them.**

## Decision

Bound every buffer, and on overflow **disconnect the slow subscriber** — incrementing
`sse_disconnects_total{reason="slow_consumer"}` — and let it reconnect and resume from
`Last-Event-ID` (ADR-003).

- Each gateway connection gets a bounded outbound buffer.
- The consumer publishes to the fan-out bus without ever blocking on a subscriber.
- No unbounded channel exists on any path between the broker and a browser.

**Why disconnect rather than drop frames.** A client silently missing events, with no way to know, is
strictly worse than a client that is disconnected and resumes gaplessly. Disconnection converts a
silent correctness problem into a visible, recoverable one, and the recovery costs one HTTP
handshake plus one indexed query.

**Why not slow the pipeline down.** Making the whole ingest path wait on the slowest browser is how
one mobile client on a train degrades every other consumer. The durable record in Postgres is the
product; the live view is a convenience. Priority follows from that, explicitly.

## Alternatives considered

| Option | Why not |
|---|---|
| **Unbounded per-connection buffers** | The failure is a gateway that OOMs under a load it accepted happily for the first ten minutes. It is also invisible until it happens, which is the worst combination. |
| **Blocking writes to slow clients** | One stuck TCP connection would stall a goroutine and, with enough of them, the fan-out loop — trading a recoverable per-client failure for a process-wide one. |
| **Dropping frames silently** | The cheapest option, and the one that makes the viewer quietly wrong. A map showing a vehicle's stale position with no indication that updates were skipped is a demo that lies. |
| **Client-side flow control (SSE has none)** | The protocol offers no windowing. Anything built here would be a bespoke handshake on top of a text stream — more moving parts than a reconnect, for less certainty. |

## Consequences

**Good.** Memory per gateway is bounded by *(connections × buffer size)* — a number that can be
stated in the README up front rather than discovered during an incident. Shedding is counted, so it
is visible on the dashboard instead of inferred from a stalled map.

**Bad.** A briefly-slow client loses its connection and must resync. The resume path makes this cheap,
but it is a real cost and the README says so.

**Measured, not asserted.** The concurrent-client ceiling of one gateway, and the buffer size at
which shedding begins, are measured in M7. Until those measurements exist, **no number appears in any
document** — this ADR included.
