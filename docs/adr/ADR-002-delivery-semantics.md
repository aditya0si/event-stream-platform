# ADR-002: At-least-once delivery, idempotent consumers, and the words we refuse to use

Status: accepted

## Context

"Exactly-once" is the most abused phrase in streaming. It appears in READMEs with nothing behind it,
and an interviewer who asks "show me" usually finds a system that delivers at least once and hopes
nobody notices.

This system has a log that guarantees an event is not lost within retention, and does **not**
guarantee it is delivered once. A consumer can process a batch and crash before committing its
offset. A rebalance can hand a partition to a second consumer whose predecessor had already applied
part of that batch. Duplicates are not an edge case here; they are the expected behaviour of the
tooling.

## Decision

Deliver **at least once**. Make processing **idempotent**. Call the composition **effectively-once**,
and never write "exactly-once" in the README or in a commit message.

Two mechanisms, in two layers:

1. **At-least-once is the log's guarantee.** Offsets are committed *after* the consumer's database
   transaction commits. A crash between the two means the batch is redelivered. The failure mode is a
   duplicate, never a loss.
2. **At-most-once application is the database's guarantee.** `processed_events.event_id` is a PRIMARY
   KEY, and `INSERT ... ON CONFLICT DO NOTHING` runs in the **same transaction** as every other
   effect of handling that event. A redelivered event inserts nothing and applies nothing.

The order is the whole argument: the log promises *not lost*, the transaction promises *not applied
twice*, and the intersection — under the stated retention relationship — is what the README calls
effectively-once.

## What would break it

These are the three ways to destroy the guarantee, named so that each can be tested rather than
trusted:

- **Pruning `processed_events` while the log still holds replayable events.** The dedup window must
  be at least the log's retention window, or a replay re-applies events whose record was deleted. A
  test asserts the retention constant is not less than the dedup window, so the relationship cannot
  drift silently when someone tunes one number.
- **Committing offsets before the transaction.** This inverts the system from at-least-once to
  at-most-once — loss instead of duplication — and it is the single most consequential line in the
  consumer. A comment marks it, and a test kills the consumer between the two commits to prove which
  side the outcome lands on.
- **A side effect outside the transaction.** The fan-out publish is deliberately outside (ADR-009),
  so it is *not* covered by this guarantee. The Postgres sink is; the live view is best-effort. Said
  plainly here, in DESIGN.md, and in the README, because a guarantee that quietly excludes the
  flagship demo would be a lie of omission.

## Alternatives considered

| Option | Why not |
|---|---|
| **Claim exactly-once via Kafka transactions** | Kafka's transaction API can atomically commit offsets together with *messages it produces*. It cannot make an arbitrary Postgres write atomic with a log append. A genuine cross-system exactly-once claim would need XA or an outbox — and building one here purely to justify a slogan is precisely the failure this ADR exists to prevent. |
| **At-most-once (commit offsets before processing)** | Simpler and cheaper, and it silently drops events under any failure. For a missing position report the hole is permanent: nothing can repair history that was never written. |
| **Deduplicate in Redis** | Redis is a cache, and a cache that loses keys must not be the record for "was this event applied" — the sibling repository's ADR-004 makes this argument at length and it applies identically. Redis sits on the fan-out path instead, where losing a key costs one live frame rather than a duplicated side effect. |

## Consequences

**Good.** The invariant is enforced by the same engine that stores the data, in the strongest form it
offers — a uniqueness constraint inside a transaction — with no cross-system protocol to get wrong.
Replay is idempotent for free, because a replayed event keeps its `event_id` and therefore meets the
same primary key (see the replay flow in DESIGN.md § E.4).

**Bad.** Every event costs a dedup write, and the table grows with the stream. Both are bounded by
policy: retention is documented, pruning may never outrun log retention (asserted), and the lookup is
a single b-tree probe on the primary key. If the dedup table ever becomes the bottleneck, that will
be a *measurement* from M7, and the first remedy is time-partitioning the table — recorded here so
the decision has a starting point.
