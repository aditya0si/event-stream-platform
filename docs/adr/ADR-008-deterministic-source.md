# ADR-008: The stream is simulated — deterministically, from committed geometry

Status: accepted

## Context

A streaming system needs a producer, and the honest options are a live external feed, a random
generator, or a deterministic simulator.

A live feed (a transit API, GitHub's firehose) makes the repository depend on a third party's
availability, terms, and rate limits — the opposite of the promise that `docker compose up` produces
a working system. A random generator produces numbers with no domain meaning, so the cases that make
this project interesting (a late report, a retransmission, a vehicle skipping ahead) never occur
naturally and would have to be injected anyway.

## Decision

A **deterministic simulator**: `cmd/simulate` walks vehicles along route geometry committed to the
repository, emits position reports at a configurable rate from a fixed seed, and can be told to
inject the cases that matter — delayed reports, retransmissions, out-of-order arrival, and clock skew.

Deterministic means *same seed, same rate, same stream*. That is what makes a failing test
reproducible, a benchmark repeatable, and any run's expected output predictable. A random producer
gives up all three.

## Why this is not a shortcut

The subject of this system is what happens to events **after** they enter the log. A simulator that
can deliberately produce a late report is strictly more useful for demonstrating late-data handling
than a live feed that produces one by accident once a week. It is also the only kind of source that
makes the M6 chaos test and the M7 benchmarks reproducible — a benchmark whose input varies run to run
measures nothing.

**The README states that the stream is simulated, in those words.** It is a demonstration of pipeline
behaviour, not a claim about a real fleet. Generating 1,800 vehicles of synthetic data and presenting
it as a live feed would be exactly the kind of false claim this project refuses to make.

## Adapter boundary

`cmd/simulate` emits the same envelope that `cmd/ingest` accepts, through the same validation, so a
real feed replaces it by pointing an adapter at `POST /v1/events` — nothing downstream can tell the
difference. That boundary is also why the simulator is a separate binary rather than a `--simulate`
flag on the ingest server: the two have different trust levels (a simulator is not an untrusted
client), and the replacement path for a real feed is a URL rather than a code change.

## Footnote: no schema-registry server

Schema versioning is implemented in the envelope (`schema_version`, validated on decode, unknown
majors dead-lettered) with `schema_versions` recording what has actually been observed. A
Confluent-compatible registry would add a container and a client library for a system with one
producer generation and one consumer generation. The *discipline* — version every event, refuse to
guess at an unknown version — is what matters at this size, and stating that is more honest than
implying a registry was never considered.
