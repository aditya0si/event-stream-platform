# ADR-007: One module, one image, six entrypoints

Status: accepted

## Context

The system has several roles: HTTP ingest, a stream simulator, a consumer-group member, an SSE
gateway, plus a replay CLI and a migration runner. They could be separate modules, separate
repositories, separate images, or one module with several entrypoints.

## Decision

**One Go module, one image, six entrypoints.**

| Entrypoint | Kind | Runs |
|---|---|---|
| `cmd/ingest` | long-running service | as a compose service |
| `cmd/simulate` | long-running producer | as a compose service (or `go run` with flags) |
| `cmd/consumer` | long-running worker | as a compose service, scale 1..partition count |
| `cmd/gateway` | long-running server | as a compose service |
| `cmd/replay` | CLI | on demand, by an operator |
| `cmd/migrate` | one-shot tool | as a compose service that exits after `up` |

The Dockerfile builds all six binaries into one image; compose selects one via `entrypoint`.

## Why

- **Shared contracts.** The envelope, its validation, the store, and the dedup logic are used by
  more than one entrypoint. Separate modules would mean either publishing an internal library
  (versioning overhead for a single-author project) or copying the code (which drifts, and the drift
  is exactly what breaks a stream).
- **One test run covers the seams.** A change to the envelope breaks the producer's tests and the
  consumer's tests in the same `go test ./...`, before either could ship half of a protocol change.
- **One image means no version skew.** The consumer and the gateway cannot run different builds of
  the same commit — the same argument the sibling repository makes for its API and worker sharing an
  image, and worth the few megabytes.
- **`cmd/replay` is a CLI, not an endpoint,** so the recovery path does not depend on the process
  that failed. When the consumer is down — precisely when an operator needs it — a CLI still runs.

## Consequences

**Good.** Cross-component changes land atomically; one CI pipeline; one version to reason about;
`go build ./...` compiles every role; a reviewer can find any entrypoint's wiring in one place.

**Bad.** A container carries a replay CLI it may never execute, so the image is larger than a
single-purpose one. Accepted deliberately: the operative risk in a system like this is drift between
roles, not image size, and the sibling repository already proved the trade worth taking.

**Revisit if.** The roles ever need genuinely different dependency sets or release cadences (for
instance, a consumer that must ship independently of the gateway). Not true here; this ADR records
the trigger rather than leaving the question open.
