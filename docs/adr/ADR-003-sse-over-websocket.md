# ADR-003: SSE over WebSocket, because reconnect is the feature

Status: accepted

## Context

The live viewer needs server-pushed updates. The two standard transports are Server-Sent Events and
WebSockets, and the choice is usually presented as taste. It is not: one of them has a resume
primitive built into the protocol, and the other has a blank page where that should be.

Every telemetry viewer will be disconnected — a laptop sleeps, a tab is backgrounded, a train enters
a tunnel, a deploy restarts the gateway. What happens next is the design question, and it is the
question the two transports answer differently.

## Decision

Use **SSE** (`text/event-stream`), written by hand, with **`Last-Event-ID` as the resume mechanism**.
Do not build a WebSocket endpoint.

SSE defines `Last-Event-ID`: the browser remembers the `id:` field of the last event it accepted and
sends it back as a request header when it reconnects. Resume is therefore a **protocol feature
implemented by the browser**, not application logic that has to be written, versioned, and tested on
both ends.

Every frame carries the event's `event_id` in its `id:` field. On reconnect with
`Last-Event-ID: <id>`, the gateway reads events newer than that event's `event_ts` from Postgres — a
bounded window, indexed — replays them in order, and then joins the live bus. The client sees a
continuous stream; the reconnect is invisible.

## Consequences

**Good.** Resume after a crash, a deploy, or a laptop waking is browser-native behaviour, and the
gateway's half of it is one indexed query. The endpoint is trivially inspectable — `curl -N` shows
the stream — which makes both the demo and the smoke test straightforward. Automatic reconnection
with backoff is also built into `EventSource`, so the client is a few dozen lines of canvas code.

**Bad.** SSE is unidirectional and text-only. Neither costs anything here: the viewer displays
positions and issues no commands, and the payload is JSON.

**Revisit if.** A requirement appears for genuine client→server messaging (say, "follow this
vehicle"). Even then, the honest shape is a `POST` for commands plus SSE for the stream — a
bidirectional *application* does not require a bidirectional *transport*, and unidirectional
transports can carry both directions.

## Note on the rejected alternative's cost

A WebSocket implementation would need, at minimum: client-side sequence tracking, a server-side
replay buffer or a query path, a bespoke resume message, and tests for the reconnect handshake — all
of it re-implementing what `Last-Event-ID` provides, with more failure modes and no standards
coverage. That is the reasoned rejection, not a preference for the simpler API.
