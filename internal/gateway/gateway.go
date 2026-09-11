// Package gateway fans positions out to browsers over Server-Sent Events.
//
// # Two sources, one stream
//
// A connected client's frames come from two places, and the gateway's job is to make the seam
// invisible:
//
//   - the live bus (Redis Pub/Sub, best-effort, ADR-009) for events as they are applied;
//   - the durable history in Postgres, for the gap between the last event a client saw and the
//     moment it reconnected.
//
// The history is what makes a reconnect gapless. `Last-Event-ID` carries the identity of the
// last event a client accepted, the history resolves it to an event time, and the replay reads
// everything newer — which is why the SSE `id:` field is the event id and not a counter
// (ADR-003). A counter would be meaningless against a durable log.
//
// # Backpressure, applied at the client
//
// Each client owns a bounded buffer. A client that fills it is disconnected, counted, and left
// to reconnect and resync from the history (ADR-004). The alternative — blocking the fan-out
// loop — would let one slow browser stall every other client, and an unbounded buffer would let
// it grow the process until the kernel intervened. Neither is acceptable; a reconnect is cheap
// and it is already implemented, because it is the same path a sleeping laptop takes.
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/aditya0si/event-stream-platform/internal/fanout"
	"github.com/aditya0si/event-stream-platform/internal/platform/metrics"
)

// FrameSource delivers live frames. internal/fanout's Subscriber satisfies it.
type FrameSource interface {
	Frames(ctx context.Context) (<-chan fanout.Position, func(), error)
}

// HistorySource answers the two questions a resume needs: what time did this event record, and
// what happened after it. sink.Store satisfies it.
//
// It is an interface so the gateway can be tested without a database, and so the dependency
// runs in the direction the design says it does: the gateway does not know what a sink is.
type HistorySource interface {
	PositionTime(ctx context.Context, eventID string) (time.Time, bool, error)

	// PositionsSince takes the resume point's id as well as its time, and that is a correctness
	// requirement rather than a convenience: a timestamp is not a position in the order the
	// client saw. Telemetry shares seconds, so `event_ts > after` drops every event that shares
	// the resume second and has not been delivered yet — a hole in the exact guarantee this read
	// exists to provide. The store compares the (event_ts, event_id) tuple instead, which is the
	// same total order the frames are sent in.
	PositionsSince(ctx context.Context, after time.Time, afterID string, limit int) ([]fanout.Position, error)
}

// Options configures the gateway.
type Options struct {
	// ClientBuffer is the per-client outbound buffer. Its size is the backpressure policy
	// (ADR-004) expressed as a number: a client that falls this many frames behind is
	// disconnected rather than allowed to grow the process.
	ClientBuffer int

	// MaxClients bounds concurrent streams. Without it, connection count is bounded by
	// whatever the operating system allows, and the failure mode is a process that dies
	// without having refused anybody — the worst possible shape, because it looks like a crash
	// rather than a limit.
	MaxClients int

	// ReplayWindow is how far back a reconnecting client may resume. A client further behind
	// than this is told to start fresh instead, because an unbounded replay is a
	// denial-of-service vector dressed as a convenience: one client presenting an ancient
	// event id would ask the database for the entire history.
	ReplayWindow time.Duration

	// ReplayLimit caps the frames a single resume may send. The window bounds how far back,
	// this bounds how many.
	ReplayLimit int

	// HeartbeatInterval sends a comment frame when nothing else has been written, so an idle
	// stream is not closed by an intermediary that mistakes silence for a dead connection.
	HeartbeatInterval time.Duration

	// MaxStreamDuration closes a stream after this long, so a client that never reconnects
	// cannot hold a slot forever. Zero means no limit.
	MaxStreamDuration time.Duration

	Log        *slog.Logger
	Authorized func(r *http.Request) bool
}

// Gateway serves the SSE endpoint.
type Gateway struct {
	source  FrameSource
	history HistorySource
	opts    Options
	log     *slog.Logger

	mu      sync.Mutex
	clients map[*client]struct{}
	// frames is the single subscription shared by every connected client. One subscriber fans
	// out to N clients rather than each client subscribing for itself: a per-client
	// subscription would multiply Redis connections by viewer count and deliver the same frame
	// N times over the network.
	frames <-chan fanout.Position
	// subStop releases the shared subscription.
	subStop func()
}

// client is one connected browser.
type client struct {
	// out carries frames to this client's writer goroutine. It is the bounded buffer that
	// makes the shedding policy real rather than aspirational.
	out chan fanout.Position
	// remote is recorded so a shed client can be named in a log line; the address is not
	// otherwise used, and no identifier derived from it is ever stored.
	remote string
}

// New builds a gateway. It does not connect to anything: Start does.
func New(source FrameSource, history HistorySource, opts Options) (*Gateway, error) {
	if source == nil {
		return nil, errors.New("gateway: nil frame source")
	}
	if history == nil {
		return nil, errors.New("gateway: nil history source: a reconnect could not be resumed")
	}
	if opts.Log == nil {
		return nil, errors.New("gateway: nil logger")
	}
	if opts.ClientBuffer < 1 {
		return nil, fmt.Errorf("gateway: ClientBuffer must be at least 1, got %d", opts.ClientBuffer)
	}
	if opts.MaxClients < 1 {
		return nil, fmt.Errorf("gateway: MaxClients must be at least 1, got %d", opts.MaxClients)
	}
	if opts.ReplayLimit < 1 {
		opts.ReplayLimit = 1000
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = 15 * time.Second
	}
	return &Gateway{
		source:  source,
		history: history,
		opts:    opts,
		log:     opts.Log,
		clients: make(map[*client]struct{}),
	}, nil
}

// Start subscribes to the bus and begins the fan-out loop.
//
// It returns once the subscription is established, so a caller that starts serving immediately
// afterwards cannot miss a frame published in between — a gap that would be silent rather than
// an error. The returned channel receives the loop's error, if any.
func (g *Gateway) Start(ctx context.Context) (<-chan error, error) {
	frames, stop, err := g.source.Frames(ctx)
	if err != nil {
		return nil, fmt.Errorf("gateway: subscribe to the fan-out bus: %w", err)
	}

	// Stored under the lock: a Shutdown racing Start would otherwise read a half-set
	// subscription, which -race flags and which in production would drop the bus subscription.
	g.mu.Lock()
	g.frames = frames
	g.subStop = stop
	g.mu.Unlock()

	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		for {
			select {
			case <-ctx.Done():
				return
			case pos, ok := <-g.frames:
				if !ok {
					// The bus subscription ended. This is not fatal: the gateway stays up and
					// keeps serving resumes from history, and existing clients keep their
					// connections. Exiting here would take down the viewer for a bus failure
					// that the durable record already covers.
					g.log.Warn("the fan-out bus subscription ended; live frames stop until it is " +
						"re-established, while resumes from history continue to work")
					return
				}
				g.broadcast(pos)
			}
		}
	}()
	return errCh, nil
}

// broadcast offers a frame to every client, shedding any that cannot keep up.
func (g *Gateway) broadcast(pos fanout.Position) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for c := range g.clients {
		select {
		case c.out <- pos:
		default:
			// The client's buffer is full. It is disconnected rather than skipped: a client
			// silently missing frames has no way to know, whereas a disconnected one reconnects
			// and resumes from its last event id with no gap at all (ADR-004).
			g.dropLocked(c, "slow_consumer")
		}
	}
}

// dropLocked removes a client and closes its buffer. The caller holds the lock.
func (g *Gateway) dropLocked(c *client, reason string) {
	if _, ok := g.clients[c]; !ok {
		return
	}
	delete(g.clients, c)
	close(c.out)
	metrics.GatewayClients.Set(float64(len(g.clients)))
	metrics.GatewayDisconnectsTotal.WithLabelValues(reason).Inc()
	g.log.Info("client disconnected", "client", c.remote, "reason", reason,
		"clients", len(g.clients))
}

// SubscriberCount reports how many clients are currently connected. Used by the smoke test and
// the metrics endpoint's own assertions.
func (g *Gateway) SubscriberCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.clients)
}

// Shutdown closes every client stream and releases the bus subscription.
func (g *Gateway) Shutdown() {
	g.mu.Lock()
	for c := range g.clients {
		g.dropLocked(c, "server_shutdown")
	}
	stop := g.subStop
	g.subStop = nil
	g.mu.Unlock()

	if stop != nil {
		stop()
	}
}

// Mount registers the routes.
func (g *Gateway) Mount(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/stream", g.handleStream)
	mux.HandleFunc("GET /v1/vehicles/{vehicleID}", g.handleCurrent)
}

// handleStream serves one SSE connection.
func (g *Gateway) handleStream(w http.ResponseWriter, r *http.Request) {
	if g.opts.Authorized != nil && !g.opts.Authorized(r) {
		http.Error(w, "a valid API key is required", http.StatusUnauthorized)
		return
	}

	// The connection must support flushing: without it, frames accumulate in a buffer and a
	// viewer shows nothing until the response ends, which for a stream is never.
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "this server cannot stream", http.StatusInternalServerError)
		g.log.Error("the ResponseWriter does not implement http.Flusher, so SSE cannot work")
		return
	}

	c, ok := g.register(w, r)
	if !ok {
		return
	}
	defer g.unregister(c)

	// Headers must be written before the first flush, and every one of these says something
	// specific:
	//
	//	text/event-stream  the media type the browser's EventSource requires
	//	no-cache           an intermediary must not serve a cached stream to a second client
	//	keep-alive         the connection is meant to stay open, so do not pool it away
	//	X-Accel-Buffering  nginx buffers proxied responses by default, which would hold frames
	//	                   until its buffer filled — the classic "SSE works locally, not behind
	//	                   a proxy" bug
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	sent := g.replay(w, r, flusher)

	heartbeat := time.NewTicker(g.opts.HeartbeatInterval)
	defer heartbeat.Stop()

	var deadline <-chan time.Time
	if g.opts.MaxStreamDuration > 0 {
		t := time.NewTimer(g.opts.MaxStreamDuration)
		defer t.Stop()
		deadline = t.C
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			// The browser navigated away or the connection died. Counted as a client close
			// rather than a failure: it is the normal way a stream ends.
			metrics.GatewayDisconnectsTotal.WithLabelValues("client_closed").Inc()
			g.log.Debug("client closed the stream", "client", c.remote, "frames_sent", sent)
			return

		case pos, ok := <-c.out:
			if !ok {
				// The gateway closed this buffer: the client was shed for falling behind, or the
				// server is shutting down. Which one it was is in the metrics and the log — the
				// frame deliberately does not say, because the client's next action is the same
				// either way and the distinction is not the client's business.
				writeFrame(w, flusher, "shed", map[string]any{
					"reason": "the stream was closed by the server; reconnect to resume",
				})
				return
			}
			if err := writePosition(w, flusher, pos); err != nil {
				metrics.GatewayDisconnectsTotal.WithLabelValues("write_error").Inc()
				g.log.Debug("writing to a client failed", "client", c.remote, "err", err)
				return
			}
			sent++
			metrics.GatewayEventsSent.Inc()

		case <-heartbeat.C:
			// A comment frame: SSE ignores it, and it keeps the connection from looking idle to
			// anything between here and the browser that closes quiet sockets.
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				metrics.GatewayDisconnectsTotal.WithLabelValues("write_error").Inc()
				return
			}
			flusher.Flush()

		case <-deadline:
			// A deliberate close so a client that never reconnects cannot hold a slot forever.
			writeFrame(w, flusher, "reconnect", map[string]any{
				"reason": "the stream reached its maximum duration; reconnect to continue",
			})
			metrics.GatewayDisconnectsTotal.WithLabelValues("max_duration").Inc()
			return
		}
	}
}

// register admits a client, enforcing the connection cap.
//
// The lock is held only to read and mutate the client set. Everything that touches the network —
// writing the refusal, logging, emitting the metric — happens after it is released, so a client
// slow to read its own 503 cannot stall the fan-out loop for every other viewer. The first
// version wrote the response and re-acquired the lock around the metric: it balanced, but it held
// the mutex across a blocking write and left a window where another goroutine could slip past the
// capacity check.
func (g *Gateway) register(w http.ResponseWriter, r *http.Request) (*client, bool) {
	c := &client{
		out:    make(chan fanout.Position, g.opts.ClientBuffer),
		remote: r.RemoteAddr,
	}

	g.mu.Lock()
	if len(g.clients) >= g.opts.MaxClients {
		n := len(g.clients)
		g.mu.Unlock()

		// 503 with Retry-After rather than 429: this is capacity, not rate. A client that
		// honours the hint reconnects after the existing viewers have cycled.
		w.Header().Set("Retry-After", "5")
		http.Error(w, "the gateway is at its configured client limit", http.StatusServiceUnavailable)
		metrics.GatewayDisconnectsTotal.WithLabelValues("at_capacity").Inc()
		g.log.Warn("refused a stream: at capacity",
			"max_clients", g.opts.MaxClients, "clients", n)
		return nil, false
	}
	g.clients[c] = struct{}{}
	n := len(g.clients)
	g.mu.Unlock()

	metrics.GatewayClients.Set(float64(n))
	g.log.Debug("client connected", "client", c.remote, "clients", n)
	return c, true
}

// unregister removes a client if it is still present. Drops and unregisters race, so this is
// tolerant of a client that was already shed.
func (g *Gateway) unregister(c *client) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.clients[c]; !ok {
		return
	}
	delete(g.clients, c)
	close(c.out)
	metrics.GatewayClients.Set(float64(len(g.clients)))
}

// replay writes the frames a reconnecting client missed, and reports how many were sent.
//
// Three outcomes, and the difference between them is what makes reconnect honest:
//
//   - no Last-Event-ID: a fresh client. It gets live frames only, because it has no reference
//     point and replaying the whole history would be a surprise, not a service.
//   - an id the history knows: everything newer, up to the window and the limit.
//   - an id it does not know, or one older than the window: the client is told to start fresh
//     rather than being silently given a partial replay, which would leave a hole in its view
//     that nothing would ever fill.
func (g *Gateway) replay(w http.ResponseWriter, r *http.Request, flusher http.Flusher) int {
	lastID := r.Header.Get("Last-Event-ID")
	if lastID == "" {
		// EventSource also supports a query parameter, which is what a hand-written client
		// uses when it cannot set headers.
		lastID = r.URL.Query().Get("last_event_id")
	}
	if lastID == "" {
		writeFrame(w, flusher, "ready", map[string]any{"resumed": false})
		return 0
	}

	ctx := r.Context()
	ts, found, err := g.history.PositionTime(ctx, lastID)
	if err != nil {
		// The database is unavailable. Say so on the stream rather than closing it: the client
		// can then decide to retry, and the live frames that follow are still valid.
		g.log.Error("could not resolve a resume point", "last_event_id", lastID, "err", err)
		writeFrame(w, flusher, "error", map[string]any{
			"reason": "the resume point could not be resolved; live frames will continue",
		})
		return 0
	}
	if !found {
		g.log.Info("resume refused: the event id is not in the history", "last_event_id", lastID)
		writeFrame(w, flusher, "reset", map[string]any{
			"reason": "that event is no longer in the retained history; starting fresh",
		})
		return 0
	}
	if g.opts.ReplayWindow > 0 && time.Since(ts) > g.opts.ReplayWindow {
		g.log.Info("resume refused: past the replay window",
			"last_event_id", lastID, "event_ts", ts.UTC().Format(time.RFC3339),
			"window", g.opts.ReplayWindow.String())
		writeFrame(w, flusher, "reset", map[string]any{
			"reason": "that event is older than the replay window; starting fresh",
			"window": g.opts.ReplayWindow.String(),
		})
		return 0
	}

	frames, err := g.history.PositionsSince(ctx, ts, lastID, g.opts.ReplayLimit)
	if err != nil {
		g.log.Error("the history read for a resume failed", "last_event_id", lastID, "err", err)
		writeFrame(w, flusher, "error", map[string]any{
			"reason": "the missed frames could not be read; live frames will continue",
		})
		return 0
	}

	// Everything invented here was applied by the ordinary consumer, and the live bus will
	// carry anything published from now on. A frame that arrives *during* this replay is
	// queued in the client's buffer, so no event is lost at the seam — it may be delivered
	// twice, and a client that keys on the event id is unaffected. That is the same trade the
	// rest of the system makes (ADR-002).
	sent := 0
	for _, pos := range frames {
		if err := writePosition(w, flusher, pos); err != nil {
			return sent
		}
		sent++
	}
	metrics.GatewayReplayEvents.Observe(float64(sent))
	writeFrame(w, flusher, "resumed", map[string]any{
		"resumed": true, "frames": sent, "from": ts.UTC().Format(time.RFC3339),
	})
	g.log.Info("resumed a client from history", "last_event_id", lastID, "frames", sent)
	return sent
}

// handleCurrent answers where one vehicle is right now — the read a viewer uses on first load,
// before it has a stream to follow.
func (g *Gateway) handleCurrent(w http.ResponseWriter, r *http.Request) {
	vehicleID := r.PathValue("vehicleID")
	if vehicleID == "" {
		http.Error(w, `{"error":{"code":"validation_failed","message":"vehicleID is required"}}`,
			http.StatusBadRequest)
		return
	}
	// Read through the same history interface the resume uses, so a viewer's first paint and its
	// later resumes cannot disagree about what "current" means.
	pos, found, err := g.currentFor(r.Context(), vehicleID)
	if err != nil {
		g.log.Error("reading a vehicle's current state failed", "vehicle_id", vehicleID, "err", err)
		http.Error(w, `{"error":{"code":"internal_error","message":"could not read the vehicle"}}`,
			http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, `{"error":{"code":"not_found","message":"no position has been recorded for that vehicle"}}`,
			http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{"data": pos})
}

// CurrentReader is an optional capability: a history source that can also answer "where is this
// vehicle now". It is separate from HistorySource so the gateway's core (streaming and resume)
// does not require it, and a test can implement only what the test is about.
type CurrentReader interface {
	CurrentPosition(ctx context.Context, vehicleID string) (fanout.Position, bool, error)
}

// currentFor reads a vehicle's latest position, degrading to a not-found answer when the
// configured history source cannot answer the question at all.
func (g *Gateway) currentFor(ctx context.Context, vehicleID string) (fanout.Position, bool, error) {
	cr, ok := g.history.(CurrentReader)
	if !ok {
		return fanout.Position{}, false, errors.New(
			"gateway: the configured history source cannot read current state")
	}
	return cr.CurrentPosition(ctx, vehicleID)
}

// writePosition writes one frame in SSE wire format.
//
// The `id:` line is the event's identity, not a counter, because that is what a reconnect sends
// back and what the history is queried by (ADR-003). `event: position` names the frame type so
// a client can route it without inspecting the payload, and a client that does not recognise
// the type still receives the data — which is how the `resumed` and `reset` control frames stay
// forward-compatible with a viewer written before they existed.
func writePosition(w http.ResponseWriter, flusher http.Flusher, pos fanout.Position) error {
	payload, err := json.Marshal(pos)
	if err != nil {
		// Returned rather than skipped, and the caller ends this client's stream so it reconnects
		// and resumes from its last id. fanout.Position cannot fail to marshal as written —
		// strings, floats, a time, and two optional floats — so a failure here means the type
		// acquired a field this function cannot serialize, and a viewer silently missing frames
		// is worse than one that reconnects. The `write_error` counter is where it shows up.
		return fmt.Errorf("marshal frame %s: %w", pos.EventID, err)
	}
	if _, err := fmt.Fprintf(w, "id: %s\nevent: position\ndata: %s\n\n", pos.EventID, payload); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// writeFrame writes a control frame: a named event with a JSON body, and no `id:` line.
//
// The absence of `id:` is deliberate. A control frame must not become the client's Last-Event-ID
// — that field has to keep pointing at the last *event* the client accepted, or a reconnect
// would resume from a frame the history has never heard of.
func writeFrame(w http.ResponseWriter, flusher http.Flusher, name string, body any) {
	payload, err := json.Marshal(body)
	if err != nil {
		payload = []byte(`{}`)
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, payload)
	flusher.Flush()
}
