package gateway_test

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aditya0si/event-stream-platform/internal/fanout"
	"github.com/aditya0si/event-stream-platform/internal/gateway"
)

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// --- fakes -------------------------------------------------------------------------------

// fakeSource stands in for the Redis subscriber, so these tests exercise the gateway's own
// logic rather than go-redis. The bus itself is proved against a real Redis by the smoke test.
type fakeSource struct {
	frames chan fanout.Position
}

func newFakeSource() *fakeSource {
	return &fakeSource{frames: make(chan fanout.Position, 128)}
}

func (f *fakeSource) Frames(context.Context) (<-chan fanout.Position, func(), error) {
	return f.frames, func() {}, nil
}

func (f *fakeSource) push(p fanout.Position) { f.frames <- p }

// fakeHistory stands in for sink.Store, and records what it was asked for: the resume point's
// *identity* is the thing under test, not just its timestamp.
type fakeHistory struct {
	mu      sync.Mutex
	times   map[string]time.Time
	since   []fanout.Position
	current fanout.Position
	found   bool
	err     error

	gotID  string
	gotLim int
}

func newFakeHistory() *fakeHistory { return &fakeHistory{times: map[string]time.Time{}} }

func (h *fakeHistory) PositionTime(_ context.Context, eventID string) (time.Time, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err != nil {
		return time.Time{}, false, h.err
	}
	ts, ok := h.times[eventID]
	return ts, ok, nil
}

func (h *fakeHistory) PositionsSince(_ context.Context, _ time.Time, afterID string, limit int) ([]fanout.Position, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.gotID, h.gotLim = afterID, limit
	if h.err != nil {
		return nil, h.err
	}
	return h.since, nil
}

func (h *fakeHistory) CurrentPosition(_ context.Context, _ string) (fanout.Position, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.err != nil {
		return fanout.Position{}, false, h.err
	}
	return h.current, h.found, nil
}

func (h *fakeHistory) resumePointID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.gotID
}

func (h *fakeHistory) setTimes(id string, ts time.Time) {
	h.mu.Lock()
	h.times[id] = ts
	h.mu.Unlock()
}

func (h *fakeHistory) setSince(ps ...fanout.Position) {
	h.mu.Lock()
	h.since = ps
	h.mu.Unlock()
}

// --- reading a stream --------------------------------------------------------------------

// streamReader owns the single goroutine that reads one stream.
//
// One reader per connection is a correctness requirement, not tidiness, and this type exists
// because the first version of these tests got it wrong. It handed the same *bufio.Reader to a
// fresh helper per assertion, each of which spawned its own reader goroutine — so the first
// goroutine kept consuming the connection's lines into a channel nobody was reading any more,
// and the second assertion saw an empty string. bufio buffers: "the next frame" only means
// anything relative to the one reader that owns the connection.
type streamReader struct {
	mu   sync.Mutex
	text string
}

func newStreamReader(body io.Reader) *streamReader {
	sr := &streamReader{}
	go func() {
		br := bufio.NewReader(body)
		for {
			line, err := br.ReadString('\n')
			if line != "" {
				sr.mu.Lock()
				sr.text += line
				sr.mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()
	return sr
}

// until waits for text satisfying need, returning everything received either way so the caller
// can assert on the failure detail instead of a bare timeout.
//
// Predicates match the *content* the test then asserts on — the data line, not the `event:`
// line. An SSE frame is several writes, so a predicate satisfied by the event name alone can
// fire before its payload exists, and the assertions that follow would then fail against text
// that was merely early rather than wrong. Both of those bugs were live in the first version of
// this file.
func (sr *streamReader) until(d time.Duration, need func(string) bool) string {
	deadline := time.Now().Add(d)
	for {
		sr.mu.Lock()
		got := sr.text
		sr.mu.Unlock()
		if need != nil && need(got) {
			return got
		}
		if time.Now().After(deadline) {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// --- harness -----------------------------------------------------------------------------

type harness struct {
	src *fakeSource
	his *fakeHistory
	gw  *gateway.Gateway
	srv *httptest.Server
}

func newHarness(t *testing.T, opts gateway.Options) *harness {
	t.Helper()
	src, his := newFakeSource(), newFakeHistory()
	opts.Log = discardLogger()
	if opts.ClientBuffer == 0 {
		opts.ClientBuffer = 8
	}
	if opts.MaxClients == 0 {
		opts.MaxClients = 8
	}

	gw, err := gateway.New(src, his, opts)
	if err != nil {
		t.Fatalf("gateway.New: %v", err)
	}
	if _, err := gw.Start(context.Background()); err != nil {
		t.Fatalf("gateway.Start: %v", err)
	}

	mux := http.NewServeMux()
	gw.Mount(mux)
	gw.MountViewer(mux)

	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		gw.Shutdown()
		srv.Close()
	})
	return &harness{src: src, his: his, gw: gw, srv: srv}
}

// open starts a stream and returns its one reader.
func (h *harness) open(t *testing.T, lastEventID string) (*http.Response, *streamReader) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.srv.URL+"/v1/stream", nil)
	if err != nil {
		cancel()
		t.Fatalf("new request: %v", err)
	}
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("GET /v1/stream: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = resp.Body.Close()
	})
	return resp, newStreamReader(resp.Body)
}

// --- the stream --------------------------------------------------------------------------

func TestStream_SendsControlFramesWithoutAnID(t *testing.T) {
	h := newHarness(t, gateway.Options{})
	_, sr := h.open(t, "")

	got := sr.until(5*time.Second, func(s string) bool { return strings.Contains(s, `"resumed":false`) })

	if !strings.Contains(got, "event: ready") {
		t.Fatalf("no ready frame within 5s; got %q", got)
	}
	// The absence of `id:` is a correctness property, not formatting: Last-Event-ID must keep
	// pointing at the last *event* the client accepted. If a control frame carried an id, a
	// reconnect would resume from a frame the history has never heard of and reset the client.
	if strings.Contains(got, "id:") {
		t.Errorf("a control frame carried an id, which would poison the client's resume point: %q", got)
	}
}

func TestStream_DeliversLiveFramesWithTheEventIdentity(t *testing.T) {
	h := newHarness(t, gateway.Options{})
	_, sr := h.open(t, "")
	sr.until(5*time.Second, func(s string) bool { return strings.Contains(s, `"resumed":false`) })

	h.src.push(fanout.Position{
		EventID:   "evt-live-1",
		VehicleID: "veh-1",
		RouteID:   "route-1",
		Lat:       12.9716,
		Lon:       77.5946,
		EventTS:   time.Now().UTC(),
		Sequence:  7,
	})

	got := sr.until(5*time.Second, func(s string) bool {
		return strings.Contains(s, `"event_id":"evt-live-1"`)
	})

	if !strings.Contains(got, "id: evt-live-1") {
		t.Errorf("the frame must carry the event id as its SSE id; got %q", got)
	}
	if !strings.Contains(got, `"vehicle_id":"veh-1"`) {
		t.Errorf("the payload must carry the vehicle; got %q", got)
	}
	if !strings.Contains(got, `"sequence":7`) {
		t.Errorf("the payload must carry the vehicle's sequence; got %q", got)
	}
}

func TestStream_ResumePassesTheIDAsTheResumePoint(t *testing.T) {
	h := newHarness(t, gateway.Options{})
	now := time.Now().UTC()
	h.his.setTimes("evt-resume", now.Add(-time.Second))
	h.his.setSince(
		fanout.Position{EventID: "evt-missed-1", VehicleID: "veh-1", EventTS: now},
		fanout.Position{EventID: "evt-missed-2", VehicleID: "veh-1", EventTS: now},
	)

	_, sr := h.open(t, "evt-resume")
	got := sr.until(5*time.Second, func(s string) bool { return strings.Contains(s, `"frames":2`) })

	for _, want := range []string{"evt-missed-1", "evt-missed-2"} {
		if !strings.Contains(got, want) {
			t.Errorf("the missed event %s was not replayed; got %q", want, got)
		}
	}
	if !strings.Contains(got, "event: resumed") {
		t.Errorf("no resumed frame; got %q", got)
	}
	if strings.Contains(got, "evt-resume") {
		t.Errorf("the resume point itself was replayed; got %q", got)
	}

	// The identity, not just the timestamp: this is the contract that makes the sink's tuple
	// comparison possible, and the reason a same-second event is not dropped.
	if id := h.his.resumePointID(); id != "evt-resume" {
		t.Errorf("PositionsSince was called with resume point %q, want %q", id, "evt-resume")
	}
}

func TestStream_UnknownResumePointResets(t *testing.T) {
	h := newHarness(t, gateway.Options{})
	_, sr := h.open(t, "evt-that-aged-out")

	got := sr.until(5*time.Second, func(s string) bool {
		return strings.Contains(s, "no longer in the retained history")
	})

	if !strings.Contains(got, "event: reset") {
		t.Fatalf("an unknown resume point must tell the client to start fresh; got %q", got)
	}
}

func TestStream_ResumeOlderThanTheWindowResets(t *testing.T) {
	h := newHarness(t, gateway.Options{ReplayWindow: time.Minute})
	h.his.setTimes("evt-old", time.Now().UTC().Add(-time.Hour))

	_, sr := h.open(t, "evt-old")
	got := sr.until(5*time.Second, func(s string) bool {
		return strings.Contains(s, "older than the replay window")
	})

	if !strings.Contains(got, "event: reset") {
		t.Fatalf("a resume point outside the window must reset rather than replay; got %q", got)
	}
}

func TestStream_RefusesAtCapacityWithRetryAfter(t *testing.T) {
	h := newHarness(t, gateway.Options{MaxClients: 1})

	// The first client must be *registered* before the second attempt, not merely connected:
	// its ready frame is what proves the handler is past register().
	_, sr := h.open(t, "")
	if got := sr.until(5*time.Second, func(s string) bool {
		return strings.Contains(s, `"resumed":false`)
	}); !strings.Contains(got, "event: ready") {
		t.Fatalf("the first client never registered; got %q", got)
	}

	resp, err := http.Get(h.srv.URL + "/v1/stream")
	if err != nil {
		t.Fatalf("second GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// 503 rather than 429: this is capacity, not rate, and the hint tells the client it is safe
	// to come back after the existing viewers have cycled.
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("Retry-After"); got != "5" {
		t.Errorf("Retry-After = %q, want \"5\"", got)
	}
}

// --- the read endpoints -------------------------------------------------------------------

func TestViewerIsServedAtTheRootOnly(t *testing.T) {
	h := newHarness(t, gateway.Options{})

	resp, err := http.Get(h.srv.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("GET / = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	// The page and the stream ship in one binary, so the frame names it handles are the
	// gateway's contract by construction rather than by agreement between two repositories.
	if !strings.Contains(string(body), "new EventSource") {
		t.Error("the viewer does not open an EventSource stream")
	}

	// `/{$}` matches the root and nothing else. A bare "/" would be a prefix pattern and would
	// turn every typo into this page instead of a 404.
	missing, err := http.Get(h.srv.URL + "/nope")
	if err != nil {
		t.Fatalf("GET /nope: %v", err)
	}
	_ = missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Errorf("GET /nope = %d, want 404", missing.StatusCode)
	}
}

func TestCurrentVehicle_ReportsNotFoundRatherThanFailing(t *testing.T) {
	h := newHarness(t, gateway.Options{})

	resp, err := http.Get(h.srv.URL + "/v1/vehicles/veh-unknown")
	if err != nil {
		t.Fatalf("GET /v1/vehicles: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown vehicle = %d, want 404", resp.StatusCode)
	}

	h.his.mu.Lock()
	h.his.current = fanout.Position{EventID: "evt-1", VehicleID: "veh-1", Lat: 1, Lon: 2, EventTS: time.Now().UTC()}
	h.his.found = true
	h.his.mu.Unlock()

	resp2, err := http.Get(h.srv.URL + "/v1/vehicles/veh-1")
	if err != nil {
		t.Fatalf("GET /v1/vehicles/veh-1: %v", err)
	}
	body, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("known vehicle = %d, want 200", resp2.StatusCode)
	}
	if !strings.Contains(string(body), `"event_id":"evt-1"`) {
		t.Errorf("response did not carry the position: %s", body)
	}
}
