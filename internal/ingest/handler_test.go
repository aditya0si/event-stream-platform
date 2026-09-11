package ingest_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aditya0si/event-stream-platform/internal/event"
	"github.com/aditya0si/event-stream-platform/internal/ingest"
	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
	"github.com/aditya0si/event-stream-platform/internal/platform/metrics"
	"github.com/aditya0si/event-stream-platform/internal/platform/reqid"
)

// fixedNow is when these tests believe it is. The handler takes its clock from Options
// precisely so a test can hold time still; without that, the future-skew check would make
// these tests depend on when they run.
var fixedNow = time.Date(2026, 9, 11, 10, 16, 0, 0, time.UTC)

// fakePublisher records what the handler asked it to publish, and can be made to fail.
//
// It is the one place in this suite that stands in for a real dependency, and it exists to
// test the handler's *decisions* — which events are refused, what the response says, whether
// a broker failure is reported as retryable. The publish path itself is verified against a
// real broker by internal/platform/broker's integration test and by the compose smoke test.
type fakePublisher struct {
	mu      sync.Mutex
	topics  []string
	batches [][]broker.Record
	err     error
}

func (f *fakePublisher) Produce(_ context.Context, topic string, recs []broker.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topics = append(f.topics, topic)
	f.batches = append(f.batches, recs)
	return f.err
}

func (f *fakePublisher) publishCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, b := range f.batches {
		n += len(b)
	}
	return n
}

func (f *fakePublisher) records() []broker.Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []broker.Record
	for _, b := range f.batches {
		out = append(out, b...)
	}
	return out
}

type harness struct {
	srv *httptest.Server
	pub *fakePublisher
}

// newHarness builds the handler with production-shaped options and serves it behind the same
// request-id middleware the binary uses, so the middleware is exercised rather than assumed.
func newHarness(t *testing.T, mutate func(*ingest.Options)) *harness {
	t.Helper()

	pub := &fakePublisher{}
	opts := ingest.Options{
		Topic:        "telemetry.test.v1",
		Source:       "test",
		MaxBodyBytes: 1 << 20,
		MaxBatchSize: 100,
		Log:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Now:          func() time.Time { return fixedNow },
	}
	if mutate != nil {
		mutate(&opts)
	}

	h, err := ingest.New(pub, opts)
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}

	mux := http.NewServeMux()
	h.Mount(mux)
	srv := httptest.NewServer(reqid.Middleware(mux))
	t.Cleanup(srv.Close)

	return &harness{srv: srv, pub: pub}
}

func (h *harness) post(t *testing.T, body string, headers map[string]string) (int, http.Header, []byte) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, h.srv.URL+"/v1/events", strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("POST /v1/events: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, resp.Header, raw
}

// observation renders one vehicle position as a producer would send it.
func observation(vehicleID string, mutate func(map[string]any)) json.RawMessage {
	m := map[string]any{
		"vehicle_id": vehicleID,
		"route_id":   "r-7",
		"lat":        12.9716,
		"lon":        77.5946,
		"event_ts":   "2026-09-11T10:15:29Z",
		"sequence":   1,
	}
	if mutate != nil {
		mutate(m)
	}
	raw, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return raw
}

// batch renders a request body from a list of observations.
func batch(events ...json.RawMessage) string {
	body, err := json.Marshal(map[string]any{"events": events})
	if err != nil {
		panic(err)
	}
	return string(body)
}

// errEnvelope mirrors the wire error shape without importing the writer, so a change to the
// response format cannot be made invisible by a change to the type it is decoded into.
type errEnvelope struct {
	Error struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details"`
	} `json:"error"`
	RequestID string `json:"request_id"`
}

func TestSubmit_AcceptsAValidBatch(t *testing.T) {
	h := newHarness(t, nil)

	status, _, raw := h.post(t, batch(
		observation("v-1", nil),
		observation("v-2", nil),
	), nil)

	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body=%s)", status, raw)
	}

	var resp struct {
		Accepted int               `json:"accepted"`
		Rejected []event.Rejection `json:"rejected"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("response is not JSON: %v (%s)", err, raw)
	}
	if resp.Accepted != 2 {
		t.Errorf("accepted = %d, want 2", resp.Accepted)
	}
	if len(resp.Rejected) != 0 {
		t.Errorf("rejected = %v, want none", resp.Rejected)
	}
	if got := h.pub.publishCount(); got != 2 {
		t.Errorf("published %d records, want 2", got)
	}
}

// TestSubmit_KeysRecordsByVehicle pins the property the ordering guarantee rests on. If the
// key were the event id, or absent, every event for one vehicle would be spread across
// partitions and per-vehicle order would be gone — with nothing failing.
func TestSubmit_KeysRecordsByVehicle(t *testing.T) {
	h := newHarness(t, nil)

	status, _, raw := h.post(t, batch(
		observation("v-42", nil),
		observation("v-42", nil),
		observation("v-7", nil),
	), nil)
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", status, raw)
	}

	seen := map[string]int{}
	for _, r := range h.pub.records() {
		seen[r.Key]++
		if len(r.Value) == 0 {
			t.Error("a record was published with no value")
		}
	}
	if seen["v-42"] != 2 || seen["v-7"] != 1 {
		t.Fatalf("record keys = %v, want v-42 twice and v-7 once: the key fixes the partition and "+
			"therefore the per-vehicle ordering guarantee", seen)
	}
}

// TestSubmit_RejectedEventDoesNotFailTheBatch covers the batch policy in DESIGN.md § E.1: one
// malformed report must not stop a fleet's other reports from landing.
func TestSubmit_RejectedEventDoesNotFailTheBatch(t *testing.T) {
	h := newHarness(t, nil)

	status, _, raw := h.post(t, batch(
		observation("v-1", nil),
		observation("v-bad", func(m map[string]any) { m["lat"] = 91.5 }),
		observation("v-3", nil),
	), nil)

	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: one bad event must not fail the batch (body=%s)", status, raw)
	}

	var resp struct {
		Accepted int               `json:"accepted"`
		Rejected []event.Rejection `json:"rejected"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("response is not JSON: %v (%s)", err, raw)
	}
	if resp.Accepted != 2 {
		t.Errorf("accepted = %d, want 2", resp.Accepted)
	}
	if len(resp.Rejected) != 1 {
		t.Fatalf("rejected = %v, want exactly one", resp.Rejected)
	}
	// The index is how a client finds which of its events was refused: a rejected event may
	// be too malformed to have an identifier, so position is the only reliable reference.
	if resp.Rejected[0].Index != 1 {
		t.Errorf("rejected index = %d, want 1", resp.Rejected[0].Index)
	}
	if resp.Rejected[0].Field != "lat" {
		t.Errorf("rejected field = %q, want lat: a client cannot attach an error to an input "+
			"the server will not name", resp.Rejected[0].Field)
	}
	if got := h.pub.publishCount(); got != 2 {
		t.Errorf("published %d records, want 2 (the bad one must not be published)", got)
	}
}

// TestSubmit_PreservesAProducersEventID is the property that makes retransmissions
// detectable at all (ADR-002, ADR-006).
func TestSubmit_PreservesAProducersEventID(t *testing.T) {
	h := newHarness(t, nil)
	id := uuid.Must(uuid.NewV7()).String()

	status, _, raw := h.post(t, batch(
		observation("v-1", func(m map[string]any) { m["event_id"] = id }),
	), nil)
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", status, raw)
	}

	recs := h.pub.records()
	if len(recs) != 1 {
		t.Fatalf("published %d records, want 1", len(recs))
	}
	env, err := event.Decode(recs[0].Value)
	if err != nil {
		t.Fatalf("the published record is not a decodable envelope: %v", err)
	}
	if env.EventID != id {
		t.Fatalf("event_id = %q, want the producer's %q: rewriting it would make every "+
			"retransmission look like a new observation", env.EventID, id)
	}
}

func TestSubmit_MintsAnEventIDWhenNoneIsGiven(t *testing.T) {
	h := newHarness(t, nil)

	if status, _, raw := h.post(t, batch(observation("v-1", nil)), nil); status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", status, raw)
	}
	if status, _, raw := h.post(t, batch(observation("v-1", nil)), nil); status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", status, raw)
	}

	recs := h.pub.records()
	if len(recs) != 2 {
		t.Fatalf("published %d records, want 2", len(recs))
	}
	a, err := event.Decode(recs[0].Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	b, err := event.Decode(recs[1].Value)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if a.EventID == "" || b.EventID == "" {
		t.Fatal("an event was published with no identifier: nothing downstream could deduplicate it")
	}
	if a.EventID == b.EventID {
		t.Fatal("two submissions produced the same identifier")
	}
}

// TestSubmit_PassesDuplicatesThroughDeliberately pins a layering decision that looks like an
// oversight until it is written down: this layer does not deduplicate. It hands both copies to
// the log, and the consumer's transaction is the single place that decides (ADR-006). A
// dedupe cache here would be a second, uncoordinated dedupe — and the one that lies when it
// loses an entry.
func TestSubmit_PassesDuplicatesThroughDeliberately(t *testing.T) {
	h := newHarness(t, nil)
	id := uuid.Must(uuid.NewV7()).String()
	body := batch(observation("v-1", func(m map[string]any) { m["event_id"] = id }))

	if status, _, raw := h.post(t, body, nil); status != http.StatusAccepted {
		t.Fatalf("first submission: status = %d (%s)", status, raw)
	}
	if status, _, raw := h.post(t, body, nil); status != http.StatusAccepted {
		t.Fatalf("retransmission: status = %d, want 202: the ingest layer's job is to durably "+
			"record what arrived, not to decide whether it is a duplicate (%s)", status, raw)
	}

	recs := h.pub.records()
	if len(recs) != 2 {
		t.Fatalf("published %d records, want 2: both copies reach the log, and the consumer's "+
			"primary key is what refuses the second effect", len(recs))
	}
	for i, r := range recs {
		env, err := event.Decode(r.Value)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if env.EventID != id {
			t.Errorf("record %d carries event_id %q, want %q", i, env.EventID, id)
		}
	}
}

func TestSubmit_RequiresAuthenticationWhenConfigured(t *testing.T) {
	const key = "0123456789abcdef0123456789abcdef"
	h := newHarness(t, func(o *ingest.Options) { o.APIKey = key })
	body := batch(observation("v-1", nil))

	// No credential.
	status, _, raw := h.post(t, body, nil)
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d with no credential, want 401 (%s)", status, raw)
	}
	var env errEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("401 body is not JSON: %v", err)
	}
	if env.Error.Code != "unauthorized" {
		t.Errorf("code = %q, want unauthorized", env.Error.Code)
	}
	if got := h.pub.publishCount(); got != 0 {
		t.Fatalf("published %d records without a credential", got)
	}

	// A wrong credential: same response, because distinguishing "no key" from "wrong key"
	// tells an attacker which half they got right.
	statusWrong, _, _ := h.post(t, body, map[string]string{"X-API-Key": "wrong-key-wrong-key-wrong-key-00"})
	if statusWrong != http.StatusUnauthorized {
		t.Errorf("status = %d with a wrong key, want 401", statusWrong)
	}

	// Correct key, as a header.
	status, _, raw = h.post(t, body, map[string]string{"X-API-Key": key})
	if status != http.StatusAccepted {
		t.Fatalf("status = %d with the right key, want 202 (%s)", status, raw)
	}

	// And as a bearer token, which is the other form the handler documents.
	status, _, raw = h.post(t, body, map[string]string{"Authorization": "Bearer " + key})
	if status != http.StatusAccepted {
		t.Fatalf("status = %d with a bearer credential, want 202 (%s)", status, raw)
	}
}

func TestSubmit_RefusesAnOversizedBody(t *testing.T) {
	h := newHarness(t, func(o *ingest.Options) { o.MaxBodyBytes = 200 })

	big := batch(observation("v-1", func(m map[string]any) {
		m["route_id"] = strings.Repeat("r", 500)
	}))
	status, _, raw := h.post(t, big, nil)

	if status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d for an oversized body, want 413 (%s)", status, raw)
	}
	var env errEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("413 body is not JSON: %v", err)
	}
	if env.Error.Code != "payload_too_large" {
		t.Errorf("code = %q, want payload_too_large", env.Error.Code)
	}
	if got := h.pub.publishCount(); got != 0 {
		t.Errorf("published %d records from a body that was refused", got)
	}
}

func TestSubmit_RefusesABatchLargerThanTheLimit(t *testing.T) {
	h := newHarness(t, func(o *ingest.Options) { o.MaxBatchSize = 2 })

	status, _, raw := h.post(t, batch(
		observation("v-1", nil),
		observation("v-2", nil),
		observation("v-3", nil),
	), nil)

	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (%s)", status, raw)
	}
	var env errEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("400 body is not JSON: %v", err)
	}
	// The response must say what the limit is, so a client can size its next attempt rather
	// than guess.
	if env.Error.Details["limit"] == nil || env.Error.Details["received"] == nil {
		t.Errorf("details = %v, want both limit and received", env.Error.Details)
	}
	if got := h.pub.publishCount(); got != 0 {
		t.Errorf("published %d records from a batch that was refused whole", got)
	}
}

func TestSubmit_RefusesAnEmptyBatch(t *testing.T) {
	h := newHarness(t, nil)

	status, _, raw := h.post(t, `{"events":[]}`, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d for an empty batch, want 400 (%s)", status, raw)
	}
	if got := h.pub.publishCount(); got != 0 {
		t.Errorf("published %d records from an empty batch", got)
	}
}

func TestSubmit_RefusesMalformedAndUnknownShapedBodies(t *testing.T) {
	h := newHarness(t, nil)

	for _, tc := range []struct {
		name string
		body string
	}{
		{"not json", `{"events":`},
		{"no events field", `{"event":[]}`},
		{"events not an array", `{"events":"nope"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, _, raw := h.post(t, tc.body, nil)
			if status != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (%s)", status, raw)
			}
			var env errEnvelope
			if err := json.Unmarshal(raw, &env); err != nil {
				t.Fatalf("400 body is not JSON: %v", err)
			}
			if env.Error.Code != "validation_failed" {
				t.Errorf("code = %q, want validation_failed", env.Error.Code)
			}
		})
	}
}

// TestSubmit_ReportsABrokerFailureAsRetryable covers the contract the handler makes when the
// log is unavailable: the batch was not accepted, it is safe to retry, and the response says
// how soon. A 500 here would tell a client its request was wrong; a 200 would tell it the
// events are durable when they are not.
func TestSubmit_ReportsABrokerFailureAsRetryable(t *testing.T) {
	h := newHarness(t, nil)
	h.pub.err = errors.New("broker: 3 of 3 records were not acknowledged: connection refused")

	status, header, raw := h.post(t, batch(observation("v-1", nil), observation("v-2", nil)), nil)

	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d when the log is down, want 503 (%s)", status, raw)
	}
	if got := header.Get("Retry-After"); got == "" {
		t.Error("no Retry-After header: a client that is told to retry should be told when")
	}
	var env errEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("503 body is not JSON: %v", err)
	}
	if env.Error.Code != "broker_unavailable" {
		t.Errorf("code = %q, want broker_unavailable", env.Error.Code)
	}
	if !strings.Contains(env.Error.Message, "retry") {
		t.Errorf("message = %q, want it to say the batch is safe to retry: the client needs to "+
			"know the events were not accepted", env.Error.Message)
	}
	// The internal detail belongs in the log, not in the response.
	if strings.Contains(env.Error.Message, "connection refused") {
		t.Errorf("the response leaks the underlying cause: %q", env.Error.Message)
	}
}

func TestSubmit_EchoesTheRequestIDOnFailures(t *testing.T) {
	h := newHarness(t, func(o *ingest.Options) { o.APIKey = "0123456789abcdef0123456789abcdef" })

	const id = "test-request-1"
	status, header, raw := h.post(t, batch(observation("v-1", nil)), map[string]string{"X-Request-Id": id})
	if status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", status)
	}
	if got := header.Get("X-Request-Id"); got != id {
		t.Errorf("response header X-Request-Id = %q, want %q", got, id)
	}

	var env errEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if env.RequestID != id {
		t.Errorf("body request_id = %q, want %q: it is the one string that finds the server-side "+
			"log line for this request", env.RequestID, id)
	}
}

// TestSubmit_CountsOutcomes checks the metrics an operator would query: how much is arriving,
// and how much of it is being refused. Deltas are used because the counters are process-wide.
func TestSubmit_CountsOutcomes(t *testing.T) {
	h := newHarness(t, nil)

	producedBefore := testutil.ToFloat64(metrics.IngestEventsTotal.WithLabelValues("produced"))
	rejectedBefore := testutil.ToFloat64(metrics.IngestEventsTotal.WithLabelValues("rejected"))

	status, _, raw := h.post(t, batch(
		observation("v-1", nil),
		observation("v-bad", func(m map[string]any) { m["lon"] = 400.0 }),
	), nil)
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (%s)", status, raw)
	}

	producedAfter := testutil.ToFloat64(metrics.IngestEventsTotal.WithLabelValues("produced"))
	rejectedAfter := testutil.ToFloat64(metrics.IngestEventsTotal.WithLabelValues("rejected"))

	if producedAfter-producedBefore != 1 {
		t.Errorf("produced counter moved by %v, want 1", producedAfter-producedBefore)
	}
	if rejectedAfter-rejectedBefore != 1 {
		t.Errorf("rejected counter moved by %v, want 1", rejectedAfter-rejectedBefore)
	}
}
