// Package ingest is the HTTP entry point for telemetry: it validates submissions, gives each
// event an identity, and publishes it to the log.
//
// The package deliberately holds no state that can grow. There is no in-memory buffer of
// pending events and no retry queue inside the process, because a buffer that absorbs a broker
// outage is a buffer that either has a limit — and therefore a silent drop — or does not, and
// therefore is a memory leak wearing a queue's clothes. When the broker is unavailable the
// endpoint says so and the client retries, which is a contract about the client's behaviour
// rather than a promise the server cannot keep.
package ingest

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aditya0si/event-stream-platform/internal/event"
	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
	"github.com/aditya0si/event-stream-platform/internal/platform/httperr"
	"github.com/aditya0si/event-stream-platform/internal/platform/metrics"
	"github.com/aditya0si/event-stream-platform/internal/platform/reqid"
)

// Publisher is the part of the broker this package uses.
//
// It is an interface so that a handler test can drive the endpoint without a broker. That is
// a narrow exception to "tests run against real dependencies" (NFR-9): the real dependencies
// are exercised by the broker package's own integration tests and by the smoke test against
// the running stack, while this interface exists to test the handler's *decisions* — which
// events are rejected, which batch shape is refused — without a broker in the loop.
type Publisher interface {
	Produce(ctx context.Context, topic string, recs []broker.Record) error
}

// Options configures the handler.
type Options struct {
	Topic         string
	Source        string
	APIKey        string
	MaxBodyBytes  int64
	MaxBatchSize  int
	Log           *slog.Logger
	Now           func() time.Time
	AllowedOrigin string
}

// Handler serves POST /v1/events.
type Handler struct {
	pub  Publisher
	opts Options
	log  *slog.Logger
	now  func() time.Time
}

// New builds the handler.
func New(pub Publisher, opts Options) (*Handler, error) {
	if pub == nil {
		return nil, errors.New("ingest: nil publisher")
	}
	if opts.Topic == "" {
		return nil, errors.New("ingest: a topic is required")
	}
	if opts.Log == nil {
		return nil, errors.New("ingest: a logger is required")
	}
	if opts.MaxBatchSize < 1 {
		return nil, errors.New("ingest: MaxBatchSize must be at least 1")
	}
	if opts.MaxBodyBytes < 1 {
		return nil, errors.New("ingest: MaxBodyBytes must be at least 1")
	}
	h := &Handler{pub: pub, opts: opts, log: opts.Log, now: opts.Now}
	if h.now == nil {
		h.now = time.Now
	}
	if h.opts.Source == "" {
		h.opts.Source = "ingest"
	}
	return h, nil
}

// Mount registers the routes on a mux.
func (h *Handler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/events", h.handleSubmit)
}

// SubmitResponse is the reply to an accepted batch.
type SubmitResponse struct {
	Accepted int               `json:"accepted"`
	Rejected []event.Rejection `json:"rejected,omitempty"`
}

// handleSubmit validates a batch and publishes the events that pass.
//
// # Why a bad event does not fail the batch
//
// A batch carries reports from many vehicles, and one malformed report must not prevent the
// others from landing — that would let a single broken device wedge a fleet's ingestion. So
// each event is judged on its own: the accepted ones are published, the rest are named in the
// response with the index and the field at fault. The response is 202, not 200: the events are
// durable in the log, but they have not been *processed* yet, and saying "accepted" while
// implying "stored" would misrepresent where the data actually is.
func (h *Handler) handleSubmit(w http.ResponseWriter, r *http.Request) {
	requestID := reqid.FromContext(r.Context())

	if !h.authorized(r) {
		metrics.IngestRequestsTotal.WithLabelValues("unauthorised").Inc()
		// The reason is logged, never returned: telling a caller that their key was wrong in
		// a specific way helps an attacker and no honest client.
		h.log.Warn("rejected an ingest request", "request_id", requestID, "reason", "missing or invalid api key")
		httperr.Unauthorized(w, r)
		return
	}

	// LimitReader before decoding, so an oversized body is refused while reading rather than
	// after the whole thing is in memory. MaxBytesReader also closes the connection, which is
	// the correct signal to a client that is streaming something enormous.
	r.Body = http.MaxBytesReader(w, r.Body, h.opts.MaxBodyBytes)

	var batch event.Batch
	dec := json.NewDecoder(r.Body)
	// Unknown fields at the batch level are refused for the same reason as inside an
	// observation: silently ignoring `event` instead of `events` would acknowledge zero
	// events as a success.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&batch); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			metrics.IngestRequestsTotal.WithLabelValues("rejected").Inc()
			httperr.TooLarge(w, r, h.opts.MaxBodyBytes)
			return
		}
		metrics.IngestRequestsTotal.WithLabelValues("malformed").Inc()
		h.log.Warn("malformed ingest body", "request_id", requestID, "err", err)
		httperr.Invalid(w, r, "", "the request body is not a valid batch: "+err.Error())
		return
	}

	if len(batch.Events) == 0 {
		metrics.IngestRequestsTotal.WithLabelValues("rejected").Inc()
		httperr.Invalid(w, r, "events", "the batch contains no events")
		return
	}
	if len(batch.Events) > h.opts.MaxBatchSize {
		metrics.IngestRequestsTotal.WithLabelValues("rejected").Inc()
		httperr.Write(w, r, http.StatusBadRequest, httperr.CodeValidationFailed,
			"the batch is larger than this service accepts",
			map[string]any{"field": "events", "limit": h.opts.MaxBatchSize, "received": len(batch.Events)})
		return
	}

	now := h.now().UTC()

	var (
		recs       []broker.Record
		rejections []event.Rejection
	)
	for i, raw := range batch.Events {
		env, vp, err := event.Accept(raw, h.opts.Source, now)
		if err != nil {
			rejections = append(rejections, event.Rejection{
				Index:  i,
				Field:  event.FieldOf(err),
				Reason: err.Error(),
			})
			metrics.IngestEventsTotal.WithLabelValues("rejected").Inc()
			continue
		}

		payload, err := env.Marshal()
		if err != nil {
			// A payload that validated and then failed to marshal is a bug in this process,
			// not bad input. Log it with the event id so it can be reproduced, and fail the
			// request rather than silently dropping the event — a dropped event the client
			// believes was accepted is the one outcome this system must never produce.
			metrics.IngestRequestsTotal.WithLabelValues("error").Inc()
			h.log.Error("a validated event could not be serialized",
				"request_id", requestID, "event_id", env.EventID, "err", err)
			httperr.Internal(w, r)
			return
		}

		// The key is the vehicle, which fixes the partition. Every event for one vehicle
		// therefore lands on one partition in produce order, which is what makes the
		// per-vehicle ordering guarantee (ADR-005) mean anything.
		recs = append(recs, broker.Record{Key: vp.VehicleID, Value: payload})
	}

	if len(recs) > 0 {
		started := time.Now()
		if err := h.pub.Produce(r.Context(), h.opts.Topic, recs); err != nil {
			// Partial success is reported as failure on purpose. The caller retries the
			// batch, and the retry is safe because every event keeps its event_id and the
			// consumer deduplicates on it (ADR-002). A duplicate that is provably absorbed
			// beats a silence that is not provable at all.
			metrics.IngestRequestsTotal.WithLabelValues("error").Inc()
			h.log.Error("producing to the log failed",
				"request_id", requestID, "topic", h.opts.Topic, "events", len(recs), "err", err)
			httperr.Unavailable(w, r,
				"the event log is unavailable; the batch was not accepted and is safe to retry")
			return
		}
		metrics.IngestProduceDuration.Observe(time.Since(started).Seconds())
		for range recs {
			metrics.IngestEventsTotal.WithLabelValues("produced").Inc()
		}
	}

	metrics.IngestBatchSize.Observe(float64(len(batch.Events)))
	metrics.IngestRequestsTotal.WithLabelValues("accepted").Inc()

	accepted := len(recs)
	rejected := len(rejections)
	level := slog.LevelInfo
	if rejected > 0 {
		level = slog.LevelWarn
	}
	h.log.Log(r.Context(), level, "batch accepted",
		"request_id", requestID, "accepted", accepted, "rejected", rejected, "topic", h.opts.Topic)

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(SubmitResponse{Accepted: accepted, Rejected: rejections})
}

// authorized checks the API key.
//
// In development no key is configured and everything is accepted; the config package refuses
// to boot with an empty key when APP_ENV=prod, so this branch cannot silently become a
// production hole.
func (h *Handler) authorized(r *http.Request) bool {
	if h.opts.APIKey == "" {
		return true
	}
	got := r.Header.Get("X-API-Key")
	if got == "" {
		if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, "Bearer ") {
			got = strings.TrimPrefix(auth, "Bearer ")
		}
	}
	// Constant time: a comparison that returns early leaks the length of the matching prefix,
	// which is enough to recover a key one character at a time.
	return subtle.ConstantTimeCompare([]byte(got), []byte(h.opts.APIKey)) == 1
}
