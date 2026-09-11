// Command simulate is the deterministic fleet producer (ADR-008): it advances a set of vehicles
// along committed route geometry and posts their observations to the ingest endpoint.
//
// # Why this is a binary and not a test fixture
//
// Three things need a producer, and they need the same one. The viewer needs something to draw
// when a reviewer opens it. The benchmark needs a stream of known shape and known rate, or its
// numbers describe the generator rather than the pipeline. And a demo needs to be reproducible:
// the same seed produces the same positions in the same order, so two runs are comparable.
//
// # The rate is a target, and the summary says whether it was reached
//
// Pacing is self-correcting against a wall-clock schedule rather than sleeping a fixed interval,
// so it does not drift. But a producer cannot make a slow server fast: when the pipeline cannot
// absorb the target rate, the offered load backs up and the achieved rate falls below it. That
// difference is the most interesting number this binary prints, which is why the summary reports
// offered and achieved separately instead of claiming the target was hit.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aditya0si/event-stream-platform/internal/platform/config"
	"github.com/aditya0si/event-stream-platform/internal/platform/idgen"
	"github.com/aditya0si/event-stream-platform/internal/platform/logging"
	"github.com/aditya0si/event-stream-platform/internal/simulator"
)

// This file sets none of the envelope's own fields — schema_version, event_type, produced_at,
// source. The ingest service owns those, because a client that could assert them could claim a
// schema version the platform does not implement. A producer supplies the observation and,
// optionally, the event's identity. See the wire-shape note below.

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "simulate: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// RoleProducer, not Load: this process owns no database and never opens one. Saying so
	// is what keeps the service role's fail-fast requirement meaningful.
	cfg, err := config.LoadFor(config.RoleProducer)
	if err != nil {
		return err
	}

	// Flags override the environment, with the environment's value as each flag's default. The
	// environment is the deployment's shape; a flag is a benchmark changing one variable.
	fs := flag.NewFlagSet("simulate", flag.ExitOnError)
	fs.StringVar(&cfg.Simulator.IngestURL, "url", cfg.Simulator.IngestURL,
		"ingest endpoint the fleet posts to")
	fs.IntVar(&cfg.Simulator.Vehicles, "vehicles", cfg.Simulator.Vehicles,
		"size of the simulated fleet")
	fs.Int64Var(&cfg.Simulator.Seed, "seed", cfg.Simulator.Seed,
		"seed for placement and speed; the same seed reproduces the same run")
	fs.IntVar(&cfg.Simulator.Rate, "rate", cfg.Simulator.Rate,
		"target events per second across all workers")
	fs.IntVar(&cfg.Simulator.BatchSize, "batch", cfg.Simulator.BatchSize,
		"observations per HTTP request; bounded by INGEST_MAX_BATCH on the server")
	fs.IntVar(&cfg.Simulator.Workers, "workers", cfg.Simulator.Workers,
		"requests in flight at once")
	fs.IntVar(&cfg.Simulator.StepMS, "step-ms", cfg.Simulator.StepMS,
		"simulated milliseconds advanced per step")
	fs.DurationVar(&cfg.Simulator.Duration, "duration", cfg.Simulator.Duration,
		"stop after this long and print a summary; 0 runs until interrupted")
	if err := fs.Parse(os.Args[1:]); err != nil {
		return err
	}

	log := logging.New(cfg.LogLevel)

	geometry, err := simulator.LoadDefault()
	if err != nil {
		return err
	}
	fleet, err := simulator.NewFleet(geometry, simulator.FleetConfig{
		Seed:     cfg.Simulator.Seed,
		Vehicles: cfg.Simulator.Vehicles,
	})
	if err != nil {
		return err
	}

	log.Info("fleet ready",
		"vehicles", len(fleet.Vehicles()), "routes", len(fleet.Routes()),
		"seed", cfg.Simulator.Seed, "target_rate", cfg.Simulator.Rate,
		"batch", cfg.Simulator.BatchSize, "workers", cfg.Simulator.Workers,
		"step_ms", cfg.Simulator.StepMS, "endpoint", cfg.Simulator.IngestURL,
		"duration", cfg.Simulator.Duration.String())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Simulator.Duration > 0 {
		// A bounded run is what a benchmark uses: the process stops itself and prints a summary,
		// so a harness does not have to decide when to kill it.
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.Simulator.Duration)
		defer cancel()
	}

	p := &producer{
		cfg:  cfg.Simulator,
		log:  log,
		http: newClient(cfg.Simulator.Workers),
	}
	return p.run(ctx, fleet)
}

// producer posts batches, pacing itself against a wall-clock schedule.
type producer struct {
	cfg  config.Simulator
	log  *slog.Logger
	http *http.Client

	// The counters are atomic because the workers increment them concurrently. They are read
	// only by the periodic reporter and the summary, both single-goroutine, but a plain int64
	// written by eight goroutines is a race by definition and `-race` would say so.
	offered   atomic.Int64
	accepted  atomic.Int64
	rejected  atomic.Int64
	batches   atomic.Int64
	failed    atomic.Int64
	bytesSent atomic.Int64
}

// run drives the fleet until the context ends, then drains and reports.
func (p *producer) run(ctx context.Context, fleet *simulator.Fleet) error {
	batches := make(chan []simulator.Observation, p.cfg.Workers*2)

	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batches {
				p.post(ctx, batch)
			}
		}()
	}

	// The reporter is a separate goroutine so a slow HTTP call cannot delay the numbers that
	// describe it. Nothing in the system reads it; it exists to make a long run legible.
	reportDone := make(chan struct{})
	go func() {
		defer close(reportDone)
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		started := time.Now()
		var last int64
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				total := p.accepted.Load()
				elapsed := now.Sub(started).Seconds()
				p.log.Info("streaming",
					"accepted", total, "achieved_per_s", round1(float64(total)/elapsed),
					"interval_per_s", round1(float64(total-last)/5),
					"failed", p.failed.Load(), "rejected", p.rejected.Load())
				last = total
			}
		}
	}()

	started := time.Now()
	p.generate(ctx, fleet, batches)
	close(batches)
	wg.Wait()
	<-reportDone

	p.summary(started)
	if p.failed.Load() > 0 {
		// A non-zero exit when the pipeline refused work, so a benchmark harness cannot mistake
		// a run that mostly failed for a fast one.
		return fmt.Errorf("%d batch(es) failed to send; the offered rate was not sustainable",
			p.failed.Load())
	}
	return nil
}

// generate advances the fleet and dispatches batches, paced to the target rate.
//
// The pacing is the interesting part, and it is schedule-based rather than interval-based:
// progress is measured against "how many observations should have been offered by now" instead
// of "sleep for N milliseconds after each step". A fixed sleep accumulates the duration of every
// send and every scheduling hiccup, so a run that starts at 2,000/s quietly ends at 1,800/s and
// the shortfall looks like the pipeline's. Comparing against a deadline makes each iteration
// correct itself.
//
// The channel send blocks on purpose. When the pipeline cannot absorb the offered rate, the
// generator must slow down and the achieved rate must fall — that is the honest signal. A
// non-blocking send would drop observations instead, and a benchmark that discards its own input
// measures nothing.
func (p *producer) generate(ctx context.Context, fleet *simulator.Fleet, out chan<- []simulator.Observation) {
	dt := float64(p.cfg.StepMS) / 1000.0
	perSecond := float64(p.cfg.Rate)
	started := time.Now()
	offered := 0

	batch := make([]simulator.Observation, 0, p.cfg.BatchSize)
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		select {
		case out <- batch:
			batch = make([]simulator.Observation, 0, p.cfg.BatchSize)
			return true
		case <-ctx.Done():
			return false
		}
	}

	for {
		if ctx.Err() != nil {
			flush()
			return
		}

		for _, obs := range fleet.Step(dt) {
			batch = append(batch, obs)
			offered++
			if len(batch) >= p.cfg.BatchSize && !flush() {
				return
			}
		}
		p.offered.Store(int64(offered))

		// How far into the run we should be to have offered this many observations.
		due := started.Add(time.Duration(float64(offered) / perSecond * float64(time.Second)))
		if wait := time.Until(due); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				flush()
				return
			case <-timer.C:
			}
		}
	}
}

// The wire shape, declared here rather than imported from internal/ingest.
//
// # A submission is a flat observation; the envelope is built by the server
//
// This is the mistake the first version of this file made, and it is worth stating plainly
// because the shape is counter-intuitive: what a producer POSTs is *not* the envelope that
// docs/DESIGN.md shows. The endpoint parses each element of `events` into a flat observation,
// validates it, and then builds the envelope — schema_version, event_type, produced_at, source
// — itself before publishing that to the log.
//
// The division is deliberate. If a client supplied the envelope it could claim a schema_version
// this build does not implement, or a produced_at that predates its own observation; the fields
// that describe how the platform handled an event belong to the platform. The one identity a
// producer may assert is event_id, which is what makes a retransmission detectable.
//
// Sending the envelope shape is not a quiet failure — every unrecognised field is refused rather
// than ignored — and the deployed smoke test caught it as 680 rejections out of 680 events, with
// the reason `unknown field "schema_version"`. The comment that preceded this one described the
// contract backwards, which is why running the producer against the real server before quoting
// any measurement from it was worth the round trip.
type ingestRequest struct {
	Events []json.RawMessage `json:"events"`
}

type observation struct {
	// EventID is optional and its presence has meaning: supply it and a retransmission of the
	// same report stays detectable at the sink, because the identifier travels with it. Omit it
	// and the platform mints one, and that report has no retransmission semantics because there
	// is nothing to retransmit.
	EventID    string    `json:"event_id,omitempty"`
	VehicleID  string    `json:"vehicle_id"`
	RouteID    string    `json:"route_id"`
	Lat        float64   `json:"lat"`
	Lon        float64   `json:"lon"`
	SpeedKPH   float64   `json:"speed_kph"`
	BearingDeg float64   `json:"bearing_deg"`
	EventTS    time.Time `json:"event_ts"`
	Sequence   uint64    `json:"sequence"`
}

// post sends one batch.
func (p *producer) post(ctx context.Context, batch []simulator.Observation) {

	events := make([]json.RawMessage, 0, len(batch))
	for _, o := range batch {
		// Stamped per observation rather than per batch, and that is a correctness choice rather
		// than precision for its own sake: the sink compares event_ts to decide whether an
		// observation may move a vehicle's current state, so two observations of one vehicle
		// sharing a timestamp would make the second look stale — refused by the map while still
		// kept in the history. A batch holds one observation per vehicle at the default sizes,
		// but a batch larger than the fleet would not, and the rule should not depend on that.
		observedAt := time.Now().UTC()

		// A fresh identity per event, which is the point of it: a retransmission of the same
		// observation stays detectable at the sink, and a producer that reused identities would
		// be measuring the deduplication path instead of the pipeline.
		raw, err := json.Marshal(observation{
			EventID:    idgen.New(),
			VehicleID:  o.VehicleID,
			RouteID:    o.RouteID,
			Lat:        o.Lat,
			Lon:        o.Lon,
			SpeedKPH:   o.SpeedKPH,
			BearingDeg: o.BearingDeg,
			EventTS:    observedAt,
			Sequence:   o.Sequence,
		})
		if err != nil {
			// Unreachable for this shape — every field is a plain scalar — but a silent skip
			// here would understate the batch and make the summary disagree with what was sent.
			p.failed.Add(1)
			p.log.Error("could not encode an event", "err", err, "vehicle", o.VehicleID)
			return
		}
		events = append(events, raw)
	}

	body, err := json.Marshal(ingestRequest{Events: events})
	if err != nil {
		p.failed.Add(1)
		p.log.Error("could not encode a batch", "err", err, "events", len(events))
		return
	}
	p.bytesSent.Add(int64(len(body)))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.IngestURL, bytes.NewReader(body))
	if err != nil {
		p.failed.Add(1)
		p.log.Error("could not build a request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if p.cfg.APIKey != "" {
		req.Header.Set("X-API-Key", p.cfg.APIKey)
	}

	p.batches.Add(1)
	resp, err := p.http.Do(req)
	if err != nil {
		// A cancelled context is shutdown, not a failure: the run is ending and the in-flight
		// batch is abandoned deliberately.
		if ctx.Err() != nil {
			return
		}
		p.failed.Add(1)
		p.log.Warn("posting a batch failed", "err", err, "events", len(events))
		return
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	switch {
	case resp.StatusCode == http.StatusAccepted:
		// The server answers 202 with a count of accepted and rejected events, and the two can
		// differ within one batch: a single malformed observation is rejected while its siblings
		// are published. Counting the whole batch as accepted would hide exactly the kind of
		// contract drift this file's shape is meant to avoid.
		var decoded struct {
			Accepted int `json:"accepted"`
			Rejected []struct {
				Index  int    `json:"index"`
				Field  string `json:"field"`
				Reason string `json:"reason"`
			} `json:"rejected"`
		}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			// A 202 whose body cannot be read still means the events were accepted; the count is
			// then an estimate, and the log says so rather than pretending to precision.
			p.accepted.Add(int64(len(events)))
			p.log.Warn("accepted a batch but could not read the response body",
				"err", err, "body", truncate(string(raw), 200))
			return
		}
		p.accepted.Add(int64(decoded.Accepted))
		if n := len(decoded.Rejected); n > 0 {
			p.rejected.Add(int64(n))
			// Warn, with the first reason: a producer whose payloads are wrong should be
			// obvious on the console, not buried in a counter at the end of the run.
			p.log.Warn("some events were rejected",
				"rejected", n, "accepted", decoded.Accepted,
				"first_field", decoded.Rejected[0].Field,
				"first_reason", decoded.Rejected[0].Reason)
		}
	default:
		p.failed.Add(1)
		p.log.Warn("the server refused a batch",
			"status", resp.StatusCode, "events", len(events), "body", truncate(string(raw), 200))
	}
}

// summary prints the run's result as a single JSON object on stdout.
//
// JSON rather than prose because this is the artifact a benchmark quotes: the README's numbers
// come from here, and a machine-readable line is one a harness can capture without parsing
// English. Offered and achieved are both reported — see the package comment for why the gap
// between them is the interesting number.
func (p *producer) summary(started time.Time) {
	elapsed := time.Since(started)
	line := map[string]any{
		"duration_s":         round1(elapsed.Seconds()),
		"vehicles":           p.cfg.Vehicles,
		"seed":               p.cfg.Seed,
		"batch_size":         p.cfg.BatchSize,
		"workers":            p.cfg.Workers,
		"target_rate":        p.cfg.Rate,
		"offered":            p.offered.Load(),
		"accepted":           p.accepted.Load(),
		"rejected":           p.rejected.Load(),
		"batches":            p.batches.Load(),
		"failed_batches":     p.failed.Load(),
		"achieved_rate":      round1(float64(p.accepted.Load()) / elapsed.Seconds()),
		"accepted_per_batch": round1(float64(p.accepted.Load()) / float64(max64(1, p.batches.Load()))),
		"bytes_sent":         p.bytesSent.Load(),
	}
	encoded, err := json.Marshal(line)
	if err != nil {
		p.log.Error("could not encode the summary", "err", err)
		return
	}
	fmt.Println(string(encoded))
}

// newClient builds the HTTP client. Connection reuse is not a micro-optimisation here: at a few
// thousand requests per second, a fresh TCP connection per batch would make the producer's own
// handshakes a measurable part of the load it is trying to measure.
func newClient(workers int) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        workers * 4,
		MaxIdleConnsPerHost: workers * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	return &http.Client{
		Transport: transport,
		// A per-request timeout, so one stalled connection cannot pin a worker for the run.
		Timeout: 15 * time.Second,
	}
}

func round1(v float64) float64 { return float64(int64(v*10+0.5)) / 10 }

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
