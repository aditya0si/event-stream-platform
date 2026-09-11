// Package httpserver builds the HTTP surface every long-running binary shares: liveness,
// readiness, metrics, and a graceful shutdown that actually finishes in-flight work.
//
// Liveness and readiness are deliberately different endpoints with different meanings,
// because conflating them is how a dependency outage turns into a restart loop:
//
//	/healthz — is this process alive? It answers 200 as long as the process can serve. A
//	           database outage must NOT fail it, because restarting the process cannot fix
//	           a database and would turn one incident into two.
//	/readyz  — can this process do its job right now? It fails when a dependency it needs
//	           is unreachable, so a load balancer stops sending it traffic.
//
// Readiness checks are also what keep the dependency gauges (db_up, redis_up, broker_up)
// fresh, so a dashboard shows a dependency failure even when no traffic is arriving —
// which is precisely when an operator is staring at a quiet graph.
package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Check is one readiness dependency.
type Check struct {
	Name string
	// Fn returns nil when the dependency is usable. It is called with a timeout context,
	// so an implementation may block without the handler hanging.
	Fn func(context.Context) error
	// Gauge, when set, is updated to 1 or 0 on every evaluation of this check — including
	// the background probes, which is what makes a dependency failure visible on a
	// dashboard during a lull in traffic. Nil means the check affects only the readiness
	// verdict.
	//
	// This field exists because it was missing. The package comment above promised the
	// gauges were kept fresh while runChecks never touched them, so every dependency read 0
	// while the service was healthy — three false alarms in exactly the place an operator
	// looks first. The deployed smoke test caught it; the unit tests had not, because they
	// asserted the readiness *body* and never the metrics.
	Gauge prometheus.Gauge
}

// Config configures the server.
type Config struct {
	Addr            string
	Log             *slog.Logger
	Registry        *prometheus.Registry
	Checks          []Check
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
	// ProbeInterval is how often readiness checks run in the background to keep the
	// dependency gauges current. Zero disables the background prober and leaves the gauges
	// updated only by probe traffic.
	ProbeInterval time.Duration
	// ProbeTimeout bounds each individual dependency check.
	ProbeTimeout time.Duration
	// EnableMetrics mounts /metrics. Set false for processes whose metrics have their own
	// listener.
	EnableMetrics bool
}

// Server owns the listener and its lifecycle.
type Server struct {
	cfg      Config
	http     *http.Server
	listener net.Listener
	proberWG sync.WaitGroup

	// done stops the background prober. A ticker's Stop does not close its channel, so a
	// `for range t.C` loop never ends on its own — the prober has to be told, or Shutdown
	// waits on it forever.
	done     chan struct{}
	doneOnce sync.Once
}

// New builds a server. It binds the listener immediately so that a port conflict is a
// startup failure rather than a surprise several seconds later.
func New(cfg Config) (*Server, error) {
	if cfg.Log == nil {
		return nil, errors.New("httpserver: nil logger")
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = 3 * time.Second
	}

	mux := http.NewServeMux()
	s := &Server{cfg: cfg, done: make(chan struct{})}

	// Liveness. Never checks dependencies — see the package comment.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})

	// Readiness. Every check must pass.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		results, ok := runChecks(r.Context(), cfg.Checks, cfg.ProbeTimeout)
		code := http.StatusOK
		status := "ready"
		if !ok {
			code = http.StatusServiceUnavailable
			status = "not_ready"
		}
		writeJSON(w, code, map[string]any{"status": status, "checks": results})
	})

	if cfg.EnableMetrics {
		handler := promhttp.HandlerFor(cfg.Registry, promhttp.HandlerOpts{})
		mux.Handle("GET /metrics", handler)
	}

	s.http = &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		ErrorLog:          slog.NewLogLogger(cfg.Log.Handler(), slog.LevelWarn),
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("httpserver: listen on %s: %w", cfg.Addr, err)
	}
	s.listener = ln
	return s, nil
}

// Addr reports the address actually bound, which differs from the configured one whenever
// the config asked for port 0. Tests rely on this.
func (s *Server) Addr() string { return s.listener.Addr().String() }

// Start begins serving and returns immediately. Serve errors are reported through the
// returned channel, so a caller can distinguish "shut down cleanly" from "listener died".
func (s *Server) Start() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		err := s.http.Serve(s.listener)
		if errors.Is(err, http.ErrServerClosed) {
			errCh <- nil
			return
		}
		errCh <- err
	}()

	if s.cfg.ProbeInterval > 0 && len(s.cfg.Checks) > 0 {
		s.proberWG.Add(1)
		go s.probeLoop()
	}
	return errCh
}

// probeLoop runs the readiness checks on a timer so dependency gauges stay current even
// when no probe traffic arrives.
//
// It selects on a done channel rather than ranging over the ticker, because a stopped
// ticker is not a closed channel: ranging would block forever and Shutdown would never
// return. That is not hypothetical — it is what the first CI run on this repository caught,
// as a test-timeout panic, and it would equally have made the deployed binary ignore
// SIGTERM for the whole grace period before being killed.
func (s *Server) probeLoop() {
	defer s.proberWG.Done()
	t := time.NewTicker(s.cfg.ProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.ProbeTimeout)
			runChecks(ctx, s.cfg.Checks, s.cfg.ProbeTimeout)
			cancel()
		}
	}
}

// Shutdown stops accepting connections and waits for in-flight requests, bounded by the
// configured timeout. It then stops the prober and waits for it to finish.
//
// The order matters: the listener closes first, so no new request can start a check that
// the prober's own stop signal would then race.
func (s *Server) Shutdown(ctx context.Context) error {
	shutdownCtx, cancel := context.WithTimeout(ctx, s.cfg.ShutdownTimeout)
	defer cancel()
	err := s.http.Shutdown(shutdownCtx)
	s.doneOnce.Do(func() { close(s.done) })
	s.proberWG.Wait()
	return err
}

// runChecks executes every check concurrently and reports whether all passed, updating each
// check's gauge with the outcome.
//
// Concurrency matters: three sequential 3-second timeouts would make a readiness probe
// take 9 seconds, and a slow probe is indistinguishable from a failing dependency.
//
// The gauge update is the second half of this function's job, not a side effect: a
// dependency's health has to be observable as a *value* even when nobody is asking whether
// the process is ready, because the alternative is a metric that only exists while traffic
// exists — and a dependency outage is precisely when traffic stops.
func runChecks(ctx context.Context, checks []Check, timeout time.Duration) (map[string]string, bool) {
	if len(checks) == 0 {
		return map[string]string{}, true
	}
	type result struct {
		name  string
		err   error
		gauge prometheus.Gauge
	}
	results := make(chan result, len(checks))
	for _, c := range checks {
		go func(c Check) {
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			results <- result{name: c.Name, err: c.Fn(cctx), gauge: c.Gauge}
		}(c)
	}

	out := make(map[string]string, len(checks))
	ok := true
	for range checks {
		r := <-results
		if r.gauge != nil {
			if r.err != nil {
				r.gauge.Set(0)
			} else {
				r.gauge.Set(1)
			}
		}
		if r.err != nil {
			out[r.name] = r.err.Error()
			ok = false
			continue
		}
		out[r.name] = "ok"
	}
	return out, ok
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}
