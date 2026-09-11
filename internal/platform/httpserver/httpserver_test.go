package httpserver_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aditya0si/event-stream-platform/internal/platform/httpserver"
	"github.com/aditya0si/event-stream-platform/internal/platform/metrics"
)

// newServer builds a server on port 0 so the kernel picks a free port and tests cannot
// collide with a running stack or with each other.
func newServer(t *testing.T, checks []httpserver.Check, probeTimeout time.Duration) *httpserver.Server {
	t.Helper()
	srv, err := httpserver.New(httpserver.Config{
		Addr:            "127.0.0.1:0",
		Log:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Registry:        metrics.NewRegistry(),
		Checks:          checks,
		ShutdownTimeout: 5 * time.Second,
		ProbeTimeout:    probeTimeout,
		EnableMetrics:   true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv
}

// start runs the server for the duration of the test and shuts it down afterwards.
func start(t *testing.T, srv *httpserver.Server) string {
	t.Helper()
	serveErr := srv.Start()
	t.Cleanup(func() {
		if err := srv.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		// The channel receives nil once Serve returns after a clean shutdown; draining it
		// keeps the goroutine from outliving the test.
		select {
		case err := <-serveErr:
			if err != nil {
				t.Errorf("serve returned: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("the server did not stop within 5s of Shutdown")
		}
	})
	return "http://" + srv.Addr()
}

func get(t *testing.T, base, path string) (int, string) {
	t.Helper()
	resp, err := http.Get(base + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body of %s: %v", path, err)
	}
	return resp.StatusCode, string(body)
}

// TestLivenessSurvivesAFailingDependency is the distinction this package exists to make:
// restarting a process cannot fix a database, so a dependency outage must fail readiness
// without failing liveness. Conflating them turns one incident into a restart loop.
func TestLivenessSurvivesAFailingDependency(t *testing.T) {
	down := errors.New("connection refused")
	srv := newServer(t, []httpserver.Check{
		{Name: "postgres", Fn: func(context.Context) error { return down }},
	}, time.Second)
	base := start(t, srv)

	code, body := get(t, base, "/healthz")
	if code != http.StatusOK {
		t.Fatalf("/healthz = %d with a dependency down, want 200 (body=%s)", code, body)
	}

	code, body = get(t, base, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d with a dependency down, want 503 (body=%s)", code, body)
	}

	var parsed struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("the readiness body is not JSON (%v): %s", err, body)
	}
	if parsed.Status != "not_ready" {
		t.Errorf("status = %q, want not_ready", parsed.Status)
	}
	if parsed.Checks["postgres"] == "ok" {
		t.Error("a failing dependency was reported as ok")
	}
	// The reason must be in the body: "not ready" with no explanation is the failure this
	// endpoint is supposed to make specific.
	if !strings.Contains(parsed.Checks["postgres"], "connection refused") {
		t.Errorf("the readiness body does not say why postgres failed: %q", parsed.Checks["postgres"])
	}
}

func TestReadinessPassesWhenEveryCheckPasses(t *testing.T) {
	srv := newServer(t, []httpserver.Check{
		{Name: "postgres", Fn: func(context.Context) error { return nil }},
		{Name: "redis", Fn: func(context.Context) error { return nil }},
		{Name: "broker", Fn: func(context.Context) error { return nil }},
	}, time.Second)
	base := start(t, srv)

	code, body := get(t, base, "/readyz")
	if code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200 (body=%s)", code, body)
	}
	var parsed struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("readiness body: %v", err)
	}
	if parsed.Status != "ready" {
		t.Errorf("status = %q, want ready", parsed.Status)
	}
	for _, dep := range []string{"postgres", "redis", "broker"} {
		if parsed.Checks[dep] != "ok" {
			t.Errorf("check %q = %q, want ok", dep, parsed.Checks[dep])
		}
	}
}

// TestASlowDependencyIsBoundedByTheProbeTimeout: a dependency that hangs must be reported
// as failing, promptly. Without a timeout per check, one unresponsive dependency would hold
// the probe open until an upstream load balancer gave up — and a slow probe is
// indistinguishable from a broken one.
func TestASlowDependencyIsBoundedByTheProbeTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	srv := newServer(t, []httpserver.Check{
		{Name: "slow", Fn: func(ctx context.Context) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}},
	}, 200*time.Millisecond)
	base := start(t, srv)

	started := time.Now()
	code, body := get(t, base, "/readyz")
	elapsed := time.Since(started)

	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d for a dependency that never answers, want 503 (body=%s)", code, body)
	}
	if elapsed > 3*time.Second {
		t.Fatalf("readiness took %s; a hanging dependency must not hold the probe open", elapsed)
	}
}

func TestMetricsEndpointIsMounted(t *testing.T) {
	srv := newServer(t, nil, time.Second)
	base := start(t, srv)

	code, body := get(t, base, "/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", code)
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Errorf("/metrics does not look like a Prometheus exposition:\n%s", first(body, 300))
	}
}

func TestUnknownPathIs404(t *testing.T) {
	srv := newServer(t, nil, time.Second)
	base := start(t, srv)
	if code, _ := get(t, base, "/nope"); code != http.StatusNotFound {
		t.Fatalf("/nope = %d, want 404", code)
	}
}

// TestReadinessChecksUpdateTheirGauges is the regression test for a defect the deployed
// smoke test caught and every other test in this file missed.
//
// The package comment promised that readiness checks keep the dependency gauges fresh so a
// failure is visible on a dashboard during a lull in traffic. runChecks never touched them,
// so db_up, redis_up, and broker_up all read 0 while the service was demonstrably healthy —
// three false alarms in exactly the place an operator looks first during an incident.
//
// The gap was assertable only from outside: every other test here checks the readiness
// *body*, which was correct the whole time. This one checks the metric, which was not.
func TestReadinessChecksUpdateTheirGauges(t *testing.T) {
	healthy := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_dep_up"})
	broken := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_dep_down"})

	srv := newServer(t, []httpserver.Check{
		{Name: "healthy", Fn: func(context.Context) error { return nil }, Gauge: healthy},
		{Name: "broken", Fn: func(context.Context) error { return errors.New("refused") }, Gauge: broken},
	}, time.Second)
	base := start(t, srv)

	code, _ := get(t, base, "/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503 with a failing dependency", code)
	}

	if got := testutil.ToFloat64(healthy); got != 1 {
		t.Errorf("the healthy dependency's gauge = %v, want 1: a dashboard would show a "+
			"false alarm while the service reports itself ready", got)
	}
	if got := testutil.ToFloat64(broken); got != 0 {
		t.Errorf("the failing dependency's gauge = %v, want 0", got)
	}
}

// TestBackgroundProbesKeepGaugesFreshWithNoTraffic covers the second half of the promise:
// the gauges must be current even when nothing is asking whether the process is ready,
// because a dependency outage is exactly when traffic stops.
//
// No HTTP request is made anywhere in this test — the only thing that can move the gauge is
// the background prober.
func TestBackgroundProbesKeepGaugesFreshWithNoTraffic(t *testing.T) {
	g := prometheus.NewGauge(prometheus.GaugeOpts{Name: "test_probe_only"})

	srv, err := httpserver.New(httpserver.Config{
		Addr:            "127.0.0.1:0",
		Log:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Registry:        metrics.NewRegistry(),
		Checks:          []httpserver.Check{{Name: "dep", Fn: func(context.Context) error { return nil }, Gauge: g}},
		ShutdownTimeout: 5 * time.Second,
		ProbeTimeout:    time.Second,
		ProbeInterval:   50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	serveErr := srv.Start()
	t.Cleanup(func() {
		if err := srv.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		select {
		case <-serveErr:
		case <-time.After(5 * time.Second):
			t.Error("the server did not stop within 5s of Shutdown")
		}
	})

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(g) == 1 {
			return // the prober did its job with no request having been made
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("the background prober never set the gauge (it reads %.0f): with no traffic, a "+
		"dependency failure would be invisible", testutil.ToFloat64(g))
}

func first(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
