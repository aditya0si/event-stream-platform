// Command gateway fans live positions out to browsers over Server-Sent Events.
//
// It has no producer and no consumer group. It subscribes to the fan-out bus and reads the
// durable history when a client reconnects — and that shape is why it can be restarted at any
// moment without losing anything: a reconnect reads Postgres, and the bus is only how a *live*
// frame arrives (ADR-003, ADR-009).
//
// # Why Redis is part of readiness here, and not for the consumer
//
// The two processes have genuinely different dependency graphs, and the asymmetry is deliberate
// rather than an oversight. A consumer whose Redis is unreachable is still correct: it applies
// every event to Postgres and commits its offsets on time, and the live view is simply one frame
// behind until Redis returns. A gateway whose Redis is unreachable cannot serve the thing it
// exists to serve. So Redis gates this process's readiness and not the consumer's — failing the
// consumer's readiness would take a working process out of service to report a degraded
// convenience.
//
// # Why there is no API key here yet
//
// The ingest endpoint authenticates producers with a key. This one does not authenticate
// viewers, and the reason is mechanical rather than an oversight: the browser's EventSource
// cannot set request headers, so a header-based key is not available to the client this endpoint
// exists for, and a key in the query string would land in access logs and Referer headers. The
// honest options are a signed cookie or a short-lived stream token, and neither is in scope for
// this milestone. It is recorded here, and in the README's "not yet" column, so the gap is stated
// where a reader will find it instead of being implied by silence.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aditya0si/event-stream-platform/internal/fanout"
	"github.com/aditya0si/event-stream-platform/internal/gateway"
	"github.com/aditya0si/event-stream-platform/internal/platform/config"
	"github.com/aditya0si/event-stream-platform/internal/platform/db"
	"github.com/aditya0si/event-stream-platform/internal/platform/httpserver"
	"github.com/aditya0si/event-stream-platform/internal/platform/logging"
	"github.com/aditya0si/event-stream-platform/internal/platform/metrics"
	"github.com/aditya0si/event-stream-platform/internal/platform/reqid"
	"github.com/aditya0si/event-stream-platform/internal/sink"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancelStartup := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStartup()

	// Postgres backs every resume. A gateway that could not read the history would answer a
	// reconnect with nothing and look like a quiet fleet, which is the failure this check exists
	// to prevent.
	pool, err := db.Open(startupCtx, db.Options{
		URL:             cfg.Postgres.URL,
		MaxConns:        cfg.Postgres.MaxConns,
		MinConns:        cfg.Postgres.MinConns,
		MaxConnLifetime: cfg.Postgres.MaxConnLifetime,
		ConnectTimeout:  cfg.Postgres.ConnectTimeout,
	})
	if err != nil {
		return err
	}
	defer pool.Close()
	log.Info("connected to postgres")

	// ParseURL rather than extracting host:port, so the URL's database index and TLS settings are
	// honoured instead of silently dropped. (cmd/ingest's helper parses only the address; this is
	// the form the others should converge on, and the difference is noted where it exists rather
	// than left for someone to discover.)
	redisOpts, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return fmt.Errorf("redis: parse %q: %w", cfg.Redis.URL, err)
	}
	redisOpts.DialTimeout = cfg.Redis.DialTimeout
	redisOpts.ReadTimeout = cfg.Redis.ReadTimeout
	redisOpts.WriteTimeout = cfg.Redis.WriteTimeout

	rdb := redis.NewClient(redisOpts)
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(startupCtx).Err(); err != nil {
		return fmt.Errorf("redis: ping failed: %w", err)
	}
	log.Info("connected to redis", "channel", cfg.Redis.Channel)

	// The store is this process's read path: an event id resolves to its time, positions newer
	// than a resume point, and a vehicle's current state. It comes from internal/sink rather than
	// from a query written here, so the reader and the writer cannot disagree about column order
	// or types.
	store, err := sink.NewStore(pool, gatewayIdentity())
	if err != nil {
		return err
	}

	source, err := fanout.NewSubscriber(rdb, cfg.Redis.Channel)
	if err != nil {
		return err
	}

	gw, err := gateway.New(source, store, gateway.Options{
		ClientBuffer: cfg.Gateway.ClientBuffer,
		MaxClients:   cfg.Gateway.MaxClients,
		ReplayWindow: cfg.Gateway.ReplayWindow,
		// ReplayLimit, HeartbeatInterval and MaxStreamDuration keep the gateway's own defaults:
		// none is a deployment-level knob yet, and inventing config for them would add
		// environment surface a reader has to learn without changing any behaviour.
		Log: log,
	})
	if err != nil {
		return err
	}

	reg := metrics.NewRegistry()
	metrics.Register(reg)

	checks := []httpserver.Check{
		{Name: "postgres", Fn: func(c context.Context) error { return pool.Ping(c) }, Gauge: metrics.DBUp},
		// See the package comment: Redis gates readiness for this process, and not for the
		// consumer, because without it there are no live frames to send at all.
		{Name: "redis", Fn: func(c context.Context) error { return rdb.Ping(c).Err() }, Gauge: metrics.RedisUp},
	}

	srv, err := httpserver.New(httpserver.Config{
		Addr:     cfg.Gateway.Addr,
		Log:      log,
		Registry: reg,
		Checks:   checks,
		Mount: func(mux *http.ServeMux) {
			// The stream and the page that consumes it ship in one binary, so the viewer's frame
			// names are the gateway's contract by construction rather than by agreement between
			// two repositories.
			gw.Mount(mux)
			gw.MountViewer(mux)
		},
		Wrap:          reqid.Middleware,
		EnableMetrics: true,
		ReadTimeout:   cfg.Gateway.ReadTimeout,
		// WriteTimeout stays at its configured 0 on purpose. A write deadline applies to the
		// whole response, and this response is meant to stay open: any non-zero value closes
		// every stream at that interval, which is the classic SSE bug that looks like clients
		// "randomly" reconnecting.
		WriteTimeout:    cfg.Gateway.WriteTimeout,
		IdleTimeout:     120 * time.Second,
		ShutdownTimeout: cfg.Gateway.ShutdownTimeout,
		ProbeInterval:   10 * time.Second,
		ProbeTimeout:    5 * time.Second,
	})
	if err != nil {
		return err
	}

	// Subscribe before the listener starts serving. gateway.Start returns once the subscription
	// is established, so a client that connects a moment later cannot fall into a gap between
	// "listening" and "subscribed" — a gap that would be silent rather than an error. A bus
	// failure later does not stop serving: resumes read Postgres, and the gateway logs the
	// subscription ending.
	if _, err := gw.Start(ctx); err != nil {
		return err
	}

	serveErr := srv.Start()
	log.Info("gateway started",
		"addr", srv.Addr(), "bus_channel", cfg.Redis.Channel,
		"max_clients", cfg.Gateway.MaxClients, "client_buffer", cfg.Gateway.ClientBuffer,
		"replay_window", cfg.Gateway.ReplayWindow.String())

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received; closing client streams", "clients", gw.SubscriberCount())
		// Close the streams before the listener, so each connected client receives its closing
		// frame and reconnects rather than having the socket cut underneath it.
		gw.Shutdown()
		if err := srv.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		if err := <-serveErr; err != nil {
			return fmt.Errorf("serve: %w", err)
		}
		log.Info("stopped cleanly")
		return nil

	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("serve: %w", err)
		}
		return errors.New("server stopped unexpectedly")
	}
}

// gatewayIdentity names this process in the sink's deduplication record.
//
// The gateway never applies an event, so it never writes one — but it constructs a store, and
// the store refuses an empty identity rather than accepting a placeholder. Naming the process
// honestly is better than a value that would misattribute a write if a future change ever gave
// this binary one.
func gatewayIdentity() string {
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("gateway-%s-%d", host, os.Getpid())
}
