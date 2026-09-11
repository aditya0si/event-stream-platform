// Command ingest is the HTTP entry point for telemetry: it validates submitted observations,
// gives each event an identity, and publishes it to the partitioned log.
//
// It also serves the operational surface — liveness, readiness, metrics — on the same
// listener, so there is one shutdown path rather than two.
//
// The process-refuses-to-start-if-a-dependency-is-down rule is deliberate: a container
// that reports healthy while unable to reach its database or its log is worse than one that
// exits, because the orchestrator's restart policy is the only recovery mechanism it has.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aditya0si/event-stream-platform/internal/ingest"
	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
	"github.com/aditya0si/event-stream-platform/internal/platform/config"
	"github.com/aditya0si/event-stream-platform/internal/platform/db"
	"github.com/aditya0si/event-stream-platform/internal/platform/httpserver"
	"github.com/aditya0si/event-stream-platform/internal/platform/logging"
	"github.com/aditya0si/event-stream-platform/internal/platform/metrics"
	"github.com/aditya0si/event-stream-platform/internal/platform/reqid"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ingest: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel)

	// Signal-aware from the start, so a SIGTERM during startup aborts cleanly rather than
	// leaving a half-initialised process.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancelStartup := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStartup()

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

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisAddr(cfg.Redis.URL),
		DialTimeout:  cfg.Redis.DialTimeout,
		ReadTimeout:  cfg.Redis.ReadTimeout,
		WriteTimeout: cfg.Redis.WriteTimeout,
	})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(startupCtx).Err(); err != nil {
		return fmt.Errorf("redis: ping failed: %w", err)
	}
	log.Info("connected to redis")

	// One broker client, used both for publishing and for the readiness probe. Two clients
	// would work, but a single connection means the health signal an operator reads describes
	// the same connection the events travel over.
	producer, err := broker.NewProducer(startupCtx, broker.Options{
		SeedBrokers: cfg.Broker.SeedBrokers,
		ClientID:    "esp-ingest",
	})
	if err != nil {
		return err
	}
	defer producer.Close()
	log.Info("connected to broker", "seeds", cfg.Broker.SeedBrokers)

	reg := metrics.NewRegistry()
	metrics.Register(reg)

	// Readiness checks double as the gauge updater: every probe refreshes db_up, redis_up,
	// and broker_up, so a dependency failure shows on a dashboard even during a lull in
	// traffic.
	checks := []httpserver.Check{
		{Name: "postgres", Fn: func(c context.Context) error { return pool.Ping(c) }, Gauge: metrics.DBUp},
		{Name: "redis", Fn: func(c context.Context) error { return rdb.Ping(c).Err() }, Gauge: metrics.RedisUp},
		{Name: "broker", Fn: func(c context.Context) error { return producer.Ping(c) }, Gauge: metrics.BrokerUp},
	}

	// The application's routes are attached to the same server as the operational ones, and
	// request-id assignment wraps the whole thing — including /healthz — so every log line a
	// probe produces can also be tied to a request.
	events, err := ingest.New(producer, ingest.Options{
		Topic:        cfg.Broker.RawTopic,
		Source:       "ingest",
		APIKey:       cfg.Ingest.APIKey,
		MaxBodyBytes: cfg.Ingest.MaxBodyBytes,
		MaxBatchSize: cfg.Ingest.MaxBatchSize,
		Log:          log,
	})
	if err != nil {
		return err
	}

	srv, err := httpserver.New(httpserver.Config{
		Addr:            cfg.Ingest.Addr,
		Log:             log,
		Registry:        reg,
		Checks:          checks,
		ReadTimeout:     cfg.Ingest.ReadTimeout,
		WriteTimeout:    cfg.Ingest.WriteTimeout,
		IdleTimeout:     cfg.Ingest.IdleTimeout,
		ShutdownTimeout: cfg.Ingest.ShutdownTimeout,
		ProbeInterval:   5 * time.Second,
		ProbeTimeout:    3 * time.Second,
		EnableMetrics:   true,
		Mount:           events.Mount,
		Wrap:            reqid.Middleware,
	})
	if err != nil {
		return err
	}

	serveErr := srv.Start()
	log.Info("ingest listening", "addr", srv.Addr(), "env", cfg.Env)

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received; draining")
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
		// A nil error here means the listener closed without a shutdown having been
		// requested. Treat it as a plain return rather than an error: the loops above
		// report the real cause.
		return errors.New("server stopped unexpectedly")
	}
}

// redisAddr extracts host:port from a Redis URL.
//
// go-redis accepts a URL via ParseURL, but that form silently ignores the caller's
// per-operation timeouts in some versions; parsing the address here and passing it
// explicitly keeps the timeouts load-bearing, which is the point of setting them.
func redisAddr(raw string) string {
	opts, err := redis.ParseURL(raw)
	if err != nil {
		// config validation has already accepted the URL; falling back to the raw string
		// is the least surprising behaviour for a value go-redis itself will parse.
		return raw
	}
	return opts.Addr
}
