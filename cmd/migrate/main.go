// Command migrate applies the database schema and ensures the broker topics exist.
//
// It is the one-shot setup step: `docker compose up` runs it to completion before the
// ingest process starts, so a freshly-created deployment never serves traffic against a
// schema that does not exist. It is idempotent — running it twice is a no-op — because it
// runs on every compose up and every CI invocation.
//
// Usage:
//
//	migrate all              ensure topics, then apply migrations (what compose runs)
//	migrate up               apply pending migrations only
//	migrate status           report applied and pending migrations, change nothing
//	migrate topics           ensure broker topics only
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
	"github.com/aditya0si/event-stream-platform/internal/platform/config"
	"github.com/aditya0si/event-stream-platform/internal/platform/db"
	"github.com/aditya0si/event-stream-platform/internal/platform/logging"
	"github.com/aditya0si/event-stream-platform/internal/platform/migrations"
)

func main() {
	if err := run(); err != nil {
		// The logger may not exist yet (config could not be loaded), so failures before
		// that point go to stderr directly. Once a logger exists, run() reports through it.
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	log := logging.New(cfg.LogLevel)

	cmd := "all"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "all", "up", "status", "topics":
	default:
		return fmt.Errorf("unknown command %q: want one of all, up, status, topics", cmd)
	}

	// Only connect to what the command needs, so `migrate status` works on a host with no
	// broker running and vice versa.
	if cmd == "all" || cmd == "topics" {
		if err := ensureTopics(ctx, cfg, log); err != nil {
			return err
		}
		if cmd == "topics" {
			return nil
		}
	}

	if cmd == "up" || cmd == "all" || cmd == "status" {
		if err := runMigrations(ctx, cfg, log, cmd); err != nil {
			return err
		}
	}
	return nil
}

func ensureTopics(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	// Topic creation needs a bounded attempt: the broker announces itself healthy before
	// every internal component is ready to serve metadata requests, so a single immediate
	// attempt races it on a fresh `compose up`.
	const attempts = 10
	var lastErr error
	for i := 1; i <= attempts; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		lastErr = tryEnsureTopics(ctx, cfg, log)
		if lastErr == nil {
			return nil
		}
		wait := time.Duration(i) * time.Second
		log.Warn("broker not ready for topic creation; retrying",
			"attempt", i, "of", attempts, "retry_in", wait.String(), "err", lastErr)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	return fmt.Errorf("broker: topics not ensured after %d attempts: %w", attempts, lastErr)
}

func tryEnsureTopics(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	cl, err := broker.NewClient(ctx, broker.Options{
		SeedBrokers: cfg.Broker.SeedBrokers,
		ClientID:    "esp-migrate",
	})
	if err != nil {
		return err
	}
	defer cl.Close()

	created, err := broker.EnsureTopics(ctx, cl, []broker.TopicSpec{
		{Name: cfg.Broker.RawTopic, Partitions: cfg.Broker.RawParts, Retention: cfg.Broker.Retention},
		{Name: cfg.Broker.DLQTopic, Partitions: cfg.Broker.DLQParts, Retention: cfg.Broker.DLQRetention},
	})
	if err != nil {
		return err
	}
	if len(created) == 0 {
		log.Info("broker topics already present", "raw", cfg.Broker.RawTopic, "dlq", cfg.Broker.DLQTopic)
	} else {
		log.Info("broker topics created", "topics", created)
	}

	// Assert the partition counts match the configuration rather than trusting that they
	// do. A topic created earlier by someone else with the wrong partition count would
	// otherwise cap consumer parallelism silently — and the design's whole scaling story
	// depends on that number.
	got, err := broker.DescribeTopics(ctx, cl, cfg.Broker.RawTopic, cfg.Broker.DLQTopic)
	if err != nil {
		return err
	}
	if n := got[cfg.Broker.RawTopic]; n != cfg.Broker.RawParts {
		return fmt.Errorf("topic %s has %d partitions, configuration expects %d: "+
			"recreate the topic or fix BROKER_RAW_PARTITIONS", cfg.Broker.RawTopic, n, cfg.Broker.RawParts)
	}
	if n := got[cfg.Broker.DLQTopic]; n != cfg.Broker.DLQParts {
		return fmt.Errorf("topic %s has %d partitions, configuration expects %d",
			cfg.Broker.DLQTopic, n, cfg.Broker.DLQParts)
	}
	log.Info("broker topics verified",
		"raw_partitions", got[cfg.Broker.RawTopic], "dlq_partitions", got[cfg.Broker.DLQTopic])
	return nil
}

func runMigrations(ctx context.Context, cfg config.Config, log *slog.Logger, cmd string) error {
	pool, err := db.Open(ctx, db.Options{
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

	if cmd == "status" {
		res, err := migrations.Status(ctx, pool)
		if err != nil {
			return err
		}
		log.Info("migration status", "applied", len(res.Applied), "pending", len(res.Pending))
		for _, a := range res.Applied {
			fmt.Printf("  applied  %s_%s  (%d bytes, %s)\n", a.Version, a.Name, a.Bytes,
				a.AppliedAt.UTC().Format(time.RFC3339))
		}
		for _, p := range res.Pending {
			fmt.Printf("  pending  %s_%s  (%d bytes)\n", p.Version, p.Name, p.Bytes)
		}
		if len(res.Pending) > 0 {
			// A non-zero exit tells a script that the deployment is behind, which is the
			// only reason to run `status` non-interactively.
			return errors.New("migrations are pending")
		}
		return nil
	}

	res, err := migrations.Up(ctx, pool)
	if err != nil {
		return err
	}
	if len(res.Applied) == 0 {
		log.Info("database schema already current")
		return nil
	}
	for _, a := range res.Applied {
		log.Info("migration applied", "version", a.Version, "name", a.Name, "bytes", a.Bytes)
	}
	log.Info("migrations complete", "applied", len(res.Applied))
	return nil
}
