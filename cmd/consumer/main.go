// Command consumer is the consumer-group member: it reads events from the log, applies them
// to Postgres through the sink, and commits offsets only after the transaction that applied
// them has committed.
//
// That ordering is the entire at-least-once story. A crash between the two means redelivery,
// never loss, and the primary key on processed_events absorbs the redelivery — which is why
// this project says "effectively-once" and never "exactly-once" (ADR-002).
//
// Scaling is by partition, not by a worker setting: several copies of this binary in the same
// consumer group split the topic's partitions between them, and adding a seventh copy to a
// six-partition topic leaves one idle. The README states the ceiling rather than implying
// unbounded horizontal scaling.
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

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/aditya0si/event-stream-platform/internal/consumer"
	"github.com/aditya0si/event-stream-platform/internal/platform/broker"
	"github.com/aditya0si/event-stream-platform/internal/platform/config"
	"github.com/aditya0si/event-stream-platform/internal/platform/db"
	"github.com/aditya0si/event-stream-platform/internal/platform/httpserver"
	"github.com/aditya0si/event-stream-platform/internal/platform/logging"
	"github.com/aditya0si/event-stream-platform/internal/platform/metrics"
	"github.com/aditya0si/event-stream-platform/internal/sink"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "consumer: %v\n", err)
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

	consumerID := consumerIdentity(cfg.Consumer.Group)

	store, err := sink.NewStore(pool, consumerID)
	if err != nil {
		return err
	}

	// A producer for the dead-letter topic. The publish path and the consume path use separate
	// clients on purpose: a produce that blocks behind a large fetch on one client would delay
	// the very refusal it is trying to record.
	dlqProducer, err := broker.NewProducer(startupCtx, broker.Options{
		SeedBrokers: cfg.Broker.SeedBrokers,
		ClientID:    "esp-consumer-dlq",
	})
	if err != nil {
		return err
	}
	defer dlqProducer.Close()

	dlqSink, err := consumer.NewDLQSink(dlqProducer, store, cfg.Broker.DLQTopic, log)
	if err != nil {
		return err
	}

	// The group client. Three options are load-bearing and are explained where they are set
	// rather than in a document the reader may not have open.
	group, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Broker.SeedBrokers...),
		kgo.ClientID("esp-consumer"),
		kgo.ConsumerGroup(cfg.Consumer.Group),
		kgo.ConsumeTopics(cfg.Broker.RawTopic),

		// Offsets are a decision this process makes after its transaction commits, not a
		// side effect of polling. With auto-commit on, franz-go would commit records the
		// moment they were handed out — silently converting the system from at-least-once to
		// at-most-once, which loses events instead of duplicating them.
		kgo.DisableAutoCommit(),

		// A rebalance cannot take a partition away while a batch from it is being processed.
		// Without this, a partition can move mid-batch and the commit that follows lands on
		// a partition this process no longer owns — the duplicate-with-rewind case the
		// franz-go docs describe.
		kgo.BlockRebalanceOnPoll(),

		// A group with no committed offset reads the retained log from the beginning. That is
		// the correct default here: the log is the system of record and it retains its
		// contents, so a fresh consumer group's job is to materialise what is already there
		// before it starts following the head.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),

		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			log.Info("partitions assigned", "group", cfg.Consumer.Group, "partitions", partitionSummary(m))
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			// Anything polled but uncommitted from these partitions will be redelivered to
			// whoever receives them. That is the expected behaviour, not a fault: the sink
			// deduplicates on event_id.
			log.Warn("partitions revoked; uncommitted work will be redelivered",
				"group", cfg.Consumer.Group, "partitions", partitionSummary(m))
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			log.Error("partitions lost (not a clean handover); uncommitted work will be redelivered",
				"group", cfg.Consumer.Group, "partitions", partitionSummary(m))
		}),
	)
	if err != nil {
		return fmt.Errorf("consumer group client: %w", err)
	}
	defer group.Close()

	worker, err := consumer.New(group, store, dlqSink, consumer.Options{
		Topic:       cfg.Broker.RawTopic,
		BatchSize:   cfg.Consumer.BatchSize,
		MaxRetries:  cfg.Consumer.MaxRetries,
		BackoffBase: cfg.Consumer.RetryBackoffBase,
		BackoffMax:  cfg.Consumer.RetryBackoffMax,
		Log:         log,
	})
	if err != nil {
		return err
	}

	reg := metrics.NewRegistry()
	metrics.Register(reg)

	checks := []httpserver.Check{
		{Name: "postgres", Fn: func(c context.Context) error { return pool.Ping(c) }, Gauge: metrics.DBUp},
		{Name: "broker", Fn: func(c context.Context) error { return broker.Ping(c, group) }, Gauge: metrics.BrokerUp},
		// Lag is part of readiness for a consumer in a way it is not for a producer: a member
		// that cannot report its own lag cannot be operated, and the gauge it feeds would go
		// stale with nothing to say so.
		{Name: "consumer_group", Fn: func(c context.Context) error { return reportLag(c, cfg, log) }},
	}

	srv, err := httpserver.New(httpserver.Config{
		Addr:            cfg.Consumer.MetricsAddr,
		Log:             log,
		Registry:        reg,
		Checks:          checks,
		ReadTimeout:     10 * time.Second,
		WriteTimeout:    15 * time.Second,
		IdleTimeout:     60 * time.Second,
		ShutdownTimeout: cfg.Consumer.ShutdownTimeout,
		ProbeInterval:   10 * time.Second,
		ProbeTimeout:    5 * time.Second,
		EnableMetrics:   true,
	})
	if err != nil {
		return err
	}

	serveErr := srv.Start()
	log.Info("consumer started",
		"metrics_addr", srv.Addr(), "group", cfg.Consumer.Group, "topic", cfg.Broker.RawTopic,
		"consumer_id", consumerID)

	// Run the consume loop in its own goroutine so the select below can observe both it and
	// the signal context. A fatal consume error surfaces through the same channel as the
	// server's, so one select handles every shutdown path.
	runErr := make(chan error, 1)
	go func() { runErr <- worker.Run(ctx) }()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received; finishing the in-flight batch")
		// The worker stops at the next context check, leaving uncommitted work to be
		// redelivered rather than committing a half-finished batch. Waiting for it here is
		// what makes the drain bounded: no new poll starts, and the batch in progress either
		// completes or is abandoned.
		select {
		case err := <-runErr:
			if err != nil {
				log.Error("consume loop ended with an error during shutdown", "err", err)
			}
		case <-time.After(cfg.Consumer.ShutdownTimeout):
			log.Warn("consume loop did not stop within the shutdown window; exiting anyway. " +
				"Uncommitted offsets mean the in-flight batch is redelivered, not lost")
		}
		if err := srv.Shutdown(context.Background()); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
		if err := <-serveErr; err != nil {
			return fmt.Errorf("serve: %w", err)
		}
		log.Info("stopped cleanly")
		return nil

	case err := <-runErr:
		if err != nil {
			return fmt.Errorf("consume loop: %w", err)
		}
		return errors.New("consume loop stopped unexpectedly")

	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("metrics server: %w", err)
		}
		return errors.New("metrics server stopped unexpectedly")
	}
}

// reportLag publishes the consumer group's lag per partition.
//
// It queries the group's committed offsets against the log's end offsets rather than tracking
// a count locally, because the number an operator needs is "how far behind the head is the
// group" — and a locally-maintained counter answers a different question while looking like
// the same one.
func reportLag(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	cl, err := broker.NewClient(ctx, broker.Options{
		SeedBrokers: cfg.Broker.SeedBrokers,
		ClientID:    "esp-consumer-lagcheck",
	})
	if err != nil {
		return err
	}
	defer cl.Close()

	lags, err := kadm.NewClient(cl).Lag(ctx, cfg.Consumer.Group)
	if err != nil {
		return fmt.Errorf("query group lag: %w", err)
	}

	described, ok := lags[cfg.Consumer.Group]
	if !ok {
		// The group has never committed anything: a consumer that has just started against an
		// empty log, or one whose first batch is still in flight. That is not a readiness
		// failure — the process is connected to its broker and its database, and there is
		// simply nothing to report yet. Failing here would leave a healthy container
		// permanently unhealthy on a quiet system, which is the false-alarm shape this project
		// has already been bitten by twice.
		//
		// An error from Lag() itself — an unreachable broker — still fails the check, and that
		// is the question this check exists to answer.
		log.Debug("consumer group has no committed offsets yet", "group", cfg.Consumer.Group)
		return nil
	}
	// Two distinct errors are returned for two distinct failures: describing the group (can we
	// see its members and state at all) and fetching its committed offsets.
	if described.DescribeErr != nil {
		return fmt.Errorf("describe group %s: %w", cfg.Consumer.Group, described.DescribeErr)
	}
	// A fetch error is logged but not fatal. Its overwhelmingly common cause is a group with no
	// committed offsets, which is normal rather than a fault (see above), and the lag gauges
	// simply keep their previous values. Failing readiness for it would take a working consumer
	// out of service on a quiet system.
	if described.FetchErr != nil {
		log.Debug("could not fetch offsets for lag reporting",
			"group", cfg.Consumer.Group, "err", described.FetchErr)
		return nil
	}

	total := int64(0)
	for _, partition := range described.Lag.Sorted() {
		if partition.Lag < 0 {
			// A negative lag means the broker could not compute it for that partition. Skipping
			// is right: adding it would silently understate the total.
			continue
		}
		metrics.ConsumerLag.WithLabelValues(fmt.Sprintf("%d", partition.Partition)).Set(float64(partition.Lag))
		total += partition.Lag
	}
	log.Debug("consumer lag", "group", cfg.Consumer.Group, "total", total)
	return nil
}

// consumerIdentity names this process in the deduplication record.
//
// Hostname and pid together, because during an incident the question is "was this applied
// twice by one process, or once each by two?" — and a constant identifier cannot answer it.
func consumerIdentity(group string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return fmt.Sprintf("%s/%s/%d", group, host, os.Getpid())
}

// partitionSummary renders an assignment as a stable string, so two log lines from different
// rebalances can be compared by eye.
func partitionSummary(m map[string][]int32) string {
	if len(m) == 0 {
		return "(none)"
	}
	out := ""
	for topic, parts := range m {
		out += fmt.Sprintf("%s:%v ", topic, parts)
	}
	return out
}
