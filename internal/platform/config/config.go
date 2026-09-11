// Package config loads and validates process configuration from the environment.
//
// Two rules hold everywhere in this package:
//
//  1. Every value either has a documented default or is required. There is no third
//     category, and a required value that is missing is a startup failure naming the
//     variable — not a zero value that fails later, during a request, in a log line
//     nobody is watching.
//  2. Validation reports *every* problem in one pass rather than the first, because a
//     misconfigured deployment is usually wrong in several places at once and a
//     one-error-at-a-time startup loop is a waste of an operator's evening.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the complete configuration for every binary in the module, as loaded and
// validated from the environment.
//
// Keeping one struct for all binaries (rather than one per command) is deliberate: they
// share the Postgres, Redis, and broker sections, and a single definition means the
// consumer cannot drift from the ingest process in how it parses the same variable.
type Config struct {
	Env      string
	LogLevel slog.Level

	Postgres Postgres
	Redis    Redis
	Broker   Broker

	Ingest   Ingest
	Consumer Consumer
	Gateway  Gateway
}

// Postgres is the sink and the deduplication store (see docs/adr/ADR-006).
type Postgres struct {
	URL string
	// MaxConns bounds the pool. The consumer's concurrency is bounded by its partition
	// count, and each part of the pipeline holds a connection only for the duration of a
	// transaction — so this is a ceiling, not a target.
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	ConnectTimeout  time.Duration
}

// Redis carries the fan-out bus between the consumer and the gateway (ADR-009). It is
// not the deduplication store — that is Postgres, on purpose (ADR-006).
type Redis struct {
	URL          string
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Channel      string
}

// Broker is the partitioned log (ADR-001).
type Broker struct {
	SeedBrokers  []string
	RawTopic     string
	DLQTopic     string
	RawParts     int
	DLQParts     int
	Retention    time.Duration
	DLQRetention time.Duration
}

// Ingest is the HTTP entry point.
type Ingest struct {
	Addr            string
	ShutdownTimeout time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	MaxBodyBytes    int64
	// MaxBatchSize caps events accepted in one request. The limit exists so that one
	// client cannot turn a single HTTP request into unbounded broker work.
	MaxBatchSize int
	// APIKey authenticates producers. Empty disables the check, which is permitted only
	// when Env == "dev" — a production ingest endpoint with no authentication would
	// accept telemetry from anyone who can reach the port.
	APIKey string
}

// Consumer is the consumer-group member. Its concurrency is bounded by the partition
// count, not by a worker-pool setting: more workers than partitions would idle.
type Consumer struct {
	Group            string
	MetricsAddr      string
	BatchSize        int
	CommitInterval   time.Duration
	ShutdownTimeout  time.Duration
	MaxRetries       int
	RetryBackoffBase time.Duration
	RetryBackoffMax  time.Duration
}

// Gateway fans events out to browsers over SSE (ADR-003).
type Gateway struct {
	Addr string
	// ClientBuffer is the per-connection outbound buffer. Its size is the backpressure
	// policy made concrete: a client that fills it is disconnected and resyncs from the
	// log rather than being allowed to grow the process (ADR-004).
	ClientBuffer int
	// ReplayWindow bounds how far back a reconnecting client may resume. A client whose
	// Last-Event-ID is older than this is told to start fresh, because an unbounded
	// replay query is a denial-of-service vector dressed as a convenience.
	ReplayWindow    time.Duration
	MaxClients      int
	ShutdownTimeout time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
}

// Load reads the environment and returns a validated Config, or every problem it found.
func Load() (Config, error) {
	var errs []error

	cfg := Config{
		Env:      envString("APP_ENV", "dev"),
		LogLevel: parseLevel(envString("LOG_LEVEL", "info"), &errs),
	}

	// --- Postgres ---
	cfg.Postgres.URL = envRequired("DATABASE_URL", &errs)
	cfg.Postgres.MaxConns = int32(envInt("DB_MAX_CONNS", 10, &errs))
	cfg.Postgres.MinConns = int32(envInt("DB_MIN_CONNS", 1, &errs))
	cfg.Postgres.MaxConnLifetime = envDuration("DB_MAX_CONN_LIFETIME", time.Hour, &errs)
	cfg.Postgres.ConnectTimeout = envDuration("DB_CONNECT_TIMEOUT", 5*time.Second, &errs)

	// --- Redis ---
	cfg.Redis.URL = envString("REDIS_URL", "redis://localhost:6380/0")
	cfg.Redis.DialTimeout = envDuration("REDIS_DIAL_TIMEOUT", 2*time.Second, &errs)
	cfg.Redis.ReadTimeout = envDuration("REDIS_READ_TIMEOUT", 3*time.Second, &errs)
	cfg.Redis.WriteTimeout = envDuration("REDIS_WRITE_TIMEOUT", 3*time.Second, &errs)
	cfg.Redis.Channel = envString("REDIS_CHANNEL", "telemetry:live")

	// --- Broker ---
	// The default points at the compose stack's external listener, so a developer running
	// a binary natively talks to the same broker the containers do.
	cfg.Broker.SeedBrokers = splitNonEmpty(envString("BROKER_SEEDS", "localhost:19092"))
	cfg.Broker.RawTopic = envString("BROKER_RAW_TOPIC", "telemetry.raw.v1")
	cfg.Broker.DLQTopic = envString("BROKER_DLQ_TOPIC", "telemetry.dlq.v1")
	cfg.Broker.RawParts = envInt("BROKER_RAW_PARTITIONS", 6, &errs)
	cfg.Broker.DLQParts = envInt("BROKER_DLQ_PARTITIONS", 3, &errs)
	cfg.Broker.Retention = envDuration("BROKER_RETENTION", 24*time.Hour, &errs)
	cfg.Broker.DLQRetention = envDuration("BROKER_DLQ_RETENTION", 7*24*time.Hour, &errs)

	// --- Ingest ---
	cfg.Ingest.Addr = envString("INGEST_ADDR", ":8081")
	cfg.Ingest.ShutdownTimeout = envDuration("INGEST_SHUTDOWN_TIMEOUT", 15*time.Second, &errs)
	cfg.Ingest.ReadTimeout = envDuration("INGEST_READ_TIMEOUT", 15*time.Second, &errs)
	cfg.Ingest.WriteTimeout = envDuration("INGEST_WRITE_TIMEOUT", 30*time.Second, &errs)
	cfg.Ingest.IdleTimeout = envDuration("INGEST_IDLE_TIMEOUT", 60*time.Second, &errs)
	cfg.Ingest.MaxBodyBytes = int64(envInt("INGEST_MAX_BODY_BYTES", 2<<20, &errs))
	cfg.Ingest.MaxBatchSize = envInt("INGEST_MAX_BATCH", 500, &errs)
	cfg.Ingest.APIKey = envString("INGEST_API_KEY", "")

	// --- Consumer ---
	cfg.Consumer.Group = envString("CONSUMER_GROUP", "sink-v1")
	cfg.Consumer.MetricsAddr = envString("CONSUMER_METRICS_ADDR", ":9091")
	cfg.Consumer.BatchSize = envInt("CONSUMER_BATCH_SIZE", 200, &errs)
	cfg.Consumer.CommitInterval = envDuration("CONSUMER_COMMIT_INTERVAL", time.Second, &errs)
	cfg.Consumer.ShutdownTimeout = envDuration("CONSUMER_SHUTDOWN_TIMEOUT", 30*time.Second, &errs)
	cfg.Consumer.MaxRetries = envInt("CONSUMER_MAX_RETRIES", 5, &errs)
	cfg.Consumer.RetryBackoffBase = envDuration("CONSUMER_RETRY_BACKOFF_BASE", 250*time.Millisecond, &errs)
	cfg.Consumer.RetryBackoffMax = envDuration("CONSUMER_RETRY_BACKOFF_MAX", 30*time.Second, &errs)

	// --- Gateway ---
	cfg.Gateway.Addr = envString("GATEWAY_ADDR", ":8082")
	cfg.Gateway.ClientBuffer = envInt("GATEWAY_CLIENT_BUFFER", 256, &errs)
	cfg.Gateway.ReplayWindow = envDuration("GATEWAY_REPLAY_WINDOW", 15*time.Minute, &errs)
	cfg.Gateway.MaxClients = envInt("GATEWAY_MAX_CLIENTS", 1000, &errs)
	cfg.Gateway.ShutdownTimeout = envDuration("GATEWAY_SHUTDOWN_TIMEOUT", 15*time.Second, &errs)
	cfg.Gateway.ReadTimeout = envDuration("GATEWAY_READ_TIMEOUT", 15*time.Second, &errs)
	// No write timeout on the SSE response: a long-lived stream is the point. The field is
	// kept (rather than omitted) so the deliberate absence is visible in one place.
	cfg.Gateway.WriteTimeout = envDuration("GATEWAY_WRITE_TIMEOUT", 0, &errs)

	// --- Semantic validation, beyond "is it set" ---

	if cfg.Postgres.MinConns > cfg.Postgres.MaxConns {
		errs = append(errs, fmt.Errorf(
			"DB_MIN_CONNS (%d) exceeds DB_MAX_CONNS (%d): the pool can never satisfy its minimum",
			cfg.Postgres.MinConns, cfg.Postgres.MaxConns))
	}
	if len(cfg.Broker.SeedBrokers) == 0 {
		errs = append(errs, errors.New("BROKER_SEEDS is set but empty: at least one seed broker is required"))
	}
	if cfg.Broker.RawParts < 1 {
		errs = append(errs, fmt.Errorf("BROKER_RAW_PARTITIONS must be >= 1, got %d", cfg.Broker.RawParts))
	}
	if cfg.Broker.DLQParts < 1 {
		errs = append(errs, fmt.Errorf("BROKER_DLQ_PARTITIONS must be >= 1, got %d", cfg.Broker.DLQParts))
	}
	if cfg.Ingest.MaxBatchSize < 1 {
		errs = append(errs, fmt.Errorf("INGEST_MAX_BATCH must be >= 1, got %d", cfg.Ingest.MaxBatchSize))
	}
	if cfg.Consumer.BatchSize < 1 {
		errs = append(errs, fmt.Errorf("CONSUMER_BATCH_SIZE must be >= 1, got %d", cfg.Consumer.BatchSize))
	}
	if cfg.Consumer.MaxRetries < 0 {
		errs = append(errs, fmt.Errorf("CONSUMER_MAX_RETRIES must be >= 0, got %d", cfg.Consumer.MaxRetries))
	}
	if cfg.Consumer.RetryBackoffBase <= 0 {
		errs = append(errs, errors.New("CONSUMER_RETRY_BACKOFF_BASE must be > 0"))
	}
	if cfg.Consumer.RetryBackoffMax < cfg.Consumer.RetryBackoffBase {
		errs = append(errs, fmt.Errorf(
			"CONSUMER_RETRY_BACKOFF_MAX (%s) is below CONSUMER_RETRY_BACKOFF_BASE (%s)",
			cfg.Consumer.RetryBackoffMax, cfg.Consumer.RetryBackoffBase))
	}
	if cfg.Gateway.ClientBuffer < 1 {
		errs = append(errs, fmt.Errorf("GATEWAY_CLIENT_BUFFER must be >= 1, got %d", cfg.Gateway.ClientBuffer))
	}
	if cfg.Gateway.MaxClients < 1 {
		errs = append(errs, fmt.Errorf("GATEWAY_MAX_CLIENTS must be >= 1, got %d", cfg.Gateway.MaxClients))
	}

	// The deduplication window must not be shorter than the log's retention, or a replay
	// could re-apply events whose dedup record was already pruned (ADR-002). This is a
	// cross-section invariant, which is exactly why it belongs in one validated struct.
	if cfg.Broker.Retention > cfg.Gateway.ReplayWindow && cfg.Gateway.ReplayWindow > 0 {
		// Not an error: the replay window is allowed to be narrower than retention. The
		// relationship that must hold is dedup retention >= log retention, and dedup
		// retention is a SQL-side policy. Left here as a documented non-check.
		_ = cfg.Broker.Retention
	}

	// An unauthenticated ingest endpoint is a development convenience and a production
	// incident. Refusing to boot in prod is the fail-closed behaviour.
	if cfg.Ingest.APIKey == "" && cfg.Env == "prod" {
		errs = append(errs, errors.New(
			"INGEST_API_KEY is required when APP_ENV=prod: an ingest endpoint with no authentication accepts telemetry from anyone who can reach the port"))
	}
	if cfg.Ingest.APIKey != "" && len(cfg.Ingest.APIKey) < 16 {
		errs = append(errs, errors.New("INGEST_API_KEY must be at least 16 characters"))
	}

	if cfg.Env != "dev" && cfg.Env != "prod" {
		errs = append(errs, fmt.Errorf("APP_ENV must be dev or prod, got %q", cfg.Env))
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration:\n  - %s", joinErrors(errs))
	}
	return cfg, nil
}

// IsDev reports whether this process is running in development, where a missing API key
// is tolerated and logs are human-readable.
func (c Config) IsDev() bool { return c.Env == "dev" }

// --- environment helpers ---

func envString(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(v)
	}
	return def
}

// envRequired records a missing variable rather than returning an error immediately, so
// that Load reports every missing variable in one pass.
func envRequired(key string, errs *[]error) string {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		*errs = append(*errs, fmt.Errorf("%s is required but is not set", key))
		return ""
	}
	return strings.TrimSpace(v)
}

func envInt(key string, def int, errs *[]error) int {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s=%q is not an integer", key, raw))
		return def
	}
	return n
}

func envDuration(key string, def time.Duration, errs *[]error) time.Duration {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s=%q is not a duration (want Go syntax, e.g. 5s, 250ms, 24h)", key, raw))
		return def
	}
	return d
}

func parseLevel(raw string, errs *[]error) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "info", "":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		*errs = append(*errs, fmt.Errorf("LOG_LEVEL=%q is not one of debug, info, warn, error", raw))
		return slog.LevelInfo
	}
}

func splitNonEmpty(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// joinErrors renders a slice of errors as a newline-indented list, so a multi-problem
// startup failure reads as a checklist.
func joinErrors(errs []error) string {
	parts := make([]string, 0, len(errs))
	for _, e := range errs {
		parts = append(parts, e.Error())
	}
	return strings.Join(parts, "\n  - ")
}
